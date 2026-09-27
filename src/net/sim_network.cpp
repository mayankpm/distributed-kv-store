#include "net/sim_network.h"

#include <algorithm>

#include "common/platform.h"

namespace kv {
namespace {

class SimTransport : public Transport {
 public:
  SimTransport(SimNetwork* net, NodeId self) : net_(net), self_(self) {}
  void Send(Envelope env) override {
    env.from = self_;
    net_->Send(std::move(env));
  }

 private:
  SimNetwork* net_;
  NodeId self_;
};

}  // namespace

SimNetwork::SimNetwork(uint64_t seed) : rng_(seed) {
  dispatcher_ = std::thread([this] { DispatchLoop(); });
}

SimNetwork::~SimNetwork() {
  {
    std::lock_guard lock(mu_);
    stop_ = true;
  }
  cv_.notify_all();
  dispatcher_.join();
}

void SimNetwork::Register(NodeId id, Handler handler) {
  auto slot = std::make_shared<Slot>();
  slot->handler = std::move(handler);
  std::lock_guard lock(mu_);
  slots_[id] = slot;
}

void SimNetwork::Unregister(NodeId id) {
  std::shared_ptr<Slot> slot;
  {
    std::lock_guard lock(mu_);
    auto it = slots_.find(id);
    if (it == slots_.end()) return;
    slot = it->second;
    slots_.erase(it);
  }
  std::lock_guard slot_lock(slot->mu);  // Waits out an in-flight delivery.
  slot->alive = false;
}

std::shared_ptr<Transport> SimNetwork::TransportFor(NodeId id) { return std::make_shared<SimTransport>(this, id); }

bool SimNetwork::CanTalkLocked(NodeId a, NodeId b) const {
  if (disconnected_.count(a) || disconnected_.count(b)) return false;
  auto ga = group_of_.find(a), gb = group_of_.find(b);
  if (ga == group_of_.end() || gb == group_of_.end()) return true;
  return ga->second == gb->second;
}

void SimNetwork::Send(Envelope env) {
  sent_++;
  std::lock_guard lock(mu_);
  if (!CanTalkLocked(env.from, env.to)) return;
  auto at = std::chrono::steady_clock::now();
  if (drop_rate_ > 0 && static_cast<double>(rng_() % 1000000) < drop_rate_ * 1e6) return;  // Drop.
  if (max_delay_us_ > 0) at += std::chrono::microseconds(rng_() % static_cast<uint64_t>(max_delay_us_));
  queue_.push(Pending{at, seq_++, std::move(env)});
  cv_.notify_one();
}

void SimNetwork::DispatchLoop() {
  std::unique_lock lock(mu_);
  while (!stop_) {
    if (queue_.empty()) {
      cv_.wait(lock);
      continue;
    }
    const auto at = queue_.top().at;
    const auto now = std::chrono::steady_clock::now();
    if (now < at) {
      // Poll in <=1ms sleeps rather than cv.wait_until, which is only 15.6ms
      // precise on MinGW (see common/platform.h). A newer, earlier message is
      // picked up on the next iteration.
      lock.unlock();
      SleepMicros(std::min<int64_t>(std::chrono::duration_cast<std::chrono::microseconds>(at - now).count(), 1000));
      lock.lock();
      continue;
    }
    Envelope env = std::move(const_cast<Pending&>(queue_.top()).env);
    queue_.pop();
    if (!CanTalkLocked(env.from, env.to)) continue;
    auto it = slots_.find(env.to);
    if (it == slots_.end()) continue;
    std::shared_ptr<Slot> slot = it->second;

    lock.unlock();
    {
      std::lock_guard slot_lock(slot->mu);
      if (slot->alive) {
        slot->handler(env);
        delivered_++;
      }
    }
    lock.lock();
  }
}

void SimNetwork::SetFaults(double drop_rate, int max_delay_us) {
  std::lock_guard lock(mu_);
  drop_rate_ = drop_rate;
  max_delay_us_ = max_delay_us;
}

void SimNetwork::SetConnected(NodeId id, bool connected) {
  std::lock_guard lock(mu_);
  if (connected) {
    disconnected_.erase(id);
  } else {
    disconnected_.insert(id);
  }
}

void SimNetwork::Partition(const std::vector<std::vector<NodeId>>& groups) {
  std::lock_guard lock(mu_);
  group_of_.clear();
  for (size_t g = 0; g < groups.size(); g++) {
    for (NodeId id : groups[g]) group_of_[id] = static_cast<int>(g);
  }
}

void SimNetwork::Heal() {
  std::lock_guard lock(mu_);
  group_of_.clear();
  disconnected_.clear();
}

}  // namespace kv
