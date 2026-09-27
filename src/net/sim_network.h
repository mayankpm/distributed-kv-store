// In-process simulated network for fault-injection testing.
//
// Every message is routed through a single dispatcher thread that can drop,
// delay and reorder it, and that refuses delivery across a partition or to a
// crashed node. Connectivity is checked both when a message is sent and when it
// is delivered, so a partition also swallows messages already in flight.
#pragma once

#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <set>
#include <thread>
#include <vector>

#include "net/transport.h"

namespace kv {

class SimNetwork {
 public:
  explicit SimNetwork(uint64_t seed = 1);
  ~SimNetwork();

  // Attaches a node's inbound handler. Re-registering replaces it.
  void Register(NodeId id, Handler handler);
  // Detaches a node (crash). Blocks until any in-progress delivery to it ends,
  // so the caller may destroy the handler's target afterwards.
  void Unregister(NodeId id);

  std::shared_ptr<Transport> TransportFor(NodeId id);
  void Send(Envelope env);

  // Unreliable mode drops ~10% of messages and delays the rest by 0-25ms,
  // which also reorders them.
  void SetReliable(bool reliable) { SetFaults(reliable ? 0.0 : 0.1, reliable ? 0 : 25000); }
  void SetFaults(double drop_rate, int max_delay_us);
  // Cuts a node off from everyone (or reconnects it).
  void SetConnected(NodeId id, bool connected);
  // Nodes in different groups cannot talk. Nodes in no group (e.g. clients)
  // can talk to everyone.
  void Partition(const std::vector<std::vector<NodeId>>& groups);
  void Heal();

  uint64_t MessagesSent() const { return sent_.load(); }
  uint64_t MessagesDelivered() const { return delivered_.load(); }

 private:
  struct Slot {
    std::mutex mu;
    bool alive = true;
    Handler handler;
  };
  struct Pending {
    std::chrono::steady_clock::time_point at;
    uint64_t seq;
    Envelope env;
    bool operator>(const Pending& o) const { return at != o.at ? at > o.at : seq > o.seq; }
  };

  bool CanTalkLocked(NodeId a, NodeId b) const;
  void DispatchLoop();

  mutable std::mutex mu_;
  std::condition_variable cv_;
  std::map<NodeId, std::shared_ptr<Slot>> slots_;
  std::set<NodeId> disconnected_;
  std::map<NodeId, int> group_of_;
  double drop_rate_ = 0;
  int max_delay_us_ = 0;
  std::mt19937_64 rng_;
  std::priority_queue<Pending, std::vector<Pending>, std::greater<>> queue_;
  uint64_t seq_ = 0;
  bool stop_ = false;
  std::atomic<uint64_t> sent_{0};
  std::atomic<uint64_t> delivered_{0};
  std::thread dispatcher_;
};

}  // namespace kv
