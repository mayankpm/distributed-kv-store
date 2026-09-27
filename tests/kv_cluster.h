// Test harness: a full multi-partition cluster of KvNodes on the simulated
// network, with helpers to crash/restart nodes, partition the network, and
// record client histories for the linearizability checker.
#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

#include "check/linearizability.h"
#include "kv/kv_client.h"
#include "kv/kv_node.h"
#include "net/sim_network.h"
#include "test.h"

namespace kvtest {

class KvCluster {
 public:
  KvCluster(int nodes, uint32_t partitions, int replication, kv::LsmOptions lsm = {}, int request_timeout_ms = 1000)
      : net_(std::make_unique<kv::SimNetwork>(nodes * 31 + partitions)),
        lsm_(lsm),
        request_timeout_ms_(request_timeout_ms) {
    config_.partitions = partitions;
    config_.replication = replication;
    for (int i = 1; i <= nodes; i++) config_.nodes[static_cast<kv::NodeId>(i)] = "sim";
    nodes_.resize(nodes);
    for (int i = 0; i < nodes; i++) Start(i);
  }

  ~KvCluster() {
    for (auto& c : clients_) net_->Unregister(c->id());
    for (size_t i = 0; i < nodes_.size(); i++) Crash(static_cast<int>(i));
  }

  kv::NodeId IdOf(int i) const { return static_cast<kv::NodeId>(i + 1); }
  int size() const { return static_cast<int>(nodes_.size()); }
  const kv::ClusterConfig& config() const { return config_; }
  kv::SimNetwork& net() { return *net_; }
  kv::KvNode* node(int i) { return nodes_[i].get(); }
  bool Up(int i) const { return nodes_[i] != nullptr; }

  void Start(int i) {
    kv::NodeOptions o;
    o.id = IdOf(i);
    o.cluster = config_;
    o.data_dir = dir_.sub("node" + std::to_string(i));
    o.lsm = lsm_;
    o.request_timeout_ms = request_timeout_ms_;
    std::string err;
    auto node = kv::KvNode::Create(o, net_->TransportFor(o.id), &err);
    if (!node) throw Failure("node create failed: " + err);
    kv::KvNode* raw = node.get();
    nodes_[i] = std::move(node);
    net_->Register(o.id, [raw](const kv::Envelope& env) { raw->Deliver(env); });
    nodes_[i]->Start();
  }

  void Crash(int i) {
    if (!nodes_[i]) return;
    net_->Unregister(IdOf(i));
    nodes_[i]->Stop();
    nodes_[i].reset();
  }

  kv::KvClient* NewClient(kv::ClientOptions options = {}) {
    const kv::NodeId id = 1000000 + clients_.size();
    auto c = std::make_unique<kv::KvClient>(config_, id, net_->TransportFor(id), options);
    kv::KvClient* raw = c.get();
    net_->Register(id, [raw](const kv::Envelope& env) { raw->OnMessage(env); });
    clients_.push_back(std::move(c));
    return raw;
  }

  // Blocks until every partition has an elected leader.
  void WaitForLeaders(int timeout_ms = 5000) {
    kv::Placement placement(config_);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
      bool all = true;
      for (uint32_t p = 0; p < config_.partitions && all; p++) {
        bool has = false;
        for (kv::NodeId id : placement.Replicas(p)) {
          const int i = static_cast<int>(id - 1);
          if (nodes_[i] && nodes_[i]->replica(p)->raft()->IsLeader()) has = true;
        }
        all = has;
      }
      if (all) return;
      SleepMs(20);
    }
    throw Failure("partitions did not all elect leaders");
  }

 private:
  TempDir dir_;
  std::unique_ptr<kv::SimNetwork> net_;
  kv::LsmOptions lsm_;
  int request_timeout_ms_;
  kv::ClusterConfig config_;
  std::vector<std::unique_ptr<kv::KvNode>> nodes_;
  std::vector<std::unique_ptr<kv::KvClient>> clients_;
};

// Thread-safe log of completed client operations.
class History {
 public:
  static int64_t Now() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
  }
  void Add(kv::KvOperation op) {
    std::lock_guard lock(mu_);
    ops_.push_back(std::move(op));
  }
  std::vector<kv::KvOperation> ops() {
    std::lock_guard lock(mu_);
    return ops_;
  }

 private:
  std::mutex mu_;
  std::vector<kv::KvOperation> ops_;
};

// Random get/put/append/delete loop over a small key set, so clients contend
// on the same keys. Appended values are unique, which makes the checker very
// sensitive to lost, duplicated or reordered writes.
inline void RunWorkload(kv::KvClient* client, int client_idx, const std::vector<std::string>& keys,
                        std::atomic<bool>& stop, History& history, uint64_t seed) {
  std::mt19937_64 rng(seed);
  int n = 0;
  while (!stop) {
    kv::KvOperation op;
    op.client = client_idx;
    op.key = keys[rng() % keys.size()];
    const int dice = static_cast<int>(rng() % 100);
    op.call = History::Now();
    if (dice < 40) {
      op.op = kv::OpType::kGet;
      op.output = client->Get(op.key).value_or("");
    } else if (dice < 60) {
      op.op = kv::OpType::kPut;
      op.input = "p" + std::to_string(client_idx) + "." + std::to_string(n++) + ";";
      client->Put(op.key, op.input);
    } else if (dice < 95) {
      op.op = kv::OpType::kAppend;
      op.input = "a" + std::to_string(client_idx) + "." + std::to_string(n++) + ";";
      client->Append(op.key, op.input);
    } else {
      op.op = kv::OpType::kDelete;
      client->Delete(op.key);
    }
    op.ret = History::Now();
    history.Add(std::move(op));
  }
}

}  // namespace kvtest
