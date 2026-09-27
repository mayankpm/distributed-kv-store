// One replica of one partition: a Raft instance plus its LSM-backed state
// machine, and the bookkeeping that turns client requests into log entries
// and replies.
//
// Writes:  Propose -> wait until that log index is applied -> reply OK if the
//          applied entry carries our proposal's term, else WrongLeader (the
//          entry was overwritten by a new leader and the client retries).
// Reads:   ReadIndex (quorum-confirmed leadership) -> wait until the state
//          machine has applied that index -> read the LSM tree locally.
// Every pending request carries a deadline; expired ones reply Timeout.
#pragma once

#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "kv/kv_protocol.h"
#include "kv/kv_state_machine.h"
#include "net/transport.h"
#include "raft/raft.h"

namespace kv {

struct ReplicaOptions {
  NodeId self = 0;
  uint32_t partition = 0;
  std::vector<NodeId> members;
  std::string dir;
  LsmOptions lsm;
  bool sync = false;
  int election_timeout_min_ms = 150;
  int election_timeout_max_ms = 300;
  int heartbeat_ms = 50;
  int request_timeout_ms = 1000;
};

class PartitionReplica {
 public:
  using Reply = std::function<void(KvResponse)>;
  using Clock = std::chrono::steady_clock;

  static std::unique_ptr<PartitionReplica> Create(const ReplicaOptions& options, std::shared_ptr<Transport> transport,
                                                  std::string* error);
  ~PartitionReplica();

  void Start();
  void Stop();

  void OnRaftMessage(const Envelope& env) { raft_->Step(env); }
  void HandleClient(const KvRequest& req, Reply reply);

  Raft* raft() { return raft_.get(); }
  KvStateMachine* state_machine() { return sm_.get(); }

 private:
  struct PendingWrite {
    uint64_t term;
    Reply reply;
    Clock::time_point deadline;
  };
  struct PendingRead {
    std::string key;
    Reply reply;
    Clock::time_point deadline;
  };

  explicit PartitionReplica(const ReplicaOptions& options) : options_(options) {}
  void OnApply(const LogEntry& entry);
  void OnReadIndex(bool ok, uint64_t index, std::string key, Reply reply, Clock::time_point deadline);
  KvResponse ReadLocal(const std::string& key);
  KvResponse NotLeader() const;
  void SweepLoop();

  ReplicaOptions options_;
  std::unique_ptr<KvStateMachine> sm_;
  std::unique_ptr<Raft> raft_;

  std::mutex mu_;
  std::condition_variable sweep_cv_;
  uint64_t applied_ = 0;
  std::map<uint64_t, PendingWrite> pending_writes_;         // By log index.
  std::multimap<uint64_t, PendingRead> waiting_reads_;      // By read index.
  bool stopping_ = false;
  std::thread sweeper_;
};

}  // namespace kv
