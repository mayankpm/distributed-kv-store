// Raft consensus (Ongaro & Ousterhout, "In Search of an Understandable
// Consensus Algorithm", extended version), one instance per replica group.
//
// Implemented:
//   * Leader election with randomized timeouts and the log up-to-date check.
//   * Heartbeat-based failure detection in both directions: followers start an
//     election when heartbeats stop; a leader steps down when it has not heard
//     from a quorum within an election timeout (check-quorum), so a leader cut
//     off by a partition stops serving instead of lingering.
//   * Append-only log replication with pipelining (optimistic nextIndex) and
//     the conflict-term fast backup optimization.
//   * Commit only by counting replicas for entries of the leader's own term
//     (Figure 8 safety); a no-op is appended on election to commit prior terms.
//   * ReadIndex (Raft thesis 6.4): linearizable reads confirmed by a quorum
//     heartbeat round rather than by writing to the log.
//   * Durable term/vote and log with crash recovery.
//
// Threading: all state is guarded by one mutex. A ticker thread drives
// timeouts and heartbeats; an applier thread delivers committed entries to the
// state machine in order, outside the lock.
#pragma once

#include <condition_variable>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "net/transport.h"
#include "raft/messages.h"
#include "raft/raft_storage.h"

namespace kv {

enum class Role { kFollower, kCandidate, kLeader };

const char* RoleName(Role r);

struct RaftConfig {
  NodeId id = 0;
  std::vector<NodeId> members;  // All voters in the group, including `id`.
  uint32_t group = 0;
  std::string dir;
  bool sync = false;  // fsync term/vote/log writes.
  int election_timeout_min_ms = 150;
  int election_timeout_max_ms = 300;
  int heartbeat_ms = 50;
  size_t max_entries_per_append = 512;
  // Highest index the state machine has already made durable. Entries up to
  // here are not re-applied after a restart.
  uint64_t applied_index = 0;
};

class Raft {
 public:
  using ApplyFn = std::function<void(const LogEntry&)>;
  using SendFn = std::function<void(Envelope)>;
  using Clock = std::chrono::steady_clock;

  static std::unique_ptr<Raft> Create(const RaftConfig& config, SendFn send, ApplyFn apply, std::string* error);
  ~Raft();

  void Start();
  void Stop();

  struct Proposal {
    bool is_leader = false;
    uint64_t index = 0;
    uint64_t term = 0;
  };
  // Appends a command to the leader's log. The caller learns the outcome when
  // an entry at `index` is applied: it succeeded iff that entry's term matches.
  Proposal Propose(std::string command);

  // Linearizable read barrier. On success `index` is a commit index such that
  // once the state machine has applied it, a local read reflects every write
  // that completed before ReadIndex was called.
  void ReadIndex(std::function<void(bool ok, uint64_t index)> done);

  // Inbound Raft message for this group.
  void Step(const Envelope& env);

  struct Status {
    Role role;
    uint64_t term;
    NodeId leader;
    uint64_t commit_index;
    uint64_t last_applied;
    uint64_t last_log_index;
  };
  Status GetStatus() const;
  bool IsLeader() const;
  NodeId LeaderHint() const;
  // Copy of the log, for tests that compare replicas.
  std::vector<LogEntry> LogSnapshot() const;

 private:
  struct PendingRead {
    uint64_t seq;
    uint64_t index;
    std::set<NodeId> acks;
    std::function<void(bool, uint64_t)> done;
  };

  Raft(const RaftConfig& config, SendFn send, ApplyFn apply);

  void TickerLoop();
  void ApplierLoop();

  // All of the following require mu_ held.
  void ResetElectionTimer();
  void StartElection();
  void BecomeFollower(uint64_t term);
  void BecomeLeader();
  void BroadcastAppend();
  void SendAppend(NodeId peer);
  void AdvanceCommitIndex();
  void FailPendingReads();
  void AckReads(NodeId peer, uint64_t read_seq);
  bool Persist(uint64_t term, NodeId voted_for);
  size_t Quorum() const { return config_.members.size() / 2 + 1; }
  void SendMsg(NodeId to, MsgType type, std::string payload);

  void HandleRequestVote(NodeId from, const RequestVote& req);
  void HandleRequestVoteResp(NodeId from, const RequestVoteResp& resp);
  void HandleAppendEntries(NodeId from, const AppendEntries& req);
  void HandleAppendEntriesResp(NodeId from, const AppendEntriesResp& resp);

  // Runs callbacks queued under the lock once it has been released.
  void RunDeferred(std::unique_lock<std::mutex>& lock);

  RaftConfig config_;
  SendFn send_;
  ApplyFn apply_;

  mutable std::mutex mu_;
  std::condition_variable apply_cv_;
  RaftStorage storage_;
  Role role_ = Role::kFollower;
  NodeId leader_ = 0;
  uint64_t commit_index_ = 0;
  uint64_t last_applied_ = 0;
  std::set<NodeId> votes_;
  std::map<NodeId, uint64_t> next_index_;
  std::map<NodeId, uint64_t> match_index_;
  std::map<NodeId, Clock::time_point> last_ack_;
  Clock::time_point election_deadline_;
  Clock::time_point next_heartbeat_;
  uint64_t read_seq_ = 0;
  std::vector<PendingRead> pending_reads_;
  std::vector<std::function<void()>> deferred_;
  std::mt19937_64 rng_;
  bool running_ = false;
  bool stopping_ = false;
  std::thread ticker_;
  std::thread applier_;
};

}  // namespace kv
