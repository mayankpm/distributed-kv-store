#include "raft/raft.h"

#include <algorithm>
#include <cstdio>

#include "common/platform.h"

namespace kv {

const char* RoleName(Role r) {
  switch (r) {
    case Role::kFollower: return "follower";
    case Role::kCandidate: return "candidate";
    case Role::kLeader: return "leader";
  }
  return "?";
}

std::unique_ptr<Raft> Raft::Create(const RaftConfig& config, SendFn send, ApplyFn apply, std::string* error) {
  std::unique_ptr<Raft> r(new Raft(config, std::move(send), std::move(apply)));
  if (!r->storage_.Open(config.dir, config.sync, error)) return nullptr;
  r->commit_index_ = r->last_applied_ = std::min(config.applied_index, r->storage_.LastIndex());
  return r;
}

Raft::Raft(const RaftConfig& config, SendFn send, ApplyFn apply)
    : config_(config),
      send_(std::move(send)),
      apply_(std::move(apply)),
      rng_(config.id * 0x9E3779B97F4A7C15ULL ^ static_cast<uint64_t>(Clock::now().time_since_epoch().count())) {}

Raft::~Raft() { Stop(); }

void Raft::Start() {
  std::lock_guard lock(mu_);
  if (running_) return;
  running_ = true;
  ResetElectionTimer();
  ticker_ = std::thread([this] { TickerLoop(); });
  applier_ = std::thread([this] { ApplierLoop(); });
}

void Raft::Stop() {
  {
    std::lock_guard lock(mu_);
    if (!running_ || stopping_) return;
    stopping_ = true;
    FailPendingReads();
  }
  apply_cv_.notify_all();
  if (ticker_.joinable()) ticker_.join();
  if (applier_.joinable()) applier_.join();
  std::vector<std::function<void()>> deferred;
  {
    std::lock_guard lock(mu_);
    deferred.swap(deferred_);
    running_ = false;
  }
  for (auto& f : deferred) f();
}

void Raft::RunDeferred(std::unique_lock<std::mutex>& lock) {
  while (!deferred_.empty()) {
    std::vector<std::function<void()>> batch;
    batch.swap(deferred_);
    lock.unlock();
    for (auto& f : batch) f();
    lock.lock();
  }
}

// ---------------------------------------------------------------------------
// Background threads

void Raft::TickerLoop() {
  std::unique_lock lock(mu_);
  while (!stopping_) {
    // Not a timed condition wait: those are only 15.6ms precise on MinGW.
    lock.unlock();
    SleepMicros(5000);
    lock.lock();
    if (stopping_) break;
    const auto now = Clock::now();
    if (role_ == Role::kLeader) {
      if (now >= next_heartbeat_) {
        BroadcastAppend();
        next_heartbeat_ = now + std::chrono::milliseconds(config_.heartbeat_ms);
      }
      // Check-quorum: a leader that cannot reach a majority for a full election
      // timeout has probably been partitioned away and must stop acting as leader.
      size_t reachable = 1;
      const auto window = std::chrono::milliseconds(config_.election_timeout_max_ms);
      for (NodeId p : config_.members) {
        if (p != config_.id && now - last_ack_[p] <= window) reachable++;
      }
      if (reachable < Quorum()) BecomeFollower(storage_.term());
    } else if (now >= election_deadline_) {
      StartElection();
    }
    RunDeferred(lock);
  }
}

void Raft::ApplierLoop() {
  std::unique_lock lock(mu_);
  while (true) {
    apply_cv_.wait(lock, [&] { return stopping_ || last_applied_ < commit_index_; });
    if (stopping_) break;
    const auto& log = storage_.log();
    std::vector<LogEntry> batch(log.begin() + static_cast<std::ptrdiff_t>(last_applied_ + 1),
                                log.begin() + static_cast<std::ptrdiff_t>(commit_index_ + 1));
    lock.unlock();
    for (const LogEntry& e : batch) apply_(e);
    lock.lock();
    last_applied_ = batch.back().index;
  }
}

// ---------------------------------------------------------------------------
// Role transitions

void Raft::ResetElectionTimer() {
  std::uniform_int_distribution<int> dist(config_.election_timeout_min_ms, config_.election_timeout_max_ms);
  election_deadline_ = Clock::now() + std::chrono::milliseconds(dist(rng_));
}

bool Raft::Persist(uint64_t term, NodeId voted_for) {
  if (!storage_.SetHardState(term, voted_for)) {
    std::fprintf(stderr, "raft %llu: failed to persist hard state\n", static_cast<unsigned long long>(config_.id));
    return false;
  }
  return true;
}

void Raft::SendMsg(NodeId to, MsgType type, std::string payload) {
  Envelope env;
  env.type = type;
  env.group = config_.group;
  env.from = config_.id;
  env.to = to;
  env.payload = std::move(payload);
  send_(std::move(env));
}

void Raft::StartElection() {
  role_ = Role::kCandidate;
  leader_ = 0;
  FailPendingReads();
  Persist(storage_.term() + 1, config_.id);
  votes_ = {config_.id};
  ResetElectionTimer();
  if (votes_.size() >= Quorum()) {
    BecomeLeader();
    return;
  }
  RequestVote req{storage_.term(), config_.id, storage_.LastIndex(), storage_.LastTerm()};
  const std::string payload = req.Encode();
  for (NodeId p : config_.members) {
    if (p != config_.id) SendMsg(p, MsgType::kRequestVote, payload);
  }
}

void Raft::BecomeFollower(uint64_t term) {
  if (term > storage_.term()) {
    Persist(term, 0);
    leader_ = 0;
  }
  if (role_ == Role::kLeader) {
    // The deadline was not maintained while leading; start a fresh one.
    ResetElectionTimer();
    leader_ = 0;
  }
  if (role_ != Role::kFollower) FailPendingReads();
  role_ = Role::kFollower;
  // Deliberately no timer reset on a mere term bump: otherwise a candidate with
  // a stale log could keep everyone from ever timing out.
}

void Raft::BecomeLeader() {
  role_ = Role::kLeader;
  leader_ = config_.id;
  const auto now = Clock::now();
  for (NodeId p : config_.members) {
    if (p == config_.id) continue;
    next_index_[p] = storage_.LastIndex() + 1;
    match_index_[p] = 0;
    last_ack_[p] = now;
  }
  // A no-op in the new term lets the leader commit entries from earlier terms
  // and is required before ReadIndex can serve reads.
  LogEntry noop;
  noop.term = storage_.term();
  noop.index = storage_.LastIndex() + 1;
  noop.type = EntryType::kNoop;
  storage_.Append({noop});
  BroadcastAppend();
  next_heartbeat_ = now + std::chrono::milliseconds(config_.heartbeat_ms);
  AdvanceCommitIndex();
}

// ---------------------------------------------------------------------------
// Replication (leader side)

void Raft::BroadcastAppend() {
  for (NodeId p : config_.members) {
    if (p != config_.id) SendAppend(p);
  }
}

void Raft::SendAppend(NodeId peer) {
  const uint64_t next = next_index_[peer];
  const uint64_t last = storage_.LastIndex();
  AppendEntries req;
  req.term = storage_.term();
  req.leader = config_.id;
  req.prev_log_index = next - 1;
  req.prev_log_term = storage_.TermAt(next - 1);
  req.leader_commit = commit_index_;
  req.read_seq = read_seq_;
  const uint64_t end = std::min<uint64_t>(last, next + config_.max_entries_per_append - 1);
  const auto& log = storage_.log();
  for (uint64_t i = next; i <= end; i++) req.entries.push_back(log[i]);
  // Pipelining: assume delivery and keep streaming. A lost message shows up as
  // a rejection on the next heartbeat, which rewinds nextIndex.
  next_index_[peer] = end + 1;
  SendMsg(peer, MsgType::kAppendEntries, req.Encode());
}

void Raft::AdvanceCommitIndex() {
  std::vector<uint64_t> matched = {storage_.LastIndex()};
  for (NodeId p : config_.members) {
    if (p != config_.id) matched.push_back(match_index_[p]);
  }
  std::sort(matched.begin(), matched.end(), std::greater<>());
  const uint64_t n = matched[Quorum() - 1];  // Highest index stored on a majority.
  // Only entries from the current term are committed by counting replicas
  // (Raft paper Figure 8); earlier entries commit indirectly.
  if (n > commit_index_ && storage_.TermAt(n) == storage_.term()) {
    commit_index_ = n;
    apply_cv_.notify_one();
  }
}

// ---------------------------------------------------------------------------
// Client-facing API

Raft::Proposal Raft::Propose(std::string command) {
  std::lock_guard lock(mu_);
  Proposal p;
  if (!running_ || stopping_ || role_ != Role::kLeader) return p;
  LogEntry e;
  e.term = storage_.term();
  e.index = storage_.LastIndex() + 1;
  e.type = EntryType::kCommand;
  e.data = std::move(command);
  if (!storage_.Append({e})) return p;
  p.is_leader = true;
  p.index = e.index;
  p.term = e.term;
  BroadcastAppend();
  AdvanceCommitIndex();  // Single-node groups commit immediately.
  return p;
}

void Raft::ReadIndex(std::function<void(bool, uint64_t)> done) {
  std::unique_lock lock(mu_);
  if (!running_ || stopping_ || role_ != Role::kLeader) {
    deferred_.push_back([done = std::move(done)] { done(false, 0); });
  } else if (storage_.TermAt(commit_index_) != storage_.term()) {
    // Until this term's no-op commits, the leader may not know the true commit index.
    deferred_.push_back([done = std::move(done)] { done(false, 0); });
  } else if (Quorum() == 1) {
    deferred_.push_back([done = std::move(done), idx = commit_index_] { done(true, idx); });
  } else {
    pending_reads_.push_back({++read_seq_, commit_index_, {}, std::move(done)});
    BroadcastAppend();
  }
  RunDeferred(lock);
}

void Raft::AckReads(NodeId peer, uint64_t read_seq) {
  if (pending_reads_.empty() || read_seq == 0) return;
  auto it = pending_reads_.begin();
  while (it != pending_reads_.end()) {
    if (it->seq <= read_seq) it->acks.insert(peer);
    if (it->acks.size() + 1 >= Quorum()) {
      deferred_.push_back([done = std::move(it->done), idx = it->index] { done(true, idx); });
      it = pending_reads_.erase(it);
    } else {
      ++it;
    }
  }
}

void Raft::FailPendingReads() {
  for (auto& r : pending_reads_) deferred_.push_back([done = std::move(r.done)] { done(false, 0); });
  pending_reads_.clear();
}

// ---------------------------------------------------------------------------
// Message handling

void Raft::Step(const Envelope& env) {
  std::unique_lock lock(mu_);
  if (!running_ || stopping_) return;
  switch (env.type) {
    case MsgType::kRequestVote: {
      RequestVote m;
      if (m.Decode(env.payload)) HandleRequestVote(env.from, m);
      break;
    }
    case MsgType::kRequestVoteResp: {
      RequestVoteResp m;
      if (m.Decode(env.payload)) HandleRequestVoteResp(env.from, m);
      break;
    }
    case MsgType::kAppendEntries: {
      AppendEntries m;
      if (m.Decode(env.payload)) HandleAppendEntries(env.from, m);
      break;
    }
    case MsgType::kAppendEntriesResp: {
      AppendEntriesResp m;
      if (m.Decode(env.payload)) HandleAppendEntriesResp(env.from, m);
      break;
    }
    default:
      break;
  }
  RunDeferred(lock);
}

void Raft::HandleRequestVote(NodeId from, const RequestVote& req) {
  if (req.term > storage_.term()) BecomeFollower(req.term);
  const bool log_ok = req.last_log_term > storage_.LastTerm() ||
                      (req.last_log_term == storage_.LastTerm() && req.last_log_index >= storage_.LastIndex());
  const bool grant = req.term == storage_.term() &&
                     (storage_.voted_for() == 0 || storage_.voted_for() == req.candidate) && log_ok;
  if (grant) {
    Persist(storage_.term(), req.candidate);
    ResetElectionTimer();
  }
  SendMsg(from, MsgType::kRequestVoteResp, RequestVoteResp{storage_.term(), grant}.Encode());
}

void Raft::HandleRequestVoteResp(NodeId from, const RequestVoteResp& resp) {
  if (resp.term > storage_.term()) {
    BecomeFollower(resp.term);
    return;
  }
  if (role_ != Role::kCandidate || resp.term != storage_.term() || !resp.granted) return;
  votes_.insert(from);
  if (votes_.size() >= Quorum()) BecomeLeader();
}

void Raft::HandleAppendEntries(NodeId from, const AppendEntries& req) {
  AppendEntriesResp resp;
  resp.read_seq = req.read_seq;
  if (req.term < storage_.term()) {
    resp.term = storage_.term();
    SendMsg(from, MsgType::kAppendEntriesResp, resp.Encode());
    return;
  }
  if (req.term > storage_.term() || role_ != Role::kFollower) BecomeFollower(req.term);
  resp.term = storage_.term();
  leader_ = req.leader;
  ResetElectionTimer();  // Heartbeat received: the leader is alive.

  const uint64_t last = storage_.LastIndex();
  if (req.prev_log_index > last) {
    resp.conflict_index = last + 1;
    SendMsg(from, MsgType::kAppendEntriesResp, resp.Encode());
    return;
  }
  if (storage_.TermAt(req.prev_log_index) != req.prev_log_term) {
    resp.conflict_term = storage_.TermAt(req.prev_log_index);
    uint64_t i = req.prev_log_index;
    while (i > 1 && storage_.TermAt(i - 1) == resp.conflict_term) i--;
    resp.conflict_index = i;
    SendMsg(from, MsgType::kAppendEntriesResp, resp.Encode());
    return;
  }

  // Skip entries we already have; truncate at the first conflict and append the rest.
  uint64_t index = req.prev_log_index + 1;
  size_t i = 0;
  for (; i < req.entries.size(); i++, index++) {
    if (index > storage_.LastIndex()) break;
    if (storage_.TermAt(index) != req.entries[i].term) {
      if (index <= commit_index_) {
        std::fprintf(stderr, "raft %llu: BUG: truncating committed entry %llu\n",
                     static_cast<unsigned long long>(config_.id), static_cast<unsigned long long>(index));
      }
      storage_.TruncateFrom(index);
      break;
    }
  }
  if (i < req.entries.size()) {
    storage_.Append(std::vector<LogEntry>(req.entries.begin() + static_cast<std::ptrdiff_t>(i), req.entries.end()));
  }

  const uint64_t match = req.prev_log_index + req.entries.size();
  if (req.leader_commit > commit_index_) {
    const uint64_t new_commit = std::min(req.leader_commit, match);
    if (new_commit > commit_index_) {
      commit_index_ = new_commit;
      apply_cv_.notify_one();
    }
  }
  resp.success = true;
  resp.match_index = match;
  SendMsg(from, MsgType::kAppendEntriesResp, resp.Encode());
}

void Raft::HandleAppendEntriesResp(NodeId from, const AppendEntriesResp& resp) {
  if (resp.term > storage_.term()) {
    BecomeFollower(resp.term);
    return;
  }
  if (role_ != Role::kLeader || resp.term != storage_.term()) return;
  last_ack_[from] = Clock::now();
  // Any same-term reply proves `from` still recognizes this leader.
  AckReads(from, resp.read_seq);

  if (resp.success) {
    if (resp.match_index > match_index_[from]) {
      match_index_[from] = resp.match_index;
      AdvanceCommitIndex();
    }
    next_index_[from] = std::max(next_index_[from], match_index_[from] + 1);
    return;
  }

  uint64_t next = resp.conflict_index;
  if (resp.conflict_term != 0) {
    // If we have entries from the conflicting term, resume just after our last one.
    for (uint64_t i = storage_.LastIndex(); i > 0; i--) {
      const uint64_t t = storage_.TermAt(i);
      if (t == resp.conflict_term) {
        next = i + 1;
        break;
      }
      if (t < resp.conflict_term) break;
    }
  }
  next = std::clamp<uint64_t>(next, match_index_[from] + 1, storage_.LastIndex() + 1);
  next_index_[from] = next;
  SendAppend(from);
}

// ---------------------------------------------------------------------------
// Introspection

Raft::Status Raft::GetStatus() const {
  std::lock_guard lock(mu_);
  return Status{role_, storage_.term(), leader_, commit_index_, last_applied_, storage_.LastIndex()};
}

bool Raft::IsLeader() const {
  std::lock_guard lock(mu_);
  return role_ == Role::kLeader;
}

NodeId Raft::LeaderHint() const {
  std::lock_guard lock(mu_);
  return leader_;
}

std::vector<LogEntry> Raft::LogSnapshot() const {
  std::lock_guard lock(mu_);
  return storage_.log();
}

}  // namespace kv
