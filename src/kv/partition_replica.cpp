#include "kv/partition_replica.h"

namespace kv {

std::unique_ptr<PartitionReplica> PartitionReplica::Create(const ReplicaOptions& options,
                                                           std::shared_ptr<Transport> transport, std::string* error) {
  std::unique_ptr<PartitionReplica> r(new PartitionReplica(options));
  r->sm_ = KvStateMachine::Open(options.dir + "/lsm", options.lsm, error);
  if (!r->sm_) return nullptr;
  r->applied_ = r->sm_->applied_index();

  RaftConfig rc;
  rc.id = options.self;
  rc.members = options.members;
  rc.group = options.partition;
  rc.dir = options.dir + "/raft";
  rc.sync = options.sync;
  rc.election_timeout_min_ms = options.election_timeout_min_ms;
  rc.election_timeout_max_ms = options.election_timeout_max_ms;
  rc.heartbeat_ms = options.heartbeat_ms;
  rc.applied_index = r->applied_;
  PartitionReplica* raw = r.get();
  r->raft_ = Raft::Create(
      rc, [transport](Envelope e) { transport->Send(std::move(e)); },
      [raw](const LogEntry& e) { raw->OnApply(e); }, error);
  if (!r->raft_) return nullptr;
  return r;
}

PartitionReplica::~PartitionReplica() { Stop(); }

void PartitionReplica::Start() {
  raft_->Start();
  sweeper_ = std::thread([this] { SweepLoop(); });
}

void PartitionReplica::Stop() {
  {
    std::lock_guard lock(mu_);
    if (stopping_) return;
    stopping_ = true;
  }
  sweep_cv_.notify_all();
  if (sweeper_.joinable()) sweeper_.join();
  raft_->Stop();
  // Anything still pending has an unknown outcome from the client's view.
  std::vector<Reply> replies;
  {
    std::lock_guard lock(mu_);
    for (auto& [idx, w] : pending_writes_) replies.push_back(std::move(w.reply));
    for (auto& [idx, r] : waiting_reads_) replies.push_back(std::move(r.reply));
    pending_writes_.clear();
    waiting_reads_.clear();
  }
  for (auto& reply : replies) reply(KvResponse{KvStatus::kTimeout, 0, {}});
}

KvResponse PartitionReplica::NotLeader() const {
  KvResponse r;
  r.status = KvStatus::kWrongLeader;
  r.leader_hint = raft_->LeaderHint();
  return r;
}

KvResponse PartitionReplica::ReadLocal(const std::string& key) {
  KvResponse r;
  auto v = sm_->Read(key);
  r.status = v ? KvStatus::kOk : KvStatus::kNotFound;
  if (v) r.value = std::move(*v);
  return r;
}

void PartitionReplica::HandleClient(const KvRequest& req, Reply reply) {
  if (req.key.empty() || req.key[0] == KvStateMachine::kReservedPrefix) {
    reply(KvResponse{KvStatus::kInvalid, 0, {}});
    return;
  }
  const auto deadline = Clock::now() + std::chrono::milliseconds(options_.request_timeout_ms);

  if (req.op == OpType::kGet) {
    raft_->ReadIndex([this, key = req.key, reply = std::move(reply), deadline](bool ok, uint64_t index) mutable {
      OnReadIndex(ok, index, std::move(key), std::move(reply), deadline);
    });
    return;
  }

  bool proposed = false;
  {
    // Holding mu_ across Propose guarantees the pending entry is registered
    // before the applier (which also takes mu_) can apply that index.
    std::lock_guard lock(mu_);
    if (!stopping_) {
      auto p = raft_->Propose(req.Encode());
      if (p.is_leader) {
        pending_writes_[p.index] = PendingWrite{p.term, std::move(reply), deadline};
        proposed = true;
      }
    }
  }
  if (!proposed) reply(NotLeader());
}

void PartitionReplica::OnReadIndex(bool ok, uint64_t index, std::string key, Reply reply,
                                   Clock::time_point deadline) {
  if (!ok) {
    reply(NotLeader());
    return;
  }
  {
    std::lock_guard lock(mu_);
    if (applied_ < index) {
      waiting_reads_.emplace(index, PendingRead{std::move(key), std::move(reply), deadline});
      return;
    }
  }
  reply(ReadLocal(key));
}

void PartitionReplica::OnApply(const LogEntry& entry) {
  sm_->Apply(entry);

  std::vector<std::pair<Reply, KvResponse>> replies;
  std::vector<std::pair<Reply, std::string>> reads;
  {
    std::lock_guard lock(mu_);
    applied_ = entry.index;
    if (auto it = pending_writes_.find(entry.index); it != pending_writes_.end()) {
      // A different term at our index means a new leader replaced our entry.
      KvResponse r;
      r.status = it->second.term == entry.term ? KvStatus::kOk : KvStatus::kWrongLeader;
      replies.emplace_back(std::move(it->second.reply), r);
      pending_writes_.erase(it);
    }
    while (!waiting_reads_.empty() && waiting_reads_.begin()->first <= applied_) {
      auto node = waiting_reads_.extract(waiting_reads_.begin());
      reads.emplace_back(std::move(node.mapped().reply), std::move(node.mapped().key));
    }
  }
  for (auto& [reply, resp] : replies) reply(resp);
  for (auto& [reply, key] : reads) reply(ReadLocal(key));
}

void PartitionReplica::SweepLoop() {
  std::unique_lock lock(mu_);
  while (!stopping_) {
    sweep_cv_.wait_for(lock, std::chrono::milliseconds(50));
    const auto now = Clock::now();
    std::vector<Reply> expired;
    for (auto it = pending_writes_.begin(); it != pending_writes_.end();) {
      if (it->second.deadline <= now) {
        expired.push_back(std::move(it->second.reply));
        it = pending_writes_.erase(it);
      } else {
        ++it;
      }
    }
    for (auto it = waiting_reads_.begin(); it != waiting_reads_.end();) {
      if (it->second.deadline <= now) {
        expired.push_back(std::move(it->second.reply));
        it = waiting_reads_.erase(it);
      } else {
        ++it;
      }
    }
    if (expired.empty()) continue;
    lock.unlock();
    for (auto& reply : expired) reply(KvResponse{KvStatus::kTimeout, 0, {}});
    lock.lock();
  }
}

}  // namespace kv
