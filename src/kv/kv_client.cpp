#include "kv/kv_client.h"

#include <algorithm>
#include <thread>

namespace kv {

using Clock = std::chrono::steady_clock;

KvClient::KvClient(const ClusterConfig& config, NodeId client_id, std::shared_ptr<Transport> transport,
                   ClientOptions options)
    : placement_(config), client_id_(client_id), transport_(std::move(transport)), options_(options) {}

void KvClient::OnMessage(const Envelope& env) {
  if (env.type != MsgType::kClientResponse) return;
  std::lock_guard lock(mu_);
  auto it = inflight_.find(env.rpc_id);
  if (it == inflight_.end()) return;  // Late reply to an attempt we gave up on.
  if (!it->second.resp.Decode(env.payload)) return;
  it->second.done = true;
  cv_.notify_all();
}

bool KvClient::CallOnce(NodeId server, const KvRequest& req, KvResponse* out) {
  uint64_t rpc_id;
  {
    std::lock_guard lock(mu_);
    rpc_id = next_rpc_++;
    inflight_[rpc_id] = Inflight{};
  }
  Envelope env;
  env.type = MsgType::kClientRequest;
  env.group = req.partition;
  env.from = client_id_;
  env.to = server;
  env.rpc_id = rpc_id;
  env.payload = req.Encode();
  transport_->Send(std::move(env));

  std::unique_lock lock(mu_);
  const bool done = cv_.wait_for(lock, std::chrono::milliseconds(options_.rpc_timeout_ms),
                                 [&] { return inflight_[rpc_id].done; });
  if (done) *out = inflight_[rpc_id].resp;
  inflight_.erase(rpc_id);
  return done;
}

bool KvClient::Execute(OpType op, const std::string& key, const std::string& value, KvResponse* out) {
  std::lock_guard op_lock(op_mu_);
  KvRequest req;
  req.partition = placement_.PartitionFor(key);
  req.op = op;
  req.key = key;
  req.value = value;
  req.client_id = client_id_;
  // Reads need no dedup; writes keep the same seq across every retry.
  req.seq = op == OpType::kGet ? 0 : ++seq_;

  const auto& replicas = placement_.Replicas(req.partition);
  const auto deadline = Clock::now() + std::chrono::milliseconds(options_.op_timeout_ms);
  size_t next = 0;
  NodeId target = leader_cache_.count(req.partition) ? leader_cache_[req.partition] : replicas[0];

  while (options_.op_timeout_ms <= 0 || Clock::now() < deadline) {
    KvResponse resp;
    if (CallOnce(target, req, &resp)) {
      switch (resp.status) {
        case KvStatus::kOk:
        case KvStatus::kNotFound:
          leader_cache_[req.partition] = target;
          *out = std::move(resp);
          return true;
        case KvStatus::kInvalid:
          *out = std::move(resp);
          return true;
        case KvStatus::kWrongLeader:
          if (resp.leader_hint != 0 && resp.leader_hint != target &&
              std::find(replicas.begin(), replicas.end(), resp.leader_hint) != replicas.end()) {
            target = resp.leader_hint;
            continue;
          }
          break;
        case KvStatus::kWrongPartition:
        case KvStatus::kTimeout:
          break;
      }
    }
    // No answer or no useful hint: try the next replica.
    target = replicas[next++ % replicas.size()];
    if (next % replicas.size() == 0) std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  return false;
}

std::optional<std::string> KvClient::Get(const std::string& key, bool* ok) {
  KvResponse resp;
  const bool done = Execute(OpType::kGet, key, "", &resp);
  if (ok) *ok = done;
  if (!done || resp.status != KvStatus::kOk) return std::nullopt;
  return resp.value;
}

bool KvClient::Put(const std::string& key, const std::string& value) {
  KvResponse resp;
  return Execute(OpType::kPut, key, value, &resp) && resp.status == KvStatus::kOk;
}

bool KvClient::Append(const std::string& key, const std::string& value) {
  KvResponse resp;
  return Execute(OpType::kAppend, key, value, &resp) && resp.status == KvStatus::kOk;
}

bool KvClient::Delete(const std::string& key) {
  KvResponse resp;
  return Execute(OpType::kDelete, key, "", &resp) && resp.status == KvStatus::kOk;
}

}  // namespace kv
