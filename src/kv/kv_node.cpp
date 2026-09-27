#include "kv/kv_node.h"

namespace kv {

std::unique_ptr<KvNode> KvNode::Create(const NodeOptions& options, std::shared_ptr<Transport> transport,
                                       std::string* error) {
  std::unique_ptr<KvNode> node(new KvNode(options, std::move(transport)));
  for (uint32_t p : node->placement_.PartitionsOn(options.id)) {
    ReplicaOptions ro;
    ro.self = options.id;
    ro.partition = p;
    ro.members = node->placement_.Replicas(p);
    ro.dir = options.data_dir + "/p" + std::to_string(p);
    ro.lsm = options.lsm;
    ro.sync = options.sync;
    ro.election_timeout_min_ms = options.election_timeout_min_ms;
    ro.election_timeout_max_ms = options.election_timeout_max_ms;
    ro.heartbeat_ms = options.heartbeat_ms;
    ro.request_timeout_ms = options.request_timeout_ms;
    auto replica = PartitionReplica::Create(ro, node->transport_, error);
    if (!replica) return nullptr;
    node->replicas_[p] = std::move(replica);
  }
  return node;
}

KvNode::~KvNode() { Stop(); }

void KvNode::Start() {
  for (auto& [p, r] : replicas_) r->Start();
}

void KvNode::Stop() {
  for (auto& [p, r] : replicas_) r->Stop();
}

PartitionReplica* KvNode::replica(uint32_t partition) {
  auto it = replicas_.find(partition);
  return it == replicas_.end() ? nullptr : it->second.get();
}

void KvNode::Deliver(const Envelope& env) {
  if (env.type != MsgType::kClientRequest) {
    if (PartitionReplica* r = replica(env.group)) r->OnRaftMessage(env);
    return;
  }

  auto reply = [transport = transport_, self = options_.id, to = env.from, rpc_id = env.rpc_id,
                group = env.group](KvResponse resp) {
    Envelope out;
    out.type = MsgType::kClientResponse;
    out.group = group;
    out.from = self;
    out.to = to;
    out.rpc_id = rpc_id;
    out.payload = resp.Encode();
    transport->Send(std::move(out));
  };

  KvRequest req;
  if (!req.Decode(env.payload)) {
    reply(KvResponse{KvStatus::kInvalid, 0, {}});
    return;
  }
  PartitionReplica* r = replica(req.partition);
  if (r == nullptr || placement_.PartitionFor(req.key) != req.partition) {
    reply(KvResponse{KvStatus::kWrongPartition, 0, {}});
    return;
  }
  r->HandleClient(req, std::move(reply));
}

}  // namespace kv
