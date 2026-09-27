#include "kv/local_cluster.h"

#include <chrono>
#include <thread>

namespace kv {

LocalCluster::Client::~Client() {
  target->store(nullptr);
  transport->Stop();
}

std::unique_ptr<LocalCluster> LocalCluster::Start(int nodes, uint32_t partitions, int replication,
                                                  const std::string& dir, const NodeOptions& base,
                                                  std::string* error) {
  std::unique_ptr<LocalCluster> c(new LocalCluster());
  c->base_ = base;
  c->dir_ = dir;
  c->config_.partitions = partitions;
  c->config_.replication = replication;

  // Bind every listener first (port 0 = ephemeral) so the config can carry
  // the real addresses before any node starts dialing.
  for (int i = 0; i < nodes; i++) {
    auto slot = std::make_unique<Slot>();
    const NodeId id = static_cast<NodeId>(i + 1);
    Slot* raw = slot.get();
    slot->transport = std::make_shared<TcpTransport>(id, [raw](const Envelope& env) {
      if (KvNode* n = raw->target.load()) n->Deliver(env);
    });
    if (!slot->transport->Listen("127.0.0.1:0", error)) return nullptr;
    c->config_.nodes[id] = "127.0.0.1:" + std::to_string(slot->transport->listen_port());
    c->slots_.push_back(std::move(slot));
  }
  for (int i = 0; i < nodes; i++) {
    if (!c->Launch(i, false, error)) return nullptr;
  }
  return c;
}

bool LocalCluster::Launch(int i, bool rebind, std::string* error) {
  Slot& s = *slots_[i];
  const NodeId id = static_cast<NodeId>(i + 1);
  if (rebind) {
    s.transport = std::make_shared<TcpTransport>(id, [&s](const Envelope& env) {
      if (KvNode* n = s.target.load()) n->Deliver(env);
    });
    // The old listener's port can take a moment to become free again.
    bool bound = false;
    for (int attempt = 0; attempt < 50 && !bound; attempt++) {
      bound = s.transport->Listen(config_.nodes[id], error);
      if (!bound) std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    if (!bound) return false;
  }
  for (const auto& [peer, addr] : config_.nodes) {
    if (peer != id) s.transport->AddPeer(peer, addr);
  }
  NodeOptions o = base_;
  o.id = id;
  o.cluster = config_;
  o.data_dir = dir_ + "/node" + std::to_string(id);
  s.node = KvNode::Create(o, s.transport, error);
  if (!s.node) return false;
  s.target = s.node.get();
  s.node->Start();
  return true;
}

void LocalCluster::StopNode(int i) {
  Slot& s = *slots_[i];
  if (!s.node) return;
  s.transport->Stop();
  s.target = nullptr;
  s.node->Stop();
  s.node.reset();
}

bool LocalCluster::RestartNode(int i, std::string* error) {
  StopNode(i);
  return Launch(i, true, error);
}

LocalCluster::~LocalCluster() {
  for (int i = 0; i < size(); i++) StopNode(i);
}

std::unique_ptr<LocalCluster::Client> LocalCluster::NewClient(ClientOptions options) {
  auto c = std::make_unique<Client>();
  const NodeId id = (1ULL << 63) | next_client_++;
  c->target = std::make_unique<std::atomic<KvClient*>>(nullptr);
  auto* target = c->target.get();
  c->transport = std::make_shared<TcpTransport>(id, [target](const Envelope& env) {
    if (KvClient* k = target->load()) k->OnMessage(env);
  });
  for (const auto& [peer, addr] : config_.nodes) c->transport->AddPeer(peer, addr);
  c->client = std::make_unique<KvClient>(config_, id, c->transport, options);
  target->store(c->client.get());
  return c;
}

}  // namespace kv
