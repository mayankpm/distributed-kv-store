// kvnode: runs one server of the cluster.
//
//   kvnode --config cluster.conf --id 1 [--data ./data] [--listen 0.0.0.0:7001] [--sync] [--status]
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>

#include "common/platform.h"
#include "kv/kv_node.h"
#include "net/tcp_transport.h"

using namespace kv;

namespace {

std::atomic<bool> g_stop{false};

void OnSignal(int) { g_stop = true; }

void Usage() {
  std::fprintf(stderr,
               "usage: kvnode --config FILE --id N [--data DIR] [--listen HOST:PORT] [--sync] [--status]\n"
               "  --data     data directory (default ./data); node files go in DIR/node<N>\n"
               "  --listen   bind address (default: this node's address from the config)\n"
               "  --sync     fsync Raft and WAL writes (durable across power loss)\n"
               "  --status   print each partition's Raft role every 2s\n");
}

}  // namespace

int main(int argc, char** argv) {
  kv::EnableHighResolutionTimers();
  std::string config_path, data_dir = "data", listen;
  NodeId id = 0;
  bool sync = false, status = false;
  for (int i = 1; i < argc; i++) {
    const std::string a = argv[i];
    auto next = [&]() -> std::string { return i + 1 < argc ? argv[++i] : ""; };
    if (a == "--config") config_path = next();
    else if (a == "--id") id = std::stoull(next());
    else if (a == "--data") data_dir = next();
    else if (a == "--listen") listen = next();
    else if (a == "--sync") sync = true;
    else if (a == "--status") status = true;
    else {
      Usage();
      return 2;
    }
  }
  if (config_path.empty() || id == 0) {
    Usage();
    return 2;
  }

  ClusterConfig cluster;
  std::string err;
  if (!ClusterConfig::Load(config_path, &cluster, &err)) {
    std::fprintf(stderr, "kvnode: %s\n", err.c_str());
    return 1;
  }
  if (!cluster.nodes.count(id)) {
    std::fprintf(stderr, "kvnode: node %llu is not in %s\n", static_cast<unsigned long long>(id), config_path.c_str());
    return 1;
  }
  if (listen.empty()) listen = cluster.nodes[id];

  std::atomic<KvNode*> node_ptr{nullptr};
  auto transport = std::make_shared<TcpTransport>(id, [&node_ptr](const Envelope& env) {
    if (KvNode* n = node_ptr.load()) n->Deliver(env);
  });
  if (!transport->Listen(listen, &err)) {
    std::fprintf(stderr, "kvnode: %s\n", err.c_str());
    return 1;
  }
  for (const auto& [peer, addr] : cluster.nodes) {
    if (peer != id) transport->AddPeer(peer, addr);
  }

  NodeOptions options;
  options.id = id;
  options.cluster = cluster;
  options.data_dir = data_dir + "/node" + std::to_string(id);
  options.sync = sync;
  auto node = KvNode::Create(options, transport, &err);
  if (!node) {
    std::fprintf(stderr, "kvnode: %s\n", err.c_str());
    return 1;
  }
  node_ptr = node.get();
  node->Start();

  std::string parts;
  for (uint32_t p : node->placement().PartitionsOn(id)) parts += (parts.empty() ? "" : ",") + std::to_string(p);
  std::printf("kvnode %llu listening on %s, data in %s, replicating partitions [%s] of %u\n",
              static_cast<unsigned long long>(id), listen.c_str(), options.data_dir.c_str(), parts.c_str(),
              cluster.partitions);
  std::fflush(stdout);

  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);
  auto next_status = std::chrono::steady_clock::now();
  while (!g_stop) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    if (status && std::chrono::steady_clock::now() >= next_status) {
      next_status += std::chrono::seconds(2);
      std::string line;
      for (uint32_t p : node->placement().PartitionsOn(id)) {
        const auto s = node->replica(p)->raft()->GetStatus();
        char buf[128];
        std::snprintf(buf, sizeof(buf), " p%u:%s(term=%llu,commit=%llu)", p, RoleName(s.role),
                      static_cast<unsigned long long>(s.term), static_cast<unsigned long long>(s.commit_index));
        line += buf;
      }
      std::printf("status%s\n", line.c_str());
      std::fflush(stdout);
    }
  }

  std::printf("kvnode %llu shutting down\n", static_cast<unsigned long long>(id));
  transport->Stop();
  node_ptr = nullptr;
  node->Stop();
  return 0;
}
