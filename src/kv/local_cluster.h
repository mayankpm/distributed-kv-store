// Runs a complete cluster inside one process over real TCP loopback sockets.
// Used by the TCP integration test and the cluster benchmark.
#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "kv/kv_client.h"
#include "kv/kv_node.h"
#include "net/tcp_transport.h"

namespace kv {

class LocalCluster {
 public:
  struct Client {
    std::shared_ptr<TcpTransport> transport;
    std::unique_ptr<KvClient> client;
    std::unique_ptr<std::atomic<KvClient*>> target;
    ~Client();
    KvClient* operator->() { return client.get(); }
  };

  // `base` supplies tuning (LSM options, timeouts, sync); ids, addresses and
  // directories are filled in here.
  static std::unique_ptr<LocalCluster> Start(int nodes, uint32_t partitions, int replication, const std::string& dir,
                                             const NodeOptions& base, std::string* error);
  ~LocalCluster();

  const ClusterConfig& config() const { return config_; }
  int size() const { return static_cast<int>(slots_.size()); }
  KvNode* node(int i) { return slots_[i]->node.get(); }

  void StopNode(int i);
  // Restarts a stopped node on its original port, recovering from disk.
  bool RestartNode(int i, std::string* error);

  std::unique_ptr<Client> NewClient(ClientOptions options = {});

 private:
  struct Slot {
    std::shared_ptr<TcpTransport> transport;
    std::unique_ptr<KvNode> node;
    std::atomic<KvNode*> target{nullptr};
  };

  bool Launch(int i, bool retry_bind, std::string* error);

  ClusterConfig config_;
  NodeOptions base_;
  std::string dir_;
  std::vector<std::unique_ptr<Slot>> slots_;
  uint64_t next_client_ = 1;
};

}  // namespace kv
