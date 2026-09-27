// Client library ("clerk"). Routes each key to its partition, finds and caches
// that partition's leader, and retries through leader changes, timeouts and
// dropped messages. Writes carry (client_id, seq) so a retry of a write that
// already committed is recognized and not applied twice.
//
// One KvClient issues one operation at a time (calls are serialized
// internally); use one client per concurrent caller.
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include "kv/cluster_config.h"
#include "kv/kv_protocol.h"
#include "net/transport.h"

namespace kv {

struct ClientOptions {
  int rpc_timeout_ms = 500;  // Per attempt.
  int op_timeout_ms = 0;     // Overall; 0 retries forever.
};

class KvClient {
 public:
  KvClient(const ClusterConfig& config, NodeId client_id, std::shared_ptr<Transport> transport,
           ClientOptions options = {});

  // Feed responses from the transport here.
  void OnMessage(const Envelope& env);

  // Each returns false (or nullopt with *ok = false) only if op_timeout_ms expires.
  std::optional<std::string> Get(const std::string& key, bool* ok = nullptr);
  bool Put(const std::string& key, const std::string& value);
  bool Append(const std::string& key, const std::string& value);
  bool Delete(const std::string& key);

  NodeId id() const { return client_id_; }

 private:
  struct Inflight {
    bool done = false;
    KvResponse resp;
  };

  bool Execute(OpType op, const std::string& key, const std::string& value, KvResponse* out);
  bool CallOnce(NodeId server, const KvRequest& req, KvResponse* out);

  Placement placement_;
  NodeId client_id_;
  std::shared_ptr<Transport> transport_;
  ClientOptions options_;

  std::mutex op_mu_;  // Serializes operations from this client.
  uint64_t seq_ = 0;
  std::map<uint32_t, NodeId> leader_cache_;

  std::mutex mu_;
  std::condition_variable cv_;
  uint64_t next_rpc_ = 1;
  std::map<uint64_t, Inflight> inflight_;
};

}  // namespace kv
