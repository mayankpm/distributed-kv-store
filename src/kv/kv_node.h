// A physical server. Hosts one PartitionReplica for every partition whose
// replica set (chosen by consistent hashing) includes this node, and routes
// inbound messages: Raft traffic by group id, client requests by partition.
#pragma once

#include <map>
#include <memory>
#include <string>

#include "kv/cluster_config.h"
#include "kv/partition_replica.h"
#include "net/transport.h"

namespace kv {

struct NodeOptions {
  NodeId id = 0;
  ClusterConfig cluster;
  std::string data_dir;
  LsmOptions lsm;
  bool sync = false;
  int election_timeout_min_ms = 150;
  int election_timeout_max_ms = 300;
  int heartbeat_ms = 50;
  int request_timeout_ms = 1000;
};

class KvNode {
 public:
  static std::unique_ptr<KvNode> Create(const NodeOptions& options, std::shared_ptr<Transport> transport,
                                        std::string* error);
  ~KvNode();

  void Start();
  void Stop();
  // Inbound message from the transport.
  void Deliver(const Envelope& env);

  PartitionReplica* replica(uint32_t partition);
  const Placement& placement() const { return placement_; }
  NodeId id() const { return options_.id; }

 private:
  KvNode(const NodeOptions& options, std::shared_ptr<Transport> transport)
      : options_(options), placement_(options.cluster), transport_(std::move(transport)) {}

  NodeOptions options_;
  Placement placement_;
  std::shared_ptr<Transport> transport_;
  std::map<uint32_t, std::unique_ptr<PartitionReplica>> replicas_;
};

}  // namespace kv
