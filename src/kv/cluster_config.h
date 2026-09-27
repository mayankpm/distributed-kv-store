// Static cluster description and data placement.
//
// Keys are hashed into a fixed number of partitions. Each partition is a Raft
// group whose replicas are chosen by consistent hashing of the partition id
// onto the ring of nodes, `replication` nodes per group.
//
// Config file format (one directive per line, '#' starts a comment):
//   partitions 8
//   replication 3
//   vnodes 64
//   node 1 127.0.0.1:7001
//   node 2 127.0.0.1:7002
#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "kv/consistent_hash.h"
#include "net/transport.h"

namespace kv {

struct ClusterConfig {
  uint32_t partitions = 8;
  int replication = 3;
  int vnodes = 64;
  std::map<NodeId, std::string> nodes;  // id -> host:port

  static bool Parse(std::string_view text, ClusterConfig* out, std::string* error);
  static bool Load(const std::string& path, ClusterConfig* out, std::string* error);
  std::vector<NodeId> NodeIds() const;
};

class Placement {
 public:
  explicit Placement(const ClusterConfig& config);

  uint32_t PartitionFor(std::string_view key) const;
  const std::vector<NodeId>& Replicas(uint32_t partition) const { return replicas_[partition]; }
  std::vector<uint32_t> PartitionsOn(NodeId node) const;
  uint32_t partitions() const { return static_cast<uint32_t>(replicas_.size()); }

 private:
  std::vector<std::vector<NodeId>> replicas_;
};

}  // namespace kv
