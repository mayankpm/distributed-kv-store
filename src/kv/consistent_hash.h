// Consistent hash ring with virtual nodes.
//
// Each physical node owns `vnodes` points on a 64-bit ring. A key belongs to
// the first point clockwise from its hash, and its replica set is the first
// `n` distinct physical nodes found walking clockwise from there. Adding or
// removing a node only changes ownership of the arcs next to its points, so
// roughly 1/N of keys move instead of nearly all of them (as with hash % N).
#pragma once

#include <cstdint>
#include <map>
#include <set>
#include <string_view>
#include <vector>

#include "net/transport.h"

namespace kv {

class HashRing {
 public:
  explicit HashRing(int vnodes = 64) : vnodes_(vnodes) {}

  void AddNode(NodeId node);
  void RemoveNode(NodeId node);

  // First `n` distinct nodes clockwise from hash(key). Returns fewer if the
  // ring has fewer nodes.
  std::vector<NodeId> Lookup(std::string_view key, size_t n) const;

  size_t NodeCount() const { return nodes_.size(); }

 private:
  int vnodes_;
  std::map<uint64_t, NodeId> ring_;
  std::set<NodeId> nodes_;
};

}  // namespace kv
