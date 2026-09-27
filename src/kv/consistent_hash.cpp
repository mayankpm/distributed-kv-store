#include "kv/consistent_hash.h"

#include <algorithm>
#include <string>

#include "common/hash.h"

namespace kv {
namespace {

uint64_t PointFor(NodeId node, int v) {
  return Hash64("node-" + std::to_string(node) + "#" + std::to_string(v));
}

}  // namespace

void HashRing::AddNode(NodeId node) {
  if (!nodes_.insert(node).second) return;
  for (int v = 0; v < vnodes_; v++) ring_[PointFor(node, v)] = node;
}

void HashRing::RemoveNode(NodeId node) {
  if (nodes_.erase(node) == 0) return;
  for (int v = 0; v < vnodes_; v++) {
    auto it = ring_.find(PointFor(node, v));
    if (it != ring_.end() && it->second == node) ring_.erase(it);
  }
}

std::vector<NodeId> HashRing::Lookup(std::string_view key, size_t n) const {
  std::vector<NodeId> out;
  if (ring_.empty()) return out;
  n = std::min(n, nodes_.size());
  auto it = ring_.lower_bound(Hash64(key));
  for (size_t steps = 0; out.size() < n && steps < ring_.size(); steps++) {
    if (it == ring_.end()) it = ring_.begin();  // Wrap around.
    bool seen = false;
    for (NodeId id : out) seen = seen || id == it->second;
    if (!seen) out.push_back(it->second);
    ++it;
  }
  return out;
}

}  // namespace kv
