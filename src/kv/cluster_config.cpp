#include "kv/cluster_config.h"

#include <sstream>

#include "common/file_util.h"
#include "common/hash.h"

namespace kv {

bool ClusterConfig::Parse(std::string_view text, ClusterConfig* out, std::string* error) {
  *out = ClusterConfig{};
  std::istringstream in{std::string(text)};
  std::string line;
  int lineno = 0;
  while (std::getline(in, line)) {
    lineno++;
    if (auto hash = line.find('#'); hash != std::string::npos) line.resize(hash);
    std::istringstream ls(line);
    std::string directive;
    if (!(ls >> directive)) continue;
    bool ok = true;
    if (directive == "partitions") {
      ok = static_cast<bool>(ls >> out->partitions) && out->partitions > 0;
    } else if (directive == "replication") {
      ok = static_cast<bool>(ls >> out->replication) && out->replication > 0;
    } else if (directive == "vnodes") {
      ok = static_cast<bool>(ls >> out->vnodes) && out->vnodes > 0;
    } else if (directive == "node") {
      NodeId id;
      std::string addr;
      ok = static_cast<bool>(ls >> id >> addr) && id != 0;
      if (ok) out->nodes[id] = addr;
    } else {
      ok = false;
    }
    if (!ok) {
      *error = "config line " + std::to_string(lineno) + ": cannot parse '" + line + "'";
      return false;
    }
  }
  if (out->nodes.empty()) {
    *error = "config has no nodes";
    return false;
  }
  return true;
}

bool ClusterConfig::Load(const std::string& path, ClusterConfig* out, std::string* error) {
  std::string text;
  if (!ReadFile(path, &text)) {
    *error = "cannot read " + path;
    return false;
  }
  return Parse(text, out, error);
}

std::vector<NodeId> ClusterConfig::NodeIds() const {
  std::vector<NodeId> ids;
  for (const auto& [id, addr] : nodes) ids.push_back(id);
  return ids;
}

Placement::Placement(const ClusterConfig& config) {
  HashRing ring(config.vnodes);
  for (const auto& [id, addr] : config.nodes) ring.AddNode(id);
  for (uint32_t p = 0; p < config.partitions; p++) {
    replicas_.push_back(ring.Lookup("partition-" + std::to_string(p), static_cast<size_t>(config.replication)));
  }
}

uint32_t Placement::PartitionFor(std::string_view key) const {
  return static_cast<uint32_t>(Hash64(key, 0x5eed) % replicas_.size());
}

std::vector<uint32_t> Placement::PartitionsOn(NodeId node) const {
  std::vector<uint32_t> out;
  for (uint32_t p = 0; p < replicas_.size(); p++) {
    for (NodeId id : replicas_[p]) {
      if (id == node) out.push_back(p);
    }
  }
  return out;
}

}  // namespace kv
