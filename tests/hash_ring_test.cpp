#include <map>
#include <set>

#include "common/hash.h"
#include "kv/cluster_config.h"
#include "kv/consistent_hash.h"
#include "test.h"

using namespace kv;

namespace {

std::map<NodeId, int> Load(const HashRing& ring, int keys) {
  std::map<NodeId, int> load;
  for (int i = 0; i < keys; i++) load[ring.Lookup("key" + std::to_string(i), 1)[0]]++;
  return load;
}

}  // namespace

TEST(ring, replicas_are_distinct_and_deterministic) {
  HashRing a(64), b(64);
  for (NodeId n = 1; n <= 5; n++) {
    a.AddNode(n);
    b.AddNode(6 - n);  // Insertion order must not matter.
  }
  for (int i = 0; i < 1000; i++) {
    const std::string k = "k" + std::to_string(i);
    auto ra = a.Lookup(k, 3);
    CHECK_EQ(ra.size(), 3u);
    CHECK_EQ(std::set<NodeId>(ra.begin(), ra.end()).size(), 3u);
    CHECK(ra == b.Lookup(k, 3));
  }
  CHECK_EQ(a.Lookup("x", 10).size(), 5u);  // Capped at the node count.
}

TEST(ring, virtual_nodes_balance_load) {
  HashRing with_vnodes(128), without(1);
  for (NodeId n = 1; n <= 10; n++) {
    with_vnodes.AddNode(n);
    without.AddNode(n);
  }
  auto spread = [](const std::map<NodeId, int>& load) {
    int lo = INT32_MAX, hi = 0;
    for (auto& [n, c] : load) {
      lo = std::min(lo, c);
      hi = std::max(hi, c);
    }
    return static_cast<double>(hi) / std::max(lo, 1);
  };
  const auto balanced = Load(with_vnodes, 100000);
  CHECK_EQ(balanced.size(), 10u);
  for (auto& [n, c] : balanced) CHECK(c > 6000 && c < 14000);  // Mean is 10000.
  CHECK(spread(balanced) < spread(Load(without, 100000)));
}

TEST(ring, adding_a_node_moves_about_one_nth_of_keys) {
  HashRing ring(128);
  for (NodeId n = 1; n <= 10; n++) ring.AddNode(n);
  const int keys = 50000;
  std::vector<NodeId> before(keys);
  for (int i = 0; i < keys; i++) before[i] = ring.Lookup("key" + std::to_string(i), 1)[0];

  ring.AddNode(11);
  int moved = 0, moved_mod = 0;
  for (int i = 0; i < keys; i++) {
    const std::string k = "key" + std::to_string(i);
    const NodeId now = ring.Lookup(k, 1)[0];
    if (now != before[i]) {
      moved++;
      CHECK_EQ(now, NodeId{11});  // Keys only ever move to the new node.
    }
    // Naive hash % N placement for comparison.
    if (Hash64(k) % 10 != Hash64(k) % 11) moved_mod++;
  }
  const double frac = static_cast<double>(moved) / keys;
  CHECK(frac > 0.05 && frac < 0.14);  // Ideal is 1/11 = 9.1%.
  CHECK(moved_mod > keys * 8 / 10);   // Modulo hashing reshuffles ~91%.
}

TEST(ring, placement_gives_each_partition_distinct_replicas) {
  ClusterConfig cfg;
  std::string err;
  CHECK(ClusterConfig::Parse(
      "partitions 16\nreplication 3\nvnodes 64\n"
      "node 1 a:1\nnode 2 a:2\nnode 3 a:3\nnode 4 a:4\nnode 5 a:5  # comment\n",
      &cfg, &err));
  Placement p(cfg);
  CHECK_EQ(p.partitions(), 16u);
  size_t hosted = 0;
  for (uint32_t i = 0; i < 16; i++) {
    const auto& r = p.Replicas(i);
    CHECK_EQ(r.size(), 3u);
    CHECK_EQ(std::set<NodeId>(r.begin(), r.end()).size(), 3u);
  }
  for (NodeId n = 1; n <= 5; n++) {
    const size_t count = p.PartitionsOn(n).size();
    CHECK(count > 0);
    hosted += count;
  }
  CHECK_EQ(hosted, 48u);
  CHECK(p.PartitionFor("hello") < 16u);
  CHECK(!ClusterConfig::Parse("partitions x\n", &cfg, &err));
}
