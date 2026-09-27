// End-to-end tests of the sharded, replicated KV store on the simulated
// network. The fault-injection tests run concurrent clients against a 5-node,
// 3-partition, 3-way-replicated cluster while a "nemesis" thread partitions
// the network, crashes and restarts nodes (recovering from disk), and makes
// the network drop/delay/reorder messages. Every recorded history must pass
// the linearizability checker.
#include <cstdio>
#include <set>

#include "kv_cluster.h"

using namespace kv;
using kvtest::History;
using kvtest::KvCluster;

namespace {

struct ChaosOptions {
  bool partitions = false;
  bool crashes = false;
  bool unreliable = false;
  int seconds = 5;
  int clients = 5;
  int keys = 4;
  // Short timeouts retry fast on lossy links. Timeouts longer than the
  // server's own request timeout make clients act on the server's answer
  // instead of retrying past it, which exposes wrong acknowledgements.
  int client_rpc_timeout_ms = 100;
};

void RunChaos(const ChaosOptions& o, const char* name) {
  KvCluster c(5, 3, 3);
  c.WaitForLeaders();
  if (o.unreliable) c.net().SetReliable(false);

  std::vector<std::string> keys;
  for (int k = 0; k < o.keys; k++) keys.push_back("key" + std::to_string(k));

  History history;
  std::atomic<bool> stop{false};
  std::vector<std::thread> workers;
  for (int i = 0; i < o.clients; i++) {
    KvClient* client = c.NewClient(ClientOptions{o.client_rpc_timeout_ms, 0});
    workers.emplace_back([&, client, i] { kvtest::RunWorkload(client, i, keys, stop, history, 1000 + i); });
  }

  std::mt19937 rng(99);
  int partitions_made = 0, crashes_made = 0;
  const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(o.seconds);
  while (std::chrono::steady_clock::now() < end) {
    kvtest::SleepMs(200 + static_cast<int>(rng() % 400));
    if (o.partitions) {
      if (rng() % 3 != 0) {
        std::vector<NodeId> a, b;
        for (int i = 0; i < c.size(); i++) (rng() % 2 ? a : b).push_back(c.IdOf(i));
        c.net().Partition({a, b});
        partitions_made++;
      } else {
        c.net().Partition({});
      }
    }
    if (o.crashes) {
      int down = 0;
      for (int i = 0; i < c.size(); i++) down += c.Up(i) ? 0 : 1;
      const int victim = static_cast<int>(rng() % c.size());
      if (c.Up(victim) && down < 2 && rng() % 2 == 0) {
        c.Crash(victim);
        crashes_made++;
      } else if (!c.Up(victim)) {
        c.Start(victim);  // Recovers from its on-disk Raft log and LSM tree.
      }
    }
  }

  // Heal everything so in-flight operations can finish, then stop clients.
  c.net().Heal();
  c.net().SetReliable(true);
  for (int i = 0; i < c.size(); i++) {
    if (!c.Up(i)) c.Start(i);
  }
  stop = true;
  for (auto& w : workers) w.join();

  uint64_t max_term = 0;
  for (int i = 0; i < c.size(); i++) {
    for (uint32_t p = 0; p < 3; p++) {
      if (PartitionReplica* r = c.node(i)->replica(p)) max_term = std::max(max_term, r->raft()->GetStatus().term);
    }
  }
  const auto ops = history.ops();
  double read_ms = 0, write_ms = 0;
  int reads = 0, writes = 0;
  for (const auto& op : ops) {
    const double ms = static_cast<double>(op.ret - op.call) / 1e6;
    if (op.op == OpType::kGet) {
      read_ms += ms;
      reads++;
    } else {
      write_ms += ms;
      writes++;
    }
  }
  std::printf("         max raft term %llu, %llu messages sent, avg latency: get %.1f ms, write %.1f ms\n",
              static_cast<unsigned long long>(max_term), static_cast<unsigned long long>(c.net().MessagesSent()),
              reads ? read_ms / reads : 0.0, writes ? write_ms / writes : 0.0);

  const auto t0 = std::chrono::steady_clock::now();
  const LinearizabilityResult r = CheckLinearizable(ops);
  const auto check_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
  std::printf("         %s: %zu ops over %zu keys, %d partitions, %d crashes, checked in %lld ms -> %s\n", name,
              r.operations, r.keys, partitions_made, crashes_made, static_cast<long long>(check_ms),
              r.ok ? "linearizable" : "NOT LINEARIZABLE");
  if (!r.ok) throw kvtest::Failure("history not linearizable for key " + r.bad_key);
  CHECK(ops.size() > 100);  // The cluster made real progress despite the faults.
}

}  // namespace

TEST(kv, basic_operations) {
  KvCluster c(3, 1, 3);
  KvClient* client = c.NewClient();
  CHECK(!client->Get("missing").has_value());
  CHECK(client->Put("a", "1"));
  CHECK_EQ(*client->Get("a"), std::string("1"));
  CHECK(client->Append("a", "23"));
  CHECK_EQ(*client->Get("a"), std::string("123"));
  CHECK(client->Put("a", "x"));
  CHECK_EQ(*client->Get("a"), std::string("x"));
  CHECK(client->Delete("a"));
  CHECK(!client->Get("a").has_value());
  CHECK(!client->Put(std::string(1, '\0') + "internal", "no"));  // Reserved prefix rejected.
}

TEST(kv, keys_spread_across_partitions_and_replicas) {
  KvCluster c(5, 6, 3);
  c.WaitForLeaders();
  KvClient* client = c.NewClient();
  for (int i = 0; i < 300; i++) CHECK(client->Put("user:" + std::to_string(i), "v" + std::to_string(i)));
  for (int i = 0; i < 300; i++) CHECK_EQ(*client->Get("user:" + std::to_string(i)), "v" + std::to_string(i));

  // Every partition received data, and every replica of it applied the writes.
  kvtest::SleepMs(300);
  Placement placement(c.config());
  std::vector<int> per_partition(6, 0);
  for (int i = 0; i < 300; i++) per_partition[placement.PartitionFor("user:" + std::to_string(i))]++;
  for (uint32_t p = 0; p < 6; p++) {
    CHECK(per_partition[p] > 10);
    for (NodeId id : placement.Replicas(p)) {
      KvStateMachine* sm = c.node(static_cast<int>(id - 1))->replica(p)->state_machine();
      for (int i = 0; i < 300; i++) {
        const std::string k = "user:" + std::to_string(i);
        if (placement.PartitionFor(k) == p) CHECK_EQ(*sm->Read(k), "v" + std::to_string(i));
      }
    }
  }
}

TEST(kv, retried_writes_apply_exactly_once) {
  // On a lossy network many requests and replies are dropped, so the client
  // retries writes that may already have committed. Deduplication must make
  // each append land exactly once.
  KvCluster c(3, 1, 3);
  c.WaitForLeaders();
  c.net().SetReliable(false);
  KvClient* client = c.NewClient(ClientOptions{150, 0});
  for (int i = 0; i < 100; i++) CHECK(client->Append("counter", "x"));
  c.net().SetReliable(true);
  CHECK_EQ(*client->Get("counter"), std::string(100, 'x'));
}

TEST(kv, minority_partition_cannot_serve_stale_reads) {
  KvCluster c(3, 1, 3);
  c.WaitForLeaders();
  KvClient* writer = c.NewClient();
  CHECK(writer->Put("k", "v1"));

  int leader = -1;
  for (int i = 0; i < 3; i++) {
    if (c.node(i)->replica(0)->raft()->IsLeader()) leader = i;
  }
  CHECK(leader >= 0);

  // Isolate the old leader together with a client that can reach only it.
  KvClient* isolated = c.NewClient(ClientOptions{300, 2000});
  CHECK(isolated->Get("k").has_value());  // Caches the old leader as its target.
  std::vector<NodeId> minority = {c.IdOf(leader), isolated->id()};
  std::vector<NodeId> majority;
  for (int i = 0; i < 3; i++) {
    if (i != leader) majority.push_back(c.IdOf(i));
  }
  majority.push_back(writer->id());
  c.net().Partition({minority, majority});

  // The majority elects a new leader and accepts a newer write.
  CHECK(writer->Put("k", "v2"));

  // The old leader must refuse rather than serve the stale "v1".
  bool ok = true;
  CHECK(!isolated->Get("k", &ok).has_value());
  CHECK(!ok);
  CHECK(!isolated->Put("k", "v3"));

  c.net().Heal();
  CHECK_EQ(*writer->Get("k"), std::string("v2"));
}

TEST(kv, write_is_acknowledged_only_if_committed_in_its_term) {
  // A deposed leader must not report success for an entry that a new leader
  // overwrote. Setup: the leader is isolated with a patient client whose write
  // it appends but cannot commit; the majority elects a new leader that fills
  // that log slot with different entries; then the partition heals.
  KvCluster c(3, 1, 3, LsmOptions{}, /*request_timeout_ms=*/5000);
  c.WaitForLeaders();
  KvClient* b = c.NewClient(ClientOptions{100, 0});
  CHECK(b->Put("k", "v0"));
  int leader = -1;
  for (int i = 0; i < 3; i++) {
    if (c.node(i)->replica(0)->raft()->IsLeader()) leader = i;
  }
  CHECK(leader >= 0);

  KvClient* a = c.NewClient(ClientOptions{10000, 0});  // Acts on whatever the old leader answers.
  CHECK(a->Put("warmup", "x"));  // Caches the leader, so A's next write goes straight to it.
  std::vector<NodeId> minority = {c.IdOf(leader), a->id()};
  std::vector<NodeId> majority = {b->id()};
  for (int i = 0; i < 3; i++) {
    if (i != leader) majority.push_back(c.IdOf(i));
  }
  c.net().Partition({minority, majority});

  std::atomic<bool> a_done{false};
  bool a_ok = false;
  std::thread writer([&] {
    a_ok = a->Put("k", "from-a");
    a_done = true;
  });
  kvtest::SleepMs(100);
  CHECK(!a_done);  // Pending: the isolated leader cannot commit it.

  CHECK(b->Put("k", "from-b1"));
  CHECK(b->Put("k", "from-b2"));
  c.net().Heal();
  writer.join();

  // A's write was acknowledged, so it must be the most recent write to k.
  CHECK(a_ok);
  CHECK_EQ(*b->Get("k"), std::string("from-a"));
}

TEST(kv, full_cluster_restart_recovers_all_data) {
  // Tiny memtables force many flushes and compactions underneath Raft.
  LsmOptions lsm;
  lsm.memtable_bytes = 16 << 10;
  lsm.l0_compaction_trigger = 2;
  lsm.level1_max_bytes = 64 << 10;
  lsm.target_file_bytes = 16 << 10;
  KvCluster c(3, 2, 3, lsm);
  c.WaitForLeaders();
  KvClient* client = c.NewClient();
  for (int i = 0; i < 1500; i++) CHECK(client->Put("k" + std::to_string(i % 500), std::string(64, 'a' + i % 26)));
  for (int i = 0; i < 500; i += 5) CHECK(client->Delete("k" + std::to_string(i)));

  for (int i = 0; i < 3; i++) c.Crash(i);
  for (int i = 0; i < 3; i++) c.Start(i);
  c.WaitForLeaders();

  for (int i = 0; i < 500; i++) {
    auto v = client->Get("k" + std::to_string(i));
    if (i % 5 == 0) {
      CHECK(!v.has_value());
    } else {
      CHECK_EQ(*v, std::string(64, 'a' + (1000 + i) % 26));
    }
  }
  const LsmStats s = c.node(0)->replica(0)->state_machine()->db()->Stats();
  CHECK(s.flushes + s.compactions > 0);
}

TEST(kv, concurrent_clients_are_linearizable) {
  RunChaos(ChaosOptions{false, false, false, 3, 5, 3}, "no faults");
}

TEST(kv, linearizable_under_network_partitions) {
  RunChaos(ChaosOptions{true, false, false, 6, 5, 4}, "partitions");
}

TEST(kv, linearizable_under_crash_restart) {
  RunChaos(ChaosOptions{false, true, false, 6, 5, 4}, "crashes");
}

TEST(kv, linearizable_under_partitions_and_crashes_with_patient_clients) {
  RunChaos(ChaosOptions{true, true, false, 8, 5, 4, 2000}, "partitions+crashes, patient clients");
}

TEST(kv, linearizable_under_lossy_network) {
  RunChaos(ChaosOptions{false, false, true, 5, 5, 4}, "unreliable");
}

TEST(kv, linearizable_under_partitions_crashes_and_lossy_network) {
  RunChaos(ChaosOptions{true, true, true, 8, 5, 4}, "partitions+crashes+unreliable");
}
