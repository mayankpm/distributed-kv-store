// kvbench: storage-engine and cluster benchmarks.
//
//   kvbench lsm     [--n 200000] [--value 100] [--dir DIR]
//   kvbench cluster [--nodes 3] [--partitions 3] [--clients 16] [--seconds 5] [--value 100] [--dir DIR]
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "common/platform.h"
#include "kv/local_cluster.h"
#include "storage/lsm_tree.h"

using namespace kv;
using Clock = std::chrono::steady_clock;

namespace {

struct Args {
  std::string mode;
  int n = 200000;
  int value = 100;
  int nodes = 3;
  int partitions = 3;
  int clients = 16;
  int seconds = 5;
  std::string dir;
};

std::string Key(uint64_t i) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "key%016llu", static_cast<unsigned long long>(i));
  return buf;
}

double Seconds(Clock::time_point start) { return std::chrono::duration<double>(Clock::now() - start).count(); }

void Report(const char* name, int ops, double secs, size_t bytes_per_op = 0) {
  std::printf("  %-28s %10.0f ops/s  %8.2f us/op", name, ops / secs, secs * 1e6 / ops);
  if (bytes_per_op) std::printf("  %7.1f MB/s", ops * static_cast<double>(bytes_per_op) / secs / (1 << 20));
  std::printf("\n");
}

std::unique_ptr<LsmTree> OpenFresh(const std::string& dir, const LsmOptions& o) {
  std::filesystem::remove_all(dir);
  std::string err;
  auto db = LsmTree::Open(dir, o, &err);
  if (!db) {
    std::fprintf(stderr, "open %s: %s\n", dir.c_str(), err.c_str());
    std::exit(1);
  }
  return db;
}

int BenchLsm(const Args& a) {
  const std::string base = a.dir.empty() ? (std::filesystem::temp_directory_path() / "kvbench").string() : a.dir;
  const std::string value(static_cast<size_t>(a.value), 'v');
  const size_t entry = 19 + value.size();
  std::mt19937_64 rng(1);
  std::printf("LSM tree: %d entries, %d-byte values, 4 MB memtable, 10 bits/key bloom filters\n", a.n, a.value);

  LsmOptions opts;
  {
    auto db = OpenFresh(base + "/seq", opts);
    auto t = Clock::now();
    for (int i = 0; i < a.n; i++) db->Put(Key(i), value);
    Report("fillseq", a.n, Seconds(t), entry);
  }

  auto db = OpenFresh(base + "/rand", opts);
  std::vector<uint64_t> keys(static_cast<size_t>(a.n));
  for (auto& k : keys) k = rng() % (static_cast<uint64_t>(a.n) * 10);
  auto t = Clock::now();
  for (uint64_t k : keys) db->Put(Key(k), value);
  Report("fillrandom", a.n, Seconds(t), entry);

  t = Clock::now();
  for (int i = 0; i < a.n; i++) {
    WriteBatch b;
    b.Put(Key(keys[static_cast<size_t>(i)]), value);
    db->Write(b);
  }
  Report("overwrite", a.n, Seconds(t), entry);

  t = Clock::now();
  db->WaitForIdle();
  const double settle = Seconds(t);

  const int reads = std::min(a.n, 100000);
  t = Clock::now();
  int found = 0;
  for (int i = 0; i < reads; i++) found += db->Get(Key(keys[rng() % keys.size()])).has_value() ? 1 : 0;
  Report("readrandom (hits)", reads, Seconds(t));

  // A hot set of 1% of keys fits in the block cache.
  t = Clock::now();
  for (int i = 0; i < reads; i++) db->Get(Key(keys[rng() % (keys.size() / 100)]));
  Report("readhot (1% of keys)", reads, Seconds(t));

  // Absent keys that sort between stored keys, so every table's key range
  // covers them and only the bloom filter can rule a table out.
  auto missing = [&] { return Key(keys[rng() % keys.size()]) + "~"; };
  const LsmStats before = db->Stats();
  t = Clock::now();
  for (int i = 0; i < reads; i++) db->Get(missing());
  const double with_bloom = Seconds(t);
  Report("readmissing (bloom on)", reads, with_bloom);
  const LsmStats after = db->Stats();

  // Same data, built without bloom filters.
  LsmOptions no_bloom = opts;
  no_bloom.bloom_bits_per_key = 0;
  auto db2 = OpenFresh(base + "/nobloom", no_bloom);
  for (uint64_t k : keys) db2->Put(Key(k), value);
  db2->Flush();
  db2->WaitForIdle();
  t = Clock::now();
  for (int i = 0; i < reads; i++) db2->Get(missing());
  const double without_bloom = Seconds(t);
  Report("readmissing (bloom off)", reads, without_bloom);

  const LsmStats s = db->Stats();
  const uint64_t probes = after.table_probes - before.table_probes;
  const uint64_t skips = after.bloom_skips - before.bloom_skips;
  const double user_bytes = 2.0 * a.n * entry;
  std::printf("\n  hits found:                 %d/%d (block cache hit rate %.0f%%)\n", found, reads,
              100.0 * static_cast<double>(s.block_cache_hits) /
                  static_cast<double>(std::max<uint64_t>(1, s.block_cache_hits + s.block_cache_misses)));
  std::printf("  bloom filter:               skipped %.1f%% of table probes for missing keys; %.1fx faster\n",
              probes ? 100.0 * static_cast<double>(skips) / static_cast<double>(probes) : 0.0,
              without_bloom / with_bloom);
  std::printf("  flushes / compactions:      %llu / %llu (+%llu trivial moves), %.2fs to settle\n",
              static_cast<unsigned long long>(s.flushes), static_cast<unsigned long long>(s.compactions),
              static_cast<unsigned long long>(s.trivial_moves), settle);
  std::printf("  compaction write amp:       %.2fx user bytes\n",
              static_cast<double>(s.compaction_bytes_written) / user_bytes);
  std::printf("  files per level:           ");
  for (int l = 0; l < LsmOptions::kNumLevels; l++) std::printf(" L%d=%d", l, s.files_per_level[l]);
  std::printf("\n");
  db.reset();
  db2.reset();
  std::filesystem::remove_all(base);
  return 0;
}

int BenchCluster(const Args& a) {
  const std::string dir = a.dir.empty() ? (std::filesystem::temp_directory_path() / "kvbench-cluster").string() : a.dir;
  std::filesystem::remove_all(dir);
  std::string err;
  auto cluster = LocalCluster::Start(a.nodes, static_cast<uint32_t>(a.partitions), 3, dir, NodeOptions{}, &err);
  if (!cluster) {
    std::fprintf(stderr, "cluster: %s\n", err.c_str());
    return 1;
  }
  std::printf("Cluster over TCP loopback: %d nodes, %d partitions, 3x replication, %d clients, %d-byte values\n",
              a.nodes, a.partitions, a.clients, a.value);

  std::vector<std::unique_ptr<LocalCluster::Client>> clients;
  for (int i = 0; i < a.clients; i++) clients.push_back(cluster->NewClient());
  (*clients[0])->Put("warmup", "x");  // Wait for leaders.
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  auto run = [&](bool writes) {
    std::atomic<bool> stop{false};
    std::vector<std::vector<double>> lat(static_cast<size_t>(a.clients));
    std::vector<std::thread> threads;
    const std::string value(static_cast<size_t>(a.value), 'v');
    for (int c = 0; c < a.clients; c++) {
      threads.emplace_back([&, c] {
        std::mt19937_64 rng(static_cast<uint64_t>(c) + 7);
        while (!stop) {
          const std::string key = Key(rng() % 10000);
          const auto t = Clock::now();
          if (writes) {
            (*clients[static_cast<size_t>(c)])->Put(key, value);
          } else {
            (*clients[static_cast<size_t>(c)])->Get(key);
          }
          lat[static_cast<size_t>(c)].push_back(std::chrono::duration<double, std::micro>(Clock::now() - t).count());
        }
      });
    }
    const auto start = Clock::now();
    std::this_thread::sleep_for(std::chrono::seconds(a.seconds));
    stop = true;
    for (auto& t : threads) t.join();
    const double secs = Seconds(start);
    std::vector<double> all;
    for (auto& l : lat) all.insert(all.end(), l.begin(), l.end());
    std::sort(all.begin(), all.end());
    auto pct = [&](double p) { return all.empty() ? 0.0 : all[static_cast<size_t>(p * (all.size() - 1))] / 1000.0; };
    std::printf("  %-8s %9.0f ops/s   p50 %6.2f ms   p99 %6.2f ms   (%zu ops)\n", writes ? "put" : "get",
                static_cast<double>(all.size()) / secs, pct(0.50), pct(0.99), all.size());
  };
  run(true);
  run(false);
  clients.clear();
  cluster.reset();
  std::filesystem::remove_all(dir);
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  kv::EnableHighResolutionTimers();
  Args a;
  if (argc >= 2) a.mode = argv[1];
  for (int i = 2; i + 1 < argc; i += 2) {
    const std::string f = argv[i];
    const std::string v = argv[i + 1];
    if (f == "--n") a.n = std::stoi(v);
    else if (f == "--value") a.value = std::stoi(v);
    else if (f == "--nodes") a.nodes = std::stoi(v);
    else if (f == "--partitions") a.partitions = std::stoi(v);
    else if (f == "--clients") a.clients = std::stoi(v);
    else if (f == "--seconds") a.seconds = std::stoi(v);
    else if (f == "--dir") a.dir = v;
  }
  if (a.mode == "lsm") return BenchLsm(a);
  if (a.mode == "cluster") return BenchCluster(a);
  std::fprintf(stderr,
               "usage: kvbench lsm [--n N] [--value BYTES] [--dir DIR]\n"
               "       kvbench cluster [--nodes N] [--partitions P] [--clients C] [--seconds S] [--value BYTES]\n");
  return 2;
}
