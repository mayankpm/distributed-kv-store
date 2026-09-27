#include <cstdio>
#include <filesystem>
#include <map>
#include <random>

#include "common/file_util.h"
#include "storage/bloom_filter.h"
#include "storage/lsm_tree.h"
#include "storage/sstable.h"
#include "storage/wal.h"
#include "test.h"

using namespace kv;

namespace {

std::string Key(int i) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "key%08d", i);
  return buf;
}

LsmOptions SmallOptions() {
  // Tiny limits so a few thousand writes exercise flushes and multi-level compaction.
  LsmOptions o;
  o.memtable_bytes = 32 << 10;
  o.l0_compaction_trigger = 2;
  o.l0_stop_writes_trigger = 8;
  o.level1_max_bytes = 64 << 10;
  o.level_size_multiplier = 4;
  o.target_file_bytes = 16 << 10;
  o.block_size = 1024;
  return o;
}

}  // namespace

TEST(storage, bloom_filter_has_no_false_negatives_and_low_false_positive_rate) {
  BloomFilterBuilder b(10);
  for (int i = 0; i < 10000; i++) b.AddKey(Key(i));
  const std::string filter = b.Finish();
  for (int i = 0; i < 10000; i++) CHECK(BloomMayContain(filter, Key(i)));

  int false_positives = 0;
  for (int i = 10000; i < 20000; i++) false_positives += BloomMayContain(filter, Key(i)) ? 1 : 0;
  // 10 bits/key gives ~1% theoretical; allow slack.
  CHECK(false_positives < 200);
}

TEST(storage, wal_replay_stops_at_torn_tail) {
  kvtest::TempDir dir;
  const std::string path = dir.sub("wal.log");
  {
    LogWriter w;
    CHECK(w.Open(path, true));
    for (int i = 0; i < 100; i++) CHECK(w.Append("record-" + std::to_string(i), false));
  }
  // Simulate a crash mid-write by chopping bytes off the final record.
  const auto size = std::filesystem::file_size(path);
  std::filesystem::resize_file(path, size - 3);

  std::vector<std::string> records;
  ReadLog(path, [&](std::string_view r) { records.emplace_back(r); });
  CHECK_EQ(records.size(), 99u);
  CHECK_EQ(records.back(), std::string("record-98"));
}

TEST(storage, sstable_roundtrip_point_lookups_and_scan) {
  kvtest::TempDir dir;
  const std::string path = dir.sub("000001.sst");
  TableBuilder b(path, 10, 512);
  CHECK(b.Open());
  for (int i = 0; i < 5000; i += 2) {
    if (i % 10 == 0) {
      b.Add(Key(i), ValueType::kDeletion, "");
    } else {
      b.Add(Key(i), ValueType::kValue, "v" + std::to_string(i));
    }
  }
  CHECK(b.Finish(false));

  auto t = Table::Open(path, 1);
  CHECK(t != nullptr);
  CHECK_EQ(t->num_entries(), 2500u);

  std::string v;
  CHECK(t->Get(Key(2), &v) == LookupStatus::kFound);
  CHECK_EQ(v, std::string("v2"));
  CHECK(t->Get(Key(4998), &v) == LookupStatus::kFound);
  CHECK(t->Get(Key(10), &v) == LookupStatus::kDeleted);
  CHECK(t->Get(Key(3), &v) == LookupStatus::kNotFound);
  CHECK(t->Get("zzz", &v) == LookupStatus::kNotFound);

  int bloom_skips = 0;
  for (int i = 1; i < 5000; i += 2) {
    bool skipped = false;
    CHECK(t->Get(Key(i), &v, &skipped) == LookupStatus::kNotFound);
    bloom_skips += skipped ? 1 : 0;
  }
  CHECK(bloom_skips > 2400);  // Nearly every absent key avoids a block read.

  int n = 0;
  std::string prev;
  for (Table::Iterator it(t); it.Valid(); it.Next()) {
    CHECK(it.entry().key > prev);
    prev = it.entry().key;
    n++;
  }
  CHECK_EQ(n, 2500);
}

TEST(storage, sstable_rejects_corrupt_block) {
  kvtest::TempDir dir;
  const std::string path = dir.sub("000001.sst");
  TableBuilder b(path, 10, 256);
  CHECK(b.Open());
  for (int i = 0; i < 100; i++) b.Add(Key(i), ValueType::kValue, "value");
  CHECK(b.Finish(false));

  std::string contents;
  CHECK(ReadFile(path, &contents));
  contents[10] ^= 0x5a;  // Flip bits inside the first data block.
  std::FILE* f = std::fopen(path.c_str(), "wb");
  std::fwrite(contents.data(), 1, contents.size(), f);
  std::fclose(f);

  auto t = Table::Open(path, 1);
  CHECK(t != nullptr);
  std::string v;
  CHECK(t->Get(Key(0), &v) == LookupStatus::kNotFound);  // Checksum mismatch, never garbage.
}

TEST(storage, lsm_put_get_delete) {
  kvtest::TempDir dir;
  std::string err;
  auto db = LsmTree::Open(dir.path(), LsmOptions{}, &err);
  CHECK(db != nullptr);
  CHECK(db->Put("a", "1"));
  CHECK(db->Put("b", "2"));
  CHECK(db->Put("a", "3"));
  CHECK_EQ(*db->Get("a"), std::string("3"));
  CHECK(db->Delete("b"));
  CHECK(!db->Get("b").has_value());
  CHECK(!db->Get("c").has_value());

  CHECK(db->Flush());
  CHECK_EQ(*db->Get("a"), std::string("3"));
  CHECK(!db->Get("b").has_value());
}

TEST(storage, lsm_matches_model_through_flushes_and_compactions) {
  kvtest::TempDir dir;
  std::string err;
  auto db = LsmTree::Open(dir.path(), SmallOptions(), &err);
  CHECK(db != nullptr);

  std::map<std::string, std::string> model;
  std::mt19937 rng(42);
  for (int i = 0; i < 40000; i++) {
    const std::string k = Key(static_cast<int>(rng() % 3000));
    if (rng() % 5 == 0) {
      CHECK(db->Delete(k));
      model.erase(k);
    } else {
      const std::string v = "value-" + std::to_string(i) + std::string(rng() % 64, 'x');
      CHECK(db->Put(k, v));
      model[k] = v;
    }
    if (i % 5000 == 0) {
      for (int j = 0; j < 3000; j += 7) {
        auto got = db->Get(Key(j));
        auto want = model.find(Key(j));
        CHECK_EQ(got.has_value(), want != model.end());
        if (got) CHECK_EQ(*got, want->second);
      }
    }
  }
  db->WaitForIdle();

  for (int j = 0; j < 3000; j++) {
    auto got = db->Get(Key(j));
    auto want = model.find(Key(j));
    CHECK_EQ(got.has_value(), want != model.end());
    if (got) CHECK_EQ(*got, want->second);
  }

  const LsmStats s = db->Stats();
  CHECK(s.flushes > 10);
  CHECK(s.compactions > 5);
  CHECK(s.tombstones_dropped > 0);
  int deepest = 0;
  for (int l = 0; l < LsmOptions::kNumLevels; l++) {
    if (s.files_per_level[l] > 0) deepest = l;
  }
  CHECK(deepest >= 2);  // Data actually moved down through multiple levels.
}

TEST(storage, lsm_recovers_unflushed_and_flushed_data_after_reopen) {
  kvtest::TempDir dir;
  std::string err;
  std::map<std::string, std::string> model;
  {
    auto db = LsmTree::Open(dir.path(), SmallOptions(), &err);
    CHECK(db != nullptr);
    for (int i = 0; i < 5000; i++) {
      const std::string v = "v" + std::to_string(i);
      CHECK(db->Put(Key(i % 1500), v));
      model[Key(i % 1500)] = v;
    }
    for (int i = 0; i < 1500; i += 3) {
      CHECK(db->Delete(Key(i)));
      model.erase(Key(i));
    }
    // Destroy without flushing: the tail lives only in the WAL.
  }
  for (int round = 0; round < 2; round++) {
    auto db = LsmTree::Open(dir.path(), SmallOptions(), &err);
    CHECK(db != nullptr);
    for (int i = 0; i < 1500; i++) {
      auto got = db->Get(Key(i));
      auto want = model.find(Key(i));
      CHECK_EQ(got.has_value(), want != model.end());
      if (got) CHECK_EQ(*got, want->second);
    }
  }
}

TEST(storage, lsm_write_batch_is_atomic_across_crash) {
  kvtest::TempDir dir;
  std::string err;
  {
    auto db = LsmTree::Open(dir.path(), LsmOptions{}, &err);
    CHECK(db->Put("before", "ok"));
    WriteBatch b;
    for (int i = 0; i < 50; i++) b.Put(Key(i), "batched");
    CHECK(db->Write(b));
  }
  // Tear the final WAL record (the batch) as if the process died mid-write.
  for (const auto& e : std::filesystem::directory_iterator(dir.path())) {
    if (e.path().extension() == ".log" && std::filesystem::file_size(e.path()) > 0) {
      std::filesystem::resize_file(e.path(), std::filesystem::file_size(e.path()) - 5);
    }
  }
  auto db = LsmTree::Open(dir.path(), LsmOptions{}, &err);
  CHECK(db != nullptr);
  CHECK_EQ(*db->Get("before"), std::string("ok"));
  for (int i = 0; i < 50; i++) CHECK(!db->Get(Key(i)).has_value());
}
