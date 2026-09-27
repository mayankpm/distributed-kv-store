// Log-structured merge tree storage engine, modeled on LevelDB.
//
// Write path:  WAL append -> memtable insert. When the memtable fills up it is
//              frozen (immutable) and a background thread flushes it to a new
//              level-0 SSTable, after which its WAL is deleted.
// Read path:   memtable -> immutable memtable -> L0 tables (newest first) ->
//              L1..Ln (one candidate table per level, found by binary search).
//              Every table consult is gated by its bloom filter.
// Compaction:  leveled. L0 is compacted into L1 once it holds
//              `l0_compaction_trigger` files; level i >= 1 is compacted into
//              level i+1 once it exceeds its byte budget (10x per level). Files
//              are picked round-robin through the key space so each level is
//              rewritten evenly. Tombstones are dropped once no deeper level can
//              hold an older value for the key.
// Recovery:    the MANIFEST lists live tables and the oldest WAL still needed.
//              On open, WALs are replayed and flushed before accepting writes.
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "storage/memtable.h"
#include "storage/sstable.h"
#include "storage/wal.h"
#include "storage/write_batch.h"

namespace kv {

struct LsmOptions {
  size_t memtable_bytes = 4u << 20;
  int l0_compaction_trigger = 4;
  int l0_stop_writes_trigger = 12;
  uint64_t level1_max_bytes = 10ull << 20;
  int level_size_multiplier = 10;
  uint64_t target_file_bytes = 2ull << 20;
  int bloom_bits_per_key = 10;  // <= 0 disables bloom filters.
  size_t block_size = 4096;
  bool sync_writes = false;     // fsync the WAL on every write.
  size_t block_cache_bytes = 8u << 20;  // 0 disables the block cache.
  static constexpr int kNumLevels = 7;
};

struct LsmStats {
  uint64_t flushes = 0;
  uint64_t compactions = 0;
  uint64_t trivial_moves = 0;
  uint64_t compaction_bytes_read = 0;
  uint64_t compaction_bytes_written = 0;
  uint64_t tombstones_dropped = 0;
  uint64_t table_probes = 0;   // Tables consulted by Get.
  uint64_t bloom_skips = 0;    // Of those, how many the bloom filter skipped.
  uint64_t block_cache_hits = 0;
  uint64_t block_cache_misses = 0;
  std::vector<int> files_per_level;
  std::vector<uint64_t> bytes_per_level;
};

class LsmTree {
 public:
  static std::unique_ptr<LsmTree> Open(const std::string& dir, const LsmOptions& options, std::string* error);
  ~LsmTree();
  LsmTree(const LsmTree&) = delete;
  LsmTree& operator=(const LsmTree&) = delete;

  bool Put(std::string_view key, std::string_view value);
  bool Delete(std::string_view key);
  bool Write(const WriteBatch& batch);
  std::optional<std::string> Get(std::string_view key);

  // Freezes the current memtable and blocks until it is on disk as an SSTable.
  bool Flush();
  // Blocks until no flush or compaction work is pending.
  void WaitForIdle();

  LsmStats Stats() const;

 private:
  struct FileMeta {
    uint64_t number;
    uint64_t size;
    std::string smallest;
    std::string largest;
    std::shared_ptr<Table> table;
  };
  using FilePtr = std::shared_ptr<FileMeta>;

  // Immutable snapshot of the table layout. Replaced wholesale on every
  // flush/compaction so readers never need to lock while probing tables.
  struct Version {
    std::vector<std::vector<FilePtr>> levels = std::vector<std::vector<FilePtr>>(LsmOptions::kNumLevels);
  };

  struct Compaction {
    int level = 0;
    std::vector<FilePtr> inputs;       // From `level`.
    std::vector<FilePtr> next_inputs;  // Overlapping files from `level + 1`.
  };

  LsmTree(std::string dir, const LsmOptions& options);

  bool Recover(std::string* error);
  bool MakeRoomForWrite(std::unique_lock<std::mutex>& lock);
  bool RotateMemTable();
  void BackgroundLoop();
  bool NeedsCompaction() const;
  void DoFlush(std::unique_lock<std::mutex>& lock);
  void DoCompaction(std::unique_lock<std::mutex>& lock);
  bool PickCompaction(Compaction* c) const;
  bool WriteManifest(const Version& v, uint64_t log_number);
  // Oldest WAL that still holds data not yet in an SSTable.
  uint64_t OldestLiveLog() const { return imm_ ? imm_wal_number_ : wal_number_; }
  FilePtr WriteMemTableToTable(const MemTable& mem, std::string* error);
  bool IsBaseLevelForKey(const Version& v, int level, std::string_view key) const;
  double LevelScore(const Version& v, int level) const;
  uint64_t MaxBytesForLevel(int level) const;

  std::string TablePath(uint64_t number) const;
  std::string LogPath(uint64_t number) const;

  const std::string dir_;
  const LsmOptions options_;
  std::unique_ptr<BlockCache> block_cache_;

  mutable std::mutex mu_;
  std::condition_variable work_cv_;   // Signals the background thread.
  std::condition_variable done_cv_;   // Signals writers waiting on background work.
  std::shared_ptr<MemTable> mem_;
  std::shared_ptr<MemTable> imm_;
  std::shared_ptr<const Version> current_;
  LogWriter wal_;
  uint64_t wal_number_ = 0;
  uint64_t imm_wal_number_ = 0;
  std::atomic<uint64_t> next_file_number_{1};
  std::vector<std::string> compact_pointer_ = std::vector<std::string>(LsmOptions::kNumLevels);
  bool shutting_down_ = false;
  bool bg_error_ = false;
  std::thread bg_thread_;

  LsmStats stats_;  // Guarded by mu_.
  std::atomic<uint64_t> table_probes_{0};
  std::atomic<uint64_t> bloom_skips_{0};
};

}  // namespace kv
