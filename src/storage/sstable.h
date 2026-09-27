// Sorted String Table: an immutable, sorted, on-disk run of key/value pairs.
//
// File layout:
//   [data block]*          entries: varint klen | key | u8 type | varint vlen | value
//                          followed by fixed32 crc32 of the entries
//   [filter block]         bloom filter over every key in the table
//   [index block]          per data block: lp(last key) | fixed64 offset | fixed64 size
//   [footer, 48 bytes]     filter off/size, index off/size, entry count, magic
//
// The filter and index are loaded into memory on open, so a point lookup costs
// at most one data block read, and zero when the bloom filter says "absent".
#pragma once

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "storage/block_cache.h"
#include "storage/memtable.h"

namespace kv {

class BloomFilterBuilder;

class TableBuilder {
 public:
  TableBuilder(std::string path, int bloom_bits_per_key, size_t block_size = 4096);
  ~TableBuilder();

  bool Open();
  // Keys must be added in strictly increasing order.
  void Add(std::string_view key, ValueType type, std::string_view value);
  bool Finish(bool sync);
  // Closes and deletes a partially written file.
  void Abandon();

  uint64_t FileSize() const { return offset_ + pending_.size(); }
  uint64_t NumEntries() const { return num_entries_; }
  const std::string& smallest() const { return smallest_; }
  const std::string& largest() const { return largest_; }

 private:
  bool FlushBlock();

  std::string path_;
  size_t block_size_;
  std::FILE* f_ = nullptr;
  bool ok_ = true;
  uint64_t offset_ = 0;
  uint64_t num_entries_ = 0;
  std::string pending_;
  std::string index_;
  std::string smallest_;
  std::string largest_;
  std::string last_key_;
  std::unique_ptr<BloomFilterBuilder> bloom_;
};

class Table {
 public:
  using Entry = BlockEntry;

  // `cache` (optional, may be shared across tables) holds decoded blocks for
  // point lookups.
  static std::shared_ptr<Table> Open(const std::string& path, uint64_t file_number, BlockCache* cache = nullptr);
  ~Table();

  // Point lookup. `bloom_skipped` is set when the bloom filter proved the key
  // absent without touching the data blocks.
  LookupStatus Get(std::string_view key, std::string* value, bool* bloom_skipped = nullptr);

  // Sequential scan over the whole table, one data block in memory at a time.
  class Iterator {
   public:
    explicit Iterator(std::shared_ptr<Table> table);
    bool Valid() const { return valid_; }
    void Next();
    const Entry& entry() const { return block_[pos_]; }
    bool status_ok() const { return ok_; }

   private:
    bool LoadBlock(size_t idx);
    std::shared_ptr<Table> table_;
    size_t block_idx_ = 0;
    std::vector<Entry> block_;
    size_t pos_ = 0;
    bool valid_ = false;
    bool ok_ = true;
  };

  uint64_t file_number() const { return file_number_; }
  uint64_t num_entries() const { return num_entries_; }
  // Once marked, the file is deleted when the last reference goes away. This
  // lets compaction retire files that concurrent readers may still be using.
  void MarkObsolete() { obsolete_ = true; }

 private:
  struct IndexEntry {
    std::string last_key;
    uint64_t offset;
    uint64_t size;
  };

  Table() = default;
  bool ReadBlock(size_t idx, std::vector<Entry>* out);

  std::string path_;
  uint64_t file_number_ = 0;
  std::FILE* f_ = nullptr;
  std::mutex file_mu_;
  BlockCache* cache_ = nullptr;
  std::string filter_;
  std::vector<IndexEntry> index_;
  uint64_t num_entries_ = 0;
  std::atomic<bool> obsolete_{false};
};

}  // namespace kv
