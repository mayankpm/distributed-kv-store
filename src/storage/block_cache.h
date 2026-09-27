// LRU cache of decoded SSTable data blocks, shared by all tables of one LSM
// tree. Hot keys are served from memory without re-reading and re-parsing
// their 4KB block. Compaction scans bypass it so they do not evict the
// working set.
#pragma once

#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "storage/memtable.h"

namespace kv {

struct BlockEntry {
  std::string key;
  ValueType type;
  std::string value;
};

struct Block {
  std::vector<BlockEntry> entries;
  size_t bytes = 0;  // Approximate memory footprint.
};

class BlockCache {
 public:
  explicit BlockCache(size_t capacity_bytes) : capacity_(capacity_bytes) {}

  std::shared_ptr<const Block> Lookup(uint64_t file, uint64_t block);
  void Insert(uint64_t file, uint64_t block, std::shared_ptr<const Block> value);

  uint64_t hits() const { return hits_; }
  uint64_t misses() const { return misses_; }

 private:
  struct Key {
    uint64_t file;
    uint64_t block;
    bool operator==(const Key& o) const { return file == o.file && block == o.block; }
  };
  struct KeyHash {
    size_t operator()(const Key& k) const { return std::hash<uint64_t>()(k.file * 0x9E3779B97F4A7C15ULL ^ k.block); }
  };
  struct Slot {
    std::shared_ptr<const Block> value;
    std::list<Key>::iterator lru;
  };

  const size_t capacity_;
  std::mutex mu_;
  std::list<Key> lru_;  // Front = most recently used.
  std::unordered_map<Key, Slot, KeyHash> map_;
  size_t usage_ = 0;
  uint64_t hits_ = 0;
  uint64_t misses_ = 0;
};

}  // namespace kv
