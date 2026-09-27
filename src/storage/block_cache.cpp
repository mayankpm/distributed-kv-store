#include "storage/block_cache.h"

namespace kv {

std::shared_ptr<const Block> BlockCache::Lookup(uint64_t file, uint64_t block) {
  std::lock_guard lock(mu_);
  auto it = map_.find(Key{file, block});
  if (it == map_.end()) {
    misses_++;
    return nullptr;
  }
  hits_++;
  lru_.splice(lru_.begin(), lru_, it->second.lru);
  return it->second.value;
}

void BlockCache::Insert(uint64_t file, uint64_t block, std::shared_ptr<const Block> value) {
  std::lock_guard lock(mu_);
  const Key key{file, block};
  if (map_.count(key)) return;
  usage_ += value->bytes;
  lru_.push_front(key);
  map_[key] = Slot{std::move(value), lru_.begin()};
  while (usage_ > capacity_ && !lru_.empty()) {
    auto victim = map_.find(lru_.back());
    usage_ -= victim->second.value->bytes;
    map_.erase(victim);
    lru_.pop_back();
  }
}

}  // namespace kv
