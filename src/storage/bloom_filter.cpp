#include "storage/bloom_filter.h"

#include <algorithm>

#include "common/hash.h"

namespace kv {
namespace {

constexpr uint64_t kBloomSeed = 0xb10f11e7ULL;

// Kirsch-Mitzenmacher double hashing: probe i uses h + i * delta, which gives
// the same false positive rate as k independent hash functions.
inline uint64_t Rotate(uint64_t h) { return (h >> 33) | (h << 31); }

}  // namespace

BloomFilterBuilder::BloomFilterBuilder(int bits_per_key) : bits_per_key_(bits_per_key) {
  // Optimal k = ln(2) * bits_per_key.
  num_probes_ = std::clamp(static_cast<int>(bits_per_key * 0.69), 1, 30);
}

void BloomFilterBuilder::AddKey(std::string_view key) { hashes_.push_back(Hash64(key, kBloomSeed)); }

std::string BloomFilterBuilder::Finish() const {
  if (bits_per_key_ <= 0) return {};  // Filters disabled.
  uint64_t bits = std::max<uint64_t>(64, hashes_.size() * static_cast<uint64_t>(bits_per_key_));
  uint64_t bytes = (bits + 7) / 8;
  bits = bytes * 8;

  std::string filter(bytes, '\0');
  for (uint64_t h : hashes_) {
    const uint64_t delta = Rotate(h);
    for (int i = 0; i < num_probes_; i++) {
      const uint64_t pos = h % bits;
      filter[pos / 8] = static_cast<char>(filter[pos / 8] | (1 << (pos % 8)));
      h += delta;
    }
  }
  filter.push_back(static_cast<char>(num_probes_));
  return filter;
}

bool BloomMayContain(std::string_view filter, std::string_view key) {
  if (filter.size() < 2) return true;  // Missing filter: cannot rule anything out.
  const uint64_t bits = (filter.size() - 1) * 8;
  const int num_probes = static_cast<uint8_t>(filter.back());
  if (num_probes == 0 || num_probes > 30) return true;

  uint64_t h = Hash64(key, kBloomSeed);
  const uint64_t delta = Rotate(h);
  for (int i = 0; i < num_probes; i++) {
    const uint64_t pos = h % bits;
    if ((filter[pos / 8] & (1 << (pos % 8))) == 0) return false;
    h += delta;
  }
  return true;
}

}  // namespace kv
