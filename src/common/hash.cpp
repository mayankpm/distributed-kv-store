#include "common/hash.h"

#include "common/coding.h"

namespace kv {

uint64_t Hash64(std::string_view data, uint64_t seed) {
  constexpr uint64_t m = 0xc6a4a7935bd1e995ULL;
  constexpr int r = 47;
  const size_t len = data.size();
  uint64_t h = seed ^ (len * m);

  const char* p = data.data();
  const char* end = p + (len / 8) * 8;
  for (; p != end; p += 8) {
    uint64_t k = DecodeFixed64(p);
    k *= m;
    k ^= k >> r;
    k *= m;
    h ^= k;
    h *= m;
  }

  const auto* tail = reinterpret_cast<const uint8_t*>(p);
  switch (len & 7) {
    case 7: h ^= static_cast<uint64_t>(tail[6]) << 48; [[fallthrough]];
    case 6: h ^= static_cast<uint64_t>(tail[5]) << 40; [[fallthrough]];
    case 5: h ^= static_cast<uint64_t>(tail[4]) << 32; [[fallthrough]];
    case 4: h ^= static_cast<uint64_t>(tail[3]) << 24; [[fallthrough]];
    case 3: h ^= static_cast<uint64_t>(tail[2]) << 16; [[fallthrough]];
    case 2: h ^= static_cast<uint64_t>(tail[1]) << 8; [[fallthrough]];
    case 1:
      h ^= static_cast<uint64_t>(tail[0]);
      h *= m;
  }

  h ^= h >> r;
  h *= m;
  h ^= h >> r;
  return h;
}

}  // namespace kv
