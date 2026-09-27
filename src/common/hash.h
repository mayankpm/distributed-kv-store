#pragma once

#include <cstdint>
#include <string_view>

namespace kv {

// MurmurHash64A. Fast, well distributed, and stable across platforms, which
// matters because ring positions and bloom filter bits are persisted/shared.
uint64_t Hash64(std::string_view data, uint64_t seed = 0x9747b28c);

}  // namespace kv
