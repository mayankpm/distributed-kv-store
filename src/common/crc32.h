#pragma once

#include <cstdint>
#include <string_view>

namespace kv {

// CRC-32 (IEEE 802.3 polynomial). Used to detect torn or corrupt records.
uint32_t Crc32(std::string_view data, uint32_t seed = 0);

}  // namespace kv
