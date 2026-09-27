#include "common/crc32.h"

#include <array>

namespace kv {
namespace {

constexpr std::array<uint32_t, 256> MakeTable() {
  std::array<uint32_t, 256> table{};
  for (uint32_t i = 0; i < 256; i++) {
    uint32_t c = i;
    for (int k = 0; k < 8; k++) c = (c & 1) ? 0xEDB88320u ^ (c >> 1) : c >> 1;
    table[i] = c;
  }
  return table;
}

constexpr auto kTable = MakeTable();

}  // namespace

uint32_t Crc32(std::string_view data, uint32_t seed) {
  uint32_t c = seed ^ 0xFFFFFFFFu;
  for (char ch : data) c = kTable[(c ^ static_cast<uint8_t>(ch)) & 0xff] ^ (c >> 8);
  return c ^ 0xFFFFFFFFu;
}

}  // namespace kv
