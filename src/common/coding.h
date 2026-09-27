// Little-endian fixed-width and varint encoding helpers shared by every
// on-disk format (WAL, SSTable, Raft log) and the wire protocol.
#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

namespace kv {

inline void PutFixed32(std::string* dst, uint32_t v) {
  char buf[4];
  for (int i = 0; i < 4; i++) buf[i] = static_cast<char>((v >> (8 * i)) & 0xff);
  dst->append(buf, 4);
}

inline void PutFixed64(std::string* dst, uint64_t v) {
  char buf[8];
  for (int i = 0; i < 8; i++) buf[i] = static_cast<char>((v >> (8 * i)) & 0xff);
  dst->append(buf, 8);
}

inline uint32_t DecodeFixed32(const char* p) {
  uint32_t v = 0;
  for (int i = 0; i < 4; i++) v |= static_cast<uint32_t>(static_cast<uint8_t>(p[i])) << (8 * i);
  return v;
}

inline uint64_t DecodeFixed64(const char* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; i++) v |= static_cast<uint64_t>(static_cast<uint8_t>(p[i])) << (8 * i);
  return v;
}

inline void PutVarint64(std::string* dst, uint64_t v) {
  while (v >= 0x80) {
    dst->push_back(static_cast<char>((v & 0x7f) | 0x80));
    v >>= 7;
  }
  dst->push_back(static_cast<char>(v));
}

inline void PutLengthPrefixed(std::string* dst, std::string_view s) {
  PutVarint64(dst, s.size());
  dst->append(s.data(), s.size());
}

// Bounds-checked sequential decoder. Any read past the end flips ok() to
// false and returns zero values, so callers check ok() once at the end.
class Reader {
 public:
  explicit Reader(std::string_view data) : data_(data) {}

  uint8_t U8() {
    if (!Need(1)) return 0;
    return static_cast<uint8_t>(data_[pos_++]);
  }

  uint32_t Fixed32() {
    if (!Need(4)) return 0;
    uint32_t v = DecodeFixed32(data_.data() + pos_);
    pos_ += 4;
    return v;
  }

  uint64_t Fixed64() {
    if (!Need(8)) return 0;
    uint64_t v = DecodeFixed64(data_.data() + pos_);
    pos_ += 8;
    return v;
  }

  uint64_t Varint64() {
    uint64_t result = 0;
    for (int shift = 0; shift <= 63; shift += 7) {
      if (!Need(1)) return 0;
      uint8_t byte = static_cast<uint8_t>(data_[pos_++]);
      result |= static_cast<uint64_t>(byte & 0x7f) << shift;
      if ((byte & 0x80) == 0) return result;
    }
    ok_ = false;
    return 0;
  }

  std::string_view LengthPrefixed() {
    uint64_t n = Varint64();
    if (!Need(n)) return {};
    std::string_view s = data_.substr(pos_, n);
    pos_ += n;
    return s;
  }

  bool ok() const { return ok_; }
  bool done() const { return pos_ >= data_.size(); }
  size_t pos() const { return pos_; }

 private:
  bool Need(uint64_t n) {
    if (!ok_ || n > data_.size() - pos_) {
      ok_ = false;
      return false;
    }
    return true;
  }

  std::string_view data_;
  size_t pos_ = 0;
  bool ok_ = true;
};

}  // namespace kv
