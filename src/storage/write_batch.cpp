#include "storage/write_batch.h"

#include "common/coding.h"

namespace kv {

std::string WriteBatch::Encode() const {
  std::string out;
  PutVarint64(&out, ops_.size());
  for (const Op& op : ops_) {
    out.push_back(static_cast<char>(op.type));
    PutLengthPrefixed(&out, op.key);
    PutLengthPrefixed(&out, op.value);
  }
  return out;
}

bool WriteBatch::Decode(std::string_view data, WriteBatch* out) {
  out->Clear();
  Reader r(data);
  const uint64_t n = r.Varint64();
  for (uint64_t i = 0; i < n && r.ok(); i++) {
    const uint8_t type = r.U8();
    std::string_view key = r.LengthPrefixed();
    std::string_view value = r.LengthPrefixed();
    if (type > 1) return false;
    out->ops_.push_back({static_cast<ValueType>(type), std::string(key), std::string(value)});
  }
  return r.ok() && r.done();
}

}  // namespace kv
