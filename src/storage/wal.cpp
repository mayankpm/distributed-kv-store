#include "storage/wal.h"

#include "common/coding.h"
#include "common/crc32.h"
#include "common/file_util.h"

namespace kv {

LogWriter::~LogWriter() { Close(); }

bool LogWriter::Open(const std::string& path, bool truncate) {
  Close();
  f_ = std::fopen(path.c_str(), truncate ? "wb" : "ab");
  if (f_ == nullptr) return false;
  std::fseek(f_, 0, SEEK_END);
  size_ = static_cast<uint64_t>(std::ftell(f_));
  return true;
}

bool LogWriter::Append(std::string_view payload, bool sync) {
  std::string header;
  PutFixed32(&header, Crc32(payload));
  PutFixed32(&header, static_cast<uint32_t>(payload.size()));
  if (std::fwrite(header.data(), 1, header.size(), f_) != header.size()) return false;
  if (std::fwrite(payload.data(), 1, payload.size(), f_) != payload.size()) return false;
  size_ += header.size() + payload.size();
  return sync ? SyncFile(f_) : std::fflush(f_) == 0;
}

void LogWriter::Close() {
  if (f_ != nullptr) {
    std::fclose(f_);
    f_ = nullptr;
  }
}

uint64_t ReadLog(const std::string& path, const std::function<void(std::string_view)>& fn) {
  std::string data;
  if (!ReadFile(path, &data)) return 0;
  uint64_t pos = 0;
  while (data.size() - pos >= 8) {
    const uint32_t crc = DecodeFixed32(data.data() + pos);
    const uint32_t len = DecodeFixed32(data.data() + pos + 4);
    if (len > data.size() - pos - 8) break;  // Torn write at the tail.
    std::string_view payload(data.data() + pos + 8, len);
    if (Crc32(payload) != crc) break;  // Corrupt record: stop here.
    fn(payload);
    pos += 8 + len;
  }
  return pos;
}

}  // namespace kv
