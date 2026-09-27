#pragma once

#include <cstdint>
#include <cstdio>
#include <functional>
#include <string>
#include <string_view>

namespace kv {

// Append-only record log with per-record checksums.
//
// Record layout: fixed32 crc32(payload) | fixed32 length | payload.
//
// A crash can leave a partially written record at the tail. The reader stops
// at the first record that is short or fails its checksum, which is exactly
// the prefix of writes that fully reached the file.
class LogWriter {
 public:
  LogWriter() = default;
  ~LogWriter();
  LogWriter(const LogWriter&) = delete;
  LogWriter& operator=(const LogWriter&) = delete;

  bool Open(const std::string& path, bool truncate = false);
  bool Append(std::string_view payload, bool sync);
  void Close();
  // Bytes written through this writer plus any existing file contents.
  uint64_t Size() const { return size_; }

 private:
  std::FILE* f_ = nullptr;
  uint64_t size_ = 0;
};

// Reads every intact record. Returns the byte offset just past the last good
// record so callers can truncate a torn tail before appending again.
uint64_t ReadLog(const std::string& path, const std::function<void(std::string_view)>& fn);

}  // namespace kv
