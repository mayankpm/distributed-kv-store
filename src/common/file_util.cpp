#include "common/file_util.h"

#include <cstdint>

#ifdef _WIN32
#include <io.h>
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace kv {

bool SyncFile(std::FILE* f) {
  if (std::fflush(f) != 0) return false;
#ifdef _WIN32
  return _commit(_fileno(f)) == 0;
#else
  return fsync(fileno(f)) == 0;
#endif
}

static bool RenameReplace(const std::string& from, const std::string& to) {
#ifdef _WIN32
  return MoveFileExA(from.c_str(), to.c_str(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH) != 0;
#else
  return std::rename(from.c_str(), to.c_str()) == 0;
#endif
}

bool WriteFileAtomic(const std::string& path, const std::string& contents, bool sync) {
  const std::string tmp = path + ".tmp";
  std::FILE* f = std::fopen(tmp.c_str(), "wb");
  if (f == nullptr) return false;
  bool ok = std::fwrite(contents.data(), 1, contents.size(), f) == contents.size();
  ok = ok && (sync ? SyncFile(f) : std::fflush(f) == 0);
  ok = (std::fclose(f) == 0) && ok;
  return ok && RenameReplace(tmp, path);
}

bool ReadFile(const std::string& path, std::string* out) {
  std::FILE* f = std::fopen(path.c_str(), "rb");
  if (f == nullptr) return false;
  out->clear();
  char buf[1 << 16];
  size_t n;
  while ((n = std::fread(buf, 1, sizeof(buf), f)) > 0) out->append(buf, n);
  bool ok = std::ferror(f) == 0;
  std::fclose(f);
  return ok;
}

bool ReadAt(std::FILE* f, uint64_t offset, size_t n, std::string* out) {
#ifdef _WIN32
  if (_fseeki64(f, static_cast<int64_t>(offset), SEEK_SET) != 0) return false;
#else
  if (fseeko(f, static_cast<off_t>(offset), SEEK_SET) != 0) return false;
#endif
  out->resize(n);
  return std::fread(out->data(), 1, n, f) == n;
}

}  // namespace kv
