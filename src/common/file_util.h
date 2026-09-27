// Small portable file helpers. Everything durable in the system goes through
// these so fsync and atomic replace semantics live in one place.
#pragma once

#include <cstdint>
#include <cstdio>
#include <string>

namespace kv {

// Flushes stdio buffers and asks the OS to persist the file to disk.
bool SyncFile(std::FILE* f);

// Writes `contents` to `path` atomically: write a temp file, fsync it, then
// rename over the destination (replacing it if it exists).
bool WriteFileAtomic(const std::string& path, const std::string& contents, bool sync);

// Reads a whole file. Returns false if it does not exist or cannot be read.
bool ReadFile(const std::string& path, std::string* out);

// Reads `n` bytes at `offset`. Returns false on short read.
bool ReadAt(std::FILE* f, uint64_t offset, size_t n, std::string* out);

}  // namespace kv
