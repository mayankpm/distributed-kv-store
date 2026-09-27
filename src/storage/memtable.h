#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <shared_mutex>
#include <string>
#include <string_view>

namespace kv {

enum class ValueType : uint8_t { kDeletion = 0, kValue = 1 };

enum class LookupStatus { kFound, kDeleted, kNotFound };

// In-memory write buffer. Newer writes to the same key overwrite older ones;
// deletes are kept as tombstones so they shadow values in older SSTables.
// Reads may run concurrently with the single writer.
class MemTable {
 public:
  struct Entry {
    ValueType type;
    std::string value;
  };

  void Add(std::string_view key, ValueType type, std::string_view value);
  LookupStatus Get(std::string_view key, std::string* value) const;

  // Approximate heap footprint, used to decide when to flush.
  size_t ApproximateBytes() const;
  size_t Size() const;

  // Visits entries in key order. Only call once the memtable is immutable.
  void ForEach(const std::function<void(const std::string&, const Entry&)>& fn) const;

 private:
  mutable std::shared_mutex mu_;
  std::map<std::string, Entry, std::less<>> table_;
  size_t bytes_ = 0;
};

}  // namespace kv
