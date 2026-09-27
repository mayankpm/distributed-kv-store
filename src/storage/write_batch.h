#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "storage/memtable.h"

namespace kv {

// A group of updates applied atomically: the whole batch is one WAL record,
// so after a crash either every update is visible or none is.
class WriteBatch {
 public:
  struct Op {
    ValueType type;
    std::string key;
    std::string value;
  };

  void Put(std::string_view key, std::string_view value) {
    ops_.push_back({ValueType::kValue, std::string(key), std::string(value)});
  }
  void Delete(std::string_view key) { ops_.push_back({ValueType::kDeletion, std::string(key), {}}); }
  void Clear() { ops_.clear(); }

  const std::vector<Op>& ops() const { return ops_; }
  bool empty() const { return ops_.empty(); }

  std::string Encode() const;
  static bool Decode(std::string_view data, WriteBatch* out);

 private:
  std::vector<Op> ops_;
};

}  // namespace kv
