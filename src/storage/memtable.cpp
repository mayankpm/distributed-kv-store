#include "storage/memtable.h"

#include <mutex>

namespace kv {

namespace {
// Rough per-node overhead of a std::map entry holding two strings.
constexpr size_t kNodeOverhead = 64;
}  // namespace

void MemTable::Add(std::string_view key, ValueType type, std::string_view value) {
  std::unique_lock lock(mu_);
  auto it = table_.find(key);
  if (it == table_.end()) {
    table_.emplace(std::string(key), Entry{type, std::string(value)});
    bytes_ += key.size() + value.size() + kNodeOverhead;
  } else {
    bytes_ += value.size();
    bytes_ -= it->second.value.size();
    it->second.type = type;
    it->second.value.assign(value.data(), value.size());
  }
}

LookupStatus MemTable::Get(std::string_view key, std::string* value) const {
  std::shared_lock lock(mu_);
  auto it = table_.find(key);
  if (it == table_.end()) return LookupStatus::kNotFound;
  if (it->second.type == ValueType::kDeletion) return LookupStatus::kDeleted;
  *value = it->second.value;
  return LookupStatus::kFound;
}

size_t MemTable::ApproximateBytes() const {
  std::shared_lock lock(mu_);
  return bytes_;
}

size_t MemTable::Size() const {
  std::shared_lock lock(mu_);
  return table_.size();
}

void MemTable::ForEach(const std::function<void(const std::string&, const Entry&)>& fn) const {
  std::shared_lock lock(mu_);
  for (const auto& [k, e] : table_) fn(k, e);
}

}  // namespace kv
