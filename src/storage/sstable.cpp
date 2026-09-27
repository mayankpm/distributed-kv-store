#include "storage/sstable.h"

#include <algorithm>
#include <filesystem>

#include "common/coding.h"
#include "common/crc32.h"
#include "common/file_util.h"
#include "storage/bloom_filter.h"

namespace kv {
namespace {

constexpr uint64_t kTableMagic = 0x4b56535354424c31ULL;  // "KVSSTBL1"
constexpr size_t kFooterSize = 48;

bool ParseBlock(std::string_view contents, std::vector<Table::Entry>* out) {
  out->clear();
  Reader r(contents);
  while (!r.done() && r.ok()) {
    std::string_view key = r.LengthPrefixed();
    const uint8_t type = r.U8();
    std::string_view value = r.LengthPrefixed();
    if (!r.ok() || type > 1) return false;
    out->push_back({std::string(key), static_cast<ValueType>(type), std::string(value)});
  }
  return r.ok();
}

}  // namespace

// ---------------------------------------------------------------------------
// TableBuilder

TableBuilder::TableBuilder(std::string path, int bloom_bits_per_key, size_t block_size)
    : path_(std::move(path)), block_size_(block_size), bloom_(new BloomFilterBuilder(bloom_bits_per_key)) {}

TableBuilder::~TableBuilder() {
  if (f_ != nullptr) std::fclose(f_);
}

bool TableBuilder::Open() {
  f_ = std::fopen(path_.c_str(), "wb");
  return f_ != nullptr;
}

void TableBuilder::Add(std::string_view key, ValueType type, std::string_view value) {
  if (num_entries_ == 0) smallest_.assign(key);
  largest_.assign(key);
  last_key_.assign(key);
  bloom_->AddKey(key);
  PutLengthPrefixed(&pending_, key);
  pending_.push_back(static_cast<char>(type));
  PutLengthPrefixed(&pending_, value);
  num_entries_++;
  if (pending_.size() >= block_size_) ok_ = FlushBlock() && ok_;
}

bool TableBuilder::FlushBlock() {
  if (pending_.empty()) return true;
  PutFixed32(&pending_, Crc32(pending_));
  const bool ok = std::fwrite(pending_.data(), 1, pending_.size(), f_) == pending_.size();
  PutLengthPrefixed(&index_, last_key_);
  PutFixed64(&index_, offset_);
  PutFixed64(&index_, pending_.size());
  offset_ += pending_.size();
  pending_.clear();
  return ok;
}

bool TableBuilder::Finish(bool sync) {
  ok_ = FlushBlock() && ok_;

  const std::string filter = bloom_->Finish();
  const uint64_t filter_off = offset_;
  ok_ = ok_ && std::fwrite(filter.data(), 1, filter.size(), f_) == filter.size();
  offset_ += filter.size();

  const uint64_t index_off = offset_;
  ok_ = ok_ && std::fwrite(index_.data(), 1, index_.size(), f_) == index_.size();
  offset_ += index_.size();

  std::string footer;
  PutFixed64(&footer, filter_off);
  PutFixed64(&footer, filter.size());
  PutFixed64(&footer, index_off);
  PutFixed64(&footer, index_.size());
  PutFixed64(&footer, num_entries_);
  PutFixed64(&footer, kTableMagic);
  ok_ = ok_ && std::fwrite(footer.data(), 1, footer.size(), f_) == footer.size();
  offset_ += footer.size();

  ok_ = ok_ && (sync ? SyncFile(f_) : std::fflush(f_) == 0);
  ok_ = (std::fclose(f_) == 0) && ok_;
  f_ = nullptr;
  return ok_;
}

void TableBuilder::Abandon() {
  if (f_ != nullptr) {
    std::fclose(f_);
    f_ = nullptr;
  }
  std::error_code ec;
  std::filesystem::remove(path_, ec);
}

// ---------------------------------------------------------------------------
// Table

std::shared_ptr<Table> Table::Open(const std::string& path, uint64_t file_number, BlockCache* cache) {
  std::shared_ptr<Table> t(new Table());
  t->path_ = path;
  t->file_number_ = file_number;
  t->cache_ = cache;
  t->f_ = std::fopen(path.c_str(), "rb");
  if (t->f_ == nullptr) return nullptr;

  std::error_code ec;
  const uint64_t size = std::filesystem::file_size(path, ec);
  if (ec || size < kFooterSize) return nullptr;

  std::string footer;
  if (!ReadAt(t->f_, size - kFooterSize, kFooterSize, &footer)) return nullptr;
  Reader fr(footer);
  const uint64_t filter_off = fr.Fixed64();
  const uint64_t filter_size = fr.Fixed64();
  const uint64_t index_off = fr.Fixed64();
  const uint64_t index_size = fr.Fixed64();
  t->num_entries_ = fr.Fixed64();
  if (fr.Fixed64() != kTableMagic) return nullptr;
  if (filter_off + filter_size > size || index_off + index_size > size) return nullptr;

  if (!ReadAt(t->f_, filter_off, filter_size, &t->filter_)) return nullptr;
  std::string index;
  if (!ReadAt(t->f_, index_off, index_size, &index)) return nullptr;

  Reader ir(index);
  while (!ir.done()) {
    IndexEntry e;
    e.last_key = std::string(ir.LengthPrefixed());
    e.offset = ir.Fixed64();
    e.size = ir.Fixed64();
    if (!ir.ok()) return nullptr;
    t->index_.push_back(std::move(e));
  }
  return t;
}

Table::~Table() {
  if (f_ != nullptr) std::fclose(f_);
  if (obsolete_) {
    std::error_code ec;
    std::filesystem::remove(path_, ec);
  }
}

bool Table::ReadBlock(size_t idx, std::vector<Entry>* out) {
  const IndexEntry& ie = index_[idx];
  if (ie.size < 4) return false;
  std::string raw;
  {
    std::lock_guard lock(file_mu_);
    if (!ReadAt(f_, ie.offset, ie.size, &raw)) return false;
  }
  std::string_view contents(raw.data(), raw.size() - 4);
  if (Crc32(contents) != DecodeFixed32(raw.data() + raw.size() - 4)) return false;
  return ParseBlock(contents, out);
}

LookupStatus Table::Get(std::string_view key, std::string* value, bool* bloom_skipped) {
  if (bloom_skipped != nullptr) *bloom_skipped = false;
  if (!BloomMayContain(filter_, key)) {
    if (bloom_skipped != nullptr) *bloom_skipped = true;
    return LookupStatus::kNotFound;
  }
  // First block whose last key is >= key is the only one that can hold it.
  auto it = std::lower_bound(index_.begin(), index_.end(), key,
                             [](const IndexEntry& e, std::string_view k) { return e.last_key < k; });
  if (it == index_.end()) return LookupStatus::kNotFound;

  const auto idx = static_cast<size_t>(it - index_.begin());
  std::shared_ptr<const Block> block = cache_ ? cache_->Lookup(file_number_, idx) : nullptr;
  if (!block) {
    auto fresh = std::make_shared<Block>();
    if (!ReadBlock(idx, &fresh->entries)) return LookupStatus::kNotFound;
    fresh->bytes = index_[idx].size + fresh->entries.size() * sizeof(Entry);
    if (cache_) cache_->Insert(file_number_, idx, fresh);
    block = std::move(fresh);
  }
  const auto& entries = block->entries;
  auto e = std::lower_bound(entries.begin(), entries.end(), key,
                            [](const Entry& x, std::string_view k) { return x.key < k; });
  if (e == entries.end() || e->key != key) return LookupStatus::kNotFound;
  if (e->type == ValueType::kDeletion) return LookupStatus::kDeleted;
  *value = e->value;
  return LookupStatus::kFound;
}

Table::Iterator::Iterator(std::shared_ptr<Table> table) : table_(std::move(table)) {
  for (size_t i = 0; i < table_->index_.size(); i++) {
    if (LoadBlock(i)) return;
    if (!ok_) return;
  }
}

bool Table::Iterator::LoadBlock(size_t idx) {
  block_idx_ = idx;
  pos_ = 0;
  if (!table_->ReadBlock(idx, &block_)) {
    ok_ = false;
    valid_ = false;
    return false;
  }
  valid_ = !block_.empty();
  return valid_;
}

void Table::Iterator::Next() {
  if (++pos_ < block_.size()) return;
  valid_ = false;
  for (size_t i = block_idx_ + 1; i < table_->index_.size(); i++) {
    if (LoadBlock(i) || !ok_) return;
  }
}

}  // namespace kv
