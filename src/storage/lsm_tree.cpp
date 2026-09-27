#include "storage/lsm_tree.h"

#include <algorithm>
#include <cinttypes>
#include <cstdio>
#include <filesystem>
#include <queue>
#include <sstream>
#include <tuple>

#include "common/file_util.h"

namespace fs = std::filesystem;

namespace kv {
namespace {

// Keys are arbitrary bytes, so the text MANIFEST stores them hex encoded.
// The "k" prefix keeps empty keys from vanishing under stream extraction.
std::string EncodeKey(std::string_view s) {
  static const char* kHex = "0123456789abcdef";
  std::string out = "k";
  for (char c : s) {
    out.push_back(kHex[(static_cast<uint8_t>(c) >> 4) & 0xf]);
    out.push_back(kHex[static_cast<uint8_t>(c) & 0xf]);
  }
  return out;
}

bool DecodeKey(std::string_view s, std::string* out) {
  if (s.empty() || s[0] != 'k' || (s.size() - 1) % 2 != 0) return false;
  auto nibble = [](char c) -> int {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    return -1;
  };
  out->clear();
  for (size_t i = 1; i < s.size(); i += 2) {
    int hi = nibble(s[i]), lo = nibble(s[i + 1]);
    if (hi < 0 || lo < 0) return false;
    out->push_back(static_cast<char>((hi << 4) | lo));
  }
  return true;
}

// Parses "000123.sst" style names. Returns false for anything else.
bool ParseFileName(const std::string& name, uint64_t* number, std::string* ext) {
  const size_t dot = name.find('.');
  if (dot == std::string::npos || dot == 0) return false;
  for (size_t i = 0; i < dot; i++) {
    if (name[i] < '0' || name[i] > '9') return false;
  }
  *number = std::stoull(name.substr(0, dot));
  *ext = name.substr(dot + 1);
  return true;
}

bool Overlaps(const std::string& smallest, const std::string& largest, std::string_view lo, std::string_view hi) {
  return !(largest < lo || smallest > hi);
}

}  // namespace

LsmTree::LsmTree(std::string dir, const LsmOptions& options)
    : dir_(std::move(dir)),
      options_(options),
      block_cache_(options.block_cache_bytes > 0 ? std::make_unique<BlockCache>(options.block_cache_bytes) : nullptr) {}

std::unique_ptr<LsmTree> LsmTree::Open(const std::string& dir, const LsmOptions& options, std::string* error) {
  std::error_code ec;
  fs::create_directories(dir, ec);
  if (ec) {
    *error = "cannot create " + dir + ": " + ec.message();
    return nullptr;
  }
  std::unique_ptr<LsmTree> db(new LsmTree(dir, options));
  if (!db->Recover(error)) return nullptr;
  db->bg_thread_ = std::thread([p = db.get()] { p->BackgroundLoop(); });
  return db;
}

LsmTree::~LsmTree() {
  {
    std::lock_guard lock(mu_);
    shutting_down_ = true;
  }
  work_cv_.notify_all();
  if (bg_thread_.joinable()) bg_thread_.join();
  // Any unflushed memtable data is still in its WAL and replays on reopen.
}

std::string LsmTree::TablePath(uint64_t number) const {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%06" PRIu64 ".sst", number);
  return dir_ + "/" + buf;
}

std::string LsmTree::LogPath(uint64_t number) const {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%06" PRIu64 ".log", number);
  return dir_ + "/" + buf;
}

// ---------------------------------------------------------------------------
// Recovery and manifest

bool LsmTree::Recover(std::string* error) {
  auto v = std::make_shared<Version>();
  uint64_t log_number = 0;
  uint64_t next_file = 1;
  std::vector<uint64_t> live_tables;

  std::string manifest;
  if (ReadFile(dir_ + "/MANIFEST", &manifest)) {
    std::istringstream in(manifest);
    std::string tag;
    while (in >> tag) {
      if (tag == "next_file") {
        in >> next_file;
      } else if (tag == "log") {
        in >> log_number;
      } else if (tag == "file") {
        int level;
        auto f = std::make_shared<FileMeta>();
        std::string lo, hi;
        in >> level >> f->number >> f->size >> lo >> hi;
        if (!in || level < 0 || level >= LsmOptions::kNumLevels || !DecodeKey(lo, &f->smallest) ||
            !DecodeKey(hi, &f->largest)) {
          *error = "corrupt MANIFEST";
          return false;
        }
        f->table = Table::Open(TablePath(f->number), f->number, block_cache_.get());
        if (!f->table) {
          *error = "cannot open table " + TablePath(f->number);
          return false;
        }
        live_tables.push_back(f->number);
        v->levels[level].push_back(f);
      } else {
        *error = "corrupt MANIFEST tag: " + tag;
        return false;
      }
    }
  }
  std::sort(v->levels[0].begin(), v->levels[0].end(),
            [](const FilePtr& a, const FilePtr& b) { return a->number > b->number; });
  for (int l = 1; l < LsmOptions::kNumLevels; l++) {
    std::sort(v->levels[l].begin(), v->levels[l].end(),
              [](const FilePtr& a, const FilePtr& b) { return a->smallest < b->smallest; });
  }

  // Collect WALs to replay and clean up files a crash left behind (tables from
  // an interrupted flush/compaction, WALs already folded into tables).
  std::vector<uint64_t> logs;
  uint64_t max_number = 0;
  for (const auto& entry : fs::directory_iterator(dir_)) {
    uint64_t number;
    std::string ext;
    if (!ParseFileName(entry.path().filename().string(), &number, &ext)) continue;
    max_number = std::max(max_number, number);
    std::error_code ec;
    if (ext == "sst" && std::find(live_tables.begin(), live_tables.end(), number) == live_tables.end()) {
      fs::remove(entry.path(), ec);
    } else if (ext == "log") {
      if (number >= log_number) {
        logs.push_back(number);
      } else {
        fs::remove(entry.path(), ec);
      }
    }
  }
  std::sort(logs.begin(), logs.end());
  next_file_number_ = std::max(next_file, max_number + 1);

  auto recovered = std::make_shared<MemTable>();
  for (uint64_t number : logs) {
    const uint64_t good = ReadLog(LogPath(number), [&](std::string_view record) {
      WriteBatch batch;
      if (!WriteBatch::Decode(record, &batch)) return;
      for (const auto& op : batch.ops()) recovered->Add(op.key, op.type, op.value);
    });
    (void)good;
  }
  if (recovered->Size() > 0) {
    FilePtr f = WriteMemTableToTable(*recovered, error);
    if (!f) return false;
    v->levels[0].insert(v->levels[0].begin(), f);
  }

  wal_number_ = next_file_number_++;
  if (!wal_.Open(LogPath(wal_number_), /*truncate=*/true)) {
    *error = "cannot open WAL";
    return false;
  }
  if (!WriteManifest(*v, wal_number_)) {
    *error = "cannot write MANIFEST";
    return false;
  }
  for (uint64_t number : logs) {
    std::error_code ec;
    fs::remove(LogPath(number), ec);
  }
  current_ = v;
  mem_ = std::make_shared<MemTable>();
  return true;
}

bool LsmTree::WriteManifest(const Version& v, uint64_t log_number) {
  std::ostringstream out;
  out << "next_file " << next_file_number_.load() << "\n";
  out << "log " << log_number << "\n";
  for (int l = 0; l < LsmOptions::kNumLevels; l++) {
    for (const FilePtr& f : v.levels[l]) {
      out << "file " << l << " " << f->number << " " << f->size << " " << EncodeKey(f->smallest) << " "
          << EncodeKey(f->largest) << "\n";
    }
  }
  return WriteFileAtomic(dir_ + "/MANIFEST", out.str(), /*sync=*/true);
}

LsmTree::FilePtr LsmTree::WriteMemTableToTable(const MemTable& mem, std::string* error) {
  const uint64_t number = next_file_number_++;
  TableBuilder builder(TablePath(number), options_.bloom_bits_per_key, options_.block_size);
  if (!builder.Open()) {
    *error = "cannot create " + TablePath(number);
    return nullptr;
  }
  mem.ForEach([&](const std::string& k, const MemTable::Entry& e) { builder.Add(k, e.type, e.value); });
  auto f = std::make_shared<FileMeta>();
  f->number = number;
  f->smallest = builder.smallest();
  f->largest = builder.largest();
  // Tables are always synced: once flushed, the WAL backing them is deleted.
  if (!builder.Finish(/*sync=*/true)) {
    builder.Abandon();
    *error = "failed writing " + TablePath(number);
    return nullptr;
  }
  f->size = builder.FileSize();
  f->table = Table::Open(TablePath(number), number, block_cache_.get());
  if (!f->table) {
    *error = "cannot reopen " + TablePath(number);
    return nullptr;
  }
  return f;
}

// ---------------------------------------------------------------------------
// Writes

bool LsmTree::Put(std::string_view key, std::string_view value) {
  WriteBatch b;
  b.Put(key, value);
  return Write(b);
}

bool LsmTree::Delete(std::string_view key) {
  WriteBatch b;
  b.Delete(key);
  return Write(b);
}

bool LsmTree::Write(const WriteBatch& batch) {
  std::unique_lock lock(mu_);
  if (!MakeRoomForWrite(lock)) return false;
  if (!wal_.Append(batch.Encode(), options_.sync_writes)) {
    bg_error_ = true;
    return false;
  }
  for (const auto& op : batch.ops()) mem_->Add(op.key, op.type, op.value);
  return true;
}

bool LsmTree::MakeRoomForWrite(std::unique_lock<std::mutex>& lock) {
  while (true) {
    if (bg_error_) return false;
    if (static_cast<int>(current_->levels[0].size()) >= options_.l0_stop_writes_trigger) {
      // Too many overlapping L0 files make reads slow: stall until compaction catches up.
      done_cv_.wait(lock);
    } else if (mem_->ApproximateBytes() < options_.memtable_bytes) {
      return true;
    } else if (imm_) {
      // Previous memtable is still being flushed.
      done_cv_.wait(lock);
    } else if (!RotateMemTable()) {
      bg_error_ = true;
      return false;
    }
  }
}

bool LsmTree::RotateMemTable() {
  const uint64_t new_log = next_file_number_++;
  wal_.Close();
  if (!wal_.Open(LogPath(new_log), /*truncate=*/true)) return false;
  imm_wal_number_ = wal_number_;
  wal_number_ = new_log;
  imm_ = mem_;
  mem_ = std::make_shared<MemTable>();
  work_cv_.notify_one();
  return true;
}

// ---------------------------------------------------------------------------
// Reads

std::optional<std::string> LsmTree::Get(std::string_view key) {
  std::shared_ptr<MemTable> mem, imm;
  std::shared_ptr<const Version> v;
  {
    std::lock_guard lock(mu_);
    mem = mem_;
    imm = imm_;
    v = current_;
  }

  std::string value;
  switch (mem->Get(key, &value)) {
    case LookupStatus::kFound: return value;
    case LookupStatus::kDeleted: return std::nullopt;
    case LookupStatus::kNotFound: break;
  }
  if (imm) {
    switch (imm->Get(key, &value)) {
      case LookupStatus::kFound: return value;
      case LookupStatus::kDeleted: return std::nullopt;
      case LookupStatus::kNotFound: break;
    }
  }

  auto probe = [&](const FilePtr& f) {
    table_probes_++;
    bool skipped = false;
    LookupStatus s = f->table->Get(key, &value, &skipped);
    if (skipped) bloom_skips_++;
    return s;
  };

  // L0 files may overlap, so check every candidate newest first.
  for (const FilePtr& f : v->levels[0]) {
    if (key < f->smallest || key > f->largest) continue;
    LookupStatus s = probe(f);
    if (s == LookupStatus::kFound) return value;
    if (s == LookupStatus::kDeleted) return std::nullopt;
  }
  // Deeper levels are sorted and disjoint: at most one candidate per level.
  for (int l = 1; l < LsmOptions::kNumLevels; l++) {
    const auto& files = v->levels[l];
    auto it = std::lower_bound(files.begin(), files.end(), key,
                               [](const FilePtr& f, std::string_view k) { return f->largest < k; });
    if (it == files.end() || key < (*it)->smallest) continue;
    LookupStatus s = probe(*it);
    if (s == LookupStatus::kFound) return value;
    if (s == LookupStatus::kDeleted) return std::nullopt;
  }
  return std::nullopt;
}

// ---------------------------------------------------------------------------
// Background flush and compaction

bool LsmTree::Flush() {
  std::unique_lock lock(mu_);
  done_cv_.wait(lock, [&] { return !imm_ || bg_error_; });
  if (bg_error_) return false;
  if (mem_->Size() > 0 && !RotateMemTable()) {
    bg_error_ = true;
    return false;
  }
  done_cv_.wait(lock, [&] { return !imm_ || bg_error_; });
  return !bg_error_;
}

void LsmTree::WaitForIdle() {
  std::unique_lock lock(mu_);
  done_cv_.wait(lock, [&] { return bg_error_ || (!imm_ && !NeedsCompaction()); });
}

void LsmTree::BackgroundLoop() {
  std::unique_lock lock(mu_);
  while (true) {
    work_cv_.wait(lock, [&] { return shutting_down_ || (!bg_error_ && (imm_ || NeedsCompaction())); });
    if (shutting_down_) break;
    if (imm_) {
      DoFlush(lock);
    } else {
      DoCompaction(lock);
    }
    done_cv_.notify_all();
  }
  done_cv_.notify_all();
}

uint64_t LsmTree::MaxBytesForLevel(int level) const {
  uint64_t bytes = options_.level1_max_bytes;
  for (int l = 1; l < level; l++) bytes *= static_cast<uint64_t>(options_.level_size_multiplier);
  return bytes;
}

double LsmTree::LevelScore(const Version& v, int level) const {
  if (level >= LsmOptions::kNumLevels - 1) return 0;  // Bottom level never compacts down.
  if (level == 0) return static_cast<double>(v.levels[0].size()) / options_.l0_compaction_trigger;
  uint64_t bytes = 0;
  for (const FilePtr& f : v.levels[level]) bytes += f->size;
  return static_cast<double>(bytes) / static_cast<double>(MaxBytesForLevel(level));
}

bool LsmTree::NeedsCompaction() const {
  for (int l = 0; l < LsmOptions::kNumLevels - 1; l++) {
    if (LevelScore(*current_, l) >= 1.0) return true;
  }
  return false;
}

void LsmTree::DoFlush(std::unique_lock<std::mutex>& lock) {
  std::shared_ptr<MemTable> imm = imm_;
  const uint64_t imm_wal = imm_wal_number_;
  lock.unlock();

  FilePtr f;
  std::string error;
  bool ok = true;
  if (imm->Size() > 0) {
    f = WriteMemTableToTable(*imm, &error);
    ok = f != nullptr;
  }

  lock.lock();
  if (!ok) {
    std::fprintf(stderr, "lsm flush failed: %s\n", error.c_str());
    bg_error_ = true;
    return;
  }
  auto v = std::make_shared<Version>(*current_);
  if (f) v->levels[0].insert(v->levels[0].begin(), f);
  // The frozen memtable is now durable, so the current WAL is the oldest needed.
  if (!WriteManifest(*v, wal_number_)) {
    bg_error_ = true;
    return;
  }
  current_ = v;
  imm_.reset();
  imm_wal_number_ = 0;
  std::error_code ec;
  fs::remove(LogPath(imm_wal), ec);
  stats_.flushes++;
}

bool LsmTree::PickCompaction(Compaction* c) const {
  const Version& v = *current_;
  int level = -1;
  double best = 1.0;
  for (int l = 0; l < LsmOptions::kNumLevels - 1; l++) {
    const double score = LevelScore(v, l);
    if (score >= best) {
      best = score;
      level = l;
    }
  }
  if (level < 0) return false;
  c->level = level;

  if (level == 0) {
    // L0 files overlap each other, so compact all of them together.
    c->inputs = v.levels[0];
  } else {
    // Round-robin through the key space so the whole level gets rewritten.
    const auto& files = v.levels[level];
    FilePtr pick = files.front();
    for (const FilePtr& f : files) {
      if (f->largest > compact_pointer_[level]) {
        pick = f;
        break;
      }
    }
    c->inputs = {pick};
  }

  std::string lo = c->inputs.front()->smallest, hi = c->inputs.front()->largest;
  for (const FilePtr& f : c->inputs) {
    lo = std::min(lo, f->smallest);
    hi = std::max(hi, f->largest);
  }
  for (const FilePtr& f : v.levels[level + 1]) {
    if (Overlaps(f->smallest, f->largest, lo, hi)) c->next_inputs.push_back(f);
  }
  return true;
}

bool LsmTree::IsBaseLevelForKey(const Version& v, int level, std::string_view key) const {
  for (int l = level + 1; l < LsmOptions::kNumLevels; l++) {
    for (const FilePtr& f : v.levels[l]) {
      if (key >= f->smallest && key <= f->largest) return false;
    }
  }
  return true;
}

void LsmTree::DoCompaction(std::unique_lock<std::mutex>& lock) {
  Compaction c;
  if (!PickCompaction(&c)) return;
  const int out_level = c.level + 1;
  std::shared_ptr<const Version> base = current_;

  // Trivial move: a single file with nothing to merge against just changes level.
  if (c.level > 0 && c.inputs.size() == 1 && c.next_inputs.empty()) {
    auto v = std::make_shared<Version>(*current_);
    auto& from = v->levels[c.level];
    from.erase(std::remove(from.begin(), from.end(), c.inputs[0]), from.end());
    auto& to = v->levels[out_level];
    to.insert(std::upper_bound(to.begin(), to.end(), c.inputs[0],
                               [](const FilePtr& a, const FilePtr& b) { return a->smallest < b->smallest; }),
              c.inputs[0]);
    if (!WriteManifest(*v, OldestLiveLog())) {
      bg_error_ = true;
      return;
    }
    current_ = v;
    compact_pointer_[c.level] = c.inputs[0]->largest;
    stats_.trivial_moves++;
    return;
  }

  lock.unlock();

  // K-way merge. Lower rank means newer data, so among equal keys the entry
  // from the lowest rank wins and the rest are shadowed.
  struct Source {
    std::unique_ptr<Table::Iterator> it;
    int rank;
  };
  std::vector<Source> sources;
  uint64_t bytes_read = 0;
  for (size_t i = 0; i < c.inputs.size(); i++) {
    // L0 inputs are ordered newest first; deeper levels never overlap within a level.
    sources.push_back({std::make_unique<Table::Iterator>(c.inputs[i]->table), c.level == 0 ? static_cast<int>(i) : 0});
    bytes_read += c.inputs[i]->size;
  }
  const int next_rank = c.level == 0 ? static_cast<int>(c.inputs.size()) : 1;
  for (const FilePtr& f : c.next_inputs) {
    sources.push_back({std::make_unique<Table::Iterator>(f->table), next_rank});
    bytes_read += f->size;
  }

  using HeapItem = std::tuple<std::string, int, size_t>;  // key, rank, source index
  std::priority_queue<HeapItem, std::vector<HeapItem>, std::greater<>> heap;
  bool ok = true;
  for (size_t i = 0; i < sources.size(); i++) {
    if (sources[i].it->Valid()) heap.emplace(sources[i].it->entry().key, sources[i].rank, i);
    ok = ok && sources[i].it->status_ok();
  }

  std::vector<FilePtr> outputs;
  std::unique_ptr<TableBuilder> builder;
  uint64_t builder_number = 0;
  uint64_t bytes_written = 0;
  uint64_t dropped = 0;

  auto finish_output = [&]() -> bool {
    if (!builder) return true;
    auto f = std::make_shared<FileMeta>();
    f->number = builder_number;
    f->smallest = builder->smallest();
    f->largest = builder->largest();
    if (!builder->Finish(/*sync=*/true)) return false;
    f->size = builder->FileSize();
    bytes_written += f->size;
    f->table = Table::Open(TablePath(builder_number), builder_number, block_cache_.get());
    builder.reset();
    if (!f->table) return false;
    outputs.push_back(f);
    return true;
  };

  std::string last_key;
  bool has_last = false;
  while (ok && !heap.empty()) {
    const size_t idx = std::get<2>(heap.top());
    heap.pop();
    Table::Iterator& it = *sources[idx].it;
    const Table::Entry& e = it.entry();

    const bool shadowed = has_last && e.key == last_key;
    if (!shadowed) {
      last_key = e.key;
      has_last = true;
      if (e.type == ValueType::kDeletion && IsBaseLevelForKey(*base, out_level, e.key)) {
        dropped++;  // Nothing older survives below, so the tombstone has done its job.
      } else {
        if (!builder) {
          builder_number = next_file_number_++;
          builder = std::make_unique<TableBuilder>(TablePath(builder_number), options_.bloom_bits_per_key,
                                                   options_.block_size);
          ok = builder->Open();
        }
        if (ok) {
          builder->Add(e.key, e.type, e.value);
          if (builder->FileSize() >= options_.target_file_bytes) ok = finish_output();
        }
      }
    }

    it.Next();
    if (it.Valid()) heap.emplace(it.entry().key, sources[idx].rank, idx);
    ok = ok && it.status_ok();
  }
  ok = ok && finish_output();
  sources.clear();

  lock.lock();
  if (!ok) {
    std::fprintf(stderr, "lsm compaction failed at level %d\n", c.level);
    if (builder) builder->Abandon();
    for (const FilePtr& f : outputs) f->table->MarkObsolete();
    bg_error_ = true;
    return;
  }

  auto v = std::make_shared<Version>(*current_);
  auto remove_inputs = [](std::vector<FilePtr>& level, const std::vector<FilePtr>& inputs) {
    level.erase(std::remove_if(level.begin(), level.end(),
                               [&](const FilePtr& f) {
                                 return std::find(inputs.begin(), inputs.end(), f) != inputs.end();
                               }),
                level.end());
  };
  remove_inputs(v->levels[c.level], c.inputs);
  remove_inputs(v->levels[out_level], c.next_inputs);
  auto& to = v->levels[out_level];
  to.insert(to.end(), outputs.begin(), outputs.end());
  std::sort(to.begin(), to.end(), [](const FilePtr& a, const FilePtr& b) { return a->smallest < b->smallest; });

  if (!WriteManifest(*v, OldestLiveLog())) {
    for (const FilePtr& f : outputs) f->table->MarkObsolete();
    bg_error_ = true;
    return;
  }
  current_ = v;
  // Inputs are deleted from disk once in-flight readers drop their references.
  for (const FilePtr& f : c.inputs) f->table->MarkObsolete();
  for (const FilePtr& f : c.next_inputs) f->table->MarkObsolete();
  if (c.level > 0) compact_pointer_[c.level] = c.inputs.back()->largest;

  stats_.compactions++;
  stats_.compaction_bytes_read += bytes_read;
  stats_.compaction_bytes_written += bytes_written;
  stats_.tombstones_dropped += dropped;
}

LsmStats LsmTree::Stats() const {
  std::lock_guard lock(mu_);
  LsmStats s = stats_;
  s.table_probes = table_probes_.load();
  s.bloom_skips = bloom_skips_.load();
  if (block_cache_) {
    s.block_cache_hits = block_cache_->hits();
    s.block_cache_misses = block_cache_->misses();
  }
  for (int l = 0; l < LsmOptions::kNumLevels; l++) {
    s.files_per_level.push_back(static_cast<int>(current_->levels[l].size()));
    uint64_t bytes = 0;
    for (const FilePtr& f : current_->levels[l]) bytes += f->size;
    s.bytes_per_level.push_back(bytes);
  }
  return s;
}

}  // namespace kv
