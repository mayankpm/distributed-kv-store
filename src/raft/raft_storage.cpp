#include "raft/raft_storage.h"

#include <filesystem>
#include <sstream>

#include "common/coding.h"
#include "common/file_util.h"

namespace kv {

bool RaftStorage::Open(const std::string& dir, bool sync, std::string* error) {
  std::error_code ec;
  std::filesystem::create_directories(dir, ec);
  state_path_ = dir + "/raft_state";
  log_path_ = dir + "/raft_log";
  sync_ = sync;

  std::string state;
  if (ReadFile(state_path_, &state)) {
    std::istringstream in(state);
    if (!(in >> term_ >> voted_for_)) {
      *error = "corrupt raft_state";
      return false;
    }
  }

  log_.assign(1, LogEntry{});
  offsets_.assign(1, 0);
  uint64_t offset = 0;
  bool bad = false;
  const uint64_t good = ReadLog(log_path_, [&](std::string_view payload) {
    Reader r(payload);
    LogEntry e;
    if (bad || !e.DecodeFrom(&r) || e.index != log_.size()) {
      bad = true;
      return;
    }
    offsets_.push_back(offset);
    offset += 8 + payload.size();
    log_.push_back(std::move(e));
  });
  if (bad) {
    *error = "raft log out of sequence";
    return false;
  }
  // Drop a torn tail left by a crash so new appends start at a clean boundary.
  if (std::filesystem::exists(log_path_, ec) && std::filesystem::file_size(log_path_, ec) > good) {
    std::filesystem::resize_file(log_path_, good, ec);
  }
  if (!writer_.Open(log_path_)) {
    *error = "cannot open " + log_path_;
    return false;
  }
  return true;
}

bool RaftStorage::SetHardState(uint64_t term, NodeId voted_for) {
  if (term == term_ && voted_for == voted_for_) return true;
  term_ = term;
  voted_for_ = voted_for;
  return WriteFileAtomic(state_path_, std::to_string(term) + " " + std::to_string(voted_for) + "\n", sync_);
}

bool RaftStorage::Append(const std::vector<LogEntry>& entries) {
  std::string payload;
  for (const LogEntry& e : entries) {
    payload.clear();
    e.EncodeTo(&payload);
    offsets_.push_back(writer_.Size());
    // Sync only the last record of a batch: one fsync covers them all.
    if (!writer_.Append(payload, sync_ && &e == &entries.back())) return false;
    log_.push_back(e);
  }
  return true;
}

bool RaftStorage::TruncateFrom(uint64_t index) {
  if (index >= log_.size()) return true;
  writer_.Close();
  std::error_code ec;
  std::filesystem::resize_file(log_path_, offsets_[index], ec);
  log_.resize(index);
  offsets_.resize(index);
  return !ec && writer_.Open(log_path_);
}

}  // namespace kv
