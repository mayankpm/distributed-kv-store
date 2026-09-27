// Durable Raft state: the hard state (current term, vote) and the log.
//
// The log is an append-only file of checksummed records. Raft only ever
// removes entries from the tail (when a new leader overwrites uncommitted
// entries), which is a file truncate at a remembered offset.
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "net/transport.h"
#include "raft/messages.h"
#include "storage/wal.h"

namespace kv {

class RaftStorage {
 public:
  bool Open(const std::string& dir, bool sync, std::string* error);

  uint64_t term() const { return term_; }
  NodeId voted_for() const { return voted_for_; }
  bool SetHardState(uint64_t term, NodeId voted_for);

  // log()[0] is a sentinel with term 0 so index i lives at log()[i].
  const std::vector<LogEntry>& log() const { return log_; }
  uint64_t LastIndex() const { return log_.size() - 1; }
  uint64_t LastTerm() const { return log_.back().term; }
  uint64_t TermAt(uint64_t index) const { return log_[index].term; }

  bool Append(const std::vector<LogEntry>& entries);
  // Drops every entry with index >= `index`.
  bool TruncateFrom(uint64_t index);

 private:
  std::string state_path_;
  std::string log_path_;
  bool sync_ = false;
  uint64_t term_ = 0;
  NodeId voted_for_ = 0;
  std::vector<LogEntry> log_;
  std::vector<uint64_t> offsets_;  // offsets_[i] = file offset where entry i starts.
  LogWriter writer_;
};

}  // namespace kv
