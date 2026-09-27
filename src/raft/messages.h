#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "net/transport.h"

namespace kv {

class Reader;

enum class EntryType : uint8_t { kNoop = 0, kCommand = 1 };

struct LogEntry {
  uint64_t term = 0;
  uint64_t index = 0;
  EntryType type = EntryType::kNoop;
  std::string data;

  void EncodeTo(std::string* out) const;
  bool DecodeFrom(Reader* r);
};

struct RequestVote {
  uint64_t term = 0;
  NodeId candidate = 0;
  uint64_t last_log_index = 0;
  uint64_t last_log_term = 0;

  std::string Encode() const;
  bool Decode(std::string_view data);
};

struct RequestVoteResp {
  uint64_t term = 0;
  bool granted = false;

  std::string Encode() const;
  bool Decode(std::string_view data);
};

struct AppendEntries {
  uint64_t term = 0;
  NodeId leader = 0;
  uint64_t prev_log_index = 0;
  uint64_t prev_log_term = 0;
  uint64_t leader_commit = 0;
  uint64_t read_seq = 0;  // Latest ReadIndex round, echoed back to confirm leadership.
  std::vector<LogEntry> entries;

  std::string Encode() const;
  bool Decode(std::string_view data);
};

struct AppendEntriesResp {
  uint64_t term = 0;
  bool success = false;
  uint64_t match_index = 0;
  // On rejection, hints that let the leader skip back a whole term at a time
  // instead of one entry per round trip.
  uint64_t conflict_index = 0;
  uint64_t conflict_term = 0;
  uint64_t read_seq = 0;

  std::string Encode() const;
  bool Decode(std::string_view data);
};

}  // namespace kv
