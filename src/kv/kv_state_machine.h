// The replicated state machine: applies committed Raft commands to an LSM
// tree.
//
// Each apply is a single atomic WriteBatch containing the user mutation, the
// client's latest sequence number (for exactly-once retries), and the applied
// log index. Because all three land together, a restart resumes from exactly
// the last applied entry: nothing is lost or applied twice.
#pragma once

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include "raft/messages.h"
#include "storage/lsm_tree.h"

namespace kv {

class KvStateMachine {
 public:
  static std::unique_ptr<KvStateMachine> Open(const std::string& dir, const LsmOptions& options, std::string* error);

  // Called in log order by the Raft applier thread only.
  void Apply(const LogEntry& entry);
  // Safe to call concurrently with Apply.
  std::optional<std::string> Read(std::string_view key);

  uint64_t applied_index() const { return applied_index_; }
  LsmTree* db() { return db_.get(); }

  // User keys may not start with this byte; it prefixes internal metadata.
  static constexpr char kReservedPrefix = '\0';

 private:
  uint64_t LastSeq(uint64_t client_id);

  std::unique_ptr<LsmTree> db_;
  uint64_t applied_index_ = 0;
  std::unordered_map<uint64_t, uint64_t> last_seq_;
};

}  // namespace kv
