#include "kv/kv_state_machine.h"

#include "common/coding.h"
#include "kv/kv_protocol.h"

namespace kv {
namespace {

const std::string kAppliedKey = std::string(1, KvStateMachine::kReservedPrefix) + "applied";

std::string ClientKey(uint64_t client_id) {
  std::string k(1, KvStateMachine::kReservedPrefix);
  k += "client/";
  PutFixed64(&k, client_id);
  return k;
}

std::string EncodeU64(uint64_t v) {
  std::string s;
  PutFixed64(&s, v);
  return s;
}

uint64_t DecodeU64(const std::optional<std::string>& s) { return s && s->size() == 8 ? DecodeFixed64(s->data()) : 0; }

}  // namespace

std::unique_ptr<KvStateMachine> KvStateMachine::Open(const std::string& dir, const LsmOptions& options,
                                                     std::string* error) {
  std::unique_ptr<KvStateMachine> sm(new KvStateMachine());
  sm->db_ = LsmTree::Open(dir, options, error);
  if (!sm->db_) return nullptr;
  sm->applied_index_ = DecodeU64(sm->db_->Get(kAppliedKey));
  return sm;
}

uint64_t KvStateMachine::LastSeq(uint64_t client_id) {
  auto it = last_seq_.find(client_id);
  if (it != last_seq_.end()) return it->second;
  const uint64_t seq = DecodeU64(db_->Get(ClientKey(client_id)));
  last_seq_[client_id] = seq;
  return seq;
}

void KvStateMachine::Apply(const LogEntry& entry) {
  if (entry.index <= applied_index_) return;  // Already durable before a restart.

  WriteBatch batch;
  KvRequest cmd;
  if (entry.type == EntryType::kCommand && cmd.Decode(entry.data)) {
    // A retried request that already applied must not apply again.
    if (cmd.seq > LastSeq(cmd.client_id)) {
      switch (cmd.op) {
        case OpType::kPut:
          batch.Put(cmd.key, cmd.value);
          break;
        case OpType::kAppend:
          batch.Put(cmd.key, db_->Get(cmd.key).value_or("") + cmd.value);
          break;
        case OpType::kDelete:
          batch.Delete(cmd.key);
          break;
        case OpType::kGet:
          break;
      }
      batch.Put(ClientKey(cmd.client_id), EncodeU64(cmd.seq));
      last_seq_[cmd.client_id] = cmd.seq;
    }
  }
  batch.Put(kAppliedKey, EncodeU64(entry.index));
  db_->Write(batch);
  applied_index_ = entry.index;
}

std::optional<std::string> KvStateMachine::Read(std::string_view key) { return db_->Get(key); }

}  // namespace kv
