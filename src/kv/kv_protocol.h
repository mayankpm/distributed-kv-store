#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "net/transport.h"

namespace kv {

enum class OpType : uint8_t { kGet = 1, kPut = 2, kAppend = 3, kDelete = 4 };

enum class KvStatus : uint8_t {
  kOk = 0,
  kNotFound = 1,
  kWrongLeader = 2,     // Retry at `leader_hint` (if set) or another replica.
  kWrongPartition = 3,  // This node does not replicate the key's partition.
  kTimeout = 4,         // Outcome unknown; safe to retry (writes are deduplicated).
  kInvalid = 5,
};

const char* OpName(OpType op);
const char* StatusName(KvStatus s);

// Client request. Also the command format stored in the Raft log: the
// (client_id, seq) pair makes retried writes apply exactly once.
struct KvRequest {
  uint32_t partition = 0;
  OpType op = OpType::kGet;
  std::string key;
  std::string value;
  uint64_t client_id = 0;
  uint64_t seq = 0;

  std::string Encode() const;
  bool Decode(std::string_view data);
};

struct KvResponse {
  KvStatus status = KvStatus::kOk;
  NodeId leader_hint = 0;
  std::string value;

  std::string Encode() const;
  bool Decode(std::string_view data);
};

}  // namespace kv
