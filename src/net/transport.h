// Message envelope and transport interface shared by the simulated network
// (used for deterministic fault-injection tests) and the real TCP transport.
#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

namespace kv {

using NodeId = uint64_t;

enum class MsgType : uint8_t {
  kRequestVote = 1,
  kRequestVoteResp = 2,
  kAppendEntries = 3,
  kAppendEntriesResp = 4,
  kClientRequest = 10,
  kClientResponse = 11,
};

struct Envelope {
  MsgType type = MsgType::kRequestVote;
  uint32_t group = 0;   // Raft group (partition) the message belongs to.
  NodeId from = 0;
  NodeId to = 0;
  uint64_t rpc_id = 0;  // Correlates client requests with responses.
  std::string payload;

  std::string Encode() const;
  static bool Decode(std::string_view data, Envelope* out);
};

using Handler = std::function<void(const Envelope&)>;

class Transport {
 public:
  virtual ~Transport() = default;
  // Fire-and-forget. Must never block for long: Raft calls it under its lock.
  virtual void Send(Envelope env) = 0;
};

}  // namespace kv
