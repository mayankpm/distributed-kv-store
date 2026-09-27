#include "kv/kv_protocol.h"

#include "common/coding.h"

namespace kv {

const char* OpName(OpType op) {
  switch (op) {
    case OpType::kGet: return "get";
    case OpType::kPut: return "put";
    case OpType::kAppend: return "append";
    case OpType::kDelete: return "delete";
  }
  return "?";
}

const char* StatusName(KvStatus s) {
  switch (s) {
    case KvStatus::kOk: return "ok";
    case KvStatus::kNotFound: return "not_found";
    case KvStatus::kWrongLeader: return "wrong_leader";
    case KvStatus::kWrongPartition: return "wrong_partition";
    case KvStatus::kTimeout: return "timeout";
    case KvStatus::kInvalid: return "invalid";
  }
  return "?";
}

std::string KvRequest::Encode() const {
  std::string out;
  PutFixed32(&out, partition);
  out.push_back(static_cast<char>(op));
  PutLengthPrefixed(&out, key);
  PutLengthPrefixed(&out, value);
  PutFixed64(&out, client_id);
  PutFixed64(&out, seq);
  return out;
}

bool KvRequest::Decode(std::string_view data) {
  Reader r(data);
  partition = r.Fixed32();
  const uint8_t o = r.U8();
  key = std::string(r.LengthPrefixed());
  value = std::string(r.LengthPrefixed());
  client_id = r.Fixed64();
  seq = r.Fixed64();
  op = static_cast<OpType>(o);
  return r.ok() && o >= 1 && o <= 4;
}

std::string KvResponse::Encode() const {
  std::string out;
  out.push_back(static_cast<char>(status));
  PutFixed64(&out, leader_hint);
  PutLengthPrefixed(&out, value);
  return out;
}

bool KvResponse::Decode(std::string_view data) {
  Reader r(data);
  status = static_cast<KvStatus>(r.U8());
  leader_hint = r.Fixed64();
  value = std::string(r.LengthPrefixed());
  return r.ok();
}

}  // namespace kv
