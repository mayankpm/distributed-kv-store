#include "raft/messages.h"

#include "common/coding.h"

namespace kv {

std::string Envelope::Encode() const {
  std::string out;
  out.push_back(static_cast<char>(type));
  PutFixed32(&out, group);
  PutFixed64(&out, from);
  PutFixed64(&out, to);
  PutFixed64(&out, rpc_id);
  PutLengthPrefixed(&out, payload);
  return out;
}

bool Envelope::Decode(std::string_view data, Envelope* out) {
  Reader r(data);
  out->type = static_cast<MsgType>(r.U8());
  out->group = r.Fixed32();
  out->from = r.Fixed64();
  out->to = r.Fixed64();
  out->rpc_id = r.Fixed64();
  out->payload = std::string(r.LengthPrefixed());
  return r.ok() && r.done();
}

void LogEntry::EncodeTo(std::string* out) const {
  PutFixed64(out, term);
  PutFixed64(out, index);
  out->push_back(static_cast<char>(type));
  PutLengthPrefixed(out, data);
}

bool LogEntry::DecodeFrom(Reader* r) {
  term = r->Fixed64();
  index = r->Fixed64();
  const uint8_t t = r->U8();
  data = std::string(r->LengthPrefixed());
  type = static_cast<EntryType>(t);
  return r->ok() && t <= 1;
}

std::string RequestVote::Encode() const {
  std::string out;
  PutFixed64(&out, term);
  PutFixed64(&out, candidate);
  PutFixed64(&out, last_log_index);
  PutFixed64(&out, last_log_term);
  return out;
}

bool RequestVote::Decode(std::string_view data) {
  Reader r(data);
  term = r.Fixed64();
  candidate = r.Fixed64();
  last_log_index = r.Fixed64();
  last_log_term = r.Fixed64();
  return r.ok();
}

std::string RequestVoteResp::Encode() const {
  std::string out;
  PutFixed64(&out, term);
  out.push_back(granted ? 1 : 0);
  return out;
}

bool RequestVoteResp::Decode(std::string_view data) {
  Reader r(data);
  term = r.Fixed64();
  granted = r.U8() != 0;
  return r.ok();
}

std::string AppendEntries::Encode() const {
  std::string out;
  PutFixed64(&out, term);
  PutFixed64(&out, leader);
  PutFixed64(&out, prev_log_index);
  PutFixed64(&out, prev_log_term);
  PutFixed64(&out, leader_commit);
  PutFixed64(&out, read_seq);
  PutVarint64(&out, entries.size());
  for (const LogEntry& e : entries) e.EncodeTo(&out);
  return out;
}

bool AppendEntries::Decode(std::string_view data) {
  Reader r(data);
  term = r.Fixed64();
  leader = r.Fixed64();
  prev_log_index = r.Fixed64();
  prev_log_term = r.Fixed64();
  leader_commit = r.Fixed64();
  read_seq = r.Fixed64();
  const uint64_t n = r.Varint64();
  entries.clear();
  for (uint64_t i = 0; i < n && r.ok(); i++) {
    LogEntry e;
    if (!e.DecodeFrom(&r)) return false;
    entries.push_back(std::move(e));
  }
  return r.ok();
}

std::string AppendEntriesResp::Encode() const {
  std::string out;
  PutFixed64(&out, term);
  out.push_back(success ? 1 : 0);
  PutFixed64(&out, match_index);
  PutFixed64(&out, conflict_index);
  PutFixed64(&out, conflict_term);
  PutFixed64(&out, read_seq);
  return out;
}

bool AppendEntriesResp::Decode(std::string_view data) {
  Reader r(data);
  term = r.Fixed64();
  success = r.U8() != 0;
  match_index = r.Fixed64();
  conflict_index = r.Fixed64();
  conflict_term = r.Fixed64();
  read_seq = r.Fixed64();
  return r.ok();
}

}  // namespace kv
