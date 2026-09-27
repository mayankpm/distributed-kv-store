// Linearizability checker for key/value histories.
//
// Implements the Wing & Gong search with Lowe's memoization (the algorithm
// behind Porcupine and Knossos). A history is linearizable if there is a total
// order of operations that (1) respects real time: if A returned before B was
// invoked, A comes first, and (2) is legal for a sequential key/value map.
//
// Histories are split by key first (P-compositionality: a history is
// linearizable iff each per-key sub-history is), which keeps the search small.
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "kv/kv_protocol.h"

namespace kv {

struct KvOperation {
  int client = 0;
  OpType op = OpType::kGet;
  std::string key;
  std::string input;   // Value for put/append.
  std::string output;  // Value observed by get ("" if the key was absent).
  int64_t call = 0;    // Invocation time (any monotonic clock).
  int64_t ret = 0;     // Response time.
};

struct LinearizabilityResult {
  bool ok = true;
  std::string bad_key;   // First key whose sub-history is not linearizable.
  size_t operations = 0;
  size_t keys = 0;
};

LinearizabilityResult CheckLinearizable(const std::vector<KvOperation>& history);

}  // namespace kv
