#include "check/linearizability.h"

#include <algorithm>
#include <map>
#include <tuple>
#include <unordered_set>

namespace kv {
namespace {

// Sequential specification of a single register-like key.
bool Step(const KvOperation& op, const std::string& state, std::string* next) {
  switch (op.op) {
    case OpType::kGet:
      if (op.output != state) return false;
      *next = state;
      return true;
    case OpType::kPut:
      *next = op.input;
      return true;
    case OpType::kAppend:
      *next = state + op.input;
      return true;
    case OpType::kDelete:
      next->clear();
      return true;
  }
  return false;
}

// Doubly linked list of call/return events in time order. Linearizing an
// operation "lifts" its call and return out of the list; backtracking
// "unlifts" them back into place.
struct Node {
  int op = -1;
  bool is_call = false;
  Node* match = nullptr;  // For a call: its return event.
  Node* prev = nullptr;
  Node* next = nullptr;
};

void Lift(Node* call) {
  call->prev->next = call->next;
  if (call->next) call->next->prev = call->prev;
  Node* ret = call->match;
  ret->prev->next = ret->next;
  if (ret->next) ret->next->prev = ret->prev;
}

void Unlift(Node* call) {
  Node* ret = call->match;
  ret->prev->next = ret;
  if (ret->next) ret->next->prev = ret;
  call->prev->next = call;
  if (call->next) call->next->prev = call;
}

bool CheckKey(const std::vector<const KvOperation*>& ops) {
  const size_t n = ops.size();
  std::vector<std::tuple<int64_t, int, int>> events;  // time, is_return, op
  events.reserve(2 * n);
  for (size_t i = 0; i < n; i++) {
    events.emplace_back(ops[i]->call, 0, static_cast<int>(i));
    events.emplace_back(ops[i]->ret, 1, static_cast<int>(i));
  }
  // At equal timestamps calls sort first, which treats the operations as
  // concurrent (the permissive reading).
  std::sort(events.begin(), events.end());

  std::vector<Node> nodes(2 * n + 1);
  Node* head = &nodes[0];
  std::vector<Node*> call_of(n);
  Node* tail = head;
  for (size_t k = 0; k < events.size(); k++) {
    Node* node = &nodes[k + 1];
    node->op = std::get<2>(events[k]);
    node->is_call = std::get<1>(events[k]) == 0;
    if (node->is_call) {
      call_of[node->op] = node;
    } else {
      call_of[node->op]->match = node;
    }
    node->prev = tail;
    tail->next = node;
    tail = node;
  }

  std::vector<uint64_t> linearized((n + 63) / 64, 0);
  auto cache_key = [&](const std::vector<uint64_t>& bits, const std::string& state) {
    std::string key(reinterpret_cast<const char*>(bits.data()), bits.size() * sizeof(uint64_t));
    key.push_back('\x01');
    key += state;
    return key;
  };
  std::unordered_set<std::string> cache;

  struct Frame {
    Node* call;
    std::string state;
  };
  std::vector<Frame> stack;
  std::string state;
  Node* entry = head->next;

  while (head->next != nullptr) {
    if (entry->is_call) {
      std::string next_state;
      if (Step(*ops[entry->op], state, &next_state)) {
        std::vector<uint64_t> bits = linearized;
        bits[entry->op / 64] |= 1ULL << (entry->op % 64);
        // Memoize (set of linearized ops, state): reaching the same
        // configuration twice cannot lead anywhere new.
        if (cache.insert(cache_key(bits, next_state)).second) {
          stack.push_back({entry, state});
          state = std::move(next_state);
          linearized = std::move(bits);
          Lift(entry);
          entry = head->next;
          continue;
        }
      }
      entry = entry->next;
    } else {
      // Reached a return whose call could not be linearized yet: backtrack.
      if (stack.empty()) return false;
      Frame f = std::move(stack.back());
      stack.pop_back();
      state = std::move(f.state);
      linearized[f.call->op / 64] &= ~(1ULL << (f.call->op % 64));
      Unlift(f.call);
      entry = f.call->next;
    }
  }
  return true;
}

}  // namespace

LinearizabilityResult CheckLinearizable(const std::vector<KvOperation>& history) {
  std::map<std::string, std::vector<const KvOperation*>> by_key;
  for (const KvOperation& op : history) by_key[op.key].push_back(&op);

  LinearizabilityResult result;
  result.operations = history.size();
  result.keys = by_key.size();
  for (const auto& [key, ops] : by_key) {
    if (!CheckKey(ops)) {
      result.ok = false;
      result.bad_key = key;
      return result;
    }
  }
  return result;
}

}  // namespace kv
