// Sanity tests for the checker itself: it must accept legal concurrent
// histories and, just as importantly, reject illegal ones.
#include "check/linearizability.h"
#include "test.h"

using namespace kv;

namespace {

KvOperation Op(int client, OpType type, const std::string& in, const std::string& out, int64_t call, int64_t ret) {
  KvOperation op;
  op.client = client;
  op.op = type;
  op.key = "x";
  op.input = in;
  op.output = out;
  op.call = call;
  op.ret = ret;
  return op;
}

}  // namespace

TEST(porcupine, accepts_sequential_history) {
  std::vector<KvOperation> h = {
      Op(0, OpType::kPut, "a", "", 0, 10),
      Op(0, OpType::kGet, "", "a", 20, 30),
      Op(0, OpType::kAppend, "b", "", 40, 50),
      Op(0, OpType::kGet, "", "ab", 60, 70),
      Op(0, OpType::kDelete, "", "", 80, 90),
      Op(0, OpType::kGet, "", "", 100, 110),
  };
  CHECK(CheckLinearizable(h).ok);
}

TEST(porcupine, accepts_overlapping_operations_in_either_order) {
  // The get overlaps the put, so it may observe either the old or new value.
  std::vector<KvOperation> h1 = {Op(0, OpType::kPut, "1", "", 0, 100), Op(1, OpType::kGet, "", "", 10, 20)};
  std::vector<KvOperation> h2 = {Op(0, OpType::kPut, "1", "", 0, 100), Op(1, OpType::kGet, "", "1", 10, 20)};
  CHECK(CheckLinearizable(h1).ok);
  CHECK(CheckLinearizable(h2).ok);
}

TEST(porcupine, rejects_stale_read) {
  // The put completed before the get started, yet the get saw the old value.
  std::vector<KvOperation> h = {Op(0, OpType::kPut, "1", "", 0, 10), Op(1, OpType::kGet, "", "", 20, 30)};
  auto r = CheckLinearizable(h);
  CHECK(!r.ok);
  CHECK_EQ(r.bad_key, std::string("x"));
}

TEST(porcupine, rejects_lost_append) {
  // Two completed appends, but a later read only reflects one of them.
  std::vector<KvOperation> h = {
      Op(0, OpType::kAppend, "a", "", 0, 10),
      Op(1, OpType::kAppend, "b", "", 5, 15),
      Op(2, OpType::kGet, "", "b", 20, 30),
  };
  CHECK(!CheckLinearizable(h).ok);
}

TEST(porcupine, rejects_reads_that_go_back_in_time) {
  // Once a reader has seen "2", a later read must not see "1" again.
  std::vector<KvOperation> h = {
      Op(0, OpType::kPut, "1", "", 0, 10),
      Op(0, OpType::kPut, "2", "", 20, 100),
      Op(1, OpType::kGet, "", "2", 30, 40),
      Op(2, OpType::kGet, "", "1", 50, 60),
  };
  CHECK(!CheckLinearizable(h).ok);
}
