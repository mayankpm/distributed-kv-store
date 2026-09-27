// Raft tests on the simulated network, modeled on the MIT 6.824 Raft suites:
// election, re-election under partitions, agreement with failures, log
// backup, crash recovery, and a randomized "Figure 8" test on an unreliable
// network. Every applied entry is cross-checked against all other replicas
// (state machine safety) and apply order is checked to be gapless.
#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <set>

#include "net/sim_network.h"
#include "raft/raft.h"
#include "test.h"

using namespace kv;
using Clock = std::chrono::steady_clock;

namespace {

class RaftCluster {
 public:
  explicit RaftCluster(int n, bool reliable = true) : n_(n), net_(std::make_unique<SimNetwork>(n * 1000 + 7)) {
    net_->SetReliable(reliable);
    for (int i = 0; i < n; i++) members_.push_back(static_cast<NodeId>(i + 1));
    rafts_.resize(n);
    applied_.resize(n);
    for (int i = 0; i < n; i++) Start(i);
  }

  ~RaftCluster() {
    for (int i = 0; i < n_; i++) Crash(i);
  }

  void Start(int i) {
    RaftConfig c;
    c.id = members_[i];
    c.members = members_;
    c.dir = dir_.sub("n" + std::to_string(i));
    std::string err;
    {
      std::lock_guard lock(mu_);
      applied_[i].clear();
    }
    auto transport = net_->TransportFor(c.id);
    auto r = Raft::Create(
        c, [transport](Envelope e) { transport->Send(std::move(e)); },
        [this, i](const LogEntry& e) { OnApply(i, e); }, &err);
    if (!r) throw kvtest::Failure("raft create failed: " + err);
    Raft* raw = r.get();
    rafts_[i] = std::move(r);
    net_->Register(members_[i], [raw](const Envelope& env) { raw->Step(env); });
    rafts_[i]->Start();
  }

  void Crash(int i) {
    if (!rafts_[i]) return;
    net_->Unregister(members_[i]);
    rafts_[i]->Stop();
    rafts_[i].reset();
  }

  void Restart(int i) {
    Crash(i);
    Start(i);
  }

  void Disconnect(int i) { net_->SetConnected(members_[i], false); }
  void Connect(int i) { net_->SetConnected(members_[i], true); }
  void SetReliable(bool r) { net_->SetReliable(r); }
  Raft* raft(int i) { return rafts_[i].get(); }
  bool Up(int i) const { return rafts_[i] != nullptr; }

  // Waits for exactly one leader among nodes that are up and connected.
  int CheckOneLeader(const std::set<int>& connected) {
    for (int attempt = 0; attempt < 20; attempt++) {
      kvtest::SleepMs(150 + attempt * 20);
      std::map<uint64_t, std::vector<int>> leaders;
      for (int i : connected) {
        if (!rafts_[i]) continue;
        auto s = rafts_[i]->GetStatus();
        if (s.role == Role::kLeader) leaders[s.term].push_back(i);
      }
      for (auto& [term, ls] : leaders) {
        if (ls.size() > 1) throw kvtest::Failure("term " + std::to_string(term) + " has multiple leaders");
      }
      if (!leaders.empty()) return leaders.rbegin()->second[0];
    }
    throw kvtest::Failure("expected one leader, got none");
  }

  void CheckNoLeader(const std::set<int>& connected) {
    for (int i : connected) {
      if (rafts_[i] && rafts_[i]->GetStatus().role == Role::kLeader) {
        throw kvtest::Failure("expected no leader, node " + std::to_string(i) + " is leader");
      }
    }
  }

  uint64_t CheckTerms() {
    uint64_t term = 0;
    for (int i = 0; i < n_; i++) {
      if (!rafts_[i]) continue;
      const uint64_t t = rafts_[i]->GetStatus().term;
      if (term == 0) term = t;
      else if (t != term) throw kvtest::Failure("servers disagree on term");
    }
    return term;
  }

  // Number of servers that have applied `index`, verifying they agree on it.
  int NCommitted(uint64_t index, std::string* value = nullptr) {
    std::lock_guard lock(mu_);
    if (!apply_error_.empty()) throw kvtest::Failure(apply_error_);
    int count = 0;
    std::string seen;
    for (int i = 0; i < n_; i++) {
      auto it = applied_[i].find(index);
      if (it == applied_[i].end()) continue;
      if (count > 0 && it->second != seen) throw kvtest::Failure("committed values differ at index " + std::to_string(index));
      seen = it->second;
      count++;
    }
    if (value) *value = seen;
    return count;
  }

  // Submits `cmd` via whichever node claims leadership and waits until at
  // least `expected` servers apply it. Retries across leader changes.
  uint64_t One(const std::string& cmd, int expected, bool retry, int timeout_ms = 10000) {
    const auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
    int start = 0;
    while (Clock::now() < deadline) {
      uint64_t index = 0;
      for (int k = 0; k < n_; k++) {
        const int i = (start + k) % n_;
        if (!rafts_[i]) continue;
        auto p = rafts_[i]->Propose(cmd);
        if (p.is_leader) {
          index = p.index;
          start = i;
          break;
        }
      }
      if (index != 0) {
        const auto t1 = Clock::now() + std::chrono::seconds(2);
        while (Clock::now() < t1) {
          std::string v;
          if (NCommitted(index, &v) >= expected && v == cmd) return index;
          kvtest::SleepMs(20);
        }
        if (!retry) throw kvtest::Failure("One(" + cmd + ") failed to reach agreement");
      } else {
        kvtest::SleepMs(50);
      }
      start++;
    }
    throw kvtest::Failure("One(" + cmd + ") failed to reach agreement before deadline");
  }

  std::set<int> All() const {
    std::set<int> s;
    for (int i = 0; i < n_; i++) s.insert(i);
    return s;
  }

 private:
  void OnApply(int i, const LogEntry& e) {
    std::lock_guard lock(mu_);
    const uint64_t prev = applied_[i].empty() ? 0 : applied_[i].rbegin()->first;
    if (e.index != prev + 1) apply_error_ = "node " + std::to_string(i) + " applied out of order";
    // Record no-ops too (as empty) so indexes stay contiguous.
    const std::string value = e.type == EntryType::kCommand ? e.data : std::string();
    for (int j = 0; j < n_; j++) {
      auto it = applied_[j].find(e.index);
      if (it != applied_[j].end() && it->second != value) {
        apply_error_ = "state machine safety violated at index " + std::to_string(e.index);
      }
    }
    applied_[i][e.index] = value;
  }

  int n_;
  kvtest::TempDir dir_;
  std::unique_ptr<SimNetwork> net_;
  std::vector<NodeId> members_;
  std::vector<std::unique_ptr<Raft>> rafts_;
  std::mutex mu_;
  std::vector<std::map<uint64_t, std::string>> applied_;
  std::string apply_error_;
};

}  // namespace

TEST(raft, initial_election) {
  RaftCluster c(3);
  c.CheckOneLeader(c.All());
  kvtest::SleepMs(50);
  const uint64_t term1 = c.CheckTerms();
  CHECK(term1 >= 1);
  // With no failures, heartbeats should keep the same leader and term.
  kvtest::SleepMs(1000);
  CHECK_EQ(c.CheckTerms(), term1);
  c.CheckOneLeader(c.All());
}

TEST(raft, reelection_after_failures) {
  RaftCluster c(3);
  int leader1 = c.CheckOneLeader(c.All());

  // Leader loses connectivity: a new one must emerge from the other two.
  c.Disconnect(leader1);
  std::set<int> rest = c.All();
  rest.erase(leader1);
  int leader2 = c.CheckOneLeader(rest);

  // Old leader rejoining must not disturb the new leader's authority.
  c.Connect(leader1);
  c.CheckOneLeader(c.All());

  // No quorum: no leader may be elected.
  c.Disconnect(leader2);
  c.Disconnect((leader2 + 1) % 3);
  kvtest::SleepMs(1000);
  c.CheckNoLeader({(leader2 + 2) % 3});

  // Quorum restored.
  c.Connect((leader2 + 1) % 3);
  c.CheckOneLeader({(leader2 + 1) % 3, (leader2 + 2) % 3});
  c.Connect(leader2);
  c.CheckOneLeader(c.All());
}

TEST(raft, many_elections_with_random_disconnects) {
  RaftCluster c(7);
  c.CheckOneLeader(c.All());
  std::mt19937 rng(7);
  for (int iter = 0; iter < 8; iter++) {
    std::set<int> down;
    while (down.size() < 3) down.insert(static_cast<int>(rng() % 7));
    for (int i : down) c.Disconnect(i);
    std::set<int> up = c.All();
    for (int i : down) up.erase(i);
    c.CheckOneLeader(up);  // 4 of 7 is still a majority.
    for (int i : down) c.Connect(i);
  }
  c.CheckOneLeader(c.All());
}

TEST(raft, basic_agreement) {
  RaftCluster c(3);
  for (int i = 1; i <= 5; i++) {
    const uint64_t index = c.One("cmd" + std::to_string(i), 3, false);
    CHECK(index > 0);
  }
}

TEST(raft, agreement_despite_follower_disconnect) {
  RaftCluster c(3);
  c.One("101", 3, false);
  const int leader = c.CheckOneLeader(c.All());
  const int follower = (leader + 1) % 3;
  c.Disconnect(follower);

  c.One("102", 2, false);
  c.One("103", 2, false);
  kvtest::SleepMs(300);
  c.One("104", 2, false);
  c.One("105", 2, false);

  // Reconnected follower catches up on everything it missed.
  c.Connect(follower);
  c.One("106", 3, true);
  kvtest::SleepMs(300);
  c.One("107", 3, true);
}

TEST(raft, no_agreement_without_quorum) {
  RaftCluster c(5);
  c.One("10", 5, false);
  const int leader = c.CheckOneLeader(c.All());
  for (int k = 1; k <= 3; k++) c.Disconnect((leader + k) % 5);

  auto p = c.raft(leader)->Propose("20");
  CHECK(p.is_leader);
  kvtest::SleepMs(2000);
  CHECK_EQ(c.NCommitted(p.index), 0);

  for (int k = 1; k <= 3; k++) c.Connect((leader + k) % 5);
  // The uncommitted entry may be kept or overwritten, but the cluster must
  // make progress again.
  const int leader2 = c.CheckOneLeader(c.All());
  auto p2 = c.raft(leader2)->Propose("30");
  if (p2.is_leader) CHECK(p2.index >= 2);
  c.One("1000", 5, true);
}

TEST(raft, rejoin_of_partitioned_leader) {
  RaftCluster c(3);
  c.One("101", 3, true);

  // Leader 1 is partitioned and accepts entries that can never commit.
  const int leader1 = c.CheckOneLeader(c.All());
  c.Disconnect(leader1);
  c.raft(leader1)->Propose("102");
  c.raft(leader1)->Propose("103");
  c.raft(leader1)->Propose("104");

  // The others elect leader 2 and commit.
  c.One("103", 2, true);

  // Leader 2 is partitioned; leader 1 returns and must have its stale
  // entries overwritten.
  const int leader2 = c.CheckOneLeader([&] {
    auto s = c.All();
    s.erase(leader1);
    return s;
  }());
  c.Disconnect(leader2);
  c.Connect(leader1);
  c.One("104", 2, true);

  c.Connect(leader2);
  c.One("105", 3, true);
}

TEST(raft, leader_backs_up_quickly_over_divergent_logs) {
  RaftCluster c(5);
  c.One("start", 5, true);

  // Put leader and one follower in a partition; they take 50 doomed entries.
  const int leader1 = c.CheckOneLeader(c.All());
  c.Disconnect((leader1 + 2) % 5);
  c.Disconnect((leader1 + 3) % 5);
  c.Disconnect((leader1 + 4) % 5);
  for (int i = 0; i < 50; i++) c.raft(leader1)->Propose("doomed" + std::to_string(i));
  kvtest::SleepMs(500);
  c.Disconnect(leader1);
  c.Disconnect((leader1 + 1) % 5);

  // The other three commit 50 entries of their own.
  c.Connect((leader1 + 2) % 5);
  c.Connect((leader1 + 3) % 5);
  c.Connect((leader1 + 4) % 5);
  for (int i = 0; i < 50; i++) c.One("good" + std::to_string(i), 3, true);

  // Everyone back: the doomed entries must be replaced, and quickly.
  for (int i = 0; i < 5; i++) c.Connect(i);
  c.One("final", 5, true);
}

TEST(raft, state_persists_across_crashes) {
  RaftCluster c(3);
  c.One("11", 3, true);
  for (int i = 0; i < 3; i++) c.Restart(i);
  c.One("12", 3, true);

  const int leader1 = c.CheckOneLeader(c.All());
  c.Restart(leader1);
  c.One("13", 3, true);

  const int leader2 = c.CheckOneLeader(c.All());
  c.Crash(leader2);
  c.One("14", 2, true);
  c.Start(leader2);

  // The restarted node replays its durable log and re-applies everything.
  c.One("15", 3, true);
  std::string v;
  CHECK_EQ(c.NCommitted(1, &v), 3);
}

TEST(raft, leader_steps_down_without_quorum) {
  RaftCluster c(5);
  const int leader = c.CheckOneLeader(c.All());
  c.Disconnect(leader);
  // Check-quorum: within a couple of election timeouts the isolated leader
  // must stop believing it leads.
  kvtest::SleepMs(800);
  CHECK(c.raft(leader)->GetStatus().role != Role::kLeader);
}

TEST(raft, read_index_requires_quorum) {
  RaftCluster c(3);
  c.One("x", 3, true);
  const int leader = c.CheckOneLeader(c.All());

  std::atomic<int> result{-1};
  c.raft(leader)->ReadIndex([&](bool ok, uint64_t) { result = ok ? 1 : 0; });
  for (int i = 0; i < 50 && result == -1; i++) kvtest::SleepMs(10);
  CHECK_EQ(result.load(), 1);

  // A leader cut off from the majority must never confirm a read.
  c.Disconnect(leader);
  std::atomic<int> result2{-1};
  c.raft(leader)->ReadIndex([&](bool ok, uint64_t) { result2 = ok ? 1 : 0; });
  kvtest::SleepMs(1000);
  CHECK(result2.load() != 1);
}

TEST(raft, figure8_unreliable_network) {
  // Random leader crashes and partitions on a lossy, reordering network, while
  // clients keep proposing. Safety is checked on every apply; liveness at the end.
  RaftCluster c(5, /*reliable=*/false);
  std::mt19937 rng(8);
  c.One("seed", 1, true);

  int up = 5;
  for (int iter = 0; iter < 60; iter++) {
    if (iter == 50) c.SetReliable(true);
    int leader = -1;
    for (int i = 0; i < 5; i++) {
      if (c.Up(i) && c.raft(i)->Propose("r" + std::to_string(rng() % 10000)).is_leader) leader = i;
    }
    kvtest::SleepMs(rng() % 10 == 0 ? static_cast<int>(rng() % 500) : static_cast<int>(rng() % 20));
    if (leader != -1 && rng() % 2 == 0) {
      c.Crash(leader);
      up--;
    }
    if (up < 3) {
      const int s = static_cast<int>(rng() % 5);
      if (!c.Up(s)) {
        c.Start(s);
        up++;
      }
    }
  }
  for (int i = 0; i < 5; i++) {
    if (!c.Up(i)) c.Start(i);
  }
  c.SetReliable(true);
  c.One("final", 5, true, 20000);
}
