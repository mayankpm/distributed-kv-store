// Integration test over real TCP sockets on loopback: the same nodes and
// client as production, only the transport differs from the simulated tests.
#include "kv/local_cluster.h"
#include "test.h"

using namespace kv;

TEST(tcp, cluster_serves_requests_and_survives_node_restart) {
  kvtest::TempDir dir;
  std::string err;
  auto cluster = LocalCluster::Start(3, 3, 3, dir.path(), NodeOptions{}, &err);
  if (!cluster) throw kvtest::Failure("cluster start failed: " + err);
  auto client = cluster->NewClient();

  for (int i = 0; i < 100; i++) CHECK((*client)->Put("k" + std::to_string(i), "v" + std::to_string(i)));
  for (int i = 0; i < 100; i++) CHECK_EQ(*(*client)->Get("k" + std::to_string(i)), "v" + std::to_string(i));

  // One of three replicas down: every partition still has a majority.
  cluster->StopNode(0);
  for (int i = 0; i < 100; i++) CHECK((*client)->Append("k" + std::to_string(i), "!"));

  // The restarted node recovers from disk and catches up from the leaders.
  CHECK(cluster->RestartNode(0, &err));
  cluster->StopNode(1);
  for (int i = 0; i < 100; i++) CHECK_EQ(*(*client)->Get("k" + std::to_string(i)), "v" + std::to_string(i) + "!");
}
