// kvcli: command-line client.
//
//   kvcli --config cluster.conf get KEY
//   kvcli --config cluster.conf put KEY VALUE
//   kvcli --config cluster.conf append KEY VALUE
//   kvcli --config cluster.conf del KEY
//   kvcli --config cluster.conf            (interactive: one command per line)
#include <atomic>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "common/platform.h"
#include "kv/kv_client.h"
#include "net/tcp_transport.h"

using namespace kv;

namespace {

// Returns false for an unknown command.
bool RunCommand(KvClient& client, const std::vector<std::string>& args) {
  if (args.empty()) return true;
  const std::string& cmd = args[0];
  if (cmd == "get" && args.size() == 2) {
    bool ok = false;
    auto v = client.Get(args[1], &ok);
    if (!ok) std::printf("(error: no response from the partition's replicas)\n");
    else if (!v) std::printf("(not found)\n");
    else std::printf("%s\n", v->c_str());
  } else if ((cmd == "put" || cmd == "append") && args.size() >= 3) {
    std::string value = args[2];
    for (size_t i = 3; i < args.size(); i++) value += " " + args[i];
    const bool ok = cmd == "put" ? client.Put(args[1], value) : client.Append(args[1], value);
    std::printf(ok ? "OK\n" : "(error: write not confirmed; it may or may not have applied)\n");
  } else if ((cmd == "del" || cmd == "delete") && args.size() == 2) {
    std::printf(client.Delete(args[1]) ? "OK\n" : "(error: delete not confirmed)\n");
  } else {
    return false;
  }
  return true;
}

void Usage() {
  std::fprintf(stderr,
               "usage: kvcli --config FILE [get KEY | put KEY VALUE | append KEY VALUE | del KEY]\n"
               "       with no command, reads commands from stdin\n");
}

}  // namespace

int main(int argc, char** argv) {
  kv::EnableHighResolutionTimers();
  std::string config_path;
  std::vector<std::string> command;
  for (int i = 1; i < argc; i++) {
    const std::string a = argv[i];
    if (a == "--config" && i + 1 < argc) {
      config_path = argv[++i];
    } else {
      command.push_back(a);
    }
  }
  if (config_path.empty()) {
    Usage();
    return 2;
  }
  ClusterConfig cluster;
  std::string err;
  if (!ClusterConfig::Load(config_path, &cluster, &err)) {
    std::fprintf(stderr, "kvcli: %s\n", err.c_str());
    return 1;
  }

  // Client ids live in the upper half of the id space, away from server ids.
  std::random_device rd;
  const NodeId client_id = (static_cast<NodeId>(rd()) << 32 | rd()) | (1ULL << 63);
  std::atomic<KvClient*> client_ptr{nullptr};
  auto transport = std::make_shared<TcpTransport>(client_id, [&client_ptr](const Envelope& env) {
    if (KvClient* c = client_ptr.load()) c->OnMessage(env);
  });
  for (const auto& [id, addr] : cluster.nodes) transport->AddPeer(id, addr);
  KvClient client(cluster, client_id, transport, ClientOptions{500, 5000});
  client_ptr = &client;

  int rc = 0;
  if (!command.empty()) {
    if (!RunCommand(client, command)) {
      Usage();
      rc = 2;
    }
  } else {
    std::string line;
    std::printf("> ");
    std::fflush(stdout);
    while (std::getline(std::cin, line)) {
      std::istringstream in(line);
      std::vector<std::string> args;
      for (std::string w; in >> w;) args.push_back(w);
      if (!args.empty() && (args[0] == "quit" || args[0] == "exit")) break;
      if (!RunCommand(client, args)) std::printf("commands: get K | put K V | append K V | del K | quit\n");
      std::printf("> ");
      std::fflush(stdout);
    }
  }
  client_ptr = nullptr;
  transport->Stop();
  return rc;
}
