// TCP transport for real deployments.
//
// Wire format: each message is a frame of fixed32 length + Envelope bytes.
//
// Every connection has a reader thread (decodes frames, calls the handler)
// and a writer thread draining an outbound queue, so Send() never blocks on
// the network (Raft calls it while holding its lock). Each known peer has a
// dialer thread that (re)connects with exponential backoff and buffers a
// bounded backlog while disconnected.
//
// Peers are the other servers. Anyone else who connects in (a client) is
// answered on the connection it came from.
#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "net/socket.h"
#include "net/transport.h"

namespace kv {

class TcpTransport : public Transport {
 public:
  TcpTransport(NodeId self, Handler handler);
  ~TcpTransport() override;

  // Starts accepting inbound connections. "host:port"; port 0 picks one.
  bool Listen(const std::string& addr, std::string* error);
  uint16_t listen_port() const { return port_; }
  // Registers a server we send to by id (starts its dialer).
  void AddPeer(NodeId id, const std::string& addr);

  void Send(Envelope env) override;
  void Stop();

 private:
  struct Conn {
    SocketHandle fd = kInvalidSocket;
    std::mutex mu;
    std::condition_variable cv;
    std::deque<std::string> out;
    std::atomic<bool> closed{false};
    std::atomic<int> exited{0};
    std::thread reader;
    std::thread writer;
  };
  struct Peer {
    NodeId id = 0;
    std::string host;
    uint16_t port = 0;
    std::shared_ptr<Conn> conn;
    std::deque<std::string> backlog;
    std::thread dialer;
  };

  std::shared_ptr<Conn> StartConnLocked(SocketHandle fd);
  void ReaderLoop(std::shared_ptr<Conn> conn);
  void WriterLoop(std::shared_ptr<Conn> conn);
  void DialLoop(Peer* peer);
  void AcceptLoop();
  static void Enqueue(Conn& conn, std::string frame);
  static void Close(Conn& conn);
  void ReapLocked();

  const NodeId self_;
  Handler handler_;

  std::mutex mu_;
  std::condition_variable cv_;
  bool stopping_ = false;
  std::map<NodeId, std::unique_ptr<Peer>> peers_;
  std::map<NodeId, std::weak_ptr<Conn>> inbound_;  // Clients that connected to us.
  std::vector<std::shared_ptr<Conn>> conns_;
  SocketHandle listener_ = kInvalidSocket;
  uint16_t port_ = 0;
  std::thread acceptor_;
};

}  // namespace kv
