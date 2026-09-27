#include "net/tcp_transport.h"

#include <algorithm>

#include "common/coding.h"

namespace kv {
namespace {

constexpr size_t kMaxFrame = 64u << 20;
constexpr size_t kMaxBacklog = 4096;

}  // namespace

TcpTransport::TcpTransport(NodeId self, Handler handler) : self_(self), handler_(std::move(handler)) { NetInit(); }

TcpTransport::~TcpTransport() { Stop(); }

bool TcpTransport::Listen(const std::string& addr, std::string* error) {
  std::string host;
  uint16_t port;
  if (!ParseHostPort(addr, &host, &port)) {
    *error = "bad address " + addr;
    return false;
  }
  listener_ = ListenTcp(host, port, &port_, error);
  if (listener_ == kInvalidSocket) return false;
  acceptor_ = std::thread([this] { AcceptLoop(); });
  return true;
}

void TcpTransport::AddPeer(NodeId id, const std::string& addr) {
  auto peer = std::make_unique<Peer>();
  peer->id = id;
  if (!ParseHostPort(addr, &peer->host, &peer->port)) return;
  Peer* raw = peer.get();
  std::lock_guard lock(mu_);
  if (stopping_ || peers_.count(id)) return;
  peers_[id] = std::move(peer);
  raw->dialer = std::thread([this, raw] { DialLoop(raw); });
}

void TcpTransport::Enqueue(Conn& conn, std::string frame) {
  {
    std::lock_guard lock(conn.mu);
    conn.out.push_back(std::move(frame));
  }
  conn.cv.notify_one();
}

void TcpTransport::Close(Conn& conn) {
  if (!conn.closed.exchange(true)) ShutdownSocket(conn.fd);
  conn.cv.notify_all();
}

void TcpTransport::Send(Envelope env) {
  env.from = self_;
  const std::string body = env.Encode();
  std::string frame;
  PutFixed32(&frame, static_cast<uint32_t>(body.size()));
  frame += body;

  std::lock_guard lock(mu_);
  if (stopping_) return;
  if (auto it = peers_.find(env.to); it != peers_.end()) {
    Peer& p = *it->second;
    if (p.conn && !p.conn->closed) {
      Enqueue(*p.conn, std::move(frame));
    } else {
      // Not connected yet: hold a bounded backlog for the dialer to flush.
      if (p.backlog.size() >= kMaxBacklog) p.backlog.pop_front();
      p.backlog.push_back(std::move(frame));
      cv_.notify_all();
    }
    return;
  }
  if (auto it = inbound_.find(env.to); it != inbound_.end()) {
    if (auto conn = it->second.lock(); conn && !conn->closed) Enqueue(*conn, std::move(frame));
  }
  // Unknown destination: drop. Raft and clients both retry.
}

std::shared_ptr<TcpTransport::Conn> TcpTransport::StartConnLocked(SocketHandle fd) {
  auto conn = std::make_shared<Conn>();
  conn->fd = fd;
  conn->reader = std::thread([this, conn] { ReaderLoop(conn); });
  conn->writer = std::thread([this, conn] { WriterLoop(conn); });
  ReapLocked();
  conns_.push_back(conn);
  return conn;
}

void TcpTransport::ReapLocked() {
  // Join threads of connections that have fully shut down.
  auto done = std::partition(conns_.begin(), conns_.end(), [](const auto& c) { return c->exited < 2; });
  for (auto it = done; it != conns_.end(); ++it) {
    (*it)->reader.join();
    (*it)->writer.join();
    CloseSocket((*it)->fd);
  }
  conns_.erase(done, conns_.end());
}

void TcpTransport::ReaderLoop(std::shared_ptr<Conn> conn) {
  std::string buf;
  while (!conn->closed) {
    char len_buf[4];
    if (!RecvAll(conn->fd, len_buf, 4)) break;
    const uint32_t len = DecodeFixed32(len_buf);
    if (len > kMaxFrame) break;
    buf.resize(len);
    if (!RecvAll(conn->fd, buf.data(), len)) break;
    Envelope env;
    if (!Envelope::Decode(buf, &env)) break;
    {
      std::lock_guard lock(mu_);
      if (stopping_) break;
      // Remember how to reach senders we cannot dial (clients).
      if (!peers_.count(env.from)) inbound_[env.from] = conn;
    }
    handler_(env);
  }
  Close(*conn);
  conn->exited++;
}

void TcpTransport::WriterLoop(std::shared_ptr<Conn> conn) {
  std::unique_lock lock(conn->mu);
  while (true) {
    conn->cv.wait(lock, [&] { return conn->closed || !conn->out.empty(); });
    if (conn->closed) break;
    std::string frame = std::move(conn->out.front());
    conn->out.pop_front();
    lock.unlock();
    const bool ok = SendAll(conn->fd, frame.data(), frame.size());
    lock.lock();
    if (!ok) break;
  }
  lock.unlock();
  Close(*conn);
  conn->exited++;
}

void TcpTransport::DialLoop(Peer* peer) {
  auto backoff = std::chrono::milliseconds(50);
  std::unique_lock lock(mu_);
  while (!stopping_) {
    if (peer->conn && !peer->conn->closed) {
      cv_.wait_for(lock, std::chrono::milliseconds(100));
      continue;
    }
    lock.unlock();
    const SocketHandle fd = ConnectTcp(peer->host, peer->port, 1000);
    lock.lock();
    if (stopping_) {
      if (fd != kInvalidSocket) CloseSocket(fd);
      break;
    }
    if (fd == kInvalidSocket) {
      cv_.wait_for(lock, backoff);
      backoff = std::min(backoff * 2, std::chrono::milliseconds(1000));
      continue;
    }
    backoff = std::chrono::milliseconds(50);
    peer->conn = StartConnLocked(fd);
    while (!peer->backlog.empty()) {
      Enqueue(*peer->conn, std::move(peer->backlog.front()));
      peer->backlog.pop_front();
    }
  }
}

void TcpTransport::AcceptLoop() {
  while (true) {
    const SocketHandle fd = AcceptTcp(listener_);
    std::lock_guard lock(mu_);
    if (stopping_) {
      if (fd != kInvalidSocket) CloseSocket(fd);
      return;
    }
    if (fd == kInvalidSocket) continue;
    StartConnLocked(fd);
  }
}

void TcpTransport::Stop() {
  std::vector<std::shared_ptr<Conn>> conns;
  {
    std::lock_guard lock(mu_);
    if (stopping_) return;
    stopping_ = true;
    conns = conns_;
    if (listener_ != kInvalidSocket) {
      ShutdownSocket(listener_);
      CloseSocket(listener_);  // Unblocks accept().
    }
  }
  cv_.notify_all();
  if (acceptor_.joinable()) acceptor_.join();
  for (auto& [id, peer] : peers_) {
    if (peer->dialer.joinable()) peer->dialer.join();
  }
  for (auto& c : conns) Close(*c);
  // Dialers and the acceptor are gone, so conns_ can no longer grow. Join
  // without holding mu_: a reader may be waiting on it before it sees stopping_.
  std::vector<std::shared_ptr<Conn>> all;
  {
    std::lock_guard lock(mu_);
    all.swap(conns_);
  }
  for (auto& c : all) {
    Close(*c);
    if (c->reader.joinable()) c->reader.join();
    if (c->writer.joinable()) c->writer.join();
    CloseSocket(c->fd);
  }
}

}  // namespace kv
