#include "net/socket.h"

#include <cerrno>
#include <cstring>
#include <mutex>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace kv {

#ifdef _WIN32
const SocketHandle kInvalidSocket = static_cast<SocketHandle>(INVALID_SOCKET);
#else
const SocketHandle kInvalidSocket = -1;
#endif

namespace {

#ifdef _WIN32
using NativeSocket = SOCKET;
#else
using NativeSocket = int;
#endif

inline NativeSocket N(SocketHandle s) { return static_cast<NativeSocket>(s); }

void SetNonBlocking(SocketHandle s, bool on) {
#ifdef _WIN32
  u_long mode = on ? 1 : 0;
  ioctlsocket(N(s), FIONBIO, &mode);
#else
  int flags = fcntl(s, F_GETFL, 0);
  fcntl(s, F_SETFL, on ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK));
#endif
}

void SetNoDelay(SocketHandle s) {
  int one = 1;
  setsockopt(N(s), IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&one),
             sizeof(one));
}

bool Resolve(const std::string& host, uint16_t port, sockaddr_in* out) {
  std::memset(out, 0, sizeof(*out));
  out->sin_family = AF_INET;
  out->sin_port = htons(port);
  if (host.empty() || host == "0.0.0.0" || host == "*") {
    out->sin_addr.s_addr = htonl(INADDR_ANY);
    return true;
  }
  addrinfo hints{};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* res = nullptr;
  if (getaddrinfo(host.c_str(), nullptr, &hints, &res) != 0 || res == nullptr) return false;
  out->sin_addr = reinterpret_cast<sockaddr_in*>(res->ai_addr)->sin_addr;
  freeaddrinfo(res);
  return true;
}

}  // namespace

bool NetInit() {
#ifdef _WIN32
  static std::once_flag once;
  static bool ok = false;
  std::call_once(once, [] {
    WSADATA data;
    ok = WSAStartup(MAKEWORD(2, 2), &data) == 0;
  });
  return ok;
#else
  return true;
#endif
}

bool ParseHostPort(const std::string& addr, std::string* host, uint16_t* port) {
  const size_t colon = addr.rfind(':');
  if (colon == std::string::npos) return false;
  *host = addr.substr(0, colon);
  try {
    const int p = std::stoi(addr.substr(colon + 1));
    if (p < 0 || p > 65535) return false;
    *port = static_cast<uint16_t>(p);
  } catch (...) {
    return false;
  }
  return true;
}

SocketHandle ConnectTcp(const std::string& host, uint16_t port, int timeout_ms) {
  NetInit();
  sockaddr_in addr;
  if (!Resolve(host, port, &addr)) return kInvalidSocket;
  const auto s = static_cast<SocketHandle>(socket(AF_INET, SOCK_STREAM, IPPROTO_TCP));
  if (s == kInvalidSocket) return kInvalidSocket;

  // Non-blocking connect so an unreachable host cannot stall the caller.
  SetNonBlocking(s, true);
  int rc = connect(N(s), reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
  bool ok = rc == 0;
  if (!ok) {
#ifdef _WIN32
    const bool in_progress = WSAGetLastError() == WSAEWOULDBLOCK;
    if (in_progress) {
      fd_set wfds, efds;
      FD_ZERO(&wfds);
      FD_ZERO(&efds);
      FD_SET(N(s), &wfds);
      FD_SET(N(s), &efds);
      timeval tv{timeout_ms / 1000, (timeout_ms % 1000) * 1000};
      ok = select(0, nullptr, &wfds, &efds, &tv) > 0 && FD_ISSET(N(s), &wfds);
    }
#else
    if (errno == EINPROGRESS) {
      pollfd pfd{s, POLLOUT, 0};
      ok = poll(&pfd, 1, timeout_ms) > 0 && (pfd.revents & POLLOUT);
    }
#endif
    if (ok) {
      int err = 0;
      socklen_t len = sizeof(err);
      getsockopt(N(s), SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&err),
                 &len);
      ok = err == 0;
    }
  }
  if (!ok) {
    CloseSocket(s);
    return kInvalidSocket;
  }
  SetNonBlocking(s, false);
  SetNoDelay(s);
  return s;
}

SocketHandle ListenTcp(const std::string& host, uint16_t port, uint16_t* bound_port, std::string* error) {
  NetInit();
  sockaddr_in addr;
  if (!Resolve(host, port, &addr)) {
    *error = "cannot resolve " + host;
    return kInvalidSocket;
  }
  const auto s = static_cast<SocketHandle>(socket(AF_INET, SOCK_STREAM, IPPROTO_TCP));
  if (s == kInvalidSocket) {
    *error = "socket() failed";
    return kInvalidSocket;
  }
#ifndef _WIN32
  // On Windows SO_REUSEADDR would let two processes bind the same port, so
  // only use it on POSIX where it just skips TIME_WAIT.
  int one = 1;
  setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
#endif
  const auto native = N(s);
  if (bind(native, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 || listen(native, 128) != 0) {
    *error = "cannot bind/listen on " + host + ":" + std::to_string(port);
    CloseSocket(s);
    return kInvalidSocket;
  }
  sockaddr_in actual{};
  socklen_t len = sizeof(actual);
  getsockname(native, reinterpret_cast<sockaddr*>(&actual), &len);
  *bound_port = ntohs(actual.sin_port);
  return s;
}

SocketHandle AcceptTcp(SocketHandle listener) {
  const auto s =
      static_cast<SocketHandle>(accept(N(listener), nullptr, nullptr));
  if (s != kInvalidSocket) SetNoDelay(s);
  return s;
}

bool SendAll(SocketHandle s, const char* data, size_t n) {
  while (n > 0) {
#ifdef _WIN32
    const int sent = send(N(s), data, static_cast<int>(n), 0);
#else
    const ssize_t sent = send(s, data, n, MSG_NOSIGNAL);
#endif
    if (sent <= 0) return false;
    data += sent;
    n -= static_cast<size_t>(sent);
  }
  return true;
}

bool RecvAll(SocketHandle s, char* data, size_t n) {
  while (n > 0) {
#ifdef _WIN32
    const int got = recv(N(s), data, static_cast<int>(n), 0);
#else
    const ssize_t got = recv(s, data, n, 0);
#endif
    if (got <= 0) return false;
    data += got;
    n -= static_cast<size_t>(got);
  }
  return true;
}

void ShutdownSocket(SocketHandle s) {
#ifdef _WIN32
  shutdown(N(s), SD_BOTH);
#else
  shutdown(s, SHUT_RDWR);
#endif
}

void CloseSocket(SocketHandle s) {
#ifdef _WIN32
  closesocket(N(s));
#else
  close(s);
#endif
}

}  // namespace kv
