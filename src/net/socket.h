// Thin portable wrapper over BSD sockets / Winsock.
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace kv {

#ifdef _WIN32
using SocketHandle = uintptr_t;
#else
using SocketHandle = int;
#endif

extern const SocketHandle kInvalidSocket;

// Must be called before any other socket function (idempotent).
bool NetInit();

bool ParseHostPort(const std::string& addr, std::string* host, uint16_t* port);

// Returns kInvalidSocket on failure or if the connect takes longer than timeout_ms.
SocketHandle ConnectTcp(const std::string& host, uint16_t port, int timeout_ms);
// Binds and listens. Pass port 0 for an ephemeral port; the bound port is returned.
SocketHandle ListenTcp(const std::string& host, uint16_t port, uint16_t* bound_port, std::string* error);
SocketHandle AcceptTcp(SocketHandle listener);

bool SendAll(SocketHandle s, const char* data, size_t n);
bool RecvAll(SocketHandle s, char* data, size_t n);

// Unblocks any thread waiting in recv/accept on the socket.
void ShutdownSocket(SocketHandle s);
void CloseSocket(SocketHandle s);

}  // namespace kv
