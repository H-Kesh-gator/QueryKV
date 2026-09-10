#pragma once
// net.hpp — thin POSIX socket helpers (raw sockets, no networking
// library). Shared by the node's listener and the client/inter-node
// dialer so socket setup boilerplate lives in exactly one place.

#include <string>
#include <stdexcept>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <netdb.h>

namespace qkv {

// Opens a listening TCP socket on 0.0.0.0:port with SO_REUSEADDR so a
// crashed-and-restarted node doesn't have to wait out TIME_WAIT.
inline int tcp_listen(int port, int backlog = 64) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) throw std::runtime_error("tcp_listen: socket() failed");

    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(static_cast<uint16_t>(port));

    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        throw std::runtime_error("tcp_listen: bind() failed on port " + std::to_string(port));
    }
    if (::listen(fd, backlog) < 0) {
        ::close(fd);
        throw std::runtime_error("tcp_listen: listen() failed");
    }
    return fd;
}

// Connects to host:port with a bounded timeout (so a dead peer fails
// fast instead of hanging a Raft heartbeat / gossip probe for the
// kernel's default connect timeout of ~2 minutes). Returns -1 instead
// of throwing, since callers treat "peer unreachable" as routine.
inline int tcp_connect(const std::string& host, int port, int timeout_ms = 300) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    // Non-blocking connect + select-based timeout.
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));

    // Resolve host (supports both dotted IPs and Docker Compose service
    // names via getaddrinfo, so the same config works locally and in
    // containers).
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* res = nullptr;
    if (getaddrinfo(host.c_str(), nullptr, &hints, &res) != 0 || res == nullptr) {
        ::close(fd);
        return -1;
    }
    addr.sin_addr = reinterpret_cast<sockaddr_in*>(res->ai_addr)->sin_addr;
    freeaddrinfo(res);

    int rc = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (rc < 0 && errno != EINPROGRESS) {
        ::close(fd);
        return -1;
    }
    if (rc < 0) {
        fd_set wfds;
        FD_ZERO(&wfds);
        FD_SET(fd, &wfds);
        timeval tv{};
        tv.tv_sec = timeout_ms / 1000;
        tv.tv_usec = (timeout_ms % 1000) * 1000;
        rc = ::select(fd + 1, nullptr, &wfds, nullptr, &tv);
        if (rc <= 0) { ::close(fd); return -1; }
        int err = 0;
        socklen_t elen = sizeof(err);
        getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &elen);
        if (err != 0) { ::close(fd); return -1; }
    }

    // Back to blocking mode for the lifetime of the connection — the
    // rest of the codebase uses simple blocking send_all/recv_all.
    fcntl(fd, F_SETFL, flags);
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)); // low-latency RPCs
    return fd;
}

} // namespace qkv
