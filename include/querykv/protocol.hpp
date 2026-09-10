#pragma once
// protocol.hpp — QueryKV's custom binary wire protocol.
//
// Every message, whether it's a client GET/PUT, a Raft RequestVote,
// or a gossip PING, travels as one frame:
//
//     +-----------------+-----------+----------------------------+
//     |  length (u32)    | opcode(u8)|  payload (length-1 bytes)  |
//     +-----------------+-----------+----------------------------+
//
// `length` counts everything AFTER itself (opcode + payload), so a
// reader does exactly two recv() passes: read 4 bytes to learn the
// frame size, then read that many more bytes. This is the same
// length-prefixed framing real protocols (HTTP/2, gRPC, Kafka's wire
// protocol) use to avoid TCP's stream-not-message semantics — a
// single send() can arrive as many recv()s, or many send()s can
// coalesce into one recv(), and length-prefixing is what lets us
// reassemble frames correctly either way.

#include <cstdint>
#include <vector>
#include <string>
#include <optional>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>
#include "util.hpp"

namespace qkv {

constexpr uint32_t MAX_FRAME_BYTES = 16 * 1024 * 1024; // 16MB guardrail

enum class OpCode : uint8_t {
    // --- client <-> node ---
    CLIENT_GET       = 0x01,
    CLIENT_PUT       = 0x02,
    CLIENT_DEL       = 0x03,
    CLIENT_RESP_OK   = 0x04,
    CLIENT_RESP_NOTFOUND = 0x05,
    CLIENT_RESP_REDIRECT = 0x06, // "I'm not the leader, try this address"
    CLIENT_RESP_NOLEADER = 0x07, // election in progress, retry shortly
    CLIENT_STATUS_REQ = 0x08,    // ask a node for its raft/gossip status
    CLIENT_STATUS_RESP = 0x09,

    // --- raft <-> raft ---
    RAFT_REQUEST_VOTE       = 0x20,
    RAFT_REQUEST_VOTE_RESP  = 0x21,
    RAFT_APPEND_ENTRIES     = 0x22,
    RAFT_APPEND_ENTRIES_RESP = 0x23,

    // --- gossip <-> gossip (SWIM-lite membership) ---
    GOSSIP_PING       = 0x40,
    GOSSIP_ACK        = 0x41,
    GOSSIP_PING_REQ   = 0x42, // "ping this other node on my behalf"
};

// Low-level: send exactly `len` bytes or throw.
inline void send_all(int fd, const uint8_t* buf, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = ::send(fd, buf + sent, len - sent, 0);
        if (n <= 0) throw std::runtime_error("qkv::send_all: connection lost");
        sent += static_cast<size_t>(n);
    }
}

// Low-level: receive exactly `len` bytes or throw (peer closed / error).
inline void recv_all(int fd, uint8_t* buf, size_t len) {
    size_t got = 0;
    while (got < len) {
        ssize_t n = ::recv(fd, buf + got, len - got, 0);
        if (n <= 0) throw std::runtime_error("qkv::recv_all: connection closed");
        got += static_cast<size_t>(n);
    }
}

// A decoded frame: opcode + raw payload bytes (caller decodes payload
// with a ByteReader once it knows the opcode).
struct Frame {
    OpCode op;
    std::vector<uint8_t> payload;
};

inline void send_frame(int fd, OpCode op, const std::vector<uint8_t>& payload) {
    uint32_t total = static_cast<uint32_t>(1 + payload.size());
    std::vector<uint8_t> header;
    put_u32(header, total);
    header.push_back(static_cast<uint8_t>(op));
    send_all(fd, header.data(), header.size());
    if (!payload.empty()) send_all(fd, payload.data(), payload.size());
}

// Returns std::nullopt if the peer closed the connection cleanly
// before sending any bytes of the next frame (a normal way for a
// short-lived client connection to end). Throws on a mid-frame
// disconnect or a frame that violates MAX_FRAME_BYTES.
inline std::optional<Frame> recv_frame(int fd) {
    uint8_t len_buf[4];
    ssize_t n = ::recv(fd, len_buf, 4, MSG_WAITALL);
    if (n == 0) return std::nullopt;
    if (n < 0) throw std::runtime_error("qkv::recv_frame: recv error on length prefix");
    if (n < 4) recv_all(fd, len_buf + n, 4 - n); // finish a partial prefix read

    uint32_t total = (static_cast<uint32_t>(len_buf[0]) << 24) |
                      (static_cast<uint32_t>(len_buf[1]) << 16) |
                      (static_cast<uint32_t>(len_buf[2]) << 8) |
                      (static_cast<uint32_t>(len_buf[3]));
    if (total == 0 || total > MAX_FRAME_BYTES) {
        throw std::runtime_error("qkv::recv_frame: invalid frame length " + std::to_string(total));
    }

    std::vector<uint8_t> body(total);
    recv_all(fd, body.data(), total);

    Frame f;
    f.op = static_cast<OpCode>(body[0]);
    f.payload.assign(body.begin() + 1, body.end());
    return f;
}

} // namespace qkv
