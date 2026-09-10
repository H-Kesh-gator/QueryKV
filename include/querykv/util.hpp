#pragma once
// util.hpp — shared logging + binary encode/decode helpers.
//
// QueryKV deliberately avoids third-party serialization libraries
// (no protobuf/json). Every message on the wire — client RPCs, Raft
// RPCs, gossip pings — is hand-encoded as raw bytes. This keeps the
// project dependency-free and forces us to reason about framing,
// endianness, and buffer boundaries explicitly, which is the point
// of a "CN" (computer networks) style project.

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <chrono>
#include <iostream>
#include <mutex>
#include <sstream>
#include <thread>

namespace qkv {

// ---------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------
// A tiny thread-safe logger. Every node prints [node_id][LEVEL] lines
// with a monotonic millisecond timestamp so multi-node runs can be
// interleaved and still read chronologically.

inline std::mutex& log_mutex() {
    static std::mutex m;
    return m;
}

inline long long now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

enum class LogLevel { DEBUG, INFO, WARN, ERROR };

inline const char* level_str(LogLevel lvl) {
    switch (lvl) {
        case LogLevel::DEBUG: return "DEBUG";
        case LogLevel::INFO:  return "INFO ";
        case LogLevel::WARN:  return "WARN ";
        case LogLevel::ERROR: return "ERROR";
    }
    return "?????";
}

inline void log_line(const std::string& node_tag, LogLevel lvl, const std::string& msg) {
    std::lock_guard<std::mutex> lock(log_mutex());
    std::cerr << "[t+" << now_ms() << "ms][" << node_tag << "][" << level_str(lvl) << "] "
              << msg << std::endl;
}

#define QKV_LOG(node_tag, lvl, ...) \
    do { \
        std::ostringstream _oss; \
        _oss << __VA_ARGS__; \
        qkv::log_line((node_tag), (lvl), _oss.str()); \
    } while (0)

// ---------------------------------------------------------------------
// Binary encode/decode
// ---------------------------------------------------------------------
// All multi-byte integers are written big-endian ("network order") so
// packet dumps are human-readable in a hex editor and the protocol is
// architecture-independent.

inline void put_u8(std::vector<uint8_t>& buf, uint8_t v) {
    buf.push_back(v);
}

inline void put_u32(std::vector<uint8_t>& buf, uint32_t v) {
    buf.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>(v & 0xFF));
}

inline void put_u64(std::vector<uint8_t>& buf, uint64_t v) {
    for (int shift = 56; shift >= 0; shift -= 8) {
        buf.push_back(static_cast<uint8_t>((v >> shift) & 0xFF));
    }
}

inline void put_str(std::vector<uint8_t>& buf, const std::string& s) {
    put_u32(buf, static_cast<uint32_t>(s.size()));
    buf.insert(buf.end(), s.begin(), s.end());
}

// Cursor-based reader over a byte buffer. Throws std::out_of_range on
// truncated input rather than reading past the end — callers treat a
// decode exception as "drop this malformed frame".
struct ByteReader {
    const uint8_t* data;
    size_t len;
    size_t pos = 0;

    ByteReader(const uint8_t* d, size_t l) : data(d), len(l) {}

    void need(size_t n) const {
        if (pos + n > len) throw std::out_of_range("qkv::ByteReader: truncated buffer");
    }

    uint8_t get_u8() {
        need(1);
        return data[pos++];
    }

    uint32_t get_u32() {
        need(4);
        uint32_t v = (static_cast<uint32_t>(data[pos]) << 24) |
                     (static_cast<uint32_t>(data[pos + 1]) << 16) |
                     (static_cast<uint32_t>(data[pos + 2]) << 8) |
                     (static_cast<uint32_t>(data[pos + 3]));
        pos += 4;
        return v;
    }

    uint64_t get_u64() {
        need(8);
        uint64_t v = 0;
        for (int i = 0; i < 8; i++) v = (v << 8) | data[pos + i];
        pos += 8;
        return v;
    }

    std::string get_str() {
        uint32_t n = get_u32();
        need(n);
        std::string s(reinterpret_cast<const char*>(data + pos), n);
        pos += n;
        return s;
    }

    bool eof() const { return pos >= len; }
};

} // namespace qkv
