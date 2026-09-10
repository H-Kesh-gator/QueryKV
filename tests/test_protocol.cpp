// test_protocol.cpp — exercises frame send/recv over a REAL socket
// pair (not just in-memory buffers) so a bug in send_all/recv_all's
// partial-write/partial-read handling would actually show up, plus
// the consistent-hash shard router's determinism and balance.

#include "querykv/protocol.hpp"
#include "querykv/shard_router.hpp"
#include <sys/socket.h>
#include <unistd.h>
#include <cassert>
#include <iostream>
#include <map>
#include <thread>

using namespace qkv;

static int g_failures = 0;
#define CHECK(cond) \
    do { \
        if (!(cond)) { \
            std::cerr << "  FAIL: " << #cond << " (line " << __LINE__ << ")\n"; \
            g_failures++; \
        } \
    } while (0)

void test_frame_roundtrip() {
    std::cout << "test_frame_roundtrip\n";
    int fds[2];
    CHECK(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

    std::vector<uint8_t> payload;
    put_str(payload, "hello");
    put_u64(payload, 123456789ULL);

    std::thread sender([&] {
        send_frame(fds[0], OpCode::CLIENT_PUT, payload);
        ::close(fds[0]);
    });

    auto frame = recv_frame(fds[1]);
    sender.join();
    ::close(fds[1]);

    CHECK(frame.has_value());
    if (frame) {
        CHECK(frame->op == OpCode::CLIENT_PUT);
        ByteReader r(frame->payload.data(), frame->payload.size());
        CHECK(r.get_str() == "hello");
        CHECK(r.get_u64() == 123456789ULL);
    }
}

void test_frame_large_payload_and_partial_io() {
    std::cout << "test_frame_large_payload_and_partial_io\n";
    int fds[2];
    CHECK(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

    // Bigger than a typical socket buffer, to force the kernel (and
    // therefore send_all/recv_all) through multiple partial writes/reads.
    std::vector<uint8_t> payload(2 * 1024 * 1024, 0xAB);
    put_u32(payload, 0xDEADBEEF); // corrupt-looking prefix on purpose — payload is opaque bytes here

    std::thread sender([&] {
        send_frame(fds[0], OpCode::CLIENT_RESP_OK, payload);
        ::close(fds[0]);
    });
    auto frame = recv_frame(fds[1]);
    sender.join();
    ::close(fds[1]);

    CHECK(frame.has_value());
    if (frame) {
        CHECK(frame->payload.size() == payload.size());
        CHECK(frame->payload == payload);
    }
}

void test_shard_router_determinism_and_balance() {
    std::cout << "test_shard_router_determinism_and_balance\n";
    ShardRouter router(4, 128);

    // Same key must always map to the same shard.
    for (int i = 0; i < 1000; i++) {
        std::string key = "key-" + std::to_string(i);
        CHECK(router.shard_for_key(key) == router.shard_for_key(key));
    }

    // With enough keys and virtual nodes, no shard should be wildly
    // over/under-represented (loose bound — this is a balance sanity
    // check, not a statistical proof).
    std::map<int, int> counts;
    const int N = 20000;
    for (int i = 0; i < N; i++) {
        counts[router.shard_for_key("k" + std::to_string(i))]++;
    }
    CHECK(static_cast<int>(counts.size()) == 4);
    for (auto& [shard, count] : counts) {
        double frac = static_cast<double>(count) / N;
        CHECK(frac > 0.15 && frac < 0.35); // ideal is 0.25 each
    }
}

int main() {
    test_frame_roundtrip();
    test_frame_large_payload_and_partial_io();
    test_shard_router_determinism_and_balance();

    if (g_failures == 0) {
        std::cout << "ALL PROTOCOL TESTS PASSED\n";
        return 0;
    }
    std::cout << g_failures << " CHECK(S) FAILED\n";
    return 1;
}
