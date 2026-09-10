#pragma once
// node.hpp — one QueryKV process. Owns exactly one Raft group
// membership (i.e. serves exactly one shard), a Gossip instance that
// participates in cluster-wide failure detection, and a single TCP
// listener whose port is shared by client traffic, Raft RPCs, and
// gossip probes — the opcode byte in each frame (see protocol.hpp)
// is what tells the connection handler which subsystem owns the
// request. One port per node keeps the Docker Compose file and the
// config format simple: "reach node X" only ever means one address.

#include <atomic>
#include <string>
#include <vector>
#include "config.hpp"
#include "wal.hpp"
#include "storage.hpp"
#include "raft.hpp"
#include "gossip.hpp"
#include "shard_router.hpp"

namespace qkv {

class Node {
public:
    explicit Node(const Config& cfg);

    // Blocks forever, serving connections (until the process is
    // killed — see the README for why no graceful-shutdown path is
    // needed for correctness).
    void run();

private:
    Config cfg_;
    std::string tag_; // e.g. "s0n0" — used in every log line
    Wal wal_;
    Storage storage_;
    RaftNode raft_;
    Gossip gossip_;
    ShardRouter router_; // used only to sanity-check incoming keys belong to my shard
    std::atomic<bool> running_{true};

    static constexpr size_t kSnapshotLogThreshold = 64; // small on purpose, so a demo run
                                                          // actually exercises compaction

    void handle_connection(int fd);
    void handle_client_get(int fd, const std::vector<uint8_t>& payload);
    void handle_client_put(int fd, const std::vector<uint8_t>& payload);
    void handle_client_del(int fd, const std::vector<uint8_t>& payload);
    void handle_client_status(int fd);
    void send_redirect_or_noleader(int fd);
    void respond_propose_result(int fd, const ProposeResult& res);
};

} // namespace qkv
