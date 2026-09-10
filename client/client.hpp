#pragma once
// client.hpp — the piece that makes "a bunch of independent Raft
// groups behind a hash ring" look like one key-value store to a
// caller. For every request it:
//   1. Hashes the key to a shard with the same consistent-hash ring
//      every node uses (shard_router.hpp).
//   2. Sends the request to its best guess at that shard's current
//      leader (cached from the last successful call).
//   3. Follows CLIENT_RESP_REDIRECT if the guess was wrong, and
//      retries against another node on CLIENT_RESP_NOLEADER (a
//      shard mid-election).
// None of this is hidden behind a network round-trip abstraction the
// caller can't see — get()/put()/del() do a bounded number of
// attempts and return a definite success/failure rather than
// retrying forever, which is the honest contract for a client living
// on the other side of a partition from its cluster.

#include <cstdint>
#include <string>
#include <vector>
#include <map>
#include "querykv/protocol.hpp"
#include "querykv/shard_router.hpp"

namespace qkv {

struct ClientPeer {
    uint64_t id;
    int shard;
    std::string host;
    int port;
};

std::vector<ClientPeer> parse_client_cluster(const std::string& spec); // "id:shard:host:port,..."

struct GetResult {
    bool reached_cluster = false;
    bool found = false;
    std::string value;
};
struct WriteResult {
    bool ok = false;
};

class Client {
public:
    Client(std::vector<ClientPeer> cluster, int num_shards);

    GetResult get(const std::string& key);
    WriteResult put(const std::string& key, const std::string& value);
    WriteResult del(const std::string& key);

    // Queries every known node's CLIENT_STATUS_REQ and returns a
    // combined human-readable report — used by the CLI's `status`
    // command and the chaos test script to find the current leader.
    std::string cluster_status();

private:
    std::vector<ClientPeer> cluster_;
    ShardRouter router_;
    std::map<int, uint64_t> known_leader_; // shard -> best-guess leader node id

    std::vector<ClientPeer> nodes_for_shard(int shard);
    ClientPeer* find_node_in(std::vector<ClientPeer>& v, uint64_t id);

    struct RawResult {
        bool reached = false;
        OpCode op = OpCode::CLIENT_RESP_NOLEADER;
        std::vector<uint8_t> payload;
    };
    RawResult send_with_retry(int shard, OpCode op, const std::vector<uint8_t>& payload,
                                int max_attempts = 8);
};

} // namespace qkv
