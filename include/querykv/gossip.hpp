#pragma once
// gossip.hpp — cluster-wide membership & failure detection, modeled
// on SWIM (Das, Gupta, Motivala — "SWIM: Scalable Weakly-consistent
// Infection-style Process Group Membership Protocol"), simplified.
//
// This runs ACROSS ALL nodes in the cluster (both shards), and is
// intentionally independent of Raft's own heartbeats. That separation
// is a deliberate design choice, not redundancy — worth being able to
// explain in an interview:
//
//   - Raft heartbeats are a CONSISTENCY mechanism: they exist so a
//     leader can maintain authority and replicate its log. They only
//     tell you about liveness WITHIN one 3-node shard, and only from
//     the leader's point of view.
//   - Gossip is an OPERATIONAL mechanism: it gives every node
//     (regardless of shard or role) an eventually-consistent view of
//     "which of the N nodes in the whole cluster are up", which a
//     status endpoint / ops dashboard / the client's routing layer can
//     use without needing to know who's a Raft leader of what.
//
// Protocol per round, per node:
//   1. Pick a random other member, send a direct PING (piggybacking
//      our membership digest). Wait up to a short timeout.
//   2. If no ACK, ask k random OTHER members to PING the suspect on
//      our behalf (PING_REQ) — this filters out "my direct path to
//      them is flaky" false positives, the core SWIM insight.
//   3. If still no ACK via any indirect path, mark the member SUSPECT
//      with the current time. If a member stays SUSPECT past
//      suspect_timeout without a higher-incarnation refutation, it's
//      marked DEAD.
//   4. A node that sees itself reported SUSPECT/DEAD in someone else's
//      digest immediately bumps its own incarnation number and
//      re-announces ALIVE — this is how a node "refutes" a false
//      failure report (e.g. it was just slow/GC-paused, not down).
//
// Simplifications vs. full SWIM (documented, not accidental): no
// piggybacked membership updates beyond the digest exchanged on PING/
// ACK (no separate gossip dissemination queue with a bounded fanout);
// fixed (not adaptive) ping timeout.

#include <cstdint>
#include <string>
#include <vector>
#include <map>
#include <mutex>
#include <thread>
#include <atomic>
#include "raft.hpp" // reuses PeerInfo

namespace qkv {

enum class MemberState : uint8_t { ALIVE = 0, SUSPECT = 1, DEAD = 2 };

struct Member {
    uint64_t id;
    std::string host;
    int port;
    MemberState state = MemberState::ALIVE;
    uint64_t incarnation = 0;
    long long last_change_ms = 0;
};

class Gossip {
public:
    Gossip(uint64_t self_id, std::string tag, std::vector<PeerInfo> all_members);

    void start();
    void stop();

    std::vector<Member> snapshot();

    // Called by node.cpp's dispatcher for GOSSIP_* opcodes.
    std::vector<uint8_t> handle_ping(const std::vector<uint8_t>& payload);
    std::vector<uint8_t> handle_ping_req(const std::vector<uint8_t>& payload);

private:
    uint64_t self_id_;
    std::string tag_;
    std::mutex mu_;
    std::map<uint64_t, Member> members_; // includes self

    std::atomic<bool> running_{false};
    std::thread loop_thread_;

    void loop();
    void probe_once();
    bool direct_ping(const Member& target, int timeout_ms);
    bool indirect_ping(const Member& target, int helpers, int timeout_ms);
    void merge_entries_locked(const std::vector<Member>& incoming); // caller holds mu_
    void mark_locked(uint64_t id, MemberState st, uint64_t incarnation);
    void reap_suspects_locked();

    std::vector<uint8_t> encode_digest_locked();
};

} // namespace qkv
