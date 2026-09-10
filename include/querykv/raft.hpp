#pragma once
// raft.hpp — the consensus core. One RaftNode instance runs the Raft
// protocol for ONE replica group (one shard's 3 or 5 nodes); QueryKV
// runs one RaftNode per shard per process, so a "multi-shard" cluster
// is just multiple independent Raft groups that happen to share a
// TCP listener and a gossip layer (see node.hpp).
//
// Implements the core Raft paper (Ongaro & Ousterhout, "In Search of
// an Understandable Consensus Algorithm"):
//   - randomized election timeouts + RequestVote RPC (§5.2)
//   - log replication via AppendEntries, with the log-matching
//     property enforced by prevLogIndex/prevLogTerm checks (§5.3)
//   - commit-index advancement by majority match, leader only commits
//     entries from its OWN term directly, per the §5.4.2 restriction
//   - conflict-index fast-backtrack on AppendEntries rejection, so a
//     lagging follower catches up in O(divergent terms) RPCs instead
//     of O(divergent entries)
//   - a no-op entry committed immediately on election, so a new
//     leader can safely advance commitIndex over entries from prior
//     terms once the no-op itself is replicated (§5.4.2)
//
// Deliberately NOT implemented (documented in the README as explicit
// scope cuts, not oversights): cluster membership changes (joint
// consensus), InstallSnapshot RPC for a follower that's fallen behind
// the log's start (snapshots exist for local compaction/restart but
// aren't yet shipped to lagging followers over the wire), and
// pre-vote (so a partitioned-then-rejoining node can still disrupt
// the term briefly — visible in the chaos test).

#include <cstdint>
#include <string>
#include <vector>
#include <map>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <optional>
#include <functional>
#include "wal.hpp"
#include "storage.hpp"

namespace qkv {

enum class Role { FOLLOWER, CANDIDATE, LEADER };

struct PeerInfo {
    uint64_t id;
    std::string host;
    int port;
};

struct RequestVoteReq {
    uint64_t term;
    uint64_t candidate_id;
    uint64_t last_log_index;
    uint64_t last_log_term;
};
struct RequestVoteResp {
    uint64_t term;
    bool vote_granted;
};

struct AppendEntriesReq {
    uint64_t term;
    uint64_t leader_id;
    uint64_t prev_log_index;
    uint64_t prev_log_term;
    uint64_t leader_commit;
    std::vector<LogEntry> entries; // empty = heartbeat
};
struct AppendEntriesResp {
    uint64_t term;
    bool success;
    uint64_t conflict_index; // fast-backtrack hint, valid when !success
};

std::vector<uint8_t> encode_request_vote_req(const RequestVoteReq&);
RequestVoteReq decode_request_vote_req(const std::vector<uint8_t>&);
std::vector<uint8_t> encode_request_vote_resp(const RequestVoteResp&);
RequestVoteResp decode_request_vote_resp(const std::vector<uint8_t>&);
std::vector<uint8_t> encode_append_entries_req(const AppendEntriesReq&);
AppendEntriesReq decode_append_entries_req(const std::vector<uint8_t>&);
std::vector<uint8_t> encode_append_entries_resp(const AppendEntriesResp&);
AppendEntriesResp decode_append_entries_resp(const std::vector<uint8_t>&);

struct ProposeResult {
    bool ok = false;              // true = committed and applied
    bool is_leader = true;        // false => not_leader_hint is who to retry
    bool timed_out = false;
    std::optional<PeerInfo> leader_hint;
};

class RaftNode {
public:
    RaftNode(uint64_t self_id, std::string log_tag, std::vector<PeerInfo> peers,
              Wal& wal, Storage& storage);

    void start();
    void stop();

    Role role();
    uint64_t current_term();
    std::optional<PeerInfo> leader_hint(); // best known leader, for redirects

    // Client entry point. Blocks until the entry is committed+applied
    // or wait_ms elapses. Only succeeds if this node is (or becomes,
    // and stays) leader for the duration.
    ProposeResult propose(CmdType cmd, const std::string& key, const std::string& value,
                          int wait_ms = 2000);

    // Called by the transport layer (node.cpp) when a frame with a
    // Raft opcode arrives; returns the encoded response payload.
    std::vector<uint8_t> handle_request_vote(const std::vector<uint8_t>& payload);
    std::vector<uint8_t> handle_append_entries(const std::vector<uint8_t>& payload);

    // Snapshot trigger: called periodically by node.cpp once the log
    // exceeds a size threshold.
    void maybe_snapshot(size_t log_size_threshold);

    std::string status_line(); // human-readable, used by CLIENT_STATUS and logs

private:
    uint64_t self_id_;
    std::string tag_;
    std::vector<PeerInfo> peers_; // does not include self
    Wal& wal_;
    Storage& storage_;

    std::mutex mu_;
    std::condition_variable cv_commit_; // notified when commit_index_ advances
    Role role_ = Role::FOLLOWER;
    uint64_t commit_index_ = 0;
    uint64_t last_applied_ = 0;
    std::optional<uint64_t> voted_leader_hint_id_; // last leader we saw AppendEntries from
    long long last_heard_from_leader_ms_ = 0;       // for election timeout
    long long election_timeout_ms_ = 1000;          // current randomized threshold

    // leader-only volatile state
    std::map<uint64_t, uint64_t> next_index_;
    std::map<uint64_t, uint64_t> match_index_;

    std::atomic<bool> running_{false};
    std::thread election_thread_;
    std::thread apply_thread_;
    // Replicator threads are detached, not joined: each checks
    // leader_epoch_ every iteration and exits on its own once this
    // node stops being leader for that epoch, so nothing needs to
    // block become_follower_locked() / stop() waiting for them.
    std::atomic<uint64_t> leader_epoch_{0};

    // --- internals ---
    void election_timer_loop();
    void apply_loop();
    void replicator_loop(PeerInfo peer, uint64_t my_epoch);

    void become_follower_locked(uint64_t new_term);
    void become_candidate();
    void become_leader();

    void advance_commit_index_locked();
    long long random_election_timeout_ms();
    std::optional<PeerInfo> leader_hint_locked();

    // log helpers (must hold mu_)
    uint64_t last_log_index_locked();
    uint64_t last_log_term_locked();
    std::optional<LogEntry> entry_at_locked(uint64_t index); // nullopt if snapshotted/missing
};

} // namespace qkv
