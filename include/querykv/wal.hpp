#pragma once
// wal.hpp — durable, crash-recoverable storage for a single Raft
// node's log + persistent election state (currentTerm/votedFor) +
// snapshots.
//
// On-disk layout (per node data directory, e.g. data/s0n0/):
//
//   meta.dat       currentTerm (u64) + votedFor (i64, -1 = none)
//                  rewritten in full on every change (tiny, cheap).
//
//   wal.log        append-only sequence of log-entry records:
//                     [term u64][index u64][cmd u8][key][value]
//                  fsync()'d after every append so a crash never
//                  loses an entry the leader believed was durable.
//
//   snapshot.dat   [lastIncludedIndex u64][lastIncludedTerm u64]
//                  [serialized KV state]
//                  Written when the log grows past a size threshold;
//                  wal.log is then compacted to only the entries
//                  after lastIncludedIndex.
//
// Raft correctness depends on: (a) currentTerm/votedFor surviving a
// crash so a restarted node can't vote twice in the same term, and
// (b) every entry a leader has ACKed to a majority surviving a
// crash. Both are handled here with explicit fsync — this is the
// module a crash-recovery demo/test exercises directly.

#include <cstdint>
#include <string>
#include <vector>
#include <map>
#include <mutex>
#include "util.hpp"

namespace qkv {

enum class CmdType : uint8_t { NOOP = 0, PUT = 1, DEL = 2 };

struct LogEntry {
    uint64_t term = 0;
    uint64_t index = 0;
    CmdType cmd = CmdType::NOOP;
    std::string key;
    std::string value;
};

class Wal {
public:
    explicit Wal(const std::string& data_dir);

    // --- persistent election state ---
    uint64_t current_term();
    int64_t voted_for(); // -1 = none
    void save_term_and_vote(uint64_t term, int64_t voted_for);

    // --- log ---
    // Appends and fsyncs a single entry. Returns once durable.
    void append(const LogEntry& e);
    // Bulk append (used when replaying a leader's AppendEntries batch).
    void append_batch(const std::vector<LogEntry>& entries);
    // Drops every entry with index >= from_index (log conflict
    // resolution per Raft §5.3) and rewrites wal.log accordingly.
    void truncate_from(uint64_t from_index);

    // In-memory view of the log, indexed by log index (rebuilt from
    // disk on construction, kept in sync on every mutation). Includes
    // only entries after the last snapshot.
    const std::vector<LogEntry>& entries() const { return log_; }
    uint64_t first_index() const; // lowest index still in the log (post-snapshot)
    uint64_t last_index() const;  // 0 if log is empty and no snapshot
    uint64_t last_term() const;

    // --- snapshotting / compaction ---
    struct SnapshotMeta {
        uint64_t last_included_index = 0;
        uint64_t last_included_term = 0;
    };
    SnapshotMeta snapshot_meta() const { return snap_meta_; }
    // kv_blob is an opaque, already-serialized KV state (see storage.hpp).
    void write_snapshot(uint64_t last_included_index, uint64_t last_included_term,
                         const std::vector<uint8_t>& kv_blob);
    bool has_snapshot() const { return snap_meta_.last_included_index > 0; }
    std::vector<uint8_t> read_snapshot_blob() const;

private:
    std::string dir_;
    std::string meta_path_, wal_path_, snap_path_;
    mutable std::mutex mu_;

    uint64_t term_ = 0;
    int64_t voted_for_ = -1;
    std::vector<LogEntry> log_; // log_[i] has index = snap_meta_.last_included_index + i + 1
    SnapshotMeta snap_meta_;

    void load_meta();
    void load_snapshot_meta();
    void replay_wal();
    void rewrite_wal_locked(); // used by truncate_from and post-snapshot compaction
};

} // namespace qkv
