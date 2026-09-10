#pragma once
// storage.hpp — the actual "database": an in-memory map that Raft
// applies committed log entries to. Deliberately simple (a
// std::map guarded by a mutex) because the interesting engineering
// in this project is consensus and networking, not the storage
// engine itself — swapping this for an LSM-tree/B-tree engine later
// would not touch the Raft or protocol layers at all, which is the
// point of keeping it behind a small interface.

#include <string>
#include <map>
#include <mutex>
#include <optional>
#include <vector>
#include <cstdint>
#include "util.hpp"
#include "wal.hpp"

namespace qkv {

class Storage {
public:
    void apply(const LogEntry& e) {
        std::lock_guard<std::mutex> lock(mu_);
        if (e.cmd == CmdType::PUT) {
            map_[e.key] = e.value;
        } else if (e.cmd == CmdType::DEL) {
            map_.erase(e.key);
        }
        // NOOP entries (the empty entry a new leader commits on
        // election, per Raft §5.4.2) touch no state.
    }

    std::optional<std::string> get(const std::string& key) {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = map_.find(key);
        if (it == map_.end()) return std::nullopt;
        return it->second;
    }

    size_t size() {
        std::lock_guard<std::mutex> lock(mu_);
        return map_.size();
    }

    // Serializes the full map for snapshotting.
    std::vector<uint8_t> serialize() {
        std::lock_guard<std::mutex> lock(mu_);
        std::vector<uint8_t> buf;
        put_u32(buf, static_cast<uint32_t>(map_.size()));
        for (const auto& [k, v] : map_) {
            put_str(buf, k);
            put_str(buf, v);
        }
        return buf;
    }

    // Replaces the map wholesale from a snapshot blob (used on
    // startup when a node restores from snapshot.dat, and when a
    // follower gets caught up via an InstallSnapshot in a later
    // iteration of this project).
    void restore(const std::vector<uint8_t>& blob) {
        std::lock_guard<std::mutex> lock(mu_);
        map_.clear();
        if (blob.empty()) return;
        ByteReader r(blob.data(), blob.size());
        uint32_t n = r.get_u32();
        for (uint32_t i = 0; i < n; i++) {
            std::string k = r.get_str();
            std::string v = r.get_str();
            map_[k] = v;
        }
    }

private:
    std::mutex mu_;
    std::map<std::string, std::string> map_;
};

} // namespace qkv
