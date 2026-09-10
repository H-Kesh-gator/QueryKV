#include "querykv/wal.hpp"
#include <cstdio>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdexcept>

namespace qkv {

static void ensure_dir(const std::string& path) {
    struct stat st{};
    if (stat(path.c_str(), &st) != 0) {
        if (mkdir(path.c_str(), 0755) != 0) {
            throw std::runtime_error("wal: could not create data dir " + path);
        }
    }
}

static std::vector<uint8_t> encode_entry(const LogEntry& e) {
    std::vector<uint8_t> b;
    put_u64(b, e.term);
    put_u64(b, e.index);
    put_u8(b, static_cast<uint8_t>(e.cmd));
    put_str(b, e.key);
    put_str(b, e.value);
    return b;
}

Wal::Wal(const std::string& data_dir) : dir_(data_dir) {
    ensure_dir(dir_);
    meta_path_ = dir_ + "/meta.dat";
    wal_path_ = dir_ + "/wal.log";
    snap_path_ = dir_ + "/snapshot.dat";

    load_meta();
    load_snapshot_meta();
    replay_wal();
}

// ---------------------------------------------------------------------
// election state
// ---------------------------------------------------------------------

void Wal::load_meta() {
    FILE* f = fopen(meta_path_.c_str(), "rb");
    if (!f) { term_ = 0; voted_for_ = -1; return; }
    uint8_t buf[16];
    size_t n = fread(buf, 1, 16, f);
    fclose(f);
    if (n != 16) { term_ = 0; voted_for_ = -1; return; }
    ByteReader r(buf, 16);
    term_ = r.get_u64();
    voted_for_ = static_cast<int64_t>(r.get_u64());
}

uint64_t Wal::current_term() {
    std::lock_guard<std::mutex> lock(mu_);
    return term_;
}

int64_t Wal::voted_for() {
    std::lock_guard<std::mutex> lock(mu_);
    return voted_for_;
}

void Wal::save_term_and_vote(uint64_t term, int64_t voted_for) {
    std::lock_guard<std::mutex> lock(mu_);
    term_ = term;
    voted_for_ = voted_for;
    // Rewrite the whole (tiny, fixed-size) file and fsync — this must
    // hit disk before we send/honor any vote, or a crash could let us
    // double-vote in the same term after restart.
    FILE* f = fopen(meta_path_.c_str(), "wb");
    if (!f) throw std::runtime_error("wal: cannot open meta.dat for write");
    std::vector<uint8_t> buf;
    put_u64(buf, term_);
    put_u64(buf, static_cast<uint64_t>(voted_for_));
    fwrite(buf.data(), 1, buf.size(), f);
    fflush(f);
    fsync(fileno(f));
    fclose(f);
}

// ---------------------------------------------------------------------
// log replay + append
// ---------------------------------------------------------------------

void Wal::replay_wal() {
    FILE* f = fopen(wal_path_.c_str(), "rb");
    if (!f) return; // no wal yet — fresh node
    log_.clear();

    std::vector<uint8_t> all;
    uint8_t chunk[4096];
    size_t n;
    while ((n = fread(chunk, 1, sizeof(chunk), f)) > 0) {
        all.insert(all.end(), chunk, chunk + n);
    }
    fclose(f);

    ByteReader r(all.data(), all.size());
    while (!r.eof()) {
        // A crash mid-write can leave a truncated trailing record; stop
        // replay at the first record we can't fully decode rather than
        // treating it as corruption of everything before it.
        size_t start = r.pos;
        try {
            LogEntry e;
            e.term = r.get_u64();
            e.index = r.get_u64();
            e.cmd = static_cast<CmdType>(r.get_u8());
            e.key = r.get_str();
            e.value = r.get_str();
            log_.push_back(std::move(e));
        } catch (const std::out_of_range&) {
            r.pos = start; // rewind; leave the partial tail out
            break;
        }
    }
}

void Wal::append(const LogEntry& e) {
    std::lock_guard<std::mutex> lock(mu_);
    FILE* f = fopen(wal_path_.c_str(), "ab");
    if (!f) throw std::runtime_error("wal: cannot open wal.log for append");
    auto buf = encode_entry(e);
    fwrite(buf.data(), 1, buf.size(), f);
    fflush(f);
    fsync(fileno(f)); // durability point: entry survives a crash from here on
    fclose(f);
    log_.push_back(e);
}

void Wal::append_batch(const std::vector<LogEntry>& entries) {
    if (entries.empty()) return;
    std::lock_guard<std::mutex> lock(mu_);
    FILE* f = fopen(wal_path_.c_str(), "ab");
    if (!f) throw std::runtime_error("wal: cannot open wal.log for append");
    for (const auto& e : entries) {
        auto buf = encode_entry(e);
        fwrite(buf.data(), 1, buf.size(), f);
    }
    fflush(f);
    fsync(fileno(f)); // one fsync for the whole batch — this is why leaders
                       // batch AppendEntries instead of sending one at a time
    fclose(f);
    log_.insert(log_.end(), entries.begin(), entries.end());
}

void Wal::truncate_from(uint64_t from_index) {
    std::lock_guard<std::mutex> lock(mu_);
    uint64_t base = snap_meta_.last_included_index;
    if (from_index <= base) {
        // Truncating into/before the snapshot should never happen in a
        // correct Raft implementation (snapshotted entries are
        // committed and immutable) — guard against it defensively.
        return;
    }
    size_t keep = static_cast<size_t>(from_index - base - 1);
    if (keep < log_.size()) log_.resize(keep);
    rewrite_wal_locked();
}

void Wal::rewrite_wal_locked() {
    // Rewrite wal.log from the in-memory log_ (used after truncation
    // and after compaction). Written to a temp file + rename so a
    // crash mid-rewrite can never leave a half-written wal.log behind.
    std::string tmp = wal_path_ + ".tmp";
    FILE* f = fopen(tmp.c_str(), "wb");
    if (!f) throw std::runtime_error("wal: cannot open wal.log.tmp for rewrite");
    for (const auto& e : log_) {
        auto buf = encode_entry(e);
        fwrite(buf.data(), 1, buf.size(), f);
    }
    fflush(f);
    fsync(fileno(f));
    fclose(f);
    if (rename(tmp.c_str(), wal_path_.c_str()) != 0) {
        throw std::runtime_error("wal: rename(tmp, wal.log) failed during rewrite");
    }
}

uint64_t Wal::first_index() const {
    std::lock_guard<std::mutex> lock(mu_);
    return snap_meta_.last_included_index + 1;
}

uint64_t Wal::last_index() const {
    std::lock_guard<std::mutex> lock(mu_);
    if (!log_.empty()) return log_.back().index;
    return snap_meta_.last_included_index; // 0 if nothing at all
}

uint64_t Wal::last_term() const {
    std::lock_guard<std::mutex> lock(mu_);
    if (!log_.empty()) return log_.back().term;
    return snap_meta_.last_included_term;
}

// ---------------------------------------------------------------------
// snapshotting / compaction
// ---------------------------------------------------------------------

void Wal::load_snapshot_meta() {
    FILE* f = fopen(snap_path_.c_str(), "rb");
    if (!f) return;
    uint8_t buf[16];
    size_t n = fread(buf, 1, 16, f);
    fclose(f);
    if (n != 16) return;
    ByteReader r(buf, 16);
    snap_meta_.last_included_index = r.get_u64();
    snap_meta_.last_included_term = r.get_u64();
}

void Wal::write_snapshot(uint64_t last_included_index, uint64_t last_included_term,
                          const std::vector<uint8_t>& kv_blob) {
    std::lock_guard<std::mutex> lock(mu_);

    std::string tmp = snap_path_ + ".tmp";
    FILE* f = fopen(tmp.c_str(), "wb");
    if (!f) throw std::runtime_error("wal: cannot open snapshot.dat.tmp for write");
    std::vector<uint8_t> header;
    put_u64(header, last_included_index);
    put_u64(header, last_included_term);
    fwrite(header.data(), 1, header.size(), f);
    if (!kv_blob.empty()) fwrite(kv_blob.data(), 1, kv_blob.size(), f);
    fflush(f);
    fsync(fileno(f));
    fclose(f);
    if (rename(tmp.c_str(), snap_path_.c_str()) != 0) {
        throw std::runtime_error("wal: rename(tmp, snapshot.dat) failed");
    }

    snap_meta_.last_included_index = last_included_index;
    snap_meta_.last_included_term = last_included_term;

    // Compact: drop every log entry the snapshot now covers.
    size_t drop = 0;
    while (drop < log_.size() && log_[drop].index <= last_included_index) drop++;
    log_.erase(log_.begin(), log_.begin() + drop);
    rewrite_wal_locked();
}

std::vector<uint8_t> Wal::read_snapshot_blob() const {
    std::lock_guard<std::mutex> lock(mu_);
    FILE* f = fopen(snap_path_.c_str(), "rb");
    if (!f) return {};
    fseek(f, 16, SEEK_SET); // skip header
    std::vector<uint8_t> blob;
    uint8_t chunk[4096];
    size_t n;
    while ((n = fread(chunk, 1, sizeof(chunk), f)) > 0) blob.insert(blob.end(), chunk, chunk + n);
    fclose(f);
    return blob;
}

} // namespace qkv
