#include "querykv/raft.hpp"
#include "querykv/protocol.hpp"
#include "querykv/net.hpp"
#include <algorithm>
#include <random>
#include <sstream>
#include <unistd.h>

namespace qkv {

// ---------------------------------------------------------------------
// RPC encode / decode
// ---------------------------------------------------------------------

std::vector<uint8_t> encode_request_vote_req(const RequestVoteReq& r) {
    std::vector<uint8_t> b;
    put_u64(b, r.term);
    put_u64(b, r.candidate_id);
    put_u64(b, r.last_log_index);
    put_u64(b, r.last_log_term);
    return b;
}
RequestVoteReq decode_request_vote_req(const std::vector<uint8_t>& p) {
    ByteReader r(p.data(), p.size());
    RequestVoteReq req;
    req.term = r.get_u64();
    req.candidate_id = r.get_u64();
    req.last_log_index = r.get_u64();
    req.last_log_term = r.get_u64();
    return req;
}
std::vector<uint8_t> encode_request_vote_resp(const RequestVoteResp& r) {
    std::vector<uint8_t> b;
    put_u64(b, r.term);
    put_u8(b, r.vote_granted ? 1 : 0);
    return b;
}
RequestVoteResp decode_request_vote_resp(const std::vector<uint8_t>& p) {
    ByteReader r(p.data(), p.size());
    RequestVoteResp resp;
    resp.term = r.get_u64();
    resp.vote_granted = r.get_u8() != 0;
    return resp;
}

static void encode_entry_into(std::vector<uint8_t>& b, const LogEntry& e) {
    put_u64(b, e.term);
    put_u64(b, e.index);
    put_u8(b, static_cast<uint8_t>(e.cmd));
    put_str(b, e.key);
    put_str(b, e.value);
}
static LogEntry decode_entry_from(ByteReader& r) {
    LogEntry e;
    e.term = r.get_u64();
    e.index = r.get_u64();
    e.cmd = static_cast<CmdType>(r.get_u8());
    e.key = r.get_str();
    e.value = r.get_str();
    return e;
}

std::vector<uint8_t> encode_append_entries_req(const AppendEntriesReq& r) {
    std::vector<uint8_t> b;
    put_u64(b, r.term);
    put_u64(b, r.leader_id);
    put_u64(b, r.prev_log_index);
    put_u64(b, r.prev_log_term);
    put_u64(b, r.leader_commit);
    put_u32(b, static_cast<uint32_t>(r.entries.size()));
    for (const auto& e : r.entries) encode_entry_into(b, e);
    return b;
}
AppendEntriesReq decode_append_entries_req(const std::vector<uint8_t>& p) {
    ByteReader r(p.data(), p.size());
    AppendEntriesReq req;
    req.term = r.get_u64();
    req.leader_id = r.get_u64();
    req.prev_log_index = r.get_u64();
    req.prev_log_term = r.get_u64();
    req.leader_commit = r.get_u64();
    uint32_t n = r.get_u32();
    req.entries.reserve(n);
    for (uint32_t i = 0; i < n; i++) req.entries.push_back(decode_entry_from(r));
    return req;
}
std::vector<uint8_t> encode_append_entries_resp(const AppendEntriesResp& r) {
    std::vector<uint8_t> b;
    put_u64(b, r.term);
    put_u8(b, r.success ? 1 : 0);
    put_u64(b, r.conflict_index);
    return b;
}
AppendEntriesResp decode_append_entries_resp(const std::vector<uint8_t>& p) {
    ByteReader r(p.data(), p.size());
    AppendEntriesResp resp;
    resp.term = r.get_u64();
    resp.success = r.get_u8() != 0;
    resp.conflict_index = r.get_u64();
    return resp;
}

// ---------------------------------------------------------------------
// RaftNode
// ---------------------------------------------------------------------

RaftNode::RaftNode(uint64_t self_id, std::string log_tag, std::vector<PeerInfo> peers,
                     Wal& wal, Storage& storage)
    : self_id_(self_id), tag_(std::move(log_tag)), peers_(std::move(peers)),
      wal_(wal), storage_(storage) {}

long long RaftNode::random_election_timeout_ms() {
    static thread_local std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> dist(750, 1500);
    return dist(rng);
}

uint64_t RaftNode::last_log_index_locked() { return wal_.last_index(); }
uint64_t RaftNode::last_log_term_locked() { return wal_.last_term(); }

std::optional<LogEntry> RaftNode::entry_at_locked(uint64_t index) {
    uint64_t first = wal_.first_index();
    uint64_t last = wal_.last_index();
    if (index < first || index > last) return std::nullopt;
    const auto& entries = wal_.entries();
    size_t pos = static_cast<size_t>(index - first);
    if (pos >= entries.size()) return std::nullopt;
    return entries[pos];
}

std::optional<PeerInfo> RaftNode::leader_hint_locked() {
    if (!voted_leader_hint_id_) return std::nullopt;
    if (*voted_leader_hint_id_ == self_id_) return std::nullopt; // caller should already know role()==LEADER
    for (auto& p : peers_) if (p.id == *voted_leader_hint_id_) return p;
    return std::nullopt;
}

void RaftNode::start() {
    running_ = true;
    {
        std::lock_guard<std::mutex> lock(mu_);
        last_heard_from_leader_ms_ = now_ms();
        election_timeout_ms_ = random_election_timeout_ms();
        auto snap = wal_.snapshot_meta();
        commit_index_ = snap.last_included_index;
        last_applied_ = snap.last_included_index;
        if (wal_.has_snapshot()) storage_.restore(wal_.read_snapshot_blob());
    }
    QKV_LOG(tag_, LogLevel::INFO, "raft started: term=" << wal_.current_term()
            << " log_len=" << wal_.entries().size()
            << " snapshot_idx=" << wal_.snapshot_meta().last_included_index);
    election_thread_ = std::thread(&RaftNode::election_timer_loop, this);
    apply_thread_ = std::thread(&RaftNode::apply_loop, this);
}

void RaftNode::stop() {
    running_ = false;
    cv_commit_.notify_all();
    if (election_thread_.joinable()) election_thread_.join();
    if (apply_thread_.joinable()) apply_thread_.join();
}

Role RaftNode::role() {
    std::lock_guard<std::mutex> lock(mu_);
    return role_;
}
uint64_t RaftNode::current_term() { return wal_.current_term(); }

std::optional<PeerInfo> RaftNode::leader_hint() {
    std::lock_guard<std::mutex> lock(mu_);
    return leader_hint_locked();
}

void RaftNode::election_timer_loop() {
    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        if (!running_) break;
        bool start_election = false;
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (role_ == Role::LEADER) {
                last_heard_from_leader_ms_ = now_ms();
            } else if (now_ms() - last_heard_from_leader_ms_ > election_timeout_ms_) {
                start_election = true;
            }
        }
        if (start_election) become_candidate();
    }
}

void RaftNode::apply_loop() {
    while (running_) {
        LogEntry e;
        bool have = false;
        {
            std::unique_lock<std::mutex> lock(mu_);
            cv_commit_.wait_for(lock, std::chrono::milliseconds(200),
                [this] { return !running_.load() || commit_index_ > last_applied_; });
            if (!running_) break;
            if (commit_index_ > last_applied_) {
                auto opt = entry_at_locked(last_applied_ + 1);
                if (opt) { e = *opt; have = true; }
                else { last_applied_++; } // covered by a snapshot already — nothing to apply
            }
        }
        if (have) {
            storage_.apply(e);
            std::lock_guard<std::mutex> lock(mu_);
            last_applied_ = e.index;
        }
    }
}

void RaftNode::become_follower_locked(uint64_t new_term) {
    if (new_term > wal_.current_term()) {
        wal_.save_term_and_vote(new_term, -1);
    }
    if (role_ != Role::FOLLOWER) {
        QKV_LOG(tag_, LogLevel::INFO, "-> FOLLOWER (term " << new_term << ")");
    }
    role_ = Role::FOLLOWER;
    leader_epoch_++; // strands any still-running replicator threads from this leadership
}

void RaftNode::become_leader() {
    role_ = Role::LEADER;
    uint64_t epoch = ++leader_epoch_;
    uint64_t next = last_log_index_locked() + 1;
    next_index_.clear();
    match_index_.clear();
    for (auto& p : peers_) { next_index_[p.id] = next; match_index_[p.id] = 0; }
    voted_leader_hint_id_ = self_id_;
    last_heard_from_leader_ms_ = now_ms();

    // No-op entry committed right away: lets us advance commit_index_
    // over older-term entries once it's replicated (Raft §5.4.2),
    // instead of stalling until the next real client write.
    LogEntry noop;
    noop.term = wal_.current_term();
    noop.index = last_log_index_locked() + 1;
    noop.cmd = CmdType::NOOP;
    wal_.append(noop);
    match_index_[self_id_] = noop.index;

    QKV_LOG(tag_, LogLevel::INFO, "*** became LEADER for term " << wal_.current_term()
            << " (epoch " << epoch << ") ***");

    for (auto& p : peers_) {
        std::thread(&RaftNode::replicator_loop, this, p, epoch).detach();
    }
}

void RaftNode::become_candidate() {
    uint64_t term, last_log_idx, last_log_term;
    std::vector<PeerInfo> peers_copy;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (role_ == Role::LEADER) return;
        role_ = Role::CANDIDATE;
        term = wal_.current_term() + 1;
        wal_.save_term_and_vote(term, static_cast<int64_t>(self_id_));
        last_log_idx = last_log_index_locked();
        last_log_term = last_log_term_locked();
        peers_copy = peers_;
        election_timeout_ms_ = random_election_timeout_ms();
        last_heard_from_leader_ms_ = now_ms();
    }
    QKV_LOG(tag_, LogLevel::INFO, "-> CANDIDATE for term " << term);

    std::atomic<int> votes{1}; // vote for self
    std::vector<std::thread> workers;
    int majority = static_cast<int>(peers_copy.size() + 1) / 2 + 1;

    for (auto& p : peers_copy) {
        workers.emplace_back([this, p, term, last_log_idx, last_log_term, &votes]() {
            RequestVoteReq req{term, self_id_, last_log_idx, last_log_term};
            int fd = tcp_connect(p.host, p.port, 300);
            if (fd < 0) return;
            try {
                send_frame(fd, OpCode::RAFT_REQUEST_VOTE, encode_request_vote_req(req));
                auto frame = recv_frame(fd);
                ::close(fd);
                if (!frame) return;
                auto resp = decode_request_vote_resp(frame->payload);
                std::lock_guard<std::mutex> lock(mu_);
                if (resp.term > wal_.current_term()) {
                    become_follower_locked(resp.term);
                    return;
                }
                if (resp.vote_granted && role_ == Role::CANDIDATE && resp.term == term) {
                    votes++;
                }
            } catch (...) {
                ::close(fd);
            }
        });
    }
    for (auto& t : workers) t.join();

    std::lock_guard<std::mutex> lock(mu_);
    if (role_ == Role::CANDIDATE && wal_.current_term() == term && votes.load() >= majority) {
        become_leader();
    } else {
        QKV_LOG(tag_, LogLevel::INFO, "election for term " << term << " did not win ("
                << votes.load() << "/" << majority << " needed)");
    }
}

void RaftNode::replicator_loop(PeerInfo peer, uint64_t my_epoch) {
    for (;;) {
        if (!running_ || leader_epoch_.load() != my_epoch) return;

        AppendEntriesReq req;
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (role_ != Role::LEADER || leader_epoch_.load() != my_epoch) return;
            uint64_t ni = next_index_[peer.id];
            uint64_t prev_index = ni > 0 ? ni - 1 : 0;
            uint64_t prev_term = 0;
            auto snap = wal_.snapshot_meta();
            if (prev_index == snap.last_included_index) {
                prev_term = snap.last_included_term;
            } else if (prev_index > 0) {
                auto e = entry_at_locked(prev_index);
                prev_term = e ? e->term : 0;
            }
            req.term = wal_.current_term();
            req.leader_id = self_id_;
            req.prev_log_index = prev_index;
            req.prev_log_term = prev_term;
            req.leader_commit = commit_index_;
            uint64_t last = last_log_index_locked();
            for (uint64_t idx = ni; idx <= last; idx++) {
                auto e = entry_at_locked(idx);
                if (e) req.entries.push_back(*e);
            }
        }

        int fd = tcp_connect(peer.host, peer.port, 300);
        bool got_resp = false;
        AppendEntriesResp resp{};
        if (fd >= 0) {
            try {
                send_frame(fd, OpCode::RAFT_APPEND_ENTRIES, encode_append_entries_req(req));
                auto frame = recv_frame(fd);
                ::close(fd);
                if (frame) { resp = decode_append_entries_resp(frame->payload); got_resp = true; }
            } catch (...) {
                // peer unreachable / dropped mid-RPC — treated as a routine
                // retry-next-tick condition, not an error worth logging loudly
            }
        }

        if (got_resp) {
            std::lock_guard<std::mutex> lock(mu_);
            if (leader_epoch_.load() != my_epoch || role_ != Role::LEADER) return;
            if (resp.term > wal_.current_term()) {
                become_follower_locked(resp.term);
                return;
            }
            if (resp.success) {
                uint64_t new_match = req.prev_log_index + req.entries.size();
                if (new_match > match_index_[peer.id]) match_index_[peer.id] = new_match;
                next_index_[peer.id] = new_match + 1;
                advance_commit_index_locked();
            } else {
                uint64_t ci = resp.conflict_index;
                uint64_t cur = next_index_[peer.id];
                next_index_[peer.id] = (ci > 0 && ci < cur) ? ci : (cur > 1 ? cur - 1 : 1);
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(got_resp && resp.success ? 200 : 150));
    }
}

void RaftNode::advance_commit_index_locked() {
    std::vector<uint64_t> matches;
    matches.push_back(match_index_[self_id_]);
    for (auto& p : peers_) matches.push_back(match_index_[p.id]);
    std::sort(matches.begin(), matches.end());
    size_t n = matches.size();
    size_t majority = n / 2 + 1;
    uint64_t candidate = matches[n - majority];
    if (candidate > commit_index_) {
        auto e = entry_at_locked(candidate);
        if (e && e->term == wal_.current_term()) {
            commit_index_ = candidate;
            cv_commit_.notify_all();
        }
    }
}

ProposeResult RaftNode::propose(CmdType cmd, const std::string& key, const std::string& value,
                                   int wait_ms) {
    uint64_t my_index;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (role_ != Role::LEADER) {
            ProposeResult r;
            r.ok = false; r.is_leader = false; r.leader_hint = leader_hint_locked();
            return r;
        }
        LogEntry e;
        e.term = wal_.current_term();
        e.index = last_log_index_locked() + 1;
        e.cmd = cmd; e.key = key; e.value = value;
        wal_.append(e);
        my_index = e.index;
        match_index_[self_id_] = my_index;
    }

    std::unique_lock<std::mutex> lock(mu_);
    bool ready = cv_commit_.wait_for(lock, std::chrono::milliseconds(wait_ms), [this, my_index] {
        return !running_.load() || role_ != Role::LEADER || last_applied_ >= my_index;
    });

    ProposeResult r;
    if (!ready) { r.ok = false; r.timed_out = true; r.is_leader = true; return r; }
    if (role_ != Role::LEADER && last_applied_ < my_index) {
        r.ok = false; r.is_leader = false; r.leader_hint = leader_hint_locked(); return r;
    }
    r.ok = (last_applied_ >= my_index);
    return r;
}

std::vector<uint8_t> RaftNode::handle_request_vote(const std::vector<uint8_t>& payload) {
    auto req = decode_request_vote_req(payload);
    std::lock_guard<std::mutex> lock(mu_);
    RequestVoteResp resp;
    uint64_t cur_term = wal_.current_term();

    if (req.term < cur_term) {
        resp.term = cur_term; resp.vote_granted = false;
        return encode_request_vote_resp(resp);
    }
    if (req.term > cur_term) {
        become_follower_locked(req.term);
        cur_term = req.term;
    }

    int64_t voted = wal_.voted_for();
    uint64_t my_last_term = last_log_term_locked();
    uint64_t my_last_index = last_log_index_locked();
    bool log_ok = (req.last_log_term != my_last_term)
                      ? (req.last_log_term > my_last_term)
                      : (req.last_log_index >= my_last_index);

    if ((voted == -1 || voted == static_cast<int64_t>(req.candidate_id)) && log_ok) {
        wal_.save_term_and_vote(cur_term, static_cast<int64_t>(req.candidate_id));
        last_heard_from_leader_ms_ = now_ms();
        resp.term = cur_term; resp.vote_granted = true;
        QKV_LOG(tag_, LogLevel::INFO, "voted for " << req.candidate_id << " (term " << cur_term << ")");
    } else {
        resp.term = cur_term; resp.vote_granted = false;
    }
    return encode_request_vote_resp(resp);
}

std::vector<uint8_t> RaftNode::handle_append_entries(const std::vector<uint8_t>& payload) {
    auto req = decode_append_entries_req(payload);
    std::lock_guard<std::mutex> lock(mu_);
    AppendEntriesResp resp;
    uint64_t cur_term = wal_.current_term();

    if (req.term < cur_term) {
        resp.term = cur_term; resp.success = false; resp.conflict_index = 0;
        return encode_append_entries_resp(resp);
    }
    if (req.term > cur_term) {
        become_follower_locked(req.term);
        cur_term = req.term;
    } else if (role_ != Role::FOLLOWER) {
        role_ = Role::FOLLOWER; // a same-term leader/candidate defers to the real leader
    }
    last_heard_from_leader_ms_ = now_ms();
    voted_leader_hint_id_ = req.leader_id;

    auto snap = wal_.snapshot_meta();
    if (req.prev_log_index > 0) {
        if (req.prev_log_index == snap.last_included_index) {
            if (req.prev_log_term != snap.last_included_term) {
                resp.term = cur_term; resp.success = false;
                resp.conflict_index = snap.last_included_index; // caught by the no-InstallSnapshot scope cut
                return encode_append_entries_resp(resp);
            }
        } else if (req.prev_log_index > last_log_index_locked()) {
            resp.term = cur_term; resp.success = false;
            resp.conflict_index = last_log_index_locked() + 1;
            return encode_append_entries_resp(resp);
        } else if (req.prev_log_index < snap.last_included_index) {
            resp.term = cur_term; resp.success = false;
            resp.conflict_index = snap.last_included_index + 1;
            return encode_append_entries_resp(resp);
        } else {
            auto prevEntry = entry_at_locked(req.prev_log_index);
            if (!prevEntry || prevEntry->term != req.prev_log_term) {
                uint64_t ci = req.prev_log_index;
                if (prevEntry) {
                    uint64_t conflict_term = prevEntry->term;
                    while (ci > snap.last_included_index + 1) {
                        auto e2 = entry_at_locked(ci - 1);
                        if (e2 && e2->term == conflict_term) ci--; else break;
                    }
                }
                resp.term = cur_term; resp.success = false; resp.conflict_index = ci;
                return encode_append_entries_resp(resp);
            }
        }
    }

    uint64_t base_index = req.prev_log_index;
    for (size_t i = 0; i < req.entries.size(); i++) {
        uint64_t idx = base_index + 1 + i;
        auto existing = entry_at_locked(idx);
        if (existing) {
            if (existing->term != req.entries[i].term) {
                wal_.truncate_from(idx);
                std::vector<LogEntry> rest(req.entries.begin() + static_cast<long>(i), req.entries.end());
                wal_.append_batch(rest);
                break;
            }
            // already have this exact entry — nothing to do, keep scanning
        } else {
            std::vector<LogEntry> rest(req.entries.begin() + static_cast<long>(i), req.entries.end());
            wal_.append_batch(rest);
            break;
        }
    }

    if (req.leader_commit > commit_index_) {
        commit_index_ = std::min(req.leader_commit, last_log_index_locked());
        cv_commit_.notify_all();
    }

    resp.term = cur_term; resp.success = true; resp.conflict_index = 0;
    return encode_append_entries_resp(resp);
}

void RaftNode::maybe_snapshot(size_t log_size_threshold) {
    uint64_t idx; uint64_t term;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (wal_.entries().size() <= log_size_threshold) return;
        if (last_applied_ == 0) return;
        auto e = entry_at_locked(last_applied_);
        if (!e) return;
        idx = last_applied_;
        term = e->term;
    }
    auto blob = storage_.serialize();
    wal_.write_snapshot(idx, term, blob);
    QKV_LOG(tag_, LogLevel::INFO, "snapshot taken at index " << idx << ", log compacted");
}

std::string RaftNode::status_line() {
    std::lock_guard<std::mutex> lock(mu_);
    const char* r = role_ == Role::LEADER ? "LEADER" : role_ == Role::CANDIDATE ? "CANDIDATE" : "FOLLOWER";
    std::ostringstream oss;
    oss << "id=" << self_id_ << " role=" << r << " term=" << wal_.current_term()
        << " commit=" << commit_index_ << " applied=" << last_applied_
        << " log_len=" << wal_.entries().size()
        << " snap_idx=" << wal_.snapshot_meta().last_included_index;
    return oss.str();
}

} // namespace qkv
