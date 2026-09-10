#include "querykv/gossip.hpp"
#include "querykv/protocol.hpp"
#include "querykv/net.hpp"
#include <random>
#include <algorithm>
#include <unistd.h>

namespace qkv {

namespace {
constexpr int kGossipIntervalMs = 500;
constexpr int kDirectTimeoutMs = 250;
constexpr int kIndirectTimeoutMs = 350;
constexpr int kIndirectHelpers = 2;
constexpr long long kSuspectTimeoutMs = 3000;

std::mt19937& rng() {
    static thread_local std::mt19937 r(std::random_device{}());
    return r;
}

std::vector<Member> decode_digest(const std::vector<uint8_t>& payload, uint64_t* sender_out) {
    ByteReader r(payload.data(), payload.size());
    uint64_t sender = r.get_u64();
    if (sender_out) *sender_out = sender;
    uint32_t n = r.get_u32();
    std::vector<Member> v;
    v.reserve(n);
    for (uint32_t i = 0; i < n; i++) {
        Member m;
        m.id = r.get_u64();
        m.state = static_cast<MemberState>(r.get_u8());
        m.incarnation = r.get_u64();
        v.push_back(m);
    }
    return v;
}
} // namespace

Gossip::Gossip(uint64_t self_id, std::string tag, std::vector<PeerInfo> all_members)
    : self_id_(self_id), tag_(std::move(tag)) {
    for (auto& p : all_members) {
        Member m;
        m.id = p.id; m.host = p.host; m.port = p.port;
        m.state = MemberState::ALIVE; m.incarnation = 0; m.last_change_ms = now_ms();
        members_[p.id] = m;
    }
}

void Gossip::start() {
    running_ = true;
    loop_thread_ = std::thread(&Gossip::loop, this);
}
void Gossip::stop() {
    running_ = false;
    if (loop_thread_.joinable()) loop_thread_.join();
}

std::vector<Member> Gossip::snapshot() {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<Member> v;
    v.reserve(members_.size());
    for (auto& [id, m] : members_) v.push_back(m);
    return v;
}

std::vector<uint8_t> Gossip::encode_digest_locked() {
    std::vector<uint8_t> b;
    put_u64(b, self_id_);
    put_u32(b, static_cast<uint32_t>(members_.size()));
    for (auto& [id, m] : members_) {
        put_u64(b, m.id);
        put_u8(b, static_cast<uint8_t>(m.state));
        put_u64(b, m.incarnation);
    }
    return b;
}

void Gossip::mark_locked(uint64_t id, MemberState st, uint64_t incarnation) {
    auto it = members_.find(id);
    if (it == members_.end()) return;
    if (it->second.state != st || it->second.incarnation != incarnation) {
        it->second.state = st;
        it->second.incarnation = incarnation;
        it->second.last_change_ms = now_ms();
        if (st != MemberState::ALIVE) {
            QKV_LOG(tag_, LogLevel::WARN, "gossip: node " << id << " -> "
                    << (st == MemberState::SUSPECT ? "SUSPECT" : "DEAD"));
        } else {
            QKV_LOG(tag_, LogLevel::INFO, "gossip: node " << id << " -> ALIVE (incarnation "
                    << incarnation << ")");
        }
    }
}

void Gossip::merge_entries_locked(const std::vector<Member>& incoming) {
    for (const auto& in : incoming) {
        if (in.id == self_id_) {
            // Someone thinks WE are suspect/dead. Refute by bumping our
            // own incarnation past theirs and re-announcing ALIVE — this
            // is the SWIM "refutation" mechanism that stops a transient
            // GC pause / slow link from cascading into a false eviction.
            auto& me = members_[self_id_];
            if (in.state != MemberState::ALIVE && in.incarnation >= me.incarnation) {
                me.incarnation = in.incarnation + 1;
                me.state = MemberState::ALIVE;
                me.last_change_ms = now_ms();
                QKV_LOG(tag_, LogLevel::INFO, "gossip: refuting suspicion, incarnation -> "
                        << me.incarnation);
            }
            continue;
        }
        auto it = members_.find(in.id);
        if (it == members_.end()) continue; // unknown id — static membership, ignore
        Member& local = it->second;
        auto severity = [](MemberState s) { return static_cast<int>(s); };
        if (in.incarnation > local.incarnation ||
            (in.incarnation == local.incarnation && severity(in.state) > severity(local.state))) {
            mark_locked(in.id, in.state, in.incarnation);
        }
    }
}

void Gossip::reap_suspects_locked() {
    long long t = now_ms();
    for (auto& [id, m] : members_) {
        if (m.state == MemberState::SUSPECT && t - m.last_change_ms > kSuspectTimeoutMs) {
            mark_locked(id, MemberState::DEAD, m.incarnation);
        }
    }
}

std::vector<uint8_t> Gossip::handle_ping(const std::vector<uint8_t>& payload) {
    uint64_t sender;
    auto incoming = decode_digest(payload, &sender);
    std::lock_guard<std::mutex> lock(mu_);
    // The mere fact we received a ping proves the sender is alive.
    Member senderUpdate; senderUpdate.id = sender; senderUpdate.state = MemberState::ALIVE;
    auto it = members_.find(sender);
    senderUpdate.incarnation = it != members_.end() ? it->second.incarnation : 0;
    merge_entries_locked(incoming);
    mark_locked(sender, MemberState::ALIVE, senderUpdate.incarnation);
    return encode_digest_locked();
}

std::vector<uint8_t> Gossip::handle_ping_req(const std::vector<uint8_t>& payload) {
    ByteReader r(payload.data(), payload.size());
    uint64_t requester_id = r.get_u64();
    uint64_t target_id = r.get_u64();
    (void)requester_id;

    Member target;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = members_.find(target_id);
        if (it == members_.end()) {
            std::vector<uint8_t> resp; put_u8(resp, 0); return resp;
        }
        target = it->second;
    }
    bool ok = direct_ping(target, kDirectTimeoutMs);
    std::vector<uint8_t> resp;
    put_u8(resp, ok ? 1 : 0);
    return resp;
}

bool Gossip::direct_ping(const Member& target, int timeout_ms) {
    int fd = tcp_connect(target.host, target.port, timeout_ms);
    if (fd < 0) return false;
    std::vector<uint8_t> digest;
    { std::lock_guard<std::mutex> lock(mu_); digest = encode_digest_locked(); }
    try {
        send_frame(fd, OpCode::GOSSIP_PING, digest);
        auto frame = recv_frame(fd);
        ::close(fd);
        if (!frame) return false;
        auto incoming = decode_digest(frame->payload, nullptr);
        std::lock_guard<std::mutex> lock(mu_);
        merge_entries_locked(incoming);
        return true;
    } catch (...) {
        ::close(fd);
        return false;
    }
}

bool Gossip::indirect_ping(const Member& target, int helpers, int timeout_ms) {
    std::vector<Member> candidates;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (auto& [id, m] : members_) {
            if (id != self_id_ && id != target.id && m.state != MemberState::DEAD) {
                candidates.push_back(m);
            }
        }
    }
    std::shuffle(candidates.begin(), candidates.end(), rng());
    if (static_cast<int>(candidates.size()) > helpers) candidates.resize(helpers);

    for (auto& helper : candidates) {
        int fd = tcp_connect(helper.host, helper.port, timeout_ms);
        if (fd < 0) continue;
        try {
            std::vector<uint8_t> req;
            put_u64(req, self_id_);
            put_u64(req, target.id);
            send_frame(fd, OpCode::GOSSIP_PING_REQ, req);
            auto frame = recv_frame(fd);
            ::close(fd);
            if (frame && !frame->payload.empty() && frame->payload[0] == 1) return true;
        } catch (...) {
            ::close(fd);
        }
    }
    return false;
}

void Gossip::probe_once() {
    Member target;
    {
        std::lock_guard<std::mutex> lock(mu_);
        reap_suspects_locked();
        std::vector<uint64_t> candidates;
        for (auto& [id, m] : members_) if (id != self_id_) candidates.push_back(id);
        if (candidates.empty()) return;
        std::uniform_int_distribution<size_t> dist(0, candidates.size() - 1);
        target = members_[candidates[dist(rng())]];
    }

    if (direct_ping(target, kDirectTimeoutMs)) return;
    if (indirect_ping(target, kIndirectHelpers, kIndirectTimeoutMs)) return;

    std::lock_guard<std::mutex> lock(mu_);
    auto it = members_.find(target.id);
    if (it != members_.end() && it->second.state == MemberState::ALIVE) {
        mark_locked(target.id, MemberState::SUSPECT, it->second.incarnation);
    }
}

void Gossip::loop() {
    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kGossipIntervalMs));
        if (!running_) break;
        probe_once();
    }
}

} // namespace qkv
