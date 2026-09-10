#include "client.hpp"
#include "querykv/net.hpp"
#include "querykv/util.hpp"
#include <sstream>
#include <thread>
#include <chrono>
#include <unistd.h>
#include <stdexcept>

namespace qkv {

std::vector<ClientPeer> parse_client_cluster(const std::string& spec) {
    std::vector<ClientPeer> peers;
    std::stringstream ss(spec);
    std::string token;
    while (std::getline(ss, token, ',')) {
        if (token.empty()) continue;
        std::stringstream fs(token);
        std::string idf, shardf, hostf, portf;
        std::getline(fs, idf, ':');
        std::getline(fs, shardf, ':');
        std::getline(fs, hostf, ':');
        std::getline(fs, portf, ':');
        if (idf.empty() || shardf.empty() || hostf.empty() || portf.empty()) {
            throw std::runtime_error("client: malformed cluster entry '" + token +
                                      "' (expected id:shard:host:port)");
        }
        ClientPeer p;
        p.id = std::stoull(idf);
        p.shard = std::stoi(shardf);
        p.host = hostf;
        p.port = std::stoi(portf);
        peers.push_back(p);
    }
    return peers;
}

Client::Client(std::vector<ClientPeer> cluster, int num_shards)
    : cluster_(std::move(cluster)), router_(num_shards) {}

std::vector<ClientPeer> Client::nodes_for_shard(int shard) {
    std::vector<ClientPeer> v;
    for (auto& p : cluster_) if (p.shard == shard) v.push_back(p);
    return v;
}

ClientPeer* Client::find_node_in(std::vector<ClientPeer>& v, uint64_t id) {
    for (auto& p : v) if (p.id == id) return &p;
    return nullptr;
}

Client::RawResult Client::send_with_retry(int shard, OpCode op, const std::vector<uint8_t>& payload,
                                             int max_attempts) {
    auto candidates = nodes_for_shard(shard);
    if (candidates.empty()) return {};

    ClientPeer target = candidates[0];
    auto known = known_leader_.find(shard);
    if (known != known_leader_.end()) {
        if (auto* p = find_node_in(candidates, known->second)) target = *p;
    }

    for (int attempt = 0; attempt < max_attempts; attempt++) {
        int fd = tcp_connect(target.host, target.port, 400);
        if (fd >= 0) {
            try {
                send_frame(fd, op, payload);
                auto frame = recv_frame(fd);
                ::close(fd);
                if (frame) {
                    if (frame->op == OpCode::CLIENT_RESP_REDIRECT) {
                        ByteReader r(frame->payload.data(), frame->payload.size());
                        uint64_t leader_id = r.get_u64();
                        std::string host = r.get_str();
                        uint32_t port = r.get_u32();
                        known_leader_[shard] = leader_id;
                        if (auto* p = find_node_in(candidates, leader_id)) {
                            target = *p;
                        } else {
                            target = ClientPeer{leader_id, shard, host, static_cast<int>(port)};
                        }
                        continue; // retry against the real leader
                    }
                    if (frame->op == OpCode::CLIENT_RESP_NOLEADER) {
                        target = candidates[static_cast<size_t>(attempt + 1) % candidates.size()];
                        std::this_thread::sleep_for(std::chrono::milliseconds(250));
                        continue;
                    }
                    known_leader_[shard] = target.id;
                    RawResult res;
                    res.reached = true;
                    res.op = frame->op;
                    res.payload = frame->payload;
                    return res;
                }
            } catch (...) {
                ::close(fd);
            }
        }
        target = candidates[static_cast<size_t>(attempt + 1) % candidates.size()];
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
    }
    return {};
}

GetResult Client::get(const std::string& key) {
    int shard = router_.shard_for_key(key);
    std::vector<uint8_t> payload;
    put_str(payload, key);
    auto raw = send_with_retry(shard, OpCode::CLIENT_GET, payload);

    GetResult r;
    r.reached_cluster = raw.reached;
    if (!raw.reached) return r;
    if (raw.op == OpCode::CLIENT_RESP_OK) {
        ByteReader br(raw.payload.data(), raw.payload.size());
        r.found = true;
        r.value = br.get_str();
    }
    return r;
}

WriteResult Client::put(const std::string& key, const std::string& value) {
    int shard = router_.shard_for_key(key);
    std::vector<uint8_t> payload;
    put_str(payload, key);
    put_str(payload, value);
    auto raw = send_with_retry(shard, OpCode::CLIENT_PUT, payload);
    return WriteResult{raw.reached && raw.op == OpCode::CLIENT_RESP_OK};
}

WriteResult Client::del(const std::string& key) {
    int shard = router_.shard_for_key(key);
    std::vector<uint8_t> payload;
    put_str(payload, key);
    auto raw = send_with_retry(shard, OpCode::CLIENT_DEL, payload);
    return WriteResult{raw.reached && raw.op == OpCode::CLIENT_RESP_OK};
}

std::string Client::cluster_status() {
    std::ostringstream oss;
    for (auto& p : cluster_) {
        oss << "--- node " << p.id << " (shard " << p.shard << ", " << p.host << ":" << p.port << ") ---\n";
        int fd = tcp_connect(p.host, p.port, 400);
        if (fd < 0) {
            oss << "  UNREACHABLE\n";
            continue;
        }
        try {
            send_frame(fd, OpCode::CLIENT_STATUS_REQ, {});
            auto frame = recv_frame(fd);
            ::close(fd);
            if (frame && frame->op == OpCode::CLIENT_STATUS_RESP) {
                ByteReader r(frame->payload.data(), frame->payload.size());
                oss << "  " << r.get_str();
            } else {
                oss << "  no response\n";
            }
        } catch (...) {
            oss << "  UNREACHABLE (error mid-request)\n";
        }
    }
    return oss.str();
}

} // namespace qkv
