#include "querykv/node.hpp"
#include "querykv/protocol.hpp"
#include "querykv/net.hpp"
#include <sstream>
#include <thread>
#include <chrono>
#include <unistd.h>
#include <arpa/inet.h>

namespace qkv {

namespace {
std::string make_tag(int shard, uint64_t id) {
    return "s" + std::to_string(shard) + "n" + std::to_string(id);
}
} // namespace

Node::Node(const Config& cfg)
    : cfg_(cfg),
      tag_(make_tag(cfg.shard, cfg.id)),
      wal_(cfg.data_dir),
      raft_(cfg.id, tag_, cfg.shard_peers, wal_, storage_),
      gossip_(cfg.id, tag_, cfg.cluster_members),
      router_(cfg.num_shards) {}

void Node::run() {
    raft_.start();
    gossip_.start();

    std::thread maint([this] {
        while (running_) {
            std::this_thread::sleep_for(std::chrono::seconds(5));
            raft_.maybe_snapshot(kSnapshotLogThreshold);
        }
    });
    maint.detach();

    int listen_fd = tcp_listen(cfg_.port);
    QKV_LOG(tag_, LogLevel::INFO, "listening on :" << cfg_.port
            << " shard=" << cfg_.shard << " id=" << cfg_.id
            << " shard_peers=" << cfg_.shard_peers.size()
            << " cluster_size=" << cfg_.cluster_members.size());

    while (running_) {
        sockaddr_in addr{};
        socklen_t len = sizeof(addr);
        int fd = ::accept(listen_fd, reinterpret_cast<sockaddr*>(&addr), &len);
        if (fd < 0) {
            if (!running_) break;
            continue;
        }
        std::thread(&Node::handle_connection, this, fd).detach();
    }
}

void Node::handle_connection(int fd) {
    try {
        auto frame = recv_frame(fd);
        if (!frame) { ::close(fd); return; }

        switch (frame->op) {
            case OpCode::RAFT_REQUEST_VOTE: {
                auto resp = raft_.handle_request_vote(frame->payload);
                send_frame(fd, OpCode::RAFT_REQUEST_VOTE_RESP, resp);
                break;
            }
            case OpCode::RAFT_APPEND_ENTRIES: {
                auto resp = raft_.handle_append_entries(frame->payload);
                send_frame(fd, OpCode::RAFT_APPEND_ENTRIES_RESP, resp);
                break;
            }
            case OpCode::GOSSIP_PING: {
                auto resp = gossip_.handle_ping(frame->payload);
                send_frame(fd, OpCode::GOSSIP_ACK, resp);
                break;
            }
            case OpCode::GOSSIP_PING_REQ: {
                auto resp = gossip_.handle_ping_req(frame->payload);
                send_frame(fd, OpCode::GOSSIP_ACK, resp);
                break;
            }
            case OpCode::CLIENT_GET:
                handle_client_get(fd, frame->payload);
                break;
            case OpCode::CLIENT_PUT:
                handle_client_put(fd, frame->payload);
                break;
            case OpCode::CLIENT_DEL:
                handle_client_del(fd, frame->payload);
                break;
            case OpCode::CLIENT_STATUS_REQ:
                handle_client_status(fd);
                break;
            default:
                break; // unknown opcode — drop silently, matches "be liberal in
                       // what you tolerate" for a wire protocol still evolving
        }
    } catch (const std::exception&) {
        // Peer disconnected mid-RPC, or sent a malformed frame. Routine
        // under node restarts / packet loss — not worth logging per-event
        // (the gossip/raft layers already log state transitions that
        // matter; a dropped one-shot connection on its own doesn't).
    }
    ::close(fd);
}

void Node::send_redirect_or_noleader(int fd) {
    auto hint = raft_.leader_hint();
    if (hint) {
        std::vector<uint8_t> resp;
        put_u64(resp, hint->id);
        put_str(resp, hint->host);
        put_u32(resp, static_cast<uint32_t>(hint->port));
        send_frame(fd, OpCode::CLIENT_RESP_REDIRECT, resp);
    } else {
        send_frame(fd, OpCode::CLIENT_RESP_NOLEADER, {});
    }
}

void Node::respond_propose_result(int fd, const ProposeResult& res) {
    if (res.ok) {
        send_frame(fd, OpCode::CLIENT_RESP_OK, {});
        return;
    }
    if (!res.is_leader) {
        if (res.leader_hint) {
            std::vector<uint8_t> resp;
            put_u64(resp, res.leader_hint->id);
            put_str(resp, res.leader_hint->host);
            put_u32(resp, static_cast<uint32_t>(res.leader_hint->port));
            send_frame(fd, OpCode::CLIENT_RESP_REDIRECT, resp);
        } else {
            send_frame(fd, OpCode::CLIENT_RESP_NOLEADER, {});
        }
        return;
    }
    // Was leader when we proposed but timed out waiting for commit —
    // most likely a mid-flight leadership change or a stalled
    // replication round. Tell the client to retry rather than lie
    // about success.
    send_frame(fd, OpCode::CLIENT_RESP_NOLEADER, {});
}

void Node::handle_client_get(int fd, const std::vector<uint8_t>& payload) {
    ByteReader r(payload.data(), payload.size());
    std::string key = r.get_str();

    if (raft_.role() != Role::LEADER) {
        send_redirect_or_noleader(fd);
        return;
    }
    auto val = storage_.get(key);
    if (val) {
        std::vector<uint8_t> resp;
        put_str(resp, *val);
        send_frame(fd, OpCode::CLIENT_RESP_OK, resp);
    } else {
        send_frame(fd, OpCode::CLIENT_RESP_NOTFOUND, {});
    }
}

void Node::handle_client_put(int fd, const std::vector<uint8_t>& payload) {
    ByteReader r(payload.data(), payload.size());
    std::string key = r.get_str();
    std::string value = r.get_str();
    auto res = raft_.propose(CmdType::PUT, key, value);
    respond_propose_result(fd, res);
}

void Node::handle_client_del(int fd, const std::vector<uint8_t>& payload) {
    ByteReader r(payload.data(), payload.size());
    std::string key = r.get_str();
    auto res = raft_.propose(CmdType::DEL, key, "");
    respond_propose_result(fd, res);
}

void Node::handle_client_status(int fd) {
    std::ostringstream oss;
    oss << "shard=" << cfg_.shard << " " << raft_.status_line() << "\n";
    oss << "gossip view:\n";
    for (auto& m : gossip_.snapshot()) {
        const char* st = m.state == MemberState::ALIVE ? "ALIVE"
                        : m.state == MemberState::SUSPECT ? "SUSPECT" : "DEAD";
        oss << "  node " << m.id << " (" << m.host << ":" << m.port << ") = " << st
            << " incarnation=" << m.incarnation << "\n";
    }
    std::vector<uint8_t> resp;
    put_str(resp, oss.str());
    send_frame(fd, OpCode::CLIENT_STATUS_RESP, resp);
}

} // namespace qkv
