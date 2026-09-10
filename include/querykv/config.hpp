#pragma once
// config.hpp — parses the tiny key=value config file each node is
// launched with (see config/*.conf). Deliberately not JSON/YAML: one
// fewer dependency, and the format only needs to express "who am I,
// which shard am I in, who are my peers".
//
// File format (one key=value per line, '#' comments, blank lines ok):
//   id=0                 global node id, unique across the cluster
//   shard=0               which shard (Raft group) this node belongs to
//   num_shards=2           total shard count (for the consistent-hash ring)
//   port=7000               TCP port this node listens on (binds 0.0.0.0)
//   data_dir=/data          WAL + snapshot directory for this node
//   peers=1:node1:7001,2:node2:7002        other members of MY shard
//   cluster=0:node0:7000,1:node1:7001,...  every node in the cluster (gossip)

#include <string>
#include <vector>
#include "raft.hpp" // PeerInfo

namespace qkv {

struct Config {
    uint64_t id = 0;
    int shard = 0;
    int num_shards = 1;
    int port = 7000;
    std::string data_dir = "./data";
    std::vector<PeerInfo> shard_peers;
    std::vector<PeerInfo> cluster_members;
};

Config parse_config_file(const std::string& path);

} // namespace qkv
