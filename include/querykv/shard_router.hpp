#pragma once
// shard_router.hpp — consistent hashing ring that maps a key to a
// shard id. Each shard is an independent Raft group (see raft.hpp);
// this is the layer above Raft that turns "3 separate 3-node
// consensus groups" into "one logical key-value store".
//
// Virtual nodes (many ring points per shard) are used so keys spread
// roughly evenly across shards even with a small shard count — with
// only 1 ring point per shard, hashing would be lumpy; 100+ virtual
// points per shard smooths that out. This is the same technique
// Cassandra/DynamoDB-style stores use, just applied to "shard id"
// instead of "physical node" since replication within a shard is
// already handled by Raft.
//
// Adding/removing a shard is NOT supported at runtime in this
// project (shard count is fixed at cluster-config time) — see the
// README's "explicit scope cuts" section for what a live-resharding
// design would need on top of this (data migration between Raft
// groups, a two-phase ownership handoff, etc).

#include <cstdint>
#include <string>
#include <vector>
#include <utility>

namespace qkv {

uint64_t fnv1a_hash(const std::string& s);

class ShardRouter {
public:
    explicit ShardRouter(int num_shards, int virtual_nodes_per_shard = 128);

    int shard_for_key(const std::string& key) const;
    int num_shards() const { return num_shards_; }

private:
    int num_shards_;
    std::vector<std::pair<uint64_t, int>> ring_; // sorted (hash, shard_id)
};

} // namespace qkv
