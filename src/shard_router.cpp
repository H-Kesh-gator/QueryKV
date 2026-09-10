#include "querykv/shard_router.hpp"
#include <algorithm>

namespace qkv {

uint64_t fnv1a_hash(const std::string& s) {
    // FNV-1a 64-bit. Chosen over std::hash because std::hash's
    // implementation (and therefore the ring layout) is not specified
    // to be stable across compilers/standard libraries — FNV-1a gives
    // every node in the cluster the identical ring regardless of how
    // it was built.
    uint64_t h = 1469598103934665603ULL; // offset basis
    for (unsigned char c : s) {
        h ^= c;
        h *= 1099511628211ULL; // FNV prime
    }
    return h;
}

ShardRouter::ShardRouter(int num_shards, int virtual_nodes_per_shard) : num_shards_(num_shards) {
    ring_.reserve(static_cast<size_t>(num_shards) * virtual_nodes_per_shard);
    for (int shard = 0; shard < num_shards; shard++) {
        for (int v = 0; v < virtual_nodes_per_shard; v++) {
            std::string token = "shard-" + std::to_string(shard) + "-vnode-" + std::to_string(v);
            ring_.emplace_back(fnv1a_hash(token), shard);
        }
    }
    std::sort(ring_.begin(), ring_.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });
}

int ShardRouter::shard_for_key(const std::string& key) const {
    if (ring_.empty()) return 0;
    uint64_t h = fnv1a_hash(key);
    auto it = std::lower_bound(ring_.begin(), ring_.end(), h,
        [](const std::pair<uint64_t,int>& entry, uint64_t val) { return entry.first < val; });
    if (it == ring_.end()) it = ring_.begin(); // wrap around the ring
    return it->second;
}

} // namespace qkv
