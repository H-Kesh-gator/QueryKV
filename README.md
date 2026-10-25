# QueryKV

A sharded, fault-tolerant distributed key-value store built from scratch in C++17 with **no third-party dependencies**. Features multi-group Raft consensus, consistent-hash sharding, SWIM-style gossip failure detection, and a custom binary TCP protocol.

```
Client (consistent-hash router)
    ├── Shard 0 ──► [n0] ◄─Raft─► [n1] ◄─Raft─► [n2]
    │                 ▲                  ▲                  ▲
    │                 └──────── gossip (SWIM) ───────────┘
    └── Shard 1 ──► [n3] ◄─Raft─► [n4] ◄─Raft─► [n5]
```

## Key Features

- **Multi-Raft Consensus**: Independent Raft groups per shard for strong consistency
- **Consistent Hashing**: O(1) routing with minimal key migration on node changes
- **SWIM Gossip**: Cluster-wide failure detection decoupled from Raft heartbeats
- **Write-Ahead Logging**: Fsync-backed crash recovery and snapshotting
- **Custom Protocol**: Length-prefixed binary frames over raw TCP sockets

## Quick Start

### Prerequisites
- Docker & Docker Compose (for containerized cluster) **OR**
- GCC 13+ with C++17 support (for local builds)
- Make

### Option 1: Docker (Recommended)

```bash
# Build and start the 6-node cluster (2 shards × 3 replicas)
docker compose up --build

# In another terminal:
# Check cluster status
docker compose run --rm cli status

# Write data
docker compose run --rm cli put mykey myvalue

# Read data
docker compose run --rm cli get mykey

# Test fault tolerance
docker compose kill qkv-s0n2 -s SIGKILL   # kill shard 0 leader
docker compose run --rm cli status         # watch new leader elect (~1-2s)
docker compose start qkv-s0n2              # bring it back (recovers from WAL)
```

### Option 2: Local Build

```bash
# Build binaries
make

# Start cluster (terminal 1)
./scripts/run_cluster.sh

# Run commands (terminal 2)
./bin/querykv_cli config/local/client.conf status
./bin/querykv_cli config/local/client.conf put user:42 alice
./bin/querykv_cli config/local/client.conf get user:42

# Kill leader and watch failover
./scripts/kill_leader.sh 0
```

## Project Layout

```
QueryKV/
├── include/querykv/        # Headers: util, protocol, net, wal, storage, raft, gossip, shard_router, config, node
├── src/                    # Implementation: main, node, config, raft, gossip, wal, shard_router
├── client/                 # CLI client: client.hpp/cpp, cli_main.cpp
├── tests/                  # Unit tests (WAL, protocol) + chaos test
├── scripts/                # run_cluster.sh, kill_leader.sh, e2e_test.sh
├── config/                 # Node configs: local/ and docker/
├── docker/                 # Dockerfile (multi-stage build)
├── docker-compose.yml      # 6-node cluster definition
└── Makefile                # Build targets
```

## CLI Commands

```bash
querykv_cli <config> status              # Show cluster state + leaders
querykv_cli <config> get <key>           # Read key
querykv_cli <config> put <key> <value>  # Write key
querykv_cli <config> del <key>           # Delete key
querykv_cli <config>                     # Interactive REPL
```

## Building & Testing

```bash
make                       # Build querykv_node and querykv_cli
make test                  # Unit tests (WAL, protocol, sharding)
./tests/chaos_test.sh      # Automated failover test
./scripts/e2e_test.sh      # Narrated end-to-end test
```

## Architecture Highlights

### Raft Consensus
- Randomized timeout leader election
- Log replication with conflict fast-backtracking
- Snapshot-based log compaction

### Sharding
- Consistent-hash ring routes keys to shards
- Client handles routing and retries automatically

### Failure Detection
- SWIM protocol with direct + indirect probing
- Suspicion/refutation via incarnation numbers

### Durability
- Write-ahead log with fsync-backed crash recovery
- Snapshot + tail log replay on startup
- Docker volumes persist across restarts

## Design Decisions

- **Multi-Raft over Single-Raft**: Enables parallel writes to different shards on independent leaders. Trade-off: no atomic cross-shard transactions.
- **Gossip + Raft Heartbeats**: Gossip provides operational cluster-wide liveness; Raft heartbeats ensure per-shard consistency.
- **Custom Binary Protocol**: Lower overhead than HTTP/JSON; demonstrates framing, endianness, and partial I/O handling.
- **Fsync per WAL Append**: Required for Raft's safety proof; batching would require group commit (not implemented).

## Scope Cuts

Not implemented:
- InstallSnapshot RPC for followers behind the snapshot point
- Cluster membership changes (fixed shard count + members)
- Pre-vote to prevent disruptive elections
- ReadIndex/lease-based reads (only leader serves reads)
- Cross-shard transactions
- TLS/authentication

## Performance Notes

- Single-threaded I/O per node; background tasks use thread pool
- Optimized for **consistency** over latency
- Suitable for low-frequency, high-consistency workloads (metadata, configuration)

## Development

- **Language**: C++17
- **Build System**: Plain Makefile (no CMake/Bazel)
- **Testing**: Protocol tests + scripted chaos tests
- **Dependencies**: None (standard library only)

## License

MIT
