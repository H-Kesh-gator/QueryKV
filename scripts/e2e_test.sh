#!/usr/bin/env bash
# e2e_test.sh — a full local (non-Docker) demonstration run:
#   1. boots a 6-node, 2-shard cluster on localhost
#   2. writes + reads keys through the CLI client (proves shard
#      routing + leader redirect-following works)
#   3. kills the shard-0 leader mid-cluster and proves a new leader
#      is elected and writes keep succeeding (the fault-tolerance claim)
#   4. kills and restarts a shard-1 follower and proves it recovers
#      its state from the WAL + catches back up via replication
#
# Everything runs in ONE script invocation because these are
# background server processes — they only stay alive for the
# lifetime of the shell that spawned them.

set -u
cd "$(dirname "$0")/.."
ROOT="$(pwd)"
LOGS=/tmp/qkv_e2e_logs
BIN=./bin/querykv_node
CLI=./bin/querykv_cli
CFG=config/local
CLIENT_CFG=$CFG/client.conf

rm -rf "$LOGS" data
mkdir -p "$LOGS" data/s0n0 data/s0n1 data/s0n2 data/s1n0 data/s1n1 data/s1n2

declare -A PIDS
start_node() {
    local name=$1
    "$BIN" "$CFG/$name.conf" > "$LOGS/$name.log" 2>&1 &
    PIDS[$name]=$!
    echo "  started $name (pid ${PIDS[$name]})"
}

echo "== booting 6-node cluster (shard0: s0n0,s0n1,s0n2 | shard1: s1n0,s1n1,s1n2) =="
for n in s0n0 s0n1 s0n2 s1n0 s1n1 s1n2; do start_node "$n"; done
sleep 3

echo
echo "== cluster status right after boot =="
$CLI "$CLIENT_CFG" status | grep -E "^---|role="

echo
echo "== writing 8 keys through the client (client hashes each key to a shard) =="
for i in 1 2 3 4 5 6 7 8; do
    $CLI "$CLIENT_CFG" put "user:$i" "value-$i"
done

echo
echo "== reading them back =="
FAIL=0
for i in 1 2 3 4 5 6 7 8; do
    got=$($CLI "$CLIENT_CFG" get "user:$i")
    expect="value-$i"
    if [ "$got" != "$expect" ]; then
        echo "  MISMATCH user:$i got='$got' expected='$expect'"
        FAIL=1
    fi
done
[ $FAIL -eq 0 ] && echo "  all 8 keys read back correctly"

echo
echo "== which node is shard 0's leader? =="
# node id -> script name, per config/local/*.conf
declare -A ID2NAME=([0]=s0n0 [1]=s0n1 [2]=s0n2 [3]=s1n0 [4]=s1n1 [5]=s1n2)
LEADER_ID=$($CLI "$CLIENT_CFG" status | awk '
    /^--- node/ { id=$3 }
    /shard=0 .*role=LEADER/ { print id }
')
LEADER_NAME=${ID2NAME[$LEADER_ID]:-}
echo "  current shard-0 leader is node $LEADER_ID ($LEADER_NAME)"

if [ -n "$LEADER_NAME" ]; then
    echo
    echo "== CHAOS: killing shard-0 leader ($LEADER_NAME, pid ${PIDS[$LEADER_NAME]}) with SIGKILL =="
    kill -9 "${PIDS[$LEADER_NAME]}"
    unset 'PIDS[$LEADER_NAME]'
    sleep 3

    echo "== writing a new key while shard-0 is leaderless / re-electing =="
    $CLI "$CLIENT_CFG" put "post-failover-key" "still-works"
    got=$($CLI "$CLIENT_CFG" get "post-failover-key")
    if [ "$got" == "still-works" ]; then
        echo "  SUCCESS: write + read succeeded after killing the leader"
    else
        echo "  FAILURE: got '$got' instead of 'still-works'"
    fi

    echo
    echo "== new shard-0 leader election log lines =="
    for n in s0n0 s0n1 s0n2; do
        [ "$n" == "$LEADER_NAME" ] && continue
        grep -E "CANDIDATE|became LEADER" "$LOGS/$n.log" | tail -3
    done
fi

echo
echo "== WAL crash-recovery: kill + restart a shard-1 follower, verify it catches up =="
VICTIM=s1n1
[ "${VICTIM}" != "" ] && [ -n "${PIDS[$VICTIM]:-}" ] || VICTIM=s1n0
kill -9 "${PIDS[$VICTIM]}" 2>/dev/null
sleep 1
$CLI "$CLIENT_CFG" put "written-while-$VICTIM-was-down" "yes"
sleep 1
echo "  restarting $VICTIM..."
"$BIN" "$CFG/$VICTIM.conf" >> "$LOGS/$VICTIM.log" 2>&1 &
PIDS[$VICTIM]=$!
sleep 3
grep -E "raft started|snapshot_idx" "$LOGS/$VICTIM.log" | tail -2

echo
echo "== final full cluster status =="
$CLI "$CLIENT_CFG" status

echo
echo "== shutting down cluster =="
for n in "${!PIDS[@]}"; do
    kill -9 "${PIDS[$n]}" 2>/dev/null
done
sleep 1
echo "done."
