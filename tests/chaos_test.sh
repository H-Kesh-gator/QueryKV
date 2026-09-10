#!/usr/bin/env bash
# chaos_test.sh — a repeatable, scriptable version of the failover
# demo (as opposed to scripts/e2e_test.sh, which is the fuller
# narrated walkthrough). Exits 0 if the cluster survives losing its
# shard-0 leader, 1 otherwise — suitable for wiring into CI.

set -u
cd "$(dirname "$0")/.."
LOGS=/tmp/qkv_chaos_logs
BIN=./bin/querykv_node
CLI=./bin/querykv_cli
CFG=config/local
CLIENT_CFG=$CFG/client.conf

rm -rf "$LOGS" data
mkdir -p "$LOGS" data/s0n0 data/s0n1 data/s0n2 data/s1n0 data/s1n1 data/s1n2
declare -A ID2NAME=([0]=s0n0 [1]=s0n1 [2]=s0n2 [3]=s1n0 [4]=s1n1 [5]=s1n2)
declare -A PIDS
FAIL=0

for n in s0n0 s0n1 s0n2 s1n0 s1n1 s1n2; do
    "$BIN" "$CFG/$n.conf" > "$LOGS/$n.log" 2>&1 &
    PIDS[$n]=$!
done
cleanup() { for n in "${!PIDS[@]}"; do kill -9 "${PIDS[$n]}" 2>/dev/null; done; }
trap cleanup EXIT

sleep 3
$CLI "$CLIENT_CFG" put "chaos-key-before" "before" > /dev/null

leader_of_shard() {
    $CLI "$CLIENT_CFG" status | awk -v shard="shard=$1 " '
        /^--- node/ { id=$3 }
        $0 ~ shard && /role=LEADER/ { print id }
    '
}
term_of_shard() {
    $CLI "$CLIENT_CFG" status | awk -v shard="shard=$1 " '
        $0 ~ shard { for (i=1;i<=NF;i++) if ($i ~ /^term=/) { split($i,a,"="); print a[2] } }
    ' | head -1
}

BEFORE_LEADER=$(leader_of_shard 0)
BEFORE_TERM=$(term_of_shard 0)
echo "shard 0 leader before chaos: node $BEFORE_LEADER (term $BEFORE_TERM)"

if [ -z "$BEFORE_LEADER" ]; then
    echo "FAIL: no leader elected within startup window"
    exit 1
fi

VICTIM_NAME=${ID2NAME[$BEFORE_LEADER]}
kill -9 "${PIDS[$VICTIM_NAME]}"
echo "killed $VICTIM_NAME (pid ${PIDS[$VICTIM_NAME]})"
unset 'PIDS[$VICTIM_NAME]'

ELECTED=0
for i in $(seq 1 10); do
    sleep 0.5
    AFTER_LEADER=$(leader_of_shard 0)
    if [ -n "$AFTER_LEADER" ] && [ "$AFTER_LEADER" != "$BEFORE_LEADER" ]; then
        ELECTED=1
        break
    fi
done

if [ "$ELECTED" -eq 1 ]; then
    AFTER_TERM=$(term_of_shard 0)
    echo "PASS: new leader (node $AFTER_LEADER, term $AFTER_TERM) elected after killing node $BEFORE_LEADER"
else
    echo "FAIL: no new leader elected for shard 0 within 5s of killing node $BEFORE_LEADER"
    FAIL=1
fi

WRITE_OUT=$($CLI "$CLIENT_CFG" put "chaos-key-after" "after")
if [ "$WRITE_OUT" == "OK" ]; then
    READ_BACK=$($CLI "$CLIENT_CFG" get "chaos-key-after")
    if [ "$READ_BACK" == "after" ]; then
        echo "PASS: write+read succeeded against the new leader"
    else
        echo "FAIL: write reported OK but read back '$READ_BACK' instead of 'after'"
        FAIL=1
    fi
else
    echo "FAIL: write after failover did not return OK (got '$WRITE_OUT')"
    FAIL=1
fi

exit $FAIL
