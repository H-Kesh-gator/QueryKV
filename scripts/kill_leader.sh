#!/usr/bin/env bash
# kill_leader.sh <shard-id> — finds whichever node currently leads the
# given shard (by asking the cluster via the CLI's `status` command)
# and SIGKILLs it. Run this against a cluster started with
# run_cluster.sh, then watch `querykv_cli status` show a new leader
# and a rising term within about a second and a half.
#
# Usage: ./scripts/kill_leader.sh 0

set -u
cd "$(dirname "$0")/.."
SHARD=${1:?usage: kill_leader.sh <shard-id>}

declare -A ID2NAME=([0]=s0n0 [1]=s0n1 [2]=s0n2 [3]=s1n0 [4]=s1n1 [5]=s1n2)

LEADER_ID=$(./bin/querykv_cli config/local/client.conf status | awk -v shard="shard=$SHARD " '
    /^--- node/ { id=$3 }
    $0 ~ shard && /role=LEADER/ { print id }
')

if [ -z "$LEADER_ID" ]; then
    echo "no leader found for shard $SHARD (mid-election, or cluster not running?)"
    exit 1
fi

NAME=${ID2NAME[$LEADER_ID]}
PID=$(pgrep -f "querykv_node config/local/$NAME.conf")
if [ -z "$PID" ]; then
    echo "shard $SHARD leader is node $LEADER_ID ($NAME) but no matching local process found"
    echo "(are you running the cluster via docker compose instead? use:"
    echo "   docker compose kill $NAME -s SIGKILL"
    echo " )"
    exit 1
fi

echo "shard $SHARD leader is node $LEADER_ID ($NAME, pid $PID) — killing it"
kill -9 "$PID"
