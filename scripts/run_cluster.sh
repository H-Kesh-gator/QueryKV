#!/usr/bin/env bash
# run_cluster.sh — starts the 6-node local cluster and leaves it
# running in the foreground-adjacent background (logs to /tmp/qkv_logs).
# Ctrl+C this script to kill every node it started.
#
# Usage:
#   ./scripts/run_cluster.sh
#   (in another terminal) ./bin/querykv_cli config/local/client.conf status

set -u
cd "$(dirname "$0")/.."
LOGS=/tmp/qkv_logs
CFG=config/local

mkdir -p "$LOGS" data/s0n0 data/s0n1 data/s0n2 data/s1n0 data/s1n1 data/s1n2

PIDS=()
cleanup() {
    echo
    echo "stopping cluster..."
    for pid in "${PIDS[@]}"; do kill -9 "$pid" 2>/dev/null; done
    exit 0
}
trap cleanup INT TERM

for n in s0n0 s0n1 s0n2 s1n0 s1n1 s1n2; do
    ./bin/querykv_node "$CFG/$n.conf" > "$LOGS/$n.log" 2>&1 &
    PIDS+=($!)
    echo "started $n (pid $!, log: $LOGS/$n.log, port: see $CFG/$n.conf)"
done

echo
echo "cluster is up. try in another terminal:"
echo "  ./bin/querykv_cli config/local/client.conf status"
echo "  ./bin/querykv_cli config/local/client.conf put foo bar"
echo "  ./bin/querykv_cli config/local/client.conf get foo"
echo
echo "press Ctrl+C to stop the cluster"
wait
