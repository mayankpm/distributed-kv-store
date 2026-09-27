#!/usr/bin/env bash
# Starts every node listed in a cluster config as a background process.
#   scripts/run_local_cluster.sh [examples/cluster3.conf] [build]
# Stop them with: kill $(cat data/pids)
set -euo pipefail

CONFIG="${1:-examples/cluster3.conf}"
BUILD="${2:-build}"
mkdir -p data
: > data/pids
for id in $(awk '$1 == "node" { print $2 }' "$CONFIG"); do
  "$BUILD/kvnode" --config "$CONFIG" --id "$id" --data data > "data/node$id.log" 2>&1 &
  echo $! >> data/pids
  echo "started node $id (pid $!), log in data/node$id.log"
done
echo "try: $BUILD/kvcli --config $CONFIG put hello world && $BUILD/kvcli --config $CONFIG get hello"
