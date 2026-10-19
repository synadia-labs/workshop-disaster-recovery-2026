#!/usr/bin/env bash
# Simulate the loss of the whole 'east' region: SIGKILL the 3 servers of cluster 'east' and
# stretch-east, the node of the stretch cluster that lives in that region (no graceful shutdown).
# JetStream data is kept; run start-supercluster.sh to bring them back.
set -euo pipefail
cd "$(dirname "$0")"

SERVERS="east-1 east-2 east-3 stretch-east"

# Prints the pid of the server if it is running.
running_pid() {
  local pidfile="data/$1.pid" pid
  [[ -f $pidfile ]] || return 1
  pid=$(cat "$pidfile")
  ps -p "$pid" -o comm= 2>/dev/null | grep -q nats-server && echo "$pid"
}

for s in $SERVERS; do
  if pid=$(running_pid "$s"); then
    kill -9 "$pid"
    echo "killed $s (pid $pid)"
  else
    echo "$s not running"
  fi
done
