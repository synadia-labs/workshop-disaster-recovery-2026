#!/usr/bin/env bash
# Start both clusters: 'east' (3 nodes, hub) and 'west' (3 nodes, leaf of east).
# JetStream data, logs and pid files go to ./data (removed by teardown-clusters.sh).
# Idempotent: servers already running are left alone, so this also restarts a killed cluster.
set -euo pipefail
cd "$(dirname "$0")"

SERVERS="east-1 east-2 east-3 west-1 west-2 west-3"
TIMEOUT=30

command -v nats-server >/dev/null || { echo "nats-server not found in PATH" >&2; exit 1; }
command -v nats >/dev/null || { echo "nats CLI not found in PATH" >&2; exit 1; }

# Prints the pid of the server if it is running.
running_pid() {
  local pidfile="data/$1.pid" pid
  [[ -f $pidfile ]] || return 1
  pid=$(cat "$pidfile")
  ps -p "$pid" -o comm= 2>/dev/null | grep -q nats-server && echo "$pid"
}

mkdir -p data
for s in $SERVERS; do
  if pid=$(running_pid "$s"); then
    echo "$s already running (pid $pid)"
    continue
  fi
  nohup nats-server -c "$s.conf" >/dev/null 2>&1 &
  echo "started $s (pid $!)"
done

echo "waiting for all servers to be healthy..."
deadline=$((SECONDS + TIMEOUT))
for s in $SERVERS; do
  port=$(awk -F: '/^listen:/ {print $NF}' "$s.conf" | tr -d ' ')
  # the server's own JetStream health (what /healthz reports), asked through the system account
  until nats --server "nats://127.0.0.1:$port" --user sys --password sys server request jetstream-health --name "$s" --timeout 2s 2>/dev/null | grep -q '"status":"ok"'; do
    if ((SECONDS >= deadline)); then
      echo "$s not healthy after ${TIMEOUT}s, see data/$s.log" >&2
      exit 1
    fi
    sleep 0.5
  done
  echo "  $s healthy"
done

cat <<EOF

clusters are up
  east (hub)   nats://127.0.0.1:4121,4122,4123  (monitoring :8121-8123)  JetStream domain 'east'
  west (leaf)  nats://127.0.0.1:4111,4112,4113  (monitoring :8111-8113)  JetStream domain 'west'
users: app/app (APP account, JetStream), sys/sys (system account)
EOF
