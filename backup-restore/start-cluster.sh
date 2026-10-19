#!/usr/bin/env bash
# Start the 3-node cluster 'demo'.
# JetStream data, logs and pid files go to ./data (removed by teardown-cluster.sh).
# Idempotent: servers already running are left alone, so this also restarts a killed cluster.
set -euo pipefail
cd "$(dirname "$0")"

SERVERS="n1 n2 n3"
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

echo
echo "cluster 'demo' is up: nats://127.0.0.1:4101,4102,4103  (monitoring :8101-8103)"
echo "users: app/app (APP account, JetStream), sys/sys (system account)"
