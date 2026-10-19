#!/usr/bin/env bash
# Start the local super-cluster: 'west' (3 nodes), 'east' (3 nodes) and 'arbitrator' (1 node).
# JetStream data, logs and pid files go to ./data (removed by teardown-supercluster.sh).
set -euo pipefail
cd "$(dirname "$0")"

SERVERS="west-1 west-2 west-3 east-1 east-2 east-3 arbitrator-1"
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

# gwproxy carries every gateway connection (see gateways.conf) so partition-east.sh can cut them.
if [[ ! -x gwproxy/gwproxy || gwproxy/main.go -nt gwproxy/gwproxy ]]; then
  echo "building gwproxy..."
  (cd gwproxy && go build -o gwproxy .)
fi
if curl -fs http://127.0.0.1:9100/status >/dev/null 2>&1; then
  echo "gwproxy already running"
else
  nohup gwproxy/gwproxy -control 127.0.0.1:9100 \
    west:127.0.0.1:9111:127.0.0.1:7111 west:127.0.0.1:9112:127.0.0.1:7112 west:127.0.0.1:9113:127.0.0.1:7113 \
    east:127.0.0.1:9121:127.0.0.1:7121 east:127.0.0.1:9122:127.0.0.1:7122 east:127.0.0.1:9123:127.0.0.1:7123 \
    arbitrator:127.0.0.1:9131:127.0.0.1:7131 >data/gwproxy.log 2>&1 &
  echo $! >data/gwproxy.pid
  echo "started gwproxy (pid $!)"
  until curl -fs http://127.0.0.1:9100/status >/dev/null 2>&1; do sleep 0.2; done
fi

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

super-cluster is up
  west       nats://127.0.0.1:4111,4112,4113  (monitoring :8111-8113)
  east       nats://127.0.0.1:4121,4122,4123  (monitoring :8121-8123)
  arbitrator nats://127.0.0.1:4131            (monitoring :8131)
users: app/app (APP account, JetStream), sys/sys (system account)
EOF
