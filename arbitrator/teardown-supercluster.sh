#!/usr/bin/env bash
# Stop every server of the local super-cluster and delete all of its data, logs and pid files.
set -euo pipefail
cd "$(dirname "$0")"

SERVERS="west-1 west-2 west-3 east-1 east-2 east-3 arbitrator-1"
TIMEOUT=10

# Prints the pid of the server if it is running.
running_pid() {
  local pidfile="data/$1.pid" pid
  [[ -f $pidfile ]] || return 1
  pid=$(cat "$pidfile")
  ps -p "$pid" -o comm= 2>/dev/null | grep -q nats-server && echo "$pid"
}

pids=""
for s in $SERVERS; do
  if pid=$(running_pid "$s"); then
    kill "$pid"
    echo "stopping $s (pid $pid)"
    pids="$pids $pid"
  fi
done

deadline=$((SECONDS + TIMEOUT))
for pid in $pids; do
  while kill -0 "$pid" 2>/dev/null; do
    if ((SECONDS >= deadline)); then
      echo "pid $pid still running after ${TIMEOUT}s, killing it"
      kill -9 "$pid" 2>/dev/null || true
      break
    fi
    sleep 0.2
  done
done

if [[ -f data/gwproxy.pid ]] && kill "$(cat data/gwproxy.pid)" 2>/dev/null; then
  echo "stopped gwproxy"
fi

if [[ -f data/east-svc.pid ]] && kill "$(cat data/east-svc.pid)" 2>/dev/null; then
  echo "stopped the east.svc responder"
fi

rm -rf data
echo "super-cluster stopped and ./data removed"
