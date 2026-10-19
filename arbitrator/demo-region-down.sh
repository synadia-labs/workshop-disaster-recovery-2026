#!/usr/bin/env bash
# Scenario A: a whole region goes down. East (3 servers) dies; west and the arbitrator survive.
# Shows what keeps working, what doesn't, and how east recovers.
#   NOPAUSE=1 ./demo-region-down.sh   runs straight through
set -uo pipefail
cd "$(dirname "$0")"
source ./demo-lib.sh

step "Fresh super-cluster with streams in both regions and a mirror of west's data in east" setup_assets
step "Baseline: 7 meta peers, mirror in sync, a publish to east's stream is acked" bash -c "meta west-1; mirror_lag east ORDERS-WEST-DR; $W req orders.east.0 'east order 0' | tail -1"

# ---- failure -----------------------------------------------------------------
step "Region down: kill the whole east cluster" ./kill-east.sh
note "west + arbitrator = 4 of 7 meta peers: still a quorum. If the meta leader was in east,"
note "the survivors elect a new one within a few seconds."
wait_for "east gone from the server list" bash -c "! $WSYS server list 2>/dev/null | grep -q east-"
wait_for_meta_leader west-1 east-

step "Meta group: a leader on the surviving side, east peers not current" bash -c "meta west-1; $WSYS server report jetstream | hl '^│ east-|yes'"

step "West: data plane AND control plane work" bash -c "
  $W pub orders.west.4 'west order 4' && $W consumer next ORDERS-WEST C --count 1 2>&1 | grep -v '^$' | tail -1 &&
  $W kv put CONFIG mode degraded >/dev/null && $W kv get CONFIG mode --raw && echo &&
  $W stream add NEW-WEST --subjects 'new.>' --replicas 3 --defaults >/dev/null && echo 'created NEW-WEST (R3)'"

step "East's streams are unavailable: a publish to ORDERS-EAST gets no ack, stream info times out" bash -c "
  $W req orders.east.4 'east order 4' 2>&1 | tail -1; $W stream info ORDERS-EAST --timeout 3s 2>&1 | head -3; true"

step "The mirror in east is down too; west keeps accumulating messages" bash -c "
  for i in 5 6 7; do $W pub orders.west.\$i 'west order' >/dev/null; done
  $W stream info ORDERS-WEST -j | python3 -c 'import sys,json; print(\"ORDERS-WEST:\", json.load(sys.stdin)[\"state\"][\"messages\"], \"messages\")'
  mirror_lag east ORDERS-WEST-DR"

step "Clients of east cannot connect at all" $E rtt

# ---- recovery ----------------------------------------------------------------
step "Recovery: east comes back with its stores intact" ./start-supercluster.sh
wait_for "east peers current again" bash -c "$WSYS server report jetstream 2>/dev/null | grep -E 'east-' | grep -vq false"

step "Meta group whole again; east's stream has a leader; its consumer resumes" bash -c "meta east-1
  $E stream info ORDERS-EAST -j | python3 -c 'import sys,json; d=json.load(sys.stdin); print(\"ORDERS-EAST leader:\", d[\"cluster\"][\"leader\"], \"messages:\", d[\"state\"][\"messages\"])'
  $E consumer next ORDERS-EAST C --count 1 2>&1 | grep -v '^$' | tail -1"

step "The mirror catches up on its own" bash -c "
  wait_for 'mirror caught up with ORDERS-WEST' mirror_caught_up east ORDERS-WEST-DR west ORDERS-WEST
  mirror_lag east ORDERS-WEST-DR; $W req east.svc ping 2>&1 | tail -1"

finish
