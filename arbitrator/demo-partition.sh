#!/usr/bin/env bash
# Scenario B: network partition. East is cut off from west and the arbitrator, but its
# three servers are up, see each other, and serve their own clients.
#   NOPAUSE=1 ./demo-partition.sh   runs straight through
set -uo pipefail
cd "$(dirname "$0")"
source ./demo-lib.sh

step "Fresh super-cluster with streams in both regions and a mirror of west's data in east" setup_assets
step "Baseline: 7 meta peers, mirror in sync, core request from west answered in east" bash -c "meta west-1; mirror_lag east ORDERS-WEST-DR; $W req east.svc ping | tail -1"

# ---- failure -----------------------------------------------------------------
step "Partition: cut every gateway link between east and the rest" ./partition-east.sh
note "East still has 3 of 7 meta peers (no quorum); west + arbitrator have 4 (quorum)."
note "If the meta leader was in east it keeps leadership for ~10s, then steps down."
wait_for "east has no meta leader" bash -c '[[ -z $(meta_leader east-1) ]]'

step "Two views of the world: west's and east's" bash -c "meta west-1; meta east-1; echo
  echo 'server list from west:'; $WSYS server list 2>/dev/null | hl '^│ (west|east|arbitrator) +│'
  echo 'server list from east:'; $ESYS server list 2>/dev/null | hl '^│ (west|east|arbitrator) +│'"

step "East's data plane keeps working for east's clients" bash -c "
  $E pub orders.east.4 'east order 4' && $E consumer next ORDERS-EAST C --count 1 2>&1 | grep -v '^$' | tail -1 && $E stream info ORDERS-EAST | hl 'Leader|Messages: [0-9]'"

step "...but east's control plane is gone (no meta leader on its side)" bash -c "
  $E stream ls; $E stream add NEW-EAST --subjects 'new.>' --replicas 3 --defaults; true"

step "West: everything works, including the control plane" bash -c "
  $W pub orders.west.4 'west order 4' && $W kv get CONFIG mode --raw && echo &&
  $W stream add NEW-WEST --subjects 'new.>' --replicas 3 --defaults >/dev/null && echo 'created NEW-WEST (R3)'"

step "Across the cut: nothing. Core request to east, publish to east's stream, KV (in west) from east" bash -c "
  $W req east.svc ping 2>&1 | tail -1; $W req orders.east.5 'from west' 2>&1 | tail -1; $E kv get CONFIG mode --raw 2>&1 | tail -1; true"

step "The mirror in east stalls while west keeps writing (3 vs 7 messages; 'lag' can't know, 'active' grows)" bash -c "
  for i in 5 6 7; do $W pub orders.west.\$i 'west order' >/dev/null; done; sleep 2
  $W stream info ORDERS-WEST -j | python3 -c 'import sys,json; print(\"ORDERS-WEST:\", json.load(sys.stdin)[\"state\"][\"messages\"], \"messages\")'
  mirror_lag east ORDERS-WEST-DR"

# ---- recovery ----------------------------------------------------------------
step "Heal the partition" ./heal-partition.sh
wait_for "east peers current again" bash -c "$WSYS server report jetstream 2>/dev/null | grep -E 'east-' | grep -vq false"

step "One world again: east back in the meta group, mirror caught up, cross-site traffic flows" bash -c "
  meta east-1
  wait_for 'mirror caught up with ORDERS-WEST' mirror_caught_up east ORDERS-WEST-DR west ORDERS-WEST
  mirror_lag east ORDERS-WEST-DR; $W req east.svc ping | tail -1; $E stream ls -n | tr '\n' ' '; echo"

finish
