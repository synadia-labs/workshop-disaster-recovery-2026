#!/usr/bin/env bash
# create-mirror.sh <context> <origin-domain>: create ORDERS in that cluster as a mirror of the
# ORDERS stream living in the other cluster's JetStream domain (reached as $JS.<domain>.API
# over the leaf connection), then wait until it has caught up.
# The mirror is durable (2.12+): it reads the origin through a named, durable push consumer
# with flow-control acks, created here on the origin first and referenced by the "consumer"
# block of streams/orders-mirror-of-<origin>.json. A fresh one each time, so the new mirror
# starts from the beginning of the origin.
set -euo pipefail
cd "$(dirname "$0")"
ctx=${1:?usage: create-mirror.sh <context> <origin-domain>}
origin=${2:?usage: create-mirror.sh <context> <origin-domain>}

nats --context "$origin" consumer rm ORDERS "mirror-$ctx" -f >/dev/null 2>&1 || true
nats --context "$origin" consumer add ORDERS "mirror-$ctx" --target "_mirror.ORDERS.$ctx" \
  --ack flow_control --flow-control --heartbeat 1s --deliver all --defaults >/dev/null
echo "durable consumer mirror-$ctx created on ORDERS in $origin (flow-control acks, delivers on _mirror.ORDERS.$ctx)"
nats --context "$ctx" stream add ORDERS --config "streams/orders-mirror-of-$origin.json" >/dev/null
echo "ORDERS in $ctx created as a mirror of ORDERS in domain $origin, through that consumer"

# caught up = same last sequence as the origin (mirrors keep the origin's sequence numbers)
last_seq() { nats --context "$1" stream info ORDERS -j 2>/dev/null | python3 -c 'import sys,json; print(json.load(sys.stdin)["state"]["last_seq"])' 2>/dev/null || echo "?"; }
t=$SECONDS
until [[ $(last_seq "$ctx") == "$(last_seq "$origin")" ]]; do
  ((SECONDS - t < 60)) || { echo "mirror not caught up after 60s" >&2; exit 1; }
  sleep 1
done
echo "mirror caught up at seq $(last_seq "$ctx") after ~$((SECONDS - t))s"
