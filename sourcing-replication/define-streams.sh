#!/usr/bin/env bash
# Create the six real streams behind the virtual stream 'foo', and ORDERS:
#   foo-write-<region>   R3 in its region, receives the local writes (foo.<region>.>)
#   foo-read-<region>    R3 in its region, sources ALL THREE write streams and strips the
#                        region token (foo.west.x -> foo.x), so clients read foo.>
#   ORDERS               a regular R3 stream on orders.>, placed in east, pull consumer C
# The read streams use durable sourcing: each one reads a write stream through a named, durable
# push consumer on it (ack policy flow_control, 1s heartbeat), created here before the read
# streams, and referenced by the "consumer" block of the source in streams/foo-read-*.json.
# Each asset is created through a context in its own region; the JSON also pins the placement.
set -euo pipefail
cd "$(dirname "$0")"

REGIONS="west east central"

for r in $REGIONS; do
  nats --context "$r" stream add "foo-write-$r" --config "streams/foo-write-$r.json" >/dev/null && echo "created foo-write-$r ($r)"
done

# the durable consumers the read streams source through: <write stream> feeds <read stream>
for w in $REGIONS; do
  for r in $REGIONS; do
    nats --context "$w" consumer add "foo-write-$w" "foo-read-$r" --target "_src.foo-write-$w.foo-read-$r" \
      --ack flow_control --flow-control --heartbeat 1s --deliver all --defaults >/dev/null &&
      echo "created durable consumer foo-read-$r on foo-write-$w (flow-control acks, delivers on _src.foo-write-$w.foo-read-$r)"
  done
done

for r in $REGIONS; do
  nats --context "$r" stream add "foo-read-$r" --config "streams/foo-read-$r.json" >/dev/null && echo "created foo-read-$r ($r)"
done

nats --context east stream add ORDERS --config streams/orders.json >/dev/null && echo "created ORDERS (placed in east)"
nats --context east consumer add ORDERS C --pull --ack explicit --deliver all --defaults >/dev/null && echo "created pull consumer C on ORDERS"
