#!/usr/bin/env bash
# Create the nats CLI contexts for the local super-cluster:
#   west, east, central, stretch                 -> APP account (user app)
#   west-sys, east-sys, central-sys, stretch-sys -> system account (user sys), for `nats server ...`
# The default selected context is left unchanged; use --context <name> or `nats context select <name>`.
set -euo pipefail

command -v nats >/dev/null || { echo "nats CLI not found in PATH" >&2; exit 1; }

WEST=nats://127.0.0.1:4111,nats://127.0.0.1:4112,nats://127.0.0.1:4113
EAST=nats://127.0.0.1:4121,nats://127.0.0.1:4122,nats://127.0.0.1:4123
CENTRAL=nats://127.0.0.1:4131,nats://127.0.0.1:4132,nats://127.0.0.1:4133
STRETCH=nats://127.0.0.1:4141,nats://127.0.0.1:4142,nats://127.0.0.1:4143

# add <context> <cluster> <urls>
add() {
  nats context add "$1" --server "$3" --user app --password app \
    --description "Stretch + sourcing super-cluster: $2 cluster" >/dev/null
  nats context add "$1-sys" --server "$3" --user sys --password sys \
    --description "Stretch + sourcing super-cluster: $2 cluster (system account)" >/dev/null
  echo "created contexts $1 and $1-sys"
}

add west west "$WEST"
add east east "$EAST"
add central central "$CENTRAL"
add stretch stretch "$STRETCH"
