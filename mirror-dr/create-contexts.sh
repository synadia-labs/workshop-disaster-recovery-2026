#!/usr/bin/env bash
# Create the nats CLI contexts for the two clusters:
#   east, west            -> APP account (user app)
#   east-sys, west-sys    -> system account (user sys), for `nats server ...`
# The default selected context is left unchanged; use --context <name> or `nats context select <name>`.
set -euo pipefail

command -v nats >/dev/null || { echo "nats CLI not found in PATH" >&2; exit 1; }

EAST=nats://127.0.0.1:4121,nats://127.0.0.1:4122,nats://127.0.0.1:4123
WEST=nats://127.0.0.1:4111,nats://127.0.0.1:4112,nats://127.0.0.1:4113

# add <context> <description> <urls>
add() {
  nats context add "$1" --server "$3" --user app --password app \
    --description "Mirror DR: $2" >/dev/null
  nats context add "$1-sys" --server "$3" --user sys --password sys \
    --description "Mirror DR: $2 (system account)" >/dev/null
  echo "created contexts $1 and $1-sys"
}

add east "east cluster (hub)" "$EAST"
add west "west cluster (leaf)" "$WEST"
