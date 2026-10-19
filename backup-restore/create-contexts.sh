#!/usr/bin/env bash
# Create the nats CLI contexts for the demo cluster:
#   backup-demo      -> APP account (user app)
#   backup-demo-sys  -> system account (user sys), for `nats server ...`
# The default selected context is left unchanged; use --context <name> or `nats context select <name>`.
set -euo pipefail
command -v nats >/dev/null || { echo "nats CLI not found in PATH" >&2; exit 1; }
URLS=nats://127.0.0.1:4101,nats://127.0.0.1:4102,nats://127.0.0.1:4103
nats context add backup-demo --server "$URLS" --user app --password app --description "Backup/restore demo cluster" >/dev/null
nats context add backup-demo-sys --server "$URLS" --user sys --password sys --description "Backup/restore demo cluster (system account)" >/dev/null
echo "created contexts backup-demo and backup-demo-sys"
