#!/usr/bin/env bash
# Remove the nats CLI contexts created by create-contexts.sh.
set -euo pipefail

command -v nats >/dev/null || { echo "nats CLI not found in PATH" >&2; exit 1; }

existing=$(nats context ls --names)
for c in west west-sys east east-sys central central-sys stretch stretch-sys; do
  if grep -qx "$c" <<<"$existing"; then
    nats context rm -f "$c" >/dev/null
    echo "removed context $c"
  else
    echo "context $c not found"
  fi
done
