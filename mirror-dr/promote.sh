#!/usr/bin/env bash
# promote.sh <context>: turn the ORDERS mirror in that cluster into a regular stream that
# accepts writes on orders.> (NATS 2.12+). Sequence numbers continue from where the mirror was.
# A promoted stream cannot become a mirror again; to go back it is deleted and re-created.
set -euo pipefail
ctx=${1:?usage: promote.sh <context>}
nats --context "$ctx" stream edit ORDERS --no-mirror --subjects 'orders.>' -f >/dev/null
echo "ORDERS in $ctx promoted: now a regular stream on orders.>"
