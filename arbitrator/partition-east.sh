#!/usr/bin/env bash
# Network partition: cut every gateway link between 'east' and the other clusters.
# East's servers keep running, keep their routes to each other and keep serving their
# clients; they just cannot reach west or the arbitrator any more. Undo with heal-partition.sh.
set -euo pipefail
curl -fs -X POST 'http://127.0.0.1:9100/isolate?cluster=east'
