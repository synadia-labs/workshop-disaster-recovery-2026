#!/usr/bin/env bash
# Heal the network partition created by partition-east.sh: gateway links are allowed again
# and the servers reconnect on their own within a few seconds.
set -euo pipefail
curl -fs -X POST 'http://127.0.0.1:9100/heal'
