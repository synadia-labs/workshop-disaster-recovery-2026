#!/usr/bin/env bash
# Primary/backup across two clusters with a mirror: failover by promoting the mirror,
# failback by mirroring in the other direction and swapping roles.
#
#   east (hub, domain 'east')  ORDERS, the primary, takes writes on orders.>
#   west (leaf, domain 'west') ORDERS, a mirror of east's, serves local reads
#
#   NOPAUSE=1 ./demo-mirror-dr.sh   runs straight through
set -uo pipefail
cd "$(dirname "$0")"
source ./demo-lib.sh

step "Fresh clusters; ORDERS in east, and ORDERS in west as a durable mirror of it (across the leaf link)" setup
step "Both sides, and the durable consumer on east's ORDERS that feeds the mirror" bash -c "state east; state west; $E consumer report ORDERS"

# ---- normal operation ------------------------------------------------------------
step "Writes from east AND from west are stored by east: west has no stream on orders.>, so the publish crosses the leaf link" bash -c "
  pub east orders.1 'order 1 via east'; pub east orders.2 'order 2 via east'; pub west orders.3 'order 3 via west'
  contents east ORDERS"
sleep 2
step "The mirror follows, keeping the origin's sequence numbers; reads in west are served locally (mirror_direct)" bash -c "
  state west; echo -n 'west, direct get of orders.3: '; payload $W stream get ORDERS -S orders.3"
step "The mirror's contents: the same messages, same sequence numbers, as in east" bash -c "contents west ORDERS"

# ---- east is lost ---------------------------------------------------------------------
step "East is lost: simulated by deleting ORDERS in east (kill-east.sh is the other way)" bash -c "$E stream rm ORDERS -f && echo 'ORDERS deleted in east'"
step "Writes have nowhere to go: no stream listens on orders.> anywhere" bash -c "pub west orders.4 'order 4, east is gone'"
step "West still has everything up to the moment east died, read-only" bash -c "state west"

# ---- failover ---------------------------------------------------------------------------
step "Failover: promote the mirror in west (2.12+). It becomes a regular stream and takes writes" ./promote.sh west
step "Sequence numbers continue where the mirror stopped; west is now the primary" bash -c "
  pub west orders.4 'order 4 via west, after promotion'; pub west orders.5 'order 5 via west'; state west"

# ---- failback ------------------------------------------------------------------------------
step "Recovery, part 1: east is back; re-create ORDERS in east as a mirror of west's (new durable consumer on west) and let it catch up" ./create-mirror.sh east west
step "East has everything, including what west wrote during the outage" bash -c "state east; state west; contents east ORDERS"
note "Part 2 is the cutover. Stop or pause writers for it: for a moment both streams could listen on orders.>."
step "Recovery, part 2: promote east back to primary, delete west's stream, re-create it as a mirror of east" bash -c "
  ./promote.sh east && $W stream rm ORDERS -f && echo 'ORDERS deleted in west' && ./create-mirror.sh west east"
step "Back to the initial roles: writes from west go to east again, west mirrors" bash -c "
  pub west orders.6 'order 6 via west, back to normal'; sleep 1; state east; state west; contents west ORDERS"

finish
