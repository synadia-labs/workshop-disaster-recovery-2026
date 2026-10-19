#!/usr/bin/env bash
# Stretch cluster and sourcing replication, on one super-cluster of four clusters:
#   west, east, central   3 nodes each, one cluster per region
#   stretch               3 nodes, stretch-east / stretch-central / stretch-west: in a real
#                         deployment one node in each region (here all on this machine)
#
# Part 1, sourcing replication: clients in every region use a stream called 'foo' (publish to
# foo.>, consume from foo): the eventually consistent "virtual stream" from
# https://www.synadia.com/blog/multi-cluster-consistency-models. Behind it, foo-write-<region>
# takes the local writes and foo-read-<region> sources all three write streams (durable
# sourcing); cluster-scoped subject mappings in the account do the rewriting.
# Part 2, the stretch cluster: ORDERS is a regular stream that lives in east, so losing east
# makes it unavailable. Moved to the stretch cluster, it survives the loss of a whole region:
# two of its three replicas are elsewhere. The price is cross-region latency on every write.
#   NOPAUSE=1 ./demo-stretch-and-sourcing.sh   runs straight through
set -uo pipefail
cd "$(dirname "$0")"
source ./demo-lib.sh

step "Fresh super-cluster (west, east, central, stretch), the real streams behind the virtual stream 'foo', and ORDERS in east" setup
step "Four clusters, twelve servers; each node of the stretch cluster would live in one region" bash -c "
  $WSYS server list 2>/dev/null | hl '^│ (west|east|central|stretch) +│'"
step "Three write streams, three read streams (each sourcing all three write streams through durable consumers), and ORDERS placed in east" bash -c "
  $W stream report; $W consumer report foo-write-west"

# ---- part 1: the virtual stream ------------------------------------------------------
step "Clients publish to foo.>; the mapping sends each publish to the LOCAL write stream" bash -c "
  pub west foo.orders.1 'order 1 from west'; pub east foo.orders.2 'order 2 from east'; pub central foo.orders.3 'order 3 from central'"
step "What was stored where: each write stream holds its region's writes, with the region token added" bash -c "
  contents west foo-write-west; contents east foo-write-east; contents central foo-write-central"
wait_for "all three read streams converged" converged >/dev/null

step "'stream info foo' answers from the local read stream, and the region token is stripped: every region sees foo.orders.1..3" bash -c "
  $W stream info foo | hl 'Information for|Messages: [0-9]'; echo; counts"
step "The contents of 'foo' from each region: same messages, arrival order and sequence numbers are local" bash -c "
  contents west foo; contents east foo; contents central foo"

step "A durable 'C' on 'foo' in each region: same name, different consumers on different read streams" bash -c "
  $W consumer add foo C --pull --ack explicit --deliver all --defaults >/dev/null && $E consumer add foo C --pull --ack explicit --deliver all --defaults >/dev/null
  echo 'west reads all 3 through C:'; payload $W consumer next foo C --count 3
  echo 'east reads 1 through its own C:'; payload $E consumer next foo C --count 1
  echo; echo 'C in west:'; $W consumer info foo C | hl 'Unprocessed'; echo 'C in east:'; $E consumer info foo C | hl 'Unprocessed'"

# ---- part 2: ORDERS, a regular stream in east ----------------------------------------------
step "ORDERS: a regular R3 stream placed in east, with a pull consumer C. Clients anywhere reach it through the gateways" bash -c "
  $CE stream info ORDERS | hl 'Placement Cluster|Name: east|Leader|Replica:'
  pub central orders.4 'order 4 from central'; echo -n 'C, read from west: '; payload $W consumer next ORDERS C --count 1"

# ---- region down -------------------------------------------------------------------------
step "Region down: kill east (its 3 servers, plus the stretch cluster's node that lives in east)" ./kill-east.sh
wait_for_meta_leader west-1 east-
step "Clients of east cannot connect at all" $E rtt

step "The virtual stream: west and central keep writing and reading with local latency; east's data is still readable there" bash -c "
  pub west foo.orders.4 'order 4 from west, east is down'; pub central foo.orders.5 'order 5 from central, east is down'
  wait_for 'west and central sourced each other' bash -c 'subjects west | grep -q foo.orders.5 && subjects central | grep -q foo.orders.4'
  payload $W consumer next foo C --count 2; counts"

step "ORDERS is gone with east: a publish gets no ack, the consumer has no leader, stream info times out" bash -c "
  $CE req orders.5 'order 5 from central' 2>&1 | tail -1
  $W consumer next ORDERS C --count 1 --timeout 3s 2>&1 | tail -1
  $W stream info ORDERS --timeout 3s 2>&1 | tail -1; true"

step "Recovery: east comes back; the read streams catch up (store and forward) and ORDERS is back" bash -c "
  ./start-supercluster.sh >/dev/null && wait_for 'all three regions converged' converged; counts
  wait_for 'ORDERS has a leader' bash -c '[[ -n \$(stream_leader central ORDERS) ]]'; $CE stream info ORDERS | hl 'Leader|Messages: [0-9]'"

# ---- the stream move --------------------------------------------------------------------------
step "Move ORDERS, with its consumer, to the stretch cluster: a live placement change" bash -c "
  $E stream edit ORDERS --cluster stretch -f | hl 'Cluster:'
  wait_for 'ORDERS moved to the stretch cluster' moved ORDERS stretch
  $E stream info ORDERS | hl 'Placement Cluster|Name: stretch|Leader|Replica:'
  echo; echo 'its consumer C moved with it, state intact:'; $E consumer info ORDERS C | hl 'Name: stretch|Leader|Replica:|Acknowledgment Floor'"

step "East down again: its 3 servers and stretch-east" ./kill-east.sh
wait_for_meta_leader west-1 east-
wait_for "ORDERS has a leader on a surviving stretch node" bash -c '[[ -n $(stream_leader central ORDERS) ]]'

step "ORDERS keeps working from the other two regions: 2 of its 3 replicas are up" bash -c "
  pub central orders.5 'order 5 from central, east is down'; echo -n 'C, read from west: '; payload $W consumer next ORDERS C --count 1
  $W stream info ORDERS | hl 'Leader|Replica:|Messages: [0-9]'"
note "A stretch cluster is the 'stretch cluster' design of section 1: immediately consistent, survives a region, every write waits for a cross-region round trip."

step "Recovery: east comes back, the stretch replica in east catches up" bash -c "
  ./start-supercluster.sh >/dev/null && wait_for 'all three regions converged' converged
  wait_for 'stretch-east current again' replica_current west ORDERS stretch-east
  $W stream info ORDERS | hl 'Leader|Replica:'"

# ---- partition ---------------------------------------------------------------------------------
step "Partition: east cut off from the other clusters (its servers and clients are fine)" ./partition-east.sh
wait_for "east has no meta leader" bash -c '[[ -z $(meta_leader east-1) ]]'

step "Both sides keep writing locally; same count, different contents: eventual, not immediate" bash -c "
  pub west foo.orders.6 'order 6 from west during the split'; pub east foo.orders.7 'order 7 from east during the split'
  wait_for 'the read streams sourced their local writes' bash -c 'subjects west | grep -q foo.orders.6 && subjects east | grep -q foo.orders.7'; counts"

step "Reads keep working on both sides, even in east which has no meta leader (new consumers fail there)" bash -c "
  echo -n 'east, existing consumer C: '; payload $E consumer next foo C --count 1
  echo 'east, direct get of the last foo.orders.7 (the raw API call, mapped to foo-read-east):'
  $E req '\$JS.API.DIRECT.GET.foo' '{\"last_by_subj\":\"foo.orders.7\"}' 2>&1 | hl 'Nats-Stream|Nats-Sequence|order 7'
  echo -n 'east, a NEW consumer: '; $E consumer add foo C2 --pull --ack explicit --deliver all --defaults 2>&1 | tail -1"

step "Heal: the sources reconnect, all three read streams converge" bash -c "
  ./heal-partition.sh && wait_for 'all three regions converged' converged; counts"
note "Rejoining peers carry a higher Raft term, so healing triggers a brief meta leader election a few seconds later."
sleep 5
wait_for "control plane back after the re-election" bash -c "$W stream ls -n >/dev/null"
step "Same set of messages everywhere; the order of the split-time writes may differ" bash -c "contents west foo; contents east foo"

# ---- the fine print ----------------------------------------------------------------------------
step "Deletes are local: removing a message in west does not remove it elsewhere" bash -c "
  $W stream rmm foo 1 -f >/dev/null && echo 'removed seq 1 of foo (in west)'; sleep 1; counts"

step "'stream ls' shows the real streams, not the virtual one" bash -c "$W stream ls -n | tr '\n' ' '; echo"

finish
