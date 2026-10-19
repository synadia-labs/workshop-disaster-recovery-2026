# Stretch cluster and sourcing replication

Walkthrough for `demo-stretch-and-sourcing.sh`, on one local super-cluster of four clusters:
three regional clusters (west, east, central, 3 nodes each) and a **stretch cluster** whose
three nodes would, in a real deployment, each live in one of the regions. Two designs on it:

- **Part 1, sourcing replication:** the eventually consistent multi-region "virtual stream"
  from [Multi-cluster consistency models](https://www.synadia.com/blog/multi-cluster-consistency-models),
  extended to three regions. Regions never wait for each other; a region keeps writing and
  reading on its own when the others are down or unreachable.
- **Part 2, the stretch cluster:** a regular stream, `ORDERS`, lives in east and is lost with
  east. Moved to the stretch cluster (a live placement change) it survives the loss of a whole
  region, because two of its three replicas are elsewhere. Immediately consistent, at the
  price of a cross-region round trip on every write.

It uses the same port range as `../arbitrator`, so only one of the two can run at a time.

## The super-cluster

| cluster   | servers                                    | clients          | monitoring       |
|-----------|--------------------------------------------|------------------|------------------|
| `west`    | west-1, west-2, west-3                     | 4111, 4112, 4113 | 8111, 8112, 8113 |
| `east`    | east-1, east-2, east-3                     | 4121, 4122, 4123 | 8121, 8122, 8123 |
| `central` | central-1, central-2, central-3            | 4131, 4132, 4133 | 8131, 8132, 8133 |
| `stretch` | stretch-east, stretch-central, stretch-west | 4141, 4142, 4143 | 8141, 8142, 8143 |

Twelve meta peers, quorum 7. Every gateway connection goes through `gwproxy` so that
`partition-east.sh` can cut a cluster off. `kill-<region>.sh` kills the region's three
servers **and** the stretch node that lives there (`stretch-east` with east): that is what
losing a region means for a stretch cluster. Contexts: `west`, `east`, `central`, `stretch`
(user `app`) and `<cluster>-sys` (system account).

## Part 1: the virtual stream

Clients in every region use one stream called `foo`: they publish to `foo.>` and consume
from `foo`. There is no stream called `foo`. Behind the name, per region:

```
   west clients                                  (east and central: the same)
   publish foo.x ──► foo-write-west (foo.west.>) ──┐
                                                   ├──► sourced by foo-read-west, foo-read-east, foo-read-central
   consume foo ◄── foo-read-west ◄── sources foo-write-west, foo-write-east, foo-write-central
                   (foo.<region>.x → foo.x)
```

| stream               | where            | subjects         | role                                               |
|----------------------|------------------|------------------|----------------------------------------------------|
| `foo-write-<region>` | R3 in `<region>` | `foo.<region>.>` | takes the region's writes; `max_age` 1h            |
| `foo-read-<region>`  | R3 in `<region>` | none             | sources all three write streams, strips the region token |

Definitions are in `streams/*.json`; `define-streams.sh` creates them. Each read stream has
one source per write stream with a `subject_transforms` entry (`foo.west.>` → `foo.>`),
which both filters and renames.

The sources are **durable** (NATS 2.12+): instead of the ephemeral consumer a source
normally creates for itself on the origin, each read stream reads a write stream through a
named, durable push consumer that exists on the write stream beforehand:

```sh
nats consumer add foo-write-east foo-read-west --target _src.foo-write-east.foo-read-west \
  --ack flow_control --flow-control --heartbeat 1s --deliver all --defaults
```

and the source names it: `"consumer": { "name": "foo-read-west", "deliver_subject":
"_src.foo-write-east.foo-read-west" }`. A flow-control consumer must be push, with flow
control on, a 1s heartbeat, a max-ack-pending and no ack wait; the read stream acks through
the flow-control messages, so the consumer's ack floor is the source's progress.
`define-streams.sh` creates the nine consumers (one per write stream and reader) before the
read streams. Being durable, a consumer and its position survive its cluster being killed
and restarted; the read stream only has to reset it, which is why the regions reconverge in
seconds after an outage or a split.

The `max_age` of the write streams is the longest outage or split that can be recovered
without losing writes: a read stream that reconnects after longer than that finds the oldest
messages gone. Tune it to your requirements.

### The mappings that make it transparent

`accounts.conf` carries cluster-scoped subject mappings in the APP account (in operator mode
they would be in the account JWT). Each has one destination per region, `weight: 100%`,
scoped with `cluster: <region>`, so the same mapping file works everywhere and only the
local destination applies. The stretch cluster has no mapping: it hosts no part of `foo`.

| client uses                              | rewritten in west to                              |
|------------------------------------------|---------------------------------------------------|
| `foo.>`                                  | `foo.west.>`                                      |
| `$JS.API.STREAM.INFO.foo`                | `$JS.API.STREAM.INFO.foo-read-west`               |
| `$JS.API.CONSUMER.CREATE.foo.>`          | `$JS.API.CONSUMER.CREATE.foo-read-west.>`         |
| `$JS.API.CONSUMER.DURABLE.CREATE.foo.*`  | `…foo-read-west.{{wildcard(1)}}`                  |
| `$JS.API.CONSUMER.INFO/DELETE.foo.*`     | `…foo-read-west.{{wildcard(1)}}`                  |
| `$JS.API.CONSUMER.LIST/NAMES.foo`        | `…foo-read-west`                                  |
| `$JS.API.CONSUMER.MSG.NEXT.foo.*`        | `…foo-read-west.{{wildcard(1)}}`                  |
| `$JS.API.STREAM.MSG.GET.foo`             | `…foo-read-west`                                  |
| `$JS.API.DIRECT.GET.foo` and `.foo.>`    | `…foo-read-west` / `…foo-read-west.>`             |
| `$JS.API.STREAM.MSG.DELETE.foo`          | `…foo-read-west`                                  |
| `$JS.ACK.foo.>`                          | `$JS.ACK.foo-read-west.>`                         |

Every JetStream API subject that carries the stream name needs an entry; the list above is
what the `nats` CLI and the clients use for info, consumers (durable and ordered), pull
requests, gets and acks. Anything not mapped (`stream ls`, purge, edit) shows or touches
the real streams.

## Part 2: ORDERS and the stretch cluster

`ORDERS` (`streams/orders.json`) is an ordinary R3 stream on `orders.>` with
`placement.cluster: east` and a pull consumer `C`. Clients in any region reach it through
the gateways, but it has exactly one home: lose east, lose the stream.

A stream move is a placement change on a live stream:

```sh
nats stream edit ORDERS --cluster stretch
```

The server adds the new peers, replicates the data to them, hands over leadership and
drops the old peers; `stream info` shows a `Cluster Migration Status` block while it runs.
The stream's consumers move with it, state included. Afterwards `ORDERS` has one replica
per region (on `stretch-east`, `stretch-central`, `stretch-west`), so any one region can
be lost and the stream keeps a quorum of 2. Every write now waits for a round trip to the
nearest other region: this is the "stretch cluster" trade-off of section 1 of the workshop.

## Scripts

| script                                                | what it does                                                   |
|-------------------------------------------------------|----------------------------------------------------------------|
| `start-supercluster.sh` / `teardown-supercluster.sh`  | the 12 servers plus gwproxy                                    |
| `create-contexts.sh` / `cleanup-contexts.sh`          | the 8 `nats` contexts                                          |
| `define-streams.sh`                                   | the six real streams behind `foo` (with their nine durable consumers) and `ORDERS` |
| `kill-east.sh` / `kill-west.sh` / `kill-central.sh`   | SIGKILL a region: its cluster and its stretch node, data kept  |
| `partition-east.sh` / `heal-partition.sh`             | cut / restore east's gateway links                             |
| `demo-stretch-and-sourcing.sh`                        | the walkthrough below, step by step (Enter between steps; `NOPAUSE=1` runs through) |
| `demo-lib.sh`                                         | shared helpers (`pub`, `counts`, `contents`, `moved`, …)       |

## Walkthrough

### 1. Writes go to the local write stream

```sh
nats --context west req foo.orders.1 'order 1 from west'
nats --context east req foo.orders.2 'order 2 from east'
nats --context central req foo.orders.3 'order 3 from central'
```

`req` rather than `pub` so the JetStream PubAck is shown: `{"stream":"foo-write-west","seq":1}`
from west, `{"stream":"foo-write-east","seq":1}` from east, and so on. The write is
acknowledged by the local R3 write stream: local latency, and the message is safely stored
in the region before the ack. The demo's `contents` helper then lists each write stream:
the region token is in the stored subject (`foo.west.orders.1`).

### 2. Reads come from the local read stream, and every region has everything

```sh
nats --context west stream info foo      # answers for foo-read-west
nats --context west stream subjects foo  # foo.orders.1, foo.orders.2, foo.orders.3
```

Within a second or so all three read streams hold all three messages, with the region token
stripped by the source transform. `stream info foo` reports the real stream name
(`foo-read-west`), which is the one place the illusion shows.

To see the messages themselves: `nats --context west stream view foo` (interactive paging).
The demo's `contents` helper does the same by draining an ephemeral pull consumer created on
`foo`, printing one line per message: `seq`, subject and payload, from each region. Same
messages, but the sequence numbers and the order are each region's own.

**Caveat on consumer creation through the mapping.** `nats sub 'foo.>' --stream foo --all`
fails with `err_code=10056 stream name in subject does not match request`: its
ordered-consumer request carries `"stream_name": "foo"` in the body while the mapped subject
now names `foo-read-west`, and the server compares the two (`jsConsumerCreateRequest`).
`nats consumer add foo C`, `consumer next`, `consumer info` and `stream view foo` work
because the CLI first resolves `foo` with `STREAM.INFO` (mapped, returns the real config) and
then uses the real stream name in its subsequent calls. An application that builds
consumer-create requests with the virtual name in the body hits the 10056 error; it should
either look the stream up first (as the CLI does) or create consumers by the real name,
which it can learn from `stream info foo`.

### 3. Consumers are per region

```sh
nats --context west consumer add foo C --pull --ack explicit --deliver all --defaults
nats --context west consumer next foo C --count 3
nats --context east consumer ls foo        # east's own consumers only
```

The durable `C` lives on `foo-read-west`. Each region has its own read stream and its own
consumers; there is no global named durable. An application that moves from one region to
the other starts from that region's consumer state.

### 4. ORDERS lives in east

```sh
nats --context central stream info ORDERS        # Placement Cluster: east, leader and replicas east-*
nats --context central req orders.4 'order 4'    # acked by ORDERS in east, across the gateway
nats --context west consumer next ORDERS C       # the pull consumer, read from west
```

A regular stream: one home, reachable from everywhere.

### 5. Region down

```sh
./kill-east.sh        # east-1, east-2, east-3 and stretch-east
```

- **The virtual stream carries on.** West and central keep writing (their write streams
  ack) and reading; each already holds east's writes up to the moment east died, and they
  keep sourcing each other.
- **East's clients** are down with their region (`nats --context east rtt`: every URL
  `failed`); nothing to do about that in this design.
- **ORDERS is gone.** A publish to `orders.>` gets `No responders`, `consumer next` and
  `stream info` time out: all three replicas were in east.
- The meta group still has 8 of 12 peers, so the survivors keep their control plane.

Recovery: `./start-supercluster.sh` (starts only what is not running). `foo-read-east`
resets its durable consumers on the other write streams and catches up with everything
written in the meantime, store and forward, in a few seconds. `ORDERS` elects a leader and
is back as it was.

### 6. Move ORDERS to the stretch cluster

```sh
nats --context east stream edit ORDERS --cluster stretch
nats --context east stream info ORDERS       # Placement Cluster: stretch; leader and replicas stretch-*
nats --context east consumer info ORDERS C   # also on stretch-*, ack floor intact
```

The edit prints the placement diff and the move completes in a couple of seconds for a
stream this small (the demo waits for every replica to be a current `stretch-*` peer). The
stream and its consumer now have one replica in each region.

### 7. East down again

```sh
./kill-east.sh
nats --context central req orders.5 'order 5'    # acked: stored in ORDERS as seq 2
nats --context west consumer next ORDERS C       # works
nats --context west stream info ORDERS           # leader stretch-west or stretch-central;
                                                 # stretch-east outdated, N operations behind
```

Two of three replicas are up, so the stream keeps a quorum and both the data plane and the
consumer work from the other regions. When east returns, `stretch-east` catches up and shows
`current` again within seconds.

### 8. Partition

```sh
./partition-east.sh      # cuts east's gateway links to west, central and stretch
```

Note that only gateway links go through `gwproxy`: `stretch-east` keeps its routes to the
other stretch nodes, so locally the stretch cluster is not affected by this partition (in a
real deployment it would lose one replica, as in step 7).

- Both sides keep **writing and reading locally**. West publishes `order 6`, east publishes
  `order 7`; each side acks its own and sees only its own new message. The read streams now
  report the same count with different contents. This is the point of the design: no quorum
  across regions is needed for the data plane.
- East has no meta leader (3 of 12). Its existing consumer `C` keeps delivering, and a
  direct get works (`nats req '$JS.API.DIRECT.GET.foo' '{"last_by_subj":"foo.orders.7"}'`,
  mapped to `foo-read-east`). Creating a new consumer fails with 10008 until the partition
  heals: that is a control-plane call. Note that `nats stream get` goes through the regular
  `MSG.GET` API, which needs the meta leader, so use direct gets for reads that must survive
  a lost control plane.
- Nothing is lost: the write streams hold every message.

Recovery: `./heal-partition.sh`. The sources reconnect and all three read streams converge
to the same set of 7 messages within a few seconds; the order in which `order 6` and
`order 7` appear can differ between regions. Expect a brief meta leader election right after
healing: the rejoining east peers carry a higher Raft term, so the current leader steps down
and a new one is elected within a few seconds. Control-plane calls in that window get 10008;
the demo waits for it before listing the contents.

### 9. The fine print, shown live

- `nats stream rmm foo 1` in west removes the message from `foo-read-west` only. Deletes are
  not sourced.
- `nats stream ls` lists the seven real streams, not `foo`.

## What you cannot do with the virtual stream

- KV only if the same key is never written from two regions at once, including during a
  split: last writer wins per region, and the regions may disagree.
- No global ordering, no global sequence numbers, no compare-and-set.
- No global durable consumers.
- Message deletes are local.

If global ordering or immediate consistency is required, that is what the stretch cluster of
part 2 gives, at the cost of cross-region latency on every write.

## Cleanup

Every demo script ends by asking whether to clean up everything (stop the servers, delete
their data, remove the contexts) or leave it all running to play more; cleaning up is the
default, and `NOPAUSE=1` runs do it without asking. By hand:

```sh
./teardown-supercluster.sh && ./cleanup-contexts.sh
```
