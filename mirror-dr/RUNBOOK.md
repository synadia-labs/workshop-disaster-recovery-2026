# Primary/backup across two clusters with a mirror

Walkthrough for `demo-mirror-dr.sh`: east holds the stream, west holds a mirror of it;
when east is lost the mirror is promoted and takes over; when east is back the roles
are swapped back by mirroring in the other direction. Everything below was run on the
two local clusters defined here.

## Topology

```
   east (hub)                         west (leaf of east)
   east-1 east-2 east-3   ◄── leaf ── west-1 west-2 west-3
   JetStream domain 'east'            JetStream domain 'west'
   ORDERS  (orders.>)   ──mirror──►   ORDERS  (mirror of east's ORDERS)
```

| cluster | role                 | servers                | clients          | monitoring       |
|---------|----------------------|------------------------|------------------|------------------|
| `east`  | hub, primary         | east-1, east-2, east-3 | 4121, 4122, 4123 | 8121, 8122, 8123 |
| `west`  | leaf, backup         | west-1, west-2, west-3 | 4111, 4112, 4113 | 8111, 8112, 8113 |

- West's servers each make a **leaf node connection** to east (`leafnodes.remotes` in
  `west-*.conf`, listeners on 7421–7423 in `east-*.conf`), in the APP account.
- Each cluster is its own **JetStream domain** (`east`, `west`): two independent meta
  groups, so losing one cluster never touches the other's control plane. The other
  domain's API is reachable across the leaf link as `$JS.east.API` / `$JS.west.API`;
  the plain `$JS.API` is deliberately not shared.
- Both streams are called `ORDERS`. Names are per domain, and keeping one name is what
  lets clients fail over by reconnecting to the other cluster with no config change.

Contexts: `east`, `west` (user `app`) and `east-sys`, `west-sys` (system account).

## Scripts

| script                              | what it does                                                       |
|-------------------------------------|--------------------------------------------------------------------|
| `start-clusters.sh` / `teardown-clusters.sh` | start / stop both clusters (start is idempotent)          |
| `create-contexts.sh` / `cleanup-contexts.sh` | the 4 `nats` contexts                                     |
| `create-mirror.sh <ctx> <origin-domain>` | create ORDERS in `<ctx>` as a mirror of the other domain's ORDERS and wait until caught up |
| `promote.sh <ctx>`                  | turn the mirror in `<ctx>` into a regular stream on `orders.>`     |
| `kill-east.sh` / `kill-west.sh`     | SIGKILL a whole cluster (the realistic outage)                     |
| `demo-mirror-dr.sh`                 | the walkthrough below (Enter between steps; `NOPAUSE=1` runs through) |
| `demo-lib.sh`                       | shared helpers (`pub`, `state`, …)                                 |
| `streams/*.json`                    | the three stream definitions: origin, mirror of east, mirror of west |

The mirror definition is the interesting one:

```json
"mirror": { "name": "ORDERS", "external": { "api": "$JS.east.API" },
            "consumer": { "name": "mirror-west", "deliver_subject": "_mirror.ORDERS.west" } },
"mirror_direct": true
```

`external.api` is the other domain's API prefix; `mirror_direct` lets the mirror answer
direct gets itself, so reads in west never cross the link.

`consumer` makes it a **durable mirror** (NATS 2.12+): rather than the ephemeral consumer
a mirror normally creates for itself on the origin, it reads through a named, durable push
consumer that `create-mirror.sh` creates on the origin first:

```sh
nats --context east consumer add ORDERS mirror-west --target _mirror.ORDERS.west \
  --ack flow_control --flow-control --heartbeat 1s --deliver all --defaults
```

The `flow_control` ack policy needs a push consumer with flow control, a 1s heartbeat and
no ack wait; the mirror acks through the flow-control messages, so the consumer's ack floor
is the mirror's position, and it survives the origin cluster going down and coming back.
Since a durable consumer remembers what it delivered, `create-mirror.sh` creates a fresh
one each time a mirror is (re-)created, so the new mirror starts from the origin's start.

## Walkthrough

### 1. Normal operation

```sh
nats --context east req orders.1 'order 1 via east'   # {"stream":"ORDERS","domain":"east","seq":1}
nats --context west req orders.3 'order 3 via west'   # {"stream":"ORDERS","domain":"east","seq":3}
```

A publish from a **west** client is acked by **east**: west's `ORDERS` is a mirror and
has no subjects, so the only interest in `orders.>` is east's stream, reached over the
leaf link. One stream takes all writes (immediate consistency, single order), the
mirror follows within milliseconds and keeps the origin's sequence numbers
(`state west` → `seq 1..3, MIRROR of ORDERS in domain east, lag 0`).

Reads in west are local: `nats --context west stream get ORDERS -S orders.3` is answered
by the mirror thanks to `mirror_direct`. To see the whole mirror, `nats --context west
stream view ORDERS`. The demo's `contents` helper drains a throwaway pull consumer instead
and prints one line per message, `seq`, subject and payload, identical to east's.

### 2. East is lost

The demo deletes `ORDERS` in east (`nats --context east stream rm ORDERS -f`): the
stream is gone, the mirror has nothing to follow. `kill-east.sh` is the realistic
version; see the variations below for what changes.

- A publish to `orders.>` from anywhere gets `No responders`: no stream listens.
- West's `ORDERS` still holds everything up to the last mirrored message, read-only.
  That is the RPO of this design: the mirror lag at the moment of the loss.

### 3. Failover: promote the mirror

```sh
./promote.sh west     # nats stream edit ORDERS --no-mirror --subjects 'orders.>'
```

NATS 2.12+ allows removing the mirror configuration from a stream. The promoted
stream is a normal R3 stream in west, now listening on `orders.>`; the next write
gets `domain: west, seq 4`: **sequence numbers continue** where the mirror stopped, so
consumers' positions stay meaningful. This is the RTO: one command plus client
reconnection.

A promoted stream can never become a mirror again. Going back means deleting it and
re-creating it as a mirror, which is what failback does.

### 4. Failback, part 1: east as a mirror of west

```sh
./create-mirror.sh east west     # ORDERS in east, mirror of $JS.west.API's ORDERS
```

East is back (empty, or with its old stream deleted). Its new `ORDERS` is a mirror of
west's and catches up in a second or two with everything, including what west wrote
during the outage. Nothing changes for clients yet: writes still go to west.

### 5. Failback, part 2: the cutover

```sh
./promote.sh east                       # east becomes a regular stream on orders.> again
nats --context west stream rm ORDERS -f # west's promoted stream goes away
./create-mirror.sh west east            # west is a mirror of east again
```

Pause or stop writers for this step: between the first and second command, both
streams listen on `orders.>` and a write could land in both (with different sequence
numbers). After the third command the initial picture is back: the next write from a
west client is acked by `domain: east`, and west's mirror shows it.

## Variations

- **Real outage** (`./kill-east.sh` instead of deleting the stream): the mirror simply
  stalls (`lag` stays 0, `active` grows, as with any partition); promotion works the
  same. On recovery, east's **old** `ORDERS` comes back with its data and still owns
  `orders.>`. Verified: once its leader is elected (~5s), every write is stored by
  **both** streams, each publisher sees the ack from its own side first, east is
  missing what west wrote during the outage, and the sequence numbers diverge
  (`orders.4` was seq 3 in east and seq 4 in west). So before part 1, delete it:
  `nats --context east stream rm ORDERS -f`. Until the leader election it is harmless,
  which is a window of a few seconds, not a plan.
- **No leaf, a super-cluster instead:** the same mirror works with gateways and no
  domains (`"mirror": {"name": "ORDERS"}` plus a placement), but then losing east also
  costs the shared meta quorum, and promotion is a stream edit that needs a meta leader.
  That is the arbitrator story in `../arbitrator`.
- **Both directions at once** is not this design. Two regions writing locally is the
  sourcing design in `../sourcing-replication`, at the price of immediate consistency.

## Cleanup

Every demo script ends by asking whether to clean up everything (stop the servers,
delete their data, remove the contexts) or leave it all running to play more; cleaning up
is the default, and `NOPAUSE=1` runs do it without asking. By hand:

```sh
./teardown-clusters.sh && ./cleanup-contexts.sh
```
