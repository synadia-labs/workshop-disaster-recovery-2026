# Losing a cluster in a 3-cluster super-cluster

Walkthrough for the two demos in this directory: a **region going down** and a
**network partition**, both hitting `east`. Everything below was run on the local
super-cluster defined here.

## Topology

```
          ┌──────────────────┐   gateways   ┌──────────────────┐
          │  west (3 nodes)  │◄────────────►│  east (3 nodes)  │
          └────────┬─────────┘              └─────────┬────────┘
                   │           ┌───────────┐          │
                   └──────────►│ arbitrator│◄─────────┘
                               │ (1 node)  │
                               └───────────┘
```

| cluster      | servers                 | clients          | monitoring       |
|--------------|-------------------------|------------------|------------------|
| `west`       | west-1, west-2, west-3  | 4111, 4112, 4113 | 8111, 8112, 8113 |
| `east`       | east-1, east-2, east-3  | 4121, 4122, 4123 | 8121, 8122, 8123 |
| `arbitrator` | arbitrator-1            | 4131             | 8131             |

Meta group: 7 peers, quorum 4. Any single cluster can be lost and the remaining
4 peers still elect a meta leader. The arbitrator exists only for that vote: it
holds no streams (nothing is placed there unless asked for) and no clients use it.

Every gateway connection goes through `gwproxy` (ports 91xx in front of the real
gateway ports 71xx), which is how `partition-east.sh` cuts east off without root.

Contexts: `west`, `east`, `arbitrator` (user `app`, JetStream-enabled account) and
`west-sys`, `east-sys`, `arbitrator-sys` (system account, for `nats server …`).

## Scripts

| script                           | what it does                                                   |
|----------------------------------|----------------------------------------------------------------|
| `start-supercluster.sh`          | builds/starts gwproxy, starts the 7 servers, waits for health; idempotent, so it is also the "bring the region back" command |
| `create-contexts.sh` / `cleanup-contexts.sh` | the 6 `nats` contexts                               |
| `kill-west.sh` / `kill-east.sh` / `kill-arbitrator.sh` | SIGKILL one cluster, data kept (region down) |
| `partition-east.sh` / `heal-partition.sh` | cut / restore east's gateway links (network partition) |
| `demo-region-down.sh`            | scenario A, step by step (Enter between steps; `NOPAUSE=1` runs through) |
| `demo-partition.sh`              | scenario B, same style                                         |
| `demo-lib.sh`                    | shared helpers and the asset setup                             |
| `teardown-supercluster.sh`       | stops everything and deletes `data/`                           |

## Assets used by both scenarios

| asset            | where          | purpose                                              |
|------------------|----------------|------------------------------------------------------|
| `ORDERS-WEST`    | R3 in west     | west's own data, pull consumer `C`                   |
| `ORDERS-EAST`    | R3 in east     | east's own data, pull consumer `C`                   |
| `CONFIG` (KV)    | R3 in west     | something east's clients read from across the gateway |
| `ORDERS-WEST-DR` | R3 in east     | **durable mirror** of `ORDERS-WEST`: west's data kept in east, read through the flow-control consumer `mirror-east` on `ORDERS-WEST` (`streams/orders-west-dr.json`) |
| `east.svc`       | responder in east | core NATS request/reply across the gateway        |

Streams land in the cluster the creating client is connected to (default placement),
so "create it through the `east` context" is what puts a stream in east.

---

## Scenario A: a region goes down (`demo-region-down.sh`)

```sh
./kill-east.sh
```

### What happens

- **Meta group:** 4 of 7 peers left, still a quorum. If the meta leader was in east,
  west + arbitrator elect a new one within a few seconds (`election timeout 4–9s`).
  Until then `server request jetstream` and `server report jetstream` still name the old leader and show
  the east peers as `Current: true`: the survivors have simply not noticed yet. A few
  seconds later the east peers show `Current: false` with a growing `Lag`. Control
  plane calls made in that window time out; the demo waits for a leader outside east.
- **West: everything works**, data plane and control plane: publish, consume, KV,
  and creating streams (`NEW-WEST`) all succeed.
- **East's streams are gone with it.** All three replicas of `ORDERS-EAST` are dead,
  so there is no leader: a publish gets no ack (`nats req orders.east.4` → timeout),
  `stream info ORDERS-EAST` times out. `stream ls` still lists it: the meta layer
  remembers the assignment.
- **The DR mirror is gone too.** `ORDERS-WEST-DR` lives in east: a mirror protects
  against losing the *origin's* region, not its own. West keeps writing (`ORDERS-WEST`
  goes from 4 to 7 messages); the mirror will catch up later.
- **East's clients cannot connect**: `nats --context east rtt` reports every east URL as
  `failed`.

### Recovery

```sh
./start-supercluster.sh        # starts only what is not running
```

- East's servers come back with their stores, rejoin the meta group as themselves
  (same server names), and are `Current` again within seconds.
- `ORDERS-EAST` elects a leader; its consumer resumes where it left off.
- `ORDERS-WEST-DR` catches up on its own: 7 messages, `mirror lag 0`, within seconds.
- Nothing to do by hand. The only data not in east is what east's clients could not
  write while it was down.

Talking points: this is the RTO/RPO of "arbitrator + mirrors" from the matrix slide.
West's data had RPO 0 (R3 in west). East's data was simply unavailable, not lost. To
*serve* east's data from west during the outage you would need a mirror of
`ORDERS-EAST` in west and a promotion (2.12+), which is the primary/backup pattern.

---

## Scenario B: east is cut off (`demo-partition.sh`)

```sh
./partition-east.sh            # closes and refuses every gateway link to/from east
```

East's 3 servers are up, their routes to each other are intact, and their clients
are connected. They just cannot see west or the arbitrator.

### What happens

- **Two meta groups' worth of opinions.** West + arbitrator (4 peers) keep or elect
  a leader. East (3 peers) has none: if the leader was in east, it keeps leadership
  until it notices the lost quorum (`lostQuorumInterval` 10s, checked every 10s, so
  10–25s in practice) and then steps down. For those seconds two servers
  think they lead, but the east one cannot commit anything (no quorum), so it is
  harmless. `nats server report jetstream` from `east-sys` shows the banner
  *"expects 7 nodes but only 3 responded"*; from `west-sys` it shows a healthy
  leader with east peers not current.
- **`server list` tells each side's story:** from west, 4 servers in 2 clusters;
  from east, 3 servers in 1 cluster.
- **East's data plane keeps working for east's clients.** `ORDERS-EAST` has its 3
  replicas, so publish (acked), consume and `stream info` all work via the `east`
  context. This is the big difference from scenario A.
- **East's control plane does not.** `stream ls`, `stream add`, `consumer add` from
  the `east` context fail with `JetStream system temporarily unavailable (10008)`:
  those need the meta leader, which is on the other side of the cut.
- **West: everything works**, including creating streams.
- **Across the cut: nothing.** From west, `req east.svc` → `No responders`, a publish
  to `orders.east.*` → no ack. From east, `kv get CONFIG` (the KV is in west) fails.
- **The mirror stalls silently.** `ORDERS-WEST-DR` stays at 3 messages while
  `ORDERS-WEST` reaches 7. Its `lag` still reads 0: a mirror only learns about new
  origin messages from the origin, so it cannot know what it is missing. What grows is
  `active` (time since the last message from the origin). Alert on that, or compare
  message counts; do not trust `lag` alone during a partition.

### Recovery

```sh
./heal-partition.sh
```

- Gateways reconnect on their own within a few seconds (reconnect backoff).
- East's peers are `Current` again, east's control plane is back
  (`stream ls` from `east` works).
- The mirror catches up: 7 messages, `lag 0`.
- Core requests and KV reads across the gateway work again.

Talking points: a partition is *not* a region loss. Local reads and writes to
local R3 streams survive it; only the control plane and cross-site traffic stop.
That is exactly the split the matrix slide draws between "data plane" and
"lifecycle ops". The sourcing design extends this: with a local write stream on
each side, both sides keep writing through the partition and converge afterwards.

---

## Variations worth showing

- **Lose west instead** (`kill-west.sh`): symmetric, but now `CONFIG` and
  `ORDERS-WEST` are the ones unavailable, and `ORDERS-WEST-DR` in east still serves
  reads of west's data: the mirror doing its job. With 2.12+ it could be promoted.
- **Lose the arbitrator** (`kill-arbitrator.sh`): nothing user-visible changes
  (6 of 7 peers). It only matters when a region is *also* lost: east + arbitrator
  down leaves 3 of 7, no meta leader anywhere, and the data plane still runs on west.
  Recovering from that is the meta-rescue procedure (`nats server cluster rescue`,
  section 0 of the workshop), not covered here.
- **Partition west instead:** `curl -X POST 'http://127.0.0.1:9100/isolate?cluster=west'`.
  Then west (3 peers) loses the control plane and east + arbitrator keep it.

## Cleanup

Every demo script ends by asking whether to clean up everything (stop the servers,
delete their data, remove the contexts) or leave it all running to play more; cleaning up
is the default, and `NOPAUSE=1` runs do it without asking. By hand:

```sh
./teardown-supercluster.sh && ./cleanup-contexts.sh
```
