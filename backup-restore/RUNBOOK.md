# Backup and restore with `nats backup` (NATS 2.15)

Walkthrough for `demo-backup-restore.sh`, on a plain 3-node cluster. Everything below was
run here (nats-server 2.16-dev, `nats` CLI with the `backup` command tree).

Why this belongs in a DR workshop: replication (mirrors, sourcing, stretch clusters)
copies a bad publisher's data and an accidental purge just as faithfully as good data.
A backup is the only thing that protects against logical errors, and the only copy
that lives outside the system.

## Setup

| what                | value                                                       |
|---------------------|-------------------------------------------------------------|
| cluster `demo`      | n1, n2, n3; clients 4101–4103; monitoring 8101–8103         |
| contexts            | `backup-demo` (user `app`), `backup-demo-sys` (system)      |
| `ORDERS`            | R3 file stream on `orders.>`: 12 messages, 6 subjects × 2 revisions, header `Region: eu|us`; pull consumer `C` with 4 messages acked |
| `CONFIG` (KV)       | history 5: `mode` written 3 times, `tmp` written then deleted, `region` |
| `CACHE`             | R3 **memory** stream, 1 message                             |
| `backups/`          | where the demo writes its backups (kept after teardown)     |

| script                                      | what it does                                  |
|---------------------------------------------|-----------------------------------------------|
| `start-cluster.sh` / `teardown-cluster.sh`  | the 3 servers (teardown deletes `data/`)      |
| `create-contexts.sh` / `cleanup-contexts.sh`| the 2 contexts                                |
| `demo-backup-restore.sh`                    | the walkthrough (Enter between steps; `NOPAUSE=1` runs through) |
| `demo-lib.sh`                               | helpers and the data setup                    |

## The command tree

```
nats backup stream   <stream> <dir>      back up one stream (consumers included by default)
nats backup account  <dir>               back up every stream of the account, one subdirectory each
nats backup restore  stream|account ...  restore over the NATS network
nats backup info     <dir> [--subjects]  configuration, consumers, messages, declared state
nats backup validate <dir>               complete and restorable?
nats backup edit     <src> <dst> [...]   write a filtered/transformed copy
nats backup lookup   <keyfile> <value>   de-obfuscate names and subjects
```

`info`, `validate`, `edit` and `lookup` work on the directory alone, no server needed,
and only accept backups taken from NATS Server 2.15 or newer. Older backups restore,
nothing more (one-way compatibility).

## Walkthrough

### 1. Take a backup

```sh
nats backup stream ORDERS backups/orders
```

The result is a directory: `backup.json` (the stream's config and declared state) and
`stream.arc.s2`, a compressed archive with one entry per message plus the consumers.
For ORDERS: 15 entries = 12 messages + 1 consumer + bookkeeping, 781 B.

```sh
nats backup info backups/orders              # config, consumers (C), 12 messages, seq 1..12
nats backup info backups/orders --subjects   # the 6 subjects with their counts
nats backup validate backups/orders          # OK: 15 entries, 1 consumers, 12 messages, 6 subjects, sequences 1 to 12
```

### 2. Lose it, restore it

```sh
nats stream rm ORDERS -f
nats backup restore stream backups/orders
```

Restore creates the stream; if it still exists, delete it first (or restore under
another name, below). Afterwards `ORDERS` has its 12 messages **and** consumer `C`
is back with its acknowledgment floor at 4: the backup includes the consumers, even
R1 consumers on another node than the stream leader, which the old snapshot API got
wrong. `restore stream` can also change placement on the way in: `--cluster`,
`--tag`, `--replicas`.

### 3. Edit backups

`nats backup edit <src> <dst>` writes a new backup; the source is never touched.
Every run prints a summary (source, kept, dropped by which filter, restored sequence
range). `--dry-run` prints it without writing.

| option                                   | demo                                             | result        |
|------------------------------------------|--------------------------------------------------|---------------|
| `--subject` / `--exclude-subject`        | `--subject 'orders.eu.>'`                        | 6 of 12       |
| `--header NAME[:VALUE]` + `--payload-match REGEX` | `--header Region:us --payload-match 'rev 2'` | 3            |
| `--last-per-subject N`                   | `--last-per-subject 1`                           | 6 (one per subject) |
| `--renumber`                             | with the above                                   | seq 1..6, **consumers dropped** |
| `--after` / `--before` (RFC3339 or duration), `--first-seq` / `--last-seq` | `--after 1h --last-seq 8` | 8 |
| `--kv-compact`                           | on the KV backup                                 | 6 → 2 (see §4) |
| `--obfuscate` (+ `--obfuscation-key`)    | see §3b                                          |               |

Without `--renumber`, kept messages keep their original sequence numbers; the
filtered-out ones show up as *deleted* after a restore (`Messages: 6, Last Sequence:
12, Deleted Messages: 6`). That keeps consumer positions valid. With `--renumber` the
copy starts at 1 and consumers are dropped, since their positions would be meaningless.

#### 3a. Restore an edited copy next to the original

`restore stream --config` cannot rename: it checks the backup's own stream name before
applying the config and fails with `Stream "ORDERS" already exist`. Change the name
inside the backup instead: the stream config is plain JSON in `backup.json`.

```sh
nats backup edit backups/orders backups/orders-eu --subject 'orders.eu.>'
jq '.config.name = "ORDERS-EU" | .config.subjects = ["orders-eu.>"]' \
   backups/orders-eu/backup.json > backups/orders-eu/backup.json.new \
   && mv backups/orders-eu/backup.json.new backups/orders-eu/backup.json
nats backup validate backups/orders-eu
nats backup restore stream backups/orders-eu          # ORDERS-EU, 6 messages, subject orders-eu.>
```

Anything in the config can be changed the same way (replicas, limits, storage); the
archive only carries the messages and consumers.

#### 3b. Obfuscate for sharing

```sh
nats backup edit backups/orders backups/orders-obf --obfuscate
```

Stream name, subjects (token by token) and headers become keyed hashes; payloads
become zeros of the same length; counter values are kept. The structure, sizes and
sequence numbers survive, so the copy is good for support cases and reproductions. The
key file is written beside the target (`backups/orders-obf.keys.json`: a secret plus
the hash → original map). Share the backup, keep the key file private.

```sh
nats backup lookup backups/orders-obf.keys.json 02q5ppp8qje8e2n4                        # ORDERS
nats backup lookup backups/orders-obf.keys.json 8pautp7t9ch3vl52.1p4f6uegibm2fhih.1nmhk2o9krpn3ltk   # orders.eu.1
```

`--obfuscation-key FILE` reuses an earlier key file, so several backups of the same
system obfuscate consistently.

### 4. KV buckets

A bucket is a stream (`KV_CONFIG`), so it is backed up like one, history and tombstones
included: 6 entries (`mode` × 3, `tmp` × 2 of which one is the delete marker, `region`).

```sh
nats backup stream KV_CONFIG backups/kv
nats backup edit backups/kv backups/kv-compact --kv-compact     # 6 -> 2: latest mode, region; tmp gone
nats kv rm CONFIG -f && nats backup restore stream backups/kv-compact
nats kv history CONFIG mode                                    # one revision
```

`--kv-compact` is KV-only and exclusive with `--last-per-subject`. A compacted restore
is how you shrink a bucket's history or drop deleted keys for good.

### 5. Memory streams and whole accounts

```sh
nats backup stream CACHE backups/cache        # Storage: Memory. New in 2.15; the old snapshot API refused these.
nats backup account backups/account -f        # one subdirectory per stream: CACHE, KV_CONFIG, ORDERS, ORDERS-EU
nats backup restore account backups/account   # restores them all (--cluster / --tag to re-place)
```

## Talking points

- RPO of a backup is the backup interval; RTO is restore time (size dependent, ~1 s
  here) plus the delete of whatever is in the way.
- Consumers travel with the stream: a restored application resumes where it was.
- `edit` turns a backup into a surgical tool: drop a bad publisher's messages by
  header or payload before restoring, cut a stream to a time window, or hand an
  obfuscated copy to support.
- Planned, per the outline: incremental backups and S3-type targets.

## Cleanup

Every demo script ends by asking whether to clean up everything (stop the servers,
delete their data, remove the contexts) or leave it all running to play more; cleaning up
is the default, and `NOPAUSE=1` runs do it without asking. By hand:

```sh
./teardown-cluster.sh && ./cleanup-contexts.sh      # backups/ is left for inspection
```
