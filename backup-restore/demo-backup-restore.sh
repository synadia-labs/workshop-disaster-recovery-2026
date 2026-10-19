#!/usr/bin/env bash
# Backups with the 2.15 `nats backup` tooling: take, inspect, validate, restore, edit
# (filter, trim, renumber, rename, obfuscate), compact a KV bucket, back up memory streams
# and a whole account.
#   NOPAUSE=1 ./demo-backup-restore.sh   runs straight through
set -uo pipefail
cd "$(dirname "$0")"
source ./demo-lib.sh

step "Fresh cluster with a stream, a consumer that has acked 4 messages, a KV bucket and a memory stream" setup

# ---- take a backup -------------------------------------------------------------------
step "Back up ORDERS, consumers included" $N backup stream ORDERS backups/orders --no-progress
step "A backup is a directory: the stream's config and state, plus an archive where each message is an entry" bash -c "ls -la backups/orders; echo; python3 -m json.tool backups/orders/backup.json | head -12"
step "Inspect it without a server: config, consumers, messages" $N backup info backups/orders
step "...down to the subjects" bash -c "$N backup info backups/orders --subjects | hl '^│ [^ │]+\.'"
step "Validate it: complete and restorable?" $N backup validate backups/orders

# ---- lose it, restore it ------------------------------------------------------------------
step "Disaster: the stream is deleted" bash -c "$N stream rm ORDERS -f && echo 'ORDERS deleted'"
step "Restore it from the backup (a restore always creates the stream; delete first if it exists)" $N backup restore stream backups/orders --no-progress
step "Messages AND consumer state are back: C still has its ack floor at 4" bash -c "
  $N stream info ORDERS | hl 'Messages: [0-9]|Replicas'; $N consumer info ORDERS C | hl 'Acknowledgment Floor|Unprocessed'"

# ---- edit backups ------------------------------------------------------------------------------
step "Edit, 1: keep one region. --dry-run shows what would happen" $N backup edit backups/orders backups/orders-eu --subject 'orders.eu.>' --dry-run
step "Edit, 2: combine filters: a header value and a payload regex" bash -c "
  $N backup edit backups/orders backups/orders-us-rev2 --header Region:us --payload-match 'rev 2' | hl 'Kept|Restored Messages'
  $N backup info backups/orders-us-rev2 --subjects | hl '^│ [^ │]+\.'"
step "Edit, 3: the newest message per subject, renumbered from 1 (renumbering drops the consumers)" bash -c "
  $N backup edit backups/orders backups/orders-latest --last-per-subject 1 --renumber | hl 'Source Messages|Kept|Restored'"
step "Edit, 4: by time or sequence: --after/--before take an RFC3339 time or a duration, --first-seq/--last-seq a sequence" bash -c "
  $N backup edit backups/orders backups/orders-recent --after 1h --last-seq 8 --dry-run | hl 'Kept|Restored (First|Last)'"

step "Restore an edited copy next to the original. First the edit (the eu messages)" bash -c "
  $N backup edit backups/orders backups/orders-eu --subject 'orders.eu.>' | hl 'Kept|Restored Messages'"
note "restore --config cannot rename (it checks the backup's own stream name first), so change the name inside the backup:"
step "The stream config lives in backup.json; give it a new name and subject with jq" bash -c "
  jq '.config.name = \"ORDERS-EU\" | .config.subjects = [\"orders-eu.>\"]' backups/orders-eu/backup.json > backups/orders-eu/backup.json.new &&
  mv backups/orders-eu/backup.json.new backups/orders-eu/backup.json && jq '.config | {name, subjects}' backups/orders-eu/backup.json"
step "Validate the edited backup, restore it: ORDERS-EU appears next to ORDERS" bash -c "
  $N backup validate backups/orders-eu | hl '^OK'
  $N backup restore stream backups/orders-eu --no-progress >/dev/null && $N stream info ORDERS-EU | hl 'Subjects|Messages: [0-9]|Deleted|Last Sequence'"
note "Sequence numbers are preserved (last sequence still 12, the 6 filtered-out ones count as deleted); use --renumber for 1..6."

step "Edit, 5: obfuscate for sharing: names and subjects become keyed hashes, payloads zeros of the same length" bash -c "
  $N backup edit backups/orders backups/orders-obf --obfuscate | hl 'Kept|Restored Messages'; echo; $N backup info backups/orders-obf --subjects | hl 'Name:|Subjects:|Messages: [0-9]|^│ [^ │]+\.'"
step "The key file written beside it maps them back; keep it private" bash -c "
  ls backups | hl keys; name=\$($N backup info backups/orders-obf -j | python3 -c 'import sys,json; print(json.load(sys.stdin)[\"config\"][\"name\"])')
  subj=\$($N backup info backups/orders-obf --subjects | grep -oE '[a-z0-9]{16}\.[a-z0-9]{16}\.[a-z0-9]{16}' | head -1)
  echo \"lookup \$name:\"; $N backup lookup backups/orders-obf.keys.json \"\$name\"; echo \"lookup \$subj:\"; $N backup lookup backups/orders-obf.keys.json \"\$subj\""

# ---- KV --------------------------------------------------------------------------------------------
step "A KV bucket is a stream: back it up with its history and tombstones" bash -c "
  $N backup stream KV_CONFIG backups/kv --no-progress >/dev/null && $N backup info backups/kv --subjects | hl '^│ [^ │]+\.'"
step "--kv-compact keeps the latest revision of each key and drops deleted keys: 6 entries become 2" bash -c "
  $N backup edit backups/kv backups/kv-compact --kv-compact | hl 'Source Messages|Kept'; $N backup info backups/kv-compact --subjects | hl '^│ [^ │]+\.'"
step "Restore the compacted bucket in place of the live one" bash -c "
  $N kv rm CONFIG -f >/dev/null && $N backup restore stream backups/kv-compact --no-progress >/dev/null && $N kv ls CONFIG && $N kv history CONFIG mode"

# ---- memory streams, whole accounts --------------------------------------------------------------------
step "Memory streams can be backed up too (new in 2.15)" bash -c "$N backup stream CACHE backups/cache --no-progress | hl 'Received'; $N backup info backups/cache | hl 'Storage|Messages: [0-9]'"
step "Or everything at once: an account backup is one directory per stream" bash -c "$N backup account backups/account -f | hl 'Starting|Received'; echo; ls backups/account"
note "nats backup restore account backups/account restores them all. Edit, info, validate and lookup only accept backups taken from NATS Server 2.15 or newer."

finish
