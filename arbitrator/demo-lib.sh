# Shared helpers for the demo-*.sh scripts. Source it, don't run it.
#
#   step "title" cmd...   prints the command, waits for Enter (unless NOPAUSE=1), runs it
#   note "text"           a dim comment line
#   hl REGEX              print stdin in full, the lines matching REGEX in bold
#   meta SERVER           meta-group summary as that server sees it (nats server request jetstream)
#   contents CTX STREAM   every message of a stream, one line each
#   wait_for "desc" cmd   polls cmd (quietly) every 2s for up to 90s until it succeeds
#   setup_assets          fresh super-cluster + the streams used by every scenario

W="nats --context west"
E="nats --context east"
WSYS="nats --context west-sys"
ESYS="nats --context east-sys"

bold=$'\e[1m' dim=$'\e[2m' green=$'\e[32m' yellow=$'\e[33m' reset=$'\e[0m'
hi=$'\e[1m'   # how hl marks the relevant lines

# hl REGEX: print stdin in full, with the lines matching REGEX (an ERE) highlighted.
# The CLI's own colors are stripped first so one of its reset codes cannot end the highlight mid-line.
hl() {
  local line
  sed $'s/\e\\[[0-9;]*m//g' | while IFS= read -r line; do
    if [[ $line =~ $1 ]]; then printf '%s%s%s\n' "$hi" "$line" "$reset"; else printf '%s\n' "$line"; fi
  done
}

step() {
  local title=$1; shift
  echo; echo "${bold}${green}== $title${reset}"
  # show the command without the bodies of helper functions passed via declare -f
  printf '%s\n' "$*" | grep -vE '^    |^[a-z_]+ \(\) ?$|^[{}];?$' | sed "1s/^/${yellow}\$ /;\$s/\$/${reset}/"
  if [[ -z ${NOPAUSE:-} ]]; then read -r -p "${dim}[enter]${reset} "; fi
  "$@"
}
note() { echo "${dim}-- $*${reset}"; }

# sys_ctx SERVER: the system-account context on that server's side of the super-cluster
sys_ctx() { case $1 in west-*) echo west-sys;; east-*) echo east-sys;; *) echo arbitrator-sys;; esac; }

# jsz SERVER: that server's JetStream info (what /jsz shows), asked with `nats server request
# jetstream` through the system account of its own cluster, so it still answers during a partition
jsz() { nats --context "$(sys_ctx "$1")" server request jetstream --name "$1" --timeout 3s 2>/dev/null; }

# meta SERVER: the meta group as that server sees it
meta() {
  jsz "$1" | python3 -c '
import sys, json
raw = sys.stdin.read().strip()
if not raw: print("as seen by %s: no answer" % sys.argv[1]); sys.exit()
m = json.loads(raw)["data"].get("meta_cluster") or {}
print("as seen by %s: meta leader %s, %s peers, quorum %s" % (sys.argv[1], m.get("leader") or "NONE", m.get("cluster_size"), m.get("quorum_needed")))' "$1"
}

# meta_leader SERVER: the meta leader's name as that server sees it (empty when none, or no answer)
meta_leader() {
  jsz "$1" | python3 -c '
import sys, json
raw = sys.stdin.read().strip()
print(((json.loads(raw)["data"].get("meta_cluster") or {}).get("leader") or "") if raw else "")'
}

# wait_for_meta_leader SERVER [NOT_PREFIX]: until that server sees a meta leader, optionally one
# whose name does not start with NOT_PREFIX (right after a kill, the old leader is still named
# for a second or two).
wait_for_meta_leader() {
  wait_for "meta leader seen by $1" bash -c "l=\$(meta_leader '$1'); [[ -n \$l && \$l != '${2:-#}'* ]]"
}

# mirror_lag CTX STREAM: a mirror's message count and what it knows about its origin.
# During a partition 'lag' stays at 0 (it cannot learn about new origin messages); only
# 'active' (time since the last message from the origin) keeps growing.
mirror_lag() {
  local info
  # short timeout: this is also used to probe a mirror whose region is down, where the request hangs
  info=$(nats --context "$1" --timeout 3s stream info "$2" -j 2>/dev/null) || { echo "$2: unavailable (no replica answering)"; return; }
  python3 -c '
import sys, json
d = json.loads(sys.argv[1]); m = d.get("mirror") or {}
print("%s: %d messages, mirror lag %s, active %.1fs ago" % (d["config"]["name"], d["state"]["messages"], m.get("lag"), (m.get("active") or 0) / 1e9))' "$info"
}

# messages CTX STREAM: a stream's message count
messages() {
  nats --context "$1" stream info "$2" -j 2>/dev/null | python3 -c 'import sys,json; print(json.load(sys.stdin)["state"]["messages"])'
}

# mirror_caught_up MIRROR_CTX MIRROR ORIGIN_CTX ORIGIN: true when both hold the same number of messages
mirror_caught_up() {
  local a b
  a=$(messages "$1" "$2") && b=$(messages "$3" "$4") && [[ $a == "$b" ]]
}

# contents CTX STREAM: every message of a stream as seen from that context, one line each
# (stream sequence, subject, headers if any, payload), read through a throwaway pull consumer.
# The internal Nats-Stream-Source header that sourcing adds is left out.
contents() {
  local n name
  n=$(nats --context "$1" stream info "$2" -j 2>/dev/null | python3 -c 'import sys,json; print(json.load(sys.stdin)["state"]["messages"])' 2>/dev/null) || { echo "$2 from $1: no answer"; return; }
  echo "$2 from $1: $n message$([[ $n == 1 ]] || echo s)"
  ((n > 0)) || return 0
  name=$(nats --context "$1" consumer add "$2" --pull --ephemeral --deliver all --ack none --defaults 2>/dev/null | awk '/^ +Name:/ {print $2; exit}')
  [[ -n $name ]] || { echo "  (could not create a consumer to read it)"; return; }
  nats --context "$1" consumer next "$2" "$name" --count "$n" 2>&1 | sed $'s/\e\\[[0-9;]*m//g' | awk '
    function flush() { if (seq != "") printf "  seq %-3s %-22s %s%s\n", seq, subj, (hdr != "" ? "[" hdr "] " : ""), data }
    /^\[.*subj: / { flush(); match($0, /subj: [^ ]+/); subj = substr($0, RSTART+6, RLENGTH-6); match($0, /str seq: [0-9]+/); seq = substr($0, RSTART+9, RLENGTH-9); hdr = ""; data = ""; mode = "body"; next }
    /^Headers:/ { mode = "hdr"; next }
    /^Data:/ { mode = "data"; next }
    /^Acknowledged message/ { mode = "done"; next }
    mode == "hdr" && NF { h = $0; sub(/^ +/, "", h); if (h !~ /^Nats-Stream-Source:/) hdr = hdr (hdr != "" ? ", " : "") h; next }
    (mode == "body" || mode == "data") && NF && data == "" { data = $0; next }
    END { flush() }'
  nats --context "$1" consumer rm "$2" "$name" -f >/dev/null 2>&1
}

wait_for() {
  local desc=$1; shift
  local t=$SECONDS
  until "$@" >/dev/null 2>&1; do
    if ((SECONDS - t > 90)); then echo "gave up waiting for $desc"; return 1; fi
    sleep 2
  done
  echo "$desc after ~$((SECONDS - t))s"
}

# Streams used by the scenarios:
#   ORDERS-WEST     R3 in west, consumer C          CONFIG  KV, R3 in west
#   ORDERS-EAST     R3 in east, consumer C
#   ORDERS-WEST-DR  R3 in east, durable mirror of ORDERS-WEST (the DR copy of west's data) through the
#                   flow-control consumer 'mirror-east' on ORDERS-WEST (streams/orders-west-dr.json)
#   plus a core NATS responder on 'east.svc' connected to east
setup_assets() {
  ./teardown-supercluster.sh >/dev/null
  ./start-supercluster.sh >/dev/null && ./create-contexts.sh >/dev/null
  $W stream add ORDERS-WEST --subjects 'orders.west.>' --replicas 3 --defaults >/dev/null
  $W consumer add ORDERS-WEST C --pull --ack explicit --deliver all --defaults >/dev/null
  $W kv add CONFIG --replicas 3 >/dev/null && $W kv put CONFIG mode normal >/dev/null
  $E stream add ORDERS-EAST --subjects 'orders.east.>' --replicas 3 --defaults >/dev/null
  $E consumer add ORDERS-EAST C --pull --ack explicit --deliver all --defaults >/dev/null
  $W consumer add ORDERS-WEST mirror-east --target _mirror.ORDERS-WEST.east --ack flow_control --flow-control --heartbeat 1s --deliver all --defaults >/dev/null
  $E stream add ORDERS-WEST-DR --config streams/orders-west-dr.json >/dev/null
  for i in 1 2 3; do $W pub orders.west.$i "west order $i" >/dev/null; $E pub orders.east.$i "east order $i" >/dev/null; done
  nohup nats --context east reply east.svc "hello from east" >/dev/null 2>&1 &
  echo $! >data/east-svc.pid
  wait_for "mirror in sync" mirror_caught_up east ORDERS-WEST-DR west ORDERS-WEST >/dev/null
  $W stream report 2>&1
  echo; contents west ORDERS-WEST; contents east ORDERS-EAST; contents east ORDERS-WEST-DR
  echo; $W kv ls CONFIG -v --display-value
}

# make the helpers available inside `step ... bash -c "..."`
export -f step note hl sys_ctx jsz meta meta_leader mirror_lag contents wait_for wait_for_meta_leader messages mirror_caught_up
export W E WSYS ESYS bold dim green yellow reset hi

# finish: last step of every demo. Clean up (default) or leave everything running to play more.
# NOPAUSE=1 cleans up without asking.
TEARDOWN="./teardown-supercluster.sh"
finish() {
  local answer=y
  echo
  if [[ -z ${NOPAUSE:-} ]]; then
    echo "${bold}Do you want to clean up everything (stop the servers, delete their data, remove the contexts),${reset}"
    echo "${bold}or leave the servers running and the contexts defined to play more with the demo?${reset}"
    read -r -p "Clean up now? [Y/n] " answer
  fi
  case ${answer:-y} in
    [Nn]*)
      echo "Leaving everything running. When you are done:"
      echo "  cd $PWD && $TEARDOWN && ./cleanup-contexts.sh"
      ;;
    *)
      eval "$TEARDOWN" && ./cleanup-contexts.sh
      ;;
  esac
}
export -f finish
export TEARDOWN
