# Shared helpers for demo-stretch-and-sourcing.sh. Source it, don't run it.
#
#   step "title" cmd...   prints the command, waits for Enter (unless NOPAUSE=1), runs it
#   note "text"           a dim comment line
#   hl REGEX              print stdin in full, the lines matching REGEX in bold
#   meta SERVER           meta-group summary as that server sees it (nats server request jetstream)
#   moved STREAM CLUSTER  true once every replica of the stream lives in CLUSTER (a stream move is done)
#   stream_leader CTX STREAM  the stream's current leader, or empty
#   replica_current CTX STREAM PEER  true when PEER is a current replica of the stream
#   contents CTX STREAM   every message of a stream, one line each
#   wait_for "desc" cmd   polls cmd (quietly) every 2s for up to 90s until it succeeds
#   counts                message count of the virtual stream as seen from each region
#   pub CTX SUBJECT MSG   publish with a JetStream ack and show which real stream stored it
#   setup                 fresh super-cluster + the six real streams behind 'foo', and ORDERS in east

W="nats --context west"
E="nats --context east"
CE="nats --context central"
ST="nats --context stretch"
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
  printf '%s\n' "$*" | sed "1s/^/${yellow}\$ /;\$s/\$/${reset}/"
  if [[ -z ${NOPAUSE:-} ]]; then read -r -p "${dim}[enter]${reset} "; fi
  "$@"
}
note() { echo "${dim}-- $*${reset}"; }

# sys_ctx SERVER: the system-account context on that server's side of the super-cluster
sys_ctx() { case $1 in west-*) echo west-sys;; east-*) echo east-sys;; central-*) echo central-sys;; *) echo stretch-sys;; esac; }

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

wait_for() {
  local desc=$1; shift
  local t=$SECONDS
  until "$@" >/dev/null 2>&1; do
    if ((SECONDS - t > 90)); then echo "gave up waiting for $desc"; return 1; fi
    sleep 2
  done
  echo "$desc after ~$((SECONDS - t))s"
}

# messages CTX STREAM: a stream's message count, or "-" when it does not answer
messages() {
  nats --context "$1" stream info "$2" -j 2>/dev/null | python3 -c 'import sys,json; print(json.load(sys.stdin)["state"]["messages"])' 2>/dev/null || echo "-"
}

# subjects CTX: the sorted subjects of the virtual stream as seen from a region
subjects() {
  nats --context "$1" stream subjects foo -j 2>/dev/null | python3 -c 'import sys,json; print(" ".join(sorted(json.load(sys.stdin))))' 2>/dev/null
}

# the virtual stream 'foo' as each region sees it (its local read stream)
counts() {
  local r
  for r in west east central; do echo "foo from $r: $(messages "$r" foo) messages: $(subjects "$r")"; done
}

# converged: true when all three read streams hold the same set of subjects
converged() { local w; w=$(subjects west); [[ -n $w && $w == "$(subjects east)" && $w == "$(subjects central)" ]]; }

# stream_leader CTX STREAM: the stream's current leader as seen from that context (empty when none / no answer)
stream_leader() {
  nats --context "$1" stream info "$2" -j --timeout 2s 2>/dev/null | python3 -c 'import sys,json; print((json.load(sys.stdin).get("cluster") or {}).get("leader") or "")' 2>/dev/null
}

# replica_current CTX STREAM PEER: true when PEER is listed as a current replica of the stream
replica_current() {
  nats --context "$1" stream info "$2" -j 2>/dev/null | python3 -c '
import sys, json
c = json.load(sys.stdin).get("cluster") or {}
sys.exit(0 if any(r.get("name") == sys.argv[1] and r.get("current") for r in c.get("replicas") or []) else 1)' "$3"
}

# moved STREAM CLUSTER: true once the stream's leader and every replica live in CLUSTER and are current
moved() {
  nats --context west stream info "$1" -j 2>/dev/null | python3 -c '
import sys, json
c = (json.load(sys.stdin).get("cluster") or {}); p = sys.argv[1] + "-"
reps = c.get("replicas") or []
ok = c.get("name") == sys.argv[1] and (c.get("leader") or "").startswith(p) and len(reps) == 2 and all(r.get("current") and r["name"].startswith(p) for r in reps)
sys.exit(0 if ok else 1)' "$2"
}

# pub CTX SUBJECT MSG: publish via the virtual subject, show the PubAck's stream and sequence
pub() {
  local out
  out=$(nats --context "$1" req "$2" "$3" 2>&1 | grep -v '^$' | tail -1)
  python3 -c '
import sys, json
try:
    a = json.loads(sys.argv[2]); print("%s -> stored in %s as seq %s" % (sys.argv[1], a["stream"], a["seq"]))
except Exception:
    print("%s -> %s" % (sys.argv[1], sys.argv[2]))' "$1:$2" "$out"
}

setup() {
  ./teardown-supercluster.sh >/dev/null
  ./start-supercluster.sh >/dev/null && ./create-contexts.sh >/dev/null
  ./define-streams.sh
}

# payload CMD...: run a consumer next / stream get command and print just the message payloads
# (the CLI colors its labels even without a TTY, so strip ANSI codes before matching)
payload() {
  local out
  out=$("$@" 2>&1 | sed $'s/\e\\[[0-9;]*m//g')
  if grep -q '^Data:' <<<"$out"; then
    awk '/^Data:/ {f=1; next} f && NF {print; f=0}' <<<"$out"   # first non-empty line after each Data:
  else
    grep -v '^$' <<<"$out" | tail -1     # stream get: the payload is the last line
  fi
}

# contents CTX STREAM: every message of a stream as seen from that context, one line each
# (stream sequence, subject, headers if any, payload), read through a throwaway pull consumer.
# The internal Nats-Stream-Source header that sourcing adds is left out.
# (A pull consumer rather than `nats sub --stream`: through the virtual name 'foo' the latter
# fails, its ordered-consumer request carries stream_name "foo" in the body while the mapped
# subject names foo-read-<region>, and the server rejects the mismatch with error 10056.)
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

export -f step note hl sys_ctx jsz meta meta_leader wait_for messages subjects counts converged stream_leader replica_current moved wait_for_meta_leader pub payload contents
export W E CE ST WSYS ESYS bold dim green yellow reset hi

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
