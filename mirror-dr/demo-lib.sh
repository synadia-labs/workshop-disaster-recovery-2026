# Shared helpers for demo-mirror-dr.sh. Source it, don't run it.
#
#   step "title" cmd...   prints the command, waits for Enter (unless NOPAUSE=1), runs it
#   note "text"           a dim comment line
#   hl REGEX              print stdin in full, the lines matching REGEX in bold
#   pub CTX SUBJECT MSG   publish with a JetStream ack, show which domain/stream/seq stored it
#   state CTX             one line: messages, first/last seq, mirror-of (if any) for ORDERS
#   payload CMD...        run a stream get / consumer next command, print just the payload
#   contents CTX STREAM   every message of a stream, one line each

E="nats --context east"
W="nats --context west"

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

pub() {
  local out
  out=$(nats --context "$1" req "$2" "$3" 2>&1 | grep -v '^$' | tail -1)
  python3 -c '
import sys, json
try:
    a = json.loads(sys.argv[2]); print("%s -> acked by domain %s, stream %s, seq %s" % (sys.argv[1], a.get("domain"), a["stream"], a["seq"]))
except Exception:
    print("%s -> %s" % (sys.argv[1], sys.argv[2]))' "$1:$2" "$out"
}

state() {
  local info
  info=$(nats --context "$1" stream info ORDERS -j 2>/dev/null) || { echo "ORDERS in $1: does not exist"; return; }
  python3 -c '
import sys, json
d = json.loads(sys.argv[2]); s = d["state"]; m = d.get("mirror")
kind = "MIRROR of ORDERS in domain %s, lag %s" % (m["external"]["api"].split(".")[1], m.get("lag")) if m else "regular stream on %s" % ",".join(d["config"].get("subjects") or [])
print("ORDERS in %s: %d messages, seq %d..%d, %s" % (sys.argv[1], s["messages"], s["first_seq"], s["last_seq"], kind))' "$1" "$info"
}

payload() {
  local out
  out=$("$@" 2>&1 | sed $'s/\e\\[[0-9;]*m//g')
  if grep -q '^Data:' <<<"$out"; then
    awk '/^Data:/ {f=1; next} f && NF {print; f=0}' <<<"$out"
  else
    grep -v '^$' <<<"$out" | tail -1
  fi
}

setup() {
  ./teardown-clusters.sh >/dev/null
  ./start-clusters.sh >/dev/null && ./create-contexts.sh >/dev/null
  $E stream add ORDERS --config streams/orders-east.json >/dev/null && echo "ORDERS created in east (R3, orders.>)"
  ./create-mirror.sh west east
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

export -f step note hl pub state payload contents
export E W bold dim green yellow reset hi

# finish: last step of every demo. Clean up (default) or leave everything running to play more.
# NOPAUSE=1 cleans up without asking.
TEARDOWN="./teardown-clusters.sh"
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
