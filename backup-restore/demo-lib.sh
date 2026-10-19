# Shared helpers for demo-backup-restore.sh. Source it, don't run it.
#
#   step "title" cmd...   prints the command, waits for Enter (unless NOPAUSE=1), runs it
#   note "text"           a dim comment line
#   hl REGEX              print stdin in full, the lines matching REGEX in bold
#   N ...                 nats CLI on the demo cluster
#   setup                 fresh cluster + the streams used by the demo
#   contents CTX STREAM   every message of a stream, one line each

N="nats --context backup-demo"

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

# Data used by the demo:
#   ORDERS     R3 file stream on orders.>: 12 messages, 6 subjects x 2 revisions, header Region: eu|us
#              pull consumer C that has acked the first 4 messages
#   CONFIG     KV bucket (history 5): key mode written 3 times, key tmp written then deleted, key region
#   CACHE      R3 MEMORY stream with one message (memory streams are backed up too)
setup() {
  ./teardown-cluster.sh >/dev/null; rm -rf backups
  ./start-cluster.sh >/dev/null && ./create-contexts.sh >/dev/null
  $N stream add ORDERS --subjects 'orders.>' --replicas 3 --storage file --defaults >/dev/null
  for r in 1 2; do for i in 1 2 3; do
    $N pub orders.eu.$i "eu order $i rev $r" -H Region:eu >/dev/null
    $N pub orders.us.$i "us order $i rev $r" -H Region:us >/dev/null
  done; done
  $N consumer add ORDERS C --pull --ack explicit --deliver all --defaults >/dev/null
  $N consumer next ORDERS C --count 4 >/dev/null 2>&1
  $N kv add CONFIG --history 5 --replicas 3 >/dev/null
  for v in 1 2 3; do $N kv put CONFIG mode v$v >/dev/null; done
  $N kv put CONFIG tmp x >/dev/null; $N kv del CONFIG tmp -f >/dev/null; $N kv put CONFIG region eu >/dev/null
  $N stream add CACHE --subjects 'cache.>' --replicas 3 --storage memory --defaults >/dev/null
  $N pub cache.a 'cached value' >/dev/null
  mkdir -p backups
  $N stream report 2>&1
  echo; contents backup-demo ORDERS
  echo; $N consumer info ORDERS C | hl 'Acknowledgment Floor|Unprocessed'
  echo; $N kv ls CONFIG -v --display-value
  echo "...and the stream behind the bucket, history and tombstone included:"; contents backup-demo KV_CONFIG
  echo; contents backup-demo CACHE
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

export -f step note hl contents setup
export N bold dim green yellow reset hi

# finish: last step of every demo. Clean up (default) or leave everything running to play more.
# NOPAUSE=1 cleans up without asking.
TEARDOWN="./teardown-cluster.sh && rm -rf backups"
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
