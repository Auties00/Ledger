#!/usr/bin/env bash
# Usage: BASE_SHA=<sha> [ROUNDS=<n>] [...] ci_compare.sh <head-sha> '<jmh pattern>' ['<jmh args>']
# Dispatches .github/workflows/compare.yml once per OS and prints "<os> <run-id>" per OS on stdout (everything else goes
# to stderr). Environment:
#   ROUNDS     judged rounds of each ref (default 5). Judged compares use an even number: with an odd one base runs one
#              slot earlier on average
#   OSES       the runners (default "ubuntu-latest windows-latest")
#   CI_REF     the branch whose compare.yml runs (default master)
#   TAG        short run name for the title and concurrency group (default: a hash of the base, rounds, pattern, args
#              and the options below); the full pattern and args are in the run's results/meta.json. A tag must name
#              one configuration: the run is found by its exact title
#   HEAD_ARGS  JMH arguments for the head rounds only (head-jmh-args)
#   WARMUP     true (default) or false: the discarded round 0 and ABBA order
#   FRESH      true (default) or false: delete each side's store templates before every round (fresh-templates), so
#              that the rounds of a template-reading workload are independent; without it verdict.py marks those rows
#              TEMPLATE
#   PROFILE    ubuntu async-profiler events, for example cpu,alloc, lock or 'wall;threads=true' (profiling compares
#              only); dispatches ubuntu only. JMH keeps only the last fork of each workload, so pass -f 1
#   RERUN_OF   run ids of the compare this one reruns ("<id> [<id>...]", one per OS): dispatches only their OSes, each
#              pinned to the CPU model its first run measured on (downloaded into LEDGER_CI_DL, where ci_fetch.sh
#              looks too), so that verdict.py pools the runs. Pass the same pattern, args and options as the first run
#   CPU_MODEL  pin every OS to this model instead (a substring, such as "EPYC 7763"); RERUN_OF takes precedence
#   CPU_TRIES  dispatches per OS while the runner lands on another model (default 6); then exit 4
#   CPU_WAIT   minutes to wait for a pinned run to reach its CPU check, queueing included (default 60)
# In the args, @OUT@ is a directory of each side and round, for profiler output: -prof jfr:dir=@OUT@
#
# A run on another model stops in compare.yml's step "Stop on another CPU model" before building anything; this script
# tells that apart from a real failure, dispatches again, and prints the id of the run that matched.
set -euo pipefail
. "$(dirname "$0")/ci_lib.sh"
H=${1:?head sha}; P=${2:?pattern}; A=${3:-}
B=${BASE_SHA:?BASE_SHA required}; R=${ROUNDS:-5}; REF=${CI_REF:-master}
HA=${HEAD_ARGS:-}; WU=${WARMUP:-true}; FT=${FRESH:-true}; PR=${PROFILE:-}
TRIES=${CPU_TRIES:-6}; WAIT_MIN=${CPU_WAIT:-60}
# actions/checkout fetches a full commit id or a ref, never an abbreviated id: expand those (before the tag hash, so that
# a rerun given the same abbreviation gets the same tag)
full_sha() {
  case "$1" in
    *[!0-9a-f]*|'') echo "$1" ;;
    *) if [ ${#1} -ge 40 ]; then echo "$1"; else gh api "repos/$REPO/commits/$1" -q .sha 2>/dev/null || echo "$1"; fi ;;
  esac
}
B=$(full_sha "$B"); H=$(full_sha "$H")
# A command-line -jvmArgsAppend replaces @Fork's JVM arguments; it is let through only when it carries the ones the
# void check of verdict.py and ci_fetch.sh keys on, plus the Unsafe flag
for args in "$A" "$HA"; do
  bad=""
  case "$args" in *-jvmArgsAppend*)
    case "$args" in *--enable-native-access*) ;; *) bad=1;; esac
    case "$args" in *-Xmx4g*) ;; *) bad=1;; esac
    case "$args" in *--sun-misc-unsafe-memory-access*) ;; *) bad=1;; esac
    if [ -n "$bad" ]; then
      echo "use -jvmArgsPrepend: a command-line -jvmArgsAppend replaces @Fork's (-Xms4g -Xmx4g, --enable-native-access, --sun-misc-unsafe-memory-access)" >&2; exit 2
    fi;;
  esac
done
case "$WU" in true|false) ;; *) echo "WARMUP must be true or false" >&2; exit 2;; esac
case "$FT" in true|false) ;; *) echo "FRESH must be true or false" >&2; exit 2;; esac
case "$TRIES" in ''|*[!0-9]*|0) echo "CPU_TRIES must be a positive number" >&2; exit 2;; esac
T=${TAG:-$(printf '%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s' "$B" "$R" "$P" "$A" "$HA" "$WU" "$PR" "$FT" | sha1sum | cut -c1-10)}
echo "tag $T" >&2
oses=${OSES:-ubuntu-latest windows-latest}
if [ -n "$PR" ]; then oses="ubuntu-latest"; echo "PROFILE runs on ubuntu only: other OSes not dispatched" >&2; fi
if [ "$WU" = true ] && [ $((R % 2)) -ne 0 ] && [ -z "$PR" ]; then
  echo "note: ROUNDS=$R is odd; judged compares use an even number, so that ABBA cancels drift" >&2
fi

# The CPU model per OS: from the runs this one reruns, or CPU_MODEL for every OS
declare -A want=()
if [ -n "${RERUN_OF:-}" ]; then
  oses=""
  for id in $RERUN_OF; do
    fetch_run "$id" || { echo "cannot read the CPU model of run $id" >&2; exit 1; }
    found=""
    for sub in "$(run_dir "$id")"/compare-*/; do
      [ -d "$sub" ] || continue
      os=$(basename "$sub"); os=${os#compare-}
      model=$("$PYTHON" -I "$BENCH_DIR/verdict.py" --cpu-of "$sub" | tr -d '\r')
      [ -n "$model" ] || { echo "run $id recorded no CPU model for $os: cannot pin its rerun" >&2; exit 1; }
      want[$os]=$model; oses="$oses $os"; found=1
      echo "rerun of $id on $os: pinned to $model" >&2
    done
    [ -n "$found" ] || { echo "run $id has no compare-<os> artifact" >&2; exit 1; }
  done
elif [ -n "${CPU_MODEL:-}" ]; then
  for os in $oses; do want[$os]=$CPU_MODEL; done
fi

# dispatch <os>: starts one run and prints its id
dispatch() {
  local os=$1 before id sel
  local -a extra=(-f tag="$T" -f warmup-round="$WU" -f fresh-templates="$FT")
  [ -n "$HA" ] && extra+=(-f head-jmh-args="$HA")
  [ -n "$PR" ] && extra+=(-f profile="$PR")
  [ -n "${want[$os]:-}" ] && extra+=(-f cpu-model="${want[$os]}")
  before=$(gh run list -R "$REPO" -w compare.yml -L 30 --json databaseId -q '[.[].databaseId]|map(tostring)|join(" ")')
  gh workflow run compare.yml -R "$REPO" --ref "$REF" -f base="$B" -f head="$H" -f os="$os" -f pattern="$P" \
    -f rounds="$R" -f jmh-args="$A" "${extra[@]}" > /dev/null
  # The title ends in the tag, which names the whole configuration
  sel="(.displayTitle == \"Compare $B and $H on $os tag $T\")"
  id=""
  sleep 15
  for _ in $(seq 1 12); do
    id=$(gh run list -R "$REPO" -w compare.yml -L 30 --json databaseId,displayTitle -q ".[]|select($sel)|.databaseId" \
      | while read -r x; do case " $before " in *" $x "*) ;; *) echo "$x";; esac; done | head -1)
    [ -n "$id" ] && break
    sleep 15
  done
  [ -n "$id" ] || { echo "dispatch not seen for $os" >&2; return 1; }
  echo "$id"
}

declare -A run=() tries=() since=()
for os in $oses; do
  run[$os]=$(dispatch "$os"); tries[$os]=1; since[$os]=$SECONDS
  echo "$os: run ${run[$os]}${want[$os]:+ (wants ${want[$os]})}" >&2
done
# Unpinned runs are printed at once; pinned ones once they pass the CPU check, dispatched again while they do not
status=0
pending=""
for os in $oses; do
  if [ -n "${want[$os]:-}" ]; then pending="$pending $os"; else echo "${os%-latest} ${run[$os]}"; fi
done
while [ -n "${pending// /}" ]; do
  sleep 20
  next=""
  for os in $pending; do
    state=$(cpu_state "${run[$os]}")
    case "$state" in
      match)
        echo "$os: run ${run[$os]} is on ${want[$os]} (dispatch ${tries[$os]})" >&2
        echo "${os%-latest} ${run[$os]}" ;;
      mismatch)
        if [ "${tries[$os]}" -ge "$TRIES" ]; then
          echo "$os: $TRIES dispatches, none on ${want[$os]} (last run ${run[$os]}); giving up" >&2
          status=4
        else
          tries[$os]=$((tries[$os] + 1))
          run[$os]=$(dispatch "$os"); since[$os]=$SECONDS
          echo "$os: another CPU model; dispatch ${tries[$os]} of $TRIES is run ${run[$os]}" >&2
          next="$next $os"
        fi ;;
      failed)
        echo "$os: run ${run[$os]} ended before its CPU check (gh run view -R $REPO ${run[$os]})" >&2
        status=1 ;;
      *)
        if [ $((SECONDS - since[$os])) -ge $((WAIT_MIN * 60)) ]; then
          echo "$os: run ${run[$os]} has not reached its CPU check after $WAIT_MIN minutes; left running" >&2
          status=1
        else
          next="$next $os"
        fi ;;
    esac
  done
  pending=$next
done
exit "$status"
