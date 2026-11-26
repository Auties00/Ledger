#!/usr/bin/env bash
# Usage: ci_wait.sh <run-id>...   Polls for up to WAIT_SECONDS (default 270). Exit 0 when all runs completed (prints
# "<id> <conclusion>", with "cpu-mismatch" for a compare that stopped on another CPU model), exit 3 if any is still
# running: call it again until it returns 0 (sized to fit a 5-minute tool timeout).
. "$(dirname "$0")/ci_lib.sh"
end=$((SECONDS + ${WAIT_SECONDS:-270}))
while :; do
  pending=0; out=""
  lines=$(gh run list -R "$REPO" -L 100 --json databaseId,status,conclusion -q '.[]|"\(.databaseId) \(.status) \(.conclusion)"' 2>/dev/null)
  for id in "$@"; do
    s=$(grep "^$id " <<<"$lines" | head -1 | cut -d' ' -f2-)
    [ -n "$s" ] || s=$(gh run view -R "$REPO" "$id" --json status,conclusion -q '.status+" "+.conclusion' 2>/dev/null)
    case "$s" in
      "completed failure")
        if [ "$(cpu_state "$id")" = mismatch ]; then out+="$id cpu-mismatch"$'\n'; else out+="$id failure"$'\n'; fi ;;
      completed*) out+="$id ${s#completed }"$'\n' ;;
      *) pending=1 ;;
    esac
  done
  [ $pending -eq 0 ] && { printf "%s" "$out"; exit 0; }
  [ $SECONDS -ge $end ] && { printf "%s" "$out"; echo "still running"; exit 3; }
  sleep 60
done
