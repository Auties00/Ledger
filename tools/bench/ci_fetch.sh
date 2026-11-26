#!/usr/bin/env bash
# Usage: ci_fetch.sh <compare-run-id> [<earlier run id of the same compare>...]
# Downloads the artifacts into LEDGER_CI_DL/<run-id> (see ci_lib.sh) and prints the comparison (ci_cmp.py: per-round
# scores, ratio of medians where above 1 means head is better, plus memory/allocation secondary metrics), then the
# verdict analyzer (verdict.py: WIN/LOSS/NEUTRAL/INCONCLUSIVE per metric, flagged rows only; full table in
# <run dir>/verdict.json). Extra run ids are earlier runs of the same compare: verdict.py pools those on the same CPU
# model into this run and judges the others separately (VERDICT_ARGS=--pool-across-cpus pools them all). A pooled run
# that cannot be downloaded stops the script before anything is judged (exit 1); so does a run that stopped on
# another CPU model, which measured nothing.
. "$(dirname "$0")/ci_lib.sh"
mismatch() {
  if [ "$(cpu_state "$1")" = mismatch ]; then
    echo "run $1 stopped on another CPU model and measured nothing; pass the run that matched" >&2
    return 0
  fi
  return 1
}
id=${1:?run id}; shift; d=$(run_dir "$id")
echo "artifacts: $d"
mismatch "$id" && exit 1
fetch_run "$id" || exit 1
gh run view -R "$REPO" "$id" --json conclusion,displayTitle -q '.displayTitle+" -> "+.conclusion'
# @Fork carries --enable-native-access, --sun-misc-unsafe-memory-access and -Xmx4g in every workload, plus -Xms4g (or
# -Xms64m for the memory workloads); a command-line -jvmArgsAppend replaces them all.
find "$d" -name '*.log' -exec grep -H '^# VM options:' {} + 2>/dev/null \
  | awk '!(/--enable-native-access/ && /--sun-misc-unsafe-memory-access/ && /-Xmx4g/ && (/-Xms4g/ || /-Xms64m/))' \
  | head -3 | sed 's/^/WARNING forks without @Fork args (run void for judging): /'
for sub in "$d"/*/; do [ -d "$sub" ] && "$PYTHON" -I "$BENCH_DIR/ci_cmp.py" "$sub"; done
pool=()
for p in "$@"; do
  mismatch "$p" && exit 1
  fetch_run "$p" || { echo "WARNING pooled run $p is not available: nothing judged" >&2; exit 1; }
  pool+=("$(run_dir "$p")")
done
for sub in "$d"/compare-*/; do
  [ -d "$sub" ] || continue
  # VERDICT_ARGS is split into words on purpose
  if [ ${#pool[@]} -gt 0 ]; then
    "$PYTHON" -I "$BENCH_DIR/verdict.py" "$sub" ${VERDICT_ARGS:-} --pool "${pool[@]}"
  else
    "$PYTHON" -I "$BENCH_DIR/verdict.py" "$sub" ${VERDICT_ARGS:-}
  fi
done
