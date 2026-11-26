# Shared by the ci_*.sh scripts (sourced, not run).
#   LEDGER_REPO    the GitHub repository the workflows run in (default Auties00/Ledger)
#   LEDGER_CI_DL   where run artifacts are downloaded, one directory per run id (default: ledger-ci-dl under the
#                  system temp directory)
#   PYTHON         the Python 3 interpreter (default: the first of python3 and python that runs)
BENCH_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=${LEDGER_REPO:-Auties00/Ledger}
DL=${LEDGER_CI_DL:-${TMPDIR:-${TEMP:-/tmp}}/ledger-ci-dl}
# The cpu-model check of compare.yml: the step that fails only when the runner has another CPU model
CPU_STOP_STEP="Stop on another CPU model"

if [ -z "${PYTHON:-}" ]; then
  # On Windows, python3 may be the Store's placeholder, which runs nothing
  for candidate in python3 python; do
    if "$candidate" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 8) else 1)' > /dev/null 2>&1; then
      PYTHON=$candidate
      break
    fi
  done
fi
: "${PYTHON:?no Python 3 found: set PYTHON}"

# run_dir <run-id>: where the artifacts of a run are (or will be) downloaded
run_dir() { echo "$DL/$1"; }

# fetch_run <run-id>: downloads the run's artifacts into run_dir once; fails when the run has none
fetch_run() {
  local d
  d=$(run_dir "$1")
  [ -d "$d" ] && return 0
  mkdir -p "$DL"
  if gh run download -R "$REPO" "$1" -D "$d.partial" > /dev/null 2>&1; then
    mv "$d.partial" "$d"
    return 0
  fi
  rm -rf "${d:?}.partial"
  echo "download of $1 failed (run may have failed: gh run view -R $REPO $1 --log-failed)" >&2
  return 1
}

# cpu_state <run-id>: the cpu-model check of a compare run, one of
#   match     the runner has the wanted model, or none was asked for (the build has started or the run is done)
#   mismatch  the run stopped in CPU_STOP_STEP: another model, nothing measured
#   failed    the run ended without reaching the check (cancelled, or a failure of the runner)
#   pending   queued, or the check has not run yet
cpu_state() {
  gh run view -R "$REPO" "$1" --json status,jobs -q "
    (.jobs[0] // {}) as \$j | (\$j.steps // []) as \$s | (\$s | map(.name) | index(\"$CPU_STOP_STEP\")) as \$i |
    if \$i == null then (if .status == \"completed\" then \"failed\" else \"pending\" end)
    elif \$s[\$i].conclusion == \"failure\" then \"mismatch\"
    elif \$s[\$i].conclusion == \"skipped\" or \$s[\$i].conclusion == \"success\"
      or ([\$s[\$i + 1:][] | select(.status == \"in_progress\" or .status == \"completed\")] | length > 0) then \"match\"
    elif .status == \"completed\" then \"failed\"
    else \"pending\" end" 2> /dev/null || echo pending
}
