#!/usr/bin/env bash
# Usage: ci_tests.sh <branch>   Dispatches ci.yml (tests and the smoke run on ubuntu, windows, macos) on a pushed branch
# and prints the run id.
set -euo pipefail
. "$(dirname "$0")/ci_lib.sh"
B=${1:?branch}
before=$(gh run list -R "$REPO" -w ci.yml -b "$B" -L 1 --json databaseId -q '.[0].databaseId' 2>/dev/null || true)
gh workflow run ci.yml -R "$REPO" --ref "$B" >/dev/null
for _ in $(seq 1 30); do
  id=$(gh run list -R "$REPO" -w ci.yml -b "$B" -L 1 --json databaseId -q '.[0].databaseId' 2>/dev/null || true)
  if [ -n "$id" ] && [ "$id" != "$before" ]; then echo "$id"; exit 0; fi
  sleep 3
done
echo "dispatch not seen" >&2; exit 1
