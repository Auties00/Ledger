# Comparing two commits on CI

These scripts drive the [Compare workflow](../../.github/workflows/compare.yml), which measures two refs in one job,
in rounds of both, so that both meet the same runner. They need `gh` (logged in), bash 4 or later and Python 3; run
them from anywhere, with their own path.

```shell
BASE_SHA=<base> ROUNDS=6 tools/bench/ci_compare.sh <head> 'cobalt\.MessageLookupBenchmark' '-p engine=ledger'
tools/bench/ci_wait.sh <run id>...            # again until it exits 0
tools/bench/ci_fetch.sh <run id>              # the comparison and the verdict of one run
RERUN_OF="<ubuntu run> <windows run>" BASE_SHA=<base> ROUNDS=6 tools/bench/ci_compare.sh <head> <same pattern> <same args>
tools/bench/ci_fetch.sh <rerun id> <first run id>   # the rerun pooled with the first run
```

- `ci_compare.sh` dispatches one compare per OS (ubuntu and windows unless `OSES` says otherwise) and prints
  `<os> <run id>` per OS. Its header lists the options (`TAG`, `HEAD_ARGS`, `WARMUP`, `FRESH`, `PROFILE`, `CI_REF`,
  `RERUN_OF`, `CPU_MODEL`, `CPU_TRIES`, `CPU_WAIT`); `@OUT@` in the JMH arguments is a directory per side and round,
  for profiler output.
- `ci_wait.sh` waits up to 270 seconds for runs and prints their conclusions, `cpu-mismatch` for a run that stopped on
  another CPU model.
- `ci_fetch.sh` downloads a run's artifacts and runs `ci_cmp.py` (per-round scores and the ratio of medians, above 1
  meaning head is better) and `verdict.py` (WIN, LOSS, NEUTRAL or INCONCLUSIVE per metric against its margin, with the
  full table in `<run dir>/verdict.json`; its docstring has the rules). Further run ids are earlier runs of the same
  compare, pooled into the first.
- `ci_tests.sh <branch>` dispatches the CI workflow on a pushed branch.

Downloads go to `LEDGER_CI_DL`, one directory per run id (default `ledger-ci-dl` under the system temp directory),
and are reused when there. `LEDGER_REPO` names another repository and `PYTHON` another interpreter.

## One CPU model per comparison

Hosted runners land on several CPU models (AMD EPYC 7763, 9V74 and 9V45, Intel Xeon 8370C, 8573C and 6975P-C were all
seen), and verdicts have flipped with the model. One run is consistent, since both refs meet the same machine, but a
rerun pooled with it must run on the same model, or its rows read only as disagreement between runs (`Q!`, `cpu!`).

- With `cpu-model` set, compare.yml reads the runner's CPU before checking anything out and, on another model, fails
  at once in its step "Stop on another CPU model", the only step that fails that way. Every run records its model in
  `results/runner.txt` and `results/meta.json`.
- `ci_compare.sh` with `RERUN_OF` reads each first run's model from its artifact, dispatches the rerun with that model,
  and dispatches again while the runner lands on another one, up to `CPU_TRIES` times (default 6). A run that failed
  for any other reason is reported, not retried.
- `verdict.py` pools only runs on the main run's model. Runs on other models (or on an unknown one) are judged per
  model and printed under `NOT POOLED`; `VERDICT_ARGS=--pool-across-cpus` pools them anyway.

A first run is not pinned: which models a runner pool offers changes over time and differs between ubuntu and windows,
so a fixed model would cost dispatches without making two comparisons comparable with each other (verdicts are only
read within one comparison). `CPU_MODEL` pins a first run when there is a reason to, such as reproducing a result seen
on one model.
