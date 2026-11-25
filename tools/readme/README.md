# README benchmark charts

The Performance section of the root README is generated from the artifacts of the
[Benchmark workflow](../../.github/workflows/benchmark.yml). Two commands regenerate it:

```shell
python tools/readme/fetch.py <run id>...
python tools/readme/charts.py
```

1. Run the Benchmark workflow on the commit to measure, on every OS and group:
   `gh workflow run benchmark.yml -R Auties00/Ledger --ref master -f os=all -f group=all -f rounds=3`.
   A run of `os=all` takes several hours; the jobs run side by side.
2. `fetch.py` takes the ids of the completed runs (one run per OS and group, so a run of `os=all -f group=all` alone,
   or one run per group and OS; `RUN_ID:commit,ingest` takes only those groups of a run, so that a later run can
   measure the others), downloads their `results-<os>-<group>` artifacts with `gh` (into a new temporary directory, or
   the one `--downloads DIR` names, one subdirectory per run, reused when already there) and writes
   [`benchmarks.tsv`](benchmarks.tsv). Each result is the round with the median score, as the workflow's
   `merge_rounds.py` picks it, and the counters are divided back to their value per measurement iteration, as
   `summarize.py` divides them. The header names the commit, the runner, its CPU model, the JMH arguments and the run
   of every job, and the groups some OS has no data for. A run dispatched with JMH arguments (`-e`, `-p engine=...`, a
   profiler) measured part of the suite or measured it differently, so `fetch.py` refuses it unless `--partial` is
   given.
3. `charts.py` reads `benchmarks.tsv` and [`charts.tsv`](charts.tsv), which picks the rows each chart shows, writes
   `.github/assets/bench-<chart>-<os>-light.svg` and `-dark.svg` for every chart and OS with data, and rewrites the
   block between `<!-- bench:begin -->` and `<!-- bench:end -->` of the root README: per chart its heading, caption
   and Linux chart, the Windows and macOS charts in one `<details>`, and, under a chart with `loss` lines, the list of
   where another engine costs less than Ledger. The charts carry their own numbers: each row's title gives Ledger's
   result, each bar its ratio to Ledger's ("4.7x slower"), drawn pale with a `~` when the two times are within their
   99.9% confidence intervals, and each bar's tooltip the engine's absolute result. The header of each SVG names the
   runs, commit and rounds it draws from. An `exclude` line of `charts.tsv` replaces a metric's bars on one OS with
   its reason. A chart of an OS without data is left out, and its old SVGs are deleted.

`python tools/readme/charts.py --check` exits 1 when a committed SVG or the README block is stale. Both scripts need
Python 3 and its standard library only; `fetch.py` also needs the GitHub CLI (`gh`), logged in.

To chart another workload, add a `row` line to `charts.tsv` with its benchmark, its parameters as `benchmarks.tsv`
writes them and the metric to compare (`score`, a counter, or the difference of two counters such as
`residentBytesPeak-residentBytesFloor`); a `loss` line names a result the same way to list where Ledger loses on it.
`charts.tsv`'s header explains the format. Every result, charted or not, stays in `benchmarks.tsv`.
