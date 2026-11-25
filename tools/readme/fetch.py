#!/usr/bin/env python3
"""Downloads the results of Benchmark workflow runs and writes them to benchmarks.tsv, which charts.py draws.

    python tools/readme/fetch.py RUN_ID...
    python tools/readme/fetch.py --downloads D:/ci-downloads RUN_ID...

Every run must be a run of the Benchmark workflow (.github/workflows/benchmark.yml); pass one run per OS and group of
workloads, for example the ten runs of a dispatch per group and OS, or one run dispatched with group=all. Each run's
artifacts (results-<os>-<group>) are downloaded with gh into <downloads>/<run id>, a new temporary directory unless
--downloads names one; a run already downloaded there is not downloaded again.

Each artifact holds <os>.json, the merged result of the job's rounds: for every workload, mode and parameter set, the
round with the median primary score (.github/scripts/merge_rounds.py). An artifact with rounds but no merged file is
merged here by that same script. The secondary counters are divided by the measurement iterations, as
.github/scripts/summarize.py divides them, so every counter in the output is its value per iteration.

The header of the output names, for every run, its commit, branch, rounds, the runner and its CPU model (from the
runner.txt the workflow writes; a run that predates that file says so), and which OS has no data for which group.
Standard library and the gh command only.
"""
import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
SCRIPTS = ROOT / '.github' / 'scripts'
OUTPUT = HERE / 'benchmarks.tsv'
REPO = 'Auties00/Ledger'
WORKFLOW = 'Benchmark'

# The runners and groups of the workflow, in the order the output lists them
SYSTEMS = ['ubuntu-latest', 'windows-latest', 'macos-latest']
GROUPS = ['commit', 'ingest', 'read', 'limits-write', 'limits-query', 'memory']

# The counters the output keeps, in bytes per measurement iteration; every other counter (percentiles, the templates'
# fingerprint) stays in the JSON
COUNTERS = ['bytesWritten', 'payloadBytes', 'sizeOnDisk',
            'heapBytes', 'heapBytesFloor', 'heapBytesPeak', 'heapBytesLive', 'heapBytesCommitted', 'heapBytesPerStore',
            'residentBytes', 'residentBytesFloor', 'residentBytesPeak', 'residentBytesPerStore']
COLUMNS = ['os', 'group', 'benchmark', 'params', 'engine', 'mode', 'score', 'unit', 'error', 'rounds', 'run'] + COUNTERS

ARTIFACT = re.compile(r'^results-(?P<os>[a-z]+-latest)-(?P<group>[a-z-]+)$')


def gh(*arguments):
    try:
        return subprocess.run(['gh', *arguments], check=True, capture_output=True, text=True, encoding='utf-8').stdout
    except FileNotFoundError:
        raise SystemExit('fetch.py needs the GitHub CLI (gh) on the PATH')
    except subprocess.CalledProcessError as failure:
        raise SystemExit(f'gh {" ".join(arguments)} failed: {failure.stderr.strip()}')


def describe(run_id, repo):
    run = json.loads(gh('run', 'view', run_id, '-R', repo, '--json',
                        'workflowName,headSha,headBranch,displayTitle,status,conclusion,createdAt,jobs'))
    if run['workflowName'] != WORKFLOW:
        raise SystemExit(f'run {run_id} is a run of {run["workflowName"]}, not of {WORKFLOW}')
    if run['status'] != 'completed':
        raise SystemExit(f'run {run_id} is still {run["status"]}; wait for it to complete')
    return run


def download(run_id, repo, downloads):
    target = downloads / run_id
    if target.is_dir() and any(target.glob('results-*')):
        return target
    target.mkdir(parents=True, exist_ok=True)
    gh('run', 'download', run_id, '-R', repo, '-D', str(target), '-p', 'results-*')
    return target


def merged_results(artifact, system):
    merged = artifact / f'{system}.json'
    if merged.is_file():
        return json.loads(merged.read_text(encoding='utf-8'))
    rounds = sorted((artifact / 'rounds').glob(f'{system}-*.json'))
    if not rounds:
        return []
    # Merge the rounds as the workflow does, with its own script
    with tempfile.TemporaryDirectory() as scratch:
        output = Path(scratch) / 'merged.json'
        subprocess.run([sys.executable, '-I', str(SCRIPTS / 'merge_rounds.py'), str(output), *map(str, rounds)], check=True)
        return json.loads(output.read_text(encoding='utf-8'))


def runner_info(artifact):
    """{key: value} from the runner.txt the workflow writes next to the results, empty for an older run."""
    path = artifact / 'runner.txt'
    info = {}
    if path.is_file():
        for line in path.read_text(encoding='utf-8', errors='replace').splitlines():
            key, _, value = line.partition(':')
            if value.strip():
                info[key.strip()] = value.strip()
    return info


def number(value):
    return format(value, '.6g')


def rows_of(results, system, group, run_id):
    rows = []
    for result in results:
        params = dict(result.get('params', {}))
        engine = params.pop('engine', '')
        metric = result['primaryMetric']
        error = metric.get('scoreError')
        # JMH adds the counters up over the measurement iterations; summarize.py divides them back the same way
        iterations = max(1, result.get('measurementIterations', 1) * result.get('forks', 1))
        secondary = result.get('secondaryMetrics', {})
        counters = []
        for name in COUNTERS:
            value = secondary.get(name, {}).get('score')
            counters.append(str(round(value / iterations)) if isinstance(value, (int, float)) and value == value else '')
        rows.append([
            system, group, '.'.join(result['benchmark'].split('.')[-2:]),
            ','.join(f'{k}={v}' for k, v in sorted(params.items())), engine, result.get('mode', ''),
            number(metric['score']), metric['scoreUnit'],
            number(error) if isinstance(error, (int, float)) and error == error else '',
            ';'.join(number(score) for score in metric.get('roundScores', [metric['score']])), run_id,
        ] + counters)
    return rows


def main():
    parser = argparse.ArgumentParser(description='Writes tools/readme/benchmarks.tsv from Benchmark workflow runs.')
    parser.add_argument('runs', nargs='+', metavar='RUN_ID', help='Benchmark workflow run ids')
    parser.add_argument('--downloads', type=Path, help='directory for the artifacts, one subdirectory per run')
    parser.add_argument('--repo', default=REPO, help=f'repository of the runs (default {REPO})')
    parser.add_argument('--output', type=Path, default=OUTPUT, help='file to write (default tools/readme/benchmarks.tsv)')
    arguments = parser.parse_args()
    downloads = arguments.downloads or Path(tempfile.mkdtemp(prefix='ledger-bench-'))

    header = []
    rows = []
    sources = {}
    commits = set()
    for run_id in dict.fromkeys(arguments.runs):
        if not run_id.isdigit():
            raise SystemExit(f'not a run id: {run_id}')
        run = describe(run_id, arguments.repo)
        jobs = {job['name']: job.get('conclusion') or job.get('status') for job in run.get('jobs', [])}
        directory = download(run_id, arguments.repo, downloads)
        artifacts = sorted(path for path in directory.iterdir() if path.is_dir() and ARTIFACT.match(path.name))
        if not artifacts:
            raise SystemExit(f'run {run_id} has no results-<os>-<group> artifact')
        for artifact in artifacts:
            match = ARTIFACT.match(artifact.name)
            system, group = match['os'], match['group']
            if (system, group) in sources:
                raise SystemExit(f'runs {sources[(system, group)]} and {run_id} both measured {group} on {system}; pass one')
            results = merged_results(artifact, system)
            if not results:
                continue
            sources[(system, group)] = run_id
            commits.add(run['headSha'])
            rounds = len(list((artifact / 'rounds').glob(f'{system}-*.json'))) or 1
            runner = runner_info(artifact)
            cpu = runner.get('CPU model', 'not recorded (the run predates runner.txt)')
            cpus = runner.get('CPUs')
            jdk = results[0].get('vmVersion', results[0].get('jdkVersion', '?'))
            conclusion = jobs.get(f'{group} on {system}', 'unknown')
            header.append(f'# run {run_id}: {group} on {system}, {rounds} rounds, commit {run["headSha"]} '
                          f'({run["headBranch"]}), started {run["createdAt"]}, job {conclusion}; '
                          f'CPU {cpu}{f", {cpus} CPUs" if cpus else ""}; JDK {jdk}')
            rows += rows_of(results, system, group, run_id)
    if not rows:
        raise SystemExit('the runs hold no results')

    order = {(system, group): (SYSTEMS.index(system) if system in SYSTEMS else 99, GROUPS.index(group) if group in GROUPS else 99)
             for system, group in sources}
    rows.sort(key=lambda row: order[(row[0], row[1])])
    header.sort(key=lambda line: order[tuple(re.match(r'# run \d+: (\S+) on (\S+),', line).group(2, 1))])
    commits = sorted(commits)
    missing = [f'{group} on {", ".join(system for system in SYSTEMS if (system, group) not in sources)}'
               for group in GROUPS if any((system, group) not in sources for system in SYSTEMS)]

    lines = ['# The benchmark results the README charts: tools/readme/charts.py reads this file.',
             '# Generated by tools/readme/fetch.py from the artifacts of the Benchmark workflow runs below; do not edit by hand.',
             f'# Ledger commits measured: {", ".join(commits)}' + (' (the runs measured different commits: compare engines '
                                                                   'within one run, not one run with another)' if len(commits) > 1 else ''),
             *header,
             f'# No data: {"; ".join(missing)}' if missing else '# Every group has data on every OS.',
             '# One row per OS, group, workload, parameter set and engine. score and error (the half-width of JMH\'s 99.9%',
             '# confidence interval) are those of the round with the median score; rounds lists every round\'s score. The counters',
             '# are bytes per measurement iteration, divided back as .github/scripts/summarize.py divides them; empty when the',
             '# workload does not record them.',
             '\t'.join(COLUMNS)]
    lines += ['\t'.join(row) for row in rows]
    arguments.output.write_text('\n'.join(lines) + '\n', encoding='utf-8', newline='\n')
    print(f'wrote {len(rows)} results of {len(sources)} jobs to {os.path.relpath(arguments.output)}'
          + (f'; downloads in {downloads}' if not arguments.downloads else ''))


if __name__ == '__main__':
    main()
