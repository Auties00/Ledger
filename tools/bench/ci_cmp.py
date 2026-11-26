"""Prints a compare.yml artifact directory (base-<round>.json / head-<round>.json) as per-round scores per metric, with
the ratio of medians (above 1 = head better: base/head for times, memory and allocation, head/base for a throughput score, by JMH mode).
Usage: ci_cmp.py <dir>"""
import glob
import json
import math
import os
import statistics
import sys

d = sys.argv[1]


def load(side):
    rounds = []
    files = sorted(glob.glob(os.path.join(d, side + '-*.json')), key=lambda p: int(p.rsplit('-', 1)[1].split('.')[0]))
    for path in files:
        try:
            rounds.append(json.load(open(path, encoding='utf-8')))
        except Exception:
            rounds.append([])
    return rounds


def key(e):
    name = '.'.join(e['benchmark'].split('.')[-2:])
    params = ','.join(f'{k}={v}' for k, v in sorted((e.get('params') or {}).items()))
    return name, params


def per_jvm(name, v):
    # JMH sums EVENTS aux counters (heapBytes*, residentBytes*, template*, ...) over forks and iterations;
    # report those as the median of the non-zero per-JVM values instead, so absolute numbers are per JVM.
    raw = [x for fork in (v.get('rawData') or []) for x in fork]
    if name.startswith('gc.') or len(raw) < 2:
        return v['score']
    tot = sum(raw)
    if tot and abs(tot - v['score']) <= 1e-6 * abs(tot):
        nz = [x for x in raw if x]
        return statistics.median(nz) if nz else 0.0
    return v['score']


MODES = {}


def collect(rounds):
    out = {}
    for i, r in enumerate(rounds):
        for e in r:
            k = key(e)
            MODES.setdefault(k, e.get('mode'))
            m = out.setdefault(k, {})
            pm = e['primaryMetric']
            m.setdefault(('', pm['scoreUnit']), [None] * len(rounds))[i] = pm['score']
            for name, v in (e.get('secondaryMetrics') or {}).items():
                if not isinstance(v.get('score'), (int, float)):
                    continue  # JFR and other profilers report text results
                m.setdefault((name, v['scoreUnit']), [None] * len(rounds))[i] = per_jvm(name, v)
    return out


base, head = collect(load('base')), collect(load('head'))
print('===== ' + os.path.basename(os.path.normpath(d)))
for k in sorted(set(base) | set(head)):
    metrics = sorted(set(base.get(k, {})) | set(head.get(k, {})), key=lambda m: (m[0] == '', m[0]))
    for metric in metrics:
        b = [x for x in base.get(k, {}).get(metric, []) if x is not None]
        h = [x for x in head.get(k, {}).get(metric, []) if x is not None]
        name, unit = metric
        ratio = float('nan')
        if b and h:
            bm, hm = statistics.median(b), statistics.median(h)
            # Only a throughput score is higher-is-better, whatever its unit (ops/s, ops/ms, ops/us)
            higher = name == '' and MODES.get(k) == 'thrpt'
            num, den = (hm, bm) if higher else (bm, hm)
            ratio = num / den if den else float('nan')
        r = 'nan' if math.isnan(ratio) else f'{ratio:.2f}'
        label = f'    {name} of ' if name else ''
        print(f'{r:>5}x  {label}{k[0]} [{k[1]}] {unit}')
        print('        base ' + ' '.join(f'{x:.4g}' for x in b))
        print('        head ' + ' '.join(f'{x:.4g}' for x in h))
