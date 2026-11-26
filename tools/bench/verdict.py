"""Verdict analyzer for the artifacts of .github/workflows/compare.yml.

Usage:
  python -I verdict.py <run dir | compare-<os> dir> [--pool <earlier run dir>...] [--pool-across-cpus]
                       [--judge-unverified-templates] [--q 0.05] [--quiet] [--no-write]
  python -I verdict.py --cpu-of <compare-<os> dir>     prints the run's CPU model (empty when unknown)

Input: the directory ci_fetch.sh downloads (<id>/compare-<os>/ with base-N.json, head-N.json, base.log, head.log,
runner.txt, probe.txt, meta.json and warmup/ with the discarded round 0; runs of older workflows lack some of them).
--pool adds earlier runs of the same compare (a rerun is pooled with its first run, never a replacement); each is
matched to the main run by its compare-<os> subdirectory, and a --pool path with no compare dir of the main run's name
stops the script (exit 2). Pass the latest run (the rerun) as the main run: the rows, and so the Benjamini-Hochberg
family, are the main run's metrics, and the pooled runs add their rounds to those metrics.

Runs on different CPU models are not pooled: verdicts have flipped with the model, and a row pooled from two models
mostly reads as Q! cpu!. Only the --pool runs on the main run's model (meta.json, runner.txt or probe.txt; an unknown
model matches nothing) are pooled into it; the others are judged per model, each model's runs pooled among themselves,
and printed after the main verdict under a NOT POOLED header. --pool-across-cpus pools every run regardless (Q! and
cpu! then flag rows whose runs disagree). ci_compare.sh keeps a compare's reruns on one model.

Per benchmark, params and metric:
  - per-JVM round values: the score for the primary metric and gc.*; for EVENTS aux counters (heapBytes*,
    residentBytes*, template*, ...), which JMH sums over forks and iterations, the median of the non-zero rawData values
    (the same rule as ci_cmp.py per_jvm);
  - ratio = ratio of geometric means of the round values, oriented so that above 1 = head better (only a thrpt-mode
    score is higher-is-better);
  - a 95% Welch interval on the log round values (Satterthwaite df), with the two-sided Mann-Whitney p and the
    Hodges-Lehmann shift as checks (flags mw! / hl! when they disagree with a WIN or LOSS);
  - runs are pooled by DerSimonian-Laird random effects on the log ratio: runs that disagree (Cochran's Q above its df)
    get a between-run variance tau^2 that widens the interval, and the df is the Satterthwaite df of the pooled
    variance (each run's variance with its Welch df, tau^2 with k-1), so a run with little weight adds little df;
    Q! flags p(Q) < 0.05; with --pool-across-cpus, runs on different CPU models (an unknown model is its own group)
    are also pooled per model and cpu! flags a disagreement between models;
  - Benjamini-Hochberg at q (default 0.05) over every judged metric of the compare (one OS);
  - verdict against the metric's margin m (table MARGINS below):
      WIN / LOSS     BH-significant and the interval is not inside [1/(1+m), 1+m]; "small" when the point estimate is
                     inside the margin (a small loss does not block an item);
      NEUTRAL        the interval lies inside [1/(1+m), 1+m] (equivalence; flag sig when it is also BH-significant);
      INCONCLUSIVE   anything else: add rounds and pool, never "push"; also an ubuntu GroupCommit c>=64 WIN or LOSS
                     (flag needs-windows(WIN|LOSS)), which moves ~20% with iteration spacing there;
      TEMPLATE       the workload reads a store template (its [template] log lines, or on refs without them the
                     TEMPLATE_WORKLOADS list) and the two sides did not measure replicated, equal templates: the
                     fingerprints (per-file sizes, templateBytes/templateShape) differ or are missing (refs with timed
                     templates), or a side read one template build in every round (fewer `[template] built` lines
                     than judged rounds and no fresh_templates in meta.json), so the fixed offset between two builds
                     of one layout would read as an effect; not judged on this run. Fingerprints that differ when
                     both sides built every template the workload reads in order (`[template] built ... (ordered)`
                     or `mode=ordered`, one fingerprint per side) are not stratified: the ordered build makes a
                     template a function of the code, so the difference is the head's own layout change; the row is
                     judged like a match (with the same replication rule), flagged `layout`, and the summary prints
                     both sides' template bytes;
      VOID           not comparable: a fork without @Fork's four arguments (a command-line -jvmArgsAppend), base and
                     head forks with different VM options, or head-only JMH arguments (meta head_jmh_args);
      PROFILED       a profiling run (meta profile/profiler, a -prof other than gc, profiler metrics, JFR or an agent
                     in the forks): its scores never feed a verdict;
      UNJUDGED       a metric missing on one side, with fewer than 2 values on a side, or with values <= 0 (reported,
                     never judged).
    TEMPLATE, VOID, PROFILED and UNJUDGED rows are outside the BH family.
  - the old SEP flag (round ranges do not overlap, main run) as a column; committed heap and RSS also as value counts
    (MiB), since G1 sizes them in a handful of discrete steps.

Output: verdict.json in the run directory (merged by compare-<os> key) and a printed summary of flagged rows only
(LOSS, WIN, INCONCLUSIVE on primary scores, warnings), under 100 lines.
"""
import glob
import itertools
import json
import math
import os
import re
import statistics
import sys

# ---------------------------------------------------------------------------------------------------------------------
# Margins: one table. (regex on the metric name; '' is the JMH primary score), mode filter or None, margin, class.
# Rows are tried in order; a metric matching none (gc.alloc.rate, gc.count, gc.time, percentiles, *Floor, template*,
# profiler counters) is neither judged nor reported: ci_cmp.py prints it.
MARGINS = [
    ('^$', 'ss', 0.03, 'time'),                       # single-shot score: 3%
    ('^$', None, 0.02, 'time'),                       # thrpt / avgt / sample score: 2%
    (r'^gc\.alloc\.rate\.norm$', None, 0.01, 'alloc'),  # allocation per op: 1%
    (r'^heapBytes(PerStore|Live)?$', None, 0.01, 'heap'),  # heap after GC: 1%
    (r'^heapBytesPeak$', None, 0.03, 'peak'),         # heap peak: 3%
    (r'^heapBytesCommitted$', None, 0.05, 'committed'),  # committed heap (G1 sizing, discrete): 5% + value counts
    (r'^residentBytes(PerStore)?$', None, 0.05, 'rss'),  # RSS: 5% + value counts
    (r'^residentBytesPeak$', None, 0.05, 'rss'),      # RSS peak: 5% + value counts
    (r'^(sizeOnDisk|bytesWritten)$', None, 0.01, 'disk'),  # bytes on disk / written: 1%
]
VALUE_COUNT_CLASSES = ('committed', 'rss')
# The workloads that read a store template (Stores.loaded, copyOf, loadedGeneric), for refs whose logs carry no
# [template] lines; on refs that print them, the lines of each workload's own forks decide.
TEMPLATE_WORKLOADS = re.compile(
    r'^(SessionMemoryBenchmark\.session|ReopenBenchmark\.|ChatPageBenchmark\.|ChatDeleteBenchmark\.|'
    r'MessageLookupBenchmark\.|OfflineDrainBenchmark\.|AdHocQueryBenchmark\.|CallerThreadsReadBenchmark\.|'
    r'RandomOverwriteBenchmark\.|OperationMemoryBenchmark\.(checkpointUnderLoad|recoverAfterCrash)$|'
    r'SteadyWritesMemoryBenchmark\.)')
PROFILER_METRICS = re.compile(r'^(cpu\.|alloc\.(carrier|worker|io|other|total|prepare|checkpoint|commonpool|profiler)|jit\.ms|op\.|jfr|async|CPI|IPC|cycles|instructions|branch)')
# @Fork's arguments: every workload carries these three, plus -Xms4g (or -Xms64m for the memory workloads)
FORK_FLAGS = ('--enable-native-access', '--sun-misc-unsafe-memory-access', '-Xmx4g')
FORK_XMS = ('-Xms4g', '-Xms64m')
Q_DEFAULT = 0.05
MAX_LINES = 99
UNJUDGED_VERDICTS = ('TEMPLATE', 'VOID', 'PROFILED', 'UNJUDGED')


# --------------------------------------------------------------------------------------------------------- statistics
def _betacf(a, b, x):
    qab, qap, qam = a + b, a + 1, a - 1
    c, d = 1.0, 1 - qab * x / qap
    d = 1 / (d if abs(d) > 1e-300 else 1e-300)
    h = d
    for m in range(1, 300):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1 + aa * d
        d = 1 / (d if abs(d) > 1e-300 else 1e-300)
        c = 1 + aa / c if abs(1 + aa / c) > 1e-300 else 1e-300
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1 + aa * d
        d = 1 / (d if abs(d) > 1e-300 else 1e-300)
        c = 1 + aa / c if abs(1 + aa / c) > 1e-300 else 1e-300
        de = d * c
        h *= de
        if abs(de - 1) < 3e-14:
            break
    return h


def betai(a, b, x):
    if x <= 0:
        return 0.0
    if x >= 1:
        return 1.0
    lb = math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b) + a * math.log(x) + b * math.log(1 - x)
    if x < (a + 1) / (a + b + 2):
        return math.exp(lb) * _betacf(a, b, x) / a
    return 1 - math.exp(lb) * _betacf(b, a, 1 - x) / b


def t_p2(t, df):
    """Two-sided p of Student t."""
    if df <= 0 or math.isnan(t):
        return 1.0
    if math.isinf(t):
        return 0.0
    return betai(df / 2, 0.5, df / (df + t * t))


_TQ = {}


def t_q975(df):
    """97.5% quantile of Student t (bisection on t_p2)."""
    k = round(df, 2)
    if k in _TQ:
        return _TQ[k]
    lo, hi = 0.0, 1000.0
    for _ in range(100):
        mid = (lo + hi) / 2
        if t_p2(mid, df) > 0.05:
            lo = mid
        else:
            hi = mid
    _TQ[k] = (lo + hi) / 2
    return _TQ[k]


def chi2_sf(x, k):
    """Upper tail of chi-square with k degrees of freedom (regularized upper incomplete gamma)."""
    if x <= 0:
        return 1.0
    a, x = k / 2, x / 2
    gln = math.lgamma(a)
    if x < a + 1:
        ap, s, de = a, 1 / a, 1 / a
        for _ in range(500):
            ap += 1
            de *= x / ap
            s += de
            if abs(de) < abs(s) * 1e-14:
                break
        return 1 - s * math.exp(-x + a * math.log(x) - gln)
    b = x + 1 - a
    c, d = 1e300, 1 / b
    h = d
    for i in range(1, 500):
        an = -i * (i - a)
        b += 2
        d = an * d + b
        d = 1 / (d if abs(d) > 1e-300 else 1e-300)
        c = b + an / c
        c = c if abs(c) > 1e-300 else 1e-300
        de = d * c
        h *= de
        if abs(de - 1) < 1e-14:
            break
    return math.exp(-x + a * math.log(x) - gln) * h


def mw_p(a, b):
    """Two-sided Mann-Whitney p (midranks): exact enumeration when C(n1+n2, n1) <= 4000, else normal with tie and
    continuity correction."""
    n1, n2 = len(a), len(b)
    allv = sorted(a + b)
    ranks, i = {}, 0
    while i < len(allv):
        j = i
        while j + 1 < len(allv) and allv[j + 1] == allv[i]:
            j += 1
        ranks[allv[i]] = (i + j) / 2 + 1
        i = j + 1
    u1 = sum(ranks[x] for x in a) - n1 * (n1 + 1) / 2
    mu = n1 * n2 / 2
    dev = abs(u1 - mu)
    if math.comb(n1 + n2, n1) <= 4000:
        rk = [ranks[x] for x in a + b]
        cnt = tot = 0
        for comb in itertools.combinations(range(n1 + n2), n1):
            tot += 1
            if abs(sum(rk[c] for c in comb) - n1 * (n1 + 1) / 2 - mu) >= dev - 1e-9:
                cnt += 1
        return cnt / tot
    n = n1 + n2
    ties = {}
    for x in allv:
        ties[x] = ties.get(x, 0) + 1
    var = n1 * n2 / 12 * ((n + 1) - sum(t ** 3 - t for t in ties.values()) / (n * (n - 1)))
    if var <= 0:
        return 1.0
    return min(1.0, math.erfc(max(0.0, dev - 0.5) / math.sqrt(var) / math.sqrt(2)))


def van_elteren_p(strata):
    """Two-sided p of the van Elteren stratified rank-sum test (normal approximation, no tie correction) over
    [(base values, head values)] per run."""
    t = e = var = 0.0
    for a, b in strata:
        n1, n2 = len(a), len(b)
        n = n1 + n2
        allv = sorted(a + b)
        ranks, i = {}, 0
        while i < n:
            j = i
            while j + 1 < n and allv[j + 1] == allv[i]:
                j += 1
            ranks[allv[i]] = (i + j) / 2 + 1
            i = j + 1
        t += sum(ranks[x] for x in a) / (n + 1)
        e += n1 / 2
        var += n1 * n2 / (12 * (n + 1))
    if var <= 0:
        return 1.0
    return min(1.0, math.erfc(abs(t - e) / math.sqrt(var) / math.sqrt(2)))


def welch(lb, lh, sign):
    """Oriented difference of log means, its Welch se and df."""
    nb, nh = len(lb), len(lh)
    d = (statistics.fmean(lh) - statistics.fmean(lb)) * sign
    vb, vh = statistics.variance(lb), statistics.variance(lh)
    sb, sh = vb / nb, vh / nh
    se2 = sb + sh
    if se2 <= 0:
        return d, 0.0, float(nb + nh - 2)
    den = (sb * sb / (nb - 1) if nb > 1 else 0) + (sh * sh / (nh - 1) if nh > 1 else 0)
    df = se2 * se2 / den if den > 0 else float(nb + nh - 2)
    return d, math.sqrt(se2), df


def pool(ests):
    """DerSimonian-Laird random-effects pooling of [(d, se, df)]; returns d, se, df, Q, p(Q), tau^2.

    tau^2 = max(0, (Q - (k-1)) / (W - sum w^2 / W)) on the fixed-effect weights w = 1/se^2; the pooled estimate weighs
    each run by 1/(se^2 + tau^2), so runs that disagree widen the interval instead of only setting Q!. df is the
    Satterthwaite df of the pooled variance sum(w*^2 (se^2 + tau^2)) / W*^2: each run's part with its Welch df and the
    tau^2 part with k-1. With tau^2 = 0 it is W^2 / sum(w^2 / df), close to the df of a run that carries the weight
    (never the sum of the dfs, which let a near-weightless run make a result more significant)."""
    if len(ests) == 1:
        d, se, df = ests[0]
        return d, se, df, 0.0, 1.0, 0.0
    floor = 1e-7
    k = len(ests)
    v = [max(se, floor) ** 2 for _, se, _ in ests]
    w = [1 / x for x in v]
    sw = sum(w)
    dfe = sum(wi * e[0] for wi, e in zip(w, ests)) / sw
    q = sum(wi * (e[0] - dfe) ** 2 for wi, e in zip(w, ests))
    c = sw - sum(wi * wi for wi in w) / sw
    tau2 = max(0.0, (q - (k - 1)) / c) if c > 0 else 0.0
    if all(e[1] == 0 for e in ests) and all(abs(e[0] - ests[0][0]) <= 1e-12 for e in ests):
        return dfe, 0.0, float(sum(e[2] for e in ests)), q, chi2_sf(q, k - 1), 0.0
    ws = [1 / (x + tau2) for x in v]
    sws = sum(ws)
    d = sum(wi * e[0] for wi, e in zip(ws, ests)) / sws
    parts = [wi * wi * x / (sws * sws) for wi, x in zip(ws, v)]
    tau_part = tau2 * sum(wi * wi for wi in ws) / (sws * sws)
    var = sum(parts) + tau_part
    den = sum(p * p / max(e[2], 1e-9) for p, e in zip(parts, ests)) + (tau_part * tau_part / (k - 1))
    df = var * var / den if den > 0 else float(sum(e[2] for e in ests))
    return d, math.sqrt(var), df, q, chi2_sf(q, k - 1), tau2


def interval(d, se, df):
    if se == 0:
        p = 0.0 if abs(d) > 1e-12 else 1.0
        return math.exp(d), math.exp(d), p
    tq = t_q975(df)
    return math.exp(d - tq * se), math.exp(d + tq * se), t_p2(d / se, df)


def bimodal(xs):
    """'B' when the values split into two clusters whose gap exceeds 3x the within-cluster sd."""
    xs = sorted(xs)
    if len(xs) < 6:
        return False
    best = None
    for c in range(2, len(xs) - 1):
        a, b = xs[:c], xs[c:]
        ma, mb = statistics.fmean(a), statistics.fmean(b)
        sw = math.sqrt((sum((x - ma) ** 2 for x in a) + sum((x - mb) ** 2 for x in b)) / len(xs))
        score = (b[0] - a[-1]) / sw if sw > 0 else float('inf')
        if best is None or score > best[0]:
            best = (score, c, statistics.median(a), statistics.median(b))
    med = statistics.median(xs)
    rel = (best[3] - best[2]) / abs(med) if med else 0
    return best[0] > 3 and rel > 0.02 and min(best[1], len(xs) - best[1]) >= 2


def bh(ps, q):
    """Benjamini-Hochberg: (rejected flags, adjusted p) for a list of p-values."""
    m = len(ps)
    order = sorted(range(m), key=lambda i: ps[i])
    adj = [1.0] * m
    prev = 1.0
    for rank in range(m, 0, -1):
        i = order[rank - 1]
        prev = min(prev, ps[i] * m / rank)
        adj[i] = prev
    return [a <= q for a in adj], adj



# ------------------------------------------------------------------------------------------------------------ loading
def margin_for(name, mode):
    for pat, md, m, cls in MARGINS:
        if re.search(pat, name) and (md is None or md == mode):
            return m, cls
    return None, None


def higher_better(name, mode):
    """Only a throughput score is higher-is-better, whatever its unit (ops/s, ops/ms, ops/us)."""
    return name == '' and mode == 'thrpt'


def num(x):
    """A finite float, or `None` (JMH writes "NaN" as a string for counters it could not read)."""
    try:
        f = float(x)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def per_jvm(name, v):
    score = num(v.get('score'))
    raw = [num(x) for fork in (v.get('rawData') or []) for x in fork]
    if score is None or None in raw or name.startswith('gc.') or len(raw) < 2:
        return score
    tot = sum(raw)
    if tot and abs(tot - score) <= 1e-6 * abs(tot):
        nz = [x for x in raw if x]
        return statistics.median(nz) if nz else 0.0
    return score


def fork_values(v):
    """Per fork, the non-zero value (templateBytes/templateShape are set in the first iteration of each fork)."""
    out = []
    for fork in v.get('rawData') or []:
        nz = [num(x) for x in fork if num(x)]
        if nz:
            out.append(nz[0])
    return out


def short(bench):
    return '.'.join(bench.split('.')[-2:])


def params_str(p):
    return ','.join(f'{k}={v}' for k, v in sorted((p or {}).items()))


def parse_log(path):
    """One side's log -> dict(vm={(workload, params): set(VM options)}, fps={template: set(layout)},
    used={(workload, params): set(template)}, built={template: number of builds}), or `None` without a log.

    A [template] line is printed by every fork that uses a template, so the lines inside a workload's section name the
    templates it reads; a `[template] built` line is a build, one per template per round unless the round rebuilt it."""
    try:
        lines = open(path, encoding='utf-8', errors='replace').read().splitlines()
    except OSError:
        return None
    vm, fps, used, built, modes = {}, {}, {}, {}, {}
    pending = None
    bench, params = None, ''
    for ln in lines:
        if ln.startswith('# VM options:'):
            pending = ln[len('# VM options:'):].strip()
        elif ln.startswith('# Benchmark:'):
            bench = short(ln.split(':', 1)[1].strip())
            params = ''
        elif ln.startswith('# Parameters:'):
            body = ln.split(':', 1)[1].strip().strip('()')
            params = params_str(dict(kv.split(' = ', 1) for kv in body.split(', ') if ' = ' in kv))
        elif ln.startswith('# Run progress') and pending is not None and bench:
            vm.setdefault((bench, params), set()).add(pending)
        if '[template]' not in ln:
            continue
        b = re.search(r'\[template\] built (\S+) in\b', ln)
        if b:
            built[b.group(1)] = built.get(b.group(1), 0) + 1
            bm = re.search(r'\((ordered|timed)\)\s*$', ln)
            if bm:
                modes.setdefault(b.group(1), set()).add(bm.group(1))
            if bench:
                used.setdefault((bench, params), set()).add(b.group(1))
            continue
        m = re.search(r'\[template\] (\S+) bytes=(\d+)\b.*?\bshape=(\S+)(?: (\S+))?', ln)
        if m:
            fps.setdefault(m.group(1), set()).add(template_layout(int(m.group(2)), m.group(3), m.group(4)))
            um = re.search(r'\bmode=(ordered|timed)\b', ln)
            if um:
                modes.setdefault(m.group(1), set()).add(um.group(1))
            if bench:
                used.setdefault((bench, params), set()).add(m.group(1))
    return dict(vm=vm, fps=fps, used=used, built=built, modes=modes)


def template_layout(total, shape, files):
    """Fingerprint of one [template] line: the per-file sizes with the log segments summed (their names carry a segment
    number that can differ between two builds of the same layout), or the shape hash when the line lists no files."""
    if not files:
        return total, shape
    sizes, log = {}, 0
    for item in files.split(','):
        name, _, size = item.rpartition('=')
        if not name or not size.isdigit():
            return total, shape
        if re.match(r'^log[/\\]', name):
            log += int(size)
        else:
            sizes[name] = int(size)
    return (total,) + tuple(sorted(sizes.items())) + (('log', log),)


def fork_args_ok(opts):
    """Whether a fork's VM options carry all of @Fork's arguments (a command-line -jvmArgsAppend replaces them)."""
    words = opts.split()
    has = lambda flag: any(w == flag or w.startswith(flag + '=') for w in words)  # noqa: E731
    return all(has(f) for f in FORK_FLAGS) and any(w in FORK_XMS for w in words)


def non_gc_profilers(args):
    """The -prof names in a JMH argument string other than gc (which a judged run may carry)."""
    return [p for p in (m.split(':', 1)[0] for m in re.findall(r'(?:^|\s)-prof\s+(\S+)', args or '')) if p != 'gc']


def normalize_cpu(model):
    """The model with (R), (TM) and " CPU" removed and runs of spaces collapsed, as compare.yml writes it."""
    model = re.sub(r'\s+', ' ', model).replace('(R)', '').replace('(TM)', '').replace(' CPU', '')
    return re.sub(r'\s+', ' ', model).strip()


def cpu_model(sub):
    """The runner's CPU model from meta.json, runner.txt or probe.txt (older runs have only some of them), or `None`."""
    try:
        m = json.load(open(os.path.join(sub, 'meta.json'), encoding='utf-8')).get('cpu_model')
        if isinstance(m, str) and m.strip() and m.strip() != 'unknown':
            return normalize_cpu(m)
    except (OSError, ValueError, AttributeError):
        pass
    # cpu_model= is written on every OS (the Windows probe has nothing else); lscpu's line on older probes
    for name, pats in (('runner.txt', (r'^CPU model:\s*(.+)$',)),
                       ('probe.txt', (r'^cpu_model=(.+)$', r'^Model name:\s*(.+)$', r'^Name\s*:\s*(.+)$'))):
        for p in (os.path.join(sub, name), os.path.join(sub, 'results', name)):
            try:
                txt = open(p, encoding='utf-8-sig', errors='replace').read()
            except OSError:
                continue
            for pat in pats:
                m = re.search(pat, txt, re.M)
                if m and m.group(1).strip() and m.group(1).strip() != 'unknown':
                    return normalize_cpu(m.group(1))
    return None


def load_run(sub):
    """One compare-<os> directory -> dict with rows {(workload, params, metric, unit): {...}} and run facts."""
    run = dict(dir=os.path.normpath(sub), id=os.path.basename(os.path.dirname(os.path.normpath(sub))),
               cpu=cpu_model(sub), warnings=[], rows={}, void=set(), void_reason={}, profiled=False, profiled_why=[],
               asym=None, rounds=0, rounds_side={}, round0=False)
    try:
        run['meta'] = json.load(open(os.path.join(sub, 'meta.json'), encoding='utf-8'))
    except (OSError, ValueError):
        run['meta'] = None
    meta = run['meta'] or {}
    sides = {}
    for side in ('base', 'head'):
        rs = {}
        for p in glob.glob(os.path.join(sub, side + '-*.json')):
            m = re.search(r'-(\d+)\.json$', p)
            if not m:
                continue
            try:
                rs[int(m.group(1))] = json.load(open(p, encoding='utf-8'))
            except (OSError, ValueError):
                rs[int(m.group(1))] = []
        sides[side] = rs
    # compare.yml writes round 0 to warmup/; older workflows wrote it beside the judged rounds or ran none
    run['round0'] = (0 in sides['base'] or 0 in sides['head']
                     or any(os.path.exists(os.path.join(sub, 'warmup', s + '-0.json')) for s in ('base', 'head'))
                     or meta.get('warmup_round') is True)
    run['rounds_side'] = {s: len([r for r in sides[s] if r != 0]) for s in sides}
    run['rounds'] = max(run['rounds_side'].values())
    if run['rounds_side']['base'] != run['rounds_side']['head']:
        run['warnings'].append(f"WARNING unequal rounds: base {run['rounds_side']['base']}, head "
                               f"{run['rounds_side']['head']} (a side's round failed or is missing)")
    for p in (os.path.join(sub, 'watchdog.txt'), os.path.join(sub, 'results', 'watchdog.txt')):
        if os.path.exists(p):
            try:
                first = open(p, encoding='utf-8', errors='replace').readline().strip()
            except OSError:
                first = ''
            run['warnings'].append(f'WARNING watchdog.txt present: a workload hung and its fork was killed ({first})')
            break
    if (meta.get('profile') or '').strip() or (meta.get('profiler') or '').strip():
        run['profiled'] = True
        run['profiled_why'].append(f"meta profile '{meta.get('profile') or meta.get('profiler')}'")
    for f in ('jmh_args', 'head_jmh_args'):
        prof = non_gc_profilers(meta.get(f))
        if prof:
            run['profiled'] = True
            run['profiled_why'].append(f'{f} -prof {prof[0]}')
    if (meta.get('head_jmh_args') or '').strip():
        run['asym'] = f"head-only JMH arguments '{meta['head_jmh_args'].strip()[:80]}'"
    tpl = {'base': {}, 'head': {}}
    for side, rs in sides.items():
        for rnd, entries in rs.items():
            if rnd == 0:
                continue  # discarded round
            for e in entries or []:
                wl, pr = short(e['benchmark']), params_str(e.get('params'))
                mode = e.get('mode')
                pm = e['primaryMetric']
                metrics = [('', pm)] + list((e.get('secondaryMetrics') or {}).items())
                for name, v in metrics:
                    if PROFILER_METRICS.search(name) and not run['profiled']:
                        run['profiled'] = True
                        run['profiled_why'].append(f'profiler metric {name}')
                    if name in ('templateBytes', 'templateShape'):
                        t = tpl[side].setdefault((wl, pr), {})
                        t.setdefault(name, set()).update(fork_values(v))
                        continue
                    k = (wl, pr, name, v['scoreUnit'])
                    row = run['rows'].setdefault(k, dict(mode=mode, base={}, head={}))
                    row[side][rnd] = per_jvm(name, v)
    logs = {}
    for side in ('base', 'head'):
        lg = parse_log(os.path.join(sub, side + '.log'))
        logs[side] = lg or dict(vm=None, fps={}, used={}, built={}, modes={})
        # The build mode of a template can be printed only in round 0, which compare.yml writes to warmup/
        wl_log = parse_log(os.path.join(sub, 'warmup', side + '.log'))
        for n, ms in (wl_log or {}).get('modes', {}).items():
            logs[side]['modes'].setdefault(n, set()).update(ms)
        if lg is None:
            continue
        voided = []
        for (wl, pr), opts in lg['vm'].items():
            bad = [o for o in opts if not fork_args_ok(o)]
            if bad:
                run['void'].add((wl, pr))
                run['void_reason'].setdefault((wl, pr), f'{side} forked without @Fork args: {bad[0][:90] or "(none)"}')
                voided.append((wl, pr, bad[0]))
            if any(re.search(r'StartFlightRecording|-agentpath|-agentlib', o) for o in opts) and not run['profiled']:
                run['profiled'] = True
                run['profiled_why'].append('JFR or an agent in the VM options')
        if voided:
            wl, pr, opt = voided[0]
            run['warnings'].append(f'WARNING {side}: {len(voided)} workloads forked without @Fork args (void for '
                                   f'judging), e.g. {wl} [{pr}] with VM options: {opt[:100] or "(none)"}')
    vb, vh = logs['base']['vm'], logs['head']['vm']
    if vb is not None and vh is not None:
        differ = [k for k in sorted(set(vb) & set(vh)) if vb[k] != vh[k]]
        for k in differ:
            run['void'].add(k)
            run['void_reason'].setdefault(k, 'base and head forks ran with different VM options')
        if differ:
            k = differ[0]
            run['warnings'].append(f'WARNING {len(differ)} workloads ran with different VM options on base and head '
                                   f'(void), e.g. {k[0]} [{k[1]}]: base {sorted(vb[k])[0][:70]} | head '
                                   f'{sorted(vh[k])[0][:70]}')
    if run['asym'] and not run['profiled']:
        run['warnings'].append(f"WARNING asymmetric run, {run['asym']}: every row is VOID")
    run['tpl_json'] = tpl
    run['tpl_log'] = {s: logs[s]['fps'] for s in logs}
    run['tpl_used'] = {s: logs[s]['used'] for s in logs}
    run['tpl_built'] = {s: logs[s]['built'] for s in logs}
    run['tpl_modes'] = {s: logs[s]['modes'] for s in logs}
    if run['profiled']:
        run['warnings'].append(f"WARNING profiled run ({', '.join(run['profiled_why'][:2])}): every row is PROFILED, "
                               'never a verdict')
    return run


def template_names(run, wl, pr):
    return run['tpl_used']['base'].get((wl, pr), set()) | run['tpl_used']['head'].get((wl, pr), set())


def is_template(run, wl, pr):
    return bool(template_names(run, wl, pr)) or bool(TEMPLATE_WORKLOADS.search(wl))


def template_state(run, wl, pr):
    """'match', 'differ', 'unreplicated' or 'unknown' for a template-reading workload on one run.

    'match' needs equal fingerprints on both sides and, per side, a build of each template the workload reads in every
    judged round: a side that read one build in all its rounds carries that build's fixed offset (the bytes of two
    builds differ, as every checkpoint draws its own nonces) into every round, which then reads as a side effect."""
    names = template_names(run, wl, pr)
    lb, lh = run['tpl_log']['base'], run['tpl_log']['head']
    state = None
    if names and all(n in lb and n in lh for n in names):
        # The [template] lines list the file sizes, so two builds of one layout match even when their log segment
        # numbers (part of the templateShape hash on older harnesses) differ.
        state = 'match' if all(lb[n] == lh[n] for n in names) else 'differ'
        modes = run.get('tpl_modes') or {}
        if state == 'differ' and all(
                (modes.get('base') or {}).get(n) == {'ordered'} and (modes.get('head') or {}).get(n) == {'ordered'}
                and len(lb[n]) == 1 and len(lh[n]) == 1 for n in names):
            # Both sides built in order, one layout each: the head's own layout change, not a random draw
            state = 'layout'
    else:
        tb, th = run['tpl_json']['base'].get((wl, pr)), run['tpl_json']['head'].get((wl, pr))
        if tb and th and tb.get('templateBytes') and th.get('templateBytes'):
            fb = (frozenset(tb.get('templateBytes', ())), frozenset(tb.get('templateShape', ())))
            fh = (frozenset(th.get('templateBytes', ())), frozenset(th.get('templateShape', ())))
            state = 'match' if fb == fh else 'differ'
    if state is None:
        return 'unknown'
    if state == 'differ':
        return state
    if names:
        for side in ('base', 'head'):
            if any(run['tpl_built'][side].get(n, 0) < run['rounds_side'].get(side, 0) for n in names):
                return 'unreplicated'
        return state
    return 'match' if (run['meta'] or {}).get('fresh_templates') is True else 'unreplicated'


def template_bytes(run, wl, pr):
    """{side: {template: sorted total bytes of its [template] lines}} for the templates a workload reads whose
    fingerprints differ between the sides."""
    lb, lh = run['tpl_log']['base'], run['tpl_log']['head']
    names = sorted(n for n in template_names(run, wl, pr) if lb.get(n) != lh.get(n))
    return {side: {n: sorted({fp[0] for fp in run['tpl_log'][side].get(n, ())}) for n in names}
            for side in ('base', 'head')}


def expand(paths):
    """Run dirs or compare dirs -> list of compare-<os> dirs."""
    out = []
    for p in paths:
        p = os.path.normpath(p)
        if glob.glob(os.path.join(p, 'base-*.json')) or glob.glob(os.path.join(p, 'head-*.json')):
            out.append(p)
        else:
            out.extend(sorted(d.rstrip('/\\') for d in glob.glob(os.path.join(p, 'compare-*' + os.sep))))
    return out


# ----------------------------------------------------------------------------------------------------------- analysis
def estimate(row, drop_round1):
    b = {r: v for r, v in row['base'].items() if v is not None}
    h = {r: v for r, v in row['head'].items() if v is not None}
    if drop_round1 and len(b) >= 4 and len(h) >= 4:
        b.pop(1, None)
        h.pop(1, None)
    return [b[r] for r in sorted(b)], [h[r] for r in sorted(h)]


def value_issue(bs, hs):
    """Why a run's values of one metric cannot be judged, or `None`."""
    if not bs and not hs:
        return 'no values on either side'
    for side, xs in (('base', bs), ('head', hs)):
        if not xs:
            return f'missing on {side}'
    for side, xs in (('base', bs), ('head', hs)):
        if len(xs) < 2:
            return f'fewer than 2 values on {side} ({len(xs)})'
    if min(bs + hs) <= 0:
        return 'values <= 0 (not on the log scale)'
    return None


def value_counts(xs, cls, ref):
    """Counts of the values in MiB: committed heap at 3 significant digits (G1 commits whole regions), RSS in buckets
    of 2% of `ref` (the median of both sides), so that the few steps G1 sizing takes show as a handful of keys."""
    c = {}
    w = 0.02 * ref if cls != 'committed' and ref > 0 else 0
    for x in sorted(xs):
        k = f'{(round(x / w) * w if w else x) / 2 ** 20:.3g}'
        c[k] = c.get(k, 0) + 1
    return c


def analyze(main_sub, pool_subs, q=Q_DEFAULT, judge_unverified=False):
    return analyze_runs([load_run(main_sub)] + [load_run(s) for s in pool_subs], q, judge_unverified)


def pool_warnings(runs):
    """Warnings about pooling runs[1:] into runs[0]: missing meta.json, other refs, pattern or arguments, CPU models."""
    out = []
    main = runs[0]
    mm = main['meta']
    if len(runs) > 1 and not mm:
        out.append(f'WARNING main run {main["id"]} has no meta.json: the pooled runs\' refs, pattern and arguments '
                   'are unchecked')
    for r in runs[1:]:
        out += [w + f' (pooled run {r["id"]})' for w in r['warnings']]
        if not r['meta']:
            out.append(f'WARNING pooled run {r["id"]} has no meta.json: its refs, pattern and arguments are unchecked')
            continue
        if not mm:
            continue
        for f, alt in (('base_sha', 'base'), ('head_sha', 'head'), ('pattern', None), ('jmh_args', None),
                       ('head_jmh_args', None)):
            a = mm.get(f) if mm.get(f) is not None else (mm.get(alt) if alt else None)
            b = r['meta'].get(f) if r['meta'].get(f) is not None else (r['meta'].get(alt) if alt else None)
            if a is not None and b is not None and str(a).strip() != str(b).strip():
                out.append(f'WARNING pooled run {r["id"]} has another {f} ({str(b)[:60]} vs {str(a)[:60]})')
    cpus = [r['cpu'] for r in runs]
    if len(runs) > 1 and None in cpus and any(cpus):
        out.append('WARNING pooled runs mix known and unknown CPU models (' +
                   ', '.join(f'{r["id"]}={r["cpu"] or "?"}' for r in runs) + '); the unknown ones are their own group')
    return out


def analyze_runs(runs, q=Q_DEFAULT, judge_unverified=False):
    """Judges runs[0] with runs[1:] pooled into its metrics (runs as load_run returns them)."""
    main = runs[0]
    warnings = list(main['warnings']) + pool_warnings(runs)
    profiled = [r['id'] for r in runs if r['profiled']]
    rows = []
    for k in sorted(main['rows']):
        wl, pr, name, unit = k
        mode = main['rows'][k]['mode']
        margin, cls = margin_for(name, mode)
        if margin is None:
            continue
        sign = 1 if higher_better(name, mode) else -1
        is_tpl = any(is_template(r, wl, pr) for r in runs)
        per_run, states, void_reasons, unpooled = [], [], [], []
        main_issue = None
        for r in runs:
            if k not in r['rows']:
                unpooled.append(f'{r["id"]}: absent')
                continue
            vr = r['void_reason'].get((wl, pr)) or r['void_reason'].get((wl, None))
            if vr:
                void_reasons.append(vr)
            if r['asym']:
                void_reasons.append(r['asym'])
            if is_tpl:
                states.append(template_state(r, wl, pr))
            bs, hs = estimate(r['rows'][k], is_tpl and not r['round0'])
            issue = value_issue(bs, hs)
            if issue:
                if r is main:
                    main_issue = issue
                else:
                    unpooled.append(f'{r["id"]}: {issue}')
                continue
            lb, lh = [math.log(x) for x in bs], [math.log(x) for x in hs]
            d, se, df = welch(lb, lh, sign)
            lo, hi, p = interval(d, se, df)
            per_run.append(dict(run=r['id'], main=r is main, cpu=r['cpu'], nb=len(bs), nh=len(hs), d=d, se=se, df=df,
                                ratio=math.exp(d), lo=lo, hi=hi, p=p, bs=bs, hs=hs))
        if is_tpl:
            tstate = ('differ' if 'differ' in states else 'match' if states and all(s == 'match' for s in states)
                      else 'unknown' if 'unknown' in states or not states
                      else 'unreplicated' if 'unreplicated' in states else 'layout')
        else:
            tstate = None
        mrow = main['rows'][k]
        row = dict(workload=wl, params=pr, metric=name or 'score', unit=unit, mode=mode, primary=name == '',
                   cls=cls, margin=margin, flags=[], template=tstate, unpooled=unpooled or None)
        if unpooled and len(runs) > 1:
            row['flags'].append('pool-')
        if tstate == 'layout':
            row['flags'].append('layout')
            row['template_bytes'] = next(template_bytes(r, wl, pr) for r in runs if k in r['rows'])
        main_e = next((e for e in per_run if e['main']), None)
        if main_e is None:
            nan = float('nan')
            row.update(ratio=nan, lo=nan, hi=nan, p=nan, df=nan, mw_p=nan, hl=nan, sep=False,
                       n_base=len([v for v in mrow['base'].values() if v is not None]),
                       n_head=len([v for v in mrow['head'].values() if v is not None]), Q=None, Q_p=None, tau2=None,
                       by_cpu=None, runs=[], reason=main_issue,
                       base=[float(f'{x:.6g}') for _, x in sorted(mrow['base'].items()) if x is not None],
                       head=[float(f'{x:.6g}') for _, x in sorted(mrow['head'].items()) if x is not None])
        else:
            d, se, df, Q, pQ, tau2 = pool([(e['d'], e['se'], e['df']) for e in per_run])
            lo, hi, p = interval(d, se, df)
            mb, mh = main_e['bs'], main_e['hs']
            sep = max(mb) < min(mh) or max(mh) < min(mb)
            # Hodges-Lehmann stratified by run (pairs within a run only) and Mann-Whitney (van Elteren across runs),
            # so that a level difference between runs is not read as a difference between sides.
            hl = math.exp(statistics.median(sign * (math.log(y) - math.log(x))
                                            for e in per_run for x in e['bs'] for y in e['hs']))
            mwp = mw_p(mb, mh) if len(per_run) == 1 else van_elteren_p([(e['bs'], e['hs']) for e in per_run])
            if len(per_run) > 1 and pQ < 0.05:
                row['flags'].append('Q!')
            by_cpu = {}
            groups = {}
            for e in per_run:
                groups.setdefault(e['cpu'] or '?', []).append((e['d'], e['se'], e['df']))
            if len(groups) > 1:
                gs = []
                for c, g in sorted(groups.items()):
                    gd, gse, gdf, _, _, _ = pool(g)
                    glo, ghi, _ = interval(gd, gse, gdf)
                    by_cpu[c] = dict(ratio=math.exp(gd), lo=glo, hi=ghi, runs=len(g))
                    gs.append((gd, gse, gdf))
                if pool(gs)[4] < 0.05:
                    row['flags'].append('cpu!')
            if bimodal(mb) or bimodal(mh):
                row['flags'].append('B')
            allb = [x for e in per_run for x in e['bs']]
            allh = [x for e in per_run for x in e['hs']]
            row.update(ratio=math.exp(d), lo=lo, hi=hi, p=p, df=df, mw_p=mwp, hl=hl, sep=sep, n_base=len(allb),
                       n_head=len(allh), Q=Q if len(per_run) > 1 else None, Q_p=pQ if len(per_run) > 1 else None,
                       tau2=tau2 if len(per_run) > 1 else None, by_cpu=by_cpu or None,
                       runs=[{kk: (round(v, 6) if isinstance(v, float) else v) for kk, v in e.items()
                              if kk not in ('bs', 'hs', 'd')} for e in per_run],
                       base=[float(f'{x:.6g}') for x in mb], head=[float(f'{x:.6g}') for x in mh])
            if cls in VALUE_COUNT_CLASSES:
                ref = statistics.median(allb + allh)
                row['counts_base'] = value_counts(allb, cls, ref)
                row['counts_head'] = value_counts(allh, cls, ref)
        if wl.startswith('GroupCommitBenchmark') and 'ubuntu' in main['dir'] and \
                int(dict(kv.split('=', 1) for kv in pr.split(',') if '=' in kv).get('committers', '0') or 0) >= 64:
            row['flags'].append('needs-windows')
        if profiled:
            row['verdict'] = 'PROFILED'
        elif void_reasons:
            row['verdict'] = 'VOID'
            row['reason'] = void_reasons[0]
        elif main_e is None:
            row['verdict'] = 'UNJUDGED'
        elif is_tpl and tstate not in ('match', 'layout') and not (
                judge_unverified and tstate in ('unknown', 'unreplicated')):
            row['verdict'] = 'TEMPLATE'
        else:
            row['verdict'] = None
        rows.append(row)
    family = [r for r in rows if r['verdict'] is None]
    rej, adj = bh([r['p'] for r in family], q) if family else ([], [])
    for r, rj, a in zip(family, rej, adj):
        m = r['margin']
        r['q'] = a
        inside = r['lo'] >= 1 / (1 + m) and r['hi'] <= 1 + m
        if inside:
            r['verdict'] = 'NEUTRAL'
            if rj:
                r['flags'].append('sig')
        elif rj:
            r['verdict'] = 'WIN' if r['ratio'] > 1 else 'LOSS'
            r['small'] = 1 / (1 + m) < r['ratio'] < 1 + m
            if r['mw_p'] > 0.10:
                r['flags'].append('mw!')
            if (r['hl'] > 1) != (r['ratio'] > 1) or not (r['lo'] <= r['hl'] <= r['hi']):
                r['flags'].append('hl!')
            if 'needs-windows' in r['flags']:
                # Ubuntu GroupCommit c>=64 cannot be credited or blamed without Windows agreeing
                r['flags'][r['flags'].index('needs-windows')] = f"needs-windows({r['verdict']})"
                r['would_be'] = r['verdict'] + (' small' if r.pop('small') else '')
                r['verdict'] = 'INCONCLUSIVE'
        else:
            r['verdict'] = 'INCONCLUSIVE'
    counts = {}
    for r in rows:
        counts[r['verdict']] = counts.get(r['verdict'], 0) + 1
    return dict(compare=os.path.basename(main['dir']), dir=main['dir'], run=main['id'],
                pooled=[r['id'] for r in runs[1:]], cpu={r['id']: r['cpu'] for r in runs},
                rounds={r['id']: r['rounds'] for r in runs}, round0=main['round0'], meta=main['meta'],
                void=bool(main['void']) or bool(main['asym']), profiled=bool(profiled), warnings=warnings, q=q,
                family=len(family), counts=counts, rows=rows)


# ------------------------------------------------------------------------------------------------------------- output
def fmt_row(r):
    if r['verdict'] == 'UNJUDGED':
        return (f"UNJUDGED     {r.get('reason')} n={r['n_base']}/{r['n_head']}  {r['workload']} [{r['params']}] "
                f"{r['metric']} {r['unit']}")
    tag = r['verdict'] + (' small' if r.get('small') else '')
    pr = f"{r['ratio']:.3f} [{r['lo']:.3f},{r['hi']:.3f}]"
    extra = f" Q={r['Q']:.1f}" if r.get('Q') is not None else ''
    fl = (' ' + ' '.join(r['flags'])) if r['flags'] else ''
    s = (f"{tag:<12} {pr} p={r['p']:.2g} q={r.get('q', float('nan')):.2g} mw={r['mw_p']:.2g} hl={r['hl']:.3f} "
         f"{'SEP' if r['sep'] else 'ov '} n={r['n_base']}/{r['n_head']} m={r['margin'] * 100:g}%{extra}{fl}  "
         f"{r['workload']} [{r['params']}] {r['metric']} {r['unit']}")
    if 'counts_base' in r and max(len(r['counts_base']), len(r['counts_head'])) <= 6:
        cb = ' '.join(f'{k}:{v}' for k, v in r['counts_base'].items())
        ch = ' '.join(f'{k}:{v}' for k, v in r['counts_head'].items())
        s += f"\n{'':13}MiB counts base {cb} | head {ch}"
    return s


def summary(res, budget):
    out = []
    cpu = ', '.join(f'{k}={v or "?"}' for k, v in res['cpu'].items())
    rounds = '+'.join(str(v) for v in res['rounds'].values())
    cnt = ' '.join(f'{k} {v}' for k, v in sorted(res['counts'].items()))
    out.append(f"== verdict {res['run']}/{res['compare']}  rounds {rounds}  cpu {cpu}")
    out.append(f"   judged {res['family']} metrics (BH q={res['q']}): {cnt}")
    out += res['warnings']
    order = {'LOSS': 0, 'WIN': 1, 'INCONCLUSIVE': 2}
    flagged = [r for r in res['rows'] if r['verdict'] in ('LOSS', 'WIN') or (r['verdict'] == 'INCONCLUSIVE' and r['primary'])]
    flagged.sort(key=lambda r: (order[r['verdict']], bool(r.get('small')), r['p']))
    tail = []
    tpl = sorted({(f"{r['workload']} [{r['params']}]", r['template']) for r in res['rows'] if r['verdict'] == 'TEMPLATE'})
    if tpl:
        why = {}
        for _, st in tpl:
            why[st] = why.get(st, 0) + 1
        tail.append(f"   TEMPLATE (not judged; " + ', '.join(f'{v} {k}' for k, v in sorted(why.items())) +
                    f"): {len(tpl)} workloads, e.g. {tpl[0][0]} ({tpl[0][1]})")
    lay = {}
    for r in res['rows']:
        if 'layout' in r['flags'] and r.get('template_bytes'):
            tb = r['template_bytes']
            for n in tb['base']:
                lay.setdefault(n, (tb['base'][n], tb['head'].get(n)))
    if lay:
        shown = '; '.join(f"{n} base {'/'.join(map(str, b))} head {'/'.join(map(str, h or []))}"
                          for n, (b, h) in sorted(lay.items())[:3])
        tail.append(f"   LAYOUT CHANGE (judged, flag layout: both sides built in order, so the head changed its own "
                    f"template layout): {len(lay)} templates, bytes {shown}")
    for v in ('VOID', 'UNJUDGED'):
        lst = [r for r in res['rows'] if r['verdict'] == v]
        if lst:
            tail.append(f"   {v}: {len(lst)} rows, e.g. {lst[0]['workload']} [{lst[0]['params']}] {lst[0]['metric']}: "
                        f"{lst[0].get('reason')}")
    lines = []
    for r in flagged:
        lines += fmt_row(r).split('\n')
    room = budget - len(out) - len(tail)
    if len(lines) > room:
        lines = lines[:max(0, room - 1)] + [f'   ... {len(flagged)} flagged rows in all; the rest in verdict.json']
    return out + lines + tail


def write_json(res_list):
    by_run = {}
    for res in res_list:
        by_run.setdefault(os.path.dirname(res['dir']), []).append(res)
    for rd, lst in by_run.items():
        path = os.path.join(rd, 'verdict.json')
        try:
            doc = json.load(open(path, encoding='utf-8'))
        except (OSError, ValueError):
            doc = {}
        doc['tool'] = 'verdict.py'
        doc['margins'] = [dict(metric=p, mode=md, margin=m, cls=c) for p, md, m, c in MARGINS]
        doc.setdefault('compares', {})
        for res in lst:
            doc['compares'][res['compare']] = res
        tmp = path + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(doc, f, indent=1)
        os.replace(tmp, path)


def cpu_key(run):
    """The pooling group of a run: its CPU model, or the run alone when the model is unknown (nothing confirms it)."""
    return run['cpu'] or f"an unknown CPU ({run['id']})"


def analyze_by_cpu(main_sub, pool_subs, q=Q_DEFAULT, judge_unverified=False):
    """Judges the main run pooled with the pooled runs on its CPU model; the pooled runs on other models are judged per
    model, among themselves, into the result's 'separate' list."""
    main = load_run(main_sub)
    others = [load_run(s) for s in pool_subs]
    same = [r for r in others if main['cpu'] and r['cpu'] == main['cpu']]
    rest = [r for r in others if not any(r is s for s in same)]
    res = analyze_runs([main] + same, q, judge_unverified)
    if rest:
        groups = {}
        for r in rest:
            groups.setdefault(cpu_key(r), []).append(r)
        res['separate'] = [analyze_runs(rs, q, judge_unverified) for rs in groups.values()]
        res['warnings'].insert(0, f"NOTE not pooled across CPU models: main run {main['id']} ran on {cpu_key(main)}; "
                               + '; '.join(f"{', '.join(r['id'] for r in rs)} on {k}" for k, rs in groups.items())
                               + ' (judged separately below; --pool-across-cpus pools them anyway)')
    return res


def main(argv):
    args, pools, q, judge, quiet, nowrite, across = [], [], Q_DEFAULT, False, False, False, False
    i = 0
    mode = None
    if argv[:1] == ['--cpu-of']:
        if len(argv) != 2:
            print(__doc__)
            return 2
        print(cpu_model(argv[1]) or '')
        return 0
    while i < len(argv):
        a = argv[i]
        if a == '--pool':
            mode = 'pool'
        elif a == '--pool-across-cpus':
            across = True
        elif a == '--q':
            q = float(argv[i + 1])
            i += 1
        elif a == '--judge-unverified-templates':
            judge = True
        elif a == '--quiet':
            quiet = True
        elif a == '--no-write':
            nowrite = True
        elif mode == 'pool':
            pools.append(a)
        else:
            args.append(a)
        i += 1
    if not args:
        print(__doc__)
        return 2
    mains = expand(args)
    if not mains:
        print(f'WARNING no compare dir (base-N.json/head-N.json or compare-*/) in {" ".join(args)}')
        return 2
    names = {os.path.basename(s) for s in mains}
    pool_subs, bad = [], []
    for p in pools:
        subs = expand([p])
        if not os.path.isdir(p):
            bad.append(f'{p}: no such directory (not downloaded?)')
        elif not [s for s in subs if os.path.basename(s) in names]:
            bad.append(f'{p}: no compare dir named {" or ".join(sorted(names))} (found: '
                       f'{" ".join(os.path.basename(s) for s in subs) or "none"})')
        pool_subs += subs
    if bad:
        for b in bad:
            print(f'WARNING --pool {b}; nothing judged')
        return 2
    results = []
    for sub in mains:
        mine = [p for p in pool_subs if os.path.basename(p) == os.path.basename(sub) and os.path.normpath(p) != sub]
        results.append(analyze(sub, mine, q, judge) if across else analyze_by_cpu(sub, mine, q, judge))
    if not nowrite:
        write_json(results)
    if not quiet:
        budget = MAX_LINES // max(1, sum(1 + len(res.get('separate') or []) for res in results))
        for res in results:
            print('\n'.join(summary(res, budget)))
            for sep in res.get('separate') or []:
                print(f"-- NOT POOLED with run {res['run']}: another CPU model, judged on its own")
                print('\n'.join(summary(sep, budget - 1)))
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
