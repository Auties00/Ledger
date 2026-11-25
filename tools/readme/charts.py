#!/usr/bin/env python3
"""Draws the benchmark charts of the root README from benchmarks.tsv, as charts.tsv selects them.

    python tools/readme/charts.py          # rewrite the charts and the README block
    python tools/readme/charts.py --check  # exit 1 when a committed file is stale

For every chart of charts.tsv and every OS with data for it, it writes .github/assets/bench-<chart>-<os>-light.svg and
-dark.svg, and it rewrites the block between the "bench:begin" and "bench:end" markers of README.md: per chart its
heading and caption, the Linux chart, the Windows and macOS charts in one <details>, and, under the charts that have
"loss" lines, the list of where another engine costs less than Ledger. Standard library only.

Every row compares each engine's cost with Ledger's (encrypted, the reference at 1x): its time per operation, the
inverse of its throughput, or the bytes a counter measured. On the logarithmic axis a bar runs right when the engine
costs more than Ledger and left when it costs less, and its label says by how much ("4.7x slower"). A time whose
99.9% confidence interval overlaps Ledger's is drawn pale with a "~" before its label, since the run does not settle
which is lower. Each row's title carries Ledger's own result; every engine's absolute result is in the tooltip of its
bar and in benchmarks.tsv.
"""
import math
import os
import re
import sys
from xml.sax.saxutils import escape

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
DATA = os.path.join(HERE, 'benchmarks.tsv')
CONFIG = os.path.join(HERE, 'charts.tsv')
ASSETS = os.path.join(ROOT, '.github', 'assets')
README = os.path.join(ROOT, 'README.md')
BEGIN, END = '<!-- bench:begin -->', '<!-- bench:end -->'

REFERENCE = 'ledger'
# Every engine has one color, the same in every chart and both themes: each clears 3:1 against the light and the dark
# background, and neighbours in this order stay apart for the common color-vision deficiencies. Each bar also carries
# its engine's name, so no reading depends on color alone.
ENGINES = {
    'ledger-plain': '#2f7fd8',
    'mvstore': '#e0702a',
    'mvstore-encrypted': '#9a520e',
    'rocksdb': '#0f9d82',
    'lmdb': '#8a63d2',
    'sqlite': '#d1478a',
    'berkeley-je': '#5f8f1f',
    'floor': '#8b949e',
}
FLOOR = 'floor'
FLOOR_NAME = 'floor (device bound)'
# Left out of the lists of where another engine costs less than Ledger: Ledger's own variant and the floor, no engine
NOT_COMPARED = (FLOOR, 'ledger-plain')
UNKNOWN_ENGINE = '#8b949e'
SYSTEMS = {'ubuntu-latest': 'Linux', 'windows-latest': 'Windows', 'macos-latest': 'macOS'}
# The OS whose chart the README shows first; the others go in one <details> per chart
MAIN_SYSTEM = 'ubuntu-latest'
UNITS = {'ns/op': 'ns', 'us/op': 'us', 'ms/op': 'ms', 's/op': 's'}

THEMES = {
    'light': dict(ink='#1f2328', muted='#59636e', grid='#d8dee4'),
    'dark': dict(ink='#f0f6fc', muted='#9198a1', grid='#3d444d'),
}

WIDTH = 800
LABEL_X = 8
NAME_X = 16           # engine names
NAMES_END = 128       # the right end of the longest engine name
PLOT_L, PLOT_R = 150, 690
TITLE_H = 24          # a row's title line
BAR_PITCH = 13
BAR = 9
ROW_GAP = 12
PALE = 0.35           # the opacity of a bar inside Ledger's 99.9% interval
CHAR_W = 6.3          # the average width of a character at 11-12px, to place labels
FONT = "-apple-system, BlinkMacSystemFont, 'Segoe UI', 'Noto Sans', Helvetica, Arial, sans-serif"
POWERS_OF_TWO = [2 ** n for n in range(-6, 7)]
DECADES = [10.0 ** n for n in range(-4, 5)]


def read_config():
    charts, rows, excluded, losses = {}, [], {}, []
    with open(CONFIG, encoding='utf-8') as f:
        for n, line in enumerate(f, 1):
            line = line.rstrip('\n')
            if not line.strip() or line.startswith('#'):
                continue
            p = line.split('\t')
            if p[0] == 'chart' and len(p) == 4:
                charts[p[1]] = dict(id=p[1], heading=p[2], caption=p[3])
            elif p[0] == 'exclude' and len(p) == 4:
                excluded[(p[1], p[2])] = p[3]
            elif p[0] in ('row', 'loss') and len(p) == 7:
                if p[1] not in charts:
                    raise SystemExit(f'{CONFIG}:{n}: {p[0]} of an unknown chart "{p[1]}"')
                (rows if p[0] == 'row' else losses).append(
                    dict(chart=p[1], group=p[2], benchmark=p[3], params='' if p[4] == '(none)' else p[4],
                         metric=p[5], label=p[6]))
            else:
                raise SystemExit(f'{CONFIG}:{n}: expected "chart" with 3 fields, "row" or "loss" with 6, '
                                 f'or "exclude" with 3')
    return charts, rows, excluded, losses


def read_data():
    results, runs = [], {}
    with open(DATA, encoding='utf-8') as f:
        columns = None
        for line in f:
            line = line.rstrip('\n')
            if line.startswith('#'):
                m = re.match(r'# run (\d+): (\S+) on (\S+), (\d+) rounds?, commit (\w+)', line)
                if m:
                    runs[(m[3], m[2])] = dict(id=m[1], rounds=m[4], commit=m[5])
                continue
            if not line.strip():
                continue
            fields = line.split('\t')
            if columns is None:
                columns = fields
                continue
            results.append(dict(zip(columns, fields)))
    return results, runs


def num(text):
    return float(text) if text not in ('', None) else None


def metric_value(result, metric):
    """The cost the row compares: lower is better."""
    if metric == 'score':
        score = num(result['score'])
        if score is None or score <= 0:
            return None
        return 1 / score if result['mode'] == 'thrpt' else score
    names = metric.split('-')
    values = [num(result.get(name)) for name in names]
    if any(value is None for value in values):
        return None
    return values[0] - sum(values[1:])


def number(value):
    """A number as the workflow summaries print it."""
    magnitude = abs(value)
    if magnitude >= 100:
        return f'{value:,.0f}'
    if magnitude >= 10:
        return f'{value:.1f}'
    return f'{value:.3g}'


def size(value):
    magnitude = abs(value)
    for unit, scale in (('GiB', 1 << 30), ('MiB', 1 << 20), ('KiB', 1 << 10)):
        if magnitude >= scale:
            return f'{value / scale:,.1f} {unit}'
    return f'{value:,.0f} B'


def factor(ratio):
    """A ratio of at least 1, two significant figures."""
    if ratio >= 100:
        return f'{ratio:,.0f}x'
    if ratio >= 9.95:
        return f'{ratio:.0f}x'
    return f'{ratio:.1f}x'


def same(ratio):
    """Whether a ratio rounds to no difference at all."""
    return (ratio if ratio >= 1 else 1 / ratio) < 1.005


def overlaps(result, reference, metric):
    """Whether two times are within their 99.9% confidence intervals of each other; counters carry no interval."""
    if metric != 'score':
        return False
    a, b = num(result['error']), num(reference['error'])
    if not a or not b or a != a or b != b:
        return False
    return abs(float(result['score']) - float(reference['score'])) <= a + b


def words(metric):
    """What a cost above and below Ledger's is called."""
    if metric == 'score':
        return 'slower', 'faster'
    if metric == 'sizeOnDisk':
        return 'larger', 'smaller'
    return 'more', 'less'


def comparison(ratio, metric):
    """How an engine compares with Ledger, short: a percentage below 1.1x either way, a factor above."""
    if same(ratio):
        return 'same'
    more, less = words(metric)
    factor_ = ratio if ratio >= 1 else 1 / ratio
    word = more if ratio > 1 else less
    return f'{(factor_ - 1) * 100:.0f}% {word}' if factor_ < 1.095 else f'{factor(factor_)} {word}'


def score_text(result, interval=True):
    error = num(result['error'])
    unit = result['unit']
    text = number(float(result['score']))
    if interval and error and error > 0:
        text += f' +- {number(error)}'
    return f'{text} {unit if interval else UNITS.get(unit, unit)}'


def metric_text(result, metric, interval=True):
    if metric == 'score':
        return score_text(result, interval)
    value = metric_value(result, metric)
    return size(value) if value is not None else '-'


def reference_text(reference, metric):
    """Ledger's own result for a row's title, with the payload when the row counts the bytes written."""
    text = metric_text(reference, metric, interval=False)
    payload = num(reference.get('payloadBytes'))
    if metric == 'bytesWritten' and payload:
        text += f' for {size(payload)} of payload'
    return text


def engine_order(engines):
    known = [engine for engine in ENGINES if engine in engines]
    return known + sorted(engine for engine in engines if engine not in ENGINES and engine != REFERENCE)


def engine_name(engine):
    return FLOOR_NAME if engine == FLOOR else engine


def domain(ratios):
    lo = min([0.5] + ratios)
    hi = max([2.0] + ratios)
    ticks = POWERS_OF_TWO if (hi <= 64 and lo >= 1 / 64) else DECADES
    lo = max(lo, ticks[0])
    hi = min(hi, ticks[-1])
    dmin = max(t for t in ticks if t <= lo)
    dmax = min(t for t in ticks if t >= hi)
    # Keep the reference line inside the plot with room on its weaker side
    dmin = min(dmin, ticks[ticks.index(1) - 1] if 1 in ticks else 0.5)
    dmax = max(dmax, ticks[ticks.index(1) + 1] if 1 in ticks else 2)
    return dmin, dmax, [t for t in ticks if dmin <= t <= dmax]


def bar_path(x0, x1, y, h, r=3):
    """A bar from the reference line x0 to its data end x1, rounded at the data end only."""
    width = abs(x1 - x0)
    if width < 2 * r + 1:
        x = min(x0, x1)
        return f'M{x:.1f},{y:.1f}h{max(width, 2):.1f}v{h}h{-max(width, 2):.1f}z'
    s = 1 if x1 > x0 else -1
    sweep = 1 if s > 0 else 0
    return (f'M{x0:.1f},{y:.1f}H{x1 - s * r:.1f}a{r},{r} 0 0 {sweep} {s * r},{r}v{h - 2 * r}'
            f'a{r},{r} 0 0 {sweep} {-s * r},{r}H{x0:.1f}z')


def tick_label(t):
    if t == 1:
        return '1x'
    value = t if t > 1 else 1 / t
    return f'{value:,.0f}x' if value >= 1 else f'{value:g}x'


def source_text(system, groups, runs):
    """The runs a chart of one OS draws from, by commit and number of rounds."""
    by_source = {}
    for group in groups:
        run = runs.get((system, group))
        if run:
            by_source.setdefault((run['commit'][:7], run['rounds']), set()).add(run['id'])
    parts = []
    for (commit, rounds), ids in sorted(by_source.items()):
        ids = sorted(ids)
        name = 'run ' + ids[0] if len(ids) == 1 else 'runs ' + ', '.join(ids)
        parts.append(f'{name} of {commit}, {rounds} {"round" if rounds == "1" else "rounds"}')
    return f'{SYSTEMS.get(system, system)} ({system}): ' + '; '.join(parts)


def draw(chart, system, entries, source, theme):
    """entries: [(row, reference result, {engine: (result, ratio or None)})] in the order of charts.tsv, and
    [(row, None, None)] for a row whose metric is excluded on this OS, which shows the exclusion's reason instead."""
    c = THEMES[theme]
    rows = [entry for entry in entries if entry[1] is not None]
    engines = engine_order({engine for _, _, others in rows for engine in others})
    ratios = [ratio for _, _, others in rows for _, ratio in others.values() if ratio]
    dmin, dmax, ticks = domain(ratios)
    span = math.log(dmax / dmin)

    def x(v):
        v = min(max(v, dmin), dmax)
        return PLOT_L + (PLOT_R - PLOT_L) * math.log(v / dmin) / span

    # Legend items, flowed into lines of the chart's width
    items = [('reference', 'Ledger (1x)')] + [(engine, engine_name(engine)) for engine in engines]
    if any(overlaps(result, reference, row['metric']) for row, reference, others in rows
           for result, ratio in others.values() if ratio):
        items.append(('pale', '~ pale: within the 99.9% intervals'))
    lines, line, lx = [], [], LABEL_X
    for kind, text in items:
        width = 17 + CHAR_W * len(text) + 16
        if line and lx + width > WIDTH - 8:
            lines.append(line)
            line, lx = [], LABEL_X
        line.append((kind, text, lx))
        lx += width
    lines.append(line)

    header_y = 16
    legend_y = header_y + 12
    top = legend_y + 20 * len(lines) + 38
    height = (top + sum(TITLE_H + BAR_PITCH * len(engines) + ROW_GAP for _ in rows)
              + (TITLE_H + 6) * (len(entries) - len(rows)) + 4)
    heading = f'{chart["heading"]} on {SYSTEMS.get(system, system)}'
    o = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {WIDTH} {height}" width="{WIDTH}" height="{height}" '
         f'font-family="{FONT}" role="img" aria-label="{escape(heading)}: each engine\'s cost relative to Ledger">',
         f'<title>{escape(heading)}: each engine\'s cost relative to Ledger (a bar to the right is slower or larger '
         f'than Ledger, to the left faster or smaller)</title>',
         '<defs>',
         f'<pattern id="hatch" width="5" height="5" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">'
         f'<rect width="5" height="5" fill="{ENGINES[FLOOR]}" fill-opacity="0.25"/>'
         f'<line x1="0" y1="0" x2="0" y2="5" stroke="{ENGINES[FLOOR]}" stroke-width="2.5"/></pattern>',
         '</defs>',
         f'<text x="{LABEL_X}" y="{header_y}" font-size="13" font-weight="600" fill="{c["ink"]}">'
         f'{escape(SYSTEMS.get(system, system))}<tspan font-weight="400" fill="{c["muted"]}">'
         f'{escape(source[len(SYSTEMS.get(system, system)):])}</tspan></text>']

    for i, line in enumerate(lines):
        ly = legend_y + 20 * i
        for kind, text, lx in line:
            if kind == 'reference':
                o.append(f'<line x1="{lx + 6}" y1="{ly}" x2="{lx + 6}" y2="{ly + 12}" stroke="{c["ink"]}" '
                         f'stroke-width="2"/>')
            elif kind == 'pale':
                o.append(f'<rect x="{lx}" y="{ly}" width="12" height="12" rx="2" fill="{c["muted"]}" '
                         f'fill-opacity="{PALE}"/>')
            else:
                fill = 'url(#hatch)' if kind == FLOOR else ENGINES.get(kind, UNKNOWN_ENGINE)
                stroke = f' stroke="{ENGINES[FLOOR]}" stroke-width="1"' if kind == FLOOR else ''
                o.append(f'<rect x="{lx}" y="{ly}" width="12" height="12" rx="2" fill="{fill}"{stroke}/>')
            o.append(f'<text x="{lx + 17}" y="{ly + 10.5}" font-size="12" fill="{c["muted"]}">{escape(text)}</text>')

    # Axis: the halves, the ticks and the reference line
    axis_y = top - 12
    x1 = x(1)
    o.append(f'<text x="{x1 - 8:.1f}" y="{axis_y - 16}" font-size="11" text-anchor="end" fill="{c["muted"]}">'
             f'&lt; faster or smaller</text>')
    o.append(f'<text x="{x1 + 8:.1f}" y="{axis_y - 16}" font-size="11" fill="{c["muted"]}">slower or larger &gt;</text>')

    def grid(y0, y1):
        for t in ticks:
            o.append(f'<line x1="{x(t):.1f}" y1="{y0:.1f}" x2="{x(t):.1f}" y2="{y1:.1f}" '
                     f'stroke="{c["ink"] if t == 1 else c["grid"]}" stroke-width="{2 if t == 1 else 1}"/>')

    grid(axis_y + 4, top)
    for t in ticks:
        weight = ' font-weight="600"' if t == 1 else ''
        o.append(f'<text x="{x(t):.1f}" y="{axis_y}" font-size="11" text-anchor="middle" '
                 f'fill="{c["ink"] if t == 1 else c["muted"]}"{weight}>{tick_label(t)}</text>')

    y = top
    for index, (row, reference, others) in enumerate(entries):
        if index:
            o.append(f'<line x1="{LABEL_X}" y1="{y - ROW_GAP / 2:.1f}" x2="{WIDTH - 8}" y2="{y - ROW_GAP / 2:.1f}" '
                     f'stroke="{c["grid"]}" stroke-width="1" stroke-dasharray="1 4"/>')
        if reference is None:
            o.append(f'<text x="{LABEL_X}" y="{y + 15}" font-size="13" font-weight="600" fill="{c["ink"]}">'
                     f'{escape(row["label"])}<tspan font-weight="400" font-style="italic" fill="{c["muted"]}">  '
                     f'{escape(row["excluded"])}</tspan></text>')
            y += TITLE_H + 6
            continue
        metric = row['metric']
        o.append(f'<text x="{LABEL_X}" y="{y + 15}" font-size="13" font-weight="600" fill="{c["ink"]}">'
                 f'<title>Ledger: {escape(metric_text(reference, metric))}</title>{escape(row["label"])}'
                 f'<tspan font-weight="400" fill="{c["muted"]}">  Ledger {escape(reference_text(reference, metric))}'
                 f'</tspan></text>')
        y += TITLE_H
        grid(y - 2, y + BAR_PITCH * len(engines) + 2)
        for engine in engines:
            mid = y + BAR_PITCH / 2
            o.append(f'<text x="{NAME_X}" y="{mid + 4:.1f}" font-size="11" fill="{c["muted"]}">{escape(engine)}</text>')
            entry = others.get(engine)
            if entry is None or entry[1] is None:
                text = 'not run' if entry is None else 'no value'
                o.append(f'<text x="{x1 + 8:.1f}" y="{mid + 4:.1f}" font-size="10.5" font-style="italic" '
                         f'fill="{c["muted"]}">{text}</text>')
                y += BAR_PITCH
                continue
            result, ratio = entry
            pale = overlaps(result, reference, metric)
            xe = x(ratio)
            label = ('~' if pale else '') + comparison(ratio, metric)
            tip = f'{engine}: {metric_text(result, metric)}, {label} than Ledger ({metric_text(reference, metric)})'
            if engine == FLOOR:
                paint = f'fill="url(#hatch)" stroke="{ENGINES[FLOOR]}" stroke-width="1"'
            else:
                paint = f'fill="{ENGINES.get(engine, UNKNOWN_ENGINE)}"'
            if pale:
                paint += f' fill-opacity="{PALE}"'
            if abs(xe - x1) < 1.5:
                o.append(f'<circle cx="{x1:.1f}" cy="{mid:.1f}" r="3.5" {paint}><title>{escape(tip)}</title></circle>')
            else:
                o.append(f'<path d="{bar_path(x1, xe, mid - BAR / 2, BAR)}" {paint}><title>{escape(tip)}</title></path>')
            width = CHAR_W * len(label)
            if ratio >= 1 or min(xe, x1) - 5 - width < NAMES_END:
                lx = max(xe, x1) + 5
                o.append(f'<text x="{lx:.1f}" y="{mid + 4:.1f}" font-size="11" fill="{c["ink"]}">{escape(label)}</text>')
            else:
                o.append(f'<text x="{min(xe, x1) - 5:.1f}" y="{mid + 4:.1f}" font-size="11" text-anchor="end" '
                         f'fill="{c["ink"]}">{escape(label)}</text>')
            y += BAR_PITCH
        y += ROW_GAP
    o.append('</svg>')
    return '\n'.join(o) + '\n'


def comparisons(results, reference, metric):
    """{engine: (result, ratio of its cost to Ledger's, or None)} for every engine but Ledger."""
    base = metric_value(reference, metric)
    others = {}
    for engine, result in results.items():
        if engine == REFERENCE:
            continue
        value = metric_value(result, metric)
        ratio = value / base if value is not None and base and base > 0 and value > 0 else None
        others[engine] = (result, ratio)
    return others


def chart_rows(chart_id, system, config_rows, index, excluded):
    """The rows of a chart on one OS that have a Ledger result, with every engine's ratio, in order; a row whose metric
    is excluded on that OS comes as (row with its reason, None, None)."""
    out = []
    for row in config_rows:
        if row['chart'] != chart_id:
            continue
        results = index.get((system, row['group'], row['benchmark'], row['params']), {})
        reference = results.get(REFERENCE)
        if reference is None:
            continue
        if (system, row['metric']) in excluded:
            out.append((dict(row, excluded=excluded[(system, row['metric'])]), None, None))
            continue
        out.append((row, reference, comparisons(results, reference, row['metric'])))
    return out


def loss_lines(chart_id, losses, index, excluded, systems):
    """The loss lines of a chart as a list ranked by the largest factor: for each, every engine (not Ledger's own
    variant, not the floor) that costs less than Ledger on some OS by more than the 99.9% intervals, with the range of
    its factors and the OSes it wins on when not all of them."""
    items = []
    for loss in losses:
        if loss['chart'] != chart_id:
            continue
        metric = loss['metric']
        wins, measured = {}, []
        for system in systems:
            results = index.get((system, loss['group'], loss['benchmark'], loss['params']), {})
            reference = results.get(REFERENCE)
            if reference is None or (system, metric) in excluded:
                continue
            measured.append(system)
            for engine, (result, ratio) in comparisons(results, reference, metric).items():
                if (engine in NOT_COMPARED or not ratio or ratio >= 1 or same(ratio)
                        or overlaps(result, reference, metric)):
                    continue
                wins.setdefault(engine, []).append((1 / ratio, system))
        if not wins:
            print(f'charts.tsv: no engine costs less than Ledger in "{loss["label"]}" any more', file=sys.stderr)
            continue
        ranked = sorted(wins.items(), key=lambda item: -max(f for f, _ in item[1]))
        where = {engine: [s for s in systems if s in {s for _, s in found}] for engine, found in ranked}
        # The OSes every listed engine wins on, when that is not every OS measured, go after the label once
        common = set.intersection(*(set(w) for w in where.values()))
        shared = len(common) < len(measured) and all(set(w) == common for w in where.values())
        label = loss['label'] + (f' (on {", ".join(SYSTEMS[s] for s in systems if s in common)})' if shared else '')
        parts = []
        for i, (engine, found) in enumerate(ranked[:4]):
            lo, hi = min(f for f, _ in found), max(f for f, _ in found)
            text = f'{engine} {factor(lo)}' if factor(lo) == factor(hi) else f'{engine} {factor(lo)[:-1]}-{factor(hi)}'
            if i == 0:
                text += ' ' + words(metric)[1]
            if not shared and len(found) < len(measured):
                text += f' ({", ".join(SYSTEMS[s] for s in where[engine])})'
            parts.append(text)
        if len(ranked) > 4:
            parts.append(f'and {len(ranked) - 4} more {"engine" if len(ranked) == 5 else "engines"}')
        items.append((max(f for f, _ in ranked[0][1]), f'- {label}: {", ".join(parts)}.'))
    return [text for _, text in sorted(items, key=lambda item: -item[0])]


def picture(base, alt):
    return [f'<picture><source media="(prefers-color-scheme: dark)" srcset="{base}-dark.svg">'
            f'<img src="{base}-light.svg" width="100%" alt="{escape(alt)}"></picture>']


def readme_block(charts, config_rows, index, runs, excluded, losses):
    o = [BEGIN, '<!-- Generated by tools/readme/charts.py from tools/readme/benchmarks.tsv and tools/readme/charts.tsv; '
                'do not edit by hand. -->', '']
    systems = [s for s in SYSTEMS if any(key[0] == s for key in index)]
    files = {}
    for chart in charts.values():
        groups = sorted({row['group'] for row in config_rows if row['chart'] == chart['id']})
        drawn = []
        for system in systems:
            rows = chart_rows(chart['id'], system, config_rows, index, excluded)
            if any(reference is not None for _, reference, _ in rows):
                drawn.append(system)
                base = f'.github/assets/bench-{chart["id"]}-{system.split("-")[0]}'
                source = source_text(system, groups, runs)
                for theme in THEMES:
                    files[os.path.join(ROOT, f'{base}-{theme}.svg')] = draw(chart, system, rows, source,
                                                                            theme)
        if not drawn:
            continue
        o += [f'### {chart["heading"]}', '', chart['caption'], '']
        first = MAIN_SYSTEM if MAIN_SYSTEM in drawn else drawn[0]
        rest = [system for system in drawn if system != first]

        def chart_picture(system):
            name = SYSTEMS[system]
            return picture(f'.github/assets/bench-{chart["id"]}-{system.split("-")[0]}',
                           f'{chart["heading"]} on {name}: each engine\'s cost relative to Ledger\'s')

        o += chart_picture(first) + ['']
        if rest:
            o += ['<details>', f'<summary>{" and ".join(SYSTEMS[s] for s in rest)}</summary>', '']
            for system in rest:
                o += chart_picture(system)
            o += ['', '</details>', '']
        lost = loss_lines(chart['id'], losses, index, excluded, systems)
        if lost:
            o += ['Where another engine costs less than Ledger, by more than the 99.9% intervals:', ''] + lost + ['']
        missing = [SYSTEMS[s] for s in SYSTEMS if s not in drawn]
        if missing:
            o += [f'No {" or ".join(missing)} results for this chart yet.', '']
    o.append(END)
    return '\n'.join(o), files


def main():
    check = '--check' in sys.argv
    charts, config_rows, excluded, losses = read_config()
    results, runs = read_data()
    index = {}
    for result in results:
        key = (result['os'], result['group'], result['benchmark'], result['params'])
        index.setdefault(key, {})[result['engine']] = result
    block, files = readme_block(charts, config_rows, index, runs, excluded, losses)
    with open(README, encoding='utf-8') as f:
        readme = f.read()
    a, b = readme.find(BEGIN), readme.find(END)
    if a < 0 or b < a:
        raise SystemExit(f'{README}: no "{BEGIN}" ... "{END}" block')
    files[README] = readme[:a] + block + readme[b + len(END):]
    stale = []
    for path, text in files.items():
        old = None
        if os.path.exists(path):
            with open(path, encoding='utf-8') as f:
                old = f.read()
        if old != text:
            stale.append(os.path.relpath(path, ROOT))
            if not check:
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, 'w', encoding='utf-8', newline='\n') as f:
                    f.write(text)
    # Charts no longer drawn
    drawn = {os.path.normpath(path) for path in files}
    for name in sorted(os.listdir(ASSETS)) if os.path.isdir(ASSETS) else []:
        path = os.path.normpath(os.path.join(ASSETS, name))
        if name.startswith('bench-') and name.endswith('.svg') and path not in drawn:
            stale.append(os.path.relpath(path, ROOT) + ' (no longer drawn)')
            if not check:
                os.remove(path)
    if check and stale:
        print('stale: ' + ', '.join(stale))
        raise SystemExit(1)
    print('up to date' if not stale else ('stale: ' if check else 'wrote: ') + ', '.join(stale))


if __name__ == '__main__':
    main()
