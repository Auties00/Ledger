#!/usr/bin/env python3
"""Draws the benchmark charts of the root README from benchmarks.tsv, as charts.tsv selects them.

    python tools/readme/charts.py          # rewrite the charts and the README block
    python tools/readme/charts.py --check  # exit 1 when a committed file is stale

For every chart of charts.tsv and every OS with data for it, it writes .github/assets/bench-<chart>-<os>-light.svg and
-dark.svg, and it rewrites the block between the "bench:begin" and "bench:end" markers of README.md: per chart its
heading and caption, the Linux chart and the Windows and macOS charts in one <details>. Standard library only.

Every row compares each engine's cost with Ledger's (encrypted, the reference at 1x): its time per operation, the
inverse of its throughput, or the bytes a counter measured. On the logarithmic axis a bar runs right when the engine
costs more than Ledger and left when it costs less, and its label gives the factor ("4.7x") or, under 1.1x, the signed
percentage ("+8%"). A result the run does not settle is drawn pale with a "~" before its label: a time whose 99.9%
confidence interval overlaps Ledger's, or a memory result within 10% of Ledger's, since memory results carry no
interval. Each row's title carries Ledger's own result; every engine's absolute result is in benchmarks.tsv and in
the tooltip of its bar, which a browser shows when it opens the SVG itself but GitHub does not in a README.
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
# Every engine has one color in every chart, the same in both themes but where DARK_ENGINES gives another: each clears
# 3:1 against the background of its theme, and neighbours in this order stay apart for the common color-vision
# deficiencies. Each bar also carries its engine's name, so no reading depends on color alone.
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
# mvstore-encrypted's brown is only 3.2:1 on the dark background, and fades into it as a pale bar
DARK_ENGINES = {'mvstore-encrypted': '#c58a4a'}
FLOOR = 'floor'
# The legend's names. The floor is a plain file appended and forced per batch, which Ledger's log beats in most durable
# workloads, so it is named for what it does, not as a bound. ledger-plain, like every engine but ledger, stores the
# values a sealed workload encrypted itself, which is why it can be slower than ledger there.
LEGEND_NAMES = {FLOOR: 'floor (plain append + force)', 'ledger-plain': 'ledger-plain (unencrypted)'}
SEALED_PLAIN_NAME = 'ledger-plain (unencrypted, values sealed by the workload)'
UNKNOWN_ENGINE = '#8b949e'
SYSTEMS = {'ubuntu-latest': 'Linux', 'windows-latest': 'Windows', 'macos-latest': 'macOS'}
# The OS whose chart the README shows first; the others go in one <details> per chart
MAIN_SYSTEM = 'ubuntu-latest'
UNITS = {'ns/op': 'ns', 'us/op': 'us', 'ms/op': 'ms', 's/op': 's'}

THEMES = {
    # pale: the opacity of an unsettled bar; outline: whether it also gets a stroke of its color, so that on the dark
    # background a pale bar does not fade into it
    'light': dict(ink='#1f2328', muted='#59636e', grid='#d8dee4', pale=0.35, outline=False),
    'dark': dict(ink='#f0f6fc', muted='#9198a1', grid='#3d444d', pale=0.45, outline=True),
}

# Drawn 600 wide and shown at that size, so that a desktop shows the text at 12 to 14 px and a phone scales the chart
# down no further than its own width
WIDTH = 600
PAD = 8
TEXT = 13             # engine names, bar labels and the legend
SMALL = 12            # the header's runs and the axis
TITLE = 14            # a row's title
EM = 0.56             # the average width of a character in ems, to place and wrap text
NAMES_END = PAD + 17 * TEXT * EM  # the right end of the longest engine name, mvstore-encrypted
PLOT_L = NAMES_END + 12
TITLE_H = 24          # a row's title line
TITLE_LINE = 18       # the second line of a title that does not fit on one
BAR_PITCH = 17
BAR = 11
ROW_GAP = 12
UNSETTLED_MEMORY = 1.10  # a memory result closer to Ledger's than this factor is not settled: it has no interval
FONT = "-apple-system, BlinkMacSystemFont, 'Segoe UI', 'Noto Sans', Helvetica, Arial, sans-serif"
POWERS_OF_TWO = [2 ** n for n in range(-6, 7)]
DECADES = [10.0 ** n for n in range(-4, 5)]


def read_config():
    charts, rows, excluded = {}, [], {}
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
            elif p[0] == 'row' and len(p) == 7:
                if p[1] not in charts:
                    raise SystemExit(f'{CONFIG}:{n}: row of an unknown chart "{p[1]}"')
                rows.append(dict(chart=p[1], group=p[2], benchmark=p[3], params='' if p[4] == '(none)' else p[4],
                                 metric=p[5], label=p[6]))
            else:
                raise SystemExit(f'{CONFIG}:{n}: expected "chart" with 3 fields, "row" with 6 or "exclude" with 3 '
                                 f'(a result worth stating is charted as a row, not listed)')
    return charts, rows, excluded


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


def memory(metric):
    return 'resident' in metric or 'heap' in metric


def unsettled(result, reference, metric, ratio):
    """Whether the run does not settle which of two results is lower: two times within their 99.9% confidence
    intervals of each other, or two memory results, which carry no interval, within 10%. The other counters count the
    bytes an engine wrote or kept, which do not vary between runs as a time or a resident size does."""
    if memory(metric):
        return max(ratio, 1 / ratio) < UNSETTLED_MEMORY
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
    """How an engine compares with Ledger, in words: a percentage below 1.1x either way, a factor above."""
    if same(ratio):
        return 'the same as'
    more, less = words(metric)
    factor_ = ratio if ratio >= 1 else 1 / ratio
    word = more if ratio > 1 else less
    return (f'{(factor_ - 1) * 100:.0f}% {word}' if factor_ < 1.095 else f'{factor(factor_)} {word}') + ' than'


def short(ratio):
    """A bar's label: the factor, or under 1.1x the signed percentage; the side of the 1x line gives the direction."""
    if same(ratio):
        return '='
    factor_ = ratio if ratio >= 1 else 1 / ratio
    if factor_ < 1.095:
        return f'{"+" if ratio > 1 else "-"}{(factor_ - 1) * 100:.0f}%'
    return factor(factor_)


def width(text, size, bold=False):
    """The estimated width of a text, to place and wrap it."""
    return len(text) * size * (EM + (0.04 if bold else 0))


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


def legend_name(engine, sealed):
    if engine == 'ledger-plain' and sealed:
        return SEALED_PLAIN_NAME
    return LEGEND_NAMES.get(engine, engine)


def engine_color(engine, theme):
    if theme == 'dark' and engine in DARK_ENGINES:
        return DARK_ENGINES[engine]
    return ENGINES.get(engine, UNKNOWN_ENGINE)


def domain(bars):
    """The axis of a chart whose bars are [(ratio, label)]: its ends, its ticks and the right end of the plot. The
    plot leaves room for the longest label right of a bar, and the axis extends left until the label of every bar to
    the left fits at that bar's left end, right of the engine names."""
    ratios = [ratio for ratio, _ in bars]
    lo = min([0.5] + ratios)
    hi = max([2.0] + ratios)
    ticks = POWERS_OF_TWO if (hi <= 64 and lo >= 1 / 64) else DECADES
    lo = max(lo, ticks[0])
    hi = min(hi, ticks[-1])
    dmin = max(t for t in ticks if t <= lo)
    dmax = min(t for t in ticks if t >= hi)
    # Keep the reference line inside the plot with room on its weaker side
    dmin = min(dmin, ticks[ticks.index(1) - 1])
    dmax = max(dmax, ticks[ticks.index(1) + 1])
    right = max([width(label, TEXT) for ratio, label in bars if ratio >= 1] + [width('2.0x', TEXT)])
    plot_r = WIDTH - PAD - 5 - right
    while True:
        def x(v, lo_=dmin):
            v = min(max(v, lo_), dmax)
            return PLOT_L + (plot_r - PLOT_L) * math.log(v / lo_) / math.log(dmax / lo_)

        fits = all(x(ratio) - 5 - width(label, TEXT) >= NAMES_END + 2 for ratio, label in bars if ratio < 1)
        if fits or dmin == ticks[0]:
            return dmin, dmax, [t for t in ticks if dmin <= t <= dmax], plot_r
        dmin = ticks[ticks.index(dmin) - 1]


def bar_path(x0, x1, y, h, r=3):
    """A bar from the reference line x0 to its data end x1, rounded at the data end only."""
    length = abs(x1 - x0)
    if length < 2 * r + 1:
        x = min(x0, x1)
        return f'M{x:.1f},{y:.1f}h{max(length, 2):.1f}v{h}h{-max(length, 2):.1f}z'
    s = 1 if x1 > x0 else -1
    sweep = 1 if s > 0 else 0
    return (f'M{x0:.1f},{y:.1f}H{x1 - s * r:.1f}a{r},{r} 0 0 {sweep} {s * r},{r}v{h - 2 * r}'
            f'a{r},{r} 0 0 {sweep} {-s * r},{r}H{x0:.1f}z')


def tick_label(t):
    if t == 1:
        return '1x'
    value = t if t > 1 else 1 / t
    return f'{value:,.0f}x' if value >= 1 else f'{value:g}x'


def source_parts(system, groups, runs):
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
    return parts


def wrap(parts, first, limit):
    """Lines of at most limit px at SMALL size: the first starts after first, and each part goes whole on a line."""
    lines, line = [], ''
    for part in parts:
        offset = width(first, TEXT, True) if not lines else 0
        candidate = line + ('; ' if line else '') + part
        if line and offset + width(candidate, SMALL) > limit:
            lines.append(line + ';')
            line = part
        else:
            line = candidate
    lines.append(line)
    return lines


def draw(chart, system, entries, parts, theme):
    """entries: [(row, reference result, {engine: (result, ratio or None)})] in the order of charts.tsv, and
    [(row, None, None)] for a row whose metric is excluded on this OS, which shows the exclusion's reason instead."""
    c = THEMES[theme]
    rows = [entry for entry in entries if entry[1] is not None]
    engines = engine_order({engine for _, _, others in rows for engine in others})
    bars = []
    for row, reference, others in rows:
        for result, ratio in others.values():
            if ratio:
                pale = unsettled(result, reference, row['metric'], ratio)
                bars.append((ratio, ('~' if pale else '') + short(ratio)))
    dmin, dmax, ticks, plot_r = domain(bars)
    span = math.log(dmax / dmin)

    def x(v):
        v = min(max(v, dmin), dmax)
        return PLOT_L + (plot_r - PLOT_L) * math.log(v / dmin) / span

    # Legend items, flowed into lines of the chart's width
    sealed = all('sealed=true' in row['params'] for row, _, _ in rows)
    items = [('reference', 'Ledger (1x)')] + [(engine, legend_name(engine, sealed)) for engine in engines]
    pales = {('memory' if memory(row['metric']) else 'time')
             for row, reference, others in rows for result, ratio in others.values()
             if ratio and unsettled(result, reference, row['metric'], ratio)}
    if 'time' in pales:
        items.append(('pale', '~ pale: within the 99.9% intervals'))
    if 'memory' in pales:
        items.append(('pale', '~ pale: within 10% (no interval)'))
    lines, line, lx = [], [], PAD
    for kind, text in items:
        item = 17 + width(text, TEXT) + 14
        if line and lx + item > WIDTH - PAD:
            lines.append(line)
            line, lx = [], PAD
        line.append((kind, text, lx))
        lx += item
    lines.append(line)

    name = SYSTEMS.get(system, system)
    header = wrap(parts, f'{name} ({system}): ', WIDTH - 2 * PAD)

    def title_lines(row, reference):
        label = row['label']
        rest = row['excluded'] if reference is None else f'Ledger {reference_text(reference, row["metric"])}'
        one = width(label, TITLE, True) + width('  ' + rest, TEXT) <= WIDTH - 2 * PAD
        return label, rest, one

    header_y = 17
    legend_y = header_y + 18 * (len(header) - 1) + 14
    top = legend_y + 20 * len(lines) + 40
    height = top + 4
    for row, reference, _ in entries:
        _, _, one = title_lines(row, reference)
        height += TITLE_H + (0 if one else TITLE_LINE)
        height += BAR_PITCH * len(engines) + ROW_GAP if reference is not None else 6
    heading = f'{chart["heading"]} on {name}'
    o = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {WIDTH} {height}" width="{WIDTH}" height="{height}" '
         f'font-family="{FONT}" role="img" aria-label="{escape(heading)}: each engine\'s cost relative to Ledger">',
         f'<title>{escape(heading)}: each engine\'s cost relative to Ledger (a bar to the right is slower or larger '
         f'than Ledger, to the left faster or smaller)</title>',
         '<defs>',
         f'<pattern id="hatch" width="5" height="5" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">'
         f'<rect width="5" height="5" fill="{ENGINES[FLOOR]}" fill-opacity="0.25"/>'
         f'<line x1="0" y1="0" x2="0" y2="5" stroke="{ENGINES[FLOOR]}" stroke-width="2.5"/></pattern>',
         '</defs>']
    for i, text in enumerate(header):
        y = header_y + 18 * i
        lead = (f'<tspan font-size="{TEXT}" font-weight="600" fill="{c["ink"]}">{escape(name)}</tspan>'
                f'<tspan> ({escape(system)}): </tspan>') if i == 0 else ''
        o.append(f'<text x="{PAD}" y="{y}" font-size="{SMALL}" fill="{c["muted"]}">{lead}{escape(text)}</text>')

    def pale_paint(engine):
        paint = f' fill-opacity="{c["pale"]}"'
        if c['outline']:
            paint += f' stroke="{engine_color(engine, theme)}" stroke-width="1"'
        return paint

    for i, line in enumerate(lines):
        ly = legend_y + 20 * i
        for kind, text, lx in line:
            if kind == 'reference':
                o.append(f'<line x1="{lx + 6}" y1="{ly}" x2="{lx + 6}" y2="{ly + 12}" stroke="{c["ink"]}" '
                         f'stroke-width="2"/>')
            elif kind == 'pale':
                stroke = f' stroke="{c["muted"]}" stroke-width="1"' if c['outline'] else ''
                o.append(f'<rect x="{lx}" y="{ly}" width="12" height="12" rx="2" fill="{c["muted"]}" '
                         f'fill-opacity="{c["pale"]}"{stroke}/>')
            else:
                fill = 'url(#hatch)' if kind == FLOOR else engine_color(kind, theme)
                stroke = f' stroke="{ENGINES[FLOOR]}" stroke-width="1"' if kind == FLOOR else ''
                o.append(f'<rect x="{lx}" y="{ly}" width="12" height="12" rx="2" fill="{fill}"{stroke}/>')
            o.append(f'<text x="{lx + 17}" y="{ly + 10.5}" font-size="{TEXT}" fill="{c["muted"]}">'
                     f'{escape(text)}</text>')

    # Axis: the halves, the ticks and the reference line
    axis_y = top - 12
    x1 = x(1)
    more = sorted({words(row['metric'])[0] for row, _, _ in rows}, key=['slower', 'larger', 'more'].index)
    less = sorted({words(row['metric'])[1] for row, _, _ in rows}, key=['faster', 'smaller', 'less'].index)
    o.append(f'<text x="{x1 - 8:.1f}" y="{axis_y - 17}" font-size="{SMALL}" text-anchor="end" fill="{c["muted"]}">'
             f'&lt; {" or ".join(less)}</text>')
    o.append(f'<text x="{x1 + 8:.1f}" y="{axis_y - 17}" font-size="{SMALL}" fill="{c["muted"]}">'
             f'{" or ".join(more)} &gt;</text>')

    def grid(y0, y1):
        for t in ticks:
            o.append(f'<line x1="{x(t):.1f}" y1="{y0:.1f}" x2="{x(t):.1f}" y2="{y1:.1f}" '
                     f'stroke="{c["ink"] if t == 1 else c["grid"]}" stroke-width="{2 if t == 1 else 1}"/>')

    grid(axis_y + 4, top)
    for t in ticks:
        weight = ' font-weight="600"' if t == 1 else ''
        o.append(f'<text x="{x(t):.1f}" y="{axis_y}" font-size="{SMALL}" text-anchor="middle" '
                 f'fill="{c["ink"] if t == 1 else c["muted"]}"{weight}>{tick_label(t)}</text>')

    y = top
    for index, (row, reference, others) in enumerate(entries):
        if index:
            o.append(f'<line x1="{PAD}" y1="{y - ROW_GAP / 2:.1f}" x2="{WIDTH - PAD}" y2="{y - ROW_GAP / 2:.1f}" '
                     f'stroke="{c["grid"]}" stroke-width="1" stroke-dasharray="1 4"/>')
        label, rest, one = title_lines(row, reference)
        style = ' font-style="italic"' if reference is None else ''
        tip = '' if reference is None else f'<title>Ledger: {escape(metric_text(reference, row["metric"]))}</title>'
        if one:
            o.append(f'<text x="{PAD}" y="{y + 16}" font-size="{TITLE}" font-weight="600" fill="{c["ink"]}">'
                     f'{tip}{escape(label)}<tspan font-size="{TEXT}" font-weight="400"{style} fill="{c["muted"]}">'
                     f'  {escape(rest)}</tspan></text>')
            y += TITLE_H
        else:
            o.append(f'<text x="{PAD}" y="{y + 16}" font-size="{TITLE}" font-weight="600" fill="{c["ink"]}">'
                     f'{tip}{escape(label)}</text>')
            o.append(f'<text x="{PAD}" y="{y + 16 + TITLE_LINE}" font-size="{TEXT}"{style} fill="{c["muted"]}">'
                     f'{escape(rest)}</text>')
            y += TITLE_H + TITLE_LINE
        if reference is None:
            y += 6
            continue
        metric = row['metric']
        grid(y - 2, y + BAR_PITCH * len(engines) + 2)
        for engine in engines:
            mid = y + BAR_PITCH / 2
            o.append(f'<text x="{PAD}" y="{mid + 4.5:.1f}" font-size="{TEXT}" fill="{c["muted"]}">'
                     f'{escape(engine)}</text>')
            entry = others.get(engine)
            if entry is None or entry[1] is None:
                text = 'not run' if entry is None else 'no value'
                o.append(f'<text x="{x1 + 8:.1f}" y="{mid + 4.5:.1f}" font-size="{SMALL}" font-style="italic" '
                         f'fill="{c["muted"]}">{text}</text>')
                y += BAR_PITCH
                continue
            result, ratio = entry
            pale = unsettled(result, reference, metric, ratio)
            xe = x(ratio)
            text = ('~' if pale else '') + short(ratio)
            tip = (f'{engine}: {metric_text(result, metric)}, {"~" if pale else ""}{comparison(ratio, metric)} '
                   f'Ledger ({metric_text(reference, metric)})')
            if engine == FLOOR:
                paint = f'fill="url(#hatch)" stroke="{ENGINES[FLOOR]}" stroke-width="1"'
                if pale:
                    paint += f' fill-opacity="{c["pale"]}"'
            else:
                paint = f'fill="{engine_color(engine, theme)}"' + (pale_paint(engine) if pale else '')
            if abs(xe - x1) < 1.5:
                o.append(f'<circle cx="{x1:.1f}" cy="{mid:.1f}" r="3.5" {paint}><title>{escape(tip)}</title></circle>')
            else:
                o.append(f'<path d="{bar_path(x1, xe, mid - BAR / 2, BAR)}" {paint}><title>{escape(tip)}</title></path>')
            # A label goes at the end of its bar, on the bar's own side of the 1x line: domain() makes room for it
            if ratio >= 1:
                o.append(f'<text x="{max(xe, x1) + 5:.1f}" y="{mid + 4.5:.1f}" font-size="{TEXT}" '
                         f'fill="{c["ink"]}">{escape(text)}</text>')
            else:
                o.append(f'<text x="{min(xe, x1) - 5:.1f}" y="{mid + 4.5:.1f}" font-size="{TEXT}" text-anchor="end" '
                         f'fill="{c["ink"]}">{escape(text)}</text>')
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


def picture(base, alt):
    return (f'<picture><source media="(prefers-color-scheme: dark)" srcset="{base}-dark.svg">'
            f'<img src="{base}-light.svg" width="{WIDTH}" alt="{escape(alt)}"></picture>')


def readme_block(charts, config_rows, index, runs, excluded):
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
                parts = source_parts(system, groups, runs)
                for theme in THEMES:
                    files[os.path.join(ROOT, f'{base}-{theme}.svg')] = draw(chart, system, rows, parts, theme)
        if not drawn:
            continue
        o += [f'### {chart["heading"]}', '', chart['caption'], '']
        first = MAIN_SYSTEM if MAIN_SYSTEM in drawn else drawn[0]
        rest = [system for system in drawn if system != first]

        def chart_picture(system):
            return picture(f'.github/assets/bench-{chart["id"]}-{system.split("-")[0]}',
                           f'{chart["heading"]} on {SYSTEMS[system]}: each engine\'s cost relative to Ledger\'s')

        # One HTML block, which a blank line would end: the Linux chart, then the others in one <details>
        o += [chart_picture(first)]
        if rest:
            o += [f'<details><summary>{" and ".join(SYSTEMS[s] for s in rest)}</summary>']
            o += [chart_picture(system) for system in rest]
            o += ['</details>']
        o += ['']
        missing = [SYSTEMS[s] for s in SYSTEMS if s not in drawn]
        if missing:
            o += [f'No {" or ".join(missing)} results for this chart yet.', '']
    o.append(END)
    return '\n'.join(o), files


def main():
    check = '--check' in sys.argv
    charts, config_rows, excluded = read_config()
    results, runs = read_data()
    index = {}
    for result in results:
        key = (result['os'], result['group'], result['benchmark'], result['params'])
        index.setdefault(key, {})[result['engine']] = result
    block, files = readme_block(charts, config_rows, index, runs, excluded)
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
