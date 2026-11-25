#!/usr/bin/env python3
"""Draws the benchmark charts of the root README from benchmarks.tsv, as charts.tsv selects them.

    python tools/readme/charts.py          # rewrite the charts and the README block
    python tools/readme/charts.py --check  # exit 1 when a committed file is stale

For every chart of charts.tsv and every OS with data for it, it writes .github/assets/bench-<chart>-<os>-light.svg and
-dark.svg, and it rewrites the block between the "bench:begin" and "bench:end" markers of README.md: the <picture>
elements that pick a variant by color scheme, the numbers of each chart as a text list, and every result no chart
shows. Standard library only.

Every row compares each engine's cost with Ledger's (encrypted, the reference at 1.0x): its time per operation, the
inverse of its throughput, or the bytes a counter measured. On the logarithmic axis a bar runs right when the engine
costs more than Ledger, so it is slower or takes more, and left when it costs less, whatever the unit.
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
UNKNOWN_ENGINE = '#8b949e'
SYSTEMS = {'ubuntu-latest': 'Linux', 'windows-latest': 'Windows', 'macos-latest': 'macOS'}

THEMES = {
    'light': dict(ink='#1f2328', muted='#59636e', grid='#d8dee4', axis='#59636e'),
    'dark': dict(ink='#f0f6fc', muted='#9198a1', grid='#3d444d', axis='#9198a1'),
}

WIDTH = 960
LABEL_X = 8
NAME_X = 20           # engine names
PLOT_L, PLOT_R = 236, 872
TITLE_H = 26          # a row's title line
BAR_PITCH = 13
BAR = 9
ROW_GAP = 12
FONT = "-apple-system, BlinkMacSystemFont, 'Segoe UI', 'Noto Sans', Helvetica, Arial, sans-serif"
POWERS_OF_TWO = [2 ** n for n in range(-6, 7)]
DECADES = [10.0 ** n for n in range(-4, 5)]



def read_config():
    charts, rows = {}, []
    with open(CONFIG, encoding='utf-8') as f:
        for n, line in enumerate(f, 1):
            line = line.rstrip('\n')
            if not line.strip() or line.startswith('#'):
                continue
            p = line.split('\t')
            if p[0] == 'chart' and len(p) == 4:
                charts[p[1]] = dict(id=p[1], heading=p[2], caption=p[3])
            elif p[0] == 'row' and len(p) == 7:
                if p[1] not in charts:
                    raise SystemExit(f'{CONFIG}:{n}: row of an unknown chart "{p[1]}"')
                rows.append(dict(chart=p[1], group=p[2], benchmark=p[3], params='' if p[4] == '(none)' else p[4],
                                 metric=p[5], label=p[6]))
            else:
                raise SystemExit(f'{CONFIG}:{n}: expected "chart" with 3 fields or "row" with 6')
    return charts, rows


def read_data():
    results, runs, notes = [], {}, []
    with open(DATA, encoding='utf-8') as f:
        columns = None
        for line in f:
            line = line.rstrip('\n')
            if line.startswith('#'):
                m = re.match(r'# run (\d+): (\S+) on (\S+), (\d+) rounds, commit (\w+)', line)
                if m:
                    runs[(m[3], m[2])] = dict(id=m[1], rounds=m[4], commit=m[5])
                elif line.startswith('# No data:'):
                    notes.append(line[2:])
                continue
            if not line.strip():
                continue
            fields = line.split('\t')
            if columns is None:
                columns = fields
                continue
            results.append(dict(zip(columns, fields)))
    return results, runs, notes


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


def shown_ratio(ratio):
    """The ratio as a bar's label: how many times more or less the engine costs than Ledger."""
    return factor(ratio if ratio >= 1 else 1 / ratio)


def verdict(ratio, metric):
    if metric != 'score':
        return 'the same as Ledger' if 0.995 <= ratio <= 1.005 else \
            f'{factor(ratio)} Ledger\'s' if ratio >= 1 else f'{factor(1 / ratio)} less than Ledger'
    if 0.995 <= ratio <= 1.005:
        return 'the same as Ledger'
    return f'{factor(ratio)} slower than Ledger' if ratio > 1 else f'{factor(1 / ratio)} faster than Ledger'


def score_text(result):
    error = num(result['error'])
    text = number(float(result['score']))
    if error and error > 0:
        text += f' +- {number(error)}'
    return f'{text} {result["unit"]}'


def metric_text(result, metric):
    if metric == 'score':
        return score_text(result)
    value = metric_value(result, metric)
    return size(value) if value is not None else '-'


def reference_text(reference, metric):
    """Ledger's own figure for a row's title, with the payload when the row counts the bytes written."""
    text = metric_text(reference, metric)
    payload = num(reference.get('payloadBytes'))
    if metric == 'bytesWritten' and payload:
        text += f' for {size(payload)} of payload'
    return text


def engine_order(engines):
    known = [engine for engine in ENGINES if engine in engines]
    return known + sorted(engine for engine in engines if engine not in ENGINES and engine != REFERENCE)



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


def draw(chart, system, rows, theme):
    """rows: [(row, reference result, {engine: (result, ratio or None)})] with a reference for every row."""
    c = THEMES[theme]
    engines = engine_order({engine for _, _, others in rows for engine in others})
    ratios = [ratio for _, _, others in rows for _, ratio in others.values() if ratio]
    dmin, dmax, ticks = domain(ratios)
    span = math.log(dmax / dmin)

    def x(v):
        v = min(max(v, dmin), dmax)
        return PLOT_L + (PLOT_R - PLOT_L) * math.log(v / dmin) / span

    legend_rows = 2 if len(engines) > 5 else 1
    top = 26 + 20 * legend_rows + 40
    height = top + sum(TITLE_H + BAR_PITCH * len(engines) + ROW_GAP for _ in rows) + 4
    heading = f'{chart["heading"]} on {SYSTEMS.get(system, system)}'
    o = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {WIDTH} {height}" width="{WIDTH}" height="{height}" '
         f'font-family="{FONT}" role="img" aria-label="{escape(heading)}: each engine\'s cost relative to Ledger">',
         f'<title>{escape(heading)}: each engine\'s cost relative to Ledger (a bar to the right is slower or larger '
         f'than Ledger, to the left faster or smaller)</title>',
         '<defs>',
         f'<pattern id="hatch" width="5" height="5" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">'
         f'<rect width="5" height="5" fill="{ENGINES[FLOOR]}" fill-opacity="0.25"/>'
         f'<line x1="0" y1="0" x2="0" y2="5" stroke="{ENGINES[FLOOR]}" stroke-width="2.5"/></pattern>',
         '</defs>']

    # Legend: the reference line, then one swatch per engine
    ly = 12
    o.append(f'<line x1="{LABEL_X + 6}" y1="{ly}" x2="{LABEL_X + 6}" y2="{ly + 12}" stroke="{c["ink"]}" stroke-width="2"/>')
    o.append(f'<text x="{LABEL_X + 16}" y="{ly + 10.5}" font-size="12" fill="{c["muted"]}">Ledger, the reference (1x)</text>')
    lx, row_y = LABEL_X + 200, ly
    for i, engine in enumerate(engines):
        if legend_rows == 2 and i == math.ceil(len(engines) / 2):
            lx, row_y = LABEL_X + 200, ly + 20
        fill = 'url(#hatch)' if engine == FLOOR else ENGINES.get(engine, UNKNOWN_ENGINE)
        stroke = f' stroke="{ENGINES[FLOOR]}" stroke-width="1"' if engine == FLOOR else ''
        o.append(f'<rect x="{lx}" y="{row_y}" width="12" height="12" rx="2" fill="{fill}"{stroke}/>')
        name = 'floor (device bound)' if engine == FLOOR else engine
        o.append(f'<text x="{lx + 17}" y="{row_y + 10.5}" font-size="12" fill="{c["muted"]}">{escape(name)}</text>')
        lx += 17 + 7.2 * len(name) + 22

    # Axis: the halves, the ticks and the reference line
    axis_y = top - 12
    x1 = x(1)
    o.append(f'<text x="{x1 - 8:.1f}" y="{axis_y - 16}" font-size="11" text-anchor="end" fill="{c["muted"]}">'
             f'faster or smaller than Ledger</text>')
    o.append(f'<text x="{x1 + 8:.1f}" y="{axis_y - 16}" font-size="11" fill="{c["muted"]}">slower or larger than Ledger</text>')
    def grid(y0, y1):
        for t in ticks:
            o.append(f'<line x1="{x(t):.1f}" y1="{y0:.1f}" x2="{x(t):.1f}" y2="{y1:.1f}" '
                     f'stroke="{c["ink"] if t == 1 else c["grid"]}" stroke-width="{2 if t == 1 else 1}"/>')

    grid(axis_y + 4, top)
    for t in ticks:
        tx = x(t)
        reference = t == 1
        o.append(f'<text x="{tx:.1f}" y="{axis_y}" font-size="11" text-anchor="middle" '
                 f'fill="{c["ink"] if reference else c["muted"]}"'
                 f'{" font-weight=\"600\"" if reference else ""}>{tick_label(t)}</text>')

    y = top
    for index, (row, reference, others) in enumerate(rows):
        if index:
            o.append(f'<line x1="{LABEL_X}" y1="{y - ROW_GAP / 2:.1f}" x2="{WIDTH - 8}" y2="{y - ROW_GAP / 2:.1f}" '
                     f'stroke="{c["grid"]}" stroke-width="1" stroke-dasharray="1 4"/>')
        title = row['label']
        o.append(f'<text x="{LABEL_X}" y="{y + 15}" font-size="13" font-weight="600" fill="{c["ink"]}">{escape(title)}'
                 f'<tspan font-weight="400" fill="{c["muted"]}">  Ledger: {escape(reference_text(reference, row["metric"]))}'
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
            xe = x(ratio)
            by = mid - BAR / 2
            tip = f'{engine}: {metric_text(result, row["metric"])}, {verdict(ratio, row["metric"])}'
            if engine == FLOOR:
                paint = f'fill="url(#hatch)" stroke="{ENGINES[FLOOR]}" stroke-width="1"'
            else:
                paint = f'fill="{ENGINES.get(engine, UNKNOWN_ENGINE)}"'
            if abs(xe - x1) < 1.5:
                o.append(f'<circle cx="{x1:.1f}" cy="{mid:.1f}" r="3.5" {paint}><title>{escape(tip)}</title></circle>')
            else:
                o.append(f'<path d="{bar_path(x1, xe, by, BAR)}" {paint}><title>{escape(tip)}</title></path>')
            label = escape(shown_ratio(ratio))
            if ratio >= 1:
                o.append(f'<text x="{max(xe, x1) + 5:.1f}" y="{mid + 4:.1f}" font-size="11" font-weight="600" '
                         f'fill="{c["ink"]}">{label}</text>')
            else:
                o.append(f'<text x="{min(xe, x1) - 5:.1f}" y="{mid + 4:.1f}" font-size="11" font-weight="600" '
                         f'text-anchor="end" fill="{c["ink"]}">{label}</text>')
            y += BAR_PITCH
        y += ROW_GAP
    o.append('</svg>')
    return '\n'.join(o) + '\n'



def chart_rows(chart_id, system, config_rows, index):
    """The rows of a chart on one OS that have a Ledger result, with every engine's ratio."""
    out = []
    for row in config_rows:
        if row['chart'] != chart_id:
            continue
        results = index.get((system, row['group'], row['benchmark'], row['params']), {})
        reference = results.get(REFERENCE)
        if reference is None:
            continue
        base = metric_value(reference, row['metric'])
        others = {}
        for engine, result in results.items():
            if engine == REFERENCE:
                continue
            value = metric_value(result, row['metric'])
            ratio = value / base if value is not None and base and base > 0 and value > 0 else None
            others[engine] = (result, ratio)
        out.append((row, reference, others))
    return out


def text_list(rows):
    o = []
    for row, reference, others in rows:
        o.append(f'- **{row["label"]}.** Ledger: {reference_text(reference, row["metric"])}')
        for engine in engine_order(others):
            result, ratio = others[engine]
            name = 'floor (device bound)' if engine == FLOOR else engine
            tail = f', {verdict(ratio, row["metric"])}' if ratio else ''
            o.append(f'  - {name}: {metric_text(result, row["metric"])}{tail}')
    return o


def other_results(system, groups, charted, index):
    """Every result of `groups` on `system` that no chart row shows, as a text list."""
    o = []
    for (sys_, group, benchmark, params), results in index.items():
        if sys_ != system or group not in groups or (group, benchmark, params) in charted:
            continue
        reference = results.get(REFERENCE)
        title = benchmark + (f' ({params.replace(",", ", ")})' if params else '')
        if REFERENCE in results:
            o.append(f'- **{title}.** Ledger: {score_text(reference)}')
        else:
            o.append(f'- **{title}.**')
        for engine in ([e for e in results if e != REFERENCE] if reference is None else engine_order(results)):
            result = results[engine]
            tail = ''
            mine, theirs = metric_value(result, 'score'), metric_value(reference, 'score') if reference else None
            if mine and theirs:
                tail = f', {verdict(mine / theirs, "score")}'
            o.append(f'  - {engine or "(no engine parameter)"}: {score_text(result)}{tail}')
    return o


def losses(chart_rows_by_system):
    """The rows where an engine other than Ledger's own variant and the floor costs less than Ledger, as a text list."""
    o = []
    for system, rows in chart_rows_by_system:
        for row, _, others in rows:
            better = sorted((ratio, engine) for engine, (_, ratio) in others.items()
                            if ratio and ratio < 0.95 and engine not in (FLOOR, 'ledger-plain'))
            if better:
                engines = ', '.join(f'{engine} ({verdict(ratio, row["metric"]).replace(" than Ledger", "")})'
                                    for ratio, engine in better)
                o.append(f'- {SYSTEMS[system]}, {row["label"][0].lower() + row["label"][1:]}: {engines}')
    return o


def picture(base, alt):
    return ['<picture>',
            f'  <source media="(prefers-color-scheme: dark)" srcset="{base}-dark.svg">',
            f'  <img src="{base}-light.svg" width="100%" alt="{escape(alt)}">',
            '</picture>']


def readme_block(charts, config_rows, index, runs):
    o = [BEGIN, '<!-- Generated by tools/readme/charts.py from tools/readme/benchmarks.tsv and tools/readme/charts.tsv; '
                'do not edit by hand. -->', '']
    systems = [s for s in SYSTEMS if any(key[0] == s for key in index)]
    charted = {(row['group'], row['benchmark'], row['params']) for row in config_rows}
    files = {}
    for chart in charts.values():
        groups = sorted({row['group'] for row in config_rows if row['chart'] == chart['id']})
        drawn = [(system, chart_rows(chart['id'], system, config_rows, index)) for system in systems]
        drawn = [(system, rows) for system, rows in drawn if rows]
        if not drawn:
            continue
        o += [f'### {chart["heading"]}', '', chart['caption'], '']
        lost = losses(drawn)
        o += (['Where another engine costs less than Ledger in these runs:', ''] + lost + [''] if lost else
              ['No other engine costs less than Ledger in any row of this chart in these runs.', ''])
        for position, (system, rows) in enumerate(drawn):
            name = SYSTEMS[system]
            base = f'.github/assets/bench-{chart["id"]}-{system.split("-")[0]}'
            for theme in THEMES:
                files[os.path.join(ROOT, f'{base}-{theme}.svg')] = draw(chart, system, rows, theme)
            sources = sorted({(runs[(system, g)]['id'], runs[(system, g)]['commit'][:7], runs[(system, g)]['rounds'])
                              for g in groups if (system, g) in runs})
            source = '; '.join(f'run [{run}](https://github.com/Auties00/Ledger/actions/runs/{run}), commit {commit}, '
                               f'{rounds} rounds' for run, commit, rounds in sources)
            alt = (f'{chart["heading"]} on {name}: bar chart of each engine\'s cost relative to Ledger\'s; '
                   f'the numbers follow as text.')
            body = [f'**{name}** ({system}; {source})', ''] + picture(base, alt) + [
                '', '<details>', f'<summary>{chart["heading"]} on {name}: the numbers as text</summary>', ''] + \
                text_list(rows) + ['', '</details>', '']
            if position == 0:
                o += body
            else:
                o += ['<details>', f'<summary><b>{chart["heading"]} on {name}</b></summary>', ''] + body + ['</details>', '']
        for system in systems:
            rest = other_results(system, groups, charted, index)
            if rest:
                o += ['<details>', f'<summary>Every other result of the {", ".join(groups)} workloads on '
                                   f'{SYSTEMS[system]}</summary>', ''] + rest + ['', '</details>', '']
        missing = [SYSTEMS[s] for s in SYSTEMS if s not in {system for system, _ in drawn}]
        if missing:
            o += [f'No {" or ".join(missing)} results for this chart yet.', '']
    o.append(END)
    return '\n'.join(o), files


def main():
    check = '--check' in sys.argv
    charts, config_rows = read_config()
    results, runs, _ = read_data()
    index = {}
    for result in results:
        key = (result['os'], result['group'], result['benchmark'], result['params'])
        index.setdefault(key, {})[result['engine']] = result
    block, files = readme_block(charts, config_rows, index, runs)
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
