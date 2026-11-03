#!/usr/bin/env python3
"""Renders JMH JSON results as Markdown tables, one per benchmark method.

usage: summarize.py [--by-os] FILE...

By default each table lists every parameter combination with its score, the median and 99th
percentile of sampled latencies, and the workload's secondary counters. JMH adds the counters of a
result up over its measurement iterations, so they are divided back to their value per iteration. With --by-os the files
are named after the runner that produced them (ubuntu-latest.json, ...) and each runner gets a
score column, so one table compares the operating systems.
"""

import json
import math
import sys
from collections import defaultdict
from pathlib import Path

BYTE_COUNTERS = {"bytesWritten", "payloadBytes", "sizeOnDisk", "heapBytes", "residentBytes"}


def number(value):
    if value is None or isinstance(value, str) or math.isnan(value):
        return "-"
    magnitude = abs(value)
    if magnitude >= 100:
        return f"{value:,.0f}"
    if magnitude >= 10:
        return f"{value:.1f}"
    return f"{value:.3g}"


def mebibytes(value):
    if value is None or isinstance(value, str) or math.isnan(value) or value < 0:
        return "-"
    return f"{value / (1 << 20):,.1f} MiB"


def score(metric):
    error = metric.get("scoreError")
    if isinstance(error, (int, float)) and not math.isnan(error) and error > 0:
        return f"{number(metric['score'])} ± {number(error)}"
    return number(metric["score"])


def method_name(benchmark):
    return ".".join(benchmark.split(".")[-2:])


def load(paths):
    results = []
    for path in paths:
        with open(path, encoding="utf-8") as file:
            for result in json.load(file):
                result["_source"] = Path(path).stem
                results.append(result)
    return results


def parameter_names(results):
    names = []
    for result in results:
        for name in result.get("params", {}):
            if name not in names:
                names.append(name)
    # The engine is what a table compares, so it leads
    return sorted(names, key=lambda name: name != "engine")


def counters(results):
    names = []
    for result in results:
        for name in result.get("secondaryMetrics", {}):
            # Sampled modes also report their percentiles as secondary metrics
            if not name.startswith("p0.") and not name.startswith("p1.") and name not in names:
                names.append(name)
    return names


def table(rows):
    lines = ["| " + " | ".join(rows[0]) + " |", "|" + "---|" * len(rows[0])]
    lines += ["| " + " | ".join(row) + " |" for row in rows[1:]]
    return "\n".join(lines)


def by_result(results):
    output = []
    for benchmark, group in sorted(group_by_benchmark(results).items()):
        parameters = parameter_names(group)
        secondary = counters(group)
        sampled = any(result["mode"] == "sample" for result in group)
        header = parameters + ["Score", "Unit"] + (["p50", "p99"] if sampled else []) + secondary
        rows = [header]
        for result in group:
            primary = result["primaryMetric"]
            row = [str(result.get("params", {}).get(name, "")) for name in parameters]
            row += [score(primary), primary["scoreUnit"]]
            if sampled:
                percentiles = primary.get("scorePercentiles", {})
                row += [number(percentiles.get("50.0")), number(percentiles.get("99.0"))]
            iterations = result.get("measurementIterations", 1) * result.get("forks", 1)
            for name in secondary:
                value = result.get("secondaryMetrics", {}).get(name, {}).get("score")
                if isinstance(value, (int, float)):
                    value /= max(1, iterations)
                row.append(mebibytes(value) if name in BYTE_COUNTERS else number(value))
            rows.append(row)
        output.append(f"### {method_name(benchmark)}\n\n{table(rows)}\n")
    return "\n".join(output)


def by_os(results):
    output = []
    systems = sorted({result["_source"] for result in results})
    for benchmark, group in sorted(group_by_benchmark(results).items()):
        parameters = parameter_names(group)
        scores = defaultdict(dict)
        units = {}
        for result in group:
            key = tuple(str(result.get("params", {}).get(name, "")) for name in parameters)
            scores[key][result["_source"]] = score(result["primaryMetric"])
            units[key] = result["primaryMetric"]["scoreUnit"]
        rows = [parameters + systems + ["Unit"]]
        for key, values in scores.items():
            rows.append(list(key) + [values.get(system, "-") for system in systems] + [units[key]])
        output.append(f"### {method_name(benchmark)}\n\n{table(rows)}\n")
    return "\n".join(output)


def group_by_benchmark(results):
    groups = defaultdict(list)
    for result in results:
        groups[result["benchmark"]].append(result)
    return groups


def main(arguments):
    # The step summary is UTF-8 whatever the runner's console encoding is
    sys.stdout.reconfigure(encoding="utf-8")
    compare = "--by-os" in arguments
    paths = [argument for argument in arguments if argument != "--by-os"]
    results = load(paths)
    if not results:
        print("No results.")
        return
    print(by_os(results) if compare else by_result(results))


if __name__ == "__main__":
    main(sys.argv[1:])
