"""Compares the JMH results of two refs measured on the same runner in alternating rounds.

Usage: compare.py <results directory>

The directory holds base-<round>.json and head-<round>.json. For every workload, parameter set and engine, prints the
median score of each ref over its rounds, the range of the rounds, and how the head compares with the base: above 1
is better, whether the score is a rate or a time.
"""
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path


def load(directory, side):
    scores = defaultdict(list)
    units = {}
    for path in sorted(Path(directory).glob(f"{side}-*.json")):
        try:
            results = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        for result in results:
            params = dict(result.get("params", {}))
            engine = params.pop("engine", "")
            name = ".".join(result["benchmark"].rsplit(".", 2)[-2:])
            key = (name, ", ".join(f"{k}={v}" for k, v in sorted(params.items())), engine)
            metric = result["primaryMetric"]
            scores[key].append(metric["score"])
            units[key] = (metric["scoreUnit"], result.get("mode", ""))
    return scores, units


def describe(values):
    if not values:
        return "-"
    median = statistics.median(values)
    if len(values) == 1:
        return f"{median:.4g}"
    return f"{median:.4g} ({min(values):.4g}-{max(values):.4g})"


def main():
    directory = sys.argv[1]
    base, base_units = load(directory, "base")
    head, head_units = load(directory, "head")
    units = {**base_units, **head_units}
    print("| Workload | Parameters | Engine | Unit | Base | Head | Head vs base |")
    print("|---|---|---|---|---|---|---|")
    for key in sorted(set(base) | set(head)):
        name, params, engine = key
        unit, mode = units[key]
        old, new = base.get(key, []), head.get(key, [])
        ratio = "-"
        if old and new:
            old_median, new_median = statistics.median(old), statistics.median(new)
            # Throughput is better higher; average and sample times are better lower
            value = new_median / old_median if mode == "thrpt" else old_median / new_median
            ratio = f"{value:.2f}x"
        print(f"| {name} | {params} | {engine} | {unit} | {describe(old)} | {describe(new)} | {ratio} |")


if __name__ == "__main__":
    main()
