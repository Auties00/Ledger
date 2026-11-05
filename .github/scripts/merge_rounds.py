#!/usr/bin/env python3
"""Merges the JMH JSON results of several rounds of the same workloads into one file.

usage: merge_rounds.py OUTPUT ROUND...

Each round runs every engine of a workload in turn, so the rounds interleave the engines: a runner that slows down
during a job slows every engine of a later round, not only the engines JMH happened to run last. For every workload,
mode and parameter set, the output keeps the result of the round with the median primary score (the worse of the two
middle rounds for an even number of them), so its percentiles and counters stay those of one real run, and records the
score of every round under primaryMetric.roundScores. A result missing from some rounds keeps the rounds that have it.
"""

import json
import sys
from pathlib import Path


def key(result):
    return result["benchmark"], result.get("mode"), tuple(sorted(result.get("params", {}).items()))


def main():
    output, rounds = Path(sys.argv[1]), [Path(path) for path in sys.argv[2:]]
    order = []
    grouped = {}
    for path in rounds:
        try:
            results = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        for result in results:
            if key(result) not in grouped:
                order.append(key(result))
                grouped[key(result)] = []
            grouped[key(result)].append(result)
    merged = []
    for entry in order:
        # Best first: a higher score is better only for throughput
        results = sorted(grouped[entry], key=lambda result: result["primaryMetric"]["score"], reverse=entry[1] == "thrpt")
        chosen = dict(results[len(results) // 2])
        chosen["primaryMetric"] = dict(chosen["primaryMetric"])
        chosen["primaryMetric"]["roundScores"] = [result["primaryMetric"]["score"] for result in grouped[entry]]
        merged.append(chosen)
    output.write_text(json.dumps(merged, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
