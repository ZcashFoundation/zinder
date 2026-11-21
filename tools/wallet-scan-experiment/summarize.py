#!/usr/bin/env python3
"""Summarize complete counterbalanced scanner runs. Positive reductions mean faster scans."""

import argparse
import hashlib
import itertools
import json
import math
import re
from collections import defaultdict
from pathlib import Path
from statistics import median

ARMS = ("stock", "ordinary-prefilter", "hinted")
ORDERS = tuple(itertools.permutations(ARMS))


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(value):
    require(isinstance(value, str) and re.fullmatch("[0-9a-f]{64}", value), "invalid digest")
    return value


def read_run(report):
    diagnostics = report["diagnostics"]
    digest(diagnostics["receipt_digest_blake2b256"])
    require(diagnostics["archived_actions"] >= 0, "invalid archive action count")
    require(diagnostics["actions_with_fixture"] ==
            diagnostics["archived_actions"] + diagnostics["fixture"]["fixture_actions"],
            "incorrect count after adding fixture")
    if "environment" in report:
        runtime, env, inputs = "chromium", report["environment"], report["inputs"]
        corpus = digest(inputs["corpusSha256"])
        build = (digest(inputs["wasmSha256"]), digest(inputs["bindingsSha256"]),
                 *(env[key] for key in ("os", "arch", "chromium", "kernel", "cpu", "availableCpus", "node")))
        batch, cycles = inputs["batchSize"], inputs["cycles"]
        require(report["validatedReceiptCount"] == diagnostics["total_receipts"], "receipt count differs")
        wall_key, cpu_key = "elapsedMs", "threadCpuMs"
    else:
        runtime, corpus = "native", digest(report["corpus_sha256"])
        build = (digest(report["executable_sha256"]), report["os"], report["arch"])
        batch, cycles = report["batch_size"], report["cycles"]
        wall_key, cpu_key = "elapsed_ms", "process_cpu_ms"
    require(type(batch) is int and batch > 0 and type(cycles) is int and cycles > 0,
            "invalid batch size or cycle count")

    rows = report["samples"]
    browser = runtime == "chromium"
    require(len(rows) == cycles * len(ORDERS) * (1 if browser else len(ARMS)),
            "sample count differs from declared cycles")
    pairs = []
    for index in range(cycles * len(ORDERS)):
        cycle, order = divmod(index, len(ORDERS))
        if browser:
            require(rows[index]["cycle"] == cycle, "unexpected Chromium cycle")
            observations = rows[index]["observations"]
        else:
            observations = rows[index * len(ARMS):(index + 1) * len(ARMS)]
            require(all((row["cycle"], row["order"]) == (cycle, order) for row in observations),
                    "unexpected native cycle or order")
        require(tuple(row["arm"] for row in observations) == ORDERS[order], "wrong arm ordering")
        pair = {}
        for row in observations:
            wall, cpu = row[wall_key], row[cpu_key]
            times = (wall,) if cpu is None and not browser else (wall, cpu)
            require(all(type(t) in (int, float) and math.isfinite(t) and t > 0 for t in times),
                    "timings must be positive and finite")
            pair[row["arm"]] = (wall, cpu)
        pairs.append(pair)
    return runtime, corpus, build, batch, cycles, diagnostics, pairs


def summarize(runtime, corpus, diagnostics, batch, runs):
    pairs = [pair for run in runs for pair in run]
    cpu_presence = {times[1] is not None for pair in pairs for times in pair.values()}
    require(len(cpu_presence) == 1, "inconsistent CPU clock availability")
    summary = {
        "runtime": runtime, "corpus_sha256": corpus, "archived_actions": diagnostics["archived_actions"],
        "receipts": diagnostics["total_receipts"], "receipt_digest_blake2b256": diagnostics["receipt_digest_blake2b256"],
        "batch_size": batch, "processes": len(runs), "samples_per_arm": len(pairs),
        "median_ms": {}, "paired_hinted_reduction_pct": {},
    }
    for metric, slot in (("wall", 0), ("cpu", 1)):
        if metric == "cpu" and cpu_presence == {False}:
            summary["median_ms"][metric] = summary["paired_hinted_reduction_pct"][metric] = None
            continue
        summary["median_ms"][metric] = {
            arm: round(median(pair[arm][slot] for pair in pairs), 3) for arm in ARMS
        }
        differences = {}
        for control in ARMS[:-1]:
            by_run = [[100 * (1 - pair["hinted"][slot] / pair[control][slot]) for pair in run]
                      for run in runs]
            run_medians = [median(run) for run in by_run]
            differences[control] = {
                "median": round(median(itertools.chain.from_iterable(by_run)), 3),
                "process_median_range": [round(min(run_medians), 3), round(max(run_medians), 3)],
            }
        summary["paired_hinted_reduction_pct"][metric] = differences
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", type=Path, nargs="+", help="one raw JSON report per fresh runner process")
    args = parser.parse_args()
    groups = defaultdict(list)
    outcomes, profiles, seen = {}, {}, set()
    try:
        for path in args.reports:
            report = json.loads(path.read_text())
            fingerprint = hashlib.sha256(json.dumps(report, sort_keys=True).encode()).digest()
            require(fingerprint not in seen, "duplicate report")
            seen.add(fingerprint)
            runtime, corpus, build, batch, cycles, diagnostics, pairs = read_run(report)
            require(outcomes.setdefault(corpus, (diagnostics, batch)) == (diagnostics, batch),
                    "receipt digest, fixture, action counts, or batch size differs for the same corpus")
            require(profiles.setdefault((runtime, corpus), (build, cycles)) == (build, cycles),
                    "runtime, executable, or cycle count differs within a comparison")
            groups[runtime, corpus].append(pairs)
        result = [summarize(runtime, corpus, *outcomes[corpus], runs)
                  for (runtime, corpus), runs in sorted(groups.items())]
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.exit(2, f"{path}: {error}\n")
    print(json.dumps(result, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
