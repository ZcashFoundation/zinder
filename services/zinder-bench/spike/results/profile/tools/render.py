#!/usr/bin/env python3
"""Render REPORT.md from a template, filling tables from analysis outputs.

Usage: render.py TEMPLATE OUT RUNS_DIR

Placeholders (RUN/SCENARIO names an analysis output under RUNS_DIR):
  {{incl:RUN/SCENARIO}}       top 25 functions by inclusive samples
  {{self:RUN/SCENARIO}}       top 25 functions by self samples
  {{groups:RUN/SCENARIO}}     inclusive group shares
  {{partition:RUN/SCENARIO}}  exclusive partition
  {{getpath:RUN/SCENARIO}}    RocksDB get-path breakdown
  {{hash:RUN/SCENARIO}}       SHA-256 and BLAKE2 call sites
  {{meta:RUN/SCENARIO}}       blocks, samples, on-CPU and timer seconds
  {{offcpu:RUN/FILE}}         wall-time attribution table from analyze_offcpu.py
Thread scaffolding frames present in at least 99.9% of a scenario's samples
(thread start, tokio blocking pool, the replay commit loop) are left out of
the inclusive table.
"""
import collections
import json
import re
import sys

SCAFFOLD = re.compile(
    r"^\[unknown:libc|^\[libc\]|thread_start|^tokio::|^<tokio::|std::sys::backtrace|FnOnce::call_once|"
    r"core::ops::function|replay::commit_loop|Committer::commit_append|BlockingTask|Harness<|Core<T,S>::poll|"
    r"blocking::pool|pipeline::spawn_pipeline|derive_block|run_replay")


def code(name: str, limit: int = 120) -> str:
    if len(name) > limit:
        name = name[: limit - 1] + "…"
    return "`" + name.replace("|", "\\|").replace("`", "'") + "`"


def load_folded(path: str):
    rows = []
    for line in open(path):
        stack, count = line.rstrip("\n").rsplit(" ", 1)
        rows.append((stack.split(";"), int(count)))
    return rows


def top_tables(path: str, kind: str) -> str:
    rows = load_folded(path)
    total = sum(count for _, count in rows)
    self_counts = collections.Counter()
    incl = collections.Counter()
    for frames, count in rows:
        self_counts[frames[-1]] += count
        for frame in set(frames):
            incl[frame] += count
    if kind == "incl":
        counter = collections.Counter({f: c for f, c in incl.items()
                                       if not (c >= 0.999 * total and SCAFFOLD.search(f))})
        header = "Inclusive"
    else:
        counter = self_counts
        header = "Self"
    lines = [f"| # | {header} % | Samples | Function |", "|---:|---:|---:|---|"]
    for rank, (frame, count) in enumerate(counter.most_common(25), 1):
        lines.append(f"| {rank} | {100 * count / total:.2f} | {count:,} | {code(frame)} |")
    return "\n".join(lines)


def kv_table(mapping: dict, left: str, right: str = "% of scenario samples", skip_zero: bool = True) -> str:
    lines = [f"| {left} | {right} |", "|---|---:|"]
    for key, value in mapping.items():
        if skip_zero and not value:
            continue
        lines.append(f"| {key} | {value:.2f} |")
    return "\n".join(lines)


def render_placeholder(kind: str, target: str, runs: str) -> str:
    run, name = target.split("/", 1)
    base = f"{runs}/{run}/analysis/{name}"
    if kind in ("incl", "self"):
        return top_tables(base + ".folded", kind)
    if kind == "offcpu":
        data = json.load(open(f"{runs}/{run}/{name}"))
        out = []
        for scenario in data["scenarios"]:
            out.append(f"**{scenario['name']}** ({scenario['blocks']} blocks): planner timer "
                       f"{scenario['planner_wall_seconds_bench_timer']:.2f} s; on-CPU "
                       f"{scenario['planner_on_cpu_seconds']:.2f} s; off-CPU "
                       f"{scenario['planner_off_cpu_seconds']:.2f} s "
                       f"({scenario['off_cpu_switches']:,} switches); accounted "
                       f"{scenario['accounted_pct_of_bench_timer']}% of the timer. "
                       f"RocksDB reads {scenario['rocksdb_reads_wall_pct']}% of wall, hashing "
                       f"{scenario['hashing_wall_pct']}%.")
            out.append("")
            out.append(kv_table(scenario["wall_share_pct_of_accounted"], "Where planner wall time goes",
                                "% of accounted wall time"))
            out.append("")
        return "\n".join(out)
    data = json.load(open(base + ".json"))
    if kind == "groups":
        return kv_table(data["groups_inclusive_pct"], "Group (inclusive; groups overlap)")
    if kind == "partition":
        return kv_table(data["partition_pct"], "Category (exclusive; first match wins)")
    if kind == "getpath":
        mapping = data["rocksdb_get_breakdown_pct_of_base"]
        return kv_table(mapping, "RocksDB get path, by leaf-most frame")
    if kind == "hash":
        lines = ["| Hash | Nearest zinder/zcash caller | % of scenario samples |", "|---|---|---:|"]
        for algo, sites in data["hash_call_sites_pct_of_base"].items():
            for site, value in sites.items():
                if value >= 0.01:
                    lines.append(f"| {algo} | {code(site, 100)} | {value:.2f} |")
        return "\n".join(lines)
    if kind == "meta":
        wall = data["base_wall_seconds_from_block_samples"]
        return (f"{data['blocks']:,} blocks (heights {data['height_min']:,} to {data['height_max']:,}); "
                f"{data['base_samples']:,} samples = {data['base_on_cpu_seconds_at_999hz']:.2f} s on-CPU at 999 Hz; "
                f"the bench's own timer for this phase sums to {wall:.2f} s.")
    raise ValueError(kind)


def main() -> None:
    template, out, runs = sys.argv[1:4]
    text = open(template).read()
    text = re.sub(r"\{\{(\w+):([^}]+)\}\}", lambda m: render_placeholder(m.group(1), m.group(2), runs), text)
    open(out, "w").write(text)


if __name__ == "__main__":
    main()
