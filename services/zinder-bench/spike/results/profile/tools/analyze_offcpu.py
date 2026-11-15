#!/usr/bin/env python3
"""Wall-time attribution (on-CPU samples plus off-CPU intervals) per scenario.

Usage:
  analyze_offcpu.py RUN_PREFIX REPLAY_LOG SAMPLES_JSONL CLOCK_OFFSETS SCENARIO_SPEC OUT_JSON

Uses RUN_PREFIX.{stacks,samples,offcpu}.tsv from collapse_offcpu.py. Only the
commit thread is attributed. An off-CPU interval belongs to the block whose
commit window contains its switch-out time, and to the phase its switch-out
stack shows. On-CPU seconds are cycles samples / 999.
"""
import bisect
import collections
import json
import re
import sys

sys.path.insert(0, __file__.rsplit("/", 1)[0])
import analyze as A  # noqa: E402

HZ = 999.0
GET = A.compile_group(A.GROUPS["rocksdb get: all"])
OFF_CATEGORIES = [
    ("preempted while runnable (CPU contention)", None),
    ("RocksDB get: SST block read wait (direct-I/O pread)", [A.GROUPS["rocksdb get: all"][0], r"pread64_\[k\]|BlockFetcher|RandomAccessFileReader::Read"]),
    ("RocksDB get: other wait", [A.GROUPS["rocksdb get: all"][0]]),
    ("page fault wait", [r"asm_exc_page_fault_\[k\]"]),
    ("WAL/SST write and fsync wait", [r"WritableFileWriter|fsync|fdatasync|Sync_\[k\]|WriteToWAL|PosixWritableFile"]),
    ("lock wait (futex)", [r"futex_wait|futex_\[k\]|__x64_sys_futex"]),
    ("pipeline wait for the next derived block", [r"blocking_recv|tokio::runtime::park|CachedParkThread"]),
]
OFF_RE = [(name, A.compile_group(cond) if cond else None) for name, cond in OFF_CATEGORIES]
# Planner sub-functions, first match wins.
PLANNER_PARTS = [
    ("require_publishable_undo_suffix (undo-suffix walk)", re.compile(r"require_publishable_undo_suffix")),
    ("advance_settled_tip (prune settled undo row)", re.compile(r"WalletTransitionPlanner::advance_settled_tip")),
    ("apply_replay / apply_block_rows (this block's rows)", re.compile(r"WalletTransitionPlanner::(apply_replay|apply_block_rows|apply_replay_range)")),
    ("publication and control checks", re.compile(r"into_publication|prepare_publication|require_current_ready_control|decode_control_in_family|validate_ready_evidence")),
]


def planner_part(frames: list) -> str:
    for name, pattern in PLANNER_PARTS:
        if any(pattern.search(frame) for frame in frames):
            return name
    return "other planner"


def main() -> None:
    prefix, log_path, samples_path, offsets_path, spec_path, out_path = sys.argv[1:7]
    offsets = [float(x) for x in open(offsets_path).read().split()]
    ends = A.load_ends(log_path, sum(offsets) / len(offsets))
    heights = sorted(ends)
    end_times = [ends[h] for h in heights]
    blocks = {}
    for line in open(samples_path):
        row = json.loads(line)
        blocks[row["height"]] = row
    stacks = {}
    for line in open(prefix + ".stacks.tsv"):
        sid, stack = line.rstrip("\n").split("\t", 1)
        stacks[int(sid)] = stack.split(";")
    commit_like = {sid for sid, frames in stacks.items() if any(A.COMMIT_THREAD.search(f) for f in frames)}
    per_tid = collections.Counter()
    cycles = []
    for line in open(prefix + ".samples.tsv"):
        time_s, tid, comm, sid = line.rstrip("\n").split("\t")
        cycles.append((float(time_s), tid, int(sid)))
        if int(sid) in commit_like:
            per_tid[tid] += 1
    commit_tid = per_tid.most_common(1)[0][0]

    def height_of(time_s: float):
        index = bisect.bisect_left(end_times, time_s)
        if index == 0 or index >= len(heights):
            return None
        return heights[index]

    phase_cache = {}

    def phase(sid: int) -> str:
        if sid not in phase_cache:
            phase_cache[sid] = A.classify_phase(stacks[sid])
        return phase_cache[sid]

    on_cpu = collections.defaultdict(collections.Counter)  # height -> phase -> samples
    on_partition = collections.defaultdict(collections.Counter)  # height -> part -> samples (wallet_plan)
    on_sub = collections.defaultdict(collections.Counter)  # height -> planner part -> samples
    sub_cache = {}
    part_cache = {}
    for time_s, tid, sid in cycles:
        if tid != commit_tid:
            continue
        height = height_of(time_s)
        if height is None:
            continue
        ph = phase(sid)
        on_cpu[height][ph] += 1
        if ph == "wallet_plan":
            if sid not in part_cache:
                part = "everything else"
                for name, compiled in A.PARTITION_RE:
                    if A.group_matches(compiled, stacks[sid]):
                        part = name
                        break
                part_cache[sid] = part
            on_partition[height][part_cache[sid]] += 1
            if sid not in sub_cache:
                sub_cache[sid] = planner_part(stacks[sid])
            on_sub[height][sub_cache[sid]] += 1

    off = collections.defaultdict(collections.Counter)  # height -> phase -> seconds
    off_cat = collections.defaultdict(collections.Counter)  # height -> category -> seconds (wallet_plan)
    off_count = collections.defaultdict(int)
    off_sub = collections.defaultdict(collections.Counter)
    for line in open(prefix + ".offcpu.tsv"):
        t_out, t_in, tid, preempt, sid = line.rstrip("\n").split("\t")
        if tid != commit_tid:
            continue
        t_out = float(t_out)
        height = height_of(t_out)
        if height is None:
            continue
        sid = int(sid)
        duration = float(t_in) - t_out
        ph = phase(sid)
        off[height][ph] += duration
        if ph == "wallet_plan":
            off_count[height] += 1
            if sid not in sub_cache:
                sub_cache[sid] = planner_part(stacks[sid])
            off_sub[height][sub_cache[sid]] += duration
            category = "other blocked"
            if preempt == "1":
                category = OFF_CATEGORIES[0][0]
            else:
                for name, compiled in OFF_RE[1:]:
                    if A.group_matches(compiled, stacks[sid]):
                        category = name
                        break
            off_cat[height][category] += duration

    results = []
    for scenario in json.load(open(spec_path)):
        if scenario.get("phase") != "wallet_plan":
            continue
        hs = [h for h in scenario["heights"] if h in blocks and h in ends]
        wall = sum(blocks[h]["wallet_plan_ms"] for h in hs) / 1000.0
        on_s = sum(on_cpu[h]["wallet_plan"] for h in hs) / HZ
        off_s = sum(off[h]["wallet_plan"] for h in hs)
        parts = collections.Counter()
        cats = collections.Counter()
        for h in hs:
            parts.update(on_partition[h])
            cats.update(off_cat[h])
        part_seconds = {k: v / HZ for k, v in parts.items()}
        wall_shares = collections.Counter()
        for k, v in part_seconds.items():
            wall_shares["on-CPU: " + k] += v
        for k, v in cats.items():
            wall_shares["off-CPU: " + k] += v
        accounted = on_s + off_s
        sub_wall = collections.Counter()
        for h in hs:
            for k, v in on_sub[h].items():
                sub_wall[k] += v / HZ
            for k, v in off_sub[h].items():
                sub_wall[k] += v
        results.append({
            "name": scenario["name"],
            "blocks": len(hs),
            "planner_wall_seconds_bench_timer": round(wall, 3),
            "planner_on_cpu_seconds": round(on_s, 3),
            "planner_off_cpu_seconds": round(off_s, 3),
            "accounted_pct_of_bench_timer": round(100 * accounted / wall, 1) if wall else None,
            "off_cpu_switches": sum(off_count[h] for h in hs),
            "off_cpu_categories_seconds": {k: round(v, 3) for k, v in cats.most_common()},
            "wall_share_pct_of_accounted": {k: round(100 * v / accounted, 2) for k, v in wall_shares.most_common()},
            "planner_parts_wall_pct": {k: round(100 * v / accounted, 2) for k, v in sub_wall.most_common()} if accounted else {},
            "rocksdb_reads_wall_pct": round(100 * (part_seconds.get(A.PARTITION[0][0], 0.0)
                                                  + sum(v for k, v in cats.items() if k.startswith("RocksDB get"))) / accounted, 2) if accounted else None,
            "hashing_wall_pct": round(100 * sum(v for k, v in part_seconds.items() if k.startswith("hashing")) / accounted, 2) if accounted else None,
        })
    json.dump({"commit_tid": commit_tid, "scenarios": results}, open(out_path, "w"), indent=1)
    for r in results:
        print(json.dumps({k: r[k] for k in ("name", "blocks", "planner_wall_seconds_bench_timer", "planner_on_cpu_seconds", "planner_off_cpu_seconds", "accounted_pct_of_bench_timer", "rocksdb_reads_wall_pct", "hashing_wall_pct")}))


if __name__ == "__main__":
    main()
