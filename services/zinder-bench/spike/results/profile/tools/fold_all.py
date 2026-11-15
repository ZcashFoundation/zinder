#!/usr/bin/env python3
"""Whole-run folded stacks for flame graphs.

Usage: fold_all.py RUN_PREFIX OUT_DIR
Writes OUT_DIR/all-threads.on-cpu.folded (thread name as the root frame,
weight = samples) and, when RUN_PREFIX.offcpu.tsv exists,
OUT_DIR/commit-thread.off-cpu.folded (weight = microseconds off-CPU, root
frame "preempted" or "blocked").
"""
import collections
import os
import re
import sys

sys.path.insert(0, __file__.rsplit("/", 1)[0])
import analyze as A  # noqa: E402


def main() -> None:
    prefix, out_dir = sys.argv[1:3]
    stacks = {}
    for line in open(prefix + ".stacks.tsv"):
        sid, stack = line.rstrip("\n").split("\t", 1)
        stacks[int(sid)] = stack
    on = collections.Counter()
    per_tid = collections.Counter()
    commit_like = {sid for sid, s in stacks.items() if A.COMMIT_THREAD.search(s)}
    for line in open(prefix + ".samples.tsv"):
        _, tid, comm, sid = line.rstrip("\n").split("\t")
        sid = int(sid)
        on[(re.sub(r"\s+", "_", comm), sid)] += 1
        if sid in commit_like:
            per_tid[tid] += 1
    with open(os.path.join(out_dir, "all-threads.on-cpu.folded"), "w") as out:
        for (comm, sid), count in on.most_common():
            out.write(f"{comm};{stacks[sid]} {count}\n")
    offcpu = prefix + ".offcpu.tsv"
    if os.path.exists(offcpu):
        commit_tid = per_tid.most_common(1)[0][0]
        off = collections.Counter()
        for line in open(offcpu):
            t_out, t_in, tid, preempt, sid = line.rstrip("\n").split("\t")
            if tid != commit_tid:
                continue
            micros = round((float(t_in) - float(t_out)) * 1e6)
            off[("preempted" if preempt == "1" else "blocked", int(sid))] += micros
        with open(os.path.join(out_dir, "commit-thread.off-cpu.folded"), "w") as out:
            for (kind, sid), micros in off.most_common():
                if micros:
                    out.write(f"{kind};{stacks[sid]} {micros}\n")


if __name__ == "__main__":
    main()
