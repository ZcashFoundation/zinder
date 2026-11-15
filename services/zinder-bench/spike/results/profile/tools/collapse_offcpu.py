#!/usr/bin/env python3
"""Collapse an on- plus off-CPU recording.

Input: `perf script -F comm,tid,time,event,ip,sym,dsoff --no-inline --show-switch-events`
of a recording made with `-e cycles -e sched:sched_switch --switch-events -g`.

Writes, next to OUT_PREFIX:
  OUT_PREFIX.stacks.tsv    stack_id <TAB> root;...;leaf (shared table)
  OUT_PREFIX.samples.tsv   cycles samples, same format as collapse.py
  OUT_PREFIX.offcpu.tsv    t_out <TAB> t_in <TAB> tid <TAB> preempt(0/1) <TAB> stack_id
An off-CPU interval runs from a thread's PERF_RECORD_SWITCH OUT to its next
PERF_RECORD_SWITCH IN; its stack is that thread's preceding sched_switch
sample (the stack at switch-out).
"""
import re
import sys

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from collapse import frame_name  # noqa: E402

HEADER = re.compile(rb"^\s*(.+?)\s+(\d+)\s+(\d+\.\d+):\s+(.*)$")


def main() -> None:
    prefix = sys.argv[1]
    stack_ids: dict = {}
    cache: dict = {}
    samples_out = open(prefix + ".samples.tsv", "w", buffering=1 << 20)
    offcpu_out = open(prefix + ".offcpu.tsv", "w", buffering=1 << 20)
    last_switch_stack: dict = {}
    pending_out: dict = {}
    current = None  # (kind, comm, tid, time)
    frames: list = []
    counts = {"cycles": 0, "switch": 0, "intervals": 0}

    def sid_for(frames_root_first: list) -> int:
        stack = ";".join(frames_root_first) if frames_root_first else "[no-stack]"
        sid = stack_ids.get(stack)
        if sid is None:
            sid = len(stack_ids)
            stack_ids[stack] = sid
        return sid

    def flush() -> None:
        if current is None:
            return
        kind, comm, tid, time_s = current
        sid = sid_for(list(reversed(frames)))
        if kind == "cycles":
            samples_out.write(f"{time_s}\t{tid}\t{comm}\t{sid}\n")
            counts["cycles"] += 1
        elif kind == "switch":
            last_switch_stack[tid] = sid
            counts["switch"] += 1

    for line in sys.stdin.buffer:
        if not line.strip():
            flush()
            current = None
            frames = []
            continue
        if line[:1] in (b"\t", b" ") and current is not None:
            parts = line.split(None, 1)
            if len(parts) == 2 and re.fullmatch(rb"[0-9a-f]+", parts[0]):
                frames.append(frame_name(parts[1], cache))
                continue
        match = HEADER.match(line)
        if not match:
            continue
        flush()
        frames = []
        current = None
        comm = match.group(1).decode("utf-8", "replace").replace("\t", " ")
        tid = match.group(2).decode()
        time_s = match.group(3).decode()
        rest = match.group(4)
        if rest.startswith(b"PERF_RECORD_SWITCH OUT"):
            pending_out[tid] = (time_s, b"preempt" in rest)
        elif rest.startswith(b"PERF_RECORD_SWITCH IN"):
            started = pending_out.pop(tid, None)
            if started is not None:
                sid = last_switch_stack.get(tid, sid_for(["[no-switch-stack]"]))
                offcpu_out.write(f"{started[0]}\t{time_s}\t{tid}\t{int(started[1])}\t{sid}\n")
                counts["intervals"] += 1
        elif rest.startswith(b"sched:sched_switch"):
            current = ("switch", comm, tid, time_s)
        elif rest.startswith(b"cycles"):
            current = ("cycles", comm, tid, time_s)
    flush()
    samples_out.close()
    offcpu_out.close()
    with open(prefix + ".stacks.tsv", "w") as stacks_out:
        for stack, sid in stack_ids.items():
            stacks_out.write(f"{sid}\t{stack}\n")
    print(counts, f"unique_stacks={len(stack_ids)}", file=sys.stderr)


if __name__ == "__main__":
    main()
