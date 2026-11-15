#!/usr/bin/env python3
"""Count uprobe hits per block by thread role and calling path.

Usage: perf script -F comm,tid,time,event,ip,sym --no-inline | count-probes.py REPLAY_LOG CLOCK_OFFSETS FIRST LAST

Only hits inside commit windows of heights FIRST..LAST are counted, so
open-time and post-replay work is excluded. A hit's role is "commit" when
its stack contains the commit loop and "derive" when it contains the
pipeline's block derivation. The calling path is the chain of zinder,
incrementalmerkletree, and commitment-tree frames between the role frame
and the probed function, outermost first.
"""
import bisect
import collections
import re
import sys

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from analyze import load_ends  # noqa: E402

HEADER = re.compile(r"^\s*(.+?)\s+(\d+)\s+(\d+\.\d+):\s+(\S+):")
KEEP = re.compile(r"zinder_(store|core|ingest|wallet_rocksdb|wallet_projection)::|incrementalmerkletree::frontier::Frontier")
ROLE = [("commit", re.compile(r"replay::commit_loop|Committer::commit_append")),
        ("derive", re.compile(r"spike_derive_live_block|pipeline::derive_block"))]


def short(frame: str) -> str:
    frame = re.sub(r"<impl [^>]*>::", "", frame)
    frame = frame.replace("zinder_store::canonical_store::", "store::")
    frame = frame.replace("zinder_core::commitment_tree_codec::", "codec::")
    frame = frame.replace("incrementalmerkletree::frontier::", "")
    frame = frame.replace("zinder_wallet_rocksdb::transition::", "wallet::")
    return frame


def main() -> None:
    log_path, offsets_path, first, last = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
    offsets = [float(x) for x in open(offsets_path).read().split()]
    ends = load_ends(log_path, sum(offsets) / len(offsets))
    heights = sorted(ends)
    end_times = [ends[h] for h in heights]
    start = ends[first - 1] if first - 1 in ends else ends[first] - 1.0
    stop = ends[last]
    totals = collections.Counter()
    paths = collections.Counter()
    header = None
    frames = []

    def flush() -> None:
        if header is None:
            return
        event, time_s = header
        if not (start < time_s <= stop):
            return
        role = "other"
        for name, pattern in ROLE:
            if any(pattern.search(f) for f in frames):
                role = name
                break
        chain = [short(f) for f in reversed(frames[1:]) if KEEP.search(f)]
        totals[(event, role)] += 1
        paths[(event, role, " > ".join(chain[-4:]))] += 1

    for raw in sys.stdin:
        if not raw.strip():
            flush()
            header = None
            frames = []
            continue
        match = HEADER.match(raw)
        if match and not raw.startswith("\t"):
            flush()
            header = (match.group(4).split(":")[-1], float(match.group(3)))
            frames = []
            continue
        parts = raw.split(None, 1)
        if header is not None and len(parts) == 2:
            frames.append(parts[1].rsplit(" (", 1)[0].strip())
    flush()
    blocks = last - first + 1
    print(f"blocks={blocks} window=({start:.3f},{stop:.3f}]")
    print("\n# hits per block by event and role")
    for (event, role), count in sorted(totals.items()):
        print(f"{event:18s} {role:7s} {count:9d} {count / blocks:9.3f}")
    print("\n# hits per block by calling path (innermost four frames)")
    for (event, role, chain), count in sorted(paths.items(), key=lambda kv: (kv[0][0], -kv[1])):
        if count / blocks >= 0.01:
            print(f"{event:18s} {role:7s} {count / blocks:8.3f}  {chain}")


if __name__ == "__main__":
    main()
