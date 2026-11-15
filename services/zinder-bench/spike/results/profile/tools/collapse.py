#!/usr/bin/env python3
"""Collapse `perf script -F comm,tid,time,ip,sym,dsoff` output (with callchains).

Writes two files next to OUT_PREFIX:
  OUT_PREFIX.stacks.tsv   stack_id <TAB> root;...;leaf
  OUT_PREFIX.samples.tsv  time <TAB> tid <TAB> comm <TAB> stack_id
and prints a summary. Frames are written root first, leaf last, the order
FlameGraph and inferno expect. C++ parameter lists are dropped for
readability; unknown frames keep their DSO basename.

This host's glibc (build ID 9851d79b1f32e900a27f8d0a68e29d750303f0c6) has no
local symbols, so unknown libc frames are named from their file offset:
the IFUNC implementations that dlsym resolves on this CPU (memcmp/bcmp at
0x1a7400, memcpy/memmove at 0x1b0a40, memset at 0x1b12c0) and the static
malloc.c functions that sit between 0xb0500 and malloc at 0xb4dd0.
"""
import re
import sys

HEADER = re.compile(rb"^\s*(.+?)\s+(\d+)\s+(\d+\.\d+):")


def strip_params(sym: str) -> str:
    if not sym.endswith(")") and not sym.endswith(") const"):
        return sym
    depth = 0
    for index, char in enumerate(sym):
        if char == "<":
            depth += 1
        elif char == ">":
            depth -= 1
        elif char == "(" and depth == 0 and index > 0:
            prev = sym[index - 1]
            if prev.isalnum() or prev in "_>~":
                return sym[:index]
    return sym


LIBC_RANGES = [
    (0x1A7400, 0x1A7800, "[libc] memcmp (IFUNC impl)"),
    (0x1B0A00, 0x1B12C0, "[libc] memcpy/memmove (IFUNC impl)"),
    (0x1B12C0, 0x1B1800, "[libc] memset (IFUNC impl)"),
    (0xB0500, 0xB4DD0, "[libc] malloc internals"),
]


def libc_name(offset: int) -> str:
    for low, high, name in LIBC_RANGES:
        if low <= offset < high:
            return name
    return "[unknown:libc.so.6]"


def frame_name(rest: bytes, cache: dict) -> str:
    cached = cache.get(rest)
    if cached is not None:
        return cached
    text = rest.decode("utf-8", "replace").strip()
    dso = ""
    if text.endswith(")") and " (" in text:
        text, dso = text.rsplit(" (", 1)
        dso = dso[:-1]
    offset = None
    if "+0x" in dso:
        dso, off = dso.rsplit("+0x", 1)
        offset = int(off, 16)
    if text.startswith("[unknown]") or not text:
        base = dso.rsplit("/", 1)[-1] if dso else "?"
        if base == "libc.so.6" and offset is not None:
            name = libc_name(offset)
        else:
            name = f"[unknown:{base}]"
    else:
        name = strip_params(text)
        # Drop the legacy Rust hash suffix if the demangler kept it.
        name = re.sub(r"::h[0-9a-f]{16}$", "", name)
        if dso.startswith("[kernel") or dso.endswith(".ko") or dso.startswith("[") or dso == "/proc/kcore":
            name = name + "_[k]"
    name = name.replace(";", ":")
    cache[rest] = name
    return name


def main() -> None:
    out_prefix = sys.argv[1]
    stack_ids: dict = {}
    frame_cache: dict = {}
    samples_out = open(out_prefix + ".samples.tsv", "w", buffering=1 << 20)
    header = None
    frames: list = []
    total = 0

    def flush() -> None:
        nonlocal total
        if header is None:
            return
        comm, tid, time = header
        stack = ";".join(reversed(frames)) if frames else "[no-stack]"
        sid = stack_ids.get(stack)
        if sid is None:
            sid = len(stack_ids)
            stack_ids[stack] = sid
        samples_out.write(f"{time}\t{tid}\t{comm}\t{sid}\n")
        total += 1

    for line in sys.stdin.buffer:
        if not line.strip():
            flush()
            header = None
            frames = []
            continue
        if line[:1] in (b"\t", b" ") and header is not None:
            parts = line.split(None, 1)
            if len(parts) == 2 and re.fullmatch(rb"[0-9a-f]+", parts[0]):
                frames.append(frame_name(parts[1], frame_cache))
                continue
        match = HEADER.match(line)
        if match:
            flush()
            frames = []
            header = (
                match.group(1).decode("utf-8", "replace").replace("\t", " "),
                match.group(2).decode(),
                match.group(3).decode(),
            )
    flush()
    samples_out.close()
    with open(out_prefix + ".stacks.tsv", "w") as stacks_out:
        for stack, sid in stack_ids.items():
            stacks_out.write(f"{sid}\t{stack}\n")
    print(f"samples={total} unique_stacks={len(stack_ids)}", file=sys.stderr)


if __name__ == "__main__":
    main()
