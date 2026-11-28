#!/usr/bin/env python3
"""Measure writer-to-reader publication delay from one retained process interval."""

import argparse
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import re
import sys


APPEND = "canonical_live_append_committed"
REPLACE = "canonical_live_replacement_committed"
FORK = "canonical_source_fork_discovered"
PUBLISH = "wallet_serving_pair_publisher_published"
PROJECT = "wallet_projection_following_applied"
SELECT = "wallet_serving_pair_publisher_generation_selected"
WAIT_START = "wallet_serving_pair_publisher_generation_wait_started"
WAIT_END = "wallet_serving_pair_publisher_generation_wait_ended"
OWNERS = {
    APPEND: "writer", REPLACE: "writer", FORK: "writer", PROJECT: "projector",
    PUBLISH: "reader", SELECT: "reader", WAIT_START: "reader", WAIT_END: "reader",
}
EVENT_FIELDS = {
    APPEND: "chain_epoch chain_event_sequence visible_tip_height visible_tip_hash sequence_digest",
    REPLACE: ("chain_epoch chain_event_sequence visible_tip_height visible_tip_hash sequence_digest "
              "previous_tip_height previous_tip_hash source_tip_height"),
    FORK: ("common_parent_height common_parent_hash local_tip_height source_tip_height "
           "replacement_tip_height replacement_block_count"),
    PUBLISH: "chain_epoch event_sequence visible_height generation",
    PROJECT: "chain_epoch event_sequence visible_height visible_tip_hash sequence_digest",
    SELECT: "chain_epoch event_sequence generation",
    WAIT_START: "chain_epoch event_sequence reason",
    WAIT_END: "reason outcome elapsed_seconds",
}
CANONICAL_FIELD = {"chain_event_sequence": "event_sequence", "visible_tip_height": "visible_height"}
EVENT = re.compile(r'\bevent="(' + "|".join(OWNERS) + r')"')
FIELD = re.compile(r'\b(\w+)=((?:BlockHash\()?\[[^\]]*\]\)?|"[^"]*"|\S+)(?=\s|$)')
ANSI = re.compile(r"\x1b\[[0-9;]*m")
OUTCOMES = ("exact", "coalesced", "superseded", "unmatched", "pending")


def timestamp(value):
    if not isinstance(value, str) or "T" not in value:
        raise ValueError("expected an ISO 8601 timestamp with UTC offset")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("invalid ISO 8601 timestamp") from None
    if parsed.utcoffset() is None:
        raise ValueError("timestamp requires UTC offset")
    return parsed.astimezone(timezone.utc)


def utc(t):
    return t.isoformat().replace("+00:00", "Z")


def distribution(samples):
    ordered = sorted(samples)

    def percentile(q):
        if not ordered:
            return None
        index = q * (len(ordered) - 1)
        left, right = math.floor(index), math.ceil(index)
        return round(ordered[left] + (ordered[right] - ordered[left]) * (index - left), 6)

    return {"n": len(ordered), "p50": percentile(.5), "p90": percentile(.9),
            "p99": percentile(.99), "max": percentile(1)}


def require_text(mapping, keys):
    for key in keys:
        if not isinstance(mapping.get(key), str) or not mapping[key].strip():
            raise ValueError(f"expected nonempty {key}")


def validate_scope(scope, start, end, through):
    require_text(scope, ("release", "deployment", "replica", "store_lineage", "event_schema"))
    schema = scope["event_schema"]
    if schema not in ("fence", "v0.17.0"):
        raise ValueError("event_schema must be fence or v0.17.0")
    for role in ("writer", "reader", "projector"):
        if role == "projector" and role not in scope:
            continue
        source = scope[role]
        require_text(source, ("lifetime", "log"))
        if role == "reader":
            require_text(source, ("role",))
        if role != "writer" and schema == "fence":
            if type(source.get("pid")) is not int or source["pid"] <= 0:
                raise ValueError(f"{role} requires an expected positive pid")
        elif "pid" in source or (role == "projector" and schema == "v0.17.0"):
            raise ValueError(f"{role} PID is unavailable for this schema")
    begin = timestamp(scope["coverage_start"])
    stop = timestamp(scope["coverage_end"])
    if not begin <= start < end <= through <= stop:
        raise ValueError("requested interval exceeds the declared continuous process coverage")
    if type(scope.get("ends_at_restart", False)) is not bool:
        raise ValueError("ends_at_restart must be a boolean")
    if not isinstance(scope["coverage_gaps"], list):
        raise ValueError("coverage_gaps must be an array of [start, end] pairs")
    for gap in scope["coverage_gaps"]:
        if not isinstance(gap, list) or len(gap) != 2:
            raise ValueError("coverage gap requires a [start, end] pair")
        left, right = map(timestamp, gap)
        if left >= right:
            raise ValueError("coverage gap has invalid bounds")
        if left < through and right > begin:
            raise ValueError("coverage gap intersects replay; split the process interval")
    return begin, stop


@dataclass(frozen=True)
class Record:
    t: datetime
    event: str
    fields: dict

    def __getitem__(self, key):
        return self.fields[key]

    @property
    def seq(self):
        return self["event_sequence"]


def decode(message, t, event, schema, source):
    pairs = FIELD.findall(message)
    fields = dict(pairs)
    if len(fields) != len(pairs):
        raise ValueError("ambiguous repeated trace field")
    names = EVENT_FIELDS[event].split()
    if schema == "fence":
        if event == REPLACE:
            names += ["common_parent_height", "common_parent_hash"]
        if event == PUBLISH:
            names += ["visible_tip_hash", "sequence_digest"]
        if OWNERS[event] != "writer" and int(fields["process_id"]) != source["pid"]:
            raise ValueError("unexpected process_id; split process lifetimes and readers")
    elif event not in (APPEND, REPLACE, FORK, PUBLISH) or (
            event == PUBLISH and any(k in fields for k in ("visible_tip_hash", "sequence_digest", "process_id"))
    ) or (event == REPLACE and "common_parent_height" in fields):
        raise ValueError("event does not match v0.17.0 schema")
    parsed = {}
    for key in names:
        raw = fields[key]
        if key.endswith("_hash") or key == "sequence_digest":
            pattern = r"BlockHash\(\[([0-9, ]+)\]\)" if key.endswith("_hash") else r"\[([0-9, ]+)\]"
            match = re.fullmatch(pattern, raw)
            if not match:
                raise ValueError("invalid fence identity")
            val = bytes(int(byte) for byte in match[1].split(","))
            if len(val) != 32:
                raise ValueError("invalid fence identity length")
        elif key == "reason" or key == "outcome":
            val = raw.strip('"')
            allowed = ("retention_window", "in_flight_requests") if key == "reason" else (
                "available", "reason_changed", "interrupted")
            if val not in allowed:
                raise ValueError(f"invalid wait {key}")
        elif key == "elapsed_seconds":
            val = float(raw)
            if not math.isfinite(val) or val < 0:
                raise ValueError("invalid elapsed_seconds")
        else:
            val = int(raw)
            bits = 32 if key.endswith("height") else 64
            if not 0 <= val < 1 << bits:
                raise ValueError("integer event field out of range")
        parsed[CANONICAL_FIELD.get(key, key)] = val
    return Record(t, event, parsed)


def load_records(scope, directory, begin, through):
    paths = defaultdict(dict)
    for role in ("writer", "reader", "projector"):
        if role in scope:
            paths[(directory / scope[role]["log"]).resolve()][role] = scope[role]
    records = defaultdict(dict)
    duplicates = Counter()
    for path, roles in paths.items():
        with path.open(encoding="utf-8") as lines:
            for number, line in enumerate(lines, 1):
                try:
                    message = ANSI.sub("", json.loads(line)["message"])
                    event = EVENT.search(message)
                    if event is None or OWNERS[event[1]] not in roles:
                        continue
                    t = timestamp(message.split(maxsplit=1)[0])
                    if begin <= t < through:
                        row = decode(message, t, event[1], scope["event_schema"], roles[OWNERS[event[1]]])
                        key = (row.t, tuple(row.fields.items()))
                        if key in records[row.event]:
                            duplicates[row.event] += 1
                        records[row.event][key] = row
                except (KeyError, ValueError, IndexError, TypeError):
                    # Provider messages can contain sensitive unrelated fields.
                    raise ValueError(f"invalid {','.join(roles)} record on line {number}; check schema, PID, and fields") from None
    return {event: sorted(records[event].values(), key=lambda r: r.t) for event in OWNERS}, duplicates


def replacement_parent(previous, current, known, discoveries, fork_times, legacy):
    if current["visible_tip_hash"] == previous["visible_tip_hash"]:
        return None
    if (current["previous_tip_height"], current["previous_tip_hash"]) != (
        previous["visible_height"], previous["visible_tip_hash"]
    ):
        return None
    witness = current
    if legacy:
        candidates = discoveries[bisect_right(fork_times, previous.t):bisect_right(fork_times, current.t)]
        matches = [f for f in candidates if f["local_tip_height"] == previous["visible_height"]
                   and f["source_tip_height"] == current["source_tip_height"]
                   and f["replacement_tip_height"] == current["visible_height"]
                   and f["replacement_block_count"] == current["visible_height"] - f["common_parent_height"]]
        if len(matches) != 1:
            return None
        witness = matches[0]
    height, block = witness["common_parent_height"], witness["common_parent_hash"]
    if height >= min(current["visible_height"], previous["visible_height"]):
        return None
    if height in known:
        return height if known[height] == block else None
    # A committed parent is authoritative even when older than the retained writer log.
    return None if legacy else height


def important_transitions(commits, discoveries, legacy):
    changes, known = {}, {}
    fork_times = [r.t for r in discoveries]
    for previous, current in zip(commits, commits[1:]):
        known[previous["visible_height"]] = previous["visible_tip_hash"]
        parent = None
        if current.seq == previous.seq + 1:
            if (current.event == APPEND and current["visible_height"] == previous["visible_height"] + 1
                    and current["visible_tip_hash"] != previous["visible_tip_hash"]):
                continue
            if current.event == REPLACE:
                parent = replacement_parent(previous, current, known, discoveries, fork_times, legacy)
        changes[current.seq] = parent
        known = {} if parent is None else {h: b for h, b in known.items() if h <= parent}
    return changes


def matches(commit, observation, legacy):
    fields = ["chain_epoch", "event_sequence", "visible_height"]
    if not legacy:
        fields += ["visible_tip_hash", "sequence_digest"]
    return all(commit[k] == observation[k] for k in fields)


def reconcile(records, start, end, horizon, legacy, closed):
    commits = sorted((c for kind in (APPEND, REPLACE) for c in records[kind] if c.t < horizon),
                     key=lambda c: c.seq)
    pubs = [p for p in records[PUBLISH] if p.t < horizon]
    # next_live_fence in zinder-store admits version-1 commits only when epoch equals sequence.
    if any(c.seq != c["chain_epoch"] for c in commits):
        raise ValueError("writer epoch and sequence differ on a version-1 fence")
    if any(
            a.seq >= b.seq or a.t > b.t for a, b in zip(commits, commits[1:])):
        raise ValueError("ambiguous writer sequence or clock; split lineages and process lifetimes")
    for a, b in zip(pubs, pubs[1:]):
        if b.seq < a.seq or (a.t == b.t and a.seq != b.seq):
            raise ValueError("reader sequence reversed or ambiguous; split reader process lifetimes")
        if a.seq == b.seq and not matches(a, b, legacy):
            raise ValueError("conflicting reader fences for one sequence")
    by_seq = {c.seq: c for c in commits}
    changes = important_transitions(commits, records[FORK], legacy)
    changed = list(changes)
    sequences = [p.seq for p in pubs]
    observations = []
    for c in commits:
        if not start <= c.t < end:
            continue
        i = bisect_left(sequences, c.seq)
        p = pubs[i] if i < len(pubs) else None
        last = p.seq if p else commits[-1].seq
        row = {"committed_at": utc(c.t), "sequence": c.seq, "height": c["visible_height"]}
        if p and (last not in by_seq or by_seq[last].t > p.t or not matches(by_seq[last], p, legacy)):
            row.update(outcome="unmatched", reason="unverified_publication")
        else:
            for index in range(bisect_right(changed, c.seq), bisect_right(changed, last)):
                seq = changed[index]
                if changes[seq] is None or c["visible_height"] > changes[seq]:
                    row.update(outcome="unmatched" if changes[seq] is None else "superseded",
                               transition_sequence=seq)
                    break
            else:
                if p:
                    row.update(outcome="exact" if c.seq == last else "coalesced",
                               published_at=utc(p.t), published_sequence=last,
                               delay_seconds=(p.t - c.t).total_seconds())
                elif closed:
                    row.update(outcome="unmatched", reason="process_interval_ended")
                else:
                    row.update(outcome="pending")
        observations.append(row)
    counts = Counter(r["outcome"] for r in observations)
    latency = {group: distribution([r["delay_seconds"] for r in observations if r["outcome"] in types])
               for group, types in {"exact": ["exact"], "coalesced": ["coalesced"],
                                    "first_covering": ["exact", "coalesced"]}.items()}
    summary = {"through": utc(horizon), "committed_observed": len(observations),
               "writer_gap_touches_cohort": any(a.seq + 1 < b.seq and a.t < end and b.t >= start
                                                for a, b in zip(commits, commits[1:])),
               "counts": {k: counts[k] for k in OUTCOMES}, "delay_seconds": latency,
               "unverified_transitions": [s for s in changed if changes[s] is None],
               "missing_writer_ranges": [[a.seq + 1, b.seq - 1] for a, b in zip(commits, commits[1:])
                                         if a.seq + 1 < b.seq]}
    return summary, observations


def annotate_stages(records, observations):
    """Only exact cohort endpoints can contribute stage samples."""
    projections = defaultdict(list)
    for applied in records[PROJECT]:
        projections[applied.seq].append(applied)
    selections = records[SELECT]
    selected_times = [s.t for s in selections]
    first_pubs, previous_pub = {}, None
    for p in records[PUBLISH]:
        first_pubs.setdefault(p.seq, (p, previous_pub))
        previous_pub = p
    samples = {name: [] for name in (
        "commit_to_projection", "projection_to_selection", "commit_to_selection",
        "selection_to_publication", "projection_to_publication",
    )}
    for row in observations:
        if row["outcome"] != "exact":
            continue
        committed = timestamp(row["committed_at"])
        pub, prior = first_pubs[row["sequence"]]
        times = {"commit": committed, "publication": pub.t}
        applied = projections[pub.seq]
        if len(applied) == 1 and committed <= applied[0].t <= pub.t and matches(applied[0], pub, False):
            times["projection"] = applied[0].t
            row["projected_at"] = utc(times["projection"])
        # A failed or superseded attempt can select the same generation again.
        first = bisect_left(selected_times, prior.t) if prior else 0
        last = bisect_right(selected_times, pub.t)
        if last - first == 1:
            selected = selections[first]
            if (not prior or prior.t < selected.t) and committed <= selected.t < pub.t and all(
                    selected[k] == pub[k] for k in (
                    "generation", "chain_epoch", "event_sequence")):
                times["selection"] = selected.t
                row["selected_at"] = utc(times["selection"])
        for name, durations in samples.items():
            before, after = name.split("_to_")
            if before in times and after in times and times[before] <= times[after]:
                durations.append((times[after] - times[before]).total_seconds())
    return {name: distribution(values) for name, values in samples.items()}


def wait_intervals(records, start, through):
    events = sorted((r for kind in (WAIT_END, WAIT_START) for r in records[kind]
                     if start <= r.t < through), key=lambda r: r.t)
    intervals = []
    incomplete = 0
    opened = None
    for r in events:
        if r.event == WAIT_START:
            incomplete += opened is not None
            opened = r
        else:
            if opened and opened["reason"] == r["reason"] and opened.t < r.t:
                intervals.append({"started_at": utc(opened.t), "ended_at": utc(r.t),
                                  "reason": r["reason"], "outcome": r["outcome"],
                                  "start_epoch": opened["chain_epoch"], "start_sequence": opened.seq,
                                  "elapsed_seconds": r["elapsed_seconds"]})
            else:
                incomplete += 1 + (opened is not None)
            opened = None
    return {"complete": intervals, "unpaired_events": incomplete + (opened is not None)}


def report(scope, directory, start, end, through):
    begin, stop = validate_scope(scope, start, end, through)
    records, duplicates = load_records(scope, directory, begin, through)
    if not records[PUBLISH] or not (records[APPEND] or records[REPLACE]):
        raise ValueError("no retained writer or reader observations in the supplied coverage")
    legacy = scope["event_schema"] == "v0.17.0"
    closed = scope.get("ends_at_restart", False)
    cutoff, before = reconcile(records, start, end, end, legacy, closed and end == stop)
    if end == through:
        summary, observations = cutoff, before
    else:
        summary, observations = reconcile(records, start, end, through, legacy, closed and through == stop)
    for row, at_cutoff in zip(observations, before, strict=True):
        row["at_cutoff_outcome"] = at_cutoff["outcome"]
    return {
        "scope": {key: scope[key] for key in ("release", "deployment", "replica", "store_lineage", "event_schema")}
                 | {role: {k: v for k, v in scope[role].items() if k in ("pid", "lifetime", "role")}
                    for role in ("writer", "reader", "projector") if role in scope},
        "window": {"start": utc(start), "end": utc(end), "through": utc(through)},
        "coverage": {"start": utc(begin), "end": utc(stop), "ends_at_restart": closed,
                     "events": {kind: {"n": len(rows), "duplicates": duplicates[kind],
                                       "last": utc(rows[-1].t) if rows else None}
                                for kind, rows in records.items()}},
        "at_cutoff": cutoff, "as_of": summary,
        "exact_fence_stage_seconds": None if legacy else annotate_stages(records, observations),
        "reader_waits": None if legacy else wait_intervals(records, start, through),
        "observations": observations,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scope", type=Path, help="JSON scope naming the source files, lifetimes and coverage")
    parser.add_argument("--start", required=True, help="inclusive cohort start (ISO 8601 with offset)")
    parser.add_argument("--end", required=True, help="exclusive cohort end")
    parser.add_argument("--through", help="exclusive lookahead cutoff (defaults to --end)")
    args = parser.parse_args()
    try:
        scope = json.loads(args.scope.read_text(encoding="utf-8"))
        result = report(scope, args.scope.parent, timestamp(args.start), timestamp(args.end),
                        timestamp(args.through or args.end))
    except (ValueError, OSError, TypeError, KeyError, AttributeError) as error:
        print(f"publication report: {error}", file=sys.stderr)
        return 2
    json.dump(result, sys.stdout, indent=2, allow_nan=False)
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
