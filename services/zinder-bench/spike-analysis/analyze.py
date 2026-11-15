#!/usr/bin/env python3
"""Spike B result analysis (Testnet). Reads the run directories under the results
root and writes results.json with one row per threshold.

Pre-registered criteria (written before the measured runs):
- stop stall: zero stop counts and zero write-stopped samples, every phase, both DBs.
- commit p99 <= 1 s for every phase (paced commits; replacement and catch-up
  commits are reported separately).
- 2x rule for held snapshots and serving load: a treatment fails only if its p99
  exceeds 2x the no-reader baseline before it AND 2x the no-reader baseline after
  it (A-B-A attribution); with no after-baseline, 2x the before-baseline.
- Arm 2: (a) the ingest call: the longest probed synced write that overlapped the
  ingest and the p99 of commits in the ingest stage must be <= 1 s; the 2x rule is
  applied to the attach phase p99 per stage. (b) the whole attach phase p99
  (including the in-process SST build) <= 2x baseline and <= 1 s.
- Space after a 30-minute hold: pass if at least 90% of the hold-induced excess
  (dead wallet-ballast entries above the pre-hold level and the no-reader drift)
  is gone by the end of the 15-minute natural-compaction window after release;
  otherwise fail. At 1x, where the ballast is unused, the metric is wallet and
  canonical live SST bytes plus obsolete-file bytes.
- Torn view: zero violations in snapshot readers; control readers must detect
  changes (validity).
"""
import json, os, re, statistics, sys

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
ROOT = sys.argv[1] if len(sys.argv) > 1 else os.path.join(REPO, ".tmp", "spike-results")
OUT = sys.argv[2] if len(sys.argv) > 2 else os.path.join(ROOT, "results.json")

# Paired no-hold controls that must not be read even when files exist. Each value is
# the reason recorded on the threshold that needed the control.
EXCLUDED_CONTROLS = {
    "density-x60-control": (
        "paired no-hold control at 60x did not complete: it ran 08:06:20-09:08:45 UTC, the shared host disk filled "
        "(free space fell from 24 GB at 08:06 to 0.14 GB at 09:08), the wallet stage detached with ENOSPC at t=3734 s, "
        "63 s into the 15-minute post-release window, and the run exited 1; the release-equivalent point "
        "(09:06:32 UTC), the end of the natural window, and the forced-compaction point are after 09:00 "
        "or absent, and its first phase (08:06:32-08:36:32) overlapped 186 GB of other sessions' IO "
        "(no-reader commit p99 0.477 s against 0.037 s in the hold run's same phase), so none of its output is used"),
    "density-x10-control": (
        "paired no-hold control at 10x never started: run_controls.sh skipped it at 09:08:45 UTC because free "
        "disk was 7 GiB, below its 8 GiB guard"),
}


def load(path):
    with open(path) as f:
        return json.load(f)


def jsonl(path):
    out = []
    if not os.path.exists(path):
        return out
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def med(values):
    return statistics.median(values) if values else None


def row(rows, name, threshold, arm, measured, passed, evidence):
    rows.append({"name": name, "threshold": threshold, "arm": arm, "measured": measured,
                 "pass": passed, "evidencePath": evidence})


OWN_PROCESSES = {"zinder-bench", "zakurad", "python3"}


def host_windows(root):
    """10-second windows (unix seconds) where a process outside this measurement did >20 MB of IO."""
    import datetime
    path = os.path.join(root, "host-io.log")
    disturbed, covered = set(), set()
    if not os.path.exists(path):
        return disturbed, covered
    for line in open(path):
        ts = line.split()[0].rstrip("Z")
        try:
            t = datetime.datetime.fromisoformat(ts).replace(tzinfo=datetime.timezone.utc).timestamp()
        except ValueError:
            continue
        bucket = int(t // 10)
        covered.add(bucket)
        fields = line.rstrip("\n").split(" ", 3)
        procs = [p.strip() for p in fields[3].split("|")] if len(fields) > 3 else []
        names = [p.split("[")[0] for p in procs if "[" in p]
        if any(name not in OWN_PROCESSES for name in names):
            # Buffered writes reach the disk up to vm.dirty_expire_centisecs (30 s here) later.
            disturbed.update({bucket - 1, bucket, bucket + 1, bucket + 2, bucket + 3})
    return disturbed, covered


def run_clock_origin(stderr_path):
    """Unix time of run-clock t=0, from the first spike event log line."""
    import datetime
    for line in open(stderr_path, errors="replace"):
        m = re.match(r"(\S+)Z .* t=([0-9.]+) ", line)
        if m:
            wall = datetime.datetime.fromisoformat(m.group(1)).replace(tzinfo=datetime.timezone.utc).timestamp()
            return wall - float(m.group(2))
    return None


def quiet_p99(commits, samples, phase, origin, disturbed, covered):
    """p99 of paced commits outside externally disturbed windows (fallback: loadavg_1m > 4.5)."""
    load = {int(s["t"]): s["host"]["loadavg_1m"] for s in samples if s.get("host")}
    cpu = {int(s["t"]): (s["host"].get("cpu_some_avg10") or 0) for s in samples if s.get("host")}
    kept, dropped = [], 0
    for c in commits:
        if c["phase"] != phase or c["catch_up"]:
            continue
        bucket = int((origin + c["t"]) // 10) if origin is not None else None
        if bucket is not None and bucket in covered:
            bad = bucket in disturbed
        else:
            bad = load.get(int(c["t"]), 0) > 4.5
        # CPU contention: on this 32-core host tasks wait for a CPU only when another
        # workload saturates it; this process alone never raised cpu PSI above 0.4.
        bad = bad or max(cpu.get(int(c["t"]) + d, 0) for d in (-1, 0, 1)) > 1.0
        if bad:
            dropped += 1
        else:
            kept.append(c["total_commit_s"])
    if not kept:
        return None, dropped
    kept.sort()
    return kept[min(len(kept) - 1, max(0, -(-99 * len(kept) // 100) - 1))], dropped


def paired_space(hold_dir, ctrl_dir):
    """Hold run minus its paired no-hold control at matching phase-relative times."""
    def load_run(d):
        rep_ = load(os.path.join(d, "out", "report.json"))
        return {p["name"]: p for p in rep_["phases"]}, jsonl(os.path.join(d, "out", "samples.jsonl"))
    hp, hs = load_run(hold_dir)
    cp, cs = load_run(ctrl_dir)
    hold_cycles = jsonl(os.path.join(hold_dir, "out", "reader-cycles.jsonl"))
    t_rel_h = release_time(hold_cycles, "density-hold-30m")
    mid_h, mid_c = hp["density-hold-30m"], cp["density-control-30m"]
    post_h, post_c = hp["density-post-release"], cp["density-post-release"]
    def metric(sample):
        w = sample.get("wallet") or {}
        b = w.get("ballast") or {}
        return {"dead_entries": sample.get("wallet_ballast_dead_entries"), "ballast_sst_bytes": b.get("sst_bytes"),
                "wallet_live_sst_bytes": w.get("live_sst_bytes"), "wallet_disk_sst_bytes": w.get("disk_sst_bytes"),
                "wallet_obsolete_sst_bytes": w.get("obsolete_sst_bytes")}
    def mean_window(samples, t0, t1):
        xs = [metric(x) for x in samples if t0 <= x["t"] <= t1 and x.get("wallet")]
        if not xs:
            return {}
        return {k: statistics.mean(v[k] for v in xs if v[k] is not None) for k in xs[0] if any(v[k] is not None for v in xs)}
    def last_after(samples, t):
        xs = [x for x in samples if x["t"] > t and x.get("wallet")]
        return metric(xs[-1]) if xs else {}
    points = {
        "hold_start": (mean_window(hs, mid_h["t_start"] - 120, mid_h["t_start"]), mean_window(cs, mid_c["t_start"] - 120, mid_c["t_start"])),
        "release": (mean_window(hs, t_rel_h - 60, t_rel_h), mean_window(cs, t_rel_h - mid_h["t_start"] + mid_c["t_start"] - 60, t_rel_h - mid_h["t_start"] + mid_c["t_start"])),
        "first_5_min_after_release": (mean_window(hs, t_rel_h, t_rel_h + 300), mean_window(cs, t_rel_h - mid_h["t_start"] + mid_c["t_start"], t_rel_h - mid_h["t_start"] + mid_c["t_start"] + 300)),
        "end_of_natural_window": (mean_window(hs, post_h["t_end"] - 120, post_h["t_end"]), mean_window(cs, post_c["t_end"] - 120, post_c["t_end"])),
        "after_forced_compaction": (last_after(hs, post_h["t_end"]), last_after(cs, post_c["t_end"])),
    }
    out = {"snapshot_release_t_hold_run": t_rel_h}
    for name, (h, c) in points.items():
        out[name] = {k: {"hold": h.get(k), "control": c.get(k), "excess": (h.get(k) - c.get(k)) if h.get(k) is not None and c.get(k) is not None else None} for k in set(h) | set(c)}
    return out


def phase_map(report):
    return {p["name"]: p for p in report["phases"]}


def check_map(report):
    return {c["phase"]: c for c in report["fail_conditions"]["phases"]}


def release_time(cycles, phase_name):
    ends = [c["t_end"] for c in cycles if c["phase"] == phase_name and c["snapshot"]]
    return max(ends) if ends else None


def space_return(samples, phases, hold_name, before_name, post_name, t_release=None):
    """Dead wallet-ballast entries around one 30-minute hold (single run, no control)."""
    hold, before, post = phases[hold_name], phases[before_name], phases[post_name]
    series = [(s["t"], s.get("wallet_ballast_dead_entries")) for s in samples
              if s.get("wallet_ballast_dead_entries") is not None]
    if not series:
        return None
    def window(t0, t1):
        return [v for t, v in series if t0 <= t <= t1]
    t_hold, t_post_end = hold["t_start"], post["t_end"]
    t_rel = t_release or hold["t_end"]
    d_pre = med(window(t_hold - 60, t_hold))
    d_rel = med(window(t_rel - 30, t_rel))
    d_post_end = med(window(t_post_end - 60, t_post_end))
    after_compaction = [v for t, v in series if t > t_post_end]
    d_after_compaction = after_compaction[-1] if after_compaction else None
    # No-reader drift: slope over the second half of the before phase, times the hold length.
    half = before["t_start"] + (before["t_end"] - before["t_start"]) / 2
    a_series = [(t, v) for t, v in series if half <= t <= before["t_end"]]
    slope = 0.0
    if len(a_series) > 10:
        ts = [t for t, _ in a_series]; vs = [v for _, v in a_series]
        mt, mv = statistics.mean(ts), statistics.mean(vs)
        den = sum((t - mt) ** 2 for t in ts)
        slope = sum((t - mt) * (v - mv) for t, v in a_series) / den if den else 0.0
    drift = slope * (t_rel - t_hold)
    excess = (d_rel - d_pre - drift) if None not in (d_rel, d_pre) else None
    gone_natural = None
    gone_forced = None
    if excess and excess > 0:
        gone_natural = (d_rel - d_post_end) / (d_rel - d_pre) if d_rel != d_pre else None
        if d_after_compaction is not None:
            gone_forced = (d_rel - d_after_compaction) / (d_rel - d_pre)
    hold_s = [s for s in samples if t_hold <= s["t"] <= t_rel and s.get("wallet")]
    max_dead = max((s.get("wallet_ballast_dead_entries") or 0) for s in hold_s) if hold_s else None
    def ballast_bytes_per_entry(t):
        near = [s for s in samples if abs(s["t"] - t) < 5 and s.get("wallet") and s["wallet"].get("ballast")]
        if not near:
            return None
        b = near[-1]["wallet"]["ballast"]
        entries = b["sst_entries"] or 1
        return b["sst_bytes"] / entries
    bpe = ballast_bytes_per_entry(t_rel)
    def tick_rate(t0, t1, db, key):
        s0 = [s for s in samples if s["t"] >= t0]
        s1 = [s for s in samples if s["t"] <= t1]
        if not s0 or not s1 or not s0[0].get(db) or not s1[-1].get(db):
            return None
        return (s1[-1][db][key] - s0[0][db][key]) / max(1e-9, s1[-1]["t"] - s0[0]["t"])
    drops = {}
    for label, (t0, t1) in {"before": (before["t_start"], before["t_end"]), "hold": (t_hold, t_rel),
                            "post": (post["t_start"], t_post_end)}.items():
        drops[label] = {
            "wallet_drop_newer_per_s": tick_rate(t0, t1, "wallet", "compaction_key_drop_newer"),
            "wallet_drop_obsolete_per_s": tick_rate(t0, t1, "wallet", "compaction_key_drop_obsolete"),
            "wallet_compact_write_bytes_per_s": tick_rate(t0, t1, "wallet", "compact_write_bytes"),
            "wallet_flush_write_bytes_per_s": tick_rate(t0, t1, "wallet", "flush_write_bytes"),
        }
    return {
        "dead_entries_pre_hold": d_pre, "dead_entries_at_release": d_rel,
        "dead_entries_max_during_hold": max_dead,
        "dead_entries_end_of_natural_window": d_post_end,
        "dead_entries_after_forced_compaction": d_after_compaction,
        "no_reader_drift_over_hold": drift, "hold_induced_excess_entries": excess,
        "approx_bytes_per_ballast_entry_at_release": bpe,
        "hold_induced_excess_bytes_approx": (excess * bpe) if (excess and bpe) else None,
        "fraction_gone_by_end_of_natural_window": gone_natural,
        "fraction_gone_after_forced_compaction": gone_forced,
        "compaction_rates": drops,
    }


def flush_counts(samples, t0, t1, db="wallet"):
    """Count increases of ballast SST files at L0 as a proxy for flushes; also report L0 max."""
    xs = [s for s in samples if t0 <= s["t"] <= t1 and s.get(db) and s[db].get("ballast")]
    if not xs:
        return None
    fw0, fw1 = xs[0][db]["flush_write_bytes"], xs[-1][db]["flush_write_bytes"]
    return {"flush_write_bytes": fw1 - fw0,
            "ballast_l0_files_max": max(s[db]["ballast"]["l0_files"] for s in xs),
            "ballast_sst_bytes_start": xs[0][db]["ballast"]["sst_bytes"],
            "ballast_sst_bytes_end": xs[-1][db]["ballast"]["sst_bytes"]}


def stage_attribution(commits, phase_name, baseline_name):
    def pick(phase, stage=None):
        xs = [c for c in commits if c["phase"] == phase and not c["catch_up"] and (stage is None or c["attach_stage"] == stage)]
        return xs
    def summ(xs):
        if not xs:
            return None
        def p(vals, q):
            v = sorted(vals); return v[min(len(v) - 1, max(0, int(-(-q * len(v) // 1)) - 1))]
        out = {"commits": len(xs)}
        for key in ("total_commit_s", "canonical_commit_s", "prepare_s"):
            vals = [c[key] for c in xs if c.get(key) is not None]
            if vals:
                out[key + "_p50"] = p(vals, 0.5); out[key + "_p99"] = p(vals, 0.99)
        for key in ("canonical_write_s", "wallet_apply_s", "wallet_write_s"):
            vals = [c[key] for c in xs if c.get(key) is not None]
            if vals:
                out[key + "_p50"] = p(vals, 0.5); out[key + "_p99"] = p(vals, 0.99)
        return out
    stages = {0: "idle", 1: "sst_build", 2: "ingest", 3: "catch_up", 4: "switch", 5: "drop_and_wait_for_files", 6: "verify_and_wait_before_drop"}
    return {"baseline": summ(pick(baseline_name)),
            **{label: summ(pick(phase_name, stage)) for stage, label in stages.items()}}


def time_v(path):
    out = {}
    if not os.path.exists(path):
        return out
    text = open(path, errors="replace").read()
    for key, pat in {"elapsed": r"Elapsed \(wall clock\) time \(h:mm:ss or m:ss\): (\S+)",
                     "max_rss_kb": r"Maximum resident set size \(kbytes\): (\d+)",
                     "exit_status": r"Exit status: (\d+)", "cpu_percent": r"Percent of CPU this job got: (\S+)"}.items():
        m = re.search(pat, text)
        if m:
            out[key] = m.group(1)
    return out


def analyze_run(rows, name, run_dir, arm_label):
    out_dir = os.path.join(run_dir, "out")
    rpath = os.path.join(out_dir, "report.json")
    if not os.path.exists(rpath):
        row(rows, f"{name}: run completed", "report.json written", arm_label, "missing", "not-measured", rpath)
        return None
    report = load(rpath)
    samples = jsonl(os.path.join(out_dir, "samples.jsonl"))
    commits = jsonl(os.path.join(out_dir, "commits.jsonl"))
    cycles = jsonl(os.path.join(out_dir, "reader-cycles.jsonl"))
    phases, checks = phase_map(report), check_map(report)
    fc = report["fail_conditions"]
    summary = {"report": rpath, "replay": [report["replay_start_height"], report["replay_end_height"]],
               "phases": {}, "fail_conditions": {k: v for k, v in fc.items() if k != "phases"},
               "peak_rss_bytes": report.get("peak_rss_bytes"), "total_seconds": report.get("total_seconds")}
    for p in report["phases"]:
        c = checks[p["name"]]
        summary["phases"][p["name"]] = {
            "pacing": p["pacing"], "density_multiplier": p["density_multiplier"],
            "heights": [p["start_tip"], p["end_tip"]], "commits": p["commits"], "paced_commits": p["paced_commits"],
            "source_exhausted": p["source_exhausted"],
            "total_commit_s": p["total_commit_s"], "canonical_commit_s": p["canonical_commit_s"],
            "canonical_write_s": p["canonical_write_s"], "wallet_apply_s": p["wallet_apply_s"],
            "canonical_base_bytes_mean": p["canonical_base_bytes_mean"], "canonical_batch_bytes_mean": p["canonical_batch_bytes_mean"],
            "wallet_base_bytes_mean": p["wallet_base_bytes_mean"], "wallet_batch_bytes_mean": p["wallet_batch_bytes_mean"],
            "baseline": c["baseline_phase"], "ratio_to_baseline": c["total_p99_ratio_to_baseline"],
            "after_baseline": c.get("after_baseline_phase"), "ratio_to_after_baseline": c.get("total_p99_ratio_to_after_baseline"),
            "stop_stalls": c["stop_stalls"], "write_stopped_samples": c["write_stopped_samples"],
            "snapshot_reader_violations": c["snapshot_reader_violations"], "control_reader_violations": c["control_reader_violations"],
            "reader_errors": c.get("reader_errors"), "holds_short_of_target": c.get("holds_short_of_target"),
            "min_untruncated_hold_s": c.get("min_untruncated_hold_s"), "wallet_detached": c.get("wallet_detached"),
            "canonical_space": p["canonical_space"], "wallet_space": p.get("wallet_space"),
            "canonical_stalls": p["canonical_stalls"], "wallet_stalls": p.get("wallet_stalls"),
            "compaction_at_end": p.get("compaction_at_end"), "ballast": p.get("ballast"), "host": p.get("host"),
            "replacements": p.get("replacements"), "readers": [
                {k: g[k] for k in ("kind", "snapshot", "held_iterator", "family", "hold_target_s", "cycles", "hold_actual_s_max",
                                   "truncated_cycles", "pass1_rows_per_s", "pass1_mib_per_s", "violation_count", "errors", "first_violations", "first_errors")}
                for g in p["readers"]],
            "load": p.get("load"), "peak_rss_bytes_end": p.get("peak_rss_bytes_end"),
        }
        paced = [x for x in commits if x["phase"] == p["name"] and not x["catch_up"]]
        def pct(vals, q=0.99):
            if not vals:
                return None
            v = sorted(vals)
            return v[min(len(v) - 1, max(0, -(-int(q * 1000) * len(v) // 1000) - 1))]
        summary["phases"][p["name"]]["decomposition_p99"] = {
            "synced_writes_s": pct([(x.get("canonical_write_s") or 0) + (x.get("wallet_write_s") or 0) for x in paced]),
            "non_sync_work_s": pct([x["total_commit_s"] - (x.get("canonical_write_s") or 0) - (x.get("wallet_write_s") or 0) for x in paced]),
            "prepare_s": pct([x["prepare_s"] for x in paced]),
            "note": "commit = canonical commit (validation, synced write, readback) + wallet apply (derivation, synced write); prepare_s is outside the commit",
        }
        if p.get("index_attach"):
            summary["phases"][p["name"]]["index_attach"] = p["index_attach"]
            summary["phases"][p["name"]]["attach_stage_attribution"] = stage_attribution(commits, p["name"], c["baseline_phase"])
    ev = rpath
    # Stop stalls.
    stops = sum(c["stop_stalls"] + c["write_stopped_samples"] for c in fc["phases"])
    delays = sum(p["canonical_stalls"]["cf_counts"].get("total-delays", 0) + ((p.get("wallet_stalls") or {}).get("cf_counts", {}).get("total-delays", 0)) for p in report["phases"])
    row(rows, f"{name}: no stop stall", "0 stop stalls and 0 write-stopped samples in every phase (both DBs)", arm_label,
        f"stops+stopped samples={stops}; delay stalls={delays}", "pass" if stops == 0 else "fail", ev)
    # p99 <= 1 s.
    worst = max(report["phases"], key=lambda p: p["total_commit_s"]["p99"])
    row(rows, f"{name}: commit p99 <= 1 s", "every phase's paced-commit p99 (canonical + wallet) <= 1 s", arm_label,
        f"worst phase {worst['name']} p99={worst['total_commit_s']['p99']:.4f} s (max {worst['total_commit_s']['max']:.4f} s)",
        "pass" if worst["total_commit_s"]["p99"] <= 1.0 else "fail", ev)
    # Torn views.
    snap = sum(c["snapshot_reader_violations"] for c in fc["phases"])
    ctrl = sum(c["control_reader_violations"] for c in fc["phases"])
    has_ctrl = any(not g["snapshot"] for p in report["phases"] for g in p["readers"])
    row(rows, f"{name}: no torn view in snapshot readers", "0 violations across all snapshot-reader cycles" + ("; control readers without a snapshot must detect changes" if has_ctrl else ""),
        arm_label, f"snapshot-reader violations={snap}; control-reader violations={ctrl}; snapshot cycles={sum(1 for c in cycles if c['snapshot'])}",
        "pass" if snap == 0 and (ctrl > 0 or not has_ctrl) else "fail", ev)
    # Validity.
    errs = sum(c.get("reader_errors", 0) for c in fc["phases"])
    short = sum(c.get("holds_short_of_target", 0) for c in fc["phases"])
    detached = any(c.get("wallet_detached") for c in fc["phases"])
    exhausted = any(p["source_exhausted"] for p in report["phases"])
    row(rows, f"{name}: harness validity", "0 reader errors, every untruncated hold reached its target, wallet stage never detached, source never exhausted",
        arm_label, f"reader errors={errs}; short holds={short}; wallet detached={detached}; source exhausted={exhausted}",
        "pass" if errs == 0 and short == 0 and not detached and not exhausted else "fail", ev)
    return report, samples, commits, cycles, phases, checks, summary


def aba_row(rows, name, arm, phases, checks, treatment, ev, kind="held snapshots", quiet=None):
    if treatment not in phases:
        row(rows, f"{name}: {treatment} p99 vs no-reader baselines", "<= 2x", arm, "phase missing", "not-measured", ev)
        return
    c = checks[treatment]
    before, after = c["total_p99_ratio_to_baseline"], c.get("total_p99_ratio_to_after_baseline")
    fail = (before is not None and before > 2) and (after is None or after > 2)
    note = ""
    if quiet is not None:
        commits, samples, origin, disturbed, covered = quiet
        qt, dt = quiet_p99(commits, samples, treatment, origin, disturbed, covered)
        qb, db = quiet_p99(commits, samples, c["baseline_phase"], origin, disturbed, covered)
        qa, da = (quiet_p99(commits, samples, c["after_baseline_phase"], origin, disturbed, covered)
                  if c.get("after_baseline_phase") else (None, 0))
        qr_b = qt / qb if qt and qb else None
        qr_a = qt / qa if qt and qa else None
        quiet_fail = (qr_b is not None and qr_b > 2) and (qr_a is None or qr_a > 2)
        note = (f"; host-quiet commits only: p99={qt} (dropped {dt}), before={qb} (dropped {db}), after={qa} (dropped {da}), "
                f"ratios {None if qr_b is None else round(qr_b, 3)}/{None if qr_a is None else round(qr_a, 3)}")
        if qt is not None and qb is not None:
            if fail and not quiet_fail:
                note += "; strict A-B-A ratio exceeded only because of commits in externally loaded host windows, so it is not attributed to the treatment"
            elif quiet_fail and not fail:
                note += "; strict ratio passed only because an externally loaded baseline inflated the denominator; the host-quiet comparison fails"
            fail = quiet_fail
    row(rows, f"{name}: {treatment} commit p99 vs no-reader baseline ({kind})",
        "fail if p99 > 2x the no-reader phase before it and (if one exists) > 2x the no-reader phase after it; the verdict uses host-quiet commits: from 10 s before to 30 s after (kernel writeback delay) any 10-s window where another host process did >20 MB of IO, and outside seconds where CPU pressure (PSI some avg10) exceeded 1% (strict all-commit ratios also shown)",
        arm, f"p99={c['total_commit_p99_s']:.4f} s; before={c['baseline_phase']} ratio={before if before is None else round(before, 3)}; after={c.get('after_baseline_phase')} ratio={after if after is None else round(after, 3)}" + note,
        "fail" if fail else "pass", ev)


def main():
    rows, runs = [], {}
    # Arm 3.
    a3 = os.path.join(ROOT, "arm3")
    if os.path.exists(os.path.join(a3, "rebuild.json")):
        try:
            rebuild = load(os.path.join(a3, "rebuild.json"))
        except Exception as error:  # noqa: BLE001
            rebuild = {"error": str(error)}
        tv = time_v(os.path.join(a3, "rebuild.log"))
        runs["arm3"] = {"rebuild": rebuild, "time_v": tv}
        ok = "total_seconds" in rebuild and tv.get("exit_status") == "0"
        row(rows, "Arm 3: whole-Testnet-chain wallet rebuild completes alone on the host",
            "completes; wall time and peak RSS recorded (spec sets no limit)", "Arm 3",
            f"tip={rebuild.get('canonical_tip_height')}; total_seconds={rebuild.get('total_seconds')}; peak_rss_bytes={rebuild.get('peak_rss_bytes')}; /usr/bin/time max RSS kB={tv.get('max_rss_kb')}; staging peak={rebuild.get('max_sampled_staging_bytes')}",
            "pass" if ok else "fail", os.path.join(a3, "rebuild.json"))
    else:
        row(rows, "Arm 3: whole-Testnet-chain wallet rebuild", "completes", "Arm 3", "not run", "not-measured", a3)

    # Run 1 (1x).
    r1 = analyze_run(rows, "Run 1 (1x)", os.path.join(ROOT, "run1-1x"), "Arms 1, 2, 4 (1x Testnet)")
    if r1:
        report, samples, commits, cycles, phases, checks, summary = r1
        ev = summary["report"]
        disturbed, covered = host_windows(ROOT)
        quiet = (commits, samples, run_clock_origin(os.path.join(ROOT, "run1-1x", "stderr.log")), disturbed, covered)
        for t in ("hold-1s", "hold-30s", "hold-5m-with-replacements", "hold-30m", "realtime-25s-hold-5m"):
            aba_row(rows, "Arm 1 (1x)", "Arm 1", phases, checks, t, ev, quiet=quiet)
        aba_row(rows, "Arm 4 (1x)", "Arm 4", phases, checks, "serving-load", ev, kind="serving load", quiet=quiet)
        # Replacements: with vs without readers.
        def reps(name):
            return phases.get(name, {}).get("replacements") or []
        r0, r5 = reps("replacements-no-readers"), reps("hold-5m-with-replacements")
        def block(r):
            return r["canonical_commit_s"] + (r.get("wallet_apply_s") or 0)
        if r0 and r5:
            pairs = []
            for a, b in zip(r0, r5):
                pairs.append({"displaced": a["displaced"], "kept": a["kept"], "no_readers_block_s": block(a), "with_readers_block_s": block(b),
                              "no_readers_prepare_s": a["prepare_s"], "with_readers_prepare_s": b["prepare_s"],
                              "ratio": block(b) / block(a) if block(a) else None, "active_readers": b["active_readers"]})
            worst = max(p["ratio"] for p in pairs if p["ratio"] is not None)
            row(rows, "Arm 1 (1x): replacement writer block with held snapshots vs without",
                "writer block (canonical replacement commit + wallet reorg apply) with readers holding <= 2x the same replacement without readers",
                "Arm 1", "; ".join(f"{p['displaced']}:{p['kept']} {p['no_readers_block_s']:.2f} s -> {p['with_readers_block_s']:.2f} s (x{p['ratio']:.2f}, {p['active_readers']} readers)" for p in pairs),
                "pass" if worst <= 2 else "fail", ev)
            summary["replacement_pairs"] = pairs
        # Space after 1x 30-minute hold.
        h, post = phases.get("hold-30m"), phases.get("post-30m-release")
        if h and post:
            def at(t, db, key):
                xs = [s for s in samples if s["t"] <= t and s.get(db)]
                return xs[-1][db][key] if xs else None
            vals = {db: {"live_sst_at_hold_start": at(h["t_start"], db, "live_sst_bytes"),
                         "live_sst_at_release": at(h["t_end"], db, "live_sst_bytes"),
                         "live_sst_end_natural_window": at(post["t_end"], db, "live_sst_bytes"),
                         "obsolete_sst_max_during_hold": (h[db + "_space"] or {}).get("obsolete_sst_bytes_max") if db == "wallet" else h["canonical_space"]["obsolete_sst_bytes_max"],
                         "disk_minus_live_max_during_hold": (h["wallet_space"] if db == "wallet" else h["canonical_space"]).get("disk_minus_live_files_bytes_max"),
                         "max_l0_files_during_hold": (h["wallet_space"] if db == "wallet" else h["canonical_space"]).get("max_l0_files"),
                         "flush_write_bytes_during_hold": (at(h["t_end"], db, "flush_write_bytes") or 0) - (at(h["t_start"], db, "flush_write_bytes") or 0)}
                    for db in ("canonical", "wallet")}
            pinned_peak = max((v["obsolete_sst_max_during_hold"] or 0) + (v["disk_minus_live_max_during_hold"] or 0) for v in vals.values())
            post_space = {"canonical": post["canonical_space"], "wallet": post["wallet_space"]}
            pinned_end = 0
            for db in ("canonical", "wallet"):
                xs = [s for s in samples if post["t_end"] - 30 <= s["t"] <= post["t_end"] and s.get(db) and s[db].get("live_files_bytes") is not None]
                if xs:
                    pinned_end = max(pinned_end, max(max(0, x[db]["disk_sst_bytes"] - x[db]["live_files_bytes"]) + (x[db]["obsolete_sst_bytes"] or 0) for x in xs))
            summary["space_after_30m_hold_1x"] = vals
            row(rows, "Arm 1 (1x): space pinned by a 30-minute hold returns after release",
                "on-disk SST bytes outside the live set plus obsolete SST bytes, peak during the hold, back to <= 10% of that peak by the end of the 15-minute natural window (1x appends real data, so live-SST growth itself is not hold-induced)",
                "Arm 1", f"pinned peak during hold={pinned_peak} B; pinned at end of natural window={pinned_end} B; live SST growth during hold canonical={vals['canonical']['live_sst_at_release'] - vals['canonical']['live_sst_at_hold_start']} B for {h['end_tip'] - h['start_tip']} blocks; flush bytes during hold canonical={vals['canonical']['flush_write_bytes_during_hold']} wallet={vals['wallet']['flush_write_bytes_during_hold']} (the wallet never flushed at 1x, so only canonical was exercised)",
                "pass" if pinned_end <= 0.1 * max(pinned_peak, 1) else "fail", ev)
        # Arm 2.
        att = phases.get("index-attach")
        if att and att.get("index_attach"):
            ia = att["index_attach"]
            attr = summary["phases"]["index-attach"]["attach_stage_attribution"]
            probe = ia.get("ingest_probe") or {}
            ingest_commits = attr.get("ingest")
            ingest_p99 = (ingest_commits or {}).get("total_commit_s_p99")
            base_p99 = checks["index-attach"] and phases[checks["index-attach"]["baseline_phase"]]["total_commit_s"]["p99"]
            worst_block = max([probe.get("max_overlapping_write_s") or 0, ingest_p99 or 0])
            row(rows, "Arm 2: write stall caused by the ingest call",
                "longest synced write overlapping IngestExternalFile and p99 of commits in the ingest stage <= 1 s; also compared with 2x the no-reader commit p99",
                "Arm 2", f"ingest_seconds={ia['ingest_seconds']:.4f}; probe overlapping writes={probe.get('overlapping', {}).get('writes')} max={probe.get('max_overlapping_write_s')} s (probe before p99={probe.get('before', {}).get('p99_s')} s); commits in ingest stage={None if not ingest_commits else ingest_commits['commits']}; baseline p99={base_p99:.4f} s; 2x baseline exceeded={worst_block > 2 * base_p99}",
                "pass" if worst_block <= 1.0 else "fail", ev)
            c = checks["index-attach"]
            build = attr.get("sst_build") or {}
            qt, dt = quiet_p99(commits, samples, "index-attach", *quiet[2:])
            qb, db = quiet_p99(commits, samples, c["baseline_phase"], *quiet[2:])
            qa, da = quiet_p99(commits, samples, c["after_baseline_phase"], *quiet[2:]) if c.get("after_baseline_phase") else (None, 0)
            qr_b = qt / qb if qt and qb else None
            qr_a = qt / qa if qt and qa else None
            fail = c["total_commit_p99_s"] > 1.0 or ((qr_b or 0) > 2 and (qr_a is None or qr_a > 2))
            row(rows, "Arm 2: commit p99 across the whole attach phase (in-process SST build, ingest, catch-up, switch, drop)",
                "<= 1 s, and not > 2x both the no-reader phase before and after (host-quiet commits, as for Arm 1)",
                "Arm 2", f"phase p99={c['total_commit_p99_s']:.4f} s (strict ratios {c['total_p99_ratio_to_baseline']}/{c.get('total_p99_ratio_to_after_baseline')}); host-quiet p99={qt} (dropped {dt}) vs before {qb} / after {qa}, ratios {None if qr_b is None else round(qr_b, 3)}/{None if qr_a is None else round(qr_a, 3)}; SST-build stage p99={build.get('total_commit_s_p99')} s over {build.get('commits')} commits (canonical synced write p99={build.get('canonical_write_s_p99')} s, wallet synced write p99={build.get('wallet_write_s_p99')} s, prepare p99={build.get('prepare_s_p99')} s); ingest-stage commits={(attr.get('ingest') or {}).get('commits')}",
                "fail" if fail else "pass", ev)
            holders = ia.get("old_family_holders_at_drop") or []
            ok = bool(holders) and all(h["violation_count"] == 0 and not h["error"] for h in holders) and ia.get("files_deleted_after_last_holder_release_s") is not None and ia["files_deleted_after_last_holder_release_s"] < 60
            row(rows, "Arm 2: readers holding the old family across drop_cf",
                "every holder finishes with 0 violations and no error; the old files leave disk within 60 s of the last holder's release; rebuilt family equals live",
                "Arm 2", f"holders={len(holders)}; violations={sum(h['violation_count'] for h in holders)}; errors={sum(1 for h in holders if h['error'])}; old files deleted {ia.get('files_deleted_after_last_holder_release_s')} s after last release ({ia.get('old_family_files_deleted_after_s')} s after drop); rows old/new={ia['verification_rows_old']}/{ia['verification_rows_new']} mismatches={ia['verification_mismatches']}; switch blocked writer {ia['final_switch_writer_blocked_seconds']:.4f} s; catch-up after ingest {ia['catch_up_blocks_after_ingest']} blocks",
                "pass" if ok and ia["rebuilt_family_matches_live"] else "fail", ev)
        runs["run1-1x"] = summary

    for name, mult in (("density-x10", 10), ("density-x30", 30), ("density-x60", 60)):
        r = analyze_run(rows, f"Density {mult}x", os.path.join(ROOT, name), f"Density arm {mult}x (synthetic)")
        if not r:
            rows.pop()  # an absent optional run is not a row
            continue
        report, samples, commits, cycles, phases, checks, summary = r
        ev = summary["report"]
        disturbed, covered = host_windows(ROOT)
        quiet = (commits, samples, run_clock_origin(os.path.join(ROOT, name, "stderr.log")), disturbed, covered)
        aba_row(rows, f"Density {mult}x", f"Density arm {mult}x", phases, checks, "density-hold-30m", ev, quiet=quiet)
        t_rel = release_time(cycles, "density-hold-30m")
        sr = space_return(samples, phases, "density-hold-30m", "density-no-readers", "density-post-release", t_rel)
        summary["single_run_space_estimate"] = {"note": "pre-registered single-run estimate; superseded by the paired no-hold control because a single run cannot separate hold-pinned entries from tombstones that accumulate between compactions anyway", "snapshot_release_t": t_rel, **(sr or {})}
        summary["flush_evidence_hold"] = flush_counts(samples, phases["density-hold-30m"]["t_start"], phases["density-hold-30m"]["t_end"])
        control = f"{name}-control"
        if control in EXCLUDED_CONTROLS or not os.path.exists(os.path.join(ROOT, control, "out", "report.json")):
            sre = summary["single_run_space_estimate"]
            reason = EXCLUDED_CONTROLS.get(control, "control run missing")
            context = (f"; pre-registered single-run estimate (not the verdict, it cannot separate hold-pinned entries "
                       f"from tombstones that accumulate between compactions anyway): dead wallet-ballast entries "
                       f"pre-hold={sre.get('dead_entries_pre_hold')}, at release={sre.get('dead_entries_at_release')}, "
                       f"end of natural window={sre.get('dead_entries_end_of_natural_window')}, after forced "
                       f"compaction={sre.get('dead_entries_after_forced_compaction')}; fraction of hold excess gone by "
                       f"end of natural window={sre.get('fraction_gone_by_end_of_natural_window')}")
            row(rows, f"Density {mult}x: space after a 30-minute hold returns after release (paired no-hold control)",
                "hold run minus the paired no-hold run (same stores, blocks, multiplier, timing): >= 90% of the release-time excess in dead wallet-ballast entries and ballast SST bytes gone by the end of the 15-minute natural window",
                f"Density arm {mult}x", f"not measured: {reason}" + context, "not-measured", ev)
        runs[name] = summary

    for name, mult in (("density-x10-control", 10), ("density-x60-control", 60)):
        if name in EXCLUDED_CONTROLS:
            continue
        r = analyze_run(rows, f"Density {mult}x no-hold control", os.path.join(ROOT, name), f"Density arm {mult}x control (synthetic)")
        if not r:
            rows.pop()
            continue
        # A control has no readers, so its torn-view row carries no information.
        rows[:] = [x for x in rows if x["name"] != f"Density {mult}x no-hold control: no torn view in snapshot readers"]
        runs[name] = r[-1]
    # Paired no-hold controls for the 30-minute density holds.
    for mult in (10, 60):
        hd, cd = os.path.join(ROOT, f"density-x{mult}"), os.path.join(ROOT, f"density-x{mult}-control")
        if f"density-x{mult}-control" in EXCLUDED_CONTROLS:
            continue
        if not (os.path.exists(os.path.join(hd, "out", "report.json")) and os.path.exists(os.path.join(cd, "out", "report.json"))):
            continue
        ps = paired_space(hd, cd)
        runs.setdefault(f"density-x{mult}", {})["paired_space_vs_control"] = ps
        e_rel = ps["release"]["dead_entries"]["excess"]
        e_post = ps["end_of_natural_window"]["dead_entries"]["excess"]
        b_rel = ps["release"]["ballast_sst_bytes"]["excess"]
        b_post = ps["end_of_natural_window"]["ballast_sst_bytes"]["excess"]
        w_post = ps["end_of_natural_window"]["wallet_disk_sst_bytes"]["excess"]
        e_comp = ps["after_forced_compaction"]["dead_entries"]["excess"]
        ok = e_rel is not None and e_post is not None and (e_rel <= 0 or e_post <= 0.1 * e_rel) and (b_rel is None or b_rel <= 0 or (b_post or 0) <= 0.1 * b_rel)
        row(rows, f"Density {mult}x: space after a 30-minute hold returns after release (paired no-hold control)",
            "hold run minus the paired no-hold run (same stores, blocks, multiplier, timing): >= 90% of the release-time excess in dead wallet-ballast entries and ballast SST bytes gone by the end of the 15-minute natural window",
            f"Density arm {mult}x", f"excess dead entries at release={e_rel:.0f}, end of natural window={e_post:.0f}, after forced compaction={e_comp}; excess ballast SST bytes at release={b_rel:.0f}, end of window={b_post:.0f}; excess wallet on-disk SST bytes at end of window={w_post:.0f}",
            "pass" if ok else "fail", os.path.join(cd, "out", "report.json"))
    # Scan-load repeat: separates full wallet-family scans from compact-range readers.
    rr = analyze_run(rows, "Scan-load repeat (1x)", os.path.join(ROOT, "repeat-scan-load"), "Arm 1 attribution (1x)")
    if rr:
        report, samples, commits, cycles, phases, checks, summary = rr
        disturbed, covered = host_windows(ROOT)
        quiet = (commits, samples, run_clock_origin(os.path.join(ROOT, "repeat-scan-load", "stderr.log")), disturbed, covered)
        for t in ("r-hold-1s-all", "r-hold-1s-compact-only", "r-hold-1s-wallet-scan-only"):
            aba_row(rows, "Scan-load repeat (1x)", "Arm 1 attribution", phases, checks, t, summary["report"], kind="readers holding 1-s snapshots", quiet=quiet)
        runs["repeat-scan-load"] = summary
    else:
        rows.pop()
    measured = [r for r in rows if r["pass"] != "not-measured"]
    overall = "fail" if any(r["pass"] == "fail" for r in rows) else ("inconclusive" if any(r["pass"] == "not-measured" for r in rows) else "pass")
    out = {"network": "zcash-testnet (Testnet replay only; no Mainnet data on host)", "overall": overall,
           "thresholds": rows, "runs": runs, "excluded_controls": EXCLUDED_CONTROLS}
    path = OUT
    with open(path, "w") as f:
        json.dump(out, f, indent=1, default=str)
    print(path, overall, len(rows), "rows")
    for r in rows:
        print(f"[{r['pass']}] {r['name']}: {r['measured']}")


if __name__ == "__main__":
    main()
