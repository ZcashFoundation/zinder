#!/usr/bin/env python3
"""Spike B final report (Testnet). Reads results.json written by analyze.py plus the run
logs, and adds a verdict for each fail condition of the spike spec:

1. any stop stall;
2. commit p99 above 1 s, or above 2x the no-reader baseline attributable to held
   snapshots or to the ingest;
3. space growth from a 30-minute snapshot hold that does not return after release;
4. a reader observing a torn view.

The overall verdict is fail if any condition fails, else inconclusive if any is not
measured, else pass. Usage: final_report.py [results_dir] (rewrites results_dir/results.json).
"""
import datetime, json, os, re, statistics, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import analyze as A  # noqa: E402

ROOT = sys.argv[1] if len(sys.argv) > 1 else A.ROOT
RESULTS = os.path.join(ROOT, "results.json")
RUN_PIDS = {"run1-1x": "26331", "density-x10": "2811724", "density-x60": "3566238",
            "repeat-scan-load": "152786", "density-x60-control": "822696"}
MEASURED_RUNS = ("run1-1x", "density-x10", "density-x60", "repeat-scan-load")


def path(*parts):
    return os.path.join(ROOT, *parts)


def report(run):
    return A.load(path(run, "out", "report.json"))


def ts(t):
    return datetime.datetime.fromtimestamp(t, datetime.timezone.utc).strftime("%H:%M:%S")


def host_io_lines():
    out = []
    for line in open(path("host-io.log")):
        stamp = line.split()[0].rstrip("Z")
        try:
            t = datetime.datetime.fromisoformat(stamp).replace(tzinfo=datetime.timezone.utc).timestamp()
        except ValueError:
            continue
        procs = re.findall(r"(\S+)\[(\d+)\] \+(\d+)MB", line)
        out.append((t, procs))
    return out


def phase_io(io, run):
    """Own and external IO per phase from host-io.log (10-s samples of /proc/<pid>/io deltas)."""
    rep = report(run)
    origin = A.run_clock_origin(path(run, "stderr.log"))
    pid = RUN_PIDS[run]
    rows = []
    for p in rep["phases"]:
        t0, t1 = origin + p["t_start"], origin + p["t_end"]
        own, ext, windows = [], 0, 0
        for t, procs in io:
            if t0 + 10 <= t <= t1:
                own.append(sum(int(mb) for name, pd, mb in procs if pd == pid))
                e = sum(int(mb) for name, pd, mb in procs if name not in A.OWN_PROCESSES)
                ext += e
                windows += e > 0
        rows.append({"run": run, "phase": p["name"], "utc": f"{ts(t0)}-{ts(t1)}",
                     "own_io_mb_per_s_median": statistics.median(own) / 10 if own else None,
                     "external_io_mb": ext, "external_io_windows": f"{windows}/{len(own)}",
                     "paced_commit_p99_s": p["total_commit_s"]["p99"]})
    return rows


def quiet(run, phase_name):
    rep = report(run)
    commits = A.jsonl(path(run, "out", "commits.jsonl"))
    samples = A.jsonl(path(run, "out", "samples.jsonl"))
    disturbed, covered = A.host_windows(ROOT)
    origin = A.run_clock_origin(path(run, "stderr.log"))
    return A.quiet_p99(commits, samples, phase_name, origin, disturbed, covered)


def rows_by_name(results):
    return {r["name"]: r for r in results["thresholds"]}


def build_stage_slow_commits(io, threshold_s):
    """Classifies SST-build-stage commits above threshold_s as synced-write or other dominated."""
    origin = A.run_clock_origin(path("run1-1x", "stderr.log"))
    own = [(t, sum(int(mb) for name, pd, mb in procs if pd == RUN_PIDS["run1-1x"])) for t, procs in io]
    commits = [c for c in A.jsonl(path("run1-1x", "out", "commits.jsonl"))
               if c["phase"] == "index-attach" and not c["catch_up"] and c["attach_stage"] == 1]
    slow = [c for c in commits if c["total_commit_s"] > threshold_s]
    sync = [c for c in slow if c["canonical_write_s"] + (c.get("wallet_write_s") or 0) >= 0.5 * c["total_commit_s"]]
    other = [c for c in slow if c not in sync]
    def burst(c):
        t = origin + c["t"]
        return max([mb for tt, mb in own if t - 40 <= tt <= t + 10] or [0])
    return {"build_commits": len(commits), "threshold_s": threshold_s, "slow": len(slow),
            "synced_write_dominated": len(sync),
            "synced_write_dominated_min_own_io_burst_mb_per_10s": min((burst(c) for c in sync), default=None),
            "synced_write_max_s": max((max(c["canonical_write_s"], c.get("wallet_write_s") or 0) for c in sync), default=None),
            "other": len(other),
            "other_window_utc": [ts(origin + min(c["t"] for c in other)), ts(origin + max(c["t"] for c in other))] if other else None,
            "other_wallet_apply_median_s": statistics.median(c["wallet_apply_s"] for c in other) if other else None}


def fc1_stop_stalls(results):
    names = [n for n in rows_by_name(results) if n.endswith(": no stop stall")]
    rows = [rows_by_name(results)[n] for n in names]
    ok = all(r["pass"] == "pass" for r in rows)
    return {"id": 1, "condition": "any stop stall", "verdict": "pass" if ok else "fail",
            "measured": "; ".join(f"{r['name'].split(':')[0]}: {r['measured']}" for r in rows),
            "arm": "Arms 1, 2, 4 (1x), density 10x and 60x (synthetic), scan-load repeat (1x)",
            "evidencePaths": [r["evidencePath"] for r in rows]}


def fc2_commit_latency(results, io_rows, io):
    by = rows_by_name(results)
    phases = [(run, p) for run in MEASURED_RUNS for p in report(run)["phases"]]
    worst_run, worst = max(phases, key=lambda rp: rp[1]["total_commit_s"]["p99"])
    reps = report("run1-1x")
    rep_pairs = results["runs"]["run1-1x"].get("replacement_pairs", [])
    over_1s = worst["total_commit_s"]["p99"] > 1.0
    sub = []
    sub.append({"check": "paced-commit p99 <= 1 s in every phase of every run",
                "verdict": "fail" if over_1s else "pass",
                "measured": f"worst phase {worst_run}/{worst['name']} p99={worst['total_commit_s']['p99']:.4f} s (max single commit {worst['total_commit_s']['max']:.4f} s)",
                "evidencePath": path(worst_run, "out", "report.json")})

    # Held snapshots: every A-B-A treatment row; a raw fail is not attributed to the hold when
    # a repeat of the same readers passes strict and host-quiet A-B-A with no external IO.
    hold_rows = [r for n, r in by.items() if "commit p99 vs no-reader baseline" in n and "serving load" not in n]
    io_by_phase = {(x["run"], x["phase"]): x for x in io_rows}
    attributions = []
    for r in hold_rows:
        if r["pass"] != "fail":
            continue
        if r["name"].startswith("Arm 1 (1x): hold-1s "):
            orig, rep = io_by_phase[("run1-1x", "hold-1s")], io_by_phase[("repeat-scan-load", "r-hold-1s-all")]
            rep_row = by["Scan-load repeat (1x): r-hold-1s-all commit p99 vs no-reader baseline (readers holding 1-s snapshots)"]
            rq, rq_drop = quiet("repeat-scan-load", "r-hold-1s-all")
            oq, oq_drop = quiet("run1-1x", "hold-1s")
            loaded_baselines = [x for x in io_rows if x["phase"] in ("r-baseline-c", "replacements-no-readers")]
            same_load = rep["own_io_mb_per_s_median"] >= 0.9 * orig["own_io_mb_per_s_median"]
            clean = rep["external_io_mb"] == 0 and rep_row["pass"] == "pass" and rq_drop == 0
            attributed_to_hold = not (same_load and clean)
            attributions.append({
                "row": r["name"], "raw": "fail", "attributed_to_held_snapshots": attributed_to_hold,
                "evidence": (f"Run 1 hold-1s: own IO {orig['own_io_mb_per_s_median']:.0f} MB/s, other sessions' IO {orig['external_io_mb']} MB "
                             f"in {orig['external_io_windows']} 10-s windows, p99 {orig['paced_commit_p99_s']:.4f} s, host-quiet p99 {oq} s on "
                             f"{600 - oq_drop} of 600 commits. Repeat r-hold-1s-all (same readers): own IO {rep['own_io_mb_per_s_median']:.0f} MB/s, "
                             f"other sessions' IO {rep['external_io_mb']} MB, p99 {rep['paced_commit_p99_s']:.4f} s, host-quiet p99 {rq} s on all 600 "
                             f"commits; {rep_row['measured']}. No-reader phases that overlapped other sessions' IO: "
                             + "; ".join(f"{x['run']}/{x['phase']} p99 {x['paced_commit_p99_s']:.4f} s with {x['external_io_mb']} MB" for x in loaded_baselines)),
                "evidencePaths": [path("run1-1x", "out", "commits.jsonl"), path("repeat-scan-load", "out", "report.json"), path("host-io.log")]})
        else:
            attributions.append({"row": r["name"], "raw": "fail", "attributed_to_held_snapshots": True,
                                 "evidence": "no repeat of this treatment exists", "evidencePaths": [r["evidencePath"]]})
    hold_fail = any(a["attributed_to_held_snapshots"] for a in attributions)
    sub.append({"check": "held snapshots: p99 not > 2x both adjacent no-reader baselines (host-quiet), Arm 1 at 1x, density 10x and 60x, scan-load repeat",
                "verdict": "fail" if hold_fail else "pass",
                "measured": "; ".join(f"{r['name']}: {r['pass']}" for r in hold_rows),
                "attribution": attributions,
                "evidencePath": path("run1-1x", "out", "report.json")})

    rep_txt = "; ".join(f"{p['displaced']}:{p['kept']} writer block {p['no_readers_block_s']:.2f} s without readers, {p['with_readers_block_s']:.2f} s with {p['active_readers']} readers holding (x{p['ratio']:.2f})" for p in rep_pairs)
    sub.append({"check": "1000-block replacement writer block with held snapshots vs without (outside the per-commit percentiles)",
                "verdict": by["Arm 1 (1x): replacement writer block with held snapshots vs without"]["pass"],
                "measured": rep_txt, "evidencePath": path("run1-1x", "out", "report.json")})

    # Ingest: the IngestExternalFile call, and the whole attach phase (Arm 2 treatment).
    ing = by["Arm 2: write stall caused by the ingest call"]
    att = by["Arm 2: commit p99 across the whole attach phase (in-process SST build, ingest, catch-up, switch, drop)"]
    attr = results["runs"]["run1-1x"]["phases"]["index-attach"]["attach_stage_attribution"]
    ia = results["runs"]["run1-1x"]["phases"]["index-attach"]["index_attach"]
    aq, aq_drop = quiet("run1-1x", "index-attach")
    baselines = {}
    for run, names in (("run1-1x", ("baseline-no-readers", "post-30m-release", "load-baseline-no-readers", "load-baseline-after")),
                       ("repeat-scan-load", ("r-baseline-a", "r-baseline-b", "r-baseline-c", "r-baseline-d"))):
        for n in names:
            q, d = quiet(run, n)
            baselines[f"{run}/{n}"] = {"host_quiet_p99_s": q, "dropped": d, "attach_ratio": round(aq / q, 3) if q else None}
    min_ratio = min(b["attach_ratio"] for b in baselines.values() if b["attach_ratio"])
    build = attr["sst_build"]
    after_q = baselines["run1-1x/load-baseline-no-readers"]["host_quiet_p99_s"]
    slow = build_stage_slow_commits(io, 2 * after_q)
    probe = ia.get("ingest_probe") or {}
    sub.append({"check": "ingest call: longest write overlapping IngestExternalFile and ingest-stage commit p99 <= 1 s and <= 2x baseline",
                "verdict": ing["pass"], "measured": ing["measured"], "evidencePath": ing["evidencePath"]})
    sub.append({"check": "index attach (Arm 2 treatment: in-process SST build from canonical through a held canonical snapshot, ingest, catch-up, switch, drop): phase p99 not > 2x both adjacent no-reader baselines (host-quiet)",
                "verdict": att["pass"],
                "measured": (f"{att['measured']}. Host-quiet attach p99 {aq} s ({aq_drop} commits dropped, no other-session IO in the phase) is "
                             f"{min_ratio}x to {max(b['attach_ratio'] for b in baselines.values() if b['attach_ratio'])}x every host-quiet "
                             f"no-reader 1x phase on disk. Per stage: SST build {build['commits']} commits p99 {build['total_commit_s_p99']:.4f} s "
                             f"(canonical synced write p99 {build['canonical_write_s_p99']:.4f} s, wallet synced write p99 {build['wallet_write_s_p99']:.4f} s, "
                             f"against {attr['idle']['canonical_write_s_p99']:.4f} s and {attr['idle']['wallet_write_s_p99']:.4f} s before the build); "
                             f"ingest stage {attr['ingest']['commits']} commits p99 {attr['ingest']['total_commit_s_p99']:.4f} s; drop stage "
                             f"{attr['drop_and_wait_for_files']['commits']} commits p99 {attr['drop_and_wait_for_files']['total_commit_s_p99']:.4f} s. "
                             f"SST build {ia['sst_build_seconds']:.0f} s wrote {ia['sst_all_families_file_bytes']} B of SSTs for all wallet families; "
                             f"ingest {ia['ingest_seconds'] * 1000:.1f} ms"),
                "attribution": (f"The excess is in the SST-build stage, not the IngestExternalFile call. Of {slow['build_commits']} build-stage commits, "
                                f"{slow['slow']} exceeded 2x the adjacent baseline p99 ({slow['threshold_s']:.4f} s): {slow['synced_write_dominated']} were "
                                f"dominated by synced writes (up to {slow['synced_write_max_s']:.3f} s), each within 40 s after this process did at least "
                                f"{slow['synced_write_dominated_min_own_io_burst_mb_per_10s']} MB of IO in a 10-s sample; {slow['other']} fell in "
                                f"{slow['other_window_utc'][0]}-{slow['other_window_utc'][1]} UTC with wallet apply at a median "
                                f"{slow['other_wallet_apply_median_s'] * 1000:.0f} ms and no slower synced write (cause not isolated). The stage holds one "
                                "canonical snapshot for the whole build, and the harness cannot separate that hold from the build's IO and CPU; Arm 1 "
                                "holds of 30 s to 30 min alone show no excess. The spec defines Arm 2 as building the SSTs and ingesting while the writer "
                                "commits, and the pre-registered criterion counts the whole attach phase, so this is counted as a trip of condition 2."),
                "build_stage_slow_commits": slow,
                "baselines": baselines,
                "evidencePath": att["evidencePath"]})
    verdict = "fail" if any(s["verdict"] == "fail" for s in sub) else "pass"
    return {"id": 2, "condition": "commit p99 above 1 s or above 2 times the no-reader baseline attributable to held snapshots or to the ingest",
            "verdict": verdict,
            "measured": (f"worst phase p99 {worst['total_commit_s']['p99']:.4f} s ({worst_run}/{worst['name']}); held snapshots: "
                         + ("no attributable excess at 1x, 10x, or 60x" if not hold_fail else "attributable excess found")
                         + f"; ingest call: {ia['ingest_seconds'] * 1000:.1f} ms, longest overlapping write {probe.get('max_overlapping_write_s', 0) * 1000:.1f} ms; "
                         f"index attach phase: host-quiet p99 {aq:.4f} s, {min_ratio}x or more of every host-quiet no-reader 1x baseline, "
                         f"driven by the in-process SST build stage (p99 {build['total_commit_s_p99']:.4f} s)"),
            "summary_numbers": {"attach_quiet_p99_s": aq, "attach_min_ratio": min_ratio, "build_stage_p99_s": build["total_commit_s_p99"],
                                "ingest_seconds": ia["ingest_seconds"], "worst_phase_p99_s": worst["total_commit_s"]["p99"]},
            "arm": "Arm 1, Arm 2, Arm 4 (1x Testnet); density 10x and 60x (synthetic); scan-load repeat",
            "subchecks": sub,
            "evidencePaths": [path("run1-1x", "out", "report.json"), path("run1-1x", "out", "commits.jsonl"), path("repeat-scan-load", "out", "report.json"), path("host-io.log")]}


def l0_compaction_tombstones(run):
    """sst_deletions in the wallet ballast family right after an L0 compaction empties L0."""
    out, prev = [], None
    ends = {p["name"]: p["t_end"] for p in report(run)["phases"]}
    for s in A.jsonl(path(run, "out", "samples.jsonl")):
        b = (s.get("wallet") or {}).get("ballast")
        # Samples after a phase's t_end belong to the forced compaction at the end of the run.
        if not b or s["t"] > ends.get(s["phase"], float("inf")):
            continue
        if prev is not None and prev > 0 and b["l0_files"] == 0:
            out.append({"t": round(s["t"]), "phase": s["phase"], "snapshots": s["wallet"]["num_snapshots"],
                        "ballast_sst_bytes": b["sst_bytes"], "tombstones_left": b["sst_deletions"],
                        "dead_entries": s.get("wallet_ballast_dead_entries")})
        prev = b["l0_files"]
    return out


def dead_band(run, phase_name, skip_s=600):
    rep = {p["name"]: p for p in report(run)["phases"]}
    p = rep[phase_name]
    xs = [s["wallet_ballast_dead_entries"] for s in A.jsonl(path(run, "out", "samples.jsonl"))
          if s.get("wallet_ballast_dead_entries") is not None and p["t_start"] + skip_s <= s["t"] <= p["t_end"]]
    return [min(xs), max(xs)] if xs else None


def fc3_space(results):
    by = rows_by_name(results)
    one = by["Arm 1 (1x): space pinned by a 30-minute hold returns after release"]
    dens = {}
    for run in ("density-x10", "density-x60"):
        r = results["runs"][run]
        sre = r["single_run_space_estimate"]
        comp = r["phases"]["density-post-release"]["compaction_at_end"]
        tomb = l0_compaction_tombstones(run)
        dens[run] = {
            "paired_control_threshold": by[f"Density {run.split('x')[1]}x: space after a 30-minute hold returns after release (paired no-hold control)"]["pass"],
            "no_reader_dead_entry_band_after_600s": dead_band(run, "density-no-readers"),
            "dead_entries_pre_hold": sre["dead_entries_pre_hold"], "dead_entries_at_release": sre["dead_entries_at_release"],
            "dead_entries_max_during_hold": sre["dead_entries_max_during_hold"],
            "dead_entries_end_of_natural_window": sre["dead_entries_end_of_natural_window"],
            "dead_entries_after_forced_compaction": sre["dead_entries_after_forced_compaction"],
            "single_run_fraction_gone_by_end_of_natural_window": sre["fraction_gone_by_end_of_natural_window"],
            "hold_induced_excess_bytes_approx": sre["hold_induced_excess_bytes_approx"],
            "wallet_live_sst_reclaimed_by_forced_compaction_bytes": comp["wallet_live_sst_before"] - comp["wallet_live_sst_after"],
            "wallet_pending_compaction_bytes_max_by_phase": {k: v["wallet_space"]["pending_compaction_bytes_max"] for k, v in r["phases"].items()},
            "tombstones_left_after_l0_compactions": tomb,
            "flush_evidence_hold": r["flush_evidence_hold"],
        }
    x10, x60 = dens["density-x10"], dens["density-x60"]
    post_end = {p["name"]: p["t_end"] for p in report("density-x10")["phases"]}["density-post-release"]
    post10 = [s for s in A.jsonl(path("density-x10", "out", "samples.jsonl"))
              if s["phase"] == "density-post-release" and s["t"] <= post_end and (s.get("wallet") or {}).get("ballast")]
    grew_from, grew_to = x10["flush_evidence_hold"]["ballast_sst_bytes_start"], post10[-1]["wallet"]["ballast"]["sst_bytes"]
    sre60 = results["runs"]["density-x60"]["single_run_space_estimate"]
    def band60(phase, snapshots=0, min_t=0):
        xs = [c["dead_entries"] for c in x60["tombstones_left_after_l0_compactions"]
              if c["phase"] == phase and c["snapshots"] == snapshots and c["t"] >= min_t]
        return f"{min(xs)} to {max(xs)}" if xs else "none"
    before = [c["tombstones_left"] for c in x10["tombstones_left_after_l0_compactions"] if c["phase"] == "density-no-readers"]
    after = [c["tombstones_left"] for c in x10["tombstones_left_after_l0_compactions"] if c["phase"] == "density-post-release" and c["snapshots"] == 0]
    note = (f"10x: before the hold every L0 compaction left {max(before) if before else None} tombstones in the ballast family and dead entries near zero; after release, "
            f"with no snapshot open, L0 compactions left {after} tombstones, so compaction behavior changed as the family grew "
            f"({grew_from / 1e6:.0f} MB at the hold start to {grew_to / 1e6:.0f} MB) independently of the hold, and a single run cannot attribute the residual to the hold")
    return {"id": 3, "condition": "space growth from a 30-minute snapshot hold that does not return after release",
            "verdict": "not-measured",
            "measured": (f"1x (Run 1): {one['measured']}. 60x: dead wallet-ballast entries {dens['density-x60']['dead_entries_pre_hold']:.0f} before the hold, "
                         f"{dens['density-x60']['dead_entries_at_release']:.0f} at release, {dens['density-x60']['dead_entries_end_of_natural_window']:.0f} at the end of the 15-min "
                         f"natural window (no-reader band {dens['density-x60']['no_reader_dead_entry_band_after_600s']}); 10x: "
                         f"{x10['dead_entries_pre_hold']:.0f}, {x10['dead_entries_at_release']:.0f}, {x10['dead_entries_end_of_natural_window']:.0f} "
                         f"(no-reader band {x10['no_reader_dead_entry_band_after_600s']}), 0 after forced compaction, which reclaimed "
                         f"{x10['wallet_live_sst_reclaimed_by_forced_compaction_bytes']} B of wallet live SST. The paired no-hold controls that decide "
                         "the density verdict were cancelled (see cancelled_controls), so the condition is not measured where the wallet side churns"),
            "arm": "Arm 1 (1x Testnet) and density arm 10x and 60x (synthetic)",
            "notes": [note,
                      "1x exercises only the canonical side: the wallet never flushed during the 30-minute hold (wallet flush bytes 0), so its clean result can barely fail",
                      (f"60x single-run evidence: dead entries rose from {x60['dead_entries_pre_hold']:.0f} to {x60['dead_entries_at_release']:.0f} during the hold "
                       f"(excess {sre60['hold_induced_excess_entries']:.0f} entries, about {sre60['hold_induced_excess_bytes_approx'] / 1e6:.0f} MB, after the no-reader drift) and fell back "
                       f"inside the no-reader band by the end of the natural window (fraction gone {sre60['fraction_gone_by_end_of_natural_window']:.3f}); "
                       f"dead entries right after each L0 compaction were {band60('density-no-readers', min_t=600)} before the hold (after a 600-s warm-up), rose to {band60('density-hold-30m', 2)} "
                       f"while snapshots were held, and were {band60('density-post-release')} after release; this supports return but is not the pre-registered paired comparison"),
                      (f"10x single-run evidence: {x10['single_run_fraction_gone_by_end_of_natural_window']:.3f} of the estimated excess was gone after 15 minutes of natural "
                       "compaction and all of it after forced compaction; under the pre-registered single-run rule this would fail, and it is unresolved without the control")],
            "density": dens,
            "evidencePaths": [one["evidencePath"], path("density-x10", "out", "samples.jsonl"), path("density-x60", "out", "samples.jsonl"),
                              path("density-x10", "out", "report.json"), path("density-x60", "out", "report.json")]}


def fc4_torn_view(results):
    by = rows_by_name(results)
    rows = [r for n, r in by.items() if n.endswith(": no torn view in snapshot readers")]
    cycles = A.jsonl(path("run1-1x", "out", "reader-cycles.jsonl"))
    rep = {p["name"]: p for p in report("run1-1x")["phases"]}
    spans = []
    for r in rep["hold-5m-with-replacements"]["replacements"]:
        cs = [c for c in cycles if c["phase"] == "hold-5m-with-replacements" and c["t_start"] <= r["t"] <= c["t_end"]]
        spans.append({"replacement": f"{r['displaced']}:{r['kept']}", "t": round(r["t"], 1),
                      "snapshot_cycles_spanning": sum(1 for c in cs if c["snapshot"]),
                      "snapshot_violations": sum(c["violation_count"] for c in cs if c["snapshot"]),
                      "control_cycles_spanning": sum(1 for c in cs if not c["snapshot"]),
                      "control_violations": sum(c["violation_count"] for c in cs if not c["snapshot"])})
    holders = by["Arm 2: readers holding the old family across drop_cf"]
    ok = all(r["pass"] == "pass" for r in rows) and all(s["snapshot_violations"] == 0 for s in spans) and holders["pass"] == "pass"
    return {"id": 4, "condition": "a reader observing a torn view", "verdict": "pass" if ok else "fail",
            "measured": "; ".join(f"{r['name'].split(':')[0]}: {r['measured']}" for r in rows)
                        + "; across the 1000-block replacements: " + "; ".join(f"{s['replacement']} spanned by {s['snapshot_cycles_spanning']} snapshot cycles with {s['snapshot_violations']} violations ({s['control_cycles_spanning']} control cycles, {s['control_violations']} violations)" for s in spans)
                        + f"; old-family holders across drop_cf: {holders['measured']}",
            "arm": "Arm 1 and Arm 2 (1x Testnet), density 10x and 60x, scan-load repeat",
            "notes": ["torn views between canonical and wallet rows are not tested: the two stores are still separate databases and the wallet commits its own synced batch after canonical"],
            "replacement_spans": spans,
            "evidencePaths": [r["evidencePath"] for r in rows] + [path("run1-1x", "out", "reader-cycles.jsonl")]}


def arm3(results):
    rb = results["runs"]["arm3"]["rebuild"]
    deriv = A.load(path("density-derivation.json"))
    lo, hi = deriv["mainnet_targets"]["whole_chain_average_by_transparent_outpoint_volume"]["whole_chain_ratio_range"]
    t = rb["total_seconds"]
    return {
        "label": "Testnet measurement; the Mainnet figures are an extrapolation, not a measurement",
        "testnet": {"tip": rb["canonical_tip_height"], "total_seconds": t, "peak_rss_bytes": rb["peak_rss_bytes"],
                    "max_sampled_staging_bytes": rb["max_sampled_staging_bytes"], "wallet_store_bytes": rb["wallet_store_bytes"],
                    "stages_s": {k: rb[k] for k in ("canonical_scan_seconds", "outpoint_merge_seconds", "secondary_row_derivation_seconds", "cold_validation_seconds")},
                    "outpoint_sort": rb["outpoint_sort"]},
        "method": ("Scale total wall time linearly by the ratio of whole-chain transparent outpoint volume, measured as outpoint sorter bytes: "
                   "Testnet 2,600,446,123 B at the rebuilt tip against about 99 GB for a late-2026 Mainnet build (docs/runbooks/initial-sync.md:129-130); "
                   f"the ratio is {lo}x to {hi}x depending on whether the 99 GB counts the merge's double footprint (density-derivation.json)"),
        "mainnet_wall_time_estimate_hours": [round(t * lo / 3600, 1), round(t * hi / 3600, 1)],
        "mainnet_anchor": "one measured Mainnet wallet construction from genesis took more than two hours (docs/runbooks/deploying-wallet-serving.md:327-329); the linear estimate sits above that, so treat it as an upper-leaning range",
        "rss": ("not extrapolated from chain size: the Testnet outpoint sorter fit in one run of 2.79 GB under this bench's 4 GiB outpoint budget "
                "(secondary sorters 1 GiB each); a Mainnet build fills the budget and spills, so peak RSS is set by the sorter budgets, not measured here"),
        "staging": "Testnet staging peaked at 6.06 GB; the runbook reports about 117 GB on Mainnet (docs/runbooks/initial-sync.md:136-138)",
        "caveats": ["Mainnet's outpoint sorter needs several runs and a merge pass; Testnet needed one run and no merge, so merge IO is absent from the Testnet time",
                    "this bench sets a 64 GiB per-sorter spill ceiling (services/zinder-bench/src/rocksdb_wallet_rebuild.rs:26), below the about 99 GB a Mainnet outpoint sorter needs, so this exact configuration would stop on Mainnet",
                    "Arm 3 ran 01:58:48-02:09:51 UTC, before host-io.log started (02:21:46), so other sessions' IO during it is not recorded; load averages (1, 5, 15 min) were 2.75, 3.07, 2.71 at the start and 2.40, 3.40, 3.31 at the end (host-load.log)"],
        "evidencePath": path("arm3", "rebuild.json")}


def main():
    results = A.load(RESULTS)
    io = host_io_lines()
    io_rows = [r for run in MEASURED_RUNS for r in phase_io(io, run)]
    fcs = [fc1_stop_stalls(results), fc2_commit_latency(results, io_rows, io), fc3_space(results), fc4_torn_view(results)]
    overall = "fail" if any(f["verdict"] == "fail" for f in fcs) else ("inconclusive" if any(f["verdict"] == "not-measured" for f in fcs) else "pass")
    deriv = A.load(path("density-derivation.json"))
    results["overall_threshold_rows"] = results.get("overall_threshold_rows", results["overall"])
    results["overall"] = overall
    n = fcs[1]["summary_numbers"]
    results["overall_basis"] = (
        f"Condition 2 fails on Arm 2: the index-attach phase, with no other-session IO on the host, had commit p99 {n['attach_quiet_p99_s']:.4f} s, "
        f"{n['attach_min_ratio']}x or more of every host-quiet no-reader baseline, from the in-process SST build stage (p99 {n['build_stage_p99_s']:.4f} s), "
        f"not from the IngestExternalFile call ({n['ingest_seconds'] * 1000:.1f} ms). Conditions 1 and 4 pass. Condition 3 is not measured at density "
        "because both paired no-hold controls were cancelled. Run 1's hold-1s excess is not counted: a repeat with the same readers on a quiet "
        "host passed, and the Run 1 excess coincided with other sessions' IO.")
    results["verdict_sensitivity"] = (
        "If 'the ingest' in condition 2 means only the IngestExternalFile call, the Arm 2 excess does not count, no measured condition "
        "trips, and the overall verdict becomes inconclusive because condition 3 is not measured at density.")
    results["spec_fail_conditions"] = fcs
    results["host_io_by_phase"] = io_rows
    results["cancelled_controls"] = [
        {"run": "density-x60-control", "purpose": "paired no-hold control for the 60x 30-minute hold: same stores, blocks, multiplier, and phase timing with no readers in the middle phase, so that dead wallet-ballast entries and ballast SST bytes can be compared with the hold run at release, at the end of the 15-minute natural window, and after forced compaction",
         "status": A.EXCLUDED_CONTROLS["density-x60-control"], "evidencePaths": [path("sequence.log"), path("density-x60-control", "stderr.log"), path("host-load.log"), path("host-io.log")],
         "thresholds_not_measured": ["Density 60x: space after a 30-minute hold returns after release (paired no-hold control)", "spec condition 3 at 60x"]},
        {"run": "density-x10-control", "purpose": "the same paired control for the 10x hold",
         "status": A.EXCLUDED_CONTROLS["density-x10-control"], "evidencePaths": [path("sequence.log")],
         "thresholds_not_measured": ["Density 10x: space after a 30-minute hold returns after release (paired no-hold control)", "spec condition 3 at 10x"]}]
    results["density_target"] = {
        "label": deriv["label"], "derivation": path("density-derivation.json"),
        "targets": {"recent_tip_by_transactions_per_block": [deriv["mainnet_targets"]["recent_tip_by_transaction_count"]["multiplier_all_transactions"], deriv["mainnet_targets"]["recent_tip_by_transaction_count"]["multiplier_non_coinbase_transactions"]],
                    "whole_chain_per_block_by_outpoint_volume": deriv["mainnet_targets"]["whole_chain_average_by_transparent_outpoint_volume"]["per_block_ratio_range"]},
        "multipliers_run": [10, 60],
        "lowest_multiplier_tripping_a_measured_condition": None,
        "result": "no measured fail condition tripped at 10x or 60x (0 stop and 0 delay stalls, p99 <= 0.0412 s, hold-to-baseline ratios <= 1.22, 0 torn views); condition 3 is not measured at either multiplier",
        "sources_note": "the Mainnet transaction-rate and NU7 figures are web citations recorded by the previous agent and were not re-verified here"}
    results["arm3_mainnet_extrapolation"] = arm3(results)
    results["not_shown"] = [
        "Mainnet behavior: every number is a Testnet replay of heights 4,580,001 to 4,596,483 or less; recent Testnet blocks average 11.9 KB and 1.7 transactions, and the density arms add synthetic ballast bytes, not real Mainnet rows",
        "space return after a 30-minute hold under wallet-side churn: both paired no-hold controls were cancelled (60x died of a full disk 63 s into its post-release window; 10x was skipped), so condition 3 is not measured at density, and the 10x single-run result (20% returned in 15 minutes, 100% after forced compaction) is unresolved",
        "the topology under test: canonical and wallet stay separate databases with separate synced batches, so one shared WriteBatch, one database-wide write controller, and canonical-to-wallet torn views are not exercised",
        "absolute Mainnet commit latency: density ballast scales bytes written and compacted but not per-commit CPU (prepare, validation, readback, wallet derivation); canonical ballast keys are append-only, so hash-keyed canonical compaction debt is understated",
        "real block spacing: only realtime-25s-hold-5m (72 commits, p99 equals max 0.0377 s) ran at 25-second spacing; every other number comes from 500 ms accelerated pacing",
        "statistical depth: one run per arm, one repeat of hold-1s, n=1 per replacement shape, and a shared host where other sessions' IO reached 183 GB in one 30-minute phase; the host-quiet filter sees only processes it can read and treats zakurad as own load",
        "a separation of Arm 2's causes: the SST build holds a canonical snapshot while sorting and writing, and the harness cannot apportion the slowdown between the hold and the IO; ingest into a fresh empty column family is the best case; catch-up copies delta rows from the live old family rather than deriving them",
        "the design review's no-live-drop_cf amendment: Arm 2 tested live drop_cf (old files left disk 0.19 s after the last holder released), not DeleteRange truncation of a fixed family set",
        "Mainnet rebuild cost: Arm 3 measured only Testnet; the Mainnet range is a linear extrapolation, and this bench's spill ceiling is too small for a Mainnet outpoint sorter",
        "serving realism: Arm 4 is 24 in-process worker threads with zero think time, not the gRPC serving path, and wallet-family readers scan whole families",
        "fault isolation: no crash, OOM, or reader-fault coupling between serving and the writer was tested"]
    results["disk"] = {
        "current_bytes": {"spike_results_dir": "94 MB", "worktree_tmp_total": "147 MB", "target_build_artifacts_pre_existing": "2.3 GB"},
        "largest_run_work_dir_at_run_end_bytes": {"density-x60": 22660739408, "density-x60-control": 21279771930, "density-x10": 14517904276, "run1-1x": 13158385116, "repeat-scan-load": 12982105888},
        "note": "work directories were hard-link copies of the input stores, so du at run end counts about 12.9 GB of shared input; all work directories and database copies are deleted; no new database copy or build artifact was made for this report"}
    results["analysis"] = {"scripts": [os.path.join(HERE, "analyze.py"), os.path.abspath(__file__)],
                           "prior_results_backup": path("results.prior-2026-11-15T0807Z.json")}
    with open(RESULTS, "w") as f:
        json.dump(results, f, indent=1, default=str)
    print(RESULTS, overall)
    for fc in fcs:
        print(f"[{fc['verdict']}] {fc['id']}. {fc['condition']}")


if __name__ == "__main__":
    main()
