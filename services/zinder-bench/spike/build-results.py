#!/usr/bin/env python3
"""Spike A: map every spec threshold to its measured value, arm, and evidence file.

Throwaway. Reads .tmp/spike-results/reports/*.json and writes
.tmp/spike-results/results.json. A threshold whose report is missing is
"not-measured"; the overall verdict follows the thresholds strictly.
"""
import json
import os

R = "/home/ubuntu/.t3/worktrees/zinder/spike-combined-commit/.tmp/spike-results"
REP = f"{R}/reports"
GIB = 1024 ** 3
MIB = 1024 ** 2


def load(name):
    path = f"{REP}/{name}.json"
    if not os.path.exists(path):
        return None, path
    with open(path) as handle:
        return json.load(handle), path


rows = []


def add(name, threshold, arm, measured, passed, path, scope, note=None):
    rows.append({
        "name": name,
        "threshold": threshold,
        "arm": arm,
        "scope": scope,
        "measured": measured,
        "pass": "not-measured" if passed is None else ("pass" if passed else "fail"),
        "evidencePath": path,
        **({"note": note} if note else {}),
    })


def verify_row(name, label, scope):
    report, path = load(name)
    if report is None:
        add("wallet digest equals reference", "equal", label, "report missing", None, path, scope)
        return
    flags = {key: report.get(key) for key in (
        "digest_equal", "combined_rows_match_evidence", "row_counts_equal", "utxo_summary_equal",
        "source_tip_equal", "settled_tip_equal", "source_sequence_digest_equal",
        "baseline_canonical_sequence_equal")}
    ok = bool(report["digest_equal"] and report["combined_rows_match_evidence"])
    add("wallet digest equals reference", "equal", label,
        f"combined {report['combined_wallet']['projection_digest_hex']} vs reference "
        f"{report['reference']['projection_digest_hex']}; flags {flags}", ok, path, scope)


def replay_rows(name, label, scope, compare_name=None, rss=True):
    report, path = load(name)
    if report is None:
        for check in ("tip commit p99 at most 1 s", "replay at least 10 blocks per second"):
            add(check, "", label, "report missing", None, path, scope)
        return
    commit = report["latency"]["commit_total"]
    add("tip commit p99 at most 1 s", "<= 1000 ms", label,
        f"p99 {commit['p99_ms']:.1f} ms (p50 {commit['p50_ms']:.1f}, p99.9 {commit['p999_ms']:.1f}, "
        f"max {commit['max_ms']:.1f})", commit["p99_ms"] <= 1000.0, path, scope)
    if compare_name:
        compare, cpath = load(compare_name)
        if compare is None:
            add("tip commit p99 at most 2x canonical-only p99", "<= 2.0x", label, "report missing", None, cpath, scope)
        else:
            ratio = next(r for r in compare["ratios"] if r["metric"] == "latency.commit_total.p99_ms")
            add("tip commit p99 at most 2x canonical-only p99", "<= 2.0x", label,
                f"{ratio['ratio']:.2f}x (combined {ratio['combined']:.1f} ms, canonical-only "
                f"{ratio['baseline']:.1f} ms)", ratio["ratio"] <= 2.0, cpath, scope)
    add("replay at least 10 blocks per second", ">= 10 blocks/s", label,
        f"{report['blocks_per_second']:.2f} blocks/s over {report['blocks']} blocks "
        f"({report['wall_seconds']:.0f} s)", report["blocks_per_second"] >= 10.0, path, scope)
    if rss:
        peak = report["rss"]["peak_vmhwm_bytes"] or 0
        add("writer peak RSS at most 6 GiB (replay)", "<= 6 GiB", label,
            f"{peak / GIB:.3f} GiB VmHWM", peak <= 6 * GIB, path, scope)
    stops = report["stalls"]["db_stop_entries"] + report["properties"]["write_stopped_samples"]
    add("zero stop stalls", "0", label,
        f"{report['stalls']['db_stop_entries']} stop entries, "
        f"{report['properties']['write_stopped_samples']} stopped samples; "
        f"{report['stalls']['db_delay_entries']} delay entries, "
        f"{report['stall_rates']['ticker_stall_seconds']:.3f} s stall.micros",
        stops == 0, path, scope)


def reorg_rows(name, label, scope):
    report, path = load(name)
    if report is None:
        add("1000-block replacement batch at most 2.5 GiB", "<= 2.5 GiB", label, "report missing", None, path, scope)
        return
    reps = report["replacements"]
    batch = max(r["batch_bytes"] for r in reps)
    add("1000-block replacement batch at most 2.5 GiB", "<= 2.5 GiB", label,
        f"{batch / MIB:.1f} MiB max over {len(reps)} replacements of {reps[0]['blocks']} blocks "
        f"({reps[0]['first_height']}..{reps[0]['tip_height']}); density x{report.get('density_multiplier', 1)}, "
        f"effective byte multiplier {reps[0].get('effective_byte_multiplier', 1.0):.2f}",
        batch <= 2.5 * GIB, path, scope)
    peak = report["peak_vmhwm_bytes"] or 0
    estimate = report.get("peak_rss_with_unmirrored_source_estimate_bytes")
    add("writer peak RSS during the replacement at most 6 GiB", "<= 6 GiB", label,
        f"{peak / GIB:.3f} GiB VmHWM" + (f"; {estimate / GIB:.3f} GiB with unmirrored source bytes added (estimate)"
                                          if estimate and report.get("density_multiplier", 1) > 1 else ""),
        peak <= 6 * GIB and (estimate is None or estimate <= 6 * GIB), path, scope)
    stops = report["stalls"]["db_stop_entries"] + report["properties"]["write_stopped_samples"]
    add("zero stop stalls", "0", label,
        f"{report['stalls']['db_stop_entries']} stop entries, {report['properties']['write_stopped_samples']} "
        f"stopped samples, {report['stalls']['db_delay_entries']} delay entries", stops == 0, path, scope)
    catch = [r.get("secondary_catch_up_seconds") for r in reps]
    fences = [r.get("secondary_fence_matches_primary") for r in reps]
    if any(value is not None for value in catch):
        worst = max(value for value in catch if value is not None)
        add("secondary catch-up after the reorg batch under 30 s", "< 30 s", label,
            f"{worst:.3f} s max; fence matched primary after every batch: {all(fences)}",
            worst < 30.0 and all(fences), path, scope)
    commits = [r["commit_seconds"] for r in reps]
    rows[-1].setdefault("context", {})["commit_seconds"] = commits


verify_row("verify-arm1", "Arm 1 combined at T vs build_wallet_from_canonical over the canonical-only result", "Testnet")
verify_row("verify-arm1-dataprep", "Arm 1 combined at T vs data prep's projector-built and followed wallet", "Testnet")
verify_row("verify-arm2", "after Arm 2 (5 x 1000-block replacements at T) vs the same reference", "Testnet")
dense, dense_path = load("arm2-dense-x1-combined")
if dense is not None:
    ok = dense.get("cold_readback_seconds") is not None and \
        dense["wallet_before"]["projection_digest_hex"] == dense["wallet_final"]["projection_digest_hex"]
    add("wallet rows equal READY evidence after dense replacements", "cold readback equal; digest unchanged",
        "Arm 2 dense window x1", f"cold readback {dense.get('cold_readback_seconds')} s; digest before "
        f"{dense['wallet_before']['projection_digest_hex']} after {dense['wallet_final']['projection_digest_hex']}",
        ok, dense_path, "Testnet")

replay_rows("arm1-combined", "Arm 1 combined (H0+1..T)", "Testnet", "compare-arm1")
replay_rows("arm1-canonical-only", "Arm 1 canonical-only baseline", "Testnet", None)
replay_rows("arm1-repeat-to-4618271-combined", "Arm 1 combined repeat (H0+1..4618271)", "Testnet")
for k in (4, 7):
    replay_rows(f"density{k}-combined", f"density x{k} combined (lanes clamped at 2,000,000-byte blocks)",
                "Mainnet-like synthetic", f"compare-density{k}")
    replay_rows(f"density{k}-canonical-only", f"density x{k} canonical-only baseline", "Mainnet-like synthetic")

reorg_rows("arm2-combined", "Arm 2 combined at T (sparse window)", "Testnet")
reorg_rows("arm2-canonical-only", "Arm 2 canonical-only at T", "Testnet")
for k in (4, 7):
    reorg_rows(f"arm2-density{k}-combined", f"Arm 2 combined at T, density x{k}", "Testnet window scaled")
for name in sorted(os.listdir(REP)):
    if name.startswith("arm2-dense-x") and name.endswith("-combined.json"):
        k = name[len("arm2-dense-x"):-len("-combined.json")]
        report, path = load(name[:-5])
        if report.get("action") == "skipped":
            add("1000-block replacement batch at most 2.5 GiB", "<= 2.5 GiB",
                f"Arm 2 dense window x{k} (ingest-cap probe)", report["reason"], None, path,
                "probe, not a spec arm")
            rows[-1]["pass"] = "skipped"
            continue
        scope = {
            "1": "Testnet; window 4617272..4618271 is 0.85x the densest 2026 Mainnet day in bytes and 3.6x its transparent inputs plus outputs",
            "2": "synthetic; 1.7x the densest 2026 Mainnet day in bytes, 7.3x in transparent inputs plus outputs",
            "4": "synthetic; 3.4x the densest 2026 Mainnet day in bytes, 14.6x in transparent inputs plus outputs",
        }.get(k, "synthetic")
        reorg_rows(name[:-5], f"Arm 2 combined over the densest Testnet window, density x{k}", scope)

for name, label in (("arm3-combined", "Arm 3 combined"), ("arm3-canonical-only", "Arm 3 canonical-only"),
                    ("arm3-budget-canonical-combined", "Arm 3 combined, canonical budget profile"),
                    ("arm3-wallet-debt-combined", "Arm 3 combined, wallet-family pressure only")):
    report, path = load(name)
    if report is None:
        add("delay stalls at most 1 s per hour (Arm 3)", "<= 1 s/h", label, "report missing", None, path, "Testnet")
        continue
    rates = report["stall_rates"]
    add("delay stalls at most 1 s per hour (Arm 3)", "<= 1 s per replayed hour", label,
        f"{rates['ticker_stall_seconds_per_chain_hour']:.4f} s per chain hour; "
        f"{rates['ticker_stall_seconds_per_wall_hour']:.2f} s per wall hour; "
        f"{rates['ticker_stall_seconds']:.3f} s stall.micros total; commit-thread write delay "
        f"{report['perf']['write_delay_total_seconds']:.3f} s; pressure ratio "
        f"{report.get('pressure_to_commit_bytes_ratio') or 0:.2f}x",
        rates["ticker_stall_seconds_per_chain_hour"] <= 1.0, path, "Testnet")
    stops = report["stalls"]["db_stop_entries"] + report["properties"]["write_stopped_samples"]
    add("stop stalls under compaction pressure (reported; spec gates Arms 1 and 2)", "0", label,
        f"{report['stalls']['db_stop_entries']} stop entries, {report['properties']['write_stopped_samples']} "
        f"stopped samples, {report['stalls']['db_delay_entries']} delay entries", stops == 0, path, "Testnet")
    commit = report["latency"]["commit_total"]
    rows[-1].setdefault("context", {}).update({
        "commit_p99_ms": commit["p99_ms"], "commit_max_ms": commit["max_ms"],
        "blocks_per_second": report["blocks_per_second"],
        "committer_blocked_seconds": (report.get("pressure") or {}).get("committer_blocked_seconds"),
    })

report, path = load("arm1-combined")
if report is not None:
    sec = report["secondary"]
    add("secondary catch-up after the replay", "< 30 s", "Arm 1 combined (secondary in its own process, 1 s poll)",
        f"final {sec['final_catch_up_seconds']:.4f} s; periodic p99 {sec['periodic_catch_ups']['p99_ms']:.1f} ms, "
        f"max {sec['periodic_catch_ups']['max_ms']:.1f} ms; fence matched {sec['final_fence_matches_primary']}; "
        f"wallet control names the fence {sec['wallet_control_matches_fence']}",
        sec["final_catch_up_seconds"] < 30.0 and sec["final_fence_matches_primary"], path, "Testnet")

gated = [row for row in rows if not row["name"].startswith("stop stalls under compaction")
         and row["pass"] != "skipped"]
if any(row["pass"] == "fail" for row in gated):
    overall = "fail"
elif any(row["pass"] == "not-measured" for row in gated):
    overall = "inconclusive"
else:
    overall = "pass"
non_spec = []
for name in sorted(os.listdir(REP)):
    if not name.endswith(".json"):
        continue
    report, path = load(name[:-5])
    if isinstance(report, dict) and report.get("action") == "reorg":
        for ceiling in report["ceilings"]:
            if any(word in ceiling["check"] for word in ("wallet plan", "ingest", "estimate")):
                non_spec.append({"arm": name[:-5], **ceiling, "evidencePath": path})

derived = []
dense, dense_path = load("arm2-dense-x1-combined")
with open(f"{R}/mainnet-density-reference.json") as handle:
    mainnet = json.load(handle)
if dense is not None:
    first = dense["replacements"][0]
    window_bytes, window_io = 197743920, 479950 + 363
    densest_bytes_day = mainnet["queries"][4]["values"][0]
    densest_io_day = mainnet["queries"][4]["values"][4]
    canonical = first["canonical_batch_bytes"] * densest_bytes_day["avg_size_bytes"] * 1000 / window_bytes
    io_day = (densest_io_day["avg_transparent_inputs"] + densest_io_day["avg_transparent_outputs"]) * 1000
    wallet = first["wallet_batch_bytes"] * io_day / window_io
    derived.append({
        "name": "1000-block replacement at the densest 2026 Mainnet day (estimate, not measured)",
        "method": "canonical batch bytes of the dense Testnet window scaled by raw bytes (2026-09-03, 231,594 B/block); wallet batch bytes scaled by transparent inputs plus outputs (2026-09-23, 131.7 per block)",
        "canonical_batch_bytes": round(canonical), "wallet_batch_bytes": round(wallet),
        "batch_bytes": round(canonical + wallet),
        "within_2_5_gib": canonical + wallet <= 2.5 * GIB,
        "bounded_by_measured_run": f"{dense_path} (x1, {first['batch_bytes']} bytes, VmHWM {dense['peak_vmhwm_bytes']})",
    })
    held = max(r["ingest_held_bytes"] for r in dense["replacements"])
    cap = 2 * GIB / held
    derived.append({
        "name": "largest dense-window replacement ingest admits (ingest.follow.max_replacement_bytes = 2 GiB)",
        "method": "linear in the multiplier, from the measured x1, x2, x4 batch sizes (each exactly k times x1)",
        "multiplier": round(cap, 2),
        "batch_bytes": round(first["batch_bytes"] * cap),
        "within_2_5_gib": first["batch_bytes"] * cap <= 2.5 * GIB,
        "batch_to_held_ratio": round(first["batch_bytes"] / held, 3),
        "held_bytes_at_2_5_gib_batch": round(2.5 * GIB * held / first["batch_bytes"]),
    })
    vm = {}
    for k in ("1", "2", "4"):
        rep, _ = load(f"arm2-dense-x{k}-combined")
        if rep is not None:
            vm[k] = {"batch_bytes": rep["replacements"][0]["batch_bytes"], "peak_vmhwm_bytes": rep["peak_vmhwm_bytes"]}
    derived.append({"name": "measured writer VmHWM against replacement batch size (dense window)", "points": vm})

with open(f"{R}/bin/zinder-bench.commit") as handle:
    revision = handle.read().strip()
out = {
    "spike": "A-combined-commit",
    "network": "Testnet stores and blocks (T = 4620122, H0 = 4570122); Mainnet-like rows are synthetic density lanes",
    "source_revision": revision,
    "overall": overall,
    "failed": [row for row in gated if row["pass"] == "fail"],
    "thresholds": rows,
    "non_spec_ceilings": non_spec,
    "derived_estimates": derived,
    "mainnet_reference": f"{R}/mainnet-density-reference.json",
}
with open(f"{R}/results.json", "w") as handle:
    json.dump(out, handle, indent=2)
print(f"{R}/results.json overall={overall}")
for row in rows:
    print(f"[{row['pass']}] {row['name']} | {row['arm']} | {row['measured'][:160]}")
