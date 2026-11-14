#!/usr/bin/env python3
"""Spike A: fold the per-arm reports into one machine-readable results file.

Throwaway. Reads the reports under .tmp/spike-results/reports and writes
.tmp/spike-results/results.json. Every value is copied from a report; a missing
report makes its threshold "not-measured".
"""
import json
import os
import sys

R = "/home/ubuntu/.t3/worktrees/zinder/spike-combined-commit/.tmp/spike-results"
REP = f"{R}/reports"
GIB = 1024 ** 3


def load(name):
    path = f"{REP}/{name}"
    if not os.path.exists(path):
        return None, path
    with open(path) as handle:
        return json.load(handle), path


def threshold(name, limit, arm, measured, passed, path, note=None):
    row = {
        "name": name,
        "threshold": limit,
        "arm": arm,
        "measured": measured,
        "pass": "not-measured" if passed is None else ("pass" if passed else "fail"),
        "evidencePath": path,
    }
    if note:
        row["note"] = note
    return row


def replay_row(report):
    if report is None:
        return None
    lat = report["latency"]
    return {
        "label": report.get("label"),
        "mode": report["mode"],
        "density_multiplier": report["density_multiplier"],
        "density_effective_byte_multiplier": report["density_effective_byte_multiplier"],
        "pressure_to_commit_bytes_ratio": report.get("pressure_to_commit_bytes_ratio"),
        "pressure_committer_blocked_seconds": (report.get("pressure") or {}).get("committer_blocked_seconds"),
        "budget": report["writer_budget"],
        "blocks": report["blocks"],
        "h0": report["h0"]["height"],
        "through": report["through"]["height"],
        "wall_seconds": report["wall_seconds"],
        "blocks_per_second": report["blocks_per_second"],
        "commit_p50_ms": lat["commit_total"]["p50_ms"],
        "commit_p99_ms": lat["commit_total"]["p99_ms"],
        "commit_p999_ms": lat["commit_total"]["p999_ms"],
        "commit_max_ms": lat["commit_total"]["max_ms"],
        "canonical_prepare_p99_ms": lat["canonical_prepare"]["p99_ms"],
        "wallet_plan_p99_ms": lat["wallet_plan"]["p99_ms"],
        "mirror_p99_ms": lat["mirror_lanes"]["p99_ms"],
        "synced_write_p50_ms": lat["synced_write"]["p50_ms"],
        "synced_write_p99_ms": lat["synced_write"]["p99_ms"],
        "synced_write_max_ms": lat["synced_write"]["max_ms"],
        "batch_bytes_total": report["batch"]["total_bytes"]["total"],
        "batch_bytes_p99": report["batch"]["total_bytes"]["p99"],
        "batch_bytes_max": report["batch"]["total_bytes"]["max"],
        "canonical_bytes_total": report["batch"]["canonical_bytes"]["total"],
        "wallet_bytes_total": report["batch"]["wallet_bytes"]["total"],
        "mirror_bytes_total": report["batch"]["mirror_bytes"]["total"],
        "wallet_point_reads_mean": report["batch"]["wallet_point_reads"]["mean"],
        "rss_peak_vmhwm_bytes": report["rss"]["peak_vmhwm_bytes"],
        "rss_max_sampled_bytes": report["rss"]["max_sampled_bytes"],
        "ticker_stall_seconds": report["stall_rates"]["ticker_stall_seconds"],
        "ticker_stall_seconds_per_chain_hour": report["stall_rates"]["ticker_stall_seconds_per_chain_hour"],
        "ticker_stall_seconds_per_wall_hour": report["stall_rates"]["ticker_stall_seconds_per_wall_hour"],
        "commit_write_delay_total_seconds": report["perf"]["write_delay_total_seconds"],
        "commits_with_write_delay": report["perf"]["commits_with_write_delay"],
        "db_stop_entries": report["stalls"]["db_stop_entries"],
        "db_delay_entries": report["stalls"]["db_delay_entries"],
        "write_stopped_samples": report["properties"]["write_stopped_samples"],
        "per_family_stall_entries": {
            family: {key: value for key, value in entry.items() if "entries" in key}
            for family, entry in report["stalls"].get("per_column_family", {}).items()
        },
        "write_amplification": report["write_amplification"]["amplification"],
        "secondary_final_catch_up_seconds": (report.get("secondary") or {}).get("final_catch_up_seconds"),
        "secondary_periodic_p99_ms": ((report.get("secondary") or {}).get("periodic_catch_ups") or {}).get("p99_ms"),
        "secondary_periodic_max_ms": ((report.get("secondary") or {}).get("periodic_catch_ups") or {}).get("max_ms"),
        "secondary_fence_matches": (report.get("secondary") or {}).get("final_fence_matches_primary"),
        "secondary_wallet_control_matches_fence": (report.get("secondary") or {}).get("wallet_control_matches_fence"),
        "wallet_digest": (report.get("wallet_final") or {}).get("projection_digest_hex"),
        "work_store_bytes": report.get("work_store_bytes"),
    }


def reorg_row(report):
    if report is None:
        return None
    reps = report["replacements"]
    return {
        "label": report.get("label"),
        "mode": report["mode"],
        "density_multiplier": report.get("density_multiplier", 1),
        "replacements": len(reps),
        "first_height": reps[0]["first_height"] if reps else None,
        "tip_height": reps[0]["tip_height"] if reps else None,
        "commit_seconds": [r["commit_seconds"] for r in reps],
        "canonical_prepare_seconds": [r["canonical_prepare_seconds"] for r in reps],
        "wallet_plan_seconds": [r["wallet_plan_seconds"] for r in reps],
        "mirror_seconds": [r.get("mirror_seconds") for r in reps],
        "synced_write_seconds": [r["synced_write_seconds"] for r in reps],
        "batch_bytes_max": max((r["batch_bytes"] for r in reps), default=None),
        "canonical_batch_bytes": reps[0]["canonical_batch_bytes"] if reps else None,
        "wallet_batch_bytes": reps[0]["wallet_batch_bytes"] if reps else None,
        "mirror_batch_bytes": (reps[0].get("mirror_canonical_batch_bytes", 0) + reps[0].get("mirror_wallet_batch_bytes", 0)) if reps else None,
        "effective_byte_multiplier": reps[0].get("effective_byte_multiplier") if reps else None,
        "batch_operations_max": max((r["batch_operations"] for r in reps), default=None),
        "scaled_wallet_plan_logical_bytes_max": max((r.get("scaled_wallet_plan_logical_bytes") or (r["wallet_plan_batch_logical_bytes"] + r["wallet_plan_overlay_logical_bytes"]) for r in reps), default=None),
        "ingest_held_bytes": reps[0]["ingest_held_bytes"] if reps else None,
        "scaled_ingest_held_bytes_estimate": reps[0].get("scaled_ingest_held_bytes_estimate") if reps else None,
        "peak_vmhwm_bytes": report["peak_vmhwm_bytes"],
        "peak_rss_with_unmirrored_source_estimate_bytes": report.get("peak_rss_with_unmirrored_source_estimate_bytes"),
        "secondary_catch_up_seconds": [r.get("secondary_catch_up_seconds") for r in reps],
        "secondary_fence_matches": [r.get("secondary_fence_matches_primary") for r in reps],
        "secondary_wallet_control_matches_fence": [r.get("secondary_wallet_control_matches_fence") for r in reps],
        "db_stop_entries": report["stalls"]["db_stop_entries"],
        "db_delay_entries": report["stalls"]["db_delay_entries"],
        "write_stopped_samples": report["properties"]["write_stopped_samples"],
        "ticker_stall_seconds": report["tickers"].get("rocksdb.stall.micros", 0) / 1e6,
        "wallet_digest_before": (report.get("wallet_before") or {}).get("projection_digest_hex"),
        "wallet_digest_after": (report.get("wallet_final") or {}).get("projection_digest_hex"),
        "cold_readback_seconds": report.get("cold_readback_seconds"),
        "ceilings": report["ceilings"],
    }


def main():
    arms = {}
    paths = {}
    for name in sorted(os.listdir(REP)):
        if not name.endswith(".json"):
            continue
        report, path = load(name)
        key = name[:-5]
        paths[key] = path
        action = report.get("action") if isinstance(report, dict) else None
        if action == "replay":
            arms[key] = replay_row(report)
        elif action == "reorg":
            arms[key] = reorg_row(report)
        else:
            arms[key] = report
    out = {
        "network_label": "Testnet (all stores and blocks); Mainnet-like rows come from synthetic density lanes",
        "source_revision": open(f"{R}/bin/zinder-bench.commit").read().strip(),
        "arms": arms,
        "report_paths": paths,
    }
    with open(f"{R}/results-arms.json", "w") as handle:
        json.dump(out, handle, indent=2)
    print(f"{R}/results-arms.json")


if __name__ == "__main__":
    sys.exit(main())
