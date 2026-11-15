#!/usr/bin/env python3
"""Scenario height sets for analyze.py from a replay's per-block samples.

Usage: build-spec.py SAMPLES_JSONL MODE OUT_JSON
  MODE canonical-only: near-empty blocks, canonical prepare and whole commit.
  MODE combined: wallet planner on near-empty (clean and after-dense),
                 input-heavy, and fan-out blocks.
Near-empty: <= 2 transactions, <= 2 transparent inputs, <= 3 transparent outputs.
Dense: more than 1,000 transparent inputs plus outputs. "After dense" means
within the 1,000 heights (the reorg window) after a dense block.
"""
import json
import sys

REORG_WINDOW = 1000
NAMED_INPUT_HEAVY = [4613105, 4613143, 4618153, 4618168]
NAMED_FAN_OUT = range(4602623, 4602665)


def main() -> None:
    samples_path, mode, out_path = sys.argv[1:4]
    rows = [json.loads(line) for line in open(samples_path)]
    first = rows[0]["height"]

    def near_empty(row: dict) -> bool:
        return row["transactions"] <= 2 and row["transparent_inputs"] <= 2 and row["transparent_outputs"] <= 3

    dense = [r["height"] for r in rows if r["transparent_inputs"] + r["transparent_outputs"] > 1000]
    after_dense = set()
    for height in dense:
        after_dense.update(range(height + 1, height + 1 + REORG_WINDOW))
    # Skip the first 10 blocks (warm-up after open).
    usable = [r for r in rows if r["height"] >= first + 10]
    empty = [r["height"] for r in usable if near_empty(r)]
    spec = []
    if mode == "canonical-only":
        spec.append({"name": "canonical-prepare-near-empty", "phase": "canonical_prepare", "heights": empty})
        spec.append({"name": "canonical-commit-near-empty", "phase": "commit", "heights": empty})
        # Derive runs up to two blocks ahead of the committer, so keep blocks
        # whose neighbours are near-empty too.
        empty_set = set(empty)
        steady = [h for h in empty if all(h + d in empty_set for d in range(-3, 4))]
        spec.append({"name": "derive-near-empty", "phase": "derive", "thread": "derive", "heights": steady})
        fan = [r["height"] for r in usable if r["transparent_outputs"] >= 1000 and r["transparent_inputs"] == 0]
        heavy = [r["height"] for r in usable if r["transparent_inputs"] >= 1000]
        spec.append({"name": "canonical-prepare-fan-out", "phase": "canonical_prepare", "heights": fan})
        spec.append({"name": "canonical-prepare-input-heavy", "phase": "canonical_prepare", "heights": heavy})
    else:
        clean = [h for h in empty if h not in after_dense]
        after = [h for h in empty if h in after_dense]
        heavy = [r["height"] for r in usable if r["transparent_inputs"] >= 1000]
        fan = [r["height"] for r in usable if r["transparent_outputs"] >= 1000 and r["transparent_inputs"] == 0]
        named_fan = [h for h in fan if h in NAMED_FAN_OUT]
        spec += [
            {"name": "plan-near-empty-clean", "phase": "wallet_plan", "heights": clean},
            {"name": "plan-near-empty-after-dense", "phase": "wallet_plan", "heights": after},
            {"name": "plan-input-heavy", "phase": "wallet_plan", "heights": heavy},
            {"name": "plan-input-heavy-named", "phase": "wallet_plan", "heights": NAMED_INPUT_HEAVY},
            {"name": "plan-fan-out", "phase": "wallet_plan", "heights": fan},
            {"name": "plan-fan-out-4602623-4602664", "phase": "wallet_plan", "heights": named_fan},
            {"name": "combined-commit-near-empty-clean", "phase": "commit", "heights": clean},
            {"name": "combined-commit-input-heavy", "phase": "commit", "heights": heavy},
            {"name": "combined-commit-fan-out", "phase": "commit", "heights": fan},
            {"name": "combined-prepare-near-empty-clean", "phase": "canonical_prepare", "heights": clean},
        ]
    json.dump(spec, open(out_path, "w"))
    for scenario in spec:
        print(scenario["name"], len(scenario["heights"]), file=sys.stderr)


if __name__ == "__main__":
    main()
