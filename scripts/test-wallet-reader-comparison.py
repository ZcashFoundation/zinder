#!/usr/bin/env python3
"""Exercise the complete local comparison using one reader binary as both arms."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reader", required=True)
    parser.add_argument("--bench", required=True)
    parser.add_argument("--writer", required=True)
    args = parser.parse_args()
    with TemporaryDirectory(prefix="wallet-comparison-smoke-") as tmp:
        root = Path(tmp)
        policy = root / "policy.json"
        policy.write_text(json.dumps({
            "baseline": "baseline", "candidate": "candidate", "min_pairs": 2,
            "metrics": [{"name": "verified payload bytes", "pointer": "/totals/bytes",
                         "direction": "higher_is_better", "tolerance_absolute": 0}],
        }))
        output = root / "results"
        command = [sys.executable, Path(__file__).with_name("compare-wallet-readers.py"),
                   "--baseline", args.reader, "--candidate", args.reader, "--bench", args.bench,
                   "--writer", args.writer, "--output", str(output), "--policy", str(policy),
                   "--blocks", "32", "--width", "4", "--pairs", "2", "--clients", "2",
                   "--repetitions", "4"]
        result = subprocess.run(command, stdout=subprocess.DEVNULL, check=False)
        if result.returncode:
            for log in sorted(output.rglob("*.log")):
                if log.stat().st_size:
                    print(log.relative_to(output), log.read_text(errors="replace")[-1600:], file=sys.stderr)
            raise SystemExit(result.returncode)
        decision = json.loads((output / "decision.json").read_text())
        if decision["verdict"] != "no_clear_change" or decision["issues"]:
            raise RuntimeError("A/A did not return identical verified bytes")
        for name in ("frontier", "historical_control"):
            for period in (1, 2):
                for arm in ("baseline", "candidate"):
                    report = json.loads((output / "trials" / name / f"round-{period}" / arm / "report.json").read_text())
                    if report["verified"] is not True or report["totals"]["requests"] != 16:
                        raise RuntimeError(f"missing verified streams for {name}/{period}/{arm}")
        print("Verified both range cases on two private listeners across both trial orders.")


if __name__ == "__main__":
    main()
