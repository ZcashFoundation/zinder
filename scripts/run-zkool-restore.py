#!/usr/bin/env python3
"""Run a pinned Zkool sync_eval restore against a fixed lightwalletd fixture."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time


# Public BIP-39 test seed used by zcash-sync-evals/.github/workflows/bench.yml.
SYNTHETIC_SEED = "abandon " * 23 + "art"


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run_logged(command, stdout_path, stderr_path):
    with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
        return subprocess.run(command, stdout=stdout, stderr=stderr, check=False).returncode


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path, help="pinned sync_eval evaluator executable")
    parser.add_argument("--endpoint", required=True, help="prepared lightwalletd fixture URL")
    parser.add_argument("--birthday", required=True, type=int)
    parser.add_argument("--expected-tip", required=True, type=int)
    parser.add_argument("--scratch", required=True, type=Path, help="new directory for the wallet and logs")
    parser.add_argument("--expected-sha256", help="optional required evaluator SHA-256")
    args = parser.parse_args()

    try:
        binary = args.binary.absolute()
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise ValueError("binary must be an executable file")
        binary_sha256 = sha256(binary)
        if args.expected_sha256 and binary_sha256.lower() != args.expected_sha256.lower():
            raise ValueError("evaluator SHA-256 does not match --expected-sha256")
        if not 0 <= args.birthday <= args.expected_tip <= 2**32 - 1:
            raise ValueError("birthday and expected tip must be ordered u32 heights")
        scratch = args.scratch.absolute()
        scratch.mkdir(mode=0o700)
    except (OSError, ValueError) as error:
        parser.error(str(error))

    wallet_dir = scratch / "wallet"
    sync_command = [str(binary), "sync", args.endpoint, str(args.birthday), SYNTHETIC_SEED, str(wallet_dir)]
    try:
        sync_start = time.monotonic()
        sync_exit = run_logged(sync_command, scratch / "sync.stdout.log", scratch / "sync.stderr.log")
        sync_seconds = time.monotonic() - sync_start
        if sync_exit:
            raise ValueError(f"evaluator sync exited {sync_exit}. See {scratch / 'sync.stderr.log'}")

        report_exit = run_logged(
            [str(binary), "report", str(wallet_dir)],
            scratch / "report.stdout.log",
            scratch / "report.stderr.log",
        )
        if report_exit:
            raise ValueError(f"evaluator report exited {report_exit}. See {scratch / 'report.stderr.log'}")

        report = json.loads((scratch / "report.stdout.log").read_text(encoding="utf-8"))
        synced_height = report["synced_height"]
        if type(synced_height) is not int or synced_height != args.expected_tip:
            raise ValueError(f"reported synced_height {synced_height!r}, expected {args.expected_tip}")
        report.update({
            "sync_seconds": sync_seconds,
            "client": {"binary": str(binary), "sha256": binary_sha256},
        })
        print(json.dumps(report, allow_nan=False))
    except (OSError, KeyError, TypeError, ValueError) as error:
        print(f"Zkool restore failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
