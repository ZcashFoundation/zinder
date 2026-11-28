#!/usr/bin/env python3
"""Run the funded devtool wallet lifecycle against a ready, disposable Zebra fixture."""
import argparse
import json
import os
from pathlib import Path
import re
import runpy
import shutil
import subprocess
import sys
from wallet_fixture import binding


def main():
    directory = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", required=True, type=Path, help="regtest_support devtool executable")
    parser.add_argument("--client-revision", required=True, help="commit used to build the client")
    parser.add_argument("--client-sha256", required=True, help="expected executable SHA-256")
    parser.add_argument("--ready", required=True, type=Path, help="fixture readiness JSON with node and wallet endpoints")
    parser.add_argument("--candidate", type=Path, help="candidate service receipt for the same fixture")
    parser.add_argument("--input", action="append", default=[], type=Path,
                        help="additional immutable inputs, such as the devtool lockfile")
    parser.add_argument("--output", required=True, type=Path, help="new private campaign directory")
    parser.add_argument("--gate", type=Path, default=directory.parent / "run-wallet-lifecycle.py")
    parser.add_argument("--trace-tool", type=Path, default=directory.parent / "trace-wallet-endpoint.py")
    args = parser.parse_args()
    if not re.fullmatch("[0-9a-f]{40}", args.client_revision):
        parser.error("--client-revision requires a full commit SHA")

    try:
        endpoint, node, _, _ = binding(args.ready, args.candidate)
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.error(str(error))
    adapter = directory / "devtool-regtest.py"
    contract = runpy.run_path(str(adapter))
    benchmark = args.gate.resolve().with_name("run-benchmark-campaign.py")
    sha256 = runpy.run_path(str(benchmark))["sha256"]
    python = Path(sys.executable).resolve()
    trace = args.trace_tool.resolve()
    client = args.client.resolve()
    strace = shutil.which("strace")
    if not strace:
        parser.error("Linux strace is required to verify wallet connections")
    paths = [args.ready, args.gate, benchmark, python, trace, client, adapter, directory / "wallet_fixture.py",
             __file__, strace, *args.input]
    fixture_args = ["--ready", str(args.ready.resolve())]
    if args.candidate:
        paths.append(args.candidate)
        fixture_args.extend(["--candidate", str(args.candidate.resolve())])
    inputs = {str(path): sha256(path) for path in {Path(p).resolve() for p in paths}}
    if inputs[str(client)] != args.client_sha256:
        parser.error("devtool binary differs from the expected SHA-256")

    os.umask(0o077)
    output = args.output.resolve()
    output.mkdir()
    interpreter = [str(python)] + (["-" + "O" * sys.flags.optimize] if sys.flags.optimize else [])
    steps = {}
    for name in contract["STEPS"]:
        steps[name] = {
            "command": [*interpreter, str(adapter), "--step", name, "--client", str(client),
                        "--endpoint", "{endpoint}", "--node", node, *fixture_args, "--trace-tool", str(trace),
                        "--scratch", "{scratch}", "--report", "{report}", "--evidence", str(output / "raw")],
            "expect": {"/assertions/" + name: True},
        }
    case = {
        "wallet": "zcash-devtool", "version": args.client_revision, "variant": "regtest-sapling",
        "network": "zcash-regtest", "endpoint": endpoint, "implementation": "engine",
        "interface": "lightwalletd", "require_no_fallback": True, "restart_scope": "process-restart",
        "client": {"path": str(client), "sha256": args.client_sha256}, "inputs": inputs, "steps": steps,
    }
    manifest = output / "manifest.json"
    manifest.write_text(json.dumps({"schema_version": 2, "timeout_seconds": 420,
                                    "cases": {"devtool": case}}, indent=2) + "\n")
    return subprocess.run([*interpreter, str(args.gate.resolve()), "--manifest", str(manifest),
                           "--output", str(output / "gate")], check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
