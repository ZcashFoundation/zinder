#!/usr/bin/env python3
"""Pin Cake 6.5.4's engine and create a schema-v2 wallet lifecycle manifest."""
import argparse
import json
import os
from pathlib import Path
import runpy
import shutil
import sys
from wallet_fixture import binding


ENGINE_LOCK_SHA256 = "e58b4de00913ddcb33e0d4af56936872140cafa4e97407babffddb379de0a9a2"
RELEASED_PUBSPEC_SHA256 = "ab2a8c5bdb62d5bc64262c4fae0eabf8f4180344fcd71ad75cbc27bd9a1125c7"


def main():
    scripts = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", required=True, type=Path)
    parser.add_argument("--engine-sha256", help="expected executable digest from a previous build")
    parser.add_argument("--engine-lock", required=True, type=Path)
    parser.add_argument("--released-pubspec", required=True, type=Path, help="Cake v6.5.4 cw_zcash/pubspec.yaml")
    parser.add_argument("--fixture", required=True, type=Path, help="disposable Regtest readiness receipt")
    parser.add_argument("--candidate", type=Path, help="candidate service receipt for the same fixture")
    parser.add_argument("--source-input", action="append", default=[], type=Path, help="additional build or source receipt to pin")
    parser.add_argument("--trace", type=Path, default=scripts / "trace-wallet-endpoint.py")
    parser.add_argument("--runner", type=Path, default=scripts / "run-wallet-lifecycle.py")
    parser.add_argument("--output", required=True, type=Path, help="new manifest path")
    args = parser.parse_args()

    try:
        trace = args.trace.resolve(strict=True)
        runner = args.runner.resolve(strict=True)
        gate = runpy.run_path(str(runner))
        sha256 = gate["benchmark"].sha256
        client = args.engine.resolve(strict=True)
        fixture = args.fixture.resolve(strict=True)
        adapter = Path(__file__).resolve().with_name("cake-regtest.py")
        python = Path(sys.executable).resolve()
        strace = shutil.which("strace")
        if not strace or not os.access(client, os.X_OK):
            parser.error("the wallet engine and strace must both be executable")
        paths = [client, python, trace, runner, Path(gate["benchmark"].__file__), adapter,
                 args.engine_lock, args.released_pubspec,
                 fixture, Path(strace), Path(__file__), adapter.with_name("wallet_fixture.py"), *args.source_input]
        if args.candidate:
            paths.append(args.candidate)
        inputs = {str(p.resolve(strict=True)): sha256(p) for p in paths}
        for file, expected in ((args.engine_lock, ENGINE_LOCK_SHA256),
                               (args.released_pubspec, RELEASED_PUBSPEC_SHA256),
                               (client, args.engine_sha256)):
            if expected is not None and inputs[str(file.resolve())] != expected:
                parser.error(f"unexpected digest for {file}")
        endpoint, node, _, _ = binding(fixture, args.candidate)

        def command(step):
            argv = [str(python), "-O", str(adapter), "--step", step, "--scratch", "{scratch}",
                    "--report", "{report}", "--endpoint", "{endpoint}", "--node", node,
                    "--engine", str(client), "--trace", str(trace), "--fixture", str(fixture)]
            if args.candidate:
                argv.extend(["--candidate", str(args.candidate.resolve())])
            assertion = "suite_ran" if step == "prepare" else step
            return {"command": argv, "expect": {"/assertions/" + assertion: True}}

        case = {"wallet": "cake", "version": "6.5.4-cb3e96f1", "variant": "internal-enabled",
                "network": "zcash-regtest", "implementation": "engine", "interface": "lightwalletd",
                "endpoint": endpoint, "require_no_fallback": True, "restart_scope": "process-restart",
                "client": {"path": str(client), "sha256": inputs[str(client)]}, "inputs": inputs,
                "prepare": command("prepare"), "steps": {step: command(step) for step in gate["STEPS"]}}
        manifest = {"schema_version": 2, "timeout_seconds": 360, "cases": {"cake": case}}
        gate["validate"](manifest)
        os.umask(0o077)
        with args.output.open("x") as output:
            json.dump(manifest, output, indent=2)
            output.write("\n")
    except (OSError, ValueError, KeyError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
