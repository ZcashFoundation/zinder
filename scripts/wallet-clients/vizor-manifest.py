#!/usr/bin/env python3
"""Pin the Vizor Rust engine and local fixture for the wallet lifecycle runner."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys


def sha256(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", required=True, type=Path)
    parser.add_argument("--engine-provenance", required=True, type=Path)
    parser.add_argument("--fixture", required=True, type=Path, help="v0.21.0 Regtest ready.json")
    parser.add_argument("--candidate", type=Path, help="optional patched compatibility endpoint receipt")
    parser.add_argument("--trace-tool", required=True, type=Path)
    parser.add_argument("--evidence", required=True, type=Path, help="fresh directory for private process traces")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    source = json.loads(args.engine_provenance.read_text())
    if source["vizor_source"] != "d0edb5b13890dd33d9bb93b5c4e50fd9973fa4e6" or source["vizor_release"] != "v0.0.65":
        parser.error("expected the v0.0.65 engine provenance")
    if source["engine_sha256"] != sha256(args.engine):
        parser.error("engine binary differs from its build provenance")
    fixture = json.loads(args.fixture.read_text())
    if fixture["network"] != "zcash-regtest":
        parser.error("expected a Regtest fixture")
    if args.evidence.exists() or args.output.exists():
        parser.error("manifest and private evidence paths must be fresh")
    interpreter = Path(sys.executable).resolve()
    adapter = Path(__file__).with_name("vizor-regtest.py").resolve()
    driver = adapter.with_suffix(".rs")
    inputs = [interpreter, args.engine, args.engine_provenance, args.fixture, args.trace_tool,
              adapter, driver, Path(__file__)]
    endpoint = fixture["lightwalletd"]
    command = [str(interpreter), "-O", str(adapter), "--engine", str(args.engine.resolve()),
               "--fixture", str(args.fixture.resolve()), "--trace-tool", str(args.trace_tool.resolve()),
               "--evidence", str(args.evidence.resolve()), "--scratch", "{scratch}",
               "--report", "{report}", "--endpoint", "{endpoint}"]
    if args.candidate:
        inputs.append(args.candidate)
        command += ["--candidate", str(args.candidate.resolve())]
        endpoint = json.loads(args.candidate.read_text())["client_endpoint"]
    inputs = {str(path.resolve()): sha256(path) for path in inputs}
    steps = ["bootstrap", "restore", "transparent-discovery", "shielded-discovery",
             "send", "pending", "mined", "scanned", "restart", "resume"]
    case = {"wallet": "vizor", "version": "v0.0.65", "variant": "rust-engine", "network": "zcash-regtest",
            "implementation": "engine", "interface": "lightwalletd", "endpoint": endpoint,
            "require_no_fallback": True, "restart_scope": "process-restart",
            "client": {"path": str(args.engine.resolve()), "sha256": inputs[str(args.engine.resolve())]},
            "inputs": inputs,
            "steps": {step: {"command": command + ["--step", step], "expect": {"/assertions/" + step: True}}
                      for step in steps}}
    os.umask(0o077)
    with args.output.open("x") as output:
        json.dump({"schema_version": 2, "timeout_seconds": 240, "cases": {"vizor": case}}, output, indent=2)
        output.write("\n")


if __name__ == "__main__":
    main()
