#!/usr/bin/env python3
"""Run and audit a signed wallet round trip on two fresh, disposable Regtest fixtures."""
import argparse
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import sys


SCRIPTS = Path(__file__).resolve().parent
FIXTURE = SCRIPTS / "regtest-fixture.py"
WALLET = SCRIPTS / "run-wallet-roundtrip.py"
AUDIT = SCRIPTS / "wallet-clients/audit-wallet-roundtrip.py"
WALLET_PORT_OFFSET = 82


def check_ports(bases):
    offsets = (2, 5, 6, 7, 10, 32, 67, WALLET_PORT_OFFSET)
    ports = [base + offset for base in bases for offset in offsets]
    if min(bases) < 1024 or max(ports) > 65535 or len(set(ports)) != len(ports):
        raise ValueError("choose two distinct port bases with disjoint fixture and wallet ports")
    for port in ports:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", port))


def execute(script, args, log):
    cmd = [sys.executable, "-B", *(["-O"] * sys.flags.optimize), str(script), *map(str, args)]
    with log.open("x") as output, subprocess.Popen(
            cmd, stdout=output, stderr=subprocess.STDOUT, start_new_session=True) as proc:
        try:
            code = proc.wait()
        except KeyboardInterrupt:
            # Let the owner close its detached wallet process and Docker relay.
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGINT)
            proc.wait()
            raise
    if code:
        raise RuntimeError(f"{script.name} stopped with exit {code}. See {log}")


def campaign(args):
    state = args.state.resolve()
    check_ports(args.port_bases)
    state.mkdir(mode=0o700, parents=True, exist_ok=False)
    operator_args = ["--state-root", state / "projects"]
    owned = []
    audit_args = []
    receipts = []
    try:
        for label, base in zip(("first", "second"), args.port_bases, strict=True):
            project = "zinder-fixture-roundtrip-" + secrets.token_hex(8)
            fixture = state / "projects" / project
            owned.append((label, project, fixture))
            up = ["up", *operator_args, "--project", project, "--port-base", base, "--images", args.images,
                  "--revision", args.revision, "--node-config", SCRIPTS.parent / "deploy/zebra/regtest-nu6-at-one.toml",
                  "--raw-blob-policy", "all", "--tip", 150]
            if args.bin_dir:
                up.extend(["--bin-dir", args.bin_dir])
            execute(FIXTURE, up, state / f"{label}-up.log")
            ready = fixture / "ready.initial.json"
            wallet = state / label
            execute(WALLET, ["regtest", "--ready", ready, "--state", wallet, "--revision", args.revision,
                             "--zingo", args.zingo, "--zallet-backend", args.zallet_backend,
                             "--wallet-rpc-port", base + WALLET_PORT_OFFSET], state / f"{label}-wallet.log")
            receipts.append(wallet / "receipt.json")
            audit_args.extend(["--run", wallet, ready])
        execute(WALLET, ["compare", *receipts], state / "compare.log")
        (state / "compare.log").rename(state / "paired.json")
        execute(AUDIT, ["--revision", args.revision, *audit_args, "--output", state / "canonical-audit.json"],
                state / "audit.log")
    finally:
        statuses = {}
        for label, project, fixture in reversed(owned):
            if not (fixture / "state.json").is_file():
                statuses[label] = "no fixture started"
                continue
            try:
                execute(FIXTURE, ["down", *operator_args, "--project", project], state / f"{label}-down.log")
                statuses[label] = "removed"
            except (OSError, RuntimeError, KeyboardInterrupt) as error:
                statuses[label] = f"cleanup failed: {error}"
        (state / "cleanup.json").write_text(json.dumps(statuses, indent=2) + "\n")
        if any(v.startswith("cleanup failed") for v in statuses.values()):
            raise RuntimeError(f"fixture cleanup incomplete. See {state / 'cleanup.json'}")
    print(f"Two Regtest round trips passed and both fixtures were removed. Evidence: {state}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, required=True, help="new private evidence directory")
    parser.add_argument("--port-bases", nargs=2, type=int, required=True, metavar=("FIRST", "SECOND"))
    parser.add_argument("--images", type=Path, required=True, help="fixture operator image map")
    parser.add_argument("--bin-dir", type=Path, help="release binaries to mount over the image set")
    parser.add_argument("--revision", required=True, help="expected full Zinder release commit")
    parser.add_argument("--zingo", type=Path, required=True)
    parser.add_argument("--zallet-backend", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    def interrupt(*_):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupt)
    campaign(args)


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, ValueError) as error:
        sys.exit(f"Regtest round trip failed: {error}")
