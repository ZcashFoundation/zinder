#!/usr/bin/env python3
"""Build and drive the Zally typed headless wallet as a counterparty payer or payee.

The harness is a small out-of-tree crate compiled against the pinned Zally
source tree. It keeps the Zally workspace untouched. Import `Zally` from another
script, or run the CLI form for one step.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def build(source, build_dir, harness):
    source = Path(source).resolve()
    build_dir = Path(build_dir).resolve()
    require((source / "crates/zally-wallet/Cargo.toml").is_file(),
            "Zally source path does not contain crates/zally-wallet")
    build_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    (build_dir / "src").mkdir(exist_ok=True)
    manifest = build_dir / "Cargo.toml"
    manifest.write_text(f"""[package]
name = "zally-payment-harness"
version = "0.1.0"
edition = "2024"

[workspace]

[dependencies]
zally-wallet = {{ path = "{source}/crates/zally-wallet", features = ["zinder"] }}
zally-chain = {{ path = "{source}/crates/zally-chain", features = ["zinder"] }}
zally-core = {{ path = "{source}/crates/zally-core" }}
zally-keys = {{ path = "{source}/crates/zally-keys" }}
zally-storage = {{ path = "{source}/crates/zally-storage" }}
tokio = {{ version = "1", features = ["macros", "rt-multi-thread", "sync", "time"] }}
tokio-stream = "0.1"
serde_json = "1.0"
zcash_protocol = {{ version = "=0.11.0", features = ["local-consensus"] }}
""")
    if not (build_dir / "Cargo.lock").is_file():
        shutil.copy2(source / "Cargo.lock", build_dir / "Cargo.lock")
    shutil.copy2(harness, build_dir / "src/main.rs")
    binary = build_dir / "target/debug/zally-payment-harness"
    if not binary.is_file():
        command = ["cargo", "build", "--locked", "--manifest-path", str(manifest)]
        if (build_dir / "Cargo.lock").is_file():
            command.insert(2, "--offline")
        subprocess.run(command, cwd=build_dir, check=True)
    require(binary.is_file(), "Zally payment harness build produced no binary")
    return binary


class Zally:
    def __init__(self, binary, wallet_dir, endpoint, node, name):
        self.binary = str(Path(binary).resolve())
        self.directory = Path(wallet_dir).resolve()
        self.endpoint, self.node, self.name = endpoint, node, name
        require(endpoint.startswith("http://127.0.0.1:") and node.startswith("http://127.0.0.1:"),
                "explicit loopback endpoints required")
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        (self.directory / "raw").mkdir(mode=0o700, exist_ok=True)
        self.sequence = 0

    def _run(self, command, *arguments):
        self.sequence += 1
        label = f"{self.sequence:02}-{command}"
        result = subprocess.run([self.binary, command, str(self.directory), self.endpoint,
                                 self.node, *map(str, arguments)],
                                capture_output=True, text=True, timeout=900)
        (self.directory / "raw" / (label + ".stdout")).write_text(result.stdout)
        (self.directory / "raw" / (label + ".stderr")).write_text(result.stderr)
        require(result.returncode == 0, f"zally harness command {command} failed: {result.stderr[-400:]}")
        lines = [line for line in result.stdout.splitlines() if line.strip()]
        require(lines, "zally harness produced no report")
        return json.loads(lines[-1])

    def prepare(self):
        if not (self.directory / "wallet.db").exists():
            created = self._run("create")
            (self.directory / "address").write_text(created["unified"] + "\n")
            return created["unified"]
        # Deriving again would advance the address index; reuse the persisted payee.
        return (self.directory / "address").read_text().strip()

    def snapshot(self):
        snapshot = self._run("read")
        snapshot["wallet"] = "zally"
        return snapshot

    def sync(self):
        snapshot = self._run("sync")
        snapshot["wallet"] = "zally"
        return snapshot

    def send(self, address, amount_zat, memo):
        require(type(amount_zat) is int and amount_zat > 0, "invalid amount")
        return self._run("send", address, amount_zat, memo or "-")["txid"]

    def detail(self, txid):
        rows = [row for row in self.snapshot()["receives"] if row["txid"] == txid]
        require(len(rows) == 1, "transaction absent or duplicated in wallet receives")
        return rows[0]

    def restart(self):
        return self.snapshot()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, help="pinned Zally checkout")
    parser.add_argument("--build", type=Path, help="private harness build directory")
    parser.add_argument("--harness", type=Path, default=Path(__file__).with_name("zally-payment.rs"))
    parser.add_argument("--binary", type=Path, help="existing harness binary")
    parser.add_argument("--wallet", type=Path)
    parser.add_argument("--endpoint")
    parser.add_argument("--node")
    parser.add_argument("command", choices=["build", "prepare", "snapshot", "sync", "send", "detail"])
    parser.add_argument("args", nargs="*")
    args = parser.parse_args()
    if args.command == "build":
        require(args.source and args.build, "build requires --source and --build")
        binary = build(args.source, args.build, args.harness)
        print(json.dumps({"binary": str(binary), "sha256": digest(binary)}))
        return
    require(args.binary and args.wallet and args.endpoint and args.node,
            "wallet commands require --binary --wallet --endpoint --node")
    wallet = Zally(args.binary, args.wallet, args.endpoint, args.node, "counterparty")
    if args.command == "prepare":
        result = {"address": wallet.prepare()}
    elif args.command == "snapshot":
        result = wallet.snapshot()
    elif args.command == "sync":
        result = wallet.sync()
    elif args.command == "send":
        require(len(args.args) == 3, "send ADDRESS ATOMS MEMO-OR-DASH")
        memo = None if args.args[2] == "-" else args.args[2]
        result = {"txid": wallet.send(args.args[0], int(args.args[1]), memo)}
    else:
        require(len(args.args) == 1, "detail TXID")
        result = wallet.detail(args.args[0])
    print(json.dumps(result))


if __name__ == "__main__":
    os.umask(0o077)
    main()
