#!/usr/bin/env python3
"""Drive the pinned Vizor v0.0.65 Rust engine as a counterparty payer or payee.

The engine driver is `vizor-payment.rs`, built as an example inside a private
copy of the pinned Vizor source. This adapter builds it on demand, or reuses an
existing engine binary, and keeps every wallet directory private.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tomllib


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def build(source, build_dir, driver, jobs=8):
    source = Path(source).resolve()
    build_dir = Path(build_dir).resolve()
    root = source / "rust" if (source / "rust/Cargo.toml").is_file() else source
    require((root / "Cargo.toml").is_file() and (root / "examples").is_dir(),
            "Vizor source path does not contain the rust crate")
    (build_dir / "target").mkdir(mode=0o700, parents=True, exist_ok=True)
    manifest = root / "Cargo.toml"
    if "workspace" not in tomllib.loads(manifest.read_text()):
        with manifest.open("a") as f:
            f.write("\n[workspace]\n")
    target = root / "examples/vizor-payment.rs"
    contents = Path(driver).read_bytes()
    if not target.is_file() or target.read_bytes() != contents:
        target.write_bytes(contents)
    result = subprocess.run(["cargo", "build", "--locked", "--example", "vizor-payment",
                             "--message-format=json-render-diagnostics", "--jobs", str(jobs),
                             "--manifest-path", str(manifest), "--target-dir", str(build_dir / "target")],
                            cwd=root, check=True, stdout=subprocess.PIPE, text=True)
    require(target.read_bytes() == contents == Path(driver).read_bytes(),
            "Vizor payment driver changed during compilation")
    executables = [Path(item["executable"]) for line in result.stdout.splitlines()
                   if (item := json.loads(line))["reason"] == "compiler-artifact" and item.get("executable")
                   and item["target"]["name"] == "vizor-payment" and "example" in item["target"]["kind"]]
    require(len(executables) == 1 and executables[0].is_file(), "Vizor payment driver build produced no binary")
    return executables[0]


class Vizor:
    def __init__(self, engine, wallet_dir, endpoint, activation, revision, anchor_height,
                 anchor_hash):
        self.engine = str(Path(engine).resolve())
        self.directory = Path(wallet_dir).resolve()
        self.endpoint, self.activation = endpoint, str(activation)
        self.revision, self.anchor_height, self.anchor_hash = revision, str(anchor_height), anchor_hash
        require(endpoint.startswith("http://127.0.0.1:"), "explicit loopback endpoint required")
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        (self.directory / "raw").mkdir(mode=0o700, exist_ok=True)
        self.sequence = 0

    def _run(self, command, *arguments):
        self.sequence += 1
        label = f"{self.sequence:02}-{command}"
        argv = [self.engine, command, str(self.directory), self.endpoint, self.activation,
                self.revision, self.anchor_height, self.anchor_hash, *map(str, arguments)]
        result = subprocess.run(argv, capture_output=True, text=True, timeout=900)
        (self.directory / "raw" / (label + ".stdout")).write_text(result.stdout)
        (self.directory / "raw" / (label + ".stderr")).write_text(result.stderr)
        require(result.returncode == 0, f"vizor engine command {command} failed: {result.stderr[-400:]}")
        lines = [line for line in result.stdout.splitlines() if line.strip()]
        require(lines, "vizor engine produced no report")
        return json.loads(lines[-1])

    def prepare(self):
        if not (self.directory / "original.sqlite").exists():
            created = self._run("create")
            (self.directory / "address").write_text(created["unified"] + "\n")
            (self.directory / "account").write_text(created["account"] + "\n")
            return created["unified"]
        return (self.directory / "address").read_text().strip()

    def snapshot(self):
        snapshot = self._run("read-original")
        return self._normalize(snapshot)

    def sync(self):
        return self._normalize(self._run("sync-original"))

    @staticmethod
    def _normalize(snapshot):
        for row in snapshot["history"]:
            # The engine stores wallet database byte order; the node displays the reverse.
            row["txid"] = bytes.fromhex(row["txid"])[::-1].hex()
        snapshot["total_zat"] = sum(snapshot.get(pool, 0)
                                    for pool in ("transparent", "sapling", "orchard", "ironwood"))
        snapshot["wallet"] = "vizor"
        return snapshot

    def send(self, address, amount_zat, memo):
        require(type(amount_zat) is int and amount_zat > 0, "invalid amount")
        sent = self._run("send", address, amount_zat, memo or "-")
        txids = sent["txids"]
        if isinstance(txids, str):
            txids = [txid for txid in txids.split(",") if txid]
        require(len(txids) == 1, "expected one Vizor transaction")
        return txids[0], sent

    def detail(self, txid):
        return self._run("detail", txid)

    def restart(self):
        return self.snapshot()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, help="pinned Vizor source checkout")
    parser.add_argument("--build", type=Path, help="private build directory")
    parser.add_argument("--driver", type=Path, default=Path(__file__).with_name("vizor-payment.rs"))
    parser.add_argument("--engine", type=Path, help="existing engine binary")
    parser.add_argument("--wallet", type=Path)
    parser.add_argument("--endpoint")
    parser.add_argument("--activation", type=int, help="NU6.3 activation height")
    parser.add_argument("--revision")
    parser.add_argument("--anchor-height", type=int)
    parser.add_argument("--anchor-hash")
    parser.add_argument("command", choices=["build", "prepare", "snapshot", "sync", "send", "detail"])
    parser.add_argument("args", nargs="*")
    args = parser.parse_args()
    if args.command == "build":
        require(args.source and args.build, "build requires --source and --build")
        binary = build(args.source, args.build, args.driver)
        print(json.dumps({"binary": str(binary), "sha256": digest(binary)}))
        return
    require(args.engine and args.wallet and args.endpoint and args.activation is not None
            and args.revision and args.anchor_height is not None and args.anchor_hash,
            "wallet commands require engine, wallet, endpoint, and anchor pins")
    wallet = Vizor(args.engine, args.wallet, args.endpoint, args.activation, args.revision,
                   args.anchor_height, args.anchor_hash)
    if args.command == "prepare":
        result = {"address": wallet.prepare()}
    elif args.command == "snapshot":
        result = wallet.snapshot()
    elif args.command == "sync":
        result = wallet.sync()
    elif args.command == "send":
        require(len(args.args) == 3, "send ADDRESS ATOMS MEMO-OR-DASH")
        memo = None if args.args[2] == "-" else args.args[2]
        txid, sent = wallet.send(args.args[0], int(args.args[1]), memo)
        result = {"txid": txid, "fee_zatoshi": sent.get("fee_zatoshi")}
    else:
        require(len(args.args) == 1, "detail TXID")
        result = wallet.detail(args.args[0])
    print(json.dumps(result))


if __name__ == "__main__":
    os.umask(0o077)
    main()
