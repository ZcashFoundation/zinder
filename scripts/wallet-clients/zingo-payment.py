#!/usr/bin/env python3
"""Known-good independent Zingo CLI counterparty for the payment-role cases.

Reuses the pinned Zingo adapter from `roundtrip_wallets.py` and adds funding of
one Regtest coinbase plus a shield, and a send that may target a Sapling or
Unified address.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from urllib.request import ProxyHandler, Request, build_opener

sys.path.insert(0, str(Path(__file__).resolve().parent))

from roundtrip_wallets import Zingo, require, valid_txid  # noqa: E402


class PinnedZingo(Zingo):
    """The pinned adapter with a longer deadline for a fresh post-mining scan."""

    def _cli(self, *args, online=False, timeout=900):
        if (self.directory / "receiving-address").exists():
            require((self.directory / "zingo-wallet.dat").is_file(), "persisted Zingo wallet file missing")
        argv = [self.binary, "--chain", self.network, "--data-dir", str(self.directory),
                "--birthday", str(self.birthday)]
        argv += ["--server", self.endpoint, "--waitsync"] if online else ["--offline"]
        env = {k: v for k, v in os.environ.items() if not k.startswith("ZINGO_")}
        result = subprocess.run([*argv, *args], capture_output=True, text=True, env=env,
                                timeout=timeout)
        require(result.returncode == 0 and "error: sync error" not in result.stderr.lower(),
                f"Zingo command failed: {args[0]} (exit {result.returncode}): {result.stderr[-300:]}")
        return result.stdout


class Counterparty:
    def __init__(self, binary, directory, endpoint, node, network="regtest", birthday=2, label="zingo"):
        self.wallet = PinnedZingo(binary, directory, network, endpoint, birthday)
        self.node = node
        self.label = label
        self.directory = Path(directory).resolve()
        self.opener = build_opener(ProxyHandler({}))
        require(node.startswith("http://127.0.0.1:"), "explicit loopback node endpoint required")

    def rpc(self, method, *params):
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
        request = Request(self.node, body, {"Content-Type": "application/json"})
        with self.opener.open(request, timeout=120) as response:
            answer = json.load(response)
        require(not answer.get("error"), "node RPC failed: " + method)
        return answer["result"]

    def fund(self, confirmations=101, maturity_blocks=10):
        # The CLI creates an offline wallet without a scan state; its first
        # online sync then never advances. Create it online first.
        self.wallet.prepare()
        address = self.wallet.transparent_address()
        [block] = self.rpc("generatetoaddress", 1, address)
        identity = self.rpc("getblock", block, 1)
        require(identity["height"] == self.rpc("getblockcount"), "unexpected funding height")
        self.rpc("generate", confirmations)
        target = self.rpc("getblockcount")
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            snapshot = self.wallet.snapshot()
            if snapshot["height"] >= target and snapshot["total_zat"] > 0:
                break
            time.sleep(3)
        else:
            raise ValueError("Zingo did not discover its transparent coinbase")
        shield = self.wallet.shield()
        self.rpc("generate", maturity_blocks)
        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            snapshot = self.wallet.snapshot()
            if any(row["txid"] == shield and row["height"] for row in snapshot["transactions"]) \
                    and snapshot["spendable_zat"] > 1_000_000:
                return {"funding_txid": identity["tx"][0], "shield_txid": shield,
                        "funding_height": identity["height"], "spendable_zat": snapshot["spendable_zat"]}
            time.sleep(3)
        raise ValueError("Zingo shielded funding did not become spendable")

    def prepare(self):
        return self.wallet.prepare()

    def snapshot(self):
        result = self.wallet.snapshot()
        result["wallet"] = "zingo"
        return result

    def send(self, address, amount_zat, memo):
        require(type(amount_zat) is int and amount_zat > 0, "invalid amount")
        txid = self.wallet._broadcast("quicksend", address, str(amount_zat), memo)
        return valid_txid(txid)

    def detail(self, txid):
        rows = [row for row in self.snapshot()["transactions"] if row["txid"] == txid]
        require(len(rows) == 1, "transaction absent or duplicated in Zingo history")
        return rows[0]

    def restart(self):
        return self.wallet.snapshot(online=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", required=True, type=Path)
    parser.add_argument("--wallet", required=True, type=Path)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--node", required=True)
    parser.add_argument("--birthday", type=int, default=2)
    parser.add_argument("command", choices=["prepare", "fund", "snapshot", "send", "detail"])
    parser.add_argument("args", nargs="*")
    args = parser.parse_args()
    wallet = Counterparty(args.client, args.wallet, args.endpoint, args.node, birthday=args.birthday)
    if args.command == "prepare":
        result = {"address": wallet.prepare()}
    elif args.command == "fund":
        result = wallet.fund()
    elif args.command == "snapshot":
        result = wallet.snapshot()
    elif args.command == "send":
        require(len(args.args) == 3, "send ADDRESS ATOMS MEMO-OR-DASH")
        result = {"txid": wallet.send(args.args[0], int(args.args[1]), args.args[2])}
    else:
        require(len(args.args) == 1, "detail TXID")
        result = wallet.detail(args.args[0])
    print(json.dumps(result))


if __name__ == "__main__":
    os.umask(0o077)
    main()
