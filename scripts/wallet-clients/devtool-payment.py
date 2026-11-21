#!/usr/bin/env python3
"""Drive the pinned zcash-devtool Sapling CLI as a counterparty payer or payee.

The adapter owns one disposable wallet directory, reads the wallet's own
persisted view for fees, memos, and history, and never signs or sends outside
the selected loopback fixture. Import `Devtool` from another script or run the
CLI form for a manual step.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import time
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, Request, build_opener


def require(condition, message):
    if not condition:
        raise ValueError(message)


def tx_digest(txid):
    require(re.fullmatch(r"[0-9a-f]{64}", txid), "invalid transaction id")
    return txid


def decode_memo(blob):
    if blob is None:
        return None
    # The SQLite view omits trailing zero padding, including on the ZIP 302 empty marker.
    raw = bytes(blob).rstrip(b"\x00")
    if raw == b"\xf6":
        return None
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return {"non_text": True}


class Devtool:
    def __init__(self, client, wallet_dir, endpoint, node, name, trace_commands=False,
                 trace_script=None):
        self.client = str(Path(client).resolve())
        self.directory = Path(wallet_dir).resolve()
        self.endpoint = urlsplit(endpoint)
        self.node = node
        self.name = name
        self.identity = self.directory / "identity"
        self.heights = self.directory / "activations.toml"
        self.opener = build_opener(ProxyHandler({}))
        require(self.endpoint.scheme == "http" and self.endpoint.hostname == "127.0.0.1"
                and self.endpoint.port, "loopback lightwalletd endpoint required")
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.raw = self.directory / "raw"
        self.raw.mkdir(mode=0o700, exist_ok=True)
        self.sequence = 0
        self.trace_commands = trace_commands
        self.trace_script = Path(trace_script) if trace_script else \
            Path(__file__).resolve().parents[1] / "trace-wallet-endpoint.py"
        if self.trace_commands:
            require(self.trace_script.is_file(), "connect trace helper is missing")
            self.traces = self.directory / "endpoint-traces"
            self.traces.mkdir(mode=0o700, exist_ok=True)
        self.online_commands = 0
        self.trace_reports = []

    def rpc(self, method, *params):
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
        request = Request(self.node, body, {"Content-Type": "application/json"})
        with self.opener.open(request, timeout=120) as response:
            answer = json.load(response)
        require(not answer.get("error"), "node RPC failed: " + method)
        return answer["result"]

    def _cli(self, *command, online=False):
        self.sequence += 1
        label = f"{self.sequence:02}-{command[0]}"
        argv = [self.client, "wallet", "--wallet-dir", str(self.directory), *map(str, command)]
        if online:
            argv.extend(["--server", f"{self.endpoint.hostname}:{self.endpoint.port}",
                         "--connection", "direct"])
        if online and self.trace_commands:
            self.online_commands += 1
            result = self._traced(label, argv)
        else:
            result = subprocess.run(argv, capture_output=True, text=True, timeout=600)
        (self.raw / (label + ".stdout")).write_text(result.stdout)
        (self.raw / (label + ".stderr")).write_text(result.stderr)
        require(result.returncode == 0, f"devtool command {command[0]} failed")
        return result.stdout.strip()

    def _traced(self, label, argv):
        """Run one online command under the connect-syscall tracer."""
        output = self.traces / label
        traced = [sys.executable, str(self.trace_script),
                  "--endpoint-port", str(self.endpoint.port), "--output", str(output), "--", *argv]
        result = subprocess.run(traced, capture_output=True, text=True, timeout=600)
        require(result.returncode == 0, f"connect trace for {label} did not pass")
        report = json.loads((output / "report.json").read_text())
        require(report.get("status") == "passed"
                and report.get("endpoint") == f"loopback:{self.endpoint.port}"
                and report.get("connections")
                and report.get("fallback_used") is False,
                f"connect trace for {label} is not bound to the wallet endpoint")
        self.trace_reports.append({
            "command": label.split("-", 1)[1],
            "report_sha256": hashlib.sha256((output / "report.json").read_bytes()).hexdigest(),
            "connect_log_sha256": hashlib.sha256(
                (output / "connect.log").read_bytes()).hexdigest(),
            "connections": len(report["connections"]),
            "endpoint": report["endpoint"],
        })
        return result

    def observe_server(self):
        """Report the server and chain the wallet itself observes."""
        info = json.loads(self._cli("get-info", online=True))
        require(info.get("chain_name") == "test", "wallet reports another chain")
        require(info.get("server_uri") == f"http://{self.endpoint.hostname}:{self.endpoint.port}",
                "wallet reports another server")
        return info

    def endpoint_binding(self):
        """Summarize the per-command connect traces for the receipt."""
        return {
            "scope": "Linux process tree connect syscalls per online CLI invocation; "
                     "no payload capture or method attribution",
            "online_commands": self.online_commands,
            "traced_commands": len(self.trace_reports),
            "bound": (self.online_commands > 0
                      and len(self.trace_reports) == self.online_commands),
            "endpoint": f"{self.endpoint.hostname}:{self.endpoint.port}",
            "reports": self.trace_reports,
        }

    def prepare(self):
        if not (self.directory / "data.sqlite").exists():
            info = self.rpc("getblockchaininfo")
            upgrades = {value["name"].lower().replace(".", "_"): value["activationheight"]
                        for value in info["upgrades"].values()}
            require("nu6_3" in upgrades and "nu7" not in upgrades,
                    "fixture must activate through NU6.3 with NU7 disabled")
            self.heights.write_text("".join(f"{key} = {value}\n" for key, value in upgrades.items()))
            (self.directory / "birthday").write_text("2\n")
            self._cli("init", "--name", self.name, "--identity", self.identity, "--birthday", "2",
                      "--network", "regtest", "--activation-heights", self.heights, online=True)
            self.sync()
        return self.address("sapling")

    def address(self, pool):
        line = self._cli("list-addresses", "--receiver", pool)
        prefix = f"Receiver({pool}): "
        require(line.startswith(prefix) and len(line.splitlines()) == 1, "missing receiver")
        return line[len(prefix):]

    def payee_address(self):
        line = self._cli("list-addresses", "--receiver", "unified")
        match = re.search(r"Default Address: (uregtest1[0-9a-z]+)", line)
        require(match is not None, "missing unified receiver")
        return match.group(1)

    def balance(self):
        return json.loads(self._cli("balance", "--min-confirmations", "1", "--json"))

    def scanned_height(self):
        path = (self.directory / "data.sqlite").resolve().as_uri()
        with sqlite3.connect(path + "?mode=ro", uri=True) as connection:
            return connection.execute("SELECT MAX(height) FROM blocks").fetchone()[0]

    def sync(self, timeout=240):
        target = self.rpc("getblockcount")
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self._cli("sync", online=True)
            try:
                balance = self.balance()
            except (ValueError, KeyError):
                time.sleep(2)
                continue
            if balance["chain_tip_height"] == target and self.scanned_height() == target:
                return self.snapshot()
            time.sleep(2)
        raise ValueError("devtool sync did not reach the node tip")

    def transactions(self):
        path = (self.directory / "data.sqlite").resolve().as_uri()
        with sqlite3.connect(path + "?mode=ro", uri=True) as connection:
            connection.row_factory = sqlite3.Row
            memos, outputs = {}, {}
            for row in connection.execute(
                    "SELECT txid, output_pool, output_index, value, is_change, memo "
                    "FROM v_tx_outputs ORDER BY txid, output_index"):
                txid = bytes(row["txid"])[::-1].hex()
                memo = decode_memo(row["memo"])
                outputs.setdefault(txid, []).append({"pool": row["output_pool"],
                                                     "output_index": row["output_index"],
                                                     "value": row["value"],
                                                     "is_change": bool(row["is_change"]),
                                                     "memo": memo})
                if memo is not None:
                    memos.setdefault(txid, []).append(memo)
            rows = []
            for row in connection.execute(
                    "SELECT txid, mined_height, account_balance_delta, fee_paid, "
                    "expired_unmined, sent_note_count, received_note_count FROM v_transactions"):
                txid = tx_digest(bytes(row["txid"])[::-1].hex())
                delta = row["account_balance_delta"]
                rows.append({"txid": txid, "height": row["mined_height"],
                             "pending": row["mined_height"] is None and not row["expired_unmined"],
                             "expired": bool(row["expired_unmined"]), "delta": delta,
                             "amount_zat": delta if delta > 0 else None,
                             "fee_zat": row["fee_paid"],
                             "sent_notes": row["sent_note_count"],
                             "received_notes": row["received_note_count"],
                             "memos": memos.get(txid, []), "outputs": outputs.get(txid, [])})
            return rows

    def snapshot(self):
        balance = self.balance()
        return {"wallet": "zcash-devtool", "address_sapling": self.address("sapling"),
                "total_zat": balance["total"], "sapling_zat": balance["sapling_spendable"],
                "transparent_zat": balance["transparent_spendable"],
                "height": balance["chain_tip_height"], "scanned": self.scanned_height(),
                "transactions": self.transactions()}

    def send(self, address, amount_zat, memo):
        require(type(amount_zat) is int and amount_zat > 0, "invalid amount")
        argv = ["send", "--identity", self.identity, "--address", address,
                "--value", str(amount_zat), "--min-confirmations", "1"]
        if memo:
            argv.extend(["--memo", memo])
        output = self._cli(*argv, online=True)
        txid = tx_digest(output.splitlines()[-1])
        return txid

    def detail(self, txid):
        rows = [row for row in self.transactions() if row["txid"] == txid]
        require(len(rows) == 1, "transaction absent or duplicated in wallet history")
        return rows[0]

    def restart(self):
        # Every CLI invocation opens the persisted wallet in a new process.
        return self.snapshot()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", required=True, type=Path)
    parser.add_argument("--wallet", required=True, type=Path)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--node", required=True)
    parser.add_argument("--name", default="counterparty")
    parser.add_argument("command", choices=["prepare", "snapshot", "sync", "send", "detail"])
    parser.add_argument("args", nargs="*")
    args = parser.parse_args()
    for option in ("endpoint", "node"):
        require(getattr(args, option).startswith("http://127.0.0.1:"),
                "explicit loopback endpoints required")
    wallet = Devtool(args.client, args.wallet, args.endpoint, args.node, args.name)
    if args.command == "prepare":
        result = {"address": wallet.prepare()}
    elif args.command == "snapshot":
        result = wallet.snapshot()
    elif args.command == "sync":
        wallet.sync()
        result = wallet.snapshot()
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
