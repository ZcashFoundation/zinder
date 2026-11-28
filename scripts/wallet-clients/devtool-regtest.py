#!/usr/bin/env python3
"""Exercise the zcash-devtool wallet CLI on one disposable Zebra Regtest chain."""

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
from wallet_fixture import binding

STEPS = ("bootstrap", "restore", "transparent-discovery", "shielded-discovery",
         "send", "pending", "mined", "scanned", "restart", "resume")
REGTEST_GENESIS = "029f11d80ef9765602235e1bc9727e3eb6ba20839319f761fee920d63401e327"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(value):
    return hashlib.sha256(value).hexdigest()


def tx_digest(txid):
    require(re.fullmatch(r"[0-9a-f]{64}", txid), "invalid transaction id")
    return digest(bytes.fromhex(txid))


def write_json(path, data):
    path.write_text(json.dumps(data, indent=2) + "\n")


def loopback(url):
    parts = urlsplit(url)
    require(parts.scheme == "http" and parts.hostname == "127.0.0.1" and
            parts.port and parts.netloc == f"127.0.0.1:{parts.port}" and
            parts.path in ("", "/") and not parts.query and not parts.fragment,
            "a single explicit plaintext loopback endpoint is required")
    return parts


class Wallet:
    def __init__(self, args):
        self.args = args
        self.server = loopback(args.endpoint)
        loopback(args.node)
        self.db = args.scratch / "restored"
        self.source = args.scratch / "source"
        self.key = args.scratch / "identity"
        self.heights = args.scratch / "activations.toml"
        self.raw = args.evidence / args.step
        self.raw.mkdir(parents=True, mode=0o700)
        self.state_path = args.scratch / "state.json"
        self.state = json.loads(self.state_path.read_text()) if self.state_path.exists() else {}
        self.opener = build_opener(ProxyHandler({}))
        self.sequence = 0
        self.last_pid = None
        self.traces = []
        self.online_connections = 0

    def cli(self, *command, db=None, online=False, phrase="", secret=False):
        db = db or self.db
        self.sequence += 1
        label = f"{self.sequence:02}-{command[0]}"
        argv = [str(self.args.client), "wallet", "--wallet-dir", str(db), *map(str, command)]
        trace = self.raw / (label + "-trace")
        if online:
            argv.extend(["--server", self.server.netloc, "--connection", "direct"])
            argv = [sys.executable, str(self.args.trace_tool), "--endpoint-port",
                    str(self.server.port), "--output", str(trace), "--", *argv]
        else:
            trace.mkdir()
            # A detached tracer leaves the wallet as our direct child, preserving its true PID.
            argv = ["strace", "-D", "-f", "-e", "trace=connect", "-o", str(trace / "connect.log"),
                    "--", *argv]
        with subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, text=True) as child:
            self.last_pid = child.pid
            stdout, stderr = child.communicate(phrase + "\n")
            if not secret:
                (self.raw / (label + ".stdout")).write_text(stdout)
                (self.raw / (label + ".stderr")).write_text(stderr)
            require(child.returncode == 0, f"wallet command {command[0]} failed")
        trace_path = trace / "report.json"
        if online:
            trace_result = json.loads(trace_path.read_text())
            require(trace_result["status"] == "passed" and trace_result["fallback_used"] is False,
                    "wallet connection did not stay on the selected endpoint")
            self.online_connections += len(trace_result["connections"])
        else:
            raw = (trace / "connect.log").read_text()
            require("sa_family=AF_INET" not in raw, "wallet-local operation opened an Internet socket")
            require(re.search(rf"^{self.last_pid}\s+\+\+\+ exited with 0 \+\+\+", raw, re.MULTILINE),
                    "wallet-local trace did not record the wallet process exit")
            write_json(trace_path, {"scope": "local-only", "wallet_process_id": self.last_pid,
                                    "unexpected_connections": 0, "target_connected": False,
                                    "connect_log_sha256": digest(raw.encode())})
        self.traces.append(digest(trace_path.read_bytes()))
        return stdout.strip()

    def rpc(self, method, params=()):
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
        req = Request(self.args.node, data=body.encode(), headers={"Content-Type": "application/json"})
        with self.opener.open(req, timeout=120) as rsp:
            result = json.load(rsp)
        require(not result.get("error"), f"node {method} failed")
        if method in {"generate", "generatetoaddress", "getrawmempool", "getblock"}:
            write_json(self.raw / f"node-{time.time_ns()}-{method}.json", result)
        return result["result"]

    def verify_chain(self):
        _, _, height, block_hash = binding(self.args.ready, self.args.candidate,
                                           self.args.endpoint, self.args.node)
        require(self.rpc("getblockhash", [height]) == block_hash, "fixture block hash mismatch")
        require(self.rpc("getblockhash", [0]) == REGTEST_GENESIS, "expected a Zebra Regtest chain")
        info = self.rpc("getblockchaininfo")
        upgrades = {v["name"].lower().replace(".", "_"): v["activationheight"]
                    for v in info["upgrades"].values()}
        expected = "overwinter sapling blossom heartwood canopy nu5 nu6 nu6_1 nu6_2 nu6_3".split()
        require(upgrades == dict.fromkeys(expected, 1), "expected Regtest upgrades through NU6.3 at one")
        if self.args.step == "bootstrap":
            self.heights.write_text("".join(f"{k} = {v}\n" for k, v in upgrades.items()))

    def init(self, db, phrase=""):
        db.mkdir(mode=0o700)
        self.cli("init", "--name", "qualification", "--identity", self.key, "--birthday", "2",
                 "--network", "regtest", "--activation-heights", self.heights,
                 db=db, online=True, phrase=phrase)

    def receiver(self, pool, db=None):
        line = self.cli("list-addresses", "--receiver", pool, db=db)
        prefix = f"Receiver({pool}): "
        require(line.startswith(prefix) and len(line.splitlines()) == 1, "missing receiver")
        return line[len(prefix):]

    def balance(self, db=None):
        return json.loads(self.cli("balance", "--min-confirmations", "1", "--json", db=db))

    def scanned_height(self, db=None):
        path = ((db or self.db) / "data.sqlite").resolve().as_uri()
        with sqlite3.connect(path + "?mode=ro", uri=True) as connection:
            return connection.execute("SELECT MAX(height) FROM blocks").fetchone()[0]

    def sync(self, db=None):
        target = self.rpc("getblockcount")
        deadline = time.monotonic() + 150
        while time.monotonic() < deadline:
            self.cli("sync", db=db, online=True)
            balance = self.balance(db)
            if balance["chain_tip_height"] == target and self.scanned_height(db) == target:
                return balance
            time.sleep(2)
        raise ValueError("wallet sync did not reach the frozen expected height")

    def history(self, txid, db=None):
        rows = json.loads(self.cli("list-tx", "--json", db=db))
        matches = [r for r in rows if r["txid"] == txid]
        require(len(matches) == 1, "wallet history does not contain the expected transaction")
        return matches[0]

    def coinbase(self, address):
        [block_hash] = self.rpc("generatetoaddress", [1, address])
        block = self.rpc("getblock", [block_hash, 1])
        return {"txid": block["tx"][0], "height": block["height"]}

    def receipt(self):
        step = self.args.step
        result = {"status": "completed", "endpoint": self.args.endpoint, "fallback_used": False,
                  "assertions_enabled": True, "assertion_mode": "always-on", "assertions": {step: True}}
        state = self.state
        if step == "bootstrap":
            self.init(self.source)
            state["transparent"] = self.receiver("transparent", self.source)
            state["sapling"] = self.receiver("sapling", self.source)
            for pool in ("transparent", "sapling"):
                state[f"{pool}_fund"] = self.coinbase(state[pool])
            self.rpc("generate", [100])
            # Keep a commitment at the tip for the SDK’s exact-height shielded anchor lookup.
            self.coinbase(self.receiver("orchard", self.source))
            self.sync(self.source)
            expected = state["sapling_fund"]
            require(self.history(expected["txid"], self.source)["mined_height"] == expected["height"],
                    "funded source wallet did not see its Sapling coinbase")
        elif step == "restore":
            mnemonic = self.cli("display-mnemonic", "--identity", self.key, "--enable",
                                db=self.source, secret=True)
            require(len(mnemonic.split()) == 24, "invalid recovery phrase")
            self.init(self.db, mnemonic)
            self.sync()
            source_address = self.receiver("sapling", self.source)
            target_address = self.receiver("sapling")
            expected = state["sapling_fund"]
            old = self.history(expected["txid"], self.source)
            new = self.history(expected["txid"])
            require(source_address == target_address and old == new and new["mined_height"] == expected["height"],
                    "restored wallet identity or funded activity differs")
            result["restore"] = {
                "fresh_database": True,
                "source_database_fingerprint_sha256": digest(str((self.source / "data.sqlite").resolve()).encode()),
                "restored_database_fingerprint_sha256": digest(str((self.db / "data.sqlite").resolve()).encode()),
                "source_wallet_fingerprint_sha256": digest(source_address.encode()),
                "restored_wallet_fingerprint_sha256": digest(target_address.encode()),
                "source_activity_fingerprint_sha256": tx_digest(old["txid"]),
                "restored_activity_fingerprint_sha256": tx_digest(new["txid"]),
                "restored_activity_count": 1,
            }
        elif step in ("transparent-discovery", "shielded-discovery"):
            pool = "transparent" if step == "transparent-discovery" else "sapling"
            funding = state[f"{pool}_fund"]
            require(self.history(funding["txid"])["mined_height"] == funding["height"],
                    "wallet did not discover its funding transaction")
            value = self.balance()[pool + "_spendable"]
            require(value > 0, "no spendable discovered funds")
            result["discovery"] = {"pool": pool, "spendable_zat": value,
                                   "funding_fingerprint_sha256": tx_digest(funding["txid"])}
        elif step == "send":
            out = self.cli("send", "--identity", self.key, "--address", state["sapling"],
                           "--value", "100000", "--min-confirmations", "1", online=True)
            txid = out.splitlines()[-1]
            result["transaction"] = {"fingerprint_sha256": tx_digest(txid), "observation": "submitted"}
            state["txid"] = txid
        elif step == "pending":
            txid = state["txid"]
            require(self.history(txid)["mined_height"] is None and txid in self.rpc("getrawmempool"),
                    "wallet and node did not observe the same unmined transaction")
            result["transaction"] = {"fingerprint_sha256": tx_digest(txid), "observation": "wallet-pending"}
        elif step == "mined":
            [block_hash] = self.rpc("generate", [1])
            block = self.rpc("getblock", [block_hash, 1])
            require(state["txid"] in block["tx"] and self.rpc("getblockhash", [block["height"]]) == block_hash,
                    "transaction absent from canonical mined block")
            require(self.history(state["txid"])["mined_height"] is None,
                    "wallet unexpectedly scanned the block before its next sync")
            state["mined"] = block["height"]
            result["transaction"] = {"fingerprint_sha256": tx_digest(state["txid"]),
                                     "observation": "canonical-mined", "mined_height": state["mined"]}
        elif step == "scanned":
            self.sync()
            row = self.history(state["txid"])
            require(row["mined_height"] == state["mined"], "wallet's confirmation height differs")
            result["transaction"] = {"fingerprint_sha256": tx_digest(row["txid"]),
                                     "observation": "wallet-scanned", "mined_height": row["mined_height"]}
        elif step == "restart":
            before = self.balance()
            old_pid = self.last_pid
            after = self.balance()
            new_pid = self.last_pid
            require(before == after and old_pid != new_pid, "wallet process restart did not preserve state")
            require(self.history(state["txid"])["mined_height"] == state["mined"], "lost prior wallet activity")
            state["restarted"] = self.scanned_height()
            require(state["restarted"] == after["chain_tip_height"], "wallet tip is not fully scanned")
            result["restart_scope"] = "process-restart"
            result["restart"] = {"before_process_id": old_pid, "after_process_id": new_pid,
                                 "previous_process_exited": True, "before_height": before["chain_tip_height"],
                                 "after_height": state["restarted"]}
        elif step == "resume":
            self.rpc("generate", [1])
            self.sync()
            after = self.scanned_height()
            require(after > state["restarted"] and self.history(state["txid"])["mined_height"] == state["mined"],
                    "wallet failed to resume advancing with persisted history")
            result["progress"] = {"before_height": state["restarted"], "after_height": after,
                                  "new_block_mined_after_restart": True}
        require(self.traces, "missing wallet process traces")
        result["endpoint_evidence"] = {
            "scope": "process-trace" if self.online_connections else "local-only",
            "target_connected": bool(self.online_connections), "unexpected_connections": 0,
            "trace_fingerprint_sha256": digest("\n".join(self.traces).encode()),
        }
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", required=True, type=Path)
    parser.add_argument("--trace-tool", required=True, type=Path)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--node", required=True)
    parser.add_argument("--ready", required=True, type=Path, help="immutable Regtest fixture receipt")
    parser.add_argument("--candidate", type=Path, help="candidate service receipt for the same fixture")
    parser.add_argument("--scratch", required=True, type=Path)
    parser.add_argument("--evidence", required=True, type=Path, help="new private directory per campaign")
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--step", required=True, choices=STEPS)
    args = parser.parse_args()
    os.umask(0o077)
    wallet = Wallet(args)
    wallet.verify_chain()
    report = wallet.receipt()
    write_json(wallet.state_path, wallet.state)
    write_json(args.report, report)
    write_json(wallet.raw / "step.json", report)


if __name__ == "__main__":
    main()
