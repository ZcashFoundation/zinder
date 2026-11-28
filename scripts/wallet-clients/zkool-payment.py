#!/usr/bin/env python3
"""Two-party payment adapter for the pinned Zkool engine.

Wraps the private `zkool_payment` example, which is built inside Zkool's own
crate. Every online command is executed under the connection trace so the
adapter can prove the wallet reached only the fixture lightwalletd endpoint.

Reused by `zkool-payment-matrix.py`; not run on its own.
"""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from roundtrip_wallets import pin_directory, require, shielded_address, valid_txid, write_private

COMMAND_TIMEOUT = 900


def _lifecycle():
    """Reuse the tested bounded process-group lifecycle helper."""
    name = "wallet_roundtrip_regtest"
    if name in sys.modules:
        return sys.modules[name]
    path = Path(__file__).resolve().parent.parent / "run-wallet-roundtrip-regtest.py"
    spec = importlib.util.spec_from_file_location(name, path)
    loaded = importlib.util.module_from_spec(spec)
    sys.modules[name] = loaded
    try:
        spec.loader.exec_module(loaded)
    except BaseException:
        sys.modules.pop(name, None)
        raise
    return loaded


def _terminate(process, grace=10):
    """Terminate the traced wallet process group, including stubborn descendants."""
    if process.poll() is not None:
        return
    try:
        _lifecycle().stop_group(process, signal.SIGTERM, grace)
    except (OSError, subprocess.TimeoutExpired):
        pass


def _capture(command, env, timeout):
    """Run one online or offline wallet command in its own bounded session."""
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True, env=env, start_new_session=True)
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired as error:
        _terminate(process)
        try:
            process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        raise TimeoutError(f"Zkool command exceeded {timeout}s") from error
    except BaseException:
        _terminate(process)
        raise
    return process.returncode, stdout, stderr

# Official BIP-39 24-word test vectors with valid checksums. Disposable Regtest
# only: each case must use its own seed so two wallets never share a key.
SEEDS = {
    "zero": "abandon " * 23 + "art",
    "sevenf": "legal winner thank year wave sausage worth useful legal winner thank year wave sausage "
              "worth useful legal winner thank year wave sausage worth title",
    "eighty": "letter advice cage absurd amount doctor acoustic avoid letter advice cage absurd amount "
              "doctor acoustic avoid letter advice cage absurd amount doctor acoustic bless",
}


def _last_json(text):
    for line in reversed(text.strip().splitlines()):
        line = line.strip()
        if line.startswith("{"):
            return json.loads(line)
    raise RuntimeError("wallet command produced no JSON receipt")


class ZkoolPayment:
    def __init__(self, binary, directory, network, endpoint, birthday, variant, trace_tool, evidence, seed):
        require(variant in {"internal-enabled", "external-only"}, "unknown Zkool variant")
        require(seed in SEEDS, "unknown Regtest seed")
        require(network == "regtest", "Zkool two-party payments are exercised on Regtest only")
        self.seed = SEEDS[seed]
        self.binary = str(Path(binary).resolve())
        self.directory = Path(directory).resolve()
        self.network, self.endpoint, self.birthday = network, endpoint, birthday
        self.variant = variant
        self.trace_tool = Path(trace_tool).resolve()
        require(self.trace_tool.is_file(), "missing connection trace tool")
        self.evidence = Path(evidence)
        self.evidence.mkdir(mode=0o700, parents=True, exist_ok=True)
        require(not self.directory.is_symlink(), "refusing a symlinked wallet directory")
        pin_directory(self.directory, network, endpoint)
        marker = self.directory / "variant"
        if marker.exists():
            require(marker.read_text() == variant, "Zkool wallet was created for a different variant")
        else:
            write_private(marker, variant)
        self.address = None
        self.database = self.directory / "zkool.db"
        self.traces = []

    # -- process plumbing ---------------------------------------------------
    def _run(self, args, online, label):
        command = [self.binary, *map(str, args)]
        stamp = f"{label}-{time.time_ns()}"
        if online:
            trace = self.evidence / ("trace-" + stamp)
            command = [sys.executable, str(self.trace_tool), "--endpoint-port",
                       str(int(self.endpoint.rsplit(":", 1)[1])), "--output", str(trace), "--", *command]
        env = dict(os.environ,
                   ZINDER_ZKOOL_USE_INTERNAL="true" if self.variant == "internal-enabled" else "false")
        code, stdout, stderr = _capture(command, env, COMMAND_TIMEOUT)
        (self.evidence / (stamp + ".log")).write_text(json.dumps({
            "operation": label, "online": online, "exit": code,
            "stdout": stdout, "stderr": stderr,
        }, indent=2) + "\n")
        require(code == 0, f"Zkool {label} failed; inspect private evidence")
        if online:
            report = json.loads((trace / "report.json").read_text())
            require(report.get("status") == "passed" and report.get("fallback_used") is False,
                    f"Zkool {label} endpoint attribution failed")
            self.traces.append({"operation": label, "endpoint": self.endpoint,
                                "connect_log_sha256": hashlib.sha256(
                                    (trace / "connect.log").read_bytes()).hexdigest()})
        return stdout

    def _account(self):
        return int((self.directory / "account").read_text())

    # -- lifecycle ----------------------------------------------------------
    def prepare(self, target=None):
        if not (self.directory / "zkool.db").exists():
            stdout = self._run(["create", self.endpoint, self.birthday, self.seed, self.directory],
                               True, "create")
        else:
            stdout = self._run(["sync", self.endpoint, self.directory], True, "sync")
        require("synced to" in stdout, "Zkool sync did not complete")
        addresses = _last_json(self._run(["addresses", self.directory], False, "addresses"))
        address = shielded_address(addresses["unified"], self.network)
        saved = self.directory / "receiving-address"
        if saved.exists():
            require(saved.read_text() == address, "Zkool receiving address changed")
        else:
            write_private(saved, address)
        self.address = address
        self.addresses = addresses
        return address

    def transparent_address(self):
        return self.addresses["transparent"]

    def snapshot(self, target=None):
        stdout = self._run(["sync", self.endpoint, self.directory], True, "sync")
        require("synced to" in stdout, "Zkool sync did not complete")
        report = _last_json(self._run(["report", self.directory], False, "report"))
        memos = {}
        for row in report["memos"]:
            if row.get("text"):
                txid = valid_txid(row["txid"])
                memos.setdefault(txid, []).append(row["text"])
                memos.setdefault(bytes.fromhex(txid)[::-1].hex(), []).append(row["text"])
        transactions = []
        for row in report["transactions"]:
            stored = valid_txid(row["txid_stored"])
            txid = valid_txid(row["txid"])
            height = row["height"]
            value = row["value_zat"]
            transactions.append({
                "txid": stored, "txid_alt": txid, "height": height if height > 0 else None,
                "pending": height == 0,
                "amount_zat": abs(value), "delta_zat": value,
                "fee_zat": row["fee_zat"] if row["fee_zat"] > 0 else None,
                "kind": "received" if value > 0 else "sent",
                "memos": memos.get(stored, []), "change_zat": None,
            })
        balance = report["balance"]
        return {"height": report["synced_height"], "total_zat": balance["total"],
                "spendable_zat": balance["total"], "transactions": transactions,
                "pools": {k: balance[k] for k in ("transparent", "sapling", "orchard", "ironwood")}}

    def send(self, address, amount_zat, memo):
        require(type(amount_zat) is int and amount_zat > 0, "invalid amount")
        result = _last_json(self._run(
            ["send", self.endpoint, self.directory, shielded_address(address, self.network),
             amount_zat, 15, memo], True, "send"))
        return valid_txid(result["txid"])

    def restart(self, target=None):
        # Every invocation is a fresh process against the persisted database.
        return self.prepare(target)

    def close(self):
        pass

    def shutdown_evidence(self):
        # Every Zkool command is a bounded foreground process, already reaped.
        return {"scope": "no-resident-process", "processes_remaining": 0}

    def endpoint_evidence(self):
        require(self.traces, "no traced Zkool operations")
        return {"scope": "process-trace", "target_connected": True,
                "endpoint": self.endpoint, "operations": len(self.traces),
                "trace_fingerprint_sha256": hashlib.sha256(
                    json.dumps(self.traces, sort_keys=True).encode()).hexdigest(),
                "traces": self.traces}
