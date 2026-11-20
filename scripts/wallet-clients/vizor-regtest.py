#!/usr/bin/env python3
"""Run one released Vizor engine lifecycle step on a pinned local Regtest fixture."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, Request, build_opener


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(value):
    return hashlib.sha256(value).hexdigest()


def fingerprint(txid):
    require(re.fullmatch("[a-f0-9]{64}", txid), "invalid transaction identifier")
    return digest(bytes.fromhex(txid))


def loopback(url):
    parts = urlsplit(url)
    require(url == f"http://127.0.0.1:{parts.port}" and parts.port, "local endpoint required")
    return parts.port


def run(args):
    ready = json.loads(args.fixture.read_text())
    endpoint = args.endpoint
    node = ready["node_rpc"]
    port = loopback(endpoint)
    loopback(node)
    opener = build_opener(ProxyHandler({}))
    require(ready["network"] == "zcash-regtest", "Regtest fixture required")

    def rpc(method, *params):
        message = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
        request = Request(node, message, {"Content-Type": "application/json"})
        with opener.open(request, timeout=120) as response:
            result = json.load(response)
        require(not result.get("error"), f"node RPC failed for {method}")
        return result["result"]

    chain = rpc("getblockchaininfo")
    heights = {key: value["activationheight"] for key, value in chain["upgrades"].items()}
    earlier = "5ba81b19 76b809bb 2bb40e60 f5b9230b e9ff75a6 c2d6d0b4 c8e71055 4dec4df0 5437f330".split()
    require(all(heights.get(key) == 1 for key in earlier), "early upgrade schedule mismatch")
    require("77190ad9" not in heights and heights["37a5165b"] >= 2, "NU6.3/NU7 schedule mismatch")
    anchor_height = ready.get("initial_tip", ready.get("current_tip"))
    anchor_hash = ready.get("initial_hash", ready.get("current_hash"))
    require(type(anchor_height) is int and anchor_hash, "fixture chain anchor missing")
    require(rpc("getblockhash", anchor_height) == anchor_hash, "fixture hash mismatch")
    selected = ready["lightwalletd"]
    ops = ready["compat_ops"]
    if args.candidate:
        candidate = json.loads(args.candidate.read_text())
        require(candidate["network"] == "zcash-regtest" and candidate["node_rpc"] == node, "candidate node mismatch")
        selected = candidate["client_endpoint"]
        ops = candidate["ops_endpoint"]
    require(endpoint == selected, "fixture endpoint mismatch")
    loopback(ops)
    args.scratch.mkdir(mode=0o700, exist_ok=True)
    args.evidence.mkdir(mode=0o700, parents=True, exist_ok=True)
    step = args.step
    state_path = args.scratch / "vizor.json"
    state = json.loads(state_path.read_text()) if state_path.exists() else {}
    report = {"status": "completed", "endpoint": endpoint, "fallback_used": False,
              "assertions_enabled": True, "assertion_mode": "always-on", "assertions": {step: True}}
    traces = []

    def wallet(command):
        trace = args.evidence / f"{step}-{command}-{time.time_ns()}"
        argv = [sys.executable, str(args.trace_tool), "--endpoint-port", str(port),
                "--output", str(trace), "--", str(args.engine), command, str(args.scratch),
                endpoint, str(heights["37a5165b"])]
        with (args.evidence / f"{trace.name}.stderr").open("w") as errors:
            output = subprocess.run(argv, stdout=subprocess.PIPE, stderr=errors, text=True, check=True)
        observed = json.loads((trace / "report.json").read_text())
        require(observed["status"] == "passed" and observed["fallback_used"] is False, "wallet connection not attributed")
        traces.append({"path": trace.name, "sha256": digest((trace / "connect.log").read_bytes())})
        return json.loads(output.stdout)

    def synced(command):
        tip = rpc("getblockcount")
        # A burst of new coinbases can put the served pair one refresh interval behind Zebra.
        deadline = time.monotonic() + 150
        while time.monotonic() < deadline:
            with opener.open(ops + "/readyz", timeout=5) as response:
                served = json.load(response)
            if served.get("current_height") != tip or served.get("status") != "ready":
                time.sleep(1)
                continue
            response = wallet(command)
            if response["scanned"] == tip and response["tip"] == tip:
                return response
            time.sleep(1)
        raise ValueError("wallet did not reach the pinned node tip")

    def find_tx(observation, txid):
        # Vizor history exposes wallet database bytes; Zebra displays their reverse.
        matches = [{**row, "txid": bytes.fromhex(row["txid"])[::-1].hex()}
                   for row in observation["history"] if bytes.fromhex(row["txid"])[::-1].hex() == txid]
        require(len(matches) == 1, "transaction absent or ambiguous in wallet history")
        return matches[0]

    def transaction(txid, kind, height=None):
        report["transaction"] = {"fingerprint_sha256": fingerprint(txid), "observation": kind}
        if height is not None:
            report["transaction"]["mined_height"] = height

    if step == "bootstrap":
        created = wallet("create")
        state["unified"] = created["unified"]
        for route, address in (("transparent", created["transparent_address"]), ("shielded", created["unified"])):
            block = rpc("generatetoaddress", 1, address)[0]
            state[route] = rpc("getblock", block, 1)["tx"][0]
        rpc("generate", 101)
        source = synced("sync-original")
        require(source["transparent"] > 0 and source["ironwood"] > 0, "original wallet missing funded pools")
        original = find_tx(source, state["shielded"])
        require(original["balance_delta"] > 0 and original["mined_height"] > 0, "original shielded activity missing")
        state["source_activity"] = fingerprint(original["txid"])
    elif step == "restore":
        restored = wallet("restore")
        observed = synced("sync")
        require(restored["unified"] == state["unified"], "restored identity changed")
        recovered = find_tx(observed, state["shielded"])
        require(recovered["balance_delta"] > 0 and recovered["mined_height"] > 0, "restored funding absent")
        report["restore"] = {
            "fresh_database": True,
            "source_database_fingerprint_sha256": digest(os.fsencode((args.scratch / "original.sqlite").resolve())),
            "restored_database_fingerprint_sha256": digest(os.fsencode((args.scratch / "restored.sqlite").resolve())),
            "source_wallet_fingerprint_sha256": digest(state["unified"].encode()),
            "restored_wallet_fingerprint_sha256": digest(restored["unified"].encode()),
            "source_activity_fingerprint_sha256": state["source_activity"],
            "restored_activity_fingerprint_sha256": fingerprint(recovered["txid"]),
            "restored_activity_count": len(observed["history"]),
        }
        state["account"] = restored["account"]
    elif step in ("transparent-discovery", "shielded-discovery"):
        key, pool = ("transparent", "transparent") if step == "transparent-discovery" else ("shielded", "ironwood")
        observed = wallet("read")
        tx = find_tx(observed, state[key])
        require(observed[pool] > 0 and tx["balance_delta"] > 0 and tx["mined_height"] > 0, "funded activity missing")
        report["discovery"] = {"pool": pool, "amount_zat": observed[pool], "funding_fingerprint_sha256": fingerprint(tx["txid"])}
    elif step == "send":
        state["sent"] = wallet("send")["txid"]
        transaction(state["sent"], "submitted")
    elif step == "pending":
        tx = find_tx(wallet("read"), state["sent"])
        mempool = rpc("getrawmempool")
        require(tx["mined_height"] == 0 and not tx["expired"] and tx["txid"] in mempool, "pending payment missing")
        transaction(tx["txid"], "wallet-pending")
    elif step == "mined":
        block = rpc("generate", 1)[0]
        canonical = rpc("getblock", block, 1)
        require(state["sent"] in canonical["tx"] and canonical["confirmations"] >= 1, "payment missing from mined block")
        state["mined"] = canonical["height"]
        transaction(next(tx for tx in canonical["tx"] if tx == state["sent"]), "canonical-mined", state["mined"])
        wallet("read")
    elif step in ("scanned", "restart"):
        observed = synced("sync") if step == "scanned" else wallet("read")
        tx = find_tx(observed, state["sent"])
        require(tx["mined_height"] == state["mined"] and observed["account"] == state["account"], "confirmed wallet state changed")
        if step == "scanned":
            state["scanned"] = observed["scanned"]
            state["previous_process"] = observed["pid"]
            transaction(tx["txid"], "wallet-scanned", tx["mined_height"])
        else:
            require(observed["pid"] != state["previous_process"], "expected fresh wallet process")
            state["restart_height"] = observed["scanned"]
            report["restart_scope"] = "process-restart"
            report["restart"] = {"before_process_id": state["previous_process"], "after_process_id": observed["pid"],
                                 "before_height": state["scanned"], "after_height": state["restart_height"]}
    elif step == "resume":
        before = wallet("read")["scanned"]
        require(before >= state["restart_height"], "wallet scan rewound")
        block = rpc("generate", 1)[0]
        height = rpc("getblock", block, 1)["height"]
        observed = synced("sync")
        require(height > before and observed["scanned"] == height, "new block was not scanned")
        require(find_tx(observed, state["sent"])["mined_height"] == state["mined"], "resumed transaction height changed")
        report["progress"] = {"before_height": before, "after_height": observed["scanned"], "new_block_mined_after_restart": True}
    else:
        raise ValueError("unknown lifecycle step")
    require(traces, "missing engine connection evidence")
    index = args.evidence / f"{step}.trace-index.json"
    index.write_text(json.dumps(traces, indent=2) + "\n")
    report["endpoint_evidence"] = {"scope": "process-trace", "target_connected": True,
                                   "unexpected_connections": 0, "trace_fingerprint_sha256": digest(index.read_bytes())}
    state_path.write_text(json.dumps(state))
    args.report.write_text(json.dumps(report))
    (args.evidence / f"{step}.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ("engine", "trace-tool", "fixture", "scratch", "report", "evidence"):
        parser.add_argument("--" + option, required=True, type=Path)
    parser.add_argument("--candidate", type=Path)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--step", required=True)
    args = parser.parse_args()
    try:
        run(args)
    except Exception as error:
        args.evidence.mkdir(mode=0o700, parents=True, exist_ok=True)
        (args.evidence / f"{args.step}.error").write_text(f"{type(error).__name__}: {error}\n")
        raise
