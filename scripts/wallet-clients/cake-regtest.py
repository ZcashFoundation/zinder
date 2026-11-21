#!/usr/bin/env python3
"""Exercise Cake's pinned Zcash engine through its stock local wallet GraphQL API."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from decimal import Decimal
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, Request, build_opener

from wallet_fixture import binding


STEPS = ("bootstrap", "restore", "transparent-discovery", "shielded-discovery",
         "send", "pending", "mined", "scanned", "restart", "resume")
HTTP = build_opener(ProxyHandler({}))


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(value):
    return hashlib.sha256(value).hexdigest()


def dump(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def loopback(url):
    u = urlsplit(url)
    require(u.scheme == "http" and u.hostname == "127.0.0.1" and u.port
            and u.netloc == f"127.0.0.1:{u.port}" and not any((u.path, u.query, u.fragment)),
            "expected an explicit HTTP IPv4 loopback endpoint")
    return u.port


def post(url, document, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    req = Request(url, json.dumps(document).encode(), headers)
    with HTTP.open(req, timeout=150) as response:
        return json.load(response)


def node(url, method, *params):
    r = post(url, {"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    require(not r.get("error"), f"node {method} returned {r.get('error', {}).get('code') if r.get('error') else ''}")
    return r["result"]


def until(fn, timeout=50):
    end = time.monotonic() + timeout
    last = None
    while time.monotonic() < end:
        try:
            value = fn()
            if value:
                return value
        except (ValueError, OSError) as e:
            last = type(e).__name__ + ": " + str(e)
        time.sleep(.5)
    raise RuntimeError("timed out waiting for wallet evidence" + (" (" + last + ")" if last else ""))


def credentials(folder):
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec, utils

    key = ec.generate_private_key(ec.SECP256R1())
    (folder / "jwt.pub").write_bytes(key.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo))
    (folder / "db.password").write_text(secrets.token_urlsafe(48))

    def encode(b):
        return base64.urlsafe_b64encode(b).rstrip(b"=")
    header = encode(b'{"alg":"ES256","typ":"JWT"}')
    claims = {"exp": int(time.time()) + 3600, "sub": 0, "write": True}
    signing = header + b"." + encode(json.dumps(claims).encode())
    r, s = utils.decode_dss_signature(key.sign(signing, ec.ECDSA(hashes.SHA256())))
    return (signing + b"." + encode(r.to_bytes(32, "big") + s.to_bytes(32, "big"))).decode()


def find_engine_pid(first, executable):
    todo = [first]
    while todo:
        pid = todo.pop()
        p = Path("/proc") / str(pid)
        try:
            if (p / "exe").resolve(strict=True) == executable:
                return pid
            todo.extend(map(int, (p / f"task/{pid}/children").read_text().split()))
        except (FileNotFoundError, ProcessLookupError):
            continue
    return None


@contextmanager
def server(args, run, db, name, token, traces):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    trace = run / (name + "-trace")
    command = [sys.executable, str(args.trace), "--endpoint-port", str(loopback(args.endpoint)),
               "--output", str(trace), "--", str(args.engine), "--coin", "2", "--db-path", str(db),
               "--db-password-file", str(run / "db.password"), "--lwd-url", args.endpoint,
               "--bind-address", "127.0.0.1", "--port", str(port), "--jwt-public-key-file", str(run / "jwt.pub")]
    def gql(query, variables=None):
        r = post(f"http://127.0.0.1:{port}/graphql", {"query": query, "variables": variables or {}}, token)
        require(not r.get("errors"), "GraphQL returned " + "; ".join(e["message"][:250] for e in r.get("errors", [])))
        return r["data"]

    with (run / (name + ".log")).open("wb") as log:
        proc = subprocess.Popen(command, cwd=run, stdout=log, stderr=log)
        pid = None
        try:
            def started():
                require(proc.poll() is None, "wallet exited before readiness")
                return find_engine_pid(proc.pid, args.engine) and gql("{__typename}")
            until(started, 18)
            pid = find_engine_pid(proc.pid, args.engine)
            require(pid, "could not identify wallet process")
            yield gql, pid
        finally:
            pid = pid or find_engine_pid(proc.pid, args.engine)
            if pid:
                try:
                    os.kill(pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                if pid:
                    os.kill(pid, signal.SIGKILL)
                proc.wait(timeout=10)
            if (trace / "report.json").exists():
                traces.append(json.loads((trace / "report.json").read_text()))


def account(gql, birthday, seed=None):
    data = {"name": "Qualification", "birth": birthday, "aindex": 0, "pools": 15, "useInternal": True}
    if seed:
        data["key"] = seed
    a = gql("mutation($a:NewAccount!){createAccount(newAccount:$a)}", {"a": data})["createAccount"]
    require(a == 1, "expected account 1 in unused database")
    addresses = gql("{addressByAccount(idAccount:1,pools:15){transparent sapling ua}}")["addressByAccount"]
    require(all(addresses.values()), "missing wallet receiver")
    return addresses


def sync(gql, height):
    def synced():
        result = gql("mutation{synchronizeAccount(idAccount:1,fast:false)}")["synchronizeAccount"]
        return result >= height
    until(synced, 75)
    r = gql("{balanceByAccount(idAccount:1){height transparent sapling ironwood total} transactionsByAccount(idAccount:1){txid height}}")
    return r["balanceByAccount"], {tx["txid"]: tx["height"] for tx in r["transactionsByAccount"]}


def at(block, rpc):
    return node(rpc, "getblock", block, 1)["tx"][0]


def run_suite(args):
    run = args.run.resolve()
    run.mkdir(mode=0o700, parents=False, exist_ok=False)
    args.engine = args.engine.resolve(strict=True)
    args.trace = args.trace.resolve(strict=True)
    rpc = args.node
    loopback(rpc)
    loopback(args.endpoint)
    _, _, height, block_hash = binding(args.fixture, args.candidate, args.endpoint, rpc)
    require(node(rpc, "getblockhash", height) == block_hash, "fixture block hash mismatch")
    info = node(rpc, "getblockchaininfo")
    upgrades = {v["name"]: v["activationheight"] for v in info["upgrades"].values()}
    require(info["chain"] in ("test", "regtest") and upgrades.get("NU6.3") == 250
            and "NU7" not in upgrades and upgrades.get("Sapling") == 1, "wrong Cake Regtest activation schedule")
    birthday = int(info["blocks"]) - 1
    token = credentials(run)
    receipts = {}
    traces = []

    def record(step, **attributes):
        receipts[step] = {"status": "completed", "endpoint": args.endpoint, "fallback_used": None,
                          "assertions": {step: True}, "assertions_enabled": True, "assertion_mode": "always-on",
                          **attributes}
        dump(run / "steps.json", receipts)

    first = run / "origin.regtest.db"
    second = run / "restored.regtest.db"
    try:
        with server(args, run, first, "source", token, traces) as (gql, _):
            addresses = account(gql, birthday)
            seed = gql("{accounts {seed}}")["accounts"][0]["seed"]
            require(seed and len(seed.split()) >= 12, "wallet did not return its fresh backup")
            sync(gql, birthday)
            record("bootstrap", generated_account=True)
            t_block = node(rpc, "generatetoaddress", 1, addresses["transparent"])[0]
            z_block = node(rpc, "generatetoaddress", 1, addresses["ua"])[0]
            node(rpc, "generate", 100)
            height = node(rpc, "getblockcount")
            funded = {at(t_block, rpc), at(z_block, rpc)}
            require(len(funded) == 2, "expected two distinct funding transactions")
            starting, source_history = sync(gql, height)
            require(all(source_history.get(tx, 0) > 0 for tx in funded), "source wallet did not discover the two mined rewards")

        with server(args, run, second, "restored", token, traces) as (gql, before_pid):
            fresh = account(gql, birthday, seed)
            balance, history = sync(gql, height)
            require(fresh == addresses and balance == starting and all(history.get(tx, 0) > 0 for tx in funded),
                    "fresh database did not recover the original activity")
            def activity(rows):
                return digest(json.dumps(sorted((tx, rows[tx]) for tx in funded)).encode())
            record("restore", restore={"fresh_database": True,
                   "source_database_fingerprint_sha256": digest(str(first).encode()),
                   "restored_database_fingerprint_sha256": digest(str(second).encode()),
                   "source_wallet_fingerprint_sha256": digest(addresses["ua"].encode()),
                   "restored_wallet_fingerprint_sha256": digest(fresh["ua"].encode()),
                   "source_activity_fingerprint_sha256": activity(source_history),
                   "restored_activity_fingerprint_sha256": activity(history),
                   "restored_activity_count": len(funded)})
            for step, name in (("transparent-discovery", "transparent"), ("shielded-discovery", "ironwood")):
                require(Decimal(balance[name]) > 0, "missing funded " + name + " pool")
                record(step, pool=name, funded_balance_zec=str(balance[name]))

            pay = {"recipients": [{"address": fresh["ua"], "amount": "0.001"}], "srcPools": 8}
            tx = gql("mutation($p:Payment!){pay(idAccount:1,payment:$p)}", {"p": pay})["pay"]
            require(len(bytes.fromhex(tx)) == 32, "invalid broadcast txid")
            txhash = digest(bytes.fromhex(tx))
            def observation(kind, height=None):
                return {"fingerprint_sha256": txhash, "observation": kind,
                        **({"mined_height": height} if height is not None else {})}
            record("send", transaction=observation("submitted"))
            require(tx in node(rpc, "getrawmempool"), "node did not observe submitted transaction")
            try:
                until(lambda: tx in [row["txid"] for row in gql("{unconfirmedByAccount(idAccount:1){txid}}")["unconfirmedByAccount"]], 35)
                record("pending", transaction=observation("wallet-pending"), observer="unconfirmedByAccount")
            except RuntimeError:
                receipts["pending"] = {"status": "no-evidence", "endpoint": args.endpoint, "node_mempool_observed": True,
                                       "reason": "unconfirmedByAccount did not return the broadcast transaction"}
            node(rpc, "generate", 1)
            mined = node(rpc, "getrawtransaction", tx, 1)
            height = node(rpc, "getblockcount")
            require(mined.get("confirmations", 0) > 0 and mined.get("blockhash") == node(rpc, "getblockhash", height),
                    "broadcast absent from newly mined block")
            record("mined", transaction=observation("canonical-mined", height))
            _, history = sync(gql, height)
            require(history.get(tx) == height, "wallet did not scan the mined transaction")
            record("scanned", transaction=observation("wallet-scanned", height))

        with server(args, run, second, "reopened", token, traces) as (gql, after_pid):
            loaded, history = sync(gql, height)
            before = int(loaded["height"])
            require(after_pid != before_pid and before >= height and history.get(tx) == height,
                    "wallet did not reopen the previous confirmed state")
            record("restart", restart_scope="process-restart", restart={"before_process_id": before_pid,
                   "after_process_id": after_pid, "before_height": height, "after_height": before})
            node(rpc, "generate", 1)
            final, history = sync(gql, node(rpc, "getblockcount"))
            after = int(final["height"])
            require(after > before and history.get(tx) == height, "restarted wallet did not advance to new block")
            record("resume", progress={"before_height": before, "after_height": after,
                   "new_block_mined_after_restart": True})
    finally:
        trace_ok = bool(traces) and all(r.get("status") == "passed" and r.get("fallback_used") is False for r in traces)
        for receipt in receipts.values():
            receipt["endpoint_evidence"] = {
                "scope": "process-trace", "target_connected": trace_ok,
                "unexpected_connections": sum(1 for t in traces if t.get("status") != "passed"),
                "trace_fingerprint_sha256": digest(json.dumps(traces, sort_keys=True).encode()),
            }
            receipt["fallback_used"] = False if trace_ok else None
        dump(run / "steps.json", receipts)
    require(len(traces) == 3 and trace_ok, "incomplete or unexpected process connection trace")
    return receipts


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--engine", type=Path, required=True, help="pinned zkool_graphql executable")
    p.add_argument("--trace", type=Path, required=True, help="Zinder trace-wallet-endpoint.py")
    p.add_argument("--fixture", type=Path, required=True, help="disposable stack readiness receipt")
    p.add_argument("--candidate", type=Path, help="candidate service receipt for the same fixture")
    p.add_argument("--endpoint", required=True)
    p.add_argument("--node", required=True)
    dirs = p.add_mutually_exclusive_group(required=True)
    dirs.add_argument("--run", type=Path, help="new private evidence and wallet state directory")
    dirs.add_argument("--scratch", type=Path, help="shared wallet-lifecycle case scratch directory")
    p.add_argument("--step", choices=("prepare", *STEPS))
    p.add_argument("--report", type=Path)
    args = p.parse_args()
    os.umask(0o077)
    if args.scratch:
        if not args.step or not args.report:
            p.error("--scratch requires --step and --report")
        args.run = args.scratch / "cake"
        if args.step == "prepare":
            receipts = run_suite(args)
            output = {"status": "completed", "endpoint": args.endpoint,
                      "fallback_used": any(r.get("fallback_used") is not False for r in receipts.values()),
                      "assertions_enabled": True, "assertion_mode": "always-on",
                      "endpoint_evidence": receipts["resume"]["endpoint_evidence"], "assertions": {"suite_ran": True}}
        else:
            output = json.loads((args.run / "steps.json").read_text())[args.step]
        dump(args.report, output)
        return 0
    if args.step or args.report:
        p.error("--step and --report require --scratch")
    receipts = run_suite(args)
    return 0 if all(receipts.get(k, {}).get("status") == "completed" for k in STEPS) else 1


if __name__ == "__main__":
    sys.exit(main())
