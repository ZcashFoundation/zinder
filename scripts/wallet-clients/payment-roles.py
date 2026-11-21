#!/usr/bin/env python3
"""Qualify Vizor, zcash-devtool, and Zally as counterparty payer and payee.

Each run owns one disposable Zinder Regtest fixture at the selected release tag,
one engine wallet per role, and independently keyed Zingo CLI counterparties.
The engine pays a fresh Zingo payee with a memo, then a funded Zingo payer pays
a fresh engine wallet, which spends the received funds back.
The private receipt records each wallet's available fee, balance, memo, and
history observations, alongside canonical inclusion. Use the coverage collector
to export the public matrix rows.
"""
import argparse
import hashlib
import importlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
NODE_CONFIG = ROOT / "deploy/zebra/regtest-nu6-at-one.toml"
BASES = {"vizor": 51000, "devtool": 51100, "zally": 51200}
AMOUNT_ENGINE_PAYS = 1_000_000
AMOUNT_ENGINE_RECEIVES = 600_000
AMOUNT_ENGINE_RETURNS = 300_000
MEMO_ENGINE_PAYS = "zinder-payment-roles/v1/engine-pays"
MEMO_ENGINE_RECEIVES = "zinder-payment-roles/v1/engine-receives"
MEMO_ENGINE_RETURNS = "zinder-payment-roles/v1/engine-spends-received"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def fingerprint(txid):
    require(re.fullmatch(r"[0-9a-f]{64}", txid), "invalid transaction id")
    return hashlib.sha256(bytes.fromhex(txid)).hexdigest()


def write_json(path, value):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def resolve_tag(tag, root=ROOT):
    """Resolve a release tag to its full commit SHA in the checked-out repository."""
    require(isinstance(tag, str) and re.fullmatch(r"v\d+\.\d+\.\d+(?:[-+][\w.-]+)?", tag),
            "invalid release tag")
    result = subprocess.run(["git", "-C", str(root), "rev-parse", "--verify",
                             f"refs/tags/{tag}^{{commit}}"], capture_output=True, text=True)
    if result.returncode != 0:
        raise ValueError(f"release tag {tag!r} does not resolve to a commit: {result.stderr.strip()}")
    commit = result.stdout.strip()
    require(re.fullmatch(r"[0-9a-f]{40}", commit),
            f"release tag {tag!r} resolved to a non-commit reference")
    return commit


def reconcile_revision(tag, revision):
    """Return the release tag's commit, refusing an explicit --revision that disagrees."""
    resolved = resolve_tag(tag)
    if revision:
        require(revision == resolved,
                f"--revision {revision} does not match release tag {tag} ({resolved})")
    return resolved


def images_filename(tag):
    return f"images-{tag}.json"


def engine_required_inputs(engine, args):
    """Return the explicit input names the selected engine still needs."""
    common = ["images", "bin_dir", "zingo", "evidence"]
    per_engine = {
        "vizor": ["vizor_engine", "vizor_source", "vizor_provenance"],
        "devtool": ["devtool", "devtool_source", "identity_probe_python", "identity_proto_dir"],
        "zally": ["zally_source", "native_probe_python", "native_proto_dir"],
    }
    names = common + per_engine[engine]
    missing = [name for name in names if getattr(args, name, None) is None]
    if engine == "zally" and not (args.zally_binary or getattr(args, "zally_build", None)):
        missing.append("zally_binary or --zally-build")
    return missing


class Node:
    def __init__(self, url):
        require(url.startswith("http://127.0.0.1:"), "explicit loopback node endpoint required")
        self.url = url
        self.opener = build_opener(ProxyHandler({}))

    def call(self, method, *params):
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
        request = Request(self.url, body, {"Content-Type": "application/json"})
        with self.opener.open(request, timeout=120) as response:
            answer = json.load(response)
        require(not answer.get("error"), f"node RPC {method} failed: {answer.get('error')}")
        return answer["result"]

    def tip(self):
        return self.call("getblockcount")

    def mine(self, count=1):
        return self.call("generate", count)

    def block(self, height_or_hash):
        return self.call("getblock", height_or_hash, 1)

    def mempool(self):
        return self.call("getrawmempool")

    def transaction(self, txid):
        return self.call("getrawtransaction", txid, 1)


def serving_height(ready, opener):
    ops = ready["compat_ops"]
    try:
        with opener.open(ops + "/readyz", timeout=5) as response:
            state = json.load(response)
    except HTTPError as error:
        error.close()
        return None
    except URLError:
        return None
    if state.get("status") != "ready":
        return None
    return state.get("current_height")


def wait_served(ready, target, timeout=300):
    opener = build_opener(ProxyHandler({}))
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        height = serving_height(ready, opener)
        if height is not None and height >= target:
            return height
        time.sleep(1)
    raise ValueError(f"serving endpoint did not publish height {target}")


def verify_services(ready, revision):
    opener = build_opener(ProxyHandler({}))
    for name, key in (("compat-lightwalletd", "compat_ops"), ("query", "query_ops")):
        with opener.open(ready[key] + "/healthz", timeout=10) as response:
            health = json.load(response)
        require(health.get("network") == "zcash-regtest", f"{name} network mismatch")
        require(health.get("git_commit") == revision, f"{name} revision mismatch")


def directory_files(path):
    path = Path(path)
    if path.is_dir():
        return sorted(entry for entry in path.iterdir() if entry.is_file())
    return [path] if path.is_file() else []


def input_hashes(paths):
    hashes = {}
    for root in paths:
        for path in directory_files(root):
            hashes[str(path.resolve())] = digest(path)
    return hashes


def fixture_up(state_root, base, evidence, project, node_config, images_path, bin_dir,
               revision, images_name):
    images = evidence / images_name
    write_json(images, json.loads(Path(images_path).read_text()))
    command = [sys.executable, str(ROOT / "scripts/regtest-fixture.py"), "up",
               "--project", project, "--state-root", str(state_root), "--port-base", str(base),
               "--images", str(images), "--revision", revision,
               "--node-config", str(node_config), "--tip", "150", "--timeout", "420"]
    if bin_dir:
        command.extend(["--bin-dir", str(bin_dir)])
    subprocess.run(command, check=True, cwd=ROOT)
    ready = state_root / project / "ready.initial.json"
    require(ready.is_file(), "fixture produced no immutable readiness receipt")
    snapshot = evidence / "fixtures" / project / ready.name
    if snapshot != ready:
        snapshot.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with snapshot.open("xb") as copy:
            copy.write(ready.read_bytes())
    return json.loads(snapshot.read_bytes()), snapshot


def fixture_down(state_root, project):
    subprocess.run([sys.executable, str(ROOT / "scripts/regtest-fixture.py"), "down",
                    "--project", project, "--state-root", str(state_root)],
                   check=True, cwd=ROOT)


def project_resources(project):
    resources = {}
    for kind, listing in (("container", ["ps", "-aq"]), ("network", ["network", "ls", "-q"]),
                          ("volume", ["volume", "ls", "-q"])):
        output = subprocess.check_output(["docker", *listing, "--filter",
                                          f"label=com.docker.compose.project={project}"],
                                         text=True)
        resources[kind] = [line for line in output.splitlines() if line.strip()]
    return resources


def make_engine(engine, directory, ready, extra, revision):
    if engine == "vizor":
        module = importlib.import_module("vizor-payment")
        return module.Vizor(extra["vizor_engine"], directory, ready["lightwalletd"],
                            ready["activation_heights"]["NU6.3"], revision,
                            ready.get("current_tip"), ready.get("current_hash"))
    if engine == "devtool":
        module = importlib.import_module("devtool-payment")
        return module.Devtool(extra["devtool"], directory, ready["lightwalletd"],
                              ready["node_rpc"], "payment-roles",
                              trace_commands=True,
                              trace_script=ROOT / "scripts/trace-wallet-endpoint.py")
    module = importlib.import_module("zally-payment")
    return module.Zally(extra["zally_binary"], directory, ready["native_wallet_query"],
                        ready["node_rpc"], "payment-roles")


def spendable(snapshot):
    if "spendable_zat" in snapshot:
        return snapshot["spendable_zat"]
    if "ironwood" in snapshot:
        return sum(snapshot.get(pool, 0) for pool in ("transparent", "sapling", "orchard", "ironwood"))
    return sum(snapshot.get(pool, 0) for pool in ("transparent_zat", "sapling_zat"))


def ledger_delta(engine, snapshot, txid):
    if engine == "vizor":
        rows = [row for row in snapshot["history"] if row["txid"] == txid]
        return rows[0]["balance_delta"] if len(rows) == 1 else None
    if engine == "devtool":
        rows = [row for row in snapshot["transactions"] if row["txid"] == txid]
        return rows[0]["delta"] if len(rows) == 1 else None
    return None


def engine_payer(engine, wallet, address, amount, memo, txid, sent, before, after, detail,
                 delta=None):
    fee, fee_source = None, None
    if engine == "vizor":
        detail = detail or {}
        fee = sent.get("fee_zatoshi")
        if fee is None:
            fee = detail.get("network_fee")
        fee_source = "wallet proposal fee" if fee is not None else None
    elif engine == "devtool":
        detail = detail or {}
        fee = detail.get("fee_zat")
        fee_source = "wallet transaction fee_paid" if fee is not None else None
    if not isinstance(fee, int) or fee <= 0:
        fee = before - after - amount
        fee_source = "derived from persisted balance delta minus payment amount"
    require(isinstance(fee, int) and fee > 0, "payer fee evidence missing")
    require(before - after == amount + fee, "payer debit does not reconcile payer fee")
    if isinstance(delta, int):
        require(-delta == amount + fee, "payer ledger delta does not reconcile payer fee")
    return {"fee_zat": fee, "fee_source": fee_source,
            "debit_zat": before - after, "amount_zat": amount,
            "ledger_delta_zat": delta}


def engine_receive(engine, snapshot, detail, txid, amount, memo):
    if engine == "zally":
        rows = [row for row in snapshot["receives"] if row["txid"] == txid]
        require(len(rows) == 1, "received transaction absent or duplicated in Zally receives")
        row = rows[0]
        require(row["value"] == amount, "Zally did not credit the exact payee amount")
        received = row["memo"]
        require(received is None or received == memo, "Zally retained a different payee memo")
        return {"amount_zat": row["value"], "memo_verified": received == memo,
                "memo_observed": received is not None, "pool": row["pool"],
                "mined_height": row["mined_height"]}
    detail = detail or {}
    rows = [row for row in snapshot["history"] if row["txid"] == txid] \
        if engine == "vizor" else [row for row in snapshot["transactions"] if row["txid"] == txid]
    require(len(rows) == 1, "received transaction absent or duplicated in wallet history")
    row = rows[0]
    credit = row["balance_delta"] if engine == "vizor" else row["amount_zat"]
    require(credit == amount, f"{engine} did not credit the exact payee amount")
    if engine == "vizor":
        received_memo = detail["memo"]
    else:
        outputs = [o for o in row["outputs"] if not o["is_change"] and o["value"] == amount]
        require(len(outputs) == 1, "devtool payee output absent or ambiguous")
        received_memo = outputs[0]["memo"]
    require(received_memo is None or received_memo == memo,
            f"{engine} retained a different payee memo")
    return {"amount_zat": credit, "memo_verified": received_memo == memo,
            "memo_observed": received_memo is not None, "pool": row.get("pool"),
            "mined_height": row.get("mined_height", row.get("height"))}


def engine_rows(engine, snapshot):
    if engine == "vizor":
        return snapshot["history"]
    if engine in ("devtool", "zingo"):
        return snapshot["transactions"]
    return snapshot["receives"]


def row_count(engine, snapshot, txid):
    return sum(1 for row in engine_rows(engine, snapshot) if row["txid"] == txid)


def inclusion(node, txid):
    tx = node.transaction(txid)
    require(tx.get("confirmations", 0) >= 1 and tx.get("blockhash"), "transaction is not mined")
    require(node.call("getblockhash", tx["height"]) == tx["blockhash"], "transaction is off the active chain")
    block = node.block(tx["blockhash"])
    require(block["height"] == tx["height"], "block height differs from the transaction height")
    require(block["tx"].count(txid) == 1, "transaction appears more than once in its active block")
    return {"mined_height": tx["height"], "fingerprint_sha256": fingerprint(txid),
            "active_chain_block": True, "occurrences_in_block": 1}


def mining_wait(node, ready, count=1):
    node.mine(count)
    return wait_served(ready, node.tip())


def settled_balances(node, ready, txid, wallet, zingo):
    mined = inclusion(node, txid)["mined_height"]
    target = mined + 9
    tip = node.tip()
    require(mined <= tip <= target, "payment is outside the ten-confirmation window")
    if tip < target:
        mining_wait(node, ready, target - tip)
    engine_snapshot = wallet.sync()
    zingo_snapshot = zingo.snapshot()
    engine_height = engine_snapshot.get("scanned", engine_snapshot.get("height"))
    zingo_height = zingo_snapshot["height"]
    require(node.tip() == target == engine_height == zingo_height,
            f"ten-confirmation height {target} differs from engine {engine_height} or Zingo {zingo_height}")
    confirmed = node.transaction(txid)
    require(confirmed["height"] == mined and confirmed["confirmations"] == 10,
            "canonical payment does not have exactly ten confirmations")
    observation = {"engine_balance_after_zat": engine_snapshot["total_zat"],
                   "zingo_balance_after_zat": zingo_snapshot["total_zat"],
                   "observed_after_confirmations": confirmed["confirmations"],
                   "engine_height": engine_height, "zingo_height": zingo_height}
    return engine_snapshot, zingo_snapshot, observation


class ZingoCounterparty:
    def __init__(self, zingo, wallet_dir, endpoint, node, birthday=2):
        module = importlib.import_module("zingo-payment")
        self.wallet = module.Counterparty(zingo, wallet_dir, endpoint, node, birthday=birthday)

    def prepare(self):
        return self.wallet.prepare()

    def fund(self):
        return self.wallet.fund()

    def snapshot(self):
        return self.wallet.snapshot()

    def send(self, address, amount, memo):
        return self.wallet.send(address, amount, memo)

    def detail(self, txid):
        return self.wallet.detail(txid)

    def restart(self):
        return self.wallet.restart()


def payee_credit_zingo(snapshot, txid, amount, memo):
    rows = [row for row in snapshot["transactions"] if row["txid"] == txid]
    require(len(rows) == 1, "Zingo payee transaction absent or duplicated")
    row = rows[0]
    require(row["amount_zat"] == amount, "Zingo payee amount mismatch")
    require(memo in row["memos"], "Zingo payee memo mismatch")
    return {"amount_zat": row["amount_zat"], "memo_verified": True,
            "mined_height": row["height"]}


def pending_observation(engine, snapshot, txid):
    if snapshot is None:
        return None
    rows = [row for row in engine_rows(engine, snapshot) if row["txid"] == txid]
    if not rows:
        return {"row_observed": False}
    row = rows[0]
    if engine == "zingo" or engine == "devtool":
        pending = bool(row.get("pending"))
    else:
        pending = row.get("mined_height") in (0, None) and not row.get("expired")
    return {"row_observed": True, "pending": pending}


def run_engine_pays(engine, node, ready, evidence, zingo_recv, payer, amount, memo):
    """The engine pays an independently keyed Zingo wallet."""
    payee_address = zingo_recv.prepare()
    payer_before = payer.snapshot()
    payee_before = zingo_recv.snapshot()
    require(spendable(payer_before) >= amount + 100_000, "engine payer lacks spendable funds")
    if engine == "vizor":
        txid, sent_report = payer.send(payee_address, amount, memo)
    else:
        txid, sent_report = payer.send(payee_address, amount, memo), {}
    pending_height = node.tip()
    require(txid in node.mempool(), "engine payment is absent from the node mempool")
    engine_pending = None if engine == "zally" else pending_observation(engine, payer.snapshot(), txid)
    zingo_pending = pending_observation("zingo", zingo_recv.snapshot(), txid)
    mining_wait(node, ready)
    payer_after_send = payer.sync()
    payee_after = zingo_recv.snapshot()
    detail = payer.detail(txid) if engine != "zally" else {}
    received = payee_credit_zingo(payee_after, txid, amount, memo)
    require(received["mined_height"] == node.transaction(txid)["height"], "payee height differs from node")
    delta = ledger_delta(engine, payer_after_send, txid)
    send_persistence = "ledger sent row" if delta is not None else None
    if engine == "zally":
        change = [row for row in payer_after_send["receives"]
                  if row["txid"] == txid and row["is_change"]]
        send_persistence = "change row in receives" if len(change) == 1 else "not observed"
    payer_final, payee_final, outbound = settled_balances(node, ready, txid, payer, zingo_recv)
    paid = engine_payer(engine, payer, payee_address, amount, memo, txid, sent_report,
                        payer_before["total_zat"], payer_final["total_zat"], detail, delta)
    require(payee_final["total_zat"] == payee_before["total_zat"] + amount,
            "Zingo payee balance did not rise by the exact amount")
    reopened_payer = payer.restart()
    reopened_payee = zingo_recv.restart()
    require(reopened_payer["total_zat"] == payer_final["total_zat"], "engine balance changed on reopen")
    require(reopened_payee["total_zat"] == payee_final["total_zat"], "Zingo balance changed on reopen")
    engine_history = None
    reopen_ledger = False
    if engine != "zally":
        require(row_count(engine, reopened_payer, txid) == 1, "engine history did not retain one entry")
        engine_history = 1
        reopen_ledger = True
    require(row_count("zingo", reopened_payee, txid) == 1, "Zingo history did not retain one entry")
    return {"status": "passed", "direction": "engine-pays-zingo", "txid": txid,
            "amount_zat": amount, "memo_verified_by_payee": True, "payer_fee": paid,
            "payee_balance_after_zat": payee_final["total_zat"],
            "payer_balance_after_zat": payer_final["total_zat"],
            "leg_observations": {"outbound": outbound},
            "pending": {"node_mempool": True, "observed_height": pending_height,
                        "engine_wallet": engine_pending,
                        "zingo_payee_wallet": zingo_pending},
            "send_persistence": send_persistence,
            "reopen": {"balance": True, "engine_sent_row": engine_history,
                       "zingo_payee_row": 1},
            "reopened_ledger_verified": reopen_ledger,
            "engine_history_entries_after_reopen": engine_history,
            "zingo_history_entries_after_reopen": 1,
            "node_inclusion": inclusion(node, txid)}


def run_engine_receives(engine, node, ready, zingo_payer, receiver, receive_amount,
                        return_amount, receive_memo, return_memo):
    """A funded Zingo wallet pays the engine, which then spends the received funds."""
    engine_address = receiver.prepare()
    zingo_address = zingo_payer.prepare()
    require(engine_address != zingo_address, "payer and payee share a wallet")
    payer_before = zingo_payer.snapshot()
    payee_before = receiver.snapshot()
    require(spendable(payer_before) >= receive_amount + 100_000, "Zingo payer lacks spendable funds")
    receive_txid = zingo_payer.send(engine_address, receive_amount, receive_memo)
    pending_height = node.tip()
    require(receive_txid in node.mempool(), "Zingo payment is absent from the node mempool")
    zingo_pending = pending_observation("zingo", zingo_payer.snapshot(), receive_txid)
    engine_pending = None if engine == "zally" else \
        pending_observation(engine, receiver.snapshot(), receive_txid)
    mining_wait(node, ready)
    payee_after = receiver.sync()
    detail = receiver.detail(receive_txid) if engine != "zally" else {}
    received = engine_receive(engine, payee_after, detail, receive_txid, receive_amount, receive_memo)
    payer_row = zingo_payer.detail(receive_txid)
    zingo_fee = payer_row.get("fee_zat")
    require(isinstance(zingo_fee, int) and zingo_fee > 0, "Zingo payer fee evidence missing")
    require(receive_memo in payer_row["memos"], "Zingo sender did not record the memo it sent")
    require(received["mined_height"] == node.transaction(receive_txid)["height"],
            "engine payee height differs from node")
    received_note = next((row for row in engine_rows(engine, payee_after)
                          if row["txid"] == receive_txid and not row.get("is_change", False)), None)
    require(received_note is not None, "engine payee did not record the received note")
    note = {"tx_fingerprint_sha256": fingerprint(receive_txid),
            "output_index": received_note.get("output_index"),
            "value_zat": received["amount_zat"],
            "pool": received_note.get("pool")}
    if note["output_index"] is None:
        matches = [entry for entry in received_note.get("outputs", [])
                   if entry["value"] == receive_amount and not entry["is_change"]]
        if len(matches) == 1:
            note["output_index"] = matches[0]["output_index"]
    funded, payer_confirmed, inbound = settled_balances(node, ready, receive_txid, receiver, zingo_payer)
    require(payer_before["total_zat"] - payer_confirmed["total_zat"] == receive_amount + zingo_fee,
            "Zingo payer debit does not reconcile its fee")
    require(funded["total_zat"] == payee_before["total_zat"] + receive_amount,
            "engine payee balance did not rise by the exact amount")
    require(spendable(funded) >= return_amount, "engine cannot spend the received funds")
    if engine == "vizor":
        txid, sent_report = receiver.send(zingo_address, return_amount, return_memo)
    else:
        txid, sent_report = receiver.send(zingo_address, return_amount, return_memo), {}
    require(txid in node.mempool(), "engine return payment is absent from the node mempool")
    mining_wait(node, ready)
    return_observed = receiver.sync()
    detail = receiver.detail(txid) if engine != "zally" else {}
    return_delta = ledger_delta(engine, return_observed, txid)
    return_after, zingo_after_return, spendback = settled_balances(node, ready, txid, receiver, zingo_payer)
    returned = engine_payer(engine, receiver, zingo_address, return_amount, return_memo,
                            txid, sent_report, funded["total_zat"],
                            return_after["total_zat"], detail, return_delta)
    returned_credit = payee_credit_zingo(zingo_after_return, txid, return_amount, return_memo)
    require(zingo_after_return["total_zat"] == payer_confirmed["total_zat"] + return_amount,
            "Zingo balance did not rise by the returned amount")
    reopened_engine = receiver.restart()
    reopened_zingo = zingo_payer.restart()
    require(reopened_engine["total_zat"] == return_after["total_zat"], "engine balance changed on reopen")
    require(reopened_zingo["total_zat"] == zingo_after_return["total_zat"], "Zingo balance changed on reopen")
    reopen_engine_rows = None
    reopen_ledger = False
    if engine != "zally":
        require(row_count(engine, reopened_engine, receive_txid) == 1,
                "engine history lost or duplicated the received transaction")
        require(row_count(engine, reopened_engine, txid) == 1,
                "engine history lost or duplicated the returned transaction")
        reopen_engine_rows = {"receive": 1, "return": 1}
        reopen_ledger = True
    else:
        rows = [row for row in reopened_engine["receives"] if row["txid"] == receive_txid]
        require(len(rows) == 1, "Zally receives lost or duplicated the funding transaction")
        reopen_engine_rows = {"receive": 1}
    require(row_count("zingo", reopened_zingo, receive_txid) == 1, "Zingo lost the payment entry")
    require(row_count("zingo", reopened_zingo, txid) == 1, "Zingo lost the returned entry")
    spent_evidence = None
    unspent_after = []
    if engine == "zally":
        unspent_txids = {row["txid"] for row in return_after["unspent"]}
        return_change = [row for row in reopened_engine["receives"]
                        if row["txid"] == txid and row["is_change"]]
        require(len(return_change) == 1, "Zally did not record the return change row")
        require(receive_txid not in unspent_txids,
                "the received note is still in the Zally unspent set")
        spent_evidence = "received note absent from the unspent set; return change row present"
        reopen_engine_rows["return_change"] = len(return_change)
        unspent_after = [{"tx_fingerprint_sha256": fingerprint(row["txid"]),
                          "value_zat": row["value"], "pool": row["pool"]}
                         for row in return_after["unspent"]]
    else:
        spent_evidence = "ledger-derived (received row plus return sent row)"
        require(row_count(engine, return_after, txid) == 1, "missing return sent row")
    received_funds_spent = bool(
        return_amount > 0
        and return_after["total_zat"] == funded["total_zat"] - return_amount
        - returned["fee_zat"])
    require(received_funds_spent, "engine return does not reconcile against the received funds")
    return {"status": "passed", "direction": "zingo-pays-engine", "receive_txid": receive_txid,
            "receive_amount_zat": receive_amount,
            "memo_verified_by_engine": received["memo_verified"],
            "engine_memo_status": "decrypted" if received["memo_verified"]
                else "sender recorded the memo; the receiving wallet stored no memo for the output",
            "zingo_payer_fee_zat": zingo_fee,
            "leg_observations": {
                "inbound": {**inbound, "before_return": True},
                "return": spendback,
            },
            "pending": {"node_mempool": True, "observed_height": pending_height,
                        "zingo_payer_wallet": zingo_pending,
                        "engine_payee_wallet": engine_pending},
            "received_note": note,
            "received_note_spent": spent_evidence,
            "unspent_after": unspent_after,
            "received_funds_spent": received_funds_spent, "return_txid": txid,
            "return_amount_zat": return_amount, "return_memo_verified_by_zingo":
                returned_credit["memo_verified"],
            "engine_return_fee": returned,
            "engine_balance_after_zat": return_after["total_zat"],
            "zingo_balance_after_zat": zingo_after_return["total_zat"],
            "reopen": {"balance": True, "engine_rows": reopen_engine_rows,
                       "zingo_rows": {"receive": 1, "return": 1}},
            "reopened_ledger_verified": reopen_ledger,
            "node_inclusion": [inclusion(node, receive_txid), inclusion(node, txid)]}


def missing_api_evidence(engine):
    evidence = []
    if engine == "devtool":
        evidence.append({"capability": "method-level RPC attribution",
                         "status": "unexercised (no payload capture)",
                         "detail": "every online devtool command is traced for connect syscalls to the loopback lightwalletd port and the endpoint's live identity and anchor are probed before and after use; the trace does not decode gRPC methods or payloads"})
        evidence.append({"capability": "unified-address receiver",
                         "status": "unexercised (Sapling receiver used)",
                         "detail": "the payer and payee roles use the Sapling receiver; the unified receiver is available but unqualified here"})
    if engine == "zally":
        evidence.extend([
            {"capability": "outgoing transaction history",
             "status": "unexercised (no public API)",
             "detail": "zally-wallet exposes list_shielded_receives and list_unspent_shielded_notes; there is no sent-transaction list, so a payer history row cannot be asserted"},
            {"capability": "payer fee read",
             "status": "unexercised (no public API)",
             "detail": "no fee accessor on SendOutcome; the fee is residual-only, derived from the persisted balance delta and labelled as such"},
            {"capability": "wallet shielded pending state",
             "status": "unexercised (no public API)",
             "detail": "pending observation for the shielded payment is the control node mempool check, recorded with its observed height"},
            {"capability": "wallet endpoint attribution",
             "status": "unexercised (no process trace)",
             "detail": "the native server's live revision and anchor are checked by an out-of-band ServerInfo and TreeStateAtHeight probe; the wallet process itself is not traced"},
        ])
    if engine == "vizor":
        evidence.append({"capability": "mobile application",
                         "status": "out of scope",
                         "detail": "only the Linux Rust engine is exercised"})
    return evidence


def git_state(path):
    path = Path(path)
    if not (path / ".git").exists() and not (path / ".git").is_file():
        return {"git_metadata": False}
    commit = subprocess.check_output(["git", "-C", str(path), "rev-parse", "HEAD"], text=True).strip()
    status = subprocess.check_output(["git", "-C", str(path), "status", "--porcelain"], text=True).strip()
    return {"git_metadata": True, "commit": commit, "clean": not bool(status)}


def rust_version():
    return subprocess.check_output(["rustc", "--version"], text=True).strip()


def provenance(engine, args, engine_binary):
    record = {"engine_binary_sha256": digest(engine_binary), "rustc": rust_version()}
    if engine == "devtool":
        state = git_state(args.devtool_source)
        record.update({"source": str(args.devtool_source), **state,
                       "lock_sha256": digest(Path(args.devtool_source) / "Cargo.lock")
                       if (Path(args.devtool_source) / "Cargo.lock").is_file() else None,
                       "build_command": "cargo build --locked --features regtest_support"})
        record["label"] = "zcash-devtool c8322f7e"
        record["label_verified"] = state.get("commit") == "c8322f7e71ae46ab24801a721523ba83b27d911b" \
            and state.get("clean") is True
    elif engine == "zally":
        state = git_state(args.zally_source)
        record.update({"source": str(args.zally_source), **state,
                       "lock_sha256": digest(Path(args.zally_source) / "Cargo.lock")
                       if (Path(args.zally_source) / "Cargo.lock").is_file() else None,
                       "harness_sha256": digest(HERE / "zally-payment.rs"),
                       "build_command": "cargo build --locked (out-of-tree harness, --features zinder)"})
        record["label"] = "Zally 31ee6c3a"
        record["label_verified"] = state.get("commit") == "31ee6c3ae09e109d189c6dcd691b192fb4c5b29e" \
            and state.get("clean") is True
    else:
        provenance_path = args.vizor_provenance
        source = json.loads(provenance_path.read_text()) if provenance_path.is_file() else {}
        build_log = args.vizor_build_log
        record.update({
            "source": str(args.vizor_source),
            "source_commit": source.get("vizor_source"),
            "archive_sha256": source.get("release_archive_sha256"),
            "lock_sha256": source.get("lock_sha256"),
            "driver_sha256": digest(HERE / "vizor-payment.rs"),
            "provenance_sha256": digest(provenance_path) if provenance_path.is_file() else None,
            "build_log_sha256": digest(build_log) if build_log and Path(build_log).is_file() else None,
            "build_command": "cargo build --locked --example vizor-payment",
        })
        record["label"] = "Vizor v0.0.65"
        record["label_verified"] = (
            source.get("vizor_source") == "d0edb5b13890dd33d9bb93b5c4e50fd9973fa4e6"
            and source.get("release_archive_sha256")
            == "0641e4f5ad8a37250dde1f90bd4faf36b981ed4bf07ff9048966d94ea5667d0c")
    return record


def zingo_provenance(zingo):
    expected = "0eb8c60c097058f9750caaafcc27e9e3692a9b6cff6927f92e684784161ea79a"
    version = subprocess.check_output([str(zingo), "--version"], text=True).strip()
    observed = digest(zingo)
    return {"sha256": observed, "version": version, "source": "81d799775f11e882896e8710e8af12d6d4d29a41",
            "label": "Zingo CLI from Zingolib 81d7997", "label_verified": observed == expected}


def native_probe(args, ready, revision):
    command = [str(args.native_probe_python), str(HERE / "native-anchor.py"),
               "--proto-dir", str(args.native_proto_dir),
               "--endpoint", ready["native_wallet_query"],
               "--height", str(ready["current_tip"]), "--hash", ready["current_hash"],
               "--revision", revision]
    output = subprocess.run(command, capture_output=True, text=True, check=True).stdout
    lines = [line for line in output.splitlines() if line.strip()]
    result = json.loads(lines[-1])
    require(result.get("ok") is True, "native server identity or anchor differs")
    return result


def lightwalletd_probe(args, ready, height, expected_height, expected_hash, revision,
                       phase, expected_chain="test"):
    command = [str(args.identity_probe_python), str(HERE / "lightwalletd-identity.py"),
               "--proto-dir", str(args.identity_proto_dir),
               "--endpoint", ready["lightwalletd"],
               "--height", str(height), "--expect-height", str(expected_height),
               "--hash", expected_hash, "--revision", revision,
               "--expected-chain", expected_chain, "--phase", phase]
    completed = subprocess.run(command, capture_output=True, text=True, timeout=120)
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    require(lines, f"lightwalletd identity probe {phase} produced no record")
    record = json.loads(lines[-1])
    record["command_exit"] = completed.returncode
    return record


def require_identity(record, ready, revision, where):
    require(record.get("command_exit") == 0 and record.get("ok") is True,
            f"lightwalletd identity probe {where} did not pass")
    require(record.get("endpoint") == ready["lightwalletd"],
            f"lightwalletd identity probe {where} dialled another endpoint")
    require(record.get("build_git_commit") == revision
            and record.get("revision_verified") is True,
            f"lightwalletd identity probe {where} saw another build commit")
    require(record.get("chain_name") == "test"
            and record.get("network_verified") is True,
            f"lightwalletd identity probe {where} saw another chain")
    require(record.get("anchor_height") == ready["current_tip"]
            and record.get("anchor_hash") == ready["current_hash"]
            and record.get("anchor_verified") is True,
            f"lightwalletd identity probe {where} saw another anchor")
    require(record.get("served_height") == record.get("served_tip_height"),
            f"lightwalletd identity probe {where} saw a lagging tip")


def lightwalletd_negatives(args, ready, node, revision):
    """Prove the identity, chain, and anchor comparisons can fail."""
    stale = ("1" if revision[0] == "0" else "0") + revision[1:]
    revision_control = lightwalletd_probe(
        args, ready, ready["current_tip"], ready["current_tip"], ready["current_hash"],
        stale, "negative-stale-revision")
    require(revision_control.get("command_exit") != 0
            and revision_control.get("ok") is False
            and revision_control.get("revision_verified") is False
            and revision_control.get("network_verified") is True
            and revision_control.get("anchor_verified") is True,
            "stale-revision negative did not reject only the revision")
    chain_control = lightwalletd_probe(
        args, ready, ready["current_tip"], ready["current_tip"], ready["current_hash"],
        revision, "negative-wrong-chain", expected_chain="main")
    require(chain_control.get("command_exit") != 0
            and chain_control.get("ok") is False
            and chain_control.get("network_verified") is False
            and chain_control.get("revision_verified") is True
            and chain_control.get("anchor_verified") is True,
            "wrong-chain negative did not reject only the chain name")
    previous_height = ready["current_tip"] - 1
    previous_hash = node.call("getblockhash", previous_height)
    anchor_control = lightwalletd_probe(
        args, ready, previous_height, ready["current_tip"], ready["current_hash"],
        revision, "negative-stale-anchor")
    require(anchor_control.get("command_exit") != 0
            and anchor_control.get("ok") is False
            and anchor_control.get("anchor_verified") is False
            and anchor_control.get("revision_verified") is True
            and anchor_control.get("network_verified") is True
            and anchor_control.get("anchor_height") == previous_height
            and anchor_control.get("anchor_hash") == previous_hash,
            "stale-anchor negative did not reject only the anchor")
    return [revision_control, chain_control, anchor_control]


def wallet_endpoint_binding(wallets):
    """Collect per-command connect traces from the devtool wallets."""
    bindings = {name: wallet.endpoint_binding() for name, wallet in wallets.items()
                if wallet is not None}
    require(bindings, "no devtool wallet recorded endpoint traces")
    require(all(binding["bound"] is True for binding in bindings.values()),
            "a devtool command was not traced to the wallet endpoint")
    require(all(binding["endpoint"] == next(iter(bindings.values()))["endpoint"]
                for binding in bindings.values()),
            "devtool wallets were configured with different endpoints")
    return bindings


def endpoint_evidence(engine, native, live=None):
    if engine == "vizor":
        return {"scope": "driver-verified",
                "lightwalletd_identity": "every engine command checks GetLightdInfo revision and the fixture anchor block",
                "anchor_verified": True}
    if engine == "devtool":
        require(live is not None and live.get("after") is not None,
                "devtool endpoint evidence is missing the live probes")
        before, after = live["before"], live["after"]
        for key in ("build_git_commit", "chain_name", "expected_hash"):
            require(before.get(key) == after.get(key),
                    f"lightwalletd identity changed between probes: {key}")
        require(after.get("served_height", 0) >= before.get("served_height", 0),
                "lightwalletd served height regressed between probes")
        wallet_identity = live.get("wallet_identity")
        require(isinstance(wallet_identity, dict) and wallet_identity,
                "devtool endpoint evidence is missing the wallet's own server view")
        for wallet, observations in wallet_identity.items():
            for phase, observed in observations.items():
                require(observed.get("server_uri") == live["endpoint"],
                        f"{wallet} {phase} reports another server")
                require(observed.get("chain_name") == "test",
                        f"{wallet} {phase} reports another chain")
        return {"scope": "live-probe-before-and-after",
                "lightwalletd_identity": ("GetLightdInfo build commit and chain name plus the "
                                          "GetBlock hash at the readiness anchor, read from the "
                                          "wallet's loopback endpoint before admission and "
                                          "after use"),
                "lightwalletd_server_info": after,
                "observations": {"before_admission": before, "after_use": after},
                "negative_controls": live["negative_controls"],
                "wallet_endpoint": live["endpoint"],
                "wallet_server_info": wallet_identity,
                "wallet_commands": live["wallet_commands"],
                "anchor_verified": True,
                "provenance": {
                    "probe": "scripts/wallet-clients/lightwalletd-identity.py",
                    "probe_sha256": digest(HERE / "lightwalletd-identity.py"),
                    "transport": "grpc insecure loopback GetLightdInfo/GetBlock",
                    "scope": "server identity and anchor only; no wallet payloads or "
                             "method-level RPC attribution",
                }}
    return {"scope": "native-probe",
            "native_server_info": native,
            "anchor_verified": native.get("anchor_verified") if native else False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", required=True, choices=sorted(BASES))
    parser.add_argument("--base", type=int)
    parser.add_argument("--evidence", type=Path, help="new private evidence directory for the receipt")
    parser.add_argument("--ready", type=Path, help="reuse an existing immutable fixture receipt")
    parser.add_argument("--state-root", type=Path, help="fixture state root for --ready reuse")
    parser.add_argument("--project", help="existing fixture project for --ready reuse")
    parser.add_argument("--roles", default="both", choices=["both", "engine-pays", "engine-receives"])
    parser.add_argument("--keep", action="store_true", help="leave the fixture running")
    parser.add_argument("--release-tag", required=True,
                        help="published Zinder release tag, resolved to its commit in this checkout")
    parser.add_argument("--images", type=Path, help="JSON map of the four runtime image references")
    parser.add_argument("--bin-dir", type=Path, help="release binaries mounted over the runtime images")
    parser.add_argument("--revision", help="expected Zinder source commit (defaults to the release tag's commit)")
    parser.add_argument("--node-config", type=Path, help="Regtest node TOML for the fixture")
    parser.add_argument("--fixture-state", type=Path, help="private fixture state root for an owned run")
    parser.add_argument("--native-probe-python", type=Path,
                        help="Python interpreter with grpcio for the native anchor probe (Zally)")
    parser.add_argument("--native-proto-dir", type=Path,
                        help="generated native protobuf modules (Zally)")
    parser.add_argument("--zingo", type=Path, help="pinned Zingo CLI binary")
    parser.add_argument("--vizor-engine", type=Path, help="built Vizor payment driver binary")
    parser.add_argument("--vizor-source", type=Path, help="pinned Vizor source checkout")
    parser.add_argument("--vizor-provenance", type=Path, help="Vizor engine-source provenance JSON")
    parser.add_argument("--vizor-build-log", type=Path, help="Vizor build log (optional provenance)")
    parser.add_argument("--devtool", type=Path, help="built zcash-devtool binary")
    parser.add_argument("--identity-probe-python", type=Path, help="Python with grpcio for devtool")
    parser.add_argument("--identity-proto-dir", type=Path, help="generated lightwalletd protobuf modules for devtool")
    parser.add_argument("--devtool-source", type=Path, help="pinned zcash-devtool checkout")
    parser.add_argument("--zally-source", type=Path, help="pinned Zally checkout")
    parser.add_argument("--zally-binary", type=Path, help="built Zally payment harness binary")
    parser.add_argument("--zally-build", type=Path, help="Zally harness build directory (only when --zally-binary is absent)")
    args = parser.parse_args()
    os.umask(0o077)
    engine = args.engine
    revision = reconcile_revision(args.release_tag, args.revision)
    missing = engine_required_inputs(engine, args)
    require(not missing,
            "missing explicit input(s) for engine " + engine + ": " + ", ".join(
                "--" + name.replace("_", "-") for name in missing))
    zingo = args.zingo.resolve()
    evidence = args.evidence.resolve()
    evidence.mkdir(mode=0o700, parents=True)
    bin_dir = args.bin_dir.resolve()
    images_path = args.images.resolve()
    images_name = images_filename(args.release_tag)
    node_config_default = args.node_config.resolve() if args.node_config else NODE_CONFIG
    extra = {}
    if engine == "vizor":
        extra["vizor_engine"] = args.vizor_engine.resolve()
    elif engine == "devtool":
        extra["devtool"] = args.devtool.resolve()
    elif args.zally_binary is not None:
        extra["zally_binary"] = args.zally_binary.resolve()
    else:
        module = importlib.import_module("zally-payment")
        extra["zally_binary"] = module.build(args.zally_source, args.zally_build,
                                             HERE / "zally-payment.rs")
    engine_binary = extra["zally_binary"] if engine == "zally" else \
        extra["vizor_engine"] if engine == "vizor" else extra["devtool"]
    require(Path(engine_binary).is_file(), "engine binary is missing: " + str(engine_binary))
    input_paths = [engine_binary, zingo, HERE / "payment-roles.py",
                   HERE / f"{engine}-payment.py", ROOT / "scripts/regtest-fixture.py",
                   ROOT / "scripts/wallet-clients/wallet_fixture.py",
                   ROOT / "scripts/wallet-clients/roundtrip_wallets.py",
                   ROOT / "scripts/wallet-clients/zingo-payment.py"]
    if engine == "vizor":
        input_paths.extend([HERE / "vizor-payment.rs", args.vizor_provenance])
    if engine == "devtool":
        require(args.identity_probe_python.is_file(),
                "lightwalletd identity probe interpreter is missing")
        require(args.identity_proto_dir.is_dir(),
                "lightwalletd identity proto directory is missing")
        input_paths.extend([Path(args.devtool_source) / "Cargo.lock",
                            HERE / "lightwalletd-identity.py",
                            ROOT / "scripts/trace-wallet-endpoint.py",
                            args.identity_probe_python,
                            args.identity_proto_dir / "service_pb2.py",
                            args.identity_proto_dir / "compact_formats_pb2.py"])
    if engine == "zally":
        input_paths.extend([HERE / "zally-payment.rs", HERE / "native-anchor.py",
                            Path(args.zally_source) / "Cargo.lock",
                            args.native_proto_dir / "zinder/v1/wallet/wallet_pb2.py",
                            args.native_proto_dir / "zinder/v1/ops/server_info_pb2.py"])
    if images_path:
        input_paths.append(images_path)
    if bin_dir:
        input_paths.append(bin_dir)
    owned = args.ready is None
    project = None
    state_root = None
    ready = None
    ready_path = None
    receipt = {"schema_version": 1, "engine": engine, "roles": args.roles,
               "amounts_zat": {"engine_pays": AMOUNT_ENGINE_PAYS,
                               "engine_receives": AMOUNT_ENGINE_RECEIVES,
                               "engine_returns": AMOUNT_ENGINE_RETURNS},
               "memos": {"engine_pays": MEMO_ENGINE_PAYS,
                         "engine_receives": MEMO_ENGINE_RECEIVES,
                         "engine_returns": MEMO_ENGINE_RETURNS}}
    role_status = False
    before_inputs = None
    try:
        if owned:
            base = args.base or BASES[engine]
            require(base >= 1024 and base + 82 <= 65535,
                    "fixture base must fit unprivileged TCP ports")
            project = f"zinder-fixture-pay-{engine}-{base}-{secrets.token_hex(6)}"
            state_root = args.fixture_state.resolve() if args.fixture_state else evidence / "fixtures"
            node_config = node_config_default
            if engine == "vizor":
                node_config = evidence / "regtest-nu6.3-at-2.toml"
                node_config.write_text(node_config_default.read_text().replace('"NU6.3" = 1',
                                                                               '"NU6.3" = 2'))
                require('"NU6.3" = 2' in node_config.read_text(),
                        "could not derive the Vizor schedule")
            input_paths.append(node_config)
            before_inputs = input_hashes(input_paths)
            ready, ready_path = fixture_up(state_root, base, evidence, project, node_config,
                                           images_path=images_path, bin_dir=bin_dir,
                                           revision=revision, images_name=images_name)
        else:
            require(args.project and args.state_root,
                    "--ready reuse requires --project and --state-root")
            project, state_root = args.project, args.state_root.resolve()
            ready_path = args.ready.resolve()
            require(ready_path == state_root / project / "ready.initial.json",
                    "--ready must be the named project's immutable readiness receipt")
            ready = json.loads(ready_path.read_text())
            require(ready.get("project") == project, "reused fixture receipt names another project")
            before_inputs = input_hashes(input_paths)
        from wallet_fixture import binding, loopback
        binding(ready_path)
        for service in ("compat_ops", "query_ops", "native_wallet_query"):
            loopback(ready[service])
        require(ready["zinder_commit"] == revision, "fixture does not run the selected revision")
        node = Node(ready["node_rpc"])
        verify_services(ready, revision)
        wait_served(ready, node.tip())
        require(node.call("getblockhash", ready["current_tip"]) == ready["current_hash"],
                "control node anchor differs from the fixture receipt")
        native = native_probe(args, ready, revision) if engine == "zally" else None
        live = None
        if engine == "devtool":
            before = lightwalletd_probe(args, ready, ready["current_tip"],
                                        ready["current_tip"], ready["current_hash"],
                                        revision, "before-admission")
            require_identity(before, ready, revision, "before admission")
            live = {"before": before, "endpoint": ready["lightwalletd"],
                    "negative_controls": lightwalletd_negatives(args, ready, node, revision)}
        template = provenance(engine, args, engine_binary)
        zingo_record = zingo_provenance(zingo)
        pins = {"release": args.release_tag, "release_commit": revision,
                "fixture_ready_sha256": digest(ready_path), "node_image": ready.get("node_image"),
                "activation_schedule_sha256": ready.get("activation_schedule_sha256"),
                "zingo_sha256": zingo_record["sha256"], "engine_binary_sha256": digest(engine_binary),
                "fixture_project": project, "fixture_base": ready.get("port_base"),
                "fixture_node_rpc": ready["node_rpc"], "client_inputs_sha256": before_inputs}
        if (evidence / images_name).is_file():
            pins["images_reference_sha256"] = digest(evidence / images_name)
        receipt.update({"pins": pins, "provenance": {"engine": template, "zingo": zingo_record},
                        "fixture_isolation": ready.get("isolation"),
                        "raw_blob_policy": ready.get("raw_blob_policy")})
        engine_payer_wallet = make_engine(engine, evidence / "wallets/engine-pays", ready,
                                          extra, revision)
        payer_address = engine_payer_wallet.prepare()
        if live is not None:
            live["wallet_identity"] = {
                "engine-pays": {"before_admission": engine_payer_wallet.observe_server()}}
        block = node.call("generatetoaddress", 1, payer_address)[0]
        funding = node.block(block)
        mining_wait(node, ready, 100)
        funded = engine_payer_wallet.sync()
        require(spendable(funded) >= AMOUNT_ENGINE_PAYS + 100_000,
                "engine payer funding did not mature")
        receipt["engine_payer_funding"] = {"tx_fingerprint_sha256": fingerprint(funding["tx"][0]),
                                           "mined_height": funding["height"],
                                           "spendable_zat": spendable(funded)}
        zingo_recv = ZingoCounterparty(zingo, evidence / "wallets/zingo-pays",
                                       ready["lightwalletd"], ready["node_rpc"])
        zingo_payer = ZingoCounterparty(zingo, evidence / "wallets/zingo-sends",
                                        ready["lightwalletd"], ready["node_rpc"])
        if args.roles in ("both", "engine-pays"):
            receipt["engine_pays"] = run_engine_pays(
                engine, node, ready, evidence, zingo_recv, engine_payer_wallet,
                AMOUNT_ENGINE_PAYS, MEMO_ENGINE_PAYS)
        if args.roles in ("both", "engine-receives"):
            funded_zingo = zingo_payer.fund()
            receipt["zingo_payer_funding"] = {
                "tx_fingerprint_sha256": fingerprint(funded_zingo["shield_txid"]),
                "mined_height": node.transaction(funded_zingo["shield_txid"])["height"],
                "spendable_zat": funded_zingo["spendable_zat"]}
            engine_receiver = make_engine(engine, evidence / "wallets/engine-receives", ready,
                                          extra, revision)
            engine_receiver.prepare()
            receipt["engine_receives"] = run_engine_receives(
                engine, node, ready, zingo_payer, engine_receiver,
                AMOUNT_ENGINE_RECEIVES, AMOUNT_ENGINE_RETURNS,
                MEMO_ENGINE_RECEIVES, MEMO_ENGINE_RETURNS)
        if engine == "devtool":
            after = lightwalletd_probe(args, ready, ready["current_tip"],
                                       ready["current_tip"], ready["current_hash"],
                                       revision, "after-use")
            require_identity(after, ready, revision, "after use")
            require(after["served_height"] >= live["before"]["served_height"],
                    "lightwalletd served height regressed during the run")
            payer_identity = engine_payer_wallet.observe_server()
            live["wallet_identity"]["engine-pays"]["after_use"] = payer_identity
            require(payer_identity["chain_tip_height"]
                    >= live["wallet_identity"]["engine-pays"]["before_admission"]["chain_tip_height"],
                    "the wallet saw a regressed chain tip")
            if args.roles in ("both", "engine-receives"):
                live["wallet_identity"]["engine-receives"] = {
                    "after_use": engine_receiver.observe_server()}
            live["after"] = after
            live["wallet_commands"] = wallet_endpoint_binding(
                {"engine-pays": engine_payer_wallet,
                 "engine-receives": engine_receiver
                 if args.roles in ("both", "engine-receives") else None})
        receipt["endpoint_evidence"] = endpoint_evidence(engine, native, live)
        role_status = True
    except Exception as error:
        receipt["role_error"] = f"{type(error).__name__}: {error}"
    finally:
        cleanup = {"owned": owned}
        cleanup_ok = False
        if owned and not args.keep and project and state_root:
            try:
                fixture_down(state_root, project)
                resources = project_resources(project)
                cleanup_ok = all(not remaining for remaining in resources.values())
                cleanup.update({"fixture_removed": True, "verified": cleanup_ok,
                                "remaining_resources":
                                    {kind: len(items) for kind, items in resources.items()}})
            except (subprocess.CalledProcessError, OSError) as error:
                cleanup.update({"fixture_removed": False, "verified": False,
                                "error": str(error)[:300]})
        elif owned:
            cleanup.update({"fixture_removed": False, "kept_by_request": True, "verified": False})
        else:
            cleanup.update({"reused_fixture": True, "verified": None})
        receipt["cleanup"] = cleanup
        after_inputs = input_hashes(input_paths)
        receipt["client_inputs_sha256_after"] = after_inputs
        inputs_stable = before_inputs is not None and after_inputs == before_inputs
        receipt["inputs_stable"] = inputs_stable
        labels_verified = (receipt.get("provenance", {}).get("engine", {}).get("label_verified")
                           is True and
                           receipt.get("provenance", {}).get("zingo", {}).get("label_verified")
                           is True)
        receipt["labels_verified"] = labels_verified
        receipt["missing_api_evidence"] = missing_api_evidence(engine)
        if receipt.get("engine_receives") and \
                not receipt["engine_receives"]["memo_verified_by_engine"]:
            receipt["missing_api_evidence"].append({
                "capability": "payee memo decryption",
                "status": "unexercised (no retained memo)",
                "detail": ("the Zingo sender recorded the memo and the engine credited the exact "
                           "amount, but the engine's persisted receive row stored no memo for the "
                           "output"),
            })
        receipt["evidence_limits"] = {
            "network": "isolated zcash-regtest fixture owned by this run",
            "public_network_payments": "none",
            "production_capacity": "not measured",
        }
        if engine == "vizor":
            receipt["evidence_limits"]["activation_schedule"] = (
                "NU6.3 activates at height 2 because the Vizor engine refuses a lower "
                "Regtest height; all wallet birthdays are at or above 150 and the Zingo "
                "counterparty never scans block 1")
        if engine == "devtool":
            receipt["evidence_limits"]["receiver_pool"] = "Sapling receiver for both roles"
        if not role_status:
            receipt["status"] = "failed"
        elif not inputs_stable:
            receipt["status"] = "failed"
            receipt["role_error"] = "a bound client, image, or source input changed during the run"
        elif not labels_verified:
            receipt["status"] = "unverified-inputs"
        elif args.roles != "both":
            receipt["status"] = "partial"
        elif owned and args.keep:
            receipt["status"] = "roles-passed-fixture-retained"
        elif owned and not cleanup_ok:
            receipt["status"] = "failed"
            receipt["role_error"] = "fixture cleanup is missing or unverified"
        elif not owned:
            receipt["status"] = "roles-passed-fixture-retained"
        else:
            receipt["status"] = "passed"
        write_json(evidence / "receipt.json", receipt)
    print(json.dumps({"status": receipt["status"], "engine": engine,
                      "receipt": str(evidence / "receipt.json")}))
    if receipt["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    sys.path.insert(0, str(HERE))
    main()
