#!/usr/bin/env python3
"""Exercise a Zingo to Zallet payment and return, on disposable Regtest or live Testnet."""
import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import uuid

CLIENTS = Path(__file__).resolve().parent / "wallet-clients"
sys.path.insert(0, str(CLIENTS))
from coverage_roundtrip import compare_runs

FAUCET = "https://fauzec.com/api/v1"
OUTBOUND = 10_000_000
RETURN = 4_000_000
DRIP = 100_000_000
TXID = re.compile(r"[0-9a-f]{64}")


def require(value, message):
    if not value:
        raise ValueError(message)


def digest(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def fingerprint(text):
    return hashlib.sha256(text.encode()).hexdigest()


def write_json(path, value):
    temporary = path.with_suffix(".tmp")
    with temporary.open("w") as dest:
        json.dump(value, dest, indent=2, sort_keys=True)
        dest.write("\n")
        dest.flush()
        os.fsync(dest.fileno())
    temporary.replace(path)
    fd = os.open(path.parent, os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class Journal:
    def __init__(self, path, config):
        self.path = path
        if path.exists():
            self.data = json.loads(path.read_text())
            require(self.data["config"] == config, "state belongs to different clients, fixture or network")
        else:
            self.data = {"schema_version": 1, "config": config, "operations": {}, "observations": {}}
            self.save()

    def save(self):
        write_json(self.path, self.data)

    def observe(self, key, value):
        self.data["observations"][key] = value
        self.save()
        return value

    def once(self, name, intent, act, receipt_key="txid", reconcile=None):
        operations = self.data["operations"]
        if name in operations:
            previous = operations[name]
            require(previous["intent"] == intent, "stored operation intent differs: " + name)
            if receipt_key not in previous and reconcile:
                restored = reconcile()
                if restored and TXID.fullmatch(restored):
                    previous[receipt_key] = restored
                    previous["recovered_from_wallets"] = True
                    self.save()
            require(receipt_key in previous, f"{name}: submission outcome unknown; inspect the existing intent before retrying")
            return previous[receipt_key]
        operations[name] = {"intent": intent}
        self.save()
        result = act()
        if receipt_key == "txid":
            require(isinstance(result, str) and TXID.fullmatch(result), f"{name}: invalid transaction ID after submission")
        operations[name][receipt_key] = result
        self.save()
        return result


@contextmanager
def locked(directory):
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    require(directory.stat().st_mode & 0o077 == 0, "wallet state directory must be private (mode 0700)")
    with (directory / ".lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError("this wallet run is already open") from error
        yield


def faucet_api(path, data=None):
    body = json.dumps(data).encode() if data is not None else None
    req = Request(FAUCET + path, body, {"Content-Type": "application/json"})
    try:
        with urlopen(req, timeout=20) as result:
            return json.load(result)
    except HTTPError as error:
        try:
            detail = json.load(error).get("error_code", "")
        except (ValueError, AttributeError):
            detail = ""
        raise ValueError(f"Fauzec {path.split('/')[1]} returned HTTP {error.code} {detail}") from None


def wait_for(label, timeout, interval, inspect):
    deadline = time.monotonic() + timeout
    while True:
        result = inspect()
        if result:
            return result
        if time.monotonic() >= deadline:
            raise TimeoutError(f"{label}: timed out; rerun with the same state directory")
        time.sleep(interval)


def wallet_tx(snapshot, txid):
    matches = [row for row in snapshot["transactions"] if row["txid"] == txid]
    require(len(matches) <= 1, "wallet returned duplicate transaction entries")
    return matches[0] if matches else None


class RoundTrip:
    def __init__(self, network, fixture, rpc, journal, zingo, zallet, timeout):
        self.network, self.fixture, self.rpc = network, fixture, rpc
        self.journal, self.zingo, self.zallet = journal, zingo, zallet
        self.timeout = timeout
        self.regtest = network == "regtest"

    def node(self, method, *params):
        return self.rpc(self.fixture["node_rpc"], method, *params)

    def wait(self, label, inspect):
        return wait_for(label, self.timeout, 2 if self.regtest else 10, inspect)

    def mine_to(self, name, count):
        require(self.regtest, "mining requires Regtest")
        heights = self.journal.data.setdefault("mine_targets", {})
        tip = self.node("getblockcount")
        if name not in heights:
            heights[name] = tip + count
            self.journal.save()
        if tip < heights[name]:
            self.node("generate", heights[name] - tip)

    def mined(self, txid):
        def check():
            if txid in self.node("getrawmempool"):
                return None
            from roundtrip_fixture import NodeRpcError
            try:
                tx = self.node("getrawtransaction", txid, 1)
            except NodeRpcError as error:
                if error.code == -5:
                    return None
                raise
            if not tx.get("blockhash") or not tx.get("confirmations", 0):
                return None
            require(tx["txid"] == txid and self.node("getblockhash", tx["height"]) == tx["blockhash"],
                    "transaction is outside the active chain")
            return {"height": tx["height"], "hash": tx["blockhash"]}
        return self.wait("canonical inclusion", check)

    def confirm(self, name, txid):
        if self.regtest:
            self.mine_to(name, 10)
        return self.mined(txid)

    def fund_live(self, address):
        require(self.network == "testnet" and address.startswith("utest1"), "expected a new Testnet Unified Address")
        j = self.journal
        if "claim" not in j.data:
            info = faucet_api("/network")
            health = faucet_api("/faucet-status")
            require(info.get("network") == health.get("network") == "testnet" and
                    "unified" in info["supported_address_kinds"] and info["drip_zat"] == DRIP and
                    health.get("cause") == "ready", "faucet unavailable for requested Testnet drip")
            j.data["claim"] = {"request_id": str(uuid.uuid4()), "address": address,
                               "amount_zat": DRIP, "memo": "zinder-roundtrip-v1/funding", "network": "testnet"}
            j.save()
        claim = j.data["claim"]
        require(claim["address"] == address, "faucet recipient changed")
        name = "fauzec-funding"
        if name not in j.data["operations"]:
            def submit():
                response = faucet_api("/claim", claim)
                require(response.get("request_id") == claim["request_id"] and response.get("network") == "testnet"
                        and response.get("state") != "failed", "faucet rejected the persisted claim")
                return claim["request_id"]
            j.once(name, claim, submit, receipt_key="request_id")
        def status():
            reply = faucet_api("/status/testnet/" + claim["request_id"])
            require(all(reply.get(k) == claim[k] for k in ("request_id", "network", "address", "amount_zat")),
                    "faucet status does not describe this claim")
            require(reply.get("state") != "failed", f"faucet claim failed: {reply.get('error_code')}")
            if reply.get("state") == "confirmed":
                require(TXID.fullmatch(reply.get("txid", "")), "confirmed claim missing transaction ID")
                return reply["txid"]
            return None
        txid = self.wait("Fauzec confirmed status", status)
        j.data["operations"][name].update(request_id=claim["request_id"], txid=txid)
        j.save()
        return txid

    def fund_regtest(self):
        address = self.zingo.transparent_address()
        j = self.journal
        tip = j.data["birthday_anchor"]
        # The coinbase at this fixed height identifies a previous successful mining call after interruption.
        if self.node("getblockcount") == tip:
            self.node("generatetoaddress", 1, address)
        block = self.node("getblock", self.node("getblockhash", tip + 1), 1)
        coinbase = self.node("getrawtransaction", block["tx"][0], 1)
        outputs = [o["scriptPubKey"] for o in coinbase["vout"]]
        require(any(address in s.get("addresses", []) or address == s.get("address") for s in outputs),
                "fixture funding height belongs to a different recipient")
        height = self.node("getblockcount")
        if height < tip + 101:
            self.node("generate", tip + 101 - height)
        self.wait("Zingo mature coinbase scan", lambda: self.zingo.snapshot()["height"] >= tip + 101)
        txid = j.once("shield", {"coinbase": coinbase["txid"]}, self.zingo.shield)
        self.confirm("shield", txid)
        return txid

    def fund(self, payer):
        j = self.journal
        if "funding" in j.data["observations"]:
            return
        txid = self.fund_regtest() if self.regtest else self.fund_live(payer)
        block = self.mined(txid)
        def received():
            snap = self.zingo.snapshot()
            tx = wallet_tx(snap, txid)
            if not tx or tx["height"] != block["height"] or snap["spendable_zat"] < OUTBOUND + 100_000:
                return None
            if not self.regtest:
                require(tx["amount_zat"] == DRIP and j.data["claim"]["memo"] in tx["memos"],
                        "funding amount or decrypted memo mismatch")
            return {"txid": txid, "block": block, "total_zat": snap["total_zat"],
                    "spendable_zat": snap["spendable_zat"]}
        j.observe("funding", self.wait("Zingo funding and spendability", received))

    def transfer(self, name, sender, receiver, address, amount):
        j = self.journal
        if name in j.data["observations"]:
            return
        before_key = name + "-before"
        if before_key not in j.data["observations"]:
            a, b = sender.snapshot(), receiver.snapshot()
            require(a["spendable_zat"] >= amount, "insufficient spendable funds for " + name)
            if name == "outbound":
                require(b["total_zat"] == 0, "expected a fresh recipient with no previous balance")
            j.observe(before_key, {"payer": a["total_zat"], "payee": b["total_zat"]})
        before = j.data["observations"][before_key]
        memo = "zinder-roundtrip-v1/" + name
        def reconcile():
            a, b = sender.snapshot(), receiver.snapshot()
            candidates = [r["txid"] for r in b["transactions"] if r["amount_zat"] == amount and memo in r["memos"]
                          and wallet_tx(a, r["txid"])]
            require(len(candidates) <= 1, "ambiguous transaction reconciliation")
            return candidates[0] if candidates else None
        txid = j.once(name, {"address": address, "amount_zat": amount, "memo": memo},
                      lambda: sender.send(address, amount, memo), reconcile=reconcile)
        pending_key = name + "-pending"
        if pending_key not in j.data["observations"]:
            seen = {"payer": False, "payee": False, "node_mempool": False}
            end = time.monotonic() + (45 if self.regtest else 20)
            while True:
                seen["node_mempool"] |= txid in self.node("getrawmempool")
                current = [wallet_tx(w.snapshot(), txid) for w in (sender, receiver)]
                for party, tx in zip(("payer", "payee"), current):
                    seen[party] |= bool(tx and tx["pending"])
                if (seen["payer"] and seen["payee"]) or any(tx and tx["height"] for tx in current) or time.monotonic() >= end:
                    break
                time.sleep(2)
            if self.regtest:
                require(all(seen.values()), f"{name}: missing Regtest mempool, payer or payee pending observation")
            j.observe(pending_key, seen)
        block = self.confirm(name, txid)
        def checked():
            a, b = sender.snapshot(), receiver.snapshot()
            sent, received = wallet_tx(a, txid), wallet_tx(b, txid)
            if not all(t and not t["pending"] and t["height"] == block["height"] for t in (sent, received)):
                return None
            fee = sent["fee_zat"]
            require(type(fee) is int and fee > 0, "sender lacks usable fee evidence")
            require(received["amount_zat"] == amount and memo in received["memos"],
                    f"{name}: recipient amount or decrypted memo mismatch")
            require(a["total_zat"] == before["payer"] - amount - fee and b["total_zat"] == before["payee"] + amount,
                    f"{name}: payer debit or payee credit does not reconcile")
            if b["spendable_zat"] < amount and name == "outbound":
                return None
            return {"txid": txid, "block": block, "amount_zat": amount, "fee_zat": fee, "memo_verified": True,
                    "payer_after_zat": a["total_zat"], "payee_after_zat": b["total_zat"],
                    "spendable_after_zat": b["spendable_zat"], "change_zat": sent.get("change_zat")}
        j.observe(name, self.wait("wallet accounting for " + name, checked))

    def run(self):
        j = self.journal
        payer, payee = self.zingo.prepare(), self.zallet.prepare()
        require(payer != payee, "payer and payee are the same wallet")
        identities = [fingerprint(payer), fingerprint(payee)]
        if "wallets" in j.data:
            require(j.data["wallets"] == identities, "wallet identities changed")
        else:
            j.data["wallets"] = identities
            j.save()
        if "wallets-ready" not in j.data["observations"]:
            a, b = self.zingo.snapshot(), self.zallet.snapshot()
            require(all(type(wallet["height"]) is int and wallet["height"] >= j.data["birthday"] for wallet in (a, b)),
                    "wallet has not indexed its starting height")
            j.observe("wallets-ready", {"payer_height": a["height"], "payee_height": b["height"]})
        self.fund(payer)
        self.transfer("outbound", self.zingo, self.zallet, payee, OUTBOUND)
        self.transfer("return", self.zallet, self.zingo, payer, RETURN)
        self.zingo.restart()
        self.zallet.restart()
        a, b = self.zingo.snapshot(), self.zallet.snapshot()
        obs = j.data["observations"]
        require(a["total_zat"] == obs["return"]["payee_after_zat"] and b["total_zat"] == obs["return"]["payer_after_zat"],
                "wallet balances changed on reopen")
        for leg in ("outbound", "return"):
            txid, height = obs[leg]["txid"], obs[leg]["block"]["height"]
            for wallet in (a, b):
                tx = wallet_tx(wallet, txid)
                require(tx and tx["height"] == height and not tx["pending"], "reopened wallet lost a confirmed transfer")
            require(self.node("getblockhash", height) == obs[leg]["block"]["hash"], "confirmed block changed during run")
        funding = obs["funding"]
        return {"status": "passed", "schema_version": 1, "network": self.network, "inputs": j.data["config"],
                "independent_wallets": True, "wallet_identity_sha256": identities, "reopen_ledger_verified": True,
                "funding": {"source": "regtest-coinbase-and-shield" if self.regtest else "fauzec-testnet",
                            "tx_fingerprint": hashlib.sha256(bytes.fromhex(funding["txid"])).hexdigest(),
                            "mined_height": funding["block"]["height"], "spendable_after_zat": funding["spendable_zat"],
                            "faucet_amount_and_memo_verified": not self.regtest},
                "pending_certified": all(all(obs[leg + "-pending"].values()) for leg in ("outbound", "return")),
                "legs":{leg:{**{k:v for k,v in obs[leg].items() if k not in ("txid","block")},
                             "tx_fingerprint":hashlib.sha256(bytes.fromhex(obs[leg]["txid"])).hexdigest(),
                             "mined_height":obs[leg]["block"]["height"], "pending":obs[leg+"-pending"]}
                        for leg in ("outbound", "return")}}


def main():
    if sys.argv[1:2] == ["compare"]:
        compare = argparse.ArgumentParser(description="Compare wallet outcomes across two independent Regtest fixtures.")
        compare.add_argument("receipts", type=Path, nargs=2)
        inputs = compare.parse_args(sys.argv[2:]).receipts
        print(json.dumps(compare_runs(*(json.loads(p.read_text()) for p in inputs)), indent=2))
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("network", choices=["regtest", "testnet"])
    parser.add_argument("--ready", required=True, type=Path, help="immutable controlled fixture receipt")
    parser.add_argument("--revision", required=True, help="expected Zinder release commit")
    parser.add_argument("--zingo", required=True, type=Path, help="pinned Zingo CLI")
    parser.add_argument("--zallet-backend", required=True, type=Path, help="ordinary native Zallet executable")
    parser.add_argument("--state", required=True, type=Path, help="unique private directory; reuse it to resume")
    parser.add_argument("--wallet-rpc-port", required=True, type=int, help="unused loopback port for this Zallet")
    parser.add_argument("--wait-seconds", type=int, default=2400, help="maximum wait per public-chain observation")
    args = parser.parse_args()
    os.umask(0o077)
    require(re.fullmatch("[0-9a-f]{40}", args.revision) and args.wait_seconds > 0, "invalid revision or timeout")
    from roundtrip_fixture import verify, rpc
    from roundtrip_wallets import Zingo, Zallet
    ready = json.loads(args.ready.read_text())
    config = {"network":args.network,"revision":args.revision,"ready_sha256":digest(args.ready),
              "zingo_sha256":digest(args.zingo),"zallet_sha256":digest(args.zallet_backend),
              "outbound_zat":OUTBOUND,"return_zat":RETURN}
    state = args.state.resolve()
    with locked(state):
        j = Journal(state / "run.json", config)
        def native_wallet():
            return Zallet(args.zallet_backend.resolve(), state / "zallet", args.network, ready["native_wallet_query"],
                          j.data["birthday"], args.wallet_rpc_port, warmup_timeout=args.wait_seconds)

        receipt = state / "receipt.json"
        if receipt.exists():
            require(json.loads(receipt.read_text())["inputs"] == config, "completed receipt identity mismatch")
            # Older runners could write a receipt before their daemon exited.
            if (state / "zallet" / "daemon.pid").is_file():
                native_wallet().close()
            print(receipt)
            return
        sources = [Path(__file__), CLIENTS/"roundtrip_wallets.py", CLIENTS/"roundtrip_fixture.py", CLIENTS/"wallet_fixture.py"]
        source_hashes = {p.name: digest(p) for p in sources}
        j.data.setdefault("invocation_harnesses", []).append(source_hashes)
        j.save()
        preflight = verify(ready, args.network, args.revision)
        write_json(state / "preflight.json", preflight)
        if "birthday_anchor" not in j.data:
            h = rpc(ready["node_rpc"], "getblockcount")
            j.data["birthday_anchor"] = h
            j.data["birthday"] = max(1, h - 10)
            j.save()
        zingo = Zingo(args.zingo.resolve(), state / "zingo", args.network, ready["lightwalletd"], j.data["birthday"])
        zallet = native_wallet()
        try:
            result = RoundTrip(args.network, ready, rpc, j, zingo, zallet, args.wait_seconds).run()
            require(source_hashes == {p.name:digest(p) for p in sources}, "harness changed during run; rerun to verify")
            result["harness_sha256"] = source_hashes
            result["invocation_harnesses"] = j.data["invocation_harnesses"]
            result["fixture_recipe"] = {"network":args.network,"genesis":preflight["genesis"],
                                        "activation_schedule_sha256": ready.get("activation_schedule_sha256"),
                                        "raw_blob_policy": ready.get("raw_blob_policy"), "node_image":ready.get("node_image")}
            if args.network == "regtest":
                result["fixture_recipe"]["starting_height"] = j.data["birthday_anchor"]
                result["fixture_recipe"]["release_binaries"] = {
                    name: binary["sha256"] for name, binary in ready["binaries"].items()
                }
        finally:
            original_error = sys.exception()
            try:
                zallet.close()
            except Exception as cleanup_error:
                if original_error is None:
                    raise
                original_error.add_note(f"Zallet cleanup also failed: {cleanup_error}")
            finally:
                zingo.close()
        write_json(receipt, result)
        print(receipt)


if __name__ == "__main__":
    main()
