#!/usr/bin/env python3
"""Verify two completed Regtest round trips against both live control nodes before teardown."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import runpy
import sys
from tempfile import NamedTemporaryFile

from coverage_roundtrip import check_receipt
from roundtrip_fixture import GENESIS, anchor, require, rpc
from wallet_fixture import loopback


def pinned_json(path):
    raw = path.read_bytes()
    return json.loads(raw), hashlib.sha256(raw).hexdigest()


def verify_leg(node, leg, observation, before, pending):
    txid = observation["txid"]
    require(isinstance(txid, str) and re.fullmatch(r"[0-9a-f]{64}", txid), "invalid transfer ID")
    fingerprint = hashlib.sha256(bytes.fromhex(txid)).hexdigest()
    height, block_hash = observation["block"]["height"], observation["block"]["hash"]
    observed = {k: v for k, v in observation.items() if k not in {"block", "txid"}}
    require(leg == dict(observed, tx_fingerprint=fingerprint, mined_height=height, pending=pending),
            "receipt and wallet observations disagree")
    require(leg["payer_after_zat"] == before["payer"] - leg["amount_zat"] - leg["fee_zat"] and
            leg["payee_after_zat"] == before["payee"] + leg["amount_zat"], "wallet balance changes differ")
    tx = rpc(node, "getrawtransaction", txid, 1)
    require(tx["txid"] == txid and tx["blockhash"] == block_hash and tx["height"] == height
            and tx["confirmations"] > 0 and rpc(node, "getblockhash", height) == block_hash,
            "confirmed transaction differs from the active chain")
    block = rpc(node, "getblock", block_hash, 1)
    require(block["hash"] == block_hash and block["height"] == height and block["tx"].count(txid) == 1,
            "transaction membership in the canonical block differs")
    return fingerprint


def verify_run(state, ready_path, revision):
    receipt, receipt_sha = pinned_json(state / "receipt.json")
    ready, ready_sha = pinned_json(ready_path)
    check_receipt(receipt, revision)
    require(receipt["inputs"]["ready_sha256"] == ready_sha and ready["network"] == "zcash-regtest"
            and ready["zinder_commit"] == revision, "fixture and signed receipt disagree")
    journal = json.loads((state / "run.json").read_bytes())
    require(journal["config"] == receipt["inputs"] and journal["wallets"] == receipt["wallet_identity_sha256"],
            "private journal belongs to another run")
    node = loopback(ready["node_rpc"])
    height, initial_hash = anchor(ready)
    require(rpc(node, "getblockhash", 0) == GENESIS["regtest"] and rpc(node, "getblockhash", height) == initial_hash,
            "control node does not serve the Regtest fixture anchor")
    require(rpc(node, "getpeerinfo") == [] and ready["isolation"]["default_ipv4_route"] is False,
            "expected peer-isolated Regtest fixture")
    tip = rpc(node, "getblockcount")
    tip_hash = rpc(node, "getblockhash", tip)
    require(tip >= receipt["legs"]["return"]["mined_height"], "node behind recorded return payment")
    obs = journal["observations"]
    txs = {name: verify_leg(node, leg, obs[name], obs[name + "-before"], obs[name + "-pending"])
           for name, leg in receipt["legs"].items()}
    require(rpc(node, "getblockhash", tip) == tip_hash, "chain reorganized during canonical audit")
    return receipt, (ready["owner"], initial_hash, node), {
        "receipt_sha256": receipt_sha, "ready_sha256": ready_sha, "canonical_txid_fingerprints": txs, "end_height": tip}


def audit(runs, revision):
    require(re.fullmatch(r"[0-9a-f]{40}", revision) and len(runs) == 2, "two runs and full release commit required")
    first, second = [verify_run(state, ready, revision) for state, ready in runs]
    runner = runpy.run_path(str(Path(__file__).resolve().parents[1] / "run-wallet-roundtrip.py"))
    runner["compare_runs"](first[0], second[0])
    require(all(a != b for a, b in zip(first[1], second[1])), "expected two independently owned chains and nodes")
    require(len({f for result in (first, second) for f in result[2]["canonical_txid_fingerprints"].values()}) == 4,
            "expected four independent canonical transfers")
    receipt = first[0]
    outcomes = {k: {f: v for f, v in leg.items() if f != "tx_fingerprint"} for k, leg in receipt["legs"].items()}
    require(outcomes == {k: {f: v for f, v in leg.items() if f != "tx_fingerprint"}
                         for k, leg in second[0]["legs"].items()}, "independent wallet outcomes differ")
    return {"schema_version": 1, "passed": True, "independent_chains": 2, "distinct_wallet_identities": 4,
            "independent_transactions": 4, "runs": {"first": first[2], "second": second[2]},
            "matching_outcomes": {"network": "regtest", "schema_version": 1, "leg_outcomes": outcomes,
                                  "binaries": {k: v for k, v in receipt["inputs"].items() if k != "ready_sha256"},
                                  "harness_sha256": receipt["harness_sha256"]}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, nargs=2, action="append", required=True, metavar=("STATE", "READY"),
                        help="supply once for each private state directory and immutable readiness receipt")
    parser.add_argument("--revision", required=True, help="expected full Zinder release commit")
    parser.add_argument("--output", type=Path, required=True, help="new privacy-safe audit file")
    args = parser.parse_args()
    result = audit(args.run, args.revision)
    with NamedTemporaryFile(mode="w", dir=args.output.parent) as tmp:
        json.dump(result, tmp, indent=2)
        tmp.write("\n")
        tmp.flush()
        os.fsync(tmp.fileno())
        os.link(tmp.name, args.output)
    print(f"Verified four canonical transfers on two independent Regtest chains. Audit written to {args.output}")


if __name__ == "__main__":
    try:
        main()
    except (KeyError, TypeError, ValueError, RuntimeError, OSError) as error:
        sys.exit(f"round-trip audit failed: {error}")
