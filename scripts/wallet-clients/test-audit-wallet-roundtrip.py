#!/usr/bin/env python3
"""Canonical Regtest audit boundary tests."""
import copy
import hashlib
import json
import tempfile
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location("wallet_roundtrip_audit", Path(__file__).with_name("audit-wallet-roundtrip.py"))
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


class CanonicalAuditTests(unittest.TestCase):
    def setUp(self):
        self.txid, self.block_hash = "a" * 64, "b" * 64
        self.pending = dict.fromkeys(("node_mempool", "payee", "payer"), True)
        self.before = {"payer": 80, "payee": 5}
        accounting = dict(amount_zat=10, fee_zat=2, payer_after_zat=68, payee_after_zat=15)
        self.observation = dict(accounting, txid=self.txid, block={"height": 163, "hash": self.block_hash})
        self.fingerprint = hashlib.sha256(bytes.fromhex(self.txid)).hexdigest()
        self.leg = dict(accounting, tx_fingerprint=self.fingerprint, mined_height=163, pending=self.pending)
        self.tx = {"txid": self.txid, "blockhash": self.block_hash, "height": 163, "confirmations": 3}
        self.block = {"hash": self.block_hash, "height": 163, "tx": [self.txid]}
        self.active_block = self.block_hash

    def rpc(self, _, method, *args):
        replies = {("getrawtransaction", self.txid, 1): self.tx, ("getblockhash", 163): self.active_block,
                   ("getblock", self.block_hash, 1): self.block}
        return replies[method, *args]

    def verify(self):
        with patch.object(audit, "rpc", self.rpc):
            return audit.verify_leg("http://127.0.0.1:49032", self.leg, self.observation, self.before, self.pending)

    def test_requires_same_confirmed_transaction_and_raw_block(self):
        self.assertEqual(self.verify(), self.fingerprint)
        for txids in ([], [self.txid, self.txid]):
            self.block["tx"] = txids
            with self.assertRaisesRegex(ValueError, "membership"):
                self.verify()

    def test_wrong_chain_or_height_fails_even_if_transaction_exists(self):
        self.active_block = "c" * 64
        with self.assertRaisesRegex(ValueError, "active chain"):
            self.verify()
        self.active_block = self.block_hash
        self.tx["height"] = 164
        with self.assertRaisesRegex(ValueError, "active chain"):
            self.verify()

    def test_observations_must_match_summary_and_reconcile_balances(self):
        self.leg["fee_zat"] = 3
        with self.assertRaisesRegex(ValueError, "observations disagree"):
            self.verify()
        self.observation["fee_zat"] = 3
        with self.assertRaisesRegex(ValueError, "balance changes"):
            self.verify()

    def test_ready_build_or_journal_harness_mismatch_stops_before_node_access(self):
        from coverage_test import payment_receipt
        for changed in ("binaries", "journal"):
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                receipt = payment_receipt("a")
                recipe = receipt["fixture_recipe"]
                recipe["genesis"] = audit.GENESIS["regtest"]
                ready = {k: recipe[k] for k in ("activation_schedule_sha256", "raw_blob_policy", "node_image")}
                ready.update(network="zcash-regtest", zinder_commit=receipt["inputs"]["revision"],
                             binaries={k: {"sha256": v} for k, v in recipe["release_binaries"].items()})
                if changed == "binaries":
                    ready["binaries"]["query"]["sha256"] = "c" * 64
                ready_path = root / "ready.json"
                ready_path.write_text(json.dumps(ready))
                receipt["inputs"]["ready_sha256"] = hashlib.sha256(ready_path.read_bytes()).hexdigest()
                journal = dict(config=receipt["inputs"], wallets=receipt["wallet_identity_sha256"],
                               birthday_anchor=recipe["starting_height"],
                               invocation_harnesses=receipt["invocation_harnesses"] if changed != "journal" else [])
                (root / "receipt.json").write_text(json.dumps(receipt))
                (root / "run.json").write_text(json.dumps(journal))
                with patch.object(audit, "rpc") as rpc, self.assertRaisesRegex(ValueError, "build|harness"):
                    audit.verify_run(root, ready_path, receipt["inputs"]["revision"])
                rpc.assert_not_called()

    def test_independent_canonical_heights_can_differ(self):
        leg = dict(self.leg, memo_verified=True, spendable_after_zat=15, change_zat=None)
        legs = {"outbound": leg, "return": dict(leg, mined_height=173)}
        def run(index):
            values = copy.deepcopy(legs)
            for leg in values.values():
                leg["mined_height"] += index
            sha = str(index) * 64
            ids = {name: sha[:63] + str(i) for i, name in enumerate(values)}
            for name, fingerprint in ids.items():
                values[name]["tx_fingerprint"] = fingerprint
            receipt = {"legs": values, "inputs": {"ready_sha256": sha}, "harness_sha256": {}}
            return receipt, (sha, sha, sha), {"canonical_txid_fingerprints": ids,
                    "receipt_sha256": sha, "ready_sha256": sha, "end_height": 180}
        first, second = run(1), run(2)
        with patch.object(audit, "verify_run", side_effect=[first, second]), patch.object(audit, "compare_runs"):
            result = audit.audit([("a", "b"), ("c", "d")], "a" * 40)
        from coverage_roundtrip import audit_pair
        receipts = {r[2]["receipt_sha256"]: r[0] for r in (first, second)}
        audit_pair(result, receipts)
        previous_schema = copy.deepcopy(result)
        shared_leg = previous_schema["matching_outcomes"]["leg_outcomes"]["outbound"]
        shared_leg["mined_height"] = first[0]["legs"]["outbound"]["mined_height"]
        with self.assertRaisesRegex(ValueError, "accounting"):
            audit_pair(previous_schema, receipts)
        result["matching_outcomes"]["leg_outcomes"]["outbound"]["fee_zat"] += 1
        with self.assertRaisesRegex(ValueError, "accounting"):
            audit_pair(result, receipts)

    def test_same_endpoint_or_owner_cannot_count_as_replication(self):
        f = ("a", "b", "c")
        tx = {"canonical_txid_fingerprints": {"outbound": "a", "return": "b"}}
        for duplicate in [("a", "d", "e"), ("d", "b", "e"), ("d", "e", "c")]:
            with patch.object(audit, "verify_run", side_effect=[({}, f, tx), ({}, duplicate, tx)]), \
                 patch.object(audit, "compare_runs"):
                with self.assertRaisesRegex(ValueError, "independently owned"):
                    audit.audit([("a", "b"), ("c", "d")], "a" * 40)


if __name__ == "__main__":
    unittest.main()
