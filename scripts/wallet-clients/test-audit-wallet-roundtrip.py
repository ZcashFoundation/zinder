#!/usr/bin/env python3
"""Canonical Regtest audit boundary tests."""
import hashlib
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

    def test_same_endpoint_or_owner_cannot_count_as_replication(self):
        f = ("a", "b", "c")
        tx = {"canonical_txid_fingerprints": {"outbound": "a", "return": "b"}}
        for duplicate in [("a", "d", "e"), ("d", "b", "e"), ("d", "e", "c")]:
            with patch.object(audit, "verify_run", side_effect=[({}, f, tx), ({}, duplicate, tx)]), \
                 patch.object(audit.runpy, "run_path", return_value={"compare_runs": lambda *_: None}):
                with self.assertRaisesRegex(ValueError, "independently owned"):
                    audit.audit([("a", "b"), ("c", "d")], "a" * 40)


if __name__ == "__main__":
    unittest.main()
