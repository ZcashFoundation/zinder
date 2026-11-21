#!/usr/bin/env python3
"""Check retry, identity, and comparability rules without contacting a faucet or wallet."""
import copy
from pathlib import Path
import runpy
import tempfile
import unittest
from unittest.mock import patch

M = runpy.run_path(str(Path(__file__).resolve().parents[1] / "run-wallet-roundtrip.py"))
Journal = M["Journal"]
RoundTrip = M["RoundTrip"]
TXID = "ab" * 32


class RoundTripTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "journal.json"
        self.j = Journal(self.path, {"network": "regtest"})

    def test_repeat_and_changed_intent(self):
        self.assertEqual(self.j.once("payment", {"amount": 1}, lambda: TXID), TXID)
        resumed = Journal(self.path, {"network": "regtest"})
        self.assertEqual(resumed.once("payment", {"amount": 1}, self.fail), TXID)
        with self.assertRaisesRegex(ValueError, "intent differs"):
            resumed.once("payment", {"amount": 2}, self.fail)
        with self.assertRaisesRegex(ValueError, "different clients"):
            Journal(self.path, {"network": "testnet"})

    def test_ambiguous_send_recovers_only_from_observed_transaction(self):
        def lost_reply():
            raise TimeoutError("response lost after submission")
        with self.assertRaises(TimeoutError):
            self.j.once("payment", {}, lost_reply)
        resumed = Journal(self.path, {"network": "regtest"})
        with self.assertRaisesRegex(ValueError, "outcome unknown"):
            resumed.once("payment", {}, self.fail, reconcile=lambda: None)
        self.assertEqual(resumed.once("payment", {}, self.fail, reconcile=lambda: TXID), TXID)
        self.assertTrue(resumed.data["operations"]["payment"]["recovered_from_wallets"])

    def test_caller_crash_after_faucet_commit_resumes_same_id_without_post(self):
        case = RoundTrip("testnet", {}, None, self.j, None, None, 1)
        address = "utest1example"
        posts = []
        def faucet(path, data=None):
            if path == "/network":
                return {"network": "testnet", "supported_address_kinds": ["unified"], "drip_zat": M["DRIP"]}
            if path == "/faucet-status":
                return {"network": "testnet", "cause": "ready"}
            if path == "/claim":
                posts.append(data)
                raise TimeoutError("server accepted; reply was lost")
            self.assertEqual(data, None)
            self.assertEqual(path, "/status/testnet/" + posts[0]["request_id"])
            return {**posts[0], "state": "confirmed", "txid": TXID}
        with patch.dict(case.fund_live.__globals__, faucet_api=faucet):
            with self.assertRaises(TimeoutError):
                case.fund_live(address)
            self.assertEqual(case.fund_live(address), TXID)
        self.assertEqual(len(posts), 1)

    def test_regtest_mining_target_is_fixed_before_rpc(self):
        height = [150]
        calls = []
        def rpc(_url, method, *params):
            if method == "getblockcount":
                return height[0]
            self.assertEqual(method, "generate")
            calls.append(params[0])
            height[0] += params[0]
            raise TimeoutError("mined; reply was lost")
        case = RoundTrip("regtest", {"node_rpc":"local"}, rpc, self.j, None, None, 1)
        with self.assertRaises(TimeoutError):
            case.mine_to("payment", 10)
        case.mine_to("payment", 10)
        self.assertEqual(calls, [10])
        case.regtest = False
        with self.assertRaisesRegex(ValueError, "requires Regtest"):
            case.mine_to("other", 10)

    def test_corrupt_wallet_history_and_comparison(self):
        with self.assertRaisesRegex(ValueError, "duplicate"):
            M["wallet_tx"]({"transactions":[{"txid":TXID}] * 2}, TXID)
        leg = dict(tx_fingerprint="1"*64, amount_zat=5, fee_zat=1, memo_verified=True, payer_after_zat=4,
                   payee_after_zat=5, spendable_after_zat=5, change_zat=None,
                   pending={"node_mempool":True,"payer":True,"payee":True})
        left = {"status":"passed","network":"regtest","independent_wallets":True,"reopen_ledger_verified":True,
                "inputs":dict(zingo_sha256="x",zallet_sha256="y",revision="z",outbound_zat=5,return_zat=2,ready_sha256="r1"),
                "harness_sha256":{"runner":"s"},"invocation_harnesses":[{"runner":"s"}],"pending_certified":True,
                "wallet_identity_sha256":["x","y"],"fixture_recipe":{},"legs":{"outbound":leg,"return":dict(leg)}}
        other = copy.deepcopy(left)
        with self.assertRaisesRegex(ValueError, "same fixture"):
            M["compare_runs"](left, other)
        other["inputs"]["ready_sha256"] = "r2"
        other["wallet_identity_sha256"] = ["c","d"]
        for x in other["legs"].values():
            x["tx_fingerprint"] = "2" * 64
        self.assertTrue(M["compare_runs"](left, other)["same_wallet_outcomes"])
        other["legs"]["return"]["fee_zat"] = 2
        with self.assertRaisesRegex(ValueError, "fee_zat"):
            M["compare_runs"](left, other)


if __name__ == "__main__":
    unittest.main()
