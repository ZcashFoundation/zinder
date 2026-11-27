#!/usr/bin/env python3
"""Regression rules for the wallet coverage matrix."""
import copy
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import coverage as cov


class ZkoolVersionTests(unittest.TestCase):
    def test_only_the_pinned_builds_inherit_the_baseline_version(self):
        from coverage_evidence import zkool_version

        reference = "6a9f90adb7f53caa739a0274f7653096678d523244449b8f42bde6ca1cfc99eb"
        self.assertEqual(zkool_version(reference), "6.37.0")
        self.assertEqual(zkool_version("0" * 64, "6.38.1"), "6.38.1")
        with self.assertRaises(ValueError):
            zkool_version("0" * 64)
        with self.assertRaises(ValueError):
            zkool_version(reference, "6.38.1")


def sample():
    return {"schema_version": 1, "release": {"tag": "v1.0.0", "commit": "a" * 40},
            "inventory": {"lightwalletd": ["GetSubtreeRoots", "GetBlock"], "native-wallet-query": ["SubtreeRoots"]},
            "scenarios": [{"id": "wallet-restore", "wallet": "test-client", "version": "1.0", "variant": "cli",
                           "implementation": "engine", "interface": "lightwalletd", "network": "regtest",
                           "fixture": "same independent chain schedule", "client_sha256": "c" * 64,
                           "action": "Restore prior activity", "status": "pass", "method_scope": "partial",
                           "assertions": {"history": "pass", "hash-order": "pass"},
                           "methods": {"GetSubtreeRoots": {"calls": 3, "outcomes": {"ok": 3},
                                       "fields": {"SubtreeRoot.completingBlockHash":
                                                  {"handling": "ignored", "assertion": "hash-order"}}}},
                           "observed_methods": ["GetSubtreeRoots", "GetBlock"],
                           "evidence": {"description": "Synthetic contract example", "sha256": "b" * 64}}]}


def payment_receipt(ch):
    release = sample()["release"]
    def token(s):
        import hashlib
        return hashlib.sha256((ch + s).encode()).hexdigest()
    def leg(amount, fee, payer, payee, height, change):
        return {"amount_zat": amount, "fee_zat": fee, "payer_after_zat": payer, "payee_after_zat": payee,
                "spendable_after_zat": payee, "tx_fingerprint": token(str(height)), "change_zat": change,
                "mined_height": height, "pending": dict.fromkeys(("node_mempool", "payee", "payer"), True),
                "memo_verified": True}
    harness = dict.fromkeys(("run-wallet-roundtrip.py", "roundtrip_fixture.py", "roundtrip_wallets.py", "wallet_fixture.py"), "a" * 64)
    return {"schema_version": 1, "status": "passed", "network": "regtest", "independent_wallets": True,
            "pending_certified": True, "reopen_ledger_verified": True, "harness_sha256": harness,
            "invocation_harnesses": [harness], "wallet_identity_sha256": [token("zingo"), token("zallet")],
            "inputs": {"revision": release["commit"], "network": "regtest", "zingo_sha256": "a" * 64,
                       "zallet_sha256": "b" * 64, "ready_sha256": token("fixture"), "outbound_zat": 100,
                       "return_zat": 40},
            "fixture_recipe": {"network": "regtest", "genesis": "a" * 64, "raw_blob_policy": "all",
                               "activation_schedule_sha256": "a" * 64, "starting_height": 150,
                               "node_image": "sha256:" + "a" * 64,
                               "release_binaries": dict.fromkeys(("compat-lightwalletd", "ingest",
                                                                  "projector", "query"), "a" * 64)},
            "funding": {"source": "regtest-coinbase-and-shield", "mined_height": 160,
                        "spendable_after_zat": 500, "tx_fingerprint": token("funding")},
            "legs": {"outbound": leg(100, 1, 399, 100, 161, None),
                     "return": leg(40, 2, 58, 439, 163, 58)}}


class CoverageTests(unittest.TestCase):
    def test_call_volume_alone_is_not_a_regression(self):
        old = sample()
        fresh = copy.deepcopy(old)
        fresh["scenarios"][0]["methods"]["GetSubtreeRoots"].update(calls=8, outcomes={"ok": 8})
        self.assertEqual(cov.compare(cov.validate(old), cov.validate(fresh)), [])

    def test_distinguishes_lost_method_success_field_and_assertion(self):
        base = sample()
        for change, expected in (
                (lambda r: r["methods"].clear(), "lost observed method GetSubtreeRoots"),
                (lambda r: r["methods"]["GetSubtreeRoots"].update(outcomes={"cancelled": 3}),
                 "lost successful GetSubtreeRoots"),
                (lambda r: r["methods"]["GetSubtreeRoots"]["fields"].clear(), "changed GetSubtreeRoots"),
                (lambda r: r["assertions"].update(history="unexercised"), "lost passing assertion history"),
                (lambda r: r["observed_methods"].remove("GetBlock"), "lost observation of GetBlock")):
            with self.subTest(expected=expected):
                fresh = copy.deepcopy(base)
                change(fresh["scenarios"][0])
                self.assertIn(expected, "\n".join(cov.compare(base, cov.validate(fresh))))

    def test_cross_interface_methods_are_invalid(self):
        data = sample()
        data["scenarios"][0]["methods"]["SubtreeRoots"] = {"calls": 1}
        with self.assertRaises(ValueError):
            cov.validate(data)

    def test_socket_only_scope_cannot_report_a_method(self):
        data = sample()
        data["scenarios"][0]["method_scope"] = "none"
        with self.assertRaises(ValueError):
            cov.validate(data)

    def test_claim_requires_named_assertion(self):
        data = sample()
        del data["scenarios"][0]["assertions"]["hash-order"]
        with self.assertRaises(ValueError):
            cov.validate(data)

    def test_scope_does_not_infer_omitted_methods_or_field_handling(self):
        data = sample()
        row = data["scenarios"][0]
        del row["observed_methods"]
        self.assertNotIn("wallet-restore` uncalled", cov.render(cov.validate(data)))
        row["observed_methods"] = ["GetSubtreeRoots", "GetBlock"]
        self.assertIn("wallet-restore` uncalled", cov.render(cov.validate(data)))
        del row["methods"]["GetSubtreeRoots"]["fields"]
        self.assertNotIn("completingBlockHash", cov.render(cov.validate(data)))

    def test_different_wallet_binary_is_incomparable(self):
        old = sample()
        new = copy.deepcopy(old)
        new["scenarios"][0]["client_sha256"] = "d" * 64
        self.assertIn("incomparable client_sha256", "\n".join(cov.compare(old, new)))

    def test_duplicate_and_unexpected_fields_refused(self):
        for mutate in (lambda d: d["scenarios"].append(d["scenarios"][0]),
                       lambda d: d["scenarios"][0].update(raw_trace="example"),
                       lambda d: d["scenarios"][0]["evidence"].update(address="private")):
            data = sample()
            mutate(data)
            with self.assertRaises(ValueError):
                cov.validate(data)

    def test_missing_scope_refused(self):
        with self.assertRaisesRegex(ValueError, "no scenario matching"):
            cov.compare(sample(), sample(), ["unknown"])

    def test_reorg_accepts_first_sync_and_catchup_without_changing_action_identity(self):
        release = sample()["release"]
        action = {"client_connected_only_to_target": True, "methods": [
            {"method": "GetBlockRange", "count": 1, "grpc_code": "ok"}]}
        phases = ["bootstrap", "scan-surviving-receipt", "scan-orphan-candidate",
                  "follow-replacement", "restart-and-advance"]
        result = {"schema_version": 1, "case": "zkool-ironwood-coinbase-reorg",
                  "status": "passed", "fixture_teardown_completed": True, "interface": "lightwalletd", "client": {
                      "name": "zkool", "version": "6.37.0", "version_source": "operator",
                      "variant": "internal-enabled", "binary_sha256": "c" * 64},
                  "fixture": {"network": "zcash-regtest", "release_revision": release["commit"],
                              "release_version": release["tag"][1:], "activation_schedule_sha256": "d" * 64},
                  "actions": dict.fromkeys(phases, action), "fork": {"new_tip": 266},
                  "first_reorg_sync_reached_tip": True, "assertions": dict.fromkeys(["distinct_branch_hash_at_mined_height",
                      "orphan_removed_from_confirmed_history", "owned_note_and_balance_reverted",
                      "surviving_receipt_preserved", "rollback_persisted_after_new_process_and_next_block"], True),
                  "wallet_states": {"surviving-receipt": {"received_notes": 1, "ironwood_balance": 10},
                                    "before-reorg": {"received_notes": 2, "ironwood_balance": 20},
                                    "after-first-reorg-sync": {"scanned_height": 266}}}
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "receipt.json"
            path.write_text(json.dumps(result))
            fast = list(cov.reorg_actions(path, release))
            result["first_reorg_sync_reached_tip"] = False
            result["actions"]["resume-replacement"] = action
            result["wallet_states"]["after-first-reorg-sync"]["scanned_height"] = 262
            result["wallet_states"]["after-reorg-catchup"] = {"scanned_height": 266}
            path.write_text(json.dumps(result))
            slow = list(cov.reorg_actions(path, release))
            self.assertEqual([r["id"] for r in fast], [r["id"] for r in slow])
            self.assertEqual([r["assertions"] for r in fast], [r["assertions"] for r in slow])
            self.assertEqual(slow[3]["methods"]["GetBlockRange"], {"calls": 2, "outcomes": {"ok": 2}})

            for label, mutate in (
                    ("absent cleanup", lambda r: r.pop("fixture_teardown_completed")),
                    ("failed cleanup", lambda r: r.update(fixture_teardown_completed=False)),
                    ("missing assertion", lambda r: r["assertions"].pop("distinct_branch_hash_at_mined_height")),
                    ("missing phase", lambda r: r["actions"].pop("scan-surviving-receipt")),
                    ("missing calls", lambda r: r["actions"]["resume-replacement"].update(methods=[]))):
                changed = copy.deepcopy(result)
                mutate(changed)
                path.write_text(json.dumps(changed))
                with self.subTest(label), self.assertRaises(ValueError):
                    list(cov.reorg_actions(path, release))

    def test_interrupted_execution_cannot_use_a_replayed_prefix_as_resume_proof(self):
        from coverage_evidence import digest, interrupted_actions
        fixture_path = Path(cov.__file__).with_name("subtree-testnet.json")
        fixture = cov.load(fixture_path)
        start, end = fixture["birthday"] + 1, fixture["tip"]
        release = sample()["release"]
        wallet = {"synced_height": end, "history_entries": 2, "received_notes": 1,
                  "ledger_sha256": "a" * 64, "balance": {"total": 0}}
        def trace(exit_code):
            return {"connects": 1, "trace_sha256": "b" * 64, "exit": exit_code}
        result = dict(schema_version=1, status="pass", revision=release["commit"], client_sha256="c" * 64, client_version="6.37.0",
                      fixture_sha256=digest(fixture_path), birthday=fixture["birthday"], tip=end,
                      tip_hash=fixture["tip_hash"],
                      control={"fault": None, "methods": {"GetBlockRange": 1}, "wallet": wallet,
                               "compact_block_ranges": [[start, end]], "online_traces": {"restore": trace(0)}},
                      interrupted={"fault": {"delivered": 64, "start": start, "end": end,
                                             "last_delivered": start + 63, "wallet_height_before_resume": start + 63},
                                   "methods": {"GetBlockRange": 2}, "wallet": wallet,
                                   "compact_block_ranges": [[start, end], [start + 64, end]],
                                   "online_traces": {"restore": trace(1), "resume": trace(0)}})
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "receipt.json"
            path.write_text(json.dumps(result))
            self.assertEqual(len(list(interrupted_actions(path, release))), 2)
            result["interrupted"]["compact_block_ranges"][1] = [start, end]
            path.write_text(json.dumps(result))
            with self.assertRaisesRegex(ValueError, "persisted last delivered block"):
                list(interrupted_actions(path, release))

    def test_matching_receipts_must_pin_all_wallet_harness_sources(self):
        left, right = payment_receipt("a"), payment_receipt("b")
        for receipt in (left, right):
            del receipt["harness_sha256"]["roundtrip_wallets.py"]
        with self.assertRaisesRegex(ValueError, "harness"):
            from coverage_roundtrip import compare_runs
            compare_runs(left, right)

    def test_standalone_compare_checks_wallet_observations(self):
        import runpy
        compare = runpy.run_path(str(Path(__file__).resolve().parents[1] / "run-wallet-roundtrip.py"))["compare_runs"]
        left, right = payment_receipt("a"), payment_receipt("b")
        self.assertTrue(compare(left, right)["same_wallet_outcomes"])
        with self.assertRaisesRegex(ValueError, "same fixture"):
            compare(left, left)
        for r in (left, right):
            r["legs"]["outbound"]["pending"]["payee"] = False
        with self.assertRaisesRegex(ValueError, "pre-mining"):
            compare(left, right)
        left, right = payment_receipt("a"), payment_receipt("b")
        right["legs"]["return"]["tx_fingerprint"] = left["legs"]["outbound"]["tx_fingerprint"]
        with self.assertRaisesRegex(ValueError, "reused transfer"):
            compare(left, right)
        left, right = payment_receipt("a"), payment_receipt("b")
        right["legs"]["return"]["fee_zat"] += 1
        with self.assertRaisesRegex(ValueError, "balance or change"):
            compare(left, right)

    def test_paired_signed_wallet_accounting(self):
        release = sample()["release"]
        with tempfile.TemporaryDirectory() as temp:
            paths = [Path(temp) / f"{s}.json" for s in ("first", "second")]
            a, b = payment_receipt("a"), payment_receipt("b")
            for r in (a, b):
                r["fixture_recipe"]["release_binaries"] = {"zinder": "a" * 64}
            for path, r in zip(paths, (a, b)):
                path.write_text(json.dumps(r))
            self.assertEqual(len(list(cov.roundtrip_actions(paths, release))), 4)
            a["fixture_recipe"]["release_binaries"] = {"zinder": "a" * 64, "query": "a" * 64}
            paths[0].write_text(json.dumps(a))
            with self.assertRaisesRegex(ValueError, "release binary"):
                list(cov.roundtrip_actions(paths, release))

            a, b = payment_receipt("a"), payment_receipt("b")
            def write_pair():
                for path, r in zip(paths, (a, b)):
                    path.write_text(json.dumps(r))
            write_pair()
            rows = list(cov.roundtrip_actions(paths, release))
            self.assertEqual(len(rows), 4)
            self.assertEqual({(r["wallet"], r["interface"]) for r in rows},
                             {("zingo", "lightwalletd"), ("zallet", "native-wallet-query")})
            self.assertTrue(all(r["method_scope"] == "none" and not r["methods"] for r in rows))
            by_id = {r["id"]: r for r in rows}
            returned = by_id["roundtrip-return-zallet-send"]
            self.assertEqual(returned["accounting_zat"]["change"], 58)
            matrix = sample()
            matrix["scenarios"] = rows
            variant = copy.deepcopy(matrix)
            variant["scenarios"][0]["accounting_zat"]["fee"] = 3
            self.assertIn("changed wallet accounting for fee", "\n".join(cov.compare(matrix, variant)))
            a0, b0 = copy.deepcopy(a), copy.deepcopy(b)
            for mutate in (
                lambda: a["legs"]["outbound"].update(memo_verified=False),
                lambda: a["legs"]["outbound"].update(fee_zat=2),
                lambda: b["legs"]["return"].update(change_zat=0),
                lambda: a["legs"]["outbound"]["pending"].update(payee=False),
                lambda: b.update(wallet_identity_sha256=a["wallet_identity_sha256"]),
                lambda: b["legs"]["return"].update(tx_fingerprint=a["legs"]["outbound"]["tx_fingerprint"]),
                lambda: a["inputs"].update(revision="c" * 40),
                lambda: b.update(reopen_ledger_verified=False),
            ):
                a, b = copy.deepcopy(a0), copy.deepcopy(b0)
                mutate()
                write_pair()
                with self.assertRaises(ValueError):
                    list(cov.roundtrip_actions(paths, release))
            a, b = a0, b0
            write_pair()
            audit = Path(temp) / "audit.json"
            audit.write_text(json.dumps({"schema_version": 1, "passed": True, "independent_chains": 2,
                                         "distinct_wallet_identities": 4, "independent_transactions": 4,
                                         "runs": {"a": {"receipt_sha256": "f" * 64},
                                                  "b": {"receipt_sha256": "e" * 64}}}))
            with self.assertRaisesRegex(ValueError, "does not bind both receipts"):
                list(cov.roundtrip_actions(paths, release, audit))

    def test_cli_exit_codes_and_output(self):
        with tempfile.TemporaryDirectory() as temp:
            baseline, current = (Path(temp) / name for name in ["old.json", "next.json"])
            old = sample()
            baseline.write_text(json.dumps(old))
            current.write_text(json.dumps(old))
            cmd = [sys.executable, "-B", str(Path(cov.__file__)), "compare", "--baseline", str(baseline), str(current)]
            for run, code in ((0, 0), (1, 1), (2, 2)):
                if run == 1:
                    old["scenarios"][0]["methods"].clear()
                    current.write_text(json.dumps(old))
                if run == 2:
                    current.write_text("{}")
                proc = subprocess.run(cmd, capture_output=True, text=True)
                self.assertEqual(proc.returncode, code, proc.stderr)


if __name__ == "__main__":
    unittest.main()
