#!/usr/bin/env python3
"""Focused checks for the counterparty payment-role runner's reproducibility.

The runner must stay reproducible across machines: it has no host- or
checkout-specific default paths, it binds the receipt to the release tag the
caller selects (resolved to a commit), and it honestly records the Zally
residual fee and per-actor pending gaps instead of folding them into a pass.
These tests exercise that logic without starting a fixture.
"""
import importlib
import json
import runpy
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

SCRIPTS = Path(__file__).resolve().parent
MODULE = runpy.run_path(str(SCRIPTS / "payment-roles.py"))
V0_22_0_COMMIT = "999d51ab835037a2466763ca1a1e7739a26b72c1"


class ReproducibilityTests(unittest.TestCase):
    def test_release_tag_resolves_to_commit(self):
        self.assertEqual(MODULE["resolve_tag"]("v0.22.0"), V0_22_0_COMMIT)
        with self.assertRaises(ValueError):
            MODULE["resolve_tag"]("v0.99.0-not-a-tag")

    def test_revision_must_match_the_resolved_tag(self):
        reconcile = MODULE["reconcile_revision"]
        self.assertEqual(reconcile("v0.22.0", None), V0_22_0_COMMIT)
        self.assertEqual(reconcile("v0.22.0", V0_22_0_COMMIT), V0_22_0_COMMIT)
        with self.assertRaises(ValueError):
            reconcile("v0.22.0", "0" * 40)

    def test_images_filename_derives_from_the_tag(self):
        filename = MODULE["images_filename"]
        self.assertEqual(filename("v0.22.0"), "images-v0.22.0.json")
        self.assertEqual(filename("v0.23.0"), "images-v0.23.0.json")
        self.assertNotEqual(filename("v0.22.0"), filename("v0.23.0"))

    def test_release_must_name_a_tag(self):
        for ref in ("HEAD", V0_22_0_COMMIT, "refs/heads/main"):
            with self.subTest(ref=ref), self.assertRaisesRegex(ValueError, "invalid release tag"):
                MODULE["resolve_tag"](ref)

    def test_fixture_up_uses_selected_state_root_and_keeps_exact_readiness_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state, evidence = root / "state", root / "evidence"
            evidence.mkdir()
            images = root / "images.json"
            images.write_text("{}")
            ready_bytes = b'{ "project": "owned" }\n'

            def provision(command, **kwargs):
                self.assertEqual(command[command.index("--state-root") + 1], str(state))
                (state / "owned").mkdir(parents=True)
                (state / "owned/ready.initial.json").write_bytes(ready_bytes)

            with patch("subprocess.run", side_effect=provision):
                result, snapshot = MODULE["fixture_up"](state, 24300, evidence, "owned",
                    root / "node.toml", images, root / "bin", V0_22_0_COMMIT, "images-v0.22.0.json")
            self.assertEqual(result, json.loads(ready_bytes))
            self.assertEqual(snapshot, evidence / "fixtures/owned/ready.initial.json")
            self.assertEqual(snapshot.read_bytes(), ready_bytes)

    def test_required_inputs_are_engine_specific(self):
        required = MODULE["engine_required_inputs"]
        vizor = SimpleNamespace(images=None, bin_dir=None, zingo=None, evidence=None,
                                vizor_engine=None, vizor_source=None, vizor_provenance=None,
                                devtool=None, devtool_source=None,
                                zally_source=None, zally_binary=None,
                                native_probe_python=None, native_proto_dir=None)
        self.assertEqual(required("vizor", vizor),
                         ["images", "bin_dir", "zingo", "evidence",
                          "vizor_engine", "vizor_source", "vizor_provenance"])
        zally = SimpleNamespace(**{**vars(vizor),
                                   "images": "i", "bin_dir": "b", "zingo": "z", "evidence": "e",
                                   "zally_source": "s", "zally_binary": "h",
                                   "native_probe_python": "p", "native_proto_dir": "d"})
        self.assertEqual(required("zally", zally), [])
        # devtool inputs are not required when a different engine is selected.
        devtool_only = SimpleNamespace(images="i", bin_dir="b", zingo="z", evidence="e",
                                       vizor_engine=None, vizor_source=None, vizor_provenance=None,
                                       devtool=None, devtool_source=None,
                                       zally_source=None, zally_binary=None,
                                       native_probe_python=None, native_proto_dir=None)
        self.assertNotIn("devtool", required("vizor", devtool_only))

    def test_zally_can_select_build_directory_without_a_binary(self):
        args = SimpleNamespace(images="i", bin_dir="b", zingo="z", evidence="e",
                               zally_source="s", zally_binary=None, zally_build="build",
                               native_probe_python="p", native_proto_dir="d")
        self.assertEqual(MODULE["engine_required_inputs"]("zally", args), [])
        args.zally_build = None
        self.assertTrue(MODULE["engine_required_inputs"]("zally", args))

    def test_build_only_cli_enters_cargo_builder_before_fixture_setup(self):
        zally = importlib.import_module("zally-payment")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            flags = ["payment-roles", "--engine", "zally", "--release-tag", "v0.22.0",
                     "--images", "images.json", "--bin-dir", "bin", "--zingo", "zingo",
                     "--evidence", str(root / "new"), "--zally-source", "source",
                     "--zally-build", "build", "--native-probe-python", "python",
                     "--native-proto-dir", "proto"]
            with patch.object(sys, "argv", flags), patch("os.umask"), \
                    patch.object(zally, "build", side_effect=RuntimeError("builder reached")) as build:
                with self.assertRaisesRegex(RuntimeError, "builder reached"):
                    MODULE["main"]()
            build.assert_called_once_with(Path("source"), Path("build"), SCRIPTS / "zally-payment.rs")

    def test_devtool_reads_truncated_and_padded_sqlite_memos(self):
        decode = importlib.import_module("devtool-payment").decode_memo
        for raw in (None, b"\xf6", b"\xf6".ljust(512, b"\0")):
            self.assertIsNone(decode(raw))
        for raw in (b"fixture", b"fixture".ljust(512, b"\0")):
            self.assertEqual(decode(raw), "fixture")
        for raw in (b"", b"\xff", b"\xfe", b"\xf6\x01"):
            self.assertIsNotNone(decode(raw))

    def test_memo_absence_match_and_mismatch_for_each_receiver(self):
        receive = MODULE["engine_receive"]
        for observed in (None, "expected", "wrong", "", {"non_text": True}):
            fixtures = {
                "devtool": ({"transactions": [{"txid": "tx", "amount_zat": 50,
                    "memos": [] if observed is None else [observed],
                    "outputs": [{"value": 50, "is_change": False, "memo": observed},
                                {"value": 60, "is_change": True, "memo": "expected"}]}]}, {}),
                "vizor": ({"history": [{"txid": "tx", "balance_delta": 50}]}, {"memo": observed}),
                "zally": ({"receives": [{"txid": "tx", "memo": observed, "value": 50,
                                          "pool": "sapling", "mined_height": 20}]}, {}),
            }
            for engine, (snapshot, detail) in fixtures.items():
                with self.subTest(engine=engine, memo=observed):
                    if observed is None or observed == "expected":
                        result = receive(engine, snapshot, detail, "tx", 50, "expected")
                        self.assertEqual(result["memo_verified"], observed is not None)
                    else:
                        with self.assertRaisesRegex(ValueError, "different payee memo"):
                            receive(engine, snapshot, detail, "tx", 50, "expected")

    def test_zingo_reopen_only_uses_offline_commands(self):
        module = importlib.import_module("zingo-payment")
        with tempfile.TemporaryDirectory() as temp:
            wallet = module.Counterparty("/unused", temp, "http://127.0.0.1:10067",
                                         "http://127.0.0.1:10032")
            responses = ['{"scan_ranges":[]}',
                         "\n".join("confirmed_" + pool + "_balance: 0"
                                   for pool in ("transparent", "sapling", "orchard", "ironwood")),
                         '{"spendable_balance":0}', "", '{"value_transfers":[]}']
            with patch.object(wallet.wallet, "_cli", side_effect=responses) as cli:
                self.assertEqual(wallet.restart()["total_zat"], 0)
            self.assertEqual([call.args[0] for call in cli.call_args_list],
                             ["sync", "balance", "spendable_balance", "transactions", "messages"])
            self.assertTrue(all(not call.kwargs.get("online") for call in cli.call_args_list))

    def test_ten_confirmations_count_inclusion_and_both_wallet_heights(self):
        settle = MODULE["settled_balances"]
        node, engine, zingo = Mock(), Mock(), Mock()
        node.tip.side_effect = [100, 109, 110, 109, 109]
        node.transaction.return_value = {"height": 100, "confirmations": 10}
        engine.sync.return_value = {"scanned": 109, "total_zat": 3}
        zingo.snapshot.return_value = {"height": 109, "total_zat": 7}
        with patch.dict(settle.__globals__, inclusion=Mock(return_value={"mined_height": 100}),
                        mining_wait=Mock()) as state:
            result = settle(node, {}, "tx", engine, zingo)
            state["mining_wait"].assert_called_once_with(node, {}, 9)
            self.assertEqual(result[2]["observed_after_confirmations"], 10)
            with self.assertRaisesRegex(ValueError, "outside"):
                settle(node, {}, "tx", engine, zingo)
            zingo.snapshot.return_value["height"] = 108
            with self.assertRaisesRegex(ValueError, "ten-confirmation height"):
                settle(node, {}, "tx", engine, zingo)

    def test_zally_scope_names_residual_fee_and_pending(self):
        evidence = {entry["capability"]: entry for entry in MODULE["missing_api_evidence"]("zally")}
        self.assertIn("payer fee read", evidence)
        self.assertIn("residual-only", evidence["payer fee read"]["detail"])
        self.assertIn("wallet shielded pending state", evidence)
        self.assertEqual(evidence["wallet shielded pending state"]["status"],
                         "unexercised (no public API)")
        self.assertIn("outgoing transaction history", evidence)

    def test_zally_change_must_survive_reopen_even_when_balances_match(self):
        run = MODULE["run_engine_receives"]
        node, zingo, engine = Mock(), Mock(), Mock()
        inbound, returned = "ab" * 32, "cd" * 32
        node.mempool.return_value = [inbound, returned]
        node.transaction.return_value = {"height": 100}
        zingo.send.return_value, engine.send.return_value = inbound, returned
        zingo.snapshot.return_value = {"total_zat": 200000, "spendable_zat": 200000}
        engine.snapshot.return_value = {"total_zat": 0}
        received = {"txid": inbound, "value": 60, "memo": "in", "pool": "sapling",
                    "output_index": 0, "mined_height": 100}
        change = {"txid": returned, "value": 29, "is_change": True, "pool": "sapling"}
        funded = {"total_zat": 60, "spendable_zat": 60, "receives": [received]}
        after = {"total_zat": 29, "receives": [received, change], "unspent": [change]}
        engine.sync.side_effect = [funded, after]
        engine.restart.return_value = dict(after, receives=[received])
        zingo.detail.return_value = {"fee_zat": 1, "memos": ["in"]}
        zingo_after = {"total_zat": 199969, "transactions": [{"txid": inbound}, {"txid": returned}]}
        zingo.restart.return_value = zingo_after
        settle = Mock(side_effect=[(funded, {"total_zat": 199939}, {}),
                                   (after, zingo_after, {})])
        with patch.dict(run.__globals__, settled_balances=settle, mining_wait=Mock(),
                        pending_observation=Mock(), payee_credit_zingo=Mock()):
            with self.assertRaisesRegex(ValueError, "return change row"):
                run("zally", node, {}, zingo, engine, 60, 30, "in", "back")

    def test_source_has_no_host_specific_defaults(self):
        source = (SCRIPTS / "payment-roles.py").read_text()
        for marker in ("/home/ubuntu/", "RELEASE_BIN", "ZALLY_SOURCE", "ZALLY_BUILD",
                       "ZALLY_BINARY", "VIZOR_ENGINE", "REVISION = ", "STATE = ",
                       "IMAGES = {", "global ZINGO"):
            self.assertNotIn(marker, source, f"host-specific default {marker!r} survives")
        self.assertIn('"--release-tag", required=True', source)
        self.assertIn('"--revision"', source)


if __name__ == "__main__":
    unittest.main()
