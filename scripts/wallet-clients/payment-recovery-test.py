#!/usr/bin/env python3
"""Check the payment recovery accounting, canonical, rollback, and receipt contracts."""
import argparse
import contextlib
import io
import json
import runpy
import signal
import subprocess
import sys
import tempfile
import time
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

M = runpy.run_path(str(Path(__file__).resolve().with_name("payment-recovery.py")))
GLOBALS = M["campaign"].__globals__
TXID = "ab" * 32
OTHER = "cd" * 32
HASH_A = "11" * 32
HASH_B = "22" * 32


def row(txid=TXID, height=20, pending=False, amount=10, fee=1, memos=("memo",)):
    return {"txid": txid, "height": height, "pending": pending, "amount_zat": amount,
            "fee_zat": fee, "memos": list(memos)}


def balance(total, transactions=(), height=30, spendable=0):
    return {"total_zat": total, "spendable_zat": spendable, "height": height,
            "transactions": list(transactions)}


class Fingerprints(unittest.TestCase):
    def test_addresses_and_transactions_use_separate_fingerprints(self):
        address = M["address_fingerprint"]("uregtest1example")
        transaction = M["tx_fingerprint"](TXID)
        self.assertEqual(len(address), 64)
        self.assertEqual(len(transaction), 64)
        self.assertNotEqual(address, transaction)
        with self.assertRaisesRegex(ValueError, "invalid transaction ID"):
            M["tx_fingerprint"]("uregtest1example")


class LedgerAccounting(unittest.TestCase):
    def test_exactly_once_debit_credit_and_fee(self):
        result = M["assert_transfer_accounting"](100, 89, 0, 10, 10, 1)
        self.assertEqual(result["payer_debit_zat"], 11)
        self.assertEqual(result["payee_credit_zat"], 10)

    def test_double_debit_or_credit_fails(self):
        with self.assertRaisesRegex(ValueError, "payer debit"):
            M["assert_transfer_accounting"](100, 78, 0, 10, 10, 1)
        with self.assertRaisesRegex(ValueError, "payee credit"):
            M["assert_transfer_accounting"](100, 89, 0, 20, 10, 1)

    def test_missing_fee_fails(self):
        with self.assertRaisesRegex(ValueError, "fee"):
            M["assert_transfer_accounting"](100, 89, 0, 10, 10, 0)

    def test_single_confirmed_row(self):
        snap = balance(0, [row()])
        self.assertEqual(M["require_single_confirmed"](snap, TXID, 20)["height"], 20)
        with self.assertRaisesRegex(ValueError, "confirmed at the expected"):
            M["require_single_confirmed"](snap, TXID, 21)
        with self.assertRaisesRegex(ValueError, "confirmed at the expected"):
            M["require_single_confirmed"](balance(0, [row(height=None, pending=True)]), TXID, 20)
        with self.assertRaisesRegex(ValueError, "exactly one"):
            M["require_single_confirmed"](balance(0, [row(), row()]), TXID, 20)


class WalletSettlement(unittest.TestCase):
    def test_startup_records_the_frozen_height_after_both_wallets_reach_it(self):
        with tempfile.TemporaryDirectory() as folder:
            private = Path(folder)
            fixture = private / "fixture-initial.json"
            fixture.write_text("{}")
            args = argparse.Namespace(output=private, zingo=fixture, zallet_backend=fixture,
                                      port_base=20000, revision="a" * 40, wait_seconds=5)
            recovery = M["Recovery"]("disconnect", args)
            recovery.private = private
            recovery.ready = {"lightwalletd": "http://127.0.0.1:20067",
                              "native_wallet_query": "http://127.0.0.1:20002"}
            recovery.receipt["fixture"] = {"anchor": {"height": 150}}
            recovery.rpc = Mock(return_value=150)
            zingo, zallet = Mock(), Mock()
            zingo.prepare.return_value, zallet.prepare.return_value = "payer", "payee"
            zingo.snapshot.return_value = balance(0, height=150)
            zallet.snapshot.side_effect = [balance(0, height=149), balance(0, height=150)]
            with (patch.dict(GLOBALS, Zingo=Mock(return_value=zingo), Zallet=Mock(return_value=zallet),
                             RoundTrip=Mock()), patch("time.sleep")):
                recovery.prepare_wallets()
            self.assertEqual(recovery.receipt["initial_wallet_heights"], {"zingo": 150, "zallet": 150})
            self.assertEqual(zallet.snapshot.call_count, 2)

    def test_preexisting_spendable_funds_cannot_satisfy_return_maturity(self):
        recovery = M["Recovery"]("disconnect", argparse.Namespace(output=Path("."), wait_seconds=5))
        existing, returned = 10_000_000, M["RETURN"]
        total = existing + returned
        recovery.zingo, recovery.zallet = Mock(), Mock()
        recovery.zingo.snapshot.side_effect = [balance(total, spendable=existing),
                                              balance(total, spendable=total)]
        recovery.trip = Mock()
        recovery.broadcast = Mock(return_value=TXID)
        recovery.observe_pending = Mock(return_value={})
        recovery.wait_tip = Mock(return_value={})
        recovery.canonical = Mock(return_value={"height": 20, "block_hash": HASH_A})
        recovery.wait_confirmed = Mock(return_value={
            "zingo": balance(total, [row(amount=returned, memos=(M["MEMO_RETURN"],))]),
            "zallet": balance(1_000_000, [row(fee=25_000)])})
        before = {"zingo": balance(existing), "zallet": balance(1_000_000 + returned + 25_000)}
        with patch("time.sleep"):
            result = recovery.settle_return(before, {})
        self.assertEqual(result["spendable_after_zat"], total)
        self.assertEqual(recovery.zingo.snapshot.call_count, 2)


class CanonicalInclusion(unittest.TestCase):
    def node(self, transactions, block_hash=HASH_A, height=20, confirmed=3):
        def call(method, *params):
            if method == "getrawtransaction":
                txid = params[0]
                return {"txid": txid, "height": height, "blockhash": block_hash,
                        "confirmations": confirmed}
            if method == "getblockhash":
                return block_hash
            if method == "getblock":
                return {"hash": block_hash, "height": height, "tx": list(transactions)}
            raise AssertionError(method)
        return call

    def test_canonical_block_and_membership(self):
        result = M["canonical_summary"](self.node([TXID]), TXID, 20)
        self.assertEqual(result["block_hash"], HASH_A)
        self.assertEqual(result["confirmations"], 3)

    def test_duplicate_membership_fails(self):
        with self.assertRaisesRegex(ValueError, "exactly once"):
            M["canonical_summary"](self.node([TXID, TXID]), TXID)

    def test_wrong_active_block_fails(self):
        def call(method, *params):
            if method == "getrawtransaction":
                return {"txid": params[0], "height": 20, "blockhash": HASH_A, "confirmations": 1}
            return HASH_B
        with self.assertRaisesRegex(ValueError, "not the active block"):
            M["canonical_summary"](call, TXID)

    def test_unconfirmed_and_wrong_height_fail(self):
        with self.assertRaisesRegex(ValueError, "confirmation"):
            M["canonical_summary"](self.node([TXID], confirmed=0), TXID)
        with self.assertRaisesRegex(ValueError, "height differs"):
            M["canonical_summary"](self.node([TXID]), TXID, 21)


class RemineIdentity(unittest.TestCase):
    def test_same_signed_bytes_on_a_distinct_block(self):
        result = M["assert_remine_identity"](
            {"tx_fingerprint": "f" * 64, "raw_sha256": "a" * 64},
            {"tx_fingerprint": "f" * 64, "raw_sha256_after": "a" * 64}, HASH_A, HASH_B)
        self.assertTrue(result["same_signed_transfer"])
        self.assertTrue(result["distinct_block_hash"])
        self.assertEqual(result["observed_raw_sha256"], "a" * 64)

    def test_different_transaction_or_block_fails(self):
        with self.assertRaisesRegex(ValueError, "same signed transaction"):
            M["assert_remine_identity"](
                {"tx_fingerprint": "f" * 64, "raw_sha256": "a" * 64},
                {"tx_fingerprint": "e" * 64, "raw_sha256_after": "a" * 64}, HASH_A, HASH_B)
        with self.assertRaisesRegex(ValueError, "reused the invalidated"):
            M["assert_remine_identity"](
                {"tx_fingerprint": "f" * 64, "raw_sha256": "a" * 64},
                {"tx_fingerprint": "f" * 64, "raw_sha256_after": "a" * 64}, HASH_A, HASH_A)

    def test_independently_observed_bytes_must_match(self):
        with self.assertRaisesRegex(ValueError, "signed bytes differ"):
            M["assert_remine_identity"](
                {"tx_fingerprint": "f" * 64, "raw_sha256": "a" * 64},
                {"tx_fingerprint": "f" * 64, "raw_sha256_after": "b" * 64}, HASH_A, HASH_B)

    def test_missing_original_digest_fails(self):
        for before in ({"tx_fingerprint": "f" * 64},
                       {"tx_fingerprint": "f" * 64, "raw_sha256": "a" * 63},
                       {"tx_fingerprint": "f" * 64, "raw_sha256": None}):
            with self.assertRaisesRegex(ValueError, "original signed transaction digest"):
                M["assert_remine_identity"](before, {"tx_fingerprint": "f" * 64,
                                                     "raw_sha256_after": "a" * 64}, HASH_A, HASH_B)

    def test_missing_observed_after_digest_fails(self):
        for after in ({"tx_fingerprint": "f" * 64},
                      {"tx_fingerprint": "f" * 64, "raw_sha256_after": "a" * 63},
                      {"tx_fingerprint": "f" * 64, "raw_sha256_after": None}):
            with self.assertRaisesRegex(ValueError, "independently observed remined transaction digest"):
                M["assert_remine_identity"]({"tx_fingerprint": "f" * 64,
                                             "raw_sha256": "a" * 64}, after, HASH_A, HASH_B)

    def test_old_digest_copy_is_not_an_observation(self):
        with self.assertRaisesRegex(ValueError, "independently observed remined transaction digest"):
            M["assert_remine_identity"](
                {"tx_fingerprint": "f" * 64, "raw_sha256": "a" * 64},
                {"tx_fingerprint": "f" * 64, "raw_sha256": "a" * 64}, HASH_A, HASH_B)


class ScanEvidence(unittest.TestCase):
    def test_harness_surface_reads_are_not_wallet_scans(self):
        delta = M["wallet_scan_delta"]({("GetLatestBlock", "ok"): 1},
                                       {("GetLatestBlock", "ok"): 2})
        self.assertEqual(delta, {"GetLatestBlock/ok": 1})

    def test_wallet_scan_during_disconnect_fails(self):
        with self.assertRaisesRegex(ValueError, "scanned"):
            M["wallet_scan_delta"]({("GetBlockRange", "ok"): 1},
                                   {("GetBlockRange", "ok"): 2})

    def test_counter_reset_fails(self):
        with self.assertRaisesRegex(ValueError, "reset"):
            M["wallet_scan_delta"]({("GetBlock", "ok"): 2}, {("GetBlock", "ok"): 1})


class PortSelection(unittest.TestCase):
    def test_named_bases_are_disjoint_and_in_range(self):
        ports = M["derived_ports"]([50000, 50100])
        self.assertEqual(len(ports), 16)
        self.assertEqual(len(set(ports)), 16)

    def test_out_of_range_or_overlapping_bases_fail(self):
        for base in (1023, 65535 - max(M["PORT_OFFSETS"]) + 1):
            with self.assertRaisesRegex(ValueError, "TCP range"):
                M["derived_ports"]([base])
        with self.assertRaisesRegex(ValueError, "overlap"):
            M["derived_ports"]([50000, 50000])


class ReceiptContract(unittest.TestCase):
    def receipt(self, case="disconnect", status="passed"):
        return {"schema_version": 1, "case": case, "status": status, "network": "zcash-regtest",
                "assertions": {"canonical": True},
                "cleanup": {"fixture_removed": True, "project_resources_remaining": 0}}

    def test_passed_and_failed_receipts(self):
        M["validate_receipt"](self.receipt(), "disconnect")
        failed = self.receipt(status="failed")
        failed["assertions"] = {}
        failed["cleanup"] = {"project_resources_remaining": 3}
        M["validate_receipt"](failed, "disconnect")

    def test_unproven_assertion_or_remaining_resources_fail(self):
        bad = self.receipt()
        bad["assertions"]["canonical"] = False
        with self.assertRaisesRegex(ValueError, "unproven"):
            M["validate_receipt"](bad, "disconnect")
        bad = self.receipt()
        bad["cleanup"]["project_resources_remaining"] = 1
        with self.assertRaisesRegex(ValueError, "Docker resources"):
            M["validate_receipt"](bad, "disconnect")

    def test_wrong_case_or_network_fails(self):
        with self.assertRaisesRegex(ValueError, "identity"):
            M["validate_receipt"](self.receipt(), "rollback")
        bad = self.receipt()
        bad["network"] = "zcash-testnet"
        with self.assertRaisesRegex(ValueError, "Regtest"):
            M["validate_receipt"](bad, "disconnect")


class MemoEvidence(unittest.TestCase):
    def test_only_the_known_fixture_memo_is_retained(self):
        evidence = M["memo_evidence"](["private-a", "private-b", "expected"], "expected")
        self.assertEqual(evidence["expected_present"], True)
        self.assertEqual(len(evidence["expected_sha256"]), 64)
        self.assertNotIn("private-a", json.dumps(evidence))
        self.assertNotIn("private-b", json.dumps(evidence))

    def test_missing_duplicate_or_invalid_expected_memo_fails(self):
        for memos, expected, message in (
                (["private-a"], "expected", "exactly once"),
                (["expected", "expected"], "expected", "exactly once"),
                ([], "", "invalid expected fixture memo"),
                (None, "expected", "did not report decrypted memos")):
            with self.assertRaisesRegex(ValueError, message):
                M["memo_evidence"](memos, expected)


class InputPins(unittest.TestCase):
    def test_pins_are_deterministic_and_bound_to_content(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            zingo = root / "zingo-cli"
            zallet = root / "zallet-zinder"
            source = root / "payment-recovery.py"
            zingo.write_bytes(b"zingo")
            zallet.write_bytes(b"zallet")
            source.write_text("harness", encoding="utf-8")
            wallets = (("zingo", zingo), ("zallet_backend", zallet))
            pins = M["input_pins"](wallets, (source,))
            self.assertEqual(pins, M["input_pins"](wallets, (source,)))
            self.assertEqual(pins["wallets"]["zingo"], M["digest"](zingo))
            self.assertEqual(pins["wallets"]["zallet_backend"], M["digest"](zallet))
            self.assertEqual(pins["harness"]["payment-recovery.py"], M["digest"](source))
            self.assertEqual(M["input_pins"](wallets, (source,)), pins)
            source.write_text("changed harness", encoding="utf-8")
            self.assertNotEqual(M["input_pins"](wallets, (source,)), pins)

    def test_missing_wallet_or_harness_source_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(ValueError, "not a readable file"):
                M["input_pins"]((("zingo", root / "missing"),), ())


class FakeRecovery(M["Recovery"]):
    def __init__(self, output):
        super().__init__("disconnect", argparse.Namespace(output=output))
        self.events = []

    def wallet_inputs(self):
        return ()

    def harness_sources(self):
        return ()

    def close_wallets(self):
        self.events.append("close")

    def record_teardown(self):
        self.events.append("teardown")
        self.receipt["cleanup"] = {"fixture_removed": True, "project_resources_remaining": 0}


class PinnedRecovery(FakeRecovery):
    def __init__(self, output, zingo, zallet, source):
        self.pin_zingo = zingo
        self.pin_zallet = zallet
        self.pin_source = source
        super().__init__(output)

    def wallet_inputs(self):
        return (("zingo", self.pin_zingo), ("zallet_backend", self.pin_zallet))

    def harness_sources(self):
        return (self.pin_source,)


def passed(recovery):
    recovery.receipt.update(status="passed", assertions={"canonical": True})


class ExecutionAndCleanup(unittest.TestCase):
    def test_successful_run_records_matching_pre_and_post_pins(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "evidence"
            recovery = FakeRecovery(output)
            recovery.execute(lambda: passed(recovery))
            self.assertEqual(recovery.events, ["close", "teardown"])
            receipt = json.loads((output / "receipt.json").read_text())
            self.assertEqual(receipt["status"], "passed")
            self.assertEqual(receipt["pins"]["pre"], receipt["pins"]["post"])

    def test_existing_output_is_not_reused_or_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "evidence"
            output.mkdir()
            receipt = output / "receipt.json"
            receipt.write_text('{"previous": true}', encoding="utf-8")
            marker = output / "keep.txt"
            marker.write_text("keep", encoding="utf-8")
            recovery = FakeRecovery(output)
            with self.assertRaises(FileExistsError):
                recovery.execute(lambda: passed(recovery))
            self.assertEqual(receipt.read_text(encoding="utf-8"), '{"previous": true}')
            self.assertEqual(marker.read_text(encoding="utf-8"), "keep")

    def test_execution_failure_is_recorded_and_teardown_still_runs(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "evidence"
            recovery = FakeRecovery(output)

            def exercise():
                raise RuntimeError("exercise failed")

            with self.assertRaisesRegex(RuntimeError, "exercise failed"):
                recovery.execute(exercise)
            self.assertEqual(recovery.events, ["close", "teardown"])
            receipt = json.loads((output / "receipt.json").read_text())
            self.assertEqual(receipt["status"], "failed")
            self.assertEqual(receipt["cleanup"]["project_resources_remaining"], 0)
            self.assertIn("RuntimeError: exercise failed",
                          (output / "private" / "failure.log").read_text())

    def test_close_or_teardown_failure_is_recorded(self):
        cases = ((FailingClose, "close failed"), (FailingTeardown, "teardown failed"))
        for factory, message in cases:
            with self.subTest(message=message), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "evidence"
                recovery = factory(output)
                with self.assertRaisesRegex(RuntimeError, message):
                    recovery.execute(lambda: passed(recovery))
                self.assertEqual(recovery.events, ["close", "teardown"])
                receipt = json.loads((output / "receipt.json").read_text())
                self.assertEqual(receipt["status"], "failed")
                self.assertTrue((output / "private" / "failure.log").is_file())

    def test_incomplete_teardown_fails_the_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "evidence"
            recovery = IncompleteTeardown(output)
            with self.assertRaisesRegex(ValueError, "Docker resources"):
                recovery.execute(lambda: passed(recovery))
            receipt = json.loads((output / "receipt.json").read_text())
            self.assertEqual(receipt["status"], "failed")
            self.assertFalse(receipt["cleanup"]["fixture_removed"])

    def test_changed_wallet_or_harness_pin_fails_the_run(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            zingo = root / "zingo-cli"
            zallet = root / "zallet-zinder"
            source = root / "payment-recovery.py"
            zingo.write_bytes(b"zingo")
            zallet.write_bytes(b"zallet")
            source.write_text("harness", encoding="utf-8")
            output = root / "evidence"
            recovery = PinnedRecovery(output, zingo, zallet, source)

            def exercise():
                source.write_text("changed harness", encoding="utf-8")
                passed(recovery)

            with self.assertRaisesRegex(ValueError, "changed during the run"):
                recovery.execute(exercise)
            receipt = json.loads((output / "receipt.json").read_text())
            self.assertEqual(receipt["status"], "failed")
            self.assertNotEqual(receipt["pins"]["pre"], receipt["pins"]["post"])


class FailingClose(FakeRecovery):
    def close_wallets(self):
        self.events.append("close")
        raise RuntimeError("close failed")


class FailingTeardown(FakeRecovery):
    def record_teardown(self):
        self.events.append("teardown")
        self.receipt["cleanup"] = {"fixture_removed": False, "error": "teardown failed"}
        raise RuntimeError("teardown failed")


class IncompleteTeardown(FakeRecovery):
    def record_teardown(self):
        self.events.append("teardown")
        self.receipt["cleanup"] = {"fixture_removed": False, "project_resources_remaining": 1}


class CommandConstruction(unittest.TestCase):
    def test_case_command_is_reproducible(self):
        args = argparse.Namespace(output=Path("/tmp/evidence"), port_base=50100,
                                  images=Path("/tmp/images.json"), revision="a" * 40,
                                  zingo=Path("/tmp/zingo"), zallet_backend=Path("/tmp/zallet"),
                                  wait_seconds=600, bin_dir=None)
        command = M["case_command"]("rollback", args)
        self.assertNotIn("--bin-dir", command)
        self.assertNotIn("--project", command)
        self.assertEqual(command[command.index("--port-base") + 1], "50100")
        args.bin_dir = Path("/tmp/bin")
        self.assertIn("--bin-dir", M["case_command"]("rollback", args))

    def test_case_command_carries_the_supervisor_project_identity(self):
        project = M["project_name"]("rollback")
        args = argparse.Namespace(output=Path("/tmp/evidence"), port_base=50100,
                                  images=Path("/tmp/images.json"), revision="a" * 40,
                                  zingo=Path("/tmp/zingo"), zallet_backend=Path("/tmp/zallet"),
                                  wait_seconds=600, bin_dir=Path("/tmp/bin"), project=project)
        command = M["case_command"]("rollback", args)
        self.assertEqual(command[command.index("--project") + 1], project)


class ProjectIdentity(unittest.TestCase):
    def test_generated_identities_are_case_bound_and_unique(self):
        disconnect = M["project_name"]("disconnect")
        rollback = M["project_name"]("rollback")
        self.assertRegex(disconnect, r"^zinder-fixture-payment-recovery-disconnect-[a-f0-9]{12}$")
        self.assertRegex(rollback, r"^zinder-fixture-payment-recovery-rollback-[a-f0-9]{12}$")
        self.assertNotEqual(disconnect, M["project_name"]("disconnect"))
        name, path = M["case_project"]("transport", Path("/unused-test-projects"))
        self.assertEqual(path.name, name)
        self.assertTrue(name.startswith("zinder-fixture-payment-recovery-transport-"))
        with self.assertRaisesRegex(ValueError, "unsupported recovery case"):
            M["project_name"]("unrecognized")

    def test_fresh_state_is_accepted_and_existing_state_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "projects"
            state.mkdir()
            name, path = M["case_project"]("disconnect", state)
            self.assertEqual(path, state / name)
            self.assertFalse(path.exists())
            path.mkdir()
            with self.assertRaisesRegex(ValueError, "already exists"):
                M["case_project"]("disconnect", state, name)

    def test_foreign_or_malformed_projects_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "projects"
            state.mkdir()
            with self.assertRaisesRegex(ValueError, "different recovery case"):
                M["case_project"]("rollback", state, M["project_name"]("disconnect"))
            for name in ("zinder-fixture-payment-recovery-disconnect-xyz",
                         "zinder-fixture-payment-recovery-disconnect-" + "a" * 11,
                         "zinder-fixture-roundtrip-" + "a" * 12, ""):
                with self.assertRaisesRegex(ValueError, "invalid fixture project"):
                    M["case_project"]("disconnect", state, name)


def campaign_args(output, **changes):
    values = {"output": output, "port_bases": [50000, 50100], "case_timeout": 30,
              "case_grace": 2, "wait_seconds": 60, "images": Path("/tmp/images.json"),
              "bin_dir": Path("/tmp/bin"), "revision": "a" * 40, "zingo": Path("/tmp/zingo"),
              "zallet_backend": Path("/tmp/zallet")}
    values.update(changes)
    return argparse.Namespace(**values)


def process_alive(pid):
    try:
        state = (Path("/proc") / str(pid) / "stat").read_bytes().rsplit(b")", 1)[1].split()[0]
    except (FileNotFoundError, ProcessLookupError, PermissionError, IndexError):
        return False
    return state != b"Z"


class CampaignSupervision(unittest.TestCase):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.scratch = Path(scratch.name)
        self.root = self.scratch / "campaign"
        self.args = campaign_args(self.root)
        self.launched = []
        self.recovered = []

    def write_receipt(self, output, case, status="passed"):
        output.mkdir(mode=0o700, parents=True, exist_ok=True)
        passed = status == "passed"
        receipt = {"schema_version": 1, "case": case, "status": status,
                   "network": "zcash-regtest", "assertions": {"canonical": True} if passed else {},
                   "cleanup": {"fixture_removed": passed, "project_resources_remaining": 0}}
        (output / "receipt.json").write_text(json.dumps(receipt), encoding="utf-8")

    def launch(self, exit_code=0, timeout=False, status="passed", receipt=True, interrupted=None):
        def run(command, log, timeout_value, grace):
            case = command[3]
            output = Path(command[command.index("--output") + 1])
            self.launched.append((case, output, timeout_value, grace, command))
            if interrupted is not None:
                raise interrupted
            if receipt:
                self.write_receipt(output, case, status)
            return {"exit_code": exit_code, "timeout": timeout}
        return run

    def recovery(self, **changes):
        value = {"wallet_stopped": False, "fixture_torn_down": False, "relay_stopped": False,
                 "resources_remaining": 0, "problems": [], "repaired": False}
        value.update(changes)

        def run(case, child, args):
            self.recovered.append((case, child.project, child.output))
            return dict(value)
        return run

    def campaign(self, launch, recovery, args=None):
        with patch.dict(GLOBALS, {"launch_case": launch, "recover_case": recovery,
                                  "check_ports": lambda bases: bases}), \
                contextlib.redirect_stdout(io.StringIO()):
            return M["campaign"](args or self.args)

    def summary(self):
        return json.loads((self.root / "summary.json").read_text(encoding="utf-8"))

    def supervision(self, case):
        return json.loads((self.root / case / "supervision.json").read_text(encoding="utf-8"))

    def test_cooperative_children_pass_with_known_fresh_projects(self):
        self.campaign(self.launch(), self.recovery())
        self.assertEqual([row[0] for row in self.launched], list(M["CASES"]))
        self.assertEqual([row[2] for row in self.launched], [30, 30])
        self.assertEqual([row[3] for row in self.launched], [2, 2])
        projects = [row[1] for row in self.recovered]
        self.assertEqual(len(set(projects)), 2)
        for case, project in zip(M["CASES"], projects, strict=True):
            self.assertRegex(project, rf"^zinder-fixture-payment-recovery-{case}-[a-f0-9]{{12}}$")
            self.assertEqual(self.supervision(case)["status"], "passed")
            self.assertEqual(self.supervision(case)["supervision"]["kind"], "cooperative")
            self.assertEqual(self.supervision(case)["project"], project)
        summary = self.summary()
        self.assertEqual({row["status"] for row in summary["cases"].values()}, {"passed"})
        self.assertEqual((summary["case_timeout"], summary["case_grace"]), (30, 2))
        self.assertEqual(summary["port_bases"], {"disconnect": 50000, "rollback": 50100})

    def test_timed_out_child_never_counts_as_passed(self):
        with self.assertRaisesRegex(ValueError, "cases failed"):
            self.campaign(self.launch(exit_code=-9, timeout=True), self.recovery())
        summary = self.summary()
        failed = summary["cases"]["disconnect"]
        self.assertEqual((failed["status"], failed["supervision"]), ("failed", "timeout"))
        self.assertEqual(summary["cases"]["rollback"]["status"], "skipped")
        self.assertEqual(summary["cases"]["rollback"]["supervision"], "not-run")
        record = self.supervision("disconnect")
        self.assertEqual((record["status"], record["supervision"]["kind"]), ("failed", "timeout"))
        self.assertEqual(record["receipt"]["status"], "passed")  # child evidence is preserved, not rewritten
        self.assertEqual(record["receipt"]["sha256"], M["digest"](self.root / "disconnect" / "receipt.json"))
        self.assertEqual(len(self.recovered), 1)

    def test_killed_child_without_a_receipt_publishes_a_failure_receipt(self):
        recovery = self.recovery(wallet_stopped=True, fixture_torn_down=True, relay_stopped=True,
                                 repaired=True)
        with self.assertRaisesRegex(ValueError, "cases failed"):
            self.campaign(self.launch(exit_code=-9, timeout=True, receipt=False), recovery)
        receipt = json.loads((self.root / "disconnect" / "receipt.json").read_text(encoding="utf-8"))
        self.assertEqual((receipt["status"], receipt["source"]), ("failed", "campaign-supervision"))
        self.assertEqual(receipt["cleanup"]["project_resources_remaining"], 0)
        self.assertTrue(receipt["cleanup"]["recovered"])
        self.assertIn("timeout", receipt["supervision"]["kind"])
        M["validate_receipt"](receipt, "disconnect")
        record = self.supervision("disconnect")
        self.assertEqual(record["receipt"]["status"], "failed")
        self.assertEqual(record["cleanup"]["wallet_stopped"], True)

    def test_cleanup_failure_marks_an_otherwise_passing_case_failed(self):
        recovery = self.recovery(problems=["owned Docker resources remain"], resources_remaining=2)
        with self.assertRaisesRegex(ValueError, "cases failed"):
            self.campaign(self.launch(), recovery)
        failed = self.summary()["cases"]["disconnect"]
        self.assertEqual(failed["status"], "failed")
        self.assertIn("cleanup is incomplete", failed["error"])
        self.assertEqual(self.supervision("disconnect")["cleanup"]["problems"],
                         ["owned Docker resources remain"])

    def test_resources_removed_by_parent_recovery_do_not_count_as_passed(self):
        recovery = self.recovery(wallet_stopped=True, repaired=True)
        with self.assertRaisesRegex(ValueError, "cases failed"):
            self.campaign(self.launch(), recovery)
        failed = self.summary()["cases"]["disconnect"]
        self.assertEqual(failed["status"], "failed")
        self.assertIn("parent recovery removed", failed["error"])

    def test_interrupted_child_is_failed_and_the_summary_is_published(self):
        with self.assertRaises(KeyboardInterrupt):
            self.campaign(self.launch(interrupted=KeyboardInterrupt()), self.recovery())
        summary = self.summary()
        self.assertEqual(summary["cases"]["disconnect"]["status"], "failed")
        self.assertEqual(summary["cases"]["disconnect"]["supervision"], "interrupted")
        self.assertEqual(summary["cases"]["rollback"]["status"], "skipped")
        receipt = json.loads((self.root / "disconnect" / "receipt.json").read_text(encoding="utf-8"))
        self.assertEqual(receipt["status"], "failed")
        self.assertEqual(receipt["supervision"]["kind"], "interrupted")
        self.assertEqual(len(self.recovered), 1)

    def interrupt_parent_cleanup(self, signum, error):
        cleanup_finished = []

        def interrupt(_signum, _frame):
            raise error

        def stop_wallet(*args):
            signal.raise_signal(signum)
            cleanup_finished.append("wallet")
            return False

        previous = signal.signal(signum, interrupt)
        self.addCleanup(signal.signal, signum, previous)
        with patch.dict(GLOBALS, {
                "close_owned_wallet": stop_wallet, "owned_relay": lambda *args: None,
                "project_resource_count": lambda project: cleanup_finished.append("readback") or 0}):
            with self.assertRaises(type(error)):
                self.campaign(self.launch(), M["recover_case"])
        self.assertEqual(cleanup_finished, ["wallet", "readback"])
        self.assertIs(signal.getsignal(signum), interrupt)
        self.assertEqual(self.supervision("disconnect")["supervision"]["kind"], "interrupted")
        cases = self.summary()["cases"]
        self.assertEqual(cases["disconnect"]["status"], "failed")
        self.assertEqual(cases["disconnect"]["cleanup"]["resources_remaining"], 0)
        self.assertEqual(cases["rollback"]["status"], "skipped")
        self.assertEqual(len(self.launched), 1)

    def test_ctrl_c_during_parent_wallet_cleanup_finishes_teardown(self):
        self.interrupt_parent_cleanup(signal.SIGINT, KeyboardInterrupt())

    def test_sigterm_during_parent_wallet_cleanup_finishes_teardown(self):
        self.interrupt_parent_cleanup(signal.SIGTERM, SystemExit("payment recovery interrupted"))

    def test_existing_campaign_output_is_not_reused(self):
        self.root.mkdir()
        marker = self.root / "keep.txt"
        marker.write_text("keep", encoding="utf-8")
        with self.assertRaises(FileExistsError):
            self.campaign(self.launch(), self.recovery())
        self.assertEqual(self.launched, [])
        self.assertEqual(marker.read_text(encoding="utf-8"), "keep")

    def test_nonpositive_case_bounds_fail_before_launching(self):
        for field in ("case_timeout", "case_grace"):
            with self.subTest(field=field):
                scratch = tempfile.TemporaryDirectory()
                self.addCleanup(scratch.cleanup)
                args = campaign_args(Path(scratch.name) / "campaign", **{field: 0})
                with self.assertRaisesRegex(ValueError, "must be positive"):
                    self.campaign(self.launch(), self.recovery(), args=args)
                self.assertEqual(self.launched, [])

    def test_hung_child_and_descendants_are_killed_after_its_timeout(self):
        script = self.scratch / "hang.py"
        pids_path = self.scratch / "hang.pids"
        script.write_text(
            "import os, signal, subprocess, sys, time\n"
            "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
            "child = subprocess.Popen([sys.executable, '-c', "
            "'import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(60)'])\n"
            "with open(sys.argv[1], 'w') as out:\n"
            "    out.write(f'{os.getpid()} {child.pid}')\n"
            "time.sleep(60)\n", encoding="utf-8")
        result = M["launch_case"]([sys.executable, str(script), str(pids_path)],
                                  self.scratch / "hang.log", timeout=.5, grace=.2)
        leader, descendant = map(int, pids_path.read_text(encoding="utf-8").split())
        self.assertTrue(result["timeout"])
        self.assertNotEqual(result["exit_code"], 0)
        for pid in (leader, descendant):
            deadline = time.monotonic() + 2
            while process_alive(pid) and time.monotonic() < deadline:
                time.sleep(.02)
            self.assertFalse(process_alive(pid))

    def test_parent_cancellation_during_timeout_grace_waits_for_the_child_to_be_reaped(self):
        child = ("import os, signal, sys, time\n"
                 "signal.signal(signal.SIGTERM, lambda *_: os.kill(os.getppid(), int(sys.argv[1])))\n"
                 "time.sleep(60)\n")
        popen = subprocess.Popen
        processes = []

        def start(*args, **kwargs):
            process = popen(*args, **kwargs)
            processes.append(process)
            self.addCleanup(M["stop_group"], process, signal.SIGKILL, 1)
            return process

        def terminate(*_):
            raise SystemExit("payment recovery interrupted")

        previous = signal.signal(signal.SIGTERM, terminate)
        self.addCleanup(signal.signal, signal.SIGTERM, previous)
        with patch.object(subprocess, "Popen", side_effect=start):
            for sig, error in ((signal.SIGINT, KeyboardInterrupt), (signal.SIGTERM, SystemExit)):
                with self.subTest(signal=sig), self.assertRaises(error):
                    M["launch_case"]([sys.executable, "-c", child, str(int(sig))],
                                     self.scratch / f"grace-{sig}.log", timeout=.75, grace=.25)
                self.assertEqual(processes[-1].returncode, -signal.SIGKILL)

    def test_launch_case_stops_the_group_before_propagating_an_interruption(self):
        stopped = []

        class FakeProcess:
            pid = 4242
            returncode = None

            def wait(self, timeout=None):
                raise KeyboardInterrupt

        fake = types.SimpleNamespace(Popen=lambda *args, **kwargs: FakeProcess(),
                                     TimeoutExpired=subprocess.TimeoutExpired,
                                     STDOUT=subprocess.STDOUT)
        with patch.dict(GLOBALS, {"subprocess": fake,
                                  "stop_group": lambda proc, sig, grace: stopped.append(sig)}):
            with self.assertRaises(KeyboardInterrupt):
                M["launch_case"](["fake"], self.scratch / "interrupt.log", 5, 1)
        self.assertEqual(stopped, [signal.SIGINT])


if __name__ == "__main__":
    unittest.main()
