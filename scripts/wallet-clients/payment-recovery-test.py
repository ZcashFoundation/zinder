#!/usr/bin/env python3
"""Check the payment recovery accounting, canonical, rollback, and receipt contracts."""
import argparse
import json
import runpy
import tempfile
import unittest
from pathlib import Path

M = runpy.run_path(str(Path(__file__).resolve().with_name("payment-recovery.py")))
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
        self.assertEqual(command[command.index("--port-base") + 1], "50100")
        args.bin_dir = Path("/tmp/bin")
        self.assertIn("--bin-dir", M["case_command"]("rollback", args))


if __name__ == "__main__":
    unittest.main()
