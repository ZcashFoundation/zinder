#!/usr/bin/env python3
"""Check transport-payment port and receipt rules without starting a chain."""
import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import Mock, patch


source = Path(__file__).resolve().parent / "transport-payment.py"
spec = importlib.util.spec_from_file_location("transport_payment", source)
tool = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tool)


class TransportPortTests(unittest.TestCase):
    def test_derived_ports_stay_inside_the_private_range(self):
        ports = tool.RECOVERY.derived_ports([20900], tool.ROUTE_OFFSETS)
        self.assertEqual(ports, [20902, 20905, 20906, 20907, 20910, 20932, 20967, 20982, 20990])
        self.assertEqual(len(set(ports)), len(ports))
        self.assertTrue(all(20900 <= port <= 21100 for port in ports))

    def test_last_usable_base_keeps_every_port_in_range(self):
        self.assertEqual(max(tool.RECOVERY.derived_ports([65535 - tool.ROUTE_OFFSET], tool.ROUTE_OFFSETS)), 65535)

    def test_base_outside_the_range_is_rejected(self):
        for base in (1023, 65535 - tool.ROUTE_OFFSET + 1):
            with self.assertRaises(ValueError):
                tool.RECOVERY.derived_ports([base], tool.ROUTE_OFFSETS)


class TransportMemoTests(unittest.TestCase):
    def test_only_the_known_fixture_memo_is_retained(self):
        evidence = tool.memo_evidence(["arbitrary-decrypted-note", tool.MEMO_OUTBOUND],
                                      tool.MEMO_OUTBOUND)
        self.assertTrue(evidence["expected_present"])
        self.assertNotIn("arbitrary-decrypted-note", json.dumps(evidence))
        with self.assertRaisesRegex(ValueError, "exactly once"):
            tool.memo_evidence(["arbitrary-decrypted-note"], tool.MEMO_OUTBOUND)


class TransportCleanupTests(unittest.TestCase):
    def test_offline_teardown_closes_wallet_without_restoring_route(self):
        case = object.__new__(tool.Transport)
        case.direct_native, case.route_process = "http://127.0.0.1:32002", None
        with patch.object(case, "start_route", side_effect=AssertionError("unexpected relay startup")), \
             patch.object(tool.RECOVERY.Recovery, "close_wallets") as close:
            case.close_wallets()
            close.assert_called_once_with()


class TransportHarnessPinTests(unittest.TestCase):
    def test_transport_pins_both_recovery_and_transport_sources(self):
        def module(name):
            return type("Module", (), {"__file__": name.split(".")[-1] + ".py"})
        with patch.object(tool.RECOVERY, "import_module", side_effect=module):
            sources = object.__new__(tool.Transport).harness_sources()
        names = {Path(source).name for source in sources}
        self.assertIn("payment-recovery.py", names)
        self.assertIn("transport-payment.py", names)
        self.assertEqual(len(names), len(sources))


class TransportScanTests(unittest.TestCase):
    def test_only_reconnect_errors_run_the_restart_control(self):
        for error in (KeyboardInterrupt(), SystemExit(143), TimeoutError("wallet did not reconnect")):
            with self.subTest(error=type(error).__name__):
                transport = Mock()
                transport.ready = {"native_wallet_query": "http://127.0.0.1:20902"}
                transport.receipt = {"transport": {}, "assertions": {}}
                transport.broadcast.return_value = "payment"
                transport.balances.return_value = {"zallet": {"total_zat": 0}}
                transport.rpc.side_effect = [["payment"], 271, []]
                transport.canonical.return_value = {"height": 262}
                transport.wait_process_confirmed.side_effect = error
                with patch.object(tool, "reachable", side_effect=[False, True, False, True, True]):
                    with self.assertRaises(type(error)):
                        tool.Transport.run_transport(transport)
                if isinstance(error, TimeoutError):
                    transport.restart_control.assert_called_once_with("payment", 262, 271)
                else:
                    transport.restart_control.assert_not_called()

    def test_confirmed_payment_waits_for_the_same_process_to_scan_the_outage_tip(self):
        transport = object.__new__(tool.Transport)
        transport.args = Mock(wait_seconds=30)
        transport.zallet = Mock()
        transport.zallet._owned_identity.return_value = (41, 1)
        row = {"txid": "payment", "height": 262, "pending": False,
               "amount_zat": tool.OUTBOUND, "memos": [tool.MEMO_OUTBOUND]}
        snapshots = [{"height": h, "transactions": [row]} for h in (266, 271)]
        transport.zallet._snapshot_once.side_effect = snapshots
        with patch.object(tool.time, "sleep"):
            result = transport.wait_process_confirmed("payment", 262, 271, identity=(41, 1))
        self.assertEqual(result["height"], 271)
        self.assertEqual(transport.zallet._owned_identity.call_count, 2)


class TransportReceiptTests(unittest.TestCase):
    def receipt(self, **updates):
        receipt = {
            "schema_version": 1,
            "case": "transport",
            "network": "zcash-regtest",
            "status": "passed",
            "assertions": {"same_process_reconnected_without_restart": True},
            "same_process": {"reconnected": True},
            "scenario": {"outbound": {"fully_synced_height": 271}, "outage": {"node_tip": 271}},
            "cleanup": {"project_resources_remaining": 0},
        }
        receipt.update(updates)
        return receipt

    def test_passed_receipt_requires_same_process_and_no_owned_resources(self):
        tool.validate_receipt(self.receipt())
        with self.assertRaises(ValueError):
            tool.validate_receipt(self.receipt(same_process={"reconnected": False}))
        with self.assertRaises(ValueError):
            tool.validate_receipt(self.receipt(cleanup={"project_resources_remaining": 1}))
        with self.assertRaises(ValueError):
            tool.validate_receipt(self.receipt(assertions={"same_process_reconnected_without_restart": False}))

    def test_passed_receipt_requires_a_scanned_outage_tip(self):
        for height in (None, 266, "271"):
            with self.subTest(height=height), self.assertRaisesRegex(ValueError, "scan"):
                tool.validate_receipt(self.receipt(scenario={
                    "outbound": {"fully_synced_height": height}, "outage": {"node_tip": 271}}))

    def test_failed_receipt_may_record_an_unproven_assertion(self):
        tool.validate_receipt(self.receipt(status="failed", assertions={"uninterrupted_process_reconnect": False},
                                           same_process={"reconnected": False}))

    def test_non_regtest_or_unknown_case_is_rejected(self):
        with self.assertRaises(ValueError):
            tool.validate_receipt(self.receipt(network="zcash-testnet"))
        with self.assertRaises(ValueError):
            tool.validate_receipt(self.receipt(case="disconnect"))


if __name__ == "__main__":
    unittest.main()
