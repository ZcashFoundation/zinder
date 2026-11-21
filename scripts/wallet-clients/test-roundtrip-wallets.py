#!/usr/bin/env python3
"""Check normalized wallet observations and durable identity guards."""
import io
import json
import signal
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.error import HTTPError

from roundtrip_wallets import WalletRpcError, Zallet, Zingo, pin_directory, write_private, zingo_transactions


class WalletObservations(unittest.TestCase):
    def setUp(self):
        self.native = Zallet.__new__(Zallet)
        self.native.process = None
        self.native.warmup_timeout = 12

    def configure_test_child(self, root):
        self.native.directory = root
        self.native.binary = str(Path(sys.executable).resolve())
        self.native.argv = [self.native.binary, "-I", "-S", "-c", "import signal; signal.pause()"]
        self.native._rpc = Mock(return_value={})

    def test_process_identity_visible_only_after_delayed_exec(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.configure_test_child(root)
            gate = root / "accept-exec"
            marker = root / "daemon.pid"
            write_private(marker, '{"pid": 2147483647, "start_ticks": 42}')
            real_popen = subprocess.Popen
            real_start = self.native._process_start
            wrapper = ("import os,sys,time; from pathlib import Path\n"
                       "while not Path(sys.argv[1]).exists(): time.sleep(.01)\n"
                       "os.execv(sys.argv[2], sys.argv[2:])")

            def spawn(argv, **options):
                return real_popen([self.native.binary, "-I", "-S", "-c", wrapper, str(gate), *argv], **options)

            def inspect(pid):
                start = real_start(pid)
                if pid != 2147483647 and start is None:
                    gate.touch()
                return start

            with patch("roundtrip_wallets.subprocess.Popen", side_effect=spawn), \
                 patch.object(self.native, "_process_start", side_effect=inspect):
                try:
                    self.native._start()
                    child = self.native.process
                    self.assertTrue(gate.is_file())
                    self.assertEqual(self.native._owned_identity(), (child.pid, real_start(child.pid)))
                    self.native._rpc.assert_called_once_with("getwalletstatus")
                finally:
                    if self.native.process is not None:
                        self.native.process.terminate()
                        self.native.process.wait(timeout=5)

    def test_unverified_new_child_is_reaped_and_prior_record_is_preserved(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.configure_test_child(root)
            marker = root / "daemon.pid"
            stale = '{"pid": 2147483647, "start_ticks": 42}'
            write_private(marker, stale)
            real_popen = subprocess.Popen
            children = []

            def spawn(argv, **options):
                children.append(real_popen(argv, **options))
                return children[-1]

            with patch("roundtrip_wallets.subprocess.Popen", side_effect=spawn), \
                 patch.object(self.native, "_process_start", return_value=None), \
                 patch("roundtrip_wallets.time.monotonic", side_effect=[10, 16]), \
                 self.assertRaisesRegex(TimeoutError, "expected process identity"):
                self.native._start()
            self.assertIsNotNone(children[0].returncode)
            self.assertIsNone(self.native.process)
            self.assertEqual(marker.read_text(), stale)
            self.native._rpc.assert_not_called()

    def test_record_and_rpc_failures_reap_only_this_start_attempt(self):
        for fail_at in ("_record_identity", "_rpc"):
            with self.subTest(fail_at=fail_at), tempfile.TemporaryDirectory() as temp:
                self.configure_test_child(Path(temp))
                real_popen = subprocess.Popen
                children = []

                def spawn(argv, **options):
                    children.append(real_popen(argv, **options))
                    return children[-1]

                failure = PermissionError("cannot record") if fail_at == "_record_identity" else WalletRpcError("getwalletstatus", {"code": -13})
                with patch("roundtrip_wallets.subprocess.Popen", side_effect=spawn), \
                     patch.object(self.native, fail_at, side_effect=failure), self.assertRaises(type(failure)) as error:
                    self.native._start()
                self.assertIs(error.exception, failure)
                self.assertIsNotNone(children[0].returncode)
                self.assertIsNone(self.native.process)

        self.native._owned_identity = Mock(return_value=(12345, 678))
        self.native._rpc = Mock(side_effect=WalletRpcError("getwalletstatus", {"code": -13}))
        with patch("roundtrip_wallets.subprocess.Popen") as launch, self.assertRaises(WalletRpcError):
            self.native._start()
        launch.assert_not_called()

    def test_identity_replaced_during_authentication_keeps_the_other_child(self):
        with tempfile.TemporaryDirectory() as temp:
            self.configure_test_child(Path(temp))
            with subprocess.Popen([*self.native.argv, "start"]) as other:
                try:
                    other_identity = self.native._new_child_identity(other)

                    def replaced(method):
                        self.assertEqual(method, "getwalletstatus")
                        self.native._record_identity(*other_identity)
                        return {}
                    self.native._rpc = replaced
                    with self.assertRaisesRegex(RuntimeError, "changed during startup"):
                        self.native._start()
                    self.assertIsNone(self.native.process)
                    self.assertIsNone(other.poll())
                    self.assertEqual(self.native._owned_identity(), other_identity)
                finally:
                    other.terminate()

    def test_exited_child_does_not_wait_for_its_pid_to_reappear(self):
        child = Mock(returncode=17)
        child.poll.return_value = 17
        self.native._process_start = Mock()
        with self.assertRaisesRegex(RuntimeError, "exited with status 17"):
            self.native._new_child_identity(child)
        self.native._process_start.assert_not_called()

    def test_json_rpc_error_preserves_code_on_http_failure(self):
        self.native.rpc_url = "http://127.0.0.1:12345"
        self.native.auth = "fixture"
        self.native.opener = Mock()
        for code in (-10, -32601):
            response = io.BytesIO(json.dumps({"error": {"code": code, "message": "fixture"}}).encode())
            self.native.opener.open.side_effect = HTTPError(self.native.rpc_url, 500, "fixture", {}, response)
            with self.assertRaises(WalletRpcError) as error:
                self.native._rpc("test")
            self.assertEqual((error.exception.method, error.exception.code), ("test", code))
        self.native.opener.open.side_effect = HTTPError(self.native.rpc_url, 401, "denied", {}, io.BytesIO(b"denied"))
        with self.assertRaises(HTTPError):
            self.native._rpc("test")

    def test_snapshot_waits_only_for_wallet_catchup(self):
        locked = WalletRpcError("z_getbalanceforaccount", {"code": -10})
        expected = {"height": 12, "spendable_zat": 200}
        self.native._snapshot_once = Mock(side_effect=[locked, expected])
        with patch("roundtrip_wallets.time.sleep") as sleep:
            self.assertEqual(self.native.snapshot(), expected)
            sleep.assert_called_once()
        self.native._snapshot_once = Mock(side_effect=WalletRpcError("z_listtransactions", {"code": -5}))
        with self.assertRaises(WalletRpcError) as other:
            self.native.snapshot()
        self.assertEqual(other.exception.code, -5)
        self.native._snapshot_once.assert_called_once()
        self.native._snapshot_once = Mock(side_effect=locked)
        with patch("roundtrip_wallets.time.monotonic", side_effect=[1, 13]), self.assertRaises(TimeoutError) as err:
            self.native.snapshot()
        self.assertIs(err.exception.__cause__, locked)

    def test_unsupported_stop_signals_verified_pidfd(self):
        self.native._rpc = Mock(side_effect=WalletRpcError("stop", {"code": -32601}))
        identity = (1234, 5678)
        self.native._owned_identity = Mock(side_effect=[identity, identity, None, None])
        with patch("roundtrip_wallets.os.pidfd_open", return_value=42) as op, \
             patch("roundtrip_wallets.os.close") as close, patch("roundtrip_wallets.signal.pidfd_send_signal") as send:
            self.native.close()
            op.assert_called_once_with(1234)
            send.assert_called_once_with(42, signal.SIGTERM)
            close.assert_called_once_with(42)
        self.native._owned_identity = Mock(side_effect=[identity, (1234, 10000)])
        with patch("roundtrip_wallets.os.pidfd_open", return_value=42), patch("roundtrip_wallets.os.close"), \
             patch("roundtrip_wallets.signal.pidfd_send_signal") as send, self.assertRaisesRegex(RuntimeError, "changed"):
            self.native.close()
        send.assert_not_called()

    def test_other_rpc_shutdown_errors_do_not_signal(self):
        self.native._owned_identity = Mock(return_value=(1234, 5678))
        self.native._rpc = Mock(side_effect=WalletRpcError("stop", {"code": -13}))
        with patch("roundtrip_wallets.os.pidfd_open") as op, self.assertRaises(WalletRpcError):
            self.native.close()
        op.assert_not_called()

    def test_legacy_pid_is_bound_to_start_time_after_authentication(self):
        with tempfile.TemporaryDirectory() as temp:
            self.native.directory = Path(temp)
            marker = self.native.directory / "daemon.pid"
            write_private(marker, "1234")
            self.native._process_start = Mock(return_value=5678)
            self.native._rpc = Mock(return_value={})
            self.native._start()
            self.native._rpc.assert_called_once_with("getwalletstatus")
            self.assertEqual(json.loads(marker.read_text()), {"pid": 1234, "start_ticks": 5678})
            self.native._process_start.return_value = 5679
            self.assertIsNone(self.native._owned_identity())

    def test_fee_and_memo_are_bound_to_transaction(self):
        first, second = "01" * 32, "02" * 32
        records = ""
        for txid, status, kind, amount, fee in [(first, "confirmed", "received", 12345, "not available"),
                                                 (second, "transmitted", "send", 3600, "10000")]:
            records += (f"\n{{\n    txid: {txid}\n    datetime: 2026\n    status: {status}\n    blockheight: 22\n"
                        f"    kind: {kind}\n    value: {amount}\n    fee: {fee}\n}}")
        rows = zingo_transactions(records, {"value_transfers": [{"txid": first, "memos": ["receipt"]}]})
        self.assertEqual([(r["height"], r["pending"], r["amount_zat"], r["fee_zat"], r["memos"]) for r in rows],
                         [(22, False, 12345, None, ["receipt"]), (None, True, 3600, 10000, [])])

    def test_pinned_wallet_rejects_a_different_server_or_chain(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            pin_directory(root, "regtest", "http://127.0.0.1:12000")
            for network, endpoint in (("regtest", "http://127.0.0.1:12001"), ("testnet", "http://127.0.0.1:12000")):
                with self.assertRaisesRegex(RuntimeError, "differs"):
                    pin_directory(root, network, endpoint)

    def test_pinned_address_blocks_silent_recreation_after_wallet_loss(self):
        with tempfile.TemporaryDirectory() as temp:
            w = Zingo("/missing-zingo", temp, "testnet", "http://127.0.0.1:12000", 150)
            write_private(Path(temp) / "receiving-address", "previously-recorded")
            with self.assertRaisesRegex(RuntimeError, "wallet file missing"):
                w.prepare()

    def test_atomic_backup_is_private_and_preserves_bytes(self):
        with tempfile.TemporaryDirectory() as temp:
            p = Path(temp) / "wallet"
            for value in (b"initial", b"\0replacement"):
                write_private(p, value)
                self.assertEqual(p.read_bytes(), value)
                self.assertEqual(p.stat().st_mode & 0o777, 0o600)


if __name__ == "__main__":
    unittest.main()
