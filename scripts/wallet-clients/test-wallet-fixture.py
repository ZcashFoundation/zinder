#!/usr/bin/env python3
"""Exercise wallet fixture binding, service readiness, and process cleanup."""
import hashlib
import io
import json
import os
from pathlib import Path
import runpy
import signal
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError
from wallet_fixture import binding


SCRIPTS = Path(__file__).resolve().parent


class FixtureTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.release = {"network": "zcash-regtest", "zinder_commit": "1" * 40,
                        "node_rpc": "http://127.0.0.1:11132", "lightwalletd": "http://127.0.0.1:11167",
                        "current_tip": 10, "current_hash": "a" * 64}
        self.ready = self.root/"ready.initial.json"
        self.ready.write_text(json.dumps(self.release))
        self.overlay = {"network": "zcash-regtest", "node_rpc": self.release["node_rpc"],
                        "shared_release_dependencies": self.release["zinder_commit"],
                        "client_endpoint": "http://127.0.0.1:11187", "binary_sha256": "2" * 64,
                        "source_commit": "3" * 40}
        self.candidate = self.root/"candidate.json"
        self.candidate.write_text(json.dumps(self.overlay))

    def test_release_and_candidate_share_only_the_node_and_anchor(self):
        release = binding(self.ready)
        candidate = binding(self.ready, self.candidate)
        self.assertEqual(release[1:], candidate[1:])
        self.assertEqual(release[0], self.release["lightwalletd"])
        self.assertEqual(candidate[0], self.overlay["client_endpoint"])

    def test_mismatched_requested_endpoint_or_node_is_rejected(self):
        with self.assertRaises(ValueError):
            binding(self.ready, self.candidate, endpoint=self.release["lightwalletd"])
        with self.assertRaises(ValueError):
            binding(self.ready, node="http://127.0.0.1:22232")

    def test_cross_fixture_overlay_is_rejected(self):
        for override in ({"node_rpc": "http://127.0.0.1:22232"},
                         {"shared_release_dependencies": "f"*40}, {"network": "zcash-testnet"}):
            self.candidate.write_text(json.dumps(dict(self.overlay, **override)))
            with self.assertRaises(ValueError):
                binding(self.ready, self.candidate)

    def test_cake_rejects_reused_node_port_before_wallet_setup(self):
        suite = runpy.run_path(str(SCRIPTS / 'cake-regtest.py'))['run_suite']
        args = SimpleNamespace(run=self.root / 'run', engine=Path(sys.executable), trace=Path(sys.executable),
                               fixture=self.ready, candidate=self.candidate,
                               node=self.release['node_rpc'], endpoint=self.overlay['client_endpoint'])
        node = Mock(return_value='b' * 64)
        with patch.dict(suite.__globals__, node=node), self.assertRaisesRegex(ValueError, 'block hash mismatch'):
            suite(args)
        node.assert_called_once_with(args.node, 'getblockhash', self.release['current_tip'])
        self.assertFalse((args.run / 'jwt.pub').exists())

    def test_devtool_rejects_reused_node_port_before_wallet_setup(self):
        wallet_class = runpy.run_path(str(SCRIPTS / 'devtool-regtest.py'))['Wallet']
        args = SimpleNamespace(scratch=self.root, evidence=self.root / 'evidence', step='bootstrap',
                               ready=self.ready, candidate=None, node=self.release['node_rpc'],
                               endpoint=self.release['lightwalletd'])
        wallet = wallet_class(args)
        with patch.object(wallet, 'rpc', return_value='b' * 64) as rpc:
            with self.assertRaisesRegex(ValueError, 'block hash mismatch'):
                wallet.verify_chain()
        rpc.assert_called_once_with('getblockhash', [self.release['current_tip']])
        self.assertFalse(wallet.source.exists())

    def test_vizor_retries_only_temporary_unavailability(self):
        serving = runpy.run_path(str(SCRIPTS / 'vizor-regtest.py'))['serving_at_tip']
        url = 'http://127.0.0.1:11106'

        def error(code):
            return HTTPError(url + '/readyz', code, 'fixture', {}, io.BytesIO())

        opener = Mock()
        opener.open.side_effect = [error(503), io.BytesIO(b'{"status":"ready","current_height":10}'), error(403)]
        self.assertFalse(serving(opener, url, 10))
        self.assertTrue(serving(opener, url, 10))
        with self.assertRaises(HTTPError) as raised:
            serving(opener, url, 10)
        self.assertEqual(raised.exception.code, 403)
        raised.exception.close()

    def test_wallet_children_remain_in_the_lifecycle_process_group(self):
        for wallet in ('zingo', 'zkool'):
            with self.subTest(wallet=wallet):
                self.check_wallet_process_group(wallet)

    def check_wallet_process_group(self, wallet):
        marker = self.root / (wallet + '-pids')
        client = self.root / (wallet + '-probe')
        client.write_text(
            f'#!{sys.executable}\nimport os, subprocess, time\nfrom pathlib import Path\n'
            'child = subprocess.Popen(["sleep", "30"])\n'
            f'marker = Path({str(marker)!r})\n'
            'marker.with_suffix(".tmp").write_text(f"{os.getpid()} {child.pid}")\n'
            'marker.with_suffix(".tmp").replace(marker)\n'
            'child.wait()\n'
        )
        client.chmod(0o700)
        # The first bootstrap invocation runs the offline wallet in Zingo and the tracer in Zkool.
        command = [sys.executable, '-c',
                   "import sys, runpy, wallet_fixture; wallet_fixture.verify_node = lambda *args: None; "
                   "sys.argv.pop(0); runpy.run_path(sys.argv[0], run_name='__main__')",
                   str(SCRIPTS / (wallet + '-regtest.py')), '--binary', str(client),
                   '--trace-tool', str(client), '--step', 'bootstrap', '--ready', str(self.ready),
                   '--endpoint', self.release['lightwalletd'], '--node', self.release['node_rpc'],
                   '--fence-python', '/bin/true', '--proto-dir', str(self.root),
                   '--evidence-dir', str(self.root / (wallet + '-evidence')),
                   '--scratch', str(self.root / (wallet + '-scratch')), '--report', str(self.root / 'report')]
        if wallet == 'zkool':
            command.extend(['--variant', 'internal-enabled'])
        adapter = subprocess.Popen(command, cwd=SCRIPTS, start_new_session=True)
        pids = []

        def alive(pid):
            try:
                return Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()[0] != 'Z'
            except FileNotFoundError:
                return False

        try:
            deadline = time.monotonic() + 5
            while not marker.exists() and adapter.poll() is None and time.monotonic() < deadline:
                time.sleep(.02)
            pids = [int(pid) for pid in marker.read_text().split()]
            self.assertEqual(len(pids), 2)
            self.assertEqual([os.getpgid(pid) for pid in pids], [adapter.pid] * 2)
            os.killpg(adapter.pid, signal.SIGKILL)
            adapter.wait(timeout=5)
            deadline = time.monotonic() + 5
            while any(map(alive, pids)) and time.monotonic() < deadline:
                time.sleep(.02)
            self.assertFalse(any(map(alive, pids)))
        finally:
            if adapter.poll() is None:
                os.killpg(adapter.pid, signal.SIGKILL)
            adapter.wait(timeout=5)
            for pid in filter(alive, pids):
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass

    def test_generated_input_paths_and_caller_supplied_digest(self):
        self.root.joinpath('proto').mkdir()
        for file in ('service_pb2.py', 'compact_formats_pb2.py', 'service_pb2_grpc.py'):
            (self.root/'proto'/file).touch()
        client = self.root/'client'
        client.write_bytes(b'fixture')
        command = [sys.executable, str(Path(__file__).resolve().with_name('make-manifest.py')),
                   '--wallet', 'zingo', '--binary', 'client', '--trace-tool', 'client',
                   '--fence-python', sys.executable, '--proto-dir', 'proto', '--ready', 'ready.initial.json',
                   '--candidate', 'candidate.json', '--evidence-dir', 'evidence', '--output', 'manifest',
                   '--binary-sha256']
        result = subprocess.run(command+['0'*64], cwd=self.root, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.root/'manifest').exists())
        subprocess.run(command+[hashlib.sha256(client.read_bytes()).hexdigest()], cwd=self.root, check=True)
        case = json.loads((self.root/'manifest').read_text())['cases']['zingo-nakednet-cli']
        self.assertTrue(all(Path(p).is_absolute() for p in case['inputs']))
        self.assertEqual(case['endpoint'], self.overlay['client_endpoint'])
        for key in ('--ready', '--candidate'):
            self.assertIn(key, case['steps']['bootstrap']['command'])


if __name__ == '__main__':
    unittest.main()
