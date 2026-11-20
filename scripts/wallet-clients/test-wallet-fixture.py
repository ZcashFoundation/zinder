#!/usr/bin/env python3
"""Exercise the Zingo and Zkool manifest's fixture and input bindings."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from wallet_fixture import binding


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
