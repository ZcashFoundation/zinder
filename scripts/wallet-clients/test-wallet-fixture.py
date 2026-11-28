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
import tarfile
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

    def vizor_manifest(self, driver_digest, schema_version=2):
        engine = self.root / 'engine'
        engine.write_bytes(b'fixture build')
        provenance = {'vizor_release': 'v0.0.65', 'vizor_source': 'd0edb5b13890dd33d9bb93b5c4e50fd9973fa4e6',
                      'engine_sha256': hashlib.sha256(engine.read_bytes()).hexdigest()}
        if schema_version is not None:
            provenance['schema_version'] = schema_version
        if driver_digest is not None:
            provenance['driver_sha256'] = driver_digest
        (self.root / 'build.json').write_text(json.dumps(provenance))
        return subprocess.run([sys.executable, '-O', str(SCRIPTS / 'vizor-manifest.py'),
                               '--engine', 'engine', '--engine-provenance', 'build.json',
                               '--fixture', str(self.ready), '--candidate', str(self.candidate),
                               '--trace-tool', sys.executable, '--evidence', 'evidence', '--output', 'manifest'],
                              cwd=self.root, capture_output=True, text=True)

    def test_vizor_manifest_requires_build_time_driver_digest(self):
        manifest = self.root / 'manifest'
        driver = SCRIPTS / 'vizor-regtest.rs'
        digest = hashlib.sha256(driver.read_bytes()).hexdigest()
        legacy = self.vizor_manifest(digest, schema_version=None)
        self.assertNotEqual(legacy.returncode, 0)
        self.assertIn('rebuild the Vizor engine', legacy.stderr)
        self.assertFalse(manifest.exists())
        for invalid in [None, '0' * 64]:
            with self.subTest(digest=invalid):
                result = self.vizor_manifest(invalid)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('driver differs from its build provenance', result.stderr)
                self.assertFalse(manifest.exists())
        self.vizor_manifest(digest).check_returncode()
        case = json.loads(manifest.read_text())['cases']['vizor']
        self.assertEqual(case['endpoint'], self.overlay['client_endpoint'])
        self.assertEqual(case['inputs'][str(driver)], digest)
        helper = SCRIPTS / 'wallet_fixture.py'
        self.assertEqual(case['inputs'][str(helper)], hashlib.sha256(helper.read_bytes()).hexdigest())

    def test_vizor_build_records_only_the_driver_compiled_by_cargo(self):
        build = runpy.run_path(str(SCRIPTS / 'build-vizor-regtest.py'))['main']
        root_name = 'vizor-wallet-' + build.__globals__['SOURCE']
        source = self.root / root_name / 'rust'
        (source / 'examples').mkdir(parents=True)
        (source / 'Cargo.toml').touch()
        (source / 'Cargo.lock').touch()
        archive = self.root / 'source.tar.gz'
        with tarfile.open(archive, 'w:gz') as packed:
            packed.add(source.parent, arcname=root_name)
        checkout = self.root / 'vizor-regtest.rs'
        original = b'original driver'
        for edited in ('checkout', 'copied'):
            with self.subTest(edited=edited):
                checkout.write_bytes(original)
                output = self.root / edited
                copied = output / root_name / 'rust/examples/vizor-regtest.rs'

                def compile_example(*args, **kwargs):
                    self.assertEqual(copied.read_bytes(), original)
                    (checkout if edited == 'checkout' else copied).write_bytes(b'changed driver')
                    engine = output / 'target/debug/examples/vizor-regtest'
                    engine.parent.mkdir(parents=True)
                    engine.write_bytes(original)

                receipt = output / 'engine-source.json'
                argv = ['build-vizor-regtest.py', '--archive', str(archive), '--output', str(output)]
                old_umask = os.umask(0o077)
                try:
                    with patch.dict(build.__globals__, __file__=str(self.root / argv[0]),
                                    ARCHIVE_SHA256=hashlib.sha256(archive.read_bytes()).hexdigest()), \
                         patch.object(sys, 'argv', argv), patch.object(subprocess, 'run', side_effect=compile_example), \
                         patch.object(subprocess, 'check_output', return_value='rustc'):
                        if edited == 'copied':
                            with self.assertRaises(SystemExit), patch('sys.stderr', new_callable=io.StringIO):
                                build()
                            self.assertFalse(receipt.exists())
                        else:
                            build()
                            self.assertEqual(json.loads(receipt.read_text())['driver_sha256'],
                                             hashlib.sha256(original).hexdigest())
                finally:
                    os.umask(old_umask)

    def test_vizor_refuses_wrong_live_service_before_node_or_wallet_setup(self):
        run = runpy.run_path(str(SCRIPTS / 'vizor-regtest.py'))['run']
        self.release['compat_ops'] = 'http://127.0.0.1:11107'
        self.ready.write_text(json.dumps(self.release))
        self.overlay['ops_endpoint'] = 'http://127.0.0.1:11127'
        self.candidate.write_text(json.dumps(self.overlay))
        for candidate, fixture, revision, ops, endpoint in (
                (None, self.release, 'zinder_commit', 'compat_ops', 'lightwalletd'),
                (self.candidate, self.overlay, 'source_commit', 'ops_endpoint', 'client_endpoint')):
            health = {'service': 'zinder-compat-lightwalletd', 'network': 'zcash-regtest',
                      'git_commit': fixture[revision]}
            for field, wrong in [('git_commit', 'f' * 40), ('network', 'zcash-testnet'), ('service', 'zinder-query')]:
                with self.subTest(candidate=bool(candidate), field=field):
                    opener = Mock()
                    opener.open.return_value = io.BytesIO(json.dumps(dict(health, **{field: wrong})).encode())
                    args = SimpleNamespace(fixture=self.ready, candidate=candidate, endpoint=fixture[endpoint])
                    with patch.dict(run.__globals__, build_opener=Mock(return_value=opener)), \
                         self.assertRaisesRegex(ValueError, 'live service identity'):
                        run(args)
                    opener.open.assert_called_once_with(fixture[ops] + '/healthz', timeout=5)

    def test_vizor_accepts_ready_zinder_supervisor_and_rejects_unready_role(self):
        run = runpy.run_path(str(SCRIPTS / 'vizor-regtest.py'))['run']
        self.release['compat_ops'] = 'http://127.0.0.1:11108'
        self.ready.write_text(json.dumps(self.release))
        revision = self.release['zinder_commit']
        health = {'service': 'zinder', 'status': 'alive', 'network': 'zcash-regtest',
                  'git_commit': revision}
        ready_bad = {'status': 'ready', 'roles': [{'role': 'compat-lightwalletd', 'ready': False, 'state': 'not_ready'}]}

        def opener_for(payloads):
            opener = Mock()
            opener.open.side_effect = [io.BytesIO(json.dumps(p).encode()) for p in payloads]
            return opener

        args = SimpleNamespace(fixture=self.ready, candidate=None, endpoint=self.release['lightwalletd'])
        with patch.dict(run.__globals__, build_opener=Mock(return_value=opener_for([health, ready_bad]))), \
             self.assertRaisesRegex(ValueError, 'live service identity'):
            run(args)

        ready_ok = {'status': 'ready', 'roles': [{'role': 'compat-lightwalletd', 'ready': True, 'state': 'ready'}]}
        opener_ok = opener_for([health, ready_ok])
        with patch.dict(run.__globals__, build_opener=Mock(return_value=opener_ok)), \
             self.assertRaises(StopIteration):
            run(args)
        opener_ok.open.assert_any_call(self.release['compat_ops'] + '/healthz', timeout=5)
        opener_ok.open.assert_any_call(self.release['compat_ops'] + '/readyz', timeout=5)

    def test_vizor_rejects_unbound_candidate_before_wallet_or_node_access(self):
        run = runpy.run_path(str(SCRIPTS / 'vizor-regtest.py'))['run']
        digest = hashlib.sha256((SCRIPTS / 'vizor-regtest.rs').read_bytes()).hexdigest()
        for override in ({'shared_release_dependencies': 'f' * 40}, {'source_commit': ''},
                         {'binary_sha256': ''}, {'client_endpoint': self.release['lightwalletd']}):
            with self.subTest(override=override):
                candidate = dict(self.overlay, **override)
                self.candidate.write_text(json.dumps(candidate))
                result = self.vizor_manifest(digest)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('candidate', result.stderr)
                self.assertFalse((self.root / 'manifest').exists())
                args = SimpleNamespace(fixture=self.ready, candidate=self.candidate,
                                       endpoint=candidate['client_endpoint'])
                with self.assertRaises(ValueError):
                    run(args)

    def test_zingo_retries_incomplete_scan_but_propagates_client_and_trace_failures(self):
        wait = runpy.run_path(str(SCRIPTS / 'zingo-regtest.py'))['wait_for_scan']
        ready = {'scan_ranges': [{'priority': 'Scanned', 'end_block': 8}]}
        for failure in (RuntimeError('client sync error'), RuntimeError('rejected endpoint trace'),
                        ValueError('invalid trace JSON')):
            with self.subTest(error=str(failure)):
                read = Mock(side_effect=[failure, json.dumps(ready)])
                with self.assertRaises(type(failure)) as error:
                    wait(read, 8)
                self.assertIs(error.exception, failure)
                read.assert_called_once_with()
        read = Mock(side_effect=['', '{"scan_ranges":[]}', json.dumps(ready)])
        with patch.object(time, 'sleep'):
            self.assertEqual(wait(read, 8), ready)
        self.assertEqual(read.call_count, 3)

    def test_zingo_requires_confirmed_notes_for_every_new_shielding_transaction(self):
        confirm = runpy.run_path(str(SCRIPTS / 'zingo-regtest.py'))['confirm_shielding_notes']
        old, first, second = ('a' * 64, 'b' * 64, 'c' * 64)
        tx_heights = {old: 10, first: 12, second: 13}

        def rpc(method, params):
            if method == 'getblockhash':
                return f'{params[0]:064x}'
            self.assertEqual(method, 'getrawtransaction')
            txid, verbose = params
            self.assertEqual(verbose, 1)
            height = tx_heights[txid]
            return {'txid': txid, 'height': height, 'blockhash': f'{height:064x}', 'confirmations': 1}

        def notes(*items):
            rows = [{'txid': txid, 'status': f'confirmed at block height {height}', 'value': 1}
                    for txid, height in items]
            return {'ironwood_notes': {'note_summaries': rows}, 'orchard_notes': {'note_summaries': []},
                    'sapling_notes': {'note_summaries': []}}

        for ids, rows in (([first], [(old, 10)]), ([first], [(first, 13)]),
                          ([old], [(old, 10)]), ([first, second], [(first, 12)])):
            with self.subTest(ids=ids, rows=rows), self.assertRaises(RuntimeError):
                confirm(ids, notes(*rows), 11, rpc)
        self.assertEqual(confirm([first, second], notes(*tx_heights.items()), 11, rpc),
                         {first: 12, second: 13})

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
            except (FileNotFoundError, ProcessLookupError):
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

    def test_native_expiry_selects_unified_serve_or_legacy_query_container(self):
        lookup = runpy.run_path(str(SCRIPTS / 'native-expiry.py'))['owned_query_container_id']
        project = self.root / 'project'
        container_id = 'a' * 64
        compose = Mock(return_value=container_id)
        unified = {'health': {'index': {}, 'serve': {}}}
        legacy = {'health': {'query': {}}}

        self.assertEqual(lookup(compose, project, unified), container_id)
        compose.assert_called_once_with(project, 'ps', '-q', 'zinder-serve')

        compose.reset_mock()
        self.assertEqual(lookup(compose, project, legacy), container_id)
        compose.assert_called_once_with(project, 'ps', '-q', 'zinder-query')

        for bad_output in ('', 'short', 'g' * 64):
            with self.subTest(bad_output=bad_output):
                bad_compose = Mock(return_value=bad_output)
                with self.assertRaisesRegex(ValueError, 'missing owned query container'):
                    lookup(bad_compose, project, unified)


if __name__ == '__main__':
    unittest.main()
