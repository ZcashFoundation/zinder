#!/usr/bin/env python3
"""Test fixture routing and cleanup, with optional Docker smoke checks."""
import argparse
import asyncio
import concurrent.futures
import json
import os
from pathlib import Path
import runpy
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, call, patch

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / 'scripts/regtest-fixture.py'
RELAY = ROOT / 'scripts/regtest-fixture-relay.py'
operator = runpy.run_path(str(FIXTURE))['start_relay'].__globals__
relay = runpy.run_path(str(RELAY))['main'].__globals__


class FixtureTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which('bash') and shutil.which('jq'), 'requires Bash and jq')
    def test_mining_with_crlf_height_and_tree_size_output(self):
        shell = r'''
set -euo pipefail
jq() {
  command jq "$@" | while IFS= read -r line; do printf '%s\r\n' "$line"; done
}
curl() {
  case "${@: -2:1}" in
    *'"method":"getblockcount"'*)
      printf '{"result":%s}\n' "$(<"$MINED_TIP")"
      ;;
    *'"method":"generatetoaddress"'*)
      printf '10\n' > "$MINED_TIP"
      printf '{"result":[]}\n'
      ;;
    *'"method":"getblock"'*)
      printf '{"result":{"trees":{"sapling":{"size":2},"orchard":{"size":3},"ironwood":{"size":4}}}}\n'
      ;;
    *'"method":"generate","params":[2]'*)
      printf '12\n' > "$MINED_TIP"
      printf '{"result":[]}\n'
      ;;
    *) return 1 ;;
  esac
}
source "$1" http://127.0.0.1:1 12
'''
        script_bytes = (ROOT / 'scripts/mine-live-regtest.sh').read_bytes().replace(b'\r\n', b'\n')
        for line_endings, newline in [('LF', b'\n'), ('CRLF', b'\r\n')]:
            with self.subTest(line_endings=line_endings), tempfile.TemporaryDirectory() as scratch:
                checkout_bytes = script_bytes.replace(b'\n', newline)
                mining_script = Path(scratch) / 'mine-live-regtest.sh'
                mining_script.write_bytes(checkout_bytes.replace(b'\r\n', b'\n'))
                tip_path = Path(scratch) / 'tip'
                tip_path.write_text('0')
                output = operator['run'](
                    shutil.which('bash'), '-c', shell, 'test-mining', mining_script.as_posix(),
                    env=dict(os.environ, MINED_TIP=tip_path.as_posix()), timeout=10)
                self.assertEqual(tip_path.read_text().strip(), '12')
                self.assertEqual(output.splitlines(), [
                    'shielded prefix: Sapling 2, Orchard 3, Ironwood 4 notes at height 10',
                    'regtest tip: 12',
                ])

    def test_successful_miner_below_requested_tip_does_not_start_indexer(self):
        model = {'services': {
            'state-init': {},
            'zinder-index': {'environment': {}, 'healthcheck': {'test': ['CMD']}},
            'zinder-serve': {'environment': {}, 'healthcheck': {}},
        }}
        with tempfile.TemporaryDirectory() as scratch:
            directory = Path(scratch) / 'fixture'
            images = Path(scratch) / 'images.json'
            images.write_text('{"zinder":"test-image"}')
            args = SimpleNamespace(node_config=ROOT / 'deploy/zebra/regtest.toml', port_base=34100,
                                   project=directory.name, images=images, subnet_pool='10.253.0.0/16',
                                   bin_dir=None, raw_blob_policy='none', revision='0' * 40, tip=12, timeout=1)
            compose = Mock(side_effect=['', 'node-id'])
            cleanup = Mock()
            with (patch.object(socket, 'socket'), patch.object(shutil, 'which', return_value='test-command'),
                  patch.dict(operator, compose=compose, tear_down_fixture=cleanup, image_id=Mock(return_value='image'),
                             direct_bridge_access=Mock(return_value=True), fixture_subnets=Mock(return_value=[]),
                             run=Mock(side_effect=['', '', '', json.dumps(model), 'regtest tip: 0']),
                             start_relay=Mock(), wait=Mock(), rpc=Mock(return_value=0))):
                with self.assertRaisesRegex(RuntimeError, 'node reached height 0. Expected height 12'):
                    operator['provision'](args, directory)
            self.assertEqual(compose.call_args_list, [call(directory, 'up', '-d', 'node'),
                                                     call(directory, 'ps', '-q', 'node')])
            cleanup.assert_called_once()

    def test_cleanup_stops_relay_when_docker_is_unavailable(self):
        with tempfile.TemporaryDirectory() as scratch:
            directory = Path(scratch)
            (directory / 'ready.json').touch()
            state = {'owner': 'test-owner'}
            cleanup = Mock()
            cleanup.verify.side_effect = OSError('cannot contact Docker')
            with patch.dict(operator, verify_owner=cleanup.verify, compose=cleanup.compose, stop_relay=cleanup.stop):
                with self.assertRaisesRegex(OSError, 'cannot contact Docker'):
                    operator['tear_down_fixture'](directory, state, collect_logs=True)
            self.assertFalse((directory / 'ready.json').exists())
            self.assertEqual(cleanup.mock_calls, [call.verify(state), call.stop(directory, state)])

    def test_failed_log_collection_still_stops_relay_and_removes_owned_project(self):
        with tempfile.TemporaryDirectory() as scratch:
            directory = Path(scratch)
            state = {'owner': 'test-owner'}
            cleanup = Mock()
            cleanup.compose.side_effect = [OSError('cannot collect logs'), '']
            with patch.dict(operator, verify_owner=cleanup.verify, compose=cleanup.compose, stop_relay=cleanup.stop):
                operator['tear_down_fixture'](directory, state, collect_logs=True)
            self.assertEqual(cleanup.mock_calls, [
                call.verify(state), call.compose(directory, 'logs', '--no-color', '--tail', '80'),
                call.stop(directory, state), call.compose(directory, 'down', '-v', '--remove-orphans'),
            ])

    def test_subprocess_preserves_lf_on_binary_stdin(self):
        script = b'first line\nsecond line\n'
        received = operator['run'](sys.executable, '-c', 'import sys; print(sys.stdin.buffer.read().hex())',
                                   stdin_bytes=script)
        self.assertEqual(received, script.hex())

    def test_host_route_selection(self):
        for platform, engine, expected in [
            ('linux', 'Ubuntu', True), ('linux', 'Docker Desktop', False),
            ('darwin', None, False), ('win32', None, False),
        ]:
            with (self.subTest(platform=platform, engine=engine),
                  patch.object(sys, 'platform', platform), patch.dict(operator, run=Mock(return_value=engine))):
                self.assertEqual(operator['direct_bridge_access'](), expected)
                self.assertEqual(operator['run'].call_count, int(engine is not None))

    def test_desktop_targets_stay_inside_the_node_network(self):
        state = dict(port_base=34100)
        with patch.dict(operator, run=Mock(side_effect=AssertionError('unexpected Docker lookup'))):
            targets = operator['relay_targets'](state, serving=True, direct_bridge=False)
        self.assertEqual(targets, [
            [34132, '127.0.0.1', 39232], [34102, 'zinder-index', 9102], [34105, 'zinder-index', 9105],
            [34108, 'zinder-index', 9108], [34167, 'zinder-index', 9067],
        ])

    def test_linux_targets_use_only_owned_bridge_addresses(self):
        state = dict(port_base=34100, project='fixture', owner='owner')

        def inspect(address):
            return json.dumps([dict(NetworkSettings=dict(Networks={'fixture_node': dict(IPAddress=address)}))])

        with patch.dict(operator, run=Mock(side_effect=[
                'node-id', inspect('10.0.0.2'), 'index-id', inspect('10.0.0.3')])):
            targets = operator['relay_targets'](state, serving=True, direct_bridge=True)
            self.assertEqual(operator['run'].call_count, 4)
            for lookup in operator['run'].call_args_list[::2]:
                self.assertIn('label=io.zinder.fixture.owner=owner', lookup.args)
        self.assertEqual(targets[0], [34132, '10.0.0.2', 39232])
        self.assertEqual([target[1] for target in targets[1:]], ['10.0.0.3'] * 4)

    def test_desktop_subnet_selection_needs_only_docker(self):
        with patch.dict(operator, run=Mock(side_effect=[
                'network-id', '[{"Subnet":"10.253.0.0/28"}]\n[]'])):
            subnets = operator['fixture_subnets']('10.253.0.0/26', direct_bridge=False)
            self.assertEqual(operator['run'].call_count, 2)
        self.assertEqual(len(set(subnets)), 2)
        self.assertNotIn('10.253.0.0/28', subnets)

    def test_relay_starts_detached_on_each_host(self):
        for platform, direct_bridge, launch_options in [
            ('linux', True, dict(start_new_session=True)),
            ('darwin', False, dict(start_new_session=True)),
            ('win32', False, dict(creationflags=8)),
        ]:
            with (self.subTest(platform=platform), tempfile.TemporaryDirectory() as scratch,
                  patch.object(sys, 'platform', platform),
                  patch.object(subprocess, 'DETACHED_PROCESS', 8, create=True),
                  patch.object(subprocess, 'Popen', return_value=SimpleNamespace(pid=42)) as launch,
                  patch.dict(operator, relay_targets=Mock(return_value=[]), wait=Mock())):
                directory = Path(scratch)
                state = dict(node_container='node-id', owner='owner')
                operator['start_relay'](directory, state, direct_bridge=direct_bridge)
                self.assertEqual(state['relay_pid'], 42)
                for option, setting in launch_options.items():
                    self.assertEqual(launch.call_args.kwargs[option], setting)
                routes = json.loads((directory / 'relay.json').read_text())
                self.assertEqual(routes['exec_container'], None if direct_bridge else 'node-id')

    def test_portable_shutdown_authenticates_and_stops_the_relay(self):
        with tempfile.TemporaryDirectory() as scratch:
            directory = Path(scratch)
            routes = directory / 'relay.json'
            control_path = directory / 'relay-control.json'
            routes.write_text(json.dumps(dict(targets=[], exec_container=None)))
            process = subprocess.Popen([sys.executable, str(RELAY), str(routes), 'test-owner'])
            try:
                operator['wait'](control_path.exists, bool, 5)
                state = dict(relay_pid=process.pid, owner='test-owner')
                control = json.loads(control_path.read_text())
                with socket.create_connection(('127.0.0.1', control['port']), timeout=2) as peer:
                    peer.sendall(b'0' * len(control['token']))
                    self.assertEqual(peer.recv(1), b'')
                with patch.dict(operator, os=SimpleNamespace()):
                    operator['stop_relay'](directory, state)
                    operator['stop_relay'](directory, state)
                self.assertEqual(process.wait(timeout=5), 0)
                self.assertFalse(control_path.exists())
            finally:
                if process.poll() is None:
                    process.kill()
                process.wait(timeout=5)


class RelayTests(unittest.IsolatedAsyncioTestCase):
    async def test_forward_and_control_connections_have_separate_limits(self):
        for exec_container in (None, 'node-id'):
            with self.subTest(exec_container=exec_container), tempfile.TemporaryDirectory() as scratch:
                routes = Path(scratch) / 'relay.json'
                control_path = routes.with_name('relay-control.json')
                routes.write_text(json.dumps(dict(targets=[[0, '127.0.0.1', 1]], exec_container=exec_container)))
                listeners_ready = asyncio.Event()
                accept_connections = []
                server = Mock(wait_closed=AsyncMock())
                server.sockets = [Mock(getsockname=Mock(return_value=('127.0.0.1', 12345)))]

                async def listen(accept, _host, _port):
                    accept_connections.append(accept)
                    if len(accept_connections) == 2:
                        listeners_ready.set()
                    return server

                forwarding_started = asyncio.Event()

                async def hold_stream(*_args):
                    forwarding_started.set()
                    await asyncio.Future()

                forward = AsyncMock(side_effect=hold_stream)
                with (patch.dict(relay, MAX_FORWARD_STREAMS=1, MAX_CONTROL_CONNECTIONS=2, forward=forward),
                      patch.object(asyncio, 'start_server', side_effect=listen),
                      patch.object(sys, 'argv', [str(RELAY), str(routes), 'test-owner'])):
                    running = asyncio.create_task(relay['main']())
                    try:
                        await asyncio.wait_for(listeners_ready.wait(), timeout=5)
                        accept_forward, accept_control = accept_connections
                        client = Mock()
                        accept_forward(asyncio.StreamReader(), client)
                        await asyncio.wait_for(forwarding_started.wait(), timeout=5)

                        rejected_forward = Mock()
                        accept_forward(asyncio.StreamReader(), rejected_forward)
                        rejected_forward.close.assert_called_once()
                        self.assertEqual(forward.await_count, 1)

                        idle = asyncio.StreamReader()
                        accept_control(idle, Mock(wait_closed=AsyncMock()))
                        owner = asyncio.StreamReader()
                        token = json.loads(control_path.read_text())['token']
                        owner.feed_data(token.encode('ascii'))
                        shutdown = Mock(drain=AsyncMock(), wait_closed=AsyncMock())
                        accept_control(owner, shutdown)

                        rejected_control = Mock()
                        accept_control(asyncio.StreamReader(), rejected_control)
                        rejected_control.close.assert_called_once()
                        idle.feed_eof()

                        await asyncio.wait_for(running, timeout=5)
                        shutdown.write.assert_called_once_with(b'stopped\n')
                        client.close.assert_called_once()
                    finally:
                        running.cancel()
                        await asyncio.gather(running, return_exceptions=True)


def main():
    if not __debug__:
        sys.exit('fixture smoke checks require assertions: unset PYTHONOPTIMIZE and omit -O/-OO')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--images', required=True, type=Path)
    parser.add_argument('--revision', required=True)
    parser.add_argument('--bin-dir', type=Path)
    parser.add_argument('--port-base', type=int, default=43100)
    args = parser.parse_args()
    prefix = 'zinder-fixture-smoke-' + str(time.time_ns())
    projects = [prefix + '-' + str(n) for n in range(4)]
    state_root = ROOT / '.tmp/regtest-fixture-smoke'
    common = ['--images', str(args.images.resolve()), '--revision', args.revision]
    if args.bin_dir:
        common += ['--bin-dir', str(args.bin_dir.resolve())]

    def invoke(action, project, *extra, success=True, root=state_root, expected_error=None):
        command = [sys.executable, str(FIXTURE), action,
                   '--state-root', str(root), '--project', project, *extra]
        response = subprocess.run(command, capture_output=True, text=True, timeout=300)
        (state_root / (project + '-' + action + ('-reused' if root != state_root else '') + '.log')).write_text(response.stdout + response.stderr)
        if expected_error and expected_error not in response.stdout + response.stderr:
            raise RuntimeError('missing expected failure: ' + expected_error + response.stdout + response.stderr)
        if (response.returncode == 0) != success:
            raise RuntimeError(response.stdout + response.stderr)

    def receipt(project):
        return json.loads((state_root / project / 'ready.json').read_text())

    state_root.mkdir(parents=True, exist_ok=True)
    alternate = state_root / 'reused'
    try:
        # Two independent projects coexist, including different storage contracts and schedules.
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            jobs = [executor.submit(invoke, 'up', projects[0], *common,
                                    '--port-base', str(args.port_base)),
                    executor.submit(invoke, 'up', projects[1], *common,
                                    '--port-base', str(args.port_base + 100),
                                    '--node-config', str(ROOT / 'deploy/zebra/regtest-nu6-at-one.toml'),
                                    '--raw-blob-policy', 'all')]
            for job in jobs:
                job.result()
        initial = (state_root / projects[0] / 'ready.initial.json').read_bytes()
        peer = receipt(projects[1])
        invoke('mine', projects[0], '--tip', '152')
        assert receipt(projects[0])['current_tip'] == 152
        assert (state_root / projects[0] / 'ready.initial.json').read_bytes() == initial
        invoke('status', projects[1])
        assert receipt(projects[1])['current_tip'] == peer['current_tip'] == 150
        assert receipt(projects[1])['current_hash'] == peer['current_hash']
        capabilities = peer['health']['serve']['capabilities']
        assert 'wallet.read.full_block_at_v1' in capabilities
        assert 'wallet.read.full_block_range_v1' in capabilities
        invoke('down', projects[0])
        # Fresh repeat after teardown, with the same ports and a delayed activation schedule.
        invoke('up', projects[2], *common, '--port-base', str(args.port_base),
               '--node-config', str(ROOT / 'deploy/zebra/regtest-ironwood-at-250.toml'), '--tip', '260')
        assert receipt(projects[2])['current_tip'] == 260
        invoke('mine', projects[0], '--tip', '261', success=False, expected_error='owned node is not running')
        invoke('status', projects[2])
        assert receipt(projects[2])['current_tip'] == 260
        invoke('mine', projects[2], '--tip', '262')
        # A candidate mismatch must clean up all partially started resources.
        wrong = common.copy()
        wrong[wrong.index('--revision') + 1] = '0' * 40
        invoke('up', projects[3], *wrong, '--port-base', str(args.port_base + 200), success=False, expected_error='network or revision differs')
        assert not (state_root / projects[3] / 'ready.json').exists()
        invoke('up', projects[0], *common, '--port-base', str(args.port_base + 200),
               '--node-config', str(ROOT / 'deploy/zebra/regtest-nu6-at-one.toml'), root=alternate)
        invoke('down', projects[0], success=False, expected_error='owned by a different fixture instance')
        invoke('status', projects[0], success=False, expected_error='owned by a different fixture instance')
        invoke('status', projects[0], root=alternate)
    finally:
        if (alternate / projects[0] / 'state.json').exists():
            invoke('down', projects[0], root=alternate)
        for project in projects:
            if (state_root / project / 'state.json').exists():
                invoke('down', project)
                assert not (state_root / project / 'relay-control.json').exists()
            for kind, extra in [('ps', ['-a']), ('volume', ['ls']), ('network', ['ls'])]:
                remaining = subprocess.check_output(['docker', kind, *extra, '-q', '--filter',
                    'label=com.docker.compose.project=' + project], text=True).strip()
                assert not remaining, (project, kind, remaining)
        for base in range(args.port_base, args.port_base + 201, 100):
            for offset in operator['OFFSETS']:
                with socket.socket() as listener:
                    if sys.platform != 'win32':
                        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    listener.bind(('127.0.0.1', base + offset))
    print('PASS: internal-network following, concurrent schedules, full-block capability, immutable receipt, fresh repeat, failure cleanup, stale-owner rejection')
    print(state_root)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        main()
    else:
        unittest.main()
