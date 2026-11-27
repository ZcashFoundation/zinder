#!/usr/bin/env python3
"""Run isolated Docker fixture setup, following, concurrent-project, and cleanup checks."""
import argparse
import concurrent.futures
import json
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]


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
        command = [sys.executable, str(ROOT / 'scripts/regtest-fixture.py'), action,
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
                state = json.loads((state_root / project / 'state.json').read_text())
                proc = Path('/proc') / str(state.get('relay_pid', 0)) / 'cmdline'
                assert not proc.exists() or state['owner'].encode() not in proc.read_bytes().split(b'\0')
            for kind, extra in [('ps', ['-a']), ('volume', ['ls']), ('network', ['ls'])]:
                remaining = subprocess.check_output(['docker', kind, *extra, '-q', '--filter',
                    'label=com.docker.compose.project=' + project], text=True).strip()
                assert not remaining, (project, kind, remaining)
    print('PASS: internal-network following, concurrent schedules, full-block capability, immutable receipt, fresh repeat, failure cleanup, stale-owner rejection')
    print(state_root)


if __name__ == '__main__':
    main()
