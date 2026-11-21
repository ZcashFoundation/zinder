#!/usr/bin/env python3
"""Check that Zkool rolls back an owned Ironwood coinbase on a private Regtest fork."""
import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import re
import runpy
import sqlite3
import subprocess
import sys
import time
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, Request, build_opener

from wallet_fixture import binding, loopback, verify_node


SCRIPTS = Path(__file__).resolve().parent.parent
campaign = runpy.run_path(str(SCRIPTS / 'run-benchmark-campaign.py'))
sha256 = campaign['sha256']
run_command = campaign['run_command']
CONFIG = SCRIPTS.parent / 'deploy/zebra/regtest-ironwood-at-250.toml'
PUBLIC_VECTOR = 'abandon ' * 23 + 'art'


def require(condition, message):
    if not condition:
        raise ValueError(message)


class Scenario:
    def __init__(self, args, ready_path, receipt):
        import grpc
        import service_pb2
        import service_pb2_grpc

        self.args = args
        self.receipt = receipt
        self.wallet = args.output / 'wallet'
        self.endpoint, self.node, height, anchor = binding(ready_path)
        self.opener = build_opener(ProxyHandler({}))
        ready = json.loads(ready_path.read_text())
        self.metrics_url = loopback(ready['compat_ops']) + '/metrics'
        require(ready['zinder_commit'] == args.revision and ready['isolation']['peer_count'] == 0
                and ready['isolation']['default_ipv4_route'] is False, 'unexpected fixture identity')
        verify_node(self.node, height, anchor, 'zkool')
        self.pb = service_pb2
        self.channel = grpc.insecure_channel(urlsplit(self.endpoint).netloc)
        self.grpc = service_pb2_grpc.CompactTxStreamerStub(self.channel)
        info = self.grpc.GetLightdInfo(self.pb.Empty(), timeout=5)
        require(info.gitCommit == args.revision and info.chainName == 'test', 'wrong wallet server')
        self.fence(height, anchor)
        receipt['fixture'] = {
            'network': 'zcash-regtest', 'release_revision': info.gitCommit, 'release_version': info.version,
            'anchor_height': height, 'anchor_hash': anchor, 'ready_sha256': sha256(ready_path),
            'activation_schedule_sha256': ready['activation_schedule_sha256'], 'node_image': ready['node_image'],
            'release_binary_sha256': {name: b['sha256'] for name, b in ready['binaries'].items()},
            'image_ids': ready['images'],
        }

    def rpc(self, method, *params):
        data = json.dumps({'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params}).encode()
        with self.opener.open(Request(self.node, data, {'Content-Type': 'application/json'}), timeout=45) as response:
            result = json.load(response)
        require(not result.get('error'), f'{method} failed: {result.get("error")}')
        return result['result']

    def fence(self, height, block_hash):
        import grpc
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                tip = self.grpc.GetLatestBlock(self.pb.ChainSpec(), timeout=3)
                if tip.height == height and tip.hash[::-1].hex() == block_hash:
                    block = self.grpc.GetBlock(self.pb.BlockID(height=height), timeout=3)
                    end = self.pb.BlockID(height=height)
                    rows = list(self.grpc.GetBlockRange(self.pb.BlockRange(start=end, end=end), timeout=3))
                    tree = self.grpc.GetTreeState(end, timeout=3)
                    if (block.height == tree.height == height and len(rows) == 1
                            and block.hash == rows[0].hash == tip.hash):
                        return
            except grpc.RpcError:
                pass
            time.sleep(0.25)
        raise RuntimeError('compat did not publish the expected block hash at the fixed height')

    def counters(self):
        with self.opener.open(self.metrics_url, timeout=5) as response:
            metrics = response.read().decode()
        counts = Counter()
        for line in metrics.splitlines():
            if not line.startswith('zinder_wallet_rpc_requests_total{'):
                continue
            labels = dict(re.findall(r'(\w+)="([^"]*)"', line))
            if labels.get('surface') == 'lightwalletd':
                counts[(labels['method'], labels['grpc_code'])] += int(float(line.rsplit(' ', 1)[1]))
        return counts

    def wallet_command(self, phase, op, args, *, online=False):
        argv = [str(self.args.engine), op, *map(str, args)]
        trace = self.args.output / 'private' / phase
        if online:
            before = self.counters()
            argv = [sys.executable, str(SCRIPTS / 'trace-wallet-endpoint.py'),
                    '--endpoint-port', str(urlsplit(self.endpoint).port), '--output', str(trace), '--', *argv]
        stdout_path = self.args.output / 'private' / f'{phase}.stdout'
        with stdout_path.open('w') as stdout, stdout_path.with_suffix('.stderr').open('w') as stderr:
            result = run_command(argv, stdout, stderr, timeout=120)
        if online:
            after = self.counters()
            require(all(after[key] >= value for key, value in before.items()), 'server RPC counter reset')
            observed = after - before
            attribution = json.loads((trace / 'report.json').read_text())
            self.receipt['actions'][phase] = {
                'methods': [{'method': method, 'grpc_code': code, 'count': n}
                            for (method, code), n in sorted(observed.items())],
                'client_connected_only_to_target': attribution['status'] == 'passed',
                'connect_trace_sha256': sha256(trace / 'connect.log'),
            }
            require(attribution['status'] == 'passed' and attribution['fallback_used'] is False,
                    'wallet connections differ from the selected endpoint')
        require(result == 0, f'wallet {phase} failed (see private stderr)')
        if online:
            require(observed['GetBlockRange', 'ok'] > 0,
                    f'{phase}: expected an observed wallet GetBlockRange')
        return stdout_path.read_text()

    def state(self, phase, txids):
        report = json.loads(self.wallet_command(phase, 'report', [self.wallet]))
        with sqlite3.connect(f'file:{self.wallet / "zkool.db"}?mode=ro', uri=True) as db:
            heights = [db.execute('SELECT height FROM transactions WHERE txid = ?',
                                  (bytes.fromhex(tx)[::-1],)).fetchall() for tx in txids]
        state = {'scanned_height': report['synced_height'], 'ironwood_balance': report['balance']['ironwood'],
                 'received_notes': report['received_notes'], 'history_entries': report['history_entries'],
                 'owned_tx_heights': [[row[0] for row in matches] for matches in heights]}
        self.receipt['wallet_states'][phase] = state
        return state

    def fund(self, address):
        block_hash, = self.rpc('generatetoaddress', 1, address)
        block = self.rpc('getblock', block_hash, 1)
        return block_hash, block['height'], block['tx'][0]

    def run(self):
        self.wallet_command('bootstrap', 'sync', [self.endpoint, 2, PUBLIC_VECTOR, self.wallet], online=True)
        address = json.loads(self.wallet_command('addresses', 'addresses', [self.wallet]))['unified']
        _, kept_height, kept = self.fund(address)
        self.rpc('generate', 1)
        self.fence(self.rpc('getblockcount'), self.rpc('getbestblockhash'))
        self.wallet_command('scan-surviving-receipt', 'resume', [self.endpoint, self.wallet], online=True)
        baseline = self.state('surviving-receipt', [kept])
        require(baseline['owned_tx_heights'] == [[kept_height]] and baseline['received_notes'] > 0,
                'surviving coinbase missing from wallet history or notes')

        removed_block, removed_height, removed = self.fund(address)
        require(kept != removed, 'expected two distinct wallet receipts')
        self.rpc('generate', 1)
        self.fence(self.rpc('getblockcount'), self.rpc('getbestblockhash'))
        self.wallet_command('scan-orphan-candidate', 'resume', [self.endpoint, self.wallet], online=True)
        mined = self.state('before-reorg', [kept, removed])
        require(mined['owned_tx_heights'] == [[kept_height], [removed_height]]
                and mined['received_notes'] > baseline['received_notes']
                and mined['ironwood_balance'] > baseline['ironwood_balance'], 'second coinbase was not credited')

        require(self.rpc('getblockhash', removed_height) == removed_block, 'old block changed before invalidation')
        self.rpc('invalidateblock', removed_block)
        require(self.rpc('getblockcount') == removed_height - 1, 'unexpected fork point')
        self.rpc('generate', 4)
        new_tip = self.rpc('getblockcount')
        new_hash = self.rpc('getblockhash', removed_height)
        require(new_hash != removed_block and self.rpc('getblock', new_hash, 1)['tx'][0] != removed,
                'replacement did not remove the owned coinbase')
        self.fence(new_tip, self.rpc('getbestblockhash'))
        self.receipt['owned_transaction_fingerprints'] = {
            name: hashlib.sha256(bytes.fromhex(txid)).hexdigest() for name, txid in [('surviving', kept), ('orphan', removed)]
        }
        self.receipt['fork'] = {'old_height': removed_height, 'new_tip': new_tip,
                                'old_block_hash': removed_block, 'replacement_hash': new_hash}
        self.wallet_command('follow-replacement', 'resume', [self.endpoint, self.wallet], online=True)
        restored = self.state('after-first-reorg-sync', [kept, removed])
        expected = dict(baseline, owned_tx_heights=[[kept_height], []])
        require(restored == dict(expected, scanned_height=restored['scanned_height'])
                and kept_height <= restored['scanned_height'] <= new_tip,
                'wallet failed to remove only the orphaned receipt and balance')
        self.receipt['first_reorg_sync_reached_tip'] = restored['scanned_height'] == new_tip
        if restored['scanned_height'] < new_tip:
            self.wallet_command('resume-replacement', 'resume', [self.endpoint, self.wallet], online=True)
            restored = self.state('after-reorg-catchup', [kept, removed])
        require(restored == dict(expected, scanned_height=new_tip), 'wallet did not catch up to the replacement tip')
        self.rpc('generate', 1)
        self.fence(new_tip + 1, self.rpc('getbestblockhash'))
        self.wallet_command('restart-and-advance', 'resume', [self.endpoint, self.wallet], online=True)
        reopened = self.state('reopened', [kept, removed])
        require(reopened == dict(restored, scanned_height=new_tip + 1), 'reopened wallet lost the rollback')
        self.receipt['assertions'] = {
            'distinct_branch_hash_at_mined_height': True, 'orphan_removed_from_confirmed_history': True,
            'owned_note_and_balance_reverted': True, 'surviving_receipt_preserved': True,
            'rollback_persisted_after_new_process_and_next_block': True,
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--engine', type=Path, required=True, help='Zkool v6.37.0 Regtest sync_eval executable')
    parser.add_argument('--engine-sha256', required=True)
    parser.add_argument('--images', type=Path, required=True, help='regtest-fixture image map')
    parser.add_argument('--revision', required=True, help='selected Zinder release commit')
    parser.add_argument('--bin-dir', type=Path, help='released binaries to overlay on the pinned images')
    parser.add_argument('--proto-dir', type=Path, required=True, help='generated lightwalletd Python modules')
    parser.add_argument('--output', required=True, type=Path, help='new private evidence directory')
    parser.add_argument('--port-base', type=int, default=48100)
    args = parser.parse_args()
    args.engine = args.engine.resolve()
    require(sha256(args.engine) == args.engine_sha256, 'client binary digest mismatch')
    sys.path.insert(0, str(args.proto_dir.resolve()))
    os.umask(0o077)
    os.environ['ZINDER_ZKOOL_USE_INTERNAL'] = 'true'
    args.output = args.output.resolve()
    args.output.mkdir(parents=True)
    (args.output / 'private').mkdir()
    name = 'zinder-fixture-wallet-reorg-' + os.urandom(6).hex()
    root = args.output / 'projects'
    operator = [sys.executable, str(SCRIPTS / 'regtest-fixture.py')]
    scope = ['--project', name, '--state-root', str(root)]
    receipt = {'schema_version': 1, 'case': 'zkool-ironwood-coinbase-reorg', 'interface': 'lightwalletd',
               'status': 'failed', 'runner_sha256': sha256(Path(__file__)),
               'method_evidence': 'exclusive compat server completed RPC counter deltas per wallet invocation',
               'client': {'name': 'zkool', 'version': '6.37.0', 'variant': 'internal-enabled',
                                                'binary_sha256': args.engine_sha256}, 'actions': {}, 'wallet_states': {}}
    up = operator + ['up', *scope, '--port-base', str(args.port_base), '--node-config', str(CONFIG),
                     '--tip', '260', '--revision', args.revision, '--images', str(args.images)]
    if args.bin_dir:
        up += ['--bin-dir', str(args.bin_dir)]
    try:
        with (args.output / 'private' / 'up.log').open('w') as log:
            require(run_command(up, log, subprocess.STDOUT, timeout=400) == 0, 'fixture startup failed')
        case = Scenario(args, root / name / 'ready.initial.json', receipt)
        try:
            case.run()
        finally:
            case.channel.close()
        require(sha256(args.engine) == args.engine_sha256, 'client binary changed during test')
        receipt['status'] = 'passed'
    finally:
        try:
            if (root / name / 'state.json').is_file():
                with (args.output / 'private' / 'down.log').open('w') as log:
                    down = run_command(operator + ['down', *scope], log, subprocess.STDOUT, timeout=90)
                receipt['fixture_teardown_completed'] = down == 0
                require(down == 0, 'owned fixture teardown failed')
        finally:
            campaign['write_json'](args.output / 'receipt.json', receipt)


if __name__ == '__main__':
    main()
