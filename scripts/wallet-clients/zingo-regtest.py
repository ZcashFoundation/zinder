#!/usr/bin/env python3
"""Exercise the pinned Zingo engine on an explicitly isolated local Regtest node."""
import argparse
import hashlib
import re
import json
import os
from pathlib import Path
import sys
import subprocess
import time
from urllib.request import Request, urlopen
from urllib.parse import urlsplit

from wallet_fixture import binding, verify_node


def wait_for_scan(read_status, target):
    for _ in range(10):
        output = read_status()
        try:
            status = json.loads(output)
            scanned = max((int(row['end_block']) for row in status.get('scan_ranges', [])
                           if row['priority'] == 'Scanned'), default=0)
            if scanned >= target:
                return status
        except ValueError:
            pass
        time.sleep(1)
    raise RuntimeError(f'wallet did not scan through height {target}')


def confirm_shielding_notes(txids, notes, previous_height, rpc):
    if not isinstance(txids, list) or not txids or not all(
        isinstance(txid, str) and re.fullmatch(r'[0-9a-f]{64}', txid) for txid in txids
    ) or len(set(txids)) != len(txids):
        raise ValueError('quickshield returned invalid transaction IDs')
    observed = {(note['txid'], note['status']) for pool in ('ironwood_notes', 'orchard_notes', 'sapling_notes')
                for note in notes[pool]['note_summaries'] if note['value'] > 0}
    heights = {}
    for txid in txids:
        tx = rpc('getrawtransaction', [txid, 1])
        height = tx['height']
        if (tx['txid'] != txid or tx['confirmations'] < 1 or height <= previous_height
                or tx['blockhash'] != rpc('getblockhash', [height])):
            raise RuntimeError('new shielding transaction has no canonical confirmation')
        if (txid, f'confirmed at block height {height}') not in observed:
            raise RuntimeError('wallet did not discover a confirmed note from the new shielding transaction')
        heights[txid] = height
    return heights


def main():
    if not __debug__:
        raise RuntimeError('optimized Python disables required wallet assertions')
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary', required=True)
    p.add_argument('--endpoint', required=True)
    p.add_argument('--node', required=True)
    p.add_argument('--trace-tool', required=True)
    p.add_argument('--scratch', required=True, type=Path)
    p.add_argument('--report', required=True, type=Path)
    p.add_argument('--evidence-dir', required=True, type=Path)
    p.add_argument('--step', required=True)
    p.add_argument('--birthday', type=int, default=2)
    p.add_argument('--fence-python', required=True)
    p.add_argument('--proto-dir', required=True)
    p.add_argument('--ready', required=True, type=Path)
    p.add_argument('--candidate', type=Path)
    a = p.parse_args()
    endpoint, control, anchor_height, anchor_hash = binding(a.ready, a.candidate, a.endpoint, a.node)
    verify_node(control, anchor_height, anchor_hash, "zingo")
    if a.step == 'bootstrap':
        a.evidence_dir.mkdir(mode=0o700, parents=True)
        subprocess.run([a.fence_python, str(Path(__file__).with_name('wait-serving-fence.py')),
                        '--endpoint', endpoint, '--height', str(anchor_height), '--expected-hash', anchor_hash,
                        '--proto-dir', a.proto_dir, '--output', str(a.evidence_dir/'fixture-anchor.json')],
                       check=True, timeout=190)
    for endpoint in (a.endpoint, a.node):
        url = urlsplit(endpoint)
        if url.scheme != 'http' or url.hostname != '127.0.0.1':
            p.error('only explicitly isolated loopback plaintext endpoints are admitted')
    a.scratch.mkdir(mode=0o700, parents=True, exist_ok=True)
    statefile = a.scratch / 'state.json'
    state = json.loads(statefile.read_text()) if statefile.exists() else {}
    logdir = a.evidence_dir / a.step if a.evidence_dir else a.scratch / 'logs'
    logdir.mkdir(parents=True, exist_ok=True)
    count = len(list(logdir.iterdir()))
    endpoint_evidence = {'scope':'configured','target_connected':False,'unexpected_connections':0}

    def cli(command, wallet='restored', online=False, seed=None):
        nonlocal count, endpoint_evidence
        argv = [a.binary, '--chain', 'regtest', '--data-dir', str(a.scratch / wallet)]
        argv += ['--server', a.endpoint, '--waitsync'] if online else ['--offline']
        if seed is not None or not online:
            argv += ['--birthday', str(a.birthday)]
        env = dict(os.environ)
        env.pop('ZINGO_SEED', None)
        if seed is not None:
            env['ZINGO_SEED'] = seed
        full_command = argv + command
        trace_dir = logdir / f'trace-{time.time_ns()}'
        if online:
            full_command = [sys.executable, a.trace_tool, '--endpoint-port', str(urlsplit(a.endpoint).port), '--output', str(trace_dir), '--', *full_command]
        with subprocess.Popen(full_command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as process:
            stdout, stderr = process.communicate()
        run = subprocess.CompletedProcess(full_command, process.returncode, stdout, stderr)
        if command != ['recovery_info']:
            count += 1
            (logdir / f'{count:03d}.json').write_text(json.dumps({'command': command, 'exit': run.returncode, 'pid': process.pid, 'stdout': run.stdout, 'stderr': run.stderr}))
        if run.returncode or 'error: Sync error.' in run.stderr:
            raise RuntimeError('client command failed; inspect private logs')
        if online:
            trace = json.loads((trace_dir / 'report.json').read_text())
            if not (trace['status'] == 'passed' and trace['fallback_used'] is False):
                raise RuntimeError('wallet evidence check failed')
        if online:
            endpoint_evidence = {'scope':'process-trace','target_connected':True,'unexpected_connections':0,'trace_fingerprint_sha256':hashlib.sha256((trace_dir/'connect.log').read_bytes()).hexdigest()}
        state['_last_pid'] = process.pid
        return run.stdout

    def rpc(method, params):
        request = Request(a.node, json.dumps({'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params}).encode(), {'Content-Type': 'application/json'})
        with urlopen(request, timeout=180) as response:
            result = json.load(response)
        if result.get('error'):
            raise RuntimeError(f"isolated node {method} failed: {result['error']}")
        (logdir / f'node-{time.time_ns()}.json').write_text(json.dumps({'method': method, 'params': params, 'result': result['result']}))
        return result['result']

    def wait_published(target):
        receipt = logdir / f'publication-{time.time_ns()}.json'
        subprocess.run([a.fence_python, str(Path(__file__).with_name('wait-serving-fence.py')), '--endpoint', a.endpoint, '--height', str(target), '--proto-dir', a.proto_dir, '--output', str(receipt)], check=True, timeout=190)

    def sync(wallet="restored", minimum=None):
        target = rpc('getblockcount', []) if minimum is None else minimum
        rpc('generate', [1])
        wait_published(rpc('getblockcount', []))
        state['scan_fence_height'] = target
        state['guard_blocks'] = state.get('guard_blocks', 0) + 1
        return wait_for_scan(lambda: cli(['sync', 'status'], wallet=wallet, online=True), target)

    def mine():
        rpc('generate', [1])
        time.sleep(2)
        return sync()
    step = a.step
    domain = {}
    envelope = {}
    def fingerprint(txid):
        return hashlib.sha256(bytes.fromhex(txid)).hexdigest()
    def text_fingerprint(text):
        return hashlib.sha256(text.encode()).hexdigest()
    if step == 'bootstrap':
        cli(['addresses'], wallet='generated')
        generated = cli(['recovery_info'], wallet='generated')
        phrase = next((' '.join(line.split()[-24:]).strip('\" ,') for line in generated.splitlines() if len(line.split()) == 26))
        cli(['sync', 'status'], wallet='created', online=True, seed=phrase)
        addresses = json.loads(cli(['addresses'], wallet='created'))
        if not (addresses and addresses[0]['encoded_address'].startswith('uregtest')):
            raise RuntimeError('wallet evidence check failed')
        state['address'] = addresses[0]['encoded_address']
        domain['address'] = state['address']
        taddr = json.loads(cli(['t_addresses'], wallet='created'))[0]['encoded_address']
        rpc('generatetoaddress', [1, taddr])
        rpc('generate', [100])
        sync(wallet='created')
        shield = json.loads(cli(['quickshield'], wallet='created', online=True))
        state['activity_txid'] = shield['txids'][0]
        rpc('generate', [1])
        sync(wallet='created')
        if 'txid: ' + state['activity_txid'] not in cli(['transactions'], wallet='created'):
            raise RuntimeError('funded source activity absent')
    elif step == 'restore':
        if (a.scratch/'restored'/'zingo-wallet.dat').exists():
            raise RuntimeError('restore requires independent empty database')
        recovery = cli(['recovery_info'], wallet='created')
        phrase = next((' '.join(line.split()[-24:]).strip('" ,') for line in recovery.splitlines() if len(line.split()) == 26))
        status = json.loads(cli(['sync', 'status'], online=True, seed=phrase))
        sync()
        addresses = json.loads(cli(['addresses']))
        if not addresses[0]['encoded_address'] == state['address']:
            raise RuntimeError('wallet evidence check failed')
        domain['same_address'] = True
        if 'txid: ' + state['activity_txid'] not in cli(['transactions']):
            raise RuntimeError('restored known source activity absent')
        identity = text_fingerprint(state['address'])
        activity = fingerprint(state['activity_txid'])
        envelope['restore'] = {'fresh_database': True, 'source_database_fingerprint_sha256':text_fingerprint(str((a.scratch/'created').resolve())), 'restored_database_fingerprint_sha256':text_fingerprint(str((a.scratch/'restored').resolve())), 'source_wallet_fingerprint_sha256':identity,'restored_wallet_fingerprint_sha256':identity,'source_activity_fingerprint_sha256':activity,'restored_activity_fingerprint_sha256':activity,'restored_activity_count':1}
    elif step == 'transparent-discovery':
        address = json.loads(cli(['t_addresses']))[0]['encoded_address']
        rpc('generatetoaddress', [1, address])
        rpc('generate', [100])
        time.sleep(2)
        sync()
        coins = json.loads(cli(['coins']))['transparent_coins']
        if not coins:
            raise RuntimeError('wallet evidence check failed')
        domain['coins'] = len(coins)
    elif step == 'shielded-discovery':
        sync()
        previous_height = rpc('getblockcount', [])
        shield = json.loads(cli(['quickshield'], online=True))
        state['shield_tx'] = shield
        mine()
        notes = json.loads(cli(['notes']))
        domain['confirmed_heights'] = confirm_shielding_notes(shield['txids'], notes, previous_height, rpc)
        domain['funding_shield'] = shield
    elif step == 'send':
        rpc('generate', [2])
        time.sleep(2)
        sync()
        sent = json.loads(cli(['quicksend', state['address'], '100000'], online=True))
        if not sent.get('txids'):
            raise RuntimeError('wallet evidence check failed')
        state['txid'] = sent['txids'][0]
        domain['txid'] = state['txid']
        envelope['transaction'] = {'fingerprint_sha256':fingerprint(sent['txids'][0]), 'observation':'submitted'}
    elif step == 'pending':
        mempool = rpc('getrawmempool', [])
        if not state['txid'] in mempool:
            raise RuntimeError('wallet evidence check failed')
        history = cli(['transactions'])
        status = history.split('txid: ' + state['txid'], 1)[1].split('kind:', 1)[0]
        if not any(('status: ' + word in status for word in ('transmitted', 'mempool', 'pending'))):
            raise RuntimeError('wallet evidence check failed')
        domain['txid'] = state['txid']
        domain['mempool_contains_txid'] = True
        envelope['transaction'] = {'fingerprint_sha256':fingerprint(state['txid']), 'observation':'wallet-pending'}
    elif step == 'mined':
        mine()
        tx = rpc('getrawtransaction', [state['txid'], 1])
        if not tx.get('confirmations', 0) >= 1:
            raise RuntimeError('wallet evidence check failed')
        domain['confirmations'] = tx['confirmations']
        state['mined_height'] = tx['height']
        envelope['transaction'] = {'fingerprint_sha256':fingerprint(tx['txid']), 'observation':'canonical-mined','mined_height':tx['height']}
    elif step == 'scanned':
        sync()
        history = cli(['transactions'])
        status = history.split('txid: ' + state['txid'], 1)[1].split('kind:', 1)[0]
        if not 'status: confirmed' in status:
            raise RuntimeError('wallet evidence check failed')
        domain['txid'] = state['txid']
        domain['new_process'] = True
        height = int(re.search(r'blockheight: (\d+)', status).group(1))
        if height != state['mined_height']:
            raise RuntimeError('wallet confirmation height differs')
        envelope['transaction'] = {'fingerprint_sha256':fingerprint(state['txid']), 'observation':'wallet-scanned','mined_height':height}
    elif step == 'restart':
        before = sync()
        before_pid = state['_last_pid']
        after = sync()
        after_pid = state['_last_pid']
        if before_pid == after_pid:
            raise RuntimeError('wallet process did not restart')
        bh = max(int(r['end_block']) for r in before['scan_ranges'] if r['priority']=='Scanned')
        ah = max(int(r['end_block']) for r in after['scan_ranges'] if r['priority']=='Scanned')
        state['restart_height'] = ah
        envelope['restart'] = {'before_process_id':before_pid,'after_process_id':after_pid,'previous_process_exited':True,'before_height':bh,'after_height':ah,'process_id_scope':'CLI process-tree supervisor'}
    elif step == 'resume':
        before = state['restart_height']
        after_status = mine()
        after = max(int(r['end_block']) for r in after_status['scan_ranges'] if r['priority']=='Scanned')
        if after <= before:
            raise RuntimeError('wallet did not scan a new block after restart')
        envelope['progress'] = {'before_height':before,'after_height':after,'new_block_mined_after_restart':True}
    else:
        p.error('unknown lifecycle step')
    statefile.write_text(json.dumps(state))
    a.report.write_text(json.dumps({'endpoint_evidence':endpoint_evidence,'assertions_enabled':True, 'assertion_mode':'always-on', **envelope, 'status': 'completed', 'endpoint': a.endpoint, 'fallback_used': False, 'assertions': {step: True}, 'restart_scope': 'process-restart' if step == 'restart' else 'unspecified', 'domain': domain}))
if __name__ == '__main__':
    main()
