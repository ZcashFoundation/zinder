#!/usr/bin/env python3
"""Drive the private Zkool example against a disposable loopback Regtest chain."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import sqlite3
import subprocess
import time
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from wallet_fixture import binding, verify_node


def main():
    if not __debug__:
        raise RuntimeError('optimized Python disables required wallet assertions')
    p = argparse.ArgumentParser(description=__doc__)
    for name in ['binary', 'endpoint', 'node', 'trace-tool', 'step']:
        p.add_argument('--' + name, required=True)
    for name in ['scratch', 'report', 'evidence-dir']:
        p.add_argument('--' + name, required=True, type=Path)
    p.add_argument('--variant', choices=['internal-enabled','external-only'], required=True)
    p.add_argument('--fence-python', required=True)
    p.add_argument('--proto-dir', required=True)
    p.add_argument('--ready', required=True, type=Path)
    p.add_argument('--candidate', type=Path)
    a = p.parse_args()
    endpoint, control, anchor_height, anchor_hash = binding(a.ready, a.candidate, a.endpoint, a.node)
    verify_node(control, anchor_height, anchor_hash, "zkool")
    if a.step == 'bootstrap':
        a.evidence_dir.mkdir(mode=0o700, parents=True)
        subprocess.run([a.fence_python, str(Path(__file__).with_name('wait-serving-fence.py')),
                        '--endpoint', endpoint, '--height', str(anchor_height), '--expected-hash', anchor_hash,
                        '--proto-dir', a.proto_dir, '--output', str(a.evidence_dir/'fixture-anchor.json')],
                       check=True, timeout=190)
    for endpoint in [a.endpoint, a.node]:
        if urlsplit(endpoint).scheme != 'http' or urlsplit(endpoint).hostname != '127.0.0.1':
            p.error('explicit isolated loopback HTTP endpoints required')
    a.scratch.mkdir(mode=0o700, parents=True, exist_ok=True)
    logs = a.evidence_dir / a.step
    logs.mkdir(parents=True, exist_ok=True)
    statefile = a.scratch / 'state.json'
    state = json.loads(statefile.read_text()) if statefile.exists() else {}
    wallet = a.scratch / 'restored'
    seed = 'abandon ' * 23 + 'art'
    endpoint_evidence = {'scope':'configured','target_connected':False,'unexpected_connections':0}

    def cli(op, args, online=False):
        nonlocal endpoint_evidence
        command = [a.binary, op, *args]
        trace = logs / f'trace-{time.time_ns()}'
        if online:
            command = [sys.executable, a.trace_tool, '--endpoint-port', str(urlsplit(a.endpoint).port), '--output', str(trace), '--', *command]
        env = dict(os.environ, ZINDER_ZKOOL_USE_INTERNAL='true' if a.variant == 'internal-enabled' else 'false')
        with subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as process:
            stdout, stderr = process.communicate()
        (logs / f'{op}-{time.time_ns()}.json').write_text(json.dumps({'operation': op, 'pid': process.pid, 'exit': process.returncode, 'stdout': stdout, 'stderr': stderr}))
        if not process.returncode == 0:
            raise RuntimeError('native engine operation failed; inspect private evidence')
        if online:
            attribution = json.loads((trace / 'report.json').read_text())
            if not (attribution['status'] == 'passed' and attribution['fallback_used'] is False):
                raise RuntimeError('wallet evidence check failed')
        if online:
            endpoint_evidence = {'scope':'process-trace','target_connected':True,'unexpected_connections':0,'trace_fingerprint_sha256':hashlib.sha256((trace/'connect.log').read_bytes()).hexdigest()}
        state['_last_pid'] = process.pid
        return stdout

    def rpc(method, params):
        q = Request(a.node, json.dumps({'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params}).encode(), {'Content-Type': 'application/json'})
        with urlopen(q, timeout=180) as f:
            r = json.load(f)
        if not not r.get('error'):
            raise RuntimeError(r.get('error'))
        (logs / f'node-{time.time_ns()}.json').write_text(json.dumps({'method': method, 'params': params, 'result': r['result']}))
        return r['result']

    def report():
        return json.loads(cli('report', [str(wallet)]))

    def wait_published(target):
        receipt = logs / f'publication-{time.time_ns()}.json'
        subprocess.run([a.fence_python, str(Path(__file__).with_name('wait-serving-fence.py')), '--endpoint', a.endpoint, '--height', str(target), '--proto-dir', a.proto_dir, '--output', str(receipt)], check=True, timeout=190)

    def resume():
        wait_published(rpc('getblockcount', []))
        for _ in range(10):
            cli('resume', [a.endpoint, str(wallet)], True)
            r = report()
            if r['synced_height'] >= rpc('getblockcount', []):
                return r
            time.sleep(1)
        raise RuntimeError('engine did not reach controlled node tip')

    def txrow(table):
        with sqlite3.connect(wallet / 'zkool.db') as con:
            rows = con.execute(f'SELECT hex(txid), height FROM {table}').fetchall()
        return [row for row in rows if row[0].lower() in [state['txid'], bytes.fromhex(state['txid'])[::-1].hex()]]
    step = a.step
    domain = {}
    envelope = {}
    def fp(text):
        return hashlib.sha256(text.encode()).hexdigest()
    def txfp(txid):
        return hashlib.sha256(bytes.fromhex(txid)).hexdigest()
    def observed_txid(row):
        candidate = row[0].lower()
        return candidate if candidate == state['txid'] else bytes.fromhex(candidate)[::-1].hex()
    if step == 'bootstrap':
        created = a.scratch / 'created'
        cli('sync', [a.endpoint, '2', seed, str(created)], True)
        addresses = json.loads(cli('addresses', [str(created)]))
        if not addresses['unified'].startswith('uregtest'):
            raise RuntimeError('wallet evidence check failed')
        state['addresses'] = addresses
        domain['database_created'] = (created / 'zkool.db').is_file()
        block_hash, = rpc('generatetoaddress', [1, addresses['unified']])
        created_txid = rpc('getblock', [block_hash, 1])['tx'][0]
        rpc('generate', [100])
        wait_published(rpc('getblockcount', []))
        cli('resume', [a.endpoint, str(created)], True)
        with sqlite3.connect(created/'zkool.db') as con:
            activity = con.execute('SELECT hex(txid) FROM transactions WHERE height>0 AND txid=?',
                                   (bytes.fromhex(created_txid)[::-1],)).fetchone()
        if activity is None:
            raise RuntimeError('known source activity required')
        state['activity_sql_txid'] = activity[0]
    elif step == 'restore':
        if (wallet/'zkool.db').exists():
            raise RuntimeError('restore requires independent empty database')
        cli('sync', [a.endpoint, '2', seed, str(wallet)], True)
        if not json.loads(cli('addresses', [str(wallet)])) == state['addresses']:
            raise RuntimeError('wallet evidence check failed')
        domain['same_addresses'] = True
        with sqlite3.connect(wallet/'zkool.db') as con:
            count = con.execute('SELECT COUNT(*) FROM transactions WHERE hex(txid)=? AND height>0',(state['activity_sql_txid'],)).fetchone()[0]
        if count < 1:
            raise RuntimeError('known restored activity absent')
        identity=fp(state['addresses']['unified'])
        activity=txfp(bytes.fromhex(state['activity_sql_txid'])[::-1].hex())
        envelope['restore']={'fresh_database':True,'source_database_fingerprint_sha256':fp(str((a.scratch/'created').resolve())),'restored_database_fingerprint_sha256':fp(str(wallet.resolve())),'source_wallet_fingerprint_sha256':identity,'restored_wallet_fingerprint_sha256':identity,'source_activity_fingerprint_sha256':activity,'restored_activity_fingerprint_sha256':activity,'restored_activity_count':count}
    elif step == 'transparent-discovery':
        before = report()['balance']['transparent']
        rpc('generatetoaddress', [1, state['addresses']['transparent']])
        rpc('generate', [100])
        time.sleep(2)
        after = resume()['balance']['transparent']
        if not after > before:
            raise RuntimeError('wallet evidence check failed')
        domain.update({'before': before, 'after': after})
    elif step == 'shielded-discovery':
        before = report()['balance']['ironwood']
        rpc('generatetoaddress', [1, state['addresses']['unified']])
        rpc('generate', [100])
        time.sleep(2)
        after = resume()['balance']['ironwood']
        if not after > before:
            raise RuntimeError('wallet evidence check failed')
        domain.update({'before': before, 'after': after})
    elif step == 'send':
        resume()
        r = json.loads(cli('send', [a.endpoint, str(wallet), state['addresses']['unified'], '100000', '8'], True))
        if not len(r['txid']) == 64:
            raise RuntimeError('wallet evidence check failed')
        state['txid'] = r['txid']
        domain['txid'] = r['txid']
        envelope['transaction']={'fingerprint_sha256':txfp(r['txid']),'observation':'submitted'}
    elif step == 'pending':
        if not state['txid'] in rpc('getrawmempool', []):
            raise RuntimeError('wallet evidence check failed')
        rows = txrow('pending_txs')
        if not rows:
            raise RuntimeError('wallet evidence check failed')
        domain.update({'wallet_pending_rows': rows, 'txid': state['txid']})
        envelope['transaction']={'fingerprint_sha256':txfp(observed_txid(rows[0])),'observation':'wallet-pending'}
    elif step == 'mined':
        rpc('generate', [1])
        r = rpc('getrawtransaction', [state['txid'], 1])
        if not r.get('confirmations', 0) > 0:
            raise RuntimeError('wallet evidence check failed')
        domain.update({'txid': state['txid'], 'confirmations': r['confirmations']})
        state['mined_height']=r['height']
        envelope['transaction']={'fingerprint_sha256':txfp(r['txid']),'observation':'canonical-mined','mined_height':r['height']}
    elif step == 'scanned':
        resume()
        rows = txrow('transactions')
        if not (rows and rows[0][1] > 0):
            raise RuntimeError('wallet evidence check failed')
        domain.update({'confirmed_rows': rows, 'new_process': True})
        if rows[0][1] != state['mined_height']:
            raise RuntimeError('wallet confirmation height differs')
        envelope['transaction']={'fingerprint_sha256':txfp(observed_txid(rows[0])),'observation':'wallet-scanned','mined_height':rows[0][1]}
    elif step == 'restart':
        before = report()['synced_height']
        before_pid=state['_last_pid']
        after = report()['synced_height']
        after_pid=state['_last_pid']
        if before_pid == after_pid:
            raise RuntimeError('wallet process did not restart')
        state['restart_height']=after
        envelope['restart']={'before_process_id':before_pid,'after_process_id':after_pid,'previous_process_exited':True,'before_height':before,'after_height':after}
    elif step == 'resume':
        before = state['restart_height']
        rpc('generate', [1])
        after = resume()['synced_height']
        if not after > before:
            raise RuntimeError('wallet evidence check failed')
        domain.update({'before': before, 'after': after})
        envelope['progress']={'before_height':before,'after_height':after,'new_block_mined_after_restart':True}
    else:
        p.error('unknown step')
    statefile.write_text(json.dumps(state))
    a.report.write_text(json.dumps({'endpoint_evidence':endpoint_evidence,'assertions_enabled':True,'assertion_mode':'always-on',**envelope,'status': 'completed', 'endpoint': a.endpoint, 'fallback_used': False, 'restart_scope': 'process-restart' if step == 'restart' else 'unspecified', 'assertions': {step: True}, 'domain': domain}))
if __name__ == '__main__':
    main()
