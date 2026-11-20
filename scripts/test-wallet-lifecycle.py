#!/usr/bin/env python3
"""Exercise lifecycle certification with an isolated stateful adapter."""

import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

RUNNER = Path(__file__).with_name("run-wallet-lifecycle.py").resolve()
STEPS = ['bootstrap', 'restore', 'transparent-discovery', 'shielded-discovery', 'send', 'pending', 'mined', 'scanned', 'restart', 'resume']
ADAPTER = '''import hashlib, json, os, sys, time
from pathlib import Path
step, scratch, report, endpoint, mode = sys.argv[1:]
state = Path(scratch) / 'state.json'
steps = %r
previous = json.loads(state.read_text()) if state.exists() else []
if previous != steps[:steps.index(step)]: raise RuntimeError('step order')
if mode == 'timeout': time.sleep(30)
if mode == 'exit': sys.exit(7)
if mode == 'unsupported':
    Path(report).write_text(json.dumps({'status':'unsupported'}))
    sys.exit(0)
previous.append(step)
state.write_text(json.dumps(previous))
def digest(text): return hashlib.sha256(text.encode()).hexdigest()
primary = digest('primary transaction fixture')
root = Path(scratch)
source, restored = root/'source-db.json', root/'restored-db.json'
if step == 'bootstrap':
    source.write_text(json.dumps({'account':'public-fixture-account','activity':[digest('known activity')],'height':12}))
payload = {'status':'completed', 'endpoint':endpoint, 'fallback_used':mode=='fallback',
    'count':len(previous), 'assertions_enabled':True, 'assertion_mode':'always-on',
    'secret':'SECRET_DO_NOT_RETAIN',
    'endpoint_evidence':{'scope':'configured','target_connected':True,'unexpected_connections':0}}
if step == 'restore':
    if mode != 'no-restore':
        if restored.exists(): raise RuntimeError('restore must be fresh')
        restored.write_text(source.read_text())
    source_rows=json.loads(source.read_text())
    restored_rows=json.loads(restored.read_text()) if restored.exists() else source_rows
    payload['restore']={'fresh_database':restored.exists(),
        'source_database_fingerprint_sha256':digest(str(source)),
        'restored_database_fingerprint_sha256':digest(str(restored if restored.exists() else source)),
        'source_wallet_fingerprint_sha256':digest(source_rows['account']),
        'restored_wallet_fingerprint_sha256':digest(restored_rows['account']),
        'source_activity_fingerprint_sha256':source_rows['activity'][0],
        'restored_activity_fingerprint_sha256':restored_rows['activity'][0],
        'restored_activity_count':len(restored_rows['activity'])}
observations={'send':'submitted','pending':'wallet-pending','mined':'canonical-mined','scanned':'wallet-scanned'}
if step in observations:
    payload['transaction']={'fingerprint_sha256':digest('unrelated') if mode=='mismatch' else primary,
        'observation':observations[step]}
    if step in ('mined','scanned'): payload['transaction']['mined_height']=12 if mode!='wrong-height' else 13
if step=='scanned': (root/'previous-pid').write_text(str(os.getpid()))
if step=='restart':
    payload['restart_scope']=mode if mode in ('wallet-reopen','unspecified') else 'process-restart'
    payload['restart']={'before_process_id':int((root/'previous-pid').read_text()),
        'after_process_id':os.getpid(),'before_height':12,'after_height':12}
if step=='resume' and mode!='counting':
    rows=json.loads(restored.read_text())
    before=rows['height']
    rows['height']+=0 if mode=='no-progress' else 1
    restored.write_text(json.dumps(rows))
    payload['progress']={'before_height':before,'after_height':rows['height'],'new_block_mined_after_restart':True}
if mode=='trace':
    payload['endpoint_evidence']={'scope':'process-trace','target_connected':True,
        'unexpected_connections':0,'trace_fingerprint_sha256':digest('trace fixture')}
if mode in ('local-only','local-trace','local-positive'):
    payload['endpoint_evidence']={'scope':'local-only','target_connected':mode=='local-positive','unexpected_connections':0}
    if mode=='local-trace': payload['endpoint_evidence']['trace_fingerprint_sha256']=digest('local trace fixture')
if mode=='no-endpoint': payload.pop('endpoint_evidence',None)
if mode=='unexpected':
    payload['endpoint_evidence']={'scope':'process-trace','target_connected':True,
        'unexpected_connections':1,'trace_fingerprint_sha256':digest('unexpected trace fixture')}
if mode=='assert-only':
    assert False, 'optimization must not bypass a domain check'
    payload.update(assertion_mode='python-assert',assertions_enabled=__debug__)
if mode=='counting': payload={k:payload[k] for k in ('status','endpoint','fallback_used','count')}
print('SECRET_DO_NOT_RETAIN')
Path(report).write_text(json.dumps(payload))
'''


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.adapter = self.root / 'adapter.py'
        self.adapter.write_text(ADAPTER % STEPS)
        self.binary = str(Path(sys.executable).resolve())
        self.case = {
            'wallet': 'test-client', 'version': '1.0', 'variant': 'default',
            'implementation': 'engine', 'interface': 'native-wallet-query',
            'network': 'zcash-regtest', 'endpoint': 'http://127.0.0.1:32402',
            'require_no_fallback': True, 'restart_scope': 'process-restart',
            'client': {'path': self.binary, 'sha256': self.digest(self.binary)},
            'inputs': {self.binary: self.digest(self.binary), str(self.adapter): self.digest(self.adapter)},
            'steps': {step: {'command': [self.binary, str(self.adapter), step, '{scratch}', '{report}', '{endpoint}', 'normal'],
                             'expect': {'/count': index + 1}} for index, step in enumerate(STEPS)},
        }
        self.manifest = {'schema_version': 2, 'timeout_seconds': 10, 'cases': {'fixture': self.case}}

    def digest(self, path):
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()

    def run_gate(self, expected):
        manifest = self.root / 'manifest.json'
        manifest.write_text(json.dumps(self.manifest))
        output = self.root / 'evidence'
        completed = subprocess.run([sys.executable, str(RUNNER), '--manifest', str(manifest), '--output', str(output)],
                                   capture_output=True, text=True, timeout=15)
        self.assertEqual(completed.returncode, expected, completed.stderr)
        summary = json.loads((output / 'summary.json').read_text())
        self.assertEqual(summary['certified'], expected == 0)
        self.assertFalse((output / 'fixture/scratch').exists())
        for path in output.rglob('*'):
            if path.is_file():
                self.assertNotIn('SECRET_DO_NOT_RETAIN', path.read_text())
        return summary['cases']['fixture']

    def test_ordered_workflow_asserts_persistent_state(self):
        receipt = self.run_gate(0)
        self.assertEqual([s['status'] for s in receipt['steps']], ['completed'] * 10)
        self.assertTrue(all(s['checks'][0]['passed'] for s in receipt['steps']))

    def test_missing_fixture_blocks_certification(self):
        self.case['inputs'][str(self.root / 'missing')] = '0' * 64
        receipt = self.run_gate(1)
        self.assertTrue(all(s['status'] == 'blocked' for s in receipt['steps']))

    def test_missing_step_cannot_certify(self):
        del self.case['steps']['pending']
        receipt = self.run_gate(1)
        self.assertEqual(receipt['steps'][5]['status'], 'no-evidence')
        self.assertEqual(receipt['steps'][6]['status'], 'blocked')

    def test_failed_assertion_cannot_certify(self):
        self.case['steps']['send']['expect']['/count'] = 99
        self.assertEqual(self.run_gate(1)['steps'][4]['status'], 'failed')

    def test_timeout_cannot_certify_and_cleanup_runs(self):
        self.manifest['timeout_seconds'] = 1
        self.case['steps']['bootstrap']['command'][-1] = 'timeout'
        self.case['cleanup'] = {'command': [self.binary, '-c',
            "import pathlib,sys; pathlib.Path(sys.argv[1]).write_text('{\"status\":\"completed\",\"endpoint\":\"http://127.0.0.1:32402\",\"fallback_used\":false,\"cleaned\":true,\"assertions_enabled\":true,\"assertion_mode\":\"always-on\"}')", '{report}'], 'expect': {'/cleaned': True}}
        receipt = self.run_gate(1)
        self.assertIn('timeout', receipt['steps'][0]['problem'])
        self.assertEqual(receipt['cleanup']['status'], 'completed')

    def test_fallback_cannot_certify_trusted_endpoint(self):
        self.case['steps']['bootstrap']['command'][-1] = 'fallback'
        self.assertEqual(self.run_gate(1)['steps'][0]['status'], 'failed')

    def test_simulated_profile_cannot_certify_real_wallet(self):
        self.case['implementation'] = 'simulated-profile'
        self.assertEqual(self.run_gate(1)['steps'][-1]['status'], 'completed')

    def test_nonzero_exit_cannot_certify(self):
        self.case['steps']['bootstrap']['command'][-1] = 'exit'
        self.assertEqual(self.run_gate(1)['steps'][0]['exit_code'], 7)

    def test_unsupported_cannot_certify(self):
        self.case['steps']['bootstrap']['command'][-1] = 'unsupported'
        self.assertEqual(self.run_gate(1)['steps'][0]['status'], 'unsupported')

    def test_cleanup_failure_cannot_certify(self):
        self.case['cleanup'] = {'command': [self.binary, '-c', 'raise SystemExit(7)', '{report}'],
                                'expect': {'/cleaned': True}}
        self.assertEqual(self.run_gate(1)['cleanup']['status'], 'failed')

    def teardown_command(self, script, args=()):
        script.write_text(
            "import json, sys\nfrom pathlib import Path\n"
            "Path(sys.argv[1]).write_text(json.dumps({'status':'completed', 'endpoint':sys.argv[2], "
            "'fallback_used':False, 'assertion_mode':'always-on', 'assertions_enabled':True, 'cleaned':True}))\n"
            "for path in sys.argv[3:]: Path(path).write_text('changed')\n"
        )
        self.case['inputs'][str(script)] = self.digest(script)
        return {'command': [self.binary, str(script), '{report}', '{endpoint}', *map(str, args)],
                'input_paths': [], 'expect': {'/cleaned': True}}

    def test_cleanup_input_change_fails_qualification(self):
        fixture = self.root / 'fixture'
        fixture.write_text('initial')
        self.case['inputs'][str(fixture)] = self.digest(fixture)
        self.case['cleanup'] = self.teardown_command(self.root / 'teardown.py', [fixture])
        receipt = self.run_gate(1)
        self.assertEqual(receipt['cleanup']['status'], 'completed')
        self.assertIn('changed', receipt['postflight'])

    def test_cleanup_releases_resources_after_unrelated_input_changes(self):
        self.case['cleanup'] = self.teardown_command(self.root / 'teardown.py')
        self.case['prepare'] = self.teardown_command(self.root / 'prepare.py', [self.adapter])
        receipt = self.run_gate(1)
        self.assertEqual(receipt['cleanup']['status'], 'completed')
        self.assertIn('changed', receipt['postflight'])

    def test_changed_teardown_script_cannot_run(self):
        teardown = self.root / 'teardown.py'
        self.case['cleanup'] = self.teardown_command(teardown)
        self.case['prepare'] = self.teardown_command(self.root / 'prepare.py', [teardown])
        receipt = self.run_gate(1)
        self.assertEqual(receipt['cleanup']['status'], 'failed')
        self.assertFalse(receipt['cleanup'].get('command_entered', False))

    def test_reopen_does_not_certify_process_restart(self):
        self.case['steps']['restart']['command'][-1] = 'wallet-reopen'
        receipt = self.run_gate(1)
        self.assertFalse(receipt['process_restart_certified'])
        self.assertEqual(receipt['steps'][8]['status'], 'failed')

    def test_unspecified_restart_is_not_process_restart_evidence(self):
        self.case['restart_scope'] = 'unspecified'
        self.case['steps']['restart']['command'][-1] = 'unspecified'
        receipt = self.run_gate(1)
        self.assertTrue(receipt['workflow_completed'])
        self.assertFalse(receipt['process_restart_certified'])
        self.assertFalse(receipt['wallet_reopen_certified'])

    def test_counting_only_report_cannot_qualify(self):
        for step in self.case['steps'].values(): step['command'][-1] = 'counting'
        self.assertFalse(self.run_gate(1)['certified'])

    def test_legacy_counting_workflow_is_not_qualification(self):
        self.manifest['schema_version'] = 1
        for step in self.case['steps'].values(): step['command'][-1] = 'counting'
        # Preserve only the general workflow contract in v1.
        self.case['restart_scope'] = 'unspecified'
        receipt = self.run_gate(1)
        self.assertTrue(receipt['workflow_completed'])
        self.assertFalse(receipt['certified'])

    def test_unrelated_pending_transaction_cannot_qualify(self):
        self.case['steps']['pending']['command'][-1] = 'mismatch'
        receipt = self.run_gate(1)
        self.assertTrue(receipt['workflow_completed'])
        self.assertFalse(receipt['transaction_linkage_certified'])
        self.assertNotEqual(receipt['steps'][4]['transaction']['fingerprint_sha256'],
                            receipt['steps'][5]['transaction']['fingerprint_sha256'])

    def test_scanned_height_must_match_canonical_mined_height(self):
        self.case['steps']['scanned']['command'][-1] = 'wrong-height'
        self.assertFalse(self.run_gate(1)['transaction_linkage_certified'])

    def test_reopen_same_database_is_not_restore(self):
        self.case['steps']['restore']['command'][-1] = 'no-restore'
        receipt = self.run_gate(1)
        self.assertFalse(receipt['restore_certified'])
        self.assertEqual(receipt['steps'][1]['status'], 'failed')

    def test_reopen_without_new_progress_cannot_qualify(self):
        self.case['steps']['resume']['command'][-1] = 'no-progress'
        self.assertFalse(self.run_gate(1)['resume_progress_certified'])

    def test_fallback_allowed_is_not_target_endpoint_certification(self):
        self.case['require_no_fallback'] = False
        self.case['steps']['bootstrap']['command'][-1] = 'fallback'
        receipt = self.run_gate(1)
        self.assertTrue(receipt['workflow_completed'])
        self.assertFalse(receipt['target_endpoint_certified'])

    def test_configured_endpoint_and_trace_are_separate_claims(self):
        receipt = self.run_gate(0)
        self.assertTrue(receipt['target_endpoint_certified'])
        self.assertFalse(receipt['endpoint_trace_certified'])

    def test_trace_claim_retains_its_fingerprint(self):
        for step in self.case['steps'].values(): step['command'][-1] = 'trace'
        receipt = self.run_gate(0)
        self.assertTrue(receipt['endpoint_trace_certified'])
        self.assertEqual(len(receipt['steps'][0]['endpoint_evidence']['trace_fingerprint_sha256']), 64)

    def test_optimized_assert_only_adapter_cannot_certify(self):
        self.case['steps']['bootstrap']['command'].insert(1, '-O')
        self.case['steps']['bootstrap']['command'][-1] = 'assert-only'
        self.assertEqual(self.run_gate(1)['steps'][0]['status'], 'failed')

    def test_optimized_always_on_checks_still_qualify(self):
        for step in self.case['steps'].values(): step['command'].insert(1, '-O')
        self.assertTrue(self.run_gate(0)['resume_progress_certified'])

    def test_preflight_pin_mismatch_does_not_enter_cleanup(self):
        sentinel = self.root / 'sentinel'
        self.case['_command_entered'] = True
        self.case['inputs'][str(self.adapter)] = '0' * 64
        self.case['cleanup'] = {'command':[self.binary, '-c',
            'from pathlib import Path; import sys; Path(sys.argv[1]).touch()', str(sentinel), '{report}'],
            'expect':{'/cleaned':True}}
        receipt = self.run_gate(1)
        self.assertFalse(sentinel.exists())
        self.assertEqual(receipt['cleanup']['reason_code'], 'no-pinned-command-entered')

    def test_mixed_online_and_local_steps_observe_a_target_connection(self):
        for name in ('restore','pending','scanned','restart','resume'):
            self.case['steps'][name]['command'][-1] = 'local-only'
        receipt = self.run_gate(0)
        self.assertTrue(receipt['target_connection_observed'])
        self.assertFalse(receipt['steps'][1]['endpoint_evidence']['target_connected'])
        self.assertFalse(receipt['endpoint_trace_certified'])

    def test_all_offline_steps_cannot_qualify_target_connection(self):
        for step in self.case['steps'].values(): step['command'][-1] = 'local-only'
        receipt = self.run_gate(1)
        self.assertTrue(receipt['workflow_completed'])
        self.assertFalse(receipt['target_connection_observed'])
        self.assertFalse(receipt['target_endpoint_certified'])

    def test_missing_observations_are_not_more_favorable_than_offline(self):
        for step in self.case['steps'].values(): step['command'][-1] = 'no-endpoint'
        receipt = self.run_gate(1)
        self.assertTrue(receipt['workflow_completed'])
        self.assertFalse(receipt['target_connection_observed'])
        self.assertFalse(receipt['target_endpoint_certified'])

    def test_trace_can_include_proven_socketless_steps(self):
        for step in self.case['steps'].values(): step['command'][-1] = 'trace'
        self.case['steps']['resume']['command'][-1] = 'local-trace'
        receipt = self.run_gate(0)
        self.assertTrue(receipt['target_connection_observed'])
        self.assertTrue(receipt['endpoint_trace_certified'])

    def test_unexpected_connection_cannot_qualify(self):
        self.case['steps']['send']['command'][-1] = 'unexpected'
        receipt = self.run_gate(1)
        self.assertTrue(receipt['workflow_completed'])
        self.assertFalse(receipt['target_endpoint_certified'])

    def test_local_only_cannot_declare_a_positive_connection(self):
        self.case['steps']['bootstrap']['command'][-1] = 'local-positive'
        self.assertEqual(self.run_gate(1)['steps'][0]['status'], 'failed')

    def test_prepare_connection_does_not_qualify_offline_wallet_steps(self):
        for step in self.case['steps'].values(): step['command'][-1] = 'local-only'
        self.case['prepare'] = {'command':[self.binary,'-c',
            'import json,sys; from pathlib import Path; Path(sys.argv[1]).write_text(json.dumps({"status":"completed","endpoint":"http://127.0.0.1:32402","fallback_used":False,"assertions_enabled":True,"assertion_mode":"always-on","prepared":True,"endpoint_evidence":{"scope":"configured","target_connected":True,"unexpected_connections":0}}))', '{report}'],
            'expect':{'/prepared':True}}
        receipt = self.run_gate(1)
        self.assertTrue(receipt['workflow_completed'])
        self.assertFalse(receipt['target_connection_observed'])

    def test_changed_client_cannot_certify(self):
        self.case['inputs'][str(self.adapter)] = '0' * 64
        self.assertIn('blocked', self.run_gate(1)['preflight'])


if __name__ == '__main__':
    unittest.main()
