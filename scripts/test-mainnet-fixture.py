#!/usr/bin/env python3
"""Exercise the Mainnet fixture restore scripts at their command lines.

The workstation script runs against a stub gcloud that plays the guest's side.
The route check runs against a fake reader: a metrics endpoint, a grpcurl stub
that serves blocks and moves the canonical read counters, and a process that
holds segment files open. Guest tests run only as a non-root user, because
restore-guest.sh changes the host once it is past its early checks.
"""

import hashlib
import http.server
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import tomllib
import unittest

ROOT = Path(__file__).resolve().parent / 'mainnet-fixture'
RESTORE = ROOT / 'restore-fixture.sh'
GUEST = ROOT / 'guest' / 'restore-guest.sh'
GATE = ROOT / 'guest' / 'fixture-gate.py'
ROUTE = ROOT / 'guest' / 'route-check.py'
STACK = ROOT / 'guest' / 'stack.sh'
FIXTURE = json.loads((ROOT / 'testdata' / 'fixture-905dac4e-signed-history.json').read_text())
OLD_RECEIPT = json.loads((ROOT / 'testdata' / 'guest-receipt-476-zfx476-d.json').read_text())
TICKERS = ('rocksdb.number.multiget.keys.read', 'rocksdb.number.multiget.bytes.read', 'rocksdb.db.iter.bytes.read',
           'rocksdb.number.db.seek', 'rocksdb.number.db.next', 'rocksdb.bytes.read',
           'rocksdb.block.cache.data.hit', 'rocksdb.block.cache.data.miss')


def stores(fixture):
    return next(c for c in fixture['components'] if c['kind'] == 'stores')


def write_json(path, value):
    path.write_text(json.dumps(value))
    return path


def run(command, **kwargs):
    return subprocess.run([str(c) for c in command], capture_output=True, text=True, **kwargs)


def passing_route(fixture):
    history = stores(fixture)['signed_history']
    last = history['highest_height']
    part = {'passed': True}
    return {
        'declared': True, 'proof': 'external', 'passed': True, 'failed': [],
        'manifest_sha256': history['manifest_sha256'], 'publisher_key_sha256': history['publisher_key_sha256'],
        'segment_count': history['segment_count'], 'first_height': 1, 'last_height': last,
        'admission': {'passed': True, 'open_segment_files': history['segment_count'], 'matches_manifest': True},
        'covered': dict(part, range=[1730000, 1730099]), 'boundary_last': dict(part, range=[last, last]),
        'boundary_next': dict(part, range=[last + 1, last + 1]), 'canonical_control': dict(part),
        'refusal': dict(part)}


class Workspace(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix='zfx-test-'))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)


class WorkstationRestore(Workspace):
    """restore-fixture.sh with a stub gcloud that plays the guest's side."""

    STUB = r'''#!/usr/bin/env bash
d=$STUB_DIR
printf '%s\n' "$*" >> "$d/calls"
case "$1 $2" in
  "compute snapshots") echo READY; exit 0 ;;
  "storage cat") cat "$d/stage.sha256"; exit 0 ;;
  "compute images") echo https://example.invalid/image; exit 0 ;;
  "compute instances") exit 0 ;;
  "auth print-access-token") echo test-token; exit 0 ;;
  "compute ssh") ;;
  *) exit 1 ;;
esac
cmd=""; while [[ $# -gt 0 ]]; do [[ $1 == --command ]] && cmd=$2; shift; done
case $cmd in
  true) exit 0 ;;
  *"storage cp"*) exit 0 ;;
  *"/dev/stdin /root/.gcs-token"*) cat > /dev/null; exit 0 ;;
  *"tee /tmp/zfx-stage/fixture.json"*) cat > /dev/null; exit 0 ;;
  *systemd-run*) echo launch >> "$d/launches"; exit 0 ;;
  *outcome.json*) if [[ -f $d/outcome.json ]]; then cat "$d/outcome.json"; else cat "$d/status" 2>/dev/null || echo RUNNING; fi; exit 0 ;;
  *journalctl*) echo "journal tail"; exit 0 ;;
esac
exit 1
'''

    def setUp(self):
        super().setUp()
        self.bin = self.tmp / 'bin'
        self.bin.mkdir()
        (self.bin / 'gcloud').write_text(self.STUB)
        (self.bin / 'gcloud').chmod(0o755)
        self.stub = self.tmp / 'stub'
        self.stub.mkdir()
        (self.stub / 'stage.sha256').write_text(FIXTURE['stage_sha256'] + '\n')

    def restore(self, fixture=FIXTURE, outcome=None, status=None, extra=(), receipt=None, hide_jsonschema=False):
        if outcome is not None:
            write_json(self.stub / 'outcome.json', outcome)
        if status is not None:
            (self.stub / 'status').write_text(status + '\n')
        if hide_jsonschema:
            python = self.bin / 'python3'
            python.write_text('#!/bin/sh\ncase "$*" in *jsonschema*) exit 1 ;; esac\nexec %s "$@"\n' % sys.executable)
            python.chmod(0o755)
        receipt = receipt or self.tmp / 'receipt.json'
        env = dict(os.environ, PATH=f'{self.bin}:{os.environ["PATH"]}', STUB_DIR=str(self.stub))
        done = run([RESTORE, '--fixture', write_json(self.tmp / 'fixture.json', fixture), '--stage', 'gs://stub/stage.tar.zst',
                    '--name', 'fx490-test', '--receipt', receipt, '--poll-seconds', '1', *extra], env=env, timeout=600)
        return done, receipt

    def launches(self):
        path = self.stub / 'launches'
        return len(path.read_text().splitlines()) if path.exists() else 0

    def test_old_guest_receipt_fails_a_catalog_fixture(self):
        """Finding 1: the real #476 guest receipt has no route proof."""
        done, receipt = self.restore(outcome={'status': 'succeeded', 'receipt': OLD_RECEIPT})
        self.assertEqual(done.returncode, 1, done.stderr)
        self.assertFalse(receipt.exists())
        failed = json.loads(Path(f'{receipt}.failed').read_text())
        self.assertEqual(failed['workstation_step'], 'accept')
        self.assertIn('no route proof', failed['reason'])

    def test_stage_without_attempt_outcomes_fails(self):
        """Finding 1: an older stage writes no attempt outcome, so the restore fails instead of waiting forever."""
        done, receipt = self.restore(status='GONE')
        self.assertEqual(done.returncode, 1, done.stderr)
        self.assertIn('without writing an outcome', json.loads(Path(f'{receipt}.failed').read_text())['reason'])

    def test_stage_pin_rejects_another_stage(self):
        """Finding 1: a catalog fixture pins its stage."""
        (self.stub / 'stage.sha256').write_text('6' * 64 + '\n')
        done, _ = self.restore(outcome={'status': 'succeeded', 'receipt': OLD_RECEIPT})
        self.assertEqual(done.returncode, 1)
        self.assertIn('the fixture pins', done.stderr)
        self.assertEqual(self.launches(), 0)

    def test_catalog_fixture_without_stage_pin_is_refused(self):
        fixture = json.loads(json.dumps(FIXTURE))
        del fixture['stage_sha256']
        done, _ = self.restore(fixture=fixture)
        self.assertEqual(done.returncode, 1)
        self.assertIn('must pin stage_sha256', done.stderr)

    def test_passing_route_proof_is_accepted(self):
        receipt_body = dict(OLD_RECEIPT, signed_history=passing_route(FIXTURE))
        done, receipt = self.restore(outcome={'status': 'succeeded', 'receipt': receipt_body})
        self.assertEqual(done.returncode, 0, done.stderr)
        written = json.loads(receipt.read_text())
        self.assertTrue(written['accepted'])
        self.assertEqual(written['guest']['signed_history']['last_height'], 3592192)
        self.assertEqual(self.launches(), 1)

    def test_route_proof_must_match_the_fixture(self):
        for field, value in (('manifest_sha256', '0' * 64), ('segment_count', 876), ('last_height', 3592191)):
            with self.subTest(field=field):
                route = dict(passing_route(FIXTURE), **{field: value})
                receipt = self.tmp / f'receipt-{field}.json'
                done, _ = self.restore(outcome={'status': 'succeeded', 'receipt': dict(OLD_RECEIPT, signed_history=route)},
                                       receipt=receipt)
                self.assertEqual(done.returncode, 1)
                self.assertIn(field, Path(f'{receipt}.failed').read_text())

    def test_failed_route_report_fails(self):
        route = dict(passing_route(FIXTURE), passed=False, failed=['boundary_next'])
        route['boundary_next'] = dict(route['boundary_next'], passed=False)
        done, receipt = self.restore(outcome={'status': 'succeeded', 'receipt': dict(OLD_RECEIPT, signed_history=route)})
        self.assertEqual(done.returncode, 1)
        self.assertIn('boundary_next', Path(f'{receipt}.failed').read_text())

    def test_failed_guest_outcome_is_recorded(self):
        """Finding 5: a guest failure leaves a nonempty failure receipt with the step."""
        done, receipt = self.restore(outcome={'status': 'failed', 'step': 'check_history', 'reason': 'manifest differs', 'receipt': None})
        self.assertEqual(done.returncode, 1)
        failed = json.loads(Path(f'{receipt}.failed').read_text())
        self.assertIn('check_history', failed['reason'])
        self.assertEqual(failed['guest_outcome']['step'], 'check_history')

    def test_poll_has_a_deadline(self):
        """Finding 5: a guest that never finishes fails the restore."""
        done, receipt = self.restore(status='RUNNING', extra=['--guest-timeout', '2'])
        self.assertEqual(done.returncode, 1)
        self.assertIn('did not finish within 2 seconds', Path(f'{receipt}.failed').read_text())

    def test_existing_receipt_path_is_refused(self):
        """Finding 5: an old success at the receipt path cannot survive a later failure."""
        receipt = self.tmp / 'receipt.json'
        receipt.write_text('{"accepted": true}')
        done, _ = self.restore(receipt=receipt)
        self.assertEqual(done.returncode, 1)
        self.assertIn('already exists', done.stderr)
        self.assertFalse((self.stub / 'calls').exists())

    def test_non_object_signed_history_fails_preflight_without_jsonschema(self):
        """Finding 2: the mandatory preflight, not the optional schema check, refuses it."""
        for value in (False, 'catalog', None):
            with self.subTest(value=value):
                fixture = json.loads(json.dumps(FIXTURE))
                stores(fixture)['signed_history'] = value
                done, _ = self.restore(fixture=fixture, hide_jsonschema=True, receipt=self.tmp / f'r-{value}.json')
                self.assertEqual(done.returncode, 1)
                self.assertIn('signed_history must be an object', done.stderr)
                self.assertFalse((self.stub / 'calls').exists())

    def test_catalog_ending_at_the_settled_tip_is_accepted(self):
        """Finding 4: only the visible tip bounds the catalog."""
        fixture = json.loads(json.dumps(FIXTURE))
        stores(fixture)['signed_history']['highest_height'] = fixture['chain']['settled_height']
        route = passing_route(fixture)
        done, _ = self.restore(fixture=fixture, outcome={'status': 'succeeded', 'receipt': dict(OLD_RECEIPT, signed_history=route)})
        self.assertEqual(done.returncode, 0, done.stderr)

    def test_fixture_without_a_catalog_accepts_a_receipt_without_a_route(self):
        fixture = json.loads(json.dumps(FIXTURE))
        component = stores(fixture)
        del component['signed_history']
        for key in ('catalog', 'publisher_key'):
            del component['paths'][key]
        done, _ = self.restore(fixture=fixture, outcome={'status': 'succeeded', 'receipt': OLD_RECEIPT})
        self.assertEqual(done.returncode, 0, done.stderr)


def make_catalog(directory, first, blocks_per_segment, segments):
    """Writes a catalog directory with segment files named by their digests and returns the manifest bytes."""
    directory.mkdir(parents=True, exist_ok=True)
    entries, height = [], first
    for index in range(segments):
        content = os.urandom(65536) + bytes([index])
        digest = hashlib.sha256(content).digest()
        (directory / f'segment-{digest.hex()}.bin').write_bytes(content)
        entries.append({'first': height, 'last': height + blocks_per_segment - 1, 'sha256': list(digest)})
        height += blocks_per_segment
    manifest = json.dumps({'manifest': json.dumps({'format': 'zinder.shielded-range.catalog.v1', 'segments': entries}),
                           'signature': '00' * 64}).encode()
    (directory / 'current.json').write_bytes(manifest)
    (directory / '.publisher-head.json').write_text('{}')
    return manifest, height - 1


def catalog_fixture(mount, manifest, key, last, segments, visible):
    fixture = json.loads(json.dumps(FIXTURE))
    fixture['chain']['visible_height'] = visible
    component = stores(fixture)
    component['mount_path'] = str(mount)
    component['signed_history'].update(manifest_sha256=hashlib.sha256(manifest).hexdigest(),
                                       publisher_key_sha256=hashlib.sha256(key).hexdigest(),
                                       segment_count=segments, highest_height=last)
    return fixture


class ManifestGate(Workspace):
    """fixture-gate.py verify-manifest derives bounds from the verified manifest bytes."""

    def setUp(self):
        super().setUp()
        self.mount = self.tmp / 'fixture data'
        self.manifest, self.last = make_catalog(self.mount / 'catalog', 1000, 50, 3)
        self.key = os.urandom(32)
        (self.mount / 'publisher.pub').write_bytes(self.key)

    def verify(self, fixture):
        return run([sys.executable, '-I', GATE, 'verify-manifest', write_json(self.tmp / 'f.json', fixture), self.tmp / 'bounds.json'])

    def test_bounds_come_from_the_manifest(self):
        """Findings 3 and 8: bounds and files come from the verified bytes, under a path with a space."""
        done = self.verify(catalog_fixture(self.mount, self.manifest, self.key, self.last, 3, self.last + 500))
        self.assertEqual(done.returncode, 0, done.stderr)
        bounds = json.loads((self.tmp / 'bounds.json').read_text())
        self.assertEqual((bounds['first_height'], bounds['last_height'], bounds['segments']), (1000, 1149, 3))
        self.assertEqual(len(bounds['segment_files']), 3)

    def test_declared_upper_bound_must_equal_the_manifest(self):
        """Finding 3: a declared height one below the real end is refused."""
        done = self.verify(catalog_fixture(self.mount, self.manifest, self.key, self.last - 1, 3, self.last + 500))
        self.assertEqual(done.returncode, 1)
        self.assertIn('manifest ends at 1149', done.stderr)

    def test_changed_manifest_or_key_is_refused(self):
        fixture = catalog_fixture(self.mount, self.manifest, self.key, self.last, 3, self.last + 500)
        (self.mount / 'publisher.pub').write_bytes(os.urandom(32))
        done = self.verify(fixture)
        self.assertEqual(done.returncode, 1)
        self.assertIn('publisher_key_sha256', done.stderr)


class FakeReader:
    """A metrics endpoint, a grpcurl stub, and a process holding segment files open."""

    GRPCURL = r'''#!/usr/bin/env python3
import json, os, sys
state_path = os.environ['FAKE_READER_STATE']
state = json.load(open(state_path))
request = json.loads(sys.argv[sys.argv.index('-d') + 1])
start, end = request['start']['height'], request['end']['height']
native = bool(request.get('poolTypes'))
if state.get('reset_on') == start:
    state['tickers'][state['reset_ticker']] = 0.0
for height in range(start, end + 1):
    from_catalog = state['first'] <= height <= state['served_last'] and not native
    from_tail = height > state['last'] and state['tail_warm'] and not native
    if not (from_catalog or from_tail):
        for ticker, step in (('rocksdb.number.multiget.keys.read', 1), ('rocksdb.number.multiget.bytes.read', 300)):
            if ticker in state['tickers']:
                state['tickers'][ticker] += step
    print(json.dumps({'height': str(height), 'hash': 'h%d' % height}, indent=1))
json.dump(state, open(state_path, 'w'))
'''

    def __init__(self, tmp, first, last, files, served_last=None, tail_warm=False, drop=(), reset=None):
        self.state_path = tmp / 'reader-state.json'
        tickers = {t: 1000.0 for t in TICKERS if t not in drop}
        state = {'first': first, 'last': last, 'served_last': last if served_last is None else served_last,
                 'tail_warm': tail_warm, 'tickers': tickers}
        if reset:
            state['reset_ticker'], state['reset_on'] = reset
        write_json(self.state_path, state)
        self.refreshes = 0
        reader = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                reader.refreshes += 1
                state = json.loads(reader.state_path.read_text())
                lines = [f'zinder_wallet_serving_pair_publisher_refresh_total{{status="ok",error_class="none"}} {reader.refreshes}']
                for ticker, value in state['tickers'].items():
                    lines.append(f'zinder_store_rocksdb_ticker{{store_role="canonical_secondary",ticker="{ticker}"}} {value}')
                body = ('\n'.join(lines) + '\n').encode()
                self.send_response(200)
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        self.server = http.server.HTTPServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.ops = f'127.0.0.1:{self.server.server_address[1]}'
        self.bin = tmp / 'reader-bin'
        self.bin.mkdir()
        (self.bin / 'grpcurl').write_text(self.GRPCURL)
        (self.bin / 'grpcurl').chmod(0o755)
        holder = 'import sys, time\nfiles = [open(p, "rb") for p in sys.argv[1:]]\nprint("ready", flush=True)\ntime.sleep(600)\n'
        self.process = subprocess.Popen([sys.executable, '-c', holder, *map(str, files)], stdout=subprocess.PIPE, text=True)
        self.process.stdout.readline()

    def close(self):
        self.process.kill()
        self.process.wait()
        self.server.shutdown()


class RouteCheck(Workspace):
    """route-check.py against a fake reader."""

    def setUp(self):
        super().setUp()
        self.mount = self.tmp / 'mount'
        manifest, self.last = make_catalog(self.mount / 'catalog', 1000, 50, 3)
        key = os.urandom(32)
        (self.mount / 'publisher.pub').write_bytes(key)
        self.fixture = catalog_fixture(self.mount, manifest, key, self.last, 3, self.last + 500)
        done = run([sys.executable, '-I', GATE, 'verify-manifest', write_json(self.tmp / 'f.json', self.fixture), self.tmp / 'bounds.json'])
        self.assertEqual(done.returncode, 0, done.stderr)
        self.bounds = json.loads((self.tmp / 'bounds.json').read_text())
        refusal = self.tmp / 'refusing-reader'
        refusal.write_text('#!/bin/sh\necho "ERROR compat run failed error=invalid configuration: shielded range admission failed: '
                           'message: \\"shielded range manifest signature verification failed\\""\nexit 1\n')
        refusal.chmod(0o755)
        self.refusal = refusal

    def check(self, hold=None, reference=None, **reader):
        files = [self.mount / 'catalog' / name for name in (hold if hold is not None else self.bounds['segment_files'])]
        fake = FakeReader(self.tmp, self.bounds['first_height'], self.last, files, **reader)
        self.addCleanup(fake.close)
        out = self.tmp / 'route'
        extra = ['--reference-range', reference[0], '--reference', reference[1]] if reference else []
        done = run([sys.executable, '-I', ROUTE, '--grpc', 'fake:0', '--ops', fake.ops, '--pid', fake.process.pid,
                    '--bounds', self.tmp / 'bounds.json', '--tip', self.last + 500, '--out', out,
                    '--refusal-binary', self.refusal, '--refusal-config', '/dev/null', '--refusal-timeout', '120', *extra],
                   env=dict(os.environ, PATH=f'{fake.bin}:{os.environ["PATH"]}', FAKE_READER_STATE=str(fake.state_path)), timeout=900)
        return done, json.loads((out / 'route-check.json').read_text())

    def gate(self, report):
        return run([sys.executable, '-I', GATE, 'route-verdict', write_json(self.tmp / 'report.json', report)])

    def test_correct_reader_passes_without_page_residency(self):
        """Finding 6: the verdict rests on counters; the fake reader never reads segment pages."""
        done, report = self.check()
        self.assertEqual(done.returncode, 0, report)
        self.assertEqual(self.gate(report).returncode, 0)
        self.assertEqual(report['boundary_last']['range'], [self.last, self.last])
        self.assertEqual(report['boundary_next']['range'], [self.last + 1, self.last + 1])
        self.assertEqual(report['covered']['range'], [1000, 1099])
        self.assertEqual(report['covered']['supporting_segment_resident_bytes'], [0, 0])
        self.assertEqual(report['manifest_sha256'], self.bounds['manifest_sha256'])
        self.assertEqual(report['proof'], 'external')

    def test_reader_that_ends_one_height_early_fails_the_boundary(self):
        """Finding 3: the last height read from canonical storage fails boundary_last."""
        done, report = self.check(served_last=self.last - 1)
        self.assertEqual(done.returncode, 1)
        self.assertIn('boundary_last', report['failed'])
        self.assertNotEqual(self.gate(report).returncode, 0)

    def test_warm_tail_fails_closed_and_control_still_sees_canonical_reads(self):
        """Finding 4: the native-pool control does not depend on tail warmth."""
        done, report = self.check(tail_warm=True)
        self.assertEqual(report['failed'], ['boundary_next'])
        self.assertTrue(report['canonical_control']['passed'])
        self.assertEqual(report['canonical_control']['pool_types'], [1, 2, 3])

    def test_open_files_must_equal_the_manifest_set(self):
        done, report = self.check(hold=self.bounds['segment_files'][:2])
        self.assertIn('admission', report['failed'])

    def test_missing_counter_fails(self):
        """Finding 6: a missing counter is an error, not a zero."""
        done, report = self.check(drop=('rocksdb.number.multiget.bytes.read',))
        self.assertEqual(done.returncode, 1)
        self.assertIn('rocksdb.number.multiget.bytes.read is missing', report['error'])

    def test_decreasing_counter_fails(self):
        done, report = self.check(reset=('rocksdb.number.multiget.keys.read', self.last + 1))
        self.assertEqual(done.returncode, 1)
        self.assertIn('decreased', report['error'])

    def test_counters_are_reported_separately(self):
        """Finding 6: keys and bytes are never summed."""
        done, report = self.check()
        delta = report['canonical_control']['canonical_payload_read_delta']
        self.assertEqual(delta['rocksdb.number.multiget.keys.read'], 100)
        self.assertEqual(delta['rocksdb.number.multiget.bytes.read'], 30000)
        self.assertNotIn('canonical_payload_reads', report['covered'])

    def test_reference_bytes_are_compared(self):
        reference = self.tmp / 'reference.json'
        reference.write_text('not the response')
        done, report = self.check(reference=('1000:1099', reference))
        self.assertIn('covered', report['failed'])
        self.assertFalse(report['covered']['matches_reference'])

    def test_reference_outside_the_catalog_falls_back_to_its_first_heights(self):
        """Finding 4: a catalog that starts above the sample range still gets a covered probe."""
        done, report = self.check(reference=('1730000:1730099', self.tmp / 'unused.json'))
        self.assertEqual(done.returncode, 0, report)
        self.assertEqual(report['covered']['range'], [1000, 1099])
        self.assertIsNone(report['covered']['byte_reference'])

    def test_changed_manifest_fails_and_reports_the_verified_hash(self):
        """Finding 7: the reported hash is the one verified before startup."""
        (self.mount / 'catalog' / 'current.json').write_text('{}')
        done, report = self.check()
        self.assertIn('admission', report['failed'])
        self.assertFalse(report['admission']['manifest_unchanged'])
        self.assertEqual(report['manifest_sha256'], self.bounds['manifest_sha256'])

    def test_verdict_needs_every_part(self):
        """Finding 7: a report that says passed but lacks a part does not pass."""
        report = {'proof': 'external', 'passed': True, 'failed': [], 'admission': {'passed': True}}
        self.assertEqual(self.gate(report).returncode, 1)
        unreadable = run([sys.executable, '-I', GATE, 'route-verdict', self.tmp / 'missing.json'])
        self.assertEqual(unreadable.returncode, 1)


@unittest.skipIf(os.geteuid() == 0, 'restore-guest.sh changes the host when run as root')
class GuestEarlyExit(Workspace):
    """restore-guest.sh records early failures and runs one attempt per host."""

    def guest(self, fixture, attempt):
        env = dict(os.environ, ZFX_LAUNCH_RECORD=str(self.tmp / 'launch'))
        done = run(['bash', GUEST, write_json(self.tmp / 'fixture.json', fixture), 'stores', self.tmp / 'attempts' / attempt],
                   env=env, timeout=60)
        outcome_path = self.tmp / 'attempts' / attempt / 'outcome.json'
        return done, json.loads(outcome_path.read_text()) if outcome_path.exists() else None

    def test_early_failure_writes_an_outcome_with_its_step(self):
        """Finding 5: a non-root bootstrap fails, and the outcome says where."""
        done, outcome = self.guest(FIXTURE, 'a1')
        self.assertEqual(done.returncode, 1)
        self.assertEqual((outcome['status'], outcome['step']), ('failed', 'bootstrap'))
        self.assertTrue(outcome['reason'])

    def test_non_object_signed_history_fails_at_the_guest(self):
        """Finding 2: the guest applies the same rule."""
        fixture = json.loads(json.dumps(FIXTURE))
        stores(fixture)['signed_history'] = False
        done, outcome = self.guest(fixture, 'a1')
        self.assertEqual((outcome['status'], outcome['step']), ('failed', 'check_fixture'))
        self.assertIn('must be an object', outcome['reason'])

    def test_one_attempt_per_host(self):
        """Finding 5: a repeated launch leaves the first outcome alone; another attempt is refused."""
        self.guest(FIXTURE, 'a1')
        first = (self.tmp / 'attempts' / 'a1' / 'outcome.json').read_text()
        done, _ = self.guest(FIXTURE, 'a1')
        self.assertEqual(done.returncode, 0)
        self.assertEqual((self.tmp / 'attempts' / 'a1' / 'outcome.json').read_text(), first)
        done, outcome = self.guest(FIXTURE, 'a2')
        self.assertEqual((outcome['status'], outcome['step']), ('failed', 'launch'))
        self.assertIn('already ran attempt a1', outcome['reason'])


class StackConfig(Workspace):
    """stack.sh, with its system paths redirected into the test directory."""

    def stack(self, history_dir, history_key='/srv/fixture data/publisher.pub'):
        script = STACK.read_text()
        for old, new in (('/etc/zfx/', f'{self.tmp}/etc/zfx/'), ('/etc/systemd/system/', f'{self.tmp}/units/'),
                         ('/var/log/zfx', f'{self.tmp}/log'), ('install -d -o zinder -g zinder -m 0700', 'install -d -m 0700'),
                         ('chown zinder:zinder', 'true'), ('systemctl daemon-reload', 'true')):
            script = script.replace(old, new)
        (self.tmp / 'units').mkdir(exist_ok=True)
        copy = self.tmp / 'stack.sh'
        copy.write_text(script)
        env = dict(os.environ, HISTORY_DIRECTORY=history_dir, HISTORY_PUBLISHER_KEY=history_key, HISTORY_REQUIRE_FS_VERITY='true')
        return run(['bash', copy, 'fixture', self.tmp / 'root', '00', '0', '/run/cookie'], env=env)

    def test_paths_with_spaces_round_trip(self):
        """Finding 8: a path with a space is a valid TOML string."""
        done = self.stack('/srv/fixture data/catalog')
        self.assertEqual(done.returncode, 0, done.stderr)
        config = tomllib.loads((self.tmp / 'etc/zfx/fixture/compat.toml').read_text())
        self.assertEqual(config['compat']['shielded_history'],
                         {'directory': '/srv/fixture data/catalog', 'publisher_key': '/srv/fixture data/publisher.pub',
                          'require_fs_verity': True})

    def test_paths_that_need_escaping_are_refused(self):
        """Finding 8: a quote or backslash would break the TOML string."""
        for path in ('/srv/a"b', '/srv/a\\b'):
            with self.subTest(path=path):
                done = self.stack(path)
                self.assertEqual(done.returncode, 1)
                self.assertIn('cannot appear in TOML', done.stderr)


if __name__ == '__main__':
    unittest.main()
