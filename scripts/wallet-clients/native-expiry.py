#!/usr/bin/env python3
"""Expire a native Zallet chain view in an owned Regtest and verify its complete retry."""

import argparse
from collections import Counter
from contextlib import suppress
import json
import os
from pathlib import Path
import re
import runpy
import signal
import subprocess
import sys
from urllib.request import ProxyHandler, build_opener

import toml

from wallet_fixture import loopback


SCRIPTS = Path(__file__).resolve().parent.parent
operator = runpy.run_path(str(SCRIPTS / 'regtest-fixture.py'))
campaign = runpy.run_path(str(SCRIPTS / 'run-benchmark-campaign.py'))
sha256 = campaign['sha256']
bounded = campaign['run_command']


def require(condition, message):
    if not condition:
        raise ValueError(message)


def native_counts(ops_url):
    counts = Counter()
    with build_opener(ProxyHandler({})).open(ops_url + '/metrics', timeout=5) as response:
        for line in response:
            if not line.startswith(b'zinder_wallet_rpc_requests_total{'):
                continue
            labels = dict(re.findall(r'(\w+)="([^"]*)"', line.decode()))
            if labels.get('surface') == 'wallet_query':
                counts[labels['method'], labels['grpc_code'], labels['error_class']] += int(float(line.rsplit(None, 1)[1]))
    return counts


def delta(before, after):
    require(all(after[key] >= value for key, value in before.items()), 'unexpected metrics reset')
    return [{'method': method, 'grpc_code': code, 'error_class': error_class, 'count': n}
            for (method, code, error_class), n in sorted((after - before).items())]


def exercise(args, project, receipt):
    private = args.output / 'private'
    ready_path = project / 'ready.initial.json'
    ready = json.loads(ready_path.read_text())
    endpoint = loopback(ready['native_wallet_query'])
    ops = loopback(ready['query_ops'])
    require(ready['network'] == 'zcash-regtest' and ready['zinder_commit'] == args.revision
            and ready['raw_blob_policy'] == 'all' and ready['isolation']['peer_count'] == 0
            and not ready['isolation']['default_ipv4_route'], 'unexpected fixture identity')
    initial_tip = ready['current_tip']
    node_rpc = lambda method, *params: operator['rpc'](ready, method, list(params))
    require(node_rpc('getblockhash', initial_tip) == ready['current_hash'], 'node anchor changed')
    receipt['fixture'] = {'network': ready['network'], 'release_revision': ready['zinder_commit'],
                          'release_version': ready['health']['query']['version'],
                          'anchor_height': initial_tip, 'anchor_hash': ready['current_hash'],
                          'ready_sha256': sha256(ready_path), 'raw_blob_policy': ready['raw_blob_policy'],
                          'image_ids': ready['images'], 'node_image': ready['node_image'],
                          'release_binary_sha256': {n: b['sha256'] for n, b in ready['binaries'].items()}}
    wallet = private / 'wallet'
    wallet.mkdir()
    config = private / 'zallet.toml'
    example = subprocess.check_output([str(args.backend), '--datadir=' + str(wallet), 'example-config',
                                       '-o', '-', '--this-is-beta-code-and-you-will-need-to-recreate-the-example-later'],
                                      text=True, stderr=subprocess.DEVNULL, timeout=10)
    conf = toml.loads(example)
    conf.update(backend='zinder', zinder={'wallet_query_endpoint': endpoint})
    conf['consensus'] = {'network': 'regtest', 'regtest_nuparams': [
        f'{branch}:{spec["activationheight"]}' for branch, spec in ready['chain']['upgrades'].items()]}
    config.write_text(toml.dumps(conf))
    barrier = private / 'barrier'
    barrier.mkdir()
    result_path = private / 'driver-result.json'
    start = initial_tip - 1
    os.environ.update(ZIT_ZALLET=str(args.backend), ZIT_ZINDER_ENDPOINT=endpoint, ZIT_ZALLET_CONFIG=str(config),
                      ZIT_CERTIFICATION_DATADIR=str(wallet), ZIT_CERTIFICATION_RESULT=str(result_path),
                      ZIT_REQUESTED_START_HEIGHT=str(start), ZIT_REQUESTED_END_HEIGHT_EXCLUSIVE=str(initial_tip + 1),
                      ZIT_RETRY_END_HEIGHT_EXCLUSIVE=str(initial_tip + 2), ZIT_RANGE_BARRIER_DIR=str(barrier),
                      ZIT_RANGE_REQUEST_PAUSE_START_HEIGHT=str(start))

    def traced(name, test):
        return [sys.executable, str(SCRIPTS / 'trace-wallet-endpoint.py'), '--endpoint-port',
                str(ready['port_base'] + 2), '--output', str(private / name), '--', str(args.driver),
                '--exact', 'chain::tests::' + test, '--ignored', '--nocapture']

    before = native_counts(ops)
    with (private / 'initialize.log').open('w') as log:
        require(bounded(traced('initialize', 'initialize_bounded_scan_wallet'), log, subprocess.STDOUT, 60) == 0,
                'ZIT wallet initialization failed')
    initialized = native_counts(ops)
    receipt['actions']['initialize'] = delta(before, initialized)
    with (private / 'retry.log').open('w') as log:
        with subprocess.Popen(traced('scan', 'expired_epoch_reacquires_complete_bounded_scan'),
                              stdout=log, stderr=subprocess.STDOUT, start_new_session=True) as process:
            try:
                def paused():
                    if process.poll() is not None:
                        raise ChildProcessError('external driver exited before the range barrier')
                    return (barrier / 'range-request-paused.json').exists()

                operator['wait'](paused, bool, 45)
                receipt['actions']['acquire-and-pause'] = delta(initialized, native_counts(ops))
                new_hash, = node_rpc('generate', 1)
                new_height = initial_tip + 1

                def published():
                    health = operator['request'](ops + '/healthz')
                    readiness = operator['request'](ops + '/readyz')
                    return (health['network'] == ready['network'] and health['git_commit'] == args.revision
                            and readiness.get('current_height') == new_height and readiness.get('status') == 'ready')

                operator['wait'](published, bool, 60)
                operator['verify_owner'](ready)
                query_id = operator['compose'](project, 'ps', '-q', 'zinder-query')
                require(re.fullmatch('[a-f0-9]{64}', query_id), 'missing owned query container')
                def started_at():
                    return subprocess.check_output(['docker', 'inspect', '-f', '{{.State.StartedAt}}', query_id],
                                                   text=True, timeout=5).strip()

                prior_start = started_at()
                subprocess.run(['docker', 'restart', '--time', '5', query_id], check=True, timeout=30,
                               stdout=subprocess.DEVNULL)
                operator['wait'](published, bool, 60)
                require(started_at() != prior_start, 'query did not restart')
                reset = native_counts(ops)
                (barrier / 'continue-range-request').touch()
                require(process.wait(timeout=90) == 0, 'external native driver failed its expiry or retry assertions')
                receipt['actions']['stale-read-and-retry'] = delta(reset, native_counts(ops))
            finally:
                if process.poll() is None:
                    with suppress(ProcessLookupError):
                        os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=5)

    for name in ('initialize', 'scan'):
        report = json.loads((private / name / 'report.json').read_text())
        require(report['status'] == 'passed' and report['fallback_used'] is False, 'unexpected client connection')
        receipt['trace_sha256'][name] = sha256(private / name / 'connect.log')

    result = json.loads(result_path.read_text())
    stale = result['expiry_evidence']
    retry = result['retry_evidence']
    attempts = result['range_request_attempts']
    require(result['expiry_source_classification'] == 'IndexerError::ChainEpochPinUnavailable'
            and stale['block_metadata_before'] == stale['block_metadata_after'] == []
            and stale['has_outstanding_scan_work_from_wallet_birthday_through_requested_last_height'],
            'old view did not preserve the pre-scan metadata and pending work')
    require(len(attempts) == 2 and attempts[0]['chain_epoch_id'] != attempts[1]['chain_epoch_id']
            and all(row['requested_start_height_inclusive'] == start for row in attempts)
            and [row['requested_end_height_inclusive'] for row in attempts] == [initial_tip, new_height],
            'driver did not retry the complete requested interval in a new epoch')
    blocks = retry['block_metadata_after']
    heights = list(range(start, new_height + 1))
    require([b['block_height'] for b in blocks] == heights and retry['block_metadata_before'] == []
            and all(b['block_hash'] == node_rpc('getblockhash', b['block_height']) for b in blocks)
            and retry['captured_tip_hash'] == new_hash and retry['captured_tip_height'] == new_height
            and not retry['has_outstanding_scan_work_from_wallet_birthday_through_captured_tip'],
            'persisted retry metadata differs from the canonical interval')
    statuses = {r['grpc_code'] for r in receipt['actions']['stale-read-and-retry']
                if r['method'] == 'FullBlocksInRange'}
    require(statuses >= {'failed_precondition', 'ok'}, 'missing failed old epoch or successful full-range retry')
    receipt.update(result_sha256=sha256(result_path), expired_source=result['expiry_source_classification'],
                   attempts=attempts, persisted_heights=heights,
                   assertions={'expired_metadata_unchanged': True, 'expired_scan_work_still_pending': True,
                               'complete_retry_on_new_epoch': True, 'retry_hashes_match_canonical': True,
                               'retry_scan_work_completed': True})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--driver', required=True, type=Path, help='compiled ZIT zinder_driver.rs test executable')
    parser.add_argument('--driver-sha256', required=True)
    parser.add_argument('--backend', required=True, type=Path, help='ordinary zallet-zinder backend')
    parser.add_argument('--backend-sha256', required=True)
    parser.add_argument('--images', required=True, type=Path)
    parser.add_argument('--bin-dir', type=Path, help='release binaries to overlay on pinned images')
    parser.add_argument('--revision', required=True)
    parser.add_argument('--port-base', type=int, default=48300)
    parser.add_argument('--output', required=True, type=Path, help='new private output directory')
    args = parser.parse_args()
    os.umask(0o077)
    args.driver, args.backend = args.driver.resolve(), args.backend.resolve()
    pins = {args.driver: args.driver_sha256, args.backend: args.backend_sha256}
    require(all(sha256(p) == pin for p, pin in pins.items()), 'driver or backend hash differs')
    args.output = args.output.resolve()
    args.output.mkdir(parents=True)
    private = args.output / 'private'
    private.mkdir()
    project = args.output / 'projects' / ('zinder-fixture-native-expiry-' + os.urandom(6).hex())
    cli = [sys.executable, str(SCRIPTS / 'regtest-fixture.py')]
    scope = ['--project', project.name, '--state-root', str(project.parent)]
    receipt = {'schema_version': 1, 'case': 'zallet-bounded-scan-expiry', 'interface': 'native-wallet-query',
               'status': 'failed', 'fixture_teardown_completed': False, 'client': {'name': 'zallet', 'driver_sha256': args.driver_sha256,
               'backend_sha256': args.backend_sha256, 'driver_feature': 'bounded-scan-certification'},
               'runner_sha256': sha256(Path(__file__)), 'actions': {}, 'trace_sha256': {},
               'method_evidence': 'exclusive native query RPC counters, with a new baseline after query restart'}
    up = cli + ['up', *scope, '--images', str(args.images), '--revision', args.revision,
                '--raw-blob-policy', 'all', '--port-base', str(args.port_base), '--tip', '150']
    if args.bin_dir:
        up.extend(['--bin-dir', str(args.bin_dir)])
    passed = False
    try:
        with (private / 'up.log').open('w') as log:
            require(bounded(up, log, subprocess.STDOUT, 400) == 0, 'fixture startup failed')
        exercise(args, project, receipt)
        require(all(sha256(p) == pin for p, pin in pins.items()), 'driver or backend changed')
        passed = True
    finally:
        try:
            if (project / 'state.json').exists():
                with (private / 'down.log').open('w') as log:
                    receipt['fixture_teardown_completed'] = bounded(cli + ['down', *scope], log, subprocess.STDOUT, 90) == 0
                require(receipt['fixture_teardown_completed'], 'fixture teardown failed')
        finally:
            if passed and receipt['fixture_teardown_completed']:
                receipt['status'] = 'passed'
            campaign['write_json'](args.output / 'receipt.json', receipt)
    require(receipt['status'] == 'passed', 'owned fixture teardown was not verified')


if __name__ == '__main__':
    main()
