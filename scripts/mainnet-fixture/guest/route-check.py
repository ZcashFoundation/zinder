#!/usr/bin/env python3
"""Prove which route a restored lightwalletd-compatible reader serves block ranges from.

usage: route-check.py --grpc ADDR --ops ADDR --pid PID --catalog DIR --segments N
                      --covered START:END [--reference FILE] --highest H --tip T
                      --out DIR [--refusal-binary BIN --refusal-config FILE
                      --trusted-key FILE [--run-as USER]]

The reader must already be ready. The check passes when all of these hold:

  admission  The reader process holds exactly N open segment files from the
             catalog directory, one for each admitted segment.
  covered    GetBlockRange over START:END, and over the last 100 heights
             through H, returns every height, reads segment pages, and adds
             no canonical MultiGet or iterator reads. The START:END response
             equals --reference byte for byte.
  uncovered  GetBlockRange over the 100 heights above H returns every height
             and does add canonical payload reads, so the ticker witness can
             see a canonical read when one happens.
  refusal    The same reader configuration with a different, valid publisher
             key exits before serving, reporting a failed catalog admission.

RocksDB tickers reach /metrics only when the wallet serving pair refreshes, so
each probe is bracketed by two refreshes. Canonical block ranges are read with
MultiGet. The catalog follower's periodic anchor check makes a few canonical
point lookups, so point lookups are reported, with an idle interval for
comparison, but do not decide the result. Segment pages are evicted from the
page cache before each probe so that pages present afterwards were read by it.
Writes route-check.json to --out and exits 1 when any part fails.
"""
import argparse, glob, json, os, pwd, re, shutil, socket, subprocess, sys, tempfile, time, urllib.request
from pathlib import Path

# A canonical block-range chunk reads compact block rows with MultiGet; scans
# would show as iterator reads.
PAYLOAD_READ_TICKERS = (
    'rocksdb.number.multiget.keys.read', 'rocksdb.number.multiget.bytes.read',
    'rocksdb.db.iter.bytes.read', 'rocksdb.number.db.seek', 'rocksdb.number.db.next')
# Point lookups and the data blocks they touch. Reported only: the catalog
# follower's anchor check makes them on its own schedule.
POINT_READ_TICKERS = ('rocksdb.bytes.read', 'rocksdb.block.cache.data.hit', 'rocksdb.block.cache.data.miss')
REFRESH = 'zinder_wallet_serving_pair_publisher_refresh_total'
PROBE_BLOCKS = 100


def scrape(ops):
    with urllib.request.urlopen(f'http://{ops}/metrics', timeout=10) as response:
        return response.read().decode()


def series(text, name):
    out = {}
    for line in text.splitlines():
        if line.startswith(name + '{') or line.startswith(name + ' '):
            key, _, number = line.rpartition(' ')
            out[key] = float(number)
    return out


def labels(key):
    return dict(re.findall(r'(\w+)="([^"]*)"', key))


def refreshes(text):
    return sum(v for k, v in series(text, REFRESH).items() if labels(k).get('status') == 'ok')


def settled_tickers(ops):
    """Waits for two serving-pair refreshes, then returns the canonical secondary's tickers and the refresh count."""
    start = refreshes(scrape(ops))
    for _ in range(240):
        time.sleep(0.25)
        text = scrape(ops)
        if refreshes(text) >= start + 2:
            tickers = {labels(k)['ticker']: v for k, v in series(text, 'zinder_store_rocksdb_ticker').items()
                       if labels(k).get('store_role') == 'canonical_secondary'}
            return tickers, refreshes(text)
    raise RuntimeError('the wallet serving pair did not refresh twice within 60 seconds')


def segment_files(catalog):
    return sorted(glob.glob(os.path.join(catalog, 'segment-*.bin')))


def evict(files):
    for path in files:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        finally:
            os.close(fd)


def resident_bytes(files):
    total = 0
    for start in range(0, len(files), 200):
        out = subprocess.run(['fincore', '--bytes', '--noheadings', '--output', 'RES', *files[start:start + 200]],
                             check=True, capture_output=True, text=True).stdout
        total += sum(int(line) for line in out.split())
    return total


def rchar(pid):
    for line in Path(f'/proc/{pid}/io').read_text().splitlines():
        if line.startswith('rchar:'):
            return int(line.split()[1])
    raise RuntimeError('no rchar in /proc/PID/io')


def open_segments(pid, catalog):
    root = os.path.realpath(catalog)
    held = set()
    for link in glob.glob(f'/proc/{pid}/fd/*'):
        try:
            target = os.readlink(link)
        except OSError:
            continue
        if os.path.dirname(target) == root and re.fullmatch(r'segment-[0-9a-f]{64}\.bin', os.path.basename(target)):
            held.add(target)
    return len(held)


def block_range(grpc, start, end, path):
    body = json.dumps({'start': {'height': start}, 'end': {'height': end}})
    with path.open('wb') as out:
        subprocess.run(['grpcurl', '-plaintext', '-max-msg-sz', '268435456', '-format', 'json', '-d', body, grpc,
                        'cash.z.wallet.sdk.rpc.CompactTxStreamer/GetBlockRange'], check=True, stdout=out)
    text, decoder, at, heights = path.read_text(), json.JSONDecoder(), 0, []
    while True:
        while at < len(text) and text[at].isspace():
            at += 1
        if at == len(text):
            return heights
        block, at = decoder.raw_decode(text, at)
        heights.append(int(block.get('height', 0)))


def probe(args, name, start, end, files):
    """Measures one block range, or an idle interval of the same shape when start is None."""
    path = Path(args.out) / f'{name}-{start}-{end}.json'
    evict(files)
    (before, refreshed_before), pages_before, read_before = settled_tickers(args.ops), resident_bytes(files), rchar(args.pid)
    heights = block_range(args.grpc, start, end, path) if start is not None else None
    (after, refreshed_after), pages_after, read_after = settled_tickers(args.ops), resident_bytes(files), rchar(args.pid)
    payload = {t: after.get(t, 0) - before.get(t, 0) for t in PAYLOAD_READ_TICKERS}
    point = {t: after.get(t, 0) - before.get(t, 0) for t in POINT_READ_TICKERS}
    report = {'canonical_payload_read_delta': payload, 'canonical_payload_reads': sum(payload.values()),
              'canonical_point_read_delta': point, 'refreshes': refreshed_after - refreshed_before,
              'segment_resident_bytes_before': pages_before, 'segment_resident_bytes_after': pages_after,
              'process_rchar_delta': read_after - read_before}
    if start is None:
        return report
    return {'range': [start, end], 'response': path.name, 'response_bytes': path.stat().st_size,
            'complete': heights == list(range(start, end + 1)), **report}


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def refusal(args):
    """Starts the reader's configuration with a different, valid publisher key and expects refusal."""
    work = Path(tempfile.mkdtemp(prefix='route-check-refusal-', dir=args.out))
    der = subprocess.run('openssl genpkey -algorithm ed25519 | openssl pkey -pubout -outform DER',
                         shell=True, check=True, capture_output=True).stdout
    wrong = work / 'wrong-publisher.pub'
    wrong.write_bytes(der[-32:])
    trusted = Path(args.trusted_key).read_bytes()
    assert len(trusted) == 32 and wrong.read_bytes() != trusted
    log = work / 'reader.log'
    env = [f'ZINDER_COMPAT__SHIELDED_HISTORY__PUBLISHER_KEY={wrong}',
           f'ZINDER_COMPAT__LISTEN_ADDR=127.0.0.1:{free_port()}', f'ZINDER_OPS__LISTEN_ADDR=127.0.0.1:{free_port()}',
           f'ZINDER_STORAGE__SECONDARY_PATH={work}/canonical-secondary',
           f'ZINDER_WALLET__SECONDARY_PATH={work}/wallet-secondary']
    command = ['timeout', '-k', '10', str(args.refusal_timeout), 'env', *env, args.refusal_binary, '--config', args.refusal_config]
    if args.run_as:
        user = pwd.getpwnam(args.run_as)
        for path in (work, wrong):
            os.chown(path, user.pw_uid, user.pw_gid)
        command = ['runuser', '-u', args.run_as, '--', *command]
    started = time.monotonic()
    with log.open('wb') as out:
        code = subprocess.run(command, stdout=out, stderr=subprocess.STDOUT).returncode
    text = log.read_text(errors='replace')
    reason = next((line for line in text.splitlines() if 'shielded range admission failed' in line), '')
    for path in work.iterdir():
        if path.is_dir():
            shutil.rmtree(path)
        elif path != log:
            path.unlink()
    return {'exit_code': code, 'seconds': round(time.monotonic() - started, 1),
            'admission_failure': reason[-400:], 'log': str(log.relative_to(args.out)),
            'refused': code not in (0, 124) and 'signature' in reason}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    for flag in ('--grpc', '--ops', '--catalog', '--covered', '--out'):
        parser.add_argument(flag, required=True)
    for flag in ('--pid', '--segments', '--highest', '--tip'):
        parser.add_argument(flag, required=True, type=int)
    parser.add_argument('--reference')
    parser.add_argument('--refusal-binary')
    parser.add_argument('--refusal-config')
    parser.add_argument('--trusted-key')
    parser.add_argument('--run-as')
    parser.add_argument('--refusal-timeout', type=int, default=900)
    args = parser.parse_args()
    Path(args.out).mkdir(parents=True, exist_ok=True)
    covered = tuple(int(h) for h in args.covered.split(':'))
    if not covered[0] <= covered[1] <= args.highest < args.tip:
        sys.exit('route-check: --covered must end at or below --highest, which must be below --tip')
    files = segment_files(args.catalog)
    boundary = (max(args.highest - PROBE_BLOCKS + 1, 1), args.highest)
    above = (args.highest + 1, min(args.highest + PROBE_BLOCKS, args.tip))

    report = {'admission': {'expected_segments': args.segments, 'segment_files': len(files),
                            'open_segment_files': open_segments(args.pid, args.catalog)}}
    report['admission']['passed'] = report['admission']['open_segment_files'] == args.segments > 0
    # The first covered range after startup can check its settled anchor against canonical storage.
    for start, end in (covered, boundary):
        block_range(args.grpc, start, end, Path(args.out) / 'warm-up.json')
    report['idle'] = probe(args, 'idle', None, None, files)
    report['covered'] = probe(args, 'covered', *covered, files)
    report['boundary'] = probe(args, 'boundary', *boundary, files)
    report['uncovered'] = probe(args, 'uncovered', *above, files)
    for name in ('covered', 'boundary'):
        p = report[name]
        p['passed'] = p['complete'] and p['canonical_payload_reads'] == 0 \
            and p['segment_resident_bytes_after'] > p['segment_resident_bytes_before']
    if args.reference:
        same = (Path(args.out) / report['covered']['response']).read_bytes() == Path(args.reference).read_bytes()
        report['covered']['matches_reference'] = same
        report['covered']['passed'] &= same
    report['uncovered']['passed'] = report['uncovered']['complete'] \
        and report['uncovered']['canonical_payload_read_delta']['rocksdb.number.multiget.keys.read'] > 0
    report['highest_height_served_from_segments'] = args.highest if report['boundary']['passed'] and report['uncovered']['passed'] else None
    if args.refusal_binary:
        report['refusal'] = refusal(args)
        report['refusal']['passed'] = report['refusal']['refused']
    parts = ['admission', 'covered', 'boundary', 'uncovered'] + (['refusal'] if args.refusal_binary else [])
    report['failed'] = [part for part in parts if not report[part]['passed']]
    report['passed'] = not report['failed']
    (Path(args.out) / 'route-check.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))
    sys.exit(0 if report['passed'] else 1)


if __name__ == '__main__':
    main()
