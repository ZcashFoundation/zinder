#!/usr/bin/env python3
"""Prove which route a restored lightwalletd-compatible reader serves block ranges from.

usage: route-check.py --grpc ADDR --ops ADDR --pid PID --bounds FILE --tip T --out DIR
                      [--reference-range START:END --reference FILE]
                      [--refusal-binary BIN --refusal-config FILE [--run-as USER]]

--bounds is the JSON that `fixture-gate.py verify-manifest` wrote before the
reader started: the catalog directory, the verified manifest and key SHA-256,
and the first height, last height, and segment files derived from the
verified manifest bytes. The reader must already be ready.

This is an external proof. The pinned reader exports no identity for the
catalog generation it admitted, so the check infers admission from the files
the reader holds open and infers the route from its storage counters:

  admission          The reader holds open exactly the manifest's segment
                     files, and current.json still has the verified hash.
  covered            GetBlockRange over a covered range adds no canonical
                     MultiGet or iterator reads. With --reference, the
                     response equals that file byte for byte.
  boundary_last      The catalog's last height alone adds no canonical
                     payload reads.
  boundary_next      The height above it adds at least one canonical
                     MultiGet key. A warm live tail would fail this probe,
                     never pass it.
  canonical_control  The covered range with explicit pools, which the reader
                     always reads from canonical storage, adds at least one
                     MultiGet key per height, so the counters can see a
                     canonical read in this process.
  refusal            The same configuration with a different, valid publisher
                     key exits before serving with a signature failure.

Counters reach /metrics only when the wallet serving pair refreshes, so each
probe is bracketed by two refreshes, and every counter used must be present
and must not decrease. Segment page residency (mincore after eviction advice)
and point lookups are reported as supporting evidence only. Writes
route-check.json to --out and exits 1 when any part fails.
"""
import argparse, ctypes, hashlib, json, mmap, os, pwd, re, shutil, socket, subprocess, sys, tempfile, time, urllib.request
from pathlib import Path

# A canonical block-range chunk reads compact block rows with MultiGet; scans
# would show as iterator reads. Each counter is checked on its own.
PAYLOAD_READ_TICKERS = (
    'rocksdb.number.multiget.keys.read', 'rocksdb.number.multiget.bytes.read',
    'rocksdb.db.iter.bytes.read', 'rocksdb.number.db.seek', 'rocksdb.number.db.next')
MULTIGET_KEYS = 'rocksdb.number.multiget.keys.read'
# Point lookups, which the catalog follower's anchor check also makes.
POINT_READ_TICKERS = ('rocksdb.bytes.read', 'rocksdb.block.cache.data.hit', 'rocksdb.block.cache.data.miss')
REFRESH = 'zinder_wallet_serving_pair_publisher_refresh_total'
PROBE_BLOCKS = 100
# Explicit transparent, Sapling, and Orchard pools differ from the default
# shielded projection, so the reader serves them from canonical storage.
NATIVE_POOLS = [1, 2, 3]


class ProbeError(Exception):
    pass


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
    raise ProbeError('the wallet serving pair did not refresh twice within 60 seconds')


def deltas(before, after, names):
    """Per-counter increases. A missing or decreasing counter is an error, never a zero."""
    out = {}
    for name in names:
        if name not in before or name not in after:
            raise ProbeError(f'counter {name} is missing from the canonical secondary series')
        if after[name] < before[name]:
            raise ProbeError(f'counter {name} decreased from {before[name]} to {after[name]}')
        out[name] = after[name] - before[name]
    return out


def evict(files):
    for path in files:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        finally:
            os.close(fd)


LIBC = ctypes.CDLL(None, use_errno=True)
LIBC.mmap.restype = ctypes.c_void_p
LIBC.mmap.argtypes = (ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_long)
LIBC.mincore.argtypes = (ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p)
LIBC.munmap.argtypes = (ctypes.c_void_p, ctypes.c_size_t)


def resident_bytes(files):
    """Sums the page-cache bytes of files with mincore(2), as fincore does, without faulting pages in."""
    page, total = os.sysconf('SC_PAGE_SIZE'), 0
    for path in files:
        size = os.path.getsize(path)
        if size == 0:
            continue
        fd = os.open(path, os.O_RDONLY)
        try:
            addr = LIBC.mmap(None, size, mmap.PROT_READ, mmap.MAP_SHARED, fd, 0)
            if addr in (None, ctypes.c_void_p(-1).value):
                raise OSError(ctypes.get_errno(), f'mmap {path}')
            try:
                pages = (size + page - 1) // page
                vector = (ctypes.c_ubyte * pages)()
                if LIBC.mincore(addr, size, vector) != 0:
                    raise OSError(ctypes.get_errno(), f'mincore {path}')
                total += sum(v & 1 for v in vector) * page
            finally:
                LIBC.munmap(addr, size)
        finally:
            os.close(fd)
    return total


def open_files(pid, directory):
    """Names of the regular files in directory that the process holds open."""
    root, held = os.path.realpath(directory), set()
    for link in Path(f'/proc/{pid}/fd').iterdir():
        try:
            target = os.readlink(link)
        except OSError:
            continue
        if os.path.dirname(target) == root:
            held.add(os.path.basename(target))
    return held


def plan(first, last, tip, reference_range=None):
    """Chooses the probe ranges from the catalog's verified bounds."""
    if not first <= last < tip:
        raise ProbeError(f'catalog {first}..{last} must end below the tip {tip}')
    if reference_range and first <= reference_range[0] <= reference_range[1] <= last:
        covered, reference = tuple(reference_range), True
    else:
        covered, reference = (first, min(first + PROBE_BLOCKS - 1, last)), False
    return {'covered': covered, 'reference': reference,
            'boundary_last': (last, last), 'boundary_next': (last + 1, last + 1)}


def block_range(grpc, start, end, path, pools=None):
    request = {'start': {'height': start}, 'end': {'height': end}}
    if pools:
        request['poolTypes'] = pools
    with path.open('wb') as out:
        subprocess.run(['grpcurl', '-plaintext', '-max-msg-sz', '268435456', '-format', 'json', '-d',
                        json.dumps(request), grpc, 'cash.z.wallet.sdk.rpc.CompactTxStreamer/GetBlockRange'],
                       check=True, stdout=out)
    text, decoder, at, heights = path.read_text(), json.JSONDecoder(), 0, []
    while True:
        while at < len(text) and text[at].isspace():
            at += 1
        if at == len(text):
            return heights
        block, at = decoder.raw_decode(text, at)
        heights.append(int(block.get('height', 0)))


def probe(args, name, span, files, pools=None):
    """Measures one block range, or an idle interval when span is None."""
    start, end = span if span else (None, None)
    path = Path(args.out) / f'{name}-{start}-{end}.json'
    evict(files)
    (before, refreshed_before), pages_before = settled_tickers(args.ops), resident_bytes(files)
    heights = block_range(args.grpc, start, end, path, pools) if span else None
    (after, refreshed_after), pages_after = settled_tickers(args.ops), resident_bytes(files)
    report = {'canonical_payload_read_delta': deltas(before, after, PAYLOAD_READ_TICKERS),
              'canonical_point_read_delta': {t: after[t] - before[t] for t in POINT_READ_TICKERS if t in before and t in after},
              'refreshes': refreshed_after - refreshed_before,
              'supporting_segment_resident_bytes': [pages_before, pages_after]}
    if span:
        report.update({'range': [start, end], 'response': path.name, 'response_bytes': path.stat().st_size,
                       'complete': heights == list(range(start, end + 1))})
        if pools:
            report['pool_types'] = pools
    return report


def no_payload_reads(report):
    return all(value == 0 for value in report['canonical_payload_read_delta'].values())


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def refusal(args, trusted_key):
    """Starts the reader's configuration with a different, valid publisher key and expects refusal."""
    work = Path(tempfile.mkdtemp(prefix='refusal-', dir=args.out))
    try:
        der = subprocess.run('openssl genpkey -algorithm ed25519 | openssl pkey -pubout -outform DER',
                             shell=True, check=True, capture_output=True).stdout
        wrong = work / 'wrong-publisher.pub'
        wrong.write_bytes(der[-32:])
        if wrong.read_bytes() == Path(trusted_key).read_bytes():
            raise ProbeError('generated the trusted key by chance')
        log = work / 'reader.log'
        env = [f'ZINDER_COMPAT__SHIELDED_HISTORY__PUBLISHER_KEY={wrong}',
               f'ZINDER_COMPAT__LISTEN_ADDR=127.0.0.1:{free_port()}', f'ZINDER_OPS__LISTEN_ADDR=127.0.0.1:{free_port()}',
               f'ZINDER_STORAGE__SECONDARY_PATH={work}/canonical-secondary',
               f'ZINDER_WALLET__SECONDARY_PATH={work}/wallet-secondary']
        command = ['timeout', '-k', '10', str(args.refusal_timeout), 'env', *env,
                   args.refusal_binary, '--config', args.refusal_config]
        if args.run_as:
            user = pwd.getpwnam(args.run_as)
            for path in (work, wrong):
                os.chown(path, user.pw_uid, user.pw_gid)
            command = ['runuser', '-u', args.run_as, '--', *command]
        started = time.monotonic()
        with log.open('wb') as out:
            code = subprocess.run(command, stdout=out, stderr=subprocess.STDOUT).returncode
        reason = next((line for line in log.read_text(errors='replace').splitlines()
                       if 'shielded range admission failed' in line), '')
        return {'exit_code': code, 'seconds': round(time.monotonic() - started, 1),
                'admission_failure': reason[-400:], 'log': str(log.relative_to(args.out)),
                'passed': code not in (0, 124) and 'signature verification failed' in reason}
    finally:
        for path in work.iterdir():
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
            elif path.name != 'reader.log':
                path.unlink()


def run(args):
    bounds = json.loads(Path(args.bounds).read_text())
    directory = bounds['directory']
    files = [os.path.join(directory, name) for name in bounds['segment_files']]
    reference_range = tuple(int(h) for h in args.reference_range.split(':')) if args.reference_range else None
    chosen = plan(bounds['first_height'], bounds['last_height'], args.tip, reference_range)
    report = {'proof': 'external',
              'proof_note': 'The pinned reader exports no identity for the catalog generation it admitted. '
                            'Admission is inferred from the segment files the reader process holds open, '
                            'and the route from its canonical storage counters.',
              'manifest_sha256': bounds['manifest_sha256'], 'publisher_key_sha256': bounds['publisher_key_sha256'],
              'directory': directory, 'require_fs_verity': bounds['require_fs_verity'],
              'first_height': bounds['first_height'], 'last_height': bounds['last_height'],
              'segment_count': bounds['segments']}
    expected, held = set(bounds['segment_files']), open_files(args.pid, directory)
    held_segments = {name for name in held if name.startswith('segment-')}
    report['admission'] = {'open_segment_files': len(held_segments), 'matches_manifest': held_segments == expected,
                           'unexpected_open_segments': sorted(held_segments - expected)[:5],
                           'missing_segments': sorted(expected - held_segments)[:5]}
    # The first range after startup can check its settled anchor against canonical storage.
    for span in (chosen['covered'], chosen['boundary_last']):
        block_range(args.grpc, *span, Path(args.out) / 'warm-up.json')
    report['idle'] = probe(args, 'idle', None, files)
    report['covered'] = probe(args, 'covered', chosen['covered'], files)
    report['boundary_last'] = probe(args, 'boundary-last', chosen['boundary_last'], files)
    report['boundary_next'] = probe(args, 'boundary-next', chosen['boundary_next'], files)
    report['canonical_control'] = probe(args, 'canonical-control', chosen['covered'], files, NATIVE_POOLS)
    unchanged = hashlib.sha256((Path(directory) / 'current.json').read_bytes()).hexdigest() == bounds['manifest_sha256']
    report['admission']['manifest_unchanged'] = unchanged
    report['admission']['passed'] = report['admission']['matches_manifest'] and unchanged
    for name in ('covered', 'boundary_last'):
        report[name]['passed'] = report[name]['complete'] and no_payload_reads(report[name])
    covered = report['covered']
    if chosen['reference']:
        covered['byte_reference'] = args.reference
        covered['matches_reference'] = (Path(args.out) / covered['response']).read_bytes() == Path(args.reference).read_bytes()
        covered['passed'] = covered['passed'] and covered['matches_reference']
    else:
        covered['byte_reference'] = None
    span = chosen['covered'][1] - chosen['covered'][0] + 1
    for name, minimum in (('boundary_next', 1), ('canonical_control', span)):
        p = report[name]
        p['passed'] = p['complete'] and p['canonical_payload_read_delta'][MULTIGET_KEYS] >= minimum
    if args.refusal_binary:
        report['refusal'] = refusal(args, bounds['publisher_key'])
    parts = ['admission', 'covered', 'boundary_last', 'boundary_next', 'canonical_control'] + (['refusal'] if args.refusal_binary else [])
    report['failed'] = [part for part in parts if not report[part]['passed']]
    report['passed'] = not report['failed']
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    for flag in ('--grpc', '--ops', '--bounds', '--out'):
        parser.add_argument(flag, required=True)
    for flag in ('--pid', '--tip'):
        parser.add_argument(flag, required=True, type=int)
    parser.add_argument('--reference-range')
    parser.add_argument('--reference')
    parser.add_argument('--refusal-binary')
    parser.add_argument('--refusal-config')
    parser.add_argument('--run-as')
    parser.add_argument('--refusal-timeout', type=int, default=900)
    args = parser.parse_args()
    if bool(args.reference_range) != bool(args.reference):
        parser.error('--reference-range and --reference go together')
    Path(args.out).mkdir(parents=True, exist_ok=True)
    try:
        report = run(args)
    except (ProbeError, OSError, subprocess.CalledProcessError, ValueError, KeyError) as error:
        report = {'proof': 'external', 'passed': False, 'failed': ['error'], 'error': f'{type(error).__name__}: {error}'}
    tmp = Path(args.out) / 'route-check.json.partial'
    tmp.write_text(json.dumps(report, indent=2) + '\n')
    tmp.replace(Path(args.out) / 'route-check.json')
    print(json.dumps(report, indent=2))
    sys.exit(0 if report['passed'] else 1)


if __name__ == '__main__':
    main()
