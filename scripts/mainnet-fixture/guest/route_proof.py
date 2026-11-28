"""Route-proof predicates shared by route-check.py, which records the proof,
and fixture-gate.py, which decides from the recorded measurements.

Every decision is recomputed from measurements; a part's own `passed` flag is
never trusted.
"""

# A canonical block-range chunk reads compact block rows with MultiGet; scans
# would show as iterator reads. Each counter is checked on its own.
PAYLOAD_READ_TICKERS = (
    'rocksdb.number.multiget.keys.read', 'rocksdb.number.multiget.bytes.read',
    'rocksdb.db.iter.bytes.read', 'rocksdb.number.db.seek', 'rocksdb.number.db.next')
MULTIGET_KEYS = 'rocksdb.number.multiget.keys.read'
# Explicit transparent, Sapling, and Orchard pools differ from the default
# shielded projection, so the reader serves them from canonical storage.
NATIVE_POOLS = [1, 2, 3]
# The wallet sample's 100-block range, which a fixture's sample digest pins.
SAMPLE_RANGE = (1730000, 1730099)
PROBE_BLOCKS = 100
PROOF = 'external'
PARTS = ('admission', 'covered', 'boundary_last', 'boundary_next', 'canonical_control', 'refusal')


def is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def is_count(value):
    return (is_int(value) or isinstance(value, float)) and not isinstance(value, bool) and value >= 0


def reference_range(first, last, sample_pinned):
    """The covered range that must equal the pinned wallet sample, or None."""
    if sample_pinned and first <= SAMPLE_RANGE[0] and SAMPLE_RANGE[1] <= last:
        return list(SAMPLE_RANGE)
    return None


def covered_range(first, last, sample_pinned):
    return reference_range(first, last, sample_pinned) or [first, min(first + PROBE_BLOCKS - 1, last)]


def _deltas(probe, errors, name):
    delta = probe.get('canonical_payload_read_delta')
    if not isinstance(delta, dict) or not all(is_count(delta.get(t)) for t in PAYLOAD_READ_TICKERS):
        errors.append(f'{name} does not record all {len(PAYLOAD_READ_TICKERS)} canonical payload counters')
        return None
    return delta


def _range_probe(report, name, expected_range, errors):
    probe = report.get(name)
    if not isinstance(probe, dict):
        errors.append(f'{name} is missing')
        return None, None
    if probe.get('range') != expected_range:
        errors.append(f"{name} probed {probe.get('range')}, expected {expected_range}")
    if probe.get('complete') is not True:
        errors.append(f'{name} did not return every height')
    return probe, _deltas(probe, errors, name)


def part_errors(report, expect):
    """Reasons each part fails, from the recorded measurements alone.

    expect holds first, last, segment_count, sample_pinned, and refusal (whether
    a refusal result is required).
    """
    first, last = expect['first'], expect['last']
    errors = {part: [] for part in PARTS}
    if not (is_int(first) and is_int(last) and 1 <= first <= last):
        for part in PARTS:
            errors[part].append(f'catalog bounds {first}..{last} are invalid')
        return errors

    admission = report.get('admission')
    if not isinstance(admission, dict):
        errors['admission'].append('admission is missing')
    else:
        if admission.get('open_segment_files') != expect['segment_count']:
            errors['admission'].append(f"the reader holds {admission.get('open_segment_files')} segment files, "
                                       f"the manifest lists {expect['segment_count']}")
        if admission.get('matches_manifest') is not True or admission.get('unexpected_open_segments') \
                or admission.get('missing_segments'):
            errors['admission'].append('the open segment files differ from the manifest')
        if admission.get('manifest_unchanged') is not True:
            errors['admission'].append('current.json changed after it was verified')

    covered = covered_range(first, last, expect['sample_pinned'])
    probe, delta = _range_probe(report, 'covered', covered, errors['covered'])
    if delta and any(delta[t] != 0 for t in PAYLOAD_READ_TICKERS):
        errors['covered'].append('the covered range read canonical storage')
    if probe is not None and reference_range(first, last, expect['sample_pinned']):
        if not probe.get('byte_reference') or probe.get('matches_reference') is not True:
            errors['covered'].append('the covered range does not equal the pinned wallet sample')

    _, delta = _range_probe(report, 'boundary_last', [last, last], errors['boundary_last'])
    if delta and any(delta[t] != 0 for t in PAYLOAD_READ_TICKERS):
        errors['boundary_last'].append('the catalog\'s last height read canonical storage')

    _, delta = _range_probe(report, 'boundary_next', [last + 1, last + 1], errors['boundary_next'])
    if delta and delta[MULTIGET_KEYS] < 1:
        errors['boundary_next'].append('the height above the catalog read no canonical row')

    probe, delta = _range_probe(report, 'canonical_control', covered, errors['canonical_control'])
    if probe is not None and probe.get('pool_types') != NATIVE_POOLS:
        errors['canonical_control'].append(f'the control did not request pools {NATIVE_POOLS}')
    if delta and delta[MULTIGET_KEYS] < covered[1] - covered[0] + 1:
        errors['canonical_control'].append('the control read fewer canonical keys than heights')

    refusal = report.get('refusal')
    if expect['refusal']:
        if not isinstance(refusal, dict):
            errors['refusal'].append('the wrong-key refusal is missing')
        elif refusal.get('exit_code') in (0, 124, None) \
                or 'signature verification failed' not in str(refusal.get('admission_failure', '')):
            errors['refusal'].append('the reader did not refuse a different publisher key with a signature failure')
    return errors


def failures(report, expect):
    """All failing parts with reasons, plus any disagreement with the report's own flags."""
    if not isinstance(report, dict):
        return ['the route report is not an object']
    reasons = []
    if report.get('proof') != PROOF:
        reasons.append('the route report does not declare an external proof')
    for part, errors in part_errors(report, expect).items():
        reasons += [f'{part}: {error}' for error in errors]
    if not reasons and not (report.get('passed') is True and report.get('failed') == []):
        reasons.append('the route report records a failure its measurements do not explain')
    return reasons
