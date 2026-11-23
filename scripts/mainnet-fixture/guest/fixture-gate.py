#!/usr/bin/env python3
"""Signed-history checks shared by the workstation and the fixture guest.

usage:
  fixture-gate.py preflight FIXTURE
      Validates every component that names a signed history catalog and the
      stage pin a catalog fixture needs. Exits 1 with the reasons.
  fixture-gate.py history FIXTURE
      Prints the signed history the guest must configure, as JSON, or null
      when the fixture declares none. Applies the preflight rules again.
  fixture-gate.py verify-manifest FIXTURE OUT
      Hashes the catalog's current.json and publisher key once, compares them
      with the fixture, derives the catalog bounds and segment files from the
      verified manifest bytes, and writes them to OUT.
  fixture-gate.py route-verdict REPORT
      Exits 0 only when route-check.py's report shows every required part.
  fixture-gate.py accept FIXTURE GUEST_RECEIPT
      Exits 0 only when the guest receipt proves what the fixture requires.

The route proof is external: the pinned reader exports no identity for the
catalog generation it admitted, so admission is inferred from the segment
files the reader process holds open.
"""
import hashlib, json, re, sys
from pathlib import Path

HEX64 = re.compile(r'^[0-9a-f]{64}$')
ROUTE_PARTS = ('admission', 'covered', 'boundary_last', 'boundary_next', 'canonical_control', 'refusal')
PROOF = 'external'


class GateError(Exception):
    pass


def is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def relative_path(value):
    return isinstance(value, str) and value != '' and not value.startswith('/') \
        and '..' not in Path(value).parts and '\0' not in value


def names_catalog(component):
    paths = component.get('paths') if isinstance(component.get('paths'), dict) else {}
    return component.get('kind') == 'catalog' or 'catalog' in paths or 'publisher_key' in paths \
        or 'signed_history' in component


def history_errors(fixture):
    """Returns the reasons a fixture's signed history cannot be served and proven."""
    errors = []
    components = fixture.get('components')
    if not isinstance(components, list):
        return ['components must be a list']
    chain = fixture.get('chain') if isinstance(fixture.get('chain'), dict) else {}
    visible = chain.get('visible_height')
    described = 0
    for component in components:
        if not isinstance(component, dict) or not names_catalog(component):
            continue
        name = component.get('name', '?')
        history = component.get('signed_history')
        if not isinstance(history, dict):
            errors.append(f'component {name} names a signed history catalog, so signed_history must be an object')
            continue
        described += 1
        for field in ('directory', 'publisher_key'):
            if not relative_path(history.get(field)):
                errors.append(f'{name}: signed_history.{field} must be a path relative to mount_path')
        for field in ('manifest_sha256', 'publisher_key_sha256'):
            if not isinstance(history.get(field), str) or not HEX64.match(history[field]):
                errors.append(f'{name}: signed_history.{field} must be a lowercase SHA-256')
        if not is_int(history.get('segment_count')) or history['segment_count'] < 1:
            errors.append(f'{name}: signed_history.segment_count must be a positive integer')
        highest = history.get('highest_height')
        if not is_int(highest) or highest < 1:
            errors.append(f'{name}: signed_history.highest_height must be a positive integer')
        elif not is_int(visible) or highest >= visible:
            errors.append(f'{name}: signed_history.highest_height must be below chain.visible_height, '
                          'so the height above the catalog can be probed')
        if not isinstance(component.get('mount_path'), str) or not component['mount_path'].startswith('/'):
            errors.append(f'{name}: mount_path must be absolute')
    if described > 1:
        errors.append('at most one component may carry signed_history')
    if described and not (isinstance(fixture.get('stage_sha256'), str) and HEX64.match(fixture['stage_sha256'])):
        errors.append('a fixture with signed_history must pin stage_sha256, so an older stage cannot restore it')
    return errors


def load(path):
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError) as error:
        raise GateError(f'cannot read {path}: {error}') from error


def checked_history(fixture):
    errors = history_errors(fixture)
    if errors:
        raise GateError('; '.join(errors))
    for component in fixture['components']:
        if isinstance(component, dict) and isinstance(component.get('signed_history'), dict):
            history = component['signed_history']
            mount = Path(component['mount_path'])
            return {'component': component['name'], 'kind': component.get('kind'),
                    'directory': str(mount / history['directory']),
                    'publisher_key': str(mount / history['publisher_key']),
                    'require_fs_verity': component.get('filesystem') == 'ext4-verity',
                    'manifest_sha256': history['manifest_sha256'],
                    'publisher_key_sha256': history['publisher_key_sha256'],
                    'segment_count': history['segment_count'], 'highest_height': history['highest_height']}
    return None


def manifest_bounds(manifest_bytes):
    """Derives first, last, and the segment file names from signed manifest bytes."""
    try:
        signed = json.loads(manifest_bytes)
        inner = json.loads(signed['manifest'])
        segments = inner['segments']
    except (ValueError, KeyError, TypeError) as error:
        raise GateError(f'manifest is not a signed shielded-range catalog: {error}') from error
    if not isinstance(segments, list) or not segments:
        raise GateError('manifest lists no segments')
    files, previous = [], None
    for index, entry in enumerate(segments):
        first, last, digest = entry.get('first'), entry.get('last'), entry.get('sha256')
        if not (is_int(first) and is_int(last) and first <= last):
            raise GateError(f'manifest segment {index} has invalid bounds')
        if previous is not None and first != previous + 1:
            raise GateError(f'manifest segment {index} starts at {first}, not after {previous}')
        if not (isinstance(digest, list) and len(digest) == 32 and all(is_int(b) and 0 <= b < 256 for b in digest)):
            raise GateError(f'manifest segment {index} has no 32-byte digest')
        files.append(f'segment-{bytes(digest).hex()}.bin')
        previous = last
    return {'first_height': segments[0]['first'], 'last_height': previous,
            'segments': len(segments), 'segment_files': files}


def verify_manifest(fixture, out):
    history = checked_history(fixture)
    if history is None:
        raise GateError('fixture declares no signed history')
    directory = Path(history['directory'])
    manifest_bytes = (directory / 'current.json').read_bytes()
    key_bytes = Path(history['publisher_key']).read_bytes()
    report = dict(history, manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
                  publisher_key_sha256=hashlib.sha256(key_bytes).hexdigest())
    for field, actual in (('manifest_sha256', report['manifest_sha256']),
                          ('publisher_key_sha256', report['publisher_key_sha256'])):
        if actual != history[field]:
            raise GateError(f'{field} is {actual}, the fixture records {history[field]}')
    if len(key_bytes) != 32:
        raise GateError(f'publisher key has {len(key_bytes)} bytes, not 32')
    if not (directory / '.publisher-head.json').is_file():
        raise GateError(f'{directory} has no publisher checkpoint')
    bounds = manifest_bounds(manifest_bytes)
    if bounds['segments'] != history['segment_count']:
        raise GateError(f"manifest lists {bounds['segments']} segments, the fixture records {history['segment_count']}")
    if bounds['last_height'] != history['highest_height']:
        raise GateError(f"manifest ends at {bounds['last_height']}, the fixture records {history['highest_height']}")
    missing = [name for name in bounds['segment_files'] if not (directory / name).is_file()]
    if missing:
        raise GateError(f'{len(missing)} manifest segments are missing, first {missing[0]}')
    report.update(bounds)
    tmp = Path(str(out) + '.partial')
    tmp.write_text(json.dumps(report) + '\n')
    tmp.replace(out)
    return report


def route_errors(report):
    if not isinstance(report, dict):
        return ['the route report is not an object']
    errors = [] if report.get('passed') is True and report.get('failed') == [] else ['the route report did not pass']
    for part in ROUTE_PARTS:
        if not (isinstance(report.get(part), dict) and report[part].get('passed') is True):
            errors.append(f'route part {part} is missing or failed')
    if report.get('proof') != PROOF:
        errors.append('the route report does not declare its proof kind')
    return errors


def accept_errors(fixture, receipt):
    """Returns the reasons a guest receipt does not prove what the fixture requires."""
    if not isinstance(receipt, dict):
        return ['the guest receipt is not an object']
    errors = []
    chain = fixture.get('chain', {})
    if receipt.get('fixture_id') != fixture.get('fixture_id'):
        errors.append('the guest receipt names another fixture')
    tip = receipt.get('served_tip') if isinstance(receipt.get('served_tip'), dict) else {}
    if tip.get('height') != chain.get('visible_height') or tip.get('hash') != chain.get('visible_block_hash'):
        errors.append('the served tip differs from the fixture')
    if isinstance(fixture.get('wallet_sample'), dict) and receipt.get('wallet_sample_matches_fixture') is not True:
        errors.append('the wallet sample does not match the fixture')
    try:
        history = checked_history(fixture)
    except GateError as error:
        return errors + [str(error)]
    route = receipt.get('signed_history')
    if history is None:
        if route not in (None, {}) and not (isinstance(route, dict) and route.get('declared') is False):
            errors.append('the fixture declares no signed history, but the receipt reports one')
        return errors
    if not isinstance(route, dict):
        return errors + ['the fixture declares signed history, but the guest receipt has no route proof; '
                         'the stage may predate signed history']
    errors += route_errors(route)
    for field in ('manifest_sha256', 'publisher_key_sha256', 'segment_count'):
        if route.get(field) != history[field]:
            errors.append(f'route proof {field} is {route.get(field)!r}, the fixture records {history[field]!r}')
    if route.get('last_height') != history['highest_height']:
        errors.append(f"route proof last_height is {route.get('last_height')!r}, the fixture records {history['highest_height']}")
    admission = route.get('admission') if isinstance(route.get('admission'), dict) else {}
    if admission.get('open_segment_files') != history['segment_count'] or admission.get('matches_manifest') is not True:
        errors.append('the reader does not hold exactly the manifest segments open')
    for part, height in (('boundary_last', history['highest_height']), ('boundary_next', history['highest_height'] + 1)):
        probe = route.get(part) if isinstance(route.get(part), dict) else {}
        if probe.get('range') != [height, height]:
            errors.append(f'route part {part} did not probe height {height}')
    return errors


def main(argv):
    if len(argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    command, args = argv[1], argv[2:]
    try:
        if command == 'preflight' and len(args) == 1:
            errors = history_errors(load(args[0]))
        elif command == 'history' and len(args) == 1:
            print(json.dumps(checked_history(load(args[0]))))
            return 0
        elif command == 'verify-manifest' and len(args) == 2:
            print(json.dumps({k: v for k, v in verify_manifest(load(args[0]), args[1]).items() if k != 'segment_files'}))
            return 0
        elif command == 'route-verdict' and len(args) == 1:
            errors = route_errors(load(args[0]))
        elif command == 'accept' and len(args) == 2:
            errors = accept_errors(load(args[0]), load(args[1]))
        else:
            print(__doc__, file=sys.stderr)
            return 2
    except (GateError, OSError) as error:
        print(f'fixture-gate: {error}', file=sys.stderr)
        return 1
    for error in errors:
        print(f'fixture-gate: {error}', file=sys.stderr)
    return 1 if errors else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
