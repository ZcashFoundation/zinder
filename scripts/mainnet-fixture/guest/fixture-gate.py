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
  fixture-gate.py route-verdict FIXTURE BOUNDS REPORT
      Exits 0 only when route-check.py's report, read from REPORT or from
      standard input for -, passes every part when judged from its recorded
      measurements against the verified BOUNDS.
  fixture-gate.py mount-options FIXTURE COMPONENT
      Prints the mount options for a component: read-only without journal
      recovery for catalog and oracle disks, chosen for the component's
      declared filesystem, and noatime for every other disk.
  fixture-gate.py accept FIXTURE OUTCOME ATTEMPT VERIFY
      Exits 0 only when the guest OUTCOME of attempt ATTEMPT, restored with
      --verify-hashes VERIFY, proves what the fixture requires.

Route decisions come from route_proof.py, the predicates route-check.py uses,
applied to the recorded measurements; a part's own passed flag is not trusted.

The route proof is external: the pinned reader exports no identity for the
catalog generation it admitted, so admission is inferred from the segment
files the reader process holds open.
"""
import hashlib, json, os, re, sys
from pathlib import Path

# The local import must not write __pycache__ next to the stage's sources.
sys.dont_write_bytecode = True
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from route_proof import failures as route_failures, is_int  # noqa: E402

HEX64 = re.compile(r'^[0-9a-f]{64}$')


class GateError(Exception):
    pass


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
        highest, first = history.get('highest_height'), history.get('first_height')
        if not is_int(first) or first < 1 or (is_int(highest) and first > highest):
            errors.append(f'{name}: signed_history.first_height must be a positive integer no greater than highest_height')
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
                    'segment_count': history['segment_count'], 'first_height': history['first_height'],
                    'highest_height': history['highest_height']}
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
    if bounds['first_height'] != history['first_height']:
        raise GateError(f"manifest starts at {bounds['first_height']}, the fixture records {history['first_height']}")
    missing = [name for name in bounds['segment_files'] if not (directory / name).is_file()]
    if missing:
        raise GateError(f'{len(missing)} manifest segments are missing, first {missing[0]}')
    report.update(bounds)
    tmp = Path(str(out) + '.partial')
    tmp.write_text(json.dumps(report) + '\n')
    tmp.replace(out)
    return report


# Read-only disks are mounted without journal recovery, which each filesystem
# spells differently.
NO_RECOVERY = {'ext4': 'noload', 'ext4-verity': 'noload', 'xfs': 'norecovery'}


def mount_options(fixture, name):
    component = next((c for c in fixture.get('components', []) if isinstance(c, dict) and c.get('name') == name), None)
    if component is None:
        raise GateError(f'fixture has no component {name}')
    if component.get('kind') not in ('catalog', 'oracle'):
        return 'noatime'
    filesystem = component.get('filesystem')
    if filesystem not in NO_RECOVERY:
        raise GateError(f'component {name} has filesystem {filesystem!r}, which the restore cannot mount read-only')
    return f'ro,{NO_RECOVERY[filesystem]}'


def route_verdict(fixture, bounds, report):
    """Judges a route report from its measurements against the bounds verified before startup."""
    errors = route_failures(report, {'first': bounds.get('first_height'), 'last': bounds.get('last_height'),
                                     'segment_count': bounds.get('segments'),
                                     'sample_pinned': isinstance(fixture.get('wallet_sample'), dict), 'refusal': True})
    if isinstance(report, dict):
        for field in ('manifest_sha256', 'publisher_key_sha256'):
            if report.get(field) != bounds.get(field):
                errors.append(f'route report {field} differs from the hash verified before startup')
    return errors


def verified_components(fixture, verify):
    components = [c for c in fixture.get('components', []) if isinstance(c, dict)]
    if verify == 'all':
        return sorted(c.get('name') for c in components)
    if verify == 'stores':
        return sorted(c.get('name') for c in components if c.get('kind') == 'stores')
    return []


def accept_errors(fixture, outcome, attempt, verify):
    """Returns the reasons a guest outcome does not prove what the fixture and the request require."""
    if not isinstance(outcome, dict) or outcome.get('status') != 'succeeded':
        return ['the guest outcome is not a success']
    receipt = outcome.get('receipt')
    if not isinstance(receipt, dict):
        return ['the guest outcome has no receipt']
    errors = []
    if outcome.get('attempt') != attempt or receipt.get('attempt') != attempt:
        errors.append(f"the guest outcome is for attempt {outcome.get('attempt')!r} and its receipt for "
                      f"{receipt.get('attempt')!r}, not this restore's {attempt!r}")
    if receipt.get('verify_hashes') != verify:
        errors.append(f"the guest verified with {receipt.get('verify_hashes')!r}, the restore requested {verify!r}")
    components = receipt.get('verified_components')
    if not isinstance(components, list) or sorted(map(str, components)) != verified_components(fixture, verify):
        errors.append(f'the guest verified components {components!r}, --verify-hashes {verify} requires '
                      f'{verified_components(fixture, verify)!r}')
    chain = fixture.get('chain', {})
    if receipt.get('fixture_id') != fixture.get('fixture_id'):
        errors.append('the guest receipt names another fixture')
    tip = receipt.get('served_tip') if isinstance(receipt.get('served_tip'), dict) else {}
    if tip.get('height') != chain.get('visible_height') or tip.get('hash') != chain.get('visible_block_hash'):
        errors.append('the served tip differs from the fixture')
    sample = fixture.get('wallet_sample')
    if isinstance(sample, dict) and (receipt.get('wallet_sample_matches_fixture') is not True
                                     or receipt.get('wallet_sample_sha256sums_sha256') != sample.get('sha256sums_sha256')):
        errors.append('the wallet sample listing does not match the fixture')
    try:
        history = checked_history(fixture)
    except GateError as error:
        return errors + [str(error)]
    route = receipt.get('signed_history')
    if history is None:
        if route not in (None, {}):
            errors.append('the fixture declares no signed history, but the receipt reports one')
        return errors
    if not isinstance(route, dict):
        return errors + ['the fixture declares signed history, but the guest receipt has no route proof; '
                         'the stage may predate signed history']
    # Every expectation comes from the fixture, never from the report.
    expected = {'manifest_sha256': history['manifest_sha256'], 'publisher_key_sha256': history['publisher_key_sha256'],
                'segment_count': history['segment_count'], 'first_height': history['first_height'],
                'last_height': history['highest_height'], 'directory': history['directory'],
                'require_fs_verity': history['require_fs_verity']}
    for field, value in expected.items():
        if route.get(field) != value:
            errors.append(f'route proof {field} is {route.get(field)!r}, the fixture requires {value!r}')
    errors += route_failures(route, {'first': history['first_height'], 'last': history['highest_height'],
                                     'segment_count': history['segment_count'],
                                     'sample_pinned': isinstance(sample, dict), 'refusal': True})
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
        elif command == 'mount-options' and len(args) == 2:
            print(mount_options(load(args[0]), args[1]))
            return 0
        elif command == 'route-verdict' and len(args) == 3:
            report = json.load(sys.stdin) if args[2] == '-' else load(args[2])
            errors = route_verdict(load(args[0]), load(args[1]), report)
        elif command == 'accept' and len(args) == 4:
            errors = accept_errors(load(args[0]), load(args[1]), args[2], args[3])
        else:
            print(__doc__, file=sys.stderr)
            return 2
    except (GateError, OSError, ValueError) as error:
        print(f'fixture-gate: {error}', file=sys.stderr)
        return 1
    for error in errors:
        print(f'fixture-gate: {error}', file=sys.stderr)
    return 1 if errors else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
