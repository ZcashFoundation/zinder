#!/usr/bin/env python3
"""Own a disposable single-host Regtest Compose project and its readiness receipt."""
import argparse
from contextlib import suppress
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import secrets
import select
import signal
import shutil
import socket
import subprocess
import sys
import time
import tomllib
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
NODE_IMAGE = 'docker.io/zfnd/zebra:7.1.0@sha256:4f9536c8061e407f9f2ec81b7e0dd2f1762f81deec316abb1530dca8796a33ba'
SERVICES = {'ingest': 5, 'projector': 10, 'query': 6, 'compat-lightwalletd': 7}
OFFSETS = [2, 5, 6, 7, 10, 32, 67]
RELAY_DOCKER_TIMEOUT = 10
RELAY_STOP_GRACE = 5


def run(*args, env=None, timeout=None):
    return subprocess.check_output(args, text=True, stderr=subprocess.STDOUT, env=env, timeout=timeout).strip()


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write(path, content):
    path.write_text(json.dumps(content, indent=2) + '\n')


def request(url, payload=None):
    req = urllib.request.Request(url, data=None if payload is None else json.dumps(payload).encode(),
                                 headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=5) as response:
        return json.load(response)


def rpc(state, method, params):
    response = request(state['node_rpc'], dict(jsonrpc='2.0', id=1, method=method, params=params))
    if response.get('error'):
        raise RuntimeError(str(response['error']))
    return response['result']


def wait(call, predicate, timeout):
    deadline = time.monotonic() + timeout
    last = 'no response'
    while time.monotonic() < deadline:
        try:
            response = call()
            if predicate(response):
                return response
            last = str(response)[:300]
        except (OSError, ValueError, RuntimeError) as error:
            last = str(error)[:300]
        time.sleep(0.5)
    raise RuntimeError('readiness deadline: ' + last)


def compose(directory, *args):
    return run('docker', 'compose', '-p', directory.name, '-f', str(directory / 'compose.json'), *args)


def verify_owner(state):
    for kind, listing in [('container', ['ps', '-aq']), ('network', ['network', 'ls', '-q']),
                          ('volume', ['volume', 'ls', '-q'])]:
        identifiers = run('docker', *listing, '--filter',
                          'label=com.docker.compose.project=' + state['project']).split()
        for identifier in identifiers:
            resource = json.loads(run('docker', kind, 'inspect', identifier))[0]
            labels = resource.get('Config', resource).get('Labels', {})
            if labels.get('io.zinder.fixture.owner') != state['owner']:
                raise RuntimeError('project name is owned by a different fixture instance')


def stop_relay(directory, state):
    pid = state.get('relay_pid')
    if not pid:
        return
    expected = [os.fsencode(directory / 'relay.json'), state['owner'].encode()]
    try:
        fd = os.pidfd_open(pid)
    except ProcessLookupError:
        return
    try:
        proc = Path('/proc') / str(pid)
        argv = (proc / 'cmdline').read_bytes().rstrip(b'\0').split(b'\0')
        if (proc.stat().st_uid != os.geteuid() or len(argv) != 4 or
                os.path.basename(argv[1]) != b'regtest-fixture-relay.py' or argv[2:] != expected):
            return
        exited = select.poll()
        exited.register(fd, select.POLLIN)
        for sig in (signal.SIGTERM, signal.SIGKILL):
            signal.pidfd_send_signal(fd, sig)
            if exited.poll(int(RELAY_STOP_GRACE * 1000)):
                return
        raise RuntimeError('owned fixture relay failed to exit')
    except (FileNotFoundError, ProcessLookupError):
        pass
    finally:
        os.close(fd)
        with suppress(ChildProcessError):
            os.waitpid(pid, os.WNOHANG)


def start_relay(directory, state, serving=False):
    stop_relay(directory, state)

    def address(service):
        identifiers = run('docker', 'ps', '-q', '--no-trunc',
                          '--filter', 'label=com.docker.compose.project=' + state['project'],
                          '--filter', 'label=com.docker.compose.service=' + service,
                          '--filter', 'label=io.zinder.fixture.owner=' + state['owner'],
                          timeout=RELAY_DOCKER_TIMEOUT).splitlines()
        if len(identifiers) != 1:
            raise RuntimeError('expected one running owned container for ' + service)
        container = json.loads(run('docker', 'container', 'inspect', identifiers[0], timeout=RELAY_DOCKER_TIMEOUT))[0]
        return container['NetworkSettings']['Networks'][state['project'] + '_node']['IPAddress']

    targets = [[state['port_base'] + 32, address('node'), 39232]]
    if serving:
        host = address('zinder-ingest')
        targets += [[state['port_base'] + offset, host, port] for offset, port in
                    [(2, 9102), (5, 9105), (6, 9106), (7, 9107), (10, 9110), (67, 9067)]]
    write(directory / 'relay.json', targets)
    with (directory / 'relay.log').open('ab') as log:
        relay = subprocess.Popen([sys.executable, str(ROOT / 'scripts/regtest-fixture-relay.py'),
                                  str(directory / 'relay.json'), state['owner']],
                                 stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
    try:
        state['relay_pid'] = relay.pid
        write(directory / 'state.json', state)
        wait(lambda: rpc(state, 'getblockcount', []), lambda h: isinstance(h, int), 60)
        if relay.poll() is not None:
            raise RuntimeError('owned fixture relay exited during startup')
    except BaseException:
        relay.kill()
        relay.wait(timeout=RELAY_STOP_GRACE)
        raise


def fixture_subnets(pool):
    pool = ipaddress.ip_network(pool)
    if pool.version != 4 or not pool.is_private or not 16 <= pool.prefixlen <= 27:
        raise ValueError('--subnet-pool must be a private IPv4 /16 through /27')
    occupied = []
    networks = run('docker', 'network', 'ls', '-q').split()
    for line in run('docker', 'network', 'inspect', *networks,
                    '--format', '{{json .IPAM.Config}}').splitlines():
        for config in json.loads(line) or []:
            if config.get('Subnet'):
                occupied.append(ipaddress.ip_network(config['Subnet']))
    for route in json.loads(run('ip', '-j', '-4', 'route', 'show', 'table', 'all')):
        if route.get('dst', 'default') != 'default':
            occupied.append(ipaddress.ip_network(route['dst'], strict=False))
    candidates = list(pool.subnets(new_prefix=28))
    secrets.SystemRandom().shuffle(candidates)
    selected = [str(candidate) for candidate in candidates
                if not any(candidate.overlaps(existing) for existing in occupied if existing.version == 4)][:2]
    if len(selected) != 2:
        raise ValueError('no unused fixture subnets; select another --subnet-pool')
    return selected


def image_id(reference):
    # Resolve tags once, then only execute the immutable local image ID.
    try:
        return run('docker', 'image', 'inspect', reference, '--format', '{{.Id}}')
    except subprocess.CalledProcessError:
        run('docker', 'pull', reference)
        return run('docker', 'image', 'inspect', reference, '--format', '{{.Id}}')


def snapshot(directory, state, timeout):
    tip = rpc(state, 'getblockcount', [])
    readiness, health = {}, {}
    for name, offset in SERVICES.items():
        url = f"http://127.0.0.1:{state['port_base'] + offset}"
        readiness[name] = wait(lambda: request(url + '/readyz'),
                               lambda r: r.get('status') == 'ready' and r.get('current_height') == tip,
                               timeout)
        health[name] = request(url + '/healthz')
        if health[name]['network'] != 'zcash-regtest' or health[name]['git_commit'] != state['zinder_commit']:
            raise RuntimeError(f'{name}: network or revision differs from requested candidate')
        if name == 'query' and not health[name]['capabilities']:
            raise RuntimeError(f'{name}: empty capabilities')
    if state['raw_blob_policy'] == 'all':
        required = {'wallet.read.full_block_at_v1', 'wallet.read.full_block_range_v1'}
        if not required.issubset(health['query']['capabilities']):
            raise RuntimeError('full-block query capabilities missing')
    if rpc(state, 'getpeerinfo', []):
        raise RuntimeError('disposable node has peer connections')
    node = json.loads(run('docker', 'container', 'inspect', state['node_container']))[0]
    internal_networks = []
    for network in node['NetworkSettings']['Networks'].values():
        isolated = json.loads(run('docker', 'network', 'inspect', network['NetworkID']))[0]
        internal_networks.append(isolated['Id'])
        if not isolated['Internal'] or isolated['EnableIPv6']:
            raise RuntimeError('node network has external routing')
    routes = run('docker', 'exec', state['node_container'], 'cat', '/proc/net/route')
    if any(line.split()[1] == '00000000' for line in routes.splitlines()[1:]):
        raise RuntimeError('node has a default IPv4 route')
    chain = rpc(state, 'getblockchaininfo', [])
    if chain['chain'] != 'test':
        raise RuntimeError('node did not report Regtest test chain')
    expected = state['activation_heights']
    observed = {v['name']: v['activationheight'] for v in chain['upgrades'].values()}
    if expected != observed:
        raise RuntimeError(f'node activation heights differ: {observed}')
    receipt = dict(state, schema_version=1, readiness=readiness, health=health,
                   isolation=dict(peer_count=0, default_ipv4_route=False, internal_networks=internal_networks),
                   current_tip=tip, current_hash=chain['bestblockhash'], chain=chain,
                   checked_at=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()))
    write(directory / 'ready.tmp', receipt)
    (directory / 'ready.tmp').replace(directory / 'ready.json')
    return receipt


def provision(args, directory):
    config = args.node_config.resolve()
    node = tomllib.loads(config.read_text())
    if node.get('network', {}).get('network') != 'Regtest':
        raise ValueError('--node-config must explicitly select Regtest')
    activations = node['network']['testnet_parameters']['activation_heights']
    if args.port_base < 1024 or args.port_base + max(OFFSETS) > 65535:
        raise ValueError('port range must be unprivileged and fit TCP ports')
    sockets = []
    try:
        for offset in OFFSETS:
            sock = socket.socket()
            sockets.append(sock)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(('127.0.0.1', args.port_base + offset))
    finally:
        for sock in sockets:
            sock.close()
    # Refuse a colliding project even when its state directory is elsewhere.
    if run('docker', 'ps', '-aq', '--filter', f'label=com.docker.compose.project={args.project}'):
        raise ValueError('Compose project already exists')
    for kind in ('volume', 'network'):
        if run('docker', kind, 'ls', '-q', '--filter', f'label=com.docker.compose.project={args.project}'):
            raise ValueError('Compose project resources already exist')
    images = json.loads(args.images.read_text())
    if set(images) != set(SERVICES):
        raise ValueError('--images must map ingest/projector/query/compat-lightwalletd to image references')
    image_references = images.copy()
    images = {name: image_id(ref) for name, ref in images.items()}
    node_image = image_id(NODE_IMAGE)
    directory.mkdir(parents=True, mode=0o700)
    (directory / 'tokens').mkdir()
    for name in ('ingest', 'checkpoint'):
        token = directory / 'tokens' / (name + '.token')
        token.write_text(secrets.token_hex(32) + '\n')
        token.chmod(0o644)
    (directory / 'zebra.toml').write_bytes(config.read_bytes())
    (directory / 'zebra.toml').chmod(0o644)
    env = dict(os.environ, ZINDER_DEPLOY_NETWORK='regtest', ZINDER_NETWORK__NAME='zcash-regtest',
               ZINDER_SOURCE_NETWORK_NAME=args.project + '-node',
               ZINDER_SOURCE_COOKIE_VOLUME_NAME=args.project + '-cookie',
               ZINDER_CONTROL_SECRETS_DIR=str(directory / 'tokens'),
               ZINDER_PROJECTOR_BUILD_OWNER_HEX=secrets.token_hex(16),
               ZINDER_NODE__JSON_RPC_ADDR='http://node:39232')
    model = json.loads(run('docker', 'compose', '-f', str(ROOT / 'deploy/docker-compose.yml'),
                           'config', '--format', 'json', env=env))
    owner = secrets.token_hex(16)
    model['name'] = args.project
    model['services'] = {k: v for k, v in model['services'].items()
                         if k in ['state-init'] + ['zinder-' + n for n in SERVICES]}
    subnets = fixture_subnets(args.subnet_pool)
    model['networks'] = {name: {'internal': True, 'ipam': {'config': [{'subnet': subnet}]}}
                         for name, subnet in zip(('node', 'observability'), subnets)}
    model['volumes'] = {n: {} for n in ('data', 'checkpoint-staging', 'node-cookie', 'node-state')}
    model['services']['state-init'].update(image=node_image, entrypoint=[], pull_policy='never')
    binaries = {}
    if args.bin_dir:
        (directory / 'binaries').mkdir()
    for name in SERVICES:
        service = model['services']['zinder-' + name]
        service.pop('build', None)
        service.update(image=images[name], pull_policy='never', restart='no')
        service['environment'].update(ZINDER_STORAGE__RAW_BLOB_POLICY=args.raw_blob_policy,
                                      TOKIO_WORKER_THREADS='2', RAYON_NUM_THREADS='2')
        if name != 'projector':
            service['environment']['ZINDER_NODE__AUTH__METHOD'] = 'none'
        if name == 'compat-lightwalletd':
            service['environment']['ZINDER_COMPAT__STALE_TIP_LIMIT_MS'] = '0'
        if args.bin_dir:
            binary = args.bin_dir.resolve() / ('zinder-' + name)
            original = binary
            binary = directory / 'binaries' / binary.name
            shutil.copy2(original, binary)
            binaries[name] = {'source': str(original), 'sha256': digest(binary)}
            service['volumes'].append({'type': 'bind', 'source': str(binary),
                                      'target': '/usr/local/bin/zinder-' + name, 'read_only': True})
        service['healthcheck'].update(interval='1s', start_period='1s', retries=180)
    model['services']['zinder-ingest'].pop('ports', None)
    model['services']['node'] = dict(image=node_image, pull_policy='never', restart='no',
        networks=['node'], environment={'CONFIG_FILE_PATH': '/etc/zebrad/zebrad.toml',
        'ZEBRA_RPC__LISTEN_ADDR': '0.0.0.0:39232', 'ZEBRA_NETWORK__LISTEN_ADDR': '127.0.0.1:18233', 'ZEBRA_RPC__ENABLE_COOKIE_AUTH': 'false',
        'ZEBRA_MINING__MINER_ADDRESS': 'tmDpFafuBHKGUYmuwLsrxWJrwcnSyzEEtYx'},
        volumes=[str(directory / 'zebra.toml') + ':/etc/zebrad/zebrad.toml:ro', 'node-state:/home/zebra/.cache/zebra'])
    for section in ('services', 'networks', 'volumes'):
        for resource in model[section].values():
            resource.setdefault('labels', {})['io.zinder.fixture.owner'] = owner
    write(directory / 'compose.json', model)
    state = dict(owner=owner, project=args.project, port_base=args.port_base, network='zcash-regtest',
                 zinder_commit=args.revision, images=images, image_references=image_references,
                 image_manifest_sha256=digest(args.images), binaries=binaries, node_image=node_image,
                 activation_heights=activations, activation_schedule_sha256=digest(config),
                 raw_blob_policy=args.raw_blob_policy, node_rpc=f'http://127.0.0.1:{args.port_base + 32}',
                 native_wallet_query=f'http://127.0.0.1:{args.port_base + 2}',
                 lightwalletd=f'http://127.0.0.1:{args.port_base + 67}',
                 query_ops=f'http://127.0.0.1:{args.port_base + 6}',
                 compat_ops=f'http://127.0.0.1:{args.port_base + 7}')
    write(directory / 'state.json', state)
    try:
        compose(directory, 'up', '-d', 'node')
        state['node_container'] = compose(directory, 'ps', '-q', 'node')
        write(directory / 'state.json', state)
        start_relay(directory, state)
        wait(lambda: rpc(state, 'getblockcount', []), lambda h: h == 0, args.timeout)
        if config == (ROOT / 'deploy/zebra/regtest.toml').resolve():
            run('bash', str(ROOT / 'scripts/mine-live-regtest.sh'), state['node_rpc'], str(args.tip))
        else:
            rpc(state, 'generate', [args.tip])
        compose(directory, 'up', '-d', '--wait', '--wait-timeout', str(args.timeout),
                *['zinder-' + n for n in SERVICES])
        start_relay(directory, state, serving=True)
        snapshot(directory, state, args.timeout)
        shutil.copyfile(directory / 'ready.json', directory / 'ready.initial.json')
    except BaseException:
        try:
            (directory / 'failure.log').write_text(compose(directory, 'logs', '--no-color', '--tail', '80'))
        except subprocess.CalledProcessError:
            pass
        (directory / 'ready.json').unlink(missing_ok=True)
        verify_owner(state)
        stop_relay(directory, state)
        compose(directory, 'down', '-v', '--remove-orphans')
        raise


def main():
    os.umask(0o077)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit('fixture interrupted'))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['up', 'status', 'mine', 'down'])
    parser.add_argument('--project', required=True, help='unique owned name beginning zinder-fixture-')
    parser.add_argument('--state-root', type=Path, default=ROOT / '.tmp/regtest-fixtures')
    parser.add_argument('--port-base', type=int, default=34100)
    parser.add_argument('--subnet-pool', default='10.253.0.0/16',
                        help='private IPv4 pool for two unused /28 internal bridges')
    parser.add_argument('--images', type=Path, help='JSON map of runtime names to exact tags, digests, or built image IDs')
    parser.add_argument('--revision', help='expected full candidate commit, verified through every /healthz')
    parser.add_argument('--bin-dir', type=Path, help='optional built or released binaries to mount over runtime images')
    parser.add_argument('--node-config', type=Path, default=ROOT / 'deploy/zebra/regtest.toml')
    parser.add_argument('--raw-blob-policy', choices=['transactions', 'all'], default='transactions')
    parser.add_argument('--tip', type=int, default=150, help='initial or absolute requested mining height')
    parser.add_argument('--timeout', type=int, default=180)
    args = parser.parse_args()
    if not re.fullmatch(r'zinder-fixture-[a-z0-9-]+', args.project):
        parser.error('project must start zinder-fixture- and contain lowercase letters, digits, or hyphens')
    directory = args.state_root.resolve() / args.project
    if args.action == 'up':
        if directory.exists():
            parser.error('state directory exists; use a fresh project or remove the stopped fixture directory')
        if not args.images or not args.revision or not re.fullmatch('[0-9a-f]{40}', args.revision):
            parser.error('up requires --images and --revision (40 lowercase hex characters)')
        if args.tip < 10:
            parser.error('initial tip must be at least 10')
        provision(args, directory)
    else:
        state = json.loads((directory / 'state.json').read_text())
        if state['project'] != args.project:
            raise ValueError('project ownership mismatch')
        verify_owner(state)
        if args.action == 'down':
            stop_relay(directory, state)
            compose(directory, 'down', '-v', '--remove-orphans')
            (directory / 'ready.json').unlink(missing_ok=True)
        else:
            (directory / 'ready.json').unlink(missing_ok=True)
            running = compose(directory, 'ps', '--status', 'running', '-q', 'node')
            if not running or running != state['node_container']:
                raise RuntimeError('owned node is not running; refusing to contact the saved RPC port')
            if args.action == 'mine':
                tip = rpc(state, 'getblockcount', [])
                if args.tip < tip:
                    raise ValueError('requested height is behind current chain')
                if args.tip > tip:
                    rpc(state, 'generate', [args.tip - tip])
            snapshot(directory, state, args.timeout)
    print(directory / ('state.json' if args.action == 'down' else 'ready.json'))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        sys.exit(error.output if isinstance(error, subprocess.CalledProcessError) else str(error))
