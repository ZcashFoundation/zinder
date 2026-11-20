#!/usr/bin/env python3
"""Prepare a loopback-only Caddy route; never start or trust it globally."""
import argparse
import json
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output', required=True, type=Path)
parser.add_argument('--port', required=True, type=int)
parser.add_argument('--grpc-port', required=True, type=int)
parser.add_argument('--ops-port', required=True, type=int)
args = parser.parse_args()
if any(not 1024 <= port <= 65535 for port in (args.port, args.grpc_port, args.ops_port)):
    parser.error('ports must be between 1024 and 65535')
root = Path(__file__).resolve().parent.parent
output = args.output.resolve()
subprocess.run([str(root / 'scripts/generate-live-indexer-tls.sh'), str(output)], check=True)
# Keep the production route and readiness behavior; isolate only its listeners.
config = (root / 'deploy/compat-lightwalletd.Caddyfile').read_text()
config = config.replace('auto_https off', 'auto_https off\n\tadmin off')
config = config.replace('https://{$ZINDER_COMPAT_PUBLIC_FQDN}:443 {',
                        f'https://localhost:{args.port} {{\n\tbind 127.0.0.1')
for name, value in {
    'ZINDER_COMPAT_TLS_CERT_FILE': output / 'server.pem',
    'ZINDER_COMPAT_TLS_KEY_FILE': output / 'server-key.pem',
    'ZINDER_COMPAT_HOST_PORT': args.grpc_port,
    'ZINDER_COMPAT_OPS_HOST_PORT': args.ops_port,
}.items():
    replacement = json.dumps(str(value)) if isinstance(value, Path) else str(value)
    config = config.replace('{$' + name + '}', replacement)
if '{$' in config or f'https://localhost:{args.port}' not in config:
    raise RuntimeError('deployment template changed: inspect the local route generator')
(output / 'Caddyfile').write_text(config)
print(output / 'Caddyfile')
