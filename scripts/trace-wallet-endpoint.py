#!/usr/bin/env python3
"""Record Linux connect attempts and require only the chosen loopback endpoint."""
import argparse
import json
from pathlib import Path
import re
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--endpoint-port', required=True, type=int)
parser.add_argument('--output', required=True, type=Path)
parser.add_argument('command', nargs=argparse.REMAINDER)
args = parser.parse_args()
command = args.command[1:] if args.command[:1] == ['--'] else args.command
if not command or not 1024 <= args.endpoint_port <= 65535:
    parser.error('provide a valid endpoint port and a command after --')
args.output.mkdir(mode=0o700, parents=False)
trace = args.output / 'connect.log'
completed = subprocess.run(['strace', '-f', '-e', 'trace=connect', '-o', str(trace), '--', *command], check=False)
connections = []
unparsed = []
for line in trace.read_text().splitlines():
    if 'sa_family=AF_INET' not in line:
        continue
    port = re.search(r'(?:sin6?_port)=htons\((\d+)\)', line)
    address = re.search(r'(?:inet_addr\(|inet_pton\(AF_INET6, )"([^"]+)"', line)
    if not port or not address:
        unparsed.append(line)
        continue
    connections.append({'address': address[1], 'port': int(port[1])})
allowed = bool(connections) and not unparsed and all(
    c['address'] in ('127.0.0.1', '::1') and c['port'] == args.endpoint_port
    for c in connections
)
receipt = {
    'status': 'passed' if allowed else 'failed',
    'scope': 'Linux process tree connect syscalls; no payload capture',
    'command_exit': completed.returncode,
    'endpoint': f'loopback:{args.endpoint_port}',
    'connections': connections,
    'unparsed_internet_calls': unparsed,
    'fallback_used': False if allowed else None,
}
(args.output / 'report.json').write_text(json.dumps(receipt, indent=2) + '\n')
raise SystemExit(0 if allowed and completed.returncode == 0 else 1)
