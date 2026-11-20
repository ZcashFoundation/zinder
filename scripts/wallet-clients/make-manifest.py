#!/usr/bin/env python3
"""Pin executable client adapters and generate a wallet lifecycle v2 manifest."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

from wallet_fixture import binding


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--wallet', choices=['zingo', 'zkool'], required=True)
    for name in ['binary', 'trace-tool', 'fence-python', 'proto-dir', 'ready', 'evidence-dir', 'output']:
        p.add_argument('--' + name, required=True, type=Path)
    for name in ['endpoint', 'node']:
        p.add_argument('--' + name, help='optional cross-check against the receipt')
    p.add_argument('--candidate', type=Path, help='side-by-side compatibility service receipt')
    p.add_argument('--binary-sha256', required=True, help='expected executable digest from the pinned build')
    p.add_argument('--variant', choices=['internal-enabled', 'external-only'])
    a = p.parse_args()
    if a.wallet == 'zkool' and not a.variant:
        p.error('Zkool requires an explicit scanning variant')
    if a.wallet == 'zingo' and a.variant is not None:
        p.error('the scanning variant applies only to Zkool')
    try:
        endpoint, node, _, _ = binding(a.ready, a.candidate, a.endpoint, a.node)
    except (OSError, KeyError, ValueError, TypeError) as error:
        p.error(str(error))
    scripts = Path(__file__).resolve().parent
    adapter = scripts / f'{a.wallet}-regtest.py'
    binary = a.binary.resolve()
    if a.evidence_dir.exists():
        p.error('use a fresh private evidence directory')
    generated = ['service_pb2.py', 'service_pb2_grpc.py', 'compact_formats_pb2.py']
    paths = [binary, adapter, scripts/'wait-serving-fence.py', scripts/'wallet_fixture.py',
             Path(__file__), a.trace_tool, a.ready, Path(sys.executable), a.fence_python,
             *(a.proto_dir/name for name in generated)]
    if a.candidate:
        paths.append(a.candidate)
    if a.wallet == 'zkool':
        paths.append(scripts/'zkool-regtest.rs')
    inputs = {}
    for path in paths:
        path = path.resolve(strict=True)
        with path.open('rb') as source:
            inputs[str(path)] = hashlib.file_digest(source, 'sha256').hexdigest()
    if inputs[str(binary)] != a.binary_sha256:
        p.error('wallet executable differs from the expected build digest')
    case = {'wallet':a.wallet,'version':'8.0.0' if a.wallet=='zingo' else '6.37.0','variant':a.variant or 'nakednet-cli','network':'zcash-regtest','implementation':'engine','interface':'lightwalletd','endpoint':endpoint,'require_no_fallback':True,'restart_scope':'process-restart','inputs':inputs,'client':{'path':str(binary),'sha256':inputs[str(binary)]},'steps':{}}
    for step in ['bootstrap','restore','transparent-discovery','shielded-discovery','send','pending','mined','scanned','restart','resume']:
        command = [sys.executable,str(adapter),'--binary',str(binary),'--trace-tool',str(a.trace_tool.resolve()),'--node',node,'--endpoint','{endpoint}','--scratch','{scratch}','--report','{report}','--step',step,'--evidence-dir',str(a.evidence_dir.resolve()),'--fence-python',str(a.fence_python.absolute()),'--proto-dir',str(a.proto_dir.resolve())]
        command += ['--ready', str(a.ready.resolve())]
        if a.candidate:
            command += ['--candidate', str(a.candidate.resolve())]
        if a.wallet=='zkool':
            command += ['--variant',a.variant]
        case['steps'][step] = {'command':command,'expect':{'/assertions/'+step:True}}
    with a.output.open('x') as output:
        json.dump({'schema_version':2,'timeout_seconds':600,'cases':{a.wallet+'-'+case['variant']:case}},output,indent=2)


if __name__ == '__main__':
    main()
