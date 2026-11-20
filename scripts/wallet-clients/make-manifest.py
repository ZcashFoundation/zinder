#!/usr/bin/env python3
"""Pin executable client adapters and generate a wallet lifecycle v2 manifest."""
import argparse
import hashlib
import json
from pathlib import Path
import sys


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--wallet', choices=['zingo', 'zkool'], required=True)
    for name in ['binary', 'trace-tool', 'fence-python', 'proto-dir', 'ready', 'evidence-dir', 'output']:
        p.add_argument('--' + name, required=True, type=Path)
    for name in ['endpoint', 'node']:
        p.add_argument('--' + name, required=True)
    p.add_argument('--variant', choices=['internal-enabled', 'external-only'])
    a = p.parse_args()
    if a.wallet == 'zkool' and not a.variant:
        p.error('Zkool requires an explicit scanning variant')
    scripts = Path(__file__).resolve().parent
    adapter = scripts / f'{a.wallet}-regtest.py'
    binary = a.binary.resolve()
    paths = [binary, adapter, scripts/'wait-serving-fence.py', Path(__file__).resolve(), a.trace_tool.resolve(), a.ready.resolve(), Path(sys.executable).resolve(), a.fence_python.resolve(), *a.proto_dir.glob('*_pb2*.py')]
    if a.wallet == 'zkool':
        paths.append(scripts/'zkool-regtest.rs')
    inputs = {str(path):hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    case = {'wallet':a.wallet,'version':'8.0.0' if a.wallet=='zingo' else '6.37.0','variant':a.variant or 'nakednet-cli','network':'zcash-regtest','implementation':'engine','interface':'lightwalletd','endpoint':a.endpoint,'require_no_fallback':True,'restart_scope':'process-restart','inputs':inputs,'client':{'path':str(binary),'sha256':inputs[str(binary)]},'steps':{}}
    for step in ['bootstrap','restore','transparent-discovery','shielded-discovery','send','pending','mined','scanned','restart','resume']:
        command = [sys.executable,str(adapter),'--binary',str(binary),'--trace-tool',str(a.trace_tool.resolve()),'--node',a.node,'--endpoint','{endpoint}','--scratch','{scratch}','--report','{report}','--step',step,'--evidence-dir',str(a.evidence_dir.resolve()),'--fence-python',str(a.fence_python.absolute()),'--proto-dir',str(a.proto_dir.resolve())]
        if a.wallet=='zkool':
            command += ['--variant',a.variant]
        case['steps'][step] = {'command':command,'expect':{'/assertions/'+step:True}}
    with a.output.open('x') as output:
        json.dump({'schema_version':2,'timeout_seconds':600,'cases':{a.wallet+'-'+case['variant']:case}},output,indent=2)


if __name__ == '__main__':
    main()
