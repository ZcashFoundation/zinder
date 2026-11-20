#!/usr/bin/env python3
"""Wait for a wallet endpoint to publish a Regtest height before client scanning."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import time
from urllib.parse import urlsplit


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--endpoint', required=True)
    p.add_argument('--height', required=True, type=int)
    p.add_argument('--proto-dir', required=True)
    p.add_argument('--output', required=True, type=Path)
    a = p.parse_args()
    url = urlsplit(a.endpoint)
    if url.scheme != 'http' or url.hostname != '127.0.0.1':
        p.error('isolated loopback endpoint required')
    sys.path.insert(0, a.proto_dir)
    import grpc
    import service_pb2 as pb
    import service_pb2_grpc as svc
    client = svc.CompactTxStreamerStub(grpc.insecure_channel(url.netloc))
    started = time.monotonic()
    samples = []
    while time.monotonic() - started < 180:
        try:
            tip = client.GetLatestBlock(pb.ChainSpec(), timeout=5).height
            block = client.GetBlock(pb.BlockID(height=a.height), timeout=5)
            rows = list(client.GetBlockRange(pb.BlockRange(start=pb.BlockID(height=a.height), end=pb.BlockID(height=a.height)), timeout=5))
            tree = client.GetTreeState(pb.BlockID(height=a.height), timeout=5)
            ready = tip >= a.height and block.height == a.height and len(rows) == 1 and rows[0].height == a.height and rows[0].hash == block.hash and tree.height == a.height
            samples.append({'elapsed_seconds':time.monotonic()-started,'tip':tip,'complete':ready})
            if ready:
                a.output.write_text(json.dumps({'endpoint':a.endpoint,'height':a.height,'publication_wait_seconds':time.monotonic()-started,'samples':samples,'block_sha256':hashlib.sha256(block.SerializeToString(deterministic=True)).hexdigest()}))
                return
        except grpc.RpcError as error:
            samples.append({'elapsed_seconds':time.monotonic()-started,'status':error.code().name})
        time.sleep(2)
    a.output.write_text(json.dumps({'endpoint':a.endpoint,'height':a.height,'timed_out':True,'samples':samples}))
    raise RuntimeError('serving endpoint did not publish the target within 180 seconds')


if __name__ == '__main__':
    main()
