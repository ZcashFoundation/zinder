#!/usr/bin/env python3
"""Verify a native WalletQuery server's live identity and canonical anchor.

This probe is deliberately out-of-band from the wallet harness: it dials the
selected native endpoint and compares `ServerInfo` and `TreeStateAtHeight`
against the pinned revision and the control node's anchor block. Run it with a
Python interpreter that has `grpcio`.
"""
import argparse
import json
from pathlib import Path
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proto-dir", required=True, type=Path)
    parser.add_argument("--endpoint", required=True, help="loopback native WalletQuery URL")
    parser.add_argument("--height", required=True, type=int)
    parser.add_argument("--hash", required=True)
    parser.add_argument("--revision", required=True)
    args = parser.parse_args()
    if not args.endpoint.startswith("http://127.0.0.1:"):
        parser.error("explicit loopback endpoint required")
    sys.path.insert(0, str(args.proto_dir.resolve()))
    import grpc  # noqa: PLC0415
    from zinder.v1.wallet import wallet_pb2  # noqa: PLC0415

    target = args.endpoint.removeprefix("http://")
    channel = grpc.insecure_channel(target)
    info_call = channel.unary_unary(
        "/zinder.v1.wallet.WalletQuery/ServerInfo",
        request_serializer=wallet_pb2.ServerInfoRequest.SerializeToString,
        response_deserializer=wallet_pb2.ServerInfoResponse.FromString)
    anchor_call = channel.unary_unary(
        "/zinder.v1.wallet.WalletQuery/TreeStateAtHeight",
        request_serializer=wallet_pb2.TreeStateAtHeightRequest.SerializeToString,
        response_deserializer=wallet_pb2.TreeStateResponse.FromString)
    info = info_call(wallet_pb2.ServerInfoRequest(), timeout=20).info
    anchor = anchor_call(wallet_pb2.TreeStateAtHeightRequest(height=args.height), timeout=30)
    channel.close()
    observed = {
        "network": info.common.network,
        "service_name": info.common.service_name,
        "build_git_commit": info.common.build_git_commit,
        "contract_revision": info.common.contract_revision,
        "anchor_height": anchor.height,
        "anchor_hash": anchor.block_hash,
        "expected_height": args.height,
        "expected_hash": args.hash,
        "expected_revision": args.revision,
    }
    observed["revision_verified"] = observed["build_git_commit"] == args.revision
    observed["anchor_verified"] = (anchor.height == args.height
                                   and anchor.block_hash.lower() == args.hash.lower())
    observed["network_verified"] = observed["network"] == "zcash-regtest"
    observed["ok"] = all((observed["revision_verified"], observed["anchor_verified"],
                          observed["network_verified"]))
    print(json.dumps(observed))
    if not observed["ok"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
