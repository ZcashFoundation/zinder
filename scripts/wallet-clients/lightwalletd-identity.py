#!/usr/bin/env python3
"""Verify the live identity and immutable anchor of a lightwalletd endpoint.

The probe is deliberately out-of-band from the wallet under test: it dials the
loopback endpoint the wallet is configured to use and reads `GetLightdInfo`,
`GetBlock` at one block height, and `GetLatestBlock`. It records the observed
BIP70 chain name, build commit, served tip, and anchor block hash, then compares
them with the expected chain, revision, and readiness anchor.

The probe records no wallet payloads and does not attempt method-level RPC
attribution. Run it with an interpreter that has `grpcio` and the lightwalletd
proto modules on `--proto-dir`.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from urllib.parse import urlsplit

SHA256 = re.compile(r"[0-9a-f]{64}")
LOOPBACK = re.compile(r"http://(127\.0\.0\.1|localhost):(\d{1,5})")

# lightwalletd reports the BIP70 chain name, where Testnet and Regtest share
# "test". The fixture's network identity therefore still comes from its
# readiness record and control node; this probe verifies the wire chain name.
REGTEST_CHAIN_NAME = "test"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def loopback(endpoint):
    match = LOOPBACK.fullmatch(endpoint or "")
    require(match is not None and 1024 <= int(match.group(2)) <= 65535,
            "explicit loopback endpoint required")
    return endpoint


def observe(endpoint, height, proto_dir, timeout=30):
    """Read the live identity and one anchor block from a loopback endpoint."""
    endpoint = loopback(endpoint)
    proto_dir = Path(proto_dir).resolve()
    require(proto_dir.is_dir(), "lightwalletd proto directory is missing")
    sys.path.insert(0, str(proto_dir))
    import grpc  # noqa: PLC0415
    import compact_formats_pb2 as compact  # noqa: PLC0415
    import service_pb2 as service  # noqa: PLC0415

    target = urlsplit(endpoint).netloc
    channel = grpc.insecure_channel(target)
    service_name = service.DESCRIPTOR.services_by_name["CompactTxStreamer"].full_name

    def call(method, request, response, timeout_seconds):
        invoke = channel.unary_unary(
            f"/{service_name}/{method}",
            request_serializer=type(request).SerializeToString,
            response_deserializer=response.FromString)
        return invoke(request, timeout=timeout_seconds)

    try:
        info = call("GetLightdInfo", service.Empty(), service.LightdInfo, timeout)
        block = call("GetBlock", service.BlockID(height=height), compact.CompactBlock, timeout)
        tip = call("GetLatestBlock", service.ChainSpec(), service.BlockID, timeout)
    finally:
        channel.close()
    require(block.height == height, "endpoint served another block height")
    observed = {
        "chain_name": info.chainName,
        "build_git_commit": info.gitCommit,
        "version": info.version,
        "vendor": info.vendor,
        "sapling_activation_height": info.saplingActivationHeight,
        "consensus_branch_id": info.consensusBranchId,
        "upgrade_name": info.upgradeName,
        "lightwallet_protocol_version": info.lightwalletProtocolVersion,
        "served_height": info.blockHeight,
        "served_tip_height": tip.height,
        "served_tip_hash": bytes(tip.hash)[::-1].hex(),
        "anchor_height": block.height,
        "anchor_hash": bytes(block.hash)[::-1].hex(),
    }
    require(SHA256.fullmatch(observed["served_tip_hash"]) is not None,
            "endpoint returned an invalid tip hash")
    require(SHA256.fullmatch(observed["anchor_hash"]) is not None,
            "endpoint returned an invalid anchor hash")
    return observed


def evaluate(observed, expected_chain, expected_revision, expected_height, expected_hash):
    """Compare one observation with the expected identity and immutable anchor."""
    require(isinstance(observed, dict), "observation is not a record")
    require(isinstance(expected_chain, str) and expected_chain, "expected chain is empty")
    require(isinstance(expected_revision, str) and re.fullmatch(r"[0-9a-f]{40}",
                                                                expected_revision),
            "expected revision is not a full commit")
    require(type(expected_height) is int and expected_height > 0,
            "expected anchor height is invalid")
    require(isinstance(expected_hash, str) and SHA256.fullmatch(expected_hash),
            "expected anchor hash is invalid")
    record = dict(observed)
    record.update({
        "expected_chain": expected_chain,
        "expected_revision": expected_revision,
        "expected_height": expected_height,
        "expected_hash": expected_hash,
    })
    record["network_verified"] = observed.get("chain_name") == expected_chain
    record["revision_verified"] = observed.get("build_git_commit") == expected_revision
    record["anchor_verified"] = (observed.get("anchor_height") == expected_height
                                 and observed.get("anchor_hash") == expected_hash)
    record["tip_verified"] = (observed.get("served_tip_height")
                              == observed.get("served_height"))
    record["ok"] = all(record[key] for key in ("network_verified", "revision_verified",
                                               "anchor_verified"))
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proto-dir", required=True, type=Path)
    parser.add_argument("--endpoint", required=True, help="loopback lightwalletd URL")
    parser.add_argument("--height", required=True, type=int,
                        help="block height to fetch and verify the anchor hash for")
    parser.add_argument("--expect-height", type=int,
                        help="expected anchor height (defaults to --height)")
    parser.add_argument("--hash", required=True, help="expected anchor block hash")
    parser.add_argument("--revision", required=True, help="expected full build commit")
    parser.add_argument("--expected-chain", default=REGTEST_CHAIN_NAME,
                        help="expected BIP70 chain name, for example test")
    parser.add_argument("--phase", default="observed",
                        help="when the observation was taken, for example before-use")
    args = parser.parse_args()
    expected_height = args.expect_height or args.height
    observed = observe(args.endpoint, args.height, args.proto_dir)
    record = evaluate(observed, args.expected_chain, args.revision,
                      expected_height, args.hash)
    record.update({
        "scope": "live-probe",
        "phase": args.phase,
        "endpoint": loopback(args.endpoint),
        "provenance": {
            "transport": "grpc insecure loopback "
                         "GetLightdInfo/GetBlock/GetLatestBlock",
            "payloads_recorded": False,
            "python": sys.version.split()[0],
            "grpc": __import__("grpc").__version__,
            "proto_modules": {
                "service_pb2.py": digest(Path(args.proto_dir) / "service_pb2.py"),
                "compact_formats_pb2.py": digest(Path(args.proto_dir)
                                                 / "compact_formats_pb2.py"),
            },
        },
    })
    print(json.dumps(record, sort_keys=True))
    raise SystemExit(0 if record["ok"] else 1)


if __name__ == "__main__":
    os.umask(0o077)
    main()
