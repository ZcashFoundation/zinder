"""Check the selected local node and both Zinder wallet interfaces before a round trip."""
from hashlib import sha256
from http.client import HTTPConnection
import json
import re
import time
from urllib.parse import urlsplit

from wallet_fixture import loopback, require_service_ops, verify_node


GENESIS = {
    "regtest": "029f11d80ef9765602235e1bc9727e3eb6ba20839319f761fee920d63401e327",
    "testnet": "05a60a92d99d85997cce3b87616c089f6124d7342af37106edc76126334a2c38",
}
FULL_BLOCK_CAPS = {"wallet.read.full_block_at_v1", "wallet.read.full_block_range_v1"}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def http_json(url, path, payload=None):
    target = urlsplit(loopback(url))
    connection = HTTPConnection(target.hostname, target.port, timeout=15)
    try:
        if payload is None:
            connection.request("GET", path)
        else:
            connection.request("POST", path, json.dumps(payload), {"Content-Type": "application/json"})
        response = connection.getresponse()
        require(response.status == 200, f"local service returned HTTP {response.status}")
        return json.load(response)
    finally:
        connection.close()


class NodeRpcError(RuntimeError):
    def __init__(self, method, code):
        super().__init__(f"node RPC {method} failed with code {code}")
        self.code = code


def rpc(url, method, *params):
    """Make one JSON-RPC call to the selected loopback control node. Never retry a mutation."""
    response = http_json(url, "/", {"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    error = response.get("error")
    if error:
        require(isinstance(error, dict) and type(error.get("code")) is int, "invalid node RPC error")
        raise NodeRpcError(method, error["code"])
    require(response.get("id") == 1 and "result" in response, f"invalid {method} response")
    return response["result"]


def anchor(ready):
    pairs = [(ready.get(h), ready.get(b)) for h, b in (("initial_tip", "initial_hash"), ("current_tip", "current_hash"))
             if h in ready or b in ready]
    require(pairs and len(set(pairs)) == 1, "missing or conflicting immutable anchor")
    height, block_hash = pairs[0]
    require(type(height) is int and height > 0 and isinstance(block_hash, str)
            and re.fullmatch(r"[a-f0-9]{64}", block_hash), "invalid immutable anchor")
    return height, block_hash


def verify(ready: dict, network: str, revision: str) -> dict:
    """Return observed node, native, and lightwalletd identities for the same canonical chain."""
    import grpc
    import compact_formats_pb2 as compact
    import service_pb2 as compat
    from zinder.v1.wallet import wallet_pb2 as native

    require(network in GENESIS and ready["network"] == f"zcash-{network}", "fixture network mismatch")
    require(re.fullmatch(r"[0-9a-f]{40}", revision) and ready["zinder_commit"] == revision, "fixture commit mismatch")
    name = ready["network"]
    height, block_hash = anchor(ready)
    node = loopback(ready["node_rpc"])
    wallet_url = loopback(ready["lightwalletd"])
    query_url = loopback(ready["native_wallet_query"])
    require(rpc(node, "getblockhash", 0) == GENESIS[network], "wrong node genesis hash")
    require(rpc(node, "getblockhash", height) == block_hash, "node anchor differs from receipt")
    peer_count = len(rpc(node, "getpeerinfo"))
    if network == "regtest":
        require(peer_count == 0 and ready["isolation"]["default_ipv4_route"] is False, "Regtest not isolated")
        verify_node(node, height, block_hash, "zingo")
    else:
        require(peer_count > 0, "live Testnet node has no peers")

    for key, service, role in (
        ("compat_ops", "zinder-compat-lightwalletd", "compat-lightwalletd"),
        ("query_ops", "zinder-query", "query"),
    ):
        require_service_ops(
            lambda path, base_url=ready[key]: http_json(base_url, path),
            service,
            role,
            name,
            revision,
        )

    def call(channel, package, method, message, response, streaming=False):
        invoke = channel.unary_stream if streaming else channel.unary_unary
        return invoke(f"/{package}/{method}", request_serializer=type(message).SerializeToString,
                      response_deserializer=response.FromString)(message, timeout=15)

    def current_chain_block(tip_height, tip_hash):
        require(tip_height >= height and rpc(node, "getblockhash", tip_height) == tip_hash,
                "serving tip behind anchor or on different node chain")
        return {"height": tip_height, "hash": tip_hash}

    with grpc.insecure_channel(urlsplit(wallet_url).netloc) as channel:
        service = compat.DESCRIPTOR.services_by_name["CompactTxStreamer"].full_name
        info = call(channel, service, "GetLightdInfo", compat.Empty(), compat.LightdInfo)
        require(info.gitCommit == revision and info.chainName == "test", "wrong lightwalletd server")
        block = call(channel, service, "GetBlock", compat.BlockID(height=height), compact.CompactBlock)
        require(block.height == height and block.hash[::-1].hex() == block_hash, "lightwalletd anchor mismatch")
        tip = call(channel, service, "GetLatestBlock", compat.ChainSpec(), compat.BlockID)
        compat_tip = current_chain_block(tip.height, tip.hash[::-1].hex())

    with grpc.insecure_channel(urlsplit(query_url).netloc) as channel:
        service = native.DESCRIPTOR.services_by_name["WalletQuery"].full_name
        info = call(channel, service, "ServerInfo", native.ServerInfoRequest(), native.ServerInfoResponse).info.common
        require((info.service_name, info.network, info.build_git_commit) == ("zinder-query", name, revision),
                "wrong native wallet-query server")
        require(FULL_BLOCK_CAPS <= set(info.capabilities), "native query lacks required full-block capabilities")
        rows = call(channel, service, "FullBlocksInRange",
                    native.FullBlocksInRangeRequest(start_height=height, end_height=height),
                    native.FullBlocksInRangeChunk, streaming=True)
        first = next(rows, None)
        require(first is not None and next(rows, None) is None, "native query returned the wrong number of blocks")
        full = first.full_block
        require(full.height == height and full.block_hash == block_hash and full.payload_bytes
                and full.payload_bytes == bytes.fromhex(rpc(node, "getblock", block_hash, 0)),
                "native full block differs from node at anchor")
        tip = call(channel, service, "VisibleTipBlock", native.VisibleTipBlockRequest(),
                   native.VisibleTipBlockResponse).visible_tip_block
        query_tip = current_chain_block(tip.height, tip.block_hash)

    chain = rpc(node, "getblockchaininfo")
    require(chain["chain"] == "test" and rpc(node, "getblockhash", height) == block_hash, "node identity changed")
    node_tip = current_chain_block(chain["blocks"], chain["bestblockhash"])
    if network == "testnet":
        # A frozen store or a peer that is still bootstrapping cannot see a fresh faucet payment.
        age = time.time() - rpc(node, "getblockheader", node_tip["hash"], True)["time"]
        require(-2 * 3600 <= age <= 2 * 3600 and node_tip["height"] - min(compat_tip["height"], query_tip["height"]) <= 2,
                "live Testnet node or wallet service is behind the chain")
    return {"network": name, "revision": revision, "genesis": GENESIS[network],
            "anchor": {"height": height, "hash": block_hash}, "full_block_sha256": sha256(full.payload_bytes).hexdigest(),
            "native_full_block_capabilities": sorted(FULL_BLOCK_CAPS), "peer_count": peer_count,
            "node_tip": node_tip, "native_tip": query_tip, "compat_tip": compat_tip}


def main():
    import argparse
    import os
    from pathlib import Path
    from tempfile import NamedTemporaryFile

    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    capture = commands.add_parser("capture-testnet", help="verify a live local Testnet stack and save its immutable anchor")
    for key in ("node-rpc", "native-wallet-query", "lightwalletd", "query-ops", "compat-ops"):
        capture.add_argument("--" + key, required=True, type=loopback)
    capture.add_argument("--revision", required=True, help="full Zinder commit served on both wallet endpoints")
    capture.add_argument("--output", type=Path, required=True, help="new private JSON receipt in an existing directory")
    args = parser.parse_args()
    require(not os.path.lexists(args.output) and args.output.parent.is_dir(), "output must be a new file in an existing directory")
    require(rpc(args.node_rpc, "getblockhash", 0) == GENESIS["testnet"], "capture requires public Testnet genesis")
    # Both wallet readers can trail the writing node while a new block is published.
    height = rpc(args.node_rpc, "getblockcount") - 2
    require(height > 0, "node has no established Testnet chain")
    ready = {"network": "zcash-testnet", "zinder_commit": args.revision, "initial_tip": height,
             "initial_hash": rpc(args.node_rpc, "getblockhash", height), "node_rpc": args.node_rpc,
             "native_wallet_query": args.native_wallet_query, "lightwalletd": args.lightwalletd,
             "query_ops": args.query_ops, "compat_ops": args.compat_ops}
    ready["preflight"] = verify(ready, "testnet", args.revision)
    with NamedTemporaryFile(mode="w", encoding="utf-8", dir=args.output.parent) as private:
        json.dump(ready, private, indent=2)
        private.write("\n")
        private.flush()
        os.fsync(private.fileno())
        os.link(private.name, args.output)
    print(args.output)


if __name__ == "__main__":
    main()
