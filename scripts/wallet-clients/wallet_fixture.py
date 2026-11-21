"""Bind an engine case to its disposable node and selected lightwalletd service."""
import json
from pathlib import Path
import re
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, Request, build_opener


def loopback(url):
    parts = urlsplit(url)
    if not parts.port or url != f"http://127.0.0.1:{parts.port}":
        raise ValueError("expected an explicit loopback HTTP URL with a port")
    return url


def binding(ready_path, candidate_path=None, endpoint=None, node=None):
    ready = json.loads(Path(ready_path).read_text())
    if ready["network"] != "zcash-regtest" or not re.fullmatch("[a-f0-9]{40}", ready["zinder_commit"]):
        raise ValueError("expected a revision-specific Regtest readiness receipt")
    tip = ready.get("current_tip", ready.get("initial_tip"))
    block = ready.get("current_hash", ready.get("initial_hash"))
    if type(tip) is not int or tip < 1 or not isinstance(block, str) or not re.fullmatch("[a-f0-9]{64}", block):
        raise ValueError("missing immutable fixture height/hash")
    selected = loopback(ready["lightwalletd"])
    control = loopback(ready["node_rpc"])
    if candidate_path:
        other = json.loads(Path(candidate_path).read_text())
        if (other["network"] != ready["network"] or other["node_rpc"] != control
                or other["shared_release_dependencies"] != ready["zinder_commit"]
                or not re.fullmatch("[a-f0-9]{40}", other["source_commit"])
                or not re.fullmatch("[a-f0-9]{64}", other["binary_sha256"])):
            raise ValueError("candidate receipt does not identify the fixture and candidate build")
        selected = loopback(other["client_endpoint"])
        if selected == ready["lightwalletd"]:
            raise ValueError("a side-by-side candidate needs a distinct endpoint")
    if endpoint is not None and endpoint != selected:
        raise ValueError("wallet endpoint differs from the selected readiness receipt")
    if node is not None and node != control:
        raise ValueError("node endpoint differs from the fixture receipt")
    return selected, control, tip, block


def verify_node(url, tip, block, wallet):
    opener = build_opener(ProxyHandler({}))

    def rpc(method, *params):
        data = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
        req = Request(url, data, {"Content-Type": "application/json"})
        with opener.open(req, timeout=15) as response:
            answer = json.load(response)
        if answer.get("error"):
            raise ValueError("fixture verification failed for " + method)
        return answer["result"]

    if rpc("getblockhash", tip) != block:
        raise ValueError("live node hash differs from the immutable fixture anchor")
    info = rpc("getblockchaininfo")
    names = "Overwinter Sapling Blossom Heartwood Canopy NU5 NU6 NU6.1 NU6.2".split()
    expected = dict.fromkeys(names, 1)
    expected["NU6.3"] = 1 if wallet == "zingo" else 250
    actual = {v["name"]: v["activationheight"] for v in info["upgrades"].values()}
    if info["chain"] != "test" or actual != expected:
        raise ValueError("live fixture does not use this wallet's Regtest activation schedule")
