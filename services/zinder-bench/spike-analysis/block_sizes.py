#!/usr/bin/env python3
"""Fetch per-block size and tx count from the node (Testnet) for a height range. Cookie read from file, never printed."""
import base64, json, sys, urllib.request, concurrent.futures as cf
cookie_path, start, end, out = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
auth = base64.b64encode(open(cookie_path).read().strip().encode()).decode()
URL = "http://127.0.0.1:49232"
def call(method, params):
    req = urllib.request.Request(URL, data=json.dumps({"jsonrpc":"2.0","id":1,"method":method,"params":params}).encode(),
        headers={"Content-Type":"application/json","Authorization":"Basic "+auth})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)["result"]
def one(h):
    b = call("getblock", [str(h), 1])
    return h, b["size"], len(b["tx"])
if end <= 0:
    end = call("getblockcount", []) + end
with open(out, "w") as f, cf.ThreadPoolExecutor(16) as ex:
    for h, size, ntx in ex.map(one, range(start, end + 1)):
        f.write(f"{h} {size} {ntx}\n")
print("done", start, end)
