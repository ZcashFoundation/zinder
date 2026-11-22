#!/usr/bin/env bash
# wallet-sample.sh <compat_addr> <out_dir> [taddr]
# Captures a fixed set of lightwalletd-compatible wallet responses as JSON and
# writes SHA256SUMS. Two deployments over the same fence must match byte for byte.
set -euo pipefail
addr=$1 out=$2 taddr=${3:-}
mkdir -p "$out"; cd "$out"
g() { local name=$1 method=$2 body=$3; grpcurl -plaintext -max-msg-sz 268435456 -format json -d "$body" "$addr" "cash.z.wallet.sdk.rpc.CompactTxStreamer/$method" > "$name.json"; }
g latest-block GetLatestBlock '{}'
for h in 1 419200 1046400 1687104 1730000 2726400 3592347 3593347; do g block-$h GetBlock "{\"height\":$h}"; done
for h in 1729999 1730000 3592347 3593347; do g tree-state-$h GetTreeState "{\"height\":$h}"; done
g block-range-1730000-1730099 GetBlockRange '{"start":{"height":1730000},"end":{"height":1730099}}'
g block-range-3593248-3593347 GetBlockRange '{"start":{"height":3593248},"end":{"height":3593347}}'
g block-range-nullifiers-3593248-3593347 GetBlockRangeNullifiers '{"start":{"height":3593248},"end":{"height":3593347}}' || rm -f block-range-nullifiers-3593248-3593347.json
g subtree-roots-sapling GetSubtreeRoots '{"shieldedProtocol":"sapling","startIndex":0,"maxEntries":0}'
g subtree-roots-orchard GetSubtreeRoots '{"shieldedProtocol":"orchard","startIndex":0,"maxEntries":0}'
if [[ -n $taddr ]]; then
  g taddr-balance GetTaddressBalance "{\"addresses\":[\"$taddr\"]}"
  g taddr-utxos GetAddressUtxos "{\"addresses\":[\"$taddr\"],\"startHeight\":0,\"maxEntries\":0}"
  g taddr-txids GetTaddressTxids "{\"address\":\"$taddr\",\"range\":{\"start\":{\"height\":3592347},\"end\":{\"height\":3593347}}}"
fi
sha256sum *.json > SHA256SUMS
wc -c *.json | tail -1
