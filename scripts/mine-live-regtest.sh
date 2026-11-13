#!/usr/bin/env bash
# Mines the Regtest chain that Zinder's live and parity suites expect.
#
# Start the node with deploy/zebra/regtest.toml or deploy/zakura/regtest.toml,
# then run:
#
#   scripts/mine-live-regtest.sh <node-rpc-url> <tip-height>
#
# On a fresh chain, the script first asks the node to pay the first 10 blocks
# to the default Regtest unified address with `generatetoaddress`. Zebra
# supports it, and its coinbase then puts notes in the Sapling (Canopy),
# Orchard (NU5 through NU6.2), and Ironwood (NU6.3 onward) commitment trees;
# the script checks all three at height 10. Later blocks pay the node's
# configured miner address, which the live tests spend.
#
# A node that answers `generatetoaddress` with JSON-RPC error -32601 (method
# not found), such as Zakura, mines every block to its configured miner
# address instead. Zakura's coinbase cannot pay Sapling or Orchard, so the live
# workflows start it with the unified address as miner, mine 10 blocks with
# this script, and restart it with the transparent broadcast test key before
# mining the rest.
#
# The script then mines with `generate` until the tip reaches <tip-height>, and
# refuses a tip that has already passed it. It needs `curl` and `jq`.
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo >&2 "usage: mine-live-regtest.sh <node-rpc-url> <tip-height>"
  exit 2
fi
rpc_url="$1"
target_tip="$2"

# The default Regtest unified address (Orchard, Sapling, and transparent
# receivers) that Zebra and Zakura both ship.
shielded_miner_address='uregtest1efxggx6lduhm2fx5lnrhxv7h7kpztlpa3ahf3n4w0q0zj5epj4av9xjq6ljsja3xk8z7rzd067kc7mgpy9448rdfzpfjz5gq389zdmpgnk6rp4ykk0xk6cmqw6zqcrnmsuaxv3yzsvcwsd4gagtalh0uzrdvy03nhmltjz2eu0232qlcs0zvxuqyut73yucd9gy5jaudnyt7yqhgpqv'
# Heights 1 through 10 cover Canopy, NU5 through NU6.2, and the first NU6.3
# blocks of the checked-in Regtest schedule.
shielded_prefix_blocks=10
json_rpc_method_not_found=-32601

# Prints the node's JSON-RPC response. A JSON-RPC error can arrive with a
# non-2xx HTTP status, so the body is kept either way.
rpc() {
  curl -s -X POST -H 'content-type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$1\",\"params\":$2}" \
    "$rpc_url"
}

# Prints the result of a JSON-RPC call, or exits with the node's response.
rpc_result() {
  local response
  response="$(rpc "$1" "$2")" || true
  if ! jq -e 'has("result") and .error == null' <<<"$response" >/dev/null 2>&1; then
    echo >&2 "node $1 failed: ${response:-no response}"
    exit 1
  fi
  jq -c '.result' <<<"$response"
}

# Exits unless every shielded commitment tree holds notes at the end of the
# prefix.
require_shielded_prefix() {
  local block sizes sapling orchard ironwood
  block="$(rpc_result getblock "[\"${shielded_prefix_blocks}\",1]")"
  sizes="$(jq -r '[.trees.sapling.size // 0, .trees.orchard.size // 0, .trees.ironwood.size // 0] | @tsv' <<<"$block")"
  read -r sapling orchard ironwood <<<"$sizes"
  if [[ "$sapling" -eq 0 || "$orchard" -eq 0 || "$ironwood" -eq 0 ]]; then
    echo >&2 "shielded prefix is incomplete at height ${shielded_prefix_blocks}: Sapling ${sapling}, Orchard ${orchard}, Ironwood ${ironwood}"
    exit 1
  fi
  echo "shielded prefix: Sapling ${sapling}, Orchard ${orchard}, Ironwood ${ironwood} notes at height ${shielded_prefix_blocks}"
}

for _ in $(seq 1 60); do
  if rpc getblockchaininfo '[]' | jq -e '.result' >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

tip="$(rpc_result getblockcount '[]')"
if [[ "$tip" -eq 0 ]]; then
  response="$(rpc generatetoaddress "[${shielded_prefix_blocks},\"${shielded_miner_address}\"]")" || true
  if jq -e ".error.code == ${json_rpc_method_not_found}" <<<"$response" >/dev/null 2>&1; then
    echo "node has no generatetoaddress; mining every block to its configured miner address"
  elif jq -e 'has("result") and .error == null' <<<"$response" >/dev/null 2>&1; then
    require_shielded_prefix
  else
    echo >&2 "node generatetoaddress failed: ${response:-no response}"
    exit 1
  fi
  tip="$(rpc_result getblockcount '[]')"
fi
if [[ "$tip" -gt "$target_tip" ]]; then
  echo >&2 "Regtest tip ${tip} is already above the requested tip ${target_tip}"
  exit 1
fi
if [[ "$tip" -lt "$target_tip" ]]; then
  rpc_result generate "[$((target_tip - tip))]" >/dev/null
fi
echo "regtest tip: $(rpc_result getblockcount '[]')"
