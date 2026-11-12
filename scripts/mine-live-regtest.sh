#!/usr/bin/env bash
# Mines the Regtest chain that Zinder's live and parity suites expect.
#
# Start Zebra with deploy/zebra/regtest.toml and its miner address set to the
# transparent broadcast test key, then run:
#
#   scripts/mine-live-regtest.sh <zebra-rpc-url> <tip-height>
#
# On a fresh chain, the first blocks pay their coinbase to Zebra's default
# Regtest unified address, so the Sapling (Canopy), Orchard (NU5 through
# NU6.2), and Ironwood (NU6.3 onward) commitment trees each hold notes. Later
# blocks pay the configured transparent miner address, which the live tests
# spend. The script then mines with `generate` until the tip reaches
# <tip-height>, and refuses a tip that has already passed it.
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo >&2 "usage: mine-live-regtest.sh <zebra-rpc-url> <tip-height>"
  exit 2
fi
rpc_url="$1"
target_tip="$2"

# Zebra's built-in Regtest unified address (Orchard, Sapling, and transparent
# receivers), from zebra-rpc's default miner addresses.
shielded_miner_address='uregtest1efxggx6lduhm2fx5lnrhxv7h7kpztlpa3ahf3n4w0q0zj5epj4av9xjq6ljsja3xk8z7rzd067kc7mgpy9448rdfzpfjz5gq389zdmpgnk6rp4ykk0xk6cmqw6zqcrnmsuaxv3yzsvcwsd4gagtalh0uzrdvy03nhmltjz2eu0232qlcs0zvxuqyut73yucd9gy5jaudnyt7yqhgpqv'
# Heights 1 through 10 cover Canopy, NU5 through NU6.2, and the first NU6.3
# blocks of deploy/zebra/regtest.toml.
shielded_prefix_blocks=10

rpc() {
  local method="$1"
  local params="$2"
  curl -sf -X POST -H 'content-type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"${method}\",\"params\":${params}}" \
    "$rpc_url"
}

rpc_result() {
  local response
  response="$(rpc "$1" "$2")"
  if ! grep -q '"error":null' <<<"$response" && grep -q '"error"' <<<"$response"; then
    echo >&2 "Zebra $1 failed: $response"
    exit 1
  fi
  sed -E 's/.*"result":[[:space:]]*([^,}]*).*/\1/' <<<"$response"
}

for _ in $(seq 1 60); do
  if rpc getblockchaininfo '[]' >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

tip="$(rpc_result getblockcount '[]')"
if [[ "$tip" -eq 0 ]]; then
  rpc_result generatetoaddress "[${shielded_prefix_blocks},\"${shielded_miner_address}\"]" >/dev/null
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
