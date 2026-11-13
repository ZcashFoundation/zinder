#!/usr/bin/env bash
# Captures Zinder's Zakura shielded block fixtures from a fresh Regtest node.
#
# Start Zakura with deploy/zakura/regtest-fixture-capture.toml as
# docs/runbooks/capturing-node-fixtures.md describes, then run from the
# repository root:
#
#   scripts/capture-zakura-shielded-fixtures.sh <node-rpc-url> <image-reference> [output-dir]
#
# <image-reference> is the digest-pinned image the node runs; it is recorded in
# each fixture's provenance. The script mines the chain, signs every spend with
# zinder-testkit's broadcast test key through the `sign_test_spend` example,
# and writes three fixtures to [output-dir], by default
# services/zinder-ingest/tests/fixtures:
#
#   zakura-<version>-regtest-orchard-block-<h>.json   a v5 transparent-to-Orchard
#                                                     shielding spend before NU6.3
#   zakura-<version>-regtest-ironwood-block-<h>.json  a v6 transparent-to-Ironwood
#                                                     shielding spend and a v6
#                                                     transparent spend under NU6.3
#   zakura-<version>-regtest-nu7-block-<h>.json       the same two spends under NU7
#
# It needs `cargo`, `curl`, and `jq`.
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo >&2 "usage: capture-zakura-shielded-fixtures.sh <node-rpc-url> <image-reference> [output-dir]"
  exit 2
fi
rpc_url="$1"
image_reference="$2"
output_dir="${3:-services/zinder-ingest/tests/fixtures}"

capture_config=deploy/zakura/regtest-fixture-capture.toml
coinbase_maturity_blocks=100
# Heights of the capture schedule in deploy/zakura/regtest-fixture-capture.toml.
nu6_3_height=130
nu7_height=160
# The Orchard block comes once the first coinbase has matured; the other two
# blocks follow their activation by two blocks.
orchard_block_height=111
ironwood_block_height=$((nu6_3_height + 2))
nu7_block_height=$((nu7_height + 2))

rpc() {
  local response
  response="$(curl -s -X POST -H 'content-type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$1\",\"params\":$2}" \
    "$rpc_url")" || true
  if ! jq -e 'has("result") and .error == null' <<<"$response" >/dev/null 2>&1; then
    echo >&2 "node $1 failed: ${response:-no response}"
    exit 1
  fi
  jq -c '.result' <<<"$response"
}

cargo build -q -p zinder-testkit --example sign_test_spend --example print_broadcast_test_address
example_dir="${CARGO_TARGET_DIR:-target}/debug/examples"
broadcast_address="$("$example_dir/print_broadcast_test_address")"
miner_address="$("$example_dir/print_broadcast_test_address" --unified)"
if ! grep -qF "miner_address = \"${miner_address}\"" "$capture_config"; then
  echo >&2 "${capture_config} does not mine to the test unified address ${miner_address}"
  exit 1
fi

for _ in $(seq 1 60); do
  if rpc getblockcount '[]' >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
if [[ "$(rpc getblockcount '[]')" -ne 0 ]]; then
  echo >&2 "the node must start from an empty chain"
  exit 1
fi
schedule="$(rpc getblockchaininfo '[]' | jq -c '[.upgrades[] | {key: .name, value: .activationheight}] | from_entries')"
if [[ "$(jq -r '."NU6.3"' <<<"$schedule")" -ne "$nu6_3_height" || "$(jq -r '.NU7' <<<"$schedule")" -ne "$nu7_height" ]]; then
  echo >&2 "the node does not run the capture schedule: ${schedule}"
  exit 1
fi
node_build="$(rpc getinfo '[]' | jq -r '.build')"
node_version="$(rpc getinfo '[]' | jq -r '.subversion | capture("Zakura:(?<v>[^/]+)").v')"

mine_to() {
  local tip
  tip="$(rpc getblockcount '[]')"
  if [[ "$tip" -lt "$1" ]]; then
    rpc generate "[$(($1 - tip))]" >/dev/null
  fi
}

# Outpoints this capture already spent, one "txid:vout" per line. A file,
# because the functions below run in command-substitution subshells.
spent_outpoints="$(mktemp)"
trap 'rm -f "$spent_outpoints"' EXIT

# Prints the oldest matured output that pays the broadcast test key and that
# this capture has not spent, as a getaddressutxos entry, and reserves it.
reserve_matured_output() {
  local target_height utxos utxo outpoint
  target_height=$(($(rpc getblockcount '[]') + 1))
  utxos="$(rpc getaddressutxos "[{\"addresses\":[\"${broadcast_address}\"]}]" |
    jq -c --argjson cutoff "$((target_height - coinbase_maturity_blocks))" \
      '[.[] | select(.height <= $cutoff)] | sort_by(.height) | .[]')"
  while read -r utxo; do
    [[ -n "$utxo" ]] || continue
    outpoint="$(jq -r '"\(.txid):\(.outputIndex)"' <<<"$utxo")"
    if grep -qxF "$outpoint" "$spent_outpoints"; then
      continue
    fi
    echo "$outpoint" >>"$spent_outpoints"
    echo "$utxo"
    return
  done <<<"$utxos"
  echo >&2 "no matured output pays ${broadcast_address} at height ${target_height}"
  exit 1
}

# Signs a spend of one reserved output into <pool> for the next block,
# broadcasts it, and prints its txid.
broadcast_spend() {
  local utxo raw_transaction
  utxo="$(reserve_matured_output)"
  raw_transaction="$("$example_dir/sign_test_spend" "$rpc_url" "$1" \
    "$(jq -r '.txid' <<<"$utxo")" "$(jq -r '.outputIndex' <<<"$utxo")" "$(jq -r '.satoshis' <<<"$utxo")")"
  rpc sendrawtransaction "[\"${raw_transaction}\"]" | jq -r '.'
}

# Mines one block that must hold exactly the given txids after its coinbase,
# in any order, and prints its hash.
mine_block_with() {
  local block_hash mined
  block_hash="$(rpc generate '[1]' | jq -r '.[0]')"
  mined="$(rpc getblock "[\"${block_hash}\",1]" | jq -c '.tx[1:] | sort')"
  if [[ "$mined" != "$(jq -cn '$ARGS.positional | sort' --args "$@")" ]]; then
    echo >&2 "block ${block_hash} mined ${mined}, expected $*"
    exit 1
  fi
  echo "$block_hash"
}

capture_date="$(date -u +%Y-%m-%d)"

# Writes the fixture <kind> for <block-hash>; <description> says what the
# block holds.
write_fixture() {
  local kind="$1" block_hash="$2" description="$3"
  local block raw_block transactions height path provenance
  block="$(rpc getblock "[\"${block_hash}\",1]")"
  raw_block="$(rpc getblock "[\"${block_hash}\",0]" | jq -r '.')"
  transactions="$(jq -r '.tx[]' <<<"$block" | while read -r txid; do
    rpc getrawtransaction "[\"${txid}\",1]" | jq -c '{txid, authdigest}'
  done | jq -sc '.')"
  height="$(jq -r '.height' <<<"$block")"
  path="${output_dir}/zakura-${node_version}-regtest-${kind}-block-${height}.json"
  provenance="Captured on ${capture_date} from Zakura ${node_build} (${image_reference}) on an isolated Regtest started with ${capture_config}, which activates Overwinter through Canopy at height 1, NU5 at 2, NU6 at 3, NU6.1 at 4, NU6.2 at 5, NU6.3 at ${nu6_3_height}, and NU7 at ${nu7_height}, with lockbox and Major Grants funding streams and one zero-value lockbox disbursement. Every block was mined with generate to ${miner_address}, the unified address of zinder-testkit's broadcast test seed, which Zakura pays through its P2PKH receiver before NU6.3 and as an Ironwood note to its Orchard receiver from NU6.3 onward. Block ${height} ${description} Every spend was signed by zinder-testkit's broadcast test key, and every shielded note pays its Orchard receiver. raw_block_hex comes from getblock(hash,0); height, hash, previousblockhash, time, and trees come from getblock(hash,1); each txid and authdigest comes from getrawtransaction(txid,1)."
  jq -n \
    --arg provenance "$provenance" \
    --arg raw_block_hex "$raw_block" \
    --argjson block "$block" \
    --argjson transactions "$transactions" \
    '{
      provenance: $provenance,
      network: "zcash-regtest",
      height: $block.height,
      hash: $block.hash,
      previousblockhash: $block.previousblockhash,
      time: $block.time,
      trees: {
        sapling: ($block.trees.sapling.size // 0),
        orchard: ($block.trees.orchard.size // 0),
        ironwood: ($block.trees.ironwood.size // 0)
      },
      coinbase_txid: $transactions[0].txid,
      coinbase_authdigest: $transactions[0].authdigest,
      transactions: $transactions,
      raw_block_hex: $raw_block_hex
    }' >"$path"
  echo "wrote ${path}"
}

mine_to $((orchard_block_height - 1))
orchard_spend="$(broadcast_spend orchard)"
block_hash="$(mine_block_with "$orchard_spend")"
write_fixture orchard "$block_hash" \
  "is a pre-NU6.3 block under the NU6.2 consensus branch 0x5437f330. Besides its coinbase, which pays the P2PKH receiver, it holds one version-5 shielding spend that moves a matured coinbase output into one Orchard note, padded to two actions."

mine_to $((ironwood_block_height - 1))
ironwood_spend="$(broadcast_spend ironwood)"
transparent_spend="$(broadcast_spend transparent)"
block_hash="$(mine_block_with "$ironwood_spend" "$transparent_spend")"
write_fixture ironwood "$block_hash" \
  "is a post-NU6.3 block under the consensus branch 0x37a5165b. Besides its coinbase, whose miner reward is one Ironwood action, it holds a version-6 shielding spend that moves a matured coinbase output into one Ironwood note, padded to two actions, and a version-6 transparent spend."

mine_to $((nu7_block_height - 1))
ironwood_spend="$(broadcast_spend ironwood)"
transparent_spend="$(broadcast_spend transparent)"
block_hash="$(mine_block_with "$ironwood_spend" "$transparent_spend")"
write_fixture nu7 "$block_hash" \
  "is a post-NU7 block under the consensus branch 0x77190ad9. Besides its coinbase, whose miner reward is one Ironwood action, it holds a version-6 shielding spend that moves a matured coinbase output into one Ironwood note, padded to two actions, and a version-6 transparent spend."
