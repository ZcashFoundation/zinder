#!/usr/bin/env bash
# restore-guest.sh <fixture.json> <verify-hashes: none|stores|all> <receipt.json>
#
# Runs on a fresh fixture host as root, after the stage tarball is unpacked at
# /tmp/zfx-stage. Mounts each fixture component, checks file hashes, starts a
# frozen node and the four Zinder services with pinned CPUs, waits for the
# lightwalletd-compatible reader, checks a wallet response against the
# fixture's chain fence, and writes a receipt.
set -euo pipefail
fixture=$1 verify=$2 receipt=$3
stage=/tmp/zfx-stage
log() { printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" >&2; }
now() { date -u +%FT%T.%3NZ; }
declare -A at
mark() { at[$1]=$(now); log "step $1"; }

mark bootstrap
install -d /opt/zfx /opt/zfx/bin /var/log/zfx
getent group zinder >/dev/null || groupadd --system --gid 10001 zinder
getent passwd zinder >/dev/null || useradd --system --uid 10001 --gid 10001 \
  --home-dir /nonexistent --shell /usr/sbin/nologin zinder
cp "$stage"/bin/* /opt/zfx/bin/
cp "$stage"/tools/zebrad "$stage"/tools/jq "$stage"/tools/grpcurl /usr/local/bin/
cp "$stage"/guest/*.sh "$stage"/guest/*.py /opt/zfx/
cp -r "$stage"/proto-all /opt/zfx/ 2>/dev/null || true
chmod +x /opt/zfx/*.sh /opt/zfx/*.py
(cd /opt/zfx/bin && sha256sum --quiet -c "$stage/binaries.sha256")
(cd /usr/local/bin && sha256sum --quiet -c "$stage/tools/tools.sha256")
chown zinder:zinder /var/log/zfx

mark mount
network=$(jq -er .network "$fixture")
jq -c '.components[]' "$fixture" | while read -r c; do
  name=$(jq -r .name <<< "$c") dir=$(jq -r .mount_path <<< "$c")
  dev=/dev/disk/by-id/google-$name
  [[ -e $dev ]] || { log "component $name has no attached disk at $dev"; exit 1; }
  install -d "$dir"
  mountpoint -q "$dir" || mount -o noatime "$dev" "$dir"
done

mark verify_hashes
verified=()
if [[ $verify != none ]]; then
  while read -r c; do
    kind=$(jq -r .kind <<< "$c") dir=$(jq -r .mount_path <<< "$c")
    listing=$(jq -r '.files_listing // empty' <<< "$c") want=$(jq -r '.files_sha256 // empty' <<< "$c")
    [[ $verify == stores && $kind != stores ]] && continue
    [[ -n $listing && -n $want ]] || { log "component $(jq -r .name <<< "$c") records no file listing"; exit 1; }
    got=$(sha256sum "$dir/$listing" | cut -d' ' -f1)
    [[ $got == "$want" ]] || { log "listing $dir/$listing is $got, fixture records $want"; exit 1; }
    # Every listed file is read once; this also hydrates lazily restored blocks.
    (cd "$dir" && awk '{print $2}' "$listing" | tr '\n' '\0' | xargs -0 -P 8 -n 16 sha256sum) \
      | sort -k2 | diff -q - <(sort -k2 "$dir/$listing") >/dev/null \
      || { log "file hashes under $dir differ from $listing"; exit 1; }
    verified+=("$(jq -r .name <<< "$c")")
  done < <(jq -c '.components[]' "$fixture")
fi

mark start_node
node_dir=$(jq -er '.components[] | select(.kind == "node-state") | .mount_path + "/" + (.paths.zebra_cache // ".")' "$fixture")
# Node snapshots may be root-owned data; the node runs as the service user.
chown -R zinder:zinder "$node_dir"
sed "s#^cache_dir = \"/srv/node\"#cache_dir = \"$node_dir\"#" "$stage/guest/zebrad.toml" > /etc/zfx-zebrad.toml
cp "$stage/guest/zfx-zebrad.service" /etc/systemd/system/zfx-zebrad.service
systemctl daemon-reload
systemctl start zfx-zebrad
want_height=$(jq -er .chain.visible_height "$fixture") want_hash=$(jq -er .chain.visible_block_hash "$fixture")
rpc() {
  curl -s -u "$(cat /run/zebra-cookie/.cookie)" -H 'content-type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$1\",\"params\":$2}" http://127.0.0.1:8232
}
for _ in $(seq 1 120); do
  [[ -s /run/zebra-cookie/.cookie ]] && [[ $(rpc getbestblockhash '[]' | jq -r '.result // empty') == "$want_hash" ]] && break
  sleep 5
done
[[ $(rpc getbestblockhash '[]' | jq -r '.result // empty') == "$want_hash" ]] \
  || { log "node did not reach the fixture tip $want_height $want_hash"; exit 1; }

mark start_zinder
stores=$(jq -er '.components[] | select(.kind == "stores") | .mount_path' "$fixture")
CANONICAL_PATH=$stores/$(jq -er '.components[] | select(.kind == "stores") | .paths.canonical' "$fixture") \
WALLET_PATH=$stores/$(jq -er '.components[] | select(.kind == "stores") | .paths.wallet' "$fixture") \
  /opt/zfx/stack.sh fixture "$stores/.zfx-run" "$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')" 0 /run/zebra-cookie/.cookie >&2
chown zinder:zinder "$stores/.zfx-run"
systemctl start zfx-fixture-ingest
for _ in $(seq 1 120); do curl -sf 127.0.0.1:9105/readyz >/dev/null && break; sleep 2; done
systemctl start zfx-fixture-projector zfx-fixture-query zfx-fixture-compat

mark wait_ready
for _ in $(seq 1 600); do curl -sf 127.0.0.1:9107/readyz >/dev/null && break; sleep 2; done
curl -sf 127.0.0.1:9107/readyz > /var/log/zfx/compat-readyz.json \
  || { log "lightwalletd compatibility reader did not become ready"; exit 1; }

mark wallet_request
latest=$(grpcurl -plaintext -format json -d '{}' 127.0.0.1:9067 cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLatestBlock)
got_height=$(jq -r .height <<< "$latest")
got_hash=$(jq -r .hash <<< "$latest" | base64 -d | od -An -v -tx1 | tr -d ' \n' | fold -w2 | tac | tr -d '\n')
[[ $got_height == "$want_height" && $got_hash == "$want_hash" ]] \
  || { log "reader serves $got_height $got_hash, fixture records $want_height $want_hash"; exit 1; }
mark wallet_sample
taddr=$(jq -r '.wallet_sample.transparent_address // empty' "$fixture")
/opt/zfx/wallet-sample.sh 127.0.0.1:9067 /var/log/zfx/wallet-sample "$taddr" >&2
sample_digest=$(sha256sum /var/log/zfx/wallet-sample/SHA256SUMS | cut -d' ' -f1)
want_sample=$(jq -r '.wallet_sample.sha256sums_sha256 // empty' "$fixture")
mark finished

steps=$(for k in "${!at[@]}"; do printf '%s\t%s\n' "$k" "${at[$k]}"; done | jq -Rn '[inputs | split("\t") | {(.[0]): .[1]}] | add')
jq -n --arg fixture_id "$(jq -r .fixture_id "$fixture")" --arg network "$network" \
  --argjson steps "$steps" --argjson height "$got_height" --arg hash "$got_hash" \
  --arg verify "$verify" --argjson verified "$(printf '%s\n' "${verified[@]:-}" | jq -R . | jq -s 'map(select(. != ""))')" \
  --arg sample "$sample_digest" --arg want_sample "$want_sample" \
  --argjson ready "$(cat /var/log/zfx/compat-readyz.json)" \
  --arg binaries "$(sha256sum "$stage/binaries.sha256" | cut -d' ' -f1)" \
  '{fixture_id: $fixture_id, network: $network, guest_steps_utc: $steps,
    verify_hashes: $verify, verified_components: $verified,
    served_tip: {height: $height, hash: $hash}, compat_readyz: $ready,
    wallet_sample_sha256sums_sha256: $sample,
    wallet_sample_matches_fixture: (if $want_sample == "" then null else ($sample == $want_sample) end),
    binaries_listing_sha256: $binaries}' > "$receipt"
cat "$receipt"
