#!/usr/bin/env bash
# restore-guest.sh <fixture.json> <verify-hashes: none|stores|all> <receipt.json>
#
# Runs on a fresh fixture host as root, after the stage tarball is unpacked at
# /tmp/zfx-stage. Mounts each fixture component, checks file hashes, starts a
# frozen node and the four Zinder services with pinned CPUs, waits for the
# lightwalletd-compatible reader, checks a wallet response against the
# fixture's chain fence, and writes a receipt. When a component declares
# signed_history, the reader serves from that catalog and route-check.py must
# prove it before the receipt is written.
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
# When the fixture pins the serving binaries, the stage must carry exactly those.
while read -r bin_name bin_sha; do
  grep -q -x "$bin_sha  $bin_name" "$stage/binaries.sha256" \
    || { log "stage $bin_name does not match the fixture's pinned $bin_sha"; exit 1; }
done < <(jq -r '.verified_with_release.binaries // {} | to_entries[] | "\(.key) \(.value)"' "$fixture")
chown zinder:zinder /var/log/zfx

mark mount
network=$(jq -er .network "$fixture")
jq -c '.components[]' "$fixture" | while read -r c; do
  name=$(jq -r .name <<< "$c") dir=$(jq -r .mount_path <<< "$c")
  dev=/dev/disk/by-id/google-$name
  [[ -e $dev ]] || { log "component $name has no attached disk at $dev"; exit 1; }
  install -d "$dir"
  # Catalog and oracle disks are only read. Hyperdisk Balanced attaches
  # read-write, so the mount itself keeps them unchanged.
  opts=noatime; [[ $(jq -r .kind <<< "$c") == catalog || $(jq -r .kind <<< "$c") == oracle ]] && opts=ro,noload
  mountpoint -q "$dir" || mount -o "$opts" "$dev" "$dir"
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
    # The component must hold exactly the listed files: an extra file, such as
    # a stray RocksDB WAL, would change what the writers open. Empty RocksDB
    # LOCK files and lost+found are ignored on both sides.
    diff -q <(awk '{print $2}' "$dir/$listing" | grep -v -E '(^|/)LOCK$' | sort) \
      <(cd "$dir" && find . -type f ! -name LOCK ! -path "./$listing" ! -path './lost+found/*' | sed 's#^\./##' | sort) >/dev/null \
      || { log "files under $dir differ from the set listed in $listing"; exit 1; }
    # Every listed file is read once; this also hydrates lazily restored blocks.
    (cd "$dir" && awk '{print $2}' "$listing" | tr '\n' '\0' | xargs -0 -P 8 -n 16 sha256sum) \
      | sort -k2 | diff -q - <(sort -k2 "$dir/$listing") >/dev/null \
      || { log "file hashes under $dir differ from $listing"; exit 1; }
    verified+=("$(jq -r .name <<< "$c")")
  done < <(jq -c '.components[]' "$fixture")
fi

mark check_history
history=$(jq -c 'first(.components[] | select(.signed_history)) // empty' "$fixture")
history_dir="" history_key="" history_verity=false
if [[ -n $history ]]; then
  mnt=$(jq -r .mount_path <<< "$history")
  history_dir=$mnt/$(jq -r .signed_history.directory <<< "$history")
  history_key=$mnt/$(jq -r .signed_history.publisher_key <<< "$history")
  [[ $(jq -r .filesystem <<< "$history") == ext4-verity ]] && history_verity=true
  [[ -s $history_dir/.publisher-head.json ]] \
    || { log "signed history directory $history_dir has no publisher checkpoint"; exit 1; }
  for pair in "$history_dir/current.json manifest_sha256" "$history_key publisher_key_sha256"; do
    file=${pair% *} want=$(jq -r ".signed_history.${pair#* }" <<< "$history")
    got=$(sha256sum "$file" | cut -d' ' -f1)
    [[ $got == "$want" ]] || { log "$file is $got, fixture records $want"; exit 1; }
  done
  # A writable component may hold root-owned files; the reader runs as the service user.
  [[ $(jq -r .kind <<< "$history") == catalog ]] || chown -R zinder:zinder "$history_dir" "$history_key"
  unreadable=$(runuser -u zinder -- find "$history_dir" "$history_key" -maxdepth 1 ! -readable -print 2>&1 || true)
  [[ -z $unreadable ]] || { log "the service user cannot read ${unreadable%%$'\n'*}"; exit 1; }
  log "reader will serve signed history from $history_dir (fs-verity required: $history_verity)"
else
  log "fixture declares no signed history; the reader serves every block range from canonical storage"
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
canonical_dir=$stores/$(jq -er '.components[] | select(.kind == "stores") | .paths.canonical' "$fixture")
wallet_dir=$stores/$(jq -er '.components[] | select(.kind == "stores") | .paths.wallet' "$fixture")
# Store snapshots may also be root-owned data; the writers open them read-write.
chown -R zinder:zinder "$canonical_dir" "$wallet_dir"
CANONICAL_PATH=$canonical_dir WALLET_PATH=$wallet_dir \
HISTORY_DIRECTORY=$history_dir HISTORY_PUBLISHER_KEY=$history_key HISTORY_REQUIRE_FS_VERITY=$history_verity \
REORG_WINDOW_BLOCKS=$(jq -r '.chain.reorg_window_blocks // 1000' "$fixture") \
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
sample_ok=true
[[ -z $want_sample || $sample_digest == "$want_sample" ]] || sample_ok=false

route=null route_ok=true
if [[ -n $history ]]; then
  mark route_check
  # The sample's 100-block range is the covered probe when the catalog holds
  # it, so its signed-segment response must equal the fixture's recorded bytes.
  highest=$(jq -r .signed_history.highest_height <<< "$history")
  covered=1730000:1730099 reference=(--reference /var/log/zfx/wallet-sample/block-range-1730000-1730099.json)
  (( highest >= 1730099 )) || { covered=$((highest - 199)):$((highest - 100)); reference=(); }
  /opt/zfx/route-check.py --grpc 127.0.0.1:9067 --ops 127.0.0.1:9107 \
    --pid "$(systemctl show -p MainPID --value zfx-fixture-compat)" --catalog "$history_dir" \
    --segments "$(jq -r .signed_history.segment_count <<< "$history")" --covered "$covered" "${reference[@]}" \
    --highest "$highest" --tip "$want_height" --out /var/log/zfx/route-check \
    --refusal-binary /opt/zfx/bin/zinder-compat-lightwalletd --refusal-config /etc/zfx/fixture/compat.toml \
    --trusted-key "$history_key" --run-as zinder >&2 || route_ok=false
  route=$(jq -c --arg dir "$history_dir" --argjson verity "$history_verity" \
    --arg manifest "$(sha256sum "$history_dir/current.json" | cut -d' ' -f1)" \
    --arg key "$(sha256sum "$history_key" | cut -d' ' -f1)" \
    '{directory: $dir, require_fs_verity: $verity, manifest_sha256: $manifest, publisher_key_sha256: $key} + .' \
    /var/log/zfx/route-check/route-check.json 2>/dev/null) \
    || route=$(jq -nc --arg dir "$history_dir" '{directory: $dir, passed: false, failed: ["route-check.py wrote no report"]}')
fi
mark finished

steps=$(for k in "${!at[@]}"; do printf '%s\t%s\n' "$k" "${at[$k]}"; done | jq -Rn '[inputs | split("\t") | {(.[0]): .[1]}] | add')
jq -n --arg fixture_id "$(jq -r .fixture_id "$fixture")" --arg network "$network" \
  --argjson steps "$steps" --argjson height "$got_height" --arg hash "$got_hash" \
  --arg verify "$verify" --argjson verified "$(printf '%s\n' "${verified[@]:-}" | jq -R . | jq -s 'map(select(. != ""))')" \
  --arg sample "$sample_digest" --arg want_sample "$want_sample" \
  --argjson ready "$(cat /var/log/zfx/compat-readyz.json)" \
  --arg binaries "$(sha256sum "$stage/binaries.sha256" | cut -d' ' -f1)" --argjson route "$route" \
  '{fixture_id: $fixture_id, network: $network, guest_steps_utc: $steps,
    verify_hashes: $verify, verified_components: $verified,
    served_tip: {height: $height, hash: $hash}, compat_readyz: $ready,
    wallet_sample_sha256sums_sha256: $sample,
    wallet_sample_matches_fixture: (if $want_sample == "" then null else ($sample == $want_sample) end),
    signed_history: $route,
    binaries_listing_sha256: $binaries}' > "$receipt.partial"
if [[ $sample_ok != true ]]; then
  mv "$receipt.partial" "$receipt.failed"
  log "wallet responses differ from the fixture: SHA256SUMS digest $sample_digest, fixture records $want_sample"
  exit 1
fi
if [[ $route_ok != true ]]; then
  mv "$receipt.partial" "$receipt.failed"
  log "the fixture declares signed history, but the route check failed: $(jq -c .failed <<< "$route")"
  exit 1
fi
mv "$receipt.partial" "$receipt"
cat "$receipt"
