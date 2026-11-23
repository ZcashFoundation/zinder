#!/usr/bin/env bash
# restore-guest.sh <fixture.json> <verify-hashes: none|stores|all> <attempt-dir>
#
# Runs on a fresh fixture host as root, after the stage tarball is unpacked at
# /tmp/zfx-stage. Mounts each fixture component, checks file hashes, starts a
# frozen node and the four Zinder services with pinned CPUs, waits for the
# lightwalletd-compatible reader, checks a wallet response against the
# fixture's chain fence, and writes the outcome. When the fixture declares
# signed_history, the reader serves from that catalog and route-check.py must
# prove it.
#
# <attempt-dir> must not exist. Its outcome.json is written once, atomically,
# on every exit: {"status": "succeeded", "receipt": {...}} or
# {"status": "failed", "step": ..., "reason": ..., "receipt": {...} | null}.
# A host runs one attempt: /var/lib/zfx-launch (ZFX_LAUNCH_RECORD) records it,
# and a later attempt fails without touching the disks a previous one may have
# changed.
set -euo pipefail
fixture=$1 verify=$2 out=$3
stage=/tmp/zfx-stage launch=${ZFX_LAUNCH_RECORD:-/var/lib/zfx-launch}
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
mkdir -p "$(dirname "$out")"
# A second launch of the same attempt finds its directory and leaves it alone.
mkdir "$out" 2>/dev/null || { echo "attempt $out already exists; not starting it again" >&2; exit 0; }
attempt=$(basename "$out")
step=launch finished=false
log() { printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" | tee -a "$out/guest.log" >&2; }
now() { date -u +%FT%T.%3NZ; }
declare -A at
mark() { step=$1; at[$1]=$(now); log "step $1"; }
write_outcome() {
  python3 -I - "$out" "$attempt" "$1" "$2" "$3" "$4" <<'EOF'
import json, os, sys
out, attempt, status, step, reason, receipt_path = sys.argv[1:]
receipt = None
if receipt_path and os.path.exists(receipt_path):
    try:
        receipt = json.load(open(receipt_path))
    except ValueError:
        receipt = None
outcome = {'attempt': attempt, 'status': status, 'step': step or None, 'reason': reason or None, 'receipt': receipt}
with open(os.path.join(out, 'outcome.json.partial'), 'w') as f:
    json.dump(outcome, f)
    f.write('\n')
os.replace(os.path.join(out, 'outcome.json.partial'), os.path.join(out, 'outcome.json'))
EOF
}
on_exit() {
  local code=$?
  [[ $finished == true ]] && return
  # A host that failed its checks must not keep serving wallets.
  [[ ${at[start_zinder]+set} ]] && systemctl stop zfx-fixture-compat 2>/dev/null || true
  write_outcome failed "$step" "$(tail -n 1 "$out/guest.log" 2>/dev/null | cut -d' ' -f2-) (exit $code)" "$out/receipt.json.partial"
}
trap on_exit EXIT

mark launch
if ! mkdir "$launch" 2>/dev/null; then
  log "this host already ran attempt $(cat "$launch/attempt" 2>/dev/null || echo unknown); restore a fresh host"
  exit 1
fi
echo "$attempt" > "$launch/attempt"

mark check_fixture
if ! history=$(python3 -I "$here/fixture-gate.py" history "$fixture" 2>&1); then log "$history"; exit 1; fi
if [[ $history == null ]]; then
  log "fixture declares no signed history; the reader serves block ranges from canonical storage and its live tail"
else
  log "fixture declares signed history in $(jq -r .directory <<< "$history" 2>/dev/null || echo "$history")"
fi

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
(cd /opt/zfx/bin && sha256sum --quiet -c "$stage/binaries.sha256") || { log "stage binaries differ from binaries.sha256"; exit 1; }
(cd /usr/local/bin && sha256sum --quiet -c "$stage/tools/tools.sha256") || { log "stage tools differ from tools.sha256"; exit 1; }
# When the fixture pins the serving binaries, the stage must carry exactly those.
while read -r bin_name bin_sha; do
  grep -q -x "$bin_sha  $bin_name" "$stage/binaries.sha256" \
    || { log "stage $bin_name does not match the fixture's pinned $bin_sha"; exit 1; }
done < <(jq -r '.verified_with_release.binaries // {} | to_entries[] | "\(.key) \(.value)"' "$fixture")
chown zinder:zinder /var/log/zfx
chown -R zinder:zinder "$out"

mark mount
network=$(jq -er .network "$fixture")
while read -r c; do
  name=$(jq -r .name <<< "$c") dir=$(jq -r .mount_path <<< "$c") kind=$(jq -r .kind <<< "$c")
  dev=/dev/disk/by-id/google-$name
  [[ -e $dev ]] || { log "component $name has no attached disk at $dev"; exit 1; }
  install -d "$dir"
  # Catalog and oracle disks are only read. Hyperdisk Balanced attaches
  # read-write, so the mount itself keeps them unchanged.
  opts=noatime; [[ $kind == catalog || $kind == oracle ]] && opts=ro,noload
  mountpoint -q "$dir" || mount -o "$opts" "$dev" "$dir"
done < <(jq -c '.components[]' "$fixture")

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
    # Every listed file is read once, which also fetches the restored disk's
    # blocks that hold them.
    (cd "$dir" && awk '{print $2}' "$listing" | tr '\n' '\0' | xargs -0 -P 8 -n 16 sha256sum) \
      | sort -k2 | diff -q - <(sort -k2 "$dir/$listing") >/dev/null \
      || { log "file hashes under $dir differ from $listing"; exit 1; }
    verified+=("$(jq -r .name <<< "$c")")
  done < <(jq -c '.components[]' "$fixture")
fi

mark check_history
history_dir="" history_key="" history_verity=false
if [[ $history != null ]]; then
  history_dir=$(jq -r .directory <<< "$history") history_key=$(jq -r .publisher_key <<< "$history")
  history_verity=$(jq -r .require_fs_verity <<< "$history")
  # A writable component may hold root-owned files; the reader runs as the service user.
  [[ $(jq -r .kind <<< "$history") == catalog ]] || chown -R zinder:zinder "$history_dir" "$history_key"
  unreadable=$(runuser -u zinder -- find "$history_dir" "$history_key" -maxdepth 1 ! -readable -print 2>&1 || true)
  [[ -z $unreadable ]] || { log "the service user cannot read ${unreadable%%$'\n'*}"; exit 1; }
  # The manifest and key are hashed once, here, and the bounds and segment
  # files come from those verified bytes.
  if ! bounds=$(python3 -I /opt/zfx/fixture-gate.py verify-manifest "$fixture" "$out/history-bounds.json" 2>&1); then
    log "$bounds"; exit 1
  fi
  log "reader will serve signed history: $bounds"
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
curl -sf 127.0.0.1:9107/readyz > "$out/compat-readyz.json" \
  || { log "lightwalletd compatibility reader did not become ready"; exit 1; }

mark wallet_request
latest=$(grpcurl -plaintext -format json -d '{}' 127.0.0.1:9067 cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLatestBlock)
got_height=$(jq -r .height <<< "$latest")
got_hash=$(jq -r .hash <<< "$latest" | base64 -d | od -An -v -tx1 | tr -d ' \n' | fold -w2 | tac | tr -d '\n')
[[ $got_height == "$want_height" && $got_hash == "$want_hash" ]] \
  || { log "reader serves $got_height $got_hash, fixture records $want_height $want_hash"; exit 1; }
mark wallet_sample
taddr=$(jq -r '.wallet_sample.transparent_address // empty' "$fixture")
/opt/zfx/wallet-sample.sh 127.0.0.1:9067 "$out/wallet-sample" "$taddr" >&2
sample_digest=$(sha256sum "$out/wallet-sample/SHA256SUMS" | cut -d' ' -f1)
want_sample=$(jq -r '.wallet_sample.sha256sums_sha256 // empty' "$fixture")

route_report=""
if [[ $history != null ]]; then
  mark route_check
  # The sample's 100-block range is the covered probe when the catalog holds
  # it and the fixture pins the sample, so its signed-segment response must
  # equal bytes the fixture recorded from a canonical-only reader.
  reference=()
  if [[ -n $want_sample ]] && jq -e '.first_height <= 1730000 and 1730099 <= .last_height' "$out/history-bounds.json" >/dev/null; then
    reference=(--reference-range 1730000:1730099 --reference "$out/wallet-sample/block-range-1730000-1730099.json")
  fi
  route_report=$out/route-check/route-check.json
  /opt/zfx/route-check.py --grpc 127.0.0.1:9067 --ops 127.0.0.1:9107 \
    --pid "$(systemctl show -p MainPID --value zfx-fixture-compat)" --bounds "$out/history-bounds.json" \
    --tip "$want_height" --out "$out/route-check" "${reference[@]}" \
    --refusal-binary /opt/zfx/bin/zinder-compat-lightwalletd --refusal-config /etc/zfx/fixture/compat.toml \
    --run-as zinder >&2 || true
fi

steps=$(for k in "${!at[@]}"; do printf '%s\t%s\n' "$k" "${at[$k]}"; done | jq -Rn '[inputs | split("\t") | {(.[0]): .[1]}] | add')
route=null
if [[ -n $route_report ]]; then
  route=$(jq -c '. + {declared: true}' "$route_report" 2>/dev/null) \
    || route='{"declared": true, "proof": "external", "passed": false, "failed": ["report"], "error": "route-check.py wrote no readable report"}'
fi
jq -n --arg fixture_id "$(jq -r .fixture_id "$fixture")" --arg network "$network" --arg attempt "$attempt" \
  --argjson steps "$steps" --argjson height "$got_height" --arg hash "$got_hash" \
  --arg verify "$verify" --argjson verified "$(printf '%s\n' "${verified[@]:-}" | jq -R . | jq -s 'map(select(. != ""))')" \
  --arg sample "$sample_digest" --arg want_sample "$want_sample" \
  --slurpfile ready "$out/compat-readyz.json" --argjson route "$route" \
  --arg binaries "$(sha256sum "$stage/binaries.sha256" | cut -d' ' -f1)" \
  '{fixture_id: $fixture_id, network: $network, attempt: $attempt, guest_steps_utc: $steps,
    verify_hashes: $verify, verified_components: $verified,
    served_tip: {height: $height, hash: $hash}, compat_readyz: $ready[0],
    wallet_sample_sha256sums_sha256: $sample,
    wallet_sample_matches_fixture: (if $want_sample == "" then null else ($sample == $want_sample) end),
    signed_history: $route,
    binaries_listing_sha256: $binaries}' > "$out/receipt.json.partial"
mark verdict
if [[ -n $want_sample && $sample_digest != "$want_sample" ]]; then
  log "wallet responses differ from the fixture: SHA256SUMS digest $sample_digest, fixture records $want_sample"
  exit 1
fi
if [[ -n $route_report ]]; then
  verdict=$(python3 -I /opt/zfx/fixture-gate.py route-verdict "$route_report" 2>&1) \
    || { log "the fixture declares signed history, but the route check failed: ${verdict//$'\n'/; }"; exit 1; }
fi
finished=true
write_outcome succeeded "" "" "$out/receipt.json.partial"
cat "$out/outcome.json"
