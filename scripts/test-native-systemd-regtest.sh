#!/usr/bin/env bash
# Run on a disposable systemd host with a mined, unauthenticated Regtest node.
set -euo pipefail

[[ $# -eq 1 ]] || { echo >&2 "usage: $0 RELEASE_ARCHIVE"; exit 2; }
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
rpc_addr="${ZINDER_NODE__JSON_RPC_ADDR:?set the Regtest JSON-RPC URL}"
[[ "$rpc_addr" =~ ^http://127\.0\.0\.1:[0-9]+$ ]] || { echo >&2 "expected a loopback Regtest URL"; exit 2; }
curl -fsS --max-time 10 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"getblockchaininfo","params":[]}' "$rpc_addr" \
  | jq -e '.result | (.chain == "regtest" or .chain == "test")
      and .upgrades["77190ad9"].activationheight == 140
      and .upgrades["37a5165b"].activationheight == 6
      and .blocks >= 60' >/dev/null \
  || { echo >&2 "expected the mined CI Regtest chain through NU7"; exit 1; }
command -v grpcurl >/dev/null
sudo -n true

config=/etc/zinder/native
state=/var/lib/zinder
binary=/usr/local/bin/zinder
unit_dir=/run/systemd/system
target=zinder-native.target
retry_units=(zinder-native-{index,projector}.service)
optional_units=(zinder-native-{explorer,compat-cipherscan}.service)
units=("${retry_units[@]}" zinder-native-{query,compat-lightwalletd}.service "${optional_units[@]}" "$target")
retry_dropins=("${retry_units[@]/#/$unit_dir/}")
retry_dropins=("${retry_dropins[@]/%/.d}")
target_wants="$unit_dir/$target.wants"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/zinder-systemd.XXXXXX")"
trap 'rm -rf -- "$scratch"' EXIT
tar -xzf "$1" --no-same-owner -C "$scratch" --strip-components=1
(
  cd "$scratch"
  sha256sum --check --strict SHA256SUMS >/dev/null
)

for path in "$config" "$state" "$binary" "${units[@]/#/$unit_dir/}" "${retry_dropins[@]}" "$target_wants"; do
  if sudo test -e "$path" || sudo test -L "$path"; then
    echo >&2 "Refusing to overwrite $path"
    exit 1
  fi
done
for unit in "${units[@]}"; do
  if [[ "$(systemctl show -p LoadState --value "$unit")" != "not-found" ]]; then
    echo >&2 "Refusing to replace existing unit $unit"
    exit 1
  fi
done
if getent passwd zinder >/dev/null || getent group zinder >/dev/null; then
  echo >&2 "Refusing to reuse an existing zinder account"
  exit 1
fi

had_etc_zinder=false
if sudo test -d /etc/zinder; then
  had_etc_zinder=true
fi
sudo useradd --system --user-group --home-dir "$state" --no-create-home --shell /usr/sbin/nologin zinder
cleanup() {
  local status=$?
  if (( status != 0 )); then
    sudo journalctl -b --no-pager -n 120 "${units[@]/#/-u}" >&2 || true
  fi
  sudo systemctl stop "${units[@]}" || true
  sudo rm -f -- "$binary" "${units[@]/#/$unit_dir/}"
  sudo rm -rf -- "${retry_dropins[@]}" "$target_wants"
  sudo systemctl daemon-reload
  sudo rm -rf -- "$config" "$state"
  if ! "$had_etc_zinder"; then
    sudo rmdir -- /etc/zinder 2>/dev/null || true
  fi
  sudo userdel zinder
  rm -rf -- "$scratch"
}
trap cleanup EXIT

sudo install -m 0755 "$scratch/bin/zinder" "$binary"
sudo install -m 0644 "$scratch"/systemd/*.service "$scratch"/systemd/*.target "$unit_dir/"
sudo install -d -m 0750 -o root -g zinder "$config"
sed -e 's/^ZINDER_NETWORK__NAME=.*/ZINDER_NETWORK__NAME=zcash-regtest/' \
  -e "s|^ZINDER_NODE__JSON_RPC_ADDR=.*|ZINDER_NODE__JSON_RPC_ADDR=$rpc_addr|" \
  -e '/^ZINDER_NODE__AUTH__/d' \
  "$scratch/systemd/zinder.env" > "$scratch/regtest.env"
grep -Fxq 'ZINDER_DEPLOYMENT__PROFILE=wallet' "$scratch/regtest.env" \
  || { echo >&2 "Expected the archive to default to the wallet profile"; exit 1; }
echo 'ZINDER_INGEST__EXPLORER_VIEWS_MIN_FREE_BYTES=1073741824' >> "$scratch/regtest.env"
sudo install -m 0600 "$scratch/regtest.env" "$config/zinder.env"
for token in ingest checkpoint; do
  openssl rand -hex 32 | sudo install -m 0440 -o root -g zinder /dev/stdin "$config/$token.token"
done

sudo systemd-analyze verify "$repo_root/deploy/systemd/zinder.service" "${units[@]/#/$unit_dir/}"
echo "systemd-analyze verified the Compose wrapper and all native units"

# Fail each writer's first main process before it can bind its readiness endpoint.
for role in ingest projector; do
  dropin="$unit_dir/zinder-native-${role/ingest/index}.service.d"
  sudo install -d -m 0755 "$dropin"
  sudo tee "$dropin/fail-first-start.conf" >/dev/null <<EOF
[Service]
ExecStart=
ExecStart=/bin/sh -c 'if mkdir $state/.failed-first-$role; then sleep 1; exit 1; fi; exec $binary run $role'
EOF
done
sudo systemctl daemon-reload
timeout 180 sudo systemctl start "$target"
for unit in "${retry_units[@]}"; do
  [[ "$(systemctl show -p NRestarts --value "$unit")" == 1 ]] \
    || { echo >&2 "Expected one automatic restart of $unit"; exit 1; }
done
for port in 9105 9110 9106 9107; do
  "$binary" healthcheck --ready --addr "127.0.0.1:$port"
done
for unit in "${optional_units[@]}"; do
  [[ "$(systemctl show -p ActiveState --value "$unit")" == inactive ]] \
    || { echo >&2 "Expected $unit to be inactive with the wallet profile"; exit 1; }
done
echo "All four wallet roles returned HTTP 200 on /readyz after both writer retries, with Explorer and Cipherscan inactive"

proto="$repo_root/crates/zinder-proto/proto"
grpcurl -plaintext -max-time 10 -import-path "$proto" -proto zinder/v1/wallet/wallet.proto \
  -d '{}' 127.0.0.1:9102 zinder.v1.wallet.WalletQuery/ServerInfo \
  | jq -e '.info.common.network == "zcash-regtest" and .info.common.serviceName == "zinder-query"' >/dev/null
grpcurl -plaintext -max-time 10 -import-path "$proto/compat/lightwalletd" -proto service.proto \
  -d '{}' 127.0.0.1:9067 cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLightdInfo \
  | jq -e '.chainName == "test" and (.blockHeight | tonumber) >= 60' >/dev/null
echo "WalletQuery ServerInfo and GetLightdInfo returned Regtest data"

sudo systemctl stop "$target"
sudo rm -rf -- "${retry_dropins[@]}"
sed 's/^ZINDER_DEPLOYMENT__PROFILE=wallet$/ZINDER_DEPLOYMENT__PROFILE=explorer/' \
  "$scratch/regtest.env" > "$scratch/explorer.env"
sudo install -m 0600 "$scratch/explorer.env" "$config/zinder.env"
sudo systemctl enable --runtime "${optional_units[@]}"
timeout 180 sudo systemctl start "$target"
for port in 9069 9108; do
  "$binary" healthcheck --ready --addr "127.0.0.1:$port"
done
grpcurl -plaintext -max-time 10 -import-path "$proto" -proto zinder/v1/explorer/explorer.proto \
  -d '{}' 127.0.0.1:9068 zinder.v1.explorer.ExplorerQuery/ServerInfo \
  | jq -e '.info.common.network == "zcash-regtest" and .info.common.serviceName == "zinder-explorer"' >/dev/null
echo "Explorer and Cipherscan are ready and ExplorerQuery ServerInfo returned Regtest data"
