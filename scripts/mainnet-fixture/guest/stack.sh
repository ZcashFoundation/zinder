#!/usr/bin/env bash
# stack.sh <name> <root> <owner_hex> <port_offset> <cookie_path>
# Writes TOML configs and systemd units for ingest, projector, query, and
# lightwalletd compatibility over <root>/canonical and <root>/wallet, or over
# CANONICAL_PATH and WALLET_PATH when set. Control tokens, checkpoint staging,
# and reader secondaries go under <root>. When HISTORY_DIRECTORY and
# HISTORY_PUBLISHER_KEY are set, the compatibility reader follows that signed
# history directory as single-host Compose does, and HISTORY_REQUIRE_FS_VERITY
# (true or false, default false) sets its fs-verity requirement. Run as root.
set -euo pipefail
name=$1 root=$2 owner=$3 off=$4 cookie=$5
bin=${ZFX_BIN:-/opt/zfx/bin}
canonical=${CANONICAL_PATH:-$root/canonical} wallet=${WALLET_PATH:-$root/wallet}
# Every role must use the window the stores were built with.
reorg=${REORG_WINDOW_BLOCKS:-1000}
[[ $reorg =~ ^[1-9][0-9]*$ ]] || { echo "REORG_WINDOW_BLOCKS must be a positive integer" >&2; exit 1; }
history_dir=${HISTORY_DIRECTORY:-} history_key=${HISTORY_PUBLISHER_KEY:-} history_verity=${HISTORY_REQUIRE_FS_VERITY:-false}
[[ -n $history_dir$history_key && ( -z $history_dir || -z $history_key ) ]] && { echo "set both HISTORY_DIRECTORY and HISTORY_PUBLISHER_KEY, or neither" >&2; exit 1; }
[[ $history_verity == true || $history_verity == false ]] || { echo "HISTORY_REQUIRE_FS_VERITY must be true or false" >&2; exit 1; }
[[ $name =~ ^[a-z][a-z0-9-]*$ ]] || { echo "stack name must be lowercase letters, digits, and hyphens" >&2; exit 1; }
# Paths go into TOML basic strings verbatim, so they may not hold a quote,
# a backslash, or a control character. Spaces are fine.
for path in "$root" "$canonical" "$wallet" "$cookie" "$bin" "$history_dir" "$history_key"; do
  [[ $path != *[\"\\]* && $path != *[[:cntrl:]]* ]] || { echo "path cannot appear in TOML as is: $path" >&2; exit 1; }
done
cfg=/etc/zfx/$name; sec=$root/.control; mkdir -p "$cfg"
p() { echo $(( $1 + off )); }
install -d -o zinder -g zinder -m 0700 "$sec" "$root/checkpoints" "$root/projector" "$root/query" "$root/compat"
for t in ingest checkpoint; do
  [[ -s $sec/$t.token ]] || (umask 077; head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$sec/$t.token"; chown zinder:zinder "$sec/$t.token")
done
cat > "$cfg/ingest.toml" <<T
[network]
name = "zcash-mainnet"
[node]
json_rpc_addr = "http://127.0.0.1:8232"
request_timeout_secs = 30
max_response_bytes = 67108864
[node.auth]
method = "cookie"
path = "$cookie"
[storage]
path = "$canonical"
raw_blob_policy = "none"
[ingest]
source = "zebra-json-rpc"
explorer_views = false
reorg_window_blocks = $reorg
[ingest.run_overrides]
coverage = "wallet-serving"
[ingest.tree_state_densification]
enabled = false
[ingest_control]
listen_addr = "127.0.0.1:$(p 9100)"
checkpoint_staging_root = "$root/checkpoints"
bearer_token_path = "$sec/ingest.token"
checkpoint_bearer_token_path = "$sec/checkpoint.token"
[ops]
listen_addr = "127.0.0.1:$(p 9105)"
T
cat > "$cfg/projector.toml" <<T
[network]
name = "zcash-mainnet"
[storage]
path = "$canonical"
secondary_path = "$root/projector/canonical-secondary"
raw_blob_policy = "none"
[wallet]
path = "$wallet"
[projector]
reorg_window_blocks = $reorg
lease_duration_seconds = 14400
build_owner_hex = "$owner"
[ingest_control]
addr = "http://127.0.0.1:$(p 9100)"
bearer_token_path = "$sec/ingest.token"
[projector_control]
listen_addr = "127.0.0.1:$(p 9101)"
bearer_token_path = "$sec/checkpoint.token"
checkpoint_staging_root = "$root/checkpoints"
[ops]
listen_addr = "127.0.0.1:$(p 9110)"
T
for role in query compat; do
  if [[ $role == query ]]; then lst=$(p 9102); ops=$(p 9106); else lst=$(p 9067); ops=$(p 9107); fi
  {
  cat <<T
[network]
name = "zcash-mainnet"
[node]
json_rpc_addr = "http://127.0.0.1:8232"
request_timeout_secs = 30
broadcast_timeout_secs = 7
[node.auth]
method = "cookie"
path = "$cookie"
[storage]
path = "$canonical"
secondary_path = "$root/$role/canonical-secondary"
raw_blob_policy = "none"
[wallet]
path = "$wallet"
secondary_path = "$root/$role/wallet-secondary"
[ingest_control]
addr = "http://127.0.0.1:$(p 9100)"
bearer_token_path = "$sec/ingest.token"
[ops]
listen_addr = "127.0.0.1:$ops"
[security]
allow_public_bind = false
T
  if [[ $role == query ]]; then printf '[query]\nlisten_addr = "127.0.0.1:%s"\nreorg_window_blocks = %s\npair_convergence_attempts = 12\n' "$lst" "$reorg"
  else printf '[compat]\nlisten_addr = "127.0.0.1:%s"\nreorg_window_blocks = %s\npair_convergence_attempts = 12\nmax_concurrent_block_range_streams = 100\nping_very_insecure = false\nserving = "wallet"\n' "$lst" "$reorg"
    [[ -z $history_dir ]] || printf '[compat.shielded_history]\ndirectory = "%s"\npublisher_key = "%s"\nrequire_fs_verity = %s\n' "$history_dir" "$history_key" "$history_verity"
  fi
  } > "$cfg/$role.toml"
done
declare -A exe=([ingest]=zinder-ingest [projector]=zinder-projector [query]=zinder-query [compat]=zinder-compat-lightwalletd)
declare -A cpus=([ingest]=${CPU_INGEST:-2-3} [projector]=${CPU_PROJECTOR:-2-3} [query]=${CPU_QUERY:-4-5} [compat]=${CPU_COMPAT:-4-5})
for role in ingest projector query compat; do
  after=""; [[ $role != ingest ]] && after="zfx-$name-ingest.service"
  cat > /etc/systemd/system/zfx-$name-$role.service <<U
[Unit]
Description=zfx $name $role
After=network-online.target $after
[Service]
Type=simple
User=zinder
Group=zinder
ExecStart=$bin/${exe[$role]} --config $cfg/$role.toml
Restart=no
LimitNOFILE=65536
AllowedCPUs=${cpus[$role]}
StandardOutput=append:/var/log/zfx/$name-$role.log
StandardError=append:/var/log/zfx/$name-$role.log
[Install]
WantedBy=multi-user.target
U
done
mkdir -p /var/log/zfx; chown zinder:zinder /var/log/zfx
systemctl daemon-reload
echo "stack $name written: $cfg"
