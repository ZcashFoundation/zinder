#!/usr/bin/env bash
set -euo pipefail

repository_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
validator="$repository_root/scripts/validate-deployment-admission.sh"
image_packager="$repository_root/scripts/package-release-images.sh"
release_images_catalog="$repository_root/deploy/release-images.json"
temporary_directory="$(mktemp -d "${TMPDIR:-/tmp}/zinder-deployment-admission.XXXXXX")"
trap 'rm -rf -- "$temporary_directory"' EXIT

fail() {
  echo >&2 "deployment admission test failed: $*"
  exit 1
}

expect_rejected() {
  local description="$1"
  shift

  if bash "$validator" "$@" >/dev/null 2>&1; then
    fail "$description was admitted"
  fi
}

expect_rejected_because() {
  local description="$1"
  local expected_error="$2"
  shift 2

  local error_file="$temporary_directory/rejection-error.txt"
  if bash "$validator" "$@" >/dev/null 2>"$error_file"; then
    fail "$description was admitted"
  fi
  grep -Fq -- "$expected_error" "$error_file" \
    || fail "$description was rejected for an unrelated reason: $(cat "$error_file")"
}

bash "$validator" \
  --deployment-class production \
  --target zinder
bash "$validator" \
  --deployment-class canary \
  --target zinder
bash "$validator" \
  --deployment-class diagnostic \
  --target zinder

expect_rejected \
  "the retired single-volume wallet-serving target as production" \
  --deployment-class production \
  --target zinder-wallet-serving
expect_rejected \
  "the mixed single-container target" \
  --deployment-class production \
  --target zinder-single-container
expect_rejected \
  "the retired ingest-only canonical-runtime target as production" \
  --deployment-class production \
  --target zinder-canonical-runtime

bash "$validator" \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --release-images-catalog "$release_images_catalog"
bash "$validator" \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --dockerfile "$repository_root/deploy/Dockerfile"
grep -Fq 'group: release-publication' \
  "$repository_root/.github/workflows/release.yml" \
  || fail "the release workflow does not globally serialize publication"
bash "$validator" \
  --build-images-workflow "$repository_root/.github/workflows/build-images.yml" \
  --release-images-catalog "$release_images_catalog"
bash "$validator" \
  --prometheus-config "$repository_root/deploy/observability/prometheus.yml"
bash "$validator" \
  --kubernetes-manifest "$repository_root/deploy/kubernetes/statefulset.yaml"

if grep -Eq 'zinder[^[:space:]]*:latest' \
  "$repository_root/deploy/docker-compose.yml" \
  "$repository_root/deploy/kubernetes/statefulset.yaml"; then
  fail "the deployment Compose file or Kubernetes manifest defaults Zinder to :latest"
fi

deployment_env_names=(mainnet testnet regtest zakura-mainnet zakura-testnet)

env_file_value() {
  sed -n "s/^$2=//p" "$repository_root/deploy/.env.$1"
}

if grep -Eiq 'JSON_RPC_ADDR:?-|INDEXER_GRPC_ADDR:?-[^}]|//(zebra|zakura)\b|z3-|:1?8232\b|:8230\b|:8155\b' \
  "$repository_root/deploy/docker-compose.yml"; then
  fail "the deployment Compose topology defaults to a hardcoded node"
fi

for deployment_env in "${deployment_env_names[@]}"; do
  grep -Fqx 'ZINDER_NODE__INDEXER_GRPC_ADDR=' \
    "$repository_root/deploy/.env.$deployment_env" \
    || fail "the $deployment_env env file enables streaming mempool by default"
done
for zakura_network in mainnet testnet; do
  for shared_name in ZINDER_DEPLOY_NETWORK ZINDER_NETWORK__NAME \
    ZINDER_INDEX_OPS_HOST_PORT ZINDER_SERVE_OPS_HOST_PORT ZINDER_COMPAT_OPS_HOST_PORT \
    ZINDER_QUERY_HOST_PORT ZINDER_COMPAT_HOST_PORT ZINDER_EXPLORER_HOST_PORT; do
    [[ "$(env_file_value "zakura-$zakura_network" "$shared_name")" \
      == "$(env_file_value "$zakura_network" "$shared_name")" ]] \
      || fail "the zakura-$zakura_network env file does not share $shared_name with the $zakura_network deployment"
  done
done

resolved_compose="$temporary_directory/resolved-compose.json"
control_secrets="$temporary_directory/control-secrets"
mkdir -p "$control_secrets"
printf 'test-ingest-token\n' > "$control_secrets/ingest.token"
printf 'test-checkpoint-token\n' > "$control_secrets/checkpoint.token"
ZINDER_CONTROL_SECRETS_DIR="$control_secrets" \
docker compose \
  --env-file "$repository_root/deploy/.env.testnet" \
  -f "$repository_root/deploy/docker-compose.yml" \
  --profile observability \
  config --format json > "$resolved_compose"
bash "$validator" --compose-contract "$resolved_compose"

resolved_compose_full="$temporary_directory/resolved-compose-full.json"
ZINDER_CONTROL_SECRETS_DIR="$control_secrets" \
docker compose \
  --env-file "$repository_root/deploy/.env.testnet" \
  -f "$repository_root/deploy/docker-compose.yml" \
  --profile observability \
  --profile history-publisher \
  config --format json > "$resolved_compose_full"
bash "$validator" --compose-contract "$resolved_compose_full"
jq -e '
  .services as $services
  | $services["zinder-index"].command == ["index", "--preset", "wallet"]
  and $services["zinder-serve"].command == ["serve", "--readers-only", "--preset", "wallet"]
  and $services["zinder-index"].environment.ZINDER_DEPLOYMENT__PROFILE == "wallet"
  and $services["zinder-serve"].environment.ZINDER_DEPLOYMENT__PROFILE == "wallet"
  and $services["zinder-index"].environment.ZINDER_INGEST__SOURCE == "zebra-json-rpc"
  and $services["zinder-serve"].environment.ZINDER_QUERY__TRUST_PROXY_CLIENT_ADDRESS == "false"
  and $services["zinder-serve"].environment.ZINDER_COMPAT__TRUST_PROXY_CLIENT_ADDRESS == "false"
  and $services["zinder-serve"].environment.ZINDER_EXPLORER__TRUST_PROXY_CLIENT_ADDRESS == "false"
  and (($services["zinder-history-publisher"].tmpfs // []) == ["/tmp:rw,noexec,nosuid,size=512m"])
  and ($services["zinder-history-publisher"].command as $cmd
    | ($cmd | index("--workers")) as $idx
    | $idx != null and $cmd[$idx + 1] == "1")
' "$resolved_compose_full" >/dev/null \
  || fail "the default Compose topology does not select the wallet preset, zebra-json-rpc source, default proxy untrust, and bounded history-publisher defaults"

explorer_preset_compose="$temporary_directory/explorer-preset-compose.json"
ZINDER_CONTROL_SECRETS_DIR="$control_secrets" \
ZINDER_PRESET=explorer \
ZINDER_INGEST__SOURCE=zebra-indexer-grpc \
ZINDER_HISTORY_PUBLISHER_WORKERS=4 \
docker compose \
  --env-file "$repository_root/deploy/.env.testnet" \
  -f "$repository_root/deploy/docker-compose.yml" \
  --profile history-publisher \
  config --format json > "$explorer_preset_compose"
bash "$validator" --compose-contract "$explorer_preset_compose"
jq -e '
  .services as $services
  | $services["zinder-index"].command == ["index", "--preset", "explorer"]
  and $services["zinder-serve"].command == ["serve", "--readers-only", "--preset", "explorer"]
  and $services["zinder-index"].environment.ZINDER_DEPLOYMENT__PROFILE == "explorer"
  and $services["zinder-serve"].environment.ZINDER_DEPLOYMENT__PROFILE == "explorer"
  and $services["zinder-index"].environment.ZINDER_INGEST__SOURCE == "zebra-indexer-grpc"
  and ($services["zinder-history-publisher"].command as $cmd
    | ($cmd | index("--workers")) as $idx
    | $idx != null and $cmd[$idx + 1] == "4")
' "$explorer_preset_compose" >/dev/null \
  || fail "the Compose topology does not forward ZINDER_PRESET=explorer, ZINDER_INGEST__SOURCE, or ZINDER_HISTORY_PUBLISHER_WORKERS overrides"

light_wallet_preset_compose="$temporary_directory/light-wallet-preset-compose.json"
ZINDER_CONTROL_SECRETS_DIR="$control_secrets" \
ZINDER_DEPLOYMENT__PROFILE=light-wallet \
docker compose \
  --env-file "$repository_root/deploy/.env.testnet" \
  -f "$repository_root/deploy/docker-compose.yml" \
  config --format json > "$light_wallet_preset_compose"
bash "$validator" --compose-contract "$light_wallet_preset_compose"
jq -e '
  .services as $services
  | $services["zinder-index"].command == ["index", "--preset", "light-wallet"]
  and $services["zinder-serve"].command == ["serve", "--readers-only", "--preset", "light-wallet"]
  and $services["zinder-index"].environment.ZINDER_DEPLOYMENT__PROFILE == "light-wallet"
  and $services["zinder-serve"].environment.ZINDER_DEPLOYMENT__PROFILE == "light-wallet"
' "$light_wallet_preset_compose" >/dev/null \
  || fail "the Compose topology does not propagate ZINDER_DEPLOYMENT__PROFILE=light-wallet"

trusted_proxy_compose="$temporary_directory/trusted-proxy-compose.json"
ZINDER_CONTROL_SECRETS_DIR="$control_secrets" \
ZINDER_QUERY__TRUST_PROXY_CLIENT_ADDRESS=true \
ZINDER_COMPAT__TRUST_PROXY_CLIENT_ADDRESS=true \
ZINDER_EXPLORER__TRUST_PROXY_CLIENT_ADDRESS=true \
docker compose \
  --env-file "$repository_root/deploy/.env.testnet" \
  -f "$repository_root/deploy/docker-compose.yml" \
  config --format json > "$trusted_proxy_compose"
jq -e '
  [
    .services["zinder-serve"].environment.ZINDER_QUERY__TRUST_PROXY_CLIENT_ADDRESS,
    .services["zinder-serve"].environment.ZINDER_COMPAT__TRUST_PROXY_CLIENT_ADDRESS,
    .services["zinder-serve"].environment.ZINDER_EXPLORER__TRUST_PROXY_CLIENT_ADDRESS
  ] == ["true", "true", "true"]
' "$trusted_proxy_compose" >/dev/null \
  || fail "the Compose topology does not forward trust_proxy_client_address overrides to zinder-serve"

released_compose="$temporary_directory/released-compose.json"
ZINDER_CONTROL_SECRETS_DIR="$control_secrets" \
ZINDER_VERSION=v9.8.7 \
docker compose \
  --env-file "$repository_root/deploy/.env.testnet" \
  -f "$repository_root/deploy/docker-compose.yml" \
  --profile history-publisher \
  config --format json > "$released_compose"
jq -e '
  ["zinder-index", "zinder-serve", "zinder-history-publisher"]
  | all(. as $service
    | $released[0].services[$service].image == "ghcr.io/zcashfoundation/zinder:v9.8.7"
    and $released[0].services[$service].build.target == "zinder"
    and $source[0].services[$service].image == "ghcr.io/zcashfoundation/zinder:local")
' --slurpfile released "$released_compose" --slurpfile source "$resolved_compose_full" \
  -n >/dev/null \
  || fail "ZINDER_VERSION does not select ghcr.io/zcashfoundation/zinder:v9.8.7 while keeping its source build"

history_dir="$temporary_directory/history"
history_compose="$temporary_directory/history-compose.json"
ZINDER_HISTORY_DIR="$history_dir" ZINDER_CONTROL_SECRETS_DIR="$control_secrets" \
  docker compose --env-file "$repository_root/deploy/.env.testnet" \
  -f "$repository_root/deploy/docker-compose.yml" --profile history-publisher \
  config --format json > "$history_compose"
bash "$validator" --compose-contract "$history_compose"
jq -e --arg generation "$history_dir" '
  any(.services["zinder-history-publisher"].volumes[]; .source == ($generation + "/publisher.pk8"))
' "$history_compose" >/dev/null || fail "the publisher ignored the selected generation"

invalid_history_compose="$temporary_directory/invalid-history-compose.json"
jq '.services["zinder-serve"].volumes += [
  (.services["zinder-history-publisher"].volumes[] | select(.target == "/run/zinder-history/publisher.pk8"))]
' "$history_compose" > "$invalid_history_compose"
expect_rejected "a reader service with access to the publisher signing key" --compose-contract "$invalid_history_compose"
jq '(.services["zinder-serve"].volumes[] | select(.target == "/var/lib/zinder-history")).read_only = false' \
  "$history_compose" > "$invalid_history_compose"
expect_rejected "a writer-capable catalog mount in zinder-serve" --compose-contract "$invalid_history_compose"
for history_source in catalog catalog/segment.bin publisher.pub; do
  for read_only in true false; do
    jq --arg source "$history_dir/$history_source" --argjson readonly "$read_only" '
      .services["zinder-serve"].volumes += [
        {type: "bind", source: $source, target: "/run/history-input", read_only: $readonly,
         bind: {create_host_path: false}}]
    ' "$history_compose" > "$invalid_history_compose"
    if [[ "$read_only" == true ]]; then
      bash "$validator" --compose-contract "$invalid_history_compose"
    else
      expect_rejected "writable signed-history input $history_source outside the publisher" \
        --compose-contract "$invalid_history_compose"
    fi
  done
done

check_named_history_input() {
  local subpath="$1" read_only="$2" expected="$3"
  jq --arg backing "$temporary_directory" --arg subpath "$subpath" --argjson readonly "$read_only" '
    .volumes.fixture = {name: "fixture", driver: "local", driver_opts: {device: $backing, o: "bind"}}
    | .volumes.fixture_view = {name: "fixture", external: true}
    | .services["zinder-serve"].volumes += [{
        type: "volume", source: "fixture_view", target: "/run/fixture",
        volume: {subpath: $subpath}
      } + (if $readonly == null then {} else {read_only: $readonly} end)]
  ' "$history_compose" > "$invalid_history_compose"
  if [[ "$expected" == accept ]]; then
    bash "$validator" --compose-contract "$invalid_history_compose"
  else
    expect_rejected_because "named volume with history input $subpath" \
      "invalid signed wallet history publication or reader mounts" --compose-contract "$invalid_history_compose"
  fi
}
check_named_history_input unrelated false accept
check_named_history_input history/catalog true accept
check_named_history_input history/publisher.pub true accept
check_named_history_input history/catalog null reject
check_named_history_input history/catalog/segment.bin false reject
check_named_history_input history/publisher.pub false reject
check_named_history_input history/publisher.pk8 true reject
check_named_history_input history true reject

reject_history_contract() {
  expect_rejected_because "$1" "invalid signed wallet history publication or reader mounts" \
    --compose-contract "$invalid_history_compose"
}

for options in '{"type":"fixture-fs"}' '{"o":"fixture-option"}' '{"device":"relative","o":"bind"}'; do
  jq --argjson options "$options" '.volumes.data.driver_opts = $options' \
    "$history_compose" > "$invalid_history_compose"
  reject_history_contract "an unresolved local volume backing"
done
jq '.volumes.data.driver = "fixture-driver"' "$history_compose" > "$invalid_history_compose"
reject_history_contract "a named volume using an unverified driver"
jq --arg device "$temporary_directory/unrelated" '
  .volumes.data.driver_opts = {type: "none", device: $device, o: "rbind,ro,nodev"}
' "$history_compose" > "$invalid_history_compose"
bash "$validator" --compose-contract "$invalid_history_compose"

for resource in configs secrets; do
  for file in publisher.pub publisher.pk8; do
    jq --arg resource "$resource" --arg file "$history_dir/$file" '
      .[$resource].fixture = {file: $file}
      | .services["zinder-serve"][$resource] = [{source: "fixture", target: "fixture-input"}]
    ' "$history_compose" > "$invalid_history_compose"
    if [[ "$file" == publisher.pub ]]; then
      bash "$validator" --compose-contract "$invalid_history_compose"
    else
      reject_history_contract "a $resource grant from the publisher signing key"
    fi
  done
  jq --arg resource "$resource" '
    .[$resource].fixture = {external: true}
    | .services["zinder-serve"][$resource] = ["fixture"]
  ' "$history_compose" > "$invalid_history_compose"
  reject_history_contract "a $resource grant with an external, unverified source"
done

jq '.services["zinder-serve"].volumes_from = ["state-init:ro"]' \
  "$history_compose" > "$invalid_history_compose"
reject_history_contract "a non-publisher service inheriting mounts"
jq --arg source "$history_dir/" '
  (.services[].volumes[] | select(.type == "bind" and (.source | startswith($source))) | .source)
  |= ("/./" + ltrimstr($source))
' "$history_compose" > "$invalid_history_compose"
reject_history_contract "a history generation whose normalized path is root"

jq 'del(.services["zinder-serve"].ulimits.nofile)' "$history_compose" > "$invalid_history_compose"
expect_rejected "zinder-serve inheriting open-file limits" --compose-contract "$invalid_history_compose"
jq '.services["zinder-serve"].ulimits.nofile = {"soft":1024,"hard":524288}' \
  "$history_compose" > "$invalid_history_compose"
expect_rejected "zinder-serve using the exhausted Mainnet soft limit" --compose-contract "$invalid_history_compose"
jq 'del(.services["zinder-index"].ulimits.nofile)' "$history_compose" > "$invalid_history_compose"
expect_rejected "zinder-index inheriting open-file limits" --compose-contract "$invalid_history_compose"
jq '.services["zinder-index"].ulimits.nofile = {"soft":1024,"hard":524288}' \
  "$history_compose" > "$invalid_history_compose"
expect_rejected "zinder-index using the exhausted Mainnet soft limit" --compose-contract "$invalid_history_compose"

for deployment_env in "${deployment_env_names[@]}"; do
  resolved_env_compose="$temporary_directory/resolved-$deployment_env-compose.json"
  ZINDER_CONTROL_SECRETS_DIR="$control_secrets" \
  docker compose \
    --env-file "$repository_root/deploy/.env.$deployment_env" \
    -f "$repository_root/deploy/docker-compose.yml" \
    --profile observability \
    --profile history-publisher \
    config --format json > "$resolved_env_compose"
  bash "$validator" --compose-contract "$resolved_env_compose"
  jq -e \
    --arg deploy_network "$(env_file_value "$deployment_env" ZINDER_DEPLOY_NETWORK)" \
    --arg source_network "$(env_file_value "$deployment_env" ZINDER_SOURCE_NETWORK_NAME)" \
    --arg cookie_volume "$(env_file_value "$deployment_env" ZINDER_SOURCE_COOKIE_VOLUME_NAME)" \
    --arg json_rpc_addr "$(env_file_value "$deployment_env" ZINDER_NODE__JSON_RPC_ADDR)" '
    .name == "zinder-\($deploy_network)"
    and .networks.node.external == true
    and .networks.node.name == $source_network
    and .volumes["node-cookie"].external == true
    and .volumes["node-cookie"].name == $cookie_volume
    and .volumes.data.name == "zinder-\($deploy_network)-data"
    and any(.services["zinder-history-publisher"].volumes[];
      .source == "/srv/zinder-history/\($deploy_network)/key-1/publisher.pk8")
    and .services["zinder-index"].environment.ZINDER_NODE__INDEXER_GRPC_ADDR == ""
    and .services["zinder-index"].environment.ZINDER_NODE__INDEXER_TLS__SERVER_NAME == ""
    and .services["zinder-index"].environment.ZINDER_NODE__INDEXER_TLS__CA_CERT_PEM == ""
    and .services["zinder-index"].environment.ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PEM == ""
    and .services["zinder-index"].environment.ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PEM == ""
    and $json_rpc_addr != ""
    and (.services["zinder-index"].environment.ZINDER_NODE__JSON_RPC_ADDR == $json_rpc_addr)
    and (.services["zinder-serve"].environment.ZINDER_NODE__JSON_RPC_ADDR == $json_rpc_addr)
    and (.services["zinder-index"].ulimits.nofile == {"soft":65536,"hard":65536})
    and (.services["zinder-serve"].ulimits.nofile == {"soft":65536,"hard":65536})
    and (.services["zinder-history-publisher"].ulimits.nofile == {"soft":65536,"hard":65536})
  ' "$resolved_env_compose" >/dev/null \
    || fail "the $deployment_env env file does not resolve to its expected network, cookie volume, endpoints, and ulimits"
done

missing_node_env="$temporary_directory/missing-node.env"
grep -v '^ZINDER_NODE__JSON_RPC_ADDR=' "$repository_root/deploy/.env.testnet" > "$missing_node_env"
if ZINDER_CONTROL_SECRETS_DIR="$control_secrets" docker compose \
  --env-file "$missing_node_env" \
  -f "$repository_root/deploy/docker-compose.yml" \
  config -q >/dev/null 2>&1; then
  fail "the deployment Compose topology resolved without a node JSON-RPC address"
fi
missing_deploy_network_env="$temporary_directory/missing-deploy-network.env"
grep -v '^ZINDER_DEPLOY_NETWORK=' "$repository_root/deploy/.env.testnet" > "$missing_deploy_network_env"
if ZINDER_CONTROL_SECRETS_DIR="$control_secrets" docker compose \
  --env-file "$missing_deploy_network_env" \
  -f "$repository_root/deploy/docker-compose.yml" \
  config -q >/dev/null 2>&1; then
  fail "docker-compose.yml resolved without ZINDER_DEPLOY_NETWORK"
fi

# systemd unit must keep active syncs intact on restart and avoid unintended builds.
systemd_unit="$repository_root/deploy/systemd/zinder.service"
grep -Fqx 'ExecStart=/usr/bin/docker compose --env-file /etc/zinder/env -f docker-compose.yml up --no-build' \
  "$systemd_unit" \
  || fail "deploy/systemd/zinder.service ExecStart must pass --no-build"
grep -Fqx 'ExecStop=/usr/bin/docker compose --env-file /etc/zinder/env -f docker-compose.yml down' \
  "$systemd_unit" \
  || fail "deploy/systemd/zinder.service must preserve ExecStop down"
if grep -Ev '^[[:space:]]*#' "$systemd_unit" | grep -Eq 'ExecStartPre|--remove-orphans'; then
  fail "deploy/systemd/zinder.service must not run destructive ExecStartPre down --remove-orphans"
fi

jq 'del(.services["zinder-prometheus"].networks.observability.aliases)' \
  "$resolved_compose" > "$invalid_history_compose"
expect_rejected \
  "a Compose contract omitting the prometheus network alias for Grafana" \
  --compose-contract "$invalid_history_compose"

missing_state_init_compose="$temporary_directory/missing-state-init-compose.json"
jq 'del(.services["state-init"]) | del(.services["zinder-index"].depends_on["state-init"])' \
  "$resolved_compose" > "$missing_state_init_compose"
expect_rejected \
  "a Compose contract without root-owned volume initialization" \
  --compose-contract "$missing_state_init_compose"

missing_checkpoint_root_compose="$temporary_directory/missing-checkpoint-root-compose.json"
jq '.services["state-init"].command = ["chown", "-R", "10001:10001", "/var/lib/zinder"]' \
  "$resolved_compose" > "$missing_checkpoint_root_compose"
expect_rejected \
  "a Compose contract that does not initialize the isolated checkpoint volume" \
  --compose-contract "$missing_checkpoint_root_compose"

legacy_uid_state_init_compose="$temporary_directory/legacy-uid-state-init-compose.json"
jq '.services["state-init"].command[2] |= sub("chown -R 10001:10001 "; "chown -R 1000:1000 ")' \
  "$resolved_compose" > "$legacy_uid_state_init_compose"
expect_rejected \
  "a Compose contract that initializes the shared volumes for UID 1000" \
  --compose-contract "$legacy_uid_state_init_compose"

shared_checkpoint_volume_compose="$temporary_directory/shared-checkpoint-volume-compose.json"
jq '
  (.services["state-init"].volumes[] | select(.target == "/var/lib/zinder") | .source) as $data_source
  | .services["zinder-index"].volumes |= map(
    if .target == "/var/lib/zinder/checkpoints" then
      .source = $data_source
    else . end
  )
' "$resolved_compose" > "$shared_checkpoint_volume_compose"
expect_rejected \
  "a Compose contract that stages checkpoints on the shared data volume" \
  --compose-contract "$shared_checkpoint_volume_compose"

aliased_checkpoint_volume_compose="$temporary_directory/aliased-checkpoint-volume-compose.json"
jq '.volumes["checkpoint-staging"].name = .volumes.data.name' \
  "$resolved_compose" > "$aliased_checkpoint_volume_compose"
expect_rejected \
  "a Compose contract whose checkpoint-staging volume aliases the data volume" \
  --compose-contract "$aliased_checkpoint_volume_compose"

serve_checkpoint_mount_compose="$temporary_directory/serve-checkpoint-mount-compose.json"
jq '
  (.services["zinder-index"].volumes[] | select(.target == "/var/lib/zinder/checkpoints")) as $checkpoint_mount
  | .services["zinder-serve"].volumes += [$checkpoint_mount]
' "$resolved_compose" > "$serve_checkpoint_mount_compose"
expect_rejected \
  "a Compose contract mounting checkpoint staging into zinder-serve" \
  --compose-contract "$serve_checkpoint_mount_compose"

serve_checkpoint_secret_compose="$temporary_directory/serve-checkpoint-secret-compose.json"
jq '
  (.services["zinder-index"].volumes[] | select(.target == "/var/run/zinder-checkpoint/checkpoint.token")) as $checkpoint_secret
  | .services["zinder-serve"].volumes += [
    ($checkpoint_secret | .target = "/var/run/alternate/checkpoint-capability")
  ]
' "$resolved_compose" > "$serve_checkpoint_secret_compose"
expect_rejected \
  "a zinder-serve contract mounting the checkpoint secret" \
  --compose-contract "$serve_checkpoint_secret_compose"

toml_mount_compose="$temporary_directory/toml-mount-compose.json"
jq '.services["zinder-serve"].volumes += [{type: "bind", source: "/tmp/config.toml", target: "/etc/zinder/config.toml", read_only: true}]' \
  "$resolved_compose" > "$toml_mount_compose"
expect_rejected \
  "a Compose contract mounting /etc/zinder/config.toml" \
  --compose-contract "$toml_mount_compose"

public_host_compose="$temporary_directory/public-host-compose.json"
jq '.services["zinder-index"].ports[0].host_ip = "0.0.0.0"' \
  "$resolved_compose" > "$public_host_compose"
expect_rejected \
  "a Compose contract with a public host publication" \
  --compose-contract "$public_host_compose"

duplicate_host_port_compose="$temporary_directory/duplicate-host-port-compose.json"
jq '.services["zinder-index"].ports[1].published = .services["zinder-index"].ports[0].published' \
  "$resolved_compose" > "$duplicate_host_port_compose"
expect_rejected \
  "a Compose contract publishing two ports on the same host port" \
  --compose-contract "$duplicate_host_port_compose"

published_control_compose="$temporary_directory/published-control-compose.json"
jq '.services["zinder-index"].ports += [{"host_ip":"127.0.0.1","target":9100,"published":"9100","protocol":"tcp"}]' \
  "$resolved_compose" > "$published_control_compose"
expect_rejected \
  "a Compose contract publishing the private control port" \
  --compose-contract "$published_control_compose"

split_namespace_compose="$temporary_directory/split-namespace-compose.json"
jq '.services["zinder-serve"].network_mode = null' \
  "$resolved_compose" > "$split_namespace_compose"
expect_rejected \
  "a Compose contract splitting zinder-serve from zinder-index network namespace" \
  --compose-contract "$split_namespace_compose"

implicit_security_compose="$temporary_directory/implicit-security-compose.json"
jq 'del(.services["zinder-serve"].environment.ZINDER_SECURITY__ALLOW_PUBLIC_BIND)' \
  "$resolved_compose" > "$implicit_security_compose"
expect_rejected \
  "a Compose contract relying on an image-baked public-bind opt-in" \
  --compose-contract "$implicit_security_compose"

# Verify inline indexer TLS passthrough reaches only zinder-index when set in env.
resolved_inline_tls_compose="$temporary_directory/resolved-inline-tls-compose.json"
ZINDER_CONTROL_SECRETS_DIR="$control_secrets" \
ZINDER_NODE__INDEXER_GRPC_ADDR=https://zakura:8230 \
ZINDER_NODE__INDEXER_TLS__SERVER_NAME=zakura.internal \
ZINDER_NODE__INDEXER_TLS__CA_CERT_PEM="CA-PEM" \
ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PEM="CLIENT-CERT-PEM" \
ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PEM="CLIENT-KEY-PEM" \
docker compose \
  --env-file "$repository_root/deploy/.env.zakura-mainnet" \
  -f "$repository_root/deploy/docker-compose.yml" \
  config --format json > "$resolved_inline_tls_compose"
bash "$validator" --compose-contract "$resolved_inline_tls_compose"
jq -e '
  .services["zinder-index"].environment as $env
  | $env.ZINDER_NODE__INDEXER_GRPC_ADDR == "https://zakura:8230"
  and $env.ZINDER_NODE__INDEXER_TLS__SERVER_NAME == "zakura.internal"
  and $env.ZINDER_NODE__INDEXER_TLS__CA_CERT_PEM == "CA-PEM"
  and $env.ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PEM == "CLIENT-CERT-PEM"
  and $env.ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PEM == "CLIENT-KEY-PEM"
' "$resolved_inline_tls_compose" >/dev/null \
  || fail "Compose did not forward inline ZINDER_NODE__INDEXER_TLS__* credentials to zinder-index"

missing_tls_env_compose="$temporary_directory/missing-tls-env-compose.json"
jq 'del(.services["zinder-index"].environment.ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PEM)' \
  "$resolved_compose" > "$missing_tls_env_compose"
expect_rejected \
  "a Compose contract dropping inline indexer TLS passthrough" \
  --compose-contract "$missing_tls_env_compose"

leaked_tls_env_compose="$temporary_directory/leaked-tls-env-compose.json"
jq '.services["zinder-serve"].environment.ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PEM = ""' \
  "$resolved_compose" > "$leaked_tls_env_compose"
expect_rejected \
  "a Compose contract forwarding indexer TLS keys to zinder-serve" \
  --compose-contract "$leaked_tls_env_compose"

missing_cipherscan_port_compose="$temporary_directory/missing-cipherscan-port-compose.json"
jq '.services["zinder-index"].ports |= map(select(.target != 9070))' \
  "$resolved_compose" > "$missing_cipherscan_port_compose"
expect_rejected \
  "a Compose contract omitting loopback Cipherscan port 9070" \
  --compose-contract "$missing_cipherscan_port_compose"

missing_cipherscan_listen_compose="$temporary_directory/missing-cipherscan-listen-compose.json"
jq 'del(.services["zinder-serve"].environment.ZINDER_CIPHERSCAN__LISTEN_ADDR)' \
  "$resolved_compose" > "$missing_cipherscan_listen_compose"
expect_rejected \
  "a Compose contract omitting ZINDER_CIPHERSCAN__LISTEN_ADDR on zinder-serve" \
  --compose-contract "$missing_cipherscan_listen_compose"

mixed_release_workflow="$temporary_directory/release.yml"
cp "$repository_root/.github/workflows/release.yml" "$mixed_release_workflow"
printf '\n# forbidden release target: zinder-single-container\n' >> "$mixed_release_workflow"
expect_rejected \
  "a release workflow containing the mixed single-container target" \
  --release-workflow "$mixed_release_workflow"

empty_release_images_catalog="$temporary_directory/empty-catalog.json"
printf '[]\n' > "$empty_release_images_catalog"
expect_rejected \
  "an empty release image catalog" \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --release-images-catalog "$empty_release_images_catalog"

bench_release_images_catalog="$temporary_directory/catalog-with-bench.json"
jq '. + ["zinder-bench"]' \
  "$release_images_catalog" > "$bench_release_images_catalog"
expect_rejected \
  "a release image catalog containing the benchmark" \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --release-images-catalog "$bench_release_images_catalog"

swapped_release_images_catalog="$temporary_directory/catalog-with-bench-only.json"
printf '["zinder-bench"]\n' > "$swapped_release_images_catalog"
expect_rejected \
  "a release image catalog naming zinder-bench instead of zinder" \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --release-images-catalog "$swapped_release_images_catalog"

malformed_release_images_catalog="$temporary_directory/malformed-release-images.json"
printf '{\n' > "$malformed_release_images_catalog"
expect_rejected \
  "a malformed release image catalog" \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --release-images-catalog "$malformed_release_images_catalog"

static_merge_workflow="$temporary_directory/static-merge-release.yml"
sed '/image: \${{ fromJSON(needs.validate.outputs.release_images) }}/d' \
  "$repository_root/.github/workflows/release.yml" \
  > "$static_merge_workflow"
expect_rejected \
  "a release workflow whose manifest matrix bypasses the validated catalog" \
  --release-workflow "$static_merge_workflow"

manual_release_workflow="$temporary_directory/manual-release.yml"
sed '/^on:/a\  workflow_dispatch:' \
  "$repository_root/.github/workflows/release.yml" \
  > "$manual_release_workflow"
expect_rejected \
  "a publishing workflow with a manual-dispatch path" \
  --release-workflow "$manual_release_workflow"

single_binary_build_workflow="$temporary_directory/single-binary-build-release.yml"
sed '/^[[:space:]]*- reproduce$/d' \
  "$repository_root/.github/workflows/release.yml" \
  > "$single_binary_build_workflow"
expect_rejected \
  "a release workflow without an independent binary reproduction build" \
  --release-workflow "$single_binary_build_workflow"

late_binary_assets_workflow="$temporary_directory/late-binary-assets-release.yml"
sed '/^[[:space:]]*- collect-binary-assets$/d' \
  "$repository_root/.github/workflows/release.yml" \
  > "$late_binary_assets_workflow"
expect_rejected \
  "a release workflow assembling assets before binary archives succeed" \
  --release-workflow "$late_binary_assets_workflow"

early_latest_workflow="$temporary_directory/early-latest-release.yml"
# The literal workflow expression is the unsafe fixture insertion target.
# shellcheck disable=SC2016
sed '/-t "${image}:${RELEASE_TAG}"/a\            -t "${image}:latest"' \
  "$repository_root/.github/workflows/release.yml" \
  > "$early_latest_workflow"
expect_rejected \
  "a release workflow that promotes latest before final publication" \
  --release-workflow "$early_latest_workflow"

unprotected_release_workflow="$temporary_directory/unprotected-release.yml"
awk '
  /^  publish-sdk-crates:/ { in_publication = 1 }
  in_publication && /^    environment: release$/ { next }
  in_publication && /^  images:/ { in_publication = 0 }
  { print }
' "$repository_root/.github/workflows/release.yml" \
  > "$unprotected_release_workflow"
expect_rejected_because \
  "a release workflow that publishes without environment approval" \
  'release admission rejected: SDK publication must follow validated package' \
  --release-workflow "$unprotected_release_workflow"

bypassed_authorization_workflow="$temporary_directory/bypassed-authorization-release.yml"
sed '/^[[:space:]]*- publish-sdk-crates$/d' \
  "$repository_root/.github/workflows/release.yml" \
  > "$bypassed_authorization_workflow"
expect_rejected \
  "a release workflow whose first registry writer bypasses approval" \
  --release-workflow "$bypassed_authorization_workflow"

twice_reviewed_workflow="$temporary_directory/twice-reviewed-release.yml"
sed '/^  compile:/a\    environment: release' \
  "$repository_root/.github/workflows/release.yml" \
  > "$twice_reviewed_workflow"
expect_rejected \
  "a release workflow that asks for a second environment review" \
  --release-workflow "$twice_reviewed_workflow"

mutate_image_packager() {
  local mutated_packager="$1"
  shift
  "$@" "$image_packager" > "$mutated_packager"
  chmod 0755 "$mutated_packager"
}

unverified_image_binaries_packager="$temporary_directory/unverified-image-binaries-packager.sh"
# shellcheck disable=SC2016
mutate_image_packager "$unverified_image_binaries_packager" \
  sed '/cmp -- "\${binary_root}\/\${binary_name}" "\$image_binary"/d'
expect_rejected_because \
  "an image packager that does not compare image binaries with the archive" \
  'release admission rejected: the release image packager must' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --image-packager "$unverified_image_binaries_packager"

modeless_archive_packager="$temporary_directory/modeless-archive-packager.sh"
# shellcheck disable=SC2016
mutate_image_packager "$modeless_archive_packager" \
  sed '/require_archive_binary_mode "\$binary_root\/\$binary_name" "archive binary \$binary_name"/d'
expect_rejected_because \
  "an image packager that does not check the extracted archive binary modes" \
  'release admission rejected: the release image packager must' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --image-packager "$modeless_archive_packager"

modeless_image_packager="$temporary_directory/modeless-image-packager.sh"
# shellcheck disable=SC2016
mutate_image_packager "$modeless_image_packager" \
  sed '/require_archive_binary_mode "\$image_binary"/d'
expect_rejected_because \
  "an image packager that does not check the image binary modes" \
  'release admission rejected: the release image packager must' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --image-packager "$modeless_image_packager"

unchecked_installed_mode_packager="$temporary_directory/unchecked-installed-mode-packager.sh"
# shellcheck disable=SC2016
mutate_image_packager "$unchecked_installed_mode_packager" \
  sed '/\[\[ "\$image_binary_listing" == /d'
expect_rejected_because \
  "an image packager that ignores the mode installed inside the image" \
  'release admission rejected: the release image packager must' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --image-packager "$unchecked_installed_mode_packager"

unchecked_healthcheck_packager="$temporary_directory/unchecked-healthcheck-packager.sh"
# shellcheck disable=SC2016
mutate_image_packager "$unchecked_healthcheck_packager" \
  sed '/\[\[ "\$healthcheck_status" -eq 1 \]\]/d'
expect_rejected_because \
  "an image packager that does not run the healthcheck inside the image" \
  'release admission rejected: the release image packager must' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --image-packager "$unchecked_healthcheck_packager"

wall_clock_created_packager="$temporary_directory/wall-clock-created-packager.sh"
# shellcheck disable=SC2016
mutate_image_packager "$wall_clock_created_packager" \
  sed 's/date -u -d "@\${source_date_epoch}"/date -u/'
expect_rejected_because \
  "an image packager whose created label is the build time" \
  'release admission rejected: the release image packager must' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --image-packager "$wall_clock_created_packager"

undescribed_image_packager="$temporary_directory/undescribed-image-packager.sh"
mutate_image_packager "$undescribed_image_packager" \
  sed '/org.opencontainers.image.description=/d'
expect_rejected_because \
  "an image packager that omits the image description label" \
  'release admission rejected: the release image packager must' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --image-packager "$undescribed_image_packager"

pushing_check_packager="$temporary_directory/pushing-check-packager.sh"
mutate_image_packager "$pushing_check_packager" \
  sed 's/,push=false" ;;$/,push=true" ;;/'
expect_rejected_because \
  "an image packager whose check mode pushes" \
  'release admission rejected: the release image packager must' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --image-packager "$pushing_check_packager"

smokeless_reproduction_workflow="$temporary_directory/smokeless-reproduction-release.yml"
awk '
  /^  reproduce:/ { in_reproduction = 1 }
  /^  compare-binaries:/ { in_reproduction = 0 }
  in_reproduction && $0 == "      - name: Package every release image without pushing" { skipping = 1 }
  skipping && $0 == "      - name: Upload reproduction archive" { skipping = 0 }
  !skipping { print }
' "$repository_root/.github/workflows/release.yml" \
  > "$smokeless_reproduction_workflow"
expect_rejected_because \
  "a release workflow whose reproduction does not package the release images" \
  'release admission rejected: the reproduce job must package every release image' \
  --release-workflow "$smokeless_reproduction_workflow"

ignored_smoke_workflow="$temporary_directory/ignored-smoke-release.yml"
sed '/^      - name: Package every release image without pushing$/a\        continue-on-error: true' \
  "$repository_root/.github/workflows/release.yml" \
  > "$ignored_smoke_workflow"
expect_rejected_because \
  "a release workflow that ignores a failed image packaging smoke" \
  'release admission rejected: the reproduce job must package every release image' \
  --release-workflow "$ignored_smoke_workflow"

logged_in_reproduction_workflow="$temporary_directory/logged-in-reproduction-release.yml"
# shellcheck disable=SC2016
awk '
  /^  reproduce:/ { in_reproduction = 1 }
  /^  compare-binaries:/ { in_reproduction = 0 }
  in_reproduction && $0 == "      - name: Set up Docker Buildx" {
    print "      - name: Log in to GHCR"
    print "        uses: docker/login-action@8aa5831fb8d48d8f76594c7ddde61ae58019f550 # v4.8.0"
    print "        with:"
    print "          registry: ghcr.io"
    print "          username: ${{ github.actor }}"
    print "          password: ${{ secrets.GITHUB_TOKEN }}"
    print ""
  }
  { print }
' "$repository_root/.github/workflows/release.yml" \
  > "$logged_in_reproduction_workflow"
expect_rejected_because \
  "a release workflow whose image packaging smoke logs in to a registry" \
  'release admission rejected: the reproduce job must package every release image' \
  --release-workflow "$logged_in_reproduction_workflow"

writing_reproduction_workflow="$temporary_directory/writing-reproduction-release.yml"
sed '/^  reproduce:/,/^  compare-binaries:/ s/^      contents: read$/      contents: read\n      packages: write/' \
  "$repository_root/.github/workflows/release.yml" \
  > "$writing_reproduction_workflow"
expect_rejected_because \
  "a release workflow whose image packaging smoke may write packages" \
  'release admission rejected: job reproduce receives a write permission' \
  --release-workflow "$writing_reproduction_workflow"

rescoped_compile_workflow="$temporary_directory/rescoped-compile-release.yml"
sed '/^  compile:/,/^  reproduce:/ s/RELEASE_CACHE_SCOPE: release-\(.*\)-1$/RELEASE_CACHE_SCOPE: release-\1-3/' \
  "$repository_root/.github/workflows/release.yml" \
  > "$rescoped_compile_workflow"
expect_rejected \
  "a release workflow whose primary build leaves its pinned cache scope" \
  --release-workflow "$rescoped_compile_workflow"

approval_gated_compile_workflow="$temporary_directory/approval-gated-compile-release.yml"
sed '/^  compile:/,/^  reproduce:/ s/^    needs: validate$/    needs: [validate, publish-sdk-crates]/' \
  "$repository_root/.github/workflows/release.yml" \
  > "$approval_gated_compile_workflow"
expect_rejected_because \
  "a release workflow whose compile waits for crate publication" \
  'release admission rejected: compile must need only validate' \
  --release-workflow "$approval_gated_compile_workflow"

approval_gated_reproduction_workflow="$temporary_directory/approval-gated-reproduction-release.yml"
sed '/^  reproduce:/,/^  compare-binaries:/ s/^    needs: validate$/    needs:\n      - validate\n      - publish-sdk-crates/' \
  "$repository_root/.github/workflows/release.yml" \
  > "$approval_gated_reproduction_workflow"
expect_rejected_because \
  "a release workflow whose reproduction waits for crate publication" \
  'release admission rejected: reproduce must need only validate' \
  --release-workflow "$approval_gated_reproduction_workflow"

unproven_approval_workflow="$temporary_directory/unproven-approval-release.yml"
sed '/^  publish-sdk-crates:/,/^  images:/ { /^      - compare-binaries$/d; }' \
  "$repository_root/.github/workflows/release.yml" \
  > "$unproven_approval_workflow"
expect_rejected_because \
  "a release workflow that requests approval before the builds are compared" \
  'release admission rejected: publish-sdk-crates must need compare-binaries' \
  --release-workflow "$unproven_approval_workflow"

unproven_attestation_workflow="$temporary_directory/unproven-attestation-release.yml"
sed '/^  binary-archives:/,/^  collect-binary-assets:/ { /^      - compare-binaries$/d; }' \
  "$repository_root/.github/workflows/release.yml" \
  > "$unproven_attestation_workflow"
expect_rejected_because \
  "a release workflow that attests an archive without waiting for the comparison" \
  'release admission rejected: binary-archives must need compare-binaries' \
  --release-workflow "$unproven_attestation_workflow"

recompiling_image_packager="$temporary_directory/recompiling-image-packager.sh"
mutate_image_packager "$recompiling_image_packager" \
  sed '/--build-arg "ZINDER_BINARIES_SOURCE=zinder-archived-binaries"/d'
expect_rejected_because \
  "an image packager that compiles instead of packaging the archive" \
  'release admission rejected: the release image packager must' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --image-packager "$recompiling_image_packager"

cached_image_packager="$temporary_directory/cached-image-packager.sh"
# shellcheck disable=SC2016
mutate_image_packager "$cached_image_packager" \
  awk '
    { print }
    $0 == "    --sbom=true \\" {
      print "    --cache-from \"type=gha,scope=release\" \\"
    }
  '
expect_rejected_because \
  "an image packager that reads a CI cache" \
  'release admission rejected: the release image packager must' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --image-packager "$cached_image_packager"

inline_image_build_workflow="$temporary_directory/inline-image-build-release.yml"
awk '
  /^  images:/ { in_images = 1 }
  /^  binary-archives:/ { in_images = 0 }
  in_images && $0 == "          scripts/package-release-images.sh push \\" {
    print "          docker buildx build --file deploy/Dockerfile --target zinder ."
  }
  { print }
' "$repository_root/.github/workflows/release.yml" \
  > "$inline_image_build_workflow"
expect_rejected \
  "a release workflow whose images job builds outside the image packager" \
  --release-workflow "$inline_image_build_workflow"

early_registry_login_workflow="$temporary_directory/early-registry-login-release.yml"
awk '
  /^  compile:/ { in_compile = 1 }
  /^  reproduce:/ { in_compile = 0 }
  in_compile && $0 == "      - name: Set up Docker Buildx" {
    print "      - name: Log in to GHCR"
    print "        uses: docker/login-action@8aa5831fb8d48d8f76594c7ddde61ae58019f550 # v4.8.0"
    print ""
  }
  { print }
' "$repository_root/.github/workflows/release.yml" \
  > "$early_registry_login_workflow"
expect_rejected \
  "a release workflow that logs in to a registry before approval" \
  --release-workflow "$early_registry_login_workflow"

unapproved_writer_workflow="$temporary_directory/unapproved-writer-release.yml"
cp "$repository_root/.github/workflows/release.yml" "$unapproved_writer_workflow"
cat >> "$unapproved_writer_workflow" <<'YAML'

  push-unapproved-image:
    name: push an image without approval
    needs: compare-binaries
    permissions:
      contents: read
      packages: write
    runs-on: ubuntu-latest
    timeout-minutes: 5
    steps:
      - run: "true"
YAML
expect_rejected_because \
  "a registry-writing job that does not depend on the approved publication" \
  'release admission rejected: job push-unapproved-image receives a write permission' \
  --release-workflow "$unapproved_writer_workflow"

untimed_compile_workflow="$temporary_directory/untimed-compile-release.yml"
sed '/^  compile:/,/^  reproduce:/ { /^    timeout-minutes: 60$/d; }' \
  "$repository_root/.github/workflows/release.yml" \
  > "$untimed_compile_workflow"
expect_rejected_because \
  "a release workflow whose compile has no timeout" \
  'release admission rejected: release job compile must declare' \
  --release-workflow "$untimed_compile_workflow"

long_compile_workflow="$temporary_directory/long-compile-release.yml"
sed '/^  compile:/,/^  reproduce:/ s/^    timeout-minutes: 60$/    timeout-minutes: 360/' \
  "$repository_root/.github/workflows/release.yml" \
  > "$long_compile_workflow"
expect_rejected_because \
  "a release workflow whose compile may run for six hours" \
  'release admission rejected: release job compile must declare' \
  --release-workflow "$long_compile_workflow"

untimed_collection_workflow="$temporary_directory/untimed-collection-release.yml"
sed '/^  collect-binary-assets:/,/^  merge:/ { /^    timeout-minutes:/d; }' \
  "$repository_root/.github/workflows/release.yml" \
  > "$untimed_collection_workflow"
expect_rejected_because \
  "a release workflow with a job that has no timeout" \
  'release admission rejected: release job collect-binary-assets must declare' \
  --release-workflow "$untimed_collection_workflow"

bench_compile_dockerfile="$temporary_directory/bench-compile.Dockerfile"
awk '
  { print }
  $0 == "        --bin zinder \\" { print "        --bin zinder-bench \\" }
' "$repository_root/deploy/Dockerfile" > "$bench_compile_dockerfile"
expect_rejected_because \
  "a Dockerfile whose shared release compile also builds the benchmark" \
  'release admission rejected: the zinder-binaries stage must compile only' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --dockerfile "$bench_compile_dockerfile"

unrefreshed_bench_dockerfile="$temporary_directory/unrefreshed-bench.Dockerfile"
sed '/^FROM zinder-release-source AS zinder-bench-binary$/,/^FROM / {
  /^    find \. -path \.\/target -prune -o -type f -exec touch {} + \\$/d
  s/^    && rust_target=/    rust_target=/
}' "$repository_root/deploy/Dockerfile" > "$unrefreshed_bench_dockerfile"
cmp -s "$repository_root/deploy/Dockerfile" "$unrefreshed_bench_dockerfile" \
  && fail "the unrefreshed benchmark fixture did not change the Dockerfile"
expect_rejected_because \
  "a Dockerfile whose benchmark build can reuse another tree's workspace crates" \
  'release admission rejected: every Docker build step that mounts the Cargo' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --dockerfile "$unrefreshed_bench_dockerfile"

late_refresh_dockerfile="$temporary_directory/late-refresh.Dockerfile"
awk '
  /^FROM zinder-release-source AS zinder-binaries$/ { in_stage = 1 }
  in_stage && /^    find \. -path \.\/target -prune -o -type f -exec touch \{\} \+ \\$/ { next }
  in_stage && /^    && rust_target=/ { sub(/^    && /, "    ") }
  { print }
  in_stage && $0 == "        --bin zinder \\" {
    print "    && find . -path ./target -prune -o -type f -exec touch {} + \\"
    in_stage = 0
  }
' "$repository_root/deploy/Dockerfile" > "$late_refresh_dockerfile"
cmp -s "$repository_root/deploy/Dockerfile" "$late_refresh_dockerfile" \
  && fail "the late-refresh fixture did not change the Dockerfile"
expect_rejected_because \
  "a Dockerfile whose release compile refreshes sources only after cargo runs" \
  'release admission rejected: every Docker build step that mounts the Cargo' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --dockerfile "$late_refresh_dockerfile"

newest_test_binary_dockerfile="$temporary_directory/newest-test-binary.Dockerfile"
sed 's/ --message-format=json-render-diagnostics / /' \
  "$repository_root/deploy/Dockerfile" > "$newest_test_binary_dockerfile"
cmp -s "$repository_root/deploy/Dockerfile" "$newest_test_binary_dockerfile" \
  && fail "the newest-test-binary fixture did not change the Dockerfile"
expect_rejected_because \
  "a Dockerfile whose live-test stage does not read cargo's artifact messages" \
  'release admission rejected: every Docker build step that mounts the Cargo' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --dockerfile "$newest_test_binary_dockerfile"

compiling_image_dockerfile="$temporary_directory/compiling-image.Dockerfile"
sed 's#^COPY --from=zinder-binaries-selected /zinder-binaries/zinder #COPY --from=zinder-binaries /zinder-binaries/zinder #' \
  "$repository_root/deploy/Dockerfile" > "$compiling_image_dockerfile"
expect_rejected_because \
  "a Dockerfile whose release image copies from the compile stage" \
  'release admission rejected: the zinder-binaries stage must compile only' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --dockerfile "$compiling_image_dockerfile"

compiling_archive_dockerfile="$temporary_directory/compiling-archive.Dockerfile"
sed 's/^FROM scratch AS zinder-archived-binaries$/FROM zinder-binaries AS zinder-archived-binaries/' \
  "$repository_root/deploy/Dockerfile" > "$compiling_archive_dockerfile"
expect_rejected_because \
  "a Dockerfile whose archived-binary source compiles when its context is missing" \
  'release admission rejected: the zinder-binaries stage must compile only' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --dockerfile "$compiling_archive_dockerfile"

debian_service_dockerfile="$temporary_directory/debian-service.Dockerfile"
sed 's/^FROM zinder-service-base AS zinder$/FROM zinder-runtime-base AS zinder/' \
  "$repository_root/deploy/Dockerfile" > "$debian_service_dockerfile"
expect_rejected_because \
  "a Dockerfile whose release image builds on the Debian runtime base" \
  'release admission rejected: the unified zinder release stage must build on' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --dockerfile "$debian_service_dockerfile"

unpinned_distroless_dockerfile="$temporary_directory/unpinned-distroless.Dockerfile"
# shellcheck disable=SC2016
sed 's#^FROM gcr.io/distroless/cc-debian12:nonroot@\${DISTROLESS_IMAGE_DIGEST} AS zinder-service-base$#FROM gcr.io/distroless/cc-debian12:nonroot AS zinder-service-base#' \
  "$repository_root/deploy/Dockerfile" > "$unpinned_distroless_dockerfile"
expect_rejected_because \
  "a Dockerfile whose distroless service base is not digest-pinned" \
  'release admission rejected: the unified zinder release stage must build on' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --dockerfile "$unpinned_distroless_dockerfile"

legacy_uid_dockerfile="$temporary_directory/legacy-uid.Dockerfile"
sed 's/^USER 10001:10001$/USER 1000:1000/' \
  "$repository_root/deploy/Dockerfile" > "$legacy_uid_dockerfile"
expect_rejected_because \
  "a Dockerfile whose release image runs as UID 1000" \
  'release admission rejected: the unified zinder release stage must build on' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --dockerfile "$legacy_uid_dockerfile"

legacy_account_dockerfile="$temporary_directory/legacy-account.Dockerfile"
sed 's/useradd --system --uid 10001 --gid zinder/useradd --system --uid 1000 --gid zinder/' \
  "$repository_root/deploy/Dockerfile" > "$legacy_account_dockerfile"
expect_rejected_because \
  "a Dockerfile whose runtime-base zinder account is not UID 10001" \
  'release admission rejected: the unified zinder release stage must build on' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --dockerfile "$legacy_account_dockerfile"

curl_healthcheck_dockerfile="$temporary_directory/curl-healthcheck.Dockerfile"
sed 's#^    CMD \["/usr/local/bin/zinder", "healthcheck"\]$#    CMD curl -fsS http://localhost:9108/readyz || exit 1#' \
  "$repository_root/deploy/Dockerfile" > "$curl_healthcheck_dockerfile"
expect_rejected_because \
  "a Dockerfile whose release image probes itself with curl" \
  'release admission rejected: the unified zinder release stage must build on' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --dockerfile "$curl_healthcheck_dockerfile"

public_bind_env_dockerfile="$temporary_directory/public-bind-env.Dockerfile"
sed 's#^USER 10001:10001$#ENV ZINDER_SECURITY__ALLOW_PUBLIC_BIND=true\nUSER 10001:10001#' \
  "$repository_root/deploy/Dockerfile" > "$public_bind_env_dockerfile"
expect_rejected_because \
  "a Dockerfile whose service base allows public binds through the environment" \
  'release admission rejected: the unified zinder release stage must build on' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --dockerfile "$public_bind_env_dockerfile"

parallel_publication_workflow="$temporary_directory/parallel-publication-release.yml"
sed 's/group: release-publication/group: release-${{ github.ref }}/' \
  "$repository_root/.github/workflows/release.yml" \
  > "$parallel_publication_workflow"
expect_rejected \
  "a release workflow that allows concurrent crate publications" \
  --release-workflow "$parallel_publication_workflow"

untrusted_crate_publication_workflow="$temporary_directory/untrusted-crate-publication-release.yml"
sed '/^[[:space:]]*id-token: write$/d' \
  "$repository_root/.github/workflows/release.yml" \
  > "$untrusted_crate_publication_workflow"
expect_rejected \
  "a release workflow without trusted-publisher identity permission" \
  --release-workflow "$untrusted_crate_publication_workflow"

unpinned_crate_auth_workflow="$temporary_directory/unpinned-crate-auth-release.yml"
sed 's#rust-lang/crates-io-auth-action@3268eb413133e2960fa4ae0e963ca76380c54cd9 #rust-lang/crates-io-auth-action@main #' \
  "$repository_root/.github/workflows/release.yml" \
  > "$unpinned_crate_auth_workflow"
expect_rejected \
  "a release workflow with an unpinned crates.io authentication action" \
  --release-workflow "$unpinned_crate_auth_workflow"

unpinned_cargo_release_workflow="$temporary_directory/unpinned-cargo-release.yml"
sed 's#ZcashFoundation/cargo-release@f212e2569995d569baeb6cdeb7ed99b7602303a4#ZcashFoundation/cargo-release@main#g' \
  "$repository_root/.github/workflows/release.yml" \
  > "$unpinned_cargo_release_workflow"
expect_rejected \
  "a release workflow with an unpinned Cargo Release action" \
  --release-workflow "$unpinned_cargo_release_workflow"

credentialed_checkout_workflow="$temporary_directory/credentialed-checkout-release.yml"
sed '/persist-credentials: false/d' \
  "$repository_root/.github/workflows/release.yml" \
  > "$credentialed_checkout_workflow"
expect_rejected \
  "a release workflow exposing checkout credentials to Cargo build scripts" \
  --release-workflow "$credentialed_checkout_workflow"

unchecked_cargo_release_workflow="$temporary_directory/unchecked-cargo-release.yml"
awk '
  !changed && /phase: check/ {
    sub(/phase: check/, "phase: publish")
    changed = 1
  }
  { print }
' "$repository_root/.github/workflows/release.yml" \
  > "$unchecked_cargo_release_workflow"
expect_rejected \
  "a release workflow without a pre-auth Cargo Release check" \
  --release-workflow "$unchecked_cargo_release_workflow"

side_branch_base_workflow="$temporary_directory/side-branch-base-release.yml"
sed '/--first-parent/d' \
  "$repository_root/.github/workflows/release.yml" \
  > "$side_branch_base_workflow"
expect_rejected \
  "a release workflow that can derive its Cargo Release base from a side branch" \
  --release-workflow "$side_branch_base_workflow"

protoc_dependent_release_workflow="$temporary_directory/protoc-dependent-release.yml"
sed '/PROTOC: \/does\/not\/exist/d' \
  "$repository_root/.github/workflows/release.yml" \
  > "$protoc_dependent_release_workflow"
expect_rejected \
  "a release workflow that permits package consumers to require protoc" \
  --release-workflow "$protoc_dependent_release_workflow"

static_crate_token_workflow="$temporary_directory/static-crate-token-release.yml"
# shellcheck disable=SC2016
sed 's/${{ steps.crates-io-auth.outputs.token }}/${{ secrets.CARGO_REGISTRY_TOKEN }}/' \
  "$repository_root/.github/workflows/release.yml" \
  > "$static_crate_token_workflow"
expect_rejected \
  "a release workflow with a static crates.io token fallback" \
  --release-workflow "$static_crate_token_workflow"

leaked_oidc_token_workflow="$temporary_directory/leaked-oidc-token-release.yml"
# shellcheck disable=SC2016
sed '/name: verify public SDK packages/a\    env:\n      CARGO_REGISTRY_TOKEN: ${{ steps.crates-io-auth.outputs.token }}' \
  "$repository_root/.github/workflows/release.yml" \
  > "$leaked_oidc_token_workflow"
expect_rejected \
  "a release workflow exposing the OIDC token before publication" \
  --release-workflow "$leaked_oidc_token_workflow"

unverified_registry_consumer_workflow="$temporary_directory/unverified-registry-consumer-release.yml"
sed '/scripts\/verify-published-sdk.sh "$SDK_VERSION"/d' \
  "$repository_root/.github/workflows/release.yml" \
  > "$unverified_registry_consumer_workflow"
expect_rejected \
  "a release workflow without a registry-only SDK consumer check" \
  --release-workflow "$unverified_registry_consumer_workflow"

stable_only_crate_workflow="$temporary_directory/stable-only-crate-release.yml"
sed '/^  publish-sdk-crates:/a\    if: needs.validate.outputs.stable == '\''true'\''' \
  "$repository_root/.github/workflows/release.yml" \
  > "$stable_only_crate_workflow"
expect_rejected \
  "a release workflow that omits prerelease crates" \
  --release-workflow "$stable_only_crate_workflow"

bypassed_manifest_dependency_workflow="$temporary_directory/bypassed-manifest-release.yml"
sed '/^[[:space:]]*- images$/d' \
  "$repository_root/.github/workflows/release.yml" \
  > "$bypassed_manifest_dependency_workflow"
expect_rejected \
  "a release workflow whose manifest publisher bypasses digest builds" \
  --release-workflow "$bypassed_manifest_dependency_workflow"

early_oidc_workflow="$temporary_directory/early-oidc-release.yml"
sed '/^  sdk-packages:/a\    permissions:\n      id-token: write' \
  "$repository_root/.github/workflows/release.yml" \
  > "$early_oidc_workflow"
expect_rejected \
  "a release workflow granting OIDC before authorization" \
  --release-workflow "$early_oidc_workflow"

unpinned_attestation_workflow="$temporary_directory/unpinned-attestation-release.yml"
sed 's#actions/attest@1e69f48acb82d1966a394da916b4c1698aa569d6 #actions/attest@main #' \
  "$repository_root/.github/workflows/release.yml" \
  > "$unpinned_attestation_workflow"
expect_rejected \
  "a release workflow with an unpinned attestation action" \
  --release-workflow "$unpinned_attestation_workflow"

storage_record_workflow="$temporary_directory/storage-record-release.yml"
sed 's/create-storage-record: false/create-storage-record: true/' \
  "$repository_root/.github/workflows/release.yml" \
  > "$storage_record_workflow"
expect_rejected \
  "a personal-owner release workflow creating artifact storage records" \
  --release-workflow "$storage_record_workflow"

mutable_release_verification_workflow="$temporary_directory/mutable-release-verification.yml"
sed '/gh release verify-asset/d' \
  "$repository_root/.github/workflows/release.yml" \
  > "$mutable_release_verification_workflow"
expect_rejected \
  "a release workflow that does not verify each immutable release asset" \
  --release-workflow "$mutable_release_verification_workflow"

unchecked_draft_release_workflow="$temporary_directory/unchecked-draft-release.yml"
sed '/^  prepare-release:/,/^  publish-release:/ {
  /- name: Checkout validated commit/,/ref: \${{ needs.validate.outputs.commit }}/d
}' \
  "$repository_root/.github/workflows/release.yml" \
  > "$unchecked_draft_release_workflow"
expect_rejected \
  "a draft release job without the validated source tree" \
  --release-workflow "$unchecked_draft_release_workflow"

unbound_publish_release_workflow="$temporary_directory/unbound-publish-release.yml"
sed '/^  publish-release:/,/^  verify-release:/ {
  /GH_REPO: \${{ github.repository }}/d
}' \
  "$repository_root/.github/workflows/release.yml" \
  > "$unbound_publish_release_workflow"
expect_rejected \
  "a GitHub Release publisher without an explicit repository" \
  --release-workflow "$unbound_publish_release_workflow"

conflicting_archive_identity_workflow="$temporary_directory/conflicting-archive-identity-release.yml"
sed '/^  assemble-and-sign:/,/^  prepare-release:/ {
  /--cert-identity "\$certificate_identity"/a\
            --signer-workflow "${GITHUB_REPOSITORY}/.github/workflows/release.yml"
}' \
  "$repository_root/.github/workflows/release.yml" \
  > "$conflicting_archive_identity_workflow"
expect_rejected \
  "a release workflow combining archive certificate and signer identities" \
  --release-workflow "$conflicting_archive_identity_workflow"

conflicting_promotion_identity_workflow="$temporary_directory/conflicting-promotion-identity-release.yml"
sed '/^  promote-latest:/,$ {
  /--cert-identity "\$certificate_identity"/a\
              --signer-workflow "${GITHUB_REPOSITORY}/.github/workflows/release.yml"
}' \
  "$repository_root/.github/workflows/release.yml" \
  > "$conflicting_promotion_identity_workflow"
expect_rejected \
  "a release workflow combining promotion certificate and signer identities" \
  --release-workflow "$conflicting_promotion_identity_workflow"

unverified_image_attestations_workflow="$temporary_directory/unverified-image-attestations-release.yml"
sed 's#scripts/verify-release-image-attestations.sh#scripts/skip-release-image-attestations.sh#' \
  "$repository_root/.github/workflows/release.yml" \
  > "$unverified_image_attestations_workflow"
expect_rejected \
  "a release workflow that bypasses strict image attestation verification" \
  --release-workflow "$unverified_image_attestations_workflow"

unbound_image_attestations_workflow="$temporary_directory/unbound-image-attestations-release.yml"
sed '/scripts\/verify-release-image-attestations.sh/,/--commit "\$BUILD_GIT_COMMIT"/ {
  s/--commit "\$BUILD_GIT_COMMIT"/--commit "\$GITHUB_SHA"/
}' \
  "$repository_root/.github/workflows/release.yml" \
  > "$unbound_image_attestations_workflow"
expect_rejected \
  "a release workflow that does not bind image attestations to the validated commit" \
  --release-workflow "$unbound_image_attestations_workflow"

latest_without_identity_workflow="$temporary_directory/latest-without-identity-release.yml"
sed '/--deny-self-hosted-runners/d' \
  "$repository_root/.github/workflows/release.yml" \
  > "$latest_without_identity_workflow"
expect_rejected \
  "a release workflow that accepts self-hosted attestation identities" \
  --release-workflow "$latest_without_identity_workflow"

catalogless_build_workflow="$temporary_directory/catalogless-build-images.yml"
sed '/RELEASE_IMAGES_JSON: \${{ needs.verify.outputs.release_images }}/d' \
  "$repository_root/.github/workflows/build-images.yml" \
  > "$catalogless_build_workflow"
expect_rejected \
  "a pull-request image workflow bypassing the validated image catalog" \
  --build-images-workflow "$catalogless_build_workflow"

unverified_build_workflow="$temporary_directory/unverified-build-images.yml"
sed '/^[[:space:]]*needs: verify$/d' \
  "$repository_root/.github/workflows/build-images.yml" \
  > "$unverified_build_workflow"
expect_rejected \
  "a pull-request image workflow bypassing deployment admission" \
  --build-images-workflow "$unverified_build_workflow"

volatile_provenance_build_workflow="$temporary_directory/volatile-provenance-build-images.yml"
# shellcheck disable=SC2016
sed 's/${SMOKE_BUILD_GIT_COMMIT}/${GITHUB_SHA}/' \
  "$repository_root/.github/workflows/build-images.yml" \
  > "$volatile_provenance_build_workflow"
expect_rejected \
  "a pull-request image workflow invalidating Cargo caches with each commit" \
  --build-images-workflow "$volatile_provenance_build_workflow"

helpless_build_workflow="$temporary_directory/helpless-build-images.yml"
# shellcheck disable=SC2016
sed '/docker run --rm "\${image_name}:\${PR_TAG}" --help/d' \
  "$repository_root/.github/workflows/build-images.yml" \
  > "$helpless_build_workflow"
expect_rejected \
  "a pull-request image workflow without runtime help smokes" \
  --build-images-workflow "$helpless_build_workflow"

arm64less_build_workflow="$temporary_directory/arm64less-build-images.yml"
sed '/^[[:space:]]*-[[:space:]]*name: arm64$/,/^[[:space:]]*runner: ubuntu-24.04-arm$/d' \
  "$repository_root/.github/workflows/build-images.yml" \
  > "$arm64less_build_workflow"
expect_rejected \
  "a pull-request image workflow without native arm64 coverage" \
  --build-images-workflow "$arm64less_build_workflow"

platformless_build_workflow="$temporary_directory/platformless-build-images.yml"
# shellcheck disable=SC2016
sed '/--platform "\$PLATFORM_REF"/d' \
  "$repository_root/.github/workflows/build-images.yml" \
  > "$platformless_build_workflow"
expect_rejected \
  "a pull-request image workflow that ignores its target platform" \
  --build-images-workflow "$platformless_build_workflow"

serveless_prometheus="$temporary_directory/serveless-prometheus.yml"
sed '/targets: \["zinder-index:9108"\]/d' \
  "$repository_root/deploy/observability/prometheus.yml" \
  > "$serveless_prometheus"
expect_rejected \
  "a deploy Prometheus config omitting the zinder-serve ops target" \
  --prometheus-config "$serveless_prometheus"

mislabeled_serve_prometheus="$temporary_directory/mislabeled-serve-prometheus.yml"
sed '/job_name: "zinder-serve"/,/service: "zinder-serve"/s/service: "zinder-serve"/service: "zinder-index"/' \
  "$repository_root/deploy/observability/prometheus.yml" \
  > "$mislabeled_serve_prometheus"
expect_rejected \
  "a deploy Prometheus config mislabeling the zinder-serve target" \
  --prometheus-config "$mislabeled_serve_prometheus"

root_kube_manifest="$temporary_directory/root-statefulset.yaml"
sed 's/runAsUser: 10001/runAsUser: 0/' \
  "$repository_root/deploy/kubernetes/statefulset.yaml" \
  > "$root_kube_manifest"
expect_rejected \
  "a Kubernetes StatefulSet manifest running as root" \
  --kubernetes-manifest "$root_kube_manifest"

echo "deployment admission tests passed"
