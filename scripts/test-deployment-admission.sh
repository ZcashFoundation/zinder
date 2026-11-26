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
  --deployment-class canary \
  --target zinder-canonical-runtime
bash "$validator" \
  --deployment-class diagnostic \
  --target zinder-canonical-runtime

expect_rejected \
  "the single-volume wallet-serving target as production" \
  --deployment-class production \
  --target zinder-wallet-serving
expect_rejected \
  "the mixed single-container target" \
  --deployment-class production \
  --target zinder-single-container
expect_rejected \
  "the ingest-only canonical-runtime target as production" \
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

if grep -Eq 'zinder-[a-z-]+:latest' \
  "$repository_root/deploy/docker-compose.yml" \
  "$repository_root/deploy/docker-compose.light-wallet.yml"; then
  fail "the deployment Compose file defaults a Zinder runtime to latest"
fi

deployment_env_names=(mainnet testnet regtest zakura-mainnet zakura-testnet)

env_file_value() {
  sed -n "s/^$2=//p" "$repository_root/deploy/.env.$1"
}

if grep -Eiq 'JSON_RPC_ADDR:?-|INDEXER_GRPC_ADDR:?-[^}]|//(zebra|zakura)\b|z3-|:1?8232\b|:8230\b|:8155\b' \
  "$repository_root/deploy/docker-compose.yml" \
  "$repository_root/deploy/docker-compose.explorer.yml" \
  "$repository_root/deploy/docker-compose.transactions.yml" \
  "$repository_root/deploy/docker-compose.light-wallet.yml" \
  "$repository_root"/deploy/config/{ingest,projector,query,compat-lightwalletd,explorer}.toml \
  "$repository_root"/deploy/config/light-wallet/{ingest,compat-lightwalletd}.toml; then
  fail "the deployment Compose topology or its configs default to a node"
fi

for deployment_env in "${deployment_env_names[@]}"; do
  grep -Fqx 'ZINDER_NODE__INDEXER_GRPC_ADDR=' \
    "$repository_root/deploy/.env.$deployment_env" \
    || fail "the $deployment_env env file enables streaming mempool by default"
  projector_owner="$(env_file_value "$deployment_env" ZINDER_PROJECTOR_BUILD_OWNER_HEX)"
  [[ "$projector_owner" =~ ^[0-9a-f]{32}$ && "$projector_owner" =~ [a-f] ]] \
    || fail "the $deployment_env projector owner is not a config-safe 32-character hex string"
done
for zakura_network in mainnet testnet; do
  for shared_name in ZINDER_DEPLOY_NETWORK ZINDER_NETWORK__NAME ZINDER_PROJECTOR_BUILD_OWNER_HEX \
    ZINDER_INGEST_OPS_HOST_PORT ZINDER_QUERY_HOST_PORT ZINDER_COMPAT_HOST_PORT; do
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
jq -e '
  .services as $services
  | [
      "zinder-ingest",
      "zinder-projector",
      "zinder-query",
      "zinder-compat-lightwalletd"
    ]
  | all($services[.].environment.ZINDER_DEPLOYMENT__PROFILE == "wallet")
  and $services["zinder-ingest"].environment.ZINDER_INGEST__SOURCE == "zebra-json-rpc"
  and (($services["zinder-history-publisher"].tmpfs // []) == ["/tmp:rw,noexec,nosuid,size=512m"])
  and ($services["zinder-history-publisher"].command as $cmd
    | ($cmd | index("--workers")) as $idx
    | $idx != null and $cmd[$idx + 1] == "1")
' "$resolved_compose" >/dev/null \
  || fail "the default Compose topology does not select the wallet profile, zebra-json-rpc source, and bounded history-publisher defaults"

wallet_archive_compose="$temporary_directory/wallet-archive-compose.json"
ZINDER_CONTROL_SECRETS_DIR="$control_secrets" \
ZINDER_DEPLOYMENT__PROFILE=wallet-archive \
ZINDER_INGEST__SOURCE=zebra-indexer-grpc \
ZINDER_HISTORY_PUBLISHER_WORKERS=4 \
docker compose \
  --env-file "$repository_root/deploy/.env.testnet" \
  -f "$repository_root/deploy/docker-compose.yml" \
  config --format json > "$wallet_archive_compose"
bash "$validator" --compose-contract "$wallet_archive_compose"
jq -e '
  .services as $services
  | [
      "zinder-ingest",
      "zinder-projector",
      "zinder-query",
      "zinder-compat-lightwalletd"
    ]
  | all($services[.].environment.ZINDER_DEPLOYMENT__PROFILE == "wallet-archive")
  and $services["zinder-ingest"].environment.ZINDER_INGEST__SOURCE == "zebra-indexer-grpc"
  and ($services["zinder-history-publisher"].command as $cmd
    | ($cmd | index("--workers")) as $idx
    | $idx != null and $cmd[$idx + 1] == "4")
' "$wallet_archive_compose" >/dev/null \
  || fail "the Compose topology does not forward wallet-archive profile, ZINDER_INGEST__SOURCE, or ZINDER_HISTORY_PUBLISHER_WORKERS overrides"

history_dir="$temporary_directory/history"
history_compose="$temporary_directory/history-compose.json"
ZINDER_HISTORY_DIR="$history_dir" ZINDER_CONTROL_SECRETS_DIR="$control_secrets" \
  docker compose --env-file "$repository_root/deploy/.env.testnet" \
  -f "$repository_root/deploy/docker-compose.yml" config --format json > "$history_compose"
bash "$validator" --compose-contract "$history_compose"
jq -e --arg generation "$history_dir" '
  any(.services["zinder-history-publisher"].volumes[]; .source == ($generation + "/publisher.pk8"))
' "$history_compose" >/dev/null || fail "the publisher ignored the selected generation"

invalid_history_compose="$temporary_directory/invalid-history-compose.json"
jq '.services["zinder-query"].volumes += [
  (.services["zinder-history-publisher"].volumes[] | select(.target == "/run/zinder-history/publisher.pk8"))]
' "$history_compose" > "$invalid_history_compose"
expect_rejected "a query process with access to the publisher signing key" --compose-contract "$invalid_history_compose"
jq '(.services["zinder-compat-lightwalletd"].volumes[] | select(.target == "/var/lib/zinder-history")).read_only = false' \
  "$history_compose" > "$invalid_history_compose"
expect_rejected "a writer-capable catalog mount in a reader" --compose-contract "$invalid_history_compose"
for history_source in catalog catalog/segment.bin publisher.pub; do
  for read_only in true false; do
    jq --arg source "$history_dir/$history_source" --argjson readonly "$read_only" '
      .services["zinder-query"].volumes += [
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

# These cases exercise only the resolved JSON contract. Both declarations name
# the same Docker volume, and the mount selects a directory inside its backing.
check_named_history_input() {
  local subpath="$1" read_only="$2" expected="$3"
  jq --arg backing "$temporary_directory" --arg subpath "$subpath" --argjson readonly "$read_only" '
    .volumes.fixture = {name: "fixture", driver: "local", driver_opts: {device: $backing, o: "bind"}}
    | .volumes.fixture_view = {name: "fixture", external: true}
    | .services["zinder-query"].volumes += [{
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

# Only declarations are inspected. The fixture paths need no files or Docker volumes.
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
      | .services["zinder-query"][$resource] = [{source: "fixture", target: "fixture-input"}]
    ' "$history_compose" > "$invalid_history_compose"
    if [[ "$file" == publisher.pub ]]; then
      bash "$validator" --compose-contract "$invalid_history_compose"
    else
      reject_history_contract "a $resource grant from the publisher signing key"
    fi
  done
  jq --arg resource "$resource" '
    .[$resource].fixture = {external: true}
    | .services["zinder-query"][$resource] = ["fixture"]
  ' "$history_compose" > "$invalid_history_compose"
  reject_history_contract "a $resource grant with an external, unverified source"
done

jq '.services["zinder-query"].volumes_from = ["state-init:ro"]' \
  "$history_compose" > "$invalid_history_compose"
reject_history_contract "a non-publisher service inheriting mounts"
jq --arg source "$history_dir/" '
  (.services[].volumes[] | select(.type == "bind" and (.source | startswith($source))) | .source)
  |= ("/./" + ltrimstr($source))
' "$history_compose" > "$invalid_history_compose"
reject_history_contract "a history generation whose normalized path is root"

jq 'del(.services["zinder-compat-lightwalletd"].ulimits.nofile)' "$history_compose" > "$invalid_history_compose"
expect_rejected "a wallet reader inheriting its open-file limits" --compose-contract "$invalid_history_compose"
jq '.services["zinder-compat-lightwalletd"].ulimits.nofile = {"soft":1024,"hard":524288}' \
  "$history_compose" > "$invalid_history_compose"
expect_rejected "a wallet reader using the exhausted Mainnet soft limit" --compose-contract "$invalid_history_compose"
jq 'del(.services["zinder-query"].ulimits.nofile)' "$history_compose" > "$invalid_history_compose"
expect_rejected "the native query reader inheriting its open-file limits" --compose-contract "$invalid_history_compose"
jq '.services["zinder-query"].ulimits.nofile = {"soft":1024,"hard":524288}' \
  "$history_compose" > "$invalid_history_compose"
expect_rejected "the native query reader using the exhausted Mainnet soft limit" --compose-contract "$invalid_history_compose"

indexer_tls_directory="$temporary_directory/indexer-tls"
mkdir -p "$indexer_tls_directory"
resolve_indexer_tls_compose() {
  ZINDER_CONTROL_SECRETS_DIR="$control_secrets" docker compose \
    --env-file "$repository_root/deploy/.env.testnet" \
    -f "$repository_root/deploy/docker-compose.yml" \
    -f "$repository_root/deploy/docker-compose.indexer-tls.yml" \
    config "$@"
}
resolved_indexer_tls_compose="$temporary_directory/resolved-indexer-tls-compose.json"
ZINDER_INDEXER_TLS_DIR="$indexer_tls_directory" \
ZINDER_NODE__INDEXER_GRPC_ADDR=https://zakura:8230 \
ZINDER_NODE__INDEXER_TLS__SERVER_NAME=zakura.internal \
  resolve_indexer_tls_compose --format json > "$resolved_indexer_tls_compose"
bash "$validator" --compose-contract "$resolved_indexer_tls_compose"
jq -e --arg tls_directory "$indexer_tls_directory" '
  .services["zinder-ingest"].environment as $ingest_environment
  | $ingest_environment.ZINDER_NODE__INDEXER_GRPC_ADDR == "https://zakura:8230"
  and $ingest_environment.ZINDER_NODE__INDEXER_TLS__CA_CERT_PATH
    == "/var/run/zinder-indexer-tls/ca.pem"
  and $ingest_environment.ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PATH
    == "/var/run/zinder-indexer-tls/client.pem"
  and $ingest_environment.ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH
    == "/var/run/zinder-indexer-tls/client-key.pem"
  and $ingest_environment.ZINDER_NODE__INDEXER_TLS__SERVER_NAME == "zakura.internal"
  and (.services["zinder-ingest"].volumes
    | any(.source == $tls_directory
      and .target == "/var/run/zinder-indexer-tls"
      and .read_only == true))
  and ([.services | to_entries[] | select(.key != "zinder-ingest") | .value]
    | all(
      ((.environment // {}) | keys | all(startswith("ZINDER_NODE__INDEXER_TLS__") | not))
      and ((.volumes // []) | all(.target != "/var/run/zinder-indexer-tls"))
    ))
' "$resolved_indexer_tls_compose" >/dev/null \
  || fail "the indexer TLS overlay does not give its material to ingest alone"
if ZINDER_INDEXER_TLS_DIR="$indexer_tls_directory" ZINDER_NODE__INDEXER_GRPC_ADDR='' \
  resolve_indexer_tls_compose -q >/dev/null 2>&1; then
  fail "the indexer TLS overlay resolved without an indexer URL"
fi
if ZINDER_INDEXER_TLS_DIR='' ZINDER_NODE__INDEXER_GRPC_ADDR=https://zakura:8230 \
  resolve_indexer_tls_compose -q >/dev/null 2>&1; then
  fail "the indexer TLS overlay resolved without ZINDER_INDEXER_TLS_DIR"
fi

resolve_node_fallback_compose() {
  ZINDER_CONTROL_SECRETS_DIR="$control_secrets" docker compose \
    --env-file "$repository_root/deploy/.env.testnet" \
    -f "$repository_root/deploy/docker-compose.yml" \
    -f "$repository_root/deploy/docker-compose.node-fallback.yml" \
    config "$@"
}
resolved_node_fallback_compose="$temporary_directory/resolved-node-fallback-compose.json"
ZINDER_FALLBACK_SOURCE_NETWORK_NAME=zakura-testnet \
ZINDER_FALLBACK_SOURCE_COOKIE_VOLUME_NAME=zakura-testnet-cookie \
ZINDER_NODE_FALLBACKS__1__JSON_RPC_ADDR=http://zakura:18232 \
ZINDER_NODE_FALLBACKS__1__INDEXER_GRPC_ADDR=http://zakura:18230 \
  resolve_node_fallback_compose --format json > "$resolved_node_fallback_compose"
bash "$validator" --compose-contract "$resolved_node_fallback_compose"
jq -e '
  .services["zinder-ingest"].environment as $ingest_environment
  | $ingest_environment.ZINDER_NODE_FALLBACKS__1__JSON_RPC_ADDR == "http://zakura:18232"
  and $ingest_environment.ZINDER_NODE_FALLBACKS__1__INDEXER_GRPC_ADDR == "http://zakura:18230"
  and $ingest_environment.ZINDER_NODE_FALLBACKS__1__AUTH__PATH
    == "/var/run/auth-fallback/.cookie"
  and (.services["zinder-ingest"].networks | has("node-fallback"))
  and (.networks["node-fallback"].name == "zakura-testnet")
  and (.volumes["node-fallback-cookie"].name == "zakura-testnet-cookie")
  and ([.services["zinder-query", "zinder-compat-lightwalletd"]]
    | all(.environment.ZINDER_NODE_FALLBACKS__1__JSON_RPC_ADDR == "http://zakura:18232"
      and (.volumes | any(.target == "/var/run/auth-fallback" and .read_only == true))))
  and ((.services["zinder-projector"].environment // {})
    | keys | all(startswith("ZINDER_NODE_FALLBACKS__") | not))
' "$resolved_node_fallback_compose" >/dev/null \
  || fail "the node fallback overlay does not give the fallback endpoint to the node-reading runtimes"
if ZINDER_FALLBACK_SOURCE_COOKIE_VOLUME_NAME=zakura-testnet-cookie \
  ZINDER_NODE_FALLBACKS__1__JSON_RPC_ADDR=http://zakura:18232 \
  resolve_node_fallback_compose -q >/dev/null 2>&1; then
  fail "the node fallback overlay resolved without the fallback node's network"
fi

resolved_explorer_compose="$temporary_directory/resolved-explorer-compose.json"
ZINDER_CONTROL_SECRETS_DIR="$control_secrets" \
ZINDER_DEPLOYMENT__PROFILE=wallet-archive \
docker compose \
  --env-file "$repository_root/deploy/.env.testnet" \
  -f "$repository_root/deploy/docker-compose.yml" \
  -f "$repository_root/deploy/docker-compose.explorer.yml" \
  config --format json > "$resolved_explorer_compose"
jq -e '
  . as $services_root
  | .services["zinder-explorer"].build.target == "zinder-explorer"
  and .services["zinder-explorer"].command == ["--config", "/etc/zinder/config.toml"]
  and .services["zinder-explorer"].network_mode == "service:zinder-ingest"
  and .services["zinder-ingest"].ulimits.nofile.soft == 65536
  and .services["zinder-ingest"].ulimits.nofile.hard == 65536
  and .services["zinder-explorer"].ulimits.nofile.soft == 65536
  and .services["zinder-explorer"].ulimits.nofile.hard == 65536
  and .services["zinder-explorer"].depends_on["zinder-ingest"].condition == "service_started"
  and .services["zinder-explorer"].depends_on["zinder-query"].condition == "service_healthy"
  and .services["zinder-explorer"].healthcheck.test == [
    "CMD",
    "zinder-explorer",
    "healthcheck"
  ]
  and .services["zinder-ingest"].environment.ZINDER_INGEST__EXPLORER_VIEWS == "true"
  and ([
      "zinder-ingest",
      "zinder-projector",
      "zinder-query",
      "zinder-compat-lightwalletd"
    ] | all(. as $service
      | (($services_root.services[$service].environment // {}) | has("ZINDER_DEPLOYMENT__PROFILE") | not)))
  and .services["zinder-explorer"].environment.ZINDER_EXPLORER__LISTEN_ADDR == "[::]:9068"
  and .services["zinder-explorer"].environment.ZINDER_OPS__LISTEN_ADDR == "[::]:9069"
  and (.services["zinder-ingest"].networks.node.aliases | index("zinder-testnet-explorer")) != null
  and (.services["zinder-ingest"].ports | any(.target == 9068 and .published == "19068"))
  and (.services["zinder-ingest"].ports | any(.target == 9069 and .published == "19069"))
  and (.services["zinder-explorer"].volumes
    | any(.target == "/etc/zinder/config.toml" and .read_only == true))
  and (.services["zinder-explorer"].volumes
    | all(.target != "/var/lib/zinder/checkpoints"))
  and (.services["zinder-explorer"].volumes
    | all(.target != "/var/run/zinder-checkpoint/checkpoint.token"))
' "$resolved_explorer_compose" >/dev/null \
  || fail "the optional explorer overlay does not preserve its reader-only deployment contract"

proxy_trust_settings='
  [
    .services["zinder-query"].environment.ZINDER_QUERY__TRUST_PROXY_CLIENT_ADDRESS,
    .services["zinder-compat-lightwalletd"].environment.ZINDER_COMPAT__TRUST_PROXY_CLIENT_ADDRESS,
    .services["zinder-explorer"].environment.ZINDER_EXPLORER__TRUST_PROXY_CLIENT_ADDRESS
  ]
'
jq -e "$proxy_trust_settings == [\"false\", \"false\", \"false\"]" \
  "$resolved_explorer_compose" >/dev/null \
  || fail "a public listener trusts its proxy's client address header by default"
trusted_proxy_compose="$temporary_directory/trusted-proxy-compose.json"
ZINDER_CONTROL_SECRETS_DIR="$control_secrets" \
ZINDER_QUERY__TRUST_PROXY_CLIENT_ADDRESS=true \
ZINDER_COMPAT__TRUST_PROXY_CLIENT_ADDRESS=true \
ZINDER_EXPLORER__TRUST_PROXY_CLIENT_ADDRESS=true \
docker compose \
  --env-file "$repository_root/deploy/.env.testnet" \
  -f "$repository_root/deploy/docker-compose.yml" \
  -f "$repository_root/deploy/docker-compose.explorer.yml" \
  config --format json > "$trusted_proxy_compose"
jq -e "$proxy_trust_settings == [\"true\", \"true\", \"true\"]" \
  "$trusted_proxy_compose" >/dev/null \
  || fail "the Compose topology does not pass trust_proxy_client_address to every public listener"

released_compose="$temporary_directory/released-compose.json"
ZINDER_CONTROL_SECRETS_DIR="$control_secrets" \
ZINDER_VERSION=v9.8.7 \
docker compose \
  --env-file "$repository_root/deploy/.env.testnet" \
  -f "$repository_root/deploy/docker-compose.yml" \
  -f "$repository_root/deploy/docker-compose.explorer.yml" \
  config --format json > "$released_compose"
jq -e '
  [
    "zinder-ingest",
    "zinder-projector",
    "zinder-query",
    "zinder-compat-lightwalletd",
    "zinder-explorer"
  ]
  | all(. as $service
    | $released[0].services[$service].image == ("ghcr.io/zcashfoundation/" + $service + ":v9.8.7")
    and $released[0].services[$service].build.target == $service
    and $source[0].services[$service].image == ("ghcr.io/zcashfoundation/" + $service + ":local"))
' --slurpfile released "$released_compose" --slurpfile source "$resolved_explorer_compose" \
  -n >/dev/null \
  || fail "ZINDER_VERSION does not select the published release image for every runtime while keeping its source build"

for deployment_env in "${deployment_env_names[@]}"; do
  resolved_env_compose="$temporary_directory/resolved-$deployment_env-compose.json"
  ZINDER_CONTROL_SECRETS_DIR="$control_secrets" \
  docker compose \
    --env-file "$repository_root/deploy/.env.$deployment_env" \
    -f "$repository_root/deploy/docker-compose.yml" \
    --profile observability \
    config --format json > "$resolved_env_compose"
  bash "$validator" --compose-contract "$resolved_env_compose"
  jq -e '
    [
      "zinder-ingest",
      "zinder-projector",
      "zinder-query",
      "zinder-history-publisher",
      "zinder-compat-lightwalletd"
    ] as $store_services
    | .services as $services
    | all($store_services[]; $services[.].ulimits.nofile == {"soft":65536,"hard":65536})
  ' "$resolved_env_compose" >/dev/null \
    || fail "the $deployment_env Zinder store-opening services do not have the expected soft and hard open-file limits"
  resolved_env_explorer_compose="$temporary_directory/resolved-$deployment_env-explorer-compose.json"
  ZINDER_CONTROL_SECRETS_DIR="$control_secrets" \
  docker compose \
    --env-file "$repository_root/deploy/.env.$deployment_env" \
    -f "$repository_root/deploy/docker-compose.yml" \
    -f "$repository_root/deploy/docker-compose.explorer.yml" \
    config --format json > "$resolved_env_explorer_compose"
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
    and .services["zinder-ingest"].environment.ZINDER_NODE__INDEXER_GRPC_ADDR == ""
    and $json_rpc_addr != ""
    and (.services as $services
      | [
          "zinder-ingest",
          "zinder-query",
          "zinder-compat-lightwalletd",
          "zinder-explorer"
        ]
      | all($services[.].environment.ZINDER_NODE__JSON_RPC_ADDR == $json_rpc_addr))
    and (.services["zinder-projector"].environment | has("ZINDER_NODE__JSON_RPC_ADDR") | not)
  ' "$resolved_env_explorer_compose" >/dev/null \
    || fail "the $deployment_env env file does not resolve to its own node network, cookie volume, and endpoint"
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

# Light-wallet Compose bundle admission and override checks.
resolved_light_wallet_default="$temporary_directory/resolved-light-wallet-default.json"
ZINDER_CONTROL_SECRETS_DIR="$control_secrets" \
docker compose \
  --env-file "$repository_root/deploy/.env.testnet" \
  -f "$repository_root/deploy/docker-compose.light-wallet.yml" \
  config --format json > "$resolved_light_wallet_default"
bash "$validator" --light-wallet-compose-contract "$resolved_light_wallet_default"
jq -e '
  (.services | keys | sort) == [
    "state-init",
    "zinder-compat-lightwalletd",
    "zinder-history-publisher",
    "zinder-ingest"
  ]
  and .services["zinder-ingest"].environment.ZINDER_DEPLOYMENT__PROFILE == "light-wallet"
  and .services["zinder-ingest"].environment.ZINDER_INGEST__SOURCE == "zebra-json-rpc"
  and .services["zinder-compat-lightwalletd"].environment.ZINDER_DEPLOYMENT__PROFILE == "light-wallet"
  and .services["zinder-compat-lightwalletd"].environment.ZINDER_COMPAT__TRUST_PROXY_CLIENT_ADDRESS == "false"
  and (.services["zinder-history-publisher"].tmpfs == ["/tmp:rw,noexec,nosuid,size=512m"])
  and (.services["zinder-history-publisher"].command as $cmd
    | ($cmd | index("--workers")) as $idx
    | $idx != null and $cmd[$idx + 1] == "1")
' "$resolved_light_wallet_default" >/dev/null \
  || fail "the default light-wallet Compose bundle does not match the expected projector-free topology"

resolved_light_wallet_observability="$temporary_directory/resolved-light-wallet-observability.json"
ZINDER_CONTROL_SECRETS_DIR="$control_secrets" \
ZINDER_INGEST__SOURCE=zebra-indexer-grpc \
ZINDER_HISTORY_PUBLISHER_WORKERS=3 \
ZINDER_COMPAT__TRUST_PROXY_CLIENT_ADDRESS=true \
ZINDER_VERSION=v9.8.7 \
docker compose \
  --env-file "$repository_root/deploy/.env.testnet" \
  -f "$repository_root/deploy/docker-compose.light-wallet.yml" \
  --profile observability \
  config --format json > "$resolved_light_wallet_observability"
bash "$validator" --light-wallet-compose-contract "$resolved_light_wallet_observability"
jq -e '
  (.services | keys | sort) == [
    "state-init",
    "zinder-compat-lightwalletd",
    "zinder-grafana",
    "zinder-history-publisher",
    "zinder-ingest",
    "zinder-prometheus"
  ]
  and .services["zinder-ingest"].image == "ghcr.io/zcashfoundation/zinder-ingest:v9.8.7"
  and .services["zinder-compat-lightwalletd"].image == "ghcr.io/zcashfoundation/zinder-compat-lightwalletd:v9.8.7"
  and .services["zinder-history-publisher"].image == "ghcr.io/zcashfoundation/zinder-compat-lightwalletd:v9.8.7"
  and .services["zinder-ingest"].environment.ZINDER_INGEST__SOURCE == "zebra-indexer-grpc"
  and .services["zinder-compat-lightwalletd"].environment.ZINDER_COMPAT__TRUST_PROXY_CLIENT_ADDRESS == "true"
  and (.services["zinder-history-publisher"].command as $cmd
    | ($cmd | index("--workers")) as $idx
    | $idx != null and $cmd[$idx + 1] == "3")
' "$resolved_light_wallet_observability" >/dev/null \
  || fail "the light-wallet Compose bundle does not honor observability profile, version, source, workers, or proxy trust overrides"

for deployment_env in "${deployment_env_names[@]}"; do
  resolved_env_light_wallet="$temporary_directory/resolved-$deployment_env-light-wallet.json"
  ZINDER_CONTROL_SECRETS_DIR="$control_secrets" \
  docker compose \
    --env-file "$repository_root/deploy/.env.$deployment_env" \
    -f "$repository_root/deploy/docker-compose.light-wallet.yml" \
    --profile observability \
    config --format json > "$resolved_env_light_wallet"
  bash "$validator" --light-wallet-compose-contract "$resolved_env_light_wallet"
done

if ZINDER_CONTROL_SECRETS_DIR="$control_secrets" docker compose \
  --env-file "$missing_node_env" \
  -f "$repository_root/deploy/docker-compose.light-wallet.yml" \
  config -q >/dev/null 2>&1; then
  fail "docker-compose.light-wallet.yml resolved without a node JSON-RPC address"
fi
if ZINDER_CONTROL_SECRETS_DIR="$control_secrets" docker compose \
  --env-file "$missing_deploy_network_env" \
  -f "$repository_root/deploy/docker-compose.light-wallet.yml" \
  config -q >/dev/null 2>&1; then
  fail "docker-compose.light-wallet.yml resolved without ZINDER_DEPLOY_NETWORK"
fi

invalid_light_wallet="$temporary_directory/invalid-light-wallet.json"
jq '.services["zinder-projector"] = {}' \
  "$resolved_light_wallet_default" > "$invalid_light_wallet"
expect_rejected \
  "a light-wallet Compose bundle containing zinder-projector" \
  --light-wallet-compose-contract "$invalid_light_wallet"
jq '.services["zinder-query"] = {}' \
  "$resolved_light_wallet_default" > "$invalid_light_wallet"
expect_rejected \
  "a light-wallet Compose bundle containing zinder-query" \
  --light-wallet-compose-contract "$invalid_light_wallet"
jq 'del(.services["zinder-history-publisher"].tmpfs)' \
  "$resolved_light_wallet_default" > "$invalid_light_wallet"
expect_rejected \
  "a light-wallet Compose bundle omitting publisher tmpfs" \
  --light-wallet-compose-contract "$invalid_light_wallet"
jq '.services["zinder-compat-lightwalletd"].volumes += [
  (.services["zinder-ingest"].volumes[] | select(.target == "/var/run/zinder-checkpoint/checkpoint.token"))
]' "$resolved_light_wallet_default" > "$invalid_light_wallet"
expect_rejected \
  "a light-wallet Compose bundle exposing checkpoint.token to compat" \
  --light-wallet-compose-contract "$invalid_light_wallet"
jq '
  .services["zinder-prometheus"].volumes |= map(
    if .target == "/etc/prometheus/prometheus.yml" then
      .source |= sub("/deploy/observability/light-wallet/prometheus\\.yml$"; "/deploy/observability/prometheus.yml")
    else . end
  )
' "$resolved_light_wallet_observability" > "$invalid_light_wallet"
expect_rejected \
  "a light-wallet Compose bundle mounting the four-runtime Prometheus config" \
  --light-wallet-compose-contract "$invalid_light_wallet"

# Legacy transactions-retention overlay clears any inherited profile and pins
# transactions retention across writer and wallet-serving readers.
resolved_transactions_compose="$temporary_directory/resolved-transactions-compose.json"
ZINDER_CONTROL_SECRETS_DIR="$control_secrets" \
ZINDER_DEPLOYMENT__PROFILE=wallet \
docker compose \
  --env-file "$repository_root/deploy/.env.testnet" \
  -f "$repository_root/deploy/docker-compose.yml" \
  -f "$repository_root/deploy/docker-compose.transactions.yml" \
  config --format json > "$resolved_transactions_compose"
bash "$validator" --compose-contract "$resolved_transactions_compose"
jq -e '
  .services as $services
  | [
      "zinder-ingest",
      "zinder-projector",
      "zinder-query",
      "zinder-compat-lightwalletd"
    ]
  | all(
      (($services[.].environment // {}) | has("ZINDER_DEPLOYMENT__PROFILE") | not)
      and $services[.].environment.ZINDER_STORAGE__RAW_BLOB_POLICY == "transactions"
    )
' "$resolved_transactions_compose" >/dev/null \
  || fail "the transactions Compose overlay does not clear ZINDER_DEPLOYMENT__PROFILE and set raw_blob_policy=transactions across all runtimes"

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

missing_state_init_compose="$temporary_directory/missing-state-init-compose.json"
jq 'del(.services["state-init"]) | del(.services["zinder-ingest"].depends_on["state-init"])' \
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
  |
  .services["zinder-ingest"].volumes |= map(
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

missing_owner_checkpoint_mount_compose="$temporary_directory/missing-owner-checkpoint-mount-compose.json"
jq 'del(.services["zinder-projector"].volumes[] | select(.target == "/var/lib/zinder/checkpoints"))' \
  "$resolved_compose" > "$missing_owner_checkpoint_mount_compose"
expect_rejected \
  "a Compose contract missing a projector checkpoint-staging mount" \
  --compose-contract "$missing_owner_checkpoint_mount_compose"

compat_checkpoint_mount_compose="$temporary_directory/compat-checkpoint-mount-compose.json"
jq '
  (.services["zinder-ingest"].volumes[] | select(.target == "/var/lib/zinder/checkpoints")) as $checkpoint_mount
  |
  .services["zinder-compat-lightwalletd"].volumes += [
    $checkpoint_mount
  ]
' "$resolved_compose" > "$compat_checkpoint_mount_compose"
expect_rejected \
  "a Compose contract mounting checkpoint staging into compatibility" \
  --compose-contract "$compat_checkpoint_mount_compose"

query_checkpoint_mount_compose="$temporary_directory/query-checkpoint-mount-compose.json"
jq '
  (.services["zinder-ingest"].volumes[] | select(.target == "/var/lib/zinder/checkpoints")) as $checkpoint_mount
  |
  .services["zinder-query"].volumes += [
    $checkpoint_mount
  ]
' "$resolved_compose" > "$query_checkpoint_mount_compose"
expect_rejected \
  "a Compose contract mounting checkpoint staging into native query" \
  --compose-contract "$query_checkpoint_mount_compose"

query_checkpoint_secret_compose="$temporary_directory/query-checkpoint-secret-compose.json"
jq '
  (.services["zinder-ingest"].volumes[] | select(.target == "/var/run/zinder-checkpoint/checkpoint.token")) as $checkpoint_secret
  |
  .services["zinder-query"].volumes += [
    ($checkpoint_secret | .target = "/var/run/alternate/checkpoint-capability")
  ]
' "$resolved_compose" > "$query_checkpoint_secret_compose"
expect_rejected \
  "a native query contract mounting the checkpoint secret under an alternate target" \
  --compose-contract "$query_checkpoint_secret_compose"

swapped_query_config_compose="$temporary_directory/swapped-query-config-compose.json"
jq '
  (.services["zinder-compat-lightwalletd"].volumes[] | select(.target == "/etc/zinder/config.toml") | .source) as $compat_config
  |
  (.services["zinder-query"].volumes[] | select(.target == "/etc/zinder/config.toml") | .source) = $compat_config
' "$resolved_compose" > "$swapped_query_config_compose"
expect_rejected \
  "a native query contract mounting the compatibility runtime config" \
  --compose-contract "$swapped_query_config_compose"

overlapping_config_root="$temporary_directory/overlapping/deploy/config"
mkdir -p "$overlapping_config_root"
sed 's#/query/#/compat/#g' "$repository_root/deploy/config/query.toml" \
  > "$overlapping_config_root/query.toml"
overlapping_reader_paths_compose="$temporary_directory/overlapping-reader-paths-compose.json"
jq --arg query_config "$overlapping_config_root/query.toml" '
  (.services["zinder-query"].volumes[] | select(.target == "/etc/zinder/config.toml") | .source) = $query_config
' "$resolved_compose" > "$overlapping_reader_paths_compose"
expect_rejected \
  "native and compatibility readers sharing secondary roots" \
  --compose-contract "$overlapping_reader_paths_compose"

nested_config_root="$temporary_directory/nested/deploy/config"
mkdir -p "$nested_config_root"
sed 's#/var/lib/zinder/query/canonical-secondary#/var/lib/zinder/compat/canonical-secondary/nested#' \
  "$repository_root/deploy/config/query.toml" > "$nested_config_root/query.toml"
nested_reader_paths_compose="$temporary_directory/nested-reader-paths-compose.json"
jq --arg query_config "$nested_config_root/query.toml" '
  (.services["zinder-query"].volumes[] | select(.target == "/etc/zinder/config.toml") | .source) = $query_config
' "$resolved_compose" > "$nested_reader_paths_compose"
expect_rejected \
  "a native reader secondary nested beneath a compatibility reader root" \
  --compose-contract "$nested_reader_paths_compose"

public_host_compose="$temporary_directory/public-host-compose.json"
jq '.services["zinder-ingest"].ports[0].host_ip = "0.0.0.0"' \
  "$resolved_compose" > "$public_host_compose"
expect_rejected \
  "a Compose contract with a public host publication" \
  --compose-contract "$public_host_compose"

duplicate_host_port_compose="$temporary_directory/duplicate-host-port-compose.json"
jq '.services["zinder-ingest"].ports[1].published = .services["zinder-ingest"].ports[0].published' \
  "$resolved_compose" > "$duplicate_host_port_compose"
expect_rejected \
  "a Compose contract publishing two runtimes on the same host port" \
  --compose-contract "$duplicate_host_port_compose"

published_control_compose="$temporary_directory/published-control-compose.json"
jq '.services["zinder-ingest"].ports += [{"host_ip":"127.0.0.1","target":9100,"published":"9100","protocol":"tcp"}]' \
  "$resolved_compose" > "$published_control_compose"
expect_rejected \
  "a Compose contract publishing the private control port" \
  --compose-contract "$published_control_compose"

healthz_compose="$temporary_directory/healthz-compose.json"
jq '.services["zinder-ingest"].healthcheck.test += ["--path", "/healthz"]' \
  "$resolved_compose" > "$healthz_compose"
expect_rejected \
  "a Compose dependency gate using liveness instead of readiness" \
  --compose-contract "$healthz_compose"

short_ingest_healthcheck_grace_compose="$temporary_directory/short-ingest-healthcheck-grace-compose.json"
jq '.services["zinder-ingest"].healthcheck.start_period = "1m0s"' \
  "$resolved_compose" > "$short_ingest_healthcheck_grace_compose"
expect_rejected \
  "an ingest readiness gate that expires before the canonical construction hard gate" \
  --compose-contract "$short_ingest_healthcheck_grace_compose"

short_projector_healthcheck_grace_compose="$temporary_directory/short-projector-healthcheck-grace-compose.json"
jq '.services["zinder-projector"].healthcheck.start_period = "1m0s"' \
  "$resolved_compose" > "$short_projector_healthcheck_grace_compose"
expect_rejected \
  "a projector readiness gate that expires before the wallet construction hard gate" \
  --compose-contract "$short_projector_healthcheck_grace_compose"

split_namespace_compose="$temporary_directory/split-namespace-compose.json"
jq '.services["zinder-projector"].network_mode = null' \
  "$resolved_compose" > "$split_namespace_compose"
expect_rejected \
  "a Compose contract splitting the projector control namespace" \
  --compose-contract "$split_namespace_compose"

split_query_namespace_compose="$temporary_directory/split-query-namespace-compose.json"
jq '.services["zinder-query"].network_mode = null' \
  "$resolved_compose" > "$split_query_namespace_compose"
expect_rejected \
  "a Compose contract splitting the native query control namespace" \
  --compose-contract "$split_query_namespace_compose"

implicit_security_compose="$temporary_directory/implicit-security-compose.json"
jq 'del(.services["zinder-compat-lightwalletd"].environment.ZINDER_SECURITY__ALLOW_PUBLIC_BIND)' \
  "$resolved_compose" > "$implicit_security_compose"
expect_rejected \
  "a Compose contract relying on an image-baked public-bind opt-in" \
  --compose-contract "$implicit_security_compose"

implicit_query_security_compose="$temporary_directory/implicit-query-security-compose.json"
jq 'del(.services["zinder-query"].environment.ZINDER_SECURITY__ALLOW_PUBLIC_BIND)' \
  "$resolved_compose" > "$implicit_query_security_compose"
expect_rejected \
  "a native query contract relying on an image-baked public-bind opt-in" \
  --compose-contract "$implicit_query_security_compose"

mixed_release_workflow="$temporary_directory/release.yml"
cp "$repository_root/.github/workflows/release.yml" "$mixed_release_workflow"
printf '\n# forbidden release target: zinder-single-container\n' >> "$mixed_release_workflow"
expect_rejected \
  "a release workflow containing the mixed single-container target" \
  --release-workflow "$mixed_release_workflow"

while IFS= read -r omitted_image; do
  omitted_image_catalog="$temporary_directory/catalog-without-${omitted_image}.json"
  jq --arg omitted_image "$omitted_image" \
    'map(select(. != $omitted_image))' \
    "$release_images_catalog" > "$omitted_image_catalog"
  expect_rejected \
    "a release image catalog omitting $omitted_image" \
    --release-workflow "$repository_root/.github/workflows/release.yml" \
    --release-images-catalog "$omitted_image_catalog"
done < <(jq -r '.[]' "$release_images_catalog")

bench_release_images_catalog="$temporary_directory/catalog-with-bench.json"
jq '. + ["zinder-bench"]' \
  "$release_images_catalog" > "$bench_release_images_catalog"
expect_rejected \
  "a release image catalog containing the benchmark" \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --release-images-catalog "$bench_release_images_catalog"

swapped_release_images_catalog="$temporary_directory/catalog-with-bench-for-explorer.json"
jq 'map(if . == "zinder-explorer" then "zinder-bench" else . end)' \
  "$release_images_catalog" > "$swapped_release_images_catalog"
expect_rejected \
  "a release image catalog naming the benchmark instead of the Explorer" \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --release-images-catalog "$swapped_release_images_catalog"

mixed_release_images_catalog="$temporary_directory/catalog-with-mixed-runtime.json"
jq '.[0] = "zinder-single-container"' \
  "$release_images_catalog" > "$mixed_release_images_catalog"
expect_rejected \
  "a release image catalog containing the mixed single-container runtime" \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --release-images-catalog "$mixed_release_images_catalog"

duplicate_release_images_catalog="$temporary_directory/catalog-with-duplicate.json"
jq '.[3] = .[0]' \
  "$release_images_catalog" > "$duplicate_release_images_catalog"
expect_rejected \
  "a release image catalog containing duplicate runtimes" \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --release-images-catalog "$duplicate_release_images_catalog"

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
# The literal shell variables are the fixture removal target.
# shellcheck disable=SC2016
mutate_image_packager "$unverified_image_binaries_packager" \
  sed '/cmp -- "\${binary_root}\/\${binary_name}" "\$image_binary"/d'
expect_rejected_because \
  "an image packager that does not compare image binaries with the archive" \
  'release admission rejected: the release image packager must' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --image-packager "$unverified_image_binaries_packager"

modeless_archive_packager="$temporary_directory/modeless-archive-packager.sh"
# The literal shell variables are the fixture removal target.
# shellcheck disable=SC2016
mutate_image_packager "$modeless_archive_packager" \
  sed '/require_archive_binary_mode "\$binary_root\/\$binary_name" "archive binary \$binary_name"/d'
expect_rejected_because \
  "an image packager that does not check the extracted archive binary modes" \
  'release admission rejected: the release image packager must' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --image-packager "$modeless_archive_packager"

modeless_image_packager="$temporary_directory/modeless-image-packager.sh"
# The literal shell variables are the fixture removal target.
# shellcheck disable=SC2016
mutate_image_packager "$modeless_image_packager" \
  sed '/require_archive_binary_mode "\$image_binary"/d'
expect_rejected_because \
  "an image packager that does not check the image binary modes" \
  'release admission rejected: the release image packager must' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --image-packager "$modeless_image_packager"

unchecked_installed_mode_packager="$temporary_directory/unchecked-installed-mode-packager.sh"
# The literal shell variables are the fixture removal target.
# shellcheck disable=SC2016
mutate_image_packager "$unchecked_installed_mode_packager" \
  sed '/\[\[ "\$image_binary_listing" == /d'
expect_rejected_because \
  "an image packager that ignores the mode installed inside the image" \
  'release admission rejected: the release image packager must' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --image-packager "$unchecked_installed_mode_packager"

unchecked_healthcheck_packager="$temporary_directory/unchecked-healthcheck-packager.sh"
# The literal shell variables are the fixture removal target.
# shellcheck disable=SC2016
mutate_image_packager "$unchecked_healthcheck_packager" \
  sed '/\[\[ "\$healthcheck_status" -eq 1 \]\]/d'
expect_rejected_because \
  "an image packager that does not run the healthcheck inside the image" \
  'release admission rejected: the release image packager must' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --image-packager "$unchecked_healthcheck_packager"

wall_clock_created_packager="$temporary_directory/wall-clock-created-packager.sh"
# The literal shell variables are the fixture rewrite target.
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
# The literal workflow expressions are the fixture insertion text.
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
# The awk field reference is the fixture insertion program.
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
    print "          docker buildx build --file deploy/Dockerfile --target zinder-ingest ."
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
  $0 == "        --bin zinder-explorer \\" { print "        --bin zinder-bench \\" }
' "$repository_root/deploy/Dockerfile" > "$bench_compile_dockerfile"
expect_rejected_because \
  "a Dockerfile whose shared release compile also builds the benchmark" \
  'release admission rejected: the zinder-binaries stage must compile exactly' \
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
  in_stage && $0 == "        --bin zinder-explorer \\" {
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
sed 's#^COPY --from=zinder-binaries-selected /zinder-binaries/zinder-query #COPY --from=zinder-binaries /zinder-binaries/zinder-query #' \
  "$repository_root/deploy/Dockerfile" > "$compiling_image_dockerfile"
expect_rejected_because \
  "a Dockerfile whose query images copy from the compile stage" \
  'release admission rejected: the zinder-binaries stage must compile exactly' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --dockerfile "$compiling_image_dockerfile"

compiling_archive_dockerfile="$temporary_directory/compiling-archive.Dockerfile"
sed 's/^FROM scratch AS zinder-archived-binaries$/FROM zinder-binaries AS zinder-archived-binaries/' \
  "$repository_root/deploy/Dockerfile" > "$compiling_archive_dockerfile"
expect_rejected_because \
  "a Dockerfile whose archived-binary source compiles when its context is missing" \
  'release admission rejected: the zinder-binaries stage must compile exactly' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --dockerfile "$compiling_archive_dockerfile"

debian_service_dockerfile="$temporary_directory/debian-service.Dockerfile"
sed 's/^FROM zinder-service-base AS zinder-query$/FROM zinder-runtime-base AS zinder-query/' \
  "$repository_root/deploy/Dockerfile" > "$debian_service_dockerfile"
expect_rejected_because \
  "a Dockerfile whose query image builds on the Debian runtime base" \
  'release admission rejected: every per-service release image must build on' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --dockerfile "$debian_service_dockerfile"

shell_less_single_volume_dockerfile="$temporary_directory/shell-less-single-volume.Dockerfile"
sed 's/^FROM zinder-runtime-base AS zinder-wallet-serving$/FROM zinder-service-base AS zinder-wallet-serving/' \
  "$repository_root/deploy/Dockerfile" > "$shell_less_single_volume_dockerfile"
expect_rejected_because \
  "a Dockerfile whose wallet-serving image has no shell for its entrypoint" \
  'release admission rejected: every per-service release image must build on' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --dockerfile "$shell_less_single_volume_dockerfile"

compat_less_wallet_serving_dockerfile="$temporary_directory/compat-less-wallet-serving.Dockerfile"
sed '/^FROM zinder-runtime-base AS zinder-wallet-serving$/,/^ENTRYPOINT /{/^COPY --from=zinder-binaries-selected \/zinder-binaries\/zinder-compat-lightwalletd /d}' \
  "$repository_root/deploy/Dockerfile" > "$compat_less_wallet_serving_dockerfile"
cmp -s "$repository_root/deploy/Dockerfile" "$compat_less_wallet_serving_dockerfile" \
  && fail "the fixture did not remove the wallet-serving compatibility binary"
expect_rejected_because \
  "a Dockerfile whose wallet-serving image omits the lightwalletd compatibility binary" \
  'release admission rejected: the zinder-wallet-serving image must carry exactly' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --dockerfile "$compat_less_wallet_serving_dockerfile"

closed_compat_port_dockerfile="$temporary_directory/closed-compat-port.Dockerfile"
sed 's/^EXPOSE 9067 9102 9106 9107$/EXPOSE 9102 9106 9107/' \
  "$repository_root/deploy/Dockerfile" > "$closed_compat_port_dockerfile"
cmp -s "$repository_root/deploy/Dockerfile" "$closed_compat_port_dockerfile" \
  && fail "the fixture did not remove the wallet-serving compatibility port"
expect_rejected_because \
  "a Dockerfile whose wallet-serving image does not expose the compatibility port" \
  'release admission rejected: the zinder-wallet-serving image must carry exactly' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --dockerfile "$closed_compat_port_dockerfile"

unpinned_distroless_dockerfile="$temporary_directory/unpinned-distroless.Dockerfile"
# The variable reference is the literal Dockerfile text being rewritten.
# shellcheck disable=SC2016
sed 's#^FROM gcr.io/distroless/cc-debian12:nonroot@\${DISTROLESS_IMAGE_DIGEST} AS zinder-service-base$#FROM gcr.io/distroless/cc-debian12:nonroot AS zinder-service-base#' \
  "$repository_root/deploy/Dockerfile" > "$unpinned_distroless_dockerfile"
expect_rejected_because \
  "a Dockerfile whose distroless service base is not digest-pinned" \
  'release admission rejected: every per-service release image must build on' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --dockerfile "$unpinned_distroless_dockerfile"

legacy_uid_dockerfile="$temporary_directory/legacy-uid.Dockerfile"
sed 's/^USER 10001:10001$/USER 1000:1000/' \
  "$repository_root/deploy/Dockerfile" > "$legacy_uid_dockerfile"
expect_rejected_because \
  "a Dockerfile whose service images run as UID 1000" \
  'release admission rejected: every per-service release image must build on' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --dockerfile "$legacy_uid_dockerfile"

legacy_account_dockerfile="$temporary_directory/legacy-account.Dockerfile"
sed 's/useradd --system --uid 10001 --gid zinder/useradd --system --uid 1000 --gid zinder/' \
  "$repository_root/deploy/Dockerfile" > "$legacy_account_dockerfile"
expect_rejected_because \
  "a Dockerfile whose single-volume zinder account is not UID 10001" \
  'release admission rejected: every per-service release image must build on' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --dockerfile "$legacy_account_dockerfile"

curl_healthcheck_dockerfile="$temporary_directory/curl-healthcheck.Dockerfile"
sed 's#^    CMD \["zinder-projector", "--config", "/etc/zinder/config.toml", "healthcheck"\]$#    CMD curl -fsS http://localhost:9110/readyz || exit 1#' \
  "$repository_root/deploy/Dockerfile" > "$curl_healthcheck_dockerfile"
expect_rejected_because \
  "a Dockerfile whose projector image probes itself with curl" \
  'release admission rejected: every per-service release image must build on' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --dockerfile "$curl_healthcheck_dockerfile"

configless_healthcheck_dockerfile="$temporary_directory/configless-healthcheck.Dockerfile"
sed 's#^    CMD \["zinder-query", "--config", "/etc/zinder/config.toml", "healthcheck"\]$#    CMD ["zinder-query", "healthcheck"]#' \
  "$repository_root/deploy/Dockerfile" > "$configless_healthcheck_dockerfile"
expect_rejected_because \
  "a Dockerfile whose query healthcheck ignores the image config file" \
  'release admission rejected: every per-service release image must build on' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --dockerfile "$configless_healthcheck_dockerfile"

listener_env_dockerfile="$temporary_directory/listener-env.Dockerfile"
awk '
  { print }
  $0 == "COPY deploy/config/images/query.toml /etc/zinder/config.toml" {
    print "ENV ZINDER_OPS__LISTEN_ADDR=[::]:9106"
  }
' "$repository_root/deploy/Dockerfile" > "$listener_env_dockerfile"
expect_rejected_because \
  "a Dockerfile whose query image sets a listener through the environment" \
  'release admission rejected: every per-service release image must copy its' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --dockerfile "$listener_env_dockerfile"

public_bind_env_dockerfile="$temporary_directory/public-bind-env.Dockerfile"
sed 's#^USER 10001:10001$#ENV ZINDER_SECURITY__ALLOW_PUBLIC_BIND=true\nUSER 10001:10001#' \
  "$repository_root/deploy/Dockerfile" > "$public_bind_env_dockerfile"
expect_rejected_because \
  "a Dockerfile whose service base allows public binds through the environment" \
  'release admission rejected: every per-service release image must copy its' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --dockerfile "$public_bind_env_dockerfile"

unconfigured_cmd_dockerfile="$temporary_directory/unconfigured-cmd.Dockerfile"
awk '
  $0 == "ENTRYPOINT [\"zinder-explorer\"]" { explorer = 1 }
  explorer && $0 == "CMD [\"--config\", \"/etc/zinder/config.toml\"]" { explorer = 0; next }
  { print }
' "$repository_root/deploy/Dockerfile" > "$unconfigured_cmd_dockerfile"
expect_rejected_because \
  "a Dockerfile whose explorer image does not pass its default config" \
  'release admission rejected: every per-service release image must copy its' \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --dockerfile "$unconfigured_cmd_dockerfile"

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
# The literal workflow expressions are the credential contract being mutated.
# shellcheck disable=SC2016
sed 's/${{ steps.crates-io-auth.outputs.token }}/${{ secrets.CARGO_REGISTRY_TOKEN }}/' \
  "$repository_root/.github/workflows/release.yml" \
  > "$static_crate_token_workflow"
expect_rejected \
  "a release workflow with a static crates.io token fallback" \
  --release-workflow "$static_crate_token_workflow"

leaked_oidc_token_workflow="$temporary_directory/leaked-oidc-token-release.yml"
# The literal workflow expression is the credential contract being duplicated.
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
# The literal shell variables are the unsafe fixture replacement targets.
# shellcheck disable=SC2016
sed 's/${SMOKE_BUILD_GIT_COMMIT}/${GITHUB_SHA}/' \
  "$repository_root/.github/workflows/build-images.yml" \
  > "$volatile_provenance_build_workflow"
expect_rejected \
  "a pull-request image workflow invalidating Cargo caches with each commit" \
  --build-images-workflow "$volatile_provenance_build_workflow"

helpless_build_workflow="$temporary_directory/helpless-build-images.yml"
# The literal shell variable is the unsafe fixture removal target.
# shellcheck disable=SC2016
sed '/docker run --rm --entrypoint="\$image_name"/d' \
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
# The literal shell variable is the unsafe fixture removal target.
# shellcheck disable=SC2016
sed '/--platform "\$PLATFORM_REF"/d' \
  "$repository_root/.github/workflows/build-images.yml" \
  > "$platformless_build_workflow"
expect_rejected \
  "a pull-request image workflow that ignores its target platform" \
  --build-images-workflow "$platformless_build_workflow"

queryless_prometheus="$temporary_directory/queryless-prometheus.yml"
sed '/targets: \["zinder-ingest:9106"\]/d' \
  "$repository_root/deploy/observability/prometheus.yml" \
  > "$queryless_prometheus"
expect_rejected \
  "a deploy Prometheus config omitting the native query ops target" \
  --prometheus-config "$queryless_prometheus"

mislabeled_query_prometheus="$temporary_directory/mislabeled-query-prometheus.yml"
sed '/job_name: "zinder-query"/,/service: "zinder-query"/s/service: "zinder-query"/service: "zinder-compat-lightwalletd"/' \
  "$repository_root/deploy/observability/prometheus.yml" \
  > "$mislabeled_query_prometheus"
expect_rejected \
  "a deploy Prometheus config mislabeling the native query target" \
  --prometheus-config "$mislabeled_query_prometheus"

single_volume_images_catalog="$repository_root/deploy/release-single-volume-images.json"
while IFS= read -r omitted_image; do
  omitted_single_volume_catalog="$temporary_directory/single-volume-without-${omitted_image}.json"
  jq --arg omitted_image "$omitted_image" \
    'map(select(. != $omitted_image))' \
    "$single_volume_images_catalog" > "$omitted_single_volume_catalog"
  expect_rejected \
    "a single-volume image catalog omitting $omitted_image" \
    --release-workflow "$repository_root/.github/workflows/release.yml" \
    --single-volume-images-catalog "$omitted_single_volume_catalog"
done < <(jq -r '.[]' "$single_volume_images_catalog")

explorer_single_volume_catalog="$temporary_directory/single-volume-with-explorer.json"
jq '. + ["zinder-explorer"]' \
  "$single_volume_images_catalog" > "$explorer_single_volume_catalog"
expect_rejected \
  "a single-volume image catalog containing the optional explorer" \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --single-volume-images-catalog "$explorer_single_volume_catalog"

newline_single_volume_catalog="$temporary_directory/single-volume-with-newline.json"
printf '%s\n' '["zinder-canonical-runtime\nzinder-wallet-serving"]' \
  > "$newline_single_volume_catalog"
expect_rejected \
  "a single-volume image catalog with one newline-joined image name" \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --single-volume-images-catalog "$newline_single_volume_catalog"

runtime_only_release_workflow="$temporary_directory/runtime-only-release.yml"
# The literal shell variables are the fixture replacement targets.
# shellcheck disable=SC2016
sed 's/jq -c -s '"'"'add'"'"' "\$RELEASE_IMAGES_FILE" "\$RELEASE_SINGLE_VOLUME_IMAGES_FILE"/jq -c . "$RELEASE_IMAGES_FILE"/' \
  "$repository_root/.github/workflows/release.yml" \
  > "$runtime_only_release_workflow"
expect_rejected \
  "a release workflow that does not publish the single-volume images" \
  --release-workflow "$runtime_only_release_workflow"

single_volumeless_build_workflow="$temporary_directory/single-volumeless-build-images.yml"
sed '/release-single-volume-images.json)"$/d' \
  "$repository_root/.github/workflows/build-images.yml" \
  > "$single_volumeless_build_workflow"
expect_rejected \
  "a pull-request image workflow that skips the single-volume images" \
  --build-images-workflow "$single_volumeless_build_workflow"

echo "deployment admission tests passed"
