#!/usr/bin/env bash
set -euo pipefail

repository_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
validator="$repository_root/scripts/validate-deployment-admission.sh"
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

bash "$validator" \
  --deployment-class canary \
  --target zinder-canonical-runtime
bash "$validator" \
  --deployment-class diagnostic \
  --target zinder-canonical-runtime

expect_rejected \
  "the native-only wallet-serving target as production" \
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
grep -Fq 'group: release-publication' \
  "$repository_root/.github/workflows/release.yml" \
  || fail "the release workflow does not globally serialize publication"
bash "$validator" \
  --build-images-workflow "$repository_root/.github/workflows/build-images.yml" \
  --release-images-catalog "$release_images_catalog"
bash "$validator" \
  --prometheus-config "$repository_root/deploy/observability/prometheus.yml"

if grep -Eq 'zinder-[a-z-]+:latest' "$repository_root/deploy/docker-compose.yml"; then
  fail "the deployment Compose file defaults a Zinder runtime to latest"
fi

for z3_env in mainnet testnet; do
  grep -Fqx 'ZINDER_NODE__INDEXER_GRPC_ADDR=http://zebra:8155' \
    "$repository_root/deploy/.env.$z3_env" \
    || fail "the $z3_env lane does not use Zebra's internal indexer endpoint"
done
for z3_env in mainnet testnet regtest; do
  projector_owner="$(sed -n 's/^ZINDER_PROJECTOR_BUILD_OWNER_HEX=//p' \
    "$repository_root/deploy/.env.$z3_env")"
  [[ "$projector_owner" =~ ^[0-9a-f]{32}$ && "$projector_owner" =~ [a-f] ]] \
    || fail "the $z3_env projector owner is not a config-safe 32-character hex string"
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

resolved_explorer_compose="$temporary_directory/resolved-explorer-compose.json"
ZINDER_CONTROL_SECRETS_DIR="$control_secrets" \
docker compose \
  --env-file "$repository_root/deploy/.env.testnet" \
  -f "$repository_root/deploy/docker-compose.yml" \
  -f "$repository_root/deploy/docker-compose.explorer.yml" \
  config --format json > "$resolved_explorer_compose"
jq -e '
  .services["zinder-explorer"].build.target == "zinder-explorer"
  and .services["zinder-explorer"].command == ["--config", "/etc/zinder/config.toml"]
  and .services["zinder-explorer"].network_mode == "service:zinder-ingest"
  and .services["zinder-ingest"].ulimits.nofile.soft == 65536
  and .services["zinder-ingest"].ulimits.nofile.hard == 65536
  and .services["zinder-explorer"].ulimits.nofile.soft == 65536
  and .services["zinder-explorer"].ulimits.nofile.hard == 65536
  and .services["zinder-explorer"].depends_on["zinder-query"].condition == "service_healthy"
  and .services["zinder-explorer"].healthcheck.test == [
    "CMD",
    "curl",
    "-fsS",
    "http://localhost:9069/readyz"
  ]
  and .services["zinder-explorer"].environment.ZINDER_EXPLORER__LISTEN_ADDR == "[::]:9068"
  and .services["zinder-explorer"].environment.ZINDER_OPS__LISTEN_ADDR == "[::]:9069"
  and (.services["zinder-ingest"].networks.z3.aliases | index("zinder-testnet-explorer")) != null
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

missing_state_init_compose="$temporary_directory/missing-state-init-compose.json"
jq 'del(.services["state-init"]) | del(.services["zinder-ingest"].depends_on["state-init"])' \
  "$resolved_compose" > "$missing_state_init_compose"
expect_rejected \
  "a Compose contract without root-owned volume initialization" \
  --compose-contract "$missing_state_init_compose"

missing_checkpoint_root_compose="$temporary_directory/missing-checkpoint-root-compose.json"
jq '.services["state-init"].command = ["chown", "-R", "1000:1000", "/var/lib/zinder"]' \
  "$resolved_compose" > "$missing_checkpoint_root_compose"
expect_rejected \
  "a Compose contract that does not initialize the isolated checkpoint volume" \
  --compose-contract "$missing_checkpoint_root_compose"

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
jq '.services["zinder-ingest"].healthcheck.test[-1] = "http://localhost:9105/healthz"' \
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

explorer_release_images_catalog="$temporary_directory/catalog-with-explorer.json"
jq '. + ["zinder-explorer"]' \
  "$release_images_catalog" > "$explorer_release_images_catalog"
expect_rejected \
  "a release image catalog containing the optional explorer" \
  --release-workflow "$repository_root/.github/workflows/release.yml" \
  --release-images-catalog "$explorer_release_images_catalog"

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
sed 's/for build_number in 1 2/for build_number in 1/' \
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
  "a release workflow authenticating before binary assets succeed" \
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
  in_publication && /^  build:/ { in_publication = 0 }
  { print }
' "$repository_root/.github/workflows/release.yml" \
  > "$unprotected_release_workflow"
expect_rejected \
  "a release workflow that pushes digests before environment approval" \
  --release-workflow "$unprotected_release_workflow"

bypassed_authorization_workflow="$temporary_directory/bypassed-authorization-release.yml"
sed '/^[[:space:]]*- publish-sdk-crates$/d' \
  "$repository_root/.github/workflows/release.yml" \
  > "$bypassed_authorization_workflow"
expect_rejected \
  "a release workflow whose first registry writer bypasses approval" \
  --release-workflow "$bypassed_authorization_workflow"

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
sed 's#rust-lang/crates-io-auth-action@c6f97d42243bad5fab37ca0427f495c86d5b1a18 #rust-lang/crates-io-auth-action@main #' \
  "$repository_root/.github/workflows/release.yml" \
  > "$unpinned_crate_auth_workflow"
expect_rejected \
  "a release workflow with an unpinned crates.io authentication action" \
  --release-workflow "$unpinned_crate_auth_workflow"

unpinned_cargo_release_workflow="$temporary_directory/unpinned-cargo-release.yml"
sed 's#ZcashFoundation/cargo-release@34a37595755444456ce0e2d2b1258d9a29c14fac#ZcashFoundation/cargo-release@main#g' \
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
sed '/^[[:space:]]*- build$/d' \
  "$repository_root/.github/workflows/release.yml" \
  > "$bypassed_manifest_dependency_workflow"
expect_rejected \
  "a release workflow whose manifest publisher bypasses digest builds" \
  --release-workflow "$bypassed_manifest_dependency_workflow"

unprotected_authorization_workflow="$temporary_directory/unprotected-authorization-release.yml"
sed '/^  authorize-release:/,/^  binary-archives:/ {
  /^    environment: release$/d
}' \
  "$repository_root/.github/workflows/release.yml" \
  > "$unprotected_authorization_workflow"
unprotected_authorization_error="$temporary_directory/unprotected-authorization-error.txt"
if bash "$validator" \
  --release-workflow "$unprotected_authorization_workflow" \
  > /dev/null 2> "$unprotected_authorization_error"; then
  fail "a release workflow without protected pre-OIDC authorization was admitted"
fi
grep -Fq \
  'release admission rejected: the protected release authorization must follow' \
  "$unprotected_authorization_error" \
  || fail "the unprotected authorization workflow was rejected for an unrelated reason"

early_oidc_workflow="$temporary_directory/early-oidc-release.yml"
sed '/^  sdk-packages:/a\    permissions:\n      id-token: write' \
  "$repository_root/.github/workflows/release.yml" \
  > "$early_oidc_workflow"
expect_rejected \
  "a release workflow granting OIDC before authorization" \
  --release-workflow "$early_oidc_workflow"

unpinned_attestation_workflow="$temporary_directory/unpinned-attestation-release.yml"
sed 's#actions/attest@59d89421af93a897026c735860bf21b6eb4f7b26 #actions/attest@main #' \
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
