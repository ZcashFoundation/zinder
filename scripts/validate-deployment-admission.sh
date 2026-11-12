#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage:
  validate-deployment-admission.sh --deployment-class CLASS --target TARGET
  validate-deployment-admission.sh --release-workflow PATH [--release-images-catalog PATH] [--single-volume-images-catalog PATH]
  validate-deployment-admission.sh --build-images-workflow PATH [--release-images-catalog PATH] [--single-volume-images-catalog PATH]
  validate-deployment-admission.sh --prometheus-config PATH
  validate-deployment-admission.sh --compose-contract RESOLVED_COMPOSE_JSON

CLASS is one of production, canary, or diagnostic.
EOF
  exit 2
}

repository_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
deployment_class=""
target=""
release_workflow=""
build_images_workflow=""
release_images_catalog="$repository_root/deploy/release-images.json"
single_volume_images_catalog="$repository_root/deploy/release-single-volume-images.json"
release_images_catalog_explicit=false
prometheus_config=""
compose_contract=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --deployment-class)
      [[ $# -ge 2 ]] || usage
      deployment_class="$2"
      shift 2
      ;;
    --target)
      [[ $# -ge 2 ]] || usage
      target="$2"
      shift 2
      ;;
    --release-workflow)
      [[ $# -ge 2 ]] || usage
      release_workflow="$2"
      shift 2
      ;;
    --build-images-workflow)
      [[ $# -ge 2 ]] || usage
      build_images_workflow="$2"
      shift 2
      ;;
    --release-images-catalog)
      [[ $# -ge 2 ]] || usage
      release_images_catalog="$2"
      release_images_catalog_explicit=true
      shift 2
      ;;
    --single-volume-images-catalog)
      [[ $# -ge 2 ]] || usage
      single_volume_images_catalog="$2"
      release_images_catalog_explicit=true
      shift 2
      ;;
    --prometheus-config)
      [[ $# -ge 2 ]] || usage
      prometheus_config="$2"
      shift 2
      ;;
    --compose-contract)
      [[ $# -ge 2 ]] || usage
      compose_contract="$2"
      shift 2
      ;;
    *)
      usage
      ;;
  esac
done

validate_release_images_catalog() {
  [[ -r "$release_images_catalog" ]] || {
    echo "release admission rejected: cannot read release image catalog $release_images_catalog" >&2
    exit 1
  }

  jq -e '
    type == "array"
    and length == 4
    and all(.[]; type == "string" and test("^zinder-[a-z0-9-]+$"))
    and ([.[]] | unique | length) == length
  ' "$release_images_catalog" >/dev/null || {
    cat >&2 <<'EOF'
release admission rejected: the release image catalog must be a JSON array of
four unique, safely named Zinder runtime images.
EOF
    exit 1
  }

  required_release_images="$({
    printf '%s\n' zinder-ingest zinder-projector zinder-query zinder-compat-lightwalletd
  } | sort)"
  configured_release_images="$(jq -r '.[]' "$release_images_catalog" | sort)"
  if [[ "$configured_release_images" != "$required_release_images" ]]; then
    cat >&2 <<'EOF'
release admission rejected: the release image catalog must contain exactly
ingest, projector, native query, and lightwalletd compatibility images.
EOF
    exit 1
  fi
}

# Single-volume host images are published beside the runtime catalog but are
# not binaries and not the certified complete topology, so they stay in their
# own catalog and every name must be a build target in deploy/Dockerfile.
validate_single_volume_images_catalog() {
  [[ -r "$single_volume_images_catalog" ]] || {
    echo "release admission rejected: cannot read single-volume image catalog $single_volume_images_catalog" >&2
    exit 1
  }

  if ! jq -e 'type == "array" and (sort == ["zinder-canonical-runtime", "zinder-wallet-serving"])' \
    "$single_volume_images_catalog" > /dev/null 2>&1; then
    cat >&2 <<'EOF'
release admission rejected: the single-volume image catalog must contain
exactly the canonical-runtime canary and the wallet-serving image.
EOF
    exit 1
  fi

  local image_name
  while IFS= read -r image_name; do
    grep -Eq "^FROM [a-z0-9-]+ AS ${image_name}\$" "$repository_root/deploy/Dockerfile" || {
      echo "release admission rejected: deploy/Dockerfile has no ${image_name} target" >&2
      exit 1
    }
  done < <(jq -r '.[]' "$single_volume_images_catalog")
}

if [[ -n "$compose_contract" ]]; then
  [[ -z "$deployment_class" && -z "$target" && -z "$release_workflow" && -z "$build_images_workflow" && "$release_images_catalog_explicit" = false && -z "$prometheus_config" ]] || usage
  [[ -r "$compose_contract" ]] || {
    echo "release admission rejected: cannot read resolved Compose contract $compose_contract" >&2
    exit 1
  }

  jq -e '
    .services as $services
    |
    def exact_healthcheck($url; $start_period):
      .test == ["CMD", "curl", "-fsS", $url]
      and .interval == "30s"
      and .timeout == "5s"
      and .start_period == $start_period
      and .retries == 3;
    def volume_mounts_at($service; $target):
      [ $services[$service].volumes[]? | select(.type == "volume" and .target == $target) ];
    def resolved_volume_name($source):
      (.volumes[$source].name // $source);
    def exact_read_only_bind($service; $target; $source_suffix):
      ([ $services[$service].volumes[]?
        | select(
            .type == "bind"
            and .target == $target
            and .read_only == true
            and (.source | endswith($source_suffix))
          )
      ] | length) == 1;
    def has_checkpoint_secret_source($service):
      [ $services[$service].volumes[]?
        | select(.type == "bind" and (.source | endswith("/checkpoint.token")))
      ] | length > 0;
    volume_mounts_at("state-init"; "/var/lib/zinder") as $state_data
    | volume_mounts_at("state-init"; "/var/lib/zinder/checkpoints") as $state_checkpoint
    | volume_mounts_at("zinder-ingest"; "/var/lib/zinder/checkpoints") as $ingest_checkpoint
    | volume_mounts_at("zinder-projector"; "/var/lib/zinder/checkpoints") as $projector_checkpoint
    | volume_mounts_at("zinder-query"; "/var/lib/zinder/checkpoints") as $query_checkpoint
    | volume_mounts_at("zinder-compat-lightwalletd"; "/var/lib/zinder/checkpoints") as $compat_checkpoint
    | ($services | has("state-init"))
    and ($services | has("zinder-ingest"))
    and ($services | has("zinder-projector"))
    and ($services | has("zinder-query"))
    and ($services | has("zinder-compat-lightwalletd"))
    and ($services["state-init"].user == "0:0")
    and ($services["state-init"].network_mode == "none")
    and ($services["state-init"].restart == "no")
    and ($services["state-init"].command == [
      "sh",
      "-c",
      "mkdir -p /var/lib/zinder/checkpoints /var/lib/zinder/projector/canonical-secondary /var/lib/zinder/query/canonical-secondary /var/lib/zinder/query/wallet-secondary /var/lib/zinder/compat/canonical-secondary /var/lib/zinder/compat/wallet-secondary /var/lib/zinder/explorer/secondary && chown -R 1000:1000 /var/lib/zinder"
    ])
    and ($state_data | length == 1)
    and ($state_checkpoint | length == 1)
    and ($ingest_checkpoint | length == 1)
    and ($projector_checkpoint | length == 1)
    and ($query_checkpoint | length == 0)
    and ($compat_checkpoint | length == 0)
    and ($state_data[0].source != $state_checkpoint[0].source)
    and (resolved_volume_name($state_data[0].source) != resolved_volume_name($state_checkpoint[0].source))
    and ($state_checkpoint[0].source == $ingest_checkpoint[0].source)
    and ($state_checkpoint[0].source == $projector_checkpoint[0].source)
    and ($services["zinder-ingest"].depends_on["state-init"].condition == "service_completed_successfully")
    and ($services["zinder-projector"].network_mode == "service:zinder-ingest")
    and ($services["zinder-query"].network_mode == "service:zinder-ingest")
    and ($services["zinder-compat-lightwalletd"].network_mode == "service:zinder-ingest")
    and (($services["zinder-ingest"].ports // []) | length == 6)
    and (($services["zinder-ingest"].ports // [])
      | all(.host_ip == "127.0.0.1" and .protocol == "tcp" and .mode == "ingress"))
    and (($services["zinder-ingest"].ports | map(.published) | unique | length) == 6)
    and (($services["zinder-ingest"].ports | map(.target) | sort) == [9067, 9102, 9105, 9106, 9107, 9110])
    and ([ $services[] | (.ports // [])[] ] | all(.host_ip == "127.0.0.1"))
    and ([ $services[] | (.ports // [])[] | .target ] | index(9100) == null)
    and (($services["zinder-ingest"].volumes // [])
      | any(.target == "/var/run/zinder-checkpoint/checkpoint.token" and .read_only == true))
    and (($services["zinder-projector"].volumes // [])
      | any(.target == "/var/run/zinder-checkpoint/checkpoint.token" and .read_only == true))
    and (($services["zinder-query"].volumes // [])
      | all(.target != "/var/run/zinder-checkpoint/checkpoint.token"))
    and (($services["zinder-compat-lightwalletd"].volumes // [])
      | all(.target != "/var/run/zinder-checkpoint/checkpoint.token"))
    and (has_checkpoint_secret_source("zinder-query") | not)
    and (has_checkpoint_secret_source("zinder-compat-lightwalletd") | not)
    and exact_read_only_bind("zinder-query"; "/etc/zinder/config.toml"; "/deploy/config/query.toml")
    and exact_read_only_bind("zinder-compat-lightwalletd"; "/etc/zinder/config.toml"; "/deploy/config/compat-lightwalletd.toml")
    and exact_read_only_bind("zinder-query"; "/var/run/zinder-control/ingest.token"; "/ingest.token")
    and exact_read_only_bind("zinder-compat-lightwalletd"; "/var/run/zinder-control/ingest.token"; "/ingest.token")
    and ($services["zinder-ingest"].healthcheck | exact_healthcheck("http://localhost:9105/readyz"; "3h0m0s"))
    and ($services["zinder-projector"].healthcheck | exact_healthcheck("http://localhost:9110/readyz"; "2h0m0s"))
    and ($services["zinder-query"].healthcheck | exact_healthcheck("http://localhost:9106/readyz"; "5m0s"))
    and ($services["zinder-compat-lightwalletd"].healthcheck | exact_healthcheck("http://localhost:9107/readyz"; "5m0s"))
    and ($services["zinder-projector"].depends_on["zinder-ingest"].condition == "service_healthy")
    and ($services["zinder-compat-lightwalletd"].depends_on["zinder-ingest"].condition == "service_healthy")
    and ($services["zinder-compat-lightwalletd"].depends_on["zinder-projector"].condition == "service_healthy")
    and ($services["zinder-query"].depends_on["zinder-ingest"].condition == "service_healthy")
    and ($services["zinder-query"].depends_on["zinder-projector"].condition == "service_healthy")
    and ($services["zinder-ingest"].environment.ZINDER_OPS__LISTEN_ADDR == "[::]:9105")
    and ($services["zinder-ingest"].environment.ZINDER_SECURITY__ALLOW_PUBLIC_BIND == "true")
    and ($services["zinder-projector"].environment.ZINDER_OPS__LISTEN_ADDR == "[::]:9110")
    and ($services["zinder-projector"].environment.ZINDER_SECURITY__ALLOW_PUBLIC_BIND == "true")
    and ($services["zinder-query"].environment.ZINDER_QUERY__LISTEN_ADDR == "[::]:9102")
    and ($services["zinder-query"].environment.ZINDER_OPS__LISTEN_ADDR == "[::]:9106")
    and ($services["zinder-query"].environment.ZINDER_SECURITY__ALLOW_PUBLIC_BIND == "true")
    and ($services["zinder-compat-lightwalletd"].environment.ZINDER_COMPAT__LISTEN_ADDR == "[::]:9067")
    and ($services["zinder-compat-lightwalletd"].environment.ZINDER_OPS__LISTEN_ADDR == "[::]:9107")
    and ($services["zinder-compat-lightwalletd"].environment.ZINDER_SECURITY__ALLOW_PUBLIC_BIND == "true")
  ' "$compose_contract" >/dev/null || {
    cat >&2 <<'EOF'
release admission rejected: resolved Compose contract does not preserve the
root-owned data and isolated checkpoint-volume initialization, four-runtime
shared namespace, exact readiness probes, explicit container listener opt-ins,
loopback-only host publications, private control port, distinct native and
compatibility readers, or the ingest/projector-only checkpoint capability and staging mounts.
EOF
    exit 1
  }

  toml_section_value() {
    local config_path="$1"
    local section="$2"
    local key="$3"
    awk -v heading="[$section]" -v key="$key" '
      $0 == heading { in_section = 1; next }
      /^\[/ { in_section = 0 }
      in_section && $1 == key {
        value = $0
        sub(/^[^=]*=[[:space:]]*"/, "", value)
        sub(/".*$/, "", value)
        print value
        exit
      }
    ' "$config_path"
  }

  query_config_source="$(jq -r '
    .services["zinder-query"].volumes[]
    | select(.target == "/etc/zinder/config.toml")
    | .source
  ' "$compose_contract")"
  compat_config_source="$(jq -r '
    .services["zinder-compat-lightwalletd"].volumes[]
    | select(.target == "/etc/zinder/config.toml")
    | .source
  ' "$compose_contract")"
  reader_secondary_paths=(
    "$(toml_section_value "$query_config_source" storage secondary_path)"
    "$(toml_section_value "$query_config_source" wallet secondary_path)"
    "$(toml_section_value "$compat_config_source" storage secondary_path)"
    "$(toml_section_value "$compat_config_source" wallet secondary_path)"
  )
  nonempty_reader_secondary_path_count="$(
    printf '%s\n' "${reader_secondary_paths[@]}" | sed '/^$/d' | wc -l
  )"
  unique_reader_secondary_path_count="$(
    printf '%s\n' "${reader_secondary_paths[@]}" | sed '/^$/d' | sort -u | wc -l
  )"
  if [[ "$nonempty_reader_secondary_path_count" -ne 4 \
    || "$unique_reader_secondary_path_count" -ne 4 ]]; then
    cat >&2 <<'EOF'
release admission rejected: native query and compatibility must configure four
distinct canonical and wallet secondary roots.
EOF
    exit 1
  fi
  normalized_reader_secondary_paths=()
  for reader_secondary_path in "${reader_secondary_paths[@]}"; do
    normalized_reader_secondary_paths+=("$(realpath -m -- "$reader_secondary_path")")
  done
  for ((left_index = 0; left_index < ${#normalized_reader_secondary_paths[@]}; left_index++)); do
    for ((right_index = left_index + 1; right_index < ${#normalized_reader_secondary_paths[@]}; right_index++)); do
      left_path="${normalized_reader_secondary_paths[$left_index]}"
      right_path="${normalized_reader_secondary_paths[$right_index]}"
      if [[ "$left_path" == "$right_path" \
        || "$left_path" == "$right_path"/* \
        || "$right_path" == "$left_path"/* ]]; then
        cat >&2 <<'EOF'
release admission rejected: native query and compatibility secondary roots
must be path-disjoint; no reader root may equal or contain another.
EOF
        exit 1
      fi
    done
  done
  exit 0
fi

if [[ -n "$build_images_workflow" ]]; then
  [[ -z "$deployment_class" && -z "$target" && -z "$release_workflow" && -z "$prometheus_config" && -z "$compose_contract" ]] || usage
  [[ -r "$build_images_workflow" ]] || {
    echo "release admission rejected: cannot read build image workflow $build_images_workflow" >&2
    exit 1
  }

  validate_release_images_catalog
  validate_single_volume_images_catalog
  required_build_platforms="$({
    printf '%s\n' linux/amd64 linux/arm64
  } | sort)"
  configured_build_platforms="$(
    sed -n 's/^[[:space:]]*ref: \(linux\/[a-z0-9]*\)$/\1/p' \
      "$build_images_workflow" | sort -u
  )"
  # These literals are workflow expressions and shell variables being inspected.
  # shellcheck disable=SC2016
  if [[ "$configured_build_platforms" != "$required_build_platforms" ]] \
    || ! grep -Fq 'runner: ubuntu-24.04-arm' "$build_images_workflow" \
    || ! grep -Fq 'needs: verify' "$build_images_workflow" \
    || ! grep -Fq 'release_images: ${{ steps.images.outputs.release_images }}' "$build_images_workflow" \
    || ! grep -Fq 'RELEASE_IMAGES_JSON: ${{ needs.verify.outputs.release_images }}' "$build_images_workflow" \
    || ! grep -Fq "jq -c '.' deploy/release-images.json" "$build_images_workflow" \
    || ! grep -Fq "jq -r '.[]' <<< \"\$RELEASE_IMAGES_JSON\"" "$build_images_workflow" \
    || ! grep -Fq -- '--platform "$PLATFORM_REF"' "$build_images_workflow" \
    || ! grep -Eq 'SMOKE_BUILD_GIT_COMMIT: [0-9a-f]{40}$' "$build_images_workflow" \
    || ! grep -Fq -- '--build-arg "ZINDER_BUILD_GIT_COMMIT=${SMOKE_BUILD_GIT_COMMIT}"' \
      "$build_images_workflow" \
    || ! grep -Fq 'docker run --rm --entrypoint="$image_name" "${image_name}:${PR_TAG}" --help' \
      "$build_images_workflow" \
    || ! grep -Fq "jq -r '.[]' deploy/release-single-volume-images.json" "$build_images_workflow" \
    || ! grep -Fq 'docker run --rm --entrypoint=zinder-ingest "${image_name}:${PR_TAG}" --help' \
      "$build_images_workflow"; then
    cat >&2 <<'EOF'
release admission rejected: the pull-request image workflow must build and run
the --help smoke for the validated release and single-volume image catalogs on
native amd64 and arm64 runners.
EOF
    exit 1
  fi
  exit 0
fi

if [[ -n "$prometheus_config" ]]; then
  [[ -z "$deployment_class" && -z "$target" && -z "$release_workflow" && -z "$build_images_workflow" && "$release_images_catalog_explicit" = false && -z "$compose_contract" ]] || usage
  [[ -r "$prometheus_config" ]] || {
    echo "release admission rejected: cannot read Prometheus config $prometheus_config" >&2
    exit 1
  }

  query_job_count="$(grep -Fc 'job_name: "zinder-query"' "$prometheus_config")"
  query_job="$({
    awk '
      /job_name: "zinder-query"/ { in_query = 1 }
      in_query && /job_name:/ && !/job_name: "zinder-query"/ { exit }
      in_query { print }
    ' "$prometheus_config"
  })"
  if [[ "$query_job_count" -ne 1 ]] \
    || ! grep -Fq 'targets: ["zinder-ingest:9106"]' <<< "$query_job" \
    || ! grep -Fq 'service: "zinder-query"' <<< "$query_job"; then
    cat >&2 <<'EOF'
release admission rejected: deploy Prometheus must scrape the native query ops
endpoint at zinder-ingest:9106 and label the target as zinder-query.
EOF
    exit 1
  fi
  exit 0
fi

if [[ -n "$release_workflow" ]]; then
  [[ -z "$deployment_class" && -z "$target" && -z "$build_images_workflow" && -z "$prometheus_config" && -z "$compose_contract" ]] || usage
  [[ -r "$release_workflow" ]] || {
    echo "release admission rejected: cannot read release workflow $release_workflow" >&2
    exit 1
  }
  validate_release_images_catalog
  validate_single_volume_images_catalog

  if grep -Fq 'workflow_dispatch:' "$release_workflow"; then
    cat >&2 <<'EOF'
release admission rejected: the publishing workflow must not expose a manual
dispatch path. Use the pull-request image workflow for build-only smoke tests.
EOF
    exit 1
  fi

  trigger_block="$(sed -n '/^on:/,/^permissions:/p' "$release_workflow")"
  if ! grep -Fq '  push:' <<< "$trigger_block" \
    || ! grep -Fq '    tags:' <<< "$trigger_block" \
    || ! grep -Fq '      - "v*"' <<< "$trigger_block" \
    || grep -Eq 'pull_request:|schedule:|workflow_call:|branches:' <<< "$trigger_block"; then
    cat >&2 <<'EOF'
release admission rejected: the publishing workflow must run only for pushed
release tags matching v*.
EOF
    exit 1
  fi

  if ! grep -Fq '  group: release-publication' "$release_workflow" \
    || ! grep -Fq '  cancel-in-progress: false' "$release_workflow"; then
    cat >&2 <<'EOF'
release admission rejected: crate and image publication must share the global
release-publication concurrency group without cancellation.
EOF
    exit 1
  fi

  release_before_stable_promotion="$(sed '/^  promote-latest:/,$d' "$release_workflow")"
  if grep -Fq ':latest' <<< "$release_before_stable_promotion"; then
    cat >&2 <<'EOF'
release admission rejected: latest may only move in the final stable promotion
job after exact image manifests and the GitHub Release have succeeded.
EOF
    exit 1
  fi

  cargo_release_policy="$repository_root/.github/cargo-release.yml"
  if [[ ! -r "$cargo_release_policy" ]] \
    || [[ "$(tr -d '\r' < "$cargo_release_policy")" != 'tagTemplate: "{name}-v{version}"' ]]; then
    cat >&2 <<'EOF'
release admission rejected: Cargo Release policy must use one immutable,
package-specific tag identity while Zinder owns the product GitHub Release.
EOF
    exit 1
  fi

  if ! grep -Fq 'base_commit: ${{ steps.identity.outputs.base_commit }}' \
    "$release_workflow" \
    || ! grep -Fq 'git describe \' "$release_workflow" \
    || ! grep -Fq -- '--first-parent' "$release_workflow" \
    || ! grep -Fq -- "--match 'v[0-9]*'" "$release_workflow" \
    || ! grep -Fq "semver_pattern='^(0|[1-9][0-9]*)" "$release_workflow" \
    || ! grep -Fq 'previous_commit="$(git rev-parse "${previous_tag}^{commit}")"' \
      "$release_workflow"; then
    cat >&2 <<'EOF'
release admission rejected: the release identity job must derive the immutable
Cargo Release base from the preceding reachable product tag.
EOF
    exit 1
  fi

  sdk_package_job="$(
    awk '
      $0 == "  sdk-packages:" { in_job = 1; next }
      in_job && /^  [a-zA-Z0-9_-]+:/ { exit }
      in_job { print }
    ' "$release_workflow"
  )"
  if [[ -z "$sdk_package_job" ]] \
    || ! grep -Fq 'needs: validate' <<< "$sdk_package_job" \
    || ! grep -Fq 'fetch-depth: 0' <<< "$sdk_package_job" \
    || ! grep -Fq 'persist-credentials: false' <<< "$sdk_package_job" \
    || ! grep -Fq 'toolchain: 1.95.0' <<< "$sdk_package_job" \
    || ! grep -Fq 'scripts/verify-sdk-packages.sh' <<< "$sdk_package_job" \
    || ! grep -Fq 'uses: ZcashFoundation/cargo-release@34a37595755444456ce0e2d2b1258d9a29c14fac' \
      <<< "$sdk_package_job" \
    || ! grep -Fq 'phase: check' <<< "$sdk_package_job" \
    || ! grep -Fq 'base-sha: ${{ needs.validate.outputs.base_commit }}' \
      <<< "$sdk_package_job" \
    || ! grep -Fq 'target-sha: ${{ needs.validate.outputs.commit }}' \
      <<< "$sdk_package_job" \
    || ! grep -Fq 'github-token: ${{ github.token }}' <<< "$sdk_package_job" \
    || [[ "$(grep -Fc 'PROTOC: /does/not/exist' <<< "$sdk_package_job")" -ne 2 ]] \
    || grep -Fq 'id-token: write' <<< "$sdk_package_job" \
    || grep -Fq 'CARGO_REGISTRY_TOKEN' <<< "$sdk_package_job" \
    || grep -Fq 'crates-io-auth-action' <<< "$sdk_package_job" \
    || grep -Fq 'actions/upload-artifact' <<< "$sdk_package_job"; then
    cat >&2 <<'EOF'
release admission rejected: the pre-auth SDK package job must use exact Rust
1.95, verify the hermetic package catalog, run the complete Cargo Release
readiness check, and receive no registry credential.
EOF
    exit 1
  fi

  crate_publication_job="$(
    awk '
      $0 == "  publish-sdk-crates:" { in_job = 1; next }
      in_job && /^  [a-zA-Z0-9_-]+:/ { exit }
      in_job { print }
    ' "$release_workflow"
  )"
  auth_line="$(grep -nF 'uses: rust-lang/crates-io-auth-action@c6f97d42243bad5fab37ca0427f495c86d5b1a18 # v1.0.5' <<< "$crate_publication_job" | cut -d: -f1)"
  publish_line="$(grep -nF 'uses: ZcashFoundation/cargo-release@34a37595755444456ce0e2d2b1258d9a29c14fac' <<< "$crate_publication_job" | cut -d: -f1)"
  consumer_line="$(grep -nF 'scripts/verify-published-sdk.sh "$SDK_VERSION"' <<< "$crate_publication_job" | cut -d: -f1)"
  registry_verifier="$repository_root/scripts/verify-published-sdk.sh"
  if [[ -z "$crate_publication_job" ]] \
    || ! grep -Eq '^[[:space:]]+- validate$' <<< "$crate_publication_job" \
    || ! grep -Eq '^[[:space:]]+- api-docs$' <<< "$crate_publication_job" \
    || ! grep -Eq '^[[:space:]]+- sdk-packages$' <<< "$crate_publication_job" \
    || ! grep -Eq '^[[:space:]]+- authorize-release$' <<< "$crate_publication_job" \
    || ! grep -Fq 'environment: crates-io' <<< "$crate_publication_job" \
    || ! grep -Fq 'contents: read' <<< "$crate_publication_job" \
    || ! grep -Fq 'id-token: write' <<< "$crate_publication_job" \
    || ! grep -Fq 'fetch-depth: 0' <<< "$crate_publication_job" \
    || ! grep -Fq 'persist-credentials: false' <<< "$crate_publication_job" \
    || ! grep -Fq 'ref: ${{ needs.validate.outputs.commit }}' <<< "$crate_publication_job" \
    || ! grep -Fq 'id: crates-io-auth' <<< "$crate_publication_job" \
    || [[ -z "$auth_line" || -z "$publish_line" ]] \
    || (( auth_line >= publish_line )) \
    || ! grep -Fq 'phase: publish' <<< "$crate_publication_job" \
    || ! grep -Fq 'base-sha: ${{ needs.validate.outputs.base_commit }}' \
      <<< "$crate_publication_job" \
    || ! grep -Fq 'target-sha: ${{ needs.validate.outputs.commit }}' \
      <<< "$crate_publication_job" \
    || ! grep -Fq 'github-token: ${{ github.token }}' <<< "$crate_publication_job" \
    || ! grep -Fq 'CARGO_REGISTRY_TOKEN: ${{ steps.crates-io-auth.outputs.token }}' \
      <<< "$crate_publication_job" \
    || ! grep -Fq 'PROTOC: /does/not/exist' <<< "$crate_publication_job" \
    || [[ -z "$consumer_line" ]] \
    || ! grep -Fq 'timeout-minutes: 30' <<< "$crate_publication_job" \
    || ! grep -Fq 'SDK_VERSION: ${{ needs.validate.outputs.version }}' \
      <<< "$crate_publication_job" \
    || (( publish_line >= consumer_line )) \
    || [[ ! -x "$registry_verifier" ]] \
    || ! grep -Fq 'CARGO_HOME="$consumer_directory/cargo-home"' \
      "$registry_verifier" \
    || ! grep -Fq 'cargo +1.95.0 check' "$registry_verifier" \
    || [[ "$(grep -Fc 'CARGO_REGISTRY_TOKEN:' <<< "$crate_publication_job")" -ne 1 ]] \
    || grep -Eq 'secrets\.(CARGO_REGISTRY_TOKEN|CRATES_IO_TOKEN)' <<< "$crate_publication_job" \
    || grep -Fq 'needs.validate.outputs.stable' <<< "$crate_publication_job" \
    || grep -Eq 'phase: (all|finalize)' <<< "$crate_publication_job" \
    || grep -Fq 'actions/download-artifact' <<< "$crate_publication_job"; then
    cat >&2 <<'EOF'
release admission rejected: SDK publication must follow release authorization
and the validated package and API jobs, run in the crates-io environment,
delegate resumable publication to the pinned Cargo Release action, expose the
OIDC token only to that publication step, and compile a fresh registry-only
consumer before image publication.
EOF
    exit 1
  fi

  if grep -Eq 'secrets\.(CARGO_REGISTRY_TOKEN|CRATES_IO_TOKEN)' "$release_workflow" \
    || [[ "$(grep -Fc 'CARGO_REGISTRY_TOKEN:' "$release_workflow")" -ne 1 ]]; then
    cat >&2 <<'EOF'
release admission rejected: the release workflow must not contain a static
crates.io token or expose the trusted-publisher token outside publication.
EOF
    exit 1
  fi

  publishing_jobs=(build merge assemble-and-sign prepare-release publish-release verify-release promote-latest)
  required_predecessors=(authorize-release publish-sdk-crates merge assemble-and-sign prepare-release publish-release verify-release)
  for index in "${!publishing_jobs[@]}"; do
    publishing_job="${publishing_jobs[$index]}"
    required_predecessor="${required_predecessors[$index]}"
    job_body="$(
      awk -v heading="  ${publishing_job}:" '
        $0 == heading { in_job = 1; next }
        in_job && /^  [a-zA-Z0-9_-]+:/ { exit }
        in_job { print }
      ' "$release_workflow"
    )"
    if [[ -z "$job_body" ]] \
      || ! grep -Eq "^[[:space:]]+- ${required_predecessor}$" <<< "$job_body"; then
      cat >&2 <<EOF
release admission rejected: publishing job $publishing_job must depend on
$required_predecessor so it cannot bypass release authorization.
EOF
      exit 1
    fi
  done

  stable_promotion_job="$(sed -n '/^  promote-latest:/,$p' "$release_workflow")"
  if ! grep -Fq "if: needs.validate.outputs.stable == 'true'" \
    <<< "$stable_promotion_job"; then
    cat >&2 <<'EOF'
release admission rejected: latest promotion must be restricted to a validated
stable SemVer tag.
EOF
    exit 1
  fi

  if grep -Fq 'zinder-single-container' "$release_workflow"; then
    cat >&2 <<'EOF'
release admission rejected: the image workflow would publish the mixed
zinder-single-container bundle, which is not a release image.
EOF
    exit 1
  fi

  if grep -Eq '"zinder-explorer:zinder-explorer"' "$release_workflow"; then
    cat >&2 <<'EOF'
release admission rejected: the release image set does not publish the optional
explorer runtime.
EOF
    exit 1
  fi

  # These literals are workflow expressions and shell variables being inspected.
  # shellcheck disable=SC2016
  if ! grep -Fq 'release_images: ${{ steps.images.outputs.release_images }}' \
    "$release_workflow" \
    || ! grep -Fq "jq -c -s 'add' \"\$RELEASE_IMAGES_FILE\" \"\$RELEASE_SINGLE_VOLUME_IMAGES_FILE\"" "$release_workflow" \
    || ! grep -Fq 'RELEASE_SINGLE_VOLUME_IMAGES_FILE: deploy/release-single-volume-images.json' "$release_workflow" \
    || ! grep -Fq 'RELEASE_IMAGES_JSON: ${{ needs.validate.outputs.release_images }}' \
      "$release_workflow" \
    || ! grep -Fq "jq -r '.[]' <<< \"\$RELEASE_IMAGES_JSON\"" "$release_workflow" \
    || ! grep -Fq 'image: ${{ fromJSON(needs.validate.outputs.release_images) }}' \
      "$release_workflow"; then
    cat >&2 <<'EOF'
release admission rejected: digest builds, manifest publication, and stable
promotion must consume the validated release image catalog output.
EOF
    exit 1
  fi

  dockerfile="$repository_root/deploy/Dockerfile"
  binary_export_stage="$(
    awk '
      $0 == "FROM scratch AS release-binaries" { in_stage = 1; next }
      in_stage && /^FROM / { exit }
      in_stage { print }
    ' "$dockerfile"
  )"
  exported_binary_catalog="$(
    sed -n 's#^COPY --from=zinder-binaries .* /bin/\([^ ]*\)$#\1#p' \
      <<< "$binary_export_stage" | sort
  )"
  if [[ "$exported_binary_catalog" != "$configured_release_images" ]] \
    || grep -Eq 'zinder-(bench|explorer|compat-cipherscan|proto-codegen)' <<< "$binary_export_stage" \
    || ! grep -Fq 'ARG RUST_VERSION=1.95.0' "$dockerfile" \
    || ! grep -Eq '^ARG RUST_IMAGE_DIGEST=sha256:[0-9a-f]{64}$' "$dockerfile" \
    || ! grep -Eq '^ARG DEBIAN_IMAGE_DIGEST=sha256:[0-9a-f]{64}$' "$dockerfile" \
    || ! grep -Fq 'ENV CARGO_INCREMENTAL=0' "$dockerfile" \
    || ! grep -Fq 'ARG RELEASE_CACHE_SCOPE=shared' "$dockerfile" \
    || ! grep -Fq '${RELEASE_CACHE_SCOPE}' "$dockerfile" \
    || ! grep -Fq -- '--remap-path-prefix=/workspace=/usr/src/zinder' "$dockerfile" \
    || ! grep -Fq -- '--remap-path-prefix=/usr/local/cargo=/cargo' "$dockerfile" \
    || ! grep -Fq 'ENV CARGO_PROFILE_RELEASE_STRIP=symbols' "$dockerfile" \
    || ! grep -Fq 'cargo install cargo-auditable --version 0.7.4 --locked' "$dockerfile" \
    || ! grep -Fq 'cargo auditable build --locked --release' "$dockerfile" \
    || ! grep -Fq 'libstdc++6' "$dockerfile"; then
    cat >&2 <<'EOF'
release admission rejected: the Docker release-binaries target must export
exactly the four runtime catalog binaries with the pinned reproducible GNU
build inputs and runtime libstdc++ dependency.
EOF
    exit 1
  fi

  build_job="$(
    awk '
      $0 == "  build:" { in_job = 1; next }
      in_job && /^  [a-zA-Z0-9_-]+:/ { exit }
      in_job { print }
    ' "$release_workflow"
  )"
  reproduction_job="$(
    awk '
      $0 == "  reproduce:" { in_job = 1; next }
      in_job && /^  [a-zA-Z0-9_-]+:/ { exit }
      in_job { print }
    ' "$release_workflow"
  )"
  binary_archive_job="$(
    awk '
      $0 == "  binary-archives:" { in_job = 1; next }
      in_job && /^  [a-zA-Z0-9_-]+:/ { exit }
      in_job { print }
    ' "$release_workflow"
  )"
  collected_binary_job="$(
    awk '
      $0 == "  collect-binary-assets:" { in_job = 1; next }
      in_job && /^  [a-zA-Z0-9_-]+:/ { exit }
      in_job { print }
    ' "$release_workflow"
  )"
  prepare_release_job="$(
    awk '
      $0 == "  prepare-release:" { in_job = 1; next }
      in_job && /^  [a-zA-Z0-9_-]+:/ { exit }
      in_job { print }
    ' "$release_workflow"
  )"
  publish_release_job="$(
    awk '
      $0 == "  publish-release:" { in_job = 1; next }
      in_job && /^  [a-zA-Z0-9_-]+:/ { exit }
      in_job { print }
    ' "$release_workflow"
  )"
  authorization_job="$(
    awk '
      $0 == "  authorize-release:" { in_job = 1; next }
      in_job && /^  [a-zA-Z0-9_-]+:/ { exit }
      in_job { print }
    ' "$release_workflow"
  )"
  assemble_release_job="$(
    awk '
      $0 == "  assemble-and-sign:" { in_job = 1; next }
      in_job && /^  [a-zA-Z0-9_-]+:/ { exit }
      in_job { print }
    ' "$release_workflow"
  )"
  merge_job="$(
    awk '
      $0 == "  merge:" { in_job = 1; next }
      in_job && /^  [a-zA-Z0-9_-]+:/ { exit }
      in_job { print }
    ' "$release_workflow"
  )"
  verify_release_job="$(
    awk '
      $0 == "  verify-release:" { in_job = 1; next }
      in_job && /^  [a-zA-Z0-9_-]+:/ { exit }
      in_job { print }
    ' "$release_workflow"
  )"
  promote_latest_job="$(
    awk '
      $0 == "  promote-latest:" { in_job = 1; next }
      in_job && /^  [a-zA-Z0-9_-]+:/ { exit }
      in_job { print }
    ' "$release_workflow"
  )"
  image_attestation_verification="$(
    awk '
      /scripts\/verify-release-image-attestations\.sh/ { in_call = 1 }
      in_call { print }
      in_call && /--commit / { exit }
    ' <<< "$merge_job"
  )"
  # These literals are workflow expressions and shell variables being inspected.
  # shellcheck disable=SC2016
  if [[ -z "$build_job" || -z "$reproduction_job" ]] \
    || [[ -z "$binary_archive_job" || -z "$collected_binary_job" ]] \
    || ! grep -Eq '^[[:space:]]+- authorize-release$' <<< "$reproduction_job" \
    || [[ "$(grep -Fc -- '--no-cache' <<< "$build_job")" -ne 1 ]] \
    || [[ "$(grep -Fc -- '--no-cache' <<< "$reproduction_job")" -ne 1 ]] \
    || [[ "$(grep -Fc -- '--target release-binaries' <<< "$build_job")" -ne 1 ]] \
    || ! grep -Fq -- '--target release-binaries' <<< "$reproduction_job" \
    || ! grep -Fq 'RELEASE_CACHE_SCOPE: release-${{ needs.validate.outputs.commit }}-${{ matrix.platform.rust }}-1' \
      <<< "$build_job" \
    || ! grep -Fq 'RELEASE_CACHE_SCOPE=release-${RELEASE_COMMIT}-${RUST_TARGET}-2' \
      <<< "$reproduction_job" \
    || [[ "$(grep -Fc -- '--build-arg "RELEASE_CACHE_SCOPE=${RELEASE_CACHE_SCOPE}"' <<< "$build_job")" -ne 2 ]] \
    || [[ "$(grep -Fc -- '--build-arg "SOURCE_DATE_EPOCH=${source_date_epoch}"' <<< "$build_job")" -ne 2 ]] \
    || [[ "$(grep -Fc -- '--build-arg "ZINDER_BUILD_GIT_COMMIT=${RELEASE_COMMIT}"' <<< "$build_job")" -ne 2 ]] \
    || ! grep -Fq 'cmp -- "${binary_root}/${binary_name}" "$image_binary"' <<< "$build_job" \
    || grep -Eq -- '--cache-(from|to)' "$release_workflow" \
    || grep -Fq 'dist/' <<< "$build_job" \
    || grep -Fq 'id-token: write' <<< "$build_job" \
    || grep -Fq 'packages: write' <<< "$reproduction_job" \
    || grep -Fq 'id-token: write' <<< "$reproduction_job" \
    || ! grep -Fq 'scripts/check-release-binary-archive.sh' <<< "$build_job" \
    || ! grep -Fq 'scripts/check-release-binary-archive.sh' <<< "$reproduction_job" \
    || ! grep -Fq 'x86_64-v3-unknown-linux-gnu' <<< "$build_job" \
    || ! grep -Fq 'aarch64-unknown-linux-gnu' <<< "$build_job" \
    || ! grep -Fq 'x86_64-v3-unknown-linux-gnu' <<< "$reproduction_job" \
    || ! grep -Fq 'aarch64-unknown-linux-gnu' <<< "$reproduction_job" \
    || ! grep -Fq 'scripts/generate-release-sbom.sh' <<< "$build_job" \
    || ! grep -Fq 'scripts/check-release-sbom.sh' <<< "$build_job" \
    || ! grep -Eq '^[[:space:]]+- build$' <<< "$binary_archive_job" \
    || ! grep -Eq '^[[:space:]]+- reproduce$' <<< "$binary_archive_job" \
    || ! grep -Fq 'diff -u dist/build/binaries.sha256 dist/reproduction/binaries.sha256' \
      <<< "$binary_archive_job" \
    || ! grep -Fq 'cmp -- "dist/build/${archive_name}" "dist/reproduction/${archive_name}"' \
      <<< "$binary_archive_job" \
    || [[ "$(grep -Fc 'uses: actions/attest@59d89421af93a897026c735860bf21b6eb4f7b26 # v4.1.0' <<< "$binary_archive_job")" -ne 2 ]] \
    || ! grep -Eq '^[[:space:]]+- build$' <<< "$merge_job" \
    || ! grep -Eq '^[[:space:]]+- binary-archives$' <<< "$collected_binary_job" \
    || ! grep -Fq 'spdx.json' <<< "$collected_binary_job" \
    || ! grep -Eq '^[[:space:]]+- collect-binary-assets$' <<< "$crate_publication_job" \
    || ! grep -Eq '^[[:space:]]+- collect-binary-assets$' <<< "$assemble_release_job" \
    || ! grep -Fq 'name: release-binary-assets' <<< "$assemble_release_job" \
    || ! grep -Eq '^[[:space:]]+- assemble-and-sign$' <<< "$prepare_release_job" \
    || ! grep -Fq 'name: Checkout validated commit' <<< "$prepare_release_job" \
    || ! grep -Fq 'uses: actions/checkout@d23441a48e516b6c34aea4fa41551a30e30af803 # v6' \
      <<< "$prepare_release_job" \
    || ! grep -Fq 'ref: ${{ needs.validate.outputs.commit }}' \
      <<< "$prepare_release_job" \
    || ! grep -Fq 'persist-credentials: false' <<< "$prepare_release_job" \
    || ! grep -Fq 'name: release-assets-final' <<< "$prepare_release_job"; then
    cat >&2 <<'EOF'
release admission rejected: release binaries must build in 2 independent
uncached jobs for both GNU platforms and compare identical, images must reuse
the archived binaries, exact SBOMs must be generated and attested before
crates.io authentication, and the archives must be assembled into the final
draft GitHub Release assets.
EOF
    exit 1
  fi

  workflow_before_authorization="$(sed '/^  authorize-release:/,$d' "$release_workflow")"
  api_docs_job="$(
    awk '
      $0 == "  api-docs:" { in_job = 1; next }
      in_job && /^  [a-zA-Z0-9_-]+:/ { exit }
      in_job { print }
    ' "$release_workflow"
  )"
  if [[ -z "$authorization_job" ]] \
    || ! grep -Fq 'environment: release' <<< "$authorization_job" \
    || [[ "$(grep -Ec '^    environment:' "$release_workflow")" -ne 2 ]] \
    || [[ "$(grep -Fc 'environment: release' "$release_workflow")" -ne 1 ]] \
    || ! grep -Eq '^[[:space:]]+- validate$' <<< "$authorization_job" \
    || ! grep -Eq '^[[:space:]]+- api-docs$' <<< "$authorization_job" \
    || ! grep -Eq '^[[:space:]]+- sdk-packages$' <<< "$authorization_job" \
    || grep -Fq 'id-token: write' <<< "$authorization_job" \
    || grep -Fq 'id-token: write' <<< "$api_docs_job" \
    || grep -Fq 'id-token: write' <<< "$workflow_before_authorization"; then
    cat >&2 <<'EOF'
release admission rejected: the protected release authorization must follow
pre-auth validation, be the only reviewed environment gate, and no earlier job
may receive an OIDC token.
EOF
    exit 1
  fi

  if [[ "$(grep -Fc 'uses: actions/attest@59d89421af93a897026c735860bf21b6eb4f7b26 # v4.1.0' "$release_workflow")" -ne 5 ]] \
    || [[ "$(grep -Fc 'create-storage-record: false' "$release_workflow")" -ne 5 ]] \
    || [[ "$(grep -Fc 'uses: anchore/sbom-action/download-syft@e22c389904149dbc22b58101806040fa8d37a610 # v0.24.0' "$release_workflow")" -ne 2 ]] \
    || [[ "$(grep -Fc 'uses: sigstore/cosign-installer@6f9f17788090df1f26f669e9d70d6ae9567deba6 # v4.1.2' "$release_workflow")" -ne 3 ]] \
    || [[ "$(grep -Fc 'scripts/install-release-gh.sh' "$release_workflow")" -ne 4 ]] \
    || grep -Fq 'artifact-metadata:' "$release_workflow" \
    || ! grep -Fq -- '--provenance=mode=max' "$release_workflow" \
    || ! grep -Fq -- '--sbom=true' "$release_workflow" \
    || ! grep -Fq 'scripts/check-release-image-evidence.sh' "$release_workflow" \
    || ! grep -Fq 'cosign sign --yes' <<< "$merge_job" \
    || ! grep -Fq 'scripts/verify-release-image-attestations.sh' <<< "$image_attestation_verification" \
    || ! grep -Fq -- '--image "$IMAGE"' <<< "$image_attestation_verification" \
    || ! grep -Fq -- '--root-digest "$ROOT_DIGEST"' <<< "$image_attestation_verification" \
    || ! grep -Fq -- '--amd64-digest "$AMD64_DIGEST"' <<< "$image_attestation_verification" \
    || ! grep -Fq -- '--arm64-digest "$ARM64_DIGEST"' <<< "$image_attestation_verification" \
    || ! grep -Fq -- '--repository "$GITHUB_REPOSITORY"' <<< "$image_attestation_verification" \
    || ! grep -Fq -- '--workflow release.yml' <<< "$image_attestation_verification" \
    || ! grep -Fq -- '--tag "$RELEASE_TAG"' <<< "$image_attestation_verification" \
    || ! grep -Fq -- '--commit "$BUILD_GIT_COMMIT"' <<< "$image_attestation_verification" \
    || ! grep -Fq 'SHA256SUMS.sigstore.json' <<< "$assemble_release_job" \
    || ! grep -Fq -- '--cert-identity "$certificate_identity"' \
      <<< "$assemble_release_job" \
    || ! grep -Fq -- '--deny-self-hosted-runners' <<< "$assemble_release_job" \
    || ! grep -Fq -- '--signer-digest "$BUILD_GIT_COMMIT"' <<< "$assemble_release_job" \
    || grep -Fq -- '--signer-workflow' <<< "$assemble_release_job"; then
    cat >&2 <<'EOF'
release admission rejected: release archives and images must preserve the
pinned attestation, SBOM, provenance, signature, and verifier policy.
EOF
    exit 1
  fi

  if ! grep -Fq 'GH_REPO: ${{ github.repository }}' <<< "$publish_release_job" \
    || ! grep -Fq '.isImmutable == true' <<< "$verify_release_job" \
    || ! grep -Fq 'gh release verify "$RELEASE_TAG"' <<< "$verify_release_job" \
    || ! grep -Fq 'gh release verify-asset "$RELEASE_TAG" "$asset"' <<< "$verify_release_job" \
    || ! grep -Fq 'cosign verify' <<< "$promote_latest_job" \
    || ! grep -Fq 'gh attestation verify' <<< "$promote_latest_job" \
    || ! grep -Fq -- '--cert-identity "$certificate_identity"' \
      <<< "$promote_latest_job" \
    || grep -Fq -- '--signer-workflow' <<< "$promote_latest_job" \
    || ! grep -Fq '[[ "$latest_digest" == "$exact_digest" ]]' <<< "$promote_latest_job"; then
    cat >&2 <<'EOF'
release admission rejected: immutable release verification and signed exact
digest re-verification must complete before stable latest tags move.
EOF
    exit 1
  fi

  exit 0
fi

[[ "$release_images_catalog_explicit" = false ]] || usage

case "$deployment_class" in
  production|canary|diagnostic) ;;
  *) usage ;;
esac

[[ -n "$target" ]] || usage

case "$deployment_class:$target" in
  canary:zinder-canonical-runtime|diagnostic:zinder-canonical-runtime)
    exit 0
    ;;
  production:zinder-single-container)
    cat >&2 <<'EOF'
release admission rejected: zinder-single-container combines canonical
ingest with reader ownership and omits zinder-compat-lightwalletd.
It is not a production candidate.
EOF
    exit 1
    ;;
  production:zinder-canonical-runtime)
    cat >&2 <<'EOF'
release admission rejected: zinder-canonical-runtime is an ingest-only
diagnostic canary. It does not provide the complete topology or a
public compatibility route.
EOF
    exit 1
    ;;
  production:*)
    cat >&2 <<EOF
release admission rejected: $target is not a certified complete version-1
production topology.
EOF
    exit 1
    ;;
  canary:*|diagnostic:*)
    cat >&2 <<EOF
release admission rejected: $target is not the explicitly admitted
zinder-canonical-runtime diagnostic/canary target.
EOF
    exit 1
    ;;
esac
