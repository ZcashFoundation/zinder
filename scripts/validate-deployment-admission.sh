#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage:
  validate-deployment-admission.sh --deployment-class CLASS --target TARGET
  validate-deployment-admission.sh --release-workflow PATH [--release-images-catalog PATH] [--single-volume-images-catalog PATH] [--dockerfile PATH] [--image-packager PATH]
  validate-deployment-admission.sh --build-images-workflow PATH [--release-images-catalog PATH] [--single-volume-images-catalog PATH] [--dockerfile PATH]
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
dockerfile="$repository_root/deploy/Dockerfile"
image_packager="$repository_root/scripts/package-release-images.sh"
release_inputs_explicit=false
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
      release_inputs_explicit=true
      shift 2
      ;;
    --single-volume-images-catalog)
      [[ $# -ge 2 ]] || usage
      single_volume_images_catalog="$2"
      release_inputs_explicit=true
      shift 2
      ;;
    --dockerfile)
      [[ $# -ge 2 ]] || usage
      dockerfile="$2"
      release_inputs_explicit=true
      shift 2
      ;;
    --image-packager)
      [[ $# -ge 2 ]] || usage
      image_packager="$2"
      release_inputs_explicit=true
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
    and length == 5
    and all(.[]; type == "string" and test("^zinder-[a-z0-9-]+$"))
    and ([.[]] | unique | length) == length
  ' "$release_images_catalog" >/dev/null || {
    cat >&2 <<'EOF'
release admission rejected: the release image catalog must be a JSON array of
five unique, safely named Zinder runtime images.
EOF
    exit 1
  }

  required_release_images="$({
    printf '%s\n' zinder-ingest zinder-projector zinder-query zinder-compat-lightwalletd zinder-explorer
  } | sort)"
  configured_release_images="$(jq -r '.[]' "$release_images_catalog" | sort)"
  if [[ "$configured_release_images" != "$required_release_images" ]]; then
    cat >&2 <<'EOF'
release admission rejected: the release image catalog must contain exactly
ingest, projector, native query, lightwalletd compatibility, and Explorer
images.
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

  if ! jq -e 'type == "array" and (sort == ["zinder-canonical-runtime", "zinder-wallet-serving", "zinder-wallet-serving-explorer"])' \
    "$single_volume_images_catalog" > /dev/null 2>&1; then
    cat >&2 <<'EOF'
release admission rejected: the single-volume image catalog must contain
exactly the canonical-runtime canary, the wallet-serving image, and the
wallet-serving image with the Explorer.
EOF
    exit 1
  fi

  local image_name
  while IFS= read -r image_name; do
    grep -Eq "^FROM [a-z0-9-]+ AS ${image_name}\$" "$dockerfile" || {
      echo "release admission rejected: deploy/Dockerfile has no ${image_name} target" >&2
      exit 1
    }
  done < <(jq -r '.[]' "$single_volume_images_catalog")
}

if [[ -n "$compose_contract" ]]; then
  [[ -z "$deployment_class" && -z "$target" && -z "$release_workflow" && -z "$build_images_workflow" && "$release_inputs_explicit" = false && -z "$prometheus_config" ]] || usage
  [[ -r "$compose_contract" ]] || {
    echo "release admission rejected: cannot read resolved Compose contract $compose_contract" >&2
    exit 1
  }

  jq -e '
    .services["zinder-compat-lightwalletd"].ulimits.nofile
    | [.soft, .hard] | all(type == "number" and . >= 65536)
  ' "$compose_contract" >/dev/null || {
    echo "release admission rejected: the signed-history reader profile requires soft and hard open-file limits of at least 65536" >&2
    exit 1
  }

  jq -e '
    .services as $services
    | (.volumes // {} | to_entries) as $volumes
    | $services["zinder-history-publisher"] as $publisher
    | $services["zinder-compat-lightwalletd"] as $reader
    | "/var/lib/zinder-history" as $catalog
    | "/run/zinder-history/publisher.pub" as $public_key
    | "/run/zinder-history/publisher.pk8" as $private_key
    | ($publisher.volumes[]? | select(.target == $private_key) | .source | rtrimstr("/publisher.pk8")) as $generation
    | def binds($source; $target; $readonly):
        any(.volumes[]?; .type == "bind" and .source == $source and .target == $target
          and ((.read_only // false) == $readonly) and (.bind.create_host_path == false));
      def host_path:
        if type != "string" or (startswith("/") | not) or (split("/") | index("..") != null)
        then error("expected an absolute host mount path without parent traversal")
        else "/" + (split("/") | map(select(. != "" and . != ".")) | join("/"))
        end;
      def backing_paths($mount):
        if $mount.type == "bind" then $mount.source | host_path
        elif $mount.type == "volume" and $mount.source != null then
          ($volumes[] | select(.key == $mount.source) | .value.name // .key) as $name
          | $volumes[] | select((.value.name // .key) == $name)
          | .value | select((.driver // "local") == "local")
          | (.driver_opts // {}) as $opts
          | select((($opts.device // "") | startswith("/")) or
                   (($opts.o // "") | split(",") | any(test("^\\s*(rbind|bind)\\s*$"))))
          | ($opts.device | host_path) + "/" + ($mount.volume.subpath // "") | host_path
        else empty end;
      def permitted_source($dir; $readonly):
        . as $path
        | ($dir + "/publisher.pk8") as $key
        | $path != "/" and $path != $key and ($key | startswith($path + "/") | not)
          and (if $path == ($dir + "/publisher.pub") or $path == ($dir + "/catalog")
                 or ($path | startswith($dir + "/catalog/"))
               then $readonly == true else true end);
      $publisher.network_mode == "none" and $publisher.image == $reader.image
      and ($publisher.command | index("publish-shielded-history") != null and index("--follow-interval-secs") != null)
      and ($publisher | any(.volumes[]?; .type == "volume" and .source == "data" and .target == "/var/lib/zinder" and .read_only))
      and ($publisher | binds($generation + "/publisher.pk8"; $private_key; true))
      and ($publisher | binds($generation + "/catalog"; $catalog; false))
      and ($reader | binds($generation + "/publisher.pub"; $public_key; true))
      and ($reader | binds($generation + "/catalog"; $catalog; true))
      and $reader.environment.ZINDER_COMPAT__SHIELDED_HISTORY__DIRECTORY == $catalog
      and $reader.environment.ZINDER_COMPAT__SHIELDED_HISTORY__PUBLISHER_KEY == $public_key
      and all($services | del(.["zinder-history-publisher"]) | .[].volumes[]?;
        . as $mount | all(backing_paths($mount); permitted_source(($generation | host_path); $mount.read_only)))
  ' "$compose_contract" >/dev/null || {
    echo "release admission rejected: invalid signed wallet history publication or reader mounts" >&2
    exit 1
  }

  jq -e '
    .services as $services
    |
    def exact_healthcheck($binary; $start_period):
      .test == ["CMD", $binary, "healthcheck"]
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
      "mkdir -p /var/lib/zinder/checkpoints /var/lib/zinder/projector/canonical-secondary /var/lib/zinder/query/canonical-secondary /var/lib/zinder/query/wallet-secondary /var/lib/zinder/compat/canonical-secondary /var/lib/zinder/compat/wallet-secondary /var/lib/zinder/explorer/secondary && chown -R 10001:10001 /var/lib/zinder"
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
    and ($services["zinder-ingest"].healthcheck | exact_healthcheck("zinder-ingest"; "3h0m0s"))
    and ($services["zinder-projector"].healthcheck | exact_healthcheck("zinder-projector"; "2h0m0s"))
    and ($services["zinder-query"].healthcheck | exact_healthcheck("zinder-query"; "5m0s"))
    and ($services["zinder-compat-lightwalletd"].healthcheck | exact_healthcheck("zinder-compat-lightwalletd"; "5m0s"))
    and ($services["zinder-projector"].depends_on["zinder-ingest"].condition == "service_healthy")
    and ($services["zinder-compat-lightwalletd"].depends_on["zinder-ingest"].condition == "service_started")
    and ($services["zinder-compat-lightwalletd"].depends_on["zinder-projector"].condition == "service_healthy")
    and ($services["zinder-query"].depends_on["zinder-ingest"].condition == "service_started")
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
  [[ -z "$deployment_class" && -z "$target" && -z "$release_workflow" && -z "$build_images_workflow" && "$release_inputs_explicit" = false && -z "$compose_contract" ]] || usage
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

  release_job_body() {
    awk -v heading="  $1:" '
      $0 == heading { in_job = 1; next }
      in_job && /^  [a-zA-Z0-9_-]+:/ { exit }
      in_job { print }
    ' "$release_workflow"
  }

  release_job_ids() {
    awk '
      /^jobs:[[:space:]]*$/ { in_jobs = 1; next }
      in_jobs && /^  [a-zA-Z0-9_-]+:[[:space:]]*$/ { job = $1; sub(/:$/, "", job); print job }
    ' "$release_workflow"
  }

  # One "job need" line per dependency edge, from scalar, flow, or block
  # sequence needs.
  release_need_edges="$(
    awk '
      /^jobs:[[:space:]]*$/ { in_jobs = 1; next }
      !in_jobs { next }
      /^  [a-zA-Z0-9_-]+:[[:space:]]*$/ {
        job = $1
        sub(/:$/, "", job)
        in_needs = 0
        next
      }
      /^    needs:/ {
        line = $0
        sub(/^    needs:[[:space:]]*/, "", line)
        sub(/[[:space:]]*#.*$/, "", line)
        in_needs = (line == "")
        gsub(/[][,]/, " ", line)
        count = split(line, needs, /[[:space:]]+/)
        for (position = 1; position <= count; position++) {
          if (needs[position] != "") { print job, needs[position] }
        }
        next
      }
      in_needs && /^      - / {
        need = $0
        sub(/^      - /, "", need)
        sub(/[[:space:]]*#.*$/, "", need)
        print job, need
        next
      }
      in_needs && !/^[[:space:]]*$/ { in_needs = 0 }
    ' "$release_workflow"
  )"
  declare -A release_job_needs=()
  while read -r dependent_job needed_job; do
    [[ -n "$dependent_job" ]] || continue
    release_job_needs["$dependent_job"]+="${needed_job} "
  done <<< "$release_need_edges"

  release_job_direct_needs() {
    local need
    for need in ${release_job_needs[$1]:-}; do
      printf '%s\n' "$need"
    done | sort
  }

  release_job_has_ancestor() {
    local ancestor="$2"
    local -A visited=()
    local -a pending=("$1")
    local current need
    while [[ "${#pending[@]}" -gt 0 ]]; do
      current="${pending[0]}"
      pending=("${pending[@]:1}")
      for need in ${release_job_needs[$current]:-}; do
        [[ "$need" != "$ancestor" ]] || return 0
        [[ -z "${visited[$need]:-}" ]] || continue
        visited["$need"]=1
        pending+=("$need")
      done
    done
    return 1
  }

  # Jobs that declare any write scope, write-all, or an OIDC token.
  writing_release_jobs="$(
    awk '
      /^jobs:[[:space:]]*$/ { in_jobs = 1; next }
      !in_jobs { next }
      /^  [a-zA-Z0-9_-]+:[[:space:]]*$/ {
        job = $1
        sub(/:$/, "", job)
        in_permissions = 0
        next
      }
      /^    permissions:/ && /write/ { print job; next }
      /^    permissions:[[:space:]]*$/ { in_permissions = 1; next }
      in_permissions && /^      [a-z-]+:[[:space:]]*write[[:space:]]*$/ { print job; next }
      in_permissions && /^    [^ ]/ { in_permissions = 0 }
    ' "$release_workflow" | sort -u
  )"

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

  sdk_package_job="$(release_job_body sdk-packages)"
  if [[ -z "$sdk_package_job" ]] \
    || ! grep -Fq 'needs: validate' <<< "$sdk_package_job" \
    || ! grep -Fq 'fetch-depth: 0' <<< "$sdk_package_job" \
    || ! grep -Fq 'persist-credentials: false' <<< "$sdk_package_job" \
    || ! grep -Fq 'toolchain: 1.100.0' <<< "$sdk_package_job" \
    || ! grep -Fq 'scripts/verify-sdk-packages.sh' <<< "$sdk_package_job" \
    || ! grep -Fq 'uses: ZcashFoundation/cargo-release@f212e2569995d569baeb6cdeb7ed99b7602303a4 # v0.1.1' \
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
1.100, verify the hermetic package catalog, run the complete Cargo Release
readiness check, and receive no registry credential.
EOF
    exit 1
  fi

  crate_publication_job="$(release_job_body publish-sdk-crates)"
  auth_line="$(grep -nF 'uses: rust-lang/crates-io-auth-action@3268eb413133e2960fa4ae0e963ca76380c54cd9 # v1.0.6' <<< "$crate_publication_job" | cut -d: -f1)"
  publish_line="$(grep -nF 'uses: ZcashFoundation/cargo-release@f212e2569995d569baeb6cdeb7ed99b7602303a4 # v0.1.1' <<< "$crate_publication_job" | cut -d: -f1)"
  consumer_line="$(grep -nF 'scripts/verify-published-sdk.sh "$SDK_VERSION"' <<< "$crate_publication_job" | cut -d: -f1)"
  registry_verifier="$repository_root/scripts/verify-published-sdk.sh"
  if [[ -z "$crate_publication_job" ]] \
    || ! grep -Eq '^[[:space:]]+- validate$' <<< "$crate_publication_job" \
    || ! grep -Eq '^[[:space:]]+- api-docs$' <<< "$crate_publication_job" \
    || ! grep -Eq '^[[:space:]]+- sdk-packages$' <<< "$crate_publication_job" \
    || ! grep -Fq 'environment: release' <<< "$crate_publication_job" \
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
    || ! grep -Fq 'cargo +1.100.0 check' "$registry_verifier" \
    || [[ "$(grep -Fc 'CARGO_REGISTRY_TOKEN:' <<< "$crate_publication_job")" -ne 1 ]] \
    || grep -Eq 'secrets\.(CARGO_REGISTRY_TOKEN|CRATES_IO_TOKEN)' <<< "$crate_publication_job" \
    || grep -Fq 'needs.validate.outputs.stable' <<< "$crate_publication_job" \
    || grep -Eq 'phase: (all|finalize)' <<< "$crate_publication_job" \
    || grep -Fq 'actions/download-artifact' <<< "$crate_publication_job"; then
    cat >&2 <<'EOF'
release admission rejected: SDK publication must follow validated package and
API jobs, delegate resumable publication to the pinned Cargo Release action,
expose the OIDC token only to that publication step, and compile a fresh
registry-only consumer before image publication.
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

  publishing_jobs=(images binary-archives merge assemble-and-sign prepare-release publish-release verify-release promote-latest)
  required_predecessors=(publish-sdk-crates publish-sdk-crates binary-archives merge assemble-and-sign prepare-release publish-release verify-release)
  for index in "${!publishing_jobs[@]}"; do
    publishing_job="${publishing_jobs[$index]}"
    required_predecessor="${required_predecessors[$index]}"
    job_body="$(release_job_body "$publishing_job")"
    if [[ -z "$job_body" ]] \
      || ! grep -Eq "^[[:space:]]+- ${required_predecessor}$" <<< "$job_body"; then
      cat >&2 <<EOF
release admission rejected: publishing job $publishing_job must depend on
$required_predecessor so it cannot bypass release authorization.
EOF
      exit 1
    fi
  done

  while IFS= read -r writing_job; do
    [[ -n "$writing_job" && "$writing_job" != publish-sdk-crates ]] || continue
    if ! release_job_has_ancestor "$writing_job" publish-sdk-crates; then
      cat >&2 <<EOF
release admission rejected: job $writing_job receives a write permission or an
OIDC token without depending on the approved publish-sdk-crates job.
EOF
      exit 1
    fi
  done <<< "$writing_release_jobs"

  for binary_build_job in compile reproduce; do
    if [[ "$(release_job_direct_needs "$binary_build_job")" != validate ]]; then
      cat >&2 <<EOF
release admission rejected: $binary_build_job must need only validate, so both
release builds start right after tag validation, before approval and crate
publication.
EOF
      exit 1
    fi
  done

  comparison_needs="$(release_job_direct_needs compare-binaries)"
  for proof_dependent_job in publish-sdk-crates images binary-archives; do
    if ! grep -Fqx compare-binaries <<< "$(release_job_direct_needs "$proof_dependent_job")" \
      || ! grep -Fqx compile <<< "$comparison_needs" \
      || ! grep -Fqx reproduce <<< "$comparison_needs"; then
      cat >&2 <<EOF
release admission rejected: $proof_dependent_job must need compare-binaries,
and compare-binaries must need compile and reproduce, so the release approval,
archive attestation, and image publication wait until both builds of every
platform match.
EOF
      exit 1
    fi
  done

  while IFS= read -r release_job; do
    [[ -n "$release_job" ]] || continue
    job_timeout="$(sed -n 's/^    timeout-minutes: \([0-9]*\)$/\1/p' <<< "$(release_job_body "$release_job")")"
    case "$release_job" in
      compile|reproduce) required_timeout_pattern='^60$' ;;
      *) required_timeout_pattern='^[1-9][0-9]*$' ;;
    esac
    if [[ ! "$job_timeout" =~ $required_timeout_pattern ]]; then
      cat >&2 <<EOF
release admission rejected: release job $release_job must declare a job-level
timeout-minutes, 60 for each binary build, so a hung runner cannot hold the
release-publication group.
EOF
      exit 1
    fi
  done < <(release_job_ids)

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
    || grep -Eq 'zinder-(bench|compat-cipherscan|proto-codegen)' <<< "$binary_export_stage" \
    || ! grep -Fq 'ARG RUST_VERSION=1.100.0' "$dockerfile" \
    || ! grep -Eq '^ARG RUST_IMAGE_DIGEST=sha256:[0-9a-f]{64}$' "$dockerfile" \
    || ! grep -Eq '^ARG DEBIAN_IMAGE_DIGEST=sha256:[0-9a-f]{64}$' "$dockerfile" \
    || ! grep -Fq 'ENV CARGO_INCREMENTAL=0' "$dockerfile" \
    || ! grep -Fq 'ARG RELEASE_CACHE_SCOPE=shared' "$dockerfile" \
    || ! grep -Fq '${RELEASE_CACHE_SCOPE}' "$dockerfile" \
    || ! grep -Fq -- '--remap-path-prefix=/workspace=/usr/src/zinder' "$dockerfile" \
    || ! grep -Fq -- '--remap-path-prefix=/usr/local/cargo=/cargo' "$dockerfile" \
    || ! grep -Fq 'ENV CARGO_PROFILE_RELEASE_STRIP=symbols' "$dockerfile" \
    || ! grep -Fq 'cargo install cargo-auditable --version 0.7.7 --locked' "$dockerfile" \
    || ! grep -Fq 'cargo auditable build --locked --release' "$dockerfile" \
    || ! grep -Fq 'libstdc++6' "$dockerfile"; then
    cat >&2 <<'EOF'
release admission rejected: the Docker release-binaries target must export
exactly the runtime catalog binaries with the pinned reproducible GNU
build inputs and runtime libstdc++ dependency.
EOF
    exit 1
  fi

  # "stage NAME BASE" for each named stage and "copy STAGE SOURCE" for each
  # COPY --from inside it.
  dockerfile_stages="$(
    awk '
      /^FROM / {
        stage = ""
        if (NF >= 4 && toupper($3) == "AS") {
          stage = $4
          print "stage", stage, $2
        }
        next
      }
      stage != "" && /^COPY / {
        for (position = 2; position <= NF; position++) {
          if ($position ~ /^--from=/) {
            source = $position
            sub(/^--from=/, "", source)
            print "copy", stage, source
          }
        }
      }
    ' "$dockerfile"
  )"
  dockerfile_stage_base() {
    awk -v stage="$1" '$1 == "stage" && $2 == stage { print $3 }' <<< "$dockerfile_stages"
  }
  compiled_binary_catalog="$(
    awk '
      /^FROM / { in_stage = ($NF == "zinder-binaries"); next }
      in_stage {
        for (position = 1; position < NF; position++) {
          if ($position == "--bin") { print $(position + 1) }
        }
      }
    ' "$dockerfile" | sort
  )"
  archived_binaries_stage="$(
    awk '
      $0 == "FROM scratch AS zinder-archived-binaries" { in_stage = 1; found = 1; next }
      in_stage && /^FROM / { exit }
      in_stage && !/^[[:space:]]*(#.*)?$/ { print }
      END { if (!found) { print "missing" } }
    ' "$dockerfile"
  )"
  release_image_targets="$(jq -r '.[]' "$release_images_catalog" "$single_volume_images_catalog")"
  image_targets_package_selected_binaries=true
  while IFS= read -r image_target; do
    image_target_sources="$(
      awk -v stage="$image_target" '$1 == "copy" && $2 == stage { print $3 }' \
        <<< "$dockerfile_stages" | sort -u
    )"
    if [[ -n "$image_target_sources" && "$image_target_sources" != zinder-binaries-selected ]]; then
      image_targets_package_selected_binaries=false
    fi
  done <<< "$release_image_targets"
  # The variable reference is the literal Dockerfile text being inspected.
  # shellcheck disable=SC2016
  if [[ "$compiled_binary_catalog" != "$configured_release_images" ]] \
    || [[ "$(grep -c '^COPY --from=zinder-binaries ' "$dockerfile")" -ne "$(wc -l <<< "$configured_release_images")" ]] \
    || ! sed '/^FROM /,$d' "$dockerfile" | grep -Fqx 'ARG ZINDER_BINARIES_SOURCE=zinder-binaries' \
    || [[ -n "$archived_binaries_stage" ]] \
    || [[ "$(dockerfile_stage_base zinder-binaries-selected)" != '${ZINDER_BINARIES_SOURCE}' ]] \
    || [[ "$(dockerfile_stage_base zinder-runtime-base)" != debian:* ]] \
    || [[ "$image_targets_package_selected_binaries" != true ]]; then
    cat >&2 <<'EOF'
release admission rejected: the zinder-binaries stage must compile exactly the
runtime catalog, without the benchmark, and every release image target must
take its binaries from zinder-binaries-selected, whose archived source is an
empty stage that fails without its build context.
EOF
    exit 1
  fi

  dockerfile_stage_body() {
    awk -v stage="$1" '
      /^FROM / { in_stage = (NF >= 4 && toupper($3) == "AS" && $4 == stage); next }
      in_stage && !/^[[:space:]]*#/ { print }
    ' "$dockerfile"
  }
  single_volume_image_targets="$(jq -r '.[]' "$single_volume_images_catalog")"
  image_target_bases_match=true
  while IFS= read -r image_target; do
    image_target_body="$(dockerfile_stage_body "$image_target")"
    if [[ "$(dockerfile_stage_base "$image_target")" != zinder-service-base ]] \
      || ! grep -Fq "    CMD [\"${image_target}\", \"--config\", \"/etc/zinder/config.toml\", \"healthcheck\"" <<< "$image_target_body" \
      || grep -Fq 'curl' <<< "$image_target_body"; then
      image_target_bases_match=false
    fi
  done <<< "$configured_release_images"
  image_targets_bake_default_config=true
  if grep -Eq '^ENV ' <<< "$(dockerfile_stage_body zinder-service-base)"; then
    image_targets_bake_default_config=false
  fi
  while IFS= read -r image_target; do
    image_target_body="$(dockerfile_stage_body "$image_target")"
    image_config="deploy/config/images/${image_target#zinder-}.toml"
    if [[ ! -f "$repository_root/$image_config" ]] \
      || ! grep -Fqx "COPY ${image_config} /etc/zinder/config.toml" <<< "$image_target_body" \
      || ! grep -Fqx "ENTRYPOINT [\"${image_target}\"]" <<< "$image_target_body" \
      || ! grep -Fqx 'CMD ["--config", "/etc/zinder/config.toml"]' <<< "$image_target_body" \
      || grep -Eq '^ENV ' <<< "$image_target_body"; then
      image_targets_bake_default_config=false
    fi
  done <<< "$configured_release_images"
  if [[ "$image_targets_bake_default_config" != true ]]; then
    cat >&2 <<'EOF'
release admission rejected: every per-service release image must copy its
deploy/config/images/<service>.toml to /etc/zinder/config.toml, pass it with
CMD ["--config", "/etc/zinder/config.toml"] after its binary ENTRYPOINT, and
set no ENV, so a mounted config file is not silently overridden.
EOF
    exit 1
  fi
  while IFS= read -r image_target; do
    image_target_base="$(dockerfile_stage_base "$image_target")"
    if [[ "$image_target_base" != zinder-runtime-base ]] \
      && ! grep -Fqx -- "$image_target_base" <<< "$single_volume_image_targets"; then
      image_target_bases_match=false
    fi
  done <<< "$single_volume_image_targets"
  # The variable reference is the literal Dockerfile text being inspected.
  # shellcheck disable=SC2016
  if [[ "$image_target_bases_match" != true ]] \
    || ! grep -Eq '^ARG DISTROLESS_IMAGE_DIGEST=sha256:[0-9a-f]{64}$' "$dockerfile" \
    || [[ "$(dockerfile_stage_base zinder-service-base)" != 'gcr.io/distroless/cc-debian12:nonroot@${DISTROLESS_IMAGE_DIGEST}' ]] \
    || ! grep -Fqx 'USER 10001:10001' <<< "$(dockerfile_stage_body zinder-service-base)" \
    || ! grep -Fqx 'COPY --from=zinder-service-state --chown=10001:10001 /var/lib/zinder /var/lib/zinder' \
      <<< "$(dockerfile_stage_body zinder-service-base)" \
    || ! grep -Fq 'groupadd --system --gid 10001 zinder' <<< "$(dockerfile_stage_body zinder-runtime-base)" \
    || ! grep -Fq 'useradd --system --uid 10001 --gid zinder' <<< "$(dockerfile_stage_body zinder-runtime-base)"; then
    cat >&2 <<'EOF'
release admission rejected: every per-service release image must build on the
digest-pinned distroless zinder-service-base as UID and GID 10001 and probe
itself with its own healthcheck subcommand, and every single-volume image must
build on the Debian zinder-runtime-base, whose zinder account has the same UID
and GID, for its shell entrypoints.
EOF
    exit 1
  fi

  # The wallet-serving image is the one image a lightwalletd operator pulls, so
  # it carries both wallet readers and exposes the compatibility port.
  wallet_serving_body="$(dockerfile_stage_body zinder-wallet-serving)"
  wallet_serving_binaries="$(
    sed -n 's#^COPY --from=zinder-binaries-selected /zinder-binaries/\(zinder-[a-z-]*\) /usr/local/bin/\1$#\1#p' \
      <<< "$wallet_serving_body" | sort | tr '\n' ' '
  )"
  if [[ "$wallet_serving_binaries" != "zinder-compat-lightwalletd zinder-ingest zinder-projector zinder-query " ]] \
    || ! grep -Fqx 'COPY deploy/config/wallet-serving/compat-lightwalletd.toml /etc/zinder/compat-lightwalletd.toml' \
      <<< "$wallet_serving_body" \
    || ! grep -Eqx 'EXPOSE( [0-9]+)* 9067( [0-9]+)*' <<< "$wallet_serving_body"; then
    cat >&2 <<'EOF'
release admission rejected: the zinder-wallet-serving image must carry exactly
zinder-ingest, zinder-projector, zinder-query, and zinder-compat-lightwalletd,
bake its compatibility config, and expose port 9067, matching the packages its
release image evidence requires.
EOF
    exit 1
  fi

  compile_job="$(release_job_body compile)"
  reproduction_job="$(release_job_body reproduce)"
  comparison_job="$(release_job_body compare-binaries)"
  image_job="$(release_job_body images)"
  binary_archive_job="$(release_job_body binary-archives)"
  collected_binary_job="$(release_job_body collect-binary-assets)"
  prepare_release_job="$(release_job_body prepare-release)"
  publish_release_job="$(release_job_body publish-release)"
  assemble_release_job="$(release_job_body assemble-and-sign)"
  merge_job="$(release_job_body merge)"
  verify_release_job="$(release_job_body verify-release)"
  promote_latest_job="$(release_job_body promote-latest)"
  image_attestation_verification="$(
    awk '
      /scripts\/verify-release-image-attestations\.sh/ { in_call = 1 }
      in_call { print }
      in_call && /--commit / { exit }
    ' <<< "$merge_job"
  )"
  # The continued shell command that starts at the first line matching $1.
  shell_invocation() {
    awk -v start="$1" '
      index($0, start) { in_call = 1 }
      in_call { print }
      in_call && !/\\$/ { exit }
    '
  }
  image_push_invocation="$(shell_invocation 'scripts/package-release-images.sh push ' <<< "$image_job")"
  image_smoke_invocation="$(shell_invocation 'scripts/package-release-images.sh check ' <<< "$reproduction_job")"
  # These literals are workflow expressions and shell variables being inspected.
  # shellcheck disable=SC2016
  if [[ -z "$compile_job" || -z "$reproduction_job" || -z "$comparison_job" ]] \
    || [[ -z "$image_job" || -z "$binary_archive_job" || -z "$collected_binary_job" ]] \
    || [[ "$(grep -Fc -- '--no-cache' <<< "$compile_job")" -ne 1 ]] \
    || [[ "$(grep -Fc -- '--no-cache' <<< "$reproduction_job")" -ne 1 ]] \
    || [[ "$(grep -Fc -- '--target release-binaries' <<< "$compile_job")" -ne 1 ]] \
    || ! grep -Fq -- '--target release-binaries' <<< "$reproduction_job" \
    || ! grep -Fq 'RELEASE_CACHE_SCOPE: release-${{ needs.validate.outputs.commit }}-${{ matrix.platform.rust }}-1' \
      <<< "$compile_job" \
    || ! grep -Fq 'RELEASE_CACHE_SCOPE=release-${RELEASE_COMMIT}-${RUST_TARGET}-2' \
      <<< "$reproduction_job" \
    || [[ "$(grep -Fc -- '--build-arg "RELEASE_CACHE_SCOPE=${RELEASE_CACHE_SCOPE}"' <<< "$compile_job")" -ne 1 ]] \
    || [[ "$(grep -Fc -- '--build-arg "SOURCE_DATE_EPOCH=${source_date_epoch}"' <<< "$compile_job")" -ne 1 ]] \
    || [[ "$(grep -Fc -- '--build-arg "ZINDER_BUILD_GIT_COMMIT=${RELEASE_COMMIT}"' <<< "$compile_job")" -ne 1 ]] \
    || grep -Eq -- '--cache-(from|to)' "$release_workflow" \
    || grep -Eq 'docker/login-action|docker login' <<< "$compile_job$comparison_job" \
    || ! grep -Fq 'scripts/check-release-binary-archive.sh' <<< "$compile_job" \
    || ! grep -Fq 'scripts/check-release-binary-archive.sh' <<< "$reproduction_job" \
    || ! grep -Fq 'x86_64-v3-unknown-linux-gnu' <<< "$compile_job" \
    || ! grep -Fq 'aarch64-unknown-linux-gnu' <<< "$compile_job" \
    || ! grep -Fq 'x86_64-v3-unknown-linux-gnu' <<< "$reproduction_job" \
    || ! grep -Fq 'aarch64-unknown-linux-gnu' <<< "$reproduction_job" \
    || ! grep -Fq 'scripts/generate-release-sbom.sh' <<< "$compile_job" \
    || ! grep -Fq 'scripts/check-release-sbom.sh' <<< "$compile_job" \
    || ! grep -Fq 'name: release-build-${{ matrix.platform.name }}' <<< "$compile_job" \
    || ! grep -Fq 'name: release-build-${{ matrix.platform }}' <<< "$comparison_job" \
    || ! grep -Fq 'name: release-reproduction-${{ matrix.platform }}' <<< "$comparison_job" \
    || ! grep -Fq 'diff -u dist/build/binaries.sha256 dist/reproduction/binaries.sha256' \
      <<< "$comparison_job" \
    || ! grep -Fq 'cmp -- "dist/build/${archive_name}" "dist/reproduction/${archive_name}"' \
      <<< "$comparison_job" \
    || ! grep -Fq 'name: release-build-${{ matrix.platform.name }}' <<< "$image_job" \
    || ! grep -Fq 'path: ${{ runner.temp }}/release-candidate' <<< "$image_job" \
    || ! grep -Fq 'RELEASE_IMAGES_JSON: ${{ needs.validate.outputs.release_images }}' <<< "$image_job" \
    || [[ "$(grep -Fc 'scripts/package-release-images.sh' <<< "$image_job")" -ne 1 ]] \
    || ! grep -Fq -- '--archive "${candidate_root}/zinder-${RELEASE_VERSION}-${PLATFORM_NAME}.tar.gz"' <<< "$image_push_invocation" \
    || ! grep -Fq -- '--binary-hashes "${candidate_root}/binaries.sha256"' <<< "$image_push_invocation" \
    || ! grep -Fq -- '--images-json "$RELEASE_IMAGES_JSON"' <<< "$image_push_invocation" \
    || ! grep -Fq -- '--platform "$TARGET_PLATFORM"' <<< "$image_push_invocation" \
    || ! grep -Fq -- '--digests /tmp/digests' <<< "$image_push_invocation" \
    || grep -Fq 'docker buildx build' <<< "$image_job" \
    || grep -Fq 'RELEASE_CACHE_SCOPE' <<< "$image_job" \
    || grep -Fq -- '--target release-binaries' <<< "$image_job" \
    || ! grep -Fq 'name: release-build-${{ matrix.platform }}' <<< "$binary_archive_job" \
    || [[ "$(grep -Fc 'uses: actions/attest@1e69f48acb82d1966a394da916b4c1698aa569d6 # v4.2.2' <<< "$binary_archive_job")" -ne 2 ]] \
    || ! grep -Eq '^[[:space:]]+- images$' <<< "$merge_job" \
    || ! grep -Eq '^[[:space:]]+- binary-archives$' <<< "$collected_binary_job" \
    || ! grep -Fq 'spdx.json' <<< "$collected_binary_job" \
    || ! grep -Eq '^[[:space:]]+- collect-binary-assets$' <<< "$assemble_release_job" \
    || ! grep -Fq 'name: release-binary-assets' <<< "$assemble_release_job" \
    || ! grep -Eq '^[[:space:]]+- assemble-and-sign$' <<< "$prepare_release_job" \
    || ! grep -Fq 'name: Checkout validated commit' <<< "$prepare_release_job" \
    || ! grep -Fq 'uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1' \
      <<< "$prepare_release_job" \
    || ! grep -Fq 'ref: ${{ needs.validate.outputs.commit }}' \
      <<< "$prepare_release_job" \
    || ! grep -Fq 'persist-credentials: false' <<< "$prepare_release_job" \
    || ! grep -Fq 'name: release-assets-final' <<< "$prepare_release_job"; then
    cat >&2 <<'EOF'
release admission rejected: release binaries must build in 2 independent
uncached jobs for both GNU platforms and compare identical before approval,
images must package the proven archive binaries only through
scripts/package-release-images.sh push, exact SBOMs must be generated and
attested, and the archives must be assembled into the final draft GitHub
Release assets.
EOF
    exit 1
  fi

  # These literals are workflow expressions and shell variables being inspected.
  # shellcheck disable=SC2016
  archive_build_line="$(grep -nF 'scripts/build-release-binary-archive.sh' <<< "$reproduction_job" | head -n 1 | cut -d: -f1 || true)"
  image_smoke_line="$(grep -nF 'scripts/package-release-images.sh check ' <<< "$reproduction_job" | head -n 1 | cut -d: -f1 || true)"
  reproduction_permissions="$(
    awk '
      /^    permissions:[[:space:]]*$/ { in_permissions = 1; next }
      in_permissions && /^      / { print; next }
      in_permissions { exit }
    ' <<< "$reproduction_job"
  )"
  # shellcheck disable=SC2016
  if [[ -z "$archive_build_line" || -z "$image_smoke_line" ]] \
    || (( archive_build_line >= image_smoke_line )) \
    || [[ "$(grep -Fc 'scripts/package-release-images.sh' <<< "$reproduction_job")" -ne 1 ]] \
    || ! grep -Fq -- '--archive "dist/reproduction/zinder-${RELEASE_VERSION}-${PLATFORM_NAME}.tar.gz"' <<< "$image_smoke_invocation" \
    || ! grep -Fq -- '--binary-hashes dist/reproduction/binaries.sha256' <<< "$image_smoke_invocation" \
    || ! grep -Fq -- '--images-json "$RELEASE_IMAGES_JSON"' <<< "$image_smoke_invocation" \
    || ! grep -Fq -- '--platform "$TARGET_PLATFORM"' <<< "$image_smoke_invocation" \
    || grep -Eq -- '--(registry|digests)' <<< "$image_smoke_invocation" \
    || ! grep -Fq 'RELEASE_IMAGES_JSON: ${{ needs.validate.outputs.release_images }}' <<< "$reproduction_job" \
    || grep -Eq '^[[:space:]]+(-[[:space:]]+)?(continue-on-error|if):' <<< "$reproduction_job" \
    || grep -Eq 'docker/login-action|docker login|secrets\.|github\.token' <<< "$reproduction_job" \
    || [[ "$reproduction_permissions" != '      contents: read' ]]; then
    cat >&2 <<'EOF'
release admission rejected: the reproduce job must package every release image
from its own archive with scripts/package-release-images.sh check after
building it, with only a read-only repository token, no registry login, and no
step that may skip or ignore the packaging smoke.
EOF
    exit 1
  fi

  # These literals are the shell text of the packager being inspected.
  # shellcheck disable=SC2016
  packager_hash_check_line="$(grep -nF '(cd "$binary_root" && sha256sum --check --strict "$binary_hashes")' "$image_packager" | head -n 1 | cut -d: -f1 || true)"
  # shellcheck disable=SC2016
  packager_mode_check_line="$(grep -nF 'require_archive_binary_mode "$binary_root/$binary_name" "archive binary $binary_name"' "$image_packager" | head -n 1 | cut -d: -f1 || true)"
  # shellcheck disable=SC2016
  packager_image_check_line="$(grep -nF 'require_archive_binary_mode "$image_binary" "${image_name} binary ${binary_name}"' "$image_packager" | head -n 1 | cut -d: -f1 || true)"
  packager_first_build_line="$(grep -nF 'docker buildx build' "$image_packager" | head -n 1 | cut -d: -f1 || true)"
  # shellcheck disable=SC2016
  packager_publish_line="$(grep -nF -- '--output "$image_output"' "$image_packager" | head -n 1 | cut -d: -f1 || true)"
  # shellcheck disable=SC1003,SC2016
  if [[ ! -x "$image_packager" ]] \
    || [[ -z "$packager_hash_check_line" || -z "$packager_mode_check_line" ]] \
    || [[ -z "$packager_image_check_line" || -z "$packager_first_build_line" || -z "$packager_publish_line" ]] \
    || (( packager_hash_check_line >= packager_first_build_line )) \
    || (( packager_mode_check_line >= packager_first_build_line )) \
    || (( packager_image_check_line >= packager_publish_line )) \
    || ! grep -Fqx 'archive_binary_mode=755' "$image_packager" \
    || ! grep -Fq 'tar --extract --gzip --same-permissions --no-same-owner \' "$image_packager" \
    || ! grep -Fq 'if [[ -L "$binary_path" || ! -f "$binary_path" ]]; then' "$image_packager" \
    || ! grep -Fq "binary_mode=\"\$(stat -c '%a' -- \"\$binary_path\")\"" "$image_packager" \
    || ! grep -Fq 'if [[ "$binary_mode" != "$archive_binary_mode" ]]; then' "$image_packager" \
    || ! grep -Fq 'cmp -- "${binary_root}/${binary_name}" "$image_binary"' "$image_packager" \
    || ! grep -Fqx 'archive_binary_listing_mode=-rwxr-xr-x' "$image_packager" \
    || ! grep -Fq 'docker container export --output "$smoke_filesystem" "$smoke_container"' "$image_packager" \
    || ! grep -Fq 'tar --list --verbose --numeric-owner --file "$smoke_filesystem" \' "$image_packager" \
    || ! grep -Fq '[[ "$image_binary_listing" == "${archive_binary_listing_mode} 0/0" ]]' "$image_packager" \
    || ! grep -Fq -- '--entrypoint "$binary_name" "$smoke_image" --version' "$image_packager" \
    || ! grep -Fq -- '--entrypoint "$binary_name" "$smoke_image" healthcheck --addr 127.0.0.1:1 \' "$image_packager" \
    || ! grep -Fq '[[ "$healthcheck_status" -eq 1 ]]' "$image_packager" \
    || ! grep -Fq "jq -r '.[]' <<< \"\$images_json\"" "$image_packager" \
    || ! grep -Fq -- '--build-context "zinder-archived-binaries=${binary_context}"' "$image_packager" \
    || ! grep -Fq -- '--build-arg "ZINDER_BINARIES_SOURCE=zinder-archived-binaries"' "$image_packager" \
    || ! grep -Fq -- '--build-arg "SOURCE_DATE_EPOCH=${source_date_epoch}"' "$image_packager" \
    || ! grep -Fq 'image_created="$(date -u -d "@${source_date_epoch}" '"'"'+%Y-%m-%dT%H:%M:%SZ'"'"')"' "$image_packager" \
    || ! grep -Fq -- '--label "org.opencontainers.image.created=${image_created}"' "$image_packager" \
    || ! grep -Fq -- '--label "org.opencontainers.image.title=${image_name}"' "$image_packager" \
    || ! grep -Fq -- '--label "org.opencontainers.image.description=${image_description}"' "$image_packager" \
    || ! grep -Fq -- '--label "org.opencontainers.image.documentation=${image_documentation}"' "$image_packager" \
    || ! grep -Fq -- '--label "org.opencontainers.image.vendor=Zcash Foundation"' "$image_packager" \
    || ! grep -Fq -- '--provenance=mode=max' "$image_packager" \
    || ! grep -Fq -- '--sbom=true' "$image_packager" \
    || ! grep -Fq 'push) image_output="type=image,name=${registry}/${image_name},push-by-digest=true,name-canonical=true,push=true" ;;' "$image_packager" \
    || ! grep -Fq 'check) image_output="type=image,name=zinder-release-smoke/${image_name}:${platform_arch},push=false" ;;' "$image_packager" \
    || [[ "$(grep -Fc 'push=true' "$image_packager")" -ne 1 ]] \
    || grep -Eq -- 'RELEASE_CACHE_SCOPE|--target release-binaries|--cache-(from|to)|docker login|cargo ' "$image_packager"; then
    cat >&2 <<'EOF'
release admission rejected: the release image packager must check the archive
binary hashes and their regular-file 0755 mode before any image build, package
every image from the archived-binaries context without compiling or caching,
compare every image binary with the archive byte for byte and by mode before
publishing it, label each image with its catalog description and the commit
timestamp, build with max-mode provenance and an SBOM, push only in push
mode, and in check mode verify each installed binary's mode and owner from the
image filesystem and run its version, help, and healthcheck inside the image.
EOF
    exit 1
  fi

  workflow_before_approval="$(sed '/^  publish-sdk-crates:/,$d' "$release_workflow")"
  if [[ "$(grep -Ec '^    environment:' "$release_workflow")" -ne 1 ]] \
    || grep -Eq '(id-token|packages|attestations|contents): write' \
      <<< "$workflow_before_approval" \
    || grep -Eq 'docker/login-action|docker login' <<< "$workflow_before_approval"; then
    cat >&2 <<'EOF'
release admission rejected: the protected release environment must gate crate
publication as the only reviewed job, and no earlier job may receive a write
permission or an OIDC token or log in to a registry.
EOF
    exit 1
  fi

  if [[ "$(grep -Fc 'uses: actions/attest@1e69f48acb82d1966a394da916b4c1698aa569d6 # v4.2.2' "$release_workflow")" -ne 5 ]] \
    || [[ "$(grep -Fc 'create-storage-record: false' "$release_workflow")" -ne 5 ]] \
    || [[ "$(grep -Fc 'uses: anchore/sbom-action/download-syft@66cbf4bc1f1c0d2edc94016e65bc221b6bb0ad6c # v0.24.3' "$release_workflow")" -ne 2 ]] \
    || [[ "$(grep -Fc 'uses: sigstore/cosign-installer@756fe2d284c7292c4e9b62c8a28f1e5629c59532 # v4.1.3' "$release_workflow")" -ne 3 ]] \
    || [[ "$(grep -Fc 'scripts/install-release-gh.sh' "$release_workflow")" -ne 4 ]] \
    || grep -Fq 'artifact-metadata:' "$release_workflow" \
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

[[ "$release_inputs_explicit" = false ]] || usage

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
