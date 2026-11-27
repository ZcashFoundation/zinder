#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage: resolve-release-image-manifest.sh \
  --image IMAGE --tag TAG --commit SHA \
  --amd64-build-digest DIGEST --arm64-build-digest DIGEST \
  --output FILE
EOF
  exit 2
}

image=""
tag=""
commit=""
amd64_build_digest=""
arm64_build_digest=""
output=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --image) image="$2"; shift 2 ;;
    --tag) tag="$2"; shift 2 ;;
    --commit) commit="$2"; shift 2 ;;
    --amd64-build-digest) amd64_build_digest="$2"; shift 2 ;;
    --arm64-build-digest) arm64_build_digest="$2"; shift 2 ;;
    --output) output="$2"; shift 2 ;;
    *) usage ;;
  esac
done

digest_pattern='^sha256:[0-9a-f]{64}$'
[[ "$image" =~ ^ghcr\.io/[a-z0-9][a-z0-9._-]*/zinder(-[a-z0-9-]+)?$ ]] || usage
[[ "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([+-][0-9A-Za-z.-]+)?$ ]] || usage
[[ "$commit" =~ ^[0-9a-f]{40}$ ]] || usage
[[ "$amd64_build_digest" =~ $digest_pattern ]] || usage
[[ "$arm64_build_digest" =~ $digest_pattern ]] || usage
[[ -n "$output" ]] || usage

poll_attempts="${ZINDER_MANIFEST_POLL_ATTEMPTS:-12}"
poll_delay="${ZINDER_MANIFEST_POLL_DELAY:-5}"
[[ "$poll_attempts" =~ ^[1-9][0-9]*$ ]] || usage
[[ "$poll_delay" =~ ^[0-9]+$ ]] || usage

docker_command="${ZINDER_DOCKER:-docker}"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/zinder-release-manifest.XXXXXX")"
trap 'rm -rf -- "$scratch"' EXIT

inspect_manifest() {
  local reference="$1"
  "$docker_command" buildx imagetools inspect "$reference" \
    --format '{{json .Manifest}}'
}

platform_digest() {
  local manifest="$1"
  local architecture="$2"
  jq -er --arg architecture "$architecture" '
    [.manifests[]?
      | select(.platform.os == "linux" and .platform.architecture == $architecture)
      | .digest]
    | if length == 1 then .[0] else empty end
  ' "$manifest"
}

amd64_build_manifest="$scratch/amd64-build.json"
arm64_build_manifest="$scratch/arm64-build.json"
inspect_manifest "${image}@${amd64_build_digest}" > "$amd64_build_manifest"
inspect_manifest "${image}@${arm64_build_digest}" > "$arm64_build_manifest"
expected_amd64_digest="$(platform_digest "$amd64_build_manifest" amd64)"
expected_arm64_digest="$(platform_digest "$arm64_build_manifest" arm64)"
[[ "$expected_amd64_digest" =~ $digest_pattern ]] || usage
[[ "$expected_arm64_digest" =~ $digest_pattern ]] || usage

tag_reference="${image}:${tag}"
commit_reference="${image}:sha-${commit}"
tag_manifest="$scratch/tag.json"
commit_manifest="$scratch/commit.json"
attempt=1
while [[ "$attempt" -le "$poll_attempts" ]]; do
  if inspect_manifest "$tag_reference" > "$tag_manifest" 2>/dev/null \
    && inspect_manifest "$commit_reference" > "$commit_manifest" 2>/dev/null; then
    commit_root_digest="$(jq -r '.digest // ""' "$commit_manifest" 2>/dev/null || true)"
    if jq -e \
      --arg commit_root "$commit_root_digest" \
      --arg amd64 "$expected_amd64_digest" \
      --arg arm64 "$expected_arm64_digest" '
        (.digest | test("^sha256:[0-9a-f]{64}$"))
        and .digest == $commit_root
        and ([.manifests[]?
          | select(.platform.os == "linux" and .platform.architecture == "amd64")
          | .digest] == [$amd64])
        and ([.manifests[]?
          | select(.platform.os == "linux" and .platform.architecture == "arm64")
          | .digest] == [$arm64])
      ' "$tag_manifest" >/dev/null; then
      mkdir -p "$(dirname -- "$output")"
      cp "$tag_manifest" "$output"
      exit 0
    fi
  fi

  if [[ "$attempt" -lt "$poll_attempts" ]]; then
    sleep "$poll_delay"
  fi
  attempt=$((attempt + 1))
done

echo >&2 "release image tags did not converge after ${poll_attempts} attempts: ${tag_reference}, ${commit_reference}"
exit 1
