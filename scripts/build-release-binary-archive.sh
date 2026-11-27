#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage: build-release-binary-archive.sh \
  --binaries DIR --output DIR --version VERSION --tag TAG --commit SHA \
  --target RUST_TARGET --source-date-epoch EPOCH [--cpu-baseline ISA] [--pasta-asm 0|1]
EOF
  exit 2
}

repository_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
binaries_directory=""
output_directory=""
version=""
release_tag=""
commit=""
rust_target=""
source_date_epoch=""
pasta_asm=1
requested_cpu_baseline=""
while [[ $# -gt 0 ]]; do
  [[ $# -ge 2 ]] || usage
  case "$1" in
    --binaries) binaries_directory="$2"; shift 2 ;;
    --output) output_directory="$2"; shift 2 ;;
    --version) version="$2"; shift 2 ;;
    --tag) release_tag="$2"; shift 2 ;;
    --commit) commit="$2"; shift 2 ;;
    --target) rust_target="$2"; shift 2 ;;
    --source-date-epoch) source_date_epoch="$2"; shift 2 ;;
    --cpu-baseline) requested_cpu_baseline="$2"; shift 2 ;;
    --pasta-asm) pasta_asm="$2"; shift 2 ;;
    *) usage ;;
  esac
done

[[ -d "$binaries_directory" && -n "$output_directory" ]] || usage
[[ -n "$version" && "$release_tag" == "v${version}" ]] || usage
[[ "$commit" =~ ^[0-9a-f]{40}$ ]] || usage
[[ "$source_date_epoch" =~ ^[0-9]+$ ]] || usage
case "$pasta_asm" in
  0|1) ;;
  *)
    echo >&2 "unsupported --pasta-asm value (expected 0 or 1): $pasta_asm"
    exit 1
    ;;
esac

# shellcheck source=scripts/release-binary-platform.sh
source "$repository_root/scripts/release-binary-platform.sh"
resolve_release_binary_platform "$rust_target" "$requested_cpu_baseline" "$pasta_asm"

mapfile -t runtime_binaries < <(jq -r '.[]' "$repository_root/deploy/release-images.json")
expected_catalog="$(printf '%s\n' "${runtime_binaries[@]}" | LC_ALL=C sort)"
actual_catalog="$(find "$binaries_directory" -maxdepth 1 -type f -printf '%f\n' | LC_ALL=C sort)"
[[ "$actual_catalog" == "$expected_catalog" ]] || {
  echo >&2 "release binary input differs from deploy/release-images.json"
  diff -u <(printf '%s\n' "$expected_catalog") <(printf '%s\n' "$actual_catalog") || true
  exit 1
}

archive_root="zinder-${version}-${release_platform}"
scratch_directory="$(mktemp -d "${TMPDIR:-/tmp}/zinder-release-binaries.XXXXXX")"
trap 'rm -rf -- "$scratch_directory"' EXIT
staging_root="$scratch_directory/$archive_root"
mkdir -p "$staging_root/bin"

binary_inventory='[]'
for binary_name in "${runtime_binaries[@]}"; do
  source_binary="$binaries_directory/$binary_name"
  [[ -f "$source_binary" && -x "$source_binary" ]] || {
    echo >&2 "release binary is missing or not executable: $binary_name"
    exit 1
  }
  cp "$source_binary" "$staging_root/bin/$binary_name"
  binary_sha256="$(sha256sum "$source_binary" | awk '{print $1}')"
  binary_size="$(stat -c '%s' "$source_binary")"
  binary_inventory="$(
    jq \
      --arg name "$binary_name" \
      --arg sha256 "$binary_sha256" \
      --argjson size "$binary_size" \
      '. + [{name: $name, sha256: $sha256, size: $size}]' \
      <<< "$binary_inventory"
  )"
done

cp "$repository_root/LICENSE" "$staging_root/LICENSE"
cp "$repository_root/README.md" "$staging_root/README.md"
jq -n \
  --arg version "$version" \
  --arg tag "$release_tag" \
  --arg commit "$commit" \
  --arg rust_target "$rust_target" \
  --arg release_platform "$release_platform" \
  --arg cpu_baseline "$cpu_baseline" \
  --argjson libc "$libc" \
  --argjson required_cpu_features "$required_cpu_features" \
  --argjson source_date_epoch "$source_date_epoch" \
  --argjson binaries "$binary_inventory" \
  '{
    schema_version: 1,
    version: $version,
    tag: $tag,
    commit: $commit,
    rust_target: $rust_target,
    release_platform: $release_platform,
    libc: $libc,
    cpu_baseline: $cpu_baseline,
    required_cpu_features: $required_cpu_features,
    source_date_epoch: $source_date_epoch,
    binaries: $binaries
  }' > "$staging_root/BUILD-INFO.json"

(
  cd "$staging_root"
  find bin -maxdepth 1 -type f -printf '%p\n' \
    | LC_ALL=C sort \
    | xargs sha256sum
  printf '%s\n' BUILD-INFO.json LICENSE README.md \
    | LC_ALL=C sort \
    | xargs sha256sum
) | LC_ALL=C sort -k2 > "$staging_root/SHA256SUMS"

find "$staging_root" -type d -exec chmod 0755 {} +
find "$staging_root" -type f -exec chmod 0644 {} +
chmod 0755 "$staging_root/bin"/*
find "$staging_root" -exec touch -h -d "@${source_date_epoch}" {} +

mkdir -p "$output_directory"
archive_path="$output_directory/${archive_root}.tar.gz"
LC_ALL=C tar \
  --directory "$scratch_directory" \
  --sort=name \
  --format=gnu \
  --mtime="@${source_date_epoch}" \
  --owner=0 \
  --group=0 \
  --numeric-owner \
  -cf - \
  "$archive_root" \
  | gzip -n > "$archive_path"
