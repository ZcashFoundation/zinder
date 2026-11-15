#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage:
  package-release-images.sh check|push \
    --archive ARCHIVE --binary-hashes BINARIES_SHA256 --images-json JSON \
    --platform PLATFORM --source-date-epoch EPOCH \
    --repository OWNER/NAME --tag TAG --commit SHA \
    [--registry REGISTRY/NAMESPACE --digests DIR]

Packages every image target in the JSON catalog from the binaries in a release
binary archive, without compiling. Each image must carry its archive binaries
byte for byte, as regular files with the archive's 0755 mode.

check builds every image without pushing, then runs each carried binary inside
its image as the zinder user. push publishes each image by digest under
REGISTRY/NAMESPACE and records the digest under DIR. --registry and --digests
are required for push and rejected for check.
EOF
  exit 2
}

[[ $# -ge 1 ]] || usage
mode="$1"
shift
case "$mode" in
  check|push) ;;
  *) usage ;;
esac

archive_path=""
binary_hashes=""
images_json=""
platform=""
source_date_epoch=""
repository=""
release_tag=""
commit=""
registry=""
digest_directory=""
while [[ $# -gt 0 ]]; do
  [[ $# -ge 2 ]] || usage
  case "$1" in
    --archive) archive_path="$2" ;;
    --binary-hashes) binary_hashes="$2" ;;
    --images-json) images_json="$2" ;;
    --platform) platform="$2" ;;
    --source-date-epoch) source_date_epoch="$2" ;;
    --repository) repository="$2" ;;
    --tag) release_tag="$2" ;;
    --commit) commit="$2" ;;
    --registry) registry="$2" ;;
    --digests) digest_directory="$2" ;;
    *) usage ;;
  esac
  shift 2
done

[[ -s "$archive_path" && "$archive_path" == *.tar.gz && -s "$binary_hashes" ]] || usage
[[ "$platform" == linux/amd64 || "$platform" == linux/arm64 ]] || usage
[[ "$source_date_epoch" =~ ^[0-9]+$ && "$commit" =~ ^[0-9a-f]{40}$ ]] || usage
[[ "$release_tag" =~ ^v[0-9] && "$repository" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] || usage
jq -e 'type == "array" and length > 0 and all(.[]; type == "string" and test("^zinder-[a-z0-9-]+$"))' \
  <<< "$images_json" >/dev/null 2>&1 || usage
case "$mode" in
  push) [[ "$registry" =~ ^[a-z0-9.:-]+/[a-z0-9._/-]+$ && -n "$digest_directory" ]] || usage ;;
  check) [[ -z "$registry" && -z "$digest_directory" ]] || usage ;;
esac

repository_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
archive_path="$(realpath -- "$archive_path")"
binary_hashes="$(realpath -- "$binary_hashes")"
archive_root="$(basename -- "$archive_path" .tar.gz)"
platform_arch="${platform#linux/}"
# build-release-binary-archive.sh assigns this mode to every archive binary.
archive_binary_mode=755

work_directory="$(mktemp -d "${TMPDIR:-/tmp}/zinder-release-images.XXXXXX")"
smoke_images=()
cleanup() {
  if [[ "${#smoke_images[@]}" -gt 0 ]]; then
    docker image rm --force "${smoke_images[@]}" >/dev/null 2>&1 || true
  fi
  rm -rf -- "$work_directory"
}
trap cleanup EXIT

require_archive_binary_mode() {
  local binary_path="$1"
  local description="$2"
  if [[ -L "$binary_path" || ! -f "$binary_path" ]]; then
    echo >&2 "$description is not a regular file"
    exit 1
  fi
  local binary_mode
  binary_mode="$(stat -c '%a' -- "$binary_path")"
  if [[ "$binary_mode" != "$archive_binary_mode" ]]; then
    echo >&2 "$description has mode $binary_mode, expected $archive_binary_mode"
    exit 1
  fi
}

# --same-permissions keeps the archive's modes instead of applying the umask.
binary_context="$work_directory/archived-binaries"
binary_root="$binary_context/zinder-binaries"
mkdir -p "$binary_root"
tar --extract --gzip --same-permissions --no-same-owner \
  --file "$archive_path" \
  --directory "$binary_root" \
  --strip-components=2 \
  "${archive_root}/bin"
(cd "$binary_root" && sha256sum --check --strict "$binary_hashes")
diff -u \
  <(awk '{print $2}' "$binary_hashes" | LC_ALL=C sort) \
  <(find "$binary_root" -mindepth 1 -maxdepth 1 -printf '%f\n' | LC_ALL=C sort)
mapfile -t archive_binaries < <(awk '{print $2}' "$binary_hashes" | LC_ALL=C sort)
for binary_name in "${archive_binaries[@]}"; do
  require_archive_binary_mode "$binary_root/$binary_name" "archive binary $binary_name"
done

image_names="$(jq -r '.[]' <<< "$images_json")"
while IFS= read -r image_name; do
  image_build_args=(
    --file "$repository_root/deploy/Dockerfile"
    --target "$image_name"
    --platform "$platform"
    --build-context "zinder-archived-binaries=${binary_context}"
    --build-arg "ZINDER_BINARIES_SOURCE=zinder-archived-binaries"
    --build-arg "SOURCE_DATE_EPOCH=${source_date_epoch}"
  )

  image_root="$work_directory/image-root"
  rm -rf "$image_root"
  docker buildx build \
    "${image_build_args[@]}" \
    --provenance=false \
    --sbom=false \
    --output "type=local,dest=${image_root}" \
    "$repository_root"
  carried_binaries=()
  for binary_name in "${archive_binaries[@]}"; do
    image_binary="${image_root}/usr/local/bin/${binary_name}"
    [[ -e "$image_binary" || -L "$image_binary" ]] || continue
    cmp -- "${binary_root}/${binary_name}" "$image_binary" || {
      echo >&2 "${image_name} carries a ${binary_name} that differs from the binary archive"
      exit 1
    }
    require_archive_binary_mode "$image_binary" "${image_name} binary ${binary_name}"
    carried_binaries+=("$binary_name")
  done
  [[ "${#carried_binaries[@]}" -gt 0 ]] || {
    echo >&2 "${image_name} carries no binary from the binary archive"
    exit 1
  }
  rm -rf "$image_root"

  case "$mode" in
    push) image_output="type=image,name=${registry}/${image_name},push-by-digest=true,name-canonical=true,push=true" ;;
    check) image_output="type=image,name=zinder-release-smoke/${image_name}:${platform_arch},push=false" ;;
  esac
  metadata_file="$work_directory/metadata-${image_name}.json"
  docker buildx build \
    "${image_build_args[@]}" \
    --label "org.opencontainers.image.source=https://github.com/${repository}" \
    --label "org.opencontainers.image.revision=${commit}" \
    --label "org.opencontainers.image.version=${release_tag}" \
    --label "org.opencontainers.image.licenses=MIT" \
    --provenance=mode=max \
    --sbom=true \
    --output "$image_output" \
    --metadata-file "$metadata_file" \
    "$repository_root"
  digest="$(jq -r '."containerimage.digest"' "$metadata_file")"
  [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || {
    echo >&2 "missing digest for ${image_name}"
    exit 1
  }

  if [[ "$mode" == push ]]; then
    mkdir -p "${digest_directory}/${image_name}"
    touch "${digest_directory}/${image_name}/${platform_arch}-${digest#sha256:}"
    continue
  fi

  # The zinder user runs every release process, including the processes that
  # the single-volume entrypoints start as root and then drop.
  smoke_image="zinder-release-smoke/${image_name}:${platform_arch}"
  docker buildx build \
    "${image_build_args[@]}" \
    --provenance=false \
    --sbom=false \
    --output "type=docker,name=${smoke_image}" \
    "$repository_root"
  smoke_images+=("$smoke_image")
  for binary_name in "${carried_binaries[@]}"; do
    image_binary_stat="$(
      docker run --rm --platform "$platform" --entrypoint stat "$smoke_image" \
        -c '%a %u:%g %F' "/usr/local/bin/${binary_name}"
    )"
    [[ "$image_binary_stat" == "${archive_binary_mode} 0:0 regular file" ]] || {
      echo >&2 "${image_name} installs ${binary_name} as '${image_binary_stat}', expected '${archive_binary_mode} 0:0 regular file'"
      exit 1
    }
    image_binary_version="$(
      docker run --rm --platform "$platform" --user zinder:zinder \
        --entrypoint "$binary_name" "$smoke_image" --version
    )"
    [[ "$image_binary_version" == "${binary_name} ${release_tag#v}" ]] || {
      echo >&2 "${image_name} reports '${image_binary_version}' for ${binary_name}, expected '${binary_name} ${release_tag#v}'"
      exit 1
    }
    docker run --rm --platform "$platform" --user zinder:zinder \
      --entrypoint "$binary_name" "$smoke_image" --help >/dev/null
  done
  echo "${image_name} (${platform_arch}): ${carried_binaries[*]} match the archive and run as zinder"
done <<< "$image_names"
