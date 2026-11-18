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
archive_binary_listing_mode=-rwxr-xr-x
# The commit timestamp, so a rebuilt image reports the same creation time.
image_created="$(date -u -d "@${source_date_epoch}" '+%Y-%m-%dT%H:%M:%SZ')"

work_directory="$(mktemp -d "${TMPDIR:-/tmp}/zinder-release-images.XXXXXX")"
smoke_images=()
smoke_containers=()
cleanup() {
  if [[ "${#smoke_containers[@]}" -gt 0 ]]; then
    docker container rm --force "${smoke_containers[@]}" >/dev/null 2>&1 || true
  fi
  if [[ "${#smoke_images[@]}" -gt 0 ]]; then
    docker image rm --force "${smoke_images[@]}" >/dev/null 2>&1 || true
  fi
  rm -rf -- "$work_directory"
}
trap cleanup EXIT

# Sets image_description and image_documentation for one catalog image. An
# image without an entry fails the packaging, so a new catalog name needs one.
describe_image() {
  local documentation_path
  case "$1" in
    zinder-ingest)
      image_description="Zinder canonical ingest: follows a Zcash node and writes the canonical chain store."
      documentation_path=docs/runbooks/deploying-on-a-vm.md
      ;;
    zinder-projector)
      image_description="Zinder wallet projector: builds the wallet projection from the canonical chain store."
      documentation_path=docs/runbooks/deploying-on-a-vm.md
      ;;
    zinder-query)
      image_description="Zinder native WalletQuery gRPC server for wallets and applications."
      documentation_path=docs/runbooks/deploying-on-a-vm.md
      ;;
    zinder-compat-lightwalletd)
      image_description="Zinder lightwalletd-compatible gRPC server for existing light wallets."
      documentation_path=docs/runbooks/deploying-on-a-vm.md
      ;;
    zinder-explorer)
      image_description="Zinder ExplorerQuery gRPC server for block explorers."
      documentation_path=docs/runbooks/deploying-on-a-vm.md
      ;;
    zinder-canonical-runtime)
      image_description="Zinder ingest-only canary runtime for one volume; it serves no client protocol."
      documentation_path=docs/runbooks/deploying-single-volume.md
      ;;
    zinder-wallet-serving)
      image_description="Zinder ingest, wallet projector, native WalletQuery, and lightwalletd compatibility supervised together on one volume."
      documentation_path=docs/runbooks/deploying-single-volume.md
      ;;
    zinder-wallet-serving-explorer)
      image_description="Zinder wallet serving and the ExplorerQuery server supervised together on one volume."
      documentation_path=docs/runbooks/deploying-single-volume.md
      ;;
    *)
      echo >&2 "no image description for ${1}; add one to describe_image"
      exit 1
      ;;
  esac
  [[ -f "${repository_root}/${documentation_path}" ]] || {
    echo >&2 "${1} documentation ${documentation_path} does not exist"
    exit 1
  }
  image_documentation="https://github.com/${repository}/blob/${release_tag}/${documentation_path}"
}

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
  describe_image "$image_name"
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
    --label "org.opencontainers.image.title=${image_name}" \
    --label "org.opencontainers.image.description=${image_description}" \
    --label "org.opencontainers.image.url=https://github.com/${repository}" \
    --label "org.opencontainers.image.documentation=${image_documentation}" \
    --label "org.opencontainers.image.vendor=Zcash Foundation" \
    --label "org.opencontainers.image.created=${image_created}" \
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

  # UID 10001, the zinder user, runs every release process, including the
  # processes that the single-volume entrypoints start as root and then drop.
  smoke_image="zinder-release-smoke/${image_name}:${platform_arch}"
  docker buildx build \
    "${image_build_args[@]}" \
    --provenance=false \
    --sbom=false \
    --output "type=docker,name=${smoke_image}" \
    "$repository_root"
  smoke_images+=("$smoke_image")
  # Per-service images carry no shell or stat, so the installed type, mode,
  # and owner come from an export of the image filesystem.
  smoke_container="$(docker container create --platform "$platform" "$smoke_image")"
  smoke_containers+=("$smoke_container")
  smoke_filesystem="$work_directory/smoke-filesystem.tar"
  docker container export --output "$smoke_filesystem" "$smoke_container"
  for binary_name in "${carried_binaries[@]}"; do
    image_binary_listing="$(
      tar --list --verbose --numeric-owner --file "$smoke_filesystem" \
        "usr/local/bin/${binary_name}" | awk '{ print $1, $2 }'
    )"
    [[ "$image_binary_listing" == "${archive_binary_listing_mode} 0/0" ]] || {
      echo >&2 "${image_name} installs ${binary_name} as '${image_binary_listing}', expected '${archive_binary_listing_mode} 0/0'"
      exit 1
    }
    image_binary_version="$(
      docker run --rm --platform "$platform" --user 10001:10001 \
        --entrypoint "$binary_name" "$smoke_image" --version
    )"
    [[ "$image_binary_version" == "${binary_name} ${release_tag#v}" ]] || {
      echo >&2 "${image_name} reports '${image_binary_version}' for ${binary_name}, expected '${binary_name} ${release_tag#v}'"
      exit 1
    }
    docker run --rm --platform "$platform" --user 10001:10001 \
      --entrypoint "$binary_name" "$smoke_image" --help >/dev/null
    # Port 1 has no listener, so the healthcheck must report unhealthy (1),
    # not a usage error (2) from a binary without the subcommand.
    healthcheck_status=0
    docker run --rm --platform "$platform" --user 10001:10001 \
      --entrypoint "$binary_name" "$smoke_image" healthcheck --addr 127.0.0.1:1 \
      >/dev/null 2>&1 || healthcheck_status=$?
    [[ "$healthcheck_status" -eq 1 ]] || {
      echo >&2 "${image_name} ${binary_name} healthcheck against a closed port exited ${healthcheck_status}, expected 1"
      exit 1
    }
  done
  rm -f -- "$smoke_filesystem"
  echo "${image_name} (${platform_arch}): ${carried_binaries[*]} match the archive and run as UID 10001"
done <<< "$image_names"
