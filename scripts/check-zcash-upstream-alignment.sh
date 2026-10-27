#!/usr/bin/env bash
set -euo pipefail

repository_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$repository_root"

zebra_runtime_image='docker.io/zfnd/zebra:7.0.0@sha256:a70d2be8ffef3a4b6e37355724e36a0e6c1838e46baa28bfb216739f00cf2d34'
zebra_runtime_pin_files=(
  .github/workflows/live-tests.yml
  .github/workflows/parity-compat.yml
  docs/reference/lightwalletd-compatibility.md
)

mapfile -t zebra_runtime_images < <(
  grep -hEo \
    '(docker\.io/)?zfnd/zebra:[0-9A-Za-z._-]+(@sha256:[a-f0-9]{64})?' \
    "${zebra_runtime_pin_files[@]}" |
    sort -u
)
if [[
  "${#zebra_runtime_images[@]}" -ne 1 ||
  "${zebra_runtime_images[0]:-}" != "$zebra_runtime_image"
]]; then
  echo >&2 \
    "Zebra runtime certification must use exactly $zebra_runtime_image; found: ${zebra_runtime_images[*]:-none}"
  exit 1
fi

workspace_tree="$(cargo tree --workspace --locked --prefix none)"
zebra_tree="$(cargo tree -p zebra-chain --locked --prefix none)"

resolved_versions() {
  local package_name="$1"
  local tree="$2"

  awk -v package_name="$package_name" '
    $1 == package_name && $2 ~ /^v/ {
      version = $2
      sub(/^v/, "", version)
      print version
    }
  ' <<<"$tree" | sort -u
}

for package_name in \
  incrementalmerkletree \
  orchard \
  sapling-crypto \
  secp256k1 \
  zcash_address \
  zcash_primitives \
  zcash_protocol \
  zcash_transparent \
  zip32
do
  mapfile -t workspace_versions < <(
    resolved_versions "$package_name" "$workspace_tree"
  )
  mapfile -t zebra_versions < <(
    resolved_versions "$package_name" "$zebra_tree"
  )

  if [[ "${#workspace_versions[@]}" -ne 1 ]]; then
    echo >&2 \
      "$package_name must resolve exactly once across the workspace; found: ${workspace_versions[*]:-none}"
    exit 1
  fi
  if [[ "${#zebra_versions[@]}" -ne 1 ]]; then
    echo >&2 \
      "zebra-chain must resolve exactly one $package_name version; found: ${zebra_versions[*]:-none}"
    exit 1
  fi
  if [[ "${workspace_versions[0]}" != "${zebra_versions[0]}" ]]; then
    echo >&2 \
      "$package_name ${workspace_versions[0]} drifts from zebra-chain's ${zebra_versions[0]}"
    echo >&2 \
      "advance Zebra and the shared Zcash protocol dependency stack together"
    exit 1
  fi
done
