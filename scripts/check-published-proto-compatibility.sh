#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo >&2 "usage: check-published-proto-compatibility.sh OWNER/REPO CANDIDATE_DESCRIPTOR"
  exit 2
fi

repo="$1"
candidate="$2"
test -s "$candidate"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT

gh api "repos/$repo/releases?per_page=100" --paginate --slurp > "$scratch/releases.json"
# shellcheck disable=SC2016
filter_carrying='[.[][] | select(.draft == false and .published_at != null) | select(.tag_name as $tag | any(.assets[]?; .name == ("zinder-api-docs-" + $tag + ".tar.gz")))]'
latest_any="$(jq -r "$filter_carrying | sort_by(.published_at) | last | .tag_name // empty" "$scratch/releases.json")"
latest_stable="$(jq -r "$filter_carrying | [.[] | select(.prerelease == false)] | sort_by(.published_at) | last | .tag_name // empty" "$scratch/releases.json")"

tags=()
if [[ -n "$latest_any" ]]; then
  tags+=("$latest_any")
fi
if [[ -n "$latest_stable" && "$latest_stable" != "$latest_any" ]]; then
  tags+=("$latest_stable")
fi

if [[ ${#tags[@]} -eq 0 ]]; then
  echo "No published release with contract archive to compare"
  exit 0
fi

check_release_tag() {
  local tag="$1"
  [[ "$tag" =~ ^v[0-9A-Za-z.-]+$ ]] || {
    echo >&2 "unsupported published release tag: $tag"
    exit 1
  }

  local tag_dir="$scratch/$tag"
  local archive="zinder-api-docs-$tag.tar.gz"
  mkdir -p "$tag_dir"
  gh release download "$tag" --repo "$repo" --pattern "$archive" --dir "$tag_dir"
  tar -tzf "$tag_dir/$archive" > "$tag_dir/members"

  echo "Checking protobuf compatibility against published $tag"
  if grep -Fxq buf.yaml "$tag_dir/members"; then
    tar -xOzf "$tag_dir/$archive" zinder.contract.descriptor.bin > "$tag_dir/previous.bin"
    buf breaking "$candidate" --against "$tag_dir/previous.bin" \
      --config crates/zinder-proto/buf.release.yaml
  else
    tar -xOzf "$tag_dir/$archive" zinder.v1.descriptor.bin > "$tag_dir/previous.bin"
    buf breaking --against "$tag_dir/previous.bin"
  fi
}

for tag in "${tags[@]}"; do
  check_release_tag "$tag"
done
