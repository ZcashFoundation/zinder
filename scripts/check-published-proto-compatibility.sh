#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo >&2 "usage: check-published-proto-compatibility.sh OWNER/REPO CANDIDATE_DESCRIPTOR"
  exit 2
fi

repo="$1"
candidate="$2"
test -s "$candidate"
tag="$(gh api "repos/$repo/releases?per_page=100" --paginate --slurp \
  | jq -r '[.[][] | select(.draft == false and .published_at != null)]
           | sort_by([(.prerelease | not), .published_at]) | last | .tag_name // empty')"
if [[ -z "$tag" ]]; then
  echo "No published release to compare"
  exit 0
fi
[[ "$tag" =~ ^v[0-9A-Za-z.-]+$ ]] || {
  echo >&2 "unsupported published release tag: $tag"
  exit 1
}

scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
archive="zinder-api-docs-$tag.tar.gz"
gh release download "$tag" --repo "$repo" --pattern "$archive" --dir "$scratch"
tar -tzf "$scratch/$archive" > "$scratch/members"

echo "Checking protobuf compatibility against published $tag"
if grep -Fxq buf.yaml "$scratch/members"; then
  tar -xOzf "$scratch/$archive" zinder.contract.descriptor.bin > "$scratch/previous.bin"
  buf breaking "$candidate" --against "$scratch/previous.bin" \
    --config crates/zinder-proto/buf.release.yaml
else
  tar -xOzf "$scratch/$archive" zinder.v1.descriptor.bin > "$scratch/previous.bin"
  buf breaking --against "$scratch/previous.bin"
fi
