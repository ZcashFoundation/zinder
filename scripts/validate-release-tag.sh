#!/usr/bin/env bash
set -euo pipefail

fail() {
  echo >&2 "release tag rejected: $*"
  exit 1
}

if [[ $# -ne 1 ]]; then
  echo >&2 "usage: validate-release-tag.sh v<semver>"
  exit 2
fi

release_tag="$1"
[[ "$release_tag" == v* ]] || fail "tag must start with v"
release_version="${release_tag#v}"

semver_pattern='^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-([0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*))?$'
[[ "$release_version" =~ $semver_pattern ]] \
  || fail "tag must be a complete SemVer version without build metadata"

prerelease="${BASH_REMATCH[5]:-}"
if [[ -n "$prerelease" ]]; then
  IFS=. read -r -a prerelease_identifiers <<< "$prerelease"
  for identifier in "${prerelease_identifiers[@]}"; do
    if [[ "$identifier" =~ ^[0-9]+$ \
      && ${#identifier} -gt 1 \
      && "$identifier" == 0* ]]; then
      fail "numeric prerelease identifiers must not contain leading zeroes"
    fi
  done
fi

repository_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
metadata="$(cargo metadata --format-version 1 --no-deps --manifest-path "$repository_root/Cargo.toml")"
first_party_packages="$(
  jq '
    .workspace_members as $workspace_members
    | [
        .packages[]
        | select(.id as $id | $workspace_members | index($id))
      ]
  ' <<< "$metadata"
)"

package_count="$(jq 'length' <<< "$first_party_packages")"
[[ "$package_count" -gt 0 ]] || fail "Cargo metadata contains no first-party packages"

mapfile -t workspace_versions < <(jq -r 'map(.version) | unique[]' <<< "$first_party_packages")
[[ ${#workspace_versions[@]} -eq 1 ]] \
  || fail "first-party packages do not share one product version: ${workspace_versions[*]}"
workspace_version="${workspace_versions[0]}"
[[ "$release_version" == "$workspace_version" ]] \
  || fail "tag version $release_version does not match workspace version $workspace_version"

while IFS= read -r manifest_path; do
  relative_manifest="${manifest_path#"$repository_root"/}"
  grep -Fqx 'version.workspace = true' "$manifest_path" \
    || fail "$relative_manifest does not inherit the workspace version"
done < <(jq -r '.[].manifest_path' <<< "$first_party_packages")

expected_public_catalog='["zinder-client","zinder-core","zinder-proto"]'
public_catalog="$(
  jq -c '[.[] | select(.publish == ["crates-io"]) | .name] | sort' \
    <<< "$first_party_packages"
)"
[[ "$public_catalog" == "$expected_public_catalog" ]] \
  || fail "public crate catalog is $public_catalog, expected $expected_public_catalog"

unexpected_publishers="$(
  jq -r --argjson expected "$expected_public_catalog" '
    [
      .[]
      | select((.name as $name | $expected | index($name)) | not)
      | select(.publish != [])
      | .name
    ]
    | join(", ")
  ' <<< "$first_party_packages"
)"
[[ -z "$unexpected_publishers" ]] \
  || fail "non-SDK first-party packages are publishable: $unexpected_publishers"

bash "$repository_root/scripts/check-sdk-package-policy.sh" \
  || fail "public SDK package policy is not release-ready"

stable=true
[[ -z "$prerelease" ]] || stable=false
printf 'version=%s\n' "$release_version"
printf 'stable=%s\n' "$stable"
