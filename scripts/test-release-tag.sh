#!/usr/bin/env bash
set -euo pipefail

repository_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
validator="$repository_root/scripts/validate-release-tag.sh"
dependency_requirement_validator="$repository_root/scripts/check-sdk-dependency-requirement.sh"
workspace_version="$(
  cargo metadata \
    --format-version 1 \
    --no-deps \
    --manifest-path "$repository_root/Cargo.toml" \
    | jq -r '
        .workspace_members as $workspace_members
        | [
            .packages[]
            | select(.id as $id | $workspace_members | index($id))
            | .version
          ]
        | unique
        | if length == 1 then .[0] else error("workspace versions diverged") end
      '
)"

fail() {
  echo >&2 "release tag test failed: $*"
  exit 1
}

expect_rejected() {
  local candidate="$1"
  if bash "$validator" "$candidate" >/dev/null 2>&1; then
    fail "$candidate was admitted"
  fi
}

validation_output="$(bash "$validator" "v${workspace_version}")"
grep -Fqx "version=${workspace_version}" <<< "$validation_output" \
  || fail "validator did not emit the workspace version"

expected_stable=true
[[ "$workspace_version" != *-* ]] || expected_stable=false
grep -Fqx "stable=${expected_stable}" <<< "$validation_output" \
  || fail "validator emitted the wrong stable-release classification"

base_version="${workspace_version%%-*}"
IFS=. read -r major minor patch <<< "$base_version"
wrong_patch_version="${major}.${minor}.$((patch + 1))"

expect_rejected "$workspace_version"
expect_rejected "v${wrong_patch_version}"
expect_rejected "v${base_version}+build.1"
expect_rejected "v01.2.3"
expect_rejected "v1.02.3"
expect_rejected "v1.2.03"
expect_rejected "v1.2.3-01"
expect_rejected "v1.2"
expect_rejected "vlatest"

bash "$dependency_requirement_validator" \
  "0.6.0-rc.1" \
  zinder-proto \
  zinder-core \
  "^0.6.0-rc.1"
if bash "$dependency_requirement_validator" \
  "0.6.0-rc.1" \
  zinder-proto \
  zinder-core \
  "^0.6.0" >/dev/null 2>&1; then
  fail "a stable SDK dependency requirement admitted a prerelease workspace"
fi

registry_verifier="$repository_root/scripts/verify-published-sdk.sh"
[[ -x "$registry_verifier" ]] \
  || fail "registry-only SDK consumer verifier is not executable"
if "$registry_verifier" "$wrong_patch_version" >/dev/null 2>&1; then
  fail "registry-only SDK consumer verifier admitted a different version"
fi

crate_publication_job="$(
  awk '
    $0 == "  publish-sdk-crates:" { in_job = 1; next }
    in_job && /^  [a-zA-Z0-9_-]+:/ { exit }
    in_job { print }
  ' "$repository_root/.github/workflows/release.yml"
)"
[[ -n "$crate_publication_job" ]] \
  || fail "release workflow has no SDK publication job"
if grep -Fq 'needs.validate.outputs.stable' <<< "$crate_publication_job"; then
  fail "SDK publication is incorrectly restricted to stable tags"
fi
