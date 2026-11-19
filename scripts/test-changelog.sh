#!/usr/bin/env bash
set -euo pipefail

repository_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
validator="$repository_root/scripts/validate-changelog.sh"
extractor="$repository_root/scripts/extract-release-notes.sh"
preparer="$repository_root/scripts/prepare-changelog-release.sh"
changie_bin="${CHANGIE_BIN:-changie}"

fail() {
  echo >&2 "changelog policy test failed: $*"
  exit 1
}

write_baseline_changelog() {
  local destination="$1"
  {
    cat "$repository_root/.changes/header.tpl.md"
    printf '\n\n## [Unreleased]\n\n'
    cat "$repository_root/.changes/v0.4.0.md"
  } > "$destination"
}

require_canonical_changelog_end() {
  local changelog_path="$1"
  local release_label="$2"
  [[ -n "$(tail -n 1 "$changelog_path")" ]] \
    || fail "$release_label preparation left a trailing blank line"
}

[[ -x "$validator" ]] || fail "missing executable validator: $validator"
[[ -x "$extractor" ]] || fail "missing executable release-note extractor: $extractor"
[[ -x "$preparer" ]] || fail "missing executable release preparer: $preparer"
[[ -f "$repository_root/.changie.yaml" ]] || fail "missing .changie.yaml"
"$changie_bin" --version >/dev/null

temporary_directory="$(mktemp -d "${TMPDIR:-/tmp}/zinder-changelog.XXXXXX")"
trap 'rm -rf -- "$temporary_directory"' EXIT
fixture_repository="$temporary_directory/repository"
mkdir -p "$fixture_repository/.changes/unreleased"
cp "$repository_root/.changie.yaml" "$fixture_repository/.changie.yaml"
cp "$repository_root/.changes/header.tpl.md" "$fixture_repository/.changes/header.tpl.md"
cp "$repository_root/.changes/v0.4.0.md" "$fixture_repository/.changes/v0.4.0.md"
cp "$repository_root/.changes/unreleased/.gitkeep" \
  "$fixture_repository/.changes/unreleased/.gitkeep"
write_baseline_changelog "$fixture_repository/CHANGELOG.md"

git -C "$fixture_repository" init -q -b main
git -C "$fixture_repository" config user.email "changelog-policy@example.invalid"
git -C "$fixture_repository" config user.name "Changelog Policy Test"
git -C "$fixture_repository" add .
git -C "$fixture_repository" commit -qm "baseline"
base_commit="$(git -C "$fixture_repository" rev-parse HEAD)"

empty_body="$temporary_directory/empty-body.md"
exact_no_note_body="$temporary_directory/exact-no-note-body.md"
inexact_no_note_body="$temporary_directory/inexact-no-note-body.md"
: > "$empty_body"
printf '%s\n' '- [x] No release note required' > "$exact_no_note_body"
printf '%s\n' '- [X] No release note required' > "$inexact_no_note_body"

run_validator() {
  CHANGIE_BIN="$changie_bin" bash "$validator" "$@"
}

expect_rejected() {
  local label="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    fail "$label was admitted"
  fi
}

cat > "$fixture_repository/.changes/unreleased/added-wallet-query.yaml" <<'YAML'
kind: added
body: Serve the native wallet query API alongside lightwalletd.
time: 2026-07-21T13:04:21Z
custom:
  Bump: minor
  PR: "22"
YAML
git -C "$fixture_repository" add .changes/unreleased/added-wallet-query.yaml
git -C "$fixture_repository" commit -qm "add release note"
fragment_commit="$(git -C "$fixture_repository" rev-parse HEAD)"
run_validator pr "$base_commit" "$fragment_commit" "$empty_body" "$fixture_repository"

run_validator pr \
  "$fragment_commit" \
  "$fragment_commit" \
  "$exact_no_note_body" \
  "$fixture_repository"
expect_rejected \
  "inexact no-release-note checkbox" \
  run_validator pr \
  "$fragment_commit" \
  "$fragment_commit" \
  "$inexact_no_note_body" \
  "$fixture_repository"

git -C "$fixture_repository" rm -q .changes/unreleased/added-wallet-query.yaml
git -C "$fixture_repository" commit -qm "delete release note"
deleted_fragment_commit="$(git -C "$fixture_repository" rev-parse HEAD)"
expect_rejected \
  "deleted release-note fragment" \
  run_validator pr \
  "$fragment_commit" \
  "$deleted_fragment_commit" \
  "$empty_body" \
  "$fixture_repository"

mkdir -p "$fixture_repository/.changes/unreleased"
cat > "$fixture_repository/.changes/unreleased/malformed.yaml" <<'YAML'
kind: unknown
body: This category is not configured.
time: 2026-07-21T18:32:51Z
custom:
  Bump: minor
  PR: "24"
YAML
git -C "$fixture_repository" add .changes/unreleased/malformed.yaml
git -C "$fixture_repository" commit -qm "add malformed release note"
malformed_commit="$(git -C "$fixture_repository" rev-parse HEAD)"
expect_rejected \
  "malformed Changie fragment" \
  run_validator pr \
  "$deleted_fragment_commit" \
  "$malformed_commit" \
  "$empty_body" \
  "$fixture_repository"

git -C "$fixture_repository" rm -q .changes/unreleased/malformed.yaml
cat > "$fixture_repository/CHANGELOG.md" <<'MARKDOWN'
# Changelog

All notable changes to Zinder are documented in this file.

## [Unreleased]

## [0.5.0] - 2026-07-21

### Added

- Serve the native wallet query API alongside lightwalletd. ([#22](https://github.com/gustavovalverde/zinder/pull/22))

### Fixed

- Preserve configured artifact-store contracts. ([#20](https://github.com/gustavovalverde/zinder/pull/20))

## [0.4.0] - 2026-07-11

- Previous release.
MARKDOWN
run_validator release v0.5.0 "$fixture_repository"
valid_changelog="$temporary_directory/valid-changelog.md"
cp "$fixture_repository/CHANGELOG.md" "$valid_changelog"

extracted_notes="$temporary_directory/extracted.md"
expected_notes="$temporary_directory/expected.md"
bash "$extractor" 0.5.0 "$fixture_repository/CHANGELOG.md" > "$extracted_notes"
cat > "$expected_notes" <<'MARKDOWN'
## [0.5.0] - 2026-07-21

### Added

- Serve the native wallet query API alongside lightwalletd. ([#22](https://github.com/gustavovalverde/zinder/pull/22))

### Fixed

- Preserve configured artifact-store contracts. ([#20](https://github.com/gustavovalverde/zinder/pull/20))
MARKDOWN
cmp "$expected_notes" "$extracted_notes" \
  || fail "release-note extraction included content outside version 0.5.0"

cat > "$fixture_repository/CHANGELOG.md" <<'MARKDOWN'
# Changelog

## [Unreleased]

## [0.5.0] - 2026-07-21

## [0.4.0] - 2026-07-11

- Previous release.
MARKDOWN
expect_rejected \
  "empty release section" \
  run_validator release v0.5.0 "$fixture_repository"

cp "$valid_changelog" "$fixture_repository/CHANGELOG.md"
cat >> "$fixture_repository/CHANGELOG.md" <<'MARKDOWN'

## [0.5.0] - 2026-07-21

- Duplicate release section.
MARKDOWN
expect_rejected \
  "duplicate release section" \
  run_validator release v0.5.0 "$fixture_repository"

cp "$valid_changelog" "$fixture_repository/CHANGELOG.md"

mkdir -p "$fixture_repository/.changes/unreleased"
cat > "$fixture_repository/.changes/unreleased/pending.yaml" <<'YAML'
kind: fixed
body: Keep pending work out of a tagged release.
time: 2026-07-21T18:32:51Z
custom:
  Bump: patch
  PR: "24"
YAML
expect_rejected \
  "release with pending fragments" \
  run_validator release v0.5.0 "$fixture_repository"

rm "$fixture_repository/.changes/unreleased/pending.yaml"
sed -i 's/## \[0\.5\.0\] - 2026-07-21/## [0.5.1] - 2026-07-21/' \
  "$fixture_repository/CHANGELOG.md"
expect_rejected \
  "release without an exact changelog section" \
  run_validator release v0.5.0 "$fixture_repository"

prepare_repository="$temporary_directory/prepare-repository"
mkdir -p \
  "$prepare_repository/.changes/unreleased" \
  "$prepare_repository/crates/zinder-client/src" \
  "$prepare_repository/crates/zinder-core/src" \
  "$prepare_repository/crates/zinder-proto/src" \
  "$prepare_repository/scripts"
cp "$repository_root/.changie.yaml" "$prepare_repository/.changie.yaml"
cp "$repository_root/.changes/header.tpl.md" "$prepare_repository/.changes/header.tpl.md"
cp "$repository_root/.changes/v0.4.0.md" "$prepare_repository/.changes/v0.4.0.md"
cp "$repository_root/.changes/unreleased/.gitkeep" \
  "$prepare_repository/.changes/unreleased/.gitkeep"
write_baseline_changelog "$prepare_repository/CHANGELOG.md"
cp "$preparer" "$prepare_repository/scripts/prepare-changelog-release.sh"
cp "$validator" "$prepare_repository/scripts/validate-changelog.sh"
cp "$repository_root/scripts/validate-release-tag.sh" \
  "$prepare_repository/scripts/validate-release-tag.sh"
cp "$repository_root/scripts/check-sdk-package-policy.sh" \
  "$prepare_repository/scripts/check-sdk-package-policy.sh"
cp "$repository_root/scripts/check-sdk-dependency-requirement.sh" \
  "$prepare_repository/scripts/check-sdk-dependency-requirement.sh"
cat > "$prepare_repository/Cargo.toml" <<'TOML'
[workspace]
members = [
  "crates/zinder-client",
  "crates/zinder-core",
  "crates/zinder-proto",
]
resolver = "3"

[workspace.package]
version = "0.5.0"
edition = "2024"
publish = false
rust-version = "1.100"
TOML
cat > "$prepare_repository/crates/zinder-core/Cargo.toml" <<'TOML'
[package]
name = "zinder-core"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
publish = ["crates-io"]
readme = "README.md"
include = ["src/**", "Cargo.toml", "README.md"]
TOML
cat > "$prepare_repository/crates/zinder-proto/Cargo.toml" <<'TOML'
[package]
name = "zinder-proto"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
publish = ["crates-io"]
readme = "README.md"
include = ["src/**", "Cargo.toml", "README.md"]

[dependencies]
zinder-core = { path = "../zinder-core", version = "0.5.0" }
TOML
cat > "$prepare_repository/crates/zinder-client/Cargo.toml" <<'TOML'
[package]
name = "zinder-client"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
publish = ["crates-io"]
readme = "README.md"
include = ["src/**", "Cargo.toml", "README.md"]

[dependencies]
zinder-core = { path = "../zinder-core", version = "0.5.0" }
zinder-proto = { path = "../zinder-proto", version = "0.5.0" }
TOML
for package_name in zinder-client zinder-core zinder-proto; do
  printf '# %s\n' "$package_name" \
    > "$prepare_repository/crates/$package_name/README.md"
  cat > "$prepare_repository/crates/$package_name/src/lib.rs" <<'RUST'
pub fn example() {}
RUST
done
cat > "$prepare_repository/.changes/unreleased/fixed-example.yaml" <<'YAML'
kind: fixed
body: Correct an operator-visible release defect.
time: 2026-07-21T18:32:51Z
custom:
  Bump: patch
  PR: "24"
YAML
git -C "$prepare_repository" init -q -b main

set_prepare_product_version() {
  local product_version="$1"
  while IFS= read -r -d '' manifest; do
    sed -i -E \
      -e \
      "s/^version = \"[^\"]+\"/version = \"${product_version}\"/" \
      -e \
      "s/(path = \"[^\"]+\", version = \")[^\"]+\"/\\1${product_version}\"/g" \
      "$manifest"
  done < <(
    find "$prepare_repository" \
      -name Cargo.toml \
      -type f \
      -print0
  )
}

expect_rejected \
  "explicit version that differs from changie next auto" \
  env CHANGIE_BIN="$changie_bin" \
  bash "$prepare_repository/scripts/prepare-changelog-release.sh" 0.5.0
sed -i 's/Bump: patch/Bump: minor/' \
  "$prepare_repository/.changes/unreleased/fixed-example.yaml"
set_prepare_product_version 0.5.1
expect_rejected \
  "release version that differs from the workspace product version" \
  env CHANGIE_BIN="$changie_bin" \
  bash "$prepare_repository/scripts/prepare-changelog-release.sh" 0.5.0
set_prepare_product_version 0.5.0-rc.1
env CHANGIE_BIN="$changie_bin" \
  bash "$prepare_repository/scripts/prepare-changelog-release.sh" \
    0.5.0-rc.1 >/dev/null
run_validator release v0.5.0-rc.1 "$prepare_repository"
require_canonical_changelog_end \
  "$prepare_repository/CHANGELOG.md" \
  "RC1"
[[ -f "$prepare_repository/.changes/v0.5.0-rc.1.md" ]] \
  || fail "RC1 preparation did not archive its release notes"
[[ -n "$(find "$prepare_repository/.changes/unreleased" -type f -name '*.yaml' -print -quit)" ]] \
  || fail "RC1 preparation consumed its release-note fragments"
rc1_digest="$(sha256sum "$prepare_repository/CHANGELOG.md")"
env CHANGIE_BIN="$changie_bin" \
  bash "$prepare_repository/scripts/prepare-changelog-release.sh" \
    0.5.0-rc.1 >/dev/null
[[ "$(sha256sum "$prepare_repository/CHANGELOG.md")" == "$rc1_digest" ]] \
  || fail "repeated RC1 preparation changed CHANGELOG.md"

cat > "$prepare_repository/.changes/unreleased/changed-rc-canary.yaml" <<'YAML'
kind: changed
body: Exercise the trusted publication path before the stable release.
time: 2026-07-22T18:32:51Z
custom:
  Bump: patch
  PR: "31"
YAML
expect_rejected \
  "stale RC1 archive with a newly added fragment" \
  run_validator release v0.5.0-rc.1 "$prepare_repository"

set_prepare_product_version 0.5.0-rc.2
env CHANGIE_BIN="$changie_bin" \
  bash "$prepare_repository/scripts/prepare-changelog-release.sh" \
    0.5.0-rc.2 >/dev/null
run_validator release v0.5.0-rc.2 "$prepare_repository"
require_canonical_changelog_end \
  "$prepare_repository/CHANGELOG.md" \
  "RC2"
[[ ! -e "$prepare_repository/.changes/v0.5.0-rc.1.md" ]] \
  || fail "RC2 preparation retained the superseded RC1 archive"
if grep -Fq '## [0.5.0-rc.1] - ' "$prepare_repository/CHANGELOG.md"; then
  fail "RC2 preparation retained the superseded RC1 changelog section"
fi
[[ -f "$prepare_repository/.changes/v0.5.0-rc.2.md" ]] \
  || fail "RC2 preparation did not archive its cumulative release notes"
grep -Fq 'Correct an operator-visible release defect.' \
  "$prepare_repository/.changes/v0.5.0-rc.2.md" \
  || fail "RC2 archive omitted the RC1 release note"
grep -Fq 'Exercise the trusted publication path before the stable release.' \
  "$prepare_repository/.changes/v0.5.0-rc.2.md" \
  || fail "RC2 archive omitted the newly added release note"
rc2_digest="$(sha256sum "$prepare_repository/CHANGELOG.md")"
env CHANGIE_BIN="$changie_bin" \
  bash "$prepare_repository/scripts/prepare-changelog-release.sh" \
    0.5.0-rc.2 >/dev/null
[[ "$(sha256sum "$prepare_repository/CHANGELOG.md")" == "$rc2_digest" ]] \
  || fail "repeated RC2 preparation changed CHANGELOG.md"

set_prepare_product_version 0.5.0
env CHANGIE_BIN="$changie_bin" \
  bash "$prepare_repository/scripts/prepare-changelog-release.sh" 0.5.0 >/dev/null
run_validator release v0.5.0 "$prepare_repository"
require_canonical_changelog_end \
  "$prepare_repository/CHANGELOG.md" \
  "stable"
[[ ! -e "$prepare_repository/.changes/v0.5.0-rc.2.md" ]] \
  || fail "stable preparation retained the superseded RC2 archive"
if grep -Fq '## [0.5.0-rc.2] - ' "$prepare_repository/CHANGELOG.md"; then
  fail "stable preparation retained the superseded RC2 changelog section"
fi
[[ -f "$prepare_repository/.changes/v0.5.0.md" ]] \
  || fail "stable preparation did not archive its release notes"
[[ -z "$(find "$prepare_repository/.changes/unreleased" -type f -name '*.yaml' -print -quit)" ]] \
  || fail "stable preparation left pending fragments"
stable_digest="$(sha256sum "$prepare_repository/CHANGELOG.md")"
env CHANGIE_BIN="$changie_bin" \
  bash "$prepare_repository/scripts/prepare-changelog-release.sh" 0.5.0 >/dev/null
[[ "$(sha256sum "$prepare_repository/CHANGELOG.md")" == "$stable_digest" ]] \
  || fail "repeated stable preparation changed CHANGELOG.md"

echo "changelog policy tests passed"
