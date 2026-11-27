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
  local baseline_version_file="$2"
  {
    cat "$repository_root/.changes/header.tpl.md"
    printf '\n\n## [Unreleased]\n\n'
    cat "$baseline_version_file"
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
cp "$repository_root/.changes/v0.27.0.md" "$fixture_repository/.changes/v0.27.0.md"
cp "$repository_root/.changes/unreleased/.gitkeep" \
  "$fixture_repository/.changes/unreleased/.gitkeep"
write_baseline_changelog \
  "$fixture_repository/CHANGELOG.md" \
  "$repository_root/.changes/v0.4.0.md"

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
  Upgrade: compatible
YAML
git -C "$fixture_repository" add .changes/unreleased/added-wallet-query.yaml
git -C "$fixture_repository" commit -qm "add release note"
fragment_commit="$(git -C "$fixture_repository" rev-parse HEAD)"
run_validator fragments "$fixture_repository"
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
  Upgrade: compatible
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
git -C "$fixture_repository" commit -qm "remove malformed fragment"
clean_commit="$(git -C "$fixture_repository" rev-parse HEAD)"

# Missing custom.Upgrade must fail in both fragments and pr modes.
cat > "$fixture_repository/.changes/unreleased/missing-upgrade.yaml" <<'YAML'
kind: added
body: Add a setting that omits custom.Upgrade.
time: 2026-07-21T19:00:00Z
custom:
  Bump: minor
  PR: "25"
YAML
git -C "$fixture_repository" add .changes/unreleased/missing-upgrade.yaml
git -C "$fixture_repository" commit -qm "add fragment missing Upgrade"
missing_upgrade_commit="$(git -C "$fixture_repository" rev-parse HEAD)"
expect_rejected \
  "fragment missing Upgrade (fragments)" \
  run_validator fragments "$fixture_repository"
expect_rejected \
  "fragment missing Upgrade (pr)" \
  run_validator pr \
  "$clean_commit" \
  "$missing_upgrade_commit" \
  "$empty_body" \
  "$fixture_repository"
git -C "$fixture_repository" rm -q .changes/unreleased/missing-upgrade.yaml
git -C "$fixture_repository" commit -qm "cleanup missing Upgrade"
clean_commit="$(git -C "$fixture_repository" rev-parse HEAD)"

# Empty custom.Upgrade must fail.
cat > "$fixture_repository/.changes/unreleased/empty-upgrade.yaml" <<'YAML'
kind: added
body: Add a setting with an empty custom.Upgrade field.
time: 2026-07-21T19:05:00Z
custom:
  Bump: minor
  PR: "26"
  Upgrade: ""
YAML
expect_rejected \
  "fragment with empty Upgrade" \
  run_validator fragments "$fixture_repository"
rm "$fixture_repository/.changes/unreleased/empty-upgrade.yaml"

# Top-level Upgrade outside custom: must fail.
cat > "$fixture_repository/.changes/unreleased/toplevel-upgrade.yaml" <<'YAML'
kind: added
body: Place Upgrade outside the custom mapping.
time: 2026-07-21T19:06:00Z
Upgrade: compatible
custom:
  Bump: minor
  PR: "27"
YAML
expect_rejected \
  "fragment with top-level Upgrade outside custom" \
  run_validator fragments "$fixture_repository"
rm "$fixture_repository/.changes/unreleased/toplevel-upgrade.yaml"

# Duplicate custom.Upgrade key must fail.
cat > "$fixture_repository/.changes/unreleased/duplicate-upgrade.yaml" <<'YAML'
kind: added
body: Declare duplicate custom.Upgrade keys.
time: 2026-07-21T19:07:00Z
custom:
  Bump: minor
  PR: "28"
  Upgrade: compatible
  Upgrade: canonical-layout
YAML
expect_rejected \
  "fragment with duplicate custom.Upgrade" \
  run_validator fragments "$fixture_repository"
rm "$fixture_repository/.changes/unreleased/duplicate-upgrade.yaml"

# Unknown custom.Upgrade must fail in both fragments and pr modes.
cat > "$fixture_repository/.changes/unreleased/unknown-upgrade.yaml" <<'YAML'
kind: changed
body: Use an unknown custom.Upgrade value.
time: 2026-07-21T19:10:00Z
custom:
  Bump: minor
  PR: "29"
  Upgrade: breaking
YAML
git -C "$fixture_repository" add .changes/unreleased/unknown-upgrade.yaml
git -C "$fixture_repository" commit -qm "add fragment with unknown Upgrade"
unknown_upgrade_commit="$(git -C "$fixture_repository" rev-parse HEAD)"
expect_rejected \
  "fragment with unknown Upgrade (fragments)" \
  run_validator fragments "$fixture_repository"
expect_rejected \
  "fragment with unknown Upgrade (pr)" \
  run_validator pr \
  "$clean_commit" \
  "$unknown_upgrade_commit" \
  "$empty_body" \
  "$fixture_repository"
git -C "$fixture_repository" rm -q .changes/unreleased/unknown-upgrade.yaml
git -C "$fixture_repository" commit -qm "cleanup unknown Upgrade"
clean_commit="$(git -C "$fixture_repository" rev-parse HEAD)"

# Case-mismatched custom.Upgrade must fail.
cat > "$fixture_repository/.changes/unreleased/case-upgrade.yaml" <<'YAML'
kind: changed
body: Use a capitalized Upgrade enum value.
time: 2026-07-21T19:12:00Z
custom:
  Bump: patch
  PR: "30"
  Upgrade: Compatible
YAML
expect_rejected \
  "fragment with capitalized Upgrade" \
  run_validator fragments "$fixture_repository"
rm "$fixture_repository/.changes/unreleased/case-upgrade.yaml"

# Multiline plain-scalar continuation on custom.Upgrade must fail.
cat > "$fixture_repository/.changes/unreleased/multiline-upgrade.yaml" <<'YAML'
kind: changed
body: Split an unknown Upgrade enum value across two indented lines.
time: 2026-07-21T19:13:00Z
custom:
  Bump: patch
  PR: "30"
  Upgrade: compatible
    future
YAML
git -C "$fixture_repository" add .changes/unreleased/multiline-upgrade.yaml
git -C "$fixture_repository" commit -qm "add fragment with multiline plain Upgrade scalar"
multiline_upgrade_commit="$(git -C "$fixture_repository" rev-parse HEAD)"
expect_rejected \
  "fragment with multiline plain Upgrade scalar (fragments)" \
  run_validator fragments "$fixture_repository"
expect_rejected \
  "fragment with multiline plain Upgrade scalar (pr)" \
  run_validator pr \
  "$clean_commit" \
  "$multiline_upgrade_commit" \
  "$empty_body" \
  "$fixture_repository"
git -C "$fixture_repository" rm -q .changes/unreleased/multiline-upgrade.yaml
git -C "$fixture_repository" commit -qm "cleanup multiline Upgrade"
clean_commit="$(git -C "$fixture_repository" rev-parse HEAD)"

# Unquoted flow-mapping key without space after colon must not satisfy Upgrade.
cat > "$fixture_repository/.changes/unreleased/flow-unseparated-upgrade.yaml" <<'YAML'
kind: fixed
body: Put Upgrade without space after the colon inside a flow mapping.
time: 2026-07-21T19:14:00Z
custom: { Bump: patch, PR: "24", Upgrade:compatible }
YAML
git -C "$fixture_repository" add .changes/unreleased/flow-unseparated-upgrade.yaml
git -C "$fixture_repository" commit -qm "add fragment with Upgrade:compatible in flow mapping"
flow_unseparated_commit="$(git -C "$fixture_repository" rev-parse HEAD)"
expect_rejected \
  "fragment with Upgrade:compatible in flow mapping (fragments)" \
  run_validator fragments "$fixture_repository"
expect_rejected \
  "fragment with Upgrade:compatible in flow mapping (pr)" \
  run_validator pr \
  "$clean_commit" \
  "$flow_unseparated_commit" \
  "$empty_body" \
  "$fixture_repository"
git -C "$fixture_repository" rm -q .changes/unreleased/flow-unseparated-upgrade.yaml
git -C "$fixture_repository" commit -qm "cleanup flow-unseparated Upgrade"
clean_commit="$(git -C "$fixture_repository" rev-parse HEAD)"

# Quoted continuation line inside another custom field must not spoof custom.Upgrade.
cat > "$fixture_repository/.changes/unreleased/quoted-notes-with-upgrade.yaml" <<'YAML'
kind: changed
body: Put Upgrade continuation inside another custom field's quoted scalar.
time: 2026-07-21T19:14:10Z
custom:
  Bump: patch
  Notes: "Mention
  Upgrade: compatible"
  PR: "24"
YAML
git -C "$fixture_repository" add .changes/unreleased/quoted-notes-with-upgrade.yaml
git -C "$fixture_repository" commit -qm "add fragment with Upgrade text inside quoted Notes"
quoted_notes_commit="$(git -C "$fixture_repository" rev-parse HEAD)"
expect_rejected \
  "fragment with Upgrade text inside quoted custom.Notes (fragments)" \
  run_validator fragments "$fixture_repository"
expect_rejected \
  "fragment with Upgrade text inside quoted custom.Notes (pr)" \
  run_validator pr \
  "$clean_commit" \
  "$quoted_notes_commit" \
  "$empty_body" \
  "$fixture_repository"
git -C "$fixture_repository" rm -q .changes/unreleased/quoted-notes-with-upgrade.yaml
git -C "$fixture_repository" commit -qm "cleanup quoted Notes Upgrade"
clean_commit="$(git -C "$fixture_repository" rev-parse HEAD)"

# Escape sequences in quoted Upgrade must be decoded or preserved verbatim, never stripped.
cat > "$fixture_repository/.changes/unreleased/bad-escape-block-upgrade.yaml" <<'YAML'
kind: fixed
body: Reject YAML escape sequences inside block Upgrade value.
time: 2026-07-21T19:14:20Z
custom:
  Bump: patch
  PR: "24"
  Upgrade: "compatibl\e"
YAML
git -C "$fixture_repository" add .changes/unreleased/bad-escape-block-upgrade.yaml
git -C "$fixture_repository" commit -qm "add fragment with escaped Upgrade in block mapping"
bad_escape_block_commit="$(git -C "$fixture_repository" rev-parse HEAD)"
expect_rejected \
  "fragment with escaped Upgrade in block mapping (fragments)" \
  run_validator fragments "$fixture_repository"
expect_rejected \
  "fragment with escaped Upgrade in block mapping (pr)" \
  run_validator pr \
  "$clean_commit" \
  "$bad_escape_block_commit" \
  "$empty_body" \
  "$fixture_repository"
git -C "$fixture_repository" rm -q .changes/unreleased/bad-escape-block-upgrade.yaml
git -C "$fixture_repository" commit -qm "cleanup bad-escape block Upgrade"
clean_commit="$(git -C "$fixture_repository" rev-parse HEAD)"

cat > "$fixture_repository/.changes/unreleased/bad-escape-flow-upgrade.yaml" <<'YAML'
kind: fixed
body: Reject YAML escape sequences inside flow Upgrade value.
time: 2026-07-21T19:14:30Z
custom: { Bump: patch, PR: "25", Upgrade: "compati\ble" }
YAML
git -C "$fixture_repository" add .changes/unreleased/bad-escape-flow-upgrade.yaml
git -C "$fixture_repository" commit -qm "add fragment with escaped Upgrade in flow mapping"
bad_escape_flow_commit="$(git -C "$fixture_repository" rev-parse HEAD)"
expect_rejected \
  "fragment with escaped Upgrade in flow mapping (fragments)" \
  run_validator fragments "$fixture_repository"
expect_rejected \
  "fragment with escaped Upgrade in flow mapping (pr)" \
  run_validator pr \
  "$clean_commit" \
  "$bad_escape_flow_commit" \
  "$empty_body" \
  "$fixture_repository"
git -C "$fixture_repository" rm -q .changes/unreleased/bad-escape-flow-upgrade.yaml
git -C "$fixture_repository" commit -qm "cleanup bad-escape flow Upgrade"
clean_commit="$(git -C "$fixture_repository" rev-parse HEAD)"

# Block-scalar Upgrade values with leading/trailing spaces or unstripped newline must fail.
printf '%s\n' \
  'kind: fixed' \
  'body: Reject block-scalar Upgrade values with trailing spaces.' \
  'time: 2026-07-21T19:14:40Z' \
  'custom:' \
  '  Bump: patch' \
  '  PR: "26"' \
  '  Upgrade: |-' \
  '    compatible   ' \
  > "$fixture_repository/.changes/unreleased/block-trailing-space-upgrade.yaml"
git -C "$fixture_repository" add .changes/unreleased/block-trailing-space-upgrade.yaml
git -C "$fixture_repository" commit -qm "add fragment with trailing-space block Upgrade"
block_trailing_commit="$(git -C "$fixture_repository" rev-parse HEAD)"
expect_rejected \
  "fragment with trailing-space block Upgrade (fragments)" \
  run_validator fragments "$fixture_repository"
expect_rejected \
  "fragment with trailing-space block Upgrade (pr)" \
  run_validator pr \
  "$clean_commit" \
  "$block_trailing_commit" \
  "$empty_body" \
  "$fixture_repository"
git -C "$fixture_repository" rm -q .changes/unreleased/block-trailing-space-upgrade.yaml
git -C "$fixture_repository" commit -qm "cleanup trailing-space block Upgrade"
clean_commit="$(git -C "$fixture_repository" rev-parse HEAD)"

cat > "$fixture_repository/.changes/unreleased/block-leading-space-upgrade.yaml" <<'YAML'
kind: fixed
body: Reject block-scalar Upgrade values with leading spaces.
time: 2026-07-21T19:14:45Z
custom:
  Bump: patch
  PR: "27"
  Upgrade: |-2
      compatible
YAML
git -C "$fixture_repository" add .changes/unreleased/block-leading-space-upgrade.yaml
git -C "$fixture_repository" commit -qm "add fragment with leading-space block Upgrade"
block_leading_commit="$(git -C "$fixture_repository" rev-parse HEAD)"
expect_rejected \
  "fragment with leading-space block Upgrade (fragments)" \
  run_validator fragments "$fixture_repository"
expect_rejected \
  "fragment with leading-space block Upgrade (pr)" \
  run_validator pr \
  "$clean_commit" \
  "$block_leading_commit" \
  "$empty_body" \
  "$fixture_repository"
git -C "$fixture_repository" rm -q .changes/unreleased/block-leading-space-upgrade.yaml
git -C "$fixture_repository" commit -qm "cleanup leading-space block Upgrade"
clean_commit="$(git -C "$fixture_repository" rev-parse HEAD)"

cat > "$fixture_repository/.changes/unreleased/block-clip-newline-upgrade.yaml" <<'YAML'
kind: fixed
body: Reject clipped block-scalar Upgrade carrying a trailing newline.
time: 2026-07-21T19:14:50Z
custom:
  Bump: patch
  PR: "28"
  Upgrade: |
    compatible
YAML
git -C "$fixture_repository" add .changes/unreleased/block-clip-newline-upgrade.yaml
git -C "$fixture_repository" commit -qm "add fragment with clip block Upgrade"
block_clip_commit="$(git -C "$fixture_repository" rev-parse HEAD)"
expect_rejected \
  "fragment with clip block Upgrade (fragments)" \
  run_validator fragments "$fixture_repository"
expect_rejected \
  "fragment with clip block Upgrade (pr)" \
  run_validator pr \
  "$clean_commit" \
  "$block_clip_commit" \
  "$empty_body" \
  "$fixture_repository"
git -C "$fixture_repository" rm -q .changes/unreleased/block-clip-newline-upgrade.yaml
git -C "$fixture_repository" commit -qm "cleanup clip block Upgrade"
clean_commit="$(git -C "$fixture_repository" rev-parse HEAD)"

# Folded block-scalar Upgrade with leading blank line must fail.
cat > "$fixture_repository/.changes/unreleased/folded-leading-blank-upgrade.yaml" <<'YAML'
kind: fixed
body: Reject folded block-scalar Upgrade with a leading blank line.
time: 2026-07-21T19:14:52Z
custom:
  Bump: patch
  PR: "29"
  Upgrade: >-

    compatible
YAML
git -C "$fixture_repository" add .changes/unreleased/folded-leading-blank-upgrade.yaml
git -C "$fixture_repository" commit -qm "add fragment with folded leading blank Upgrade"
folded_leading_blank_commit="$(git -C "$fixture_repository" rev-parse HEAD)"
expect_rejected \
  "fragment with folded leading blank Upgrade (fragments)" \
  run_validator fragments "$fixture_repository"
expect_rejected \
  "fragment with folded leading blank Upgrade (pr)" \
  run_validator pr \
  "$clean_commit" \
  "$folded_leading_blank_commit" \
  "$empty_body" \
  "$fixture_repository"
git -C "$fixture_repository" rm -q .changes/unreleased/folded-leading-blank-upgrade.yaml
git -C "$fixture_repository" commit -qm "cleanup folded leading blank Upgrade"
clean_commit="$(git -C "$fixture_repository" rev-parse HEAD)"

# Literal block-scalar Upgrade with trailing whitespace-only line more indented than block indent must fail.
printf '%s\n' \
  'kind: fixed' \
  'body: Reject literal block-scalar Upgrade with trailing indented spaces.' \
  'time: 2026-07-21T19:14:54Z' \
  'custom:' \
  '  Bump: patch' \
  '  PR: "30"' \
  '  Upgrade: |-' \
  '    compatible' \
  '      ' \
  > "$fixture_repository/.changes/unreleased/block-trailing-indented-blank-upgrade.yaml"
git -C "$fixture_repository" add .changes/unreleased/block-trailing-indented-blank-upgrade.yaml
git -C "$fixture_repository" commit -qm "add fragment with trailing indented spaces in block Upgrade"
block_trailing_indented_commit="$(git -C "$fixture_repository" rev-parse HEAD)"
expect_rejected \
  "fragment with trailing indented spaces in block Upgrade (fragments)" \
  run_validator fragments "$fixture_repository"
expect_rejected \
  "fragment with trailing indented spaces in block Upgrade (pr)" \
  run_validator pr \
  "$clean_commit" \
  "$block_trailing_indented_commit" \
  "$empty_body" \
  "$fixture_repository"
git -C "$fixture_repository" rm -q .changes/unreleased/block-trailing-indented-blank-upgrade.yaml
git -C "$fixture_repository" commit -qm "cleanup trailing indented spaces in block Upgrade"
clean_commit="$(git -C "$fixture_repository" rev-parse HEAD)"

# Non-scalar custom.Upgrade must fail.
cat > "$fixture_repository/.changes/unreleased/non-scalar-upgrade.yaml" <<'YAML'
kind: fixed
body: Reject non-scalar custom.Upgrade values.
time: 2026-07-21T19:14:56Z
custom:
  Bump: patch
  PR: "30"
  Upgrade: [compatible]
YAML
expect_rejected \
  "fragment with non-scalar Upgrade" \
  run_validator fragments "$fixture_repository"
rm "$fixture_repository/.changes/unreleased/non-scalar-upgrade.yaml"

# Duplicate top-level custom mapping must fail.
cat > "$fixture_repository/.changes/unreleased/duplicate-custom.yaml" <<'YAML'
kind: fixed
body: Reject duplicate top-level custom mappings.
time: 2026-07-21T19:14:58Z
custom:
  Bump: patch
  PR: "30"
  Upgrade: compatible
custom:
  Bump: patch
  PR: "30"
  Upgrade: canonical-layout
YAML
expect_rejected \
  "fragment with duplicate top-level custom mapping" \
  run_validator fragments "$fixture_repository"
rm "$fixture_repository/.changes/unreleased/duplicate-custom.yaml"

# projection-layout missing projection name must fail in both fragments and pr modes.
cat > "$fixture_repository/.changes/unreleased/proj-no-name.yaml" <<'YAML'
kind: changed
body: Rebuild the secondary projection in a fresh path and roll back via snapshot restore.
time: 2026-07-21T19:15:00Z
custom:
  Bump: minor
  PR: "31"
  Upgrade: projection-layout
YAML
git -C "$fixture_repository" add .changes/unreleased/proj-no-name.yaml
git -C "$fixture_repository" commit -qm "projection-layout missing projection name"
proj_no_name_commit="$(git -C "$fixture_repository" rev-parse HEAD)"
expect_rejected \
  "projection-layout missing projection name (fragments)" \
  run_validator fragments "$fixture_repository"
expect_rejected \
  "projection-layout missing projection name (pr)" \
  run_validator pr \
  "$clean_commit" \
  "$proj_no_name_commit" \
  "$empty_body" \
  "$fixture_repository"
git -C "$fixture_repository" rm -q .changes/unreleased/proj-no-name.yaml
git -C "$fixture_repository" commit -qm "cleanup proj-no-name"
clean_commit="$(git -C "$fixture_repository" rev-parse HEAD)"

# projection-layout naming wallet/explorer but missing rebuild/rollback guidance must fail.
cat > "$fixture_repository/.changes/unreleased/proj-no-guidance.yaml" <<'YAML'
kind: changed
body: Change wallet projection SST encoding to version 3.
time: 2026-07-21T19:20:00Z
custom:
  Bump: minor
  PR: "32"
  Upgrade: projection-layout
YAML
git -C "$fixture_repository" add .changes/unreleased/proj-no-guidance.yaml
git -C "$fixture_repository" commit -qm "projection-layout missing guidance"
proj_no_guidance_commit="$(git -C "$fixture_repository" rev-parse HEAD)"
expect_rejected \
  "projection-layout missing rebuild/rollback guidance (fragments)" \
  run_validator fragments "$fixture_repository"
expect_rejected \
  "projection-layout missing rebuild/rollback guidance (pr)" \
  run_validator pr \
  "$clean_commit" \
  "$proj_no_guidance_commit" \
  "$empty_body" \
  "$fixture_repository"
git -C "$fixture_repository" rm -q .changes/unreleased/proj-no-guidance.yaml
git -C "$fixture_repository" commit -qm "cleanup proj-no-guidance"
clean_commit="$(git -C "$fixture_repository" rev-parse HEAD)"

# Valid projection-layout variants (wallet, explorer, multiline block materialized-view)
# and valid canonical-layout without rebuild/rollback keywords must pass.
cat > "$fixture_repository/.changes/unreleased/proj-wallet.yaml" <<'YAML'
kind: changed
body: Change wallet projection SST layout; rebuild the wallet projection in a clean path and roll back using a matching backup or clean rebuild.
time: 2026-07-21T19:25:00Z
custom:
  Bump: minor
  PR: "33"
  Upgrade: projection-layout
YAML
cat > "$fixture_repository/.changes/unreleased/proj-explorer.yaml" <<'YAML'
kind: changed
body: |-
  Update Explorer materialized-view column families.
  Rebuild explorer views in a clean directory; roll back by restoring a snapshot.
time: 2026-07-21T19:26:00Z
custom: { Bump: patch, PR: "34", Upgrade: projection-layout }
YAML
cat > "$fixture_repository/.changes/unreleased/proj-wrapped-rollback.yaml" <<'YAML'
kind: changed
body: "Update wallet projection layout; rebuild the wallet projection in a clean path and roll
  back by restoring a matching backup."
time: 2026-07-21T19:26:30Z
custom:
  Bump: minor
  PR: "34"
  Upgrade: projection-layout
YAML
cat > "$fixture_repository/.changes/unreleased/canonical-minimal.yaml" <<'YAML'
kind: changed
body: Cut serving over to ZSHRNG02 canonical settled segments and remove ZSHRNG01 catalogs.
time: 2026-07-21T19:27:00Z
custom:
  Bump: minor
  PR: "35"
  Upgrade: canonical-layout
YAML
run_validator fragments "$fixture_repository"
rm \
  "$fixture_repository/.changes/unreleased/proj-wallet.yaml" \
  "$fixture_repository/.changes/unreleased/proj-explorer.yaml" \
  "$fixture_repository/.changes/unreleased/proj-wrapped-rollback.yaml" \
  "$fixture_repository/.changes/unreleased/canonical-minimal.yaml"

# Historical pre-v0.28.0 release sections without Upgrade class remain accepted.
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

# Real historical v0.27.0 section without Upgrade class is also accepted.
write_baseline_changelog \
  "$fixture_repository/CHANGELOG.md" \
  "$repository_root/.changes/v0.27.0.md"
run_validator release v0.27.0 "$fixture_repository"

cp "$valid_changelog" "$fixture_repository/CHANGELOG.md"
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
  Upgrade: compatible
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

# >= v0.28.0 release-section validation checks.
cat > "$fixture_repository/CHANGELOG.md" <<'MARKDOWN'
# Changelog

## [Unreleased]

## [0.28.0] - 2026-11-28

**Upgrade class:** `compatible` — Reversible by redeploying the previous release against the same store and schema.

### Added

- Serve query endpoints cleanly. ([#40](https://github.com/ZcashFoundation/zinder/pull/40))
MARKDOWN
run_validator release v0.28.0 "$fixture_repository"

# Missing Upgrade class in >= v0.28.0 must fail.
cat > "$fixture_repository/CHANGELOG.md" <<'MARKDOWN'
# Changelog

## [Unreleased]

## [0.28.0] - 2026-11-28

### Added

- Serve query endpoints cleanly. ([#40](https://github.com/ZcashFoundation/zinder/pull/40))
MARKDOWN
expect_rejected \
  "v0.28.0 missing Upgrade class line" \
  run_validator release v0.28.0 "$fixture_repository"

# Duplicate Upgrade class lines must fail.
cat > "$fixture_repository/CHANGELOG.md" <<'MARKDOWN'
# Changelog

## [Unreleased]

## [0.28.0] - 2026-11-28

**Upgrade class:** `compatible` — Reversible by redeploying the previous release against the same store and schema.
**Upgrade class:** `additive-schema` — Reversible by redeploying the previous release against the same store and schema.

### Added

- Serve query endpoints cleanly. ([#40](https://github.com/ZcashFoundation/zinder/pull/40))
MARKDOWN
expect_rejected \
  "v0.28.0 duplicate Upgrade class lines" \
  run_validator release v0.28.0 "$fixture_repository"

# Indented duplicate Upgrade class lines (1-3 leading spaces) must also fail.
cat > "$fixture_repository/CHANGELOG.md" <<'MARKDOWN'
# Changelog

## [Unreleased]

## [0.28.0] - 2026-11-28

**Upgrade class:** `canonical-layout` — Requires a fresh rebuild of the canonical store and dependent projections; see release entries below for layout changes and rollback steps.
  **Upgrade class:** `compatible` — Reversible by redeploying the previous release against the same store and schema.

### Added

- Serve query endpoints cleanly. ([#40](https://github.com/ZcashFoundation/zinder/pull/40))
MARKDOWN
expect_rejected \
  "v0.28.0 indented duplicate Upgrade class lines" \
  run_validator release v0.28.0 "$fixture_repository"

# Unknown Upgrade class in CHANGELOG.md must fail.
cat > "$fixture_repository/CHANGELOG.md" <<'MARKDOWN'
# Changelog

## [Unreleased]

## [0.28.0] - 2026-11-28

**Upgrade class:** `breaking` — Requires manual steps.

### Added

- Serve query endpoints cleanly. ([#40](https://github.com/ZcashFoundation/zinder/pull/40))
MARKDOWN
expect_rejected \
  "v0.28.0 unknown Upgrade class line" \
  run_validator release v0.28.0 "$fixture_repository"

# Malformed Upgrade class line without rollback statement must fail.
cat > "$fixture_repository/CHANGELOG.md" <<'MARKDOWN'
# Changelog

## [Unreleased]

## [0.28.0] - 2026-11-28

**Upgrade class:** `compatible`

### Added

- Serve query endpoints cleanly. ([#40](https://github.com/ZcashFoundation/zinder/pull/40))
MARKDOWN
expect_rejected \
  "v0.28.0 malformed Upgrade class line without rollback statement" \
  run_validator release v0.28.0 "$fixture_repository"

# Upgrade class placed below ### Added instead of immediately under heading must fail.
cat > "$fixture_repository/CHANGELOG.md" <<'MARKDOWN'
# Changelog

## [Unreleased]

## [0.28.0] - 2026-11-28

### Added

**Upgrade class:** `compatible` — Reversible by redeploying the previous release against the same store and schema.

- Serve query endpoints cleanly. ([#40](https://github.com/ZcashFoundation/zinder/pull/40))
MARKDOWN
expect_rejected \
  "v0.28.0 Upgrade class placed after section subheading" \
  run_validator release v0.28.0 "$fixture_repository"

# Section containing ONLY Upgrade class and no release entries must fail.
cat > "$fixture_repository/CHANGELOG.md" <<'MARKDOWN'
# Changelog

## [Unreleased]

## [0.28.0] - 2026-11-28

**Upgrade class:** `compatible` — Reversible by redeploying the previous release against the same store and schema.

## [0.27.0] - 2026-11-26

- Previous release.
MARKDOWN
expect_rejected \
  "v0.28.0 section with only Upgrade class and no entries" \
  run_validator release v0.28.0 "$fixture_repository"

# End-to-end release preparation & extraction tests targeting v0.28.0.
prepare_repository="$temporary_directory/prepare-repository"
mkdir -p \
  "$prepare_repository/.changes/unreleased" \
  "$prepare_repository/crates/zinder-client/src" \
  "$prepare_repository/crates/zinder-core/src" \
  "$prepare_repository/crates/zinder-proto/src" \
  "$prepare_repository/scripts"
cp "$repository_root/.changie.yaml" "$prepare_repository/.changie.yaml"
cp "$repository_root/.changes/header.tpl.md" "$prepare_repository/.changes/header.tpl.md"
cp "$repository_root/.changes/v0.27.0.md" "$prepare_repository/.changes/v0.27.0.md"
cp "$repository_root/.changes/unreleased/.gitkeep" \
  "$prepare_repository/.changes/unreleased/.gitkeep"
write_baseline_changelog \
  "$prepare_repository/CHANGELOG.md" \
  "$repository_root/.changes/v0.27.0.md"
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
version = "0.28.0"
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
zinder-core = { path = "../zinder-core", version = "0.28.0" }
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
zinder-core = { path = "../zinder-core", version = "0.28.0" }
zinder-proto = { path = "../zinder-proto", version = "0.28.0" }
TOML
for package_name in zinder-client zinder-core zinder-proto; do
  printf '# %s\n' "$package_name" \
    > "$prepare_repository/crates/$package_name/README.md"
  cat > "$prepare_repository/crates/$package_name/src/lib.rs" <<'RUST'
pub fn example() {}
RUST
done

git -C "$prepare_repository" init -q -b main
git -C "$prepare_repository" config user.email "changelog-policy@example.invalid"
git -C "$prepare_repository" config user.name "Changelog Policy Test"
git -C "$prepare_repository" add .
git -C "$prepare_repository" commit -qm "baseline 0.27.0"
prep_base_commit="$(git -C "$prepare_repository" rev-parse HEAD)"

# Partly-waived release: commit 1 is an internal change using the exact waiver.
printf '// internal comment\n' >> "$prepare_repository/crates/zinder-core/src/lib.rs"
git -C "$prepare_repository" add crates/zinder-core/src/lib.rs
git -C "$prepare_repository" commit -qm "chore: internal-only cleanup"
waived_commit="$(git -C "$prepare_repository" rev-parse HEAD)"
expect_rejected \
  "waived PR without declaration" \
  run_validator pr "$prep_base_commit" "$waived_commit" "$empty_body" "$prepare_repository"
run_validator pr "$prep_base_commit" "$waived_commit" "$exact_no_note_body" "$prepare_repository"

# Fragment missing Upgrade must be rejected by prepare-changelog-release.sh before mutating CHANGELOG.md.
cat > "$prepare_repository/.changes/unreleased/fixed-example.yaml" <<'YAML'
kind: fixed
body: Correct an operator-visible release defect.
time: 2026-11-27T10:00:00Z
custom:
  Bump: patch
  PR: "24"
YAML
baseline_prepare_digest="$(sha256sum "$prepare_repository/CHANGELOG.md")"
expect_rejected \
  "prepare-changelog-release.sh with fragment missing Upgrade" \
  env CHANGIE_BIN="$changie_bin" \
  bash "$prepare_repository/scripts/prepare-changelog-release.sh" 0.28.0
[[ "$(sha256sum "$prepare_repository/CHANGELOG.md")" == "$baseline_prepare_digest" ]] \
  || fail "prepare-changelog-release.sh modified CHANGELOG.md after rejecting missing Upgrade"

printf '  Upgrade: compatible\n' >> "$prepare_repository/.changes/unreleased/fixed-example.yaml"

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
  bash "$prepare_repository/scripts/prepare-changelog-release.sh" 0.28.0
sed -i 's/Bump: patch/Bump: minor/' \
  "$prepare_repository/.changes/unreleased/fixed-example.yaml"
git -C "$prepare_repository" add .changes/unreleased/fixed-example.yaml
git -C "$prepare_repository" commit -qm "add compatible release fragment"
fragment_pr_commit="$(git -C "$prepare_repository" rev-parse HEAD)"
run_validator pr "$waived_commit" "$fragment_pr_commit" "$empty_body" "$prepare_repository"

set_prepare_product_version 0.27.1
expect_rejected \
  "release version that differs from the workspace product version" \
  env CHANGIE_BIN="$changie_bin" \
  bash "$prepare_repository/scripts/prepare-changelog-release.sh" 0.28.0

set_prepare_product_version 0.28.0-rc.1
env CHANGIE_BIN="$changie_bin" \
  bash "$prepare_repository/scripts/prepare-changelog-release.sh" \
    0.28.0-rc.1 >/dev/null
run_validator release v0.28.0-rc.1 "$prepare_repository"
require_canonical_changelog_end \
  "$prepare_repository/CHANGELOG.md" \
  "RC1"
[[ -f "$prepare_repository/.changes/v0.28.0-rc.1.md" ]] \
  || fail "RC1 preparation did not archive its release notes"
[[ -n "$(find "$prepare_repository/.changes/unreleased" -type f -name '*.yaml' -print -quit)" ]] \
  || fail "RC1 preparation consumed its release-note fragments"

# shellcheck disable=SC2016
expected_compatible_line='**Upgrade class:** `compatible` — Reversible by redeploying the previous release against the same store and schema.'
grep -Fqx "$expected_compatible_line" "$prepare_repository/.changes/v0.28.0-rc.1.md" \
  || fail "RC1 archive omitted expected compatible Upgrade class line"
grep -Fqx "$expected_compatible_line" "$prepare_repository/CHANGELOG.md" \
  || fail "RC1 CHANGELOG.md omitted expected compatible Upgrade class line"
rc1_extracted="$temporary_directory/rc1-extracted.md"
bash "$extractor" 0.28.0-rc.1 "$prepare_repository/CHANGELOG.md" > "$rc1_extracted"
grep -Fqx "$expected_compatible_line" "$rc1_extracted" \
  || fail "RC1 extracted notes omitted expected compatible Upgrade class line"

rc1_digest="$(sha256sum "$prepare_repository/CHANGELOG.md")"
env CHANGIE_BIN="$changie_bin" \
  bash "$prepare_repository/scripts/prepare-changelog-release.sh" \
    0.28.0-rc.1 >/dev/null
[[ "$(sha256sum "$prepare_repository/CHANGELOG.md")" == "$rc1_digest" ]] \
  || fail "repeated RC1 preparation changed CHANGELOG.md"

# Prerelease mismatch between CHANGELOG.md / archive and retained fragments must fail.
cp "$prepare_repository/CHANGELOG.md" "$temporary_directory/rc1-changelog.bak"
# shellcheck disable=SC2016
sed -i 's/`compatible`/`additive-schema`/' "$prepare_repository/CHANGELOG.md"
expect_rejected \
  "prerelease CHANGELOG.md Upgrade class mismatch" \
  run_validator release v0.28.0-rc.1 "$prepare_repository"
cp "$temporary_directory/rc1-changelog.bak" "$prepare_repository/CHANGELOG.md"

cp "$prepare_repository/.changes/v0.28.0-rc.1.md" "$temporary_directory/rc1-archive.bak"
# shellcheck disable=SC2016
sed -i 's/`compatible`/`additive-schema`/' "$prepare_repository/.changes/v0.28.0-rc.1.md"
expect_rejected \
  "prerelease archive Upgrade class mismatch" \
  run_validator release v0.28.0-rc.1 "$prepare_repository"
cp "$temporary_directory/rc1-archive.bak" "$prepare_repository/.changes/v0.28.0-rc.1.md"

# Mixed-fragment precedence escalation: compatible -> additive-schema (RC2).
cat > "$prepare_repository/.changes/unreleased/changed-rc-additive.yaml" <<'YAML'
kind: changed
body: Add an optional column readable by older and newer releases.
time: 2026-11-27T11:00:00Z
custom:
  Bump: patch
  PR: "31"
  Upgrade: additive-schema
YAML
expect_rejected \
  "stale RC1 archive with a newly added fragment" \
  run_validator release v0.28.0-rc.1 "$prepare_repository"

set_prepare_product_version 0.28.0-rc.2
env CHANGIE_BIN="$changie_bin" \
  bash "$prepare_repository/scripts/prepare-changelog-release.sh" \
    0.28.0-rc.2 >/dev/null
run_validator release v0.28.0-rc.2 "$prepare_repository"
require_canonical_changelog_end \
  "$prepare_repository/CHANGELOG.md" \
  "RC2"
[[ ! -e "$prepare_repository/.changes/v0.28.0-rc.1.md" ]] \
  || fail "RC2 preparation retained the superseded RC1 archive"
if grep -Fq '## [0.28.0-rc.1] - ' "$prepare_repository/CHANGELOG.md"; then
  fail "RC2 preparation retained the superseded RC1 changelog section"
fi
[[ -f "$prepare_repository/.changes/v0.28.0-rc.2.md" ]] \
  || fail "RC2 preparation did not archive its cumulative release notes"
# shellcheck disable=SC2016
expected_additive_line='**Upgrade class:** `additive-schema` — Reversible by redeploying the previous release against the same store and schema.'
grep -Fqx "$expected_additive_line" "$prepare_repository/.changes/v0.28.0-rc.2.md" \
  || fail "RC2 archive did not escalate to additive-schema"
grep -Fq 'Correct an operator-visible release defect.' \
  "$prepare_repository/.changes/v0.28.0-rc.2.md" \
  || fail "RC2 archive omitted the RC1 release note"
grep -Fq 'Add an optional column readable by older and newer releases.' \
  "$prepare_repository/.changes/v0.28.0-rc.2.md" \
  || fail "RC2 archive omitted the newly added additive-schema note"
rc2_digest="$(sha256sum "$prepare_repository/CHANGELOG.md")"
env CHANGIE_BIN="$changie_bin" \
  bash "$prepare_repository/scripts/prepare-changelog-release.sh" \
    0.28.0-rc.2 >/dev/null
[[ "$(sha256sum "$prepare_repository/CHANGELOG.md")" == "$rc2_digest" ]] \
  || fail "repeated RC2 preparation changed CHANGELOG.md"

# Escalate further: additive-schema -> projection-layout (RC3).
cat > "$prepare_repository/.changes/unreleased/changed-rc-projection.yaml" <<'YAML'
kind: changed
body: Update wallet projection table layout; rebuild the wallet projection in a clean path and roll back from a matching backup.
time: 2026-11-27T12:00:00Z
custom:
  Bump: patch
  PR: "32"
  Upgrade: projection-layout
YAML
set_prepare_product_version 0.28.0-rc.3
env CHANGIE_BIN="$changie_bin" \
  bash "$prepare_repository/scripts/prepare-changelog-release.sh" \
    0.28.0-rc.3 >/dev/null
run_validator release v0.28.0-rc.3 "$prepare_repository"
# shellcheck disable=SC2016
expected_projection_line='**Upgrade class:** `projection-layout` — Requires a fresh rebuild of the affected projection; see release entries below for affected projections and rollback steps.'
grep -Fqx "$expected_projection_line" "$prepare_repository/.changes/v0.28.0-rc.3.md" \
  || fail "RC3 archive did not escalate to projection-layout"
rc3_extracted="$temporary_directory/rc3-extracted.md"
bash "$extractor" 0.28.0-rc.3 "$prepare_repository/CHANGELOG.md" > "$rc3_extracted"
grep -Fqx "$expected_projection_line" "$rc3_extracted" \
  || fail "RC3 extracted notes omitted projection-layout line"

# Escalate to canonical-layout with all four classes mixed for stable v0.28.0.
cat > "$prepare_repository/.changes/unreleased/changed-canonical.yaml" <<'YAML'
kind: changed
body: Bump canonical store schema to v3; build fresh stores in clean paths before cutover.
time: 2026-11-27T13:00:00Z
custom:
  Bump: patch
  PR: "33"
  Upgrade: canonical-layout
YAML
set_prepare_product_version 0.28.0
env CHANGIE_BIN="$changie_bin" \
  bash "$prepare_repository/scripts/prepare-changelog-release.sh" 0.28.0 >/dev/null
run_validator release v0.28.0 "$prepare_repository"
require_canonical_changelog_end \
  "$prepare_repository/CHANGELOG.md" \
  "stable"
[[ ! -e "$prepare_repository/.changes/v0.28.0-rc.3.md" ]] \
  || fail "stable preparation retained the superseded RC3 archive"
if grep -Fq '## [0.28.0-rc.3] - ' "$prepare_repository/CHANGELOG.md"; then
  fail "stable preparation retained the superseded RC3 changelog section"
fi
[[ -f "$prepare_repository/.changes/v0.28.0.md" ]] \
  || fail "stable preparation did not archive its release notes"
[[ -z "$(find "$prepare_repository/.changes/unreleased" -type f -name '*.yaml' -print -quit)" ]] \
  || fail "stable preparation left pending fragments"

# shellcheck disable=SC2016
expected_canonical_line='**Upgrade class:** `canonical-layout` — Requires a fresh rebuild of the canonical store and dependent projections; see release entries below for layout changes and rollback steps.'
release_date="$(sed -n 's/^## \[0\.28\.0\] - //p' "$prepare_repository/.changes/v0.28.0.md")"
[[ -n "$release_date" ]] || fail "could not read v0.28.0 release date"

stable_extracted="$temporary_directory/stable-extracted.md"
stable_expected="$temporary_directory/stable-expected.md"
bash "$extractor" 0.28.0 "$prepare_repository/CHANGELOG.md" > "$stable_extracted"
cat > "$stable_expected" <<MARKDOWN
## [0.28.0] - ${release_date}

${expected_canonical_line}

### Changed

- Add an optional column readable by older and newer releases. ([#31](https://github.com/ZcashFoundation/zinder/pull/31))
- Update wallet projection table layout; rebuild the wallet projection in a clean path and roll back from a matching backup. ([#32](https://github.com/ZcashFoundation/zinder/pull/32))
- Bump canonical store schema to v3; build fresh stores in clean paths before cutover. ([#33](https://github.com/ZcashFoundation/zinder/pull/33))

### Fixed

- Correct an operator-visible release defect. ([#24](https://github.com/ZcashFoundation/zinder/pull/24))
MARKDOWN
cmp "$stable_expected" "$stable_extracted" \
  || fail "extracted v0.28.0 release notes did not match expected canonical-layout output"

stable_digest="$(sha256sum "$prepare_repository/CHANGELOG.md")"
env CHANGIE_BIN="$changie_bin" \
  bash "$prepare_repository/scripts/prepare-changelog-release.sh" 0.28.0 >/dev/null
[[ "$(sha256sum "$prepare_repository/CHANGELOG.md")" == "$stable_digest" ]] \
  || fail "repeated stable preparation changed CHANGELOG.md"

echo "changelog policy tests passed"
