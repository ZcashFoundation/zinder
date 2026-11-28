#!/usr/bin/env bash
# Verify each test predicate on its own, including exclusions inside a larger filter.
#
# Set NEXTEST_ARCHIVE_FILE to list tests from a `cargo nextest archive` instead
# of building the workspace. The archive must come from this source tree, and
# neither cargo nor rustc needs to be installed.
set -euo pipefail

scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
if [[ -n "${NEXTEST_ARCHIVE_FILE:-}" ]]; then
  mkdir "$scratch/archive"
  cargo-nextest nextest list --archive-file "$NEXTEST_ARCHIVE_FILE" \
    --extract-to "$scratch/archive" --workspace-remap "$PWD" \
    --list-type=binaries-only --message-format=json > /dev/null
  nextest=(cargo-nextest nextest)
  reuse_build=(
    --cargo-metadata "$scratch/archive/target/nextest/cargo-metadata.json"
    --binaries-metadata "$scratch/archive/target/nextest/binaries-metadata.json"
    --target-dir-remap "$scratch/archive/target"
    --workspace-remap "$PWD"
  )
else
  cargo metadata --locked --format-version=1 > "$scratch/cargo.json"
  cargo nextest list --locked --workspace \
    --list-type=binaries-only --message-format=json > "$scratch/binaries.json"
  nextest=(cargo nextest)
  reuse_build=(--cargo-metadata "$scratch/cargo.json" --binaries-metadata "$scratch/binaries.json")
fi

check_selectors() {
  local source=$1
  shift
  # Preserve regex predicates with parenthesized groups and escaped delimiters.
  sed '/^[[:space:]]*#/d' "$source" | grep -oE 'test\(/([^/\\]|\\.)*/\)|test\(([^)\\]|\\.)+\)' \
    | sort -u > "$scratch/selectors" || return 2
  local selector
  while IFS= read -r selector; do
    "${nextest[@]}" list "${reuse_build[@]}" \
      --run-ignored=all --message-format=oneline "$@" -E "$selector" > "$scratch/matches" || return 2
    if [[ ! -s "$scratch/matches" ]]; then
      echo "$source: no listed test matches $selector" >&2
      return 1
    fi
  done < "$scratch/selectors"
  printf '%s: verified %s nextest selectors\n' "$source" "$(wc -l < "$scratch/selectors")"
}

bogus='test(=__nonexistent_nextest_selector__)'
printf 'filter = "test(/^live::/) and not %s"\n' "$bogus" > "$scratch/regression.toml"
if check_selectors "$scratch/regression.toml" --ignore-default-filter 2> "$scratch/error"; then
  echo "Accepted an exclusion matching zero listed tests" >&2
  exit 1
fi
if ! grep -Fxq "$scratch/regression.toml: no listed test matches $bogus" "$scratch/error"; then
  cat "$scratch/error" >&2
  exit 1
fi

check_selectors .config/nextest.toml --ignore-default-filter
check_selectors .github/workflows/live-tests.yml --profile=ci-live
check_selectors .github/workflows/parity-compat.yml --profile=ci-live
check_selectors scripts/test-light-wallet-qualification.sh --profile=ci-live
