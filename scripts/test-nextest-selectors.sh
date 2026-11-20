#!/usr/bin/env bash
# Verify each test predicate on its own, including exclusions inside a larger filter.
set -euo pipefail

scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
cargo metadata --locked --format-version=1 > "$scratch/cargo.json"
cargo nextest list --locked --workspace --list-type=binaries-only --message-format=json > "$scratch/binaries.json"

check_selectors() {
  local source=$1
  shift
  # Preserve regex predicates with parenthesized groups and escaped delimiters.
  sed '/^[[:space:]]*#/d' "$source" | grep -oE 'test\(/([^/\\]|\\.)*/\)|test\(([^)\\]|\\.)+\)' \
    | sort -u > "$scratch/selectors" || return 2
  local selector
  while IFS= read -r selector; do
    cargo nextest list --cargo-metadata "$scratch/cargo.json" --binaries-metadata "$scratch/binaries.json" \
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
