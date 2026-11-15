#!/usr/bin/env bash
# Offline self-test for scripts/install-ci-tool.sh. It serves a fake release
# archive through a file:// URL and checks the checksum gate and lock parsing.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
installer="${repo_root}/scripts/install-ci-tool.sh"

scratch="$(mktemp -d "${TMPDIR:-/tmp}/zinder-install-ci-tool-test.XXXXXX")"
trap 'rm -rf -- "$scratch"' EXIT

mkdir -p "$scratch/pkg/fake-tool-1.0.0"
printf '#!/bin/sh\necho fake-tool 1.0.0\n' > "$scratch/pkg/fake-tool-1.0.0/fake-tool"
chmod +x "$scratch/pkg/fake-tool-1.0.0/fake-tool"
tar --create --gzip --file "$scratch/fake-tool.tar.gz" --directory "$scratch/pkg" fake-tool-1.0.0
good_sha256="$(sha256sum "$scratch/fake-tool.tar.gz" | cut -d' ' -f1)"
bad_sha256="$(printf '%064d' 0)"
archive_url="file://${scratch}/fake-tool.tar.gz"

run_installer() {
  local lock_file="$1" install_dir="$2" tool_name="$3"
  # Unset GITHUB_PATH so the fake tool's directory never leaks into the CI job PATH.
  env -u GITHUB_PATH \
    CI_TOOL_PLATFORM=Linux-x86_64 \
    CI_TOOLS_LOCK="$lock_file" \
    CI_TOOL_INSTALL_DIR="$install_dir" \
    CI_TOOL_ALLOWED_PROTOCOLS='=file' \
    "$installer" "$tool_name" 2> >(grep -v 'is not on PATH' >&2)
}

expect_failure() {
  local description="$1" expected="$2"
  shift 2
  if output="$("$@" 2>&1)"; then
    echo >&2 "FAIL: ${description}: installer succeeded"
    exit 1
  fi
  grep -Fq -- "$expected" <<< "$output" || {
    echo >&2 "FAIL: ${description}: expected '${expected}' in: ${output}"
    exit 1
  }
}

printf 'fake-tool 1.0.0 %s %s\nother-tool 2.0.0 %s %s\n' \
  "$archive_url" "$good_sha256" "$archive_url" "$bad_sha256" > "$scratch/good.lock"
run_installer "$scratch/good.lock" "$scratch/bin" fake-tool > /dev/null
[[ "$("$scratch/bin/fake-tool")" == "fake-tool 1.0.0" ]] \
  || { echo >&2 "FAIL: installed tool did not run"; exit 1; }

expect_failure "corrupted checksum" "sha256 mismatch" \
  run_installer "$scratch/good.lock" "$scratch/bin-bad" other-tool
[[ ! -e "$scratch/bin-bad/other-tool" ]] \
  || { echo >&2 "FAIL: a tool failing its checksum was installed"; exit 1; }

expect_failure "unlisted tool" "is not listed" \
  run_installer "$scratch/good.lock" "$scratch/bin-none" missing-tool

printf 'fake-tool 1.0.0 %s %s\nfake-tool 1.0.1 %s %s\n' \
  "$archive_url" "$good_sha256" "$archive_url" "$good_sha256" > "$scratch/duplicate.lock"
expect_failure "duplicate entry" "listed more than once" \
  run_installer "$scratch/duplicate.lock" "$scratch/bin-dup" fake-tool

printf 'fake-tool 1.0.0 %s not-a-checksum\n' "$archive_url" > "$scratch/malformed.lock"
expect_failure "malformed checksum" "64 lowercase hex" \
  run_installer "$scratch/malformed.lock" "$scratch/bin-malformed" fake-tool

expect_failure "protocol outside the allowed list" "Protocol" \
  env CI_TOOLS_LOCK="$scratch/good.lock" CI_TOOL_INSTALL_DIR="$scratch/bin-proto" \
  CI_TOOL_ALLOWED_PROTOCOLS='=https' "$installer" fake-tool

# The committed lock must stay well formed and cover every tool the workflows install.
for tool in cargo-nextest cargo-deny cargo-machete cargo-llvm-cov cargo-mutants; do
  [[ "$(grep -cE "^${tool}[[:space:]]+[0-9.]+[[:space:]]+https://[^[:space:]]+[[:space:]]+[0-9a-f]{64}\$" \
    "${repo_root}/.github/ci-tools.lock")" -eq 1 ]] \
    || { echo >&2 "FAIL: .github/ci-tools.lock needs exactly one well-formed ${tool} entry"; exit 1; }
done

echo "install-ci-tool tests passed"
