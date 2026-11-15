#!/usr/bin/env bash
# Installs one prebuilt CI tool from .github/ci-tools.lock.
#
# The lock pins the upstream release asset URL and the sha256 of the archive.
# The archive is verified before anything is extracted, and only the tool's own
# executable is taken from it.
#
# Environment:
#   CI_TOOLS_LOCK               lock file path (default: .github/ci-tools.lock)
#   CI_TOOL_INSTALL_DIR         install directory (default: ~/.cargo/bin)
#   CI_TOOL_ALLOWED_PROTOCOLS   curl --proto value (default: =https)
set -euo pipefail

usage() {
  echo >&2 "usage: install-ci-tool.sh TOOL_NAME"
  exit 2
}

[[ $# -eq 1 && -n "$1" ]] || usage
tool_name="$1"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
lock_file="${CI_TOOLS_LOCK:-${repo_root}/.github/ci-tools.lock}"
install_dir="${CI_TOOL_INSTALL_DIR:-${HOME}/.cargo/bin}"
allowed_protocols="${CI_TOOL_ALLOWED_PROTOCOLS:-=https}"

fail() {
  echo >&2 "install-ci-tool: $*"
  exit 1
}

[[ -f "$lock_file" ]] || fail "lock file not found: $lock_file"

case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) ;;
  *) fail "the lock pins x86_64 Linux assets only, not $(uname -s)-$(uname -m)" ;;
esac

tool_version=""
tool_url=""
tool_sha256=""
while read -r lock_name lock_version lock_url lock_sha256 lock_extra; do
  [[ -z "$lock_name" || "$lock_name" == \#* ]] && continue
  [[ "$lock_name" == "$tool_name" ]] || continue
  [[ -z "$tool_version" ]] || fail "$tool_name is listed more than once in $lock_file"
  [[ -n "$lock_version" && -n "$lock_url" && -n "$lock_sha256" && -z "$lock_extra" ]] \
    || fail "$tool_name needs exactly: name version url sha256"
  [[ "$lock_sha256" =~ ^[0-9a-f]{64}$ ]] \
    || fail "$tool_name sha256 must be 64 lowercase hex characters"
  tool_version="$lock_version"
  tool_url="$lock_url"
  tool_sha256="$lock_sha256"
done < "$lock_file"
[[ -n "$tool_version" ]] || fail "$tool_name is not listed in $lock_file"

scratch="$(mktemp -d "${TMPDIR:-/tmp}/zinder-ci-tool.XXXXXX")"
trap 'rm -rf -- "$scratch"' EXIT
archive_path="$scratch/archive.tar.gz"

curl --fail --location --silent --show-error \
  --proto "$allowed_protocols" --tlsv1.2 \
  --retry 3 --retry-delay 2 \
  --user-agent 'zinder-ci-tool-installer' \
  --output "$archive_path" \
  "$tool_url"

printf '%s  %s\n' "$tool_sha256" "$archive_path" | sha256sum --check --strict --status \
  || fail "sha256 mismatch for $tool_name $tool_version from $tool_url (expected $tool_sha256)"

member="$(tar --list --gzip --file "$archive_path" | grep -E "(^|/)${tool_name}\$" || true)"
[[ -n "$member" && "$(printf '%s\n' "$member" | wc -l)" -eq 1 ]] \
  || fail "the verified archive must contain exactly one file named $tool_name"
tar --extract --gzip --no-same-owner --file "$archive_path" --directory "$scratch" -- "$member"

mkdir -p "$install_dir"
install -m 0755 "$scratch/$member" "$install_dir/$tool_name"
echo "installed $tool_name $tool_version to $install_dir/$tool_name"

# Make the directory visible to later steps when it is not already on PATH.
case ":${PATH}:" in
  *":${install_dir}:"*) ;;
  *)
    if [[ -n "${GITHUB_PATH:-}" ]]; then
      echo "$install_dir" >> "$GITHUB_PATH"
    else
      echo >&2 "install-ci-tool: $install_dir is not on PATH"
    fi
    ;;
esac
