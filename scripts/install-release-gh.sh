#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo >&2 "usage: install-release-gh.sh BIN_DIRECTORY"
  exit 2
}

[[ $# -eq 1 && -n "$1" ]] || usage
bin_directory="$1"
gh_version=2.105.0
gh_archive="gh_${gh_version}_linux_amd64.tar.gz"
gh_sha256=79e7ab5db738c48777b696e374f8b428f0647debfb424f44d2f426370407fe2f
gh_url="https://github.com/cli/cli/releases/download/v${gh_version}/${gh_archive}"

scratch="$(mktemp -d "${TMPDIR:-/tmp}/zinder-release-tools.XXXXXX")"
trap 'rm -rf -- "$scratch"' EXIT
archive_path="$scratch/$gh_archive"
if [[ -n "${ZINDER_GH_ARCHIVE:-}" ]]; then
  cp "$ZINDER_GH_ARCHIVE" "$archive_path"
else
  curl --fail --location --silent --show-error --proto '=https' --tlsv1.2 \
    --user-agent 'zinder-release-tool-installer' \
    --output "$archive_path" \
    "$gh_url"
fi

printf '%s  %s\n' "$gh_sha256" "$archive_path" | sha256sum --check --strict
tar --extract --gzip --file "$archive_path" --directory "$scratch"
gh_binary="$scratch/gh_${gh_version}_linux_amd64/bin/gh"
[[ -f "$gh_binary" ]] || {
  echo >&2 "verified GitHub CLI archive did not contain the expected binary"
  exit 1
}
mkdir -p "$bin_directory"
install -m 0755 "$gh_binary" "$bin_directory/gh"
"$bin_directory/gh" version | grep -Fq "gh version ${gh_version} "
