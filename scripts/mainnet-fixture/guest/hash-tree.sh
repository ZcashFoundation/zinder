#!/usr/bin/env bash
# hash-tree.sh <dir> <out.sha256> [parallel]
# Reads every regular file once (hydrating lazily restored blocks) and records
# SHA-256 with paths relative to <dir>, sorted.
set -euo pipefail
dir=$(realpath "$1") out=$(realpath -m "$2") par=${3:-16}
cd "$dir"
# The listing may live inside the tree it describes; never hash it.
skip=${out#"$dir"/}
tmp=$(mktemp)
find . -type f ! -name LOCK ! -path "./$skip" -print0 | xargs -0 -P "$par" -n 8 sha256sum \
  | sed 's#  \./#  #' | sort -k2 > "$tmp"
mv "$tmp" "$out"
wc -l < "$out"
