#!/usr/bin/env bash
# hash-tree.sh <dir> <out.sha256> [parallel]
# Reads every regular file once (hydrating lazily restored blocks) and records
# SHA-256 with paths relative to <dir>, sorted.
set -euo pipefail
dir=$1 out=$2 par=${3:-16}
cd "$dir"
find . -type f ! -name LOCK -print0 | xargs -0 -P "$par" -n 8 sha256sum | sed 's#  \./#  #' | sort -k2 > "$out"
wc -l < "$out"
