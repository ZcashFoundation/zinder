#!/usr/bin/env bash
# offhost-upload.sh <archive_dir> <key_prefix> <manifest_inputs.json>
# Uploads a sealed private backup directory to <key_prefix>/bundle/<name>/,
# then <name>.SHA256SUMS and <name>.SHA256SUMS.sha256, then
# <name>.manifest.json last. A prefix without a manifest is a partial upload.
# The checksum list comes from the archive's own per-file hashes, so the
# archive is not read again; download checks every file against it.
set -euo pipefail
archive=$1 prefix=${2%/} inputs=$3
name=$(basename "$archive")
source "$(dirname "$0")/objstore.sh"
obj_exists "$prefix/$name.manifest.json" && { echo "manifest already published under $prefix; refusing to overwrite" >&2; exit 1; }
work=$(mktemp -d)
"$(dirname "$0")/archive-sha256sums.py" "$archive" > "$work/$name.SHA256SUMS"
sha256sum "$work/$name.SHA256SUMS" | cut -d' ' -f1 > "$work/$name.SHA256SUMS.sha256"
bytes=$(cd "$archive" && find . -type f -printf '%s\n' | awk '{s+=$1} END{print s}')
files=$(wc -l < "$work/$name.SHA256SUMS")
t0=$(date +%s.%N)
obj_put_dir "$archive" "$prefix/bundle/$name/"
t1=$(date +%s.%N)
obj_put "$work/$name.SHA256SUMS" "$prefix/$name.SHA256SUMS"
obj_put "$work/$name.SHA256SUMS.sha256" "$prefix/$name.SHA256SUMS.sha256"
jq --arg name "$name" --argjson bytes "$bytes" --argjson files "$files" \
   --arg sums "$(cat "$work/$name.SHA256SUMS.sha256")" --arg produced_at "$(date -u +%FT%TZ)" \
   --arg url "$(obj_url "$prefix/")" --argjson upload_seconds "$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.1f", b-a}')" \
   '. + {location: $url, bundle: {name: $name, prefix: ("bundle/" + $name + "/"), files: $files, bytes: $bytes,
          sha256sums: ($name + ".SHA256SUMS"), sha256sums_sha256: $sums},
         produced_at: $produced_at, upload_seconds: $upload_seconds}' "$inputs" > "$work/$name.manifest.json"
obj_put "$work/$name.manifest.json" "$prefix/$name.manifest.json"
cat "$work/$name.manifest.json"
