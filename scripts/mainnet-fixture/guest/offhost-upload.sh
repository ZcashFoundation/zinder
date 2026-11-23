#!/usr/bin/env bash
# offhost-upload.sh <archive_dir> <key_prefix> <manifest_inputs.json>
# Uploads a sealed private backup directory to
# <key_prefix>/bundle/<name>/<upload-id>/, then its checksum list, then
# <name>.manifest.json last. A prefix without a manifest is a partial upload.
# Each upload writes under its own upload ID and the manifest is created only
# if absent, so concurrent uploads never touch the payload a manifest names.
# The checksum list comes from the archive's own per-file hashes, so the
# archive is not read again; download checks every file against it. For a
# directory that is not a private backup, such as node state, set SUMS_FILE
# to an existing sha256sum listing of it with paths relative to the directory.
set -euo pipefail
archive=$1 prefix=${2%/} inputs=$3
name=$(basename "$archive")
source "$(dirname "$0")/objstore.sh"
obj_exists "$prefix/$name.manifest.json" && { echo "manifest already published under $prefix; refusing to overwrite" >&2; exit 1; }
work=$(mktemp -d)
upload_id=$(date -u +%Y%m%dT%H%M%SZ)-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')
payload="bundle/$name/$upload_id"
sums_key="$name.$upload_id.SHA256SUMS"
if [[ -n ${SUMS_FILE:-} ]]; then
  sort -k2 "$SUMS_FILE" > "$work/$name.SHA256SUMS"
else
  "$(dirname "$0")/archive-sha256sums.py" "$archive" > "$work/$name.SHA256SUMS"
fi
sha256sum "$work/$name.SHA256SUMS" | cut -d' ' -f1 > "$work/$name.SHA256SUMS.sha256"
bytes=$(cd "$archive" && awk '{print $2}' "$work/$name.SHA256SUMS" | xargs -d '\n' stat -c %s | awk '{s+=$1} END{print s}')
files=$(wc -l < "$work/$name.SHA256SUMS")
t0=$(date +%s.%N)
if [[ -n ${SUMS_FILE:-} ]]; then
  # Upload only a directory whose regular files are exactly the listed ones.
  listing_name=$(basename "$SUMS_FILE")
  # RocksDB LOCK files are empty and never listed; compare everything else.
  diff -q <(awk '{print $2}' "$work/$name.SHA256SUMS" | grep -v -E '(^|/)LOCK$' | sort) \
    <(cd "$archive" && find . -type f ! -name LOCK ! -path "./$listing_name" | sed 's#^\./##' | sort) >/dev/null \
    || { echo "files under $archive differ from $SUMS_FILE" >&2; exit 1; }
  for top in $(awk '{split($2, p, "/"); print p[1]}' "$work/$name.SHA256SUMS" | sort -u); do
    if [[ -d $archive/$top ]]; then obj_put_dir "$archive/$top" "$prefix/$payload/$top/"
    else obj_create "$archive/$top" "$prefix/$payload/$top"; fi
  done
else
  obj_put_dir "$archive" "$prefix/$payload/"
fi
t1=$(date +%s.%N)
obj_create "$work/$name.SHA256SUMS" "$prefix/$sums_key"
obj_create "$work/$name.SHA256SUMS.sha256" "$prefix/$sums_key.sha256"
jq --arg name "$name" --argjson bytes "$bytes" --argjson files "$files" \
   --arg sums "$(cat "$work/$name.SHA256SUMS.sha256")" --arg produced_at "$(date -u +%FT%TZ)" \
   --arg url "$(obj_url "$prefix/")" --arg payload "$payload" --arg sums_key "$sums_key" --argjson upload_seconds "$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.1f", b-a}')" \
   '. + {location: $url, bundle: {name: $name, prefix: ($payload + "/"), files: $files, bytes: $bytes,
          sha256sums: $sums_key, sha256sums_sha256: $sums},
         produced_at: $produced_at, upload_seconds: $upload_seconds}' "$inputs" > "$work/$name.manifest.json"
obj_create "$work/$name.manifest.json" "$prefix/$name.manifest.json"
cat "$work/$name.manifest.json"
