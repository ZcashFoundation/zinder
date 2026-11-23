#!/usr/bin/env bash
# offhost-download.sh <key_prefix> <name> <dest_parent>
# Fetches the manifest first and refuses a prefix without one, then the
# checksum list, then the bundle, and checks every file against the list.
# Run `zinder-projector verify` with the independently held pin afterwards.
# Verify hashes every file against the pinned manifest again, so for a private
# backup CHECK_FILES=0 skips this script's own pass over the payload.
set -euo pipefail
prefix=${1%/} name=$2 dest=$3
source "$(dirname "$0")/objstore.sh"
# A private backup carries the cursor authentication key, and object storage
# does not keep file modes, so everything downloaded is owner-only.
umask 077
[[ -d $(dirname "$dest") ]] || { echo "parent of $dest does not exist" >&2; exit 1; }
[[ ! -e $dest/$name ]] || { echo "$dest/$name already exists; choose a new destination" >&2; exit 1; }
[[ -d $dest ]] || mkdir -m 0700 "$dest"
[[ $(stat -c %u "$dest") == "$(id -u)" && $(( 0$(stat -c %a "$dest") & 077 )) == 0 ]] \
  || { echo "$dest must be owned by $(id -un) and closed to other users" >&2; exit 1; }
cd "$dest"
obj_get "$prefix/$name.manifest.json" "$name.manifest.json"
obj_get "$prefix/$name.SHA256SUMS" "$name.SHA256SUMS"
obj_get "$prefix/$name.SHA256SUMS.sha256" "$name.SHA256SUMS.sha256"
want=$(jq -er .bundle.sha256sums_sha256 "$name.manifest.json")
[[ $(sha256sum "$name.SHA256SUMS" | cut -d' ' -f1) == "$want" && $(cat "$name.SHA256SUMS.sha256") == "$want" ]] \
  || { echo "checksum list does not match the manifest" >&2; exit 1; }
t0=$(date +%s.%N)
obj_get_dir "$prefix/bundle/$name/" "$name"
t1=$(date +%s.%N)
if [[ ${CHECK_FILES:-1} != 0 ]]; then
  (cd "$name" && sha256sum --quiet -c "../$name.SHA256SUMS")
fi
t2=$(date +%s.%N)
awk -v a="$t0" -v b="$t1" -v c="$t2" -v n="$(wc -l < "$name.SHA256SUMS")" \
  'BEGIN{printf "{\"files\":%d,\"download_seconds\":%.1f,\"check_seconds\":%.1f}\n", n, b-a, c-b}'
