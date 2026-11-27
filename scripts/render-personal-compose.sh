#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 || ! "$1" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo >&2 "usage: render-personal-compose.sh ZINDER_IMAGE_DIGEST"
  exit 2
fi

repository_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
zinder_image="ghcr.io/zcashfoundation/zinder@$1"
manifest="$(
  awk -v image="$zinder_image" '
    /^  [^ ]/ { zinder = ($0 == "  zinder:") }
    zinder && /^    image:/ { $0 = "    image: " image; replaced++ }
    { print }
    END { if (replaced != 1) exit 1 }
  ' "$repository_root/deploy/personal/compose.yaml"
)"

docker compose --env-file /dev/null -f - config --format json <<< "$manifest" \
  | jq -e --arg zinder "$zinder_image" '
    .services.zinder.image == $zinder
    and (.services.zebra.image | test("@sha256:[0-9a-f]{64}$"))
  ' >/dev/null || {
  echo >&2 'personal Compose release manifest must pin both images by digest'
  exit 1
}

printf '%s\n' "$manifest"
