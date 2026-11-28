#!/usr/bin/env bash
set -euo pipefail

if [[ $# -gt 1 || ( $# -eq 1 && ! "$1" =~ ^v[0-9A-Za-z.-]+$ ) ]]; then
  echo >&2 "usage: generate-api-artifacts.sh [vRELEASE_VERSION]"
  exit 2
fi

repository_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$repository_root"

buf lint
buf generate
find dist/openapi -type f -name '*.yaml' -size +0c -print -quit \
  | grep -q . || {
    echo "OpenAPI generation produced no non-empty YAML artifact" >&2
    exit 1
  }
buf build --output dist/zinder.v1.descriptor.bin
test -s dist/zinder.v1.descriptor.bin

scripts/check-native-proto-closure.sh crates/zinder-proto
mkdir -p .tmp
staging_root="$(mktemp -d .tmp/zinder-api-artifacts.XXXXXX)"
trap 'rm -rf "$staging_root"' EXIT
cp -R crates/zinder-proto/proto "$staging_root/proto"
cp crates/zinder-proto/buf.release.yaml "$staging_root/buf.yaml"
cp crates/zinder-proto/buf.release.gen.yaml "$staging_root/buf.gen.yaml"
cp LICENSE "$staging_root/LICENSE"
buf build "$staging_root" --as-file-descriptor-set \
  --output "$staging_root/zinder.contract.descriptor.bin"

next_proto_dir="dist/.proto.next.$$"
previous_proto_dir="dist/.proto.previous.$$"
mv "$staging_root/proto" "$next_proto_dir"
if [[ -e dist/proto ]]; then
  mv dist/proto "$previous_proto_dir"
fi
mv "$next_proto_dir" dist/proto
rm -rf "$previous_proto_dir"
mv "$staging_root/"* dist/
scripts/check-native-proto-closure.sh dist

if [[ $# -eq 1 ]]; then
  archive="dist/zinder-api-docs-$1.tar.gz"
  tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner -C dist -cf - \
    LICENSE buf.yaml buf.gen.yaml openapi proto \
    zinder.v1.descriptor.bin zinder.contract.descriptor.bin \
    | gzip -n > "$staging_root/archive.tar.gz"
  mv "$staging_root/archive.tar.gz" "$archive"
  printf 'Built %s\n' "$archive"
fi
