#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo >&2 "usage: check-api-artifact.sh ARCHIVE"
  exit 2
fi

repository_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
source_root="$repository_root/crates/zinder-proto"
extracted="$(mktemp -d)"
trap 'rm -rf "$extracted"' EXIT
tar -xzf "$1" -C "$extracted"

"$repository_root/scripts/check-native-proto-closure.sh" "$extracted"
diff -r --no-dereference "$source_root/proto" "$extracted/proto"
cmp "$source_root/buf.release.yaml" "$extracted/buf.yaml"
cmp "$source_root/buf.release.gen.yaml" "$extracted/buf.gen.yaml"
cmp "$repository_root/LICENSE" "$extracted/LICENSE"
diff -r --no-dereference "$repository_root/dist/openapi" "$extracted/openapi"
cmp "$repository_root/dist/zinder.v1.descriptor.bin" "$extracted/zinder.v1.descriptor.bin"
cmp "$repository_root/dist/zinder.contract.descriptor.bin" "$extracted/zinder.contract.descriptor.bin"

buf build "$repository_root" --output "$extracted/source-native.bin"
cmp "$extracted/source-native.bin" "$extracted/zinder.v1.descriptor.bin"

cd "$extracted"
for vendor in proto/compat/lightwalletd proto/compat/darkside proto/external/zebra; do
  notices=(LICENSE UPSTREAM.md COMMIT)
  if [[ "$vendor" != proto/compat/darkside ]]; then
    notices+=(VERSION)
  fi
  for notice in "${notices[@]}"; do
    test -s "$vendor/$notice" || {
      echo >&2 "vendored proto notice is missing or empty: $vendor/$notice"
      exit 1
    }
  done
done

buf build --as-file-descriptor-set --output rebuilt-contract.bin
cmp rebuilt-contract.bin zinder.contract.descriptor.bin
buf build --path proto/zinder --output rebuilt-native.bin
cmp rebuilt-native.bin zinder.v1.descriptor.bin
buf generate

mapfile -d '' -t proto_files < <(find proto -name '*.proto' -type f -print0)
mkdir from-protoc
protoc -I proto/compat/lightwalletd -I proto/compat/darkside -I proto/external/zebra -I proto \
  --python_out=from-protoc "${proto_files[@]}"
for schema in "${proto_files[@]}"; do
  case "$schema" in
    proto/compat/lightwalletd/*) relative="${schema#proto/compat/lightwalletd/}" ;;
    proto/compat/darkside/*) relative="${schema#proto/compat/darkside/}" ;;
    proto/external/zebra/*) relative="${schema#proto/external/zebra/}" ;;
    *) relative="${schema#proto/}" ;;
  esac
  generated="${relative%.proto}_pb2.py"
  test -s "gen/python/$generated"
  test -s "from-protoc/$generated"
done
printf 'Verified source, descriptors, notices, and Buf/protoc generation for %s schemas\n' "${#proto_files[@]}"
