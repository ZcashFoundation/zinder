#!/usr/bin/env bash
set -euo pipefail

repository_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
generated_dir="$repository_root/crates/zinder-proto/generated"
buf_version="1.75.0"
protoc_version="36.2"
mode="${1:---check}"

case "$mode" in
  --check | --write) ;;
  *)
    echo >&2 "usage: regenerate-zinder-proto.sh [--check|--write]"
    exit 2
    ;;
esac

actual_buf_version="$(buf --version)"
[[ "$actual_buf_version" == "$buf_version" ]] || {
  echo >&2 "buf $buf_version is required; found $actual_buf_version"
  exit 1
}
actual_protoc_version="$(protoc --version)"
[[ "$actual_protoc_version" == "libprotoc $protoc_version" ]] || {
  echo >&2 "protoc $protoc_version is required; found $actual_protoc_version"
  exit 1
}

mkdir -p "$repository_root/.tmp"
generation_root="$(mktemp -d "$repository_root/.tmp/zinder-proto-generation.XXXXXX")"
trap 'rm -rf "$generation_root"' EXIT
staged_dir="$generation_root/generated"

cd "$repository_root"
buf lint
cargo run --quiet --locked -p zinder-proto-codegen -- "$repository_root" "$staged_dir"

if [[ "$mode" == "--check" ]]; then
  diff -ru "$generated_dir" "$staged_dir"
  git diff --exit-code -- crates/zinder-proto/generated
  exit 0
fi

next_dir="$repository_root/crates/zinder-proto/.generated.next.$$"
previous_dir="$repository_root/crates/zinder-proto/.generated.previous.$$"
mv "$staged_dir" "$next_dir"
if [[ -e "$generated_dir" ]]; then
  mv "$generated_dir" "$previous_dir"
fi
mv "$next_dir" "$generated_dir"
rm -rf "$previous_dir"
