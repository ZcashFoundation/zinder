#!/usr/bin/env bash
set -euo pipefail

repository_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
version="${1:-}"

if [[ -z "$version" ]] \
  || ! bash "$repository_root/scripts/validate-release-tag.sh" "v${version}" \
    >/dev/null; then
  echo >&2 "published SDK verification requires the current workspace version"
  exit 2
fi

consumer_directory="$(mktemp -d /tmp/zinder-sdk-registry-consumer.XXXXXX)"
trap 'rm -rf "$consumer_directory"' EXIT
mkdir -p "$consumer_directory/src"

cat > "$consumer_directory/Cargo.toml" <<TOML
[package]
name = "zinder-sdk-registry-consumer"
version = "0.0.0"
edition = "2024"
publish = false

[dependencies]
zinder-client = "=${version}"
TOML

cat > "$consumer_directory/src/lib.rs" <<'RUST'
use zinder_client::{Capability, CapabilityDescriptor, ErrorReason, ServerInfo};

pub fn supports_full_blocks(server: &ServerInfo) -> bool {
    server.supports(Capability::FullBlock)
}

pub fn preserve_future_reason(reason: &str) -> ErrorReason {
    ErrorReason::from_wire_name(reason)
}
RUST

for attempt in 1 2 3; do
  if CARGO_HOME="$consumer_directory/cargo-home" \
    PROTOC=/does/not/exist \
    CARGO_INCREMENTAL=0 \
    cargo +1.100.0 check --manifest-path "$consumer_directory/Cargo.toml"; then
    exit 0
  fi

  if [[ "$attempt" -lt 3 ]]; then
    sleep "$((attempt * 30))"
  fi
done

echo >&2 "zinder-client ${version} did not resolve from crates.io after 3 attempts"
exit 1
