#!/usr/bin/env bash
# Build the stage tarball that restore-fixture.sh installs on a fixture host:
# Zinder release binaries, the pinned node binary, jq, grpcurl, the guest
# scripts, and the protocol files used by the readiness and digest checks.
# Fixture hosts have no internet egress, so everything they run comes from here.
#
# Usage: build-stage.sh --binaries DIR --out FILE.tar.zst [--upload gs://BUCKET/PATH.tar.zst]
#   DIR holds zinder-ingest, zinder-projector, zinder-query,
#   zinder-compat-lightwalletd, and zinder-explorer, for example the
#   `release-binaries` target of deploy/Dockerfile exported with buildx.
set -euo pipefail

# Zebra 7.1.0, the node runtime pinned in CLAUDE.md and the live lanes.
ZEBRA_IMAGE=docker.io/zfnd/zebra:7.1.0@sha256:4f9536c8061e407f9f2ec81b7e0dd2f1762f81deec316abb1530dca8796a33ba
JQ_URL=https://github.com/jqlang/jq/releases/download/jq-1.7.1/jq-linux-amd64
JQ_SHA256=5942c9b0934e510ee61eb3e30273f1b3fe2590df93933a93d7c58b81d19c8ff5
GRPCURL_URL=https://github.com/fullstorydev/grpcurl/releases/download/v1.9.1/grpcurl_1.9.1_linux_x86_64.tar.gz
GRPCURL_SHA256=4315b81477fa25e227b693b9cdbdb2a2ee2f1e93607d3dd90401cfaa010791f7

binaries="" out="" upload=""
while [[ $# -gt 0 ]]; do
  case $1 in
    --binaries) binaries=$2; shift 2 ;;
    --out) out=$2; shift 2 ;;
    --upload) upload=$2; shift 2 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done
[[ -d $binaries && -n $out ]] || { echo "usage: build-stage.sh --binaries DIR --out FILE.tar.zst [--upload gs://...]" >&2; exit 2; }
for tool in docker curl zstd sha256sum tar; do command -v "$tool" >/dev/null || { echo "missing $tool" >&2; exit 1; }; done

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
work=$(mktemp -d); trap 'rm -rf "$work"' EXIT
stage=$work/zfx-stage
mkdir -p "$stage/bin" "$stage/tools" "$stage/guest" "$stage/proto-all"

for b in zinder-ingest zinder-projector zinder-query zinder-compat-lightwalletd zinder-explorer; do
  install -m 0755 "$binaries/$b" "$stage/bin/$b"
done
(cd "$stage/bin" && sha256sum zinder-* > "$stage/binaries.sha256")

docker pull -q "$ZEBRA_IMAGE" >/dev/null
cid=$(docker create "$ZEBRA_IMAGE")
docker cp "$cid:/usr/local/bin/zebrad" "$stage/tools/zebrad"
docker rm "$cid" >/dev/null
curl -fsSL -o "$stage/tools/jq" "$JQ_URL"
echo "$JQ_SHA256  $stage/tools/jq" | sha256sum --quiet -c
curl -fsSL -o "$work/grpcurl.tgz" "$GRPCURL_URL"
echo "$GRPCURL_SHA256  $work/grpcurl.tgz" | sha256sum --quiet -c
tar -xzf "$work/grpcurl.tgz" -C "$stage/tools" grpcurl
chmod 0755 "$stage/tools/"*
(cd "$stage/tools" && sha256sum zebrad jq grpcurl > tools.sha256)

cp "$repo"/scripts/mainnet-fixture/guest/* "$stage/guest/"
cp -r "$repo"/crates/zinder-proto/proto/. "$stage/proto-all/"

tar -C "$work" -cf - zfx-stage | zstd -q -T0 -3 > "$out"
sha256sum "$out" | cut -d' ' -f1 > "$out.sha256"
echo "stage: $out ($(stat -c %s "$out") bytes, sha256 $(cat "$out.sha256"))"
if [[ -n $upload ]]; then
  gcloud storage cp "$out" "$upload"
  gcloud storage cp "$out.sha256" "$upload.sha256"
fi
