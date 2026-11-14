#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage: check-release-image-evidence.sh MANIFEST \
  --image IMAGE --tag TAG --commit SHA --root-digest DIGEST \
  --amd64-digest DIGEST --arm64-digest DIGEST \
  --amd64-sbom FILE --arm64-sbom FILE
EOF
  exit 2
}

[[ $# -ge 1 ]] || usage
manifest="$1"
shift
image=""
tag=""
commit=""
root_digest=""
amd64_digest=""
arm64_digest=""
amd64_sbom=""
arm64_sbom=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --image) image="$2"; shift 2 ;;
    --tag) tag="$2"; shift 2 ;;
    --commit) commit="$2"; shift 2 ;;
    --root-digest) root_digest="$2"; shift 2 ;;
    --amd64-digest) amd64_digest="$2"; shift 2 ;;
    --arm64-digest) arm64_digest="$2"; shift 2 ;;
    --amd64-sbom) amd64_sbom="$2"; shift 2 ;;
    --arm64-sbom) arm64_sbom="$2"; shift 2 ;;
    *) usage ;;
  esac
done

digest_pattern='^sha256:[0-9a-f]{64}$'
[[ -s "$manifest" && -s "$amd64_sbom" && -s "$arm64_sbom" ]] || usage
[[ "$image" =~ ^ghcr\.io/[a-z0-9][a-z0-9._-]*/zinder-[a-z0-9-]+$ ]] || usage
[[ "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([+-][0-9A-Za-z.-]+)?$ ]] || usage
[[ "$commit" =~ ^[0-9a-f]{40}$ ]] || usage
[[ "$root_digest" =~ $digest_pattern && "$amd64_digest" =~ $digest_pattern && "$arm64_digest" =~ $digest_pattern ]] || usage
service_name="${image##*/}"
repository_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
# The runtime packages an image must report, and the only ones it may report.
# A single-volume host image is named for its topology, not for a binary, so
# its set is the executables deploy/Dockerfile copies into that target.
case "$service_name" in
  zinder-compat-lightwalletd)
    grep -Fqx 'zinder-query.workspace = true' \
      "$repository_root/services/zinder-compat-lightwalletd/Cargo.toml" || {
        echo >&2 "compatibility image evidence requires the declared zinder-query dependency"
        exit 1
      }
    image_runtime_packages='["zinder-compat-lightwalletd", "zinder-query"]'
    ;;
  zinder-canonical-runtime)
    image_runtime_packages='["zinder-ingest"]'
    ;;
  zinder-wallet-serving)
    image_runtime_packages='["zinder-ingest", "zinder-projector", "zinder-query"]'
    ;;
  zinder-wallet-serving-explorer)
    image_runtime_packages='["zinder-ingest", "zinder-projector", "zinder-query", "zinder-explorer"]'
    ;;
  *)
    image_runtime_packages="$(jq -c -n --arg service "$service_name" '[$service]')"
    ;;
esac
rocksdb_version="$(awk '
  $0 == "name = \"rust-librocksdb-sys\"" { found = 1; next }
  found && /^version = / { gsub(/^version = "|"$/, ""); print; exit }
' "$repository_root/Cargo.lock")"
jq -e \
  --arg root "$root_digest" \
  --arg amd64 "$amd64_digest" \
  --arg arm64 "$arm64_digest" '
    .schemaVersion == 2
    and .digest == $root
    and (.mediaType == "application/vnd.oci.image.index.v1+json"
      or .mediaType == "application/vnd.docker.distribution.manifest.list.v2+json")
    and ([.manifests[] | select(.platform.os == "linux")] | length) == 2
    and ([.manifests[] | select(.platform.os == "linux") | .platform | [.os, .architecture, (.variant // "")]] | sort) == [
      ["linux", "amd64", ""],
      ["linux", "arm64", ""]
    ]
    and ([.manifests[] | select(.platform.os == "linux") | .digest] | sort) == ([$amd64, $arm64] | sort)
    and all(.manifests[] | select(.platform.os != "linux");
      . as $entry
      | .platform.os == "unknown"
      and .platform.architecture == "unknown"
      and .annotations["vnd.docker.reference.type"] == "attestation-manifest"
      and ([ $amd64, $arm64 ] | index($entry.annotations["vnd.docker.reference.digest"])) != null
    )
  ' "$manifest" >/dev/null

check_child_sbom() {
  local sbom="$1"
  local architecture="$2"
  local digest="$3"
  [[ "$(stat -c '%s' "$sbom")" -le 16777216 ]] || {
    echo >&2 "image SBOM exceeds the attestation predicate limit"
    exit 1
  }
  jq -e \
    --arg image "$image" \
    --arg tag "$tag" \
    --arg commit "$commit" \
    --arg architecture "$architecture" \
    --arg digest "$digest" \
    --arg rocksdb_version "$rocksdb_version" \
    --argjson image_runtime_packages "$image_runtime_packages" '
      . as $document
      | .spdxVersion == "SPDX-2.3"
      and any(.packages[]?;
        (.name == $image or .name == ($image + "@" + $digest))
        and .licenseConcluded == "NOASSERTION"
        and .licenseDeclared == "NOASSERTION"
        and .copyrightText == "NOASSERTION"
        and any(.checksums[]?; .algorithm == "SHA256" and .checksumValue == ($digest | sub("^sha256:"; "")))
      )
      and any(.. | strings; . == $commit)
      and any(.. | strings; . == $tag)
      and any(.. | strings; . == $architecture)
      and all($image_runtime_packages[];
        . as $runtime_package
        | any($document.packages[]?;
          .name == $runtime_package
          and any(.externalRefs[]?;
            .referenceType == "purl"
            and (.referenceLocator | startswith("pkg:cargo/" + $runtime_package + "@"))
          )
        )
      )
      and any(.packages[]?;
        .name == "rust-librocksdb-sys"
        and .versionInfo == $rocksdb_version
        and any(.externalRefs[]?;
          .referenceType == "purl"
          and (.referenceLocator | startswith("pkg:cargo/rust-librocksdb-sys@"))
        )
      )
      and any(.packages[]?;
        .name == "libstdc++6"
        and any(.externalRefs[]?;
          .referenceType == "purl"
          and (.referenceLocator | startswith("pkg:deb/debian/libstdc%2B%2B6@"))
        )
      )
      and all(.packages[]?;
        (.name as $name
          | (
            ["zinder-ingest", "zinder-projector", "zinder-query", "zinder-compat-lightwalletd", "zinder-explorer"]
            - $image_runtime_packages
          )
          | index($name)) == null
      )
    ' "$sbom" >/dev/null
  if jq -r '.. | strings' "$sbom" \
    | grep -Eq '(/home/|/workspace|/tmp/|CARGO_REGISTRY_TOKEN|BEGIN [A-Z ]*PRIVATE KEY|:latest|@main|refs/heads/)'; then
    echo >&2 "image SBOM contains a sensitive path, mutable reference, or credential marker"
    exit 1
  fi
}

check_child_sbom "$amd64_sbom" amd64 "$amd64_digest"
check_child_sbom "$arm64_sbom" arm64 "$arm64_digest"
