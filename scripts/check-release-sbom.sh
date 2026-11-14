#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo >&2 "usage: check-release-sbom.sh SBOM --archive FILE --version VERSION --platform PLATFORM --commit SHA"
  exit 2
}

[[ $# -ge 1 ]] || usage
sbom="$1"
shift
archive=""
version=""
platform=""
commit=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --archive) archive="$2"; shift 2 ;;
    --version) version="$2"; shift 2 ;;
    --platform) platform="$2"; shift 2 ;;
    --commit) commit="$2"; shift 2 ;;
    *) usage ;;
  esac
done
[[ -s "$sbom" && -s "$archive" && -n "$version" && "$commit" =~ ^[0-9a-f]{40}$ ]] || usage
case "$platform" in
  x86_64-v3-unknown-linux-gnu|aarch64-unknown-linux-gnu) ;;
  *) usage ;;
esac
expected_sbom_name="zinder-${version}-${platform}.spdx.json"
[[ "$(basename -- "$sbom")" == "$expected_sbom_name" ]] || {
  echo >&2 "release SBOM has the wrong filename"
  exit 1
}
[[ "$(stat -c '%s' "$sbom")" -le 10485760 ]] || {
  echo >&2 "release SBOM exceeds the 10 MiB predicate limit"
  exit 1
}

repository_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
archive_name="$(basename -- "$archive")"
archive_sha256="$(sha256sum "$archive" | awk '{print $1}')"
rust_librocksdb_sys_version="$(
  awk '
    $0 == "name = \"rust-librocksdb-sys\"" { found = 1; next }
    found && /^version = / {
      gsub(/^version = "|"$/, "")
      print
      exit
    }
  ' "$repository_root/Cargo.lock"
)"
[[ -n "$rust_librocksdb_sys_version" ]]

jq -e \
  --arg archive_name "$archive_name" \
  --arg archive_sha256 "$archive_sha256" \
  --arg version "$version" \
  --arg platform "$platform" \
  --arg commit "$commit" \
  --arg rocksdb_version "$rust_librocksdb_sys_version" '
    .spdxVersion == "SPDX-2.3"
    and .dataLicense == "CC0-1.0"
    and .SPDXID == "SPDXRef-DOCUMENT"
    and (.documentNamespace | startswith("https://zinder.dev/spdx/releases/" + $commit + "/"))
    and ([.packages[] | select(.SPDXID == "SPDXRef-ZinderArchive")] | length) == 1
    and any(.packages[];
      .SPDXID == "SPDXRef-ZinderArchive"
      and .name == $archive_name
      and .versionInfo == $version
      and .licenseConcluded == "NOASSERTION"
      and .licenseDeclared == "NOASSERTION"
      and .copyrightText == "NOASSERTION"
      and any(.checksums[]?; .algorithm == "SHA256" and .checksumValue == $archive_sha256)
      and any(.externalRefs[]?; .referenceType == "zinder:release-platform" and .referenceLocator == $platform)
      and any(.externalRefs[]?; .referenceType == "zinder:source-commit" and .referenceLocator == $commit)
    )
    and ([.packages[] | select(.SPDXID | startswith("SPDXRef-Binary-")) | .name] | sort) == [
      "zinder-compat-lightwalletd",
      "zinder-explorer",
      "zinder-ingest",
      "zinder-projector",
      "zinder-query"
    ]
    and ([.files[].fileName] | sort) == [
      "./bin/zinder-compat-lightwalletd",
      "./bin/zinder-explorer",
      "./bin/zinder-ingest",
      "./bin/zinder-projector",
      "./bin/zinder-query"
    ]
    and any(.packages[]; .name == "rust-librocksdb-sys" and .versionInfo == $rocksdb_version)
    and any(.packages[]; any(.externalRefs[]?; .referenceType == "purl" and (.referenceLocator | startswith("pkg:cargo/"))))
    and all(.packages[];
      (.name | test("^zinder-(bench|testkit|proto-codegen|compat-cipherscan)$"; "i") | not)
    )
  ' "$sbom" >/dev/null

member_list="$(tar -tzf "$archive")"
! grep -Eq '(^/|(^|/)\.\.(/|$))' <<< "$member_list" || {
  echo >&2 "release SBOM verification rejected an unsafe archive path"
  exit 1
}
archive_root="zinder-${version}-${platform}"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/zinder-sbom-check.XXXXXX")"
trap 'rm -rf -- "$scratch"' EXIT
tar --extract --gzip --same-permissions --file "$archive" --directory "$scratch"
build_info="$scratch/$archive_root/BUILD-INFO.json"
while IFS= read -r binary_name; do
  binary_sha256="$(jq -r --arg name "$binary_name" '.binaries[] | select(.name == $name) | .sha256' "$build_info")"
  jq -e \
    --arg name "$binary_name" \
    --arg sha256 "$binary_sha256" \
    --arg platform "$platform" \
    --arg commit "$commit" '
      any(.packages[];
        .SPDXID == ("SPDXRef-Binary-" + $name)
        and .name == $name
        and .licenseConcluded == "NOASSERTION"
        and .licenseDeclared == "NOASSERTION"
        and .copyrightText == "NOASSERTION"
        and any(.checksums[]?; .algorithm == "SHA256" and .checksumValue == $sha256)
        and any(.externalRefs[]?; .referenceType == "zinder:release-platform" and .referenceLocator == $platform)
        and any(.externalRefs[]?; .referenceType == "zinder:source-commit" and .referenceLocator == $commit)
      )
      and any(.files[];
        .SPDXID == ("SPDXRef-File-" + $name)
        and .fileName == ("./bin/" + $name)
        and .licenseConcluded == "NOASSERTION"
        and .copyrightText == "NOASSERTION"
        and any(.checksums[]?; .algorithm == "SHA256" and .checksumValue == $sha256)
      )
      and any(.relationships[];
        .spdxElementId == "SPDXRef-ZinderArchive"
        and .relationshipType == "CONTAINS"
        and .relatedSpdxElement == ("SPDXRef-File-" + $name)
      )
    ' "$sbom" >/dev/null
done < <(jq -r '.binaries[].name' "$build_info")

if jq -r '.. | strings' "$sbom" \
  | grep -Eq '(/home/|/workspace|/tmp/|CARGO_REGISTRY_TOKEN|BEGIN [A-Z ]*PRIVATE KEY|:latest|@main|refs/heads/)'; then
  echo >&2 "release SBOM contains a sensitive path, mutable reference, or credential marker"
  exit 1
fi
