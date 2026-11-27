#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage:
  generate-release-sbom.sh --archive FILE --output FILE --version VERSION --platform PLATFORM --commit SHA
  generate-release-sbom.sh --image IMAGE --tag TAG --commit SHA --architecture ARCH --digest DIGEST --output FILE
EOF
  exit 2
}

archive=""
output=""
version=""
platform=""
commit=""
image=""
release_tag=""
architecture=""
digest=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --archive) archive="$2"; shift 2 ;;
    --output) output="$2"; shift 2 ;;
    --version) version="$2"; shift 2 ;;
    --platform) platform="$2"; shift 2 ;;
    --commit) commit="$2"; shift 2 ;;
    --image) image="$2"; shift 2 ;;
    --tag) release_tag="$2"; shift 2 ;;
    --architecture) architecture="$2"; shift 2 ;;
    --digest) digest="$2"; shift 2 ;;
    *) usage ;;
  esac
done

if [[ -n "$image" ]]; then
  [[ -z "$archive" && -z "$version" && -z "$platform" ]] || usage
  [[ "$image" =~ ^ghcr\.io/[a-z0-9][a-z0-9._-]*/zinder(-[a-z0-9-]+)?$ ]] || usage
  [[ "$release_tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([+-][0-9A-Za-z.-]+)?$ ]] || usage
  [[ "$commit" =~ ^[0-9a-f]{40}$ && "$digest" =~ ^sha256:[0-9a-f]{64}$ && -n "$output" ]] || usage
  case "$architecture" in
    amd64|arm64) ;;
    *) usage ;;
  esac

  scratch="$(mktemp -d "${TMPDIR:-/tmp}/zinder-image-sbom.XXXXXX")"
  trap 'rm -rf -- "$scratch"' EXIT
  syft_output="$scratch/syft.spdx.json"
  "${ZINDER_SYFT:-syft}" scan "registry:${image}@${digest}" \
    --output "spdx-json=$syft_output"
  [[ -s "$syft_output" ]] || {
    echo >&2 "Syft did not produce an image SPDX document"
    exit 1
  }

  mkdir -p "$(dirname -- "$output")"
  jq \
    --arg image "$image" \
    --arg tag "$release_tag" \
    --arg commit "$commit" \
    --arg architecture "$architecture" \
    --arg digest "$digest" '
      del(.source)
      | .spdxVersion = "SPDX-2.3"
      | .dataLicense = "CC0-1.0"
      | .SPDXID = "SPDXRef-DOCUMENT"
      | .name = ($image + "@" + $digest + " SBOM")
      | .documentNamespace = ("https://zinder.dev/spdx/images/" + $commit + "/" + ($digest | sub("^sha256:"; "")))
      | .comment = ("Zinder release " + $tag + ", source " + $commit + ", linux/" + $architecture)
      | .packages = ([{
          SPDXID: "SPDXRef-ZinderImage",
          name: $image,
          versionInfo: $tag,
          downloadLocation: ($image + "@" + $digest),
          filesAnalyzed: false,
          licenseConcluded: "NOASSERTION",
          licenseDeclared: "NOASSERTION",
          copyrightText: "NOASSERTION",
          primaryPackagePurpose: "CONTAINER",
          checksums: [{algorithm: "SHA256", checksumValue: ($digest | sub("^sha256:"; ""))}],
          externalRefs: [
            {referenceCategory: "OTHER", referenceType: "zinder:source-commit", referenceLocator: $commit},
            {referenceCategory: "OTHER", referenceType: "zinder:release-tag", referenceLocator: $tag},
            {referenceCategory: "OTHER", referenceType: "zinder:architecture", referenceLocator: $architecture}
          ]
        }] + (.packages // []))
      | .relationships = ([{
          spdxElementId: "SPDXRef-DOCUMENT",
          relationshipType: "DESCRIBES",
          relatedSpdxElement: "SPDXRef-ZinderImage"
        }] + (.relationships // []))
    ' "$syft_output" > "$output"
  exit 0
fi

[[ -z "$release_tag" && -z "$architecture" && -z "$digest" ]] || usage
[[ -s "$archive" && -n "$output" && -n "$version" && "$commit" =~ ^[0-9a-f]{40}$ ]] || usage
case "$platform" in
  x86_64-v3-unknown-linux-gnu|aarch64-unknown-linux-gnu) ;;
  *) usage ;;
esac

member_list="$(tar -tzf "$archive")"
! grep -Eq '(^/|(^|/)\.\.(/|$))' <<< "$member_list" || {
  echo >&2 "SBOM generation rejected an unsafe archive path"
  exit 1
}

scratch="$(mktemp -d "${TMPDIR:-/tmp}/zinder-sbom.XXXXXX")"
trap 'rm -rf -- "$scratch"' EXIT
tar --extract --gzip --same-permissions --file "$archive" --directory "$scratch"
archive_root="zinder-${version}-${platform}"
build_info="$scratch/$archive_root/BUILD-INFO.json"
[[ -s "$build_info" ]] || {
  echo >&2 "SBOM generation could not find BUILD-INFO.json"
  exit 1
}

syft_output="$scratch/syft.spdx.json"
"${ZINDER_SYFT:-syft}" scan "dir:$scratch/$archive_root" \
  --output "spdx-json=$syft_output"
[[ -s "$syft_output" ]] || {
  echo >&2 "Syft did not produce an SPDX document"
  exit 1
}

archive_name="$(basename -- "$archive")"
archive_sha256="$(sha256sum "$archive" | awk '{print $1}')"
source_epoch="$(jq -r '.source_date_epoch' "$build_info")"
created="$(date --utc --date="@${source_epoch}" '+%Y-%m-%dT%H:%M:%SZ')"
binary_packages='[]'
binary_files='[]'
relationships='[]'
while IFS= read -r binary_name; do
  binary_sha256="$(jq -r --arg name "$binary_name" '.binaries[] | select(.name == $name) | .sha256' "$build_info")"
  binary_spdx_id="SPDXRef-Binary-${binary_name}"
  binary_packages="$(
    jq \
      --arg id "$binary_spdx_id" \
      --arg name "$binary_name" \
      --arg version "$version" \
      --arg sha256 "$binary_sha256" \
      --arg platform "$platform" \
      --arg commit "$commit" '
        . + [{
          SPDXID: $id,
          name: $name,
          versionInfo: $version,
          downloadLocation: "NOASSERTION",
          filesAnalyzed: false,
          licenseConcluded: "NOASSERTION",
          licenseDeclared: "NOASSERTION",
          copyrightText: "NOASSERTION",
          primaryPackagePurpose: "APPLICATION",
          checksums: [{algorithm: "SHA256", checksumValue: $sha256}],
          externalRefs: [
            {referenceCategory: "OTHER", referenceType: "zinder:release-platform", referenceLocator: $platform},
            {referenceCategory: "OTHER", referenceType: "zinder:source-commit", referenceLocator: $commit}
          ]
        }]
      ' <<< "$binary_packages"
  )"
  relationships="$(
    jq --arg package_id "$binary_spdx_id" --arg file_id "SPDXRef-File-${binary_name}" '
      . + [
        {spdxElementId: "SPDXRef-ZinderArchive", relationshipType: "CONTAINS", relatedSpdxElement: $package_id},
        {spdxElementId: "SPDXRef-ZinderArchive", relationshipType: "CONTAINS", relatedSpdxElement: $file_id},
        {spdxElementId: $package_id, relationshipType: "CONTAINS", relatedSpdxElement: $file_id}
      ]
    ' <<< "$relationships"
  )"
  binary_files="$(
    jq \
      --arg id "SPDXRef-File-${binary_name}" \
      --arg name "./bin/${binary_name}" \
      --arg sha256 "$binary_sha256" '
        . + [{
          SPDXID: $id,
          fileName: $name,
          checksums: [{algorithm: "SHA256", checksumValue: $sha256}],
          licenseConcluded: "NOASSERTION",
          copyrightText: "NOASSERTION"
        }]
      ' <<< "$binary_files"
  )"
done < <(jq -r '.binaries[].name' "$build_info")

mkdir -p "$(dirname -- "$output")"
jq \
  --arg archive_name "$archive_name" \
  --arg archive_sha256 "$archive_sha256" \
  --arg version "$version" \
  --arg platform "$platform" \
  --arg commit "$commit" \
  --arg created "$created" \
  --arg namespace "https://zinder.dev/spdx/releases/${commit}/${archive_sha256}" \
  --argjson binary_packages "$binary_packages" \
  --argjson binary_files "$binary_files" \
  --argjson relationships "$relationships" '
    del(.source)
    | .spdxVersion = "SPDX-2.3"
    | .dataLicense = "CC0-1.0"
    | .SPDXID = "SPDXRef-DOCUMENT"
    | .name = ($archive_name + " SBOM")
    | .documentNamespace = $namespace
    | .creationInfo.created = $created
    | .creationInfo.creators = ["Tool: syft", "Organization: Zinder"]
    | .packages = ([{
        SPDXID: "SPDXRef-ZinderArchive",
        name: $archive_name,
        versionInfo: $version,
        downloadLocation: "NOASSERTION",
        filesAnalyzed: false,
        licenseConcluded: "NOASSERTION",
        licenseDeclared: "NOASSERTION",
        copyrightText: "NOASSERTION",
        primaryPackagePurpose: "APPLICATION",
        checksums: [{algorithm: "SHA256", checksumValue: $archive_sha256}],
        externalRefs: [
          {referenceCategory: "OTHER", referenceType: "zinder:release-platform", referenceLocator: $platform},
          {referenceCategory: "OTHER", referenceType: "zinder:source-commit", referenceLocator: $commit}
        ]
      }] + $binary_packages + (.packages // []))
    | .files = $binary_files
    | .relationships = ([{
        spdxElementId: "SPDXRef-DOCUMENT",
        relationshipType: "DESCRIBES",
        relatedSpdxElement: "SPDXRef-ZinderArchive"
      }] + $relationships + ((.relationships // []) | map(select(
        (.spdxElementId | startswith("SPDXRef-File") | not)
        and (.relatedSpdxElement | startswith("SPDXRef-File") | not)
      ))))
  ' "$syft_output" > "$output"
