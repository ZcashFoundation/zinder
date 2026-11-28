#!/usr/bin/env bash
set -euo pipefail

repository_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/zinder-release-sbom-test.XXXXXX")"
trap 'rm -rf -- "$scratch"' EXIT

fail() {
  echo >&2 "release SBOM test failed: $*"
  exit 1
}

expect_rejected() {
  local description="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    fail "$description was accepted"
  fi
}

cargo_package() {
  jq -c -n --arg name "$1" '{
    SPDXID: ("SPDXRef-Package-" + $name),
    name: $name,
    versionInfo: "0.0.0",
    downloadLocation: "NOASSERTION",
    filesAnalyzed: false,
    externalRefs: [{
      referenceCategory: "PACKAGE-MANAGER",
      referenceType: "purl",
      referenceLocator: ("pkg:cargo/" + $name + "@0.0.0")
    }]
  }'
}

version=0.0.0
tag=v0.0.0
commit=0123456789abcdef0123456789abcdef01234567
source_date_epoch=1700000000
platform=x86_64-v3-unknown-linux-gnu
rust_target=x86_64-unknown-linux-gnu
rocksdb_version="$(awk '
  $0 == "name = \"rust-librocksdb-sys\"" { found = 1; next }
  found && /^version = / { gsub(/^version = "|"$/, ""); print; exit }
' "$repository_root/Cargo.lock")"

mkdir -p "$scratch/bin"
while IFS= read -r binary_name; do
  cp /bin/true "$scratch/bin/$binary_name"
done < <(jq -r '.[]' "$repository_root/deploy/release-images.json")

archive_directory="$scratch/archive"
"$repository_root/scripts/build-release-binary-archive.sh" \
  --binaries "$scratch/bin" \
  --output "$archive_directory" \
  --version "$version" \
  --tag "$tag" \
  --commit "$commit" \
  --target "$rust_target" \
  --source-date-epoch "$source_date_epoch"
archive="$archive_directory/zinder-${version}-${platform}.tar.gz"

fake_syft="$scratch/fake-syft"
apply_fixture="$scratch/apply-fixture"
cat > "$apply_fixture" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
output=""
for argument in "$@"; do
  case "$argument" in
    spdx-json=*) output="${argument#spdx-json=}" ;;
  esac
done
[[ -n "$output" ]]
cp "$ZINDER_SYFT_FIXTURE" "$output"
EOF
chmod 0755 "$apply_fixture"
cp "$apply_fixture" "$fake_syft"

fixture="$scratch/syft-fixture.json"
jq -n \
  --arg rocksdb_version "$rocksdb_version" \
  --argjson zinder "$(cargo_package zinder)" \
  --argjson ingest "$(cargo_package zinder-ingest)" \
  --argjson projector "$(cargo_package zinder-projector)" \
  --argjson query "$(cargo_package zinder-query)" \
  --argjson compat "$(cargo_package zinder-compat-lightwalletd)" \
  --argjson explorer "$(cargo_package zinder-explorer)" \
  --argjson cipherscan "$(cargo_package zinder-compat-cipherscan)" '{
    spdxVersion: "SPDX-2.3",
    dataLicense: "CC0-1.0",
    SPDXID: "SPDXRef-DOCUMENT",
    name: "fixture",
    documentNamespace: "https://example.invalid/fixture",
    creationInfo: {created: "2023-11-14T22:13:20Z", creators: ["Tool: fixture"]},
    packages: [
      {
        SPDXID: "SPDXRef-Package-Rocksdb",
        name: "rust-librocksdb-sys",
        versionInfo: $rocksdb_version,
        downloadLocation: "NOASSERTION",
        filesAnalyzed: false,
        externalRefs: [{referenceCategory: "PACKAGE-MANAGER", referenceType: "purl", referenceLocator: ("pkg:cargo/rust-librocksdb-sys@" + $rocksdb_version)}]
      },
      {
        SPDXID: "SPDXRef-Package-Libstdcpp",
        name: "libstdc++6",
        versionInfo: "12.2.0",
        downloadLocation: "NOASSERTION",
        filesAnalyzed: false,
        externalRefs: [{referenceCategory: "PACKAGE-MANAGER", referenceType: "purl", referenceLocator: "pkg:deb/debian/libstdc%2B%2B6@12.2.0"}]
      },
      $zinder,
      $ingest,
      $projector,
      $query,
      $compat,
      $explorer,
      $cipherscan
    ],
    relationships: []
  }' > "$fixture"

sbom="$scratch/zinder-${version}-${platform}.spdx.json"
second_sbom="$scratch/second/zinder-${version}-${platform}.spdx.json"
for output in "$sbom" "$second_sbom"; do
  ZINDER_SYFT="$fake_syft" ZINDER_SYFT_FIXTURE="$fixture" \
    "$repository_root/scripts/generate-release-sbom.sh" \
      --archive "$archive" \
      --output "$output" \
      --version "$version" \
      --platform "$platform" \
      --commit "$commit"
done
cmp "$sbom" "$second_sbom"
"$repository_root/scripts/check-release-sbom.sh" "$sbom" \
  --archive "$archive" --version "$version" --platform "$platform" --commit "$commit"

for archive_platform in aarch64-apple-darwin x86_64-unknown-linux-gnu; do
  platform_options=(--target "$archive_platform")
  if [[ "$archive_platform" == x86_64-unknown-linux-gnu ]]; then
    platform_options+=(--cpu-baseline x86-64 --pasta-asm 0)
  fi
  "$repository_root/scripts/build-release-binary-archive.sh" \
    --binaries "$scratch/bin" --output "$archive_directory" \
    --version "$version" --tag "v$version" --commit "$commit" --source-date-epoch 1700000000 \
    "${platform_options[@]}"
  platform_archive="$archive_directory/zinder-${version}-${archive_platform}.tar.gz"
  platform_sbom="$scratch/zinder-${version}-${archive_platform}.spdx.json"
  ZINDER_SYFT="$fake_syft" ZINDER_SYFT_FIXTURE="$fixture" \
    "$repository_root/scripts/generate-release-sbom.sh" \
      --archive "$platform_archive" --output "$platform_sbom" \
      --version "$version" --platform "$archive_platform" --commit "$commit"
  "$repository_root/scripts/check-release-sbom.sh" "$platform_sbom" \
    --archive "$platform_archive" --version "$version" --platform "$archive_platform" --commit "$commit"
done

missing_file="$scratch/zinder-${version}-${platform}.missing-file.spdx.json"
jq 'del(.files[] | select(.fileName == "./bin/zinder"))' "$sbom" > "$missing_file"
cp "$missing_file" "$sbom"
expect_rejected "a missing executable file record" \
  "$repository_root/scripts/check-release-sbom.sh" "$sbom" \
    --archive "$archive" --version "$version" --platform "$platform" --commit "$commit"

ZINDER_SYFT="$fake_syft" ZINDER_SYFT_FIXTURE="$fixture" \
  "$repository_root/scripts/generate-release-sbom.sh" \
    --archive "$archive" --output "$sbom" --version "$version" --platform "$platform" --commit "$commit"

mutated="$scratch/zinder-${version}-${platform}.spdx.json.mutated"
jq '.packages[] |= if .SPDXID == "SPDXRef-ZinderArchive" then .checksums[0].checksumValue = ("0" * 64) else . end' \
  "$sbom" > "$mutated"
cp "$mutated" "$sbom"
expect_rejected "an incorrect archive digest" \
  "$repository_root/scripts/check-release-sbom.sh" "$sbom" \
    --archive "$archive" --version "$version" --platform "$platform" --commit "$commit"

ZINDER_SYFT="$fake_syft" ZINDER_SYFT_FIXTURE="$fixture" \
  "$repository_root/scripts/generate-release-sbom.sh" \
    --archive "$archive" --output "$sbom" --version "$version" --platform "$platform" --commit "$commit"
jq 'del(.packages[] | select(.SPDXID == "SPDXRef-ZinderArchive").licenseConcluded)' \
  "$sbom" > "$mutated"
cp "$mutated" "$sbom"
expect_rejected "an archive package without explicit license evidence" \
  "$repository_root/scripts/check-release-sbom.sh" "$sbom" \
    --archive "$archive" --version "$version" --platform "$platform" --commit "$commit"

ZINDER_SYFT="$fake_syft" ZINDER_SYFT_FIXTURE="$fixture" \
  "$repository_root/scripts/generate-release-sbom.sh" \
    --archive "$archive" --output "$sbom" --version "$version" --platform "$platform" --commit "$commit"
jq '.packages += [{SPDXID:"SPDXRef-Bad",name:"zinder-bench",versionInfo:"0",downloadLocation:"NOASSERTION",filesAnalyzed:false}]' \
  "$sbom" > "$mutated"
cp "$mutated" "$sbom"
expect_rejected "an excluded workspace package" \
  "$repository_root/scripts/check-release-sbom.sh" "$sbom" \
    --archive "$archive" --version "$version" --platform "$platform" --commit "$commit"

image=ghcr.io/example/zinder
image_tag=v0.0.0
amd64_digest="sha256:$(printf amd64 | sha256sum | awk '{print $1}')"
arm64_digest="sha256:$(printf arm64 | sha256sum | awk '{print $1}')"
manifest="$scratch/manifest.json"
jq -c -n --arg amd64 "$amd64_digest" --arg arm64 "$arm64_digest" '{
  schemaVersion: 2,
  mediaType: "application/vnd.oci.image.index.v1+json",
  digest: "PLACEHOLDER",
  manifests: [
    {mediaType:"application/vnd.oci.image.manifest.v1+json",digest:$amd64,size:1,platform:{os:"linux",architecture:"amd64"}},
    {mediaType:"application/vnd.oci.image.manifest.v1+json",digest:$arm64,size:1,platform:{os:"linux",architecture:"arm64"}}
  ]
}' > "$manifest"
root_digest="sha256:$(printf root | sha256sum | awk '{print $1}')"
jq --arg root "$root_digest" '.digest = $root' "$manifest" > "$manifest.tmp"
mv "$manifest.tmp" "$manifest"
amd64_sbom="$scratch/amd64.spdx.json"
arm64_sbom="$scratch/arm64.spdx.json"
for architecture in amd64 arm64; do
  digest_variable="${architecture}_digest"
  output_variable="${architecture}_sbom"
  ZINDER_SYFT="$fake_syft" ZINDER_SYFT_FIXTURE="$fixture" \
    "$repository_root/scripts/generate-release-sbom.sh" \
      --image "$image" --tag "$image_tag" --commit "$commit" \
      --architecture "$architecture" --digest "${!digest_variable}" \
      --output "${!output_variable}"
done
"$repository_root/scripts/check-release-image-evidence.sh" "$manifest" \
  --image "$image" --tag "$image_tag" --commit "$commit" --root-digest "$root_digest" \
  --amd64-digest "$amd64_digest" --arm64-digest "$arm64_digest" \
  --amd64-sbom "$amd64_sbom" --arm64-sbom "$arm64_sbom"

missing_image_license_sbom="$scratch/missing-image-license.spdx.json"
jq 'del(.packages[] | select(.SPDXID == "SPDXRef-ZinderImage").licenseConcluded)' \
  "$amd64_sbom" > "$missing_image_license_sbom"
expect_rejected "an image package without explicit license evidence" \
  "$repository_root/scripts/check-release-image-evidence.sh" "$manifest" \
    --image "$image" --tag "$image_tag" --commit "$commit" --root-digest "$root_digest" \
    --amd64-digest "$amd64_digest" --arm64-digest "$arm64_digest" \
    --amd64-sbom "$missing_image_license_sbom" --arm64-sbom "$arm64_sbom"

for required_package in zinder zinder-ingest zinder-projector zinder-query zinder-compat-lightwalletd zinder-explorer zinder-compat-cipherscan; do
  missing_runtime_sbom="$scratch/missing-${required_package}.spdx.json"
  jq --arg name "$required_package" 'del(.packages[] | select(.name == $name))' \
    "$amd64_sbom" > "$missing_runtime_sbom"
  expect_rejected "an image SBOM missing runtime package ${required_package}" \
    "$repository_root/scripts/check-release-image-evidence.sh" "$manifest" \
      --image "$image" --tag "$image_tag" --commit "$commit" --root-digest "$root_digest" \
      --amd64-digest "$amd64_digest" --arm64-digest "$arm64_digest" \
      --amd64-sbom "$missing_runtime_sbom" --arm64-sbom "$arm64_sbom"
done

for forbidden_package in zinder-bench zinder-testkit zinder-proto-codegen; do
  forbidden_sbom="$scratch/forbidden-${forbidden_package}.spdx.json"
  jq --argjson forbidden "$(cargo_package "$forbidden_package")" '.packages += [$forbidden]' \
    "$amd64_sbom" > "$forbidden_sbom"
  expect_rejected "an image SBOM containing forbidden package ${forbidden_package}" \
    "$repository_root/scripts/check-release-image-evidence.sh" "$manifest" \
      --image "$image" --tag "$image_tag" --commit "$commit" --root-digest "$root_digest" \
      --amd64-digest "$amd64_digest" --arm64-digest "$arm64_digest" \
      --amd64-sbom "$forbidden_sbom" --arm64-sbom "$arm64_sbom"
done

three_platform_manifest="$scratch/three-platforms.json"
jq '.manifests += [.manifests[0] | .platform.architecture = "s390x"]' \
  "$manifest" > "$three_platform_manifest"
three_platform_digest="$root_digest"
expect_rejected "a third image platform" \
  "$repository_root/scripts/check-release-image-evidence.sh" "$three_platform_manifest" \
    --image "$image" --tag "$image_tag" --commit "$commit" --root-digest "$three_platform_digest" \
    --amd64-digest "$amd64_digest" --arm64-digest "$arm64_digest" \
    --amd64-sbom "$amd64_sbom" --arm64-sbom "$arm64_sbom"
