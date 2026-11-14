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
  --arg rocksdb_version "$rocksdb_version" '{
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
        SPDXID: "SPDXRef-Package-Service",
        name: "zinder-query",
        versionInfo: "0.0.0",
        downloadLocation: "NOASSERTION",
        filesAnalyzed: false,
        externalRefs: [{referenceCategory: "PACKAGE-MANAGER", referenceType: "purl", referenceLocator: "pkg:cargo/zinder-query@0.0.0"}]
      },
      {
        SPDXID: "SPDXRef-Package-Libstdcpp",
        name: "libstdc++6",
        versionInfo: "12.2.0",
        downloadLocation: "NOASSERTION",
        filesAnalyzed: false,
        externalRefs: [{referenceCategory: "PACKAGE-MANAGER", referenceType: "purl", referenceLocator: "pkg:deb/debian/libstdc%2B%2B6@12.2.0"}]
      }
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

missing_file="$scratch/zinder-${version}-${platform}.missing-file.spdx.json"
jq 'del(.files[] | select(.fileName == "./bin/zinder-query"))' "$sbom" > "$missing_file"
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

image=ghcr.io/example/zinder-query
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

missing_service_sbom="$scratch/missing-service.spdx.json"
jq 'del(.packages[] | select(.name == "zinder-query"))' "$amd64_sbom" > "$missing_service_sbom"
expect_rejected "an image SBOM without its service executable package" \
  "$repository_root/scripts/check-release-image-evidence.sh" "$manifest" \
    --image "$image" --tag "$image_tag" --commit "$commit" --root-digest "$root_digest" \
    --amd64-digest "$amd64_digest" --arm64-digest "$arm64_digest" \
    --amd64-sbom "$missing_service_sbom" --arm64-sbom "$arm64_sbom"

foreign_service_sbom="$scratch/foreign-service.spdx.json"
jq '.packages += [{SPDXID:"SPDXRef-ForeignService",name:"zinder-ingest",versionInfo:"0.0.0",downloadLocation:"NOASSERTION",filesAnalyzed:false,externalRefs:[{referenceCategory:"PACKAGE-MANAGER",referenceType:"purl",referenceLocator:"pkg:cargo/zinder-ingest@0.0.0"}]}]' \
  "$amd64_sbom" > "$foreign_service_sbom"
expect_rejected "an image SBOM containing another service executable package" \
  "$repository_root/scripts/check-release-image-evidence.sh" "$manifest" \
    --image "$image" --tag "$image_tag" --commit "$commit" --root-digest "$root_digest" \
    --amd64-digest "$amd64_digest" --arm64-digest "$arm64_digest" \
    --amd64-sbom "$foreign_service_sbom" --arm64-sbom "$arm64_sbom"

compat_image=ghcr.io/example/zinder-compat-lightwalletd
compat_fixture="$scratch/compat-syft-fixture.json"
jq '.packages += [{
  SPDXID:"SPDXRef-Package-CompatService",
  name:"zinder-compat-lightwalletd",
  versionInfo:"0.0.0",
  downloadLocation:"NOASSERTION",
  filesAnalyzed:false,
  externalRefs:[{
    referenceCategory:"PACKAGE-MANAGER",
    referenceType:"purl",
    referenceLocator:"pkg:cargo/zinder-compat-lightwalletd@0.0.0"
  }]
}]' "$fixture" > "$compat_fixture"
compat_amd64_sbom="$scratch/compat-amd64.spdx.json"
compat_arm64_sbom="$scratch/compat-arm64.spdx.json"
for architecture in amd64 arm64; do
  digest_variable="${architecture}_digest"
  output_variable="compat_${architecture}_sbom"
  ZINDER_SYFT="$fake_syft" ZINDER_SYFT_FIXTURE="$compat_fixture" \
    "$repository_root/scripts/generate-release-sbom.sh" \
      --image "$compat_image" --tag "$image_tag" --commit "$commit" \
      --architecture "$architecture" --digest "${!digest_variable}" \
      --output "${!output_variable}"
done
"$repository_root/scripts/check-release-image-evidence.sh" "$manifest" \
  --image "$compat_image" --tag "$image_tag" --commit "$commit" --root-digest "$root_digest" \
  --amd64-digest "$amd64_digest" --arm64-digest "$arm64_digest" \
  --amd64-sbom "$compat_amd64_sbom" --arm64-sbom "$compat_arm64_sbom"

compat_missing_query_sbom="$scratch/compat-missing-query.spdx.json"
jq 'del(.packages[] | select(.name == "zinder-query"))' \
  "$compat_amd64_sbom" > "$compat_missing_query_sbom"
expect_rejected "a compatibility image SBOM without its declared query dependency" \
  "$repository_root/scripts/check-release-image-evidence.sh" "$manifest" \
    --image "$compat_image" --tag "$image_tag" --commit "$commit" --root-digest "$root_digest" \
    --amd64-digest "$amd64_digest" --arm64-digest "$arm64_digest" \
    --amd64-sbom "$compat_missing_query_sbom" --arm64-sbom "$compat_arm64_sbom"

compat_foreign_service_sbom="$scratch/compat-foreign-service.spdx.json"
jq '.packages += [{
  SPDXID:"SPDXRef-ForeignCompatService",
  name:"zinder-ingest",
  versionInfo:"0.0.0",
  downloadLocation:"NOASSERTION",
  filesAnalyzed:false,
  externalRefs:[{
    referenceCategory:"PACKAGE-MANAGER",
    referenceType:"purl",
    referenceLocator:"pkg:cargo/zinder-ingest@0.0.0"
  }]
}]' "$compat_amd64_sbom" > "$compat_foreign_service_sbom"
expect_rejected "a compatibility image SBOM containing an undeclared service package" \
  "$repository_root/scripts/check-release-image-evidence.sh" "$manifest" \
    --image "$compat_image" --tag "$image_tag" --commit "$commit" --root-digest "$root_digest" \
    --amd64-digest "$amd64_digest" --arm64-digest "$arm64_digest" \
    --amd64-sbom "$compat_foreign_service_sbom" --arm64-sbom "$compat_arm64_sbom"

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

check_single_volume_image_evidence() {
  local single_volume_image="$1"
  local single_volume_amd64_sbom="$2"
  local single_volume_arm64_sbom="$3"
  "$repository_root/scripts/check-release-image-evidence.sh" "$manifest" \
    --image "$single_volume_image" --tag "$image_tag" --commit "$commit" --root-digest "$root_digest" \
    --amd64-digest "$amd64_digest" --arm64-digest "$arm64_digest" \
    --amd64-sbom "$single_volume_amd64_sbom" --arm64-sbom "$single_volume_arm64_sbom"
}

wallet_serving_image=ghcr.io/example/zinder-wallet-serving
wallet_serving_fixture="$scratch/wallet-serving-syft-fixture.json"
jq --argjson ingest "$(cargo_package zinder-ingest)" \
  --argjson projector "$(cargo_package zinder-projector)" \
  '.packages += [$ingest, $projector]' "$fixture" > "$wallet_serving_fixture"
wallet_serving_explorer_image=ghcr.io/example/zinder-wallet-serving-explorer
wallet_serving_explorer_fixture="$scratch/wallet-serving-explorer-syft-fixture.json"
jq --argjson explorer "$(cargo_package zinder-explorer)" \
  '.packages += [$explorer]' "$wallet_serving_fixture" > "$wallet_serving_explorer_fixture"
explorer_image=ghcr.io/example/zinder-explorer
explorer_fixture="$scratch/explorer-syft-fixture.json"
jq --argjson explorer "$(cargo_package zinder-explorer)" \
  'del(.packages[] | select(.name == "zinder-query")) | .packages += [$explorer]' \
  "$fixture" > "$explorer_fixture"
canonical_runtime_image=ghcr.io/example/zinder-canonical-runtime
canonical_runtime_fixture="$scratch/canonical-runtime-syft-fixture.json"
jq --argjson ingest "$(cargo_package zinder-ingest)" \
  'del(.packages[] | select(.name == "zinder-query")) | .packages += [$ingest]' \
  "$fixture" > "$canonical_runtime_fixture"
for architecture in amd64 arm64; do
  digest_variable="${architecture}_digest"
  ZINDER_SYFT="$fake_syft" ZINDER_SYFT_FIXTURE="$wallet_serving_fixture" \
    "$repository_root/scripts/generate-release-sbom.sh" \
      --image "$wallet_serving_image" --tag "$image_tag" --commit "$commit" \
      --architecture "$architecture" --digest "${!digest_variable}" \
      --output "$scratch/wallet-serving-${architecture}.spdx.json"
  ZINDER_SYFT="$fake_syft" ZINDER_SYFT_FIXTURE="$wallet_serving_explorer_fixture" \
    "$repository_root/scripts/generate-release-sbom.sh" \
      --image "$wallet_serving_explorer_image" --tag "$image_tag" --commit "$commit" \
      --architecture "$architecture" --digest "${!digest_variable}" \
      --output "$scratch/wallet-serving-explorer-${architecture}.spdx.json"
  ZINDER_SYFT="$fake_syft" ZINDER_SYFT_FIXTURE="$explorer_fixture" \
    "$repository_root/scripts/generate-release-sbom.sh" \
      --image "$explorer_image" --tag "$image_tag" --commit "$commit" \
      --architecture "$architecture" --digest "${!digest_variable}" \
      --output "$scratch/explorer-${architecture}.spdx.json"
  ZINDER_SYFT="$fake_syft" ZINDER_SYFT_FIXTURE="$canonical_runtime_fixture" \
    "$repository_root/scripts/generate-release-sbom.sh" \
      --image "$canonical_runtime_image" --tag "$image_tag" --commit "$commit" \
      --architecture "$architecture" --digest "${!digest_variable}" \
      --output "$scratch/canonical-runtime-${architecture}.spdx.json"
done
check_single_volume_image_evidence "$wallet_serving_image" \
  "$scratch/wallet-serving-amd64.spdx.json" "$scratch/wallet-serving-arm64.spdx.json"
check_single_volume_image_evidence "$canonical_runtime_image" \
  "$scratch/canonical-runtime-amd64.spdx.json" "$scratch/canonical-runtime-arm64.spdx.json"
check_single_volume_image_evidence "$wallet_serving_explorer_image" \
  "$scratch/wallet-serving-explorer-amd64.spdx.json" "$scratch/wallet-serving-explorer-arm64.spdx.json"
check_single_volume_image_evidence "$explorer_image" \
  "$scratch/explorer-amd64.spdx.json" "$scratch/explorer-arm64.spdx.json"

wallet_serving_with_explorer_sbom="$scratch/wallet-serving-with-explorer.spdx.json"
jq --argjson explorer "$(cargo_package zinder-explorer)" '.packages += [$explorer]' \
  "$scratch/wallet-serving-amd64.spdx.json" > "$wallet_serving_with_explorer_sbom"
expect_rejected "a wallet-serving image SBOM containing the Explorer executable package" \
  check_single_volume_image_evidence "$wallet_serving_image" \
    "$wallet_serving_with_explorer_sbom" "$scratch/wallet-serving-arm64.spdx.json"

wallet_serving_explorer_missing_explorer_sbom="$scratch/wallet-serving-explorer-missing-explorer.spdx.json"
jq 'del(.packages[] | select(.name == "zinder-explorer"))' \
  "$scratch/wallet-serving-explorer-amd64.spdx.json" > "$wallet_serving_explorer_missing_explorer_sbom"
expect_rejected "a wallet-serving Explorer image SBOM without the Explorer executable package" \
  check_single_volume_image_evidence "$wallet_serving_explorer_image" \
    "$wallet_serving_explorer_missing_explorer_sbom" "$scratch/wallet-serving-explorer-arm64.spdx.json"

explorer_query_sbom="$scratch/explorer-query.spdx.json"
jq --argjson query "$(cargo_package zinder-query)" '.packages += [$query]' \
  "$scratch/explorer-amd64.spdx.json" > "$explorer_query_sbom"
expect_rejected "an Explorer image SBOM containing the native query executable package" \
  check_single_volume_image_evidence "$explorer_image" \
    "$explorer_query_sbom" "$scratch/explorer-arm64.spdx.json"

wallet_serving_missing_projector_sbom="$scratch/wallet-serving-missing-projector.spdx.json"
jq 'del(.packages[] | select(.name == "zinder-projector"))' \
  "$scratch/wallet-serving-amd64.spdx.json" > "$wallet_serving_missing_projector_sbom"
expect_rejected "a wallet-serving image SBOM without the projector executable package" \
  check_single_volume_image_evidence "$wallet_serving_image" \
    "$wallet_serving_missing_projector_sbom" "$scratch/wallet-serving-arm64.spdx.json"

wallet_serving_compat_sbom="$scratch/wallet-serving-compat.spdx.json"
jq --argjson compat "$(cargo_package zinder-compat-lightwalletd)" '.packages += [$compat]' \
  "$scratch/wallet-serving-amd64.spdx.json" > "$wallet_serving_compat_sbom"
expect_rejected "a wallet-serving image SBOM containing the compatibility executable package" \
  check_single_volume_image_evidence "$wallet_serving_image" \
    "$wallet_serving_compat_sbom" "$scratch/wallet-serving-arm64.spdx.json"

canonical_runtime_query_sbom="$scratch/canonical-runtime-query.spdx.json"
jq --argjson query "$(cargo_package zinder-query)" '.packages += [$query]' \
  "$scratch/canonical-runtime-amd64.spdx.json" > "$canonical_runtime_query_sbom"
expect_rejected "a canonical-runtime image SBOM containing a reader executable package" \
  check_single_volume_image_evidence "$canonical_runtime_image" \
    "$canonical_runtime_query_sbom" "$scratch/canonical-runtime-arm64.spdx.json"

three_platform_manifest="$scratch/three-platforms.json"
jq '.manifests += [.manifests[0] | .platform.architecture = "s390x"]' \
  "$manifest" > "$three_platform_manifest"
three_platform_digest="$root_digest"
expect_rejected "a third image platform" \
  "$repository_root/scripts/check-release-image-evidence.sh" "$three_platform_manifest" \
    --image "$image" --tag "$image_tag" --commit "$commit" --root-digest "$three_platform_digest" \
    --amd64-digest "$amd64_digest" --arm64-digest "$arm64_digest" \
    --amd64-sbom "$amd64_sbom" --arm64-sbom "$arm64_sbom"
