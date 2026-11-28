#!/usr/bin/env bash
set -euo pipefail

repository_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/zinder-release-image-publication-test.XXXXXX")"
trap 'rm -rf -- "$scratch"' EXIT

fail() {
  echo >&2 "release image publication test failed: $*"
  exit 1
}

command_log="$scratch/commands.log"
fake_cosign="$scratch/cosign"
fake_gh="$scratch/gh"
for tool in "$fake_cosign" "$fake_gh"; do
  cat > "$tool" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s' "$(basename "$0")" >> "$ZINDER_COMMAND_LOG"
printf ' <%s>' "$@" >> "$ZINDER_COMMAND_LOG"
printf '\n' >> "$ZINDER_COMMAND_LOG"
EOF
  chmod 0755 "$tool"
done

image=ghcr.io/example/zinder
root_digest="sha256:$(printf root | sha256sum | awk '{print $1}')"
amd64_digest="sha256:$(printf amd64 | sha256sum | awk '{print $1}')"
arm64_digest="sha256:$(printf arm64 | sha256sum | awk '{print $1}')"
commit=0123456789abcdef0123456789abcdef01234567

ZINDER_COMMAND_LOG="$command_log" \
ZINDER_COSIGN="$fake_cosign" \
ZINDER_GH="$fake_gh" \
  "$repository_root/scripts/verify-release-image-attestations.sh" \
    --image "$image" \
    --root-digest "$root_digest" \
    --amd64-digest "$amd64_digest" \
    --arm64-digest "$arm64_digest" \
    --repository example/zinder \
    --workflow release.yml \
    --tag v0.5.0-rc.4 \
    --commit "$commit"

expected_identity="https://github.com/example/zinder/.github/workflows/release.yml@refs/tags/v0.5.0-rc.4"
grep -Fq -- "<--cert-identity> <${expected_identity}>" "$command_log" \
  || fail "the exact workflow certificate identity was not enforced"
grep -Fq -- "<--source-ref> <refs/tags/v0.5.0-rc.4>" "$command_log" \
  || fail "the release tag source ref was not enforced"
grep -Fq -- "<--signer-digest> <${commit}>" "$command_log" \
  || fail "the signer digest was not enforced"
if grep -Fq -- '<--signer-workflow>' "$command_log"; then
  fail "mutually exclusive certificate identity and signer workflow flags were combined"
fi
[[ "$(grep -c '^gh <attestation> <verify>' "$command_log")" -eq 3 ]] \
  || fail "expected one provenance and two SBOM attestation verifications"

fake_docker="$scratch/docker"
fake_docker_state="$scratch/docker-state"
mkdir -p "$fake_docker_state"
cat > "$fake_docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[[ "$1" == buildx && "$2" == imagetools && "$3" == inspect ]]
reference="$4"

manifest() {
  jq -c -n \
    --arg root "$1" \
    --arg amd64 "$2" \
    --arg arm64 "$3" '{
      schemaVersion: 2,
      mediaType: "application/vnd.oci.image.index.v1+json",
      digest: $root,
      manifests: [
        {digest:$amd64,platform:{os:"linux",architecture:"amd64"}},
        {digest:$arm64,platform:{os:"linux",architecture:"arm64"}}
      ]
    }'
}

case "$reference" in
  *@"$FAKE_AMD64_BUILD_DIGEST")
    manifest "$FAKE_AMD64_BUILD_DIGEST" "$FAKE_AMD64_DIGEST" "$FAKE_UNUSED_DIGEST"
    ;;
  *@"$FAKE_ARM64_BUILD_DIGEST")
    manifest "$FAKE_ARM64_BUILD_DIGEST" "$FAKE_UNUSED_DIGEST" "$FAKE_ARM64_DIGEST"
    ;;
  *:"$FAKE_RELEASE_TAG")
    counter="$FAKE_DOCKER_STATE/tag"
    count="$(cat "$counter" 2>/dev/null || printf 0)"
    printf '%s\n' "$((count + 1))" > "$counter"
    if [[ "$count" -eq 0 ]]; then
      manifest "$FAKE_STALE_ROOT_DIGEST" "$FAKE_STALE_AMD64_DIGEST" "$FAKE_STALE_ARM64_DIGEST"
    else
      manifest "$FAKE_ROOT_DIGEST" "$FAKE_AMD64_DIGEST" "$FAKE_ARM64_DIGEST"
    fi
    ;;
  *:sha-"$FAKE_COMMIT")
    counter="$FAKE_DOCKER_STATE/commit"
    count="$(cat "$counter" 2>/dev/null || printf 0)"
    printf '%s\n' "$((count + 1))" > "$counter"
    if [[ "$count" -eq 0 ]]; then
      manifest "$FAKE_STALE_ROOT_DIGEST" "$FAKE_STALE_AMD64_DIGEST" "$FAKE_STALE_ARM64_DIGEST"
    else
      manifest "$FAKE_ROOT_DIGEST" "$FAKE_AMD64_DIGEST" "$FAKE_ARM64_DIGEST"
    fi
    ;;
  *)
    echo >&2 "unexpected image reference: $reference"
    exit 1
    ;;
esac
EOF
chmod 0755 "$fake_docker"

build_amd64_digest="sha256:$(printf build-amd64 | sha256sum | awk '{print $1}')"
build_arm64_digest="sha256:$(printf build-arm64 | sha256sum | awk '{print $1}')"
stale_root_digest="sha256:$(printf stale-root | sha256sum | awk '{print $1}')"
stale_amd64_digest="sha256:$(printf stale-amd64 | sha256sum | awk '{print $1}')"
stale_arm64_digest="sha256:$(printf stale-arm64 | sha256sum | awk '{print $1}')"
unused_digest="sha256:$(printf unused | sha256sum | awk '{print $1}')"
manifest="$scratch/converged.manifest.json"

FAKE_AMD64_BUILD_DIGEST="$build_amd64_digest" \
FAKE_ARM64_BUILD_DIGEST="$build_arm64_digest" \
FAKE_AMD64_DIGEST="$amd64_digest" \
FAKE_ARM64_DIGEST="$arm64_digest" \
FAKE_COMMIT="$commit" \
FAKE_DOCKER_STATE="$fake_docker_state" \
FAKE_RELEASE_TAG=v0.5.0-rc.4 \
FAKE_ROOT_DIGEST="$root_digest" \
FAKE_STALE_AMD64_DIGEST="$stale_amd64_digest" \
FAKE_STALE_ARM64_DIGEST="$stale_arm64_digest" \
FAKE_STALE_ROOT_DIGEST="$stale_root_digest" \
FAKE_UNUSED_DIGEST="$unused_digest" \
ZINDER_DOCKER="$fake_docker" \
ZINDER_MANIFEST_POLL_ATTEMPTS=2 \
ZINDER_MANIFEST_POLL_DELAY=0 \
  "$repository_root/scripts/resolve-release-image-manifest.sh" \
    --image "$image" \
    --tag v0.5.0-rc.4 \
    --commit "$commit" \
    --amd64-build-digest "$build_amd64_digest" \
    --arm64-build-digest "$build_arm64_digest" \
    --output "$manifest"

jq -e \
  --arg root "$root_digest" \
  --arg amd64 "$amd64_digest" \
  --arg arm64 "$arm64_digest" '
    .digest == $root
    and ([.manifests[] | select(.platform.os == "linux" and .platform.architecture == "amd64") | .digest] == [$amd64])
    and ([.manifests[] | select(.platform.os == "linux" and .platform.architecture == "arm64") | .digest] == [$arm64])
  ' "$manifest" >/dev/null || fail "the converged release manifest was not written"
[[ "$(cat "$fake_docker_state/tag")" -eq 2 && "$(cat "$fake_docker_state/commit")" -eq 2 ]] \
  || fail "manifest inspection did not retry stale registry responses"
