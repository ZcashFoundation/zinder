#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage: verify-release-image-attestations.sh \
  --image IMAGE --root-digest DIGEST \
  --amd64-digest DIGEST --arm64-digest DIGEST \
  --repository OWNER/REPOSITORY --workflow WORKFLOW \
  --tag TAG --commit SHA
EOF
  exit 2
}

image=""
root_digest=""
amd64_digest=""
arm64_digest=""
repository=""
workflow=""
tag=""
commit=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --image) image="$2"; shift 2 ;;
    --root-digest) root_digest="$2"; shift 2 ;;
    --amd64-digest) amd64_digest="$2"; shift 2 ;;
    --arm64-digest) arm64_digest="$2"; shift 2 ;;
    --repository) repository="$2"; shift 2 ;;
    --workflow) workflow="$2"; shift 2 ;;
    --tag) tag="$2"; shift 2 ;;
    --commit) commit="$2"; shift 2 ;;
    *) usage ;;
  esac
done

digest_pattern='^sha256:[0-9a-f]{64}$'
[[ "$image" =~ ^ghcr\.io/[a-z0-9][a-z0-9._-]*/zinder(-[a-z0-9-]+)?$ ]] || usage
[[ "$root_digest" =~ $digest_pattern ]] || usage
[[ "$amd64_digest" =~ $digest_pattern ]] || usage
[[ "$arm64_digest" =~ $digest_pattern ]] || usage
[[ "$repository" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] || usage
[[ "$workflow" =~ ^[A-Za-z0-9_.-]+\.(yml|yaml)$ ]] || usage
[[ "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([+-][0-9A-Za-z.-]+)?$ ]] || usage
[[ "$commit" =~ ^[0-9a-f]{40}$ ]] || usage

cosign_command="${ZINDER_COSIGN:-cosign}"
gh_command="${ZINDER_GH:-gh}"
certificate_identity="https://github.com/${repository}/.github/workflows/${workflow}@refs/tags/${tag}"

"$cosign_command" verify \
  --certificate-identity "$certificate_identity" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  "${image}@${root_digest}"

common_attestation_args=(
  --repo "$repository"
  --cert-identity "$certificate_identity"
  --deny-self-hosted-runners
  --signer-digest "$commit"
  --source-ref "refs/tags/${tag}"
  --source-digest "$commit"
)
"$gh_command" attestation verify "oci://${image}@${root_digest}" \
  "${common_attestation_args[@]}"
for child_digest in "$amd64_digest" "$arm64_digest"; do
  "$gh_command" attestation verify "oci://${image}@${child_digest}" \
    "${common_attestation_args[@]}" \
    --predicate-type https://spdx.dev/Document/v2.3
done
