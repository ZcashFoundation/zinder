# Releasing Zinder

Zinder releases one lockstep product version across every first-party crate and
service. A tagged release publishes 4 multi-architecture runtime images, 2
multi-architecture single-volume host images, the
native protocol source and descriptor, generated OpenAPI documents, and one
GitHub Release. It also publishes deterministic GNU/Linux binary bundles for
x86-64-v3 and AArch64 and the Rust SDK formed by `zinder-core`, `zinder-proto`,
and `zinder-client` to crates.io in dependency order. The optional explorer and
Cipherscan services, benchmark executable, code-generation tools, and internal
Rust crates are not release artifacts.

`deploy/release-images.json` is the authoritative runtime image and binary
set. `deploy/release-single-volume-images.json` lists the two single-volume
host images, `zinder-canonical-runtime` and `zinder-wallet-serving`, which
package those runtime binaries with an entrypoint and are published as images
only. Every image is a target of `deploy/Dockerfile`. The PR smoke build and
tagged release both consume the exact catalog values validated by their
admission job.

## Release identity

The root `[workspace.package]` version is the product version. Every
first-party package inherits it. Exactly `zinder-core`, `zinder-proto`, and
`zinder-client` allow publication to crates.io; all other workspace packages
remain non-publishable. Public-package dependencies carry both repository paths
and a requirement for the same lockstep version, including its prerelease
suffix. The public packages inherit the workspace Rust 1.95 MSRV.

The release tag must equal `v` followed by the workspace version. Stable
versions use `vMAJOR.MINOR.PATCH`, while prereleases may add a SemVer suffix
such as `v0.5.0-rc.1`. Build metadata is rejected because `+` cannot be
represented in an OCI image tag without changing the version.

The release workflow resolves the preceding complete SemVer tag from `main`'s
first-parent history as the Cargo Release base and the validated tag commit as
its target. Cargo Release compares those 2 immutable commits to reconstruct the
complete publishable package set.

Every user-visible pull request records its release note as an independent
Changie fragment under `.changes/unreleased/`. Each fragment separates its
changelog category from its SemVer impact, so fixes, features, and breaking
changes can describe their behavior accurately while still driving the next
product version. Pull requests without a user-visible effect must check the
exact `No release note required` declaration in the pull request template.
Contributor instructions live in [`.changes/README.md`](../../.changes/README.md).

Every user-invokable executable supports `--version`. Runtime health responses,
the `zinder_build_info` metric, native `ServerInfo`, and lightwalletd
`LightdInfo` report the same product version and embedded Git commit. Local
builds report `unknown` when no commit was injected; release images receive the
full tag-target commit from the release workflow.

Run the local release-policy checks before opening the version change:

```console
bash scripts/test-release-tag.sh
bash scripts/test-deployment-admission.sh
scripts/test-release-binary-archive.sh
scripts/test-release-sbom.sh
scripts/test-release-image-publication.sh
scripts/test-changelog.sh
scripts/validate-changelog.sh fragments
scripts/check-sdk-package-policy.sh
scripts/verify-sdk-packages.sh
cargo fmt --all --check
cargo check --workspace --all-targets --all-features
```

The protocol workflow runs `buf lint`, `buf generate`, and `buf build` whenever
the native schemas or Buf configuration change. A release cannot begin if API
generation fails.

## Prepare a release

After all intended changes and their fragments have merged, ask Changie for
the version implied by their SemVer impacts:

```console
next_version="$(changie next auto)"
printf '%s\n' "$next_version"
```

Set `[workspace.package].version` in `Cargo.toml` to that version without its
`v` prefix. Update the version requirements on the three public dependency
edges (`zinder-proto` to `zinder-core`, and `zinder-client` to both public
dependencies) to the same exact product version, then prepare the changelog:

```console
scripts/prepare-changelog-release.sh "${next_version#v}"
cargo check --workspace --all-targets --all-features
scripts/check-sdk-package-policy.sh
scripts/verify-sdk-packages.sh
git diff -- Cargo.toml Cargo.lock CHANGELOG.md .changes
```

The preparation command fails unless the requested stable version, excluding
any prerelease suffix, matches the version implied by the fragments, and the
complete requested version matches every first-party Cargo package. A
prerelease batch uses Changie's `--keep` mode so the next release candidate
contains the complete pending release. Preparing a later release candidate
replaces the earlier prerelease section for the same stable version. Preparing
the stable version removes the prerelease section and consumes the fragments.
Each preparation is idempotent when rerun for the same version.

Merge the release-preparation change through `main`. Because this pull request
only consumes notes recorded by earlier changes, check `No release note
required` in its pull request template.

## Publish a release

After the release-preparation change merges, wait for required checks to pass,
then create an annotated tag at that commit:

```console
git switch main
git pull --ff-only
git tag -a v0.5.0 -m "v0.5.0"
git push origin v0.5.0
```

The repository must have immutable releases enabled before the tag is pushed.
The tag must be reachable from `main`. The `release` workflow validates the tag
against Cargo metadata, requires a matching non-empty changelog section, and
rejects pending fragments for a stable tag, requires them to remain for a
prerelease tag, and refuses an already published tag before it
logs in to GitHub Container Registry (GHCR) or requests an OIDC token. Before
approval, it generates and validates the native proto source closure, OpenAPI
documents, and descriptor set. It also validates exactly the three SDK archives
with the workspace Rust 1.95 toolchain, compiles an extracted consumer without
`protoc`, and uses Cargo Release to dry-run the complete dependency-aware
publication plan. Missing crate versions and package tags are expected during
this read-only check. The package and Cargo Release checks run independently,
then one final gate reports both outcomes and includes the complete plan and
observed state in the job summary. These credential-free jobs must succeed
before the protected `release` environment authorizes all trusted publication
and attestation work.

A release asks for one approval. The `authorize release publication` job is
the only job in the reviewed `release` environment, and every credentialed job
depends on it. Crate publication runs in the `crates-io` environment, which has
no reviewers and accepts only `v*` tags; it exists so the crates.io trusted
publishers can name it. Rerunning only the failed jobs after approval does not
ask again.

After approval, the workflow performs these operations in order:

1. Build each GNU/Linux binary platform in 2 independent uncached jobs that run
   side by side. The primary job validates the catalog, embedded commit, ELF
   machine, dynamic libraries, symbol ceilings, and clean Debian runtime
   probes, and generates one SPDX 2.3 SBOM from the auditable archive.
2. In the primary job, build the 4 runtime images and 2 single-volume host
   images natively for Linux amd64 and arm64 from the same binary stage with
   BuildKit max-mode provenance and SBOMs, confirm each image carries the
   archived binaries byte for byte, and push the untagged platform digests.
3. Confirm the 2 builds produced identical binary and archive hashes, then
   attest both SLSA provenance and the SBOM for each archive.
4. Authenticate to crates.io through GitHub OIDC, then let Cargo Release
   publish only missing SDK crates in Cargo's dependency order and verify their
   source provenance.
5. Compile a fresh registry-only consumer against the exact published
   `zinder-client` version after sparse-index propagation.
6. Publish exact `vX.Y.Z` and `sha-<commit>` manifests, attest each child SBOM
   and the root provenance, sign only the root digest, and verify the exact
   two-platform topology and producer identity.
7. Assemble the exact 6 payload assets, create their sorted `SHA256SUMS`, and
   keyless-sign that checksum file into `SHA256SUMS.sigstore.json`.
8. Publish the draft GitHub Release, then verify its immutable release
   attestation and every asset.
9. For a stable release, promote the 6 `latest` tags only after immutable
   release verification succeeds.

Stable and prerelease tags publish the same three-crate SDK catalog.
Prereleases never move image `latest`, and GitHub marks their Releases as
prereleases. The release workflow has no manual-dispatch path and globally
serializes publication, so two tags cannot race the same crates or image tags.
Use the manual `build-images` workflow for a build-only smoke test; it does not
authenticate to crates.io or GHCR, publish an artifact, or create a GitHub
Release.

## Configure crates.io trusted publication

Do not publish the historical `v0.4.0` tag to crates.io. The first public SDK
publication starts from an exact, clean `v0.5.0-rc.1` tag after its release
preparation change has merged to `main`. Create and push the tag, then let the
credential-free validation, API artifact, and SDK package jobs pass. Keep the
protected `release` environment unapproved so no workflow requests registry
credentials or writes release artifacts:

```console
git switch main
git pull --ff-only
git tag -a v0.5.0-rc.1 -m "v0.5.0-rc.1"
git push origin v0.5.0-rc.1
```

From the same exact tag, use a short-lived conventional crates.io token that
can create the three crate names:

```console
git switch --detach v0.5.0-rc.1
test -z "$(git status --porcelain)"
export CARGO_REGISTRY_TOKEN='<short-lived-bootstrap-token>'
scripts/verify-sdk-packages.sh
cargo +1.95.0 publish \
  --locked \
  --registry crates-io \
  -p zinder-core \
  -p zinder-proto \
  -p zinder-client
unset CARGO_REGISTRY_TOKEN
```

The conventional token must create all three crate names. If Cargo stops after
a partial publication, keep the token active, wait for the accepted versions to
appear in the sparse index, and repeat `cargo publish` with only the remaining
`-p` packages. Do not switch to OpenID Connect (OIDC) while any crate name is
still missing.

After all three `0.5.0-rc.1` versions resolve from crates.io, revoke the
bootstrap token and configure the same GitHub trusted publisher in the
crates.io settings for `zinder-core`, `zinder-proto`, and `zinder-client`:

- owner: `gustavovalverde`
- repository: `zinder`
- workflow: `release.yml`
- environment: `crates-io`

The `crates-io` GitHub environment must exist with no required reviewers and a
deployment policy that admits only `v*` tags. A crate whose trusted publisher
still names the `release` environment fails at `Authenticate to crates.io`;
correct the publisher and rerun the failed job.

The workflow uses `rust-lang/crates-io-auth-action` to exchange GitHub's OIDC
identity for a short-lived crates.io token. Do not add a repository or
environment `CARGO_REGISTRY_TOKEN` secret; the token is exposed only to the
Cargo Release `publish` step.

Approve the waiting `release` environment only after all 3 trusted publishers
are configured. Cargo Release then verifies the already published archives
against the tag commit, while the rest of the release workflow publishes the
RC1 images and artifacts.

Prepare and tag `v0.5.0-rc.2` through the normal release process after RC1
finishes. RC2 is the OIDC upload canary: the protected workflow must obtain the
short-lived token, upload all three missing `0.5.0-rc.2` versions in dependency
order, and pass the fresh registry-only consumer smoke test. Prepare and publish
`v0.5.0` only after the RC2 workflow and publication verification succeed.

## Verify publication

Check out the exact release tag so the smoke test compiles against the API
surface named by that version. Use GitHub CLI 2.94 or newer and Cosign with
Sigstore bundle support, then confirm the Release and each exact image before
changing a deployment:

```console
git switch --detach v0.5.0
gh release view v0.5.0
gh release verify v0.5.0

gh release download v0.5.0 \
  --pattern 'zinder-0.5.0-*-unknown-linux-gnu.tar.gz' \
  --pattern 'zinder-0.5.0-*-unknown-linux-gnu.spdx.json' \
  --pattern SHA256SUMS \
  --pattern SHA256SUMS.sigstore.json
sha256sum --check SHA256SUMS
cosign verify-blob \
  --bundle SHA256SUMS.sigstore.json \
  --certificate-identity \
    'https://github.com/gustavovalverde/zinder/.github/workflows/release.yml@refs/tags/v0.5.0' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  SHA256SUMS

for package_name in zinder-core zinder-proto zinder-client; do
  cargo info "${package_name}@0.5.0"
done

scripts/verify-published-sdk.sh 0.5.0

for image in \
  zinder-ingest \
  zinder-projector \
  zinder-query \
  zinder-compat-lightwalletd \
  zinder-canonical-runtime \
  zinder-wallet-serving
do
  docker buildx imagetools inspect \
    "ghcr.io/gustavovalverde/${image}:v0.5.0"
done
```

The first release that publishes a new image name creates its GHCR package as
private. An organization admin must set the package visibility to public once.
After `docker logout ghcr.io`, an anonymous
`docker pull ghcr.io/gustavovalverde/zinder-wallet-serving:v0.5.0` of the exact
tag confirms it.

The GitHub Release must contain `zinder.v1.descriptor.bin`, the versioned
`zinder-api-docs` archive, both versioned GNU/Linux binary archives, their 2
SPDX 2.3 JSON SBOMs, `SHA256SUMS`, and `SHA256SUMS.sigstore.json`. The API
archive contains the exact native `.proto` source closure alongside OpenAPI and
the descriptor. Each binary archive is rooted and contains exactly `bin/` with
the 4 runtime executables plus
`BUILD-INFO.json`, `LICENSE`, `README.md`, and an internal `SHA256SUMS`. Each
image must contain amd64 and arm64 runtime manifests, and its OCI revision
label must equal the tag-target commit. Binary SBOMs describe the recoverable
Rust dependency graph and the owning `rust-librocksdb-sys` package; they do not
claim complete native C or C++ CPE inventory. Deployments should pin verified
image digests or binary archive checksums. The Compose file uses `:local` only
for source builds and does not treat `latest` as a deployment identity.

## Recover a failed release

Crate publication is resumable. Every run reconstructs the complete release
from the preceding product tag and the immutable target commit, dry-runs all
planned crates together, observes crates.io, and publishes only missing
versions. An existing version is accepted only when its packaged
`.cargo_vcs_info.json` records the target commit without a dirty source flag.
A version published from another commit fails closed and must be investigated;
never yank or replace a version to make the release continue.

A nonzero Cargo publication may represent an uncertain upload, so Cargo
Release reobserves crates.io before deciding the operation failed. If registry
propagation times out, rerun the same `publish public SDK crates` job;
successfully published predecessors are verified and skipped. The pinned action
always dry-runs the complete plan, including when every crate is already
published, and removes its read-only GitHub token before Cargo can execute
package build scripts.

`.github/cargo-release.yml` defines package-specific tag identities required by
Cargo Release's reconciliation model. Zinder invokes only the action's `check`
and `publish` phases: the existing product tag, artifact-rich GitHub Release,
and `latest` image promotion remain owned by the Zinder workflow.

The workflow creates the public GitHub Release only after all exact image tags
and trust evidence exist. Every binary and image build finishes before crate
publication, so a build failure leaves crates.io untouched. A failure before
publication may leave untagged platform digests or an editable draft Release; rerunning the failed workflow
jobs is safe because the workflow replaces draft assets and recreates exact
tags from the same validated commit. A published immutable Release causes
validation to fail closed rather than modify public assets.

GHCR may briefly return the previous value of an exact or commit image tag
after a manifest update. The release workflow waits until both tags resolve to
the same root manifest and that manifest contains the platform digests produced
by the current build. If this convergence gate expires, inspect GHCR for both
tags before retrying; do not bypass the topology check or publish the GitHub
Release from partially converged tags.

GitHub attestation verification accepts only one workflow identity selector.
Zinder uses the full tag-scoped `--cert-identity` together with the source and
signer digests; do not add `--signer-workflow` to the same command. If a
prerelease publishes crates or image digests but fails before creating its
GitHub Release, leave its tag and published artifacts immutable. Merge the
workflow fix, prepare the next release candidate, and run the complete
publication path from the new tag.

Image SBOM evidence may contain another runtime-named Rust package only when
the owning service declares it as a production dependency. In particular,
`zinder-compat-lightwalletd` includes and must report `zinder-query`. A
single-volume host image must report exactly the executables its target
copies: `zinder-canonical-runtime` reports `zinder-ingest`, and
`zinder-wallet-serving` reports `zinder-ingest`, `zinder-projector`, and
`zinder-query`. All other sibling runtime packages remain rejected. If image evidence fails, regenerate
the SBOM from the exact child digest and compare its runtime-named packages
with the owning service manifest before changing the policy. Do not remove a
declared dependency from the evidence or broaden the allowed package set.

If only the final stable promotion fails, rerun the failed
`promote stable image tags` job. It verifies all 6 exact manifests before moving
any `latest` tag. Do not move a release tag or rebuild it from another commit.

Application rollback remains constrained by storage compatibility. Follow the
[VM deployment runbook](deploying-on-a-vm.md) and restore only a previously
certified binary and storage lane; do not start an older binary against a store
unless that physical layout is explicitly compatible.
