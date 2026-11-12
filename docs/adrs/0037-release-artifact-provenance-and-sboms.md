# ADR-0037: Release artifact provenance and SBOMs

| Field | Value |
| --- | --- |
| Status | Accepted |
| Product | Zinder |
| Domain | Release trust, artifact identity, software inventory |
| Related | [Deterministic GNU/Linux bundles](0036-gnu-linux-binary-release-bundles.md), [Releasing Zinder](../runbooks/releasing.md), [VM deployment](../runbooks/deploying-on-a-vm.md) |

## Context

Deterministic archives and content-addressed images make release bytes stable,
but a checksum alone does not establish which repository, workflow, tag, or
commit produced them. Operators also need a machine-readable inventory for the
runtime Rust dependency graph without implying unsupported precision for
statically linked native libraries.

## Decision

The tagged release workflow requires a protected `release` environment. API
and SDK candidates complete before environment approval; no job receives an
OIDC token before that approval. All trusted operations use the tag-target
commit and the tag-only workflow identity. The repository is configured for
immutable GitHub Releases, and post-publication verification fails closed if
the resulting Release is not immutable.

Each deterministic GNU/Linux archive is accompanied by 1 SPDX 2.3 JSON SBOM.
The SBOM binds the archive digest, exact 4-binary catalog, release platform,
source commit, Cargo package URLs, and the locked `rust-librocksdb-sys` package
version. The binaries are built with `cargo-auditable`, allowing Syft to recover
their linked Rust dependency graph. The SBOM names the Rust `-sys` crate that
owns the native RocksDB build but does not invent native C or C++ CPEs that the
available evidence cannot justify. The workflow creates both SLSA provenance
and SBOM attestations for each archive.

Each Linux amd64 and arm64 image build publishes BuildKit max-mode provenance
and an SBOM. The manifest job admits exactly those 2 runtime platforms,
generates 1 SPDX 2.3 SBOM for each platform manifest digest, and attaches the
SBOM attestations to GHCR. It attaches SLSA provenance and 1 keyless Cosign
signature to the multi-platform root digest. Verification binds the signature
and attestations to the exact repository, `release.yml` workflow, tag ref,
source commit, child digests, and root digest before release assembly.

The GitHub Release contains the 2 archives, 2 archive SBOMs, API archive,
descriptor set, a sorted `SHA256SUMS`, and its keyless Sigstore bundle. The
workflow verifies the bundle before publishing the draft, then requires the
published Release and every asset to pass GitHub's immutable-release
verification before a stable `latest` image tag can move.

## Consequences

- Archive and image consumers can verify producer identity as well as content
  identity.
- The release path provides SLSA Build Level 2 provenance. It does not claim
  SLSA Build Level 3 because the repository workflow remains the build
  definition and is not an isolated, independently administered builder.
- Provenance shows how an artifact was produced; it is not proof that the
  source, dependencies, workflow, or resulting software are secure.
- Reruns may replace draft assets and exact image tags from the same validated
  commit. A published immutable Release is never modified.
- Release tooling is pinned. The workflow checksum-verifies GitHub CLI 2.94.0
  before using its release and attestation verification commands.

## Deferred work

Native C and C++ package and CPE completeness requires evidence from the actual
vendored and compiled source graph. It remains deferred until that inventory
can be produced without guessing package identities or versions.
