# ADR-0022: Ship reproducible Linux binaries with verifiable provenance

Status: Accepted

## Context

Container images suit the Compose deployment, but operators who supervise
native processes need downloadable binaries. A binary is only safe to install
when its contents, CPU baseline, C library requirements, and build identity
are explicit, and when someone else can rebuild the same bytes.

Matching bytes are not enough on their own. A checksum does not say which
repository, workflow, tag, or commit produced an artifact. Operators also
need a machine-readable inventory of what is linked into each binary, without
the release claiming precision it does not have for native libraries.

## Decision

1. **Every release tag produces two GNU/Linux archives.** One targets
   `x86_64-v3-unknown-linux-gnu`, the other `aarch64-unknown-linux-gnu`
   (Armv8-A). The x86 file name states the x86-64-v3 CPU requirement.
2. **An archive holds exactly the runtime catalog.** It contains the five
   executables listed in `deploy/release-images.json`: ingest, projector,
   query, lightwalletd compatibility, and the optional Explorer, plus build
   metadata, the license, a README, and checksums. Benchmark, Cipherscan
   adapter, and code-generation binaries are excluded.
3. **Builds are deterministic and proven by reproduction.** The build pins
   the toolchain, the source date, file order, ownership, modes, and
   compression, and fixes build settings that would otherwise depend on the
   build machine's CPU. Each platform is built twice in separate jobs without
   caches. Their binary and archive hashes must match before anyone is asked
   to approve the release.
4. **Images reuse the proven binaries.** Release images copy the binaries
   from the compared archive instead of compiling again, and each image
   binary must match its archive binary byte for byte.
5. **The C library stays dynamic with a fixed ceiling.** RocksDB and its
   compression libraries are linked statically. GNU libc and `libstdc++` stay
   dynamic. The release checker rejects symbols newer than `GLIBC_2.34` or
   `GLIBCXX_3.4.30` and runs each binary on a clean supported Debian image.
6. **No credentials before approval.** Builds run with a read-only token and
   no OIDC token. A protected `release` environment gates crate publication,
   signing, attestation, and every registry write.
7. **Every artifact carries provenance and an SBOM.** Each archive gets an
   SPDX 2.3 SBOM built from the dependency graph that `cargo-auditable`
   embeds, plus SLSA provenance and SBOM attestations. Each image platform
   gets an SBOM, and the multi-platform image gets provenance and a keyless
   Cosign signature. The release checksum file is signed with a Sigstore
   bundle. Verification binds each signature to the repository, the release
   workflow, the tag, and the commit.
8. **Published releases are immutable.** The workflow fails if the published
   GitHub Release is not immutable, and the stable `latest` image tag moves
   only after every artifact passes verification.

The release procedure and how operators verify a download are in
[Releasing](../runbooks/releasing.md) and
[Deploying on a VM](../runbooks/deploying-on-a-vm.md).

## Consequences

- Operators can run the full topology from native binaries without
  extracting a container image.
- x86 hosts must support x86-64-v3, and hosts need glibc 2.34 or newer.
- A binary that fails to build, reproduce, or package stops the whole
  release before any crate, image, or GitHub Release is published.
- Approval is requested only after the slowest build finishes, so release
  time includes two full builds before review.
- Consumers can verify who built an artifact, not only its bytes. This is
  SLSA Build Level 2: the repository workflow is the build definition and is
  not an isolated builder, so Level 3 is not claimed. Provenance shows how an
  artifact was built, not that it is secure.
- The SBOM names the Rust crate that builds RocksDB but lists no native C or
  C++ package identities, because the release cannot yet produce that
  inventory without guessing. That gap remains open.
- A rerun may replace draft assets from the same commit. A published release
  is never modified.
- Musl builds are not produced. They would be a separate product, not an
  alias for these archives.

## Alternatives rejected

- **Container images only.** Operators who supervise native processes would
  have to extract binaries from images themselves.
- **One build per platform.** Nothing would detect a build that depends on
  the runner, its cache, or its CPU.
- **Compile again for images.** The image binaries would not be the bytes
  that were reproduced.
- **Checksums without signatures or provenance.** A checksum cannot show
  which workflow and commit produced the bytes.
- **Guessing native package identities in the SBOM.** An inventory with
  invented versions is worse than one that states its limit.
