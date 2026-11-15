# ADR-0036: Deterministic GNU/Linux binary release bundles

| Field | Value |
| --- | --- |
| Status | Accepted |
| Product | Zinder |
| Domain | Release artifacts, Linux runtime compatibility |
| Related | [Releasing Zinder](../runbooks/releasing.md), [VM deployment](../runbooks/deploying-on-a-vm.md), [Canonical storage topologies](0035-canonical-storage-topologies.md), [Release provenance and SBOMs](0037-release-artifact-provenance-and-sboms.md) |

## Context

Container images are appropriate for the supported Compose topology, but they
are not the most direct installation surface for operators who supervise native
processes. A downloadable binary is useful only when its runtime catalog,
architecture baseline, ABI requirements, build identity, and archive bytes are
explicit and reproducible.

## Decision

Every stable or prerelease tag produces two GNU/Linux archives:

- `x86_64-v3-unknown-linux-gnu`, built for the x86-64-v3 CPU baseline; and
- `aarch64-unknown-linux-gnu`, built for the Armv8-A baseline.

Each archive contains exactly the five runtime catalog executables from
`deploy/release-images.json`: ingest, projector, native query, lightwalletd
compatibility, and the optional Explorer. Benchmark, Cipherscan, and
code-generation binaries are excluded. The release compile builds only these
five executables; the benchmark compiles in its own Docker stage. The archive
root also contains `BUILD-INFO.json`, `LICENSE`, `README.md`, and an internal
`SHA256SUMS`.

The release build uses Rust 1.95.0, explicit GNU target triples, an embedded
full commit, `SOURCE_DATE_EPOCH`, disabled Cargo incremental state, and remapped
source paths. Files are sorted with the C locale, owned numerically by UID/GID
0, assigned fixed modes, timestamped with the tag commit time, and compressed
with `gzip -n`. Each platform is built twice, in separate jobs with
independent Cargo target cache scopes, no Docker layer cache, and no CI cache.
Both builds start as soon as the tag is validated, with a read-only repository
token and no OIDC token.
Their binary and archive hashes must match before the protected `release`
environment is requested. The approval, crate publication, archive
attestation, and every image build therefore wait for that comparison.
Release images do not compile: they copy the binaries extracted from the
compared archive, and each image binary must match its archive binary byte for
byte and keep its `0755` mode. Before the approval, the reproduction job
packages every release image the same way from its own archive, without
pushing, and runs each binary inside its image.

RocksDB and its compression dependencies retain the existing static-link
policy. GNU libc and `libstdc++.so.6` remain dynamic runtime dependencies. The
release checker rejects unresolved libraries, GLIBC symbols newer than
`GLIBC_2.34`, and GLIBCXX symbols newer than `GLIBCXX_3.4.30`, then runs version
and help probes in a clean supported Debian runtime. `BUILD-INFO.json` binds
the version, tag, commit, target, platform, libc and CPU baselines, source
epoch, and every binary hash and size.

## Consequences

- Operators can install the same four-process topology, and the optional
  Explorer, without extracting a container image.
- x86 hosts must support x86-64-v3; the filename makes that requirement
  visible before execution.
- Binary release failure blocks crates.io authentication and every image
  registry write. A tag whose binaries do not build, reproduce, or package
  into the release images publishes no crate, image, or GitHub Release.
- The release approval is requested only after the slowest build finishes.
  With the v0.13.0 build times on GitHub-hosted runners, that is about 17
  minutes after the tag push. Approval latency adds to release time after that
  point.
- Stable and prerelease tags produce the same platform catalog, preserving the
  complete prerelease suffix in filenames and build metadata.

## Deferred work

Musl builds are a separate portability product and are not aliases for these
GNU artifacts. Artifact signing, SBOMs, and attestations are defined separately
by [ADR-0037](0037-release-artifact-provenance-and-sboms.md).

## Revision history

- 2026-11-14: The runtime catalog, and with it each archive, gained the
  optional `zinder-explorer` executable, so the Explorer release images reuse a
  reproduced binary like every other runtime image.
- 2026-11-15: Both builds of each platform now start right after tag
  validation, and the release approval waits for their byte comparison, so a
  binary failure stops the release before crate publication. Release images
  package the binaries extracted from the compared archive instead of reusing
  the compile stage, and the benchmark left the shared release compile. The
  reproduction job packages every release image from its archive without
  pushing, and image binaries must keep the archive's `0755` mode.
