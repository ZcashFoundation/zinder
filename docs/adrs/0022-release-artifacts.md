# ADR-0022: Ship reproducible binaries with verifiable provenance

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

1. **Every release tag produces four native archives.**

   | Release platform | CPU baseline | Runtime |
   | --- | --- | --- |
   | `x86_64-v3-unknown-linux-gnu` | x86-64-v3 with ADX and PCLMULQDQ | GNU/Linux |
   | `x86_64-unknown-linux-gnu` | x86-64 with portable Pasta arithmetic | GNU/Linux |
   | `aarch64-unknown-linux-gnu` | Armv8-A | GNU/Linux |
   | `aarch64-apple-darwin` | Armv8-A | Apple Silicon, macOS 11.0 or later |

   Both AArch64 builds use the Pasta assembly backend within the baseline ISA.
   The optimized x86 build uses BMI2 and ADX for Pasta assembly and
   PCLMULQDQ for pipelined CRC32C. The baseline x86 build selects
   `target-cpu=x86-64` and disables Pasta assembly. `BUILD-INFO.json` records
   the Rust target, release platform, CPU baseline, extra CPU features, and
   platform-specific system library requirements.
2. **An archive holds the single unified `zinder` executable.** It contains
   `bin/zinder` (`serve`, `index`, `run <role>`, `snapshot`, `backup`,
   `config`, and `healthcheck`, covering ingest, projector, query,
   lightwalletd compatibility, Explorer, and Cipherscan adapter), plus build
   metadata, the license, a README, and checksums.
   GNU/Linux archives also include the native units and environment sample
   under `systemd/`.
   `deploy/release-images.json` lists the single OCI image (`zinder`).
   Benchmark (`zinder-bench`) and code-generation binaries are excluded.
3. **Builds are deterministic and proven by reproduction.** The build pins
   the toolchain, the source date, file order, ownership, modes, and
   compression, and fixes build settings that would otherwise depend on the
   build machine's CPU. Each platform is built twice in separate jobs without
   caches. Their binary and archive hashes must match before anyone is asked
   to approve the release.
4. **The single image reuses the proven binary.**
   `ghcr.io/zcashfoundation/zinder` copies `bin/zinder` from the compared
   archive instead of compiling again, and the image binary matches its
   archive binary byte for byte.
5. **System libraries stay dynamic.** RocksDB and its compression libraries
   are linked statically. On Linux, the release checker rejects GNU libc and
   `libstdc++` symbols newer than `GLIBC_2.34` and `GLIBCXX_3.4.30`, and CI
   runs each binary on Debian 12. On macOS, the checker requires arm64 Mach-O,
   the declared deployment target, and system-provided dynamic libraries
   including `libSystem` and `libc++`. The workflow builds and runs the binary
   on an Apple Silicon runner using Xcode 16.4.
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
9. **The release image holds only `bin/zinder`.**
   `ghcr.io/zcashfoundation/zinder` runs on a digest-pinned
   `gcr.io/distroless/cc-debian12:nonroot` base (`USER 10001:10001`,
   `ENTRYPOINT ["/usr/local/bin/zinder"]`, default `CMD ["serve"]`) with no
   shell, package manager, HTTP client, or wrapper scripts. Its built-in
   `zinder healthcheck` probes `/healthz` or `/readyz`. Because UID/GID 10001
   matches the Zebra and Zakura images, Zinder reads the node's mode-0600 RPC
   cookie directly without root privilege dropping.

10. **Version the client contract before distributing language wrappers.**
    The release workflow packages an independently pinnable protobuf module
    and `FileDescriptorSet` in `zinder-api-docs-<tag>.tar.gz`. The archive
    includes root `buf.yaml`, `buf.gen.yaml`, and
    `zinder.contract.descriptor.bin`. Protobuf CI checks for breaking
    changes. Packaging is implemented on `main` in
    [#749](https://github.com/ZcashFoundation/zinder/pull/749), which closed
    [#9](https://github.com/ZcashFoundation/zinder/issues/9). The Apple and
    Android SDKs will distribute Mozilla UniFFI bindings as an XCFramework via
    SwiftPM and an AAR via Maven Central. The TypeScript SDK will distribute
    `@zcashfoundation/zinder-client` on npm using that protobuf module
    over gRPC-Web. The pending UniFFI and TypeScript wrapper rollout
    ([#704](https://github.com/ZcashFoundation/zinder/issues/704)) waits for
    [#571](https://github.com/ZcashFoundation/zinder/issues/571) to stabilize
    `sync_compact_blocks`.

11. **Rust SDK crates share the workspace MSRV.** `zinder-core`,
    `zinder-proto`, and `zinder-client` keep Rust 1.100 as their minimum
    supported version. The FFI artifacts and npm SDK need no consumer
    `rustc`. Reconsider a lower SDK MSRV only when a concrete Rust-source
    consumer needs it and CI covers the split dependency graph.

The release procedure and how operators verify a download are in
[Releasing](../runbooks/releasing.md) and
[Deploying on a VM](../runbooks/deploying-on-a-vm.md).

## Evidence

- Decision 10: Judgment. The versioned wire contract gives wrappers a shared
  compatibility baseline. Waiting for `sync_compact_blocks` gives them a
  common recovery contract. Reopen if a committed consumer demonstrates a
  distribution requirement the pinned contract cannot meet.
- Decision 11: Judgment. One MSRV keeps one supported Rust dependency graph. A
  concrete Rust-source consumer that needs a lower version and CI coverage of
  the split graph would reopen it.

## Consequences

- Operators run any preset (`light-wallet`, `wallet`, `explorer`) or
  individual role from the single `bin/zinder` binary without extracting a
  container image.
- x86 hosts without AVX2 or ADX can use the baseline GNU/Linux archive.
  The AMD64 OCI image continues to use the optimized x86-64-v3 archive.
- The personal-node Compose manifest is a release asset covered by the signed
  `SHA256SUMS`. It pins both images by digest. The Zinder image digest comes
  from the verified multi-platform release index. See [Deploying
  Zinder](../runbooks/deploying.md#personal-node-compose) for sizing and
  operating instructions.
- A binary that fails to build, reproduce, or package stops the whole
  release before any crate, image, or GitHub Release is published.
- Approval is requested only after the slowest build finishes, so release
  time includes two full builds before review.
- Consumers can verify who built an artifact, not only its bytes. This is
  SLSA Build Level 2: the repository workflow is the build definition and is
  not an isolated builder, so Level 3 is not claimed. Provenance shows how an
  artifact was built, not that it is secure.
- The SBOM names the Rust crate that builds RocksDB and lists no native C or
  C++ package identities, because the release does not guess at them.
- A rerun may replace draft assets from the same commit. A published release
  is never modified.
- Musl builds are not produced. They would be a separate product, not an
  alias for these archives.
- An operator cannot open a shell in a runtime container. Inspection runs the
  binary itself or attaches a separate toolbox container, as [Deploying on a
  VM](../runbooks/deploying-on-a-vm.md#inspect-a-runtime-container) shows.
- The reference Compose deployment requires the node to run as UID 10001,
  because its `state-init` service gives the stores to that UID on every
  start.

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
- **Separate Debian images with shell entrypoint scripts.** Shipping Debian
  wrappers beside distroless runtime images duplicated maintenance and added
  unnecessary packages (`zinder serve` now supervises roles natively in the
  unified distroless image).
- **A sidecar that relaxes the node cookie's mode.** It must run beside every
  node, and mode 0644 makes the cookie readable by every UID that mounts the
  volume.
- **Ship platform wrappers before the wire contract and sync API stabilize.**
  Each wrapper would establish a separate integration contract to maintain.
- **Lower the SDK MSRV for binary consumers.** SwiftPM, Maven Central, and npm
  consumers install packaged artifacts without compiling Rust.

## Revision history

- 2026-11-28. Decisions 10 and 11 record the implemented contract packaging, remaining SDK rollout gate, and workspace MSRV policy.
- 2026-11-27. Added Apple Silicon and baseline x86-64 archives to the reproducible release workflow and included the personal-node Compose manifest in the signed asset catalog.
- 2026-11-27. Removed the retired shielded history publisher role from decision 2 after Canonical Schema v2 cutover.
- 2026-11-27. Consolidated the five runtime executables and separate runtime/single-volume images into the single `bin/zinder` archive binary and single distroless `ghcr.io/zcashfoundation/zinder` OCI image (`deploy/release-images.json`).
- 2026-11-28. Added native systemd units and a loopback environment sample to the GNU/Linux archives.
