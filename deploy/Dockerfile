# syntax=docker/dockerfile:1.7

# Canonical Zinder runtime image builder.
#
# Build the unified release image from the repository root with:
#
#     docker build -f deploy/Dockerfile --target zinder -t zinder .
#
# Internal CI and benchmark targets (not shipped as release images):
#
#     docker build -f deploy/Dockerfile --target zinder-bench -t zinder-bench .
#     docker build -f deploy/Dockerfile --target zinder-ingest-live-tests -t zinder-ingest-live-tests .
#
# Every runtime target copies its binaries from the `zinder-binaries-selected`
# stage, which `ZINDER_BINARIES_SOURCE` names. By default it is
# `zinder-binaries`, which compiles the unified `zinder` binary and keeps Cargo
# registry, git, and target artifacts in BuildKit cache mounts. The benchmark
# compiles in its own stage against the same cache and is not part of the
# release invocation. `RELEASE_CACHE_SCOPE` names the target cache and defaults
# to `shared`.
#
# Cargo decides whether a workspace crate is up to date from file modification
# times, and a source tree from `git archive` or a tarball carries its commit
# time on every file. So that a cache holding another tree's build cannot pass
# as fresh, every cargo step that mounts the target cache first sets the
# modification time of every copied source file to the build time. Workspace
# crates then always recompile, while third-party crates, which Cargo
# fingerprints by version, stay cached. The release proves this does not
# change the output: it builds each platform twice from checkouts with
# different file times and compares the bytes.
#
# The release packages its image from the reproduced binary archive instead of
# compiling again. DIR/zinder-binaries/ holds the extracted archive binary:
#
#     docker buildx build -f deploy/Dockerfile --target zinder \
#       --build-arg ZINDER_BINARIES_SOURCE=zinder-archived-binaries \
#       --build-context zinder-archived-binaries=DIR .

ARG RUST_VERSION=1.100.0
ARG DEBIAN_RELEASE=bookworm
ARG RUST_IMAGE_DIGEST=sha256:ade39a710ef85f3506cffcfec3e8a24ba090badef1dff2fb798f71d878833fea
ARG DEBIAN_IMAGE_DIGEST=sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
ARG DISTROLESS_IMAGE_DIGEST=sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f
ARG ZINDER_BINARIES_SOURCE=zinder-binaries
# Pasta assembly backend. Enabled by default (`1`) so shipped images and
# optimized release archives build with `--features pasta_curves/asm` (x86-64 BMI2 + ADX,
# verified at startup, or baseline AArch64 ARMv8-A). Set to `0` for a portable
# Pasta backend. Pair with ZINDER_X86_64_CPU_BASELINE=x86-64 for hosts
# below x86-64-v3.
ARG ZINDER_PASTA_ASM=1
ARG ZINDER_X86_64_CPU_BASELINE=x86-64-v3

FROM rust:${RUST_VERSION}-${DEBIAN_RELEASE}@${RUST_IMAGE_DIGEST} AS zinder-build-base

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        clang \
        cmake \
        libclang-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

ENV CARGO_TARGET_DIR=/workspace/target
ENV CARGO_INCREMENTAL=0
WORKDIR /workspace

# Keep rustup's toolchain/component hydration in a source-independent layer.
# `cargo build` reads rust-toolchain.toml, and if this file is first seen after
# `COPY . .`, every source-change rebuild can pay for component synchronization.
COPY rust-toolchain.toml ./
RUN rustup show active-toolchain >/dev/null
RUN cargo install cargo-auditable --version 0.7.7 --locked

FROM zinder-build-base AS zinder-release-source

ARG ZINDER_BUILD_GIT_COMMIT="unknown"
ARG SOURCE_DATE_EPOCH=0

# x86-64-v3 also reaches the vendored RocksDB C++ (hardware CRC32c, AVX2);
# scoping to the target triple keeps build scripts and non-x86 images on the
# baseline ISA. Runtime hosts are verified at startup before storage opens.
# `chacha20_avx512` compiles the wallet accumulators' AVX-512 ChaCha backend,
# which runs only where AVX-512F and AVX-512VL are detected at run time and
# produces the same bytes as the AVX2 path (scripts/test-lthash16-backends.sh).
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq --cfg chacha20_avx512 --remap-path-prefix=/workspace=/usr/src/zinder --remap-path-prefix=/usr/local/cargo=/cargo"
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_RUSTFLAGS="--remap-path-prefix=/workspace=/usr/src/zinder --remap-path-prefix=/usr/local/cargo=/cargo"
# jemalloc fixes its page size at build time and aborts on a kernel with larger
# pages. 64 KiB pages run on AArch64 kernels with 4, 16, or 64 KiB pages; the
# build runners use 4 KiB, which jemalloc would otherwise measure and keep.
ENV AARCH64_UNKNOWN_LINUX_GNU_JEMALLOC_SYS_WITH_LG_PAGE=16
# On x86-64, jemalloc otherwise takes its virtual address width from the build
# CPU (48 or 57 bits), so builds on different runners would differ. Linux keeps
# user addresses below 2^47 unless a program asks for higher ones, so 48 bits
# is also correct on 57-bit hosts. AArch64 jemalloc always uses 48.
ENV X86_64_UNKNOWN_LINUX_GNU_JEMALLOC_SYS_WITH_LG_VADDR=48
ENV CARGO_PROFILE_RELEASE_STRIP=symbols
ENV SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH}
ENV ZINDER_BUILD_GIT_COMMIT=${ZINDER_BUILD_GIT_COMMIT}

COPY . .

FROM zinder-release-source AS zinder-binaries

ARG RUST_VERSION
ARG TARGETARCH
ARG TARGETVARIANT
ARG RELEASE_CACHE_SCOPE=shared
ARG ZINDER_PASTA_ASM=1
ARG ZINDER_X86_64_CPU_BASELINE

RUN --mount=type=cache,id=zinder-cargo-registry-${RUST_VERSION}-${TARGETARCH}${TARGETVARIANT},target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=zinder-cargo-git-${RUST_VERSION}-${TARGETARCH}${TARGETVARIANT},target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=zinder-cargo-target-${RUST_VERSION}-${TARGETARCH}${TARGETVARIANT}-${RELEASE_CACHE_SCOPE},target=/workspace/target,sharing=locked \
    find . -path ./target -prune -o -type f -exec touch {} + \
    && rust_target="$(case "$TARGETARCH" in amd64) printf '%s' x86_64-unknown-linux-gnu ;; arm64) printf '%s' aarch64-unknown-linux-gnu ;; *) exit 1 ;; esac)" \
    && case "${ZINDER_PASTA_ASM}" in \
        0|1) ;; \
        *) printf '%s\n' "ZINDER_PASTA_ASM must be 0 or 1, got '${ZINDER_PASTA_ASM}'" >&2; exit 1 ;; \
    esac \
    && case "${ZINDER_X86_64_CPU_BASELINE}:${TARGETARCH}:${ZINDER_PASTA_ASM}" in \
        x86-64-v3:*) ;; \
        x86-64:amd64:0) export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C target-cpu=x86-64 --remap-path-prefix=/workspace=/usr/src/zinder --remap-path-prefix=/usr/local/cargo=/cargo" ;; \
        *) printf '%s\n' 'x86-64 baseline requires amd64 and ZINDER_PASTA_ASM=0' >&2; exit 1 ;; \
    esac \
    && pasta_asm_flag="$(if [ "${ZINDER_PASTA_ASM}" = "1" ]; then printf '%s' '--features pasta_curves/asm'; else printf '%s' ''; fi)" \
    && rustup target add "$rust_target" \
    && cargo auditable build --locked --release --target "$rust_target" $pasta_asm_flag \
        --bin zinder \
    && mkdir -p /zinder-binaries \
    && cp "/workspace/target/${rust_target}/release/zinder" /zinder-binaries/zinder

FROM zinder-release-source AS zinder-bench-binary

ARG RUST_VERSION
ARG TARGETARCH
ARG TARGETVARIANT
ARG RELEASE_CACHE_SCOPE=shared
ARG ZINDER_PASTA_ASM=1

RUN --mount=type=cache,id=zinder-cargo-registry-${RUST_VERSION}-${TARGETARCH}${TARGETVARIANT},target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=zinder-cargo-git-${RUST_VERSION}-${TARGETARCH}${TARGETVARIANT},target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=zinder-cargo-target-${RUST_VERSION}-${TARGETARCH}${TARGETVARIANT}-${RELEASE_CACHE_SCOPE},target=/workspace/target,sharing=locked \
    find . -path ./target -prune -o -type f -exec touch {} + \
    && rust_target="$(case "$TARGETARCH" in amd64) printf '%s' x86_64-unknown-linux-gnu ;; arm64) printf '%s' aarch64-unknown-linux-gnu ;; *) exit 1 ;; esac)" \
    && case "${ZINDER_PASTA_ASM}" in \
        0|1) ;; \
        *) printf '%s\n' "ZINDER_PASTA_ASM must be 0 or 1, got '${ZINDER_PASTA_ASM}'" >&2; exit 1 ;; \
    esac \
    && pasta_asm_flag="$(if [ "${ZINDER_PASTA_ASM}" = "1" ]; then printf '%s' '--features pasta_curves/asm'; else printf '%s' ''; fi)" \
    && rustup target add "$rust_target" \
    && cargo auditable build --locked --release --target "$rust_target" $pasta_asm_flag --bin zinder-bench \
    && mkdir -p /zinder-bench \
    && cp "/workspace/target/${rust_target}/release/zinder-bench" /zinder-bench/zinder-bench

FROM scratch AS release-binaries

COPY --from=zinder-binaries /zinder-binaries/zinder /bin/zinder

# Release image packaging replaces this empty stage with a build context of
# archived binaries. A build that selects it without that context fails at the
# first binary copy instead of compiling.
FROM scratch AS zinder-archived-binaries

FROM ${ZINDER_BINARIES_SOURCE} AS zinder-binaries-selected

FROM zinder-build-base AS zinder-ingest-live-test-binary

ARG RUST_VERSION
ARG TARGETARCH
ARG TARGETVARIANT

ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq --cfg chacha20_avx512"

COPY . .

RUN --mount=type=cache,id=zinder-cargo-registry-${RUST_VERSION}-${TARGETARCH}${TARGETVARIANT},target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=zinder-cargo-git-${RUST_VERSION}-${TARGETARCH}${TARGETVARIANT},target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=zinder-cargo-target-${RUST_VERSION}-${TARGETARCH}${TARGETVARIANT},target=/workspace/target,sharing=locked \
    find . -path ./target -prune -o -type f -exec touch {} + \
    && mkdir -p /zinder-live-tests \
    && cargo test --locked --release -p zinder-ingest --test acceptance --no-run \
        --message-format=json-render-diagnostics > /zinder-live-tests/cargo-messages.json \
    && test_binary="$(grep '"reason":"compiler-artifact"' /zinder-live-tests/cargo-messages.json \
        | grep '"kind":\["test"\]' | grep '"name":"acceptance"' \
        | sed -n 's/.*"executable":"\([^"]*\)".*/\1/p')" \
    && rm /zinder-live-tests/cargo-messages.json \
    && test -n "$test_binary" \
    && test "$(printf '%s\n' "$test_binary" | wc -l)" -eq 1 \
    && cp "$test_binary" /zinder-live-tests/zinder-ingest-acceptance

# The release image runs the unified `zinder` binary on distroless cc, which
# carries glibc, libgcc_s, libstdc++, and CA certificates without a shell,
# package manager, or HTTP client. The image HEALTHCHECK invokes `zinder
# healthcheck`. BuildKit creates a WORKDIR in an empty stage, so the state
# directories need no RUN. UID and GID 10001 match the Zebra and Zakura images,
# so Zinder reads the node's mode-0600 RPC cookie.
FROM scratch AS zinder-service-state

WORKDIR /var/lib/zinder/secondary

FROM gcr.io/distroless/cc-debian12:nonroot@${DISTROLESS_IMAGE_DIGEST} AS zinder-service-base

COPY --from=zinder-service-state --chown=10001:10001 /var/lib/zinder /var/lib/zinder

USER 10001:10001
WORKDIR /var/lib/zinder

FROM zinder-service-base AS zinder

COPY --from=zinder-binaries-selected /zinder-binaries/zinder /usr/local/bin/zinder

USER 10001:10001
WORKDIR /var/lib/zinder

EXPOSE 9067 9068 9070 9102 9108

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD ["/usr/local/bin/zinder", "healthcheck"]

ENTRYPOINT ["/usr/local/bin/zinder"]
CMD ["serve"]

# Slim Debian base for internal CI and benchmark stages that need a shell and
# helper scripts (not shipped as release images).
FROM debian:${DEBIAN_RELEASE}-slim@${DEBIAN_IMAGE_DIGEST} AS zinder-runtime-base

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --system --gid 10001 zinder \
    && useradd --system --uid 10001 --gid zinder --home /var/lib/zinder --shell /usr/sbin/nologin zinder \
    && mkdir -p /var/lib/zinder/secondary /etc/zinder \
    && chown -R zinder:zinder /var/lib/zinder /etc/zinder

FROM zinder-runtime-base AS zinder-ingest-live-tests

COPY --from=zinder-ingest-live-test-binary /zinder-live-tests/zinder-ingest-acceptance /usr/local/bin/zinder-ingest-acceptance

USER zinder:zinder
WORKDIR /var/lib/zinder

ENTRYPOINT ["zinder-ingest-acceptance"]

FROM zinder-runtime-base AS zinder-bench

COPY --from=zinder-bench-binary /zinder-bench/zinder-bench /usr/local/bin/zinder-bench
COPY --chmod=0755 deploy/capture-container-resource-evidence.sh /usr/local/bin/zinder-capture-container-resource-evidence

USER zinder:zinder
WORKDIR /var/lib/zinder

ENTRYPOINT ["zinder-capture-container-resource-evidence", "zinder-bench"]
