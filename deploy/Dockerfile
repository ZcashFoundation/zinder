# syntax=docker/dockerfile:1.7

# Canonical Zinder runtime image builder.
#
# Build service images from the repository root with:
#
#     docker build -f deploy/Dockerfile --target zinder-ingest -t zinder-ingest .
#     docker build -f deploy/Dockerfile --target zinder-projector -t zinder-projector .
#     docker build -f deploy/Dockerfile --target zinder-query -t zinder-query .
#     docker build -f deploy/Dockerfile --target zinder-compat-lightwalletd -t zinder-compat-lightwalletd .
#     docker build -f deploy/Dockerfile --target zinder-canonical-runtime -t zinder-canonical-runtime .
#     docker build -f deploy/Dockerfile --target zinder-wallet-serving -t zinder-wallet-serving .
#     docker build -f deploy/Dockerfile --target zinder-wallet-serving-explorer -t zinder-wallet-serving-explorer .
#     docker build -f deploy/Dockerfile --target zinder-explorer -t zinder-explorer .
#     docker build -f deploy/Dockerfile --target zinder-bench -t zinder-bench .
#     docker build -f deploy/Dockerfile --target zinder-ingest-live-tests -t zinder-ingest-live-tests .
#
# Every runtime target copies its binaries from the `zinder-binaries-selected`
# stage, which `ZINDER_BINARIES_SOURCE` names. By default it is
# `zinder-binaries`, which compiles all shipped binaries in one Cargo
# invocation and keeps Cargo registry, git, and target artifacts in BuildKit
# cache mounts, so rebuilding one service warms the shared cache for every
# other service image. The benchmark compiles in its own stage against the
# same cache and is not part of that invocation.
#
# The release packages its images from the reproduced binary archive instead
# of compiling again. DIR/zinder-binaries/ holds the extracted archive binaries:
#
#     docker buildx build -f deploy/Dockerfile --target zinder-ingest \
#       --build-arg ZINDER_BINARIES_SOURCE=zinder-archived-binaries \
#       --build-context zinder-archived-binaries=DIR .

ARG RUST_VERSION=1.100.0
ARG DEBIAN_RELEASE=bookworm
ARG RUST_IMAGE_DIGEST=sha256:ade39a710ef85f3506cffcfec3e8a24ba090badef1dff2fb798f71d878833fea
ARG DEBIAN_IMAGE_DIGEST=sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
ARG DISTROLESS_IMAGE_DIGEST=sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f
ARG ZINDER_BINARIES_SOURCE=zinder-binaries
# Opt-in Pasta assembly backend. Set to 1 to build the runtime binaries with
# `--features pasta_curves/asm`, which selects the x86-64 (BMI2 + ADX) or
# AArch64 assembly field arithmetic. The default 0 keeps the portable backend
# so the shipped images run on every x86-64-v3 and AArch64 host. Any other
# value fails the build instead of silently producing a portable binary.
ARG ZINDER_PASTA_ASM=0

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
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq --remap-path-prefix=/workspace=/usr/src/zinder --remap-path-prefix=/usr/local/cargo=/cargo"
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
ARG ZINDER_PASTA_ASM=0

RUN --mount=type=cache,id=zinder-cargo-registry-${RUST_VERSION}-${TARGETARCH}${TARGETVARIANT},target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=zinder-cargo-git-${RUST_VERSION}-${TARGETARCH}${TARGETVARIANT},target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=zinder-cargo-target-${RUST_VERSION}-${TARGETARCH}${TARGETVARIANT}-${RELEASE_CACHE_SCOPE},target=/workspace/target,sharing=locked \
    rust_target="$(case "$TARGETARCH" in amd64) printf '%s' x86_64-unknown-linux-gnu ;; arm64) printf '%s' aarch64-unknown-linux-gnu ;; *) exit 1 ;; esac)" \
    && case "${ZINDER_PASTA_ASM}" in \
        0|1) ;; \
        *) printf '%s\n' "ZINDER_PASTA_ASM must be 0 or 1, got '${ZINDER_PASTA_ASM}'" >&2; exit 1 ;; \
    esac \
    && pasta_asm_flag="$(if [ "${ZINDER_PASTA_ASM}" = "1" ]; then printf '%s' '--features pasta_curves/asm'; else printf '%s' ''; fi)" \
    && rustup target add "$rust_target" \
    && cargo auditable build --locked --release --target "$rust_target" $pasta_asm_flag \
        --bin zinder-ingest \
        --bin zinder-projector \
        --bin zinder-query \
        --bin zinder-compat-lightwalletd \
        --bin zinder-explorer \
    && mkdir -p /zinder-binaries \
    && cp "/workspace/target/${rust_target}/release/zinder-ingest" /zinder-binaries/zinder-ingest \
    && cp "/workspace/target/${rust_target}/release/zinder-projector" /zinder-binaries/zinder-projector \
    && cp "/workspace/target/${rust_target}/release/zinder-query" /zinder-binaries/zinder-query \
    && cp "/workspace/target/${rust_target}/release/zinder-compat-lightwalletd" /zinder-binaries/zinder-compat-lightwalletd \
    && cp "/workspace/target/${rust_target}/release/zinder-explorer" /zinder-binaries/zinder-explorer

FROM zinder-release-source AS zinder-bench-binary

ARG RUST_VERSION
ARG TARGETARCH
ARG TARGETVARIANT
ARG RELEASE_CACHE_SCOPE=shared

RUN --mount=type=cache,id=zinder-cargo-registry-${RUST_VERSION}-${TARGETARCH}${TARGETVARIANT},target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=zinder-cargo-git-${RUST_VERSION}-${TARGETARCH}${TARGETVARIANT},target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=zinder-cargo-target-${RUST_VERSION}-${TARGETARCH}${TARGETVARIANT}-${RELEASE_CACHE_SCOPE},target=/workspace/target,sharing=locked \
    rust_target="$(case "$TARGETARCH" in amd64) printf '%s' x86_64-unknown-linux-gnu ;; arm64) printf '%s' aarch64-unknown-linux-gnu ;; *) exit 1 ;; esac)" \
    && rustup target add "$rust_target" \
    && cargo auditable build --locked --release --target "$rust_target" --bin zinder-bench \
    && mkdir -p /zinder-bench \
    && cp "/workspace/target/${rust_target}/release/zinder-bench" /zinder-bench/zinder-bench

FROM scratch AS release-binaries

COPY --from=zinder-binaries /zinder-binaries/zinder-ingest /bin/zinder-ingest
COPY --from=zinder-binaries /zinder-binaries/zinder-projector /bin/zinder-projector
COPY --from=zinder-binaries /zinder-binaries/zinder-query /bin/zinder-query
COPY --from=zinder-binaries /zinder-binaries/zinder-compat-lightwalletd /bin/zinder-compat-lightwalletd
COPY --from=zinder-binaries /zinder-binaries/zinder-explorer /bin/zinder-explorer

# Release image packaging replaces this empty stage with a build context of
# archived binaries. A build that selects it without that context fails at the
# first binary copy instead of compiling.
FROM scratch AS zinder-archived-binaries

FROM ${ZINDER_BINARIES_SOURCE} AS zinder-binaries-selected

FROM zinder-build-base AS zinder-ingest-live-test-binary

ARG RUST_VERSION
ARG TARGETARCH
ARG TARGETVARIANT

ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq"

COPY . .

RUN --mount=type=cache,id=zinder-cargo-registry-${RUST_VERSION}-${TARGETARCH}${TARGETVARIANT},target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=zinder-cargo-git-${RUST_VERSION}-${TARGETARCH}${TARGETVARIANT},target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=zinder-cargo-target-${RUST_VERSION}-${TARGETARCH}${TARGETVARIANT},target=/workspace/target,sharing=locked \
    cargo test --locked --release -p zinder-ingest --test acceptance --no-run \
    && mkdir -p /zinder-live-tests \
    && test_binary="$(find /workspace/target/release/deps -maxdepth 1 -type f -name 'acceptance-*' -perm /111 -printf '%T@ %p\n' | sort -nr | head -n 1 | cut -d' ' -f 2-)" \
    && test -n "$test_binary" \
    && cp "$test_binary" /zinder-live-tests/zinder-ingest-acceptance

# Per-service release images run one binary on distroless cc, which carries
# glibc, libgcc_s, libstdc++, and CA certificates but no shell, package
# manager, or HTTP client. The image HEALTHCHECK runs the binary's own
# `healthcheck` subcommand. BuildKit creates a WORKDIR in an empty stage, so
# the state directories need no RUN. UID and GID 10001 match the Zebra and
# Zakura images, so the services read the node's mode-0600 RPC cookie.
FROM scratch AS zinder-service-state

WORKDIR /var/lib/zinder/secondary

FROM gcr.io/distroless/cc-debian12:nonroot@${DISTROLESS_IMAGE_DIGEST} AS zinder-service-base

COPY --from=zinder-service-state --chown=10001:10001 /var/lib/zinder /var/lib/zinder

# Each service image installs deploy/config/images/<service>.toml as
# /etc/zinder/config.toml and passes it with CMD. The file binds every
# container interface so `docker run -p` works; other containers on the same
# network can still reach these listeners, and host networking binds the
# host's interfaces (see docs/architecture/service-operations.md, Security boundary). A config
# mounted at the same path replaces the file as a whole, and arguments after
# the image name replace CMD.
USER 10001:10001
WORKDIR /var/lib/zinder

FROM zinder-service-base AS zinder-ingest

COPY --from=zinder-binaries-selected /zinder-binaries/zinder-ingest /usr/local/bin/zinder-ingest

COPY deploy/config/images/ingest.toml /etc/zinder/config.toml

EXPOSE 9105

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["zinder-ingest", "--config", "/etc/zinder/config.toml", "healthcheck", "--path", "/healthz"]

ENTRYPOINT ["zinder-ingest"]
CMD ["--config", "/etc/zinder/config.toml"]

FROM zinder-service-base AS zinder-projector

COPY --from=zinder-binaries-selected /zinder-binaries/zinder-projector /usr/local/bin/zinder-projector

COPY deploy/config/images/projector.toml /etc/zinder/config.toml

EXPOSE 9110

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD ["zinder-projector", "--config", "/etc/zinder/config.toml", "healthcheck"]

ENTRYPOINT ["zinder-projector"]
CMD ["--config", "/etc/zinder/config.toml"]

FROM zinder-service-base AS zinder-query

COPY --from=zinder-binaries-selected /zinder-binaries/zinder-query /usr/local/bin/zinder-query

COPY deploy/config/images/query.toml /etc/zinder/config.toml

EXPOSE 9102 9106

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD ["zinder-query", "--config", "/etc/zinder/config.toml", "healthcheck"]

ENTRYPOINT ["zinder-query"]
CMD ["--config", "/etc/zinder/config.toml"]

FROM zinder-service-base AS zinder-compat-lightwalletd

COPY --from=zinder-binaries-selected /zinder-binaries/zinder-compat-lightwalletd /usr/local/bin/zinder-compat-lightwalletd

COPY deploy/config/images/compat-lightwalletd.toml /etc/zinder/config.toml

EXPOSE 9067 9107

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD ["zinder-compat-lightwalletd", "--config", "/etc/zinder/config.toml", "healthcheck"]

ENTRYPOINT ["zinder-compat-lightwalletd"]
CMD ["--config", "/etc/zinder/config.toml"]

FROM zinder-service-base AS zinder-explorer

COPY --from=zinder-binaries-selected /zinder-binaries/zinder-explorer /usr/local/bin/zinder-explorer

COPY deploy/config/images/explorer.toml /etc/zinder/config.toml

EXPOSE 9068 9069

HEALTHCHECK --interval=30s --timeout=5s --start-period=5m --retries=3 \
    CMD ["zinder-explorer", "--config", "/etc/zinder/config.toml", "healthcheck"]

ENTRYPOINT ["zinder-explorer"]
CMD ["--config", "/etc/zinder/config.toml"]

# Debian base for the images whose POSIX entrypoints and helper scripts need a
# shell, curl, and the zinder account: the single-volume host images, the
# benchmark, and the live-test runner.
FROM debian:${DEBIAN_RELEASE}-slim@${DEBIAN_IMAGE_DIGEST} AS zinder-runtime-base

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

# BuildKit gives directories that `COPY --chmod` creates the file mode, so
# /usr/local/lib/zinder must exist before a 0644 file is copied into it. The
# zinder account uses the per-service images' UID and GID.
RUN groupadd --system --gid 10001 zinder \
    && useradd --system --uid 10001 --gid zinder --home /var/lib/zinder --shell /usr/sbin/nologin zinder \
    && mkdir -p /var/lib/zinder/secondary /etc/zinder /usr/local/lib/zinder \
    && chown -R zinder:zinder /var/lib/zinder /etc/zinder

FROM zinder-runtime-base AS zinder-ingest-live-tests

COPY --from=zinder-ingest-live-test-binary /zinder-live-tests/zinder-ingest-acceptance /usr/local/bin/zinder-ingest-acceptance

USER zinder:zinder
WORKDIR /var/lib/zinder

ENTRYPOINT ["zinder-ingest-acceptance"]

# Single-volume host images. Platforms that attach one volume to one container
# mount it root-owned, so these targets start as root and their entrypoints
# drop every runtime process to zinder.

# Ingest-only diagnostic or canary runtime. It projects no wallet state and
# serves no client protocol.
FROM zinder-runtime-base AS zinder-canonical-runtime

COPY --from=zinder-binaries-selected /zinder-binaries/zinder-ingest /usr/local/bin/zinder-ingest

COPY --chmod=0644 deploy/materialize-indexer-tls \
    /usr/local/lib/zinder/materialize-indexer-tls
COPY --chmod=0755 deploy/canonical-runtime-entrypoint \
    /usr/local/bin/zinder-canonical-runtime-entrypoint

ENV ZINDER_OPS__LISTEN_ADDR=[::]:9105
ENV ZINDER_SECURITY__ALLOW_PUBLIC_BIND=true

USER root:root
WORKDIR /var/lib/zinder

EXPOSE 9105

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["zinder-ingest", "healthcheck", "--path", "/healthz"]

ENTRYPOINT ["zinder-canonical-runtime-entrypoint"]

# Wallet-serving runtime: the canonical writer, the wallet writer, the native
# query reader, and the lightwalletd compatibility reader run as supervised
# processes against the one volume mounted at /var/lib/zinder, because the
# readers open secondaries over paths the writers own. The compatibility
# reader can be turned off with ZINDER_WALLET_SERVING__LIGHTWALLETD=false.
# Hosts with shared volumes across containers use deploy/docker-compose.yml.
FROM zinder-runtime-base AS zinder-wallet-serving

COPY --from=zinder-binaries-selected /zinder-binaries/zinder-ingest /usr/local/bin/zinder-ingest
COPY --from=zinder-binaries-selected /zinder-binaries/zinder-projector /usr/local/bin/zinder-projector
COPY --from=zinder-binaries-selected /zinder-binaries/zinder-query /usr/local/bin/zinder-query
COPY --from=zinder-binaries-selected /zinder-binaries/zinder-compat-lightwalletd /usr/local/bin/zinder-compat-lightwalletd

COPY deploy/config/wallet-serving/ingest.toml /etc/zinder/ingest.toml
COPY deploy/config/wallet-serving/projector.toml /etc/zinder/projector.toml
COPY deploy/config/wallet-serving/query.toml /etc/zinder/query.toml
COPY deploy/config/wallet-serving/compat-lightwalletd.toml /etc/zinder/compat-lightwalletd.toml

COPY --chmod=0644 deploy/materialize-indexer-tls \
    /usr/local/lib/zinder/materialize-indexer-tls
COPY --chmod=0755 deploy/wallet-serving-runtime-entrypoint \
    /usr/local/bin/zinder-wallet-serving-runtime-entrypoint
COPY --chmod=0755 deploy/wallet-serving-healthcheck \
    /usr/local/bin/zinder-wallet-serving-healthcheck

# Platform volume mounts replace the ownership baked into the image, and the
# nofile limit is raised before any store opens. Start as root; the entrypoint
# drops every runtime process to zinder.
USER root:root
WORKDIR /var/lib/zinder

EXPOSE 9067 9102 9106 9107

# The start period covers the entrypoint's default canonical, wallet, and
# serving deadlines, which together run to five and a half hours; query and
# compatibility share the serving deadline. A parked canonical writer counts
# as healthy: both readers then refuse wallets with the typed
# REORG_WINDOW_EXCEEDED halt, and the entrypoint keeps that refusal up. A
# projector parked on a wallet transition over its logical-byte limit counts
# as healthy too, because restarting only parks it on the same transition.
HEALTHCHECK --interval=30s --timeout=5s --start-period=6h --retries=3 \
    CMD ["zinder-wallet-serving-healthcheck"]

ENTRYPOINT ["zinder-wallet-serving-runtime-entrypoint"]

# Wallet serving plus the Explorer on one volume. zinder-explorer opens the
# materialized-view store that zinder-ingest writes as a RocksDB secondary, so
# it must share the writer's filesystem. The baked ingest config differs from
# zinder-wallet-serving in one setting: ingest also writes the Explorer views.
FROM zinder-wallet-serving AS zinder-wallet-serving-explorer

COPY --from=zinder-binaries-selected /zinder-binaries/zinder-explorer /usr/local/bin/zinder-explorer

COPY deploy/config/wallet-serving/explorer.toml /etc/zinder/explorer.toml

RUN sed -i 's/^explorer_views = false$/explorer_views = true/' /etc/zinder/ingest.toml \
    && grep -qx 'explorer_views = true' /etc/zinder/ingest.toml

COPY --chmod=0755 deploy/wallet-serving-explorer-runtime-entrypoint \
    /usr/local/bin/zinder-wallet-serving-explorer-runtime-entrypoint

EXPOSE 9067 9068 9069 9102 9106 9107

# The inherited wallet-serving HEALTHCHECK deliberately ignores the Explorer:
# the entrypoint restarts the Explorer alone, and a host that restarts
# unhealthy containers must not tear down wallet serving when it fails.
ENTRYPOINT ["zinder-wallet-serving-explorer-runtime-entrypoint"]

FROM zinder-runtime-base AS zinder-bench

COPY --from=zinder-bench-binary /zinder-bench/zinder-bench /usr/local/bin/zinder-bench
COPY --chmod=0755 deploy/capture-container-resource-evidence.sh /usr/local/bin/zinder-capture-container-resource-evidence

USER zinder:zinder
WORKDIR /var/lib/zinder

ENTRYPOINT ["zinder-capture-container-resource-evidence", "zinder-bench"]
