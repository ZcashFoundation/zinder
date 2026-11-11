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
#     docker build -f deploy/Dockerfile --target zinder-explorer -t zinder-explorer .
#     docker build -f deploy/Dockerfile --target zinder-bench -t zinder-bench .
#     docker build -f deploy/Dockerfile --target zinder-ingest-live-tests -t zinder-ingest-live-tests .
#
# Every runtime target depends on the same `zinder-binaries` stage. That stage
# compiles all shipped binaries in one Cargo invocation and keeps Cargo registry,
# git, and target artifacts in BuildKit cache mounts. Rebuilding one service
# warms the shared cache for every other service image.

ARG RUST_VERSION=1.95.0
ARG DEBIAN_RELEASE=bookworm
ARG RUST_IMAGE_DIGEST=sha256:6258907abe69656e41cd992e0b705cdcfabcbbe3db374f92ed2d47121282d4a1
ARG DEBIAN_IMAGE_DIGEST=sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818

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
RUN cargo install cargo-auditable --version 0.7.4 --locked

FROM zinder-build-base AS zinder-binaries

ARG RUST_VERSION
ARG TARGETARCH
ARG TARGETVARIANT
ARG ZINDER_BUILD_GIT_COMMIT="unknown"
ARG SOURCE_DATE_EPOCH=0
ARG RELEASE_CACHE_SCOPE=shared

# x86-64-v3 also reaches the vendored RocksDB C++ (hardware CRC32c, AVX2);
# scoping to the target triple keeps build scripts and non-x86 images on the
# baseline ISA. Runtime hosts are verified at startup before storage opens.
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq --remap-path-prefix=/workspace=/usr/src/zinder --remap-path-prefix=/usr/local/cargo=/cargo"
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_RUSTFLAGS="--remap-path-prefix=/workspace=/usr/src/zinder --remap-path-prefix=/usr/local/cargo=/cargo"
ENV CARGO_PROFILE_RELEASE_STRIP=symbols
ENV SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH}
ENV ZINDER_BUILD_GIT_COMMIT=${ZINDER_BUILD_GIT_COMMIT}

COPY . .

RUN --mount=type=cache,id=zinder-cargo-registry-${RUST_VERSION}-${TARGETARCH}${TARGETVARIANT},target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=zinder-cargo-git-${RUST_VERSION}-${TARGETARCH}${TARGETVARIANT},target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=zinder-cargo-target-${RUST_VERSION}-${TARGETARCH}${TARGETVARIANT}-${RELEASE_CACHE_SCOPE},target=/workspace/target,sharing=locked \
    rust_target="$(case "$TARGETARCH" in amd64) printf '%s' x86_64-unknown-linux-gnu ;; arm64) printf '%s' aarch64-unknown-linux-gnu ;; *) exit 1 ;; esac)" \
    && rustup target add "$rust_target" \
    && cargo auditable build --locked --release --target "$rust_target" \
        --bin zinder-ingest \
        --bin zinder-projector \
        --bin zinder-query \
        --bin zinder-compat-lightwalletd \
        --bin zinder-explorer \
        --bin zinder-bench \
    && mkdir -p /zinder-binaries \
    && cp "/workspace/target/${rust_target}/release/zinder-ingest" /zinder-binaries/zinder-ingest \
    && cp "/workspace/target/${rust_target}/release/zinder-projector" /zinder-binaries/zinder-projector \
    && cp "/workspace/target/${rust_target}/release/zinder-query" /zinder-binaries/zinder-query \
    && cp "/workspace/target/${rust_target}/release/zinder-compat-lightwalletd" /zinder-binaries/zinder-compat-lightwalletd \
    && cp "/workspace/target/${rust_target}/release/zinder-explorer" /zinder-binaries/zinder-explorer \
    && cp "/workspace/target/${rust_target}/release/zinder-bench" /zinder-binaries/zinder-bench

FROM scratch AS release-binaries

COPY --from=zinder-binaries /zinder-binaries/zinder-ingest /bin/zinder-ingest
COPY --from=zinder-binaries /zinder-binaries/zinder-projector /bin/zinder-projector
COPY --from=zinder-binaries /zinder-binaries/zinder-query /bin/zinder-query
COPY --from=zinder-binaries /zinder-binaries/zinder-compat-lightwalletd /bin/zinder-compat-lightwalletd

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

FROM debian:${DEBIAN_RELEASE}-slim@${DEBIAN_IMAGE_DIGEST} AS zinder-runtime-base

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --system --gid 1000 zinder \
    && useradd --system --uid 1000 --gid zinder --home /var/lib/zinder --shell /usr/sbin/nologin zinder \
    && mkdir -p /var/lib/zinder/secondary /etc/zinder \
    && chown -R zinder:zinder /var/lib/zinder /etc/zinder

FROM zinder-runtime-base AS zinder-ingest

COPY --from=zinder-binaries /zinder-binaries/zinder-ingest /usr/local/bin/zinder-ingest

USER zinder:zinder
WORKDIR /var/lib/zinder

EXPOSE 9105

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS http://localhost:9105/healthz || exit 1

ENTRYPOINT ["zinder-ingest"]

FROM zinder-runtime-base AS zinder-projector

COPY --from=zinder-binaries /zinder-binaries/zinder-projector /usr/local/bin/zinder-projector

USER zinder:zinder
WORKDIR /var/lib/zinder

EXPOSE 9110

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD curl -fsS http://localhost:9110/readyz || exit 1

ENTRYPOINT ["zinder-projector"]

FROM zinder-runtime-base AS zinder-ingest-live-tests

COPY --from=zinder-ingest-live-test-binary /zinder-live-tests/zinder-ingest-acceptance /usr/local/bin/zinder-ingest-acceptance

USER zinder:zinder
WORKDIR /var/lib/zinder

ENTRYPOINT ["zinder-ingest-acceptance"]

FROM zinder-runtime-base AS zinder-query

COPY --from=zinder-binaries /zinder-binaries/zinder-query /usr/local/bin/zinder-query

USER zinder:zinder
WORKDIR /var/lib/zinder

EXPOSE 9102 9106

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD curl -fsS http://localhost:9106/readyz || exit 1

ENTRYPOINT ["zinder-query"]

FROM zinder-runtime-base AS zinder-compat-lightwalletd

COPY --from=zinder-binaries /zinder-binaries/zinder-compat-lightwalletd /usr/local/bin/zinder-compat-lightwalletd

USER zinder:zinder
WORKDIR /var/lib/zinder

EXPOSE 9067 9107

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD curl -fsS http://localhost:9107/readyz || exit 1

ENTRYPOINT ["zinder-compat-lightwalletd"]

# Single-volume host images. Platforms that attach one volume to one container
# mount it root-owned, so these targets start as root and their entrypoints
# drop every runtime process to zinder.

# Ingest-only diagnostic or canary runtime. It projects no wallet state and
# serves no client protocol.
FROM zinder-ingest AS zinder-canonical-runtime

COPY --chmod=0755 deploy/canonical-runtime-entrypoint \
    /usr/local/bin/zinder-canonical-runtime-entrypoint

ENV ZINDER_OPS__LISTEN_ADDR=[::]:9105
ENV ZINDER_SECURITY__ALLOW_PUBLIC_BIND=true

USER root:root

ENTRYPOINT ["zinder-canonical-runtime-entrypoint"]

# Native WalletQuery runtime: the canonical writer, the wallet writer, and the
# query reader run as three supervised processes against the one volume
# mounted at /var/lib/zinder, because the readers open secondaries over paths
# the writers own. It does not serve the lightwalletd compatibility protocol.
# Hosts with shared volumes across containers use deploy/docker-compose.yml.
FROM zinder-runtime-base AS zinder-wallet-serving

COPY --from=zinder-binaries /zinder-binaries/zinder-ingest /usr/local/bin/zinder-ingest
COPY --from=zinder-binaries /zinder-binaries/zinder-projector /usr/local/bin/zinder-projector
COPY --from=zinder-binaries /zinder-binaries/zinder-query /usr/local/bin/zinder-query

COPY deploy/config/wallet-serving/ingest.toml /etc/zinder/ingest.toml
COPY deploy/config/wallet-serving/projector.toml /etc/zinder/projector.toml
COPY deploy/config/wallet-serving/query.toml /etc/zinder/query.toml

COPY --chmod=0755 deploy/wallet-serving-runtime-entrypoint \
    /usr/local/bin/zinder-wallet-serving-runtime-entrypoint
COPY --chmod=0755 deploy/wallet-serving-healthcheck \
    /usr/local/bin/zinder-wallet-serving-healthcheck

# Platform volume mounts replace the ownership baked into the image, and the
# nofile limit is raised before any store opens. Start as root; the entrypoint
# drops every runtime process to zinder.
USER root:root
WORKDIR /var/lib/zinder

EXPOSE 9102 9106

# The start period covers the entrypoint's canonical, wallet, and serving
# construction deadlines, which together run to five and a half hours. A parked
# canonical writer counts as healthy: query then refuses wallets with the typed
# REORG_WINDOW_EXCEEDED halt, and the entrypoint keeps that refusal up.
HEALTHCHECK --interval=30s --timeout=5s --start-period=6h --retries=3 \
    CMD ["zinder-wallet-serving-healthcheck"]

ENTRYPOINT ["zinder-wallet-serving-runtime-entrypoint"]

FROM zinder-runtime-base AS zinder-explorer

COPY --from=zinder-binaries /zinder-binaries/zinder-explorer /usr/local/bin/zinder-explorer

USER zinder:zinder
WORKDIR /var/lib/zinder

EXPOSE 9068 9069

HEALTHCHECK --interval=30s --timeout=5s --start-period=5m --retries=3 \
    CMD curl -fsS http://localhost:9069/readyz || exit 1

ENTRYPOINT ["zinder-explorer"]

FROM zinder-runtime-base AS zinder-bench

COPY --from=zinder-binaries /zinder-binaries/zinder-bench /usr/local/bin/zinder-bench
COPY --chmod=0755 deploy/capture-container-resource-evidence.sh /usr/local/bin/zinder-capture-container-resource-evidence

USER zinder:zinder
WORKDIR /var/lib/zinder

ENTRYPOINT ["zinder-capture-container-resource-evidence", "zinder-bench"]
