# syntax=docker/dockerfile:1.7

# Moves Cargo state between a runner directory and the BuildKit cache mounts
# that deploy/Dockerfile compiles in. BuildKit never exports cache mounts with
# `--cache-to`, so the action copies them through these stages instead. The
# mount ids must stay identical to the `zinder-binaries` stage there.
#
# `seed` copies a restored directory into the mounts. `export` copies the mounts
# out, without workspace artifacts, because every release build recompiles the
# workspace crates anyway.
#
# Each run passes a unique CACHE_BUST so BuildKit never reuses a cached layer
# and skips the copy. `--no-cache` would not do: it also gives the RUN empty
# cache mounts and discards what the RUN writes to them.

ARG BASE_IMAGE

FROM ${BASE_IMAGE} AS seed

ARG RUST_VERSION
ARG TARGETARCH
ARG TARGETVARIANT
ARG RELEASE_CACHE_SCOPE
ARG CACHE_BUST

RUN --mount=type=bind,from=cargo-seed,target=/seed \
    --mount=type=cache,id=zinder-cargo-registry-${RUST_VERSION}-${TARGETARCH}${TARGETVARIANT},target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=zinder-cargo-git-${RUST_VERSION}-${TARGETARCH}${TARGETVARIANT},target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=zinder-cargo-target-${RUST_VERSION}-${TARGETARCH}${TARGETVARIANT}-${RELEASE_CACHE_SCOPE},target=/workspace/target,sharing=locked \
    set -eu; \
    echo "seeding run ${CACHE_BUST}"; \
    for pair in registry:/usr/local/cargo/registry git:/usr/local/cargo/git target:/workspace/target; do \
        if [ -d "/seed/${pair%%:*}" ]; then cp -a "/seed/${pair%%:*}/." "${pair#*:}/"; fi; \
    done; \
    du -sh /usr/local/cargo/registry /usr/local/cargo/git /workspace/target

FROM ${BASE_IMAGE} AS collect

ARG RUST_VERSION
ARG TARGETARCH
ARG TARGETVARIANT
ARG RELEASE_CACHE_SCOPE
ARG WORKSPACE_PACKAGES
ARG CACHE_BUST

RUN --mount=type=cache,id=zinder-cargo-registry-${RUST_VERSION}-${TARGETARCH}${TARGETVARIANT},target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=zinder-cargo-git-${RUST_VERSION}-${TARGETARCH}${TARGETVARIANT},target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=zinder-cargo-target-${RUST_VERSION}-${TARGETARCH}${TARGETVARIANT}-${RELEASE_CACHE_SCOPE},target=/workspace/target,sharing=locked \
    set -eu; \
    echo "export run ${CACHE_BUST}"; \
    test -n "${WORKSPACE_PACKAGES}"; \
    mkdir -p /out; \
    cp -a /usr/local/cargo/registry /out/registry; \
    cp -a /usr/local/cargo/git /out/git; \
    cp -a /workspace/target /out/target; \
    for profile_dir in /out/target/release /out/target/*/release; do \
        [ -d "$profile_dir" ] || continue; \
        rm -rf "$profile_dir/examples" "$profile_dir/incremental"; \
        for package in ${WORKSPACE_PACKAGES}; do \
            crate="$(printf '%s' "$package" | tr - _)"; \
            for dir in "$profile_dir" "$profile_dir/deps" "$profile_dir/.fingerprint" "$profile_dir/build"; do \
                rm -rf "$dir/$package" "$dir/$package.d" "$dir/$package-"* \
                    "$dir/$crate" "$dir/$crate.d" "$dir/$crate-"* \
                    "$dir/lib$crate."* "$dir/lib$crate-"*; \
            done; \
        done; \
    done; \
    du -sh /out/registry /out/git /out/target

FROM scratch AS export

COPY --from=collect /out/ /
