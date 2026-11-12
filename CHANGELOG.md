# Changelog

All notable changes to Zinder are documented in this file. Zinder releases one
lockstep product version across its first-party crates, services, runtime
images, and API artifacts.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]

### Added

- Each release now publishes two more multi-architecture images for hosts that attach one volume to one container. `ghcr.io/zcashfoundation/zinder-wallet-serving` runs ingest, projector, and native query as supervised processes and omits lightwalletd compatibility. `ghcr.io/zcashfoundation/zinder-canonical-runtime` is the ingest-only diagnostic or canary runtime. Both carry the same provenance, SBOM, and signature evidence as the four runtime images, and neither is the complete supported topology. ([#128](https://github.com/gustavovalverde/zinder/pull/128))

### Removed

- `deploy/Dockerfile.wallet-serving`, `deploy/Dockerfile.railway-nocache`, `railway.toml`, `deploy/railway.canonical-runtime.toml`, and `deploy/railway.wallet-serving-runtime.toml` are removed, along with the `RAILWAY_DOCKER_TARGET_STAGE` build argument. Pull the published `zinder-wallet-serving` or `zinder-canonical-runtime` image instead, or build the target of that name from `deploy/Dockerfile`. Railway services must switch from a repository build to a Docker image source, then set the healthcheck path, healthcheck timeout, restart policy, and drain window in the service settings as the deployment runbooks describe. ([#128](https://github.com/gustavovalverde/zinder/pull/128))


## [0.9.0-rc.1] - 2026-11-12

### Added

- Each release now publishes two more multi-architecture images for hosts that attach one volume to one container. `ghcr.io/zcashfoundation/zinder-wallet-serving` runs ingest, projector, and native query as supervised processes and omits lightwalletd compatibility. `ghcr.io/zcashfoundation/zinder-canonical-runtime` is the ingest-only diagnostic or canary runtime. Both carry the same provenance, SBOM, and signature evidence as the four runtime images, and neither is the complete supported topology. ([#128](https://github.com/gustavovalverde/zinder/pull/128))

### Removed

- `deploy/Dockerfile.wallet-serving`, `deploy/Dockerfile.railway-nocache`, `railway.toml`, `deploy/railway.canonical-runtime.toml`, and `deploy/railway.wallet-serving-runtime.toml` are removed, along with the `RAILWAY_DOCKER_TARGET_STAGE` build argument. Pull the published `zinder-wallet-serving` or `zinder-canonical-runtime` image instead, or build the target of that name from `deploy/Dockerfile`. Railway services must switch from a repository build to a Docker image source, then set the healthcheck path, healthcheck timeout, restart policy, and drain window in the service settings as the deployment runbooks describe. ([#128](https://github.com/gustavovalverde/zinder/pull/128))


## [0.8.0] - 2026-11-11

### Removed

- Ingest no longer accepts the configuration keys of its removed batch catch-up and tip-follow paths. Remove `ingest.construction.canonical_batch_max_blocks`, `canonical_batch_max_artifact_bytes`, `canonical_batch_max_estimated_write_bytes`, `canonical_batch_min_blocks_before_estimated_write_close`, `commit_reassembly_max_queued_artifact_bytes`, `flush_interval_epochs`, `ingest.run_overrides.allow_reorg_window_settlement`, and the ingest `storage.secondary_path` key, along with their `ZINDER_INGEST__*` environment variables, before upgrading: ingest refuses to start while any of them is set. The `zinder-ingest verify-canonical-replay` subcommand is removed; it could not open a canonical store written by the release ingest path. The storage benchmark drops its canonical-store range replay scenario and Compose profile, and the `ZinderStoreReadP95High` alert and its dashboard panel are removed because no release runtime emits the store read metrics they watched. Explorer methods that need canonical storage keep returning `FAILED_PRECONDITION` with `DEPENDENCY_NOT_CONFIGURED`, as they did before; the Cipherscan compatibility matrix now marks the routes that depend on them. ([#115](https://github.com/gustavovalverde/zinder/pull/115))

### Fixed

- Lightwalletd compatibility now refuses to start when its canonical secondary disagrees with the ingest writer fence on the visible block count, which it previously accepted. Native query now fails closed on a writer fence that diverges from its secondary instead of retrying it as replica lag until its attempts run out. Projector, native query, and lightwalletd compatibility now apply the same fence rules. ([#116](https://github.com/gustavovalverde/zinder/pull/116))
- Wallet reads served by `zinder-query` and `zinder-compat-lightwalletd` now record `zinder_query_request_*` metrics. Before, only operations proxied through the native adapter produced samples, so the `ZinderWalletQueryP95High` alert and the overview dashboard missed most wallet traffic. Expect new samples for every wallet read, including reads the serving store cannot answer. ([#117](https://github.com/gustavovalverde/zinder/pull/117))
- Explorer `/readyz` now reports `schema_mismatch` or `storage_unavailable` while its materialized-view catch-up fails, instead of staying ready. Ingest now stays `upstream_not_ready` until its Zebra health probe recovers; the follow loop no longer clears that cause, and `WriterStatus` reports the probe observation during construction and source recovery. When `[node.health]` is configured, ingest reports `starting` until the first successful probe instead of `ready`. Ingest, Explorer, and the Cipherscan adapter now report `shutting_down` as soon as termination is requested. ([#118](https://github.com/gustavovalverde/zinder/pull/118))
- A materialized-view rebuild that resumes without chain-event cursors after a reorg now reverts rows from the replaced chain before rebuilding, instead of keeping them. The transparent address activity index and the address ranking undo journals now write the same bytes regardless of hash-map order or batch size, so replaying the same chain produces identical materialized-view stores. ([#119](https://github.com/gustavovalverde/zinder/pull/119))
- Native `CompactBlocksInRange` and lightwalletd `GetBlockRange` now return `NOT_FOUND` with reason `ARTIFACT_UNAVAILABLE` for a range that reaches outside stored history, such as height 0 or a height below the retained floor, instead of `UNAVAILABLE`. A missing row inside retained history still reports a storage error, and `GetBlockRange` above the visible tip still returns `OUT_OF_RANGE`. ([#122](https://github.com/gustavovalverde/zinder/pull/122))
- Subtree-root reads on a checkpointed store now return `NOT_FOUND` with reason `ARTIFACT_UNAVAILABLE` for a root that completed below the retained history and was never loaded, instead of `UNAVAILABLE`. A missing root the store loaded, including any root of a store built with the complete subtree-root prefix, still reports a storage error. ([#123](https://github.com/gustavovalverde/zinder/pull/123))


## [0.7.0] - 2026-11-10

### Added

- Configure optional Explorer indexing and canonical compact-block compatibility at runtime without rebuilding the image; Explorer views default off and wallet serving remains independent. ([#88](https://github.com/gustavovalverde/zinder/pull/88))
- When ingest parks because a reorg exceeds its window, native query now refuses new wallet requests with `REORG_WINDOW_EXCEEDED`, carrying `replacement_depth_blocks` and `reorg_window_blocks` metadata, and ends open `ChainEvents` and `MempoolEvents` streams with the same status instead of leaving them idle on a chain that stopped advancing. Ingest also stops following the node mempool at the park, so it records no mempool events against the frozen fence. `zinder-compat-lightwalletd` refuses requests with the same status in both wallet and compact-block serving instead of reporting ready on the frozen tip, including while writer status is briefly unreachable. After an ingest restart, native query and compatibility report the new `canonical_tip_unconfirmed` readiness cause and refuse traffic until ingest has confirmed its stored tip against the node, which normally takes one poll; they stay unready while ingest cannot reach the node, rather than serve a tip the node may have abandoned. Upgrade ingest, the projector, native query, and compatibility together: the halt travels in new writer-status fields, so a reader from an earlier release keeps reporting ready against a parked writer. `zinder-client` reports it as `IndexerError::ReorgWindowExceeded` with retry policy `OperatorActionRequired`. The parked writer keeps serving its frozen fence to the projector and query, and the single-host wallet-serving image now keeps running in this state instead of restarting into the same park, so wallets keep receiving the typed refusal; its container `HEALTHCHECK` counts that state as healthy. ([#107](https://github.com/gustavovalverde/zinder/pull/107))

### Changed

- The README warns that Zinder is alpha and that native WalletQuery, lightwalletd compatibility, storage layouts, and deployment topology can still change. ([#96](https://github.com/gustavovalverde/zinder/pull/96))
- Existing canonical and wallet stores now start against a node that announces a future network upgrade, such as Zebra 7.0.0 adding NU7, without a rebuild. A node table that changes an upgrade the store has already passed is refused, and the store is left unchanged. Running query and lightwalletd-compatible services follow the newly admitted table without a restart. `GetLightdInfo` now reports the next pending upgrade in `upgradeName` and `upgradeHeight`, as the lightwalletd protocol defines, instead of the active one: NU7 and its activation height before NU7 activates, then an empty name and height 0 with NU7's `consensusBranchId`. The first start of this release writes schedule revision 1 to the canonical store even when the node's table is unchanged, so earlier Zinder releases can no longer open that store; upgrade all Zinder services together and restart ingest before the readers. Rust callers of `LightwalletdGrpcAdapter::new`, `with_response_limits`, and `from_admitted_compatibility_query` no longer pass an activation table. ([#106](https://github.com/gustavovalverde/zinder/pull/106))
- The default reorg window for ingest, projector, native query, and lightwalletd compatibility is now 1000 blocks, the depth past which Zebra 7 and Zakura finalize blocks, so ingest no longer parks on a reorg the node itself can produce. The settled tip, and every finality status derived from it, now trails the visible tip by up to 1000 blocks. Upgrade warning: deployments that rely on the default, on the shipped `deploy/config` files, or on the wallet-serving image move to 1000 on their first start, and that raise is irreversible: earlier Zinder releases cannot open the raised stores, and the window cannot be lowered in place. To keep a rollback path, capture a state bundle before starting this release. Deployments that set `reorg_window_blocks` explicitly keep their value. Starting ingest with a larger window raises an existing canonical store in place, and the projector raises the wallet store when it resumes, so restart ingest first, then the projector, native query, and compatibility with the same value. Replaceable depth grows to the new window as blocks arrive. A smaller window is refused and still requires a rebuild. A replacement now holds at most `ingest.follow.max_replacement_bytes` (2 GiB by default) of fetched and prepared blocks before its atomic commit and fails with `ReplacementBytesExceeded` beyond it; ingest then exits and retries on restart rather than parking, so raise the limit to follow such a reorg. Live commits and reader catch-ups now authenticate only the replay rows a transition changed, so their cost no longer grows with the window. Cipherscan `finality_status` now reports `Finalized` only for blocks at or below the settled tip instead of after 100 confirmations. ([#107](https://github.com/gustavovalverde/zinder/pull/107))

### Fixed

- The wallet projector no longer exits and crash-loops when the canonical writer commits while it is confirming the writer's position; it retries with backoff for up to projector.fence_convergence_timeout_seconds (default 30) and reports syncing if the lag lasts more than a second. A genuine mismatch between projector and writer still fails immediately. ([#97](https://github.com/gustavovalverde/zinder/pull/97))
- Ingest now waits for a node whose tip falls to or below the settled tip on the same branch, as after a Zebra restart, instead of parking with reorg_window_exceeded. ([#110](https://github.com/gustavovalverde/zinder/pull/110))

### Security

- HTTP/2 serving now uses a patched h2 crate so empty DATA frames cannot grow unbounded in memory. ([#96](https://github.com/gustavovalverde/zinder/pull/96))
- TLS connections now use rustls 0.23.45, which rejects TLS 1.3 handshake messages sent at the wrong encryption level (RUSTSEC-2026-0285). ([#97](https://github.com/gustavovalverde/zinder/pull/97))
- Dependencies now resolve to patched rustls, rustls-webpki, webpki-root-certs, tokio, libz-sys, zstd-sys, and pulldown-cmark releases that fix RUSTSEC-2026-0346 to 0349, 0382, 0383, 0386, 0410, 0414, 0418, and 0433. ([#111](https://github.com/gustavovalverde/zinder/pull/111))


## [0.6.0] - 2026-08-10

### Changed

- Lightwalletd-compatible deployments now advertise transparent-address support only when their serving data can answer those requests. Startup rejects configurations that lack a transparent wallet projection or retained transaction bytes. ([#71](https://github.com/gustavovalverde/zinder/pull/71))
- Document the production Testnet Zebra readiness calibration that keeps ordinary block gaps available while peer, sync, lag, and stale-tip failures remain fail-closed. ([#73](https://github.com/gustavovalverde/zinder/pull/73))

### Fixed

- Stale fixed-view mempool epochs now reach the typed view-expiration path so wallet consumers can reacquire the view without treating expected chain rotation as service unavailability. ([#69](https://github.com/gustavovalverde/zinder/pull/69))
- Secondary query and projection processes no longer create thousands of short-lived storage threads per second while following the chain. ([#72](https://github.com/gustavovalverde/zinder/pull/72))


## [0.5.0] - 2026-07-29

### Added

- Expose network upgrade activations through the native query API. ([#16](https://github.com/gustavovalverde/zinder/pull/16))
- Add the optional Cipherscan-compatible explorer API adapter. ([#15](https://github.com/gustavovalverde/zinder/pull/15))
- Ship the native WalletQuery API alongside the lightwalletd compatibility service. ([#22](https://github.com/gustavovalverde/zinder/pull/22))
- Publish guarded amd64 and arm64 runtime images, API artifacts, and lockstep GitHub Releases from validated tags. ([#24](https://github.com/gustavovalverde/zinder/pull/24))
- Add borrowed and Arc-owned chain snapshots that pin canonical wallet reads to one chain epoch and return typed refresh guidance when that epoch is unavailable. ([#28](https://github.com/gustavovalverde/zinder/pull/28))
- Prepare `zinder-core`, `zinder-proto`, and `zinder-client` as a registry-ready Rust SDK catalog with hermetic protocol artifacts and standalone package verification. ([#30](https://github.com/gustavovalverde/zinder/pull/30))
- Publish `zinder-core`, `zinder-proto`, and `zinder-client` from tagged releases through guarded crates.io trusted publishing with resumable source-provenance verification. ([#31](https://github.com/gustavovalverde/zinder/pull/31))
- Publish deterministic x86-64-v3 and AArch64 GNU/Linux bundles containing the four supported Zinder runtime executables with checksums and build metadata. ([#32](https://github.com/gustavovalverde/zinder/pull/32))
- Publish keyless signatures, SLSA provenance, and SPDX 2.3 SBOMs for Zinder GNU/Linux bundles and release images. ([#33](https://github.com/gustavovalverde/zinder/pull/33))
- Run the canonical writer, the wallet projector, and the native WalletQuery reader as one supervised container against a single volume, for hosts that attach one volume per service. ([#60](https://github.com/gustavovalverde/zinder/pull/60))

### Changed

- Establish the fact-first RocksDB lifecycle with bounded catchup, projection workloads, and coherent checkpoint recovery. ([#17](https://github.com/gustavovalverde/zinder/pull/17))
- Cut wallet serving over to canonical facts, projection-aware readiness, and native operational controls. ([#18](https://github.com/gustavovalverde/zinder/pull/18))
- Replace transitional service and configuration names with the final ingest, projector, query, and compatibility vocabulary. ([#19](https://github.com/gustavovalverde/zinder/pull/19))
- Require mempool snapshots to match the visible chain tip, bound ingestion and retention work, and treat ordinary tip movement as synchronization instead of a source failure. ([#27](https://github.com/gustavovalverde/zinder/pull/27))
- Make `zinder-client` remote-first without RocksDB or storage dependencies, remove the unused storage-backed local adapter, and add typed network-upgrade discovery for chain-index consumers. ([#29](https://github.com/gustavovalverde/zinder/pull/29))
- Update the Ironwood dependency stack and report the NU6.3 mainnet activation height in the Cipherscan migration overview. ([#51](https://github.com/gustavovalverde/zinder/pull/51))
- Build the explorer materialized views inside the ingest runtime from canonical storage. The view store is built from genesis on first start, the `[storage.materialized_views]` RocksDB budget now also applies to `zinder-ingest`, and stores built with a checkpoint height do not host materialized views. ([#52](https://github.com/gustavovalverde/zinder/pull/52))
- Certify live regtest and lightwalletd parity behavior against Zebra 6.2.2, including its current RPC consistency and security fixes. ([#57](https://github.com/gustavovalverde/zinder/pull/57))
- Derive native WalletQuery capabilities from admitted storage and providers, fail invalid compositions before serving, and support the Regtest-certified source-built Zallet backend. Regtest JSON-RPC health treats a successful node tip observation as ready because the isolated chain has no public-network tip. Native contract revision 5 removes unauthenticated retention-duration fields from WalletServerInfo and replaces COMPACT_BLOCK_RANGE_TOO_LARGE with BLOCK_RANGE_TOO_LARGE; generated native clients must be regenerated. ([#67](https://github.com/gustavovalverde/zinder/pull/67))

### Fixed

- Preserve configured artifact-store contracts when canonical ingest opens or rebuilds storage. ([#20](https://github.com/gustavovalverde/zinder/pull/20))
- Keep the ingest operations endpoint available after a reorg exceeds the configured window so operators can inspect the drained state and rebuild or restore without a crash loop. ([#25](https://github.com/gustavovalverde/zinder/pull/25))
- Allow tagged release retries to find and resume an existing draft GitHub Release while still rejecting published releases and API failures. ([#35](https://github.com/gustavovalverde/zinder/pull/35))
- Restore post-publication SDK verification against a fresh crates.io-only consumer. ([#37](https://github.com/gustavovalverde/zinder/pull/37))
- Make release image publication wait for GHCR tag convergence and verify attestations with compatible strict identity constraints. ([#39](https://github.com/gustavovalverde/zinder/pull/39))
- Keep deployment admission aligned with the strict release image attestation verifier. ([#40](https://github.com/gustavovalverde/zinder/pull/40))
- Accept the compatibility image's declared native query dependency in release SBOM evidence. ([#41](https://github.com/gustavovalverde/zinder/pull/41))
- Verify release archives and stable image promotion with compatible attestation identity selectors. ([#43](https://github.com/gustavovalverde/zinder/pull/43))
- Check out the validated release commit before preparing draft GitHub Release assets. ([#45](https://github.com/gustavovalverde/zinder/pull/45))
- Bind GitHub Release publication to the validated repository when the publication job runs without a checkout. ([#47](https://github.com/gustavovalverde/zinder/pull/47))
- Correct six documented identifiers that no longer matched the shipped API, including the mempool cursor error, the subtree roots RPC, and the capability table name. ([#50](https://github.com/gustavovalverde/zinder/pull/50))
- Expose typed wallet-sync capabilities through zinder-client, preserve cold admission when a block hash is displaced repeatedly, and provide a current-contract ExplorerQuery overlay that serves transaction detail and transparent activity without opening the canonical store. ([#53](https://github.com/gustavovalverde/zinder/pull/53))
- Prevent the single-host ingest runtime from exhausting file descriptors while opening the canonical and Explorer materialized-view stores. ([#54](https://github.com/gustavovalverde/zinder/pull/54))
- Keep the native WalletQuery service responsive under concurrent range reads; syncing wallets no longer lose their streams, and readiness stays answerable while a wallet catches up. ([#60](https://github.com/gustavovalverde/zinder/pull/60))


## [0.4.0] - 2026-07-11

Zinder's release history before the changeset-style changelog is available in
the repository at the [v0.4.0 tag](https://github.com/gustavovalverde/zinder/tree/v0.4.0).
