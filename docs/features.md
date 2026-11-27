# Features and roadmap

This page is the single overview of what Zinder provides, why teams choose it, how it deploys, and what is supported in `v0.26.0` versus in flight or planned. For consumer integration patterns, see [Use cases](use-cases.md); for a side-by-side matrix against Zebra, Zakura, ztreamer, Zingod, and Go lightwalletd, see [Indexer comparison](reference/indexer-comparison.md).

Zinder serves public chain data only. Keys, note scanning, shielded balances, transaction construction, and signing always stay in the wallet or application.

## Why choose Zinder

Zinder indexes the chain once from your Zcash node and serves every consumer from that single canonical view through isolated reader processes:

- **Drop-in lightwalletd + native gRPC-Web:** `zinder-compat-lightwalletd` serves `CompactTxStreamer` for native light wallets and enables gRPC-Web on the same HTTPS endpoint for browser wallets without a separate translating proxy. See [Lightwalletd compatibility](reference/lightwalletd-compatibility.md).
- **Native `WalletQuery` and `zinder-client` Rust SDK:** Every stored read binds to an immutable chain epoch (`ChainEpoch`). Native clients can pin multiple calls to one snapshot (`at_epoch_id` / `snapshot()`) so blocks, tree states, and transparent queries never mix competing tips across a reorg. See [Wallet data plane](architecture/wallet-data-plane.md) and [Integration surfaces](reference/integration-surfaces.md).
- **Resumable, reorg-aware `ChainEvents`:** Backends follow committed blocks (`ChainCommitted`) and reorgs (`ChainReorged`, with explicit reverted and committed ranges) using opaque cursors retained for 7 days by default. See [Chain events](architecture/chain-events.md).
- **Transparent outpoint spend status:** `TransparentOutpointSpendStatus` returns the mined spender for up to 1,024 outpoints per call at one pinned epoch, alongside local UTXO, balance, and address-history queries.
- **Typed `ErrorInfo` retry policies:** Failures carry `google.rpc.ErrorInfo` (`domain = "zinder.dev"`) with stable reason strings that map directly to client retry rules (`RefreshChainEpoch`, `RestartFromEarliestRetained`, `RetryWithBackoff`, `OperatorActionRequired`, `ClientError`). See [Error vocabulary](reference/error-vocabulary.md).
- **Explicit capability discovery:** `ServerInfo` advertises exact capability strings, network identity, and contract revision so clients check support before calling instead of guessing from version numbers. See [Capabilities](reference/capabilities.md).
- **Optional `ExplorerQuery` and Cipherscan compat:** `zinder-explorer` serves canonical-only displaced-block queries (`DisplacedBlockHistory`, `DisplacedBlockDetail`) directly from `RocksDbCanonicalSecondary` once canonical ingest is `READY`, and combines materialized views (`ingest.explorer_views = true`, enabled automatically by `--preset explorer`), canonical secondary reads, and `WalletQuery` for `BlockDetail`, block production, `BlockTransactions`, `CommitmentRootSearch`, fee/flow/ranking analytics, and mempool/search queries (qualified by view catch-up/backfill coverage and `WalletQuery` reachability), pairing with `zinder-compat-cipherscan` for Cipherscan REST compatibility while answering shielded addresses with a typed privacy refusal. See [Explorer plane](architecture/explorer-plane.md) and [Cipherscan adapter](architecture/cipherscan-adapter.md).

## Built for reliability and verification

- **Serving through node outages:** While `zinder-ingest` (or `zinder index`) stays running, wallet readers keep serving stored compact blocks and tree states across all presets — plus local transparent address data in `wallet` and `explorer` (`light-wallet` forwards transparent queries to its node, so those calls fail while the node is unreachable) — during upstream node outages. Reader operational endpoints (`zinder serve --readers-only` or standalone reader roles) keep returning `200` on `/readyz` with `served_epoch_age_seconds` and structured `degraded` dependency causes (an all-in-one `zinder serve` process aggregates `ingest` readiness and returns `503` while `ingest` is degraded), and advancing resumes automatically when a node recovers. See [Readiness](reference/readiness.md#during-a-node-outage).
- **Multi-validator ingestion and broadcast failover:** Zinder certifies **Zebra 7.1.0** and **Zakura 1.8.6** side by side. `zinder-ingest` fails over across ordered `[node]` and `[node_fallbacks.N]` archive validators while following the tip, and transaction broadcasts move to the next verified node within the request. See [Chain ingestion](architecture/chain-ingestion.md#node-endpoint-failover) and [Node source boundary](architecture/node-source-boundary.md).
- **Ed25519-signed `fs-verity` shielded segments:** `publish-shielded-history` seals settled compact-block and nullifier ranges (`ZSHRNG01`) with Ed25519 signatures and enables Linux `fs-verity` where supported. Readers admit signed segments from kernel measurements and serve settled ranges zero-copy, retaining portable verification on other filesystems.
- **SHA-256 verified node-forwarded transaction bodies:** Lean presets record every mined transaction's location and SHA-256 digest during canonical ingestion. When a reader fetches mined transaction bytes from the primary node, it verifies the digest before returning them and fails closed with `DATA_LOSS` on any mismatch. See [ADR-0023](adrs/0023-deployment-profiles.md).
- **Verified edge-snapshot bootstrap ([ADR-0024](adrs/0024-verified-edge-snapshot-bootstrap.md)):** Export a quiesced canonical checkpoint signed with Ed25519 (`zinder snapshot export-edge-snapshot` or `zinder-ingest export-edge-snapshot`) and initialize an independent regional writer (`zinder snapshot bootstrap-from-snapshot` or `zinder-ingest bootstrap-from-snapshot`) from local disk or HTTP(S). Bootstrap verifies every file digest, attests consensus roots and sampled blocks against the local validator, and generates a fresh cursor key before following the tip.
- **Coherent private backups:** `zinder backup <create|verify|restore>` (and `zinder-projector <backup|verify|restore>`) capture and authenticate the canonical and wallet RocksDB stores together at one block boundary for private host replacement, preserving cursor identity. See [Backups and edge snapshots](runbooks/private-backups.md).

## Deployment flexibility

Choose a deployment preset for what Zinder stores and serves, then choose how to package and run it. Step-by-step setup and host sizing live in [Deploying Zinder](runbooks/deploying.md).

### Deployment profiles

Set `--preset` on `zinder serve` / `zinder index` (or `deployment.profile` / `ZINDER_DEPLOYMENT__PROFILE`) to keep storage retention and reader modes aligned ([ADR-0023](adrs/0023-deployment-profiles.md)). Add `--full-blocks` (`storage.full_blocks = true`) on any preset to retain full raw blocks and transactions locally (`raw_blob_policy = "all"`). Mainnet disk and first-sync numbers below are measured on `v0.26.0` against Zebra 7.1.0 on 8 writer cores:

| Preset (`deployment.profile`) | Stores & serves | Measured Mainnet (`v0.26.0`) | Best for |
| --- | --- | --- | --- |
| `wallet` *(Recommended default)* | Canonical + wallet projection, `WalletQuery` + `CompactTxStreamer`, SHA-256 verified node tx bodies (`raw_blob_policy = "none"`) | ~123 GB disk, ~76 min sync | Production wallets, backends, and public endpoints needing local transparent indexes |
| `light-wallet` (`lightwalletd`) | Canonical store only (`ingest` + `compat` + optional `history-publisher`); node-forwarded transparent reads and tx bodies | ~72 GB disk, ~49–60 min sync | Minimal-footprint `CompactTxStreamer` endpoints focused on shielded wallets |
| `explorer` | Everything in `wallet`, plus `ingest.explorer_views = true`, `zinder-explorer` (`ExplorerQuery`), and `zinder-compat-cipherscan` | Additional view disk & build time | Block explorers, search, fee/pool analytics, and Cipherscan REST compatibility |
| Any preset + `--full-blocks` | Adds all raw block and transaction blobs (`raw_blob_policy = "all"`) to the selected preset | Adds ~251+ GiB raw block/tx blobs | Self-hosted raw transaction bodies or full-block streams (`FullBlocksInRange`); omit `deployment.profile` and set `raw_blob_policy = "transactions"` (+~251.1 GiB) for tx blobs only |

### Packaging options

| Option | Status | Summary | Guide |
| --- | --- | --- | --- |
| **Unified OCI image via Docker Compose + systemd (`ghcr.io/zcashfoundation/zinder`)** *(Recommended)* | Supported | Zero-TOML Compose (`zinder-index` + `zinder-serve --readers-only`), optional Prometheus/Grafana profile, signed shielded history, cgroup isolation | [Deploying Zinder](runbooks/deploying.md), [Deploying on a VM](runbooks/deploying-on-a-vm.md) |
| **Single-volume PaaS (`zinder serve`)** | Supported | Supervised multi-process container (`USER 10001:10001`) with T=0 `/healthz` + `/readyz` on `:9108` for Railway or similar single-volume platforms | [Deploying the single-volume container](runbooks/deploying-single-volume.md) |
| **Kubernetes (`StatefulSet`)** | Supported *(Single-host RWO)* | Single-replica `StatefulSet` ([deploy/kubernetes/statefulset.yaml](../deploy/kubernetes/statefulset.yaml)) with `ReadWriteOnce` PVC and `fsGroup: 10001` | [Deploying the single-volume container](runbooks/deploying-single-volume.md#kubernetes-statefulset) |
| **Stateless serverless (Cloud Run, AWS Lambda)** | Not supported *(Planned M3)* | RocksDB secondaries and long-lived streams require persistent stateful instances; horizontal readers wait on M3 PostgreSQL | [Deploying Zinder](runbooks/deploying.md#cloud-and-serverless-boundaries) |
| **Standalone `zinder` binary** | Supported | Reproducible GNU/Linux `x86-64-v3` and `AArch64` archive (`bin/zinder`) with signatures, provenance, and SBOMs ([ADR-0022](adrs/0022-release-artifacts.md)); `zinder run <role>` also invokes individual roles directly | [Deploying on a VM](runbooks/deploying-on-a-vm.md) |
| **Zcash Foundation public endpoints** | Supported | Connect directly on Mainnet or Testnet over gRPC-TLS with zero infrastructure to run | [Integration surfaces](reference/integration-surfaces.md#endpoints) |

## Status and roadmap

Zinder keeps shipped behavior, active engineering work, and decided future architecture explicitly separate.

### Supported today (`v0.26.0`, alpha)

- **Topology:** Single-host `rocksdb` ([ADR-0004](adrs/0004-storage-and-deployment-topologies.md)), canonical and wallet schema `v1`, native contract revision `15`.
- **Presets and surfaces:** `wallet`, `light-wallet`, and `explorer` presets (each combinable with `--full-blocks` / `storage.full_blocks`); `CompactTxStreamer` (with gRPC-Web), `WalletQuery` + `zinder-client`, `ExplorerQuery`, and `zinder-compat-cipherscan`.
- **Node sources:** Certified Zebra 7.1.0 and Zakura 1.8.6 over JSON-RPC and `zebra-indexer-grpc` binary block fetch.
- **Verification and recovery:** Ed25519-signed `ZSHRNG01` segments with `fs-verity`, SHA-256 verified node-forwarded tx bodies, verified edge-snapshot bootstrap ([ADR-0024](adrs/0024-verified-edge-snapshot-bootstrap.md)), and coherent private backups.
- **Alpha upgrade model:** Breaking API, config, or schema layout changes happen between alpha tags; schema layout changes require building a fresh store rather than in-place migration.

### Active engineering in flight (not yet in `v0.26.0`)

- **Canonical Schema `v2` deduplicated settled segments:** Eliminating duplicated settled compact-block storage between RocksDB and signed shielded segments, targeting a ~29–30 GB Mainnet `light-wallet` footprint.

### Decided and planned (not implemented or certified)

- **`postgres` stateless horizontal topology ([ADR-0004](adrs/0004-storage-and-deployment-topologies.md)):** Stateless, independently replaceable Zinder readers backed by a managed PostgreSQL database for hosted fleets. Build work is gated on passing ADR-0004's Mainnet benchmark thresholds first; the current PostgreSQL bench arm is diagnostic only.
- **Resumable initial construction:** Checkpointing from-genesis build progress so an interrupted first sync resumes instead of restarting from height 1.
