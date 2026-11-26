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
- **Optional `ExplorerQuery`:** `zinder-explorer` adds block and transaction detail, typed search, mempool views, fee summaries, and value-pool aggregates over rebuildable materialized views, answering shielded addresses with a typed privacy refusal. See [Explorer plane](architecture/explorer-plane.md).

## Built for reliability and verification

- **Serving through node outages:** While `zinder-ingest` stays running, wallet readers keep serving stored compact blocks, tree states, and transparent address data during upstream node outages. `/readyz` continues returning `200` with `served_epoch_age_seconds` and structured `degraded` dependency causes, and advancing resumes automatically when a node recovers. See [Readiness](reference/readiness.md#during-a-node-outage).
- **Multi-validator ingestion and broadcast failover:** Zinder certifies **Zebra 7.1.0** and **Zakura 1.8.6** side by side. `zinder-ingest` fails over across ordered `[node]` and `[node_fallbacks.N]` archive validators while following the tip, and transaction broadcasts move to the next verified node within the request. See [Chain ingestion](architecture/chain-ingestion.md#node-endpoint-failover) and [Node source boundary](architecture/node-source-boundary.md).
- **Ed25519-signed `fs-verity` shielded segments:** `publish-shielded-history` seals settled compact-block and nullifier ranges (`ZSHRNG01`) with Ed25519 signatures and enables Linux `fs-verity` where supported. Readers admit signed segments from kernel measurements and serve settled ranges zero-copy, retaining portable verification on other filesystems.
- **SHA-256 verified node-forwarded transaction bodies:** Lean profiles record every mined transaction's location and SHA-256 digest during canonical ingestion. When a reader fetches mined transaction bytes from the primary node, it verifies the digest before returning them and fails closed with `DATA_LOSS` on any mismatch. See [ADR-0023](adrs/0023-deployment-profiles.md).
- **Verified edge-snapshot bootstrap ([ADR-0024](adrs/0024-verified-edge-snapshot-bootstrap.md)):** Export a quiesced canonical checkpoint signed with Ed25519 (`zinder-ingest export-edge-snapshot`) and initialize an independent regional writer (`zinder-ingest bootstrap-from-snapshot`) from local disk or HTTP(S). Bootstrap verifies every file digest, attests consensus roots and sampled blocks against the local validator, and generates a fresh cursor key before following the tip.
- **Coherent private backups:** `zinder-projector backup`, `verify`, and `restore` capture and authenticate the canonical and wallet RocksDB stores together at one block boundary for private host replacement, preserving cursor identity. See [Backups and edge snapshots](runbooks/private-backups.md).

## Deployment flexibility

Choose a deployment profile for what Zinder stores and serves, then choose how to package and run it. Step-by-step setup and host sizing live in [Deploying Zinder](runbooks/deploying.md).

### Deployment profiles

Set `deployment.profile` across a deployment's runtimes to keep storage retention and reader modes aligned ([ADR-0023](adrs/0023-deployment-profiles.md)). Mainnet disk and first-sync numbers below are measured on `v0.26.0` against Zebra 7.1.0 on 8 writer cores:

| Profile | Stores & serves | Measured Mainnet (`v0.26.0`) | Best for |
| --- | --- | --- | --- |
| `wallet` *(Recommended default)* | Canonical + wallet projection, `WalletQuery` + `CompactTxStreamer`, SHA-256 verified node tx bodies (`raw_blob_policy = "none"`) | ~123 GB disk, ~76 min sync | Production wallets, backends, and public endpoints needing local transparent indexes |
| `light-wallet` | Canonical store only (`ingest` + `compat` + optional `history-publisher`); node-forwarded transparent reads and tx bodies | ~72 GB disk, ~49–60 min sync | Minimal-footprint `CompactTxStreamer` endpoints focused on shielded wallets |
| `wallet-archive` | Same as `wallet`, plus all raw block and transaction blobs (`raw_blob_policy = "all"`) | Larger than `wallet` + ~251 GiB raw tx blobs | Self-hosted raw transaction bodies or full-block streams (`FullBlocksInRange`); omit `deployment.profile` and set `raw_blob_policy = "transactions"` (+~251.1 GiB) for tx blobs only |
| Optional **Explorer** | Adds rebuildable materialized views + `zinder-explorer` (`ExplorerQuery`) alongside an explicit non-profile config (`ingest.explorer_views = true`) | Additional view disk & build time | Block explorers, search, fee/pool analytics, and Cipherscan adapter (cannot be combined with `deployment.profile`) |

### Packaging options

| Option | Status | Summary | Guide |
| --- | --- | --- | --- |
| **Docker Compose + systemd on a Linux VM** *(Recommended)* | Supported | Complete multi-process topology with isolated cgroup memory limits, signed shielded history publisher, and Prometheus/Grafana | [Deploying Zinder](runbooks/deploying.md), [Deploying on a VM](runbooks/deploying-on-a-vm.md) |
| **Single-volume container (`zinder-wallet-serving`)** | Supported | Supervised multi-process image for single-volume PaaS platforms (e.g., Railway); omits signed-history publisher today and restarts all runtimes on redeploy | [Deploying the single-volume container](runbooks/deploying-single-volume.md) |
| **Standalone binaries** | Supported | Reproducible GNU/Linux `x86-64-v3` and `AArch64` binaries with signatures, provenance, and SBOMs ([ADR-0022](adrs/0022-release-artifacts.md)); required for `light-wallet` or custom supervision | [Deploying on a VM](runbooks/deploying-on-a-vm.md) |
| **Zcash Foundation public endpoints** | Supported | Connect directly on Mainnet or Testnet over gRPC-TLS with zero infrastructure to run | [Integration surfaces](reference/integration-surfaces.md#endpoints) |

## Status and roadmap

Zinder keeps shipped behavior, active engineering work, and decided future architecture explicitly separate.

### Supported today (`v0.26.0`, alpha)

- **Topology:** Single-host `rocksdb` ([ADR-0004](adrs/0004-storage-and-deployment-topologies.md)), canonical and wallet schema `v1`, native contract revision `15`.
- **Profiles and surfaces:** `wallet`, `light-wallet`, and `wallet-archive` profiles; `CompactTxStreamer` (with gRPC-Web), `WalletQuery` + `zinder-client`, optional `ExplorerQuery`, and `zinder-compat-cipherscan`.
- **Node sources:** Certified Zebra 7.1.0 and Zakura 1.8.6 over JSON-RPC and `zebra-indexer-grpc` binary block fetch.
- **Verification and recovery:** Ed25519-signed `ZSHRNG01` segments with `fs-verity`, SHA-256 verified node-forwarded tx bodies, verified edge-snapshot bootstrap ([ADR-0024](adrs/0024-verified-edge-snapshot-bootstrap.md)), and coherent private backups.
- **Alpha upgrade model:** Breaking API, config, or schema layout changes happen between alpha tags; schema layout changes require building a fresh store rather than in-place migration.

### Active engineering in flight (not yet in `v0.26.0`)

- **Canonical Schema `v2` deduplicated settled segments:** Eliminating duplicated settled compact-block storage between RocksDB and signed shielded segments, targeting a ~29–30 GB Mainnet `light-wallet` footprint.

### Decided and planned (not implemented or certified)

- **`postgres` stateless horizontal topology ([ADR-0004](adrs/0004-storage-and-deployment-topologies.md)):** Stateless, independently replaceable Zinder readers backed by a managed PostgreSQL database for hosted fleets. Build work is gated on passing ADR-0004's Mainnet benchmark thresholds first; the current PostgreSQL bench arm is diagnostic only.
- **Resumable initial construction:** Checkpointing from-genesis build progress so an interrupted first sync resumes instead of restarting from height 1.
