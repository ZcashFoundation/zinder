# Explorer plane

This page explains what the optional `zinder-explorer` runtime serves through `ExplorerQuery`, how to turn it on, what it needs, and the rules every response follows. It is for operators deciding whether to run the Explorer and for developers building explorer pages, dashboards, or adapters on it. [ADR-0019](../adrs/0019-explorer-plane.md) records why the Explorer is shaped this way.

## What the Explorer is

A wallet asks what changed for its keys since a given height. A block explorer asks what is in a block, what happened to a transaction, which transparent addresses hold the most value, what is in the mempool, and how fresh the answer is. `zinder-explorer` answers those questions through the gRPC service `zinder.v1.explorer.ExplorerQuery`, defined in `crates/zinder-proto/proto/zinder/v1/explorer/explorer.proto`.

The Explorer is optional, and wallet serving never depends on it. `zinder-explorer` serves `ExplorerQuery` by composing three dependencies:

- **Canonical secondary (`RocksDbCanonicalSecondary`).** `zinder-explorer` reads canonical blocks, headers, transaction locations, note-commitment roots, and displaced blocks directly from the canonical store. The canonical-only displaced-block RPCs (`DisplacedBlockHistory`, `DisplacedBlockDetail`) require only an opened `READY` canonical secondary.
- **Materialized Explorer views (`ingest.explorer_views = true`).** When enabled (automatically under `--preset explorer`), `zinder-ingest` builds a separate RocksDB store of explorer-shaped indexes and aggregates from the canonical chain ([Materialized-view plane](materialized-view-plane.md)). `BlockDetail`, `BlockProductionSeries`, `BlockProductionInTimeRange`, `BlockTransactions`, `CommitmentRootSearch`, history filters, fee distributions, value-pool flows, and transparent rankings require materialized views and report availability qualified by their view catch-up and backfill coverage (canonical `READY` alone does not enable them).
- **`WalletQuery` composition and optional `zinder-compat-cipherscan`.** `zinder-explorer` reads `WalletQuery` (`explorer.wallet_query_endpoint`) for methods that compose wallet state (`BlockDetail`, `BlockSummariesInRange`, `Search`, `TransactionDetail`, `MempoolSummary`/`Snapshot`/`Activity`, `ValuePoolSummary`, `NetworkUpgradeStatus`, transparent address queries, `FeeSummary`, etc.), so their availability also requires a reachable ready `WalletQuery` endpoint. `--preset explorer` runs both `zinder-explorer` and `zinder-compat-cipherscan`.

The Explorer serves public chain facts only. It never receives viewing keys or spending keys, never scans shielded data, and answers a shielded address with a typed refusal.

## Turn it on

Run with the `explorer` preset (`--preset explorer` or `ZINDER_DEPLOYMENT__PROFILE=explorer`), or enable `[ingest].explorer_views = true` (`ZINDER_INGEST__EXPLORER_VIEWS=true`) and run `zinder-explorer` explicitly. Ingest rejects `explorer_views = true` when `ingest.run_overrides.checkpoint_height` is above 0, because cumulative address views would under-report without the blocks below the checkpoint.

| Deployment | What to run |
| --- | --- |
| Docker Compose | Set `ZINDER_PRESET=explorer` (or `ZINDER_DEPLOYMENT__PROFILE=explorer`) with `deploy/docker-compose.yml`. `zinder-index` enables `explorer_views = true`, and `zinder-serve --readers-only --preset explorer` runs `zinder-explorer` (`:9068`) and `zinder-compat-cipherscan` (`:9070`) alongside wallet readers. |
| Single-volume container or Kubernetes | Run `ghcr.io/zcashfoundation/zinder` (`zinder serve --preset explorer`, or set `ZINDER_DEPLOYMENT__PROFILE=explorer`) in [Single-volume deployment](../runbooks/deploying-single-volume.md) or [Kubernetes StatefulSet](../../deploy/kubernetes/statefulset.yaml). |
| Standalone roles | Run `zinder run explorer` (or `zinder-explorer`) against `storage.path` (or `storage.data_dir`) via `ZINDER_*` environment variables or an optional `--config` TOML file; [Deploying on a VM](../runbooks/deploying-on-a-vm.md) lists what a non-container host needs. |

The Explorer reads these settings. [Configuration](../reference/configuration.md) lists them with their environment variables.

| Setting | Meaning |
| --- | --- |
| `storage.path` | The canonical store root. Ingest writes the views in its `materialized-views` subdirectory. |
| `storage.secondary_path` | The Explorer's private RocksDB secondary metadata. |
| `explorer.listen_addr` | The `ExplorerQuery` listener. Default `127.0.0.1:9068`. A public address needs `security.allow_public_bind = true`. |
| `explorer.wallet_query_endpoint` | The `zinder-query` gRPC endpoint. Without it, every method that composes wallet reads is unadvertised and fails with `DEPENDENCY_NOT_CONFIGURED`. |
| `explorer.bearer_token_path` | Optional. When set, every request must carry `authorization: Bearer <token>`. |
| `[node]` | Optional. Used only to report the node's tip in freshness. The Explorer ignores `[node_fallbacks.N]`. |

## What it needs

Plan disk, build time, and memory for the Explorer. [Deploying Zinder](../runbooks/deploying.md) sizes the whole host.

- **Disk.** The views live in `<storage.path>/materialized-views`, on the canonical store's volume. Ingest pauses view replay while that volume has less free space than `ingest.explorer_views_min_free_bytes` (10 GiB by default, `0` disables the check), logs `materialized_view_replay_volume_space_low`, and resumes once a tenth more is free. Canonical ingest and wallet serving keep running.
- **Build time.** Canonical-only displaced-block RPCs (`DisplacedBlockHistory`, `DisplacedBlockDetail`) become available once `zinder-explorer` opens a `READY` canonical primary as a secondary. Methods backed by materialized views (`BlockDetail`, `BlockProductionSeries`, `BlockProductionInTimeRange`, `BlockTransactions`, `CommitmentRootSearch`, fee/flow/ranking aggregates) require `explorer_views = true` and track view catch-up and backfill coverage (`freshness.chain_view.indexed_tip` and method coverage envelopes); `WalletQuery`-composed methods also require `explorer.wallet_query_endpoint` (`zinder-query`) to be configured and ready.
- **Memory.** Budget extra RAM for `zinder-explorer`'s RocksDB caches alongside wallet serving; see [Deploying Zinder](../runbooks/deploying.md) for host memory sizing.

## How the Explorer reads data

`zinder-explorer` owns no primary store. It reads four sources:

- The canonical store at `storage.path`, opened read-only as a RocksDB secondary under `<storage.secondary_path>/canonical` and caught up continuously on `storage.secondary_catchup_interval` whenever the canonical primary is READY.
- The view store at `<storage.path>/materialized-views`, opened as a RocksDB secondary under `<storage.secondary_path>/materialized-views` and caught up on the same cadence.
- `WalletQuery` on `zinder-query`, for transaction status, tips, the mempool, value pools, and the network-upgrade table. Each wallet read is pinned to one chain epoch, the consistent chain snapshot the wallet reader serves ([Wallet data plane](wallet-data-plane.md)).
- Optionally, the node in `[node]`, polled in the background for the upstream tip. It never supplies a chain fact.

When the canonical or view store is absent (or not yet `READY` when the Explorer starts), the Explorer opens only what is available at startup and starts with the missing capability groups unadvertised. Because secondary handles are opened only at startup and the catch-up task advances already-opened handles, an Explorer started while the canonical primary is still constructing must be restarted after the primary reaches `READY` to advertise canonical-backed capabilities. When the view store was built by a release with a different set of views, the Explorer exits at startup with `ConsumerNotDeclared` or `ExplorerViewManifestRequiresFreshStore`. Rebuild the views on a fresh path as [Add the Explorer](../runbooks/deploying-on-a-vm.md#add-the-explorer) describes.

## Query families

Each method has a capability string. `ExplorerQuery.ServerInfo` lists only the capabilities the running Explorer can serve now, so a client checks it before calling. [Capabilities](../reference/capabilities.md) lists every string and the condition that enables it.

In the table, "canonical" means the canonical secondary store, "views" means the view store, and "wallet" means `WalletQuery`. A range above its cap is rejected with `INVALID_ARGUMENT`. A page size above its cap is clamped.

| Methods | Returns | Source and limits |
| --- | --- | --- |
| `ServerInfo` | Capabilities, view store identities, and view health (live, catching up, or paused) | Always served, even before views exist |
| `BlockSummariesInRange`, `BlockDetail` | Block summaries by height; one block by height or hash with its transaction ids | Views and wallet. At most 1,024 summaries per request |
| `BlockProductionSeries`, `BlockProductionInTimeRange` | Block summaries enriched with header `nBits` and canonical coinbase summary over a height range or bounded time window | Views and canonical. Up to 1,024 blocks per series or 1,000 candidates per time-range page |
| `BlockTransactions` | Canonical block summary, ordered block transactions with public facts, outputs, and resolved transparent inputs, plus final note-commitment roots | Views and canonical. Optional wallet fallback for network-upgrade activations |
| `BlockActivityDistribution` | Transactions and blocks by weekday and hour over a height range, with a missing-row count | Views and wallet. At most 20,000 blocks |
| `RecentTransactions`, `TransactionHistory` | Newest-first canonical transactions. `TransactionHistory` adds filters, a read fence, coverage, and intrinsic value balances when canonical is open | Views, wallet, and optional canonical. History pages up to 256 rows |
| `TransactionDetail` | Parsed public facts, mined or mempool location, transparent inputs and outputs, paid fee, spent-by relations, and intrinsic value balances when canonical is open | Wallet, plus views for fees/spends and canonical for intrinsic balances. See [Transaction detail](#transaction-detail) |
| `Search` | Typed candidates for a height, block hash, transaction id, or address | Wallet. Input up to 4 KiB |
| `CommitmentRootSearch` | Canonical and displaced block matches for a 32-byte final note-commitment root plus coverage | Views and canonical. Up to 1,024 matches per request |
| `MempoolSummary`, `MempoolSnapshot`, `MempoolActivity` | Mempool totals; a summary and one page from a single snapshot; newest-first entries with component counts and ZIP-317 fee floors | Wallet, computed per request. Snapshot reads up to 4,096 entries; activity pages up to 256 |
| `MempoolEventCounts` | Added, mined, and invalidated counts over a 60 to 3,600 second window | Views and wallet. Unadvertised; see [Methods that return no data in this release](#methods-that-return-no-data-in-this-release) |
| `TransparentAddressActivity`, `TransparentAddressDeltas`, `TransparentAddressRanking` | Per-address transaction rows and summary; per-output and per-spend value events; addresses ordered by balance | Views and wallet. See [Transparent addresses](#transparent-addresses) |
| `FeeSummary` | ZIP-317 conventional fee floors over a block range, excluding coinbase | Views and wallet. At most 256 blocks |
| `ConventionalFeeDistribution` | Exact conventional-fee frequencies by UTC day over a time range | Views and wallet, with coverage |
| `PaidFeeDistribution` | Per-UTC-day miner-collected fee frequencies | Unadvertised (`DEPENDENCY_NOT_CONFIGURED`); Cipherscan falls back to `ConventionalFeeDistribution` |
| `TransactionComponentSummary` | Transparent, Sapling, Orchard, Ironwood, and Sprout component totals by UTC day | Views and wallet, with coverage |
| `ValuePoolSummary` | The pool totals the node reports in `getblockchaininfo`, bound to one tip | Wallet endpoint with `ChainValuePoolsAtTip` support |
| `ValuePoolFlowHistory`, `ValuePoolFlowEventsInRange`, `ValuePoolFlowSummary`, `ValuePoolFlowAmountThresholdSummary`, `ValuePoolFlowRoundedAmountSummary` | Per-transaction value moving into or out of the shielded pools, and aggregates by time, amount threshold, or rounded amount | Views and wallet. Summaries fail above 500,000 scanned events |
| `ValuePoolBalanceHistory` | Pool balances at UTC-day boundaries | Views and wallet. Unadvertised; see [Methods that return no data in this release](#methods-that-return-no-data-in-this-release) |
| `NetworkUpgradeStatus` | The network-upgrade table ingest admitted, with the current tip and active upgrade | Wallet |
| `ChainReorgHistory` | Observed reorgs from the reorg-incident view | Views. Up to 1,024 events per request |
| `DisplacedBlockHistory`, `DisplacedBlockDetail` | Archived non-canonical blocks displaced by reorgs and their current canonical counterparts | Canonical. History pages up to 4,096 blocks |
| `MigrationOverview`, `MigrationCohorts`, `MigrationDenominations` | Orchard-to-Ironwood migration totals, cohorts by shared anchor, and amount bins | Views and wallet. Cohort and denomination ranges up to 4,096 blocks |
| `OverviewSnapshot` | One dashboard bundle: tip time, mempool, fee summary, value pools, recent blocks, and recent transactions, anchored on one tip | Views and wallet |

## Transaction detail

`TransactionDetail` reads the transaction's location and raw bytes from `WalletQuery.Transaction` and parses them with the parser ingest uses.

- **Inputs.** Mined inputs carry the spent outpoint and, when the fee view resolved it, the previous output's value, but never its script.
- **Outputs.** Outputs carry their value and script. A mined output carries `spent_by` when a canonical transaction spent it at the response's epoch. When the spend view does not cover exactly that epoch, every `spent_by` is omitted and `freshness.unavailable` lists `transparent_outputs.spent_by` with `UNAVAILABLE_STALE`. Unknown spent state is never reported as unspent.
- **Paid fee.** `paid_fee_zat` appears only for a transparent-only transaction whose inputs all resolved. In a shielding or mixed transaction, a transparent value difference does not prove a fee.
- **Mempool transactions** carry their inputs and outputs but no resolved parents, spent state, or paid fee.

## Block view shape

Block listing and block detail share one view: a `BlockSummaryRecord` per canonical block, keyed by big-endian height. Each record holds a `BlockSummary` and the block's transaction ids in block order, coinbase first.

`BlockSummary` carries the height, block hash and previous hash (in RPC byte order), header time, transaction count, size, fee totals, transparent coinbase reward, and shielded output and action counts. `confirmations` and `is_canonical` are computed per request from the wallet's visible tip.

`BlockSummariesInRange` returns summaries in ascending height without transaction ids, so long ranges stay cheap. `BlockDetail` resolves a height, or a hash through `WalletQuery.BlockIdBySelector`, and adds the transaction ids. A reorg deletes replaced records in the same write that adds their replacements, so the view never serves a summary for a height that left the canonical chain.

## Search and privacy refusals

`Search` classifies the raw input locally, then confirms candidates through `WalletQuery`:

- A number becomes a block candidate if a block exists at that height.
- 64 hex characters are checked as a block hash and a transaction id. Two matches each get confidence 0.5.
- Transparent, ZIP-320 TEX, and ZIP-316 unified addresses decode locally. Transparent receivers become address candidates.
- Sapling and Sprout addresses, shielded receivers inside a unified address, and viewing keys return the typed `NotPubliclyIndexable` arm with a reason. A viewing key is never echoed back.
- Unparseable input returns an unclassified candidate with a hint.

Shielded input is refused without any lookup. A refusal is a typed answer, never an empty result or an error.

## Transparent addresses

`TransparentAddressActivity` returns one newest-first row per transaction that touched an address, paged by cursor or by an `offset` of at most 100,000 rows. Rows carry the address's net value, not other inputs, outputs, or scripts. The response adds a summary from the address ranking: balance, lifetime received and sent, transaction count, first and last activity, and coverage. A valid address with no history returns a zero summary.

The answer is pinned to the epoch of `WalletQuery.VisibleTipBlock`. The views usually run a few blocks ahead of the wallet reader, so the Explorer rewinds the summary through per-block undo records and drops rows above the visible tip. The read fails with `FAILED_PRECONDITION` and `EXPLORER_PRECONDITION_UNSATISFIED` when the views and the epoch disagree about a block, or when the rewind would exceed 1,000 blocks. A retry at a newer epoch can succeed. Ranking coverage, such as `balance_complete_through_height`, says whether the summary is complete for the epoch.

`TransparentAddressDeltas` returns one row per received output and per resolved spend, oldest first, with a signed `value_zat` and a `RECEIVED` or `SPENT` kind. An address's net activity over a range equals the sum of its deltas. A spend whose previous output did not resolve produces no delta rather than a wrong value.

`TransparentAddressRanking` returns positive-balance standard transparent scripts ordered by balance, as raw `script_pub_key` bytes, in pages of up to 500. It includes totals, top-10 and top-100 sums, P2PKH and P2SH aggregates, and coverage. Ingest builds and activates the ranking without an operator step.

## Freshness envelope

Every `ExplorerQuery` response carries `ExplorerFreshness` at field tag 1:

| Field | Meaning |
| --- | --- |
| `chain_view.chain_epoch` | The wallet chain snapshot the response was built from. Absent on `ServerInfo`. |
| `chain_view.indexed_tip` | How far the views have replayed. Absent means unknown, never "at tip". |
| `chain_view.upstream_tip` | The node's tip from the background probe. Absent without `[node]` or before the first probe. |
| `chain_view.materialized_views` | View health: live, catching up, or paused, with lag in blocks. |
| `snapshot_age_millis` | Age of the mempool snapshot. Zero when the response does not read the mempool. |
| `capability_version` | The exact capability string that produced the response. |
| `unavailable` | Field paths deliberately left absent, each with a structured `reason` and a `human_reason` sentence a UI can show as is. |

View lag is the visible tip height minus the indexed tip height. Lag is reported per response. It is not a readiness cause.

## Coverage and absence

Methods over long history, such as `TransactionHistory`, the distributions and summaries, the value-pool flow methods, and `TransparentAddressRanking`, return a coverage envelope beside the data. A result is complete only when its coverage says so, so a client checks coverage before presenting a result as the whole history.

Missing data is reported as absent, through an `unavailable` entry or incomplete coverage.

## Cursors

Cursors are opaque bytes. Their lifetime depends on the method:

- Most cursors, such as those of `RecentTransactions` and the transparent-address methods, encode the last row returned and resume after it. They do not detect a reorg between pages.
- `TransactionHistory` binds its cursor to the filter and the view's read fence: epoch, revision, and view tip. Any view change, including a new block, makes the next page fail with `FAILED_PRECONDITION` and `EXPLORER_PRECONDITION_UNSATISFIED`. Restart from a new anchor, and include the fence in any cache key.
- Mempool pages can interleave with new arrivals. Use `MempoolSnapshot` when totals and rows must come from one snapshot.

The proto defines a `CursorExpiredError` message, but no method returns it.

## Unadvertised methods

`PaidFeeDistribution`, `MempoolEventCounts`, and `ValuePoolBalanceHistory` remain unadvertised in `zinder-explorer` in this release because no materialized view supplies paid-fee distribution or daily value-pool balance history, and no runtime indexes mempool event counts:

- `PaidFeeDistribution` fails with `FAILED_PRECONDITION` and `DEPENDENCY_NOT_CONFIGURED`.
- `BlockDetail`, `BlockSummariesInRange`, `BlockProductionSeries`, `BlockProductionInTimeRange`, `BlockTransactions`, `CommitmentRootSearch`, `DisplacedBlockHistory`, and `DisplacedBlockDetail` also fail with `FAILED_PRECONDITION` and `DEPENDENCY_NOT_CONFIGURED` when a required canonical secondary store, materialized-view store, or `WalletQuery` endpoint is absent (or when the canonical/view store was not yet `READY` at Explorer startup); start or restart `zinder-explorer` once `storage.path` has a `READY` canonical primary and ensure the required views (with catch-up/backfill coverage) and `WalletQuery` dependencies in the [Query families](#query-families) table are satisfied to advertise and serve each method.

When a method that also reads views runs without a view store, it fails with the same reason first. With a store that lacks the exact view manifest, it fails with `UNIMPLEMENTED` and `EXPLORER_METHOD_DISABLED`. [Error vocabulary](../reference/error-vocabulary.md) lists every reason.

## Failure isolation and operations

An Explorer crash stops neither `zinder-ingest` nor `zinder-query` when running as separate processes, and its readiness never feeds wallet readiness. Under `zinder serve --preset explorer`, any unexpected role exit triggers fail-fast supervisor shutdown so the container orchestrator restarts cleanly. A damaged view store never changes canonical or wallet state; rebuild it on a fresh path.

The Explorer serves `/healthz`, `/readyz`, and `/metrics` on its ops listener, `127.0.0.1:9069` by default. `/readyz` fails while the latest secondary catch-up (materialized views or canonical secondary) failed, with `schema_mismatch` or `storage_unavailable`, and recovers on the next successful catch-up. [Readiness](../reference/readiness.md) and [Metrics](../reference/metrics.md) describe the endpoints and the `zinder_explorer_*` metrics.

## Methods that return no data in this release

| Method or field | Behavior |
| --- | --- |
| `PaidFeeDistribution` | Never advertised; fails with `FAILED_PRECONDITION` and `DEPENDENCY_NOT_CONFIGURED`. |
| `MempoolEventCounts` | No runtime indexes mempool events. A direct call for a window with no indexed counts fails with `NOT_FOUND` and `ARTIFACT_UNAVAILABLE`. |
| `OverviewSnapshot.mempool_events` | Absent for a window with no indexed counts. `freshness.unavailable` lists `mempool_events` with `UNAVAILABLE_NOT_INDEXED`. |
| `ValuePoolBalanceHistory` | No runtime writes balance history. A direct call returns no points and reports incomplete coverage. |
| Previous-output scripts on mined `TransactionDetail` inputs | Never returned. |
