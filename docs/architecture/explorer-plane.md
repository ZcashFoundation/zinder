# Explorer plane

This page explains what the optional `zinder-explorer` runtime serves through `ExplorerQuery`, how to turn it on, what it costs, and the rules every response follows. It is for operators deciding whether to run the Explorer and for developers building explorer pages, dashboards, or adapters on it. [ADR-0019](../adrs/0019-explorer-plane.md) records why the Explorer is shaped this way.

## What the Explorer is

A wallet asks what changed for its keys since a given height. A block explorer asks what is in a block, what happened to a transaction, which transparent addresses hold the most value, what is in the mempool, and how fresh the answer is. `zinder-explorer` answers those questions through the gRPC service `zinder.v1.explorer.ExplorerQuery`, defined in `crates/zinder-proto/proto/zinder/v1/explorer/explorer.proto`.

The Explorer is optional, and wallet serving never depends on it. Running it adds two things:

- **Explorer views.** `zinder-ingest` builds a separate RocksDB store of explorer-shaped indexes and aggregates from the canonical chain. These are called materialized views. [Materialized-view plane](materialized-view-plane.md) explains how they are built.
- **The `zinder-explorer` process.** It reads those views, composes wallet reads through `zinder-query`, and serves `ExplorerQuery`.

The Explorer serves public chain facts only. It never receives viewing keys or spending keys, never scans shielded data, and answers a shielded address with a typed refusal.

## Turn it on

Two settings are needed, and both are off by default:

1. Set `[ingest].explorer_views = true` (`ZINDER_INGEST__EXPLORER_VIEWS=true`) so ingest builds the views. Ingest rejects this setting when `ingest.run_overrides.checkpoint_height` is above 0, because cumulative address views would under-report without the blocks below the checkpoint.
2. Run `zinder-explorer` on the same host, against the same storage path.

Each deployment can do both:

| Deployment | What to run |
| --- | --- |
| Docker Compose | Merge `deploy/docker-compose.explorer.yml` with `deploy/docker-compose.yml`. The overlay sets `ZINDER_INGEST__EXPLORER_VIEWS=true`, runs the `zinder-explorer` image, and publishes `ExplorerQuery` on host loopback port 9068 and its ops endpoint on 9069. These are the Mainnet ports; the Testnet and Regtest env files add 10000 and 20000. |
| One container, one volume | Run the `zinder-wallet-serving-explorer` image. It sets `explorer_views = true` and starts the Explorer after wallet serving answers. See [Single-volume deployment](../runbooks/deploying-single-volume.md). |
| Native binaries | Set `explorer_views = true` in the ingest configuration and run the `zinder-explorer` binary with `deploy/config/explorer.toml` as a starting point. The repository ships no systemd unit for it and does not test this path. [Deploying on a VM](../runbooks/deploying-on-a-vm.md) lists what a deployment without Docker must reproduce. |

The Explorer reads these settings. [Configuration](../reference/configuration.md) lists them with their environment variables.

| Setting | Meaning |
| --- | --- |
| `storage.path` | The canonical store root. Ingest writes the views in its `materialized-views` subdirectory. |
| `storage.secondary_path` | The Explorer's private RocksDB secondary metadata. |
| `explorer.listen_addr` | The `ExplorerQuery` listener. Default `127.0.0.1:9068`. A public address needs `security.allow_public_bind = true`. |
| `explorer.wallet_query_endpoint` | The `zinder-query` gRPC endpoint. Without it, every method that composes wallet reads is unadvertised and fails with `DEPENDENCY_NOT_CONFIGURED`. |
| `explorer.bearer_token_path` | Optional. When set, every request must carry `authorization: Bearer <token>`. |
| `[node]` | Optional. Used only to report the node's tip in freshness. The Explorer ignores `[node_fallbacks.N]`. |

## What it costs

None of these costs has been measured separately from the rest of Zinder.

- **Disk.** The views live in `<storage.path>/materialized-views`, on the canonical store's volume. Ingest pauses view replay while that volume has less free space than `ingest.explorer_views_min_free_bytes` (10 GiB by default, `0` disables the check), logs `materialized_view_replay_volume_space_low`, and resumes once a tenth more is free. Canonical ingest and wallet serving keep running.
- **Build time.** Ingest builds the views after canonical sync reaches the tip, replaying every block from genesis and then backfilling some aggregates. Wallet-composed methods answer long before historical aggregates do.
- **Memory.** The Compose overlay limits the Explorer container to 4 GB by default (`ZINDER_EXPLORER_MEMORY_LIMIT`).

[Deploying Zinder](../runbooks/deploying.md) sizes the whole host.

## How the Explorer reads data

`zinder-explorer` owns no primary store. It reads three sources:

- The view store, opened as a RocksDB secondary and caught up every second.
- `WalletQuery` on `zinder-query`, for transaction status, tips, the mempool, value pools, and the network-upgrade table. Each wallet read is pinned to one chain epoch, the consistent chain snapshot the wallet reader serves ([Wallet data plane](wallet-data-plane.md)).
- Optionally, the node in `[node]`, polled in the background for the upstream tip. It never supplies a chain fact.

The Explorer never opens the canonical store, so methods that need canonical reads fail ([Methods the Explorer cannot serve](#methods-the-explorer-cannot-serve)).

When the view store is absent, the Explorer starts with view-backed methods unadvertised. When the store was built by a release with a different set of views, the Explorer exits at startup with `ConsumerNotDeclared` or `ExplorerViewManifestRequiresFreshStore`. Rebuild the views on a fresh path as [Add the Explorer](../runbooks/deploying-on-a-vm.md#add-the-explorer) describes.

## Query families

Each method has a capability string. `ExplorerQuery.ServerInfo` lists only the capabilities the running Explorer can serve now, so a client checks it before calling. [Capabilities](../reference/capabilities.md) lists every string and the condition that enables it.

In the table, "views" means the method reads the view store and "wallet" means it composes `WalletQuery`. A range above its cap is rejected with `INVALID_ARGUMENT`. A page size above its cap is clamped.

| Methods | Returns | Source and limits |
| --- | --- | --- |
| `ServerInfo` | Capabilities, view store identities, and view health (live, catching up, or paused) | Always served, even before views exist |
| `BlockSummariesInRange`, `BlockDetail` | Block summaries by height; one block by height or hash with its transaction ids | Views and wallet. At most 1,024 summaries per request |
| `BlockActivityDistribution` | Transactions and blocks by weekday and hour over a height range, with a missing-row count | Views and wallet. At most 20,000 blocks |
| `RecentTransactions`, `TransactionHistory` | Newest-first canonical transactions. `TransactionHistory` adds filters, a read fence, and coverage | Views and wallet. History pages up to 256 rows |
| `TransactionDetail` | Parsed public facts, mined or mempool location, transparent inputs and outputs, paid fee, and spent-by relations | Wallet, plus views for fees and spends. See [Transaction detail](#transaction-detail) |
| `Search` | Typed candidates for a height, block hash, transaction id, or address | Wallet. Input up to 4 KiB |
| `MempoolSummary`, `MempoolSnapshot`, `MempoolActivity` | Mempool totals; a summary and one page from a single snapshot; newest-first entries with component counts and ZIP-317 fee floors | Wallet, computed per request. Snapshot reads up to 4,096 entries; activity pages up to 256 |
| `MempoolEventCounts` | Added, mined, and invalidated counts over a 60 to 3,600 second window | Views. Always zero in this release; see [Known gaps](#known-gaps) |
| `TransparentAddressActivity`, `TransparentAddressDeltas`, `TransparentAddressRanking` | Per-address transaction rows and summary; per-output and per-spend value events; addresses ordered by balance | Views and wallet. See [Transparent addresses](#transparent-addresses) |
| `FeeSummary` | ZIP-317 conventional fee floors over a block range, excluding coinbase | Views and wallet. At most 256 blocks |
| `ConventionalFeeDistribution` | Exact conventional-fee frequencies by UTC day over a time range | Views and wallet, with coverage |
| `TransactionComponentSummary` | Transparent, Sapling, Orchard, Ironwood, and Sprout component totals by UTC day | Views and wallet, with coverage |
| `ValuePoolSummary` | The pool totals the node reports in `getblockchaininfo`, bound to one tip | Wallet |
| `ValuePoolFlowHistory`, `ValuePoolFlowEventsInRange`, `ValuePoolFlowSummary`, `ValuePoolFlowAmountThresholdSummary`, `ValuePoolFlowRoundedAmountSummary` | Per-transaction value moving into or out of the shielded pools, and aggregates by time, amount threshold, or rounded amount | Views and wallet. Summaries fail above 500,000 scanned events |
| `ValuePoolBalanceHistory` | Pool balances at UTC-day boundaries | Views and wallet. No rows in this release; see [Known gaps](#known-gaps) |
| `NetworkUpgradeStatus` | The network-upgrade table ingest admitted, with the current tip and active upgrade | Wallet |
| `ChainReorgHistory` | Observed reorgs from the reorg-incident view | Views. Up to 1,024 events per request |
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

Missing data is reported as absent, through an `unavailable` entry or incomplete coverage. It is never turned into zero or an empty complete list. `MempoolEventCounts` is the one exception in this release.

## Cursors

Cursors are opaque bytes. Their lifetime depends on the method:

- Most cursors, such as those of `RecentTransactions` and the transparent-address methods, encode the last row returned and resume after it. They do not detect a reorg between pages.
- `TransactionHistory` binds its cursor to the filter and the view's read fence: epoch, revision, and view tip. Any view change, including a new block, makes the next page fail with `FAILED_PRECONDITION` and `EXPLORER_PRECONDITION_UNSATISFIED`. Restart from a new anchor, and include the fence in any cache key.
- Mempool pages can interleave with new arrivals. Use `MempoolSnapshot` when totals and rows must come from one snapshot.

The proto defines a `CursorExpiredError` message, but no method returns it.

## Methods the Explorer cannot serve

These methods exist on the wire but need canonical-store reads. The Explorer never advertises them, and each call fails with `FAILED_PRECONDITION` and `DEPENDENCY_NOT_CONFIGURED`:

- `BlockTransactions`
- `BlockProductionSeries`
- `BlockProductionInTimeRange`
- `CommitmentRootSearch`
- `PaidFeeDistribution`
- `DisplacedBlockHistory`
- `DisplacedBlockDetail`

When a method that also reads views runs without a view store, it fails with the same reason first. With a store that lacks the exact view manifest, it fails with `UNIMPLEMENTED` and `EXPLORER_METHOD_DISABLED`. [Error vocabulary](../reference/error-vocabulary.md) lists every reason.

## Failure isolation and operations

An Explorer crash stops neither `zinder-ingest` nor `zinder-query`, and its readiness never feeds wallet readiness. In the single-volume image, an Explorer exit restarts only the Explorer. A damaged view store never changes canonical or wallet state; rebuild it on a fresh path.

The Explorer serves `/healthz`, `/readyz`, and `/metrics` on its ops listener, `127.0.0.1:9069` by default. `/readyz` fails while the latest view catch-up failed, with `schema_mismatch` or `storage_unavailable`, and recovers on the next successful catch-up. [Readiness](../reference/readiness.md) and [Metrics](../reference/metrics.md) describe the endpoints and the `zinder_explorer_*` metrics.

## Known gaps

- The seven methods listed in [Methods the Explorer cannot serve](#methods-the-explorer-cannot-serve) always fail.
- `MempoolEventCounts`, and the mempool event counts in `OverviewSnapshot`, are always zero. Their view is registered, but no runtime writes mempool events to it.
- `ValuePoolBalanceHistory` returns no points and no coverage. Its view has no writer in this release.
- `TransactionDetail` never returns `intrinsic_value_balances`, and mined inputs never carry previous-output scripts.
- `TransactionHistory` never claims full-history totals, because that claim must match a canonical tip the Explorer cannot read.
