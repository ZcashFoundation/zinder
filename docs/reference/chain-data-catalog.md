# Chain data catalog

This table lists the public chain data Zinder stores or derives, which component writes it, and which service methods serve it. Use it to find where a piece of data comes from before you read the method's own reference. Each row is a family of data, not a single method.

The "Stored in" column names where the data lives. `zinder-ingest` writes the canonical store, owns the live mempool, and writes the optional materialized views that `zinder-explorer` reads. `zinder-projector` writes the wallet store from the canonical store. [Overview](../architecture/overview.md) explains the roles, and [Storage backend](../architecture/storage-backend.md) the layout.

| Data | Stored in | Served through | Notes |
| --- | --- | --- | --- |
| Block identity, headers, and best-chain hash index | Canonical | `WalletQuery` tips and `BlockIdBySelector`; lightwalletd `GetLatestBlock`, `GetBlock`; `ExplorerQuery.BlockDetail` | `BlockHeaderBySelector` is not advertised |
| Compact blocks | Canonical | `WalletQuery.CompactBlock`, `CompactBlocksInRange`; lightwalletd `GetBlock`, `GetBlockRange`, nullifier variants | Built at commit, never on request |
| Full serialized blocks | Canonical | `WalletQuery.FullBlock`, `FullBlocksInRange` | Only when `storage.raw_blob_policy = "all"` |
| Tree states (Sapling, Orchard, Ironwood) | Canonical | `WalletQuery.TreeStateAtHeight`, `LatestTreeStateCheckpoint`; lightwalletd `GetTreeState`, `GetLatestTreeState` | Historical checkpoints follow the [construction and backfill policy](../architecture/chain-ingestion.md#tree-state-densification). Live following stores one per block. Other heights replay compact blocks. |
| Shielded subtree roots | Canonical | `WalletQuery.SubtreeRoots`; lightwalletd `GetSubtreeRoots` | |
| Note commitment tree roots per block | Materialized views | None | `ExplorerQuery.CommitmentRootSearch` is not advertised and always fails |
| Network-upgrade schedule | Canonical, from the node | `WalletQuery.NetworkUpgradeActivations`; lightwalletd `GetLightdInfo`; `ExplorerQuery.NetworkUpgradeStatus` | |
| Transaction location and status | Canonical | `WalletQuery.Transaction`; lightwalletd `GetTransaction`; `ExplorerQuery.TransactionDetail` | Absence is reported only when proven |
| Raw transaction bytes | Canonical | `WalletQuery.Transaction`; lightwalletd `GetTransaction`, `GetTaddressTransactions` | Only when `raw_blob_policy` is `transactions` or `all` |
| Transaction facts: components, value balances, transparent inputs and outputs | Canonical | `ExplorerQuery.TransactionDetail` | Missing optional facts mean unknown, not zero. `BlockTransactions` is not advertised and always fails |
| Chain events (commits and reorgs) | Canonical | `WalletQuery.ChainEvents`; `ExplorerQuery.ChainReorgHistory` | Cursor-resumable, retention-bounded |
| Displaced blocks | Canonical | None | Archive of blocks a reorg replaced. `ExplorerQuery.DisplacedBlockHistory` and `DisplacedBlockDetail` are not advertised and always fail |
| Live mempool and mempool events | Ingest's mempool view and event log | `WalletQuery.MempoolSnapshot`, `MempoolEvents`; lightwalletd `GetMempoolTx`, `GetMempoolStream`, `GetTransaction`; `ExplorerQuery` mempool methods | Events kept 60 minutes after mining, 24 hours after invalidation, by default |
| Transparent unspent outputs by address | Wallet | `WalletQuery.TransparentAddressUnspentOutputs`; lightwalletd `GetAddressUtxos`, `GetAddressUtxosStream` | |
| Transparent spent outputs | Wallet | `WalletQuery.TransparentOutpointSpendStatus` | |
| Transparent address transaction history | Wallet | `WalletQuery.TransparentAddressTxIdsInRange`; lightwalletd `GetTaddressTxids`, `GetTaddressTransactions` | Ascending only |
| Transparent address balance | Wallet | lightwalletd `GetTaddressBalance`, `GetTaddressBalanceStream` | No native balance method |
| Address activity, deltas, and ranking | Materialized views | `ExplorerQuery.TransparentAddressActivity`, `TransparentAddressDeltas`, `TransparentAddressRanking` | Ranking is advertised only after its view is built |
| Block summaries, production, and activity distribution | Materialized views | `ExplorerQuery.BlockSummariesInRange`, `BlockActivityDistribution` | `BlockProductionSeries` and `BlockProductionInTimeRange` are not advertised and always fail |
| Transaction history and recent transactions | Materialized views | `ExplorerQuery.TransactionHistory`, `RecentTransactions`, `TransactionComponentSummary` | |
| Fees | Materialized views | `ExplorerQuery.FeeSummary`, `ConventionalFeeDistribution` | ZIP-317 conventional fees; `PaidFeeDistribution` is not advertised |
| Value-pool balances and flows | Materialized views | `ExplorerQuery.ValuePoolBalanceHistory` and `ValuePoolFlow*` methods | |
| Value-pool totals at the node's tip | Not stored | None | `WalletQuery.ChainValuePoolsAtTip` is not advertised. `ExplorerQuery.ValuePoolSummary` reads it, so both fail with `ENDPOINT_CAPABILITY_UNAVAILABLE` |
| Orchard-to-Ironwood migration | Materialized views | `ExplorerQuery.MigrationOverview`, `MigrationCohorts`, `MigrationDenominations` | |
| Broadcast outcomes | Not stored | `WalletQuery.BroadcastTransaction`; lightwalletd `SendTransaction` | Forwarded to the node |

## Rules that apply to every row

1. Check the method's capability in `ServerInfo` before you rely on it. [Capabilities](capabilities.md) lists the strings.
2. An absent optional fact means unknown or unavailable. Zinder does not turn missing retention, unresolved prevouts, inactive pools, or incomplete backfill into zero.
3. A materialized-view response is bounded by that view's own checkpoint and coverage, which its freshness envelope reports. The canonical tip does not prove that a view is complete. [Explorer plane](../architecture/explorer-plane.md) explains freshness.
