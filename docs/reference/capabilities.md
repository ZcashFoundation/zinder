# Capabilities

This page explains how a client learns what a Zinder server supports, and
lists every capability string with the condition under which a server
advertises it. It is for developers of wallets, explorers, and other clients
of the native `WalletQuery` and `ExplorerQuery` APIs.

## What a capability string is

A capability string names one operation, or one optional field, that a server
supports. Clients check for it before they call, instead of parsing a Zinder
version. Every string has this shape:

```text
{surface}.{noun}.{operation}_v{N}
```

For example, `wallet.read.compact_block_range_v2` gates
`WalletQuery.CompactBlocksInRange`.

- Match strings exactly. The `_vN` suffix is part of the identity, so
  `_v2` is a different capability from `_v1`, not an upgrade of it.
- A server advertises a capability only when the endpoint it composed can
  serve it: the method, the stored data it needs, and the providers behind
  it. Readiness and temporary dependency outages do not remove a capability.
  They affect individual requests instead.
- A method can exist in the protobuf schema without its capability being
  advertised. On `WalletQuery`, calling such a method fails with
  `ENDPOINT_CAPABILITY_UNAVAILABLE`.
- Field capabilities, such as `wallet.read.transaction_bytes_v1`, gate an
  optional field on another method. Without the capability, the field is
  absent, and absence says nothing about the chain.
- Capability names describe meaning, not implementation. They never name a
  storage engine, a transport, or a consumer product.

## How a client discovers capabilities

| Client | Where to read the list |
| --- | --- |
| Native wallet client | `WalletQuery.ServerInfo`, in `info.common.capabilities` |
| Rust SDK (`zinder-client`) | `EndpointBackedIndex::server_info()`, in `ServerInfo.capabilities` |
| Explorer client | `ExplorerQuery.ServerInfo`, in `info.common.capabilities` |
| Operator or dashboard | The `capabilities` field of `/healthz` on the operational endpoint |
| Lightwalletd client | None. `CompactTxStreamer` has no capability strings; see [Lightwalletd compatibility](lightwalletd-compatibility.md). |

The same `ServerInfo` also carries `contract_revision`, which increases when
the meaning of an existing wire surface changes. A client can require a
minimum revision as well as specific capabilities.

In Rust, check the typed `Capability` instead of copying a string. A string
the client does not know arrives as `Capability::Unknown`, so a newer server
does not break an older client:

```rust
use zinder_client::{Capability, CapabilityDescriptor, EndpointBackedIndex};

let info = index.server_info().await?;
if info.supports(Capability::Broadcast) {
    // The server can broadcast transactions.
}
```

`WalletServerInfo.node` is a different list. It holds the capabilities that
the first verified broadcast node reported, and it stays empty until a node
passes verification. [ADR-0015](../adrs/0015-wallet-runtime-composition.md)
records how a wallet runtime composes its capability set.

## Capability list

The list below holds every public capability string. Private `ingest.*`
capabilities of the control API are not listed.

<!-- capability-list:public-interfaces:start -->
```text
wallet.read.visible_tip_block_v1
wallet.read.settled_tip_block_v1
wallet.read.block_id_by_selector_v1
wallet.read.block_header_by_selector_v1
wallet.read.compact_block_at_v2
wallet.read.compact_block_range_v2
wallet.read.compact_block_ironwood_v2
wallet.read.full_block_at_v1
wallet.read.full_block_range_v1
wallet.read.tree_state_at_height_v2
wallet.read.latest_tree_state_checkpoint_v2
wallet.read.subtree_roots_in_range_v1
wallet.read.subtree_roots_ironwood_v1
wallet.read.transaction_by_id_v2
wallet.read.transaction_bytes_v1
wallet.read.server_info_v2
wallet.read.network_upgrade_activations_v1
wallet.broadcast.transaction_v1
wallet.events.chain_v1
wallet.snapshot.mempool_v3
wallet.events.mempool_v2
wallet.read.chain_value_pools_at_tip_v1
wallet.address.transparent_unspent_outputs_v1
wallet.address.transparent_history_v1
wallet.read.transparent_outpoint_spend_status_v1
explorer.server_info_v1
explorer.transaction.detail_v4
explorer.block.summary_v1
explorer.block.production_series_v2
explorer.block.production_time_range_v1
explorer.block.detail_v1
explorer.block.transactions_v2
explorer.block.final_note_commitment_roots_v1
explorer.block.activity_distribution_v1
explorer.search_v1
explorer.commitment_root.search_v1
explorer.commitment_root.displaced_matches_v1
explorer.mempool.summary_v1
explorer.mempool.snapshot_v1
explorer.mempool.activity_v1
explorer.transparent_address.activity_v2
explorer.transparent_address.deltas_v1
explorer.fee.summary_v1
explorer.fee.conventional_distribution_v1
explorer.fee.paid_distribution_v1
explorer.value_pool.summary_v1
explorer.network_upgrade.status_v1
explorer.value_pool.flow_history_v1
explorer.value_pool.flow_events_in_range_v1
explorer.value_pool.flow_summary_v1
explorer.value_pool.flow_amount_threshold_summary_v1
explorer.value_pool.flow_rounded_amount_summary_v1
explorer.value_pool.balance_history_v1
explorer.chain.reorg_history_v1
explorer.chain.displaced_block_history_v1
explorer.chain.displaced_block_detail_v1
explorer.mempool.event_counts_v1
explorer.transaction.fees_v1
explorer.transaction.history_v1
explorer.transaction.recent_v1
explorer.transaction.history_v2
explorer.transaction.intrinsic_value_balances_v1
explorer.transaction.component_summary_v2
explorer.transparent_address.ranking_v1
explorer.overview.snapshot_v1
explorer.migration.overview_v1
explorer.migration.cohorts_v1
explorer.migration.denominations_v1
```
<!-- capability-list:public-interfaces:end -->

## Wallet capabilities

`zinder-query` derives its wallet capability set once, after it admits its
storage and its ingest control connection, and keeps it for the life of the
process. The set follows from the deployment's configuration, so operators do
not select capabilities individually.

"Always" below means every `zinder-query` that starts advertises it.

| Capability | Gates | Advertised when |
| --- | --- | --- |
| `wallet.read.visible_tip_block_v1` | `VisibleTipBlock` | Always |
| `wallet.read.settled_tip_block_v1` | `SettledTipBlock` | Always |
| `wallet.read.block_id_by_selector_v1` | `BlockIdBySelector` | Always |
| `wallet.read.block_header_by_selector_v1` | `BlockHeaderBySelector` | Never; `zinder-query` does not serve it |
| `wallet.read.compact_block_at_v2` | `CompactBlock` | Always |
| `wallet.read.compact_block_range_v2` | `CompactBlocksInRange` | Always |
| `wallet.read.compact_block_ironwood_v2` | Ironwood fields on compact blocks | Always |
| `wallet.read.full_block_at_v1` | `FullBlock` | `storage.raw_blob_policy = "all"` |
| `wallet.read.full_block_range_v1` | `FullBlocksInRange` | `storage.raw_blob_policy = "all"` |
| `wallet.read.tree_state_at_height_v2` | `TreeStateAtHeight` | Always |
| `wallet.read.latest_tree_state_checkpoint_v2` | `LatestTreeStateCheckpoint` | Always |
| `wallet.read.subtree_roots_in_range_v1` | `SubtreeRoots` | Always |
| `wallet.read.subtree_roots_ironwood_v1` | Ironwood protocol on `SubtreeRoots` | Always |
| `wallet.read.transaction_by_id_v2` | `Transaction` | Ingest control serves mempool transaction lookups |
| `wallet.read.transaction_bytes_v1` | `raw_transaction_bytes` on `Transaction` | As above, and `storage.raw_blob_policy` is `transactions` or `all` |
| `wallet.read.server_info_v2` | `ServerInfo` | Always |
| `wallet.read.network_upgrade_activations_v1` | `NetworkUpgradeActivations` | Always |
| `wallet.broadcast.transaction_v1` | `BroadcastTransaction` | A `[node]` endpoint is configured |
| `wallet.events.chain_v1` | `ChainEvents` | Always |
| `wallet.snapshot.mempool_v3` | `MempoolSnapshot` | Ingest control serves mempool snapshots |
| `wallet.events.mempool_v2` | `MempoolEvents` | Ingest control serves mempool events |
| `wallet.read.chain_value_pools_at_tip_v1` | `ChainValuePoolsAtTip` | Never; `zinder-query` does not serve it |
| `wallet.address.transparent_unspent_outputs_v1` | `TransparentAddressUnspentOutputs` | Always |
| `wallet.address.transparent_history_v1` | `TransparentAddressTxIdsInRange` | Always |
| `wallet.read.transparent_outpoint_spend_status_v1` | `TransparentOutpointSpendStatus` | Always |

A current `zinder-ingest` serves every mempool lookup, so the three
ingest-control conditions hold in a normal deployment.

Broadcast is advertised as soon as a node endpoint is configured, before any
endpoint is verified; see
[Node source boundary](../architecture/node-source-boundary.md#capabilities)
for verification.

## Explorer capabilities

`zinder-explorer` computes its set from what it has open, each time it
answers `ServerInfo`. In the table, "wallet query" means
`explorer.wallet_query_endpoint` is set, and "views" means the Explorer
materialized-view store is open, which requires `ingest.explorer_views = true`
on the writer. The explorer does not read the canonical store, so it never
advertises a capability that needs it.

| Capability | Gates | Advertised when |
| --- | --- | --- |
| `explorer.server_info_v1` | `ServerInfo` | Always |
| `explorer.search_v1` | `Search` | Wallet query |
| `explorer.mempool.summary_v1` | `MempoolSummary` | Wallet query |
| `explorer.mempool.snapshot_v1` | `MempoolSnapshot` | Wallet query |
| `explorer.mempool.activity_v1` | `MempoolActivity` | Wallet query |
| `explorer.value_pool.summary_v1` | `ValuePoolSummary` | Wallet query. Calls still fail with `ENDPOINT_CAPABILITY_UNAVAILABLE`, because `zinder-query` does not serve `ChainValuePoolsAtTip` |
| `explorer.network_upgrade.status_v1` | `NetworkUpgradeStatus` | Wallet query |
| `explorer.chain.reorg_history_v1` | `ChainReorgHistory` | Views |
| `explorer.transaction.detail_v4` | `TransactionDetail` | Views and wallet query |
| `explorer.transaction.fees_v1` | Input values and paid fee on `TransactionDetail` | Views and wallet query |
| `explorer.transaction.recent_v1` | `RecentTransactions` | Views and wallet query |
| `explorer.transaction.component_summary_v2` | `TransactionComponentSummary` | Views and wallet query |
| `explorer.transaction.history_v1` | `TransactionHistory` | Wallet query, and the transaction history view has data |
| `explorer.block.summary_v1` | `BlockSummariesInRange` | Views and wallet query |
| `explorer.block.detail_v1` | `BlockDetail` | Views and wallet query |
| `explorer.block.activity_distribution_v1` | `BlockActivityDistribution` | Views and wallet query |
| `explorer.transparent_address.activity_v2` | `TransparentAddressActivity` | Views and wallet query, and the address ranking is built |
| `explorer.transparent_address.ranking_v1` | `TransparentAddressRanking` | Views and wallet query, and the address ranking is built |
| `explorer.transparent_address.deltas_v1` | `TransparentAddressDeltas` | Views and wallet query |
| `explorer.fee.summary_v1` | `FeeSummary` | Views and wallet query |
| `explorer.fee.conventional_distribution_v1` | `ConventionalFeeDistribution` | Views and wallet query, and the distribution covers the chain |
| `explorer.value_pool.flow_history_v1` | `ValuePoolFlowHistory` | Views and wallet query |
| `explorer.value_pool.flow_events_in_range_v1` | `ValuePoolFlowEventsInRange` | Views and wallet query |
| `explorer.value_pool.flow_summary_v1` | `ValuePoolFlowSummary` | Views and wallet query |
| `explorer.value_pool.flow_amount_threshold_summary_v1` | `ValuePoolFlowAmountThresholdSummary` | Views and wallet query |
| `explorer.value_pool.flow_rounded_amount_summary_v1` | `ValuePoolFlowRoundedAmountSummary` | Views and wallet query |
| `explorer.value_pool.balance_history_v1` | `ValuePoolBalanceHistory` | Views and wallet query |
| `explorer.mempool.event_counts_v1` | `MempoolEventCounts` | Views and wallet query |
| `explorer.overview.snapshot_v1` | `OverviewSnapshot` | Views and wallet query |
| `explorer.migration.overview_v1` | `MigrationOverview` | Views and wallet query |
| `explorer.migration.cohorts_v1` | `MigrationCohorts` | Views and wallet query |
| `explorer.migration.denominations_v1` | `MigrationDenominations` | Views and wallet query |

These explorer capabilities are defined but never advertised, because they
need the canonical store or complete canonical history:

- `explorer.block.production_series_v2`
- `explorer.block.production_time_range_v1`
- `explorer.block.transactions_v2`
- `explorer.block.final_note_commitment_roots_v1`
- `explorer.commitment_root.search_v1`
- `explorer.commitment_root.displaced_matches_v1`
- `explorer.chain.displaced_block_history_v1`
- `explorer.chain.displaced_block_detail_v1`
- `explorer.transaction.intrinsic_value_balances_v1`
- `explorer.transaction.history_v2`
- `explorer.fee.paid_distribution_v1`

[Explorer plane](../architecture/explorer-plane.md) describes what each
explorer method returns.
