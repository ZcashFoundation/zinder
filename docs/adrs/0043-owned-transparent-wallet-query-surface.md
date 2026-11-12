# ADR-0043: Owned transparent wallet-query surface

| Field | Value |
| --- | --- |
| Status | Accepted |
| Product | Zinder |
| Domain | Native `WalletQuery` transparent surface, materialized-view store manifest, wallet and Explorer discovery metadata |
| Related | [ADR-0029](0029-durable-transparent-outpoint-spend-projection.md), [ADR-0038](0038-wallet-runtime-composition-and-capability-discovery.md), [ADR-0007](0007-mempool-topology-and-retention.md), [ADR-0026](0026-utxo-set-commitment.md), [ADR-0028](0028-materialized-view-schema-versioning.md), [Wallet data plane](../architecture/wallet-data-plane.md), [Materialized-view plane](../architecture/materialized-view-plane.md), [Public interfaces](../architecture/public-interfaces.md) |

## Context

The native `WalletQuery` protocol carried more transparent methods than any
packaged consumer calls. The release serving-pair query implemented only a few
of them. The rest returned `ENDPOINT_CAPABILITY_UNAVAILABLE` from stubs, or were
advertised without a caller. Each unowned method still had a protocol entry, a
registry row, a typed client method, an adapter branch, tests, and
documentation. That surface made every capability audit harder and invited
clients to build on methods no release served.

Two neighboring surfaces had the same problem:

- The materialized-view store had a `wallet` preset. It selected the
  `transparent_address_transaction_history` and `transparent_outpoint_spend`
  consumers inside the optional Explorer store. The wallet runtimes never read
  that store; Zallet and ZODL read the canonical-plus-wallet-projection serving
  pair. The name suggested that the preset switched wallet support on.
- `zinder.v1.ops.ServerInfo` and the readiness report carried a
  `materialized_view_preset` and its identities. `zinder-query` hardcoded the
  wallet preset into its `ServerInfo`, although it never opens a
  materialized-view store. A wallet client therefore saw a store identity that
  did not exist.

Every consumer of Zinder is maintained by the same team and is updated with
Zinder, so the cleanup is a coordinated breaking change without aliases.

## Decision

The native transparent surface keeps only methods and capabilities that a
named packaged consumer calls. Ownership comes from the consumer's code, not
from the protocol, the registry, or a planned feature. Unowned methods are
removed together with their capability strings, typed client methods,
adapter branches, domain types, storage representations, tests, and
documentation.

### Ownership evidence

The audit read the current consumer sources at these revisions:

- Zallet (`zcash/zallet`) branch `feat/zinder-p2a-current` at `a28790a`, the
  current native Zinder backend.
- Zally at `4eaa0bb`.
- Zpay at `94828c7`. Zpay reaches Zinder through `zinder-client` directly and
  through the `zally-chain` Zinder source.
- Cipherscan at `dfbf0561`. The Cipherscan frontend calls the HTTP routes of
  `zinder-compat-cipherscan`, which call `ExplorerQuery`; the frontend never
  calls `WalletQuery`.
- The `zinder-compat-lightwalletd` runtime (the ZODL path) and the
  `zinder-explorer` runtime in this repository.

| Method or capability | Owning consumer and evidence | Serving reader | Decision |
| --- | --- | --- | --- |
| `TransparentAddressUnspentOutputs`, `wallet.address.transparent_unspent_outputs_v1` | Zallet `backends/zinder/src/lib.rs:34` requires it and `backends/zinder/src/chain.rs:657` calls it. Zally `crates/zally-chain/src/zinder_source.rs:59` requires it and `:337` calls it; Zpay inherits this path through `zally-chain`. The ZODL path calls the domain method at `services/zinder-compat-lightwalletd/src/grpc.rs:339` for `GetAddressUtxos`. | Wallet projection unspent rows in the admitted `WalletServingReadPair` | Kept |
| `TransparentAddressTxIdsInRange`, `wallet.address.transparent_history_v1` (ascending) | Zallet `backends/zinder/src/lib.rs:35` requires it and `backends/zinder/src/chain.rs:768` calls it. The ZODL path calls the domain method at `services/zinder-compat-lightwalletd/src/grpc.rs:853` for `GetTaddressTxids`. | Wallet projection history rows | Kept, ascending only |
| `descending` request mode | No consumer requests newest-first history. Zallet sets `descending: false` at `backends/zinder/src/chain.rs:764`. The release query rejected `true`. | None | Removed; field 6 is reserved |
| `TransparentAddressBalance`, `wallet.address.transparent_balance_v1` | No native consumer. The release query never advertised it. The ZODL path calls the in-process domain method at `services/zinder-compat-lightwalletd/src/grpc.rs:419` for `GetTaddressBalance`. | Wallet projection balance rows, in process | Native RPC, capability, and client method removed; the domain method stays for the compatibility adapter |
| `TransparentOutputsByOutpoint`, `wallet.read.transparent_outputs_by_outpoint_v1` | No consumer. Zpay ADR-0007 planned it, and Zpay ADR-0015 replaced that plan with `transaction_by_id` (`crates/zpay-runtime/src/zinder_fetcher.rs:26`). | None; the release query returned a stub | Removed |
| `TransparentSpendsByOutpoint`, `wallet.read.transparent_spends_by_outpoint_v1` | No consumer | None; the release query returned a stub | Removed |
| `TransparentUnspentOutputsByOutpoint`, `wallet.read.transparent_unspent_outputs_by_outpoint_v1` | No consumer | None; the release query returned a stub | Removed |
| `TransparentUtxoSetSummary`, `wallet.read.transparent_utxo_set_summary_v1`, `wallet.read.transparent_utxo_set_commitment_v1` | Only `ExplorerQuery.UtxoSetSummary` called it, and no release query served it. No Cipherscan route reads `UtxoSetSummary`. | None; the release query returned a stub | Removed, together with `ExplorerQuery.UtxoSetSummary`, `explorer.utxo_set.summary_v1`, and `explorer.utxo_set.commitment_v1` |
| `TransparentMempoolOutputsByAddress`, `wallet.mempool.transparent_outputs_by_address_v1` | No consumer. Zallet follows the mempool through `MempoolSnapshot` and `MempoolEvents` (`backends/zinder/src/lib.rs:37-38`). | Writer mempool index through `IngestControl` | Removed, with the matching `IngestControl` method |
| `TransparentMempoolSpendsByOutpoint`, `wallet.mempool.transparent_spends_by_outpoint_v1` | No consumer | Writer mempool index through `IngestControl` | Removed, with the matching `IngestControl` method |
| `TransparentMempoolOutputsByOutpoint`, `wallet.mempool.transparent_outputs_by_outpoint_v1` | No consumer. The release query never advertised it. | Writer mempool index through `IngestControl` | Removed, with the matching `IngestControl` method |

`ChainValuePoolsAtTip` is not a transparent read and is outside this decision.

### Durable reverse-spend resolution

Durable spender identity is not a wallet-query capability. No wallet consumer
asks where a transparent output was spent. The only reader of the
`transparent_outpoint_spend` consumer is `ExplorerQuery.TransactionDetail`
(`explorer.transaction.detail_v4`). It reads the consumer at
`services/zinder-explorer/src/grpc/transaction_detail.rs:323` to fill
`TransparentOutput.spent_by`. Cipherscan reaches that response through the
adapter route `/api/tx/{transaction_id}` at
`services/zinder-compat-cipherscan/src/lib.rs:3211`.

The consumer therefore stays as an Explorer view. ADR-0029 keeps its authority,
retention, and recovery contract and drops the wallet-query serving clause.
The consumer is also the Explorer replay progress anchor. The ingest replay
host reads its coverage to decide when materialized-view replay has reached
the canonical visible tip. It has no canonical retention coupling: canonical
storage never deletes data on the strength of materialized-view rows.

### Materialized-view manifest

Every bundled materialized-view consumer is an Explorer view, and the store
has one admissible manifest: the exact bundled consumer set. The decision
removes these items:

- `MaterializedViewPreset` and its `wallet` and `explorer` values.
- `WALLET_PROJECTION_CONSUMERS` and preset detection.
- The consumer `role` and `preset_membership` registry fields.
- The `transparent_address_transaction_history` materialized-view consumer.
  Its domain had no reader. Wallet history is served from the wallet
  projection, and Explorer address activity has its own consumers.

`MaterializedViewStore::open_explorer_views` and
`MaterializedViewStore::open_explorer_views_secondary` admit only the exact
bundled manifest. They preflight the recorded manifest before the database
opens. A store that records an identity this release does not bundle fails
with `ConsumerNotDeclared`. A store that records a different subset fails with
`ExplorerViewManifestRequiresFreshStore`. Neither failure mutates the store.

### Discovery metadata

Wallet and ingest endpoints carry no materialized-view metadata. The
`materialized_view_preset` and `materialized_view_identities` fields leave
`zinder.v1.ops.ServerInfo` (tags 6 and 7 are reserved).
`materialized_view_preset` leaves `zinder.v1.ops.ReadinessReport` (tag 5 is
reserved). Explorer view identities move to
`zinder.v1.explorer.ExplorerServerInfo.materialized_view_identities` (tag 4).
The readiness `materialized_view_identities` field and the
`zinder_materialized_view_identity_info` metric remain. Only the process that
admitted the Explorer view store sets them, and the metric no longer has a
`preset` label.

Removing request fields, methods, and response fields changes the native wire
contract in place, so `zinder_proto::CONTRACT_REVISION` is 7.

## Old-to-new mapping

| Removed item | Replacement |
| --- | --- |
| `WalletQuery.TransparentAddressBalance`, `ChainIndex::transparent_address_balance`, `Capability::TransparentAddressBalance` | None on the native endpoint. Lightwalletd clients keep `GetTaddressBalance` and `GetTaddressBalanceStream`. |
| `TransparentAddressBalance.unconfirmed_delta_zat`, `TransparentAddressBalance::projected_total_zat`, `TransparentAddressBalance::empty` | None. The domain balance is the canonical confirmed total. |
| `TransparentAddressTxIdsInRangeRequest.descending`, `TransparentAddressTxIdsQuery::descending` | None. History is ascending; omit the field. |
| `WalletQuery.TransparentOutputsByOutpoint`, `TransparentSpendsByOutpoint`, `TransparentUnspentOutputsByOutpoint`, and their `ChainIndex`, `ChainSnapshot`, and `OwnedChainSnapshot` methods | None. `ExplorerQuery.TransactionDetail` reports transparent inputs, outputs, and canonical spenders for one transaction. |
| `WalletQuery.TransparentMempoolOutputsByAddress`, `TransparentMempoolSpendsByOutpoint`, `TransparentMempoolOutputsByOutpoint`, the matching `EndpointBackedIndex` methods, and `TransparentMempoolOutputsRequest` | `MempoolSnapshot` and `MempoolEvents`. Every `MempoolEntry` carries its `transparent_outputs` and `transparent_spends`. |
| `IngestControl.TransparentMempoolOutputsByAddress`, `TransparentMempoolSpendsByOutpoint`, `TransparentMempoolOutputsByOutpoint` | None. The wallet-query admission set shrinks from seven control methods to five. |
| `WalletQuery.TransparentUtxoSetSummary`, `ExplorerQuery.UtxoSetSummary`, `TransparentUtxoSetSummaryView`, `zinder_core::TransparentUtxoSetSummary`, the UTXO-set commitment wire messages | None. The wallet projection keeps its internal `LtHash16` summary as construction evidence (ADR-0026). |
| `MaterializedViewPreset::Wallet`, `MaterializedViewPreset::Explorer` | `MaterializedViewStore::open_explorer_views` and `open_explorer_views_secondary` |
| `MaterializedViewStore::detect_materialized_view_preset_at_path` | `MaterializedViewStore::exists_at_path` |
| `MaterializedViewStore::effective_materialized_view_preset` | `MaterializedViewStore::consumer_identities` |
| `MaterializedViewStoreError::MaterializedViewPresetRequiresFreshStore` | `MaterializedViewStoreError::ExplorerViewManifestRequiresFreshStore` |
| `ops.ServerInfo.materialized_view_preset` and `materialized_view_identities`, `zinder_client::ServerInfo` fields of the same names, `WalletEndpointMetadata::materialized_view_preset` | `ExplorerServerInfo.materialized_view_identities` on Explorer endpoints; nothing on wallet endpoints |
| `ReadinessReport.materialized_view_preset`, `Readiness::set_materialized_view_workload` | `Readiness::set_materialized_view_identities`; the identities field remains |
| `zinder_materialized_view_workload_info` metric and the `preset` label | `zinder_materialized_view_identity_info{identity}` |
| `MAX_TRANSPARENT_OUTPUTS_PER_REQUEST` | None |

## Consequences

- An Explorer view store built by an earlier release records the retired
  `transparent_address_transaction_history` consumer. The current release
  refuses to open it. Operators rebuild Explorer views on a fresh
  materialized-view path from genesis-complete canonical history (ADR-0028),
  following
  [Rebuild Explorer views after a manifest change](../runbooks/deploying-on-a-vm.md#rebuild-explorer-views-after-a-manifest-change).
  Canonical storage and the wallet projection are unaffected.
- A store built with the old `wallet` preset records only part of the
  manifest and is refused the same way.
- Native clients built against contract revision 6 that call a removed method
  receive `UNIMPLEMENTED`. A revision-6 client that still sends
  `descending = true` is answered in ascending order, because the server
  ignores the reserved field.
- Zallet must stop setting `descending` on `TransparentAddressTxIdsQuery`.
- `zinder-query` admits `zinder-ingest` only at the same contract revision,
  so this release is a coordinated upgrade: stop both, replace both, and start
  ingest before query. A mixed revision-6 and revision-7 pair fails admission
  in either direction.
- The writer's mempool index keeps only the outpoint sets that reject
  conflicting live entries; it no longer holds a by-address index.
- Adding a transparent method again requires a named consumer call site,
  a registry row, a serving reader, positive and negative contract tests, and
  an update to this ownership table.

## Rejected alternatives

### Keep unserved methods as protocol vocabulary

A method that no release serves still costs a registry row, a client method,
an adapter branch, and documentation. Clients also cannot tell a reserved
future method from a supported one. The protocol can add a method when a
consumer needs it.

### Serve the UTXO summary from the wallet projection

The wallet projection already maintains an authoritative count, total, and
commitment at its source position. Exposing it would make the summary
correct, but no packaged consumer reads it. Serving a method without an owner
is the condition this decision removes.

### Keep the wallet preset under a capability-specific name

The preset's two consumers had no wallet reader. A renamed bundle would still
describe a workload that no runtime opens.

### Accept older Explorer manifests that contain the retired consumer

Dropping the obsolete column families on open would be a migration branch in
the store's admission path. The materialized-view plane already rebuilds from
canonical history when its manifest changes, so a fresh store is the supported
path.
