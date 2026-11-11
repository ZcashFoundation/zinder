# ADR-0018: Capability-gated optional payload fields

Status: Accepted
Date: 2026-05-19
Related: [ADR-0009](0009-explorer-plane-as-product-surface.md),
[ADR-0017](0017-materialized-view-consumer-and-key-codec.md)

## Context

Several wire fields in the explorer plane carry values that depend on
upstream state the operator may or may not have wired:

- `TransactionDetailResponse.paid_fee_zat` requires the wallet plane's
  transparent prevout resolution (capability
  `wallet.read.transparent_outputs_by_outpoint_v1`) to be online and the
  transaction to be classified `TransparentOnly`. Canonical facts do not
  retain the value balances needed to prove shielded transaction fees.
- `MempoolActivityEntry.paid_fee_zat` requires the same upstream
  capability plus a per-mempool-tx lookup path.
- `BlockSummary.paid_fees_collected_zat` requires the
  `TransactionFeesConsumer` to have materialized rows for every
  non-coinbase transaction in the block.
- `TransparentAddressActivityEntry.net_value_zat` requires the same
  prevout resolution; the row materializes either way.
- `TransactionHistoryEntry.zip317_conventional_fee_zat` is unset for
  coinbase rows; non-coinbase rows always carry a value.

We need one convention for "this field is present when capability X is
online (or some equivalent precondition holds), absent otherwise" that
consumers can branch on without inferring from sentinel values like
zero.

## Decision

Optional payload fields paired with named capability strings are the
convention. Specifically:

1. The proto field is `optional <scalar>` (proto3 explicit-presence) or
   `repeated <message>` (empty means absent). Sentinels like zero or
   empty bytes are never overloaded to mean "absent"; proto3
   field-presence handles that for us.

2. When a field's absence has multiple causes a reader might want to
   distinguish ("the upstream is offline" vs "this particular row
   could not be resolved"), the message carries a companion enum
   alongside the optional value. The current example is
   `TransparentAddressActivityRecord.prevout_resolution_status`
   (`Resolved | Partial | Unavailable | Unspecified`), which is set on
   every row so the handler renders a chip rather than guessing.

3. The capability string is registered in `zinder-proto::capabilities` and
   added to the `CAPABILITIES` table with the method that owns the field.

4. The handler returns the field as `None` / empty whenever the
   underlying upstream is unavailable, and the field is fully populated
   when present. The handler never emits the field with a partial
   sentinel: never `paid_fee_zat = 0` to mean "unresolved", never
   `net_value_zat = 0` to mean "we only saw one side of the
   transaction".

5. Capabilities are gated at the adapter's `advertised_capabilities()`
   on a per-named-flag basis. The flags are set by the binary at
   startup based on:

   - probing the upstream's `ServerInfo`
     (`wallet.read.transparent_outputs_by_outpoint_v1` flips on when the wallet
     advertises it);
   - whether the materialized-view store has been wired
     (`materialized_view_store_online` covers `BlockSummary`, `BlockDetail`,
     `MempoolEventCounts`, `RecentTransactions`, and
     `TransparentAddressActivity`).

   `advertised_capabilities()` is the single source of truth: the gRPC
   `ServerInfo` and the ops endpoint's `/healthz` both read from it. A flag flipped in
   one place therefore reaches every consumer.

## Examples shipped under this convention

| Field | Capability | Status-companion field |
| ----- | ---------- | ---------------------- |
| `TransactionDetailResponse.paid_fee_zat` | `explorer.transaction.fees_v1` | `prevout_resolution_status` |
| `TransactionDetailResponse.transparent_inputs[].value_zat` | `explorer.transaction.fees_v1` | (status on the parent) |
| `MempoolActivityEntry.paid_fee_zat` | `explorer.transaction.fees_v1` (when mempool prevouts are online) | (none; fall back to ZIP-317 floor) |
| `BlockSummary.paid_fees_collected_zat` | `explorer.transaction.fees_v1` (advertised when the consumer is wired and prevouts are online) | (none; the row carries `fees_collected_zat` as the ZIP-317 floor always) |
| `TransparentAddressActivityRecord.net_value_zat` | `explorer.transparent_address.activity_v1` | `prevout_resolution_status` on the record |
| `RecentTransactionEntry.zip317_conventional_fee_zat` | `explorer.transaction.recent_v1` | (none; `is_coinbase = true` explains absence) |
| `RecentTransactionEntry.paid_fee_zat` | `explorer.transaction.fees_v1` | (none; absence means "not provable from retained facts") |
| `TransactionHistoryEntry.zip317_conventional_fee_zat` | `explorer.transaction.history_v1` | (none; `is_coinbase = true` explains absence) |
| `TransactionHistoryEntry.intrinsic_value_balances` | `explorer.transaction.intrinsic_value_balances_v1` | (none; absence remains unknown and never means all-zero balances) |
| `TransactionDetailResponse.intrinsic_value_balances` | `explorer.transaction.intrinsic_value_balances_v1` | (none; absence remains unknown and never means all-zero balances) |
| `TransactionHistoryEntry.paid_fee_zat` | `explorer.transaction.fees_v1` | (none; absence means "not provable from retained facts") |
| `MinedTransaction.raw_transaction_bytes` | `wallet.read.transaction_bytes_v1` | (none; absence means "transaction blob not retained") |

`MinedTransaction.raw_transaction_bytes` is the wallet-surface example.
The field is `optional bytes`, gated on the `RequiresTransactionBlobs`
advertise policy. The handler reads the transaction blob from the store
and carries its `Option<Vec<u8>>` straight into the field: `None` when no
blob is retained, `Some(bytes)` when it is. The gate is the store's
persisted raw-blob retention, surfaced through `WalletQuery.ServerInfo`.

## Advertise policies gated on persisted blob retention

Blob-serving wallet capabilities are gated on the retention the writer
persisted, not advertised unconditionally:

- `RequiresBlockBlobs` advertises `wallet.read.full_block_at_v1` and
  `wallet.read.full_block_range_v1` when the store retains full block
  blobs (ingest `raw_blob_policy = all`).
- `RequiresTransactionBlobs` advertises
  `wallet.read.transaction_bytes_v1` when the store retains transaction
  blobs (ingest `raw_blob_policy` in `{transactions, all}`).

The signal is the `RawBlobRetention` value (`none`, `transactions`, or `all`)
that `CanonicalStoreBuildPlan` fixes before construction loads any data and
that the ready control record binds. It is the store's immutable historical
coverage contract: opening the store with another retention fails admission
and requires a rebuild. Readers use only the persisted value, and a control
record with an unknown retention value fails admission instead of being
treated as `none`.

## Materialized-view capabilities require materialized-view evidence

An online materialized-view store is not enough to advertise a backfilled materialized view as complete. The service evaluates the named consumer's materialized-view checkpoint and coverage from one read snapshot. A base capability may be advertised when partial rows are useful and the response exposes their bounds. A completeness capability is advertised only when verified contiguous coverage reaches the fenced materialized-view tip and the ending hash matches. Canonical artifact schema, block-summary freshness, and global ingest readiness are inputs, not substitutes for this evidence.

## Consequences

- Consumers branch on proto field presence (`response.paid_fee_zat
  .is_some()`) and on the capability list returned by
  `ServerInfo.capabilities`, never on sentinel comparisons. The capability
  means the fee projection and transparent-input resolution are online; it
  does not promise an actual fee for shielded or unclassified transactions.
- Adding a new field of this shape requires three diff sites in the
  same change: the proto, the capability constant + uniqueness/coverage
  tests, and the handler that populates it. When the field needs a
  status companion, the record gains a fourth diff site.
- Operators see in `/healthz` and `ExplorerQuery.ServerInfo` which
  optional fields are populated on their deployment, since the
  capability list mirrors the proto-field decisions.
- The convention scales: when a future field requires its own upstream
  flag, the binary adds a new named gate next to the existing ones,
  `advertised_capabilities()` adds one `if flag` arm, and the
  capability string lights up only when the gate is true.
