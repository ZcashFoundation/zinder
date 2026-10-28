# ADR-0042: Proven transaction absence

| Field | Value |
| --- | --- |
| Status | Accepted |
| Product | Zinder |
| Domain | Native transaction lookup, error vocabulary, Rust client |
| Related | [Wallet data plane §Transaction Status and Enrichment](../architecture/wallet-data-plane.md#transaction-status-and-enrichment), [Error vocabulary](../reference/error-vocabulary.md), [ADR-0003](0003-canonical-storage-access-boundary.md), [ADR-0007](0007-mempool-topology-and-retention.md) |

## Context

Wallets decide that a broadcast payment can never be mined, and release its
idempotency key, partly because `WalletQuery.Transaction` reports the
transaction as not found. That decision is irreversible for the payer: a wrong
"not found" lets the host pay twice.

A transaction lookup takes an id and no height, so a miss is only meaningful
relative to what the server searched. Several conditions produce a miss that is
not an absence:

- A storage fault, a missing chain epoch, or a block that is not in the best
  chain.
- A store whose canonical history starts after a checkpoint. The transaction
  may be mined at a height the store never retained.
- A replica whose answering epoch is below the mining height.
- A live mempool view that is unavailable, or that describes a different epoch
  than the canonical read. A transaction can leave the mempool for a block the
  replica has not caught up to and then miss both reads.

All of these used to reach the wire as gRPC `NOT_FOUND`, several with the same
`ARTIFACT_UNAVAILABLE` reason and no further detail. The Rust client mapped
every `NOT_FOUND` to `TxStatus::NotFound`.

## Decision

An absence is a typed answer with its own reason. `WalletQuery.Transaction`
reports a proven absence as gRPC `NOT_FOUND` with
`ErrorInfo.reason = TRANSACTION_NOT_FOUND`, and no other failure carries that
reason. Its `ErrorInfo` metadata states what was searched:

| Key | Meaning |
| --- | --- |
| `chain_epoch_id` | Epoch whose canonical chain was searched |
| `first_covered_height` | First height searched; always `1`, because an absence is issued only over complete history |
| `visible_tip_height` | Last height searched, the epoch's visible tip |
| `covers_mempool` | `true` when the writer's live mempool at the same epoch was searched too |

The server issues the absence only when all of these hold:

1. The canonical read at the answering epoch missed.
2. The store retains complete canonical history. A store whose history starts
   after a checkpoint answers a miss with `CANONICAL_HISTORY_UNAVAILABLE`
   (`FAILED_PRECONDITION`) and `metadata["first_available_height"]` instead.
3. For a lookup without `at_epoch_id`, the writer's mempool missed at the same
   epoch id. The ingest `MempoolTransaction` miss carries the same typed
   absence for the writer's epoch, and the query plane compares the two. A
   different epoch fails with `CHAIN_EPOCH_MISSING`; a missing mempool view
   fails with `DEPENDENCY_NOT_CONFIGURED`; an unavailable one keeps its
   `UNAVAILABLE` status.

   A mempool generation answers for exactly one source tip. Zebra's mempool
   changes and tip notifications are separate streams with no shared order,
   so a `Mined` change for a block outside that tip's chain (above it, or at
   its height on another branch) can arrive before the tip change. The owner
   then ends the generation without applying the change, exactly as a tip
   change does, and a snapshot whose staged changes include such a block is
   not certified. Point reads stay `UNAVAILABLE` until the next generation is
   certified, so the transaction is never missing from both views.

   An `Invalidated` change still removes the entry within the generation. The
   node reports it only for a transaction that its best chain does not
   contain, so the transaction is not mined. Removals the node makes because
   of a newer block, such as an expiry or a conflicting spend, can make an
   unpinned absence reflect the mempool slightly after the epoch's tip; such a
   transaction can no longer be mined on that chain.

A lookup with `at_epoch_id` answers from that epoch or fails with
`CHAIN_EPOCH_PIN_UNAVAILABLE`. It never consults the mempool, and its absence
states `covers_mempool = false`.

Absence travels as a reason on `NOT_FOUND` rather than as a third arm of the
`TransactionLocation` oneof. An older client that receives a new oneof arm
fails every miss with a decode error, while an older client that receives the
new reason keeps its previous behavior. The reason also reuses the existing
`ErrorInfo` vocabulary, so the absence and its failures are matched the same
way.

`zinder-client` returns `TxStatus::NotFound` only for this signal, and only
after checking that the absence answers the request: the epoch matches a pin,
an unpinned absence covers the mempool, and the covered range starts at height
1. Other outcomes are typed errors:

- `IndexerError::CanonicalHistoryUnavailable` carries the first retained
  height.
- `IndexerError::TransactionAbsenceUnverified` covers a `NOT_FOUND` without the
  typed signal: no Zinder `ErrorInfo`, or `ARTIFACT_UNAVAILABLE` without
  `ResourceInfo`. Servers below contract revision 6 answer every miss this way.
- Storage, epoch, and best-chain failures keep their existing variants.

Introducing this contract raised `zinder_proto::CONTRACT_REVISION` to 6.

## Consequences

- A caller can judge a transaction against a height by taking a
  `ChainSnapshot` whose epoch's visible tip covers that height and looking the
  transaction up through it. The snapshot pins the lookup, so `NotFound` means
  absent from that epoch's canonical chain from height 1 through its tip.
- The Rust client's `TxStatus::NotFound` carries no epoch. For an unpinned
  lookup the caller cannot tell which epoch, and so which heights, the absence
  covers, so a payment decision must use the pinned snapshot lookup.
- A caller that also needs the mempool answer makes an unpinned lookup or
  calls `is_in_mempool`. A pinned lookup never reports `InMempool`.
- Checkpoint-bootstrapped stores never answer a transaction absence. Wallet
  serving already requires complete history, so serving deployments are
  unaffected.
- A replica behind its writer returns `CHAIN_EPOCH_MISSING` for an unpinned
  miss until it catches up to the writer's epoch. Clients retry with backoff.
- A deployment without a live mempool view cannot answer an unpinned miss.
- An older client keeps mapping every `NOT_FOUND` to `TxStatus::NotFound`,
  including the failures above, until it upgrades.
