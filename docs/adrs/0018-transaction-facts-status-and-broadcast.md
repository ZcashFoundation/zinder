# ADR-0018: Parse transactions once and type every lookup and broadcast outcome

Status: Accepted

## Context

Wallet status, Explorer detail, fees, and address activity all need the same
public facts about a transaction, such as version, expiry height, consensus
branch, and privacy shape. If each reader parsed raw bytes itself, the
readers would drift.

Wallets also make irreversible decisions from lookups. A wallet that reads
"not found" for a broadcast payment may release its idempotency key and pay
again. A storage fault, a store whose history starts after a checkpoint, a
replica behind its writer, or a mempool view at a different epoch all produce
a miss that is not an absence.

The node reports most mempool rejections under one JSON-RPC code, with
free-form text. A benign "already queued" state and hard rejections such as a
bad expiry height or a full mempool need different handling. Consumers that
matched substrings of node messages broke whenever the wording changed.

## Decision

1. **One parser output.** `zinder-source` turns transaction bytes into
   `TransactionPublicFacts`. Ingest commits those facts with each canonical
   block, and every reader uses them. Readers do not re-parse on the normal
   path. Adding a public fact is a storage schema and API change. Raw
   transaction bytes are stored only as the deployment's raw-blob retention
   allows.
2. **Absence is explicit.** A read never fabricates zero for a missing fact
   and never calls the node to fill one. Read-time context such as
   confirmations, block time, and branch ID is computed from the same chain
   epoch as the response, so it cannot race the tip.
3. **"Not found" is a proof.** `WalletQuery.Transaction` answers `NOT_FOUND`
   with the reason `TRANSACTION_NOT_FOUND` only when all of these hold:
   - the canonical read at the answering epoch missed;
   - the store holds canonical history from height 1;
   - for a lookup without a pinned epoch, the writer's live mempool also
     missed at that same epoch.

   The error metadata states the epoch, the covered height range, and whether
   the mempool was searched. Every other miss gets its own reason, such as
   `CANONICAL_HISTORY_UNAVAILABLE` on a checkpoint-bootstrapped store. A pinned
   lookup never consults the mempool. The Rust client returns `NotFound` only
   for this signal and only after checking that it answers the request.
4. **Payment decisions use pinned lookups.** The canonical half of an absence
   is exact. The mempool half is best effort around reorgs and block
   boundaries, because the node's mempool changes and tip notifications are
   separate streams with no shared order.
5. **Broadcast outcomes are typed.** A broadcast returns accepted, duplicate,
   invalid encoding, rejected with a typed reason, unknown, or queued.
   Queued is its own outcome, not a rejection reason. A rejection is a
   successful response that carries a reason, not a gRPC error. Only
   `MEMPOOL_FULL` is retryable with the same bytes. A client that decodes an
   unknown or unspecified reason treats it as `UNKNOWN`.
6. **Only `zinder-source` reads node message text.** It maps Zebra and
   Zakura wording to the typed reasons in one place. Unmatched text becomes
   `UNKNOWN`, and the node message is still forwarded.
7. **Lightwalletd clients keep lightwalletd's shape.** The compatibility
   runtime forwards the node's error code and message for every rejection and
   reports queued as code `-25`. It invents no Zinder-specific codes.

8. **Mined locations carry the byte commitment.** The schema v1 canonical
   transaction index stores the SHA-256 commitment to the exact serialized
   transaction bytes alongside the block position. This lets a reader obtain
   the commitment in one index lookup under every raw-blob retention policy.
   [Storage backend](../architecture/storage-backend.md#canonical-store)
   defines the 72-byte row value.

Method behavior is in
[Wallet data plane](../architecture/wallet-data-plane.md), and error reasons
with their retry rules are in
[Error vocabulary](../reference/error-vocabulary.md).

## Consequences

- A parser fix reaches every reader together.
- A checkpoint-bootstrapped store never answers a transaction absence.
  Wallet-serving deployments build complete history, so they are unaffected.
- A replica behind its writer answers an unpinned miss with
  `CHAIN_EPOCH_MISSING` until it catches up. Clients retry with backoff.
- A caller that must judge a transaction against a height takes a snapshot
  whose tip covers that height and looks the transaction up through it,
  because the client's `NotFound` carries no epoch.
- A new node rejection message either earns a classifier rule or falls
  through to `UNKNOWN`. Substring matching cannot separate two messages that
  share a keyword without a narrower rule.
- New rejection reasons are additive enum values. Clients that predate the
  unknown-to-`UNKNOWN` rule fail on new values, so the client ships before or
  with the server that adds one.

## Alternatives rejected

- **Parse raw bytes at read time.** Each reader would own its own parser and
  its own bugs.
- **Fill missing facts from the node at read time.** The answer would mix
  chain views and depend on node availability.
- **Map every miss to `NOT_FOUND`.** A storage fault or a lagging replica
  would read as proof of absence and could cause a double payment.
- **Report absence as a new arm of the location result.** An older client
  fails every miss with a decode error. A new reason on `NOT_FOUND` leaves an
  older client's behavior unchanged.
- **Treat queued as a rejection reason.** Every caller would have to inspect
  the reason to learn the transaction is still in flight.

## Revision history

- 2026-11-24. Added the canonical transaction location byte commitment.
