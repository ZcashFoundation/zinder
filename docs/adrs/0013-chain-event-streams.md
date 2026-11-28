# ADR-0013: Event streams start explicitly and resume across reorgs

Status: Accepted

## Context

A `ChainEvents` consumer stores the opaque cursor of the last event it applied and resumes after it on reconnect. While it was away, its last block may have been reorged out. If the server resumed silently, the consumer would apply new blocks on top of a branch that no longer exists.

A cursor that records only its own tip block can detect that the tip was replaced, but cannot say where the two branches last agreed. The event log is bounded by retention, so the reorg event that crossed the consumer's tip is often already pruned. The canonical block index outlives the event log: it holds the hash of every visible height.

Subscribers arrive with one of three intents: resume after a cursor, replay everything retained, or follow only new events. One `bytes` field can express at most two of them, and only by giving an empty value a meaning. A tail-only consumer forced to replay the retained window pays that flood on every reconnect.

A consumer that pages through `MempoolSnapshot` and then subscribes to `MempoolEvents` needs a position that loses no event between the two.

Some changes revise what an existing RPC means without changing its name or fields. Capability strings announce new shapes, not new meanings of old ones.

## Decision

1. **A chain-event cursor carries a locator.** Inside its authenticated envelope, the cursor holds up to 32 `(height, hash)` pairs, starting at its tip and doubling the gap at each step toward genesis. The server builds it from the block index, and clients never parse it. Tampering fails authentication.
2. **Resume finds the fork point and heals.** On reconnect the server uses the locator to find where the consumer's chain and the current chain split, and synthesizes a `ChainReorged` back to that point when the real reorg event was pruned. [Chain events](../architecture/chain-events.md#resume-with-a-cursor) gives each resume case. A consumer therefore handles only `ChainCommitted` and `ChainReorged` and never compares hashes itself. `CHAIN_EVENT_CURSOR_INVALID` means corruption or forgery only, never a reorg. The synthesized event takes the consumer directly to the current epoch. Its sequence and cursor identify that tip because replaying any of the transitions it subsumes could undo blocks already reported as settled. The replacement starts above the consumer's previous settled tip, and settlement can advance on the replacement chain. The server recomputes recovery on each reconnect without writing it, so a secondary reader and the writer recover identically.
3. **The settled family never reorgs.** Each advance of the settled tip produces a `ChainCommitted` for the newly settled blocks. A cursor on the `Settled` family covers only blocks at or below the settled tip, so a locator miss there is an expiry.
4. **Every subscription states where it starts.** `ChainEventsRequest` and `MempoolEventsRequest` require an `EventStreamStart` with exactly one of `after_cursor`, `earliest_retained`, or `live_tail`. An unset start is `INVALID_ARGUMENT`. With `after_cursor`, the family encoded in the cursor wins, and a request that names a different family is rejected. `live_tail` resolves once, at subscribe time, to a cursor at the current head. After that the consumer resumes with `after_cursor` like any other subscriber. Paged reads keep their `from_cursor` field.
5. **A mempool snapshot names its stream position.** `MempoolSnapshotResponse.events_resume_cursor` is a `MempoolEvents` cursor captured atomically with the snapshot contents, and every page of one walk repeats it. Subscribing after it is at-least-once: events already in the snapshot may arrive again, and none is lost. When no event existed yet, the cursor is empty and the consumer subscribes with `earliest_retained`. The snapshot's chain epoch is a separate value that fences the canonical tip. It is never a mempool position.
6. **In-place semantic changes raise `contract_revision`.** `ops.ServerInfo.contract_revision` increases whenever an existing RPC changes meaning while its name and fields survive. Consumers require a minimum revision and refuse an older server. The server serves exactly one revision, so this is not a negotiation.

7. **Mobile wake-ups are level-triggered companion work.** The ratified `zinder-client` example in [Use cases](../use-cases.md#mobile-wallet-wake-ups) opens `ChainEvents` at `live_tail` on every connection. Its public tip signal prompts wallets to run their own sync. Durable replay remains a separate consumer contract under decision 2.

[Chain events](../architecture/chain-events.md) owns the envelope, cursor, and resume contract. [ADR-0002](0002-serialization-and-byte-order.md) records the cursor encoding.

## Evidence

- Decision 7: Judgment. A wake-up only asks the wallet to check current state, so replaying intermediate tips adds traffic without improving sync correctness. Reopen it if a consumer cannot recover by syncing to the current tip. Apple's advisory delivery limit is an External constraint on the companion's coalescing interval. [APNs background updates](https://developer.apple.com/documentation/usernotifications/pushing-background-updates-to-your-app).

## Consequences

- A wallet whose last block was reorged away recovers on reconnect without a full re-derive, even when the reorg event has aged out of the log.
- A reorg deeper than the locator reaches, or one whose fork-point block cannot be resolved, still expires the cursor, and the consumer re-derives from stored artifacts.
- Each delivered cursor costs a bounded number of block-index reads, and chain-event cursors are variable-length.
- A monitoring probe can follow the tail without replaying retained history.
- A snapshot followed by a subscription is a gapless bootstrap, and `GetMempoolStream` is composed from exactly that. Consumers must apply mempool events idempotently.
- Consumers check `contract_revision` against the semantics they were built for.

## Alternatives rejected

- **A cursor that holds only its tip block.** It cannot find the fork point once the reorg event is pruned.
- **Persist the synthesized reorg.** It would need a write on the read path, which secondaries cannot do.
- **An empty cursor meaning "replay everything".** An accidental empty value would silently replay the whole retained window, and tail-only intent stays inexpressible.
- **A bare snapshot sequence number.** It does not name a position in the event stream, so it cannot anchor the handoff.
- **Announce semantic revisions with capability strings.** Capabilities are additive and say a shape exists. They cannot say that an existing shape changed meaning.

## Revision history

- 2026-11-28. Decision 7 records the external mobile wake-up companion and its live-only subscription.
- 2026-11-20. Contract revision 10 makes a synthesized reorg a complete resume position and requires increasing epoch IDs and monotonic settlement across delivered chain events.
- 2026-11-20. Contract revision 12 projects settled-tip advances onto the Settled family. Initial gRPC metadata identifies a Settled subscription so clients can validate its ranges when the family comes from an opaque cursor.
- 2026-11-20. Contract revision 13 requires a writer to withdraw a stale live mempool tail and to honor the wallet’s mempool stream opening budget.
