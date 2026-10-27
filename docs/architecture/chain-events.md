# Chain Events

Chain events are the contract between source observations, canonical commits, query freshness, and derived-index replay. They are not a storage table and not a wallet API by themselves.

This document defines the event vocabulary shared by canonical storage,
`WalletQuery`, and the private `IngestControl` replay surface.

## Boundaries

Zinder has three event boundaries with different jobs:

| Boundary | Producer | Consumer | Purpose |
| -------- | -------- | -------- | ------- |
| Source observations | `NodeSource` adapter | `zinder-ingest` | Report upstream node observations before canonical commit |
| `ChainEvent` | `zinder-ingest` | ingest-owned publisher and private subscription endpoint | Describe the committed canonical transition |
| `ChainEventEnvelope` | ingest subscription plane | native adapters using `zinder-query`, `zinder-client`, `zinder-explorer`, and external derived consumers | Carry replayable, cursor-bound chain events over the wire |

Keep these layers distinct. A source observation is not a committed chain event. A wire envelope is not the internal state-machine event.

## Source Boundary

`NodeSource` normalizes configured upstream adapters into source observations. It does not decide canonical state and does not build artifacts. Trait shape, capability model, and adapter rules live in [Node source boundary](node-source-boundary.md).

For the event model: source observations are not committed chain events. Ingest drives the source through async polling. Best-chain selection uses cumulative chainwork, not tip height.

## Ingest State Machine

`zinder-ingest` consumes source events through one deterministic state machine; the pipeline and substep decomposition are in [Chain ingestion §Operation Shape](chain-ingestion.md#operation-shape). The contract this document owns:

- The event is persisted in the same `commit_chain_epoch` `WriteBatch` that advances the visible epoch pointer. Publication and commit are atomic.
- The state machine produces three outputs: a `ChainEpochArtifacts` value passed to `commit_chain_epoch`, a durable epoch pointer after the batch succeeds, and a `ChainEvent` published only after the epoch is durable.

## Canonical Event Boundary

`ChainEvent` is the in-process event emitted after canonical storage changes. It follows the Reth-style committed, reorged, reverted shape while preserving ADR-0003's post-commit event names.

```rust
pub enum ChainEvent {
    ChainCommitted {
        committed: ChainEpochCommitted,
    },
    ChainReorged {
        reverted: ChainRangeReverted,
        committed: ChainEpochCommitted,
    },
}
```

Use `ChainReorged` when one durable transition both invalidates a visible range within the reorg window and commits the replacement range. Use `ChainCommitted` for a pure append or settled-tip advance. A `ChainCommitted` event whose `committed.block_range.start > committed.block_range.end` advances epoch metadata without publishing block artifacts; materialized-view consumers should advance their cursor and apply no block contexts. Zinder does not expose an explicit rollback transition without a replacement range.

Do not publish source observations as `ChainEvent`. Do not publish `ChainEvent` before `commit_chain_epoch` succeeds.

## Reorg Replacement Contract

`ChainEpochArtifacts.reorg_window_change` carries the storage mutation that makes the event true:

| Chain transition | `ReorgWindowChange` | Published event |
| ---------------- | ------------------- | --------------- |
| Append inside the current best chain | `Extend { blocks }` | `ChainCommitted` |
| Reorg inside the configured window | `Replace { from_height }` | `ChainReorged` |
| Settled tip advances | `AdvanceSettledTipTo { height }` | `ChainCommitted` |
| No reorg-window mutation | `Unchanged` | `ChainCommitted` only when artifacts changed |

The replacement range must start at the first height where the old visible branch and the new selected branch differ. It must not replace data at or below the settled tip. If the replacement starts below the supported window or below the settled-tip boundary, `zinder-ingest` returns `ReorgWindowExceeded`, fails readiness with `reorg_window_exceeded`, and requires operator action.

A parked writer keeps its control listener open and reports the halt through `IngestControl.WriterStatus.reorg_window_exceeded`. Native query then refuses new requests with `REORG_WINDOW_EXCEEDED` and ends every open `WalletQuery.ChainEvents` stream with the same status, so a consumer sees a typed halt instead of a stream that stops advancing. The default window matches the node's finalization bound, so a park signals a change the node itself could not produce. See [ADR-0041](../adrs/0041-reorg-window-follows-the-node-bound.md).

Reject the name `ReorgTooDeep`. It describes a symptom. `ReorgWindowExceeded` names the configured boundary that was violated.

The phase-driven ingest loop's `TipFollow` phase is the only producer of
`ReorgWindowChange::Replace`; the `BulkCatchup` phase only appends and settles
already-stable ranges outside the reorg window.

## Wire Event Envelope

The ingest subscription plane exposes chain events as a resumable stream of `ChainEventEnvelope` messages:

```text
ChainEventEnvelope
  cursor: StreamCursorTokenV1
  event_sequence: u64
  chain_view: ChainView   // field tag 3
  event: ChainCommitted | ChainReorged
```

The envelope carries the cross-plane `ChainView` at field tag 3. The Substreams last-irreversible-block pattern maps to `chain_view.chain_epoch.settled_tip.height`: every envelope carries the settled tip height that was true for that event as the settled tip of its epoch, so the envelope needs no separate `settled_tip_height` field. Consumers may discard undo state at or below that height.

`StreamCursorTokenV1` uses the storage-authenticated cursor shape from [ADR-0002](../adrs/0002-boundary-specific-serialization.md). It carries the event sequence and a fork-aware locator (a tip-first, exponentially back-spaced set of `(height, hash)` pairs, capped at `CHAIN_EVENT_LOCATOR_MAX = 32`) for one chain-event stream, per [ADR-0025](../adrs/0025-chain-event-reconnect-reorg-locator.md). Adding a second cursor format for chain events requires updating this contract and the boundary-specific serialization ADR.

The wallet-facing exposure of this envelope is settled by [Wallet data plane §Chain-Event Subscription](wallet-data-plane.md#chain-event-subscription): the same `ChainEventEnvelope` shape is published as a `zinder.v1.wallet` proto message and streamed by `WalletQuery.ChainEvents`. The cursor crosses the wire as opaque bytes so wallet clients persist the exact bytes they received and replay strictly after them on reconnect.

The cursor body is not decorative state. `event_sequence` is the resume key, and the locator's `(height, hash)` pairs resolve the fork point against the canonical block index across reconnect. Every serialized cursor field belongs to its stream-specific contract; a field that is not consumed must be documented as reserved before serialization.

## Address Filters

`WalletQuery.ChainEvents` accepts an optional `address_filter` containing transparent addresses. This filter is an invalidation hint, not a per-address event stream:

- An empty filter delivers every envelope for the requested stream family.
- A non-empty filter delivers `ChainCommitted` envelopes only when at least one filtered transparent address appears in the committed block range according to the transparent-address transaction-history index.
- `ChainReorged` envelopes always pass through because consumers must invalidate cached derivations after a reorg regardless of which addresses they watch.
- Cursor bytes remain opaque and independent of the filter. A client that resumes with a different filter receives the envelope set produced by applying the new filter from the cursor forward.
- The current touch detection is backed by the transparent-address history index; clients still re-derive per-address state from canonical compact blocks or transparent-address read APIs after receiving a hint.

## Resume Semantics

Library-hosted derived consumers resume through
`chain_event_history(ChainEventHistoryRequest { from_cursor, max_events })`
during startup repair. The replay host reads the lowest durable materialized-view cursor,
replays retained events after that cursor, and dispatches each event through
`zinder_materialized_views::MaterializedViewStore::write_chain_event`. The materialized-view store persists each
cursor advance atomically with consumer writes. Fresh consumers whose persisted
cursor sits below the retention floor rebuild from canonical artifacts before
resuming retained-event replay.

Wallet consumers resume through `WalletQuery.ChainEvents` per [Wallet data plane §Chain-Event Subscription](wallet-data-plane.md#chain-event-subscription). The wire request expresses the start as the required `EventStreamStart` oneof (`after_cursor` | `earliest_retained` | `live_tail`, [ADR-0027](../adrs/0027-event-stream-start-positions.md)); the rules below govern the resolved resume position and are the same for both consumer paths.

Rules:

- With no cursor, the stream starts at the earliest retained canonical event.
- With a valid cursor, the stream returns events strictly after that cursor.
- Every history read has a non-zero `max_events` page bound. Consumers that need
  more events resume with the last cursor returned by the previous page.
- Consumers persist the cursor only after their sink has durably applied the event.
- If the cursor's events are pruned and no reorg bridges the gap, the API returns `EventCursorExpired`. It must not silently start from the current tip.
- If the cursor fails authentication or belongs to another network, store identity, or stream family, the API returns `EventCursorInvalid`. A zero or ahead-of-history sequence is also `EventCursorInvalid`; a reorg never produces it.
- If the cursor's branch was reorged out, the server resolves the fork point from the locator against the canonical block index. When the real reorg event is still retained at or after the cursor, that event replays. When it has been pruned, the server delivers a synthetic `ChainReorged` reverted from the fork point ahead of the resumed page, per [Chain events §Cursor varieties](chain-events.md#cursor-varieties). Recovery is idempotent: the synthetic envelope's cursor bookmarks the on-chain fork point, so a reconnect that has not yet applied the reorg recomputes the identical recovery. A divergence deeper than the locator cap, or an unresolvable fork-point block, degrades to `EventCursorExpired`.
- If a consumer falls behind retention, it must recover from a checkpoint or rebuild from canonical artifacts.

### Cursor varieties

`StreamCursorTokenV1`'s `flags` byte carries a family code in the lower nibble (per [Chain events §Cursor varieties](chain-events.md#cursor-varieties)). Two `ChainEvents` family codes are active:

- **`0x0` `CHAIN_EVENT_STREAM_FAMILY_VISIBLE`** — receives every `ChainCommitted` and `ChainReorged` envelope. Default for wallet consumers; clients must handle reorgs.
- **`0x1` `CHAIN_EVENT_STREAM_FAMILY_SETTLED`** — receives only non-reorg commits whose full range is at or below the settled tip. A `Settled` cursor cannot be reorged out below the settled tip by definition, so a locator miss is an expiry, not a synthesized reorg. Default for explorer and analytics consumers; trades latency for absence of reorg events. Bootstrap uses `WalletQuery.ChainEvents` with `family = Settled` and `start = earliest_retained` ([ADR-0027](../adrs/0027-event-stream-start-positions.md)).

Only the active `ChainEvents` family codes are defined here.

Do not use `epoch_history(from)` as the durable API name. It hides reorgs. Use `chain_event_history` or an explicitly equivalent gRPC name because consumers are replaying events, not only listing epochs.

Chain-event rows store event data and deterministic cursor inputs, not opaque
cursor bytes. The cursor is reconstructed when the envelope is returned, so the
durable row is not tied to one serialized token.

## Error Classification

Use these errors at the event and source boundaries:

| Error | Boundary | Meaning |
| ----- | -------- | ------- |
| `NodeUnavailable` | `NodeSource` | The configured upstream node cannot answer requests |
| `NodeCapabilityMissing` | `NodeSource` | A capability required by the caller is not advertised by the source |
| `TransactionBroadcastDisabled` | `TransactionBroadcaster` | The wired broadcaster is the no-op `()` impl, signaling a deliberately read-only deployment |
| `SourceProtocolMismatch` | `NodeSource` | The source response does not match the expected network or protocol version |
| `BlockUnavailable` | `NodeSource` | A block needed for ancestor reconstruction cannot be fetched |
| `ReorgWindowExceeded` | ingest and API | The selected branch requires replacing data outside the supported window |
| `EventCursorExpired` | `ChainEpochReadApi` | The event cursor is older than retained event history |
| `EventCursorInvalid` | `ChainEpochReadApi` | The cursor fails authentication, network, store, or stream-family validation |

Internal storage errors still map to API errors at service boundaries: `ChainEpochMissing` maps to `EpochNotFound`, and `ArtifactMissing` maps to `ArtifactUnavailable`.

## Retention And Backpressure

`zinder-ingest` must retain enough event history for expected `zinder-explorer` outages and query catch-up windows. The retention policy is an operational setting, not a protocol shortcut.

Event streams must be bounded:

- `ChainEventEnvelope` streams apply backpressure to `zinder-query` and `zinder-explorer`.
- A slow consumer must not block `commit_chain_epoch`.
- If a consumer exceeds retention, the system returns `EventCursorExpired` and requires replay from checkpoint or canonical artifacts.

Chain-event retention is governed by [Chain events §Retention And Backpressure](chain-events.md#retention-and-backpressure). The default policy is time-windowed, operator-tunable through `[retention] chain_event_retention_hours` (default 168). A background pruning task in `zinder-ingest` deletes events whose `created_at` falls below the cutoff; pruning preserves `event_sequence` monotonicity by leaving gaps rather than rewriting the sequence space. The `oldest_retained_sequence` is surfaced through the `cursor_at_risk` readiness cause when the retention window approaches exhaustion under load. Operators tune retention based on consumer characteristics; setting `chain_event_retention_hours = 0` disables pruning and is reserved for local development.

## Module Naming Guidance

Implementation modules are named after the boundary they own:

- `node_source`
- `chain_source_event`
- `chain_event`
- `chain_event_publisher`
- `event_stream`
- `chain_reorg`
- `event_cursor`

`event_stream` is the single home for the resumable subscription driver. One generic `run_event_stream`, parameterized by the `EventEnvelope` cursor-extraction trait, serves every cursor-bound family (chain events and mempool events); families differ only in the page-reader they pass in. The store-side resume resolution shares one `resolve_event_history_start_sequence` skeleton across families, with a family-specific position-check hook that owns cursor authentication, sequence-bound validation, and any fork or expiry handling.

Avoid `event_service`, `reorg_manager`, `notification_handler`, `source_processor`, `stream_utils`, and public `push` modules. Those names hide ownership and do not tell a reader which boundary they are reading.

## Cross-References

- [Wallet data plane §Chain-Event Subscription](wallet-data-plane.md#chain-event-subscription) defines the public wire surface (`WalletQuery.ChainEvents`, `ChainEventEnvelope`, `ChainCommitted`, `ChainReorged`).
- [ADR-0007](../adrs/0007-mempool-topology-and-retention.md) defines the parallel mempool event stream, which resumes through the `StreamCursorTokenV1` mempool-event family.
