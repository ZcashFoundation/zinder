# ADR-0027: Explicit Event-Stream Start Positions

| Field | Value |
| ----- | ----- |
| Status | Accepted |
| Product | Zinder |
| Domain | Event-stream subscription start contract, mempool snapshot-to-stream handoff, wire contract revision marker |
| Related | [ADR-0007](0007-mempool-topology-and-retention.md), [ADR-0024](0024-wire-format-rpc-byte-order.md), [ADR-0025](0025-chain-event-reconnect-reorg-locator.md), [Chain events](../architecture/chain-events.md), [Wallet data plane](../architecture/wallet-data-plane.md), [Public interfaces](../architecture/public-interfaces.md) |

## Context

A replayable event-stream subscriber arrives with one of three intents: resume strictly after a cursor it durably applied, replay everything the server retains, or follow only events that happen after it subscribes. A single `bytes` request field can express the first two at most, by overloading emptiness, and cannot express the third at all. A tail-only consumer forced through full retained-window replay pays a replay flood on every reconnect and must filter the backlog itself.

The mempool snapshot has a related gap. A consumer that pages through `MempoolSnapshot` and then subscribes to `MempoolEvents` needs a resume position that guarantees no event between the snapshot's construction and the subscription is lost. A bare monotonic snapshot sequence does not name a position in the event stream, so it cannot anchor that handoff.

Both changes revise the semantics of existing RPCs in place rather than adding new ones. Capability strings identify wire shapes additively; they do not signal that an existing shape's meaning changed. Consumers need a discoverable marker for in-place semantic revisions.

## Decision

### One `EventStreamStart` oneof on every replayable event stream

`ChainEventsRequest` and `MempoolEventsRequest` carry a required `EventStreamStart start` message whose `position` oneof is:

- **`after_cursor: bytes`.** Resume strictly after the opaque cursor from a previously delivered envelope.
- **`earliest_retained`.** Replay from the retention floor. This is the bootstrap path for a fresh consumer.
- **`live_tail`.** Deliver only events applied after the subscription.

An unset oneof is `INVALID_ARGUMENT`. There is no implicit default: the consumer states its intent or the request is rejected. Paging reads (`canonical_event_history`, `wallet_chain_event_history`, transparent-history streams, snapshot pages) keep their `from_cursor` field; the oneof belongs to subscriptions only.

The store owns start-position resolution: `EventStreamStartPosition` is the typed mirror of the oneof. `RocksDbCanonicalSecondary::resolve_wallet_chain_event_stream_start`, exposed to the query layer through `CanonicalReader`, and `RocksDbCanonicalStore::resolve_mempool_event_stream_start` turn a start position into the resume position the page loop reads strictly after (`ChainEventStreamResume` for the chain family). `zinder-query` resolves starts for its local-serve path through `WalletQueryApi::resolve_chain_events_start` and passes proxied requests through unchanged.

### Family resolution

The cursor's encoded family is authoritative. With `after_cursor`, the request's `family` field must be unset/default or equal to the family encoded in the cursor; a non-default family that disagrees is `INVALID_ARGUMENT` (`CHAIN_EVENT_CURSOR_INVALID`). Because proto3 cannot distinguish an unset enum from its zero value, a request carrying the default family (`Visible` for chain events) always defers to the cursor. `earliest_retained` and `live_tail` have no cursor to defer to, so they resolve within the request's `family` field.

### `live_tail` resolves once, at subscribe time

The server mints a head cursor when the subscription is accepted and the page loop resumes strictly after it. For chain events the minted cursor carries a locator anchored at the current visible tip plus the current event sequence, so the ADR-0025 reconnect-reorg machinery applies to it like any delivered cursor. For mempool events the head cursor is the newest retained envelope's own cursor. An empty event log resolves to the retention floor, which equals `earliest_retained` and only widens delivery.

`live_tail` is a start position, not a mode: after the first delivered envelope the consumer persists cursors and reconnects with `after_cursor` exactly like any other subscriber.

### Snapshot-anchored mempool resume

`MempoolSnapshotResponse` carries `events_resume_cursor: bytes`, an opaque `MempoolEvents` `after_cursor` value anchored at the moment the snapshot walk began. The snapshot-sequence concept is deleted; there is no `snapshot_sequence` field.

The response separately carries `chain_view.chain_epoch.chain_epoch_id`, which
the writer captures before reading the snapshot page. The epoch id is a
canonical-chain fence, not a mempool resume position; it advances in the same
identity space as `ChainEventEnvelope.event_sequence`. A consumer that requires
tip coherence closes or restarts its walk when it observes a larger chain-event
sequence. Keeping the 2 positions distinct prevents a mempool event cursor from
being misused as proof that the canonical tip remained stable.

The writer's mempool index records the last-applied event position (`MempoolEventPosition`: event sequence plus transaction id) under the same lock that applies entries, so the anchor and the snapshot contents are consistent by construction. The first snapshot page captures that anchor; the HMAC-authenticated snapshot-page token embeds the anchor pair so every later page of the same walk re-mints the identical resume cursor, byte-identical to the anchor envelope's own cursor. A stale paging token whose anchor sits ahead of the writer's applied sequence returns `SNAPSHOT_PAGE_CURSOR_EXPIRED` with the anchor and current event sequences in the precondition detail.

The handoff contract is at-least-once: replaying `MempoolEvents` from `events_resume_cursor` can re-deliver events already reflected in the snapshot (and a later page can enumerate an entry the stream also delivers), never lose one. Consumers apply events idempotently, which the typed `Added`/`Invalidated`/`Mined` envelopes make a keyed upsert/remove. When the writer had applied no mempool event when the walk began, `events_resume_cursor` is empty and the consumer subscribes with `earliest_retained`, which preserves at-least-once.

### `contract_revision` on `ops.ServerInfo`

`zinder.v1.ops.ServerInfo` carries `uint32 contract_revision`, a monotonically increasing marker incremented whenever the semantics of an existing wire surface are revised in place. Capability strings remain the additive mechanism for new shapes; the revision marker covers what they cannot: an RPC whose name and message tags survive while its meaning changes. The value is the single `zinder_proto::CONTRACT_REVISION` constant, returned by the ingest, query, and explorer `ServerInfo` builders. This decision sets it to 1.

Consumers assert a minimum (`contract_revision >= N` for the semantics they were built against) and refuse to run against an older server rather than misinterpreting its streams. The marker is not a negotiation surface: the server serves exactly one revision.

## Consequences

### What this enables

- A tail-only monitoring probe can subscribe with `live_tail` and avoid retained-window replay. The compat shim deliberately starts at `earliest_retained`, because its process-local watcher must recover tip changes across writer or network interruptions.
- A `MempoolSnapshot` walk followed by `MempoolEvents(after_cursor = events_resume_cursor)` is a gapless bootstrap: the lightwalletd shim's `GetMempoolStream` composes exactly this, streaming the snapshot contents first and the live stream after, with no client-side sequence filtering.
- An accidental empty-cursor request can no longer silently mean "replay everything"; every start is an explicit, validated intent.

### What this costs

- Breaking wire change: consumers built against `from_cursor` request fields or `snapshot_sequence` must migrate to `EventStreamStart` and `events_resume_cursor`, gated on `contract_revision >= 1`.
- The snapshot-to-stream overlap can deliver duplicates by design; consumers that cannot tolerate re-delivery must deduplicate by transaction id.
- The snapshot-page token grows by the embedded anchor pair; it stays fixed-length and HMAC-authenticated.

## Vocabulary

- `EventStreamStart` (the wire oneof: `after_cursor` | `earliest_retained` | `live_tail`).
- `EventStreamStartPosition` / `ChainEventStreamResume` (the store-side typed mirror and the resolved chain resume).
- `events_resume_cursor` (the snapshot-anchored `MempoolEvents` resume position on `MempoolSnapshotResponse`).
- `contract_revision` / `CONTRACT_REVISION` (the monotonic in-place-revision marker on `ops.ServerInfo`).

See [Wallet data plane §Chain-Event Subscription](../architecture/wallet-data-plane.md#chain-event-subscription) and [Public interfaces §Cursor Conventions](../architecture/public-interfaces.md#cursor-conventions).
