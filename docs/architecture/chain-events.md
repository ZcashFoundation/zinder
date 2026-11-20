# Chain events

This page describes the chain event stream that `WalletQuery.ChainEvents`
serves: what each event says, how a client starts and resumes a stream, what
happens on a reorg, how long events are kept, and which errors a client must
handle. It is for developers who build a wallet, indexer, or service that
reacts to chain changes.

Each time `zinder-ingest` changes the canonical chain, it records one chain
event in the same atomic write as the blocks. The event names the block range
that was committed and, after a reorg, the range that was replaced. A client
reads events in order and keeps an opaque cursor. After a disconnect, it sends
the cursor back to recover changes since that position. A client fetches block
data through the other `WalletQuery` methods. See [Wallet data plane](wallet-data-plane.md).

## Event contents

Each message on the stream is a `ChainEventEnvelope`:

| Field | Meaning |
| --- | --- |
| `cursor` | Opaque bytes. Send them back to resume strictly after this event. |
| `event_sequence` | Position of the event in the stream. It increases with every event; a filtered stream skips numbers. |
| `chain_view` | The chain state after the event. `chain_view.chain_epoch` holds the visible tip, the settled tip, and the note commitment tree sizes. |
| `chain_committed` or `chain_reorged` | The event itself. |

The two event types:

- **`ChainCommitted`** reports the block range that joined the selected
  family. On the Visible stream, the first publication covers every stored
  block. Each subsequent append reports the new block.
- **`ChainReorged`** carries a `reverted` range and a `committed` range that
  start at the same height. The blocks in `reverted` are no longer on the
  chain. The blocks in `committed` replace them and may extend past them.

Every range is inclusive and non-empty. There is no rollback event without a
replacement range.

A client can discard any undo state it holds for blocks at or below
`chain_view.chain_epoch.settled_tip.height`, because a reorg never replaces
them; see
[Chain ingestion](chain-ingestion.md#reorgs-and-the-reorg-window).

Zinder provides the visible tip, the settled tip at the reorg-window depth,
and a `ChainReorged` event whenever blocks above the settled tip are replaced.
How many confirmations a payment needs before your application acts on it is
your application's policy, at any depth between those two tips.

## Start a stream

`ChainEventsRequest.start` is required. A request without it fails with
`INVALID_ARGUMENT`. It takes one of three positions:

| Start | Delivers |
| --- | --- |
| `after_cursor` | Changes since the saved position. Use it to resume. |
| `earliest_retained` | Every event still retained, from the oldest. Use it to build state from scratch. |
| `live_tail` | Only events committed after the subscription opens. |

`ChainEventsRequest.family` selects which events the stream carries:

- `CHAIN_EVENT_STREAM_FAMILY_VISIBLE`, the default, carries every commit and
  every reorg. A client on this family must handle reorgs.
- `CHAIN_EVENT_STREAM_FAMILY_SETTLED` delivers one `ChainCommitted` whenever
  the settled tip advances. Its range starts one block above the previous
  settled tip and ends at the new settled tip, inclusive. The first
  publication starts at the first stored block. For example, if the settled
  tip moves from 40 to 42 while the visible tip reaches 45, the event covers
  blocks 41 and 42. Its cursor points to block 42. Transitions that leave the
  settled tip in place produce no Settled event. This family never delivers
  a reorg.

A cursor records its family. With `after_cursor`, the cursor's family applies.
The request's `family` must be left at its default or match the cursor, or the
request fails with `INVALID_ARGUMENT`.

For Settled subscriptions, use a server advertising `contract_revision` 12 or
later. It sends `zinder-chain-event-family: settled` in the initial gRPC
metadata whenever the resolved family is Settled, including on cursor resume.
Earlier servers can stay silent as blocks settle. Rust SDK consumers also
need a client that supports revision 12. Earlier clients reject a committed
range that ends below the visible tip.

## Resume with a cursor

Store a cursor only after your application has durably applied its event.
Then a crash replays the event instead of skipping it. Resume by sending the
stored bytes unchanged in `after_cursor`.

A stream never ends on its own. When the server shuts down, it ends open
streams with `UNAVAILABLE` and `SERVICE_NOT_READY`, and you reconnect with the
last cursor you stored.

The server resumes as follows:

- When the next unseen event is retained, the stream replays from that
  sequence in the cursor's family. Visible streams include retained reorgs.
- When events are missing and a visible cursor's block has been replaced,
  the server can send one synthesized `ChainReorged`. Its `reverted` range
  starts just above a verified common block and ends at the cursor's tip.
  Its `committed` range runs from the same height to the current visible tip.
  The replacement can have settled in the meantime, so
  `committed.chain_epoch.settled_tip` can advance past the common block.
  Apply the replacement before discarding undo state through the new settled tip.
- When events are missing and the cursor still points to the canonical chain,
  or the server cannot establish a common block and a valid replacement range,
  the request fails with `CHAIN_EVENT_CURSOR_EXPIRED`.

A cursor carries up to 32 recorded blocks, most recent first and spaced further
apart going back. If the matching block is below the cursor epoch's settled
tip, the server verifies the settled block against the current chain and starts
the replacement above it. Each delivered epoch advances, and settlement never
moves backward or changes hash at the same height.

The synthesized event carries the current event sequence and a cursor anchored
at the current visible tip. It covers the retained changes through that tip.
The stream continues only with later events. If you disconnect before applying
the replacement, reconnect with your saved cursor and the server computes
recovery against the tip it then serves.

For durable recovery across pruning, use a server advertising `contract_revision`
10 or later. On revisions 8 and 9, the Rust SDK replays retained events
normally. If the server synthesizes a reorg after pruning, the SDK ends the
stream with `ChainEventCursorExpired` before delivering the replacement or
its cursor. Rebuild your state and start a new stream from `earliest_retained`.
If the revision lookup times out, the SDK returns `ServiceUnavailable`.
Reconnect with your last saved cursor.

A `SETTLED` cursor whose block is no longer on the chain is always expired,
because a settled stream never carries a reorg.

## Filter by transparent address

`ChainEventsRequest.address_filter` takes up to 256 transparent addresses for
the request's network. It is a hint to skip quiet blocks, not a per-address
feed:

- With an empty filter, the stream delivers every event.
- With addresses, the stream delivers a `ChainCommitted` only when at least one
  address has a transaction in the committed range.
- A `ChainReorged` always passes, because a client must invalidate cached data
  after any reorg.

While your subscription is open, the server scans past every examined event,
including a full page of quiet blocks. Delivered events keep their original
cursors, so you can reconnect with a different filter. After a hint, the client still reads the address or block data it
needs.

## Retention

Ingest keeps chain events for `retention.chain_event_retention_hours`, 168
hours (seven days) by default, and prunes older ones without renumbering the
rest. `0` disables pruning. Events that a running projector build has leased
are kept. See [Configuration](../reference/configuration.md).

If the server cannot recover a cursor after pruning, the client gets
`CHAIN_EVENT_CURSOR_EXPIRED`. It must rebuild its state from Zinder's block and
address queries and then open a new stream with `earliest_retained` or
`live_tail`.

A slow client never delays a commit. The stream reads retained history, so a
slow client only falls behind, and its cursor can expire.

## Errors

| Status and reason | Cause | What the client does |
| --- | --- | --- |
| `INVALID_ARGUMENT`, `CHAIN_EVENT_CURSOR_INVALID` | The cursor fails authentication, belongs to another network, store, or family, or names a sequence outside the event history | Do not retry with this cursor. Restart from a known position. |
| `FAILED_PRECONDITION`, `CHAIN_EVENT_CURSOR_EXPIRED` | The cursor is older than retention, or its chain cannot be matched to the current chain | Rebuild state, then start with `earliest_retained` or `live_tail`. The error detail names the oldest retained sequence. |
| `FAILED_PRECONDITION`, `REORG_WINDOW_EXCEEDED` | Ingest found a reorg deeper than its window and stopped following | The server ends every open stream with this status. Reconnect after an operator resolves it. |
| `INVALID_ARGUMENT`, `INVALID_ADDRESS` | An address filter entry is invalid, or the filter exceeds 256 addresses | Fix the request. |

The full list of reasons and retry rules is in
[Error vocabulary](../reference/error-vocabulary.md). The cursor format is
described in [ADR-0002](../adrs/0002-serialization-and-byte-order.md), and the
design of the stream in [ADR-0013](../adrs/0013-chain-event-streams.md).
Mempool changes have their own stream, `WalletQuery.MempoolEvents`, with the
same start positions. See
[ADR-0012](../adrs/0012-mempool-topology-and-retention.md).
