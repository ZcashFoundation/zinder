# ADR-0012: Ingest owns the one live mempool, and readers proxy it

Status: Accepted

## Context

The mempool differs from chain state in three ways that shape where it lives:

- The live set changes with network behavior, not with block commits. Snapshot reads and conflict checks must answer in microseconds, and the set can be rebuilt from the node at any time.
- Readers open the RocksDB stores as secondaries ([ADR-0003](0003-epoch-bound-reads-and-storage-roles.md)). An in-process mempool index in the writer is invisible to them without a control-plane call.
- Wallets that reconnect after a disconnect need history: whether a transaction they sent was mined or evicted. Different events answer that question over different time scales.

The lightwalletd `GetMempoolStream` contract also requires the stream to close when the chain tip changes, so the compatibility runtime needs mempool events and tip changes from the same writer.

## Decision

1. **Live state is in memory, history is durable.** `zinder-ingest` holds the live mempool index in process memory. Every `Added`, `Invalidated`, and `Mined` event is committed to the canonical store before any consumer sees it. On restart, ingest rebuilds the index by replaying retained events, then reconciles it with a fresh snapshot from the node.
2. **One mempool per deployment.** Native query and lightwalletd compatibility read the mempool through the writer's `IngestControl` endpoint, which already serves writer status and chain events. No reader opens its own mempool connection to the node. The compatibility runtime closes `GetMempoolStream` on each tip change, using the writer's chain events.
3. **Only a certified snapshot serves.** Ingest serves a node snapshot only once it is complete and certified at the canonical tip. When the tip moves, ingest keeps serving the previous snapshot while the next one hydrates. A read that would mix tips waits up to five seconds for the replacement, then fails with `CHAIN_EPOCH_PIN_UNAVAILABLE`.
4. **Mempool size is admitted, never truncated.** Ingest admits at most `ingest.mempool.max_transaction_count` transactions and `ingest.mempool.max_total_raw_transaction_bytes` raw bytes. The defaults, 8,000 transactions and 80,000,000 bytes, match Zebra's default mempool cost limit. A node mempool above either limit withdraws the snapshot, and mempool reads stay unavailable until it fits or the operator raises the limit.
5. **Retention has two tiers.** `Mined` events are kept for at least 60 minutes, long enough for a mobile wallet to reconnect and see its transaction mined. `Invalidated` events are kept for at least 24 hours, long enough for a user who returns the next day to learn that a transaction was evicted. An `Added` event is kept as long as its transaction is still live. A cursor below the retained floor fails with `MEMPOOL_EVENT_CURSOR_EXPIRED`, and the consumer takes a new snapshot.
6. **Mempool conditions do not create readiness causes.** Source, hydration, and retention problems are reported through metrics. Ingest reports `syncing` until a snapshot is first certified, and again when the certified snapshot has trailed the canonical tip for 30 seconds. A normal block arrival changes no readiness state.

[Chain ingestion](../architecture/chain-ingestion.md) describes mempool ownership. [Configuration](../reference/configuration.md) lists the limit and retention keys.

## Consequences

- `IngestControl` is the one privileged path for writer status, chain events, and the mempool. Its bearer token ([ADR-0011](0011-ingest-control-transport-security.md)) covers every mempool method.
- A wallet sees the same pending transactions whichever reader answers it.
- The node serves one mempool subscription, however many readers run.
- A process crash loses only the live index, which restart rebuilds.
- Retention windows are minimum residence times. A long-lived transaction keeps later events past their own windows, so retained row count is a capacity signal. Raising the windows grows the event history on disk.
- Raising the admission limits admits more memory and event-log work. Lowering them makes mempool reads unavailable during large bursts.
- Wallet reads may wait briefly at block boundaries while the next snapshot hydrates.

## Alternatives rejected

- **Each reader runs its own mempool source.** Two indexes diverge under load, so pending-transaction visibility would depend on which reader answered. It also multiplies the node's mempool subscriptions.
- **Persist the live index in RocksDB.** It cannot match in-memory latency, and every index change would cost a write on a hot path that needs no durability, because retained events rebuild it.
- **One retention window for every event.** `Mined` and `Invalidated` events answer different questions. One window either under-serves eviction detection or bloats the event history.
- **Truncate a mempool above the limits.** A truncated mempool silently reports transactions as absent.
- **Withdraw the mempool at each block until the next snapshot is certified.** Every block would briefly make ingest and native query unready.
- **Let lightwalletd clients cope with streams that do not close on tip changes.** The lightwalletd contract requires the close, even though some clients tolerate its absence.
