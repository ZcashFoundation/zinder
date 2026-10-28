# ADR-0007: Mempool Topology and Retention

| Field | Value |
| ----- | ----- |
| Status | Accepted |
| Product | Zinder |
| Domain | Mempool surface, write-side topology, read-side proxy, retention |
| Related | [Chain events](../architecture/chain-events.md), [ADR-0003](0003-canonical-storage-access-boundary.md), [ADR-0006](0006-ingest-control-transport-security.md), [Wallet data plane](../architecture/wallet-data-plane.md), [Storage backend](../architecture/storage-backend.md), [Chain ingestion](../architecture/chain-ingestion.md), [Service operations](../architecture/service-operations.md), [Public interfaces](../architecture/public-interfaces.md) |

## Context

[ADR-0003](0003-canonical-storage-access-boundary.md) settles the writer / secondary-reader topology for canonical chain state. The mempool surface is structurally similar (writer-owned live state, reader-owned secondary access) but differs in three ways the chain-state design did not have to answer:

- A mempool transaction's lifetime is bounded by network behavior, not by chain commits. Retention is per-variant: a `Mined` event is interesting only until the wallet has resynced past the height; an `Invalidated` event is interesting until the wallet has decided whether to rebroadcast; an `Added` event is only interesting while the transaction is still in the mempool.
- The live `MempoolIndex` cannot be a column-family read. It is in-process state computed from a streaming source, and a secondary RocksDB reader cannot observe it without a control-plane handoff. Chain state has the inverse property: the visible epoch is precisely what the canonical store records.
- The lightwalletd compatibility contract requires `GetMempoolStream` to close on chain-tip change. The native `MempoolEvents` stream does not. The compat shim therefore needs both a mempool-event channel and a chain-tip-change channel from the same writer process.

The decisions to record here are:

1. The split between in-memory `MempoolIndex` (live state) and durable mempool event history (cursor-resumable history) and why the canonical store carries the latter but not the former.
2. The `IngestControlMempoolSurface` topology: secondary readers and the lightwalletd compat shim consume the writer's mempool through the same `IngestControl` endpoint that already serves chain events, instead of opening a second source connection.
3. The two-tier retention windows, their defaults, their metrics-only operational diagnostics, and the exact-tip hydration prerequisite that keeps ingest `syncing` until the mempool and canonical tips agree.
4. The `TxStatus::InMempool(MempoolEntry)` typed shape that carries the hydrated mempool entry, the chain epoch at first observation, the transparent overlay, and the precomputed compact-tx bytes; clients receive the typed status directly rather than parsing error strings.

## Decision

### Mempool live state is in-memory; mempool history is in RocksDB

`MempoolIndex` is owned by `zinder-ingest` as in-process state. Its primary
entries are ordered by `TransactionId`, with hash-indexed transparent overlays
for output and spend lookups. The ordered primary index makes snapshot paging
and source-generation reconciliation use working memory bounded by the page
size rather than cloning and sorting the full mempool. Total reconciliation
work remains proportional to the old and staged entry counts. It is not a
column family. Crashes drop the live index; the next restart rebuilds it by
replaying retained durable events before reconciling with a fresh source
generation.

The canonical store persists durable mempool event history in its `mempool_event` column family. Every typed `Added` / `Invalidated` / `Mined` envelope is committed there before consumers see it. The retention floor is stored with the canonical control records, so cursor expiration is a single durable read rather than a column-family scan.

Source-generation reconciliation holds the live owner's mutation gate while it
walks the old and new ordered indexes in bounded pages. It appends ordered
removals and additions in synchronously committed RocksDB batches with a
bounded event count and a bounded cumulative raw-transaction payload, applying
each batch's contiguous positions to the private index before continuing. The
raw-byte bound covers the variable transaction payload rather than claiming an
exact encoded RocksDB batch size; envelope and derived compact-transaction
overhead remain bounded by the event-count ceiling. A single oversized event is
still admitted as a singleton so reconciliation always makes progress. Reads
remain unavailable until every batch has been applied and the source generation
is certified. A
network-upgrade activation can therefore evict thousands of transactions
without issuing one synchronous write per transaction or constructing one
unbounded write batch, while the durable event stream still retains one typed
envelope per transaction and readers never observe a partially reconciled set.

Snapshot hydration is also backpressured. Streaming and polling sources retain
at most one hydration-concurrency window beyond the bounded event channel,
rather than collecting every raw transaction before delivery. Events emitted
before `InitialSnapshotComplete` are provisional: the live owner stages them
privately and discards the generation if hydration fails or its source tip
changes. Polling verifies the source tip between bounded-size hydration batches;
the streaming source verifies it before publishing the completion marker.
Normal source-tip movement emits a typed, non-durable `SourceTipChanged`
control event: the owner withdraws exact-tip certification, discards any
provisional generation, and immediately opens a replacement generation. It is
not a source failure or durable-state rebuild and therefore adds no source-error
telemetry or recovery backoff. Transport, monitor, hydration, admission, and
protocol failures continue through the bounded rebuild path. When the
replacement snapshot completes before canonical ingest reaches the same tip,
the owner keeps it private, continues staging source transitions, and certifies
it as soon as the canonical tip catches up.

Every polling and streaming generation also has source-admission ceilings for
distinct transaction count and cumulative raw transaction bytes. The defaults
are 8,000 transactions and 80,000,000 raw bytes, matching Zebra's default
80,000,000 transaction-cost limit and its 10,000 minimum per-transaction cost.
Limits are inclusive and apply to the complete admitted set across the initial
snapshot and subsequent live deltas. Duplicate `Added` observations consume no
additional capacity, and `Invalidated` or `Mined` observations release both
entry and raw-byte capacity. Polling emits removals before additions from the
same fenced snapshot so a capacity-neutral replacement cannot fail admission
merely because the old entries have not been released yet.

An upstream mempool above either local limit is not truncated. The source
withdraws the generation, readers remain unavailable, and ingest retries until
the upstream set fits or the operator raises the corresponding
`[ingest.mempool]` limit. Provisional entries emitted before a cumulative-byte
failure remain private and are discarded with the uncertified generation.
Raising the limits admits proportionally more staged and live-index memory and
can increase durable event-log and reconciliation work; lowering them provides
a fail-closed resource ceiling at the cost of availability during unusually
large mempool bursts.

The split exists because the live index needs to answer transparent lookups in microseconds without a RocksDB round-trip, and because it can be reconstructed deterministically by replaying retained durable events before reconciling with the source at startup. The event log needs to answer cursor resume and rebroadcast detection with millisecond latency over hours of history, which RocksDB does well and an in-process ring buffer does not.

### Compatibility and native adapters reach the writer through `IngestControl`, not a second source connection

`zinder-compat-lightwalletd` and any native adapter that embeds `zinder-query` are secondary RocksDB readers per ADR-0003. The mempool live state is not in RocksDB, so the secondary readers cannot observe it directly. Two options were considered:

1. Each reader opens its own `MempoolSource` connection to the upstream node and maintains its own `MempoolIndex`.
2. Each reader proxies `MempoolSnapshot` and `MempoolEvents` through the writer's `IngestControl` gRPC endpoint, which also serves `WriterStatus` and the narrow `VisibleChainEvents` stream.

Zinder picks option 2. The decisive constraints are:

- Two `MempoolIndex` instances diverge under load. A wallet that hits compat for `GetMempoolStream` and `WalletQuery` for `transaction_by_id` must see a single mempool, not a per-process one. Source-of-truth duplication would make pending-transaction visibility depend on which read service handled the request.
- Operators already wire one privileged path from secondary readers to the writer (writer status). Adding mempool to that path costs no new transport, no new auth surface, no new firewall rules. ADR-0006's bearer token already covers `MempoolSnapshot` and `MempoolEvents`.
- One source connection upstream uses one mempool-stream slot on Zebra. A reader-per-process design multiplies that load by the number of read services.

The concrete bindings are:

- `IngestControlMempoolSurface` (in `services/zinder-compat-lightwalletd`) implements `MempoolSurface` over the `IngestControl.MempoolSnapshot` / `MempoolEvents` proxy methods.
- The native release composition owns one `AdmittedIngestControl`. It validates
  the writer identity and required methods once before binding, then gives the
  query, gRPC handlers, pair publisher, and readiness probe clones of the same
  authenticated channel. `WalletQueryGrpcAdapter` owns no provider
  configuration or connection state.
- `WalletQueryGrpcAdapter::with_ingest_control_proxy` remains only as a
  temporary generic-query test seam. No release composition calls it. It is
  deleted with the generic primary-store query after the deferred consumers
  and tests move to admitted composition; it is not a native release binding.
- `spawn_ingest_control_tip_change_publisher` runs in the compat process and replays the retained `IngestControl.VisibleChainEvents` window before following live events. `MempoolSnapshotResponse.chain_view.chain_epoch.chain_epoch_id` fences the snapshot page, and `TipChangeWatcher::await_tip_change_after` resolves immediately when it has already retained a newer chain-event sequence. Epoch ids and chain-event sequences share one monotonic identity space. `LightwalletdGrpcAdapter` races that signal against the mempool stream and closes the gRPC stream on each best-block change. Replaying retained chain events prevents a writer or network interruption from hiding a change. This restores the lightwalletd Go contract that Zodl's `sync` loop relies on without making the compat process open its own upstream node connection.

### Retention is two-tier with separately tunable windows

`MempoolEventRetentionConfig` carries:

- `mined_window`: how long a `Mined` envelope stays before pruning. Default: 60 minutes.
- `invalidated_window`: how long an `Invalidated` envelope stays. Default: 24 hours.
- An `Added` window is derived as `min(mined_window, invalidated_window)` because once an `Added` event ages past the shorter window, neither of its possible terminal events would still be retained, so the `Added` is no longer reachable through cursor resume.

The defaults are calibrated for two distinct consumer expectations:

- A wallet that comes back online after a brief disconnect (minutes) must be able to resume the stream and see the `Mined` event for the transaction it broadcast. 60 minutes covers normal mobile-wallet reconnect behavior plus margin.
- A wallet that wants to detect "my submitted transaction was rejected by the network" needs `Invalidated` events to outlive a typical user-driven retry cadence. 24 hours covers the case where a user submits, closes the app, returns the next morning, and expects to see whether the transaction is still pending or has been evicted.

`run_mempool_retention` runs the pruner. Retention windows are minimum
residence periods, not maximum row ages: pruning removes only a contiguous
expired prefix, retains the current head, and cannot cross the last `Added`
event for a transaction that is still active. A long-lived transaction or an
unexpired floor event can therefore retain later events past their individual
windows. This is required for restart reconstruction and cursor continuity,
and it makes retained-row count and pruning duration important capacity
signals.

The canonical store advances retention through resumable process-local steps,
bounded by both event rows and encoded bytes. A step scans forward from the
durable floor, tracks unmatched `Added` anchors, and deletes only an inspected
prefix that is older than every active anchor while retaining the captured
head. Floor advancement and row deletion remain one atomic synced batch.
Restart discards only scan progress and safely resumes from the durable floor.
Budget exhaustion schedules another step immediately, but the writer yields a
canonical source turn between maintenance steps. The byte target permits one
row to exceed it so an individually large admitted envelope cannot prevent
progress.

Retention does not manufacture a cursor-at-risk readiness state from the age
of the oldest row. Zinder has no consumer cursor registration or lease, so row
age is not evidence that an active consumer is about to expire. The retention
loop instead reports retained rows, floor sequence and age, per-kind prune
counts, bounded work, and typed step outcomes through metrics. Source and
hydration failures remain source-owner diagnostics. The canonical follower is
the only publisher of ingest readiness and treats the mempool hydration gate
as a hard prerequisite. The gate carries the source tip certified by the
hydrated generation, and readiness requires exact equality with the canonical
fence, avoiding both cross-tip admission and last-writer-wins races between
independent background tasks.

`snapshot_age_millis` measures time since the current source generation's
snapshot was certified, including for a certified empty mempool. It is not the
age of the most recent transaction mutation. The matching histogram samples
that age when a snapshot page is served. This in-place semantic correction
raises `zinder_proto::CONTRACT_REVISION` to 4; native clients that expose this
field require revision 4 so they cannot interpret the earlier mutation-age
implementation as generation freshness.

Cursor expiration on read is a hard stop, not a warning. A consumer whose `from_cursor` is below `oldest_retained_mempool_event_sequence` receives `MempoolEventCursorExpired` carrying the current floor, mapped to gRPC `FailedPrecondition` with a `PreconditionFailure` detail. The consumer must resnapshot, not retry the same cursor.

### `TxStatus::InMempool` carries the hydrated entry

```rust
pub enum TxStatus {
    Mined { /* ... */ },
    InMempool(MempoolEntry),
    NotFound,
}
```

The `MempoolEntry` carries the hydrated transaction, the chain epoch at first observation, the transparent overlay, and the precomputed compact-tx bytes. The typed enum is the contract; `transaction_by_id` reports mempool presence by returning `InMempool`, not by an error string consumers parse. Typed transaction status is a product-correctness requirement.

`NotFound` is a proven absence. An unpinned lookup pairs a canonical miss with a mempool miss at the same epoch id, and the writer's `MempoolTransaction` miss names that epoch; [ADR-0042](0042-proven-transaction-absence.md) owns the absence contract.

## Consequences

### Operational

- One privileged path (`IngestControl`) serves writer status, visible chain events, mempool snapshots, and mempool events. Bearer-token auth from ADR-0006 covers every method.
- Operators tune mempool retention separately from chain-event retention. Default `mined_window = 60 minutes` and `invalidated_window = 24 hours` are shipped values; production deployments with longer wallet reconnect SLAs raise them and accept the larger `mempool_event` column-family footprint.
- Mempool lifecycle, source, hydration, and retention conditions are metrics-only diagnostics and add no readiness causes. Ingest remains `syncing` until a certified hydrated generation matches the exact canonical tip.

## Alternatives Considered

### Each reader runs its own `MempoolSource`

Rejected. Two `MempoolIndex` instances diverge under load and reintroduce the multi-cache class of bug Zinder is structurally avoiding. It also multiplies upstream-node mempool-stream slot usage by the number of read processes.

### One global retention window

Rejected. `Mined` and `Invalidated` events have different shelf lives because they answer different consumer questions. A single window forces the operator to choose between under-serving rebroadcast detection (short window) and bloating the column family (long window).

### Persist the live `MempoolIndex` as a column family

Rejected. The live index needs microsecond-latency transparent-overlay lookups. RocksDB cannot match that. Persisting also means every transparent index change causes a commit, multiplying write amplification on a hot path that has no recovery requirement (retained durable events rebuild it before source reconciliation).

### Skip `TipChangeWatcher`; let consumers handle stream-end semantics

Rejected. The Android SDK tolerates arbitrary stream end and reconnects, but the lightwalletd Go contract specifically requires close-on-tip-change. Diverging from the published contract is a compatibility regression even if a particular consumer happens to tolerate it.

## Out of Scope

- Native gRPC TLS for `IngestControl.MempoolSnapshot` / `MempoolEvents`. ADR-0006 covers transport security; this ADR does not modify it.
- A second mempool source backend type. The two existing backends (`JsonRpcMempoolSource` polling, `ZebraIndexerMempoolSource` streaming) are the supported set; new backends would be a separate ADR.
- Shielded-by-address mempool queries. The privacy boundary forbids them; transparent-only is intentional and already encoded in [Wallet data plane](../architecture/wallet-data-plane.md).
- Mempool-broadcast endpoints beyond the existing `SendTransaction` path. Broadcast remains a `TransactionBroadcaster` boundary owned by `zinder-source`.
