# ADR-0034: Exclusive index-build stages and block-local spend replay

| Field | Value |
| ----- | ----- |
| Status | Accepted |
| Product | Zinder |
| Domain | Initial sync scheduling, materialized-view replay, RocksDB resource and volume space ownership |
| Related | [ADR-0015](0015-phase-driven-ingest.md), [ADR-0017](0017-materialized-view-consumer-and-key-codec.md), [ADR-0020](0020-bounded-rocksdb-resource-budget.md), [ADR-0029](0029-durable-transparent-outpoint-spend-projection.md) |

## Context

Mainnet canary measurements showed that canonical bulk catchup completed quickly, but materialized-view replay then projected a multi-day drain. The process was not CPU- or memory-pressure-bound. In transparent-input-dense history, almost all context-build time was spent resolving hundreds of spend facts per block through random point reads. The variable-row limit correctly kept writes bounded, but it shortened nominal 1,000-block batches to tens of blocks without reducing the random-read cost per spend.

At the same time, historical backfills began as soon as canonical entered `FollowingTip`. They serialized their materialized-view writes through the same process lock and generated their own compaction work while readiness-critical replay was still millions of blocks behind. RocksDB reported substantial compaction amplification and write stalls despite low process memory pressure.

A materialized-view rebuild must not depend on canonical rows that can disappear. Canonical storage keeps block-local facts only, each block's replay row and the transaction-location index, and deletes no best-chain block facts. It has no spend-keyed family, so replay must derive spend facts from those rows.

Canary traces also showed the adaptive source controller hard-capped at 16 blocks even while measured responses averaged far below its byte target; the fixed control's proven 64-block ceiling sustained higher throughput without splits.

## Decision

Initial indexing has three exclusive storage-budget owners, in priority order:

1. Canonical construction runs while the ingest phase is `BulkCatchup`; materialized-view replay and rebuildable historical work are paused.
2. Materialized-view replay runs after canonical enters `FollowingTip` and until the `transparent_outpoint_spend` Explorer view covers the canonical visible tip; historical backfills and verifiers remain closed.
3. Historical backfills and verifiers may start bounded batches only while canonical is following tip and materialized-view replay is caught up. If replay falls behind again, the gate closes before another historical batch begins.

Startup observes and publishes the canonical phase before it admits synchronous materialized-view handoff work. Only a positively classified `FollowingTip` phase may replay; `BulkCatchup`, `AwaitingUpstream`, a failed tip observation, and the unclassified state fail closed. A restart during initial sync therefore resumes canonical work without first spending the bounded startup replay window on materialized-view debt.

This is an internal scheduling contract, not public readiness state. `HistoricalWorkGate` combines the canonical phase with the materialized-view tailer's caught-up signal. Metrics expose the two inputs and the resulting gate state.

Materialized-view replay derives transparent spend facts from block-local canonical rows. For each hydrated height range it reads the blocks' replay rows and resolves every non-coinbase input's previous output in this order:

1. Outputs produced inside the range.
2. The replay host's in-memory prevout cache, which holds the unspent outputs that earlier ranges produced.
3. The transaction-location index and the producing block's replay row, with reads grouped by producing height.

An input whose previous output cannot be resolved is a replay error, not an unresolved fact. Building or recovering a materialized-view store requires canonical history that is complete from height 1, so every previous output is retained.

The prevout cache maps an outpoint to the output's value, address script hash, and producing height and hash. After a range resolves, the cache adds the outputs the range produced and removes the outputs it spent. A range must start at the next height and name the last applied block as its parent; otherwise the cache empties before the lookup. A hit is therefore an output produced on the current block's own ancestry and not spent since. A reorg, a retried page, or a skipped height empties the cache instead of answering from a replaced chain.

Replay and the transparent-address ranking build each keep one cache, because they walk different heights. Each cache holds at most 3,500,000 outputs, about 375 MB, and drops the oldest producing heights first when it is full. A dropped output falls back to the canonical read, so the bound changes only the read count, never a result. Both caches empty while replay is paused, which returns their memory under memory pressure. The replay cache also empties once replay reaches the canonical fence, where blocks arrive one at a time, and the ranking cache empties once no ranking build is pending. The cache is process memory only. A restart starts it empty, and spends of outputs produced before the restart use the canonical read until those outputs are spent.

Explorer view replay also pauses while the volume holding the canonical store has less available space than `ingest.explorer_views_min_free_bytes`, 10 GiB by default. The view store nests inside the canonical directory, so the views share that volume with canonical storage and often with wallet storage. Views are rebuildable; canonical storage is the expensive store to lose. The guard samples the volume at every replay budget evaluation, the same page boundary at which memory pressure is checked. While it holds, replay and the ranking build stop, the historical work gate stays closed, and the persisted view status reports `paused`. Startup cursor seeding, which writes view rows before replay starts, waits behind the same guard. Replay resumes once at least a tenth more than the floor is available. The value `0` disables the guard. Canonical ingest never consults it.

Replay dispatch is serial and ordered: each page of a transition is hydrated, then applied together with its cursor advance in one atomic materialized-view write before the next page is read. The batch size follows the memory budget state, shrinking under memory pressure and pausing at the pause threshold.

A canonical live append or replacement writes only its own block-local rows, epoch, event, and control record, so settled-tip commits stay independent of historical backlog. Following never returns to construction; after a burst it appends one block per commit and reports `syncing` while lag exceeds `ingest.follow.lag_threshold_blocks`.

The default source-segment hard ceiling is 64 blocks, matching the measured fixed control. The response-byte target, p95 density estimate, network-upgrade reset, and split feedback remain the actual dense-era safeguards, so the controller may shrink below 64 but can now exploit lighter eras.

Materialized-view secondary catchup retries a narrowly classified missing-`.sst` I/O race up to three total attempts while holding its catchup barrier. This covers a primary compaction replacing a file between manifest replay and the next secondary read or schema validation; schema mismatches, corruption, and other I/O failures still surface immediately.

The materialized-view writer defaults reserve a 512 MiB aggregate memtable budget, four 16 MiB buffers per hot column family, a 256 MiB WAL ceiling, and a 256 MiB block cache. The shared write-buffer manager remains the hard memory bound. Existing RocksDB ticker and property metrics measure compaction bytes, stalls, cache use, WAL size, and memtable use.

Neither canonical nor materialized-view layouts migrate in place. A store whose persisted schema differs from the binary's fails admission, and a deployment that changes either schema rebuilds that store from genesis.

## Consequences

- End-to-end readiness work no longer competes with optional historical backfills.
- A canonical commit writes only the appended or replaced blocks' rows, so its cost does not grow with historical backlog.
- Light eras can use the fixed control's proven 64-block request ceiling; dense eras remain byte-adaptive.
- Transient materialized-view secondary compaction races recover inside one catchup operation without masking persistent failures.
- In sequential replay from genesis, the prevout cache resolves every spend of an output produced earlier in the replay without a canonical read. On a 4,000-block fixture with 40 transparent spends per block, it removed all 34,903 producing-block reads and cut hydration from about 40 seconds to about 6 seconds in a debug build on local storage. The canonical read remains for outputs the cache evicted or never saw, such as outputs produced before a restart. In transparent-dense history each of those still costs one transaction-location read per distinct parent transaction and one replay-row read per distinct producing height.
- The prevout cache adds up to about 375 MB per cache to resident memory while replay or the ranking build walks history, and releases it at the tip or under memory pressure.
- Explorer views cannot consume the last `ingest.explorer_views_min_free_bytes` of the shared volume. A volume at that floor holds the views paused until an operator frees or adds space; canonical ingest and wallet serving keep running.
- A materialized-view rebuild needs only genesis-complete canonical history, which canonical storage never deletes. A canonical store bootstrapped from a checkpoint cannot host materialized views.
- Replay writes remain ordered, with one page in flight at a time.
- The larger materialized-view write envelope trades bounded resident memory for fewer flushes and write stalls; operators validate it through existing memory and RocksDB metrics.
- Incompatible volumes cannot be reused by this binary. Rollback to a different layout likewise requires the matching checkpoint or a separate volume.

## Rejected alternatives

- Raising replay block count does not remove the per-outpoint random-read cost and violates the variable-row memory bound in dense history.
- Increasing point-read concurrency further multiplies random I/O.
- Starting backfills at `FollowingTip` treats canonical freshness as materialized-view readiness and repeats the measured compaction contention.
- A durable outpoint-keyed family in the materialized-view store would survive restarts, but adding a family requires a fresh view store under [ADR-0028](0028-materialized-view-schema-versioning.md), so every existing deployment would rebuild its views from genesis. It also could not serve the ranking build, which walks history behind the replay cursor.
- An outpoint index in canonical storage would change the canonical layout and require a canonical rebuild for a view-only cost.
- Reading the wallet store's outpoint families would make Explorer views depend on a store that `zinder-projector` owns, that may not exist in an Explorer deployment, and that can lag or be rebuilt independently.

## Revision history

- 2026-11-14: Added the prevout cache as the second step of spend resolution, with its continuity rule, memory bound, and release points. Added the volume free-space guard for Explorer view replay and `ingest.explorer_views_min_free_bytes`.
