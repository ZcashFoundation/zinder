# ADR-0009: One ingest process builds the store, then follows the tip

Status: Accepted

## Context

Canonical ingest has two workloads. A store far behind the node needs bounded, pipelined construction that uses every core and as much I/O as the host allows. A store near the tip needs serial appends and reorg replacements with low latency. If an operator or deployment script chose between them, Zinder would have two startup contracts, and a cold store could run the near-tip path for days.

The writer already holds everything the choice needs: whether a published store exists, its committed fence, the node's tip, and the node's health.

Construction must also stay inside the host's memory and I/O. Zcash history is not uniform: pre-Sapling, Sapling, and NU5-era blocks differ so much in size and transaction mix that a batch sized in blocks crosses a byte or memory limit in one era and idles the host in another. Most per-block work, such as parsing, validation, and building compact blocks, depends only on the block's own bytes. Commitment-tree positions and parent links depend on the block before.

After construction, three kinds of work compete for the same RocksDB volume: canonical following, materialized-view replay for the Explorer, and optional historical backfills. On a Mainnet canary, construction finished quickly, but view replay then projected a multi-day drain dominated by random point reads that resolve transparent spends. Backfills started at the same time added compaction stalls while the process showed little memory pressure.

Fresh construction has a three-hour budget to READY. It began as a row of a lifecycle-targets table in the earlier ADR-0035, now [ADR-0004](0004-storage-and-deployment-topologies.md), which set fresh canonical construction at a two-hour target and a three-hour hard gate, with a separate target for a wallet-ready deployment, and gave no derivation. Since wallet-serving construction began storing checkpoints before READY, the budget has come to bound when a fresh wallet-serving store can answer tree-state reads from stored checkpoints.

## Decision

1. **The writer chooses its workload from the store lifecycle.** At startup `zinder-ingest` reopens a ready store and follows it; installs a published store left in its staging path and follows it; or constructs a fresh store, publishes it, and then follows it. No configuration names the workload. Following never returns to construction. After downtime the writer follows from its persisted fence.
2. **A restart during construction does not resume.** Unpublished construction staging is discarded and construction starts again on the next process start; only a published store is reused ([#303](https://github.com/ZcashFoundation/zinder/issues/303)). Multi-node availability within one running construction process comes from failover across `[node_fallbacks.N]` ([ADR-0007](0007-node-failure-recovery-and-transport-policy.md)), while fast host replacement uses verified pre-built snapshots rather than partial staging checkpoints.
3. **Phase and readiness are separate dimensions.** The writer reports its phase (`BulkCatchup` while constructing, `FollowingTip` while following) independently from its readiness cause. A writer can be `following_tip` and report `node_unavailable`. The gap-based classifier, including `AwaitingUpstream`, serves only the `zinder-ingest probe` diagnostic and never selects the writer's workload.
4. **Construction is budgeted in the resources that bound it.** Source segments are sized by observed response bytes, not block count. Limits on in-flight bytes and prepared blocks are derived at startup from the container memory budget, the CPU count, and the node's response-size limit. The in-flight request limit has a fixed default of 12. Per-block preparation runs in parallel. Tree positioning and the store load run in height order, so the result is deterministic. Commitment-tree hashing may run on a bounded number of worker threads, but each block reaches the store in height order with the trees, checkpoints, and subtree roots that a serial loader derives. Construction that stores checkpoints beyond the 100-block cadence and build tip spends most of its loader time hashing, so its hashing threads default to the detected logical CPU count, at most 8, the most a full Mainnet build has measured with its memory. An undetectable CPU count gives one thread. Sparse construction defaults to hashing on the loader thread.
5. **Wallet-serving construction stores dense checkpoints by default.** With `wallet-serving` coverage and Explorer views off, `ingest.tree_state_densification.during_construction` defaults to `true`, so a fresh store answers tree-state reads at every constructed height from a stored checkpoint at READY. Construction applies the enabled densification selection: `every-block`, the default, stores one checkpoint at every height, and `commitment-budget` adds a checkpoint when the number of appended commitments since the previous checkpoint reaches the budget. Explorer and `explicit` coverage default to a checkpoint every 100 blocks and at the build tip, and operators can opt in with `during_construction = true` before the first sync. Setting `during_construction = false` or `enabled = false` keeps construction sparse. With parallel tree hashing, the measured Mainnet builds met the three-hour READY budget with 6 hashing workers on 8 physical cores and one thread per core: every-block construction reached READY in 1 h 51 min, and a 256-commitment budget in 1 h 37 min, against 3 h 48 min for every-block construction hashing on the loader thread. Every-block stays the wallet-serving default, and `commitment-budget` is the opt-in for faster construction and smaller checkpoint storage. The smallest writer measured to meet the budget has 4 dedicated cores; 2 cores miss it under either selection. An 8-core build with the default 8 hashing workers was not timed. Live following stores one for every appended block. The background pass fills settled history according to [ADR-0014](0014-consumer-neutral-wallet-data-plane.md). Reads between stored checkpoints reconstruct tree state from stored data.
6. **Initial indexing has exclusive stages, in priority order.** Canonical construction runs alone. Materialized-view replay starts only after the writer enters `FollowingTip`, and runs until the views cover the visible tip. Historical backfills and verifiers start a bounded batch only while the writer follows the tip and replay is caught up. The gate closes again if replay falls behind. Startup publishes the phase before it admits any view work, so a restart during construction goes back to canonical work first.
7. **View replay derives spends from block-local canonical rows.** Canonical storage keeps per-block facts and a transaction-location index, but no spend-keyed index. Replay resolves each transparent input from outputs earlier in the same range, then from a bounded in-memory cache of unspent outputs, and finally from canonical reads. An input it cannot resolve is a replay error, so view stores require canonical history that is complete from height 1.
8. **Views yield the volume to canonical data.** View replay pauses while free space on the canonical volume is below `ingest.explorer_views_min_free_bytes`. Canonical ingest never consults that floor.

### Upstream sync detection

Node health is a property of the node, not of the ingest phase. When `[node.health]` names Zebra's readiness endpoint, the writer polls it. Otherwise it uses the sync observations that the node's JSON-RPC interface reports. A node that is not ready makes the writer report `upstream_not_ready`. A missing, malformed, or unreachable health response never counts as ready. Regtest has no external network tip, so a successful observation there counts as ready.

[Chain ingestion](../architecture/chain-ingestion.md) describes the construction pipeline and the follow loop. [Bulk catch-up resource tuning](../runbooks/bulk-catchup-resource-tuning.md) lists the derived limits and how to override them.

## Evidence

- Decision 5, the three-hour READY budget: judgment without a measurement. The Context records its origin. Its cost is a minimum writer size, which the next entry measures. A measurement in which a supported host size misses the budget under the default selection, or operator demand for wallet-serving stores on writers smaller than 4 cores, reopens it.
- Decision 5, the smallest writer: measured. Fresh wallet-serving Mainnet construction to READY at height 3,593,347 ([#488](https://github.com/ZcashFoundation/zinder/issues/488)):

  | Writer cores | Every block | Commitment budget 256 |
  | ---: | ---: | ---: |
  | 8 (6 hashing workers) | 1 h 50 min 40 s | 1 h 37 min 21 s |
  | 4 | 2 h 34 min 28 s | 2 h 8 min 50 s |
  | 2 | 4 h 27 min 50 s | 3 h 40 min 40 s |

  The 8-core runs are earlier builds with the hashing workers set to 6 and the node pinned to 2 of the host's 8 cores. The 4- and 2-core runs used the derived limits with no overrides, and their writer had dedicated cores on an 8-core host with the node on other cores. A whole 4-core machine that also runs the node and the projector was not measured and would be slower.
- Decision 2, discarding unpublished construction staging on process restart ([#303](https://github.com/ZcashFoundation/zinder/issues/303)): measured and Judgment. Under Canonical Schema `v2`, fresh Mainnet canonical construction completes in ~45 minutes and verified snapshot/edge bootstrap covers rapid host replacement without introducing checkpointed partial-staging integrity state. Availability against single-node transport failures within a run is handled by `[node_fallbacks.N]` construction failover in [ADR-0007](0007-node-failure-recovery-and-transport-policy.md).
- Decision 5, every-block as the default: measured. On a fresh reader, every-block checkpoints served arbitrary historical tree-state reads at 1,519 requests per second against 182 for a 256-commitment budget at 4 clients, and 3,700 against 189 at 16. The budget reached READY 13, 26, and 47 minutes sooner on 8, 4, and 2 writer cores, but did not change which sizes met the budget ([#488](https://github.com/ZcashFoundation/zinder/issues/488)).

## Consequences

- Operators run one ingest command for an empty, recovering, or near-tip store.
- Stopping ingest during a first sync costs all unpublished construction work.
- Dense construction moves per-block frontier cross-checks and checkpoint writes ahead of READY, onto the loader's tree-hashing workers. This avoids a second commitment replay through constructed strides. A wallet-serving host with few idle CPUs reaches READY later; setting `during_construction = false` trades stored reads at READY for an earlier READY and leaves the history to the background pass.
- On fewer than 8 logical CPUs, every-block construction takes longer. A writer with 4 dedicated cores still met the three-hour budget, and one with 2 did not. Ingest warns when such a construction starts, and the operator can choose `commitment-budget` or sparse construction before the first sync. The warning counts logical CPUs, so a host with 4 physical cores and 8 hardware threads gets none. Ingest does not fall back on its own, because a CPU count cannot measure spare capacity and a fallback would silently change wallet checkpoint coverage.
- The default applies only to fresh construction. Upgrading a deployment with a published store reopens it without a rebuild, and a sparse store keeps serving while the background pass densifies it. A restart during an unpublished first sync rebuilds it, as decision 2 states, with the current default.
- Construction limits follow the host. An operator who overrides them takes responsibility for memory use.
- Historical throughput limits and near-tip latency limits are tuned separately.
- Readiness-critical view replay never competes with optional backfills, at the cost of starting backfills later.
- A canonical commit writes only the appended or replaced blocks' rows, so its cost does not grow with historical backlog.
- A store bootstrapped from a checkpoint cannot host materialized views.
- The spend cache adds a bounded amount of resident memory while replay walks history, and releases it at the tip or under memory pressure.
- A full volume pauses the Explorer views until an operator frees space. Canonical ingest and wallet serving keep running.

## Alternatives rejected

- **An operator-selected mode for construction and following.** It encodes the writer's current state in deployment configuration and lets a cold store run the slow path.
- **Fixed block-count batches.** They over- or under-size dense eras. Byte targets adapt to each era without per-height code.
- **Positioning blocks or loading them concurrently.** It breaks deterministic tree sizes and parent-link validation.
- **Resuming construction after a restart ([#303](https://github.com/ZcashFoundation/zinder/issues/303)).** Persisting intermediate construction checkpoints would save partial work across a host reboot or OOM restart, but adds on-disk staging verification and crash-recovery state for a `v2` Mainnet build that now finishes in ~45 minutes and can already recover from upstream node outages via `[node_fallbacks.N]` or bootstrap hosts from verified snapshots. Discarding unpublished staging preserves the single post-validation publication invariant.
- **Starting backfills when the writer reaches the tip.** That treats canonical freshness as view readiness and repeats the measured compaction contention.
- **Larger replay batches.** They do not reduce the per-spend random-read cost, and they break the memory bound in dense history.
- **An outpoint index in canonical storage.** It changes the canonical layout and forces a canonical rebuild for a view-only cost.
- **A durable outpoint index in the view store.** Adding it forces every deployment to rebuild its views from genesis.
- **Reading outpoints from the wallet store.** The Explorer would then depend on a store that another runtime owns, that may not exist, and that can lag or be rebuilt independently.
- **Unbounded read concurrency during replay.** It does not reduce the number of reads and lets one replay step saturate the volume that ingest and wallet serving share.

## Revision history

- 2026-11-21. Decision 5 keeps sparse checkpoints as the fresh construction default and allows operators to opt into dense writes before READY.
- 2026-11-22. Decision 5 applies the optional commitment budget during fresh construction.
- 2026-11-22. Decision 4 allows commitment-tree hashing on bounded worker threads while blocks still reach the store in height order.
- 2026-11-23. Decision 5 makes dense construction the wallet-serving default on the measured Mainnet READY result, and decision 4 defaults tree hashing to the CPU count when construction stores checkpoints beyond the cadence and build tip.
- 2026-11-28. Reaffirmed Decision 2 (discarding partial unpublished staging on process restart given ~45-minute `v2` Mainnet builds, verified snapshot bootstrap, and in-run `[node_fallbacks.N]` construction failover in ADR-0007) and closed [#303](https://github.com/ZcashFoundation/zinder/issues/303).
- 2026-11-23. The Context records the origin of the three-hour READY budget. Decision 5 keeps every-block as the wallet-serving default and states the smallest writer measured to meet the budget, with the limits of that measurement.
