# ADR-0035: Canonical storage and deployment topology

| Field | Value |
| --- | --- |
| Status | Accepted |
| Product | Zinder |
| Domain | Canonical storage, wallet projection, serving admission, deployment topology |
| Related | [Canonical and materialized-view architecture](../architecture/canonical-materialized-view-architecture.md), [Service boundaries](../architecture/service-boundaries.md), [Storage backend](../architecture/storage-backend.md) |

## Context

Canonical chain ingestion and consumer-shaped queries have different storage
workloads. Canonical construction is an ordered, block-local write stream.
Wallet serving needs cross-block address history, unspent outputs, spends, and
bounded reorg undo. Explorer products need independently selectable aggregates.
Putting all three workloads in one foreground commit makes the canonical writer
perform historical reads and maintain indexes that are not chain truth.

The runtime also needs an explicit ownership model. A storage engine name does
not define who may write, how readers prove freshness, how a projection binds
to its canonical source, or how backup and recovery preserve a coherent pair.

## Decision

Zinder separates durable ownership into canonical storage and projection
storage.

1. `zinder-ingest` is the only canonical writer. It persists block-local
   `CanonicalBlockFacts`, direct canonical indexes, chain epochs, retained chain
   events, and control records. Canonical commits do not read wallet projection
   state.
2. `zinder-projector` is the only wallet-store writer. It constructs and follows
   wallet state from authenticated canonical event fences.
3. Reader services open process-owned secondaries. A response that combines
   canonical and wallet state uses one immutable `WalletServingReadPair`
   admitted at an exact fence.
4. Explorer aggregates use independently versioned materialized views and do
   not change the wallet or canonical contracts.

### Canonical record

`CanonicalBlockFacts` is the semantic per-block record shared by construction,
following, replay, projection, and storage diagnostics. The aggregate is
backend-neutral. Its reference digest and replay envelope are separately
versioned from every physical storage layout.

Raw block and transaction bytes are retention-policy artifacts. Typed digests
bind retained bytes to semantic facts, but the raw bytes are not embedded in
the canonical facts digest. Direct indexes are added only for canonical queries
that must not decode every replay envelope.

### Store identity

Each persisted store records its domain identity, exact schema version,
network, and the immutable parameters required by that domain. Canonical
identity also includes workload, network-upgrade activation fingerprint,
construction manifest, and reorg policy. Wallet identity binds to the exact
canonical source contract and sequence position it projects.

Openers fail without mutation when identity or version does not match. Zinder
does not adopt an unknown non-empty directory, silently reinterpret an incompatible
layout, or let a reader upgrade primary storage.

### Lifecycle ownership

Canonical construction writes an inactive candidate, validates its source
manifest and ordered digest, and publishes a baseline epoch. Canonical following
then appends or replaces the visible suffix under the fixed reorg policy.

Wallet construction pins a canonical fence, acquires a projection build lease
and a canonical retention lease, builds an inactive wallet store, validates its
source identity and digest, catches up, and publishes ready evidence. Continuous
following takes ownership before the construction leases are released.

Both construction leases belong to the projector's configured build-owner
identity. The wallet build lease records it as the lease owner, and the
canonical retention lease uses it as the lease identity. The wallet build
lease refuses a different owner until it expires, so two projectors with
distinct identities never share a build. The owner of a live lease may reclaim
it at once, and may discard the unpublished wallet store it guards. A projector
restarted after a crash therefore discards its own unpublished store, reclaims
its canonical retention lease, and builds again without waiting for the lease
duration.

Every acquisition, including a reclaim, persists a strictly higher lease
generation. Both stores compare the generation on each renewal and release, and
the wallet store also compares it before READY publication. A process still
holding a replaced generation therefore cannot renew, release, or publish
under it.

Lease ownership cannot tell a restarted owner from a second live process
configured with the same identity. A non-blocking advisory lock on a
`<wallet>.build-lock` file beside the wallet path does. A projector holds it
for the whole of a wallet construction and while it discards an unpublished
store, and the operating system releases it when the process exits, including
on a kill. A restart therefore always acquires it, while a second live process
building or discarding the same path is refused. The lock file is never
removed, so every process locks the same file.

A fresh wallet store is initialized in a `<wallet>.initializing` sibling and
renamed to the wallet path after its BUILDING control record is durable. The
wallet path therefore never holds a store without that record. Under the lock,
a construction whose wallet path does not exist yet removes the staging and
initialization siblings an interrupted build left behind.

Serving admission catches canonical and wallet secondaries up independently and
publishes a pair only when their identities and positions agree. In-flight
requests retain their original immutable pair while later requests atomically
switch to the replacement pair.

### Supported topology

The supported deployment topology is `rocksdb-single-host`:

- canonical and wallet primaries have one owner each;
- reader processes use RocksDB secondary instances with process-unique metadata
  paths;
- the primary stores live on a shared host filesystem; and
- authenticated private control APIs coordinate fences, leases, mempool state,
  and recovery operations.

The four release runtimes are `zinder-ingest`, `zinder-projector`,
`zinder-query`, and `zinder-compat-lightwalletd`. The two serving runtimes open
process-unique secondary generations over the same canonical and wallet
primaries. Native `WalletQuery` and lightwalletd compatibility are independent
protocol surfaces; neither aliases the other. Explorer and Cipherscan services
are optional workspace components, not release topology members.

PostgreSQL support in `zinder-bench` is a diagnostic persistence arm for the
same canonical replay corpus and digest oracle. It does not provide the
runtime ownership, wallet projection, wallet-serving admission, replication,
recovery, or operational contracts required for a deployable topology. Zinder
therefore does not advertise a PostgreSQL deployment mode.

### Recovery boundary

A canonical checkpoint alone is not a wallet-serving backup. Production restore
requires a coherent bundle that authenticates the canonical fence and wallet
digest together, restores both stores into fresh paths, and passes wallet-serving
admission before traffic becomes ready. Independently timed directory copies do
not prove this contract.

## Consequences

- Canonical throughput is independent of historical wallet index lookups.
- Wallet and explorer schemas can change or rebuild without redefining chain
  truth.
- Each store has one unambiguous writer, and readers cannot mutate primaries.
- Readiness reflects exact source and projection agreement, not merely open
  sockets or individually healthy stores.
- A second storage engine requires a complete topology implementation, not a
  generic database adapter or a successful persistence microbenchmark.
- Operators must place the supported primary stores on one host filesystem and
  preserve separate secondary metadata paths per reader process.
- A projector that crashes during construction resumes as soon as it restarts
  with the same build-owner identity. Concurrently running projectors need
  distinct identities, because projectors that share one interrupt each
  other's builds instead of being refused.

## Rejected alternatives

### One schema for canonical, wallet, and explorer data

This couples canonical progress to every consumer workload and makes projection
schema changes part of the chain-truth storage contract.

### Per-service canonical writers

Multiple canonical writers create competing visibility and reorg authorities.
Zinder keeps one writer and distributes authenticated events instead.

### Generic database adapter

RocksDB and PostgreSQL have different transaction, replication, bulk-load, and
recovery mechanics. Domain-shaped APIs remain stable while engine-specific
modules own physical behavior. Shared abstractions are introduced only when two
supported implementations prove the same boundary.

### Release construction leases only on a clean exit

The projector already releases both leases when construction fails or is
cancelled. A process stopped by the out-of-memory killer, a kill signal, or a
host restart runs no cleanup. Releasing on exit alone would still stall
construction for the full lease duration after every crash.

### Shorter construction leases

The lease must outlive the longest durable construction phase, so
`projector.lease_duration_seconds` has a 14400-second floor. A shorter lease
would bound the stall after a crash, but a healthy long build could lose
ownership between heartbeats.

### Mixed storage-engine deployments

Per-plane engine mixing multiplies backup, readiness, failover, and support
combinations. A future topology must define one coherent application contract
for all required stores and runtimes.

## Topology review status

`rocksdb-single-host` remains the supported topology. A review of whether one
process and one database per instance would remove costs of the separate
runtimes is open. It has no result, and this section records its status rather
than a decision against the alternative. Each figure names its network and
source. The thresholds below are proposals until the project owner sets
service objectives in [issue #81](https://github.com/ZcashFoundation/zinder/issues/81).

Status at v0.16.1:

- Secondary catch-up failures under write pressure: not determined. Synthetic
  load on rust-rocksdb 0.51 produced transient catch-up errors that recovered
  on retry. Production logs sampled so far show none, but they come mostly from
  releases that opened a fresh secondary for every publication. A projector
  catch-up error, or a failed writer-status call, ends the projector process,
  and the single-host supervisor then restarts the container. In query, a
  failed candidate catch-up closes that generation and reopens it cold at its
  next use, while the published pair keeps serving through lag or a deleted
  table file until the staleness ceiling.
- Table files deleted by compaction: a read that fails on a table file the
  primary's compaction deleted marks its serving pair. The publisher replaces
  the pair at the writer's fence, and the read waits up to five seconds and
  runs once more. A pinned read whose epoch only the marked pair served ends
  with `CHAIN_EPOCH_PIN_UNAVAILABLE`. In a local 50-minute Testnet run
  (Zakura 1.8.6, v0.16.1) a deleted file failed every read on the current pair
  for about 31 s before this replacement existed. The replacement has not been
  measured on a deployment.
- Commit-to-publication delay: measured on Testnet only. In that local run,
  v0.16.1 had a median of 0.86 s, a 90th percentile of 20.1 s, and a maximum
  of 34.9 s, against 0.59 s, 0.94 s, and 7.6 s on v0.16.0 over the same
  blocks. The difference is coalescing: v0.16.1 keeps each replaced pair
  pinnable for one minute and publishes nothing while no generation is free.
  It lost no pin, where v0.16.0 failed four of 278 walks with
  `CHAIN_EPOCH_PIN_UNAVAILABLE`. Simulation on a recorded Mainnet block trace
  delays about 43% of heights, with a 95th percentile of 25 s. No v0.16 release
  has run on Mainnet, and the projector exports no lag metric.
- Long stable views: open. Native streams end with the typed epoch expiry that
  ADR-0038 specifies. In the compact-blocks-only compatibility runtime, a
  request that still holds the retired generation stops the next publication,
  and once lag passes the threshold and the staleness ceiling passes, the
  runtime refuses clients. Whether a long compatibility stream survives
  wallet-serving retention is unmeasured.
- Evidence limits: the comparison of a combined commit and in-process snapshot
  serving has not been rerun on current code. Benchmark figures come from one
  host with local NVMe and a Testnet store.

The review closes with a decision recorded here after four measurements: a
production census on Mainnet, a catch-up run under write pressure that forces
table-cache eviction, a replay comparison between rust-rocksdb 0.51 and 0.54,
and a long compatibility-stream run with a real lightwalletd client.

The review reopens early when a production deployment shows any of these:

- A runtime exit or a `storage_unavailable` refusal caused by secondary
  catch-up, a writer-status call, or a missing table file under a serving pair.
- A Mainnet commit-to-publication 95th percentile above the owner's freshness
  objective over seven days, including coalescing, reported with its
  retention-window and projector-lag components.
- A compatibility stream that fails after the stream fixes ship.
- Two or more user-visible incidents in 30 days whose root cause is
  coordination between runtimes.

Horizontal deployment, including several complete instances behind one
endpoint, is not supported by this topology. Issue #81 decides it.

## Revision history

- 2026-11-14: Construction leases belong to the build-owner identity. The same
  owner reclaims a live lease at a higher generation instead of waiting for it
  to expire, and a different owner is still refused. A process-lifetime lock
  beside the wallet path refuses a concurrent build or discard of that path,
  and a fresh store reaches the wallet path only by rename after its control
  record is durable.
- 2026-11-16: Records the open topology review, its status at v0.16.1, the
  measurements that close it, and the conditions that reopen it early.
