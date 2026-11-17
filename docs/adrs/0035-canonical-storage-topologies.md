# ADR-0035: Canonical storage and deployment topologies

| Field | Value |
| --- | --- |
| Status | Accepted |
| Product | Zinder |
| Domain | Canonical storage, wallet projection, serving admission, deployment topology, release upgrade contract |
| Related | [Canonical and materialized-view architecture](../architecture/canonical-materialized-view-architecture.md), [Service boundaries](../architecture/service-boundaries.md), [Storage backend](../architecture/storage-backend.md), [Service operations](../architecture/service-operations.md), [Wallet data plane](../architecture/wallet-data-plane.md), [ADR-0038](0038-wallet-runtime-composition-and-capability-discovery.md) |

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

The RocksDB topology ties every runtime to the host that holds the primary
stores. That has three operating costs for hosted deployments:

- On Railway, the hosting platform in use, a volume attaches to one service,
  and a service with a volume always has downtime when it is redeployed. The
  hosted deployments therefore run one container that supervises every
  runtime, and each release restarts all of them.
- A lost host means rebuilding the stores. A Mainnet rebuild is about
  16 hours of canonical and wallet construction. That figure combines builds
  on earlier releases; no rebuild has been timed on the current release.
- A second complete instance repeats the whole stack and its storage. The
  Mainnet deployment used about 465 GB of volume on v0.16.3, including its
  Explorer views.

The longest outages observed so far came from the Zcash node, not from
Zinder's storage. On Mainnet, Zebra 6.4.2 stopped at its end-of-support
height. On Testnet, Zebra 7.0.0 nodes ran out of memory and restarted in a
loop. No storage topology addresses these failures.

## Decision

Zinder separates durable ownership into canonical storage and projection
storage.

1. `zinder-ingest` is the only canonical writer. It persists block-local
   `CanonicalBlockFacts`, direct canonical indexes, chain epochs, retained chain
   events, and control records. Canonical commits do not read wallet projection
   state.
2. `zinder-projector` is the only wallet-store writer. It constructs and follows
   wallet state from authenticated canonical event fences.
3. Under `rocksdb-single-host`, reader services open process-owned
   secondaries. A response that combines canonical and wallet state uses one
   immutable `WalletServingReadPair` admitted at an exact fence.
4. Explorer aggregates use independently versioned materialized views and do
   not change the wallet or canonical contracts.

### Two production topologies

Zinder has two production topologies with different roles. An operator
selects one topology for a whole deployment.

| Topology | Role | Status |
| --- | --- | --- |
| `rocksdb-single-host` | Self-hosters and small deployments | Implemented and supported. The only topology Zinder advertises. |
| `postgres-horizontal` | Hosted fleets | Decided. Not implemented, not certified, and not advertised. |

`postgres-horizontal` does not exist in code. The sections that describe it
record a decision about future work, and no release supports it until it
passes the [benchmark gate](#benchmark-gate) and the certification of its
slices. The `postgres-horizontal`, Benchmark gate, Build order, and Changed
commitments sections record decided work that does not ship yet. The Upgrade
contract is a decision that applies to releases from now on, and no release
note carries the class label yet. The remaining sections describe
`rocksdb-single-host`, apart from the Consequences and Rejected alternatives
entries that name `postgres-horizontal`.

Parity between the topologies is promised only on the public wallet
contracts: native `WalletQuery` and lightwalletd compatibility.
[Changed commitments](#changed-commitments) lists where `postgres-horizontal`
behaves differently on those contracts. Canonical and wallet storage never use
different engines inside one deployment.

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

This section describes `rocksdb-single-host`.

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

### `rocksdb-single-host`

`rocksdb-single-host` is the supported deployment topology:

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
therefore does not advertise a PostgreSQL deployment mode, and it does not
advertise `postgres-horizontal` until that topology is implemented and
certified.

### Recovery boundary

A canonical checkpoint alone is not a wallet-serving backup. Production restore
requires a coherent bundle that authenticates the canonical fence and wallet
digest together, restores both stores into fresh paths, and passes wallet-serving
admission before traffic becomes ready. Independently timed directory copies do
not prove this contract.

### `postgres-horizontal`

`postgres-horizontal` is decided and not implemented. Its objective is
operability:

- Zinder processes are stateless and replaceable.
- Readers upgrade without downtime.
- A lost Zinder host costs minutes instead of a 16-hour rebuild.

The objective does not rest on a measured load limit of
`rocksdb-single-host`. [Topology review outcome](#topology-review-outcome)
records the evidence that existed when the decision was made. Operator
experience, for both human operators and agents, is a named deliverable of the
topology.

The first release is deliberately smaller than the full PostgreSQL design in
[issue #56](https://github.com/ZcashFoundation/zinder/issues/56):

- **Database layout.** Canonical and wallet data live in one database per
  network, in separate schemas, so one snapshot can validate both.
- **Reads.** Readers query the PostgreSQL primary only. Replica (hot standby)
  reads come in a later slice that the build order does not place yet, and
  fences are designed so they can be added. Until then one primary caps read
  scale, and the Zinder tier is what scales.
- **Writers.** Each writer domain has a durable ownership row with a writer
  term from the start. One ingest and one projector run at first. Standby
  writers and automatic failover come in a later slice, after fault-injection
  testing. Once that slice ships, a standby takes over within 60 seconds of the
  active writer stopping.
- **Mempool.** The mempool stays in the active ingest's memory and is not
  persisted in PostgreSQL. Readers find the active ingest through its
  ownership row and call it. When ingest is down, only mempool methods fail;
  block, balance, and history reads keep working.
- **Pinned reads.** Rows carry no epoch-validity ranges. A read pinned to a
  height keeps working across appends, and a reorg that replaces the pinned
  blocks ends it with a retryable error.
- **Cursor signing key.** The key that signs event cursors is a deployment
  secret shared by all readers. Today each store generates its own key, so a
  cursor does not work across instances.
- **PostgreSQL versions.** PostgreSQL 17 and 18 are certified. Issue #56
  named 18 only.
- **Database hosting.** The benchmark spike and Testnet run on a single
  database with point-in-time backups. The Mainnet database host, either a
  replicated cluster on the current platform or an external managed service,
  is decided after the spike from its measured size and cost.
- **Edge.** TLS, rate limits, and gRPC-web
  ([issue #230](https://github.com/ZcashFoundation/zinder/issues/230)) live in
  a documented reference proxy, not inside Zinder.
- **Explorer.** The Explorer joins the topology in a later slice, with a
  hosted Explorer deployment as its named production consumer. Under
  PostgreSQL its view writer runs as its own process, not inside ingest. Until
  then the Explorer stays a separate `rocksdb-single-host` deployment.

### Benchmark gate

A benchmark spike on Mainnet-shaped data runs before issue #56 is broken down
into build work. The spike passes when all four thresholds hold:

1. A block commits in under 2 seconds while readers run concurrently.
2. Long block-range streaming reaches at least half of RocksDB's rate per
   reader.
3. A Mainnet build from scratch finishes in under 48 hours.
4. The database is less than twice the size of the RocksDB stores.

A missed threshold sends that part of the design back to design. It does not
reverse the direction. The spike also reports the real database size and cost,
which decide the Mainnet database host.

### Build order

Work proceeds in this order:

1. In parallel, now: node endpoint failover and node diversity; a minimal form
   of coherent recovery bundles
   ([issue #78](https://github.com/ZcashFoundation/zinder/issues/78)); tree
   states served from local storage
   ([issue #14](https://github.com/ZcashFoundation/zinder/issues/14)); release
   upgrade classes; a traffic census; and the benchmark spike. The census is
   the only active part of the capacity envelope in
   [issue #79](https://github.com/ZcashFoundation/zinder/issues/79). The rest
   of issue #79 and the single-host qualification in
   [issue #80](https://github.com/ZcashFoundation/zinder/issues/80) are
   paused.
2. Engine-neutral asynchronous read interfaces, with RocksDB behind them and a
   shared conformance suite that both engines must pass. They ship in a
   release of their own before any PostgreSQL reader ships. Reads come first;
   writers keep engine-specific code.
3. Slice 1: a canonical writer in PostgreSQL that follows the tip, with two
   stateless lightwalletd-compatible servers serving compact blocks behind a
   proxy.
4. Slice 2: wallet projection and native queries.
5. Slice 3: writer standby and failover.
6. Slice 4: Explorer.

Each slice deploys first to a Testnet test deployment, then to the Testnet
faucet backend, and to Mainnet last.

### Changed commitments

The decision changes three existing commitments. The first two apply only to
`postgres-horizontal`; `rocksdb-single-host` behavior does not change.

**A native pinned read can end with a retryable error after a reorg.** Under
`postgres-horizontal`, a reorg that replaces the blocks a native read pinned
ends that read with a retryable error, and the client restarts that walk. This
is a visible change to the native wallet contract for this topology. Under
`rocksdb-single-host`, a retained serving pair still answers its epoch after a
reorg replaces that epoch's blocks, as the
[Wallet data plane](../architecture/wallet-data-plane.md) describes. The error
code `postgres-horizontal` returns is not chosen yet.

**A reader may serve while mempool methods are unavailable.** Under
`postgres-horizontal`, a reader stays in service for block, balance, and
history reads while the active ingest is unreachable, and only mempool methods
fail. Under `rocksdb-single-host`, native wallet query keeps the rule that no
input can erase another input's failure: it is ready only when the serving
pair, the node source, and the ingest-control channel are all healthy, as
[Service operations](../architecture/service-operations.md#native-wallet-query)
and
[ADR-0038](0038-wallet-runtime-composition-and-capability-discovery.md#support-readiness-and-request-outcomes)
describe. How a `postgres-horizontal` reader reports this partial state to
readiness and to a load balancer is not designed yet.

**One set of binaries carries both engines.** Zinder ships one set of
binaries that contains both storage engines, and configuration selects the
topology. This reverses the rule in issue #56 that each release image contains
one topology. Images are split later only if image size or licensing forces
it. `rocksdb-single-host` binaries therefore also carry the PostgreSQL engine
once it exists.

### Upgrade contract

Every release declares one upgrade class. The class label applies from now on
to every release, including `rocksdb-single-host` releases, before any
PostgreSQL code exists.

| Class | Operator action | Read downtime | Extra cost |
| --- | --- | --- | --- |
| Compatible | Rolling restart of Zinder processes | None | None |
| Additive schema change | Run the migration job, then a rolling restart | None | None |
| Projection layout change (wallet or Explorer) | A new projector builds a new versioned schema in the same database while the old one serves; switch when ready | None | That projection's storage twice for the build time |
| Canonical layout change (rare) | Build a second database side by side, then switch the proxy | None | Full storage twice for the build time |

The operator actions, downtime, and costs in the table describe
`postgres-horizontal`. A single-volume `rocksdb-single-host` host still
restarts every runtime at each release, whatever the class.

Wallet and Explorer data live in versioned schemas, so a layout change
rebuilds only that projection.

Rollback follows the class:

- Compatible and additive upgrades are reversed by redeploying the previous
  release. Each release must therefore read the schema its successor writes.
- For a layout change, the old schema or database is kept for 7 days.
  Rollback points readers back at it.

One upgrade command drives an upgrade. It reads the running version and the
target, and prints the class, the steps, the expected duration, and the disk
needed, in both human-readable and machine-readable form. It then owns the
database side: it plans, migrates, builds a new projection, verifies, and
switches the published version. It checks health at each step and stops
safely on failure. Restarting processes stays with the platform; the command
prints those steps in machine-readable form. Reference scripts cover Docker
Compose and Railway. The command's name and the parts of it that apply to
`rocksdb-single-host` are not decided yet.

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
- Zinder carries two production topologies and the release and test work of
  both. The engine-neutral reads pass one conformance suite on both engines.
- Until `postgres-horizontal` ships and passes certification, hosted
  deployments keep the release downtime and the rebuild cost described in
  [Context](#context).
- Native wallet clients of a `postgres-horizontal` deployment must handle a
  retryable error on a pinned read after a reorg and restart that walk.
- Until replica reads ship, one PostgreSQL primary caps the read scale of a
  `postgres-horizontal` deployment.
- Every release declares an upgrade class. A compatible or additive release
  must leave the previous release able to read its schema.
- Node redundancy is a separate concern that this decision does not solve.
  Ordered node endpoint failover in ingest and node diversity (Zebra and
  Zakura) are tracked separately and do not wait for PostgreSQL.

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
modules own physical behavior.

The engine-neutral read interfaces in the build order are not a generic
adapter. A generic adapter exposes storage operations such as keys, rows, and
scans, and leaves callers to compose them. The read interfaces expose the
questions the serving runtimes ask, such as a block range or an address
history, and each engine answers them in its own way. They cover only the
reads both topologies serve, a conformance suite that both engines pass
defines them, and writers keep engine-specific code.

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
combinations. Each topology defines one coherent application contract for all
required stores and runtimes. An Explorer that stays on `rocksdb-single-host`
while wallet serving moves to `postgres-horizontal` runs as a separate
deployment, not as one plane of a mixed deployment.

### Several complete RocksDB instances behind one endpoint

Each instance repeats the full stack and about 465 GB of Mainnet storage. Each
instance still has release downtime on a single volume and still needs a
manual rebuild or restore when its host is lost. Instances also diverge: epoch
ids count commits in each store and each store generates its own cursor key,
so a pin or cursor issued by one instance does not carry its meaning to
another, and a load balancer would need session affinity. A node's
end-of-support height and the Testnet memory failure both affect every node of
the same version, so identical instances do not add node redundancy either.

### Read followers fed by the event stream

Followers would rebuild their own stores from the canonical event stream. That
requires a new replication protocol: followers need full block bytes, which
readers can fetch only when `storage.raw_blob_policy` is `all`, and they must
replay reorg replacements exactly. Each follower also needs its own wallet
projection or a wallet feed, and the writer still has no automatic failover.

### RocksDB secondaries over a network filesystem

RocksDB secondary mode is not a cross-host replication protocol. A secondary
reads the primary's live files, and a read already fails when compaction
deletes a table file the secondary still uses. Moving a primary path behind a
network filesystem is outside the `rocksdb-single-host` contract, and on the
hosting platform in use a volume attaches to only one service.

### Persisting the mempool in PostgreSQL in the first release

This is deferred, not rejected. Today the canonical store keeps only the
resumable mempool event log; the mempool index lives in ingest memory. A
persisted mempool is new durable state with its own hydration and
certification work. Readers can instead call the active ingest, which they
find through its ownership row, and keep serving other reads while it is
down. The mempool is persisted later only if the benchmark spike or
production shows a need.

## Topology review outcome

A review asked whether one process and one database per instance would remove
the costs of the separate runtimes, and how Zinder should scale beyond one
host. This decision closes it. `rocksdb-single-host` stays unchanged for
self-hosters and small deployments, and `postgres-horizontal` is the
production topology for hosted fleets.

The decision rests on three facts:

- **Operability.** The objective is replaceable processes, readers that
  upgrade without downtime, and recovery in minutes. A single-process instance
  still owns its volume, so it meets none of these.
- **Platform volume constraint.** On the hosting platform in use, a volume
  attaches to one service, and a service with a volume has downtime on every
  redeploy. Any topology that keeps state on a service volume restarts its
  readers at each release.
- **Storage cost of repeated instances.** Each extra RocksDB instance repeats
  about 465 GB of Mainnet storage and the full stack.

Two pieces of evidence did not exist when the decision was made. No load limit
of `rocksdb-single-host` had been measured under client load. No PostgreSQL
measurement at the chain tip existed; the `zinder-bench` arm measures bulk
persistence of a replay corpus, not tip following under readers. The
[benchmark gate](#benchmark-gate) therefore precedes build work, and its four
thresholds are the conditions that send a part of the design back.

Measured behavior of `rocksdb-single-host` that still informs operators:

- **Commit-to-publication delay.** On Mainnet, v0.16.3 following Zebra 7.0.0
  on Railway over about six hours and 819 blocks, the delay from a canonical
  commit to the publication that serves that height had a median of 0.67 s, a
  90th percentile of 11.4 s, and a maximum of 38.4 s. 15% of heights never got
  their own publication, because a later one covered them. The tail comes
  from coalescing: each replaced pair stays pinnable for one minute, and the
  reader publishes nothing while no generation is free.
- **Pins across publication.** In a local 50-minute Testnet run against
  Zakura 1.8.6, v0.16.1 lost no pin, where v0.16.0 failed four of 278 walks
  with `CHAIN_EPOCH_PIN_UNAVAILABLE`.

## Revision history

- 2026-11-14: Construction leases belong to the build-owner identity. The same
  owner reclaims a live lease at a higher generation instead of waiting for it
  to expire, and a different owner is still refused. A process-lifetime lock
  beside the wallet path refuses a concurrent build or discard of that path,
  and a fresh store reaches the wallet path only by rename after its control
  record is durable.
- 2026-11-16: Records the open topology review, its status at v0.16.1, the
  measurements that close it, and the conditions that reopen it early.
- 2026-11-17: Records `postgres-horizontal` as the decided second production
  topology, not yet implemented, certified, or advertised, with its scope,
  benchmark gate, build order, and three changed commitments. Adds the release
  upgrade contract and replaces the open topology review with its outcome.
