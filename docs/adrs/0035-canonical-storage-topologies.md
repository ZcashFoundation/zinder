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
| `postgres-horizontal` | Hosted fleets | Decided. Not implemented, not certified, and not advertised. Becomes certified and advertised at the end of slice 2 of the [build order](#build-order). |

`postgres-horizontal` does not exist in code. The sections that describe it
record a decision about future work, and no release supports it until it
passes the [benchmark gate](#benchmark-gate) and is certified at the end of
slice 2. The `postgres-horizontal`, Benchmark gate, Build order, and Changed
commitments sections record decided work that does not ship yet. The Upgrade
contract is decided now, and its class label becomes mandatory with the first
release after the release workflow carries it. The remaining sections describe
`rocksdb-single-host`, apart from the Consequences and Rejected alternatives
entries that name `postgres-horizontal`.

Parity between the topologies is promised only on the public wallet
contracts: native `WalletQuery` and lightwalletd compatibility.
[Changed commitments](#changed-commitments) lists the differences decided so
far on those contracts. Canonical and wallet storage never use different
engines inside one deployment.

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
advertise `postgres-horizontal` until that topology is certified at the end
of slice 2.

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
  reads come in a later slice that the build order does not place yet and
  that starts only when the traffic census shows the primary's read capacity
  limits serving. Fences are designed so replica reads can be added. Until
  then one primary caps read scale, and the Zinder tier is what scales.
- **Writers.** Each writer domain has a durable ownership row with a writer
  term from the start. One ingest and one projector run at first. Standby
  writers and automatic failover come in a later slice, after fault-injection
  testing. The target once that slice ships is that a standby takes over
  within 60 seconds of the active writer stopping. Until then, an ingest
  outage longer than the staleness ceiling stops all reads, as
  [Changed commitments](#changed-commitments) describes. This is accepted
  for slices 1 and 2.
- **Mempool.** The mempool contents, the live index of pending
  transactions, stay in the active ingest's memory and are not persisted in
  PostgreSQL. Readers find the active ingest through its ownership row and
  call it. The resumable mempool event log, which the canonical store keeps
  today, is stored in PostgreSQL, so a `MempoolEvents` cursor survives an
  ingest restart. When ingest is unreachable, mempool methods fail, and so
  does a transaction lookup that misses canonical storage and needs the
  mempool to decide its answer. Block, balance, and history reads keep
  working until the staleness ceiling passes.
  [Changed commitments](#changed-commitments) describes these limits.
- **Pinned reads.** Rows carry no epoch-validity ranges. A read pinned to a
  height keeps working across appends, and a reorg that replaces the pinned
  blocks ends it with a retryable error. What a pinned read of wallet state
  that a later block changes, such as unspent outputs, returns after an
  append is not settled, because the rows carry no validity ranges. The
  wallet slice decides it.
- **Cursor signing key.** The key that signs event cursors is a deployment
  secret shared by all readers. Today each store generates its own key, so a
  cursor does not work across instances.
- **PostgreSQL versions.** The topology is certified against PostgreSQL 17
  and 18 at the end of slice 2. Issue #56 named 18 only.
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
into build work. The gate precedes the PostgreSQL build work. It does not
hold back the engine-neutral read interfaces in the
[build order](#build-order). The spike passes when all four thresholds hold:

1. A block commits in under 2 seconds while readers run concurrently.
2. Long block-range streaming reaches at least half of RocksDB's rate per
   reader.
3. A Mainnet build from scratch of canonical and wallet data together
   finishes in under 48 hours.
4. The PostgreSQL canonical and wallet data together are less than twice the
   size of the RocksDB canonical and wallet stores built from the same data.
   Explorer views are not part of the comparison on either engine.

The statistic for threshold 1 and the reader counts for thresholds 1 and 2 are
fixed in the spike's plan before its first run. The thresholds do not measure
wallet queries, the total read capacity of one primary, or serving while a
projection build runs on the same primary. Passing the gate does not show that
one primary carries a given client load.

A missed threshold sends that part of the design back for rework. It does not
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
   writers keep engine-specific code. This work does not wait for the
   benchmark gate.
3. Slice 1: a canonical writer in PostgreSQL that follows the tip, with two
   stateless lightwalletd-compatible servers serving compact blocks behind a
   proxy.
4. Slice 2: wallet projection and native queries. At the end of this slice,
   `postgres-horizontal` becomes certified and advertised.
5. Slice 3: writer standby and failover. This slice moves earlier only if
   Testnet operation shows that the one-writer ingest outage described in
   [Changed commitments](#changed-commitments) matters.
6. Slice 4: Explorer.

Each slice deploys first to a Testnet test deployment, then to the Testnet
faucet backend, and to Mainnet last.

### Changed commitments

The decision changes three existing commitments. The first two apply only to
`postgres-horizontal`; `rocksdb-single-host` behavior does not change.

**A native read can end with a retryable error after a reorg.** Under
`postgres-horizontal`, a reorg that replaces the blocks a native read pinned
ends that read with a retryable error, and the client restarts that walk. An
unpinned read can get the same kind of outcome for a short interval: after the
canonical writer replaces blocks and before the projector publishes the
matching wallet state, a read that needs both has no consistent view. Under
`rocksdb-single-host` the published pair keeps answering through that
interval. Which reads this affects and for how long is settled in the wallet
slice. This is a visible change to the native wallet contract for this
topology. Under `rocksdb-single-host`, a retained serving pair still answers
its epoch after a reorg replaces that epoch's blocks, as the
[Wallet data plane](../architecture/wallet-data-plane.md) describes. The error
code `postgres-horizontal` returns is not chosen yet. What a pinned
unspent-output or spend-status read returns after an append is also not
chosen yet.

**A reader may serve while the mempool is unavailable.** Under
`postgres-horizontal`, a reader stays in service for block, balance, and
history reads while the active ingest is unreachable, until the staleness
ceiling described below passes. Until then, only answers that need the live
mempool fail:

- Mempool methods fail.
- An unpinned `WalletQuery.Transaction` lookup and a lightwalletd
  `GetTransaction` call composed with the mempool surface fail when the
  transaction is not in canonical storage.
  The reader needs the mempool to tell a pending transaction from an absent
  one, as [ADR-0042](0042-proven-transaction-absence.md) describes, so it
  returns the mempool failure instead of an answer. The same lookups still
  answer a transaction found in canonical storage. A `WalletQuery.Transaction`
  lookup pinned with `at_epoch_id` never consults the mempool, so it keeps
  working.

Under `rocksdb-single-host`, native wallet query keeps the rule that no
input can erase another input's failure: it is ready only when the serving
pair, the node source, and the ingest-control channel are all healthy, as
[Service operations](../architecture/service-operations.md#native-wallet-query)
and
[ADR-0038](0038-wallet-runtime-composition-and-capability-discovery.md#support-readiness-and-request-outcomes)
describe. How a `postgres-horizontal` reader reports this partial state to
readiness and to a load balancer is not designed yet.

Serving without ingest has a freshness bound. Under `rocksdb-single-host`, a
wallet-serving reader that cannot refresh its pair reports
`serving_pair_stale` and keeps serving the last pair the writer attested only
until `storage.serving_pair_staleness_ceiling_ms` passes, measured from the
last writer attestation. It then refuses traffic, as
[Service operations](../architecture/service-operations.md#lightwalletd-compatibility)
describes. Native wallet query does not reach this case when ingest itself is
unreachable, because its ingest-control input already refuses traffic.

Under `postgres-horizontal`, a reader also bounds serving without ingest with
a staleness ceiling. Slices 1 and 2 run one ingest and no standby, so an
ingest outage longer than the ceiling stops all reads across the fleet, not
only the answers that need the mempool. This is accepted for slices 1 and 2.
The standby slice moves earlier only if Testnet operation shows that this
matters. The response a reader returns past the ceiling is not chosen yet.
The signal a reader uses to measure staleness without calling ingest is a
design detail of the slice and is not settled here. One candidate is the
renewal evidence the writer keeps in its ownership row, read against
database time.

**One set of binaries carries both engines.** Zinder ships one set of
binaries that contains both storage engines, and configuration selects the
topology. This reverses the rule in issue #56 that each release image contains
one topology. Images are split later only if image size or licensing forces
it. `rocksdb-single-host` binaries therefore also carry the PostgreSQL engine
once it exists.

### Upgrade contract

Every release declares one upgrade class. The contract is decided now and
applies to `rocksdb-single-host` releases too, before any PostgreSQL code
exists. The class label becomes mandatory with the first release after the
release workflow carries it. Adding the label to the release workflow is
tracked separately.

| Class | Operator action | Read downtime | Extra cost |
| --- | --- | --- | --- |
| Compatible | Rolling restart of Zinder processes | None | None |
| Additive schema change | Run the migration job, then a rolling restart | None | None |
| Projection layout change (wallet or Explorer) | A new projector builds a new versioned schema in the same database while the old one serves; switch when ready | None | That projection's storage twice for the build time |
| Canonical layout change (rare) | Build a second database side by side, then switch the proxy | None | Full storage twice for the build time |

The Extra cost column counts only the build. The previous schema or database
is then kept for 7 days for rollback, so the doubled storage lasts for the
build time plus those 7 days. A canonical layout change also runs a second
ingest, projector, and reader set against the second database while it builds.

Event cursors and pinned epochs issued before a canonical layout switch do
not carry over to the second database. Epoch ids and event sequences count
commits in each store, and the second database is not required to reproduce
them. Clients re-establish cursors and pins at such a switch. This is
accepted for this rare class. For a canonical layout change, "Read downtime:
None" therefore means that readers keep answering requests through the
switch. It does not mean that a cursor or pin issued against the first
database keeps working on the second.

The operator actions, downtime, and costs in the table describe
`postgres-horizontal`. A single-volume `rocksdb-single-host` host still
restarts every runtime at each release, whatever the class. There the class
declares what the release changes in the stores and what a rollback costs. A
layout change on RocksDB needs a fresh store or a side-by-side rebuild, as
[Storage backend](../architecture/storage-backend.md#schema-admission)
describes, and how the 7-day retention applies on a single volume is not
decided.

"Read downtime: None" covers block, balance, and history reads. A rolling
restart also restarts the active ingest, which holds the mempool contents in
memory. Until the standby slice ships, mempool methods and the transaction
lookups that need the mempool fail from the moment ingest stops until the
restarted ingest has rebuilt its mempool view, and no block commits in that
interval. Block, balance, and history reads keep answering only while that
interval stays shorter than the staleness ceiling. After the standby slice
ships, the same gap lasts until the standby takes over and builds its own
mempool view. A `MempoolEvents` cursor issued before the restart stays
valid, because the mempool event log is stored in PostgreSQL.

Wallet and Explorer data live in versioned schemas, so a layout change
rebuilds only that projection.

An additive schema change must work in both directions. During a rolling
restart, the previous and the new release run against the same schema at the
same time. The previous release must therefore read and write correctly
against the migrated schema. For example, a new column is nullable or has a
default that the previous release's writes satisfy. The new release must also
run correctly on rows the previous release writes. A change that cannot meet
both conditions is a layout change, not an additive one.

Rollback follows the class:

- Compatible and additive upgrades are reversed by redeploying the previous
  release against the same schema. Both releases must therefore read and
  write correctly against that schema and against each other's rows, for the
  whole upgrade and its rollback.
- For a layout change, the previous schema or database is kept for 7 days. It
  stops following the chain at cutover unless something keeps it current. A
  rollback therefore first brings it back to the current tip: the previous
  release's projector resumes following from where it stopped, or, for a
  canonical layout change, the previous release's whole stack does.
  Resuming needs the canonical events the retained schema has not applied to
  stay retained for the whole 7 days. How that retention is held while no
  projector follows the schema is not designed yet. If those events are
  pruned, the rollback is a rebuild of that projection. The retained schema
  or database must then pass serving admission before readers are pointed
  back at it. Readers never switch to a schema or database that
  has not passed admission. A rollback therefore takes catch-up time and is
  not instant. Keeping the retained schema or database following during the
  7 days is an operator choice that trades compute for a faster rollback.

No upgrade command, migration job, or versioned schema exists yet. The rest of
this section records the contract they are built to.

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
- Until `postgres-horizontal` is certified at the end of slice 2, hosted
  deployments keep the release downtime and the rebuild cost described in
  [Context](#context).
- Until the standby slice ships, a `postgres-horizontal` deployment has one
  ingest, and an ingest outage longer than the staleness ceiling stops all
  of its reads.
- Clients re-establish event cursors and pins after a canonical layout
  switch.
- Native wallet clients of a `postgres-horizontal` deployment must handle a
  retryable error on a pinned read after a reorg and restart that walk.
- Until replica reads ship, one PostgreSQL primary caps the read scale of a
  `postgres-horizontal` deployment.
- Once the release workflow carries the class label, every release declares
  an upgrade class. During a compatible or additive upgrade, the previous and
  the new release must each read and write correctly against the same schema
  and against each other's rows.
- Rolling back a layout change takes the time the retained schema or database
  needs to catch up to the tip and pass serving admission, unless the
  operator kept it following.
- A layout change holds the affected storage twice for its build time and
  the 7 days of rollback retention.
- `postgres-horizontal` is not chosen to reduce storage. The gate allows a
  database up to twice the size of the RocksDB canonical and wallet stores, a
  replicated database host repeats it for each replica, and a layout change
  doubles the affected part for its build and rollback retention.
- Under `postgres-horizontal` the database is the only stateful component,
  and the deployment is as available and as recoverable as that database.
  Losing a single database costs a restore from backup, or a canonical and
  wallet rebuild that the benchmark gate allows to take up to 48 hours.
- Until the Explorer slice ships, a hosted deployment that serves both
  wallets and the Explorer runs two complete stacks: `postgres-horizontal`
  for wallet serving and a separate `rocksdb-single-host` deployment for the
  Explorer, with its own ingest and canonical store. That second stack keeps
  the release downtime and rebuild cost described in [Context](#context).
- Node redundancy is a separate concern that this decision does not solve.
  Ordered node endpoint failover in ingest and node diversity (Zebra and
  Zakura) are tracked separately and do not wait for PostgreSQL. Reader node
  failover, for the query and compatibility readers that each depend on one
  node today, is tracked separately too.

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

The interfaces ship before the second engine exists, so until a PostgreSQL
reader passes the suite only one implementation has proven the boundary. This
replaces the earlier rule in this ADR that a shared abstraction is introduced
only when two supported implementations prove the same boundary.

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

Each instance repeats the full stack and about 465 GB of Mainnet storage when
it carries the Explorer views. Each instance still has release downtime on a
single volume and still needs a manual rebuild or restore when its host is
lost. Instances also diverge: epoch
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

### Persisting the mempool contents in PostgreSQL in the first release

This is deferred, not rejected. It concerns the mempool contents, the live
index that ingest keeps in memory. The resumable mempool event log, which
the canonical store keeps today, is stored in PostgreSQL in the first
release, so `MempoolEvents` cursors survive an ingest restart. Persisted
mempool contents are new durable state with their own hydration and
certification work. Readers can instead call the active ingest, which they
find through its ownership row, and keep serving other reads while it is
down, until the staleness ceiling passes. The mempool contents are persisted
later only if the benchmark spike or production shows a need.

## Topology review outcome

A review asked whether one process and one database per instance would remove
the costs of the separate runtimes, and how Zinder should scale beyond one
host. This decision closes it. `rocksdb-single-host` stays unchanged for
self-hosters and small deployments, and `postgres-horizontal` is the decided
production topology for hosted fleets once it ships.

The decision rests on three facts:

- **Operability.** The objective is replaceable processes, readers that
  upgrade without downtime, and recovery in minutes. A single-process instance
  still owns its volume, so it meets none of these.
- **Platform volume constraint.** On the hosting platform in use, a volume
  attaches to one service, and a service with a volume has downtime on every
  redeploy. Any topology that keeps state on a service volume restarts its
  readers at each release.
- **Storage cost of repeated instances.** Each extra RocksDB instance repeats
  about 465 GB of Mainnet storage when it carries the Explorer views, and the
  full stack.

Two pieces of evidence did not exist when the decision was made. No load limit
of `rocksdb-single-host` had been measured under client load. No PostgreSQL
measurement at the chain tip existed; the `zinder-bench` arm measures bulk
persistence of a replay corpus, not tip following under readers. The
[benchmark gate](#benchmark-gate) therefore precedes PostgreSQL build work,
and its four thresholds are the conditions that send a part of the design
back.

The decision also closed the review before the measurements the 2026-11-16
revision planned were complete. For `rocksdb-single-host` these remain
unmeasured: secondary catch-up under write pressure that forces table-cache
eviction, the replacement of a pair that lost a table file when provoked on a
deployment, whether a long compatibility stream survives wallet-serving
retention, and commit-to-publication delay over seven days. The Mainnet delay
figures below cover about six hours.

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
  Replaces the rule that a shared abstraction is introduced only when two
  supported implementations prove the same boundary, because the
  engine-neutral read interfaces ship before the second engine.
- 2026-11-17: Settles points the first 2026-11-17 entry left open. With one
  writer, an ingest outage longer than the staleness ceiling stops all reads,
  which is accepted for slices 1 and 2. The resumable mempool event log is
  stored in PostgreSQL, while the mempool contents stay in ingest memory.
  Clients re-establish cursors and pins at a canonical layout switch. The
  gate's build and size thresholds cover canonical and wallet data together.
  The read-interface work does not wait for the gate, and
  `postgres-horizontal` becomes certified and advertised at the end of
  slice 2.
