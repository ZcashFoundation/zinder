# ADR-0004: Separate canonical and projection storage, and support one topology per deployment

Status: Accepted

| Topology | Role | Status |
| --- | --- | --- |
| `rocksdb` | Self-hosters and small deployments | Implemented, supported, and advertised. |
| `postgres` | Hosted fleets | Decided. Not implemented, not certified, and not advertised. |

The PostgreSQL arm of `zinder-bench` is a diagnostic benchmark. It is not a deployment mode.

## Context

Zinder has three storage workloads. Canonical ingestion is an ordered, block-local write stream. Wallet serving needs cross-block address history, unspent outputs, spends, and bounded reorg undo. Explorer products need aggregates that each product selects. Doing all three in one commit would make the canonical writer perform historical reads and maintain indexes that are not chain truth.

The RocksDB topology ties every runtime to the host that holds the primary stores. On hosting platforms such as Railway, a volume attaches to one service, and that service restarts on every redeploy, so each release restarts all runtimes. A lost host means rebuilding the stores or restoring them from snapshots. Earlier releases took about 16 hours to rebuild the Mainnet stores, and a second complete instance repeated about 465 GB of Mainnet storage including Explorer views.

Current releases build faster. At Mainnet height 3,593,347, fresh canonical construction reached READY in 1 h 50 min 40 s with 6 hashing workers on 8 cores, and in 2 h 34 min 28 s on 4 writer cores ([#488](https://github.com/ZcashFoundation/zinder/issues/488)). The wallet projection built in 1 h 18 min 35 s ([#474](https://github.com/ZcashFoundation/zinder/issues/474)). Without Explorer views, the canonical column families sum to about 300 GiB of SST bytes at construction and the wallet store is about 47 GiB ([#364](https://github.com/ZcashFoundation/zinder/issues/364)); no like-for-like total with the earlier 465 GB exists. Restoring both from disk snapshots brought a fresh host to a serving wallet reader in 22 to 27 minutes, including a full read of every store file ([#485](https://github.com/ZcashFoundation/zinder/pull/485); [Restoring the Mainnet benchmark fixture](../runbooks/mainnet-benchmark-fixture.md)). The case for another topology no longer rests on a 16-hour rebuild. It rests on three things: a managed database service that handles scaling, replication, and backups; Zinder processes that hold no state; and upgrades that do not restart every runtime.

The longest outages observed so far came from the Zcash node, not from storage. Node redundancy is a separate decision ([ADR-0007](0007-node-failure-recovery-and-transport-policy.md)).

## Decision

1. **Canonical and projection storage are separate.** `zinder-ingest` is the only canonical writer, and a canonical commit never reads wallet state. `zinder-projector` is the only wallet-store writer and builds from authenticated canonical event fences. Explorer aggregates are independently versioned materialized views that change neither the canonical nor the wallet contract.
2. **Stores carry their identity, and readers state what they require.** Each store records its domain, exact schema version, network, and immutable parameters. A canonical store's parameters include its stored set: the workload, raw-blob retention, and build plan that fix which data it holds and the height its history starts at. A wallet store also binds the canonical source and position it projects. Zinder never adopts an unknown non-empty directory, and a reader never upgrades a primary store. An opener fails without mutation when its admission rule does not hold:
   - A writer requires the exact identity, including the stored set. This applies to the canonical writer, publication reopen, owner checkpoints, and recovery checkpoint admission.
   - A canonical reader requires the exact schema version, network, effective reorg window ([ADR-0010](0010-reorg-window-and-displaced-blocks.md)), admitted upgrade schedule, and READY state, and requires the store's physical inventory, control record, and construction manifest to agree exactly. For the stored data it states a minimum instead: the data it reads, the earliest height its history must reach, and, where its composition requires one, a complete subtree-root prefix from index zero in every pool. Light-wallet admission requires complete prefixes. A full-wallet serving pair may admit a store that holds only retained roots, and it refuses an unavailable earlier root on the request that asks for it. A store that holds more than the minimum is admitted.
   - Cold admission authenticates the construction manifest file. Catch-up requires the admitted stored set and checks the manifest version and digest in READY evidence against the admitted binding. A changed binding refuses the catch-up.

   Holding more data never changes which composition a reader runs. The admitted store's persisted data is one input to capabilities ([ADR-0014](0014-consumer-neutral-wallet-data-plane.md)), and the installed handlers, dependencies, and readiness rules decide the rest ([ADR-0015](0015-wallet-runtime-composition.md), [ADR-0016](0016-wallet-serving-readiness.md)).
3. **One topology per deployment.** All stores of one deployment use the same engine. Parity between topologies is promised only on the public wallet contracts: native `WalletQuery` and lightwalletd compatibility.
4. **`rocksdb` is the supported topology.** The canonical and wallet primaries live on one host filesystem, each with one owner. Readers open secondaries with process-unique metadata paths ([ADR-0003](0003-epoch-bound-reads-and-storage-roles.md)). Private, authenticated control APIs coordinate fences, leases, mempool state, and recovery.
5. **A backup of a deployment that serves a wallet projection is a coherent pair.** A canonical checkpoint alone is not a wallet-serving backup. A restore authenticates the canonical fence and wallet digest together, restores both into fresh paths, and passes wallet-serving admission before traffic. Independently timed directory copies are not a backup. Private restore uses an independently retained SHA-256 of the exact outer manifest, validates both checkpoint databases, releases the source projector's retention leases, and publishes one fresh directory. A private replacement retains the cursor authentication identity for its own deployment. [Private backups](../runbooks/private-backups.md) describes admission before cutover.
6. **`postgres` is the decided topology for hosted fleets.** Its objective is operability: stateless, replaceable Zinder processes, readers that upgrade without downtime, and recovery from a lost host in minutes. The objective does not rest on a measured load limit of `rocksdb`. Its first form is deliberately small:
   - Canonical and wallet data live in one database per network, in separate schemas, so one snapshot covers both.
   - Readers query the primary only. Replica reads come later, if the primary's read capacity limits serving.
   - One ingest and one projector run, each with a durable ownership row and writer term. Standby writers come later.
   - The live mempool stays in the active ingest's memory, and readers call that ingest. The mempool event log is stored in PostgreSQL, so `MempoolEvents` cursors survive an ingest restart.
   - The proxy terminates TLS, limits clients, and routes traffic to ready readers. The lightwalletd compatibility reader serves gRPC-Web on its existing port when the operator enables browser origins. This gives native and browser wallets one endpoint and lets local developers use the same transport without installing a translating proxy.
   - One set of binaries carries both engines, and configuration selects the topology.

   The topology exists for operators who run a managed database, such as a cloud provider's managed PostgreSQL. They scale, replicate, and back up the database with their provider's tools, and run Zinder as stateless processes they can replace or upgrade one at a time. It is a different deployment model, not a response to a performance limit of `rocksdb`.
7. **A benchmark gate precedes PostgreSQL build work.** On Mainnet-shaped data, all four must hold: a block commits in under 2 seconds while readers run; long block-range streaming reaches at least half of RocksDB's rate per reader; a from-scratch Mainnet build of canonical and wallet data finishes within four times the RocksDB build time on the same host and data; and the PostgreSQL canonical and wallet data are less than twice the size of the RocksDB stores built from the same data. Each factor is a chosen tolerance, not a measured limit. A missed threshold sends that part of the design back for rework.
8. **Reads become engine-neutral first, and both engines share one logical canonical model.** Read interfaces with RocksDB behind them and a conformance suite for both engines ship before any PostgreSQL reader. Both topologies share one block-intrinsic canonical model (an immutable settled prefix split between shielded scanning data and block supplement facts, plus an optional single-copy raw block vault and a live tail bounded by the store's effective reorg window—default 1,000 blocks, raisable in place per [ADR-0010](0010-reorg-window-and-displaced-blocks.md)—plus its parent boundary anchor, mapped to flat segments plus RocksDB on `rocksdb` and packed multi-block chunks plus live-tail tables on `postgres`), while keeping canonical, wallet projection, and Explorer writers decoupled. Writers keep engine-specific code. Slice 1 then adds a PostgreSQL canonical writer behind stateless lightwalletd-compatible servers, and slice 2 adds wallet projection and native queries. `postgres` becomes certified and advertised at the end of slice 2. Writer standby and the Explorer follow.
9. **Every release declares an upgrade class.** The classes are compatible, additive schema change, projection layout change, and canonical layout change. During a compatible or additive upgrade, the old and new releases must each read and write correctly against the same schema and each other's rows. A layout change raises the affected store's schema version, and each release refuses a store at any other version. A layout change requires a fresh store, built from scratch in a fresh path. Zinder has no in-place migration. Team-operated deployments schedule the rebuild and keep no mandatory rollback lane. A release serves only from a store at its own version, so a self-hoster who wants a fallback keeps the previous binaries and store until the new store passes admission. The release workflow does not record the class, and Zinder has no upgrade command or migration job.

Regional RocksDB instances can initialize independent single-host writers from verified snapshots. Each instance owns its copy and follows its configured validator. [ADR-0024](0024-verified-edge-snapshot-bootstrap.md).

## Evidence

- Decision 6: judgment. It serves operators who run a managed database and does not relieve a measured limit, so no RocksDB measurement reopens it. Its cost is a second storage engine to build and certify. Decision 7's gate is the observation that reopens it: a threshold the design cannot be reworked to meet. The Context gives the current RocksDB rebuild and restore figures, which this purpose does not depend on.
- Decision 7, the build threshold: judgment on a measured baseline. The RocksDB side measured about 1 h 51 min for canonical construction and about 1 h 19 min for the wallet projection on Mainnet ([#488](https://github.com/ZcashFoundation/zinder/issues/488), [#474](https://github.com/ZcashFoundation/zinder/issues/474)), so the former 48-hour limit was more than ten times the RocksDB build and could not detect a regression. The factor of four, like the factors of one half and two in the other thresholds, is a chosen tolerance, not a measurement. The commit-latency, streaming-rate, and size thresholds are unchanged.
- Decision 2, exact identity for writers and minimum data for readers: judgment. A reader still compares the schema, network, reorg window, upgrade schedule, READY state, and manifest agreement, so admitting a store that holds more data changes neither the chain nor the history it serves. It lets one store serve readers that need less. A different answer reopens it when both stores cover the request, both readers offer the same capability, and the dependencies each needs for that request are available. Availability can differ when one reader has a retained body and the other needs an unreachable node.
- Decision 2, complete subtree-root prefixes only for light-wallet admission: judgment. Light-wallet serving serves roots from index zero and refuses at startup a store that cannot. A full-wallet pair serves a checkpointed store's retained roots and refuses an unavailable earlier root on the request that asks for it. A wallet that cannot recover from that per-request refusal reopens it.
- Decision 5, backups of deployments with a wallet projection: judgment. A deployment without a wallet store has no second store to pair with its canonical checkpoint. No restore of such a deployment has been measured.
- Decision 9, a fresh store per layout change with no mandatory rollback lane: judgment, resting on the maintainer's statement that team-run deployments rebuild from scratch and need no migration, side-by-side cutover, or rollback window ([#364](https://github.com/ZcashFoundation/zinder/issues/364)). Its cost is a rebuild per layout change, which the Context measures. A supported deployment that can neither absorb the rebuild outage nor keep a fallback reopens it.

## Consequences

- Canonical throughput does not depend on wallet index lookups, and wallet or Explorer schemas can be rebuilt without redefining chain truth.
- Operators keep the supported primaries on one host filesystem and give each reader its own secondary metadata path.
- Until `postgres` is certified, hosted deployments keep release downtime and the full rebuild cost.
- Under `postgres`, three commitments change:
  - A reorg that replaces the blocks a native read pinned ends that read with a retryable error, and the client restarts its walk. Under `rocksdb`, a retained serving pair keeps answering its epoch.
  - A reader keeps serving block, balance, and history reads while ingest is unreachable. Mempool methods fail, and so does a transaction lookup that misses canonical storage and needs the mempool to answer.
  - With one ingest, an ingest outage longer than the staleness ceiling stops all reads. This is accepted until the standby slice.
- A reader's raw-blob setting is a minimum, so one canonical store can serve readers with different needs. Changing what a store holds still needs a new store.
- Clients re-establish event cursors and pins after a canonical layout change, because epoch ids and event sequences count commits in each store.
- A canonical layout change costs each affected deployment a full rebuild, including any wallet projection and Explorer views, and a serving outage for its length unless the operator builds beside the running deployment. Zinder accepts this cost because no release reads two canonical layouts, and an operator can keep a fallback without Zinder requiring one.
- `postgres` is not chosen to save storage. The gate allows twice the RocksDB size, and the database is the deployment's only stateful component.
- A deployment that omits transaction bodies and the wallet projection stores far less. On Mainnet at height 3,593,347, transaction bodies (`transaction_blob`) measured 251.1 GiB of the canonical store and the wallet projection 46.9 GiB ([#364](https://github.com/ZcashFoundation/zinder/issues/364)). The size threshold must therefore name the stored set it compares, including raw-blob retention, and use the same set on both engines.

## Alternatives rejected

- **One schema for canonical, wallet, and Explorer data.** It couples canonical progress to every consumer workload and makes projection schema changes part of the chain-truth contract.
- **A generic database adapter.** The engines differ in transactions, replication, bulk load, and recovery. A generic adapter exposes keys, rows, and scans for callers to compose. The engine-neutral read interfaces instead answer domain questions, such as a block range, and a shared conformance suite defines them.
- **Mixed engines in one deployment.** Mixing multiplies backup, readiness, failover, and support combinations. An Explorer that stays on RocksDB runs as a separate deployment.
- **Several RocksDB instances behind one endpoint.** Each repeats the full stack and storage, keeps release downtime, and needs a rebuild when its host is lost. Epoch ids and cursor keys differ per store, so a load balancer would need session affinity.
- **Read followers fed by the chain-event stream.** That needs a new replication protocol with exact reorg replay and a wallet projection per follower, and the writer still has no failover.
- **RocksDB secondaries over a network filesystem.** Secondary mode is not cross-host replication. A secondary reads the primary's live files and fails when compaction deletes one it still uses.
- **Persisting the live mempool in PostgreSQL in the first release.** Deferred, not rejected. It ships later only if the benchmark or production shows a need.
- **Exact store identity for readers.** A reader would refuse a store that holds more than it reads, such as one that retains transaction bodies behind a reader that needs none, with no gain in safety. Readers still compare everything that decides which chain and which history they serve.
- **Requiring the old layout to be kept for 7 days after a layout change.** It makes doubling a deployment's disk for a week a required step. An operator who wants a fallback keeps the previous binaries and store by choice, and otherwise rolls back by rebuilding with the previous release.
- **In-place migration.** It needs conversion code per family, recovery for a half-converted store, and tests for every pair of versions. A rebuild reuses the tested construction path.

## Revision history

- 2026-11-24. Simplified the topology identifiers to `rocksdb` and `postgres`, and updated Decision 8 to bind both engines to one logical canonical model (settled split plus a live tail bounded by the effective reorg window and parent boundary anchor) while keeping canonical, wallet projection, and Explorer writers separate.

- 2026-11-24. Linked ADR-0024 for bootstrapping an independent single-host RocksDB deployment.

- 2026-11-20. Specified private, same-schema restore into an inactive RocksDB deployment and the identity retained by its cursors.

- 2026-11-20. Decision 6 assigns optional gRPC-Web translation to the compatibility reader across storage topologies. TLS, client limits, and balancing remain at the deployment edge.

- 2026-11-23. The Context records current Mainnet build, restore, and store-size measurements. Decision 6 states the managed-database purpose of `postgres-horizontal`. Decision 7 replaces the 48-hour build threshold with four times the RocksDB build time, and the consequences require the size threshold to name the stored set it compares.

- 2026-11-23. Decision 2 separates a writer's exact store identity from a reader's minimum data requirements and distinguishes cold manifest authentication from catch-up's READY binding check. Decision 9 replaces the 7-day build-beside rollback with a schema-version bump and a fresh store, with an optional fallback for self-hosters. Decision 5 applies the coherent-pair backup to deployments that serve a wallet projection.

- 2026-11-23. Decision 2 limits the complete subtree-root prefix requirement to light-wallet admission.

- 2026-11-23. Evidence records what decisions 2, 5, and 9 rest on.
