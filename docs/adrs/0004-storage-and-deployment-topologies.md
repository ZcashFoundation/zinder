# ADR-0004: Separate canonical and projection storage, and support one topology per deployment

Status: Accepted

| Topology | Role | Status |
| --- | --- | --- |
| `rocksdb-single-host` | Self-hosters and small deployments | Implemented, supported, and advertised. |
| `postgres-horizontal` | Hosted fleets | Decided. Not implemented, not certified, and not advertised. |

The PostgreSQL arm of `zinder-bench` is a diagnostic benchmark. It is not a deployment mode.

## Context

Zinder has three storage workloads. Canonical ingestion is an ordered, block-local write stream. Wallet serving needs cross-block address history, unspent outputs, spends, and bounded reorg undo. Explorer products need aggregates that each product selects. Doing all three in one commit would make the canonical writer perform historical reads and maintain indexes that are not chain truth.

The RocksDB topology ties every runtime to the host that holds the primary stores. On hosting platforms such as Railway, a volume attaches to one service, and that service restarts on every redeploy, so each release restarts all runtimes. A lost host means rebuilding the stores, which took about 16 hours on Mainnet across earlier releases. A second complete instance repeats the whole stack and about 465 GB of Mainnet storage including Explorer views.

The longest outages observed so far came from the Zcash node, not from storage. Node redundancy is a separate decision ([ADR-0007](0007-node-failure-recovery-and-transport-policy.md)).

## Decision

1. **Canonical and projection storage are separate.** `zinder-ingest` is the only canonical writer, and a canonical commit never reads wallet state. `zinder-projector` is the only wallet-store writer and builds from authenticated canonical event fences. Explorer aggregates are independently versioned materialized views that change neither the canonical nor the wallet contract.
2. **Stores carry their identity.** Each store records its domain, exact schema version, network, and immutable parameters. A wallet store also binds the canonical source and position it projects. An opener fails without mutation when identity or version differs. Zinder never adopts an unknown non-empty directory, and a reader never upgrades a primary store.
3. **One topology per deployment.** All stores of one deployment use the same engine. Parity between topologies is promised only on the public wallet contracts: native `WalletQuery` and lightwalletd compatibility.
4. **`rocksdb-single-host` is the supported topology.** The canonical and wallet primaries live on one host filesystem, each with one owner. Readers open secondaries with process-unique metadata paths ([ADR-0003](0003-epoch-bound-reads-and-storage-roles.md)). Private, authenticated control APIs coordinate fences, leases, mempool state, and recovery.
5. **A backup is a coherent pair.** A canonical checkpoint alone is not a wallet-serving backup. A restore authenticates the canonical fence and wallet digest together, restores both into fresh paths, and passes wallet-serving admission before traffic. Independently timed directory copies are not a backup. Zinder implements capture and has no restore command. Recovery rebuilds the stores ([Service operations](../architecture/service-operations.md#recovery)).
6. **`postgres-horizontal` is the decided topology for hosted fleets.** Its objective is operability: stateless, replaceable Zinder processes, readers that upgrade without downtime, and recovery from a lost host in minutes. The objective does not rest on a measured load limit of `rocksdb-single-host`. Its first form is deliberately small:
   - Canonical and wallet data live in one database per network, in separate schemas, so one snapshot covers both.
   - Readers query the primary only. Replica reads come later, if the primary's read capacity limits serving.
   - One ingest and one projector run, each with a durable ownership row and writer term. Standby writers come later.
   - The live mempool stays in the active ingest's memory, and readers call that ingest. The mempool event log is stored in PostgreSQL, so `MempoolEvents` cursors survive an ingest restart.
   - The proxy terminates TLS, limits clients, and routes traffic to ready readers. The lightwalletd compatibility reader serves gRPC-Web on its existing port when the operator enables browser origins. This gives native and browser wallets one endpoint and lets local developers use the same transport without installing a translating proxy.
   - One set of binaries carries both engines, and configuration selects the topology.
7. **A benchmark gate precedes PostgreSQL build work.** On Mainnet-shaped data, all four must hold: a block commits in under 2 seconds while readers run; long block-range streaming reaches at least half of RocksDB's rate per reader; a from-scratch Mainnet build of canonical and wallet data finishes in under 48 hours; and the PostgreSQL canonical and wallet data are less than twice the size of the RocksDB stores built from the same data. A missed threshold sends that part of the design back for rework.
8. **Reads become engine-neutral first.** Read interfaces with RocksDB behind them and a conformance suite for both engines ship before any PostgreSQL reader. Writers keep engine-specific code. Slice 1 then adds a PostgreSQL canonical writer behind stateless lightwalletd-compatible servers, and slice 2 adds wallet projection and native queries. `postgres-horizontal` becomes certified and advertised at the end of slice 2. Writer standby and the Explorer follow.
9. **Every release declares an upgrade class.** The classes are compatible, additive schema change, projection layout change, and canonical layout change. During a compatible or additive upgrade, the old and new releases must each read and write correctly against the same schema and each other's rows. A layout change builds the new layout beside the old one and keeps the old one for 7 days for rollback. The release workflow does not record the class, and Zinder has no upgrade command or migration job.

## Consequences

- Canonical throughput does not depend on wallet index lookups, and wallet or Explorer schemas can be rebuilt without redefining chain truth.
- Operators keep the supported primaries on one host filesystem and give each reader its own secondary metadata path.
- Until `postgres-horizontal` is certified, hosted deployments keep release downtime and the full rebuild cost.
- Under `postgres-horizontal`, three commitments change:
  - A reorg that replaces the blocks a native read pinned ends that read with a retryable error, and the client restarts its walk. Under `rocksdb-single-host`, a retained serving pair keeps answering its epoch.
  - A reader keeps serving block, balance, and history reads while ingest is unreachable. Mempool methods fail, and so does a transaction lookup that misses canonical storage and needs the mempool to answer.
  - With one ingest, an ingest outage longer than the staleness ceiling stops all reads. This is accepted until the standby slice.
- Clients re-establish event cursors and pins after a canonical layout change, because epoch ids and event sequences count commits in each store.
- `postgres-horizontal` is not chosen to save storage. The gate allows twice the RocksDB size, and the database is the deployment's only stateful component.

## Alternatives rejected

- **One schema for canonical, wallet, and Explorer data.** It couples canonical progress to every consumer workload and makes projection schema changes part of the chain-truth contract.
- **A generic database adapter.** The engines differ in transactions, replication, bulk load, and recovery. A generic adapter exposes keys, rows, and scans for callers to compose. The engine-neutral read interfaces instead answer domain questions, such as a block range, and a shared conformance suite defines them.
- **Mixed engines in one deployment.** Mixing multiplies backup, readiness, failover, and support combinations. An Explorer that stays on RocksDB runs as a separate deployment.
- **Several RocksDB instances behind one endpoint.** Each repeats the full stack and storage, keeps release downtime, and needs a rebuild when its host is lost. Epoch ids and cursor keys differ per store, so a load balancer would need session affinity.
- **Read followers fed by the chain-event stream.** That needs a new replication protocol with exact reorg replay and a wallet projection per follower, and the writer still has no failover.
- **RocksDB secondaries over a network filesystem.** Secondary mode is not cross-host replication. A secondary reads the primary's live files and fails when compaction deletes one it still uses.
- **Persisting the live mempool in PostgreSQL in the first release.** Deferred, not rejected. It ships later only if the benchmark or production shows a need.

## Revision history

- 2026-11-20. Decision 6 assigns optional gRPC-Web translation to the compatibility reader across storage topologies. TLS, client limits, and balancing remain at the deployment edge.
