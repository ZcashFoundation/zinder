# Architecture decision records

An architecture decision record (ADR) records why Zinder made a lasting
decision and which alternatives it rejected. Architecture and reference pages
describe how the system works now; an ADR links to them for the mechanism
instead of repeating it.

Write an ADR for a commitment that outlives the change that introduces it,
such as a wire contract, a storage boundary, an error vocabulary, or a
release policy. When an ADR already covers the domain, update that ADR in
place instead of adding a sibling. A decision in a new domain takes the next
number.

## Template

```markdown
# ADR-00NN: <decision stated as a short phrase>

Status: Accepted

## Context
The problem and the constraints that force a choice. A few short paragraphs.

## Decision
What Zinder does. When the ADR covers several related decisions, number them.
State each as a rule a reviewer can check.

## Consequences
What this costs and what it obliges (operators, contributors, consumers).

## Alternatives rejected
Each alternative in one or two sentences with the reason it lost. Enough to
stop the debate from being repeated.
```

## Index

| ADR | Title | Decision |
| --- | --- | --- |
| [0001](0001-rocksdb-canonical-store.md) | Store chain data in RocksDB | RocksDB is the storage engine for chain, wallet, and view data in the supported single-host topology. |
| [0002](0002-serialization-and-byte-order.md) | Serialization and byte order | Each boundary chooses its own serialization, and hashes on the wire use RPC byte order. |
| [0003](0003-epoch-bound-reads-and-storage-roles.md) | Epoch-bound reads and storage roles | Every read of stored data binds to one chain epoch, node-backed reads bind to the served tip, and each store has exactly one writer with secondary readers. |
| [0004](0004-storage-and-deployment-topologies.md) | Storage and deployment topologies | Canonical and projection storage stay separate, and each deployment runs one topology. Single-host RocksDB is supported; a multi-host PostgreSQL topology for hosted fleets is decided but not implemented. |
| [0005](0005-bounded-process-memory.md) | Bounded process memory | Process memory is bounded independently of store size, and service binaries allocate through jemalloc. |
| [0006](0006-node-source-boundary.md) | Node source boundary | Node adapters sit behind one source boundary, separate from the protocols Zinder serves. |
| [0007](0007-node-failure-recovery-and-transport-policy.md) | Node failure recovery and transport policy | Ingest recovers from node failures, and each upstream transport owns its own policy. |
| [0008](0008-consensus-parameters-from-the-node.md) | Consensus parameters from the node | Network-upgrade parameters come from the running node and are admitted by the canonical writer. |
| [0009](0009-ingest-lifecycle-and-construction.md) | Ingest lifecycle and construction | One ingest process builds the store, then follows the tip. |
| [0010](0010-reorg-window-and-displaced-blocks.md) | Reorg window and displaced blocks | The reorg window matches the node's finalization bound, and the writer archives displaced blocks. |
| [0011](0011-ingest-control-transport-security.md) | Ingest control transport security | Ingest control authenticates with a bearer token, and the operator provides TLS. |
| [0012](0012-mempool-topology-and-retention.md) | Mempool topology and retention | Ingest owns the one live mempool, and readers proxy it. |
| [0013](0013-chain-event-streams.md) | Chain event streams | Event streams start at an explicit position and resume across reorgs. |
| [0014](0014-consumer-neutral-wallet-data-plane.md) | Consumer-neutral wallet data plane | Wallets read one consumer-neutral data plane from stored artifacts. |
| [0015](0015-wallet-runtime-composition.md) | Wallet runtime composition | Wallet runtimes advertise only the capabilities their composition proves. |
| [0016](0016-wallet-serving-readiness.md) | Wallet-serving readiness | Wallet-serving readiness follows the canonical and wallet serving pair. |
| [0017](0017-transparent-wallet-queries.md) | Transparent wallet queries | Only consumer-owned transparent methods are served, and wallet spend status comes from the wallet projection. |
| [0018](0018-transaction-facts-status-and-broadcast.md) | Transaction facts, status, and broadcast | Transactions are parsed once, "not found" is a typed proof, and broadcast outcomes are typed. |
| [0019](0019-explorer-plane.md) | Explorer plane | The Explorer is an optional runtime over canonical data with explicit freshness and typed privacy refusals. |
| [0020](0020-materialized-views.md) | Materialized views | Explorer views are rebuildable projections with exact per-view schema admission and explicit coverage. |
| [0021](0021-shared-configuration-sections.md) | Shared configuration sections | Shared configuration sections live in `zinder-runtime`, and each runtime composes only what it uses. |
| [0022](0022-release-artifacts.md) | Release artifacts | Releases ship reproducible GNU/Linux binaries with signatures, provenance, and SBOMs. |
| [0023](0023-deployment-profiles.md) | Deployment profiles | A named profile fixes what a deployment stores, which runtimes it runs, and what its readers require. Only the light-wallet profile forwards reads to the node. |

## Earlier numbering

The ADRs were merged by domain and renumbered. The changelog and older
commits cite the earlier numbers, so this table maps each earlier number to
the ADR that now covers it. There was no ADR-0040.

The number 0023 is used twice. A citation of ADR-0023 that predates the
renumbering means transaction facts, now [ADR-0018](0018-transaction-facts-status-and-broadcast.md).
The current [ADR-0023](0023-deployment-profiles.md) records deployment
profiles.

| Earlier | Now | Earlier | Now | Earlier | Now |
| --- | --- | --- | --- | --- | --- |
| 0001 | [0001](0001-rocksdb-canonical-store.md) | 0016 | [0006](0006-node-source-boundary.md) | 0031 | [0020](0020-materialized-views.md) |
| 0002 | [0002](0002-serialization-and-byte-order.md) | 0017 | [0020](0020-materialized-views.md) | 0032 | [0010](0010-reorg-window-and-displaced-blocks.md) |
| 0003 | [0003](0003-epoch-bound-reads-and-storage-roles.md) | 0018 | [0014](0014-consumer-neutral-wallet-data-plane.md) | 0033 | [0019](0019-explorer-plane.md) |
| 0004 | [0006](0006-node-source-boundary.md) | 0019 | [0007](0007-node-failure-recovery-and-transport-policy.md) | 0034 | [0009](0009-ingest-lifecycle-and-construction.md) |
| 0005 | [0014](0014-consumer-neutral-wallet-data-plane.md) | 0020 | [0005](0005-bounded-process-memory.md) | 0035 | [0004](0004-storage-and-deployment-topologies.md) |
| 0006 | [0011](0011-ingest-control-transport-security.md) | 0021 | [0009](0009-ingest-lifecycle-and-construction.md) | 0036 | [0022](0022-release-artifacts.md) |
| 0007 | [0012](0012-mempool-topology-and-retention.md) | 0022 | [0009](0009-ingest-lifecycle-and-construction.md) | 0037 | [0022](0022-release-artifacts.md) |
| 0008 | [0008](0008-consensus-parameters-from-the-node.md) | 0023 | [0018](0018-transaction-facts-status-and-broadcast.md) | 0038 | [0015](0015-wallet-runtime-composition.md) |
| 0009 | [0019](0019-explorer-plane.md) | 0024 | [0002](0002-serialization-and-byte-order.md) | 0039 | [0008](0008-consensus-parameters-from-the-node.md) |
| 0010 | [0018](0018-transaction-facts-status-and-broadcast.md) | 0025 | [0013](0013-chain-event-streams.md) | 0041 | [0010](0010-reorg-window-and-displaced-blocks.md) |
| 0011 | [0019](0019-explorer-plane.md) | 0026 | [0017](0017-transparent-wallet-queries.md) | 0042 | [0018](0018-transaction-facts-status-and-broadcast.md) |
| 0012 | [0019](0019-explorer-plane.md) | 0027 | [0013](0013-chain-event-streams.md) | 0043 | [0017](0017-transparent-wallet-queries.md) |
| 0013 | [0007](0007-node-failure-recovery-and-transport-policy.md) | 0028 | [0020](0020-materialized-views.md) | 0044 | [0005](0005-bounded-process-memory.md) |
| 0014 | [0021](0021-shared-configuration-sections.md) | 0029 | [0017](0017-transparent-wallet-queries.md) | 0045 | [0016](0016-wallet-serving-readiness.md) |
| 0015 | [0009](0009-ingest-lifecycle-and-construction.md) | 0030 | [0018](0018-transaction-facts-status-and-broadcast.md) | | |
