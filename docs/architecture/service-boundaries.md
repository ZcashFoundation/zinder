# Service boundaries

Zinder has four release runtimes. Ingest alone writes canonical state,
projector alone writes wallet state, and the native and compatibility servers
each serve process-owned immutable readers admitted at one exact fence.

## Boundary map

| Boundary | Owns | Must not own |
| --- | --- | --- |
| `zinder-ingest` | Zebra chain and mempool sources, canonical construction and following, reorg authority, canonical events, canonical leases, checkpoint coordination, live mempool state, materialized-view construction and following | Wallet rows, wallet-store promotion, or public wallet traffic |
| `zinder-projector` | Wallet construction, projection build leases, continuous canonical-event following, wallet reorg reconciliation, wallet-store primary | Canonical writes, chain selection, public wallet traffic, or compatibility translation |
| `zinder-query` | Canonical and wallet secondary generations, wallet-serving admission, native `WalletQuery` gRPC, transaction broadcast, and sparse tree-state fill | Either primary store, projection construction, compatibility translation, or mixed-generation responses |
| `zinder-compat-lightwalletd` | Canonical and wallet secondary generations, wallet-serving admission, `CompactTxStreamer` translation, transaction broadcast, sparse tree-state fill | Either primary store, projection construction, or mixed-generation responses |

`zinder-explorer` and `zinder-compat-cipherscan` are optional workspace
services, and the checked single-host composition does not start them. The
release workflow publishes `zinder-explorer` as a runtime image and inside the
`zinder-wallet-serving-explorer` single-volume image. It does not publish
`zinder-compat-cipherscan`.

## Ownership rules

The services may share domain types from `zinder-core`, protocol definitions
from `zinder-proto`, storage contracts from the owning storage crates, runtime
configuration, operations support, and canonical secondary attestation against
`CanonicalControl.WriterStatus` from `zinder-runtime`, and fixtures from
`zinder-testkit`.

They do not share mutable in-memory chain state or a writable RocksDB handle.
No reader repairs missing canonical or wallet rows on demand. No compatibility
adapter bypasses `WalletQueryApi` for indexed reads. Store schema changes are
executed by the process that owns that primary.

Source-node access is capability-specific:

- `zinder-ingest` owns chain selection, block acquisition, and mempool
  ingestion.
- `zinder-projector` may discover network activation parameters needed to
  validate canonical identity; it does not fetch projection rows from the node.
- `zinder-query` may broadcast transactions, discover consensus activations,
  and fill tree state explicitly delegated upstream by the native contract. It
  does not use the node as a fallback for indexed chain history.
- `zinder-compat-lightwalletd` may broadcast transactions, discover consensus
  activations, and fill tree state that the query contract explicitly delegates
  upstream. It does not use the node as a fallback for indexed chain history.

## Storage ownership

`zinder-ingest` opens canonical RocksDB as primary, plus one in-process
canonical secondary and the materialized-view store as primary.
`zinder-projector` opens a canonical secondary and wallet RocksDB as primary.
`zinder-query` and `zinder-compat-lightwalletd` each open generation-specific
secondaries for both stores. Canonical and wallet paths are siblings, and every
secondary generation has its own process-specific metadata path.

Each serving publisher catches both secondaries up, validates network, reorg
policy, canonical source identity, event fence, and wallet digest, then
atomically publishes a `WalletServingReadPair`. Each query operation retains
that pair for its full lifetime. Multi-page reads bind their resume cursor to
the admitted source and fail closed if publication advances between pages.

The supported topology is `rocksdb-single-host`. Primaries and secondaries
share one host filesystem but remain separate processes and ownership domains.
RocksDB secondary mode is not a cross-host replication protocol.
[ADR-0035](../adrs/0035-canonical-storage-topologies.md) owns the topology
decision.

PostgreSQL modules under `zinder-bench` persist canonical replay records as a
benchmark corpus for diagnostics. They do not establish a runtime service,
wallet-store implementation, replication contract, or supported deployment.

## Optional explorer boundary

`zinder-explorer` owns `ExplorerQuery` translation. It reads the
materialized-view store as a secondary and composes selected `WalletQuery`
calls over gRPC; it does not open the canonical store. `zinder-materialized-views` owns explorer projection consumers and
their schemas. Explorer state is never a prerequisite for canonical writes or
wallet projection correctness.

`zinder-compat-cipherscan` translates external REST and WebSocket contracts
onto separately composed `ExplorerQuery` and `WalletQuery` endpoints. It owns
product shapes and bounded caches, not chain data. The release topology
provides WalletQuery but not the optional ExplorerQuery endpoint.

## Anti-patterns

- A runtime that owns both public query load and a primary chain store.
- A query handler that fetches missing indexed history from the node.
- A second canonical or wallet writer for the same store.
- A reader that opens a live primary or mutates storage during admission.
- A compatibility adapter that mixes canonical and wallet generations.
- An explorer aggregate required for wallet sync.
- A generic database or service abstraction that hides the owning domain.
