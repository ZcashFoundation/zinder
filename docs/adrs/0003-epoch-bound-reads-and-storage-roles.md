# ADR-0003: Bind every read to one chain state and give each store one writer

Status: Accepted

## Context

A wallet sync or explorer request often reads several artifacts for the same range of blocks. If a reorg or a new tip lands between those reads, one response could mix two different best chains. The client would then scan or display data that never existed together.

Zinder also runs as separate processes. `zinder-ingest` writes the canonical store, `zinder-projector` writes the wallet store, and the query, compatibility, and Explorer runtimes read both. Those readers need fresh data without sharing an in-process RocksDB handle and without opening a second writer.

## Decision

1. **Every chain-dependent read is pinned.** Before it reads artifacts, a read binds to one chain epoch, which is an immutable published view of the canonical chain, or to one canonical event fence, which is a position in the chain-event history. The read finishes against that state even after a newer epoch is published.
2. **Readers fail closed.** A read refuses to answer when its epoch or fence is no longer available, the store identity differs, the schema version is unsupported, or a secondary cannot catch up within its configured bound. It never falls back to an unpinned read and never fetches a missing canonical artifact from the node.
3. **Each store has exactly one writer.** `zinder-ingest` owns the canonical primary, and `zinder-projector` owns the wallet primary. Every other process opens a RocksDB secondary with its own metadata directory, and a secondary never mutates the primary. A writer process may also hold secondaries of its own store, as ingest does to build materialized views.
4. **Persisted evidence admits data, not notifications.** A secondary catches up, then validates store identity and schema before it serves. A message from the writer may trigger a catch-up, but it does not replace the persisted evidence.
5. **Responses that combine stores read one admitted pair.** A response that needs both canonical and wallet state reads one serving pair, a canonical and a wallet secondary admitted together at the same fence ([ADR-0015](0015-wallet-runtime-composition.md)).
6. **Construction ownership survives a crash.** A projector's construction leases belong to its configured build-owner identity. A restarted projector with that identity reclaims its own lease at once, at a strictly higher generation, and discards its unpublished store. A process-lifetime file lock beside the wallet path refuses a second live process with the same identity.

The lightwalletd `GetBlockRange` stream is the one composed read across epochs. It reads each chunk at the epoch current at that moment and proves by hash linkage that every block it sends belongs to one chain ([ADR-0015](0015-wallet-runtime-composition.md)).

[Storage backend](../architecture/storage-backend.md) describes the store roles and secondary catch-up, and [Wallet data plane](../architecture/wallet-data-plane.md) describes read consistency for clients.

## Consequences

- Every multi-artifact response carries one explicit chain identity.
- One process owns every write path of a store, so reorg authority and visibility have one owner.
- Reader freshness is observable and feeds readiness ([Readiness](../reference/readiness.md)).
- Operators give each reader process a unique secondary metadata directory and keep all primaries on one host filesystem ([ADR-0004](0004-storage-and-deployment-topologies.md)).
- A long-lived reader needs an explicit retention contract for the epochs and events it may still reference.
- Two live projectors need distinct build-owner identities. A crashed projector resumes as soon as it restarts.

## Alternatives rejected

- **Several canonical writers.** They would compete over visibility and reorg decisions. Zinder keeps one writer and distributes authenticated events instead.
- **Unpinned reads.** They are simpler, but a reorg between two reads produces a response that matches no chain.
- **Fetching missing artifacts from the node.** It hides storage gaps behind node latency and node availability, and the node's answer is not bound to the reader's epoch.
- **Releasing construction leases only on a clean exit.** A process killed by the out-of-memory killer or a host restart runs no cleanup, so every crash would stall construction for the whole lease duration.
- **Shorter construction leases.** A lease must outlive the longest construction phase, so `projector.lease_duration_seconds` has a four-hour floor. A shorter lease could drop a healthy build between heartbeats.
