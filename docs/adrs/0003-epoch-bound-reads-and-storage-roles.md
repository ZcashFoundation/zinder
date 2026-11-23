# ADR-0003: Bind every read to one chain state and give each store one writer

Status: Accepted

## Context

A wallet sync or explorer request often reads several artifacts for the same range of blocks. If a reorg or a new tip lands between those reads, one response could mix two different best chains. The client would then scan or display data that never existed together.

Zinder also runs as separate processes. `zinder-ingest` writes the canonical store, `zinder-projector` writes the wallet store, and the query, compatibility, and Explorer runtimes read both. Those readers need fresh data without sharing an in-process RocksDB handle and without opening a second writer.

## Decision

1. **Every read of stored data is pinned.** Before it reads artifacts, a read binds to one chain epoch, which is an immutable published view of the canonical chain, or to one canonical event fence, which is a position in the chain-event history. The read finishes against that state even after a newer epoch is published. A node-backed read follows decision 7 instead.
2. **Readers fail closed.** A read refuses to answer when its epoch or fence is no longer available, the store no longer meets the reader's admission rule ([ADR-0004](0004-storage-and-deployment-topologies.md)), the schema version is unsupported, or a secondary cannot catch up within its configured bound. It never falls back to an unpinned read, and it never asks the node for data its store is meant to hold.
3. **Each store has exactly one writer.** `zinder-ingest` owns the canonical primary, and `zinder-projector` owns the wallet primary. Every other process opens a RocksDB secondary with its own metadata directory, and a secondary never mutates the primary. A writer process may also hold secondaries of its own store, as ingest does to build materialized views.
4. **Persisted evidence admits data, not notifications.** A secondary catches up, then validates its admission rule before it serves. A writer requires the exact store identity, and a reader requires the data it reads ([ADR-0004](0004-storage-and-deployment-topologies.md) decision 2). A message from the writer may trigger a catch-up, but it does not replace the persisted evidence.
5. **Responses that combine stores read one admitted pair.** A response that needs both canonical and wallet state reads one serving pair, a canonical and a wallet secondary admitted together at the same fence ([ADR-0015](0015-wallet-runtime-composition.md)).
6. **Construction ownership survives a crash.** A projector's construction leases belong to its configured build-owner identity. A restarted projector with that identity reclaims its own lease at once, at a strictly higher generation, and discards its unpublished store. A process-lifetime file lock beside the wallet path refuses a second live process with the same identity.
7. **Node-backed reads are bound to the served tip, not pinned.** The light-wallet compatibility composition answers some lightwalletd methods from the node ([ADR-0023](0023-deployment-profiles.md)). Each such request captures the reader's served tip before it calls the node. Its answer describes that tip, or the request fails:
   - Transaction lookup follows the admitted store's persisted retention. When the store retains transaction bodies, a mined transaction is read locally, and a body missing from such a store is an unavailable artifact, not a reason to call the node. Otherwise the canonical store, at the captured epoch, locates the transaction and its recorded digest, and the node supplies only the bytes. The node must report the same block, and the bytes must match the digest. Bytes that do not match the verified block fail with `DATA_LOSS`. A transaction the served chain does not mine gets the answer stored-data serving gives, after the node is verified against the served tip.
   - Unspent outputs are exact at the served tip. Every node page must report that tip, including each reread at a request's last height. A balance is the sum of those outputs. A page at any other tip fails the request with `UNAVAILABLE`.
   - Address history is bounded to the served tip but best-effort. The node decides which transactions belong to an address. The reader checks that each returned row is a transaction the served chain mines in the block the node reports, in canonical order, with bytes matching the recorded digest. It cannot check that the list is complete: rows can be missing if the node leaves and rejoins the served chain during one request, or if the node's address index is incomplete. A history stream that fails can already have delivered a valid prefix of rows.
   - A request that needs a node that does not answer fails with `UNAVAILABLE`, never with an empty or zero answer. The node can also refuse a request for its own reasons, such as a missing address index or an oversized answer, and the request fails with a matching status. Reads of stored data keep serving.

The lightwalletd `GetBlockRange` stream is the one composed read across epochs. It reads each chunk at the epoch current at that moment and proves by hash linkage that every block it sends belongs to one chain ([ADR-0015](0015-wallet-runtime-composition.md)).

[Storage backend](../architecture/storage-backend.md) describes the store roles and secondary catch-up, and [Wallet data plane](../architecture/wallet-data-plane.md) describes read consistency for clients. [Lightwalletd compatibility](../reference/lightwalletd-compatibility.md#runtime-serving-selection) lists which serving mode forwards which method.

## Consequences

- Every multi-artifact response carries one explicit chain identity.
- One process owns every write path of a store, so reorg authority and visibility have one owner.
- Reader freshness is observable and feeds readiness ([Readiness](../reference/readiness.md)).
- Operators give each reader process a unique secondary metadata directory and keep all primaries on one host filesystem ([ADR-0004](0004-storage-and-deployment-topologies.md)).
- A long-lived reader needs an explicit retention contract for the epochs and events it may still reference.
- A node-backed answer is exact for transaction bytes, unspent outputs, and balances at the served tip, and best-effort for address history. Exact transparent history needs a local address index, which the wallet deployment keeps, or a node reply bound to a chain tip.
- A node-backed read is only as available as the node, and its cost grows with the node calls each request makes ([ADR-0023](0023-deployment-profiles.md)).
- Two live projectors need distinct build-owner identities. A crashed projector resumes as soon as it restarts.

## Alternatives rejected

- **Several canonical writers.** They would compete over visibility and reorg decisions. Zinder keeps one writer and distributes authenticated events instead.
- **Unpinned reads.** They are simpler, but a reorg between two reads produces a response that matches no chain.
- **Fetching missing artifacts from the node.** It hides storage gaps behind node latency and node availability, and the node's answer is not bound to the reader's epoch. A node-backed read differs in both respects: the light-wallet composition declares it per method, so no stored artifact is missing, and decision 7 binds its answer to the served tip.
- **Forwarding node answers at the node's tip.** A node one block ahead of the reader returned a transaction, outputs, and a balance from a block the reader had not served, so one wallet session saw two chain states.
- **Releasing construction leases only on a clean exit.** A process killed by the out-of-memory killer or a host restart runs no cleanup, so every crash would stall construction for the whole lease duration.
- **Shorter construction leases.** A lease must outlive the longest construction phase, so `projector.lease_duration_seconds` has a four-hour floor. A shorter lease could drop a healthy build between heartbeats.

## Revision history

- 2026-11-23. Decisions 1, 2, and 4 apply pinning to reads of stored data and defer store admission to ADR-0004. Decision 7 binds node-backed reads to the served tip.
