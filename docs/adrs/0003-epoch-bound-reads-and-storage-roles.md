# ADR-0003: Bind every read to one chain state and give each store one writer

Status: Accepted

## Context

A wallet sync or explorer request often reads several artifacts for the same range of blocks. If a reorg or a new tip lands between those reads, one response could mix two different best chains. The client would then scan or display data that never existed together.

Zinder also runs as separate processes. `zinder-ingest` writes the canonical store, `zinder-projector` writes the wallet store, and the query, compatibility, and Explorer runtimes read both. Those readers need fresh data without sharing an in-process RocksDB handle and without opening a second writer.

## Decision

1. **Indexed lookups of stored data are pinned.** Before it reads artifacts, a lookup binds to one chain epoch, which is an immutable published view of the canonical chain, or to one canonical event fence, which is a position in the chain-event history. The lookup finishes against that state even after a newer epoch is published. Reads that span views follow the paragraph after the decisions, and a node-backed read follows decision 7.
2. **Readers fail closed.** A read refuses to answer when its epoch or fence is no longer available, the store no longer meets the reader's admission rule ([ADR-0004](0004-storage-and-deployment-topologies.md)), the schema version is unsupported, or a secondary cannot catch up within its configured bound. It never falls back to an unpinned read, and it never asks the node for data its store is meant to hold.
3. **Each store has exactly one writer.** `zinder-ingest` owns the canonical primary, and `zinder-projector` owns the wallet primary. Every other process opens a RocksDB secondary with its own metadata directory, and a secondary never mutates the primary. A writer process may also hold secondaries of its own store, as ingest does to build materialized views.
4. **Persisted evidence admits data, not notifications.** A secondary catches up, then validates its admission rule before it serves. A writer requires the exact store identity, and a reader declares its minimum retained data and history ([ADR-0004](0004-storage-and-deployment-topologies.md) decision 2). A message from the writer may trigger a catch-up, but it does not replace the persisted evidence.
5. **Responses that combine stores read one admitted pair.** A response that needs both canonical and wallet state reads one serving pair, a canonical and a wallet secondary admitted together at the same fence ([ADR-0015](0015-wallet-runtime-composition.md)).
6. **Construction ownership survives a crash.** A projector's construction leases belong to its configured build-owner identity. A restarted projector with that identity reclaims its own lease at once, at a strictly higher generation, and discards its unpublished store. A process-lifetime file lock beside the wallet path refuses a second live process with the same identity.
7. **Node-backed reads use the captured canonical view.** The light-wallet compatibility composition answers some lightwalletd methods from the node ([ADR-0023](0023-deployment-profiles.md)). Each such request captures the reader's served tip before it calls the node. Its answer describes that tip, or the request fails:
   - Transaction lookup follows the admitted store's persisted retention. When the store retains transaction bodies, a mined transaction is read locally, and a body missing from such a store is an unavailable artifact, not a reason to call the node. Otherwise the canonical store, at the captured epoch, locates the transaction and its recorded digest in one location lookup, and the node supplies only the bytes. The node must report the same block, and the bytes must match the digest. Missing digests and mismatched bytes fail with `DATA_LOSS`. Lean wallet serving uses the same verification for mined bytes, including epoch-pinned lookups. The captured pair remains the authority for membership and absence. On a canonical miss, a store without transaction bodies verifies the forwarding node against the served tip before it consults ingest's live mempool; a store that retains bodies consults the mempool directly. A pending hit is a live answer. An absence is reported only when the mempool answered at the epoch the lookup captured.
   - Unspent outputs are exact at the served tip. Every node page must report that tip, including each reread at a request's last height. A balance is the sum of those outputs. A page at any other tip fails the request with `UNAVAILABLE`.
   - Address history is bounded to the served tip but best-effort. The node decides which transactions belong to an address, and each page of IDs is bracketed by checks of the node's hash at the served tip height. The reader checks that each returned row is a transaction the served chain mines inside the page's height range, in canonical order, with bytes matching the recorded digest. It cannot check that the list is complete: rows can be missing if the node leaves and rejoins the served chain during one request, or if the node's address index is incomplete. A history stream that fails can already have delivered a valid prefix of rows.
   - A request that needs a node that does not answer fails with `UNAVAILABLE`, never with an empty or zero answer. The node can also refuse a request for its own reasons, such as a missing address index or an oversized answer, and the request fails with a matching status. Reads of stored data keep serving.
   - The node's capabilities and upgrade schedule are verified once per basis and reused for up to one minute. A failed tip check, an HTTP 429 refusal, or an unreachable node fails the requests that waited on it but never discards that verification. A separate check that the node's best chain holds the served tip runs only where the node's reply does not bind itself to the served view: before a canonical miss consults the mempool, before `SendTransaction` and `GetStandardFee`, and at the end of an address-history stream. A mined transaction body needs none, because the node names the mining block and the bytes must match the recorded digest. Unspent outputs need none, because every page reports the tip it answered at. Address-history rows need none, because each binds to the served view through its local location and digest. Concurrent requests checking the same tip share one node call, and no answer is reused after that call returns, so every request still observes the node at or after its own arrival.

8. **Wide native compact ranges bind to the opening chain view.** This is the
   ratified contract for [#636](https://github.com/ZcashFoundation/zinder/issues/636).
   Revision 16 still reads a bounded range against one admitted serving pair.
   For a wide stream, `header_epoch` fixes the settled boundary and the requested
   chain. Heights at or below `header_epoch.settled_tip` remain immutable across
   later serving epochs. The stream reads them from current admitted pairs
   without holding a retired serving-pair pin. This remains true through an
   in-place reorg-window raise. A reader must restart with the new window before
   it can admit a post-raise pair, as required by [ADR-0010](0010-reorg-window-and-displaced-blocks.md)
   decisions 2 and 4.

   At stream open, capture a compact `(height, hash)` fence from the opening
   pair for each requested height above `header_epoch.settled_tip`. The fence
   contains at most `reorg_window_blocks` entries. Release the opening pair
   before a slow settled scan can retain it. Each tail read must match its
   captured hash before delivery. A mismatch or a height that can no longer
   be verified ends the stream with `CHAIN_EPOCH_PIN_UNAVAILABLE`. Settled
   items already delivered remain valid.

Lightwalletd compatibility block ranges, including `GetBlockRangeNullifiers`, read each chunk at the epoch current at that moment and join chunks by hash linkage, which proves that every block sent belongs to one chain ([ADR-0015](0015-wallet-runtime-composition.md)). Light-wallet subtree-root streams bound their roots to the starting tip and check that it is still canonical before reading each page from the current generation. Event subscriptions follow [ADR-0013](0013-chain-event-streams.md).

[Storage backend](../architecture/storage-backend.md) describes the store roles and secondary catch-up, and [Wallet data plane](../architecture/wallet-data-plane.md) describes read consistency for clients. [Lightwalletd compatibility](../reference/lightwalletd-compatibility.md#runtime-serving-selection) lists which serving mode forwards which method.

A separate canonical writer can initialize from a publisher checkpoint after private READY admission and validator attestation. The receiver rotates its cursor authentication key before taking ownership. [ADR-0024](0024-verified-edge-snapshot-bootstrap.md).

## Evidence

- Decision 8: Judgment. Settlement is monotonic under [ADR-0010](0010-reorg-window-and-displaced-blocks.md), so an opening hash per requested unsettled height is enough to preserve the opening view without tying reader generation lifetime to client speed. A changed settled identity or a delivered tail item that differs from its opening fence reopens the design.
- Decision 1 and the paragraph after the decisions: judgment. Pinning an indexed lookup keeps one response on one chain. A block range or an event subscription is paced by its client, so no retained epoch covers it, and it proves continuity by hash linkage or by cursor instead ([ADR-0015](0015-wallet-runtime-composition.md) decision 9, [ADR-0013](0013-chain-event-streams.md)). The tip check lets light-wallet subtree streams keep their starting bounds without holding a reader generation for the whole stream. A delivered response that joins two chains reopens it.
- Decisions 2 and 4: judgment. They defer store admission to [ADR-0004](0004-storage-and-deployment-topologies.md) decision 2, whose evidence applies.
- Decision 7, binding forwarded transactions, unspent outputs, and balances to the served tip: measured. On Regtest with Zebra 7.1.0, a node one block ahead of the reader returned a transaction, 141 unspent outputs, and a balance that included a block the reader had not served ([#480](https://github.com/ZcashFoundation/zinder/issues/480)). With the binding, the same skew returned `NOT_FOUND` for that transaction and refused the balance and output calls, and each check's regression test fails with its guard disabled ([#481](https://github.com/ZcashFoundation/zinder/pull/481)).
- Decision 7, best-effort address history: external constraint. Neither Zebra 7.1.0 nor Zakura 1.8.6 reports the chain tip a `getaddresstxids` answer describes ([zebra#12575](https://github.com/ZcashFoundation/zebra/issues/12575)), so the reader can verify each returned row but not that the list is complete. A node reply bound to a tip reopens it.
- Decision 7, sharing and skipping tip checks: judgment, supported by tests. Each skipped check is replaced by evidence in the node reply that binds it to the served view, and the tests cover a node on another chain at the served height, a reorg inside the last history page, and concurrent checks of one tip. A forwarded mined `GetTransaction` makes one node call instead of two, and an `N`-row address history makes no per-row tip check instead of `N`. An unbound reply that skipped its check reopens it.
- Decision 7, outage behavior: measured. With the node stopped under load on Regtest, forwarded methods returned `UNAVAILABLE`, local payloads stayed byte-identical, and an address-history stream cut mid-way delivered 19 or 20 rows before `UNAVAILABLE` ([#475](https://github.com/ZcashFoundation/zinder/issues/475)).

## Consequences

- A stalled wide stream retains a bounded tail fence and relies on ordinary admission for subsequent pages. A reader restart may end an active RPC, and the client can resume from its own committed progress ([ADR-0015](0015-wallet-runtime-composition.md)).
- Every multi-artifact response carries one explicit chain identity.
- One process owns every write path of a store, so reorg authority and visibility have one owner.
- Reader freshness is observable and feeds readiness ([Readiness](../reference/readiness.md)).
- Operators give each reader process a unique secondary metadata directory and keep all primaries on one host filesystem ([ADR-0004](0004-storage-and-deployment-topologies.md)).
- A long-lived reader needs an explicit retention contract for the epochs and events it may still reference.
- A node-backed answer is exact for transaction bytes, unspent outputs, and balances at the served tip, and best-effort for address history. Exact transparent history needs a local address index, which the wallet deployment keeps, or a node reply bound to a chain tip.
- A node-backed read is only as available as the node, and its cost grows with the node calls each request makes ([ADR-0023](0023-deployment-profiles.md)).
- Two live projectors need distinct build-owner identities. A crashed projector resumes as soon as it restarts.

## Alternatives rejected

- **Retain the opening pair for the entire wide range.** A slow settled scan can outlast bounded pair retention and hold open files while newer generations need to publish.
- **Several canonical writers.** They would compete over visibility and reorg decisions. Zinder keeps one writer and distributes authenticated events instead.
- **Unpinned reads.** They are simpler, but a reorg between two reads produces a response that matches no chain.
- **Fetching missing artifacts from the node.** It hides storage gaps behind node latency and node availability, and the node's answer is not bound to the reader's epoch. A node-backed read differs in both respects: the light-wallet composition declares it per method, so no stored artifact is missing, and decision 7 binds its answer to the served tip.
- **Forwarding node answers at the node's tip.** A node one block ahead of the reader returned a transaction, outputs, and a balance from a block the reader had not served, so one wallet session saw two chain states.
- **Releasing construction leases only on a clean exit.** A process killed by the out-of-memory killer or a host restart runs no cleanup, so every crash would stall construction for the whole lease duration.
- **Shorter construction leases.** A lease must outlive the longest construction phase, so `projector.lease_duration_seconds` has a four-hour floor. A shorter lease could drop a healthy build between heartbeats.

## Revision history

- 2026-11-28. Decision 8 records settled reads and the opening tail hash fence for wide native compact ranges.
- 2026-11-24. Decision 7 shares the node schedule verification and served-tip checks across requests, and skips the tip check where the node reply binds itself to the served view.
- 2026-11-24. Decision 7 binds lean wallet byte retrieval to indexed transaction evidence.
- 2026-11-24. Linked ADR-0024 for initializing an independent writer from a verified snapshot.
- 2026-11-24. Decision 4 makes secondary storage requirements explicit.

- 2026-11-23. Decisions 1, 2, and 4 apply pinning to reads of stored data and defer store admission to ADR-0004. Decision 7 binds node-backed reads to the served tip.
- 2026-11-23. Decision 1 pins indexed lookups, and the paragraph after the decisions names compatibility block ranges, light-wallet subtree-root streams, and event subscriptions as reads that span views. Decision 7 states which lookups verify the node before consulting the mempool.
- 2026-11-23. Evidence records what decisions 1, 2, 4, and 7 rest on.
