# ADR-0015: Wallet runtimes advertise what their composition proves

Status: Accepted

## Context

A wallet reader serves from a serving pair: a canonical secondary and a wallet secondary opened at the same canonical fence ([ADR-0003](0003-epoch-bound-reads-and-storage-roles.md)). A new pair is published after each block, and a request reads exactly one pair.

Capability discovery used to come from an implementation profile and from configured booleans for retention and providers. The advertised set could disagree with the persisted store, the installed handlers, or the wired dependencies. It advertised operations that could not work and omitted some that could.

Readers start without contacting the node, so whether a node can accept a broadcast is unknown when the process advertises its capabilities. A node can also sit on another network or chain, lag behind, or run an older release after an upgrade activates.

Wallet workflows span several requests. A transparent refresh across 30 receivers reads under one epoch pin for about 20 seconds, and blocks can arrive faster than that. Every retained pair keeps a canonical and a wallet secondary open, which costs memory and holds open files that the writer's compaction has deleted.

A lightwalletd `GetBlockRange` stream has no width limit, and the client sets its pace. A catch-up of 100,000 blocks keeps one stream open for minutes while blocks arrive. No bounded pin covers that.

## Decision

1. **One composition, one capability set.** The native wallet runtime has one production composition. After composition and admission it derives an immutable capability set from concrete evidence: the pair's persisted raw-blob retention, the methods the query implements, the configured node endpoints, and the admitted ingest-control methods. There are no profiles, setters, or support flags. The protocol's capability registry defines identifiers and versions, not which subset a process supports. `WalletQuery.ServerInfo` and the ops endpoint publish the same value. Each protocol adapter derives its own set, because a shared domain method is not a claim that a given adapter supports it.
2. **Adapters enforce what they advertise.** Every native RPC except `ServerInfo` checks its capability before it touches storage or a dependency. An advertised range method still has enforced limits.
3. **Support is fixed for the process lifetime.** A supported operation stays advertised while a dependency is down. Only requests that call the dependency fail ([ADR-0016](0016-wallet-serving-readiness.md)). Discovery is not a health check.
4. **Ingest control is admitted once.** Before opening storage or binding traffic, the reader connects to `IngestControl` once and requires the `zinder-ingest` identity, the same network and contract revision, and the methods it needs. Every component shares that channel.
5. **Broadcast is advertised from configuration and gated by verification.** `wallet.broadcast.transaction_v1` is advertised exactly when at least one node endpoint is configured. Each endpoint is verified in the background on first contact, and only verified endpoints receive transactions, so a transaction never reaches a node on another network or chain. [Node source boundary](../architecture/node-source-boundary.md#capabilities) lists the checks.
6. **Broadcast fails over in order and stays put.** A broadcast moves to the next verified endpoint only when one is unreachable or rejects the consensus branch, and the endpoint that answers becomes preferred, with no timed failback.
7. **Replaced pairs are retained by time, not by count.** A replaced pair stays readable for a fixed time however fast blocks arrive, so a pinned workflow's deadline does not depend on block rate. [Wallet data plane](../architecture/wallet-data-plane.md#how-reads-stay-consistent) gives the durations.
8. **Generations are bounded and recycled.** The publisher uses a fixed number of secondary generations. It catches a drained generation up in place instead of opening a new one, and reuses drained generations in turn. A published pair is never modified. When a read reports its reader invalidated, as the `RocksDB` reader does when it finds a table file deleted by compaction, it marks its pair, the publisher replaces the pair at the writer's fence, and the read runs once more only on a pair that answers the same epoch.
9. **Compatibility block ranges bind each chunk, not the stream.** A chunk holds at most 1,000 blocks and at most 2 MiB of stored compact blocks, and the first at most 256 KiB, so a stream opens after a small read. A single block larger than its chunk's limit forms a chunk alone. The adapter holds at most two chunks of a stream: the one it hands to the gRPC server and the next, read meanwhile; blocks the gRPC server has taken can still wait in its send buffers. Each chunk is read at the current view and holds no pair while waiting for the client. A chunk continues the stream only if it links by hash to the last block sent. Otherwise the stream ends with `ABORTED`, or with `OUT_OF_RANGE` when the tip fell below a height still to send. These are the codes lightwalletd returns for the same cases.
10. **Consumers own their requirements.** A native consumer reads `ServerInfo` and reports every missing capability before it touches wallet storage. Zinder's contract tests prove its own invariants. Real consumer binaries, not fixture parity tests, certify consumers.
11. **The compatibility adapter owns signed settled history.** It can derive and serve the default shielded protobuf from an immutable projection while canonical storage remains authoritative for coverage and chain identity. A signed catalog binds each segment to its network, artifact schema, interval, terminal block, and content digest. The adapter authenticates the serving descriptor against the signed whole-file digest or the signed fs-verity measurement before trusting the segment index. The [serving contract](../reference/lightwalletd-compatibility.md#signed-settled-history) defines publication and admission.
12. **The single-host wallet deployment follows its publisher.** The compatibility reader may begin serving from canonical storage after it verifies the publisher's signed empty checkpoint using a separately provisioned key. It switches covered shielded ranges to the signed catalog after verifying the segment index and settled chain anchor. The wallet RPC and its advertised capabilities stay the same throughout. A reader configured with an explicit snapshot manifest must authenticate that catalog before binding.
13. **Kernel-verified segments may be served from a memory mapping, by operator choice.** With `compat.shielded_history.map_verified_segments`, the reader maps a segment read-only once at admission when the kernel measured its fs-verity digest on the serving descriptor and the digest matched the signed pin. It serves each payload as a slice of that mapping instead of copying it from the page cache into memory. Portable segments always use positional reads. The setting is off by default.
    - **The `unsafe` boundary.** Creating the mapping is the only `unsafe` code in the workspace. It is sound because the mapped bytes cannot change: the kernel refuses to open an fs-verity file for writing or to truncate it, fs-verity cannot be removed from a file, and renaming or unlinking the file keeps its inode. The mapping is read-only, and each payload slice holds it until the transport drops the last slice. A portable file has none of these guarantees, so it is never mapped.
    - **Damage stops the process.** The kernel verifies each page when it reads it into memory. When reader code touches a mapped page that fails verification, or that the device fails to read, the kernel raises `SIGBUS` and the listener process stops, ending every stream it serves. Reader code touches each served row: it checks the frame lengths on every read, validates the payload on the first read, and copies the payload into the response. A positional read of the same page instead fails one stream with `DATA_LOSS`, and a page that only the kernel copies into a socket fails that write and ends one connection. The reader installs no `SIGBUS` handler, because resuming safely after a fault inside Rust code is not possible. The kernel verifies the page again each time it reads it in, so after a restart the next request that touches the damaged page stops the listener again, until the segment is replaced.

[Wallet data plane](../architecture/wallet-data-plane.md) describes pinning and pair publication. [Lightwalletd compatibility](../reference/lightwalletd-compatibility.md) lists the block-range outcomes.

## Consequences

- Operators configure retention and endpoints, not a support profile. Invalid capability claims cannot be constructed.
- A misconfigured node endpoint is still advertised. Every broadcast through it fails with the retryable `NODE_UNAVAILABLE`, and the operator sees a degraded dependency instead of a missing capability.
- A client that read `ServerInfo` early never loses a capability later.
- Changing raw-blob retention needs a new store, not a restart.
- A workflow pinned to one epoch survives new blocks for one minute. Longer workflows get a typed expiry and restart.
- Retention and slow reads delay a publication by at most about 90 seconds and never stop it.
- A block-range stream can end early during a reorg in the unsettled suffix. The client requests again from the height it needs. An `OK` stream is one chain as read, not a promise that the chain is still best.
- A new capability needs a registry entry, an implementation, evidence derivation, positive and negative tests, and a production consumer flow.
- An operator who enables segment mapping trades one copy of every served payload for the failure mode above: a damaged protected page takes down the listener rather than one stream.

## Alternatives rejected

- **Configurable or renamed profiles, or per-capability booleans.** They keep a shadow model that can disagree with the composed service.
- **Derive support from the registry alone.** The registry cannot prove that storage retained an artifact or that a provider was wired.
- **A generic capability-provider framework.** There is one composition root, so it adds indirection without removing any evidence binding.
- **Advertise broadcast only after verification.** The capability set would change at runtime, and clients that cached it would never see broadcast.
- **Exclude an endpoint after a consensus-branch rejection.** Any client could take broadcast offline by sending a transaction for the wrong branch.
- **Order endpoints by node health.** It would skip a reachable syncing node whose refusal Zinder cannot yet tell apart from a transaction rejection.
- **Serve only the current pair.** On a chain with 25-second blocks, about half of the 20-second receiver walks would fail.
- **Bound retention by a count of pairs.** It ties a pin's lifetime to block spacing. On Testnet, four blocks in 11 seconds cut pins after about six seconds. More pairs cost more open secondaries and still lose to a fast burst.
- **Let requests hold a released pair until they finish.** A client can stop reading without cancelling, so a few such reads would block publication for everyone.
- **Open a new generation for every publication.** Cold admission scans retained history and took about 25 seconds on Testnet, so many heights were never published.
- **Catch the published pair up in place.** It would move the fence under in-flight reads and pins.
- **Hold one pair for a whole block-range stream, or pin each chunk.** The client sets the pace, so no retention window covers it. The hash link, not a pin, proves continuity.
- **Send the new branch after a reorg below the stream position.** One `OK` stream would no longer be one chain.
- **Contain `SIGBUS` from a mapped segment with a signal handler.** Unwinding or jumping out of a fault inside Rust code is undefined behavior, and the handler would affect the whole process.
- **Prefault each mapped range with `MADV_POPULATE_READ` before serving it.** It needs more `unsafe` code, and the kernel can evict and re-read a page between the prefault and the send, so a damaged page can still raise `SIGBUS`.
- **Map portable segments.** Another process can truncate or rewrite a portable file, which raises `SIGBUS` or changes bytes the reader treats as immutable.
- **Map protected segments by default.** It would replace the per-stream failure that operators rely on today without their choice.
- **Compatibility aliases for removed names.** Zinder's native consumers move with it, and aliases keep obsolete names available for new code.

## Revision history

- 2026-11-21. Added the trust and ownership boundary for pre-encoded settled lightwalletd history.
- 2026-11-21. Bound the compact segment index and the optional kernel measurement to the signed catalog.
- 2026-11-21. Specified trusted bootstrap and automatic catalog adoption for the single-host wallet Compose deployment.
- 2026-11-23. Allowed serving kernel-verified segments from a read-only memory mapping as an opt-in, with its `unsafe` boundary and failure behavior.
