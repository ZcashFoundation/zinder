# ADR-0015: Wallet runtimes advertise what their composition proves

Status: Accepted

## Context

A wallet reader serves from a serving pair: a canonical secondary and a wallet secondary opened at the same canonical fence ([ADR-0003](0003-epoch-bound-reads-and-storage-roles.md)). A new pair is published after each block, and a request reads exactly one pair.

Capability discovery used to come from an implementation profile and from configured booleans for retention and providers. The advertised set could disagree with the persisted store, the installed handlers, or the wired dependencies. It advertised operations that could not work and omitted some that could.

Readers start without contacting the node, so whether a node can accept a broadcast is unknown when the process advertises its capabilities. A node can also sit on another network or chain, lag behind, or run an older release after an upgrade activates.

Wallet workflows span several requests. A transparent refresh across 30 receivers reads under one epoch pin for about 20 seconds, and blocks can arrive faster than that. Every retained pair keeps a canonical and a wallet secondary open, which costs memory and holds open files that the writer's compaction has deleted.

A lightwalletd `GetBlockRange` stream has no width limit, and the client sets its pace. A catch-up of 100,000 blocks keeps one stream open for minutes while blocks arrive. No bounded pin covers that.

## Decision

1. **One composition, one capability set.** The native wallet runtime has one production composition. After composition and admission it derives an immutable capability set from concrete evidence: the pair's persisted raw-blob retention, the methods the query implements, the configured node endpoints, and the admitted ingest-control methods. A deployment profile, or a serving setting, selects which composition a process runs ([ADR-0023](0023-deployment-profiles.md)), but neither is capability evidence. There are no capability profiles, setters, or support flags. The protocol's capability registry defines identifiers and versions, not which subset a process supports. `WalletQuery.ServerInfo` and the ops endpoint publish the same value. Each protocol adapter derives its own set, because a shared domain method is not a claim that a given adapter supports it.
2. **Adapters enforce what they advertise.** Every native RPC except `ServerInfo` checks its capability before it touches storage or a dependency. An advertised range method still has enforced limits.
3. **Support is fixed for the process lifetime.** A supported operation stays advertised while a dependency is down. Only requests that call the dependency fail ([ADR-0016](0016-wallet-serving-readiness.md)). Discovery is not a health check.
4. **The ingest-control identity is admitted once.** Native query, wallet-mode compatibility, and light-wallet compatibility admit `IngestControl` before opening storage or binding traffic. Admission requires the `zinder-ingest` identity, the same network and contract revision, and the required methods. Native query shares the admitted channel across components. Compatibility uses separate channels for mempool reads and the tip watcher.
5. **Broadcast is advertised from configuration and gated by verification.** `wallet.broadcast.transaction_v1` is advertised exactly when at least one node endpoint is configured. Each endpoint is verified in the background on first contact, and only verified endpoints receive transactions, so a transaction never reaches a node on another network or chain. [Node source boundary](../architecture/node-source-boundary.md#capabilities) lists the checks.
6. **Broadcast fails over in order and stays put.** A broadcast moves to the next verified endpoint only when one is unreachable or rejects the consensus branch, and the endpoint that answers becomes preferred, with no timed failback.
7. **Replaced pairs are retained by time, not by count.** A replaced pair stays readable for a fixed time however fast blocks arrive, so a pinned workflow's deadline does not depend on block rate. [Wallet data plane](../architecture/wallet-data-plane.md#how-reads-stay-consistent) gives the durations.
8. **Generations are bounded and recycled.** The publisher uses a fixed number of secondary generations. It catches a drained generation up in place instead of opening a new one, and reuses drained generations in turn. A published pair is never modified. When a read reports its reader invalidated, as the `RocksDB` reader does when it finds a table file deleted by compaction, it marks its pair, the publisher replaces the pair at the writer's fence, and the read runs once more only on a pair that answers the same epoch.
9. **Compatibility block ranges bind each chunk, not the stream.** A chunk holds at most 1,000 blocks and at most 2 MiB of stored compact blocks, and the first at most 256 KiB, so a stream opens after a small read. A single block larger than its chunk's limit forms a chunk alone. The adapter holds at most two chunks of a stream: the one it hands to the gRPC server and the next, read meanwhile; blocks the gRPC server has taken can still wait in its send buffers. Each chunk is read at the current view and holds no pair while waiting for the client. A chunk continues the stream only if it links by hash to the last block sent. Otherwise the stream ends with `ABORTED`, or with `OUT_OF_RANGE` when the tip fell below a height still to send. These are the codes lightwalletd returns for the same cases.
10. **Consumers own their requirements.** A native consumer reads `ServerInfo` and reports every missing capability before it touches wallet storage. Zinder's contract tests prove its own invariants. Real consumer binaries, not fixture parity tests, certify consumers.
11. **The compatibility adapter owns signed settled history.** It can derive and serve the default shielded protobuf from an immutable projection while canonical storage remains authoritative for coverage and chain identity. A signed catalog binds each segment to its network, artifact schema, interval, terminal block, and content digest. The adapter authenticates the serving descriptor against the signed whole-file digest or the signed fs-verity measurement before trusting the segment index. The [serving contract](../reference/lightwalletd-compatibility.md#signed-settled-history-and-standalone-live-tail-buffer) defines publication and admission.
12. **The single-host wallet deployment follows its publisher.** The compatibility reader may begin serving from canonical storage after it verifies the publisher's signed empty checkpoint using a separately provisioned key. It switches covered shielded ranges to the signed catalog after verifying the segment index and settled chain anchor. The wallet RPC and its advertised capabilities stay the same throughout. A reader configured with an explicit snapshot manifest must authenticate that catalog before binding.
13. **The wallet listener and its consumer size HTTP/2 windows for WAN round trips.** `PublicGrpcServer` raises each stream's send buffer to 1 MiB, h2's per-stream backpressure threshold, so a block-range stream can fill a 100-200 ms bandwidth-delay product instead of stalling at hyper's ~400 KiB default; the send-stall timeout still bounds how long a stalled stream keeps that buffer. `zinder-client`'s `RemoteChainIndex` advertises fixed 4 MiB stream and 16 MiB connection receive windows and a 64 KiB frame size, so a 90 ms round trip is not capped near the 65,535-byte spec window; it does not enable h2's adaptive window, because in tonic 0.14.9 and hyper 1.12.0 that setting resets both initial windows back to 65,535 bytes and would discard the fixed sizes. How the listener keys each client for the receive limit, including a bare IP a load balancer names, is in [ADR-0007](0007-node-failure-recovery-and-transport-policy.md) Decision 10.

[Wallet data plane](../architecture/wallet-data-plane.md) describes pinning and pair publication. [Lightwalletd compatibility](../reference/lightwalletd-compatibility.md) lists the block-range outcomes.

## Evidence

- Decision 1, deployment profiles and serving settings are not capability evidence: judgment. The rejected alternative on capability profiles records the reason.
- Decision 4, ingest-control admission: judgment. Checking the service before storage admission refuses an endpoint with the wrong identity, network, or contract before it can supply writer evidence.

## Consequences

- Operators choose a deployment profile, retention, and endpoints, and none of them is a capability claim. Invalid capability claims cannot be constructed.
- A misconfigured node endpoint is still advertised. Every broadcast through it fails with the retryable `NODE_UNAVAILABLE`, and the operator sees a degraded dependency instead of a missing capability.
- A client that read `ServerInfo` early never loses a capability later.
- Changing raw-blob retention needs a new store, not a restart.
- A workflow pinned to one epoch survives new blocks for one minute. Longer workflows get a typed expiry and restart.
- Retention and slow reads delay a publication by at most about 90 seconds and never stop it.
- A block-range stream can end early during a reorg in the unsettled suffix. The client requests again from the height it needs. An `OK` stream is one chain as read, not a promise that the chain is still best.
- A new capability needs a registry entry, an implementation, evidence derivation, positive and negative tests, and a production consumer flow.

## Alternatives rejected

- **Capability profiles, or per-capability booleans.** A name or flag that declares capabilities keeps a shadow model that can disagree with the composed service. A deployment profile avoids this because capabilities still derive from the composition it selects.
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
- **Compatibility aliases for removed names.** Zinder's native consumers move with it, and aliases keep obsolete names available for new code.

## Revision history

- 2026-11-21. Added the trust and ownership boundary for pre-encoded settled lightwalletd history.
- 2026-11-21. Bound the compact segment index and the optional kernel measurement to the signed catalog.
- 2026-11-21. Specified trusted bootstrap and automatic catalog adoption for the single-host wallet Compose deployment.
- 2026-11-23. Decision 1 separates deployment profiles, which select a composition, from capability evidence.
- 2026-11-23. Decision 11 links the current signed settled-history section.
- 2026-11-23. Evidence records what decision 1 rests on.
- 2026-11-23. Decision 4 specifies which readers admit ingest control and how they use its channels.
- 2026-11-24. Decision 13 sizes the wallet listener's and consumer's HTTP/2 windows for WAN bandwidth-delay products, from the 90 ms round-trip evidence that the defaults were the binding constraint.
