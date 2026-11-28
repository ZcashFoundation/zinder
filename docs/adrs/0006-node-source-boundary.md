# ADR-0006: Keep node access behind one source boundary

Status: Accepted

## Context

Zinder reads the chain from an admitted Zcash validator and serves it through its own protocols. The two boundaries sit next to each other but change for different reasons. If they share types or owners, a node transport type can leak into canonical storage, a query handler can quietly fall back to the node, and a compatibility schema can shape the native API.

Fresh construction also needs more than one block per call. A round trip per block leaves the writer idle, while an unbounded range call can exceed node, network, or process memory limits. The fetch must also notice when the node's chain changed between the caller's cursor and the returned blocks.

## Decision

1. **`zinder-source` owns all node communication.** Fetching blocks, reading the mempool, receiving tip notifications, and broadcasting transactions sit behind traits in that crate. Only `zinder-source` depends on node client libraries, JSON-RPC libraries, and node response types. Its adapters return normalized Zinder values and typed source errors.
2. **`zinder-proto` owns every service protocol.** It holds the native wallet, Explorer, ingest control, and operations protobufs and the vendored lightwalletd protocol. `zinder-core` exposes no generated protobuf types. `zinder-store` exposes protobuf codecs only for the messages it persists or streams, such as the stored `CompactBlock` ([ADR-0002](0002-serialization-and-byte-order.md)).
3. **Capabilities are discovered by behavior.** An adapter reports what the node can do, such as serving full history or a mempool stream. A version string is diagnostic, not the compatibility contract. From-genesis construction requires archive history, confirmed by `pruned: false`. An unknown storage mode fails archive admission. The current ingest admission also requires archive history for live following. Decision 9 records the ratified extension.
4. **Consensus bytes are parsed with `zebra-chain`.** Raw blocks and transactions are decoded with Zebra's primitives across both bulk canonical construction and live tip-following, so Zinder and the node agree on consensus encoding across upgrades (`v1`–`v6` and future consensus branches). Rather than maintaining an in-tree custom structural wire parser, eliminating eager elliptic-curve decompression overhead during historical bulk parsing relies on contributing or adopting lazy curve-point decoding upstream in `zebra-chain`.
5. **Readers never use the node as a fallback for indexed data.** Query and compatibility runtimes serve blocks, tree states, and activations from Zinder's stores. They call the node only to broadcast transactions, to read the recommended fee for `GetStandardFee`, to verify each broadcast endpoint, to probe node health for readiness, and, in the light-wallet compatibility composition, for its node-backed reads ([ADR-0023](0023-deployment-profiles.md)). Those reads are checked against the served tip ([ADR-0003](0003-epoch-bound-reads-and-storage-roles.md) decision 7). No node answer stands in for data a store is meant to hold. When stored data is missing, the request fails explicitly.
6. **Historical fetches are bounded segments.** A segment request carries a cursor, a maximum block count, a maximum response size, and optional authenticated upstream-tip context. Every adapter must:
   - observe the node's tip before choosing its range, except when the request carries a caller-authenticated `(tip_height, reorg_window_blocks)` bound establishing that the entire requested range lies strictly below `tip_height - reorg_window_blocks` (adapters still observe the node's tip inside or at the reorg-window boundary, on short chains, during live following, and on any missing-block or parent-hash discontinuity);
   - return a revert when the cursor block is no longer on the node's selected chain;
   - check that the returned blocks form one parent-linked sequence;
   - enforce both limits; and
   - return typed errors for unavailable blocks, incoherent observations, protocol mismatches, and oversized responses.
7. **The JSON-RPC adapter fetches a segment as one batch.** It sends one height-keyed `getblock` request per block in a single batch and skips a separate hash lookup, because the decoded block carries its own hash and parent hash. Zakura refuses batches above 1,000 entries, so ingest refuses a segment size above 1,000 at configuration load. The writer adapts the segment size below that limit from observed response sizes.
8. **A source kind is supported only when a runtime wires it.** Ingest selects `zebra-json-rpc` (default) or `zebra-indexer-grpc` (`ZebraIndexerBlockSource`, paired with the JSON-RPC control plane and requiring `node.indexer_grpc_addr` plus any configured `node_fallbacks.<N>.indexer_grpc_addr`). When configured, the indexer gRPC endpoint also carries streaming mempool updates.

9. **An existing READY Schema v2 store can qualify for pruned following.** The store must already be verified and admitted, under any composition, lean or `--full-blocks`. The `reorg_safety_margin` is a positive block buffer of at least the store's effective reorg window (`reorg_safety_margin >= store.reorg_window`). Compute the recovery floor from the store's settled tip and first retained height.

   ```text
   required_recovery_floor = store.settled_tip.saturating_sub(reorg_safety_margin).max(store.first_retained_height)
   ```

   On short chains where `store.settled_tip <= reorg_safety_margin`, the floor clamps to `store.first_retained_height`. Admission must continuously verify `node.earliest_retained_height <= required_recovery_floor` and that the node covers the closed interval `[required_recovery_floor, node.tip]`. A pruned validator, including pruned Zakura, qualifies only after its prune horizon can be verified over RPC. [ADR-0007](0007-node-failure-recovery-and-transport-policy.md) defines the park rule. This is a ratified admission policy for [#713](https://github.com/ZcashFoundation/zinder/issues/713).

10. **An admitted validator is the sole upstream authority.** Tip observation, historical fetch, snapshot attestation, raw-transaction forwarding, and broadcast use admitted validator RPCs. Ordered validator failover follows [ADR-0007](0007-node-failure-recovery-and-transport-policy.md) and [ADR-0015](0015-wallet-runtime-composition.md). The proposed P2P `NODE_TREE_ROOTS` capability is outside this boundary. Reconsider it only when an admitted validator RPC exposes verifiable roots.

Continuous following uses one block update at a time rather than treating a historical segment as a live stream ([ADR-0009](0009-ingest-lifecycle-and-construction.md)). [Node source boundary](../architecture/node-source-boundary.md) describes the adapters, the capability model, and node authentication.

Verified snapshot bootstrap uses the configured `NodeSource` to attest the chain identity, activation schedule, final trees, subtree roots, and sampled block commitments before publication. [ADR-0024](0024-verified-edge-snapshot-bootstrap.md).

## Evidence

- Decision 3 and Decision 9, archive construction and pruned admission: External constraint. A pruned validator cannot provide a block below its retained horizon. RPC evidence of that horizon is a prerequisite for qualifying any pruned source.
- Decision 5, the light-wallet composition's node-backed reads: judgment on measured cost. [ADR-0023](0023-deployment-profiles.md) decision 4 records the measurements, and [ADR-0003](0003-epoch-bound-reads-and-storage-roles.md) decision 7 records the verification of each answer.
- Decision 4, retaining `zebra-chain` for both bulk construction and tip-following ([#602](https://github.com/ZcashFoundation/zinder/issues/602)): Judgment. Reaffirming `zebra-chain` preserves a single consensus-upgrade decoder aligned with Zinder's pinned node cohort and avoids maintaining an independent transaction wire parser in-tree, while keeping upstream lazy curve-point decoding in `zebra-chain` as the clean path to recover historical `block_parse` CPU cost once upstream support lands.
- Decision 9, the safety margin and recovery interval: Judgment. A margin of at least one effective reorg window reserves history below the settled tip, limited by the store's first retained height. Checking the full interval through the node tip detects gaps a horizon check alone can miss. A recovery request outside the admitted interval reopens the rule.
- Decision 10: Judgment. Validator RPCs preserve one admitted chain authority. A validator RPC that exposes verifiable note-commitment roots would reopen root sourcing. P2P `NODE_TREE_ROOTS` is an External constraint of the draft in [zcash/zips#1470](https://github.com/zcash/zips/pull/1470).

## Consequences

- Node transports can change without changing wallet or Explorer APIs.
- Native protocol vocabulary is not copied from compatibility protocols unless the concepts are identical.
- Readers keep serving stored data while the node is down, and a request that needs missing data fails instead of waiting on the node. A node-backed read fails while the node is down.
- Every new protocol message and every new node capability has one owner.
- Construction amortizes request overhead across a bounded segment and detects reorgs through typed updates and errors, not partial batches.
- Adding a new node kind takes an adapter plus explicit runtime wiring and validation.
- Pruned following requires verified retained-history coverage over RPC before it can be qualified. Archive construction still supplies missing history.

## Alternatives rejected

- **Letting query handlers call the node for missing data.** It hides storage gaps, ties reader availability to the node, and returns answers that are not bound to the reader's chain epoch.
- **Exposing node response types to storage or query crates.** A node release would then change Zinder's storage and public APIs.
- **Version-based compatibility.** A version string does not say what a node actually serves, and Zebra and Zakura share protocols across different version lines.
- **One block per call during construction.** Round-trip latency would leave the writer idle.
- **Unbounded range calls.** One call could exceed node, network, or process memory limits.
- **A `getblockhash` call before each `getblock`.** The decoded block already provides the identity and continuity checks, so the extra call only adds a round trip.
- **An in-process `zebra-network` adapter for Dandelion++ broadcast, P2P tip vouching, or raw-transaction fallback.** A peer can answer for a different epoch, weakening eclipse resistance. The peer stack exceeds the bounded process resource model in [ADR-0005](0005-bounded-process-memory.md), one client cannot provide meaningful Dandelion++ propagation, and peers do not guarantee historical transaction bytes. [#709](https://github.com/ZcashFoundation/zinder/issues/709) closed as `wontfix`.
- **An in-tree custom structural wire-slice parser for bulk canonical construction ([#602](https://github.com/ZcashFoundation/zinder/issues/602)).** Skipping `zebra_chain::Transaction::zcash_deserialize` in historical builds would reclaim eager elliptic-curve decompression CPU time immediately, but creates a parallel parser security surface that must track every ZIP-244 digest rule, transaction format, and future consensus branch across upgrades without divergence from `zebra-chain`. Maintaining lazy decoding upstream in `zebra-chain` preserves the single-decoder trust boundary without carrying git-forked dependencies or duplicated parsing rules.

## Revision history

- 2026-11-28. Reaffirmed Decision 4 (`zebra-chain` for both bulk construction and tip-following, deferring lazy curve-point decoding to upstream `zebra-chain`) and closed [#602](https://github.com/ZcashFoundation/zinder/issues/602).
- 2026-11-28. Reaffirmed validator-only authority and recorded the pruned live-follow admission gate.
- 2026-11-25. Clarified Decision 8 to wire `zebra-indexer-grpc` (`ZebraIndexerBlockSource`) alongside `zebra-json-rpc` (`ZebraJsonRpcSource`).
- 2026-11-25. Clarified Decision 6 to allow adapters to skip per-segment `tip_id()` polls strictly below caller-authenticated `tip_height - reorg_window_blocks` while preserving parent-hash linkage checks and in-window or discontinuity tip observation.
- 2026-11-24. Linked ADR-0024 for validator attestation before snapshot publication.
- 2026-11-23. Decision 5 adds the light-wallet composition's node-backed reads to the reasons a reader calls the node.
- 2026-11-23. Evidence records what decision 5 rests on.
