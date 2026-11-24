# ADR-0006: Keep node access behind one source boundary

Status: Accepted

## Context

Zinder reads the chain from a Zcash full node, Zebra or Zakura, and serves it through its own protocols. The two boundaries sit next to each other but change for different reasons. If they share types or owners, a node transport type can leak into canonical storage, a query handler can quietly fall back to the node, and a compatibility schema can shape the native API.

Fresh construction also needs more than one block per call. A round trip per block leaves the writer idle, while an unbounded range call can exceed node, network, or process memory limits. The fetch must also notice when the node's chain changed between the caller's cursor and the returned blocks.

## Decision

1. **`zinder-source` owns all node communication.** Fetching blocks, reading the mempool, receiving tip notifications, and broadcasting transactions sit behind traits in that crate. Only `zinder-source` depends on node client libraries, JSON-RPC libraries, and node response types. Its adapters return normalized Zinder values and typed source errors.
2. **`zinder-proto` owns every service protocol.** It holds the native wallet, Explorer, ingest control, and operations protobufs and the vendored lightwalletd protocol. `zinder-core` exposes no generated protobuf types. `zinder-store` exposes protobuf codecs only for the messages it persists or streams, such as the stored `CompactBlock` ([ADR-0002](0002-serialization-and-byte-order.md)).
3. **Capabilities are discovered by behavior.** An adapter reports what the node can do, such as serving full history or a mempool stream. A version string is diagnostic, not the compatibility contract. Ingest refuses a node that lacks a required capability, which is how it refuses a pruned node.
4. **Consensus bytes are parsed with `zebra-chain`.** Raw blocks and transactions are decoded with Zebra's primitives, so Zinder and the node agree on consensus encoding.
5. **Readers never use the node as a fallback for indexed data.** Query and compatibility runtimes serve blocks, tree states, and activations from Zinder's stores. They call the node only to broadcast transactions, to read the recommended fee for `GetStandardFee`, to verify each broadcast endpoint, to probe node health for readiness, and, in the light-wallet compatibility composition, for its node-backed reads ([ADR-0023](0023-deployment-profiles.md)). Those reads are checked against the served tip ([ADR-0003](0003-epoch-bound-reads-and-storage-roles.md) decision 7). No node answer stands in for data a store is meant to hold. When stored data is missing, the request fails explicitly.
6. **Historical fetches are bounded segments.** A segment request carries a cursor, a maximum block count, and a maximum response size. Every adapter must:
   - observe the node's tip before choosing its range;
   - return a revert when the cursor block is no longer on the node's selected chain;
   - check that the returned blocks form one parent-linked sequence;
   - enforce both limits; and
   - return typed errors for unavailable blocks, incoherent observations, protocol mismatches, and oversized responses.
7. **The JSON-RPC adapter fetches a segment as one batch.** It sends one height-keyed `getblock` request per block in a single batch and skips a separate hash lookup, because the decoded block carries its own hash and parent hash. Zakura refuses batches above 1,000 entries, so ingest refuses a segment size above 1,000 at configuration load. The writer adapts the segment size below that limit from observed response sizes.
8. **A source kind is supported only when a runtime wires it.** Ingest constructs the chain only through the JSON-RPC adapter. An indexer gRPC block adapter exists as a library, but it is not a configuration option. When configured, the indexer gRPC endpoint carries streaming mempool updates.

Continuous following uses one block update at a time rather than treating a historical segment as a live stream ([ADR-0009](0009-ingest-lifecycle-and-construction.md)). [Node source boundary](../architecture/node-source-boundary.md) describes the adapters, the capability model, and node authentication.

Verified snapshot bootstrap uses the configured `NodeSource` to attest the chain identity, activation schedule, final trees, subtree roots, and sampled block commitments before publication. [ADR-0024](0024-verified-edge-snapshot-bootstrap.md).

## Evidence

- Decision 5, the light-wallet composition's node-backed reads: judgment on measured cost. [ADR-0023](0023-deployment-profiles.md) decision 4 records the measurements, and [ADR-0003](0003-epoch-bound-reads-and-storage-roles.md) decision 7 records the verification of each answer.

## Consequences

- Node transports can change without changing wallet or Explorer APIs.
- Native protocol vocabulary is not copied from compatibility protocols unless the concepts are identical.
- Readers keep serving stored data while the node is down, and a request that needs missing data fails instead of waiting on the node. A node-backed read fails while the node is down.
- Every new protocol message and every new node capability has one owner.
- Construction amortizes request overhead across a bounded segment and detects reorgs through typed updates and errors, not partial batches.
- Adding a new node kind takes an adapter plus explicit runtime wiring and validation.

## Alternatives rejected

- **Letting query handlers call the node for missing data.** It hides storage gaps, ties reader availability to the node, and returns answers that are not bound to the reader's chain epoch.
- **Exposing node response types to storage or query crates.** A node release would then change Zinder's storage and public APIs.
- **Version-based compatibility.** A version string does not say what a node actually serves, and Zebra and Zakura share protocols across different version lines.
- **One block per call during construction.** Round-trip latency would leave the writer idle.
- **Unbounded range calls.** One call could exceed node, network, or process memory limits.
- **A `getblockhash` call before each `getblock`.** The decoded block already provides the identity and continuity checks, so the extra call only adds a round trip.

## Revision history

- 2026-11-24. Linked ADR-0024 for validator attestation before snapshot publication.

- 2026-11-23. Decision 5 adds the light-wallet composition's node-backed reads to the reasons a reader calls the node.
- 2026-11-23. Evidence records what decision 5 rests on.
