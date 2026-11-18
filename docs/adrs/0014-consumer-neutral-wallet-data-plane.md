# ADR-0014: Wallets read one consumer-neutral data plane from stored artifacts

Status: Accepted

## Context

A store built close to the node's tip can pass lightwalletd smoke tests and a new-wallet flow, then fail when a wallet asks for history. Recovery, rescan, and imported accounts need tree state at arbitrary anchors, subtree roots from index zero, and transparent outputs created at any height. Zodl and the Android SDK exposed the gap first, but Zallet, full-node wallets, and native Rust applications need the same data.

Two risks pull in opposite directions. Designing around one wallet produces app-specific patches and no durable contract. Becoming lightwalletd makes its wire vocabulary Zinder's identity, when compatibility is only one way in.

Several optional fields, such as a fee that needs resolved inputs or a raw transaction that needs retained blobs, depend on what a deployment stored or wired. A consumer has to tell "absent" from "zero" without guessing.

## Decision

1. **The product boundary is a consumer-neutral wallet data plane.** `zinder-ingest` indexes canonical artifacts once. Native consumers read them through typed, epoch-pinned gRPC or Rust surfaces. `zinder-compat-lightwalletd` is an adapter over the same artifacts and owns no storage, query semantics, or vocabulary. Native method names and errors may differ from lightwalletd's where that serves consumers better. Wallet compatibility is a coverage contract, never an app-specific workaround, and no profile is named after a consumer.
2. **The core contract is fixed.** The data plane serves compact block ranges, tree state at explicit anchors, subtree roots by pool and index, and transparent outputs by address and height. Every read is consistent with one chain epoch. Readiness and artifact availability are reported truthfully.
3. **Wallet-serving coverage is complete history.** A store built for wallet serving keeps all canonical history from height 1. Shielded activation heights cannot bound transparent history, because a transaction at any height can spend an output created earlier. Wallet-serving construction rejects a checkpoint-height override, and ingest refuses an existing store whose retained history starts later than this requires. Following cannot repair omitted history, so the operator rebuilds.
4. **Reads come from stored artifacts only.** A missing artifact is `ARTIFACT_UNAVAILABLE`. Query runtimes never fill a response from the node. Tree state at a height without a stored checkpoint is reconstructed from stored data, and a failed reconstruction is store damage, not a reason to ask the node. Concurrent reconstructions are capped, and excess requests are refused instead of queued; [Wallet data plane](../architecture/wallet-data-plane.md#tree-states-and-subtree-roots) gives the cadence and limits.
5. **Optional payload fields use explicit presence.** A field that a deployment may be unable to fill is a proto3 `optional` scalar or a `repeated` field. Absence is never encoded as zero or empty bytes. When absence has more than one cause that a reader would act on differently, the message carries a companion status enum. A named capability string says the field is filled on this deployment.
6. **Capabilities follow evidence.** A capability for an optional field is advertised only when the deployment can fill it, judged from persisted facts. Raw-block and raw-transaction capabilities follow the raw-blob retention the store recorded at construction. Completeness capabilities for materialized views require verified coverage up to the view tip. A runtime advertises one capability set through every discovery surface.

[Wallet data plane](../architecture/wallet-data-plane.md) describes what each method returns. [Capabilities](../reference/capabilities.md) lists the capability strings.

## Consequences

- Compatibility work has one general target instead of per-app exceptions.
- Operators have one serving posture: build a complete store, then run readers against it.
- Missing history becomes a deployment or readiness failure, not a hidden query fallback.
- A running reader keeps serving tree state, blocks, and transparent reads while the node is down ([ADR-0016](0016-wallet-serving-readiness.md)).
- Serving stores are larger and slower to build than near-tip stores. Local tests use disposable stores instead of a near-tip construction.
- A tree-state read between checkpoints replays at most 99 stored blocks.
- Adding an optional field touches the proto, the capability registry and its tests, and the handler that fills it, in one change.
- Consumers branch on field presence and on `ServerInfo` capabilities, never on sentinel values.

## Alternatives rejected

- **Fall back to the node for missing artifacts.** It blurs the source of truth, makes readiness lie, and turns query runtimes into partial node proxies. Repair tools may use the node to rebuild artifacts. Public reads may not.
- **A per-wallet coverage profile.** It recreates the app-specific design this decision avoids.
- **Bounded-history serving stores.** Narrowing coverage needs proven demand. The first stable profile favors correct wallet behavior.
- **Answer `GetTreeState` by proxying the node, as lightwalletd does.** Tree-state reads would then fail during node outages.
- **Store a tree-state frontier for every block.** It adds a row per block, needs a rebuild, and saves at most 99 blocks of replay per read.
- **Sentinel values for absent fields.** A consumer cannot tell "unresolved" from a real zero fee or balance.
