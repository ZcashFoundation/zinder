# ADR-0005: Consumer-Neutral Wallet Data Plane

| Field | Value |
| ----- | ----- |
| Status | Accepted |
| Product | Zinder |
| Domain | Wallet data plane, external wallet compatibility, typed client boundary |
| Related | [Wallet data plane](../architecture/wallet-data-plane.md), [Chain ingestion](../architecture/chain-ingestion.md), [Public interfaces](../architecture/public-interfaces.md), [Service operations](../architecture/service-operations.md) |

## Context

A Zinder store bootstrapped near the upstream-node tip can satisfy basic lightwalletd smoke probes and a new-wallet happy path, then fail later when a wallet asks for historical artifacts. Android SDK and Zodl expose the issue first, but the gap is not Android-specific: the same wallet data-plane needs appear across the local ecosystem.

- `lightwalletd` exposes `GetTreeState`, `GetSubtreeRoots`, `GetAddressUtxos`, and `GetAddressUtxosStream` as first-class `CompactTxStreamer` methods.
- Zallet's `wallet` code fetches birthday tree state at `birthday - 1`, loads Sapling and Orchard subtree roots from index `0`, and polls transparent UTXOs for wallet-owned transparent receivers.
- Full-node wallets and native Rust applications need snapshot semantics for atomic reads above the settled tip, whether they consume the `WalletQuery` wire protocol or the typed `ChainIndex` client.

The architectural risk is treating one wallet as the design center. App-specific
patches would leave Zinder without a durable consumer contract. The opposite
risk is becoming lightwalletd: compatibility is necessary, but it is not
Zinder's native product identity.

## Decision

Zinder's durable product boundary is a **consumer-neutral wallet data plane**:

- Canonical artifacts are indexed once by `zinder-ingest`.
- Native consumers use typed, epoch-pinned Rust or gRPC surfaces.
- `zinder-compat-lightwalletd` is an adapter over the same stored artifacts.
- External wallet compatibility is a coverage contract, not an app-specific workaround.

The core contract is:

1. Serve compact block ranges from canonical artifacts.
2. Serve tree state for explicit block anchors.
3. Serve subtree roots by shielded pool and subtree index.
4. Serve transparent UTXOs by address set and start height.
5. Preserve chain consistency through `ChainEpoch` and snapshot-like request pinning.
6. Report readiness and artifact availability truthfully.

### Wallet-serving coverage

`wallet-serving` is the operator-facing coverage profile for stores intended to serve wallet flows. It means the store was built with enough historical artifact coverage for wallet creation, recovery, rescan, imported-account, and transparent-UTXO flows supported by the published API.

`wallet-serving` is not a Zodl profile and not a lightwalletd profile. It is the conservative store posture for wallet consumers. It retains complete non-genesis canonical history. Shielded activation heights are sufficient for shielded tree data, but they cannot bound transparent history: a transaction mined at any later height can spend an output created before the earliest shielded activation. Complete history lets the global transparent projection resolve every predecessor without an unauthenticated prefix-state shortcut.

Serving coverage fails closed:

- `wallet-serving` rejects explicit `from_height` and `checkpoint_height` overrides.
- An existing or staged READY store is rejected when its immutable retained-history floor is later than the floor required by the current invocation. Following cannot repair omitted history; operators must rebuild the canonical and wallet stores.
- A serving store settles only outside the configured reorg window: construction publishes its settled tip `ingest.reorg_window_blocks` below the construction tip, and the canonical follower ingests the replaceable suffix.
- Missing artifacts remain `ArtifactUnavailable`. Query services do not synthesize responses from upstream nodes. `tree_state_at(height)` answers every retained height from stored artifacts: the stored checkpoint at that height, or a reconstruction from the nearest checkpoint below it and the stored compact blocks in between (see tree state at any height under Tradeoffs).
- Readiness does not claim production traffic is safe before secondary catchup and writer-status validation have established the reader's state.

### Compatibility and native surfaces

`zinder-compat-lightwalletd` implements `CompactTxStreamer` semantics as an
adapter. It preserves lightwalletd field names and wire behavior, but it does
not own storage, query semantics, or product vocabulary.

`WalletQuery` is the native protocol integration direction. Rust applications can use `zinder-client::ChainIndex`, while consumers that cannot link Zinder's Rust crates can generate their own client from the wire contract. Native method names, typed errors, and epoch-pinned variants may diverge from lightwalletd when that improves DX, UX, or AX. Compatibility and native-client readiness are validated separately.

### Naming

Use these terms consistently:

- `wallet birthday`: the user or wallet-provided lower bound for recovery/setup.
- `scan range start`: the start of a scanner-selected range.
- `tree-state anchor`: the block height whose tree state initializes a scan, commonly `scan_range_start - 1` or `birthday - 1`.
- `artifact coverage floor`: the earliest height or index available for an artifact family.
- `serving store`: a store built by fresh canonical construction with `wallet-serving` coverage and kept current by the canonical follower.

Do not use `birthday` as shorthand for every historical tree-state lookup. Many tree-state requests are scan anchors, not wallet birthdays.

## Consequences

Positive:

- Compatibility work has one general target instead of app-specific exceptions.
- Operators get one clear serving profile: build a serving store, then run readers against it.
- Wallet failures caused by insufficient historical coverage become deployment/readiness failures, not hidden query fallbacks.
- Native consumers get a typed surface without inheriting lightwalletd's wire vocabulary or, when using generated stubs, Zinder's internal dependency graph.
- Agents reason about coverage by artifact family and anchor height instead of guessing which wallet flow caused a lookup.

Negative:

- Initial serving stores are larger and slower to build than recent-checkpoint fixtures.
- Local test workflows use explicit disposable stores or the canonical follower rather than a near-tip fresh canonical construction.
- Full prevention of excessive transparent-UTXO materialization across many addresses requires a deeper multi-address store API; the aggregate response budget bounds the read until that lands.

Tradeoffs:

The fresh-construction coverage floor is intentionally conservative. A future profile can narrow coverage once the product has real demand for bounded historical ranges, but the first stable profile optimizes for correct wallet behavior and low operator ambiguity.

Upstream-node fallback is rejected. It would blur the source of truth, make readiness lie, and turn query services into partial node proxies. Repair tools may use upstream nodes to rebuild canonical artifacts, but public query methods read stored artifacts only.

Tree state at any height: a wallet running the canonical `zcash_client_backend` scan loop needs the tree state at exact range boundaries, which rarely land on a construction checkpoint. Construction stores a checkpoint every 100 blocks and at its build tip, and live following stores one for every block. `tree_state_at(height)` serves a stored checkpoint when one exists and otherwise seeds the commitment trees from the nearest checkpoint below the height and appends the note commitments of each stored compact block up to it. That is the derivation construction and live following check against the node's tree state before they store a block, so the result equals what the node reports for any block the store holds. Each replayed block must extend the one below it and reach the tree sizes its compact block records; a reconstruction that fails a check is store damage, not a reason to ask the node. A reconstruction replays at most 99 blocks, and each query runs at most one per available CPU at once; a request beyond that limit fails with `RESOURCE_EXHAUSTED` rather than queueing. Lightwalletd answers `GetTreeState` by proxying `z_gettreestate`; Zinder does not, so answering a tree-state read never calls the node. When transaction broadcast is not configured, the node is not a readiness input, so tree-state reads keep working while the node is unavailable. When broadcast is advertised, a node outage drains readiness, which refuses every request, tree-state reads included. Storing a frontier for every block was rejected: it costs about 2 GB on mainnet, needs a rebuild, and saves at most 99 blocks of replay.

## Out of Scope

- App-specific endpoint behavior.
- A separate per-wallet coverage profile.
- Public by-address shielded queries.
- Mempool UTXO completeness; the mempool surface owns mempool indexing.
- Cross-host read-replica architecture.

## Revision history

- 2026-11-17: `tree_state_at(height)` no longer fills from the upstream node.
  A height without a stored checkpoint is reconstructed from the nearest
  checkpoint below it and the stored compact blocks, under a per-query limit
  on concurrent reconstructions.
