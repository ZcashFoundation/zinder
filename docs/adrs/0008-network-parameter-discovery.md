# ADR-0008: Per-Network Consensus Parameters Discovered From The Running Node

| Field | Value |
| ----- | ----- |
| Status | Accepted |
| Product | Zinder |
| Domain | Per-network consensus-rule data flow across process boundaries |
| Related | [Chain ingestion](../architecture/chain-ingestion.md), [Public interfaces](../architecture/public-interfaces.md), [Service boundaries](../architecture/service-boundaries.md), [Testing runbook](../runbooks/testing.md), [ADR-0003](0003-canonical-storage-access-boundary.md) |

## Context

Three pieces of per-network consensus data flow through Zinder's read path: activation heights (Sapling, NU5, NU6, NU6\_1, ...), the consensus branch id active at a given height, and the human-facing upgrade name. Zinder serves them on at least three observable surfaces:

- `WalletQuery.Transaction` (`MinedTransactionChainContext.consensus_branch_id`), consumed by Zinder-native wallets through the native gRPC endpoint.
- `compat-lightwalletd::GetLightdInfo` (`saplingActivationHeight`, `consensusBranchId`, `upgradeName`, `upgradeHeight`), consumed by every lightwalletd-compatible wallet: Zodl, librustzcash-based wallets, and any future lightwalletd client.
- Transparent V5 transaction signing in `zinder-testkit`, which needs the active branch id to compute ZIP-244 sighashes; mismatched heights cause Zebra to reject broadcasts with `incorrect consensus branch id`.

Mainnet and testnet have stable activation schedules baked into upstream `zebra-chain`. Regtest and operator-configured custom testnets do not: their activation heights come from each operator's `zebrad.toml` (or equivalent) and are only authoritative on the running node.

Hardcoded per-network singletons cannot cover the regtest and custom-testnet case. Library defaults disagree silently with operator-configured activation heights, and every consumer that wants the active branch id at a height has to know which table to consult. The pattern question is where in the architecture a Zinder process learns the per-network consensus activations, and how that knowledge is shared with every component that needs it. Without one answer, every new read-path consumer risks a different copy of the same constants.

## Decision

Zinder treats the running upstream node as the source of truth for per-network consensus parameters on every supported network, including mainnet and testnet. Each process parses the node's table once at startup from `getblockchaininfo.upgrades`. The canonical writer admits that table into the canonical store as a schedule revision; it may change only activations above the store's visible tip. Readers serve the writer-admitted table. Every consumer that needs consensus parameters accepts the table at construction time as an immutable, shared `Arc<NetworkUpgradeActivations>`; no service degrades silently to `PRE_OVERWINTER_BRANCH_ID` when the discovery call fails.

### The carrier type

`zinder_core::NetworkUpgradeActivations` owns:

- A `Network` identifier (the chain the table describes).
- A `Vec<NetworkUpgradeActivation>` sorted by `activation_height` ascending. Each entry is `{ branch_id: u32, activation_height: BlockHeight, name: String }`. The `name` is carried verbatim from the node's `getblockchaininfo.upgrades[<branch>].name` so future upgrades remain serviceable without a Zinder code change.

The type exposes pure-data queries:

- `active_at(height: BlockHeight) -> Option<&NetworkUpgradeActivation>`: the highest activation with `activation_height <= height`.
- `consensus_branch_id_at(height: BlockHeight) -> u32`: defaults to `PRE_OVERWINTER_BRANCH_ID` (0) for heights below the earliest activation.
- `activation_height_by_name(name)` and `activation_height_by_branch_id(branch_id)` for targeted lookups.

### Source-of-truth flow

1. `ZebraJsonRpcSource::fetch_network_upgrade_activations()` parses `getblockchaininfo.upgrades` and validates branch-id uniqueness. The map key (hex branch id) becomes `branch_id`; the value's `name` and `activationheight` become `name` and `activation_height`.
2. `ZebraJsonRpcSource::discover_network_upgrade_activations(target)` wraps the fetch, emits a `network_upgrade_activations_discovered` structured log event keyed by service name, and returns `Arc<NetworkUpgradeActivations>`. The four service binaries call this one helper so the discovery event shape stays consistent across the workspace.
3. The canonical writer admits the discovered table when it opens the store (see [Schedule admission](#schedule-admission)). It follows the chain with the admitted table.
4. Readers open their canonical secondary with their own discovered table, which must agree with the admitted schedule at or below the visible tip, and then serve the admitted table. A node may advertise a different future activation, but a reader never admits it.
5. Consumers call methods on the owned type. No process ever consults `zcash_chain::Network::sapling_activation_height` or any other library-default constant.

Production code does not consult `RegtestParameters::default()` or library-default activation singletons; consumers receive `Arc<NetworkUpgradeActivations>` and call methods on the owned type. The testkit's `regtest_local_network()` is derived from `sample_regtest_upgrade_activations()` so the in-process unit-test fixtures cannot drift from the discovery path; doc comments steer live tests toward `local_network_from_activations(&activations)` and `discover_network_upgrade_activations()`.

### Wiring contract per service

- **`zinder-ingest`** discovers the activations before canonical construction or restart. Fresh construction fingerprints that table into the build plan; every writer open then admits the table as a schedule revision before following. Wallet-serving coverage independently requires complete transparent history.
- **`zinder-projector`** converges on the writer's fence and admitted schedule revision, and binds that schedule identity into each state-bundle manifest.
- **`zinder-query`** holds `Arc<NetworkUpgradeActivations>` on `WalletServingQuery` as a required constructor parameter, but serves the schedule admitted with the captured serving pair. `MinedTransactionChainContext.consensus_branch_id` reflects that table at the mined height. The pair publisher republishes when the writer reports a new schedule revision, even if the fence has not moved. The production binary errors out at startup if `[node]` is not configured (the table cannot be discovered without an RPC endpoint).
- **`zinder-compat-lightwalletd`** holds `Arc<NetworkUpgradeActivations>` on `LightwalletdGrpcAdapter` as a required constructor parameter. `GetLightdInfo` reads the table directly. Same startup-time failure if `[node]` is not configured.
- **`zinder-client`** discovers the table through `RemoteChainIndex::network_upgrade_activations`, after preflighting the native server's identity, network, contract revision, and advertised capability.
- **`zinder-testkit`** exposes `local_network_from_activations(&activations) -> LocalNetwork` for live tests; `TransparentTestKey::from_seed_with_local_network` accepts the result. `regtest_local_network()` is derived from `sample_regtest_upgrade_activations()` so the two regtest fixtures cannot drift.

### Schedule admission

The build plan's activation fingerprint is immutable construction identity. Schedule admission never rewrites it, canonical rows, the construction manifest, or wallet projection identity.

The writer keeps a journal of `CanonicalActivationSchedule` revisions in the canonical store. Each revision records the complete admitted table, the visible tip it was admitted at, and a digest link to its predecessor. Store control binds the latest revision number and digest in a versioned trailer. Releases that predate schedule admission reject both the journal keys and the trailer, so an older binary refuses a store after its first admission.

When the writer opens a store:

- **No revision yet.** No writer of this release has opened the store. The writer accepts the node's table only if an exact ordered prefix of it reproduces the build plan's fingerprint on the same network, and every entry after that prefix activates above the visible tip. It records the complete table as revision 1. Any other table is refused with the store unchanged.
- **Existing revision.** The node's table must equal the admitted table at or below the visible tip, including names and same-height order. An identical table adds no revision; a table that differs only above the tip becomes the next revision. A change at or below the tip is refused with the store unchanged.

The revision record and store control are written in one synced batch, so a crash leaves either the previous or the new revision. Revisions are admitted only by the writer that owns the primary, against its visible tip at that moment.

Readers never write a revision. Until the writer's first admission, a reader's table must reproduce the construction fingerprint exactly. `CanonicalControl.WriterStatus` and `CanonicalControl.CreateOwnerCheckpoint` responses carry the admitted table, revision, and V1 fingerprint.

### Regression test

A live test in `crates/zinder-source/tests/live/zebra_json_rpc.rs` calls `fetch_network_upgrade_activations()` against the configured node, asserts the table advertises Sapling at a height ≥ 1, asserts `active_at(tip).name` is non-empty, and asserts `consensus_branch_id_at(tip)` is non-zero whenever tip ≥ Sapling. The test runs under `ci-live` on regtest, testnet, and mainnet. If a future zebra-chain bump silently changes regtest defaults again, or the proto shape changes, this test catches it.

## Consequences

### What this enables

- A node release that announces a future network upgrade, such as Zebra 7.0.0 appending NU7 to the Zebra 6.2.2 table, does not require a canonical or wallet rebuild. Restart ingest so the writer admits the new table, then restart its readers.
- Operators can revise regtest activation heights above the current tip via their `zebrad.toml` without forking Zinder. Changing an activation the store has already crossed fails closed.
- `MinedTransactionChainContext.consensus_branch_id` is correct on every network for every supported upgrade automatically; no Zinder code-change tax per upgrade.
- New consumers that need consensus parameters have one obvious answer: accept `Arc<NetworkUpgradeActivations>` in your constructor; the production binary already discovers and shares one.
- Custom testnets become first-class: the table discovers itself from the node; nothing in Zinder hardcodes mainnet/testnet activation heights.
- Required-at-construction wiring eliminates the "silently serves branch id 0 forever" failure mode: a misconfigured service refuses to start instead of misreporting `MinedTransactionChainContext.consensus_branch_id` for its entire lifetime.

### What this costs

- Each process pays one `getblockchaininfo` round-trip at startup. The writer then pays one synced write when the table changed.
- The table is cached for process lifetime. If the operator reconfigures the node's activation heights mid-flight without restarting Zinder, the cache goes stale until restart. Documented in the runbook.
- A reader whose node advertises a newer table than the store's construction baseline refuses to open until the writer has admitted a revision, so ingest must restart first.
- The first admission makes the store unreadable by older Zinder releases. Downgrading requires a checkpoint taken before the upgrade.
- Tests that exercise `WalletQuery` or `LightwalletdGrpcAdapter` must pass an activations table. The testkit's `sample_regtest_upgrade_activations()` covers the common case; the cost is one extra line per test.

## Vocabulary

- `NetworkUpgradeActivations` (the carrier).
- `NetworkUpgradeActivation` (one row).
- `PRE_OVERWINTER_BRANCH_ID = 0` (the wire convention for "no branch active at this height").
- `ZebraJsonRpcSource::fetch_network_upgrade_activations` (the discovery function).
- `ZebraJsonRpcSource::discover_network_upgrade_activations(target)` (the shared startup wrapper that returns `Arc<NetworkUpgradeActivations>` and emits a structured discovery log).
- `local_network_from_activations` (the testkit bridge to `zcash_protocol::LocalNetwork`).
- `CanonicalActivationSchedule` (one writer-admitted schedule revision in the canonical store).

See [Public interfaces §Domain types](../architecture/public-interfaces.md#domain-types).

## Revision history

- 2026-10-27: The canonical writer admits the node's table as a schedule revision instead of requiring it to equal the construction fingerprint. Readers serve the admitted schedule, and writer status, owner checkpoints, and state bundles carry its identity.
