# ADR-0008: Per-Network Consensus Parameters Discovered From The Running Node

| Field | Value |
| ----- | ----- |
| Status | Accepted |
| Product | Zinder |
| Domain | Per-network consensus-rule data flow across process boundaries |
| Related | [Chain ingestion](../architecture/chain-ingestion.md), [Public interfaces](../architecture/public-interfaces.md), [Service boundaries](../architecture/service-boundaries.md), [Testing runbook](../runbooks/testing.md), [ADR-0003](0003-canonical-storage-access-boundary.md), [ADR-0039](0039-writer-admitted-network-upgrade-schedules.md) |

## Context

Three pieces of per-network consensus data flow through Zinder's read path: activation heights (Sapling, NU5, NU6, NU6\_1, ...), the consensus branch id active at a given height, and the human-facing upgrade name. Zinder serves them on at least three observable surfaces:

- `WalletQuery.Transaction` (`MinedTransactionChainContext.consensus_branch_id`), consumed by Zinder-native wallets through the native gRPC endpoint.
- `compat-lightwalletd::GetLightdInfo` (`saplingActivationHeight`, `consensusBranchId`, `upgradeName`, `upgradeHeight`), consumed by every lightwalletd-compatible wallet: Zodl, librustzcash-based wallets, and any future lightwalletd client.
- Transparent V5 transaction signing in `zinder-testkit`, which needs the active branch id to compute ZIP-244 sighashes; mismatched heights cause Zebra to reject broadcasts with `incorrect consensus branch id`.

Mainnet and testnet have stable activation schedules baked into upstream `zebra-chain`. Regtest and operator-configured custom testnets do not: their activation heights come from each operator's `zebrad.toml` (or equivalent) and are only authoritative on the running node.

Hardcoded per-network singletons cannot cover the regtest and custom-testnet case. Library defaults disagree silently with operator-configured activation heights, and every consumer that wants the active branch id at a height has to know which table to consult. The pattern question is where in the architecture a Zinder process learns the per-network consensus activations, and how that knowledge is shared with every component that needs it. Without one answer, every new read-path consumer risks a different copy of the same constants.

## Decision

Zinder treats the running upstream node as the source of truth for per-network consensus parameters on every supported network, including mainnet and testnet. The activations are parsed once at process startup from `getblockchaininfo.upgrades` and shared as an immutable `Arc<NetworkUpgradeActivations>`. Every consumer that needs consensus parameters accepts the table at construction time; no service degrades silently to `PRE_OVERWINTER_BRANCH_ID` when the discovery call fails. The canonical writer admits its discovered table into the canonical store, and readers converge on the writer-admitted table rather than keep their own startup observation; [ADR-0039](0039-writer-admitted-network-upgrade-schedules.md) defines that admission.

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
3. Consumers receive `Arc<NetworkUpgradeActivations>` at construction and call methods on the owned type. No process ever consults `zcash_chain::Network::sapling_activation_height` or any other library-default constant.

Production code does not consult `RegtestParameters::default()` or library-default activation singletons; consumers receive `Arc<NetworkUpgradeActivations>` and call methods on the owned type. The testkit's `regtest_local_network()` is derived from `sample_regtest_upgrade_activations()` so the in-process unit-test fixtures cannot drift from the discovery path; doc comments steer live tests toward `local_network_from_activations(&activations)` and `discover_network_upgrade_activations()`.

### Wiring contract per service

- **`zinder-ingest`** discovers the activations before canonical construction so transaction parsing and the immutable store identity use the node's exact table. Each later open admits the discovered table as a schedule revision. Wallet-serving coverage independently requires complete transparent history.
- **`zinder-query`** holds `Arc<NetworkUpgradeActivations>` on `WalletServingQuery` as a required constructor parameter. `MinedTransactionChainContext.consensus_branch_id` reflects the active branch at the mined height in the schedule admitted with the captured serving pair. The production binary errors out at startup if `[node]` is not configured (the table cannot be discovered without an RPC endpoint).
- **`zinder-compat-lightwalletd`** discovers the table at startup to open its serving readers. `GetLightdInfo` reads the writer-admitted table on each request, through `WalletQueryApi::network_upgrade_activations` in wallet-serving mode and from the served canonical reader in compact-block mode. Same startup-time failure if `[node]` is not configured.
- **`zinder-client`** discovers the table through `RemoteChainIndex::network_upgrade_activations`, after preflighting the native server's identity, network, contract revision, and advertised capability.
- **`zinder-testkit`** exposes `local_network_from_activations(&activations) -> LocalNetwork` for live tests; `TransparentTestKey::from_seed_with_local_network` accepts the result. `regtest_local_network()` is derived from `sample_regtest_upgrade_activations()` so the two regtest fixtures cannot drift.

### Regression test

A live test in `crates/zinder-source/tests/live/zebra_json_rpc.rs` calls `fetch_network_upgrade_activations()` against the configured node, asserts the table advertises Sapling at a height ≥ 1, asserts `active_at(tip).name` is non-empty, and asserts `consensus_branch_id_at(tip)` is non-zero whenever tip ≥ Sapling. The test runs under `ci-live` on regtest, testnet, and mainnet. If a future zebra-chain bump silently changes regtest defaults again, or the proto shape changes, this test catches it.

## Consequences

### What this enables

- Operators can iterate on regtest activation heights above the store's tip via their `zebrad.toml` without forking Zinder or coordinating a code change. Restart ingest to admit the new table; a change to an activation the store has crossed fails closed.
- `MinedTransactionChainContext.consensus_branch_id` is correct on every network for every supported upgrade automatically; no Zinder code-change tax per upgrade.
- New consumers that need consensus parameters have one obvious answer: accept `Arc<NetworkUpgradeActivations>` in your constructor; the production binary already discovers and shares one.
- Custom testnets become first-class: the table discovers itself from the node; nothing in Zinder hardcodes mainnet/testnet activation heights.
- Required-at-construction wiring eliminates the "silently serves branch id 0 forever" failure mode: a misconfigured service refuses to start instead of misreporting `MinedTransactionChainContext.consensus_branch_id` for its entire lifetime.

### What this costs

- Each process pays one `getblockchaininfo` round-trip at startup so consensus parsing and served upgrade metadata use the node's exact activation table.
- The table is cached for process lifetime. If the operator reconfigures the node's activation heights mid-flight without restarting Zinder, the cache goes stale until restart. Documented in the runbook.
- Tests that exercise `WalletQuery` must pass an activations table. The testkit's `sample_regtest_upgrade_activations()` covers the common case; the cost is one extra line per test.

## Vocabulary

- `NetworkUpgradeActivations` (the carrier).
- `NetworkUpgradeActivation` (one row).
- `PRE_OVERWINTER_BRANCH_ID = 0` (the wire convention for "no branch active at this height").
- `ZebraJsonRpcSource::fetch_network_upgrade_activations` (the discovery function).
- `ZebraJsonRpcSource::discover_network_upgrade_activations(target)` (the shared startup wrapper that returns `Arc<NetworkUpgradeActivations>` and emits a structured discovery log).
- `local_network_from_activations` (the testkit bridge to `zcash_protocol::LocalNetwork`).

See [Public interfaces §Core vocabulary](../architecture/public-interfaces.md#core-vocabulary).
