# ADR-0008: Take network-upgrade activations from the node and let the writer admit them

Status: Accepted

## Context

Zinder needs each network's upgrade activations: the height where each network upgrade activates, the consensus branch id active at a height, and the upgrade's name. Ingest needs them to parse blocks. `WalletQuery` reports the branch id of a mined transaction, and `GetLightdInfo` reports the active branch and the next upgrade to every lightwalletd client.

Mainnet and Testnet activations are compiled into `zebra-chain`, but Regtest and custom test networks take theirs from the node operator's configuration. A library default can disagree silently with the node, and every consumer that keeps its own copy can drift from the others.

The table also changes over time. A node release can announce a future upgrade, as Zebra 7.0.0 did when it added NU7. Rebuilding canonical and wallet history for an upgrade that has not activated is not acceptable. A change to an activation at or below the stored tip is different: the stored blocks were parsed under that activation, so a changed table would reinterpret stored history.

## Decision

1. **The running node is the source of truth on every network**, Mainnet and Testnet included. Zinder reads the table from the node's `getblockchaininfo.upgrades` and carries upgrade names verbatim, so a new upgrade needs no Zinder code change. Production code never consults library-default activation constants.
2. **Only the canonical writer admits the table.** At startup, ingest fetches the table and verifies that the node's genesis block matches the configured network before it admits the endpoint. Only the writer admits schedule revisions. A reader that verifies a node endpoint for broadcast or node-backed reads also reads that node's table, only to compare it with the admitted schedule.
3. **Consumers receive the table when they are built.** No component falls back to the pre-Overwinter branch id when the table is missing. A misconfigured component refuses to start instead of reporting branch id 0 forever.
4. **The writer admits each new table as a revision.** The table fingerprinted at construction stays immutable. When ingest opens a ready store, it compares the node's table with the store:
   - Before the first revision, the node's table must extend the construction table with entries that activate above the visible tip.
   - After it, the node's table must equal the admitted table at or below the visible tip. A table that changes only entries above the tip becomes the next revision, and an identical table adds none.
   - Any other table is refused, and the store is left unchanged.

   The revision and the store's control record are written in one synced batch, so a crash leaves either the old or the new revision.
5. **Only `[node]` can bring a new table.** Ingest admits a table only when it opens the store. Every fallback endpoint must advertise exactly the table the store follows, names included, to start ingest or to take over during a failover ([ADR-0007](0007-node-failure-recovery-and-transport-policy.md)).
6. **Readers adopt the writer's revision.** Query, compatibility, and the projector read the table the writer admitted from the store and need no node for it. The Explorer reads it through the wallet query. A reader that opens a ready store before the writer's first revision gets a retryable refusal and waits. A reader that broadcasts transactions checks its node's table against the admitted revision before the first transaction reaches that node.
7. **A network has one genesis block.** Custom activation heights are supported. A test network with its own genesis block is not, and ingest refuses a node whose block 0 differs from the configured network's.

[Chain ingestion](../architecture/chain-ingestion.md) describes startup checks and admission, and [Upgrading the node](../runbooks/upgrading-the-node.md) describes the operator procedure.

## Evidence

- Decision 2: judgment. One admitting writer gives every reader one schedule identity, as the alternatives explain. A reader uses a node's own table only to refuse an endpoint that disagrees with the admitted schedule. The revision corrects the order of ingest's startup checks to match the code.

## Consequences

- A node release that announces a future upgrade needs an ingest restart, not a rebuild.
- A table that changes an activation the store has crossed fails closed without modifying the store.
- Regtest operators can move activations above the stored tip in their node configuration and restart ingest.
- Ingest caches the table for its lifetime. A node reconfigured mid-run takes effect at the next ingest restart.
- Writer status, owner checkpoints, and state bundles carry one schedule identity.
- After an operator upgrades nodes to announce a new upgrade, every endpoint in the node list must run a release with the same table before fallbacks can take over again.
## Alternatives rejected

- **Hardcoded per-network tables.** They cannot cover Regtest or custom test networks, and they disagree silently with an operator's node.
- **Requiring the node's table to equal the construction table.** Every node release that announces a future upgrade would make every existing store refuse to start.
- **Rewriting the construction fingerprint.** The construction manifest binds that fingerprint, so the store would stop matching the manifest it was published with. A separate revision journal keeps construction identity fixed.
- **Letting each reader accept its own node's table.** Readers attached to different nodes could serve different future activations, and checkpoints would have no single schedule identity.
- **Comparing switch targets without upgrade names.** Admission at store open compares names, so a restart could refuse an endpoint that the running writer had accepted. One plain-equality rule keeps startup and switching consistent.

## Revision history

- 2026-11-23. Decision 2 says readers read a node's table only to verify an endpoint.
- 2026-11-23. Decision 2 orders ingest's table fetch and genesis check before endpoint admission.
- 2026-11-23. Evidence records what decision 2 rests on.
