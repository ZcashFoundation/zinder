# ADR-0039: Writer-admitted network-upgrade schedules

| Field | Value |
| --- | --- |
| Status | Accepted |
| Product | Zinder |
| Domain | Canonical store identity, network-upgrade activation tables, reader convergence |
| Related | [ADR-0008](0008-network-parameter-discovery.md), [ADR-0003](0003-canonical-storage-access-boundary.md), [Chain ingestion](../architecture/chain-ingestion.md), [Storage backend](../architecture/storage-backend.md), [Upgrading the node](../runbooks/upgrading-the-node.md) |

## Context

[ADR-0008](0008-network-parameter-discovery.md) makes the running node the
source of truth for network-upgrade activations. Each Zinder process discovers
the node's table from `getblockchaininfo.upgrades` at startup.

A canonical store's build plan fingerprints the complete table discovered at
construction, including upgrades that have not activated yet. That fingerprint
is immutable construction identity, and the construction manifest binds it. If
every opener required the node's table to equal it, any node release that
announces a future upgrade would make every existing store refuse to start.
Zebra 7.0.0 is such a release: it appends NU7 (branch `0x77190ad9`, Mainnet
height 3,543,405, Testnet height 4,465,026) to the table Zebra 6.2.2
advertises. Rebuilding canonical and wallet history for an upgrade that has not
activated is not an acceptable cost.

Activations at or below the store's visible tip are different. The stored
blocks were parsed and committed under those activations, so a table that
changes them describes a different interpretation of stored history and must
fail closed.

## Decision

The construction fingerprint stays immutable. The canonical writer admits the
node's current table into the store as a journaled schedule revision, and every
reader converges on the writer-admitted revision.

### Schedule revisions

`CanonicalActivationSchedule` is one admitted revision. It records the complete
admitted table, its revision number, the visible tip it was admitted at, and a
digest link to its predecessor. The writer keeps every revision in a journal in
the canonical store, and store control binds the latest revision number and
digest in a versioned trailer. Admission never rewrites the construction
fingerprint, canonical rows, the construction manifest, or wallet projection
identity.

When the writer opens a ready store, it admits the table it discovered:

- **No revision yet.** An exact ordered prefix of the node's table must
  reproduce the construction fingerprint on the same network, and every entry
  after that prefix must activate above the visible tip. The writer records the
  complete table as revision 1, even when it equals the construction table. Any
  other table is refused with the store unchanged. Before revision 1, a store
  therefore accepts only appended entries; an unactivated entry it was built
  with can move only in a later revision.
- **Existing revision.** The node's table must equal the admitted table at or
  below the visible tip, including names and same-height order. An identical
  table adds no revision. A table that adds, removes, or changes only entries
  above the tip becomes the next revision. A change at or below the tip is
  refused with the store unchanged.

The revision record and store control are written in one synced batch, so a
crash leaves either the previous or the new revision. Only the writer that owns
the primary admits a revision, and it checks the revision against its visible
tip at that moment. Live following interprets new blocks with the admitted
table.

### Readers

Readers never write a revision. Each reader checks its own discovered table
once, when it first opens a canonical secondary. Before the writer's first
admission, the table must reproduce the construction fingerprint exactly. After
it, the table must equal the admitted schedule at or below the visible tip.

After that startup check, a reader follows the writer rather than its startup
observation:

- A long-lived secondary, such as the projector's, reads each new revision when
  it catches up.
- A serving runtime that opens a fresh secondary generation for each refresh
  opens it with the table the writer last reported in
  `CanonicalControl.WriterStatus`. The query's serving-pair publisher and the
  compact-block compatibility publisher work this way, so a runtime started
  before an admission keeps refreshing after the chain crosses the newly
  admitted activation.
- The query serves the table admitted with the captured serving pair. Its
  pair publisher republishes when the writer reports a new revision at an
  unchanged fence.
- `GetLightdInfo` derives its upgrade fields on each request from the
  admitted table and the served tip: `consensusBranchId` is the active
  branch, and `upgradeName` and `upgradeHeight` name the next pending
  upgrade. It reads the table through
  `WalletQueryApi::network_upgrade_activations` in wallet-serving mode and
  from the served canonical reader in compact-block mode.
  `LightwalletdGrpcAdapter` takes no activation table of its own.
- The projector converges on the writer's fence and schedule revision, and
  binds the schedule identity into each format-3 state-bundle manifest.

`CanonicalControl.WriterStatus` and `CanonicalControl.CreateOwnerCheckpoint`
responses carry the admitted table, revision, and V1 fingerprint.

## Consequences

- A node release that announces a future upgrade does not require a canonical
  or wallet rebuild. Restarting ingest admits the new table.
- A table that changes an activation the store has crossed, or that does not
  extend the construction table before revision 1, fails closed without
  mutation.
- The first start of a release with schedule admission writes revision 1 even
  when the node's table is unchanged. Earlier releases reject the journal keys
  and the control trailer, so they cannot open the store afterwards. Rolling
  back requires a state bundle or checkpoint taken before the upgrade.
- Until the writer's first admission, a reader whose node advertises a table
  other than the construction table refuses to open, so ingest restarts before
  its readers.
- A reader that restarts against a node whose table differs from the admitted
  schedule at or below the tip refuses to open, even though readers that are
  already running keep following the writer.

## Rejected alternatives

### Rewrite the construction fingerprint

The writer could replace the build plan's fingerprint with the new table's.
The construction manifest binds the build plan, so the store would no longer
match the manifest it was published with, and the fingerprint would stop
identifying the table the stored history was built under. A separate journal
keeps construction identity fixed and records each later table explicitly.

### Let each reader accept its own table

Each reader could open whenever its node's table agrees with the construction
table at or below the tip, without a writer record. Readers attached to
different nodes could then serve different future activations, and writer
status, owner checkpoints, and state bundles would have no single schedule
identity to carry. One writer-owned revision gives every reader and checkpoint
the same identity.

## Vocabulary

- `CanonicalActivationSchedule` (one writer-admitted schedule revision in the
  canonical store).

See [Public interfaces §Core vocabulary](../architecture/public-interfaces.md#core-vocabulary).
