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

### Node endpoint switches

The writer admits a table only when it opens the store. When ingest has
fallback node endpoints ([ADR-0013](0013-source-failure-recovery-topology.md)),
it admits the table of the endpoint it starts on. Before choosing that
endpoint, it reads the store's latest admitted revision without opening the
primary, and a fallback whose table differs from that revision is refused at
startup. Only `[node]` can bring a new table, so a fallback running an older
release never removes a pending activation while `[node]` is merely slow to
answer; when `[node]` is unreachable and no fallback matches, startup fails as a
single-endpoint startup would and the restart tries again. Every other endpoint
must advertise exactly the starting endpoint's table, and a switch target must
advertise exactly the table the store admitted, compared entry by entry, names
included. An endpoint with any other table is refused, and no table is admitted
while ingest runs. After an operator aligns the nodes, for example by upgrading
both to a release that announces the same upgrade, restarting ingest admits the
new table.

Zebra 7.0.0 and Zakura 1.8.6 report identical `getblockchaininfo.upgrades`
names on Regtest, so plain equality lets them back each other up.

### Readers

Readers never write a revision, and the reader runtimes bring no activation
table of their own. Query, compatibility in both serving modes, and the
projector open every canonical secondary with
`RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule`, which takes
only the expected network and reads through the schedule the writer persisted.
None of them contacts a node to learn the table, so they start while the node
is down.

A READY store whose writer has not admitted a schedule yet refuses that open
with `CanonicalStoreError::NetworkUpgradeScheduleNotAdmitted`. The writer
admits revision 1 when it opens the store, so the refusal is transient. The
serving publishers retry it before they converge, until
`storage.initial_catchup_timeout_ms` passes, and these rounds do not count
against `pair_convergence_attempts`. The projector retries it until
`projector.fence_convergence_timeout_seconds` passes. A serving pair requires an admitted schedule and serves it as its
activation table.

A secondary opened in the writer's own process, such as ingest's
materialized-view secondary, still opens with its node-observed table through
`RocksDbCanonicalSecondary::open_ready`. Before the writer's first admission,
that table must reproduce the construction fingerprint exactly. After it, the
table must equal the admitted schedule at or below the visible tip.

After it opens, a reader follows the writer:

- A long-lived secondary, such as the projector's, reads each new revision when
  it catches up.
- A serving runtime that opens a fresh secondary generation for a refresh
  adopts the schedule that generation finds in the store, so a runtime started
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
- The projector converges on the writer's fence and schedule revision before
  it reports its wallet caught up, and binds the schedule identity into each
  format-3 state-bundle manifest. While the writer is ahead, the projector
  applies the retained events its secondary already holds. A newer revision
  cannot change how those blocks are interpreted, because a revision changes
  only entries above the tip it was admitted at.
- A commitment-tree reconstruction at one height reads only blocks at or below
  that height, so it accepts any table that equals the admitted schedule at or
  below that height. Ingest seeds its final-root accumulators this way with its
  node-observed table, on a secondary that can open before the writer admits
  that table.

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
- A reader cannot open a READY store before the writer's first admission. It
  retries until its convergence timeout passes, so a reader started before
  ingest only waits.
- Readers no longer compare a node's table with the store when they open. A
  reader that broadcasts transactions instead verifies its node's table
  against the admitted schedule through the served tip on first contact,
  before any transaction reaches the node
  ([ADR-0038](0038-wallet-runtime-composition-and-capability-discovery.md)).

## Rejected alternatives

### Compare switch targets without upgrade names

A switch target could match the admitted schedule on branch ids and heights
alone. Admission at store open compares names too, so a restart while the
first endpoint is down would refuse the store that the running writer had
accepted the endpoint for. One plain-equality rule keeps startup and switching
consistent.

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
- `NetworkUpgradeScheduleNotAdmitted` (the retryable refusal of a reader that
  adopts the writer's schedule before the writer admitted one).

See [Public interfaces §Core vocabulary](../architecture/public-interfaces.md#core-vocabulary).

## Revision history

- 2026-11-18: Reader runtimes adopt the writer-admitted schedule instead of
  checking a node-discovered table at open. Query, compatibility, and the
  projector no longer contact a node to open storage, and a store without an
  admitted schedule is refused with the retryable
  `NetworkUpgradeScheduleNotAdmitted`. A broadcasting reader verifies its
  node's table on first contact instead.
- 2026-11-18: Added "Node endpoint switches": ingest admits only its starting
  endpoint's table, a fallback starts ingest only with the store's admitted
  table, and other endpoints must advertise exactly the admitted table to take
  over.
