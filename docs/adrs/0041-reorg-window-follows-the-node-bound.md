# ADR-0041: Reorg window follows the node's finalization bound

| Field | Value |
| --- | --- |
| Status | Accepted |
| Product | Zinder |
| Domain | Canonical storage, wallet projection, and reorg policy |
| Related | [Chain events](../architecture/chain-events.md), [Storage backend](../architecture/storage-backend.md), [Initial sync](../runbooks/initial-sync.md#reorg-window-sizing) |

## Context

Zebra commits a block to its finalized state only once its best non-finalized
chain grows past `MAX_BLOCK_REORG_HEIGHT`, which is 1000 blocks in Zebra 7.0.0
(`zebra-state/src/service/write.rs` and
`zebra-chain/src/parameters/constants.rs`). Zakura, the other node Zinder
qualifies, finalizes on the same 1000-block constant. A node can therefore
present a legitimate reorg up to 1000 blocks deep, and never a deeper one.

Zinder's reorg window bounds how far below the visible tip a replacement may
start. A deeper replacement parks ingest with `reorg_window_exceeded`. The
default window was 100 blocks, and a 114-block Testnet reorg on 2026-07-20
already exceeded it. Any window below the node's bound parks on reorgs the
node itself produces.

The window was also immutable store identity. The canonical store control
record and the wallet projection control record both bind it, so moving an
existing deployment to a larger window required rebuilding both stores.

Two invariants constrain a change in place:

- The settled tip never regresses. Chain-event consumers may discard undo
  state at or below the settled tip of each event's epoch.
- Wallet projection undo rows cover exactly the canonical unsettled suffix,
  the blocks above the settled tip, and that suffix never exceeds the wallet's
  supported depth.

## Decision

### Default window

The default reorg window is 1000 blocks for ingest, projector, native query,
and lightwalletd compatibility admission. Every runtime must still configure
the same window.

### Raising the window in place

When the canonical writer opens an existing store with a window larger than
the store's effective window, it records the larger window as a monotonic
store revision:

- The construction plan, including its construction window and manifest
  binding, stays immutable.
- READY store control gains a versioned reorg-policy trailer (`RPOL`, version
  1, the new window as a little-endian `u32`) after the schedule trailer. The
  writer persists it in one synced write and reads it back. A store without
  the trailer uses its construction window.
- The trailer only raises: a value at or below the construction window, an
  unknown version, or a trailer out of order is refused at decode.
- The settled tip does not move. The writer resumes settlement only once the
  visible tip is a full new window above it, so the replaceable suffix grows
  by one block per new block until it reaches the new window. No canonical
  history is rewritten.

When the projector resumes a READY wallet, it raises the wallet's supported
reorg depth to the canonical effective window in place. It backfills no undo
rows. Undo rows already cover the unsettled suffix, and because the settled
tip holds, the suffix and its undo rows grow together as new blocks arrive.

### Lowering and mixed configuration

A window below the effective one is refused by the canonical writer and by the
projector, without mutation. Lowering requires a rebuild or a restore from a
state captured under the smaller window.

Readers require the exact effective window. The projector, native query,
lightwalletd compatibility, and the materialized-view plane refuse to open a
canonical store whose effective window differs from their configured window.
A secondary that is already open refuses catch-up after a writer raise until
it restarts with the raised window. A wallet secondary accepts a raised depth
on catch-up and refuses a lower one.

## Alternatives

**Backfill undo rows and settle at the new depth immediately.** Raising the
window from 100 to 1000 this way would make up to 900 blocks at or below the
current settled tip replaceable again. That requires the settled tip to move
backward, which breaks consumers that have already discarded undo state below
it. The projector would also need to replay those blocks to rebuild their undo
rows. Holding the settled tip reaches the same steady state without either
step.

**Rebuild on every change.** This keeps the window as construction identity
only, but costs a full construction of both stores and a serving outage for a
change that needs no history rewrite.

**A smaller default, such as the 300 blocks production Testnet moved to.** Any
value below the node's bound still parks on reorgs the node can produce, and
operators would still need to raise it as observed reorgs grow.

## Consequences

- The settled tip trails the visible tip by up to 1000 blocks. Surfaces that
  report finality from the settled tip report it later than before.
- After a raise, until the visible tip is a full window above the held settled
  tip, a reorg deeper than the current unsettled suffix still parks ingest.
- Wallet undo rows scale with the window. Construction accounts retained undo
  memory against `projector.build.max_accounted_reorg_undo_bytes` (512 MiB by
  default). A suffix of typical blocks stays far below that cap. A suffix of
  unusually transparent-heavy blocks can reach it, and construction then fails
  with `AccountedReorgUndoMemoryLimit` instead of growing memory.
- A raised canonical store is unreadable by earlier releases. Releases with
  schedule admission reject any bytes after the schedule trailer, and earlier
  releases reject any trailer. Rolling back a binary after a raise requires a
  checkpoint captured before the raise, or a rebuild.
- An operator who raises the window must restart every reader with the same
  window. A reader left on the old window fails closed.
