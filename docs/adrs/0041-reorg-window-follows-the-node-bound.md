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

### Halt visibility

A park remains possible, for example after the node is replaced or resynced,
and wallets must not keep serving a branch that stopped advancing. The parked
writer therefore stops following but keeps its store and control plane until
termination. It serves its frozen fence, retained events, retention leases,
and owner checkpoints, so the projector and query start and stay converged
against it, and it reports the halt through
`IngestControl.WriterStatus.reorg_window_exceeded` and
`CanonicalControl.WriterStatus.reorg_window_exceeded` (the existing
`ReorgWindowExceededDetail`). The halt is a terminal readiness state that no
other ingest worker can replace.

A parked writer changes no canonical row and writes the store only for
projection-build retention leases: `AcquireProjectionBuildLease` and
`RenewProjectionBuildLease` write a lease row (acquisition also advances the
lease generation counter and deletes expired leases), and
`ReleaseProjectionBuildLease` deletes one. Owner checkpoint
creation writes a RocksDB checkpoint to its staging target, outside the
store's rows. Canonical event retention runs only in the follow loop, so it
stops at the park.

The mempool stops with the writer. The node's mempool is built on a branch
the frozen fence does not contain, so the park halts the live mempool owner
for the rest of the process: it stops reading the node mempool and withdraws
its in-memory index, and mempool retention stops. The owner task stays alive,
so the ingest supervisor does not read the halt as an unexpected exit. The
parked writer refuses every mempool command, so it records and prunes no
mempool event. Every ingest-control mempool read (snapshot, transaction, and
transparent lookups) refuses with the typed `REORG_WINDOW_EXCEEDED` status,
and an open `MempoolEvents` stream ends with it on its next page. Native
query proxies those streams, so its open `MempoolEvents` streams end with the
same status.

Every wallet-serving reader refuses traffic while the writer reports the halt:

- The serving-pair publisher shared by native query and lightwalletd
  compatibility wallet serving, and compatibility compact-block serving, read
  `CanonicalControl.WriterStatus`. The frozen fence still attests their
  published reader, so the halt on the same response is what keeps them from
  reporting ready on a chain that stopped advancing. They project it as their
  `reorg_window_exceeded` readiness cause at bootstrap and on every refresh.
  The halt is sticky: a writer-status failure after it republishes the halt
  instead of the traffic-admitting `serving_pair_stale`, and a reader that
  only a parked writer attested earns no staleness allowance. Only a valid
  writer status without the halt returns them to ready.
- Native query also reads `IngestControl.WriterStatus` through its
  ingest-control health probe, and that cause takes precedence over the
  serving-pair failures the park could cause.

Carrying the halt on canonical writer status covers both compatibility serving
modes with one additive field, without giving compatibility the native
ingest-control probe and the mempool and upstream-health gating that comes
with it.

The supported single-host image keeps running in this state. Restarting would
park the writer on the same reorg and replace the typed refusal with refused
connections, so its entrypoint ends the container only for causes such as
`schema_mismatch` and `node_capability_missing`.

Wallet consumers receive the halt through the existing error vocabulary:

- New requests fail with `REORG_WINDOW_EXCEEDED` (`FAILED_PRECONDITION`),
  carrying `readiness_cause`, `replacement_depth_blocks`,
  `reorg_window_blocks`, and `covered_depth_blocks` in `ErrorInfo` metadata.
  The covered depth is the distance from the settled tip to the visible tip,
  which is below the window while a raised window is still growing, so a halt
  during that ramp does not read as a reorg deeper than the new window.
- Open `ChainEvents` and `MempoolEvents` streams end with the same status,
  because the traffic gate only rejects new requests.
- `zinder-client` decodes the status as `IndexerError::ReorgWindowExceeded`
  with retry policy `OperatorActionRequired`.

The proto changes are additive fields on private control messages. The wallet
surface gains no new message or field, so no contract revision changes. A
reader from an earlier release ignores the canonical-status halt and keeps
reporting ready on the frozen fence, so upgrade readers together with ingest.

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

**A halt field on `ChainView`.** Every wallet response already carries
`ChainView`, but the traffic gate refuses requests while the writer is parked,
so a response field would only reach consumers if Zinder kept serving the
frozen branch, which wallet-serving deliberately refuses.

**A smaller default, such as the 300 blocks production Testnet moved to.** Any
value below the node's bound still parks on reorgs the node can produce, and
operators would still need to raise it as observed reorgs grow.

## Consequences

- The halt lives in the parked process, not in the store. A restarted writer
  answers writer status without it until its first follow iteration
  rediscovers the reorg and parks again, typically after one window of block
  reads. During that window lightwalletd compatibility can report ready on
  the abandoned branch. Native query stays unready, because its
  ingest-control probe needs a mempool snapshot that cannot certify against a
  node on another branch. The same window existed when a parked writer
  exited and restarted.

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
- Every READY publication must still be explained by the replay rows from
  the settled checkpoint through the visible tip, but re-deriving that suffix
  on each transition would scale with the window. Open-time admission derives
  it once; afterwards the writer authenticates only the rows each transition
  wrote and a reader only the rows the intervening chain events changed, and
  an idle reader catch-up reads none. Rows above the settled tip change only
  through the writer's atomic, event-recorded transitions, so this accepts
  exactly what a complete derivation accepts. The one difference is when an
  out-of-band change to a row no transition touched is found: each holder
  re-derives the whole suffix once its incrementally authenticated rows reach
  one window, and the writer checks every row again when it settles, so such
  a change is found within one window of blocks instead of at the next
  transition.
- A deep replacement holds its whole replacement suffix in memory before its
  atomic commit, which scales with the window. Fork discovery fetches the
  suffix newest first, before the common parent is known, and keeps those
  raw blocks for preparation; preparation then replaces each with its
  prepared form. `ingest.follow.max_replacement_bytes` (2 GiB by default)
  bounds the estimated sum of both: the raw bytes of fetched blocks not yet
  prepared plus each prepared block's replay envelope, counted twice for its
  decoded facts, and retained raw bytes. The common parent stops counting once
  found. Transient decoding of the one block in preparation is not counted. A
  replacement beyond the budget fails with the typed
  `ReplacementBytesExceeded` before any write, during discovery if the
  fetched blocks alone exceed it. The error ends the canonical writer, so
  ingest exits instead of parking: a supervisor that restarts it retries the
  same discovery and fails the same way until the operator raises the budget
  or the node abandons the branch. A dedicated budget is used because the
  construction pipeline's preparation watermark admits a stream of blocks,
  while a replacement must hold all of its blocks at once.
