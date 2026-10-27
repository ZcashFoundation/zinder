# Chain ingestion

`zinder-ingest` is the only canonical writer. The release runtime either opens
an authenticated ready store or constructs one in a writer-owned staging path,
then continuously follows Zebra. It also owns live mempool state and the private
control APIs used by projectors and trusted readers.

## Runtime sequence

```text
load and validate configuration
  -> connect to Zebra and discover capabilities
  -> discover network-upgrade activations
  -> open a ready canonical store
     or recover a published staging store
     or construct and publish a fresh store
  -> admit the discovered activations as a schedule revision
  -> raise the store's reorg window to the configured window, if larger
  -> start continuous canonical following with the admitted schedule
  -> serve authenticated control commands and mempool state
```

Store identity, network, workload, construction activation fingerprint,
raw-blob retention, reorg policy, construction build plan, and schema are
validated before a ready store is admitted. The immutable construction manifest
must describe the same typed workload and complete build plan as the store
control record. An incompatible non-empty path fails without mutation.

The writer then admits the node's activation table. A table that adds or
changes only activations above the visible tip is journaled as the next
schedule revision; a change at or below the tip fails without mutation. See
[ADR-0039](../adrs/0039-writer-admitted-network-upgrade-schedules.md).

A configured reorg window larger than the store's effective window is recorded
as a monotonic store revision. The settled tip holds until the visible tip is a
full new window above it, so the replaceable suffix grows with new blocks. A
smaller configured window fails without mutation. See
[ADR-0041](../adrs/0041-reorg-window-follows-the-node-bound.md).

## Fresh construction

Fresh construction uses a sibling staging path. `CanonicalStoreBuildPlan`
fixes the source range, checkpoint predecessor, network, activation
fingerprint, raw-blob retention, and reorg policy before data is loaded. The
workload and complete build plan are bound into the construction manifest
before publication.

The source path fetches bounded connected segments. Block preparation validates
source identity, parses each block once, and constructs `CanonicalBlockFacts`,
retained raw blobs, compact-block material, tree commitments, and direct-index
inputs. Transaction locations are always derived from ordered canonical facts;
transaction and block bytes are written only when the build plan retains them.
CPU-heavy preparation may run in parallel; commitment-tree positioning and
publication remain ordered.

`CanonicalConstructionSettings` and `CanonicalPipelineLimits` bound:

- blocks, artifact bytes, and estimated write bytes per batch;
- source segment size and target response bytes;
- concurrent source requests and reserved response bytes;
- block-preparation concurrency; and
- queued artifact bytes while a prior batch is committed or flushed.

Construction loads an inactive `RocksDbCanonicalBuilder`, validates continuity,
manifest identity, per-block replay envelopes, and the ordered facts digest,
then publishes `CanonicalBaselinePublication`. The candidate is not readable as
a canonical store until publication succeeds.

Subtree-root construction makes its load coverage explicit. The normal
node-backed path loads `RetainedRange`: roots completed after the authenticated
history predecessor. An admitted captured corpus may instead call
`load_complete_subtree_root_prefix` with `CompletePrefix`: every root from index
zero through each protocol's completed count at the fixed tip. This import is a
caller-authenticated boundary, not a claim that the store can prove headers it
does not retain. Completion blocks strictly before retained history preserve
the captured identity; a root completing at the authenticated predecessor must
match that predecessor hash; roots completing in retained history must match
retained headers.

`CanonicalSubtreeRootLoadCoverage`, sequence-digest version 2, and the exact
ordered root rows are bound into construction-manifest format 4. Cold
publication readback recomputes the same coverage-aware digest and repeats the
available predecessor and retained-header checks. Coverage is never inferred
from row counts.

On restart, a ready staging store is installed and cold-opened at the configured
path. An unpublished or invalid staging store is removed and reconstructed. A
ready configured store is never replaced by staging data.

## Continuous following

`CanonicalFollower` polls one atomic source tip and compares it with the
admitted `CanonicalEventFence`.

- A connected next block produces `CanonicalLiveAppend`.
- A changed suffix within `CanonicalReorgPolicy` produces
  `CanonicalLiveReplacement`.
- A replacement deeper than the persisted policy fails closed.
- An unchanged source tip waits for the configured poll interval.

Append and replacement commits advance block facts, direct indexes, chain
epoch, ordered sequence digest, and retained canonical event state atomically.
The writer does not perform historical wallet-output lookups or read wallet
projection state.

Readiness becomes ready only when canonical lag is within
`ingest.follow.lag_threshold_blocks` and the live mempool gate, when configured,
has completed its current source snapshot. Source failures move readiness to
`node_unavailable`, preserve committed state, back off by failure class, and
resume from the current fence. Storage corruption, schema mismatch, invalid
reorg depth, and internal invariant failures terminate the writer.

## Canonical events and retention

Every visible append or replacement publishes an authenticated retained event
with a monotonically increasing sequence. Projectors use event history to bind
their progress to canonical state. A writer-owned retention lease prevents
pruning below a projector's required anchor. Event retention and lease
operations are serialized through the canonical owner.

See [Chain events](chain-events.md) for event semantics and cursor behavior.

## Mempool ownership

The live mempool owner starts with the control surface and consumes either the
Zebra indexer stream or the JSON-RPC source selected by configuration. It
publishes a complete in-memory snapshot before satisfying the writer readiness
gate, then applies typed added, mined, and invalidated events. Retained mempool
events support cursor resume and bounded audit queries.

Mempool state is not part of a canonical block commit. The control API exposes
snapshot pages, transaction lookup, and event streaming without granting any
reader a canonical write handle.

## Configuration

The release writer uses these sections:

- `[network]` and `[node]` select and authenticate the upstream source;
- `[storage]` selects the canonical path, immutable raw-blob retention, and
  RocksDB resource budget;
- `[ingest.construction]` bounds fresh construction;
- `[ingest.follow]` controls polling and readiness lag;
- `[ingest.run_overrides]` supplies an optional checkpoint or target height;
- `[retention]` controls canonical and mempool event windows; and
- `[ingest_control]` configures the private authenticated control listener.

`ingest.phase_classification` remains available to the diagnostic `probe`
command and the artifact-oriented ingestion library. It does not choose between
two release binaries or require an operator handoff.

## Invariants

- Only `zinder-ingest` opens canonical storage for writes.
- Construction never publishes before complete validation.
- Following never commits a replacement beyond the fixed reorg policy.
- Canonical commits contain block-local facts and direct canonical indexes, not
  wallet or explorer query models.
- Source errors are recoverable readiness states; integrity errors fail closed.
- Secrets and raw authorization material never appear in logs or
  `--print-config` output.
