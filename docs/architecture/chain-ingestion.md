# Chain ingestion

`zinder-ingest` is the only canonical writer. The release runtime either opens
an authenticated ready store or constructs one in a writer-owned staging path,
then continuously follows Zebra. It also owns live mempool state and the private
control APIs used by projectors and trusted readers.

## Runtime sequence

```text
load and validate configuration
  -> connect to Zebra and discover capabilities
     (with [node_fallbacks.N], admit every endpoint and start on the first admitted one)
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

The ordered loader appends each block's note commitments to one in-memory
frontier per active pool. It writes a tree-state checkpoint every 100 blocks
and at the build tip. Before publication, only the build-tip checkpoint is
compared with the node's tree state. The loader therefore cross-checks each
checkpoint as it writes it: the root of the in-memory frontier must equal the
root of the checkpoint's canonical `finalState` encoding. A mismatch stops
construction, and nothing is published. The check costs at most one more root
computation for each pool that changed since the previous checkpoint. The
node's tree state for the build tip is requested by the tip's hash. If that
block has left the node's best chain, construction stops and publishes nothing.

Readers serve the tree state at every other retained height from the same
derivation. The canonical reader seeds an accumulator from the nearest
checkpoint below the height and appends each stored compact block's note
commitments up to it, checking parent links and the tree sizes each compact
block records. Live following stores a checkpoint for every block it appends,
so only construction-built history is reconstructed, at most 99 blocks at a
time. See [Wallet data plane](wallet-data-plane.md) for the read contract.

The loader also derives the root of every subtree that a loaded block
completes, and construction stores those derived roots. Construction reads the
completed ranges with `z_getsubtreesbyindex` before it requests the build
tip's tree state, so a node that switched to another chain between the two
requests refuses the second one. A node that still holds the build tip must
report the derived roots exactly, with the same indexes and completing
heights. When its first answer differs, construction reads the ranges once
more, because a node that switched away and back between the two requests
answered the first one for the other chain. When the second answer also
differs, construction stops with `SourceSubtreeRootMismatch`, which names the
pool and subtree index, and publishes nothing.

`CanonicalConstructionSettings` and `CanonicalPipelineLimits` bound:

- source segment size and target response bytes;
- concurrent source requests and reserved response bytes;
- block-preparation concurrency; and
- the memory reserved by blocks admitted to preparation until their ordered
  store write completes.

Startup rejects `ingest.construction.source_segment_max_blocks` above 1000.
Each segment block is one entry in a JSON-RPC batch, and Zakura refuses a batch
with more than 1,000 entries.

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

Shutdown stops construction, and the writer returns without admitting a
store. Source requests stop as soon as shutdown is requested. While blocks
stream into the builder, the blocking loader stops at its next block boundary
and leaves its block-load staging for the next start to discard, so shutdown
does not wait for a large deletion. Two later stages cannot be interrupted:
SST finalization after the last block, and publication, which flushes and
reopens the store. The writer waits for them to finish, so no `RocksDB` work
outlives the writer. Finalization merges the reverse indexes of the whole
build, so on a long chain it can outlast the supervisor's grace window, and
the supervisor's kill is then a crash during construction. In every case the
next start applies the restart rules above: a cancelled construction is
equivalent to a crash before publication, and a publication that finished
during shutdown leaves a staged READY store that is installed.

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

Each append also authenticates the note-commitment trees. Ingest appends the
block's note commitments to the visible-tip frontier of every active pool. The
result must equal the node's validated tree state for that block, byte for
byte. The canonical store repeats the derivation from its own persisted
checkpoint before it commits. Because the result is compared with the node's
tree state, the append takes each changed pool's root from its canonical
encoding and skips the cross-check that construction checkpoints run. Frontier
validation reuses the roots of the 16 most recently validated encodings from a
process-wide memo, keyed by pool and exact `finalState` bytes. After the first
append in a writer process, a pool that the block leaves unchanged costs no
root computation. A pool that the block changes costs one, for the node's new
tree state.

Ingest reads a block and its tree state with two node requests, and the node
can switch its best chain between them. The tree-state request therefore names
the block that ingest just read by its hash, for each append and for each block
of a replacement. When that block has left the node's best chain, the request
fails as a changed upstream view. Readiness reports `node_unavailable` with
class `upstream_view_changed`, committed state and the fence are kept, and the
next iteration observes the node again. The writer logs a
`canonical_source_recovery` warning when an outage starts or changes class, and
counts every retry in `zinder_ingest_canonical_source_recoveries_total`. A tree
state for the same block whose
time or frontiers differ from the derived result is a contradiction, not a
changed view. It stops the writer with `SourceCheckpointMismatch`, which names
the part that differed.

Each append also derives the root of every subtree that the block completes,
from the same frontier that authenticates the block's tree state. A root is
taken when the commitment that fills the subtree's last leaf is appended, so a
subtree that the block completes partway through its commitments is included.
Ingest commits the derived roots. For a block that completes a subtree, it
also reads the completed range with `z_getsubtreesbyindex` and requires the
node's roots, indexes, and completing heights to equal the derived ones. A
block that completes no subtree costs no subtree-root request. The subtree-root
request names no block, so the node can answer for a sibling block that
replaced the block after ingest read it. When the roots differ, ingest requests
the block's tree state again by its hash. A node that no longer holds the block
refuses that request as a changed upstream view, and the next iteration
observes the node again. A node that still holds the block is asked for the
range once more, because it may have switched away and back between the two
requests. When the second answer also differs, the writer stops with
`SourceSubtreeRootMismatch`, which names the block height, pool, and subtree
index. Nothing is committed for that block, so a restart resumes from the kept
fence. A node that switches to the sibling again before the second request
can also cause this stop, but a stop that recurs at the same block means that
the node and ingest disagree on the root. Replacement blocks are checked the
same way.

Readiness becomes ready only when canonical lag is within
`ingest.follow.lag_threshold_blocks` and the live mempool gate, when configured,
has completed its current source snapshot. Source failures move readiness to
`node_unavailable`, preserve committed state, back off by failure class, and
resume from the current fence. Storage corruption, schema mismatch, invalid
reorg depth, and internal invariant failures terminate the writer.

### Node endpoint failover

With `[node_fallbacks.N]` endpoints configured, ingest reads through the active
endpoint of the ordered node list ([Node source boundary §Ordered node
list](node-source-boundary.md#ordered-node-list)). The rules are recorded in
[ADR-0013](../adrs/0013-source-failure-recovery-topology.md) Decision 8.

At startup, every endpoint is checked concurrently: required capabilities,
activation table, and genesis block. The first admitted endpoint in position
order becomes active as soon as every lower position has a verdict, and its
table is the one the store admits. Startup does not wait for higher positions:
an endpoint still being checked then is deferred (`node_endpoint_deferred`) to
the standby monitor, so a fallback that does not answer never delays a healthy
`[node]`. An endpoint that reports pruned storage, lacks a required capability,
answers for another network, or advertises another activation table is
excluded until restart (`node_endpoint_excluded`). A fallback that would
become active with a table other than the one the store follows, its latest
admitted revision or, before the first, its construction table, is excluded
the same way, so only `[node]` can bring a new table; the comparison
waits until every lower position has a verdict, so a fallback that already
advertises the table `[node]` brings stays a standby. An endpoint that times out,
fails a call, or does not report its storage mode is unreachable
(`node_endpoint_unreachable`) and is evaluated again. When no endpoint is
admitted, startup fails with the readiness `[node]` alone would have reported.
A single-endpoint deployment keeps its startup checks unchanged and runs no
genesis check.

While ingest follows, a standby monitor polls every endpoint on the
`[node.health]` cadence, 30 seconds by default. Each endpoint's evaluation is
bounded by that cadence, or by 5 seconds when the cadence is shorter, and is
recorded as soon as it completes, so an active endpoint that hangs never delays
the evidence about the others. The monitor records whether the active endpoint
reports ready, and whether each other endpoint could take over: it must report
ready, advertise the required capabilities and the admitted table, hold the
configured network's genesis block, and have a tip at or above the visible tip. It is a standby when it holds the
visible tip, and a diverged standby when it holds only the settled tip. The
follower switches between iterations after three consecutive
`node_unreachable` or `stream_disconnected` recoveries, or once the active
endpoint has reported not ready for 300 seconds. It tries standbys in position
order, then diverged standbys, rechecking only that each candidate still
reports ready, its tip, and the block it holds at the visible or settled tip.
Every candidate is rechecked at once within one five-second bound for the
whole attempt, and the follower switches to the first one in that order that
passes and logs
`node_endpoint_switched`. A diverged standby's suffix above the settled tip is
replaced within the reorg window as any reorg is. When no endpoint qualifies,
the follower stays on its endpoint and recovers as before, and it attempts the
switch again only after the monitor records another evaluation, so a follower
waiting at its tip does not repeat the attempt on every iteration.

A switch to a standby keeps the visible tip confirmed, because the final check
found that block on the new endpoint, so wallet-serving readers keep serving
even when the new endpoint's first iteration fails. A switch to a diverged
standby resets tip confirmation until the replacement commits, so readers see
`tip_unconfirmed` meanwhile. Either switch skips the recovery backoff, stops
admitting the mempool certification made from the previous endpoint, and ends
the live mempool stream, so the owner rebuilds from the new endpoint; ingest
readiness stays withdrawn until the owner certifies a snapshot after the
switch, even when it is still busy with the previous endpoint.
Each endpoint's client applies that endpoint's own `request_timeout_secs`, and
the writer bounds every node call by the longest timeout in the list, so a call
to a fallback slower than `[node]` is not cut short after a switch.
There is no timed failback. The next switch walks positions from 0 again, and a
restart starts at position 0. Fresh construction does not switch: a node
failure there exits, and the restart admits the first reachable endpoint.

## Canonical events and retention

Every visible append or replacement publishes an authenticated retained event
with a monotonically increasing sequence. Projectors use event history to bind
their progress to canonical state. A writer-owned retention lease prevents
pruning below a projector's required anchor. Event retention and lease
operations are serialized through the canonical owner.

See [Chain events](chain-events.md) for event semantics and cursor behavior.

## Mempool ownership

The live mempool owner and the mempool-event retention task start once the
writer admits a READY canonical store, not with the control surface. During
fresh construction the writer does not service its command queue, so durable
mempool replay and retention would only time out. The owner consumes either the
Zebra indexer stream or the JSON-RPC source selected by configuration. It
publishes a complete in-memory snapshot before satisfying the writer readiness
gate, then applies typed added, mined, and invalidated events. Retained mempool
events support cursor resume and bounded audit queries.

Mempool state is not part of a canonical block commit. The control API exposes
snapshot pages, transaction lookup, and event streaming without granting any
reader a canonical write handle.

## Configuration

The release writer uses these sections:

- `[network]` and `[node]` select and authenticate the upstream source, and
  optional `[node_fallbacks.N]` tables add fallback endpoints;
- `[storage]` selects the canonical path, immutable raw-blob retention, and
  RocksDB resource budget;
- `[ingest.construction]` bounds fresh construction;
- `[ingest.follow]` controls polling and readiness lag;
- `[ingest.run_overrides]` supplies an optional checkpoint or target height;
- `[retention]` controls canonical and mempool event windows; and
- `[ingest_control]` configures the private authenticated control listener.

`ingest.phase_classification` is read only by the diagnostic `probe` command.
The writer selects construction or following from the store lifecycle described
in [Runtime sequence](#runtime-sequence), not from a gap threshold, so no
setting chooses between two binaries or requires an operator handoff.

## Supported chains

Zinder indexes the public Mainnet and Testnet chains and Regtest. Each
`Network` carries a fixed genesis block hash. A genesis-complete build uses
that hash as its history predecessor, so it fails when the node's block 1 does
not descend from it.

A Zebra or Zakura test network configured with its own genesis block is not
supported. Its activation heights are discovered like any other network's, but
its genesis differs from the one Zinder expects. A checkpointed build never
compares the node's chain with the genesis hash. Pointing a checkpointed build
at such a node can therefore store a different chain under the `zcash-testnet`
identity.

## Invariants

- Only `zinder-ingest` opens canonical storage for writes.
- Construction never publishes before complete validation.
- Following never commits a replacement beyond the fixed reorg policy.
- Canonical commits contain block-local facts and direct canonical indexes, not
  wallet or explorer query models.
- Source errors are recoverable readiness states; integrity errors fail closed.
- Secrets and raw authorization material never appear in logs or
  `--print-config` output.
