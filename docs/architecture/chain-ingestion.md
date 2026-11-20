# Chain ingestion

This page explains how `zinder-ingest` builds and maintains the canonical
store, the block facts and indexes that every other runtime reads. It is for
operators and contributors who need to predict what ingest does on first start,
at the tip, on a reorg, after a restart, and when a node fails.

`zinder-ingest` is the only process that writes the canonical store. It works in
two phases:

1. **Fresh construction.** With no store on disk, ingest bulk-builds one from
   genesis to one reorg window below the node's tip at that moment, in a
   staging directory. Readers see nothing until the build is published.
2. **Continuous following.** With a published store, ingest polls the node and
   appends one block at a time, or replaces recent blocks after a reorg.

Ingest picks the phase from what is on disk, not from a setting. It moves from
construction to following once, when it publishes the build. Following then
appends the newest window of blocks and those mined during construction.

## Startup sequence

1. Check every configured node endpoint. See
   [What ingest checks before it writes](#what-ingest-checks-before-it-writes).
2. Admit the canonical store: open the published store at `storage.path`, or
   install a published build left in staging, or discard unpublished staging
   and construct a fresh store.
3. Record the node's activation table and the configured reorg window in the
   store when they extend what it already holds.
4. Start the mempool owner, when the private control listener is configured,
   and, with fallback endpoints, the standby monitor.
5. Follow the chain and serve the private control API to the projector and
   readers.

An existing store whose network, raw-blob retention, build plan, or schema
does not match fails startup unchanged. So does a configured reorg window
smaller than the store's. See
[Reorgs and the reorg window](#reorgs-and-the-reorg-window).

Network-upgrade activation heights come from the node. A node table that only
adds or moves activations above the store's tip becomes a new schedule
revision. A change at or below the tip fails startup. See
[ADR-0008](../adrs/0008-consensus-parameters-from-the-node.md).

## Fresh construction

Construction fixes its target before it loads data: one reorg window
(`ingest.reorg_window_blocks`) below the node's tip at that moment, or
`ingest.run_overrides.target_height` if lower. When the node's chain is
shorter than the window, the target is the first block the store retains. It
starts at genesis unless `ingest.run_overrides.checkpoint_height` names a
checkpoint.

Stopping a window below the tip keeps a reorg of the node's newest blocks from
orphaning the build. The build tip is the published store's settled tip.
Without a lower run target, that is the settled tip the store would have had if
construction built to the node's tip. Following then appends the remaining
newest blocks one at a time, as in
[Continuous following](#continuous-following), before ingest reports ready.

Ingest fetches blocks in bounded batches, prepares them in parallel, and writes
them in order. It rebuilds the note commitment trees from the blocks and stores
a tree-state checkpoint every 100 blocks and at the build tip. The loader
refuses a checkpoint that does not carry a frontier for exactly the pools the
node's activation table makes active at its height, including a pool whose
tree is still empty.

Before publishing, construction compares its derived tree state at the build
tip, and every subtree root the loaded blocks complete, with the node's. A
mismatch stops construction with an error that names it, such as
`SourceSubtreeRootMismatch`, and nothing is published. Publication validates
the whole build and installs it in one step.

The `[ingest.construction]` settings bound batch size, request concurrency, and
the memory held by blocks in flight. See
[Configuration](../reference/configuration.md) and
[ADR-0009](../adrs/0009-ingest-lifecycle-and-construction.md).

### Restart during construction

A restart during construction starts the build over. Any stop before
publication discards staging, and the next start rebuilds from genesis.
Construction does not fail over between nodes. The projector follows the same
rule; see
[Initial sync](../runbooks/initial-sync.md#restarts-during-the-first-sync).

## Continuous following

The follower polls the node's tip every `ingest.follow.poll_interval_ms`
(1,000 ms by default):

- When the next block connects to the store's tip, ingest appends it.
- When the node's chain differs within the reorg window, ingest replaces the
  differing blocks.
- When nothing changed, it waits for the next poll.

Each append or replacement is one atomic commit of the block facts, the
indexes, the next chain epoch (the store version readers pin), and a
[chain event](chain-events.md). Readers see the old state or the new state,
never a mix.

Each append also authenticates the note commitment trees. Ingest derives the
block's tree state and requires it to equal the node's, byte for byte. It asks
for the node's tree state by block hash, so a node that switched chains in
between refuses instead of answering for another block. For a block that
completes a shielded subtree, ingest also compares the derived root with the
node's. A contradiction stops the writer with `SourceCheckpointMismatch` or
`SourceSubtreeRootMismatch`, and nothing is committed for that block.

Ingest is ready when the store is within `ingest.follow.lag_threshold_blocks`
(1 by default) of the node's tip and the mempool owner, when it runs, has
loaded its snapshot. See [Readiness](../reference/readiness.md).

### Reorgs and the reorg window

Ingest replaces blocks within the newest `ingest.reorg_window_blocks` blocks,
1,000 by default. The block at the bottom of that window is the settled tip, and
ingest never rewrites it or anything below it.
[ADR-0010](../adrs/0010-reorg-window-and-displaced-blocks.md) explains why the
default matches the node's own finalization bound.

On a reorg, ingest walks back to the last block both chains share, prepares the
replacement suffix in memory, and commits it atomically. The memory held is
capped by `ingest.follow.max_replacement_bytes` (2 GiB by default). A larger
replacement stops the writer with an error that names the setting.

A fork at or below the settled tip is deeper than the window. Ingest then
parks: it stops following, keeps its control API open, and reports
`reorg_window_exceeded`. Readers refuse requests with `REORG_WINDOW_EXCEEDED`.
A parked writer needs an operator. A node whose tip is an older block of the
store's own chain is behind, not forked, so ingest reports `node_unavailable`
and waits.

Raising `ingest.reorg_window_blocks` takes effect on the next start. The
settled tip then holds until the visible tip is a full new window above it.
The projector raises the wallet store's window to match, and readers require
the exact recorded window. Lowering the window fails startup. A release that
does not know the raised format refuses the store, so a raise cannot be undone
without a rebuild.

### Node failures while following

A node failure does not stop the writer. Ingest keeps its committed state,
reports `node_unavailable` with the failure class, backs off, and resumes from
its last commit. It logs `canonical_source_recovery` and counts retries in
`zinder_ingest_canonical_source_recoveries_total`. Storage corruption, a schema
mismatch, a reorg beyond the window, and a broken internal invariant stop the
writer instead.

While it waits on the node, the follower still answers read-only control
commands such as `WriterStatus` at once. Writes, such as mempool appends, queue
(up to 64) and apply when the wait ends.

### Tree-state densification

A first sync stores a tree-state checkpoint only every 100 blocks, so a
tree-state read between two of them replays stored blocks. Once ingest follows
the tip, and Explorer views have caught up when they are enabled, a background
pass fills settled history with stored checkpoints. Live following already
stores one per block, so only history from a first sync needs the pass. The
`ingest.tree_state_densification.selection` setting decides which blocks get
one:

- `every-block`, the default, stores one for every block, so a tree-state read
  at any height in a completed stride is a stored read with no replay. Heights
  in the stride the settled tip has not yet passed, such as 301 to 359 while
  the tip is settled at 360, still replay from the nearest checkpoint until the
  tip crosses the stride's end. On Mainnet this is
  estimated to add 2.4 GB of rows before compression and about 4 to 5
  CPU-hours of one-time work; both figures await measurement on a Mainnet
  deployment.
- `commitment-budget` stores one each time `commitment_budget` note
  commitments (1,024 by default) have accumulated since the previous
  checkpoint, so a read between two of them replays fewer than that many
  commitments. On Mainnet this adds an estimated 0.13 GB. Setting
  `commitment_budget` without a `selection` chooses it.

The pass works one stride at a time, from one stored checkpoint to the next
multiple of 100. It replays the stride's compact blocks from the first
checkpoint with the per-block step a tree-state rebuild runs, checks every
replayed block against the canonical header at its height, and accepts the result
only when the replay reproduces every checkpoint already stored in the stride,
including the one that ends it, byte for byte. The writer then adds the stride's rows in one batch between live
commits, after checking that the stride ends at or below the settled tip, that
the replay ended at the block stored at the stride's end, that both stored
checkpoints are unchanged, and that a row already stored at one of its heights
holds the same bytes. A checkpoint row holds no block hash, so the end block is
what ties the replayed blocks to the writer's chain. A stride that fails a check is not written:
ingest logs `tree_state_densification_integrity_failure`, counts it, and reads
there keep replaying.

Only settled history is densified, so no reorg reaches a densified row. A
stride commits whole or not at all, and its commit is synced before the writer
answers. A stride commit leaves the writer's fence where it is, so a reader
that already has the store open sees the new rows from its next refresh after
the commit, which for the wallet-serving pair follows the next live commit.
Until then its reads in that stride replay from the cadence checkpoint and
return the same tree states. The pass scans settled strides in chunks of 200 reads, checking every gap
between stored checkpoints, and queues the strides that do not satisfy the
selection. It records how far every stride satisfies the selection in
`tree-state-densification-watermark.json` inside the canonical store
directory, together with the store's construction manifest digest and the
selection, and never past a stride it has not committed or found satisfied,
or one it skipped as damaged.

When ingest starts, the pass resumes at that height if the file names this
store and the configured selection and the stride that ends at it still holds
the selected checkpoints; otherwise it ignores the file, logs
`tree_state_densification_watermark_discarded`, and scans from the start, as
it does after a change of selection. The file is written only after the
commits it covers are durable. Only the stride that ends at the recorded
height is checked again on start, so the pass does not look for damage below
it; the [verification pass](#tree-state-verification) does. A read there
serves its stored row as written, as every stored read does
([Wallet data plane](wallet-data-plane.md#tree-states-and-subtree-roots)). A
missing densification row only makes that read replay, but a missing cadence
checkpoint is store damage: a read at its height is refused, as is any read
above it that would replay from below it. The writer commits through its
command queue, so the pass runs only when `ingest_control.listen_addr` is set,
as it is for wallet serving.

`ingest.tree_state_densification.workers` (2 by default) bounds how many
strides the pass replays at once, each on one CPU, and `max_blocks_per_second`
(500 by default) bounds, on average, how many stored blocks it replays per
second, including replays the writer could not commit and the pass retries; a scan is charged one block for every two reads it makes, counting each checkpoint row it visits and each compact block it reads as one read. A replayed block costs
about two reads, its compact block and its header, and adds at most one row,
so the defaults hold the pass near 1,000 reads per second on average, with
short bursts of up to one stride per worker. A scan stops after the stride that
brings its reads to 200, the charge of one replayed stride, so densified
history is scanned a stride or two at a time and the pass paces each chunk. A
scan that fails is charged 350 reads before its retry, the 199 reads a chunk
makes before its last stride plus the 150 reads the costliest stride adds, so
repeated failures stay within the configured rate. Progress is reported by the
`zinder_ingest_tree_state_densification_*` metrics in
[Metrics](../reference/metrics.md). A canonical rebuild returns history to the
100-block cadence until the pass runs again.

### Tree-state verification

A stored tree-state read serves its row after checking only the row's
framing, so a row rests on the checks made when it was written and on
RocksDB block checksums. A second background pass checks every settled row
again, as a writer checks a row it stores:

- Each frontier parses with the official Zcash codec and is its canonical
  encoding.
- Each frontier derives the root stored beside it.
- The row carries a frontier for exactly the pools active at its height, even
  when a pool's tree is still empty.
- The row's block time is the time in the canonical block header at its
  height. The history predecessor has no stored header, so its row carries
  the block time recorded for it when the store was built. A row at any other
  height fails this check when its header is missing, does not decode, or
  sits in a table block that fails its checksum when read.

The pass starts when densification does: once ingest follows the tip, and
Explorer views have caught up when they are enabled. A restart over a store
it reports as damaged starts it sooner, as described below. It reads through
its own canonical secondary and writes nothing to the store, so it runs
without `ingest_control.listen_addr`; without that listener, no reader
attests the store, and ingest readiness alone reports the damage.

The pass works in cycles. A cycle checks every row from the history
predecessor through the settled tip, including rows that densification or
live following added since the previous cycle. The next cycle starts 5
minutes after one ends. The pass never reads an entry above the settled tip,
including a key that is not a height just past it, so a row above the tip is
checked in the first cycle that starts after the tip passes it.

`ingest.tree_state_verification.max_rows_per_second` (25 by default) bounds
how many rows the pass checks per second, on average. The pass reads 100 rows
at a time and then waits until it is back under that rate, counting the rows
a step checked before a store error stopped it, which it does not check
again. A row with three pools costs about 4.3 ms of CPU to check, so the
default holds the pass near a tenth of one CPU and checks about 2.2 million
rows a day.

The rate also sets the detection bound: a damaged row is found within one
cycle after it is written or damaged. The waits alone take the settled row
count divided by the rate, plus 5 minutes between cycles, so a Mainnet store
with a row for each of about 3.5 million blocks needs an idealized 39 hours
per cycle at the default rate. A real cycle takes longer: checking adds up to
4.3 ms per row, the pass waits while historical work is closed, a store error
costs a 30-second retry, and a restart can check up to a minute of rows again.

The verification secondary bypasses the RocksDB block cache for checkpoint
and header rows. For rows stored in table files, each read checks the
checksum of the data block again. A block that fails its checksum is damage,
reported like a row that fails a check, at the first height it may hold. A
table file that the writer's compaction deleted under the pass's secondary is
not damage: the step stops, and the pass retries it after catching up.

The pass records the height its current cycle continues from in
`tree-state-verification-watermark.json` inside the canonical store
directory, together with the store's construction manifest digest and any
damage it found. It writes the file at most once a minute and at the end of
each cycle, so a restart checks again at most a minute of rows. When ingest
starts, the pass resumes at that height if the file names this store.
Otherwise it logs `tree_state_verification_watermark_discarded` and starts a
new cycle. A resumed scan starts just after the row below that height, so it
also reaches a key that is not a height and sorts between the two.

A row that fails a check is store damage. The pass then does five things, in
this order:

1. It reports the damage to the canonical control plane, so every
   `CanonicalControl.WriterStatus` answer carries it in
   `canonical_store_damaged`, with the row's height and, when one pool's
   frontier failed, that pool.
2. It parks ingest readiness with `canonical_store_damaged`, unless the
   writer is already parked on a reorg deeper than its window, whose
   `reorg_window_exceeded` stays the cause. Ingest stays not ready until it
   stops.
3. It logs `tree_state_verification_mismatch` with the row's height, the pool
   whose frontier failed (`sapling`, `orchard`, `ironwood`, or `none` when the
   row fails outside any frontier), and the check that failed, and counts the
   row in `zinder_ingest_tree_state_verification_mismatches_total`.
4. It records the damage and its height in its progress file. That write
   waits on two syncs, which is why the report comes first. A write that
   fails is logged as `tree_state_verification_damage_record_failed` and
   tried again after 1 second, then after twice as long each time, up to 30
   seconds, until it succeeds or ingest stops. The damage stays reported
   meanwhile.
5. Once the record is written, it stops.

The writer keeps following the tip. Wallet-serving readers, and
compatibility in compact-blocks mode, refuse traffic with
`canonical_store_damaged` on their next writer-status read, without the
staleness allowance, and keep refusing through a writer-status outage. The
projector keeps following, as it does behind a parked writer.

When ingest starts over a store whose progress file records damage, its
control plane reports that damage from its first `WriterStatus` answer, so no
reader attests the store again. The pass then opens its secondary as soon as
the store is published, without waiting for historical work, and checks the
damage again:

- Damage recorded at a row clears only once a row stored at that height
  passes. A row that is gone stays damage, because the store never drops a
  settled row and every reconstruction that seeds from it fails without it.
- Damage recorded where the pass could not read an entry as a row, such as a
  key that is not a height, clears once the verification step that starts at
  that height passes.

Damage that is still there parks readiness again and the pass stops. Damage
that clears stops being reported, which ingest logs as
`tree_state_verification_recorded_damage_cleared`, and the cycle continues.
Damage recorded for another store is discarded.

A progress file that ingest cannot read or parse, or that another version
wrote, is damage at an unknown height, logged as
`tree_state_verification_record_unreadable`. `WriterStatus` reports
`canonical_store_damaged` without a height from its first answer, and the
pass runs a full cycle from the history predecessor without waiting for
historical work. The damage clears once that cycle passes, and it is
confirmed at the row the cycle finds damaged. A missing file only starts a
new cycle. With the pass disabled, recorded damage, and damage behind an
unreadable file, stays reported. Rebuild the canonical store, as
[Recovery](service-operations.md#recovery) describes.

The pass checks each row on its own and does not replay blocks. It cannot see
two kinds of damage:

- A row that holds valid trees of another height.
- An Orchard frontier stored in the Ironwood position with its own root, or
  the reverse, because both pools hash alike.

The write-time checks prevent those. Live following compares each row with
the node's tree state, and densification replays each stride before it writes
rows.

## Node endpoint failover

You can list fallback nodes after `[node]` as `[node_fallbacks.1]`,
`[node_fallbacks.2]`, and so on; see
[Configuration](../reference/configuration.md#the-node-sections) for the format. Ingest reads
from one active endpoint at a time and switches when it fails.
[ADR-0007](../adrs/0007-node-failure-recovery-and-transport-policy.md) records
the rules.

| Situation | Failover |
| --- | --- |
| Continuous following | Ingest switches between follow iterations. |
| Broadcast from `zinder-query` or wallet-mode compatibility, and `GetStandardFee` in wallet-mode compatibility | Moves to the next verified endpoint within the request. |
| Fresh construction | None. A node failure exits, and the restart begins again. |
| Explorer | None. It reads only `[node]`. |

**At startup**, ingest checks every endpoint concurrently and starts on the
first one in list order that passes, without waiting for slower higher
positions. An endpoint is excluded until restart when it reports pruned
storage, lacks a required capability, has another genesis block, or advertises
another activation table. Only `[node]` can introduce a new table. An endpoint
that does not answer is checked again later. With an existing store, a
fallback must also be ready, reach the store's tip, and hold the store's tip
or settled tip. When no
endpoint passes, startup fails with the readiness `[node]` alone would report.

**While following**, a standby monitor checks every endpoint each
`[node.health]` poll interval (30 seconds by default). A standby must be ready,
advertise the required capabilities and the store's activation table, have the
configured genesis block, and have a tip at or above ingest's tip.

The follower switches after either:

- three consecutive recoveries of the same transport class, either
  `node_unreachable` or `stream_disconnected`;
- 300 seconds of the active endpoint reporting not ready.

Other failure classes, such as a malformed response, never switch, because
switching would hide the problem behind whichever node answers. The follower
tries standbys that hold ingest's tip first, in list order, then standbys on
another branch above the settled tip, which it handles as a reorg. It logs
`node_endpoint_switched`, restarts the mempool stream from the new endpoint,
and stays not ready until that snapshot loads.

Failover rules:

- The follower stays on its new endpoint until the next switch, which walks
  the list from position 0. A restart also starts at position 0.
- Every endpoint must advertise the same activation table.
- Each endpoint keeps its own `request_timeout_secs` and `max_response_bytes`.

[Metrics](../reference/metrics.md) lists the endpoint state gauge.

## What ingest checks before it writes

Before it opens storage, ingest checks each endpoint it will read:

- **Archive storage.** The node must report `pruned: false`. A pruned node is
  refused, including as a fallback, with readiness cause
  `node_capability_missing`. A node that does not report its storage mode
  cannot become active; with fallbacks, ingest checks it again later.
- **Capabilities.** The node must provide the JSON-RPC methods ingest needs;
  see [Node source boundary](node-source-boundary.md).
- **Network identity.** Block 0 must have the configured network's genesis
  hash. The chain name a node reports cannot replace this check, because Zebra
  reports `test` for both Testnet and Regtest.

The genesis check runs on `[node]` in a single-endpoint deployment, on every
endpoint checked at startup, and on every standby evaluation. A fallback that
startup had not reached when it chose an endpoint is logged as
`node_endpoint_deferred` and skips the startup check; the standby monitor runs
it before the endpoint can become a candidate. The diagnostic
`zinder-ingest probe` command does not run it.

When `[node]` fails the genesis check and no endpoint passes, startup fails and
logs `node_endpoints_unavailable`, and `/readyz` reports `node_unavailable`
with failure class `configuration`. A failed block 0 read, for example while
the node is starting, is not evidence of another chain. Startup then fails with
that read's own failure class.

## Mempool ownership

Ingest owns the live mempool when its private control listener
(`[ingest_control]`) is configured. The mempool owner starts once a published
store is open, not during construction. It streams from the node's indexer
gRPC when `node.indexer_grpc_addr` is set, and polls JSON-RPC otherwise. It
loads a full snapshot before ingest reports ready, then applies added, mined,
and invalidated events. Mempool state is not part of block commits. Readers get it
through the private control API. See
[ADR-0012](../adrs/0012-mempool-topology-and-retention.md).

## Supported networks

Ingest indexes Zcash Mainnet, Testnet, and Regtest, each with a fixed genesis
hash. Custom activation heights work, because ingest reads them from the node.
Ingest requires one of these genesis hashes, so it refuses to start on a Zebra
or Zakura test network with its own genesis block, checkpointed builds
included.

## Invariants

- Only `zinder-ingest` opens the canonical store for writes.
- Construction publishes nothing before it validates the whole build.
- Following never replaces a block at or below the settled tip.
- Canonical commits hold block-local facts and direct indexes, not wallet or
  explorer query models.
- Node failures are recoverable readiness states. Integrity failures stop the
  writer.
- Node credentials never appear in logs or `--print-config` output.
