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
a tree-state checkpoint every 100 blocks and at the build tip.

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
| Broadcast from `zinder-query` or wallet-mode compatibility | Moves to the next verified endpoint within the request. |
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
