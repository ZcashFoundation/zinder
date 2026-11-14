# Initial sync

This runbook covers the supported single-host wallet-serving topology. The
canonical writer constructs and publishes canonical RocksDB, the projector
builds wallet RocksDB from an authenticated canonical fence, and the native
query plus lightwalletd compatibility runtimes begin serving only after their
process-local secondary readers form a wallet-serving pair.

There is no in-place migration from an incompatible store identity or schema.
Use empty target paths or a certified coherent restore bundle.

## Prerequisites

- A Zebra or Zakura archive node on the selected network with required
  JSON-RPC capabilities, attached as described in
  [Deploying on a VM](deploying-on-a-vm.md#deploy-with-compose). See
  [Archive node required for ingest](#archive-node-required-for-ingest).
- Enough local storage for canonical data, wallet state, staging, compaction,
  checkpoints, and growth reserve.
- Stable canonical and wallet primary paths on one host filesystem.
- Unique secondary metadata roots for projector, native query, and compatibility.
- Stable projector build-owner identity and private control bearer tokens.
- Resource limits that leave headroom outside RocksDB caches and memtables.

For Z3-managed nodes, start and synchronize the selected Z3 network before
starting Zinder. The example uses the Z3 Testnet env file; pick the file for
your node and network from the table in the VM runbook.

## Start the stack

```bash
docker compose \
  --env-file deploy/.env.testnet \
  -f deploy/docker-compose.yml \
  up -d --build
```

The dependency order is ingest, projector, then both serving runtimes, but
container start does not imply traffic readiness. Compose keeps each expected
initial construction in Docker's `starting` state for the corresponding
production hard gate: three hours for canonical construction, two hours for
wallet construction, and five minutes for each serving reader. A process that
is still non-ready after its grace period becomes unhealthy and stops the
dependency chain. Follow all four operational endpoints:

```bash
curl -sS http://127.0.0.1:19105/readyz  # ingest
curl -sS http://127.0.0.1:19110/readyz  # projector
curl -sS http://127.0.0.1:19106/readyz  # native query
curl -sS http://127.0.0.1:19107/readyz  # compatibility
```

## Canonical construction

On an empty canonical path, ingest creates a sibling construction staging
directory, fixes a source range, loads and validates it, publishes a baseline
fence, installs it at the configured path, and starts continuous following.
The configured path does not become a ready store until publication succeeds.

The checked ingest configuration uses `coverage = "wallet-serving"` and
explicitly sets `explorer_views = false`. The writer uses the
genesis block as the authenticated predecessor and retains
every block from height one. This is required for transparent predecessor
resolution even though shielded wallet data begins at later activation heights.
An explicit checkpoint remains available only for non-wallet-serving workloads
and must carry the required predecessor tree state and source identity.

Useful canonical metrics:

- `zinder_ingest_source_fetch_queue_requests`
- `zinder_ingest_source_fetch_queue_bytes`
- `zinder_ingest_canonical_tip_height`
- `zinder_ingest_canonical_lag_blocks`
- `zinder_ingest_canonical_historical_prevout_reads_total`
- `zinder_ingest_canonical_cross_block_wallet_reads_total`

The last two counters must remain zero.

If the process stops during construction, restart it with the same
configuration. A published staging store is installed and reopened. An
unpublished staging store is removed and reconstructed. Do not manually move
or edit the staging directory while the owner is running.

### Archive node required for ingest

Ingest reads every historical block body, so it requires a node that stores
all of them. At startup it reads `pruned` from `getblockchaininfo` and refuses
a node that reports `pruned: true` or omits the field. Readiness reports
`node_capability_missing` for capability `historical_block_bodies`, ingest
logs that the node reports pruned storage, and the process exits before it
touches the store.

The check applies on every start, including a restart of an already-built
store, even though following only needs recent blocks. Point ingest at an
archive node; native query and lightwalletd compatibility may keep using a
pruned node.

A Zakura node built by verified-commitment-trees fast sync is an archive node
for block bodies but never wrote per-height commitment trees or subtree roots
below its last checkpoint. That band exists only on Mainnet. Reads inside the
band fail with failure class `capability_missing`, and retrying against the
same node never succeeds:

- During construction, a checkpoint or subtree-range read inside the band
  stops initial sync, and the process exits with the error. Restarting against
  the same node fails the same way.
- While following, a tree-state read inside the band reports
  `node_unavailable` with that class and retries on the 10-second
  operator-action backoff.
- Native query returns the non-retryable `NODE_CAPABILITY_MISSING`.

Use a node synchronized without fast sync, or one that serves the band from
its historical frontier artifact.

## Wallet construction

Projector opens canonical storage through its own secondary, converges on the
writer fence (retrying while the writer commits ahead of the secondary, and
reporting `syncing` for lag longer than one second, until `projector.fence_convergence_timeout_seconds`
elapses), and binds a wallet build plan to that source identity. It acquires
a wallet projection build lease and a writer-owned canonical retention lease,
then builds the wallet store in its owned path.

Construction sorts transparent outputs, spends, and address rows externally.
Each sorter spills to a staging directory beside the wallet store, up to
`projector.build.max_temporary_file_bytes_per_sorter` bytes. The default is
206158430208 (192 GiB). A sorter that spills more than one run needs about
twice its spilled bytes, because its final merge writes every record again
while the input runs still exist. In late 2026, the largest sorter of a Mainnet
build from genesis, the outpoint sorter, needed about 99 GB.

The ceiling applies to each sorter separately, but the staging directory holds
several sorters' files and the finished SST files at the same time. While the
outpoint sorter's merged run is read, the address sorters and the spent-output
SST files grow beside it. The address-transaction sorter then merges while
those SST files remain. Scaled from the measured outpoint sorter by record
size, a Mainnet build from genesis peaks at about 130 GB of staging. This is an
estimate, not a measured peak. Keep about 140 GB free on the volume when wallet
construction starts. The finished wallet store keeps part of that space. The
ceiling does not check free space, so an undersized volume fails with a
disk-full error late in construction.

If the ceiling is too small, the projector stops as soon as a sorter's final
merge can no longer fit, which is usually while the canonical scan is still
running. The figure in that error covers only the records sorted so far, so it
is not a target: unset the setting to use the default, or raise it well above
that figure. A sorter refused after its input ended reports its exact
requirement instead. A restart with the same setting fails the same way.

Wallet publication requires the complete build digest and source position to
match the admitted canonical fence. The projector then catches up and takes
continuous following ownership before releasing construction leases.

For checkpointed canonical history, construction begins at the exact
authenticated first retained block rather than assuming height one. Every
transparent spend in the retained range must still resolve to an output in
that range; an unresolved pre-checkpoint prevout fails the build closed because
no transparent prefix state is carried by the checkpoint.

The `wallet-serving` coverage profile therefore uses complete non-genesis
history, with height zero as the authenticated predecessor and height one as
the first retained block. A shielded activation checkpoint is insufficient for
the global transparent projection because later transactions may spend outputs
created before that activation. If a READY store starts later than the coverage
requested at startup, ingest rejects it; rebuild the canonical and wallet stores
instead of attempting to follow the narrower store forward.

Restarting projector is safe. Lease generation, build state, source identity,
and ready evidence are persisted. A second projector using the same wallet path
must fail lease or writer admission rather than run concurrently.

## Wallet-serving admission

Native query and compatibility each maintain generation-specific canonical and
wallet secondaries. Each runtime catches both up, validates source identity and
event position, and publishes one immutable `WalletServingReadPair`. Only then
does its `/readyz` report ready and its gRPC readiness interceptor admit traffic.

Probe a data-bearing method after readiness:

```bash
grpcurl -plaintext -d '{}' 127.0.0.1:19102 \
  zinder.v1.wallet.WalletQuery/ServerInfo
grpcurl -plaintext -d '{}' 127.0.0.1:19067 \
  cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLightdInfo
```

An open canonical secondary plus an open wallet secondary is not sufficient.
The pair must agree on network, reorg policy, canonical source identity, event
fence, and wallet digest.

## Expected readiness states

| Runtime | Normal initial state | Ready boundary |
| --- | --- | --- |
| ingest | `starting`, `syncing`, or `upstream_not_ready` | Canonical lag is within threshold, mempool snapshot is complete, and a configured `[node.health]` probe reports Zebra ready |
| projector | `starting` or `syncing` | Wallet store is published and following the admitted canonical source |
| native query | `starting`, `replica_lagging`, `writer_status_unavailable`, `ingest_control_unavailable`, or `canonical_tip_unconfirmed` | One exact canonical and wallet pair is published, the admitted node is healthy, and `WriterStatus` plus a bounded mempool snapshot succeed through the admitted ingest-control channel |
| compatibility | `starting`, `replica_lagging`, `writer_status_unavailable`, or `canonical_tip_unconfirmed` | One exact canonical and wallet pair (or, for compact-block serving, one canonical reader) is published, and the writer has confirmed its tip against its node and does not report a park |

After the first publication, a reader that loses writer status or falls behind
reports `serving_pair_stale` and keeps answering from its published pair until
the staleness ceiling passes; only then does it fall back to
`writer_status_unavailable` or `replica_lagging` and stop admitting traffic.

A parked canonical writer keeps answering writer status with its frozen fence
and its halt. Native query and both compatibility serving modes then report
`reorg_window_exceeded` and refuse wallet traffic with
`REORG_WINDOW_EXCEEDED`, never `ready` on the frozen tip.

After ingest restarts, native query and both compatibility serving modes
report `canonical_tip_unconfirmed` until ingest's first source observation
confirms the store's tip against the node, which normally takes one poll. If
ingest cannot reach the node, they stay unready for as long as the outage
lasts; restore the node rather than restarting the readers. A writer that
finds a fork deeper than the window on that first observation parks, and the
readers move straight to `reorg_window_exceeded`.

A node restart can move the node's tip back below ingest's settled tip, for
example when Zebra cannot restore its unfinalized blocks and restarts from its
finalized tip. When that tip is a block ingest already holds, ingest reports
`node_unavailable` with class `upstream_view_changed` and waits for the node to
re-verify past its tip; it parks only when the node's tip is on another
branch.

`node_unavailable`, `upstream_not_ready`, and
`ingest_control_unavailable` are recoverable dependency conditions. For native
query, inspect the classified `ingest_control_health_unavailable` warning and
restore the admitted writer endpoint, authentication, writer status, or
tip-coherent mempool snapshot. Startup identity, network, contract-revision,
or required-capability mismatches fail before traffic binds and require
configuration or deployment correction rather than health-probe retries.
`schema_mismatch`, `reorg_window_exceeded`, corrupt store errors, and source
identity mismatches require operator action.

### Reorg-window sizing

The default reorg window is 1000 blocks, the depth past which Zebra 7 and
Zakura finalize blocks. Neither node can present a deeper reorg, so ingest
parks with `reorg_window_exceeded` only on a change the node itself could not
produce, such as a replaced or resynced node. The window is not a consensus
finality guarantee.

A smaller window parks on reorgs the node can produce. On 2026-07-20, a Zcash
testnet mining race produced a 114-block reorg after the losing branch had
remained visible for more than an hour, which exceeded the 100-block window
then in use. Production testnet moved to 300 blocks, and the node's bound is
higher still.

A replacement commits atomically, so ingest holds its whole suffix, up to one
window of blocks, in memory first: the raw blocks fork discovery fetched, then
their prepared form. `ingest.follow.max_replacement_bytes` (2 GiB by default)
bounds the estimated sum of both, so set it within the container's memory with
headroom for the one block being decoded. A replacement that would exceed it
fails before any write with `ReplacementBytesExceeded`, naming the depth, the
blocks prepared (zero when discovery alone exceeded it), and the bytes held. A fork deeper than the window still parks with
`reorg_window_exceeded` even when discovery exhausts the budget.
This is not a park: ingest exits, and under the wallet-serving image's
`ALWAYS` restart policy the container restarts, discovers the same fork, and
fails again, with wallets seeing an unavailable service instead of
`REORG_WINDOW_EXCEEDED`. Raise the limit to follow the reorg.

The settled tip trails the visible tip by the window, so surfaces that report
settlement or finality report it 1000 blocks behind the visible tip. Wallet
undo rows scale with the window; construction accounts for them against
`projector.build.max_accounted_reorg_undo_bytes`. Configure the same window for
ingest, projector, native query, and compatibility admission.

Upgrading to a release whose default is 1000 blocks raises existing stores the
first time ingest starts, when the deployment relies on the default, the shipped
`deploy/config` files, or the wallet-serving image. That raise is irreversible
in place, so capture a state bundle before starting the new release if you need
a rollback path.

To raise the window on an existing deployment, restart the canonical writer
with the larger window first, then restart the projector, native query, and
compatibility with the same value. The writer records the raise in place and
holds the settled tip until the visible tip is a full new window above it, so
replaceable depth grows with new blocks rather than all at once. The projector
raises the wallet's undo depth when it resumes. A reader still on the old
window refuses to open or to catch up until it restarts with the new one.
Earlier releases cannot open a raised store. If you might need to roll back
to an earlier Zinder release, capture a state bundle or keep a certified
side-by-side lane before raising. Lowering the window
in place is refused; rebuild, or restore a state captured under the smaller
window. See [ADR-0041](../adrs/0041-reorg-window-follows-the-node-bound.md).

When a replacement exceeds the configured window, ingest drains readiness and
parks for operator inspection until termination. Restarting does not make the
settled store replaceable. Confirm the authoritative branch and the node's
health, then rebuild or restore a coherent state bundle before returning the
service to traffic.

## Memory and storage pressure

Watch cgroup memory, RocksDB block cache, memtables, WAL bytes, pending
compaction, write-stop state, construction queue reservations, and disk free
space. Store size must not determine process RSS.

If construction approaches its memory boundary, stop the owner cleanly and
reduce the source-request, reserved-response, preparation, or RocksDB budgets
before restarting. Do not delete an admitted ready store merely to change a
runtime resource limit. See
[Bulk-catchup resource tuning](bulk-catchup-resource-tuning.md).

## Recovery and restart order

For ordinary process restarts, start owners before readers:

1. `zinder-ingest`
2. `zinder-projector`
3. `zinder-compat-lightwalletd`

Readers remain unready until their owner and secondary contracts recover. Do
not replace this with manual directory copies or primary-read shortcuts.

For disaster recovery, restore a coherent canonical and wallet state bundle
into fresh paths and require cold owner admission plus normal wallet-serving
admission. Independently timed RocksDB copies are not a wallet-serving backup.

## Production acceptance

A successful initial sync proves only the observed construction and admission
run. Production acceptance also needs current evidence for sustained following,
bounded reorg replacement, coherent restore, capacity headroom, restart
behavior, mempool recovery, TLS routing, and the exact independent client and
network being advertised.

See [Testing](testing.md), [Service operations](../architecture/service-operations.md),
and [ADR-0035](../adrs/0035-canonical-storage-topologies.md).
