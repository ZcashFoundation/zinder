# Initial sync

This runbook is for an operator starting Zinder on empty storage. It explains
what each runtime does during the first sync, what `/readyz` reports while it
works, and what to do when the sync stops.

A first sync builds two stores from the node. `zinder-ingest` reads every block
from genesis into the canonical store. `zinder-projector` then derives the
wallet store from it. The readers, `zinder-query` and
`zinder-compat-lightwalletd`, serve traffic only after both stores exist.

On Mainnet the first sync takes many hours.
[Deploying Zinder](deploying.md#what-you-need) gives the measured times and
disk.

## Before you start

- The node is an archive node on the right network and has finished its own
  sync. Ingest refuses a pruned node.
- The canonical and wallet storage paths are empty or absent. Zinder does not
  migrate a store built with an incompatible identity or schema in place.
- `ZINDER_PROJECTOR_BUILD_OWNER_HEX` (`projector.build_owner_hex`) has a
  value that stays the same across restarts.

## Start and watch the sync

1. Start the stack with the start command from your deployment runbook, such
   as step 4 of the
   [Docker Compose quickstart](deploying.md#quickstart-with-docker-compose) or
   of [Deploying on a VM](deploying-on-a-vm.md).

   Compose starts ingest first, the projector once ingest is healthy, and the
   two readers once the projector is healthy. The command can wait for hours
   while it does this.

2. Poll each runtime's `/readyz` and make the two test calls, as in steps 5
   and 6 of the
   [Docker Compose quickstart](deploying.md#quickstart-with-docker-compose).
   [Readiness](../reference/readiness.md#readiness-causes) explains each
   cause that `/readyz` reports.

The startup log line with `phase=ready` means a process finished starting. It
does not mean the process is ready for traffic.

## Phases

| Phase | Runtime | What it does | `/readyz` while it runs | How to see progress |
| --- | --- | --- | --- | --- |
| Node checks | ingest | Checks the node's network, genesis block, archive storage, and network-upgrade table | `starting` | Startup log lines with `phase=connect_node` and `phase=check_schema` |
| Canonical construction | ingest | Fixes a target tip, loads every block up to it into `<canonical>.building`, then publishes and moves it to the canonical path | `syncing`, `phase` `bulk_catchup`, `target_height` set to the fixed tip | `zinder_ingest_source_segment_connected_blocks_total` rises; `<canonical>.building` grows. Logs `canonical_fresh_construction_started`, then `canonical_fresh_construction_ready` |
| Canonical catch-up | ingest | Follows the blocks the node produced during construction and loads the node's mempool | `syncing` until the lag is at most `ingest.follow.lag_threshold_blocks` (default 1) and the first mempool snapshot is complete | `zinder_ingest_canonical_tip_height` rises and `zinder_ingest_canonical_lag_blocks` falls |
| Wallet build start | projector | Opens its canonical secondary, waits until it matches the writer exactly, and takes its build leases | `syncing`; `replica_lagging` or `writer_status_unavailable` after each `projector.fence_convergence_timeout_seconds` attempt | It retries and does not exit |
| Wallet construction | projector | Builds the wallet store from the canonical store, spilling sorted data to `<wallet>.projection-load-staging` | `syncing`, `target_height` set to the canonical tip | No progress metric. The staging directory grows. Logs `wallet_projection_construction_complete` with `build_seconds` when done |
| Wallet catch-up | projector | Applies canonical blocks committed during the build, then follows | `syncing` until the wallet matches the canonical tip | `current_height` approaches `target_height` |
| Serving admission | query, compat | Opens matching canonical and wallet secondaries and checks that they describe the same chain | `starting`, `replica_lagging`, `writer_status_unavailable`, or `canonical_tip_unconfirmed` | Compose allows each reader five minutes to become `ready` |

`<canonical>` is ingest's `storage.path`, which the projector reads as
`storage.canonical_path`. `<wallet>` is the projector's `wallet.path`. The
reference configs under `deploy/config/` set them to
`/var/lib/zinder/canonical` and `/var/lib/zinder/wallet`. Outside those
configs, set `storage.path` explicitly. The ingest binary's own default is
`/var/lib/zinder/store`, which differs from the projector's default
`storage.canonical_path`.

## When each runtime becomes ready

[Readiness](../reference/readiness.md#per-runtime-readiness) lists when each
runtime turns ready. Send traffic only after all four `/readyz` endpoints
return 200 and the two test calls from step 2 succeed. To serve wallets on the
internet, see [Exposing over TLS](exposing-over-tls.md).

## Restarts during the first sync

A restart during construction does not resume it. What survives depends on
what was published before the process stopped:

| Stopped while | On restart |
| --- | --- |
| Ingest was loading blocks | Ingest deletes `<canonical>.building`, logs `canonical_unpublished_construction_restarted`, and builds again from genesis to a new target tip |
| Ingest had published but not yet moved the store into place | The next start installs the published store and does not rebuild it. A clean stop during publication waits for publication to finish |
| The projector was building | The published canonical store is kept. The projector discards the unpublished wallet store, logs `wallet_projection_unpublished_build_discarded`, and builds the wallet store again from the beginning |
| Any runtime, after its store was published | The runtime reopens its store and continues following |

A clean stop and a crash behave the same, so an out-of-memory kill restarts
the interrupted build from the beginning. Fix the cause first, or the next
attempt fails at the same point.

A stop request ends construction at the next block. Two final stages cannot be
interrupted: finalizing the store files and publication. On a long chain,
finalization can outlast the supervisor's grace period, and the kill that
follows counts as a crash.

The first sync does not fail over to `[node_fallbacks.N]` endpoints. Ingest
retries a failed node request a bounded number of times. If the node stays
unavailable longer, ingest exits and canonical construction starts over. The
restart begins on the first endpoint that passes its checks.

Do not move, edit, or delete staging directories by hand. Each runtime
removes its own unpublished staging on the next start.

## Problems

| Symptom | Cause | Action |
| --- | --- | --- |
| Ingest exits at startup. `/readyz` showed `node_capability_missing` for `historical_block_bodies` | The node is pruned or does not report its storage mode | Point ingest at an archive node |
| Ingest or the projector restarts repeatedly with exit code 137 | The container ran out of memory | Lower the limiting budget as described in [Bulk-catchup resource tuning](bulk-catchup-resource-tuning.md), or raise the container limit |
| On Mainnet, `docker compose up` reports `zinder-ingest` unhealthy after three hours | Canonical construction outlasted the Compose health-check start period. Compose does not restart the container for this | Wait until ingest `/readyz` returns 200, then run the same start command again to start the other runtimes |
| The single-volume container exits during construction on Mainnet | Construction outlasted the image's construction deadline | Raise the deadlines as described in [Deploying on a single volume](deploying-single-volume.md) |
| Construction stops on Mainnet with failure class `capability_missing` on a tree-state or subtree read | The node is a Zakura node built by fast sync, which never stored per-height trees below its last checkpoint | Use a node synchronized without fast sync |
| Projector exits with `wallet construction needs ... temporary bytes in one sorter` or `stopped a sorter whose input had not ended` | `projector.build.max_temporary_file_bytes_per_sorter` is below what a sorter needs. The figure in the second message is a lower bound | Unset the key to use the default (206158430208, 192 GiB), or raise it well above the reported figure. Make sure that much space is free |
| Projector fails with a disk-full error late in wallet construction | The volume has too little free space for the sorters' spill files | Free or add space. On Mainnet keep about 130 GB free when wallet construction starts |
| Projector exits with `wallet projection build lease is held by owner <owner> until <expiry>` | `projector.build_owner_hex` changed since the build started | Set it back to the reported owner and restart, or restart after the reported expiry |
| Projector exits with `wallet projection build at <path> is in progress in another running process` | A second projector uses the same wallet path | Stop the second projector. Do not delete the `<wallet>.build-lock` file |
| Projector stays at `replica_lagging` or `writer_status_unavailable` | Its canonical secondary cannot catch up with ingest, or ingest's control endpoint is unreachable | Check that ingest is running and that both use the same control token |
| Query and compatibility stay at `canonical_tip_unconfirmed` | Ingest has not reached the node since it started | Restore the node. Restarting the readers does not help |

Readiness causes that need an operator, such as `schema_mismatch` and
`reorg_window_exceeded`, are described in
[Readiness](../reference/readiness.md#readiness-causes). Ordinary restarts
and recovery after the first sync are covered in
[Service operations](../architecture/service-operations.md#recovery).
