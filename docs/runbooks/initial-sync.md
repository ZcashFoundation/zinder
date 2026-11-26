# Initial sync

This runbook covers starting Zinder on empty storage: what each runtime builds, how to track construction progress on `/readyz` and `/metrics`, and how to recover if a build stops.

On an empty store, `zinder-ingest` builds the canonical store from genesis (or an edge snapshot; see [Backups and Edge Snapshots](private-backups.md)), `zinder-projector` derives the wallet store from it, and `zinder-query` and `zinder-compat-lightwalletd` start serving once both stores are published. Measured Mainnet sync times and disk budgets are in [Size your host](deploying.md#what-you-need).

## Before you start

- Your node is an archive Zebra 7.1.0 or non-fast-sync Zakura 1.8.6 on the target network and is fully synced.
- `storage.path` and `wallet.path` are empty or absent.
- `ZINDER_PROJECTOR_BUILD_OWNER_HEX` (`projector.build_owner_hex`) is set to a fixed 32-character hex value across restarts.
- Your `deployment.profile` / `storage.raw_blob_policy` matches your intended profile before starting (see the config warning in [Step 1: Choose a profile](deploying.md#choose-a-profile)).

## Start and watch the sync

1. Run the start command from [Deploying Zinder](deploying.md#quickstart-with-docker-compose) or [Deploying on a VM](deploying-on-a-vm.md). Compose starts `zinder-ingest`, then `zinder-projector` once ingest is healthy, then the readers once the projector is healthy.
2. Poll `/readyz` on each runtime and run the smoke calls from steps 5–6 of the [quickstart](deploying.md#quickstart-with-docker-compose). See [Readiness](../reference/readiness.md#readiness-causes) for every `/readyz` cause.

A startup log line with `phase=ready` means startup wiring finished, not that `/readyz` is `200`.

## Phases

| Phase | Runtime | What it does | `/readyz` while running | Progress signals |
| --- | --- | --- | --- | --- |
| Node checks | `zinder-ingest` | Verifies network, genesis block, archive capability, and upgrade table | `starting` | Logs `phase=connect_node`, `phase=check_schema` |
| Canonical construction | `zinder-ingest` | Fixes `target_height` one reorg window below the node tip ([why](../architecture/chain-ingestion.md#fresh-construction)) and loads blocks into `<canonical>.building` before publishing | `syncing`, `phase: "bulk_catchup"`, updating `current_height`, `target_height`, and shrinking `lag_blocks` | `zinder_ingest_construction_loaded_height` rises toward `zinder_ingest_construction_target_height`, `zinder_readiness_sync_lag_blocks` falls, `zinder_ingest_source_segment_connected_blocks_total` rises; logs `canonical_fresh_construction_started` / `canonical_fresh_construction_ready` |
| Canonical catch-up | `zinder-ingest` | Appends the remaining reorg window plus blocks mined during construction and loads mempool | `syncing` until lag ≤ `ingest.follow.lag_threshold_blocks` (1) and mempool hydrates | `zinder_ingest_canonical_tip_height` rises; `zinder_ingest_canonical_lag_blocks` falls |
| Wallet build start | `zinder-projector` | Opens canonical secondary, converges with writer fence, and takes build leases | `syncing` (`replica_lagging` or `writer_status_unavailable` after fence timeout) | Retries automatically without exiting |
| Wallet construction | `zinder-projector` | Builds wallet store from canonical store using cgroup-scaled sort memory, spilling to `<wallet>.projection-load-staging` | `syncing`, `target_height` set to canonical tip | Staging directory grows; logs `wallet_projection_construction_complete` with `build_seconds` |
| Wallet catch-up | `zinder-projector` | Applies canonical events committed during the build, then follows | `syncing` until caught up to canonical tip | `current_height` approaches `target_height` |
| Serving admission | `zinder-query`, `zinder-compat-lightwalletd` | Opens matching canonical/wallet secondaries and verifies chain alignment | `starting`, `replica_lagging`, `writer_status_unavailable`, or `canonical_tip_unconfirmed` | Readers return `ready` once admitted |

Under `wallet-serving` coverage with Explorer views off, ingest densifies tree-state checkpoints during construction (`every-block` by default, or `commitment-budget` when configured) so tree-state reads hit stored checkpoints immediately at `READY`. See [Bulk-catchup resource tuning](bulk-catchup-resource-tuning.md) for cgroup-scaled budgets and smaller-CPU options.

## When each runtime becomes ready

Route wallet traffic only after all four `/readyz` endpoints return `200` and smoke calls succeed ([Per-runtime readiness](../reference/readiness.md#per-runtime-readiness)). To expose traffic externally, follow [Exposing over TLS](exposing-over-tls.md).

<a id="restarts-during-the-first-sync"></a>

## Restarts during the first sync

Any restart (clean stop or crash/OOM) before publication discards unfinished staging and restarts that store's build from scratch:

| Stopped while | On restart |
| --- | --- |
| `zinder-ingest` is loading blocks | Deletes `<canonical>.building`, logs `canonical_unpublished_construction_restarted`, and rebuilds from genesis to a new target tip |
| `zinder-ingest` finished publication but hadn't moved the store into place | Installs the published store without rebuilding |
| `zinder-projector` is building | Keeps the published canonical store, discards `<wallet>` staging (`wallet_projection_unpublished_build_discarded`), and rebuilds the wallet store |
| Any runtime after its store is published | Reopens the published store and resumes following |

During construction, `zinder-ingest` stays on its starting node endpoint and does not fail over to `[node_fallbacks.N]`. Do not edit or delete staging directories manually while processes may be running.

## Problems

| Symptom | Cause | Action |
| --- | --- | --- |
| Ingest exits at startup; `/readyz` showed `node_capability_missing` for `historical_block_bodies` | Node is pruned or hides storage mode | Point ingest at an archive node |
| Ingest or projector restarts with exit code 137 | Out of memory (OOM) | Tune budgets or raise container ceiling in [Bulk-catchup resource tuning](bulk-catchup-resource-tuning.md) |
| Mainnet `docker compose up` reports `zinder-ingest` unhealthy after 3 h | Construction outlasted Compose's `start_period` (container is still building) | Leave ingest running; re-run `docker compose ... up -d --no-build` once ingest `/readyz` returns `200` |
| Single-volume container exits during Mainnet build | Construction outlasted default deadline | Raise deadlines per [Deploying the single-volume container](deploying-single-volume.md) |
| Construction stops with `capability_missing` on tree-state or subtree read | Zakura node built with fast sync | Use a non-fast-sync node |
| Projector exits with `wallet construction needs ... temporary bytes in one sorter` or `stopped a sorter whose input had not ended` | `projector.build.max_temporary_file_bytes_per_sorter` below required spill size | Unset to use default (192 GiB) or raise above reported figure and ensure free disk space |
| Projector fails with disk-full late in construction | Too little free space for sort spill files | Keep ~130 GB free on the volume when Mainnet wallet construction starts |
| Projector exits with `wallet projection build lease is held by owner ...` | `projector.build_owner_hex` changed mid-build | Restore original value or wait for lease expiry |
| Projector exits with `wallet projection build at ... is in progress` | Second projector using same wallet path | Stop the duplicate projector; do not delete `<wallet>.build-lock` |
| Projector stays at `replica_lagging` or `writer_status_unavailable` | Secondary trailing or ingest control unreachable | Check ingest health and matching `ingest.token` |
| Readers stay at `canonical_tip_unconfirmed` | Ingest restarted and has not confirmed tip with node | Restore node connectivity; do not restart readers |
