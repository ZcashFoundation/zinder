# Deploying the single-volume container

Use this runbook when your platform attaches one persistent volume to one container, such as Railway. The `zinder-wallet-serving` image supervises `zinder-ingest`, `zinder-projector`, `zinder-query` (port `9102`), and `zinder-compat-lightwalletd` (port `9067`) together in one container. Read [Deploying Zinder](deploying.md) first for host sizing and profile tradeoffs.

> [!NOTE]
> **Key tradeoffs vs. the VM Compose topology:**
> - **Shared cgroup memory:** All four processes share one container memory ceiling (minimum 8 GB; count `ZINDER_WALLET__ROCKSDB__BLOCK_CACHE_BYTES` 3x across projector, query, and compatibility).
> - **All-in-one restarts:** Every redeploy or kernel OOM kill restarts all processes together (and restarts an unfinished first sync from scratch).
> - **No signed history catalog:** `zinder-wallet-serving` and `zinder-wallet-serving-explorer` do **not** bundle `publish-shielded-history` or a signed `ZSHRNG01` catalog today; `GetBlockRange` serves from canonical storage and the live-tail buffer.
> - **Mainnet deadline & blob defaults:** Default construction deadlines (`10800`s / `7200`s) are sized for Testnet and will kill a slow Mainnet build unless raised. The shipped image config (`deploy/config/wallet-serving/ingest.toml`) sets `raw_blob_policy = "transactions"` (+251 GiB on Mainnet) and `coverage = "wallet-serving"`, and the entrypoint currently strips `ZINDER_DEPLOYMENT__*`, so this image retains transaction blobs unless custom TOML configs are mounted (`ZINDER_STORAGE__RAW_BLOB_POLICY=none` without a profile fails startup).

## Get the image

Public multi-arch (`amd64`, `arm64`) release images are published without credentials required. Pin an exact tag or digest:

```text
ghcr.io/zcashfoundation/zinder-wallet-serving:vX.Y.Z
```

To build an unreleased commit:

```bash
docker build -f deploy/Dockerfile --target zinder-wallet-serving \
  -t <registry>/zinder-wallet-serving:<commit> .
```

This image uses a digest-pinned Debian slim base with `sh` and `curl`, hands `/var/lib/zinder` to UID/GID `10001` at startup, and drops privileges to `10001`.

## Configure the container

Attach one persistent volume at `/var/lib/zinder`, sized per [Size your host](deploying.md#what-you-need), with at least 8 GB of RAM. Lightwalletd compatibility starts after both builds finish; record peak memory under your client range-stream load and lower `ZINDER_COMPAT__MAX_CONCURRENT_BLOCK_RANGE_STREAMS` or raise memory if headroom is tight (or set `ZINDER_WALLET_SERVING__LIGHTWALLETD=false`).

### Required variables

| Variable | Value |
| --- | --- |
| `ZINDER_NETWORK__NAME` | `zcash-mainnet` or `zcash-testnet` |
| `ZINDER_NODE__JSON_RPC_ADDR` | Node JSON-RPC URL, e.g. `http://zebra.railway.internal:18232` |
| `ZINDER_NODE__AUTH__METHOD` | `cookie` |
| `ZINDER_NODE__AUTH__COOKIE` | Inline `user:password` cookie string |
| `ZINDER_NODE__INDEXER_GRPC_ADDR` | Node indexer gRPC URL (e.g. `http://zebra.railway.internal:8155`, or `https://...` for TLS). **Required** — the container exits at startup without it. |
| `ZINDER_PROJECTOR__BUILD_OWNER_HEX` | Stable 32-character hexadecimal projector identity |

### Optional variables

| Variable | Effect |
| --- | --- |
| `ZINDER_WALLET_SERVING__CANONICAL_CONSTRUCTION_DEADLINE_SECONDS` | Chain-store build deadline in seconds. Default `10800` (3 h). Set at least `86400` (24 h) for Mainnet. |
| `ZINDER_WALLET_SERVING__WALLET_CONSTRUCTION_DEADLINE_SECONDS` | Wallet-store build deadline in seconds. Default `7200` (2 h). Set at least `43200` (12 h) for Mainnet. |
| `ZINDER_WALLET_SERVING__LIGHTWALLETD` | `false` disables compatibility and closes port `9067`. Default `true`. |
| `INGEST_CONTROL_BEARER_TOKEN`, `CHECKPOINT_BEARER_TOKEN` | Internal control tokens (auto-generated at startup when unset). |
| `RUST_LOG` | Log filter (default `info`; unparseable values stop the container). |

On hosts with fewer than 8 logical CPUs, also check [First sync on fewer than 8 logical CPUs](bulk-catchup-resource-tuning.md#first-sync-on-fewer-than-8-logical-cpus).

Other `ZINDER_*` variables from [Configuration](../reference/configuration.md#environment-variables) are routed to matching processes (check the startup log line listing unread `ZINDER_*` names; misspelled `ZINDER_WALLET_SERVING__*` names stop startup). Compatibility runs in `wallet` mode here, so `ZINDER_COMPAT__SERVING=compact-blocks` stops the container. Do not override storage paths, listen addresses, control endpoints, or token paths; unless compatibility is disabled, setting `ZINDER_STORAGE__SECONDARY_PATH` or `ZINDER_WALLET__SECONDARY_PATH` stops startup because both readers would share a secondary path.

### Indexer TLS

For mTLS (such as Zakura off loopback), set `ZINDER_NODE__INDEXER_GRPC_ADDR` to `https://...` and either mount PEM files or pass multi-line PEM env vars (such as on Railway):

| Item | Path variable | Text variable |
| --- | --- | --- |
| CA verifying node (omit for system roots) | `ZINDER_NODE__INDEXER_TLS__CA_CERT_PATH` | `ZINDER_NODE__INDEXER_TLS__CA_CERT_PEM` |
| Client certificate chain | `ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PATH` | `ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PEM` |
| Client private key | `ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH` | `ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PEM` |

Set `ZINDER_NODE__INDEXER_TLS__SERVER_NAME` if the node certificate names another host. The entrypoint writes PEM text to `0400` files owned by `zinder` (logged at startup; `/dev/shm` stays in memory, `/run` may hit disk) and rejects missing headers, single-line values, or setting both path and text. Rotate by updating variables and redeploying.

### Fallback node endpoints

Configure fallbacks starting at position `1` ([node endpoint failover](../architecture/chain-ingestion.md#node-endpoint-failover)):

| Variable | Value |
| --- | --- |
| `ZINDER_NODE_FALLBACKS__1__JSON_RPC_ADDR` | `http://zakura.railway.internal:18232` |
| `ZINDER_NODE_FALLBACKS__1__AUTH__METHOD` | `cookie` |
| `ZINDER_NODE_FALLBACKS__1__AUTH__COOKIE` | Fallback node cookie |
| `ZINDER_NODE_FALLBACKS__1__INDEXER_GRPC_ADDR` | Fallback indexer gRPC URL (required) |
| `ZINDER_NODE_FALLBACKS__1__BROADCAST_TIMEOUT_SECS` | `7` (not inherited from `[node]`) |

Indexer TLS variables follow the same prefix (for example, `ZINDER_NODE_FALLBACKS__1__INDEXER_TLS__CLIENT_KEY_PEM`).

## Ports

| Port | Serves | Reachable from |
| --- | --- | --- |
| `9067` | lightwalletd `CompactTxStreamer` gRPC (plaintext) | Platform private network |
| `9102` | Native `WalletQuery` gRPC (plaintext) | Platform private network |
| `9106` | Query ops (`/healthz`, `/readyz`, `/metrics`) | Private network + platform health check |
| `9107` | Compatibility ops (`/healthz`, `/readyz`, `/metrics`) | Private network |
| `9100`, `9101` | Ingest and projector control | Container loopback only |
| `9105`, `9110` | Ingest and projector ops | Container loopback only |

Never attach a public domain directly to `9067` or `9102`; put TLS, limits, and auth in a proxy ([Before you expose it](deploying.md#before-you-expose-it)).

## Deploy on Railway

1. Create a service from `ghcr.io/zcashfoundation/zinder-wallet-serving:vX.Y.Z` (Railway builds only the last Dockerfile stage, so deploy from the published image).
2. Attach a volume at `/var/lib/zinder` and set the configuration variables above.
3. Set `PORT=9106` and `RAILWAY_DEPLOYMENT_DRAINING_SECONDS=30` (the entrypoint waits up to 20 seconds for graceful store flush on shutdown).
4. Set restart policy to **Always**.
5. On a first deploy to an empty volume, leave the health-check path unset (port `9106` stays closed until both builds finish, which can exceed Railway's 3600s max timeout).
6. Once the log prints `wallet serving is ready`, set health-check path to `/healthz`, timeout to `3600`, and deploy again.

Clients reach `<service>.railway.internal:9067` and `:9102` over Railway private IPv6. Because Railway's health check probes `9106` only, also monitor `/readyz` on `9106` and `9107` plus `zinder_readiness_served_epoch_age_seconds` ([Readiness](../reference/readiness.md#during-a-node-outage)). See [Measure a wallet-serving deployment](serving-census.md) for private Prometheus scraping.

## What a fresh sync looks like

Expect hours with serving ports closed on an empty volume. Key log lines:

| Log line | Phase |
| --- | --- |
| `starting canonical construction` | `zinder-ingest` building the chain store (prints deadline). |
| `starting wallet construction` | `zinder-projector` building the wallet store. |
| `starting wallet serving with zinder-query and zinder-compat-lightwalletd` | Readers opening stores (shared fixed 30-minute deadline; `zinder-query` only if compat disabled). |
| `wallet serving is ready` | Serving listeners open. |

While waiting, the active process's `/readyz` body is logged every 5 minutes. Inside `railway ssh`, inspect processes directly:

```bash
curl --fail-with-body -sS http://127.0.0.1:9105/readyz   # ingest
curl --fail-with-body -sS http://127.0.0.1:9110/readyz   # projector
curl --fail-with-body -sS http://127.0.0.1:9106/readyz   # query
curl --fail-with-body -sS http://127.0.0.1:9107/readyz   # lightwalletd compatibility
```

See [Initial sync](initial-sync.md) for construction mechanics.

## Private backups

See [Backups and Edge Snapshots](private-backups.md) for coherent capture, verification, and replacement on a fresh volume.

## Failure handling

The entrypoint polls `/readyz` on each process and `grpc.health.v1.Health/Check` on enabled readers. When a failure budget expires, the entrypoint exits non-zero so the platform restarts a clean set.

| Symptom | Cause | Container behavior | Action |
| --- | --- | --- | --- |
| Build outlives deadline | Build slower than deadline | Logs last `/readyz` and exits | Raise deadline variable (build restarts from scratch). |
| Process exits | Crash or invalid config | Exits | Inspect process error in log. |
| `/readyz` reports `schema_mismatch` or `node_capability_missing` | Incompatible store or pruned/incapable node | Exits immediately | Use empty paths for a new schema, or an archive node. |
| Process stays unready 1 hour | Lag or persistent fault | Exits | Read logged `/readyz` body. |
| Process flaps ready/unready | Repeated short faults | Exits after 6 unready episodes or 900s total unready without 10 consecutive ready minutes | Inspect logged episodes. |
| Ingest cannot reach node | Node outage | Keeps serving stored data; exits after 24 h active outage | Restore node (a restart cannot serve until a node answers). |
| Ingest reports `upstream_not_ready` | Node syncing | Keeps serving stored data indefinitely | Wait for node. |
| Readers refuse with `REORG_WINDOW_EXCEEDED` | Reorg deeper than `ZINDER_INGEST__REORG_WINDOW_BLOCKS` (default 1000) | Stays up serving typed refusal | Verify node chain, then rebuild on empty paths. |
| Ingest/readers report `canonical_store_damaged` | Checkpoint row failed verification (`tree_state_verification_mismatch`) | Stays up; readers refuse with `SERVICE_NOT_READY` | Rebuild canonical store (or wait if `tree_state_verification_record_unreadable` clears after one cycle). |
| Projector reports `wallet_transition_limit_exceeded` | Oversized atomic wallet transition | Stays up, parked | See [below](#wallet-transition-exceeds-its-logical-byte-limit). |
| Query logs `wallet_serving_pair_table_file_missing` | Compaction removed a file under an open read view | Retries and replaces view | Ignore occasional warnings; check bulk compactions if frequent. |

When ingest reports `syncing`, check `ingest_control` in query's `/readyz`: `upstream_not_ready` means the node is syncing; `node_unavailable` with `wait_reason = mempool_hydration_at_tip` means the mempool feed is down (counts toward the 24-hour clock, pausing during `canonical_progress`).

The image `HEALTHCHECK` (`zinder-wallet-serving-healthcheck`) passes when enabled readers return `200` on `/readyz`, serve `reorg_window_exceeded` / `canonical_store_damaged` refusals, or while the projector is parked on its transition limit (start period is 6 hours; lengthen for Mainnet builds). On `SIGTERM`, the entrypoint stops compat, query, projector, and ingest, ending open gRPC calls after 10s (+2s status grace) and waiting 20s before exiting non-zero.

### Wallet transition exceeds its logical-byte limit

When a deep reorg or busy-address transition exceeds `projector.follow.max_transition_logical_bytes` (default `536870912`), the projector logs `wallet_projection_following_parked`, leaves the wallet unchanged, and reports `wallet_transition_limit_exceeded` (`required_bytes`, `configured_bytes`) on `/readyz`. Query and compat keep serving briefly, then return `SERVICE_NOT_READY` (and on restart the entrypoint skips starting readers).

Recover by setting `ZINDER_PROJECTOR__FOLLOW__MAX_TRANSITION_LOGICAL_BYTES` well above `required_bytes` (at most `1073741824`) and redeploying, adding `1.5x` the added byte delta to container RAM. If `required_bytes` exceeds `1073741824`, rebuild the wallet in a fresh service/volume and cut over.

## Serve the Explorer from the same volume

`ghcr.io/zcashfoundation/zinder-wallet-serving-explorer:vX.Y.Z` adds `zinder-explorer` (`ExplorerQuery` on port `9068`, ops on `9069`) with `ingest.explorer_views = true`. Use the same configuration plus `ZINDER_EXPLORER__*` variables (`[node]` only, no fallbacks) and add RAM/disk beyond the 8 GB minimum. Explorer starts after wallet serving is healthy (with independent restart backoff if it fails). Set `ZINDER_INGEST__EXPLORER_VIEWS=false` to pause view growth while resizing a volume. To rebuild views after a schema change, move `/var/lib/zinder/canonical/materialized-views` to `/var/lib/zinder/materialized-views.previous` and restart ([Add the Explorer](deploying-on-a-vm.md#add-the-explorer)).

## Chain-store-only diagnostic image

`zinder-canonical-runtime` runs `zinder-ingest` alone for platform build checks (serves no clients; ops port `9105`, set `PORT=9105` on Railway). Accepts all `ZINDER_*` ingest and `*_PEM` TLS variables.
