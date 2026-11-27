# Deploying the single-volume container & Kubernetes

Use this runbook when your platform attaches one persistent volume to one container or pod, such as Railway or a single-replica Kubernetes `StatefulSet`. The unified distroless image `ghcr.io/zcashfoundation/zinder` (`zinder serve`) supervises the role processes for the selected `--preset` (`light-wallet`, `wallet` default, or `explorer`), binds its operational HTTP endpoint at **T=0** on `:9108`, derives storage paths beneath `/var/lib/zinder`, auto-generates private loopback control tokens, and persists `projector.build_owner_hex` automatically when unset. Read [Deploying Zinder](deploying.md) first for host sizing and preset tradeoffs.

> [!NOTE]
> **Key tradeoffs vs. the two-container VM Compose topology:**
> - **Shared cgroup memory:** Supervised child processes share one container/pod memory ceiling (minimum 8 GB for `wallet`; size 16+ GB if enabling `explorer` views or serving concurrent range clients).
> - **All-in-one restarts:** Every redeploy, kernel OOM kill, or fail-fast role exit restarts the container cleanly (which discards an unfinished initial build if interrupted before publication).
> - **Optional signed history catalog:** Unlike legacy single-volume scripts, `zinder serve` supports `history-publisher` natively when `ZINDER_HISTORY_PUBLISHER__DIRECTORY`, `ZINDER_HISTORY_PUBLISHER__SIGNING_KEY`, and `ZINDER_COMPAT__SHIELDED_HISTORY__*` are configured.

## Get the image

Public multi-arch (`amd64`, `arm64`) release images are published without credentials required. Pin an exact tag or digest:

```text
ghcr.io/zcashfoundation/zinder:vX.Y.Z
```

To build an unreleased commit:

```bash
docker build -f deploy/Dockerfile --target zinder \
  -t "<registry>/zinder:$(git rev-parse --short HEAD)" .
```

This image runs as non-root UID/GID `10001:10001` on `gcr.io/distroless/cc-debian12:nonroot` with no shell or `curl`. Its `ENTRYPOINT` is `/usr/local/bin/zinder` (default `CMD ["serve"]`), and its image `HEALTHCHECK` invokes `/usr/local/bin/zinder healthcheck`. Give mounted volumes ownership `10001:10001` (or Kubernetes `fsGroup: 10001`).

## Configure the container

Attach one persistent volume at `/var/lib/zinder`, sized per [Size your host](deploying.md#what-you-need), with at least 8 GB of RAM.

### Required variables

| Variable | Value |
| --- | --- |
| `ZINDER_NETWORK__NAME` | `zcash-mainnet` or `zcash-testnet` |
| `ZINDER_NODE__JSON_RPC_ADDR` | Node JSON-RPC URL, e.g. `http://zebra.railway.internal:18232` |
| `ZINDER_NODE__AUTH__METHOD` | `cookie` (or `basic` / `none`) |
| `ZINDER_NODE__AUTH__COOKIE` | Inline `user:password` cookie string (or `ZINDER_NODE__AUTH__PATH` for mounted files) |

Set `ZINDER_OPS__LISTEN_ADDR=[::]:9108`, `ZINDER_QUERY__LISTEN_ADDR=[::]:9102`, `ZINDER_COMPAT__LISTEN_ADDR=[::]:9067`, and `ZINDER_SECURITY__ALLOW_PUBLIC_BIND=true` (plus `ZINDER_EXPLORER__LISTEN_ADDR=[::]:9068` and `ZINDER_CIPHERSCAN__LISTEN_ADDR=[::]:9070` for `--preset explorer`) when platform health probes or private networking reach listeners off container loopback.

### Optional variables

| Variable | Effect |
| --- | --- |
| `ZINDER_DEPLOYMENT__PROFILE` | Preset (`light-wallet`, `wallet` default, or `explorer`; also selectable via `--preset`). |
| `ZINDER_STORAGE__FULL_BLOCKS` | Set `true` (or pass `--full-blocks`) to retain all raw blocks and transactions locally (`raw_blob_policy = "all"`). |
| `ZINDER_STORAGE__DATA_DIR` | Root persistent storage directory (defaults to `/var/lib/zinder`), from which canonical, wallet, secondary, checkpoint, and explorer paths derive. |
| `ZINDER_PROJECTOR__BUILD_OWNER_HEX` | Optional fixed 32-character hex projector owner; auto-generated and persisted at `<wallet_path>.build-owner` on first run when unset. |
| `ZINDER_NODE__INDEXER_GRPC_ADDR` | Optional node indexer gRPC URL (`http://...` or `https://...` for mTLS); falls back to JSON-RPC mempool polling when unset. |
| `ZINDER_INGEST__SOURCE` | `zebra-json-rpc` (default) or `zebra-indexer-grpc`. |
| `RUST_LOG` | Log filter (default `info`; unparseable values stop startup). |

On hosts with fewer than 8 logical CPUs, also check [First sync on fewer than 8 logical CPUs](bulk-catchup-resource-tuning.md#first-sync-on-fewer-than-8-logical-cpus). To carry forward an existing pre-profile `raw_blob_policy = "transactions"` store without rebuilding, leave `ZINDER_DEPLOYMENT__PROFILE` / `--preset` unset and set `ZINDER_STORAGE__RAW_BLOB_POLICY=transactions`.

### Indexer TLS

For mTLS (such as Zakura off loopback), set `ZINDER_NODE__INDEXER_GRPC_ADDR` to `https://...` and either mount PEM files (`*_PATH`) or pass multi-line inline PEM env vars (`*_PEM`) natively supported by Zinder:

| Item | Path variable | Inline PEM variable |
| --- | --- | --- |
| CA verifying node (omit for system roots) | `ZINDER_NODE__INDEXER_TLS__CA_CERT_PATH` | `ZINDER_NODE__INDEXER_TLS__CA_CERT_PEM` |
| Client certificate chain | `ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PATH` | `ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PEM` |
| Client private key | `ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH` | `ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PEM` |

Set `ZINDER_NODE__INDEXER_TLS__SERVER_NAME` if the node certificate names another host. Rotate inline PEM secrets by updating variables and redeploying.

### Fallback node endpoints

Configure fallbacks starting at position `1` ([node endpoint failover](../architecture/chain-ingestion.md#node-endpoint-failover)):

| Variable | Value |
| --- | --- |
| `ZINDER_NODE_FALLBACKS__1__JSON_RPC_ADDR` | `http://zakura.railway.internal:18232` |
| `ZINDER_NODE_FALLBACKS__1__AUTH__METHOD` | `cookie` |
| `ZINDER_NODE_FALLBACKS__1__AUTH__COOKIE` | Fallback node cookie |
| `ZINDER_NODE_FALLBACKS__1__INDEXER_GRPC_ADDR` | Fallback indexer gRPC URL |
| `ZINDER_NODE_FALLBACKS__1__BROADCAST_TIMEOUT_SECS` | `7` (not inherited from `[node]`) |

Indexer TLS variables follow the same prefix (for example, `ZINDER_NODE_FALLBACKS__1__INDEXER_TLS__CLIENT_KEY_PEM`).

## Ports

| Port | Serves | Reachable from |
| --- | --- | --- |
| `9067` | lightwalletd `CompactTxStreamer` gRPC (plaintext) | Platform/cluster private network |
| `9102` | Native `WalletQuery` gRPC (plaintext) | Platform/cluster private network |
| `9068` | `ExplorerQuery` gRPC (plaintext, `explorer` preset) | Platform/cluster private network |
| `9070` | Cipherscan REST adapter (`explorer` preset) | Platform/cluster private network |
| `9108` | Supervisor T=0 `/healthz`, `/readyz`, and aggregated `/metrics` | Private network + platform/cluster probes |
| `9100`, `9101` | Ingest and projector control | Container loopback only |

Never expose `9067`, `9102`, `9068`, `9070`, or `9108` directly to the public internet without TLS, limits, and auth in front ([Before you expose it](deploying.md#before-you-expose-it)).

## Deploy on Railway

1. Create a service from `ghcr.io/zcashfoundation/zinder:vX.Y.Z` (or a custom Dockerfile target `--target zinder`).
2. Attach a volume at `/var/lib/zinder` (ensure UID/GID `10001` ownership if Railway mounts as root, e.g. via volume permissions/init or `RAILWAY_RUN_UID=0` disabled where platform supports non-root volumes; check platform mount permissions).
3. Set `PORT=9108` and `RAILWAY_DEPLOYMENT_DRAINING_SECONDS=30` (`zinder serve` waits up to 20 seconds for graceful child drain on `SIGTERM`).
4. Set restart policy to **Always**.
5. Set health-check path to `/healthz`: `zinder serve` binds `:9108/healthz` at **T=0** (HTTP 200 immediately while stores build), so container startup succeeds even during multi-hour Mainnet syncs. Monitor `/readyz` on `:9108` separately before routing client traffic.

Clients reach `<service>.railway.internal:9067` and `:9102` over Railway private IPv6. Scrape aggregated Prometheus metrics from `127.0.0.1:9108/metrics` (or `<service>.railway.internal:9108/metrics`); see [Measure a wallet-serving deployment](serving-census.md).

<a id="kubernetes-statefulset"></a>

## Deploy on Kubernetes (`StatefulSet`)

[deploy/kubernetes/statefulset.yaml](../../deploy/kubernetes/statefulset.yaml) packages the single-volume `zinder serve` pattern as a single-replica Kubernetes `StatefulSet` (`replicas: 1`) alongside headless and cluster `Service` manifests:

- **Storage & permissions:** `volumeClaimTemplates` requests a `ReadWriteOnce` `PersistentVolumeClaim` mounted at `/var/lib/zinder`, with pod `securityContext.fsGroup: 10001`, `runAsNonRoot: true`, `runAsUser: 10001`, and `runAsGroup: 10001` so the distroless non-root container owns its RocksDB stores cleanly.
- **T=0 probes:** `startupProbe` and `livenessProbe` hit `/healthz` on `:9108` (`failureThreshold: 30` allows initial startup without premature restarts), while `readinessProbe` hits `/readyz` on `:9108` so Kubernetes `Service` endpoints admit wallet traffic only after `ingest`, `projector`, and serving listeners reach `ready`.
- **Single-replica constraint:** Keep `replicas: 1`. Because RocksDB secondaries share one local filesystem with their writers, you cannot scale `StatefulSet` replicas or convert to a stateless `Deployment` until M3 PostgreSQL ([Cloud and serverless boundaries](deploying.md#cloud-and-serverless-boundaries)).

## What a fresh sync looks like

While building on empty `/var/lib/zinder`, `/healthz` answers HTTP 200 and `/readyz` reports `{"status":"not_ready","phase":"bootstrapping_canonical",...}` -> `"catching_up_wallet_projection"` -> `"waiting_for_explorer_views"` (under `explorer`) -> `"waiting_for_serving_listeners"` -> `"ready"`. Poll `:9108/readyz` (or exec `zinder healthcheck --ready`) to inspect per-role progress. See [Initial sync](initial-sync.md) for construction mechanics.

## Private backups

See [Backups and Edge Snapshots](private-backups.md) for coherent `zinder backup` capture, verification, and replacement on a fresh volume.

## Failure handling

`zinder serve` binds `/healthz` at T=0, monitors child processes and `/readyz`, drains children for up to 20 seconds on `SIGTERM`, and **fails fast** (draining siblings and exiting non-zero) if any supervised role exits unexpectedly so Railway or Kubernetes restarts cleanly.

| Symptom | Cause | Container behavior | Action |
| --- | --- | --- | --- |
| Process exits | Crash, invalid config, or `schema_mismatch` / `node_capability_missing` | Fail-fast drains siblings and exits non-zero | Inspect failing role log (`child role ... exited`). |
| Ingest cannot reach node | Node outage | Readers keep serving stored data while ingest retries; `/readyz` reports `not_ready` on ingest / `degraded` on readers | Restore node (a restart cannot open ingest until a node answers). |
| Ingest reports `upstream_not_ready` | Node syncing | Keeps serving stored data indefinitely | Wait for node. |
| Readers refuse with `REORG_WINDOW_EXCEEDED` | Reorg deeper than `ZINDER_INGEST__REORG_WINDOW_BLOCKS` (default 1000) | Stays up serving typed refusal | Verify node chain, then rebuild on empty paths. |
| Ingest/readers report `canonical_store_damaged` | Checkpoint row failed verification (`tree_state_verification_mismatch`) | Stays up; readers refuse with `SERVICE_NOT_READY` | Rebuild canonical store. |
| Projector reports `wallet_transition_limit_exceeded` | Oversized atomic wallet transition | Stays up, parked | See [below](#wallet-transition-exceeds-its-logical-byte-limit). |

### Wallet transition exceeds its logical-byte limit

When a deep reorg or busy-address transition exceeds `projector.follow.max_transition_logical_bytes` (default `536870912`), the projector logs `wallet_projection_following_parked`, leaves the wallet unchanged, and reports `wallet_transition_limit_exceeded` (`required_bytes`, `configured_bytes`) on `/readyz`. Query and compat keep serving briefly, then return `SERVICE_NOT_READY`.

Recover by setting `ZINDER_PROJECTOR__FOLLOW__MAX_TRANSITION_LOGICAL_BYTES` well above `required_bytes` (at most `1073741824`) and redeploying, adding `1.5x` the added byte delta to container/pod RAM. If `required_bytes` exceeds `1073741824`, rebuild the wallet on a fresh volume and cut over.

## Serve the Explorer from the same volume

Pass `--preset explorer` (or set `ZINDER_DEPLOYMENT__PROFILE=explorer`) on `zinder serve` to add `ingest.explorer_views = true`, `zinder-explorer` (`ExplorerQuery` on `:9068`), and `zinder-compat-cipherscan` (`:9070`). Add RAM and disk beyond the 8 GB `wallet` minimum. To rebuild materialized views after a schema change, move `/var/lib/zinder/canonical/materialized-views` out of the canonical directory on the same volume and restart (`zinder-index` / `zinder serve`).

## Standalone canonical writer (`zinder index`)

To run the canonical (and optional projector) writers separately from `zinder serve --readers-only` on shared local storage, use `zinder index --preset <light-wallet|wallet|explorer>` (default ops listener `:9105`). `zinder index` shares the same distroless image and `ZINDER_*` / `*_PEM` environment schema.
