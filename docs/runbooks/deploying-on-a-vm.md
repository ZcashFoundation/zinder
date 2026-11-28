# Deploying on a VM

This runbook sets up the zero-TOML single-host Zinder topology on a Linux VM with Docker Compose and `systemd`, plus production procedures for Zakura, node failover, indexer mTLS, signed wallet history, Explorer, upgrades, and the standalone `zinder` binary. Read [Deploying Zinder](deploying.md) first for preset selection, host sizing, and the canonical port matrix.

## Before you start

- A Linux VM with Docker Engine and the Docker Compose v2 plugin, sized per [Size your host](deploying.md#what-you-need).
- An archive Zebra 7.1.0 or Zakura 1.8.6 node on the same Docker host, running as UID 10001 (image default) so Zinder containers can read its mode-0600 RPC `.cookie` volume. Do not override `user:` on the node or Zinder containers ([Runtime image](../architecture/service-operations.md#runtime-image)).
- A reverse proxy for TLS termination, client limits, and authentication ([Exposing over TLS](exposing-over-tls.md)).

## Install

### 1. Get Zinder at a release tag

```bash
sudo git clone https://github.com/ZcashFoundation/zinder.git /opt/zinder
sudo git -C /opt/zinder checkout vX.Y.Z
```

See [Releasing](releasing.md) to verify the signed release image and binary archive.

### 2. Prepare host control tokens

`deploy/docker-compose.yml` requires host control tokens in `${DEPLOY_CONTROL_SECRETS_DIR:-/etc/zinder/control-secrets}` (`ingest.token` and `checkpoint.token`). Initialize both before starting Compose:

```bash
sudo install -d -o root -g 10001 -m 0750 /etc/zinder/control-secrets
head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' | sudo install -o root -g 10001 -m 0440 /dev/stdin /etc/zinder/control-secrets/ingest.token
head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' | sudo install -o root -g 10001 -m 0440 /dev/stdin /etc/zinder/control-secrets/checkpoint.token
```

### 3. Write the environment files

Copy the `deploy.env` (host/Compose orchestration) and `zinder.env` (Zinder runtime configuration) pair matching your node and network to `/etc/zinder/`:

| Node | Mainnet | Testnet | Regtest |
| --- | --- | --- | --- |
| Zebra through Z3 | `deploy/profiles/mainnet/` | `deploy/profiles/testnet/` | `deploy/profiles/regtest/` |
| Zakura | `deploy/profiles/zakura-mainnet/` | `deploy/profiles/zakura-testnet/` | not provided |

```bash
sudo install -d -o root -g root -m 0755 /etc/zinder
sudo cp /opt/zinder/deploy/profiles/mainnet/deploy.env /etc/zinder/deploy.env
sudo cp /opt/zinder/deploy/profiles/mainnet/zinder.env /etc/zinder/zinder.env
sudo sed -i 's|^DEPLOY_ZINDER_ENV=.*|DEPLOY_ZINDER_ENV=/etc/zinder/zinder.env|' /etc/zinder/deploy.env
echo 'DEPLOY_VERSION=vX.Y.Z' | sudo tee -a /etc/zinder/deploy.env >/dev/null
```

Check these core variables in `/etc/zinder/deploy.env`:

| Variable | Meaning |
| --- | --- |
| `DEPLOY_NETWORK` | `mainnet`, `testnet`, or `regtest` (names the Compose project and volumes). |
| `DEPLOY_ZINDER_ENV` | Path to the shared runtime `zinder.env` loaded into both `zinder-index` and `zinder-serve`. |
| `DEPLOY_INDEX_ENV` | Optional path to a writer-only env file loaded only into `zinder-index` (for `ZINDER_NODE__INDEXER_TLS__*`, `ZINDER_NODE_FALLBACKS__*__INDEXER_TLS__*`, or `ZINDER_INGEST__SOURCE`). |
| `COMPOSE_PROFILES` | Optional Compose profiles (set `observability` for Prometheus + Grafana). |
| `DEPLOY_NODE_NETWORK` | Node's external Docker network. |
| `DEPLOY_NODE_COOKIE_VOLUME` | Node's external `.cookie` volume. |

Check these core variables in `/etc/zinder/zinder.env` (never put `DEPLOY_*` variables or empty `ZINDER_*=` placeholders in `zinder.env`, and never put writer-only `ZINDER_NODE__INDEXER_TLS__*` or `ZINDER_INGEST__SOURCE` keys in shared `zinder.env`):

| Variable | Meaning |
| --- | --- |
| `ZINDER_DEPLOYMENT__PROFILE` | Deployment profile: `light-wallet`, `wallet` (default), or `explorer`. |
| `ZINDER_NETWORK__NAME` | `zcash-mainnet`, `zcash-testnet`, or `zcash-regtest`. |
| `ZINDER_NODE__JSON_RPC_ADDR` | Node's JSON-RPC URL on that network. |
| `ZINDER_NODE__INDEXER_GRPC_ADDR` | Optional node indexer gRPC URL (omit or set `none` to poll mempool over JSON-RPC; empty `ZINDER_NODE__INDEXER_GRPC_ADDR=` is rejected). |

- **Profiles and raw-blob retention:** `deploy/profiles/*/zinder.env` sets `ZINDER_DEPLOYMENT__PROFILE=wallet` (`raw_blob_policy = "none"`), and `deploy/docker-compose.yml` passes `ZINDER_*` settings via `env_file:`. Set `ZINDER_DEPLOYMENT__PROFILE=light-wallet` for minimal shielded-only serving, `ZINDER_DEPLOYMENT__PROFILE=explorer` to add `ExplorerQuery`, `explorer_views`, and `zinder-compat-cipherscan`, and add `ZINDER_STORAGE__FULL_BLOCKS=true` (`raw_blob_policy = "all"`) to retain full raw block and transaction blobs. Changing persisted retention requires rebuilding on fresh volumes.
- **Streaming mempool & binary block fetch:** Set `ZEBRA_RPC__INDEXER_LISTEN_ADDR=0.0.0.0:8155` in Z3's `.env`, restart Z3, set `ZINDER_NODE__INDEXER_GRPC_ADDR=http://zebra:8155` in `/etc/zinder/zinder.env`, and put `ZINDER_INGEST__SOURCE=zebra-indexer-grpc` in writer-only `/etc/zinder/index.env` referenced by `DEPLOY_INDEX_ENV=/etc/zinder/index.env` in `/etc/zinder/deploy.env` (`zinder-serve` rejects writer-only ingest settings; for standalone binaries, export `ZINDER_INGEST__SOURCE=zebra-indexer-grpc` for `zinder index` / `zinder-ingest`).
- **High-concurrency Mainnet endpoints:** On 64+ GiB hosts, size `zinder-serve`'s memory ceiling above 32 GiB (`DEPLOY_SERVE_MEMORY_LIMIT`) to hold the OS page cache for canonical `ZSHRNG02` settled segments; see [Size your host](deploying.md#what-you-need).
- Leave `DEPLOY_VERSION` unset to build `:local` from the checkout instead of pulling the release image, or pin `ghcr.io/zcashfoundation/zinder@sha256:...` via `DEPLOY_IMAGE`.

### 4. Start the runtimes

```bash
cd /opt/zinder
docker compose --env-file /etc/zinder/deploy.env -f deploy/docker-compose.yml config -q
docker compose --env-file /etc/zinder/deploy.env -f deploy/docker-compose.yml pull
docker compose --env-file /etc/zinder/deploy.env -f deploy/docker-compose.yml up -d --no-build
```

If `DEPLOY_VERSION` is unset, skip `pull` and use `up -d --build` here and in later commands. Wait for `zinder-index` (`:9105/readyz`) and `zinder-serve` (`:9108/readyz`) to finish initial construction without restarting them; see [Initial sync](initial-sync.md).

### 5. Verify

Check `/readyz` (`9105` for `zinder-index`, `9108` for `zinder-serve` on Mainnet; `+10000` Testnet, `+20000` Regtest) and run the `ServerInfo` and `GetLightdInfo` smoke calls from steps 5–6 of the [quickstart](deploying.md#quickstart-with-docker-compose).

### 6. Supervise with systemd

Install the unit **after** the first sync finishes:

```bash
sudo cp /opt/zinder/deploy/systemd/zinder.service /etc/systemd/system/zinder.service
sudo systemctl daemon-reload
sudo systemctl enable --now zinder
sudo journalctl -u zinder.service -f
```

`deploy/systemd/zinder.service` runs `docker compose --env-file /etc/zinder/deploy.env -f docker-compose.yml up --no-build` with `Restart=on-failure` and tears down only on explicit `ExecStop` so failed restarts do not discard an active sync or build images unintentionally. `zinder.service` reads `COMPOSE_PROFILES` from `/etc/zinder/deploy.env` automatically (allowing optional profiles such as `observability` without modifying the unit); set `COMPOSE_FILE` in `/etc/zinder/deploy.env` or override `ExecStart=` and `ExecStop=` with `sudo systemctl edit zinder` when attaching a custom `-f /etc/zinder/docker-compose.override.yml` overlay.

### 7. Put TLS in front

Expose the wallet ports only through your reverse proxy as [Exposing over TLS](exposing-over-tls.md) describes.

## Before you take traffic

- Step 5 checks pass.
- External scan confirms only proxy port 443 answers; all Zinder gRPC, REST, ops, control, and observability ports remain unreachable from outside.
- Proxy routes only while `/readyz` returns `200` (or `grpc.health.v1.Health` reports `SERVING`).
- Monitoring alerts on readiness and `zinder_readiness_served_epoch_age_seconds` ([Metrics](../reference/metrics.md)).
- `zinder.service` is enabled.

## Attach a Zakura node

Start Zakura with:

- `ZAKURA_RPC__LISTEN_ADDR=0.0.0.0:8232` (Mainnet) or `0.0.0.0:18232` (Testnet).
- An external Docker network shared with Zinder where the node resolves as `zakura` (`DEPLOY_NODE_NETWORK`).
- `ZAKURA_RPC__COOKIE_DIR=/var/run/auth` on a dedicated named volume (`DEPLOY_NODE_COOKIE_VOLUME`; never mount Zakura's state volume into Zinder).
- An explicit `ZAKURA_NETWORK__ZAKURA__LISTEN_ADDR` (otherwise its QUIC listener binds `0.0.0.0:8234`).

Zakura refuses plaintext indexer listeners off loopback; see [Stream mempool events over indexer TLS](#stream-mempool-events-over-indexer-tls) to stream its mempool.

## Fail over to a second node

Ingest switches to a fallback node when `[node]` fails, and readers fail over broadcasts ([Node endpoint failover](../architecture/chain-ingestion.md#node-endpoint-failover)). Because `deploy/docker-compose.yml` loads `/etc/zinder/zinder.env` via `env_file:`, non-secret `ZINDER_NODE_FALLBACKS__*` settings added to `/etc/zinder/zinder.env` reach both `zinder-index` and `zinder-serve` automatically without editing Compose YAML:

```env
ZINDER_NODE_FALLBACKS__1__JSON_RPC_ADDR=http://zebra-backup:8232
ZINDER_NODE_FALLBACKS__1__AUTH__METHOD=basic
ZINDER_NODE_FALLBACKS__1__AUTH__USERNAME=rpcuser
ZINDER_NODE_FALLBACKS__1__AUTH__PASSWORD=rpcpass
ZINDER_NODE_FALLBACKS__1__BROADCAST_TIMEOUT_SECS=7
```

Put writer-only `ZINDER_NODE_FALLBACKS__1__INDEXER_TLS__*` secrets (and optionally `ZINDER_NODE_FALLBACKS__1__INDEXER_GRPC_ADDR`) in writer-only `/etc/zinder/index.env` referenced by `DEPLOY_INDEX_ENV=/etc/zinder/index.env` in `/etc/zinder/deploy.env` so `zinder-serve` never sees `INDEXER_TLS__*`. If the fallback node uses its own external Docker network or `.cookie` volume, attach that network or volume in `/etc/zinder/docker-compose.override.yml`, then recreate with both Compose files (omit `-f /etc/zinder/docker-compose.override.yml` if you do not use an override file):

```bash
cd /opt/zinder
docker compose --env-file /etc/zinder/deploy.env \
  -f deploy/docker-compose.yml \
  -f /etc/zinder/docker-compose.override.yml \
  up -d --no-build
```

Ingest `/readyz` reports `active_node.position`, and failovers log `node_endpoint_switched`.

## Stream mempool events over indexer TLS

When a node requires mutual TLS on its indexer gRPC port (such as Zakura off loopback), configure the node with `ZAKURA_RPC__INDEXER_LISTEN_ADDR=0.0.0.0:8230`, `ZAKURA_RPC__INDEXER_TLS__CERT_FILE`, `ZAKURA_RPC__INDEXER_TLS__KEY_FILE`, and `ZAKURA_RPC__INDEXER_TLS__CLIENT_CA_FILE` (with the server cert naming `zakura`).

Put `ZINDER_INGEST__SOURCE=zebra-indexer-grpc` and inline `ZINDER_NODE__INDEXER_TLS__*` PEM/SNI settings in a dedicated writer-only `/etc/zinder/index.env` (never in shared `/etc/zinder/zinder.env`, because `zinder serve --readers-only` rejects writer-only TLS secrets) and point `DEPLOY_INDEX_ENV=/etc/zinder/index.env` in `/etc/zinder/deploy.env` so Compose loads it only into `zinder-index`:

```env
ZINDER_INGEST__SOURCE=zebra-indexer-grpc
ZINDER_NODE__INDEXER_GRPC_ADDR=https://zakura:8230
ZINDER_NODE__INDEXER_TLS__SERVER_NAME=zakura.internal
ZINDER_NODE__INDEXER_TLS__CA_CERT_PEM=...
ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PEM=...
ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PEM=...
```

If you prefer file-backed PEM paths (`*_PATH` readable by UID `10001`, reread on every indexer reconnect without restarting), mount `/etc/zinder/tls:/run/zinder-tls:ro` via `/etc/zinder/docker-compose.override.yml` and put the `*_PATH` variables in `/etc/zinder/index.env` or that override:

```yaml
services:
  zinder-index:
    environment:
      ZINDER_INGEST__SOURCE: zebra-indexer-grpc
      ZINDER_NODE__INDEXER_GRPC_ADDR: https://zakura:8230
      ZINDER_NODE__INDEXER_TLS__CA_CERT_PATH: /run/zinder-tls/ca.pem
      ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PATH: /run/zinder-tls/client.pem
      ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH: /run/zinder-tls/client-key.pem
    volumes:
      - /etc/zinder/tls:/run/zinder-tls:ro
```

Recreate with `-f deploy/docker-compose.yml -f /etc/zinder/docker-compose.override.yml` when using an override file (also in `zinder.service` overrides).

## Add the Explorer

Set `ZINDER_DEPLOYMENT__PROFILE=explorer` in `/etc/zinder/zinder.env` and update the Compose deployment:

```bash
cd /opt/zinder
docker compose --env-file /etc/zinder/deploy.env \
  -f deploy/docker-compose.yml up -d --no-build
```

`zinder-index` enables `explorer_views = true`, and `zinder-serve` adds `zinder-explorer` (`:9068`) and `zinder-compat-cipherscan` (`:9070`). Views live under `<data_dir>/canonical/materialized-views` and pause automatically on low free space ([Explorer plane](../architecture/explorer-plane.md#what-it-needs)). Canonical-only displaced-block RPCs (`DisplacedBlockHistory`, `DisplacedBlockDetail`) begin serving once `zinder-index` reaches canonical READY (restart `zinder-serve` if `zinder-explorer` started before canonical primary became READY); `BlockDetail`, block production, `BlockTransactions`, `CommitmentRootSearch`, and aggregate/search methods additionally depend on `explorer_views` catch-up/backfill coverage and `WalletQuery` readiness. If an upgrade changes view schemas (`materialized_view_store_open_failed`):

1. Record capabilities from `ExplorerQuery.ServerInfo`.
2. Stop `zinder-serve` and `zinder-index` (or restart cleanly after moving views).
3. Move `/var/lib/zinder/canonical/materialized-views` out of the canonical path on the same volume.
4. Start `zinder-index` and `zinder-serve` (ingest rebuilds views while wallet serving continues).
5. Delete the moved directory once `ServerInfo` reports `lag_blocks: 0` and all expected capabilities.

## Upgrade, restart, and roll back

Starting at `v0.28.0`, every release declares an `**Upgrade class:**` line at the top of its release notes, replacing ad-hoc per-release store layout declarations. Each release's class describes compatibility against the immediately preceding release. When upgrading across skipped releases, inspect every intervening release between your running version and the target version, apply the most disruptive class (`compatible < additive-schema < projection-layout < canonical-layout`), and rebuild the union of all affected projections (`wallet`, `explorer`, or `materialized-view`) named across those releases (or follow the highest canonical rebuild contract if any intervening release is `canonical-layout`):

- **`compatible` and `additive-schema`:** Upgrade in place against existing stores and schemas (provided no skipped intervening release has a higher class). Roll back by redeploying the previous release against the same store and schema.
- **`projection-layout`:** Rebuild the affected projection(s) named across the release (and any skipped intervening releases) in fresh empty paths ([Schema admission](../architecture/storage-backend.md#schema-admission)).
- **`canonical-layout`:** Rebuild the canonical store and dependent projections from scratch in fresh empty store paths ([Schema admission](../architecture/storage-backend.md#schema-admission)).

Single-volume `rocksdb` hosts still restart every runtime at each release, whatever the class. For layout changes on `rocksdb`, keeping old and new store paths side by side for rollback is optional (~2x disk until you delete the old path); fallback rollback otherwise requires restoring a matching backup or snapshot ([Backups and Edge Snapshots](private-backups.md)) or rebuilding from empty in a clean path with the previous release. Seven-day superseded-schema retention applies strictly to future hosted `postgres` (planned M3, not yet implemented).

> Hosts installed with the legacy single `/etc/zinder/env` file must complete the one-time [Migrate from legacy `/etc/zinder/env`](#migrate-from-legacy-etczinderenv) procedure before using the steady-state `/etc/zinder/deploy.env` upgrade steps below.

1. Fetch tags, check out the release, and update `DEPLOY_VERSION` in `/etc/zinder/deploy.env`:

   ```bash
   sudo git -C /opt/zinder fetch --tags
   sudo git -C /opt/zinder checkout vX.Y.Z
   sudo sed -i 's/^DEPLOY_VERSION=.*/DEPLOY_VERSION=vX.Y.Z/' /etc/zinder/deploy.env
   ```

2. Pull the new image before restarting (add `-f /etc/zinder/docker-compose.override.yml` if you maintain an override file, or configure `COMPOSE_FILE` in `/etc/zinder/deploy.env`):

   ```bash
   cd /opt/zinder
   docker compose --env-file /etc/zinder/deploy.env -f deploy/docker-compose.yml pull
   ```

3. If the checked-out unit changed, reinstall `/opt/zinder/deploy/systemd/zinder.service` and reload systemd (`sudo cp /opt/zinder/deploy/systemd/zinder.service /etc/systemd/system/zinder.service && sudo systemctl daemon-reload`), keeping any `/etc/systemd/system/zinder.service.d/` drop-in aligned with your `/etc/zinder/docker-compose.override.yml`. Then restart `sudo systemctl restart zinder` (or restart `zinder-index` then `zinder-serve` cleanly; readers stop serving 5 minutes after `zinder-index` stops).

To roll back across releases on `rocksdb`, apply the same multi-release rule across every version stepped back over: if all intervening releases are `compatible` or `additive-schema`, redeploy the earlier release against the existing store; if any intervening release between the running version and the rollback target is `projection-layout` or `canonical-layout`, start the earlier release against an optionally retained separate store path, restore a matching backup or snapshot, or rebuild the affected stores from empty in clean paths. If you are rolling back across the env-file split to a pre-split `/etc/zinder/env` release, follow [Roll back to a pre-split `/etc/zinder/env` release](#roll-back-to-a-pre-split-etczinderenv-release).

### Migrate from legacy `/etc/zinder/env`

Perform this one-time migration when upgrading a host whose installed systemd unit and configuration still use `/etc/zinder/env` (`ZINDER_DEPLOY_NETWORK` / `ZINDER_SOURCE_*`):

1. Stop the legacy unit **before** updating `/opt/zinder`, while the running unit and `ExecStop` still match `/etc/zinder/env`, and leave `/etc/zinder/env` intact (plus backups of any `/etc/zinder/docker-compose.override.yml` and `/etc/systemd/system/zinder.service.d/` drop-ins) for rollback:

   ```bash
   sudo systemctl stop zinder
   sudo cp -a /etc/zinder /etc/zinder.pre-split.bak
   if [[ -d /etc/systemd/system/zinder.service.d ]]; then
     sudo cp -a /etc/systemd/system/zinder.service.d /etc/zinder.pre-split.bak/zinder.service.d
   fi
   ```

2. Fetch tags and check out the target release in `/opt/zinder`, then seed `/etc/zinder/deploy.env` and `/etc/zinder/zinder.env` from the matching `deploy/profiles/<name>/` pair (`mainnet`, `testnet`, `regtest`, `zakura-mainnet`, or `zakura-testnet`):

   ```bash
   sudo git -C /opt/zinder fetch --tags
   sudo git -C /opt/zinder checkout vX.Y.Z
   sudo cp /opt/zinder/deploy/profiles/mainnet/deploy.env /etc/zinder/deploy.env
   sudo cp /opt/zinder/deploy/profiles/mainnet/zinder.env /etc/zinder/zinder.env
   sudo sed -i 's|^DEPLOY_ZINDER_ENV=.*|DEPLOY_ZINDER_ENV=/etc/zinder/zinder.env|' /etc/zinder/deploy.env
   echo 'DEPLOY_VERSION=vX.Y.Z' | sudo tee -a /etc/zinder/deploy.env >/dev/null
   ```

3. Split your custom overrides from legacy `/etc/zinder/env` into `/etc/zinder/deploy.env`, `/etc/zinder/zinder.env`, and optional writer-only `/etc/zinder/index.env` (preserving the Compose project and Docker volume names):

   | Legacy `/etc/zinder/env` key | New destination and key |
   | --- | --- |
   | `ZINDER_DEPLOY_NETWORK` | `/etc/zinder/deploy.env`: `DEPLOY_NETWORK` |
   | `ZINDER_SOURCE_NETWORK_NAME` | `/etc/zinder/deploy.env`: `DEPLOY_NODE_NETWORK` |
   | `ZINDER_SOURCE_COOKIE_VOLUME_NAME` | `/etc/zinder/deploy.env`: `DEPLOY_NODE_COOKIE_VOLUME` |
   | `ZINDER_OBSERVABILITY_NETWORK_NAME` | `/etc/zinder/deploy.env`: `DEPLOY_OBSERVABILITY_NETWORK` |
   | `ZINDER_SINGLE_HOST_DATA_VOLUME_NAME`, `ZINDER_CHECKPOINT_STAGING_VOLUME_NAME`, `ZINDER_PROMETHEUS_VOLUME_NAME`, `ZINDER_GRAFANA_VOLUME_NAME` | `/etc/zinder/deploy.env`: `DEPLOY_DATA_VOLUME`, `DEPLOY_CHECKPOINT_STAGING_VOLUME`, `DEPLOY_PROMETHEUS_VOLUME`, `DEPLOY_GRAFANA_VOLUME` |
   | `ZINDER_VERSION`, `ZINDER_IMAGE`, `ZINDER_PASTA_ASM`, `ZINDER_CONTROL_SECRETS_DIR` | `/etc/zinder/deploy.env`: `DEPLOY_VERSION`, `DEPLOY_IMAGE`, `DEPLOY_PASTA_ASM`, `DEPLOY_CONTROL_SECRETS_DIR` |
   | `ZINDER_INGEST_OPS_HOST_PORT` (legacy), `ZINDER_COMPAT_OPS_HOST_PORT` (legacy), other `ZINDER_*_HOST_PORT` | `/etc/zinder/deploy.env`: `DEPLOY_INDEX_OPS_HOST_PORT`, `DEPLOY_SERVE_OPS_HOST_PORT`, `DEPLOY_*_HOST_PORT` |
   | `ZINDER_*_MEMORY_LIMIT`, `ZINDER_NOFILE_{SOFT,HARD}_LIMIT`, `ZINDER_PROMETHEUS_RETENTION`, `ZINDER_GRAFANA_ADMIN_PASSWORD` | `/etc/zinder/deploy.env`: `DEPLOY_*_MEMORY_LIMIT`, `DEPLOY_NOFILE_{SOFT,HARD}_LIMIT`, `DEPLOY_PROMETHEUS_RETENTION`, `DEPLOY_GRAFANA_ADMIN_PASSWORD` |
   | `COMPOSE_PROFILES`, `COMPOSE_FILE` | Keep in `/etc/zinder/deploy.env` as-is |
   | `ZINDER_PRESET` | `/etc/zinder/zinder.env`: `ZINDER_DEPLOYMENT__PROFILE` |
   | Non-writer `ZINDER_*__*` (`ZINDER_NETWORK__NAME`, `ZINDER_NODE__JSON_RPC_ADDR`, `ZINDER_NODE__AUTH__*`, non-empty `ZINDER_NODE__INDEXER_GRPC_ADDR`, non-TLS `ZINDER_NODE_FALLBACKS__*`, `ZINDER_STORAGE__FULL_BLOCKS`, `ZINDER_COMPAT__*`, `ZINDER_QUERY__*`, `ZINDER_EXPLORER__*`, `ZINDER_PROJECTOR__*`) | `/etc/zinder/zinder.env` (delete empty `ZINDER_*=` placeholders such as `ZINDER_NODE__INDEXER_GRPC_ADDR=`; omit unset optional keys or set `ZINDER_NODE__INDEXER_GRPC_ADDR=none`) |
   | Writer-only `ZINDER_INGEST__*`, `ZINDER_NODE__INDEXER_TLS__*`, `ZINDER_NODE_FALLBACKS__*__INDEXER_TLS__*` | Writer-only `/etc/zinder/index.env` (`root:root`, mode `0640` or `0600`), with `DEPLOY_INDEX_ENV=/etc/zinder/index.env` set in `/etc/zinder/deploy.env` |

   - If you use `/etc/zinder/docker-compose.override.yml`, update any `${ZINDER_*}` Compose interpolations (or legacy `zinder-ingest` / `zinder-query` service names if targeting single-host services) to the current Compose names and `DEPLOY_*` variables.
   - If you use a drop-in under `/etc/systemd/system/zinder.service.d/` overriding `ExecStart=` / `ExecStop=`, update `--env-file /etc/zinder/env` to `--env-file /etc/zinder/deploy.env`.

4. Validate the rendered Compose project name and Docker volume names against `docker volume ls` before starting (add `-f /etc/zinder/docker-compose.override.yml` if present), pull the release image, reinstall `deploy/systemd/zinder.service`, reload systemd, and start `zinder`:

   ```bash
   docker volume ls
   cd /opt/zinder
   docker compose --env-file /etc/zinder/deploy.env -f deploy/docker-compose.yml config -q
   docker compose --env-file /etc/zinder/deploy.env -f deploy/docker-compose.yml config \
     | awk '/^name:/ {print; next} /^volumes:/ {in_vol=1} in_vol && /^[^ ]/ && !/^volumes:/ {in_vol=0} in_vol {print}'
   docker compose --env-file /etc/zinder/deploy.env -f deploy/docker-compose.yml pull
   sudo cp /opt/zinder/deploy/systemd/zinder.service /etc/systemd/system/zinder.service
   sudo systemctl daemon-reload
   sudo systemctl start zinder
   ```

### Roll back to a pre-split `/etc/zinder/env` release

First satisfy the multi-release store compatibility rules above for every release stepped back over. Then stop `zinder`, restore the pre-split release checkout and any backed-up `/etc/zinder/docker-compose.override.yml` or `/etc/systemd/system/zinder.service.d/` drop-ins, reinstall the pre-split `deploy/systemd/zinder.service` pointing at the preserved `/etc/zinder/env`, reload systemd, pull with `--env-file /etc/zinder/env` (adding `-f /etc/zinder/docker-compose.override.yml` if used), and start `zinder`:

```bash
sudo systemctl stop zinder
sudo git -C /opt/zinder checkout vPREV.Y.Z
sudo cp /opt/zinder/deploy/systemd/zinder.service /etc/systemd/system/zinder.service
sudo systemctl daemon-reload
cd /opt/zinder
docker compose --env-file /etc/zinder/env -f deploy/docker-compose.yml pull
sudo systemctl start zinder
```

## Inspect a runtime container

The unified release image (`ghcr.io/zcashfoundation/zinder`) is distroless (no shell or `curl`). Invoke `zinder` directly with `docker compose exec`, or attach a read-only toolbox container:

```bash
cd /opt/zinder
docker compose --env-file /etc/zinder/deploy.env -f deploy/docker-compose.yml \
  exec zinder-serve zinder config print
docker compose --env-file /etc/zinder/deploy.env -f deploy/docker-compose.yml \
  exec zinder-serve zinder healthcheck --ready
```

```bash
container="$(docker compose --env-file /etc/zinder/deploy.env \
  -f deploy/docker-compose.yml ps -q zinder-serve)"
docker run --rm -it \
  --network "container:${container}" --pid "container:${container}" \
  --volumes-from "${container}:ro" \
  busybox:1.37 sh
```

## Troubleshooting

| Symptom | Cause | Action |
| --- | --- | --- |
| Ingest logs `node_endpoints_unavailable` (`block 0 is not the configured network's genesis block`) | Node network mismatches `ZINDER_NETWORK__NAME`. | Point `ZINDER_NODE__JSON_RPC_ADDR` at the right network. |
| Ingest exits; `/readyz` showed `node_capability_missing` for `historical_block_bodies` | Node is pruned. | Use an archive node. |
| Ingest `/readyz` reports `node_unavailable` while node answers | Node cookie not readable by UID 10001. | Run the node as UID 10001; do not set `user:` on Zinder containers. |
| Projector `/readyz` reports `wallet_transition_limit_exceeded` | Wallet update exceeds `max_transition_logical_bytes`. | Raise `ZINDER_PROJECTOR__FOLLOW__MAX_TRANSITION_LOGICAL_BYTES` and restart; see [Wallet transition exceeds its logical-byte limit](deploying-single-volume.md#wallet-transition-exceeds-its-logical-byte-limit). |
| Query and compat refuse with `REORG_WINDOW_EXCEEDED` | Reorg deeper than `1000` blocks parked ingest. | Verify chain health, then rebuild on empty paths (a restart parks again). |

## Private backups

See [Backups and Edge Snapshots](private-backups.md) for coherent `zinder backup` / `zinder-projector` backup/restore and `ADR-0024` verified edge-snapshot bootstrap (`zinder snapshot`).

## Side-by-side wallet rebuild and cutover

When the projector requires a wallet rebuild (for example, an expired event cursor), never rebuild in place while serving. Provision lane B on the same host reading the same canonical store with distinct `wallet.path`, secondary paths, `ZINDER_PROJECTOR__BUILD_OWNER_HEX`, and gRPC/ops ports. Wait for lane B `/readyz`, verify reads, switch the proxy upstream, then stop lane A after in-flight requests drain.

## Run the release binary without Docker

Choose the [archive for your CPU and OS](../adrs/0022-release-artifacts.md#decision).
Use `x86_64-unknown-linux-gnu` on x86-64 Linux hosts without AVX2 or ADX.
The GNU/Linux archives require glibc 2.34 or later and `GLIBCXX_3.4.30`.
On Apple Silicon, choose `aarch64-apple-darwin`.
Verify with GitHub CLI 2.94+ and Cosign:

```bash
release=vX.Y.Z
platform=x86_64-v3-unknown-linux-gnu
archive="zinder-${release#v}-${platform}.tar.gz"
sbom="zinder-${release#v}-${platform}.spdx.json"

gh release download "$release" --repo ZcashFoundation/zinder \
  --pattern "$archive" --pattern "$sbom" \
  --pattern SHA256SUMS --pattern SHA256SUMS.sigstore.json &&
  cosign verify-blob --bundle SHA256SUMS.sigstore.json \
    --certificate-identity "https://github.com/ZcashFoundation/zinder/.github/workflows/release.yml@refs/tags/${release}" \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com \
    SHA256SUMS &&
  checksums="$(awk -v archive="$archive" -v sbom="$sbom" '
    $2 == archive { print; archive_count++ }
    $2 == sbom { print; sbom_count++ }
    END { if (archive_count != 1 || sbom_count != 1) exit 1 }
  ' SHA256SUMS)" &&
  shasum -a 256 -c <<< "$checksums" &&
  tar -xzf "$archive"
```

On macOS, run the verified `bin/zinder` from the extracted directory.
To install it system-wide on Linux, run:

```bash
sudo install -m 0755 "zinder-${release#v}-${platform}"/bin/zinder /usr/local/bin/zinder
```

Run `zinder serve --preset wallet --data-dir /var/lib/zinder` (or split `zinder index` + `zinder serve --readers-only`, or invoke individual roles via `zinder run <role>`) configured through `ZINDER_*` environment variables (or an optional combined `--config` TOML file). When supervising native units under `systemd`:

- Keep one local filesystem (`--data-dir`) for all stores, and give separate `zinder serve` instances distinct `--data-dir` or per-reader `secondary_path` directories if running multiple readers.
- Run under an account that can read the node's mode-0600 cookie (or supply `ZINDER_NODE__AUTH__COOKIE` / `basic` auth).
- Set `LimitNOFILE=65536` in each Zinder `[Service]` unit, because readers keep up to 2,048 SST handles open per store generation alongside segment files and sockets.
- Keep serving listeners bound to loopback (`127.0.0.1:...`, the default outside Docker) behind a host reverse proxy.
