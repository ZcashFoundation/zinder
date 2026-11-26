# Deploying on a VM

This runbook sets up the complete single-host Zinder topology on a Linux VM with Docker Compose and `systemd`, plus production procedures for Zakura, node failover, indexer mTLS, signed wallet history, Explorer, upgrades, and standalone binaries. Read [Deploying Zinder](deploying.md) first for profile selection, host sizing, and the canonical port matrix.

## Before you start

- A Linux VM with Docker Engine and the Docker Compose v2 plugin, sized per [Size your host](deploying.md#what-you-need).
- An archive Zebra 7.1.0 or Zakura 1.8.6 node on the same Docker host, running as UID 10001 (image default) so Zinder containers can read its mode-0600 RPC `.cookie` volume. Do not override `user:` on the node or Zinder containers ([Runtime images](../architecture/service-operations.md#runtime-images)).
- A reverse proxy for TLS termination, client limits, and authentication ([Exposing over TLS](exposing-over-tls.md)).

## Install

### 1. Get Zinder at a release tag

```bash
sudo git clone https://github.com/ZcashFoundation/zinder.git /opt/zinder
sudo git -C /opt/zinder checkout vX.Y.Z
```

See [Releasing](releasing.md) to verify release image signatures.

### 2. Create the control secrets and publisher key

Create the two loopback control tokens (owned by `root:10001`, mode `0440`) and prepare a signed-history key generation on durable storage (substitute `testnet` or `regtest` as needed):

```bash
sudo install -d -o root -g 10001 -m 0750 /etc/zinder/control-secrets
openssl rand -hex 32 | sudo tee /etc/zinder/control-secrets/ingest.token >/dev/null
openssl rand -hex 32 | sudo tee /etc/zinder/control-secrets/checkpoint.token >/dev/null
sudo chown root:10001 /etc/zinder/control-secrets/*.token
sudo chmod 0440 /etc/zinder/control-secrets/*.token
sudo install -d -m 0700 /srv/zinder-history/mainnet
sudo /opt/zinder/scripts/prepare-shielded-history.sh /srv/zinder-history/mainnet/key-1
```

Compose defaults `ZINDER_HISTORY_DIR` to `/srv/zinder-history/<network>/key-1`. The script refuses to overwrite an existing generation; reuse the same generation on restart.

### 3. Write the environment file

Copy the env file matching your node and network to `/etc/zinder/env`:

| Node | Mainnet | Testnet | Regtest |
| --- | --- | --- | --- |
| Zebra through Z3 | `.env.mainnet` | `.env.testnet` | `.env.regtest` |
| Zakura | `.env.zakura-mainnet` | `.env.zakura-testnet` | not provided |

```bash
sudo install -d -o root -g root -m 0755 /etc/zinder
sudo cp /opt/zinder/deploy/.env.mainnet /etc/zinder/env
echo 'ZINDER_VERSION=vX.Y.Z' | sudo tee -a /etc/zinder/env >/dev/null
```

Check these required variables in `/etc/zinder/env`:

| Variable | Meaning |
| --- | --- |
| `ZINDER_DEPLOY_NETWORK` | `mainnet`, `testnet`, or `regtest` (names the Compose project and volumes). |
| `ZINDER_SOURCE_NETWORK_NAME` | Node's external Docker network. |
| `ZINDER_SOURCE_COOKIE_VOLUME_NAME` | Node's external `.cookie` volume. |
| `ZINDER_NETWORK__NAME` | `zcash-mainnet`, `zcash-testnet`, or `zcash-regtest`. |
| `ZINDER_NODE__JSON_RPC_ADDR` | Node's JSON-RPC URL on that network. |
| `ZINDER_NODE__INDEXER_GRPC_ADDR` | Node's indexer gRPC URL (empty polls mempool over JSON-RPC). |
| `ZINDER_PROJECTOR_BUILD_OWNER_HEX` | Stable 32-character hex projector identity. |
| `ZINDER_CONTROL_SECRETS_DIR` | Control secrets directory from step 2. |

- **Raw-blob policy (`wallet`, `wallet-archive`, and legacy `transactions` upgrades):** `deploy/docker-compose.yml` defaults `ZINDER_DEPLOYMENT__PROFILE=wallet` (`raw_blob_policy = "none"`). Set `ZINDER_DEPLOYMENT__PROFILE=wallet-archive` in `/etc/zinder/env` to retain all raw blocks and transactions locally, or run `deploy/docker-compose.light-wallet.yml` for the projector-free `light-wallet` topology. Changing persisted retention requires rebuilding on fresh volumes; when upgrading a pre-profile Compose store created with `raw_blob_policy = "transactions"` in place, merge `-f deploy/docker-compose.transactions.yml` ([Upgrade, restart, and roll back](#upgrade-restart-and-roll-back)).
- **Streaming mempool & binary block fetch:** Set `ZEBRA_RPC__INDEXER_LISTEN_ADDR=0.0.0.0:8155` in Z3's `.env`, restart Z3, and set `ZINDER_NODE__INDEXER_GRPC_ADDR=http://zebra:8155` and `ZINDER_INGEST__SOURCE=zebra-indexer-grpc` in `/etc/zinder/env` (or export `ZINDER_INGEST__SOURCE=zebra-indexer-grpc` for standalone binaries).
- **High-concurrency Mainnet endpoints:** On 64+ GiB hosts, raise `ZINDER_COMPAT_MEMORY_LIMIT=32g` (or higher) in `/etc/zinder/env`; see [Size your host](deploying.md#what-you-need).
- Leave `ZINDER_VERSION` unset to build `:local` from the checkout instead of pulling release images, or override individual images with `ZINDER_<SERVICE>_IMAGE` (for example, pinned by digest).

### 4. Start the runtimes

```bash
cd /opt/zinder
docker compose --env-file /etc/zinder/env -f deploy/docker-compose.yml config -q
docker compose --env-file /etc/zinder/env -f deploy/docker-compose.yml pull
docker compose --env-file /etc/zinder/env -f deploy/docker-compose.yml up -d --no-build
```

If `ZINDER_VERSION` is unset, skip `pull` and use `up -d --build` here and in later commands.

Compose health checks give `zinder-ingest` 3 hours and `zinder-projector` 2 hours to finish initial construction. If `up` exits because a build took longer, leave the containers running (restarting discards the unfinished build!) and re-run `up -d --no-build` once `/readyz` returns `200`. See [Initial sync](initial-sync.md) for construction phases.

### 5. Verify

Check `/readyz` and run the `ServerInfo` and `GetLightdInfo` smoke calls from steps 5–6 of the [quickstart](deploying.md#quickstart-with-docker-compose) using the ports for your network from [Processes and canonical ports](deploying.md#what-you-deploy).

### 6. Supervise with systemd

Install the unit **after** the first sync finishes:

```bash
sudo cp /opt/zinder/deploy/systemd/zinder.service /etc/systemd/system/zinder.service
sudo systemctl daemon-reload
sudo systemctl enable --now zinder
sudo journalctl -u zinder.service -f
```

`zinder.service` runs `docker compose ... up --no-build` with `Restart=on-failure` and tears down only on explicit `ExecStop` so failed restarts do not discard an active sync or build images unintentionally. If you merge overlays (such as `docker-compose.transactions.yml`, `docker-compose.explorer.yml`, `docker-compose.node-fallback.yml`, or `docker-compose.indexer-tls.yml`), override `ExecStart` and `ExecStop` with `sudo systemctl edit zinder` so restarts keep them:

```ini
[Service]
ExecStart=
ExecStart=/usr/bin/docker compose --env-file /etc/zinder/env -f docker-compose.yml -f docker-compose.indexer-tls.yml up --no-build
ExecStop=
ExecStop=/usr/bin/docker compose --env-file /etc/zinder/env -f docker-compose.yml -f docker-compose.indexer-tls.yml down
```

### 7. Put TLS in front

Expose the wallet ports only through your reverse proxy as [Exposing over TLS](exposing-over-tls.md) describes.

## Before you take traffic

- Step 5 checks pass.
- External scan confirms only proxy port 443 answers; all Zinder gRPC, ops, control, and observability ports remain unreachable from outside.
- Proxy routes only while `/readyz` returns `200` (or `grpc.health.v1.Health` reports `SERVING`).
- Monitoring alerts on readiness and `zinder_readiness_served_epoch_age_seconds` ([Metrics](../reference/metrics.md)).
- `zinder.service` is enabled and includes every merged overlay.

## Attach a Zakura node

Start Zakura with:

- `ZAKURA_RPC__LISTEN_ADDR=0.0.0.0:8232` (Mainnet) or `0.0.0.0:18232` (Testnet).
- An external Docker network shared with Zinder where the node resolves as `zakura` (`ZINDER_SOURCE_NETWORK_NAME`).
- `ZAKURA_RPC__COOKIE_DIR=/var/run/auth` on a dedicated named volume (`ZINDER_SOURCE_COOKIE_VOLUME_NAME`; never mount Zakura's state volume into Zinder).
- An explicit `ZAKURA_NETWORK__ZAKURA__LISTEN_ADDR` (otherwise its QUIC listener binds `0.0.0.0:8234`).

Zakura refuses plaintext indexer listeners off loopback; see [Stream mempool events over indexer TLS](#stream-mempool-events-over-indexer-tls) to stream its mempool.

## Fail over to a second node

Ingest switches to a fallback node when `[node]` fails, and readers fail over broadcasts ([Node endpoint failover](../architecture/chain-ingestion.md#node-endpoint-failover)). Add to `/etc/zinder/env`:

```text
ZINDER_FALLBACK_SOURCE_NETWORK_NAME=zakura-mainnet
ZINDER_FALLBACK_SOURCE_COOKIE_VOLUME_NAME=zakura-mainnet-cookie
ZINDER_NODE_FALLBACKS__1__JSON_RPC_ADDR=http://zakura:8232
ZINDER_NODE_FALLBACKS__1__INDEXER_GRPC_ADDR=
```

Merge `deploy/docker-compose.node-fallback.yml` (which sets a 7-second broadcast timeout on the fallback) and update `zinder.service`:

```bash
docker compose --env-file /etc/zinder/env \
  -f deploy/docker-compose.yml -f deploy/docker-compose.node-fallback.yml up -d --no-build
```

Ingest `/readyz` reports `active_node.position`, and failovers log `node_endpoint_switched`.

## Stream mempool events over indexer TLS

When a node requires mutual TLS on its indexer gRPC port (such as Zakura off loopback), configure the node with `ZAKURA_RPC__INDEXER_LISTEN_ADDR=0.0.0.0:8230`, `ZAKURA_RPC__INDEXER_TLS__CERT_FILE`, `ZAKURA_RPC__INDEXER_TLS__KEY_FILE`, and `ZAKURA_RPC__INDEXER_TLS__CLIENT_CA_FILE` (with the server cert naming `zakura`). Install Zinder's PEM files outside the data volume:

```bash
sudo install -d -o root -g 10001 -m 0750 /etc/zinder/indexer-tls
sudo install -o root -g 10001 -m 0440 node-indexer-ca.pem /etc/zinder/indexer-tls/ca.pem
sudo install -o root -g 10001 -m 0440 zinder-client.pem /etc/zinder/indexer-tls/client.pem
sudo install -o root -g 10001 -m 0440 zinder-client-key.pem /etc/zinder/indexer-tls/client-key.pem
```

Set in `/etc/zinder/env` (add `ZINDER_NODE__INDEXER_TLS__SERVER_NAME` if the cert names another host) and merge `deploy/docker-compose.indexer-tls.yml`:

```text
ZINDER_NODE__INDEXER_GRPC_ADDR=https://zakura:8230
ZINDER_INDEXER_TLS_DIR=/etc/zinder/indexer-tls
```

```bash
docker compose --env-file /etc/zinder/env \
  -f deploy/docker-compose.yml -f deploy/docker-compose.indexer-tls.yml up -d --no-build
```

Ingest rereads the PEM directory on every indexer reconnect, so certificate rotation takes no restart.

## Publish signed wallet history

`zinder-history-publisher` opens the canonical store read-only, builds initial 4,096-block settled segments using `min(available CPUs, 4)` `--workers` by default (pass `--workers 1` in custom overrides for sequential shared-disk I/O), and commits new settled segments every 60 seconds. On Linux `ext4 -O verity` filesystems, it enables fs-verity on each segment before publishing the signed catalog; other filesystems fall back to per-payload verification ([Signed settled history](../reference/lightwalletd-compatibility.md#signed-settled-history)).

- **Mount & admission rules:** `ZINDER_HISTORY_DIR` must be an absolute host path containing `publisher.pk8` (mounted only into the publisher), `publisher.pub`, and `catalog/` (mounted read-only into `zinder-compat-lightwalletd`). Local bind-mount volumes must use an absolute `device`, `bind`/`rbind` in `o`, and only `ro`/`rw`/`nodev`/`nosuid`/`noexec`. Custom overlays must declare mounts explicitly.
- **Startup & reorg window:** On cold bootstrap, `zinder-compat-lightwalletd` waits up to 30 seconds for the publisher's empty checkpoint before Compose retries. Once stores are admitted, `zinder-compat-lightwalletd` can reopen existing signed state without the publisher or node running (`65536` `nofile` ulimit in Compose). If you customize `reorg_window_blocks`, match it in `deploy/config/history-publisher.toml`. For cgroup v2 page-cache sizing on `ZINDER_COMPAT_MEMORY_LIMIT`, see [Size your host](deploying.md#what-you-need).

**Back up and restore.** Stop `zinder-history-publisher`, copy `publisher.pk8`, `publisher.pub`, and all of `catalog/` (including the hidden `.publisher-head.json` checkpoint) together, then restart the publisher.

**Serve canonical ranges during an incident.** If a key is compromised or a catalog is damaged, pull the reader out of traffic and launch a private recovery config (adjust `normal` for any active overlays):

```bash
set -euo pipefail
normal=(docker compose --env-file /etc/zinder/env -f deploy/docker-compose.yml)
sudo install -m 0600 /dev/null /etc/zinder/canonical-recovery.pending.json
"${normal[@]}" config --format json |
  jq 'del(.services["zinder-history-publisher"])
    | .services["zinder-compat-lightwalletd"] |=
      (.environment |= with_entries(select(.key | startswith("ZINDER_COMPAT__SHIELDED_HISTORY__") | not))
       | .volumes |= map(select(.target != "/var/lib/zinder-history"
                           and .target != "/run/zinder-history/publisher.pub")))' |
  sudo tee /etc/zinder/canonical-recovery.pending.json >/dev/null
sudo docker compose -f /etc/zinder/canonical-recovery.pending.json config -q
sudo mv /etc/zinder/canonical-recovery{.pending,}.json
"${normal[@]}" stop zinder-history-publisher
sudo docker compose -f /etc/zinder/canonical-recovery.json up -d --no-build \
  --no-deps --force-recreate zinder-compat-lightwalletd
```

Verify `/readyz` and a range read before returning traffic. To survive a reboot, point `zinder.service` at `-f /etc/zinder/canonical-recovery.json` until you restore or rotate the generation, then revert the unit and remove the recovery file.

**Rotate or recover the key.** Provision a fresh generation on the same network (example for Testnet), wait for `.publisher-head.json` (and optionally `catalog/current.json` during routine rotation), update `ZINDER_HISTORY_DIR` in `/etc/zinder/env`, and recreate compatibility:

```bash
export ZINDER_HISTORY_DIR=/srv/zinder-history/testnet/key-2
sudo scripts/prepare-shielded-history.sh "$ZINDER_HISTORY_DIR"
compose=(docker compose --env-file /etc/zinder/env -f deploy/docker-compose.yml)
"${compose[@]}" up -d --no-build --no-deps --force-recreate zinder-history-publisher
sudo test -s "$ZINDER_HISTORY_DIR/catalog/.publisher-head.json"
```

```bash
"${compose[@]}" up -d --no-build --no-deps --force-recreate zinder-compat-lightwalletd
curl -fsS http://127.0.0.1:19107/readyz
```

## Add the Explorer

Merge `deploy/docker-compose.explorer.yml` (add it to `zinder.service` as in step 6). It enables `ZINDER_INGEST__EXPLORER_VIEWS=true` and exposes `zinder-explorer` on loopback ports `9068`/`9069` (`+10000` Testnet, `+20000` Regtest; internal DNS `zinder-${ZINDER_DEPLOY_NETWORK}-explorer:9068`):

```bash
docker compose --env-file /etc/zinder/env \
  -f deploy/docker-compose.yml -f deploy/docker-compose.explorer.yml pull
docker compose --env-file /etc/zinder/env \
  -f deploy/docker-compose.yml -f deploy/docker-compose.explorer.yml up -d --no-build
```

Views live under `/var/lib/zinder/canonical/materialized-views` and pause automatically on low free space ([Explorer plane](../architecture/explorer-plane.md#what-it-needs)). If an upgrade changes view schemas (`materialized_view_store_open_failed` in ingest, `zinder-explorer` exiting):

1. Record capabilities from `ExplorerQuery.ServerInfo`.
2. Stop `zinder-explorer`.
3. Move `/var/lib/zinder/canonical/materialized-views` out of the canonical path on the same volume.
4. Restart `zinder-ingest`, then start `zinder-explorer` once ingest is ready (ingest rebuilds views while wallet serving continues).
5. Delete the moved directory once `ServerInfo` reports `lag_blocks: 0` and all expected capabilities.

## Upgrade, restart, and roll back

Check release notes for store layout changes before upgrading. A layout change requires fresh empty store paths ([Schema admission](../architecture/storage-backend.md#schema-admission)). If replacing a canonical store in place changes workload, raw-blob retention, first retained height, or subtree-root coverage, restart the readers ([Canonical store replacement](../reference/readiness.md#canonical-store-replacement)). Keep previous binaries/images and their separate store if you need rollback.

**Upgrading an existing `transactions`-retaining store.** Volumes created by earlier releases with `raw_blob_policy = "transactions"` fail writer and reader admission under the new `wallet` default (`none`) or `wallet-archive` (`all`), because changing persisted retention requires rebuilding the store. Either:

- Keep the existing store in place by merging `-f deploy/docker-compose.transactions.yml` into your `docker compose` commands and `zinder.service` overrides (which resets `ZINDER_DEPLOYMENT__PROFILE` and sets `ZINDER_STORAGE__RAW_BLOB_POLICY=transactions`); or
- Reclaim ~251.1 GiB on Mainnet by stopping the stack, replacing `data` and `checkpoint-staging` with fresh empty volumes, and resyncing under the default `wallet` profile.

1. Fetch tags, check out the release, and update `ZINDER_VERSION` in `/etc/zinder/env`:

   ```bash
   sudo git -C /opt/zinder fetch --tags
   sudo git -C /opt/zinder checkout vX.Y.Z
   sudo sed -i 's/^ZINDER_VERSION=.*/ZINDER_VERSION=vX.Y.Z/' /etc/zinder/env
   ```

2. Pull new images before restarting (include any active overlays):

   ```bash
   cd /opt/zinder
   docker compose --env-file /etc/zinder/env -f deploy/docker-compose.yml pull
   ```

3. Restart `sudo systemctl restart zinder` (stops and restarts all runtimes in order; clients see brief downtime until readers return `ready`).

To restart without upgrading, restart one process at a time in order (`zinder-ingest`, `zinder-projector`, `zinder-query`, `zinder-compat-lightwalletd`), waiting for each `/readyz` before the next (readers stop serving 5 minutes after ingest stops):

```bash
docker compose --env-file /etc/zinder/env -f /opt/zinder/deploy/docker-compose.yml restart zinder-ingest
```

To roll back, start the earlier release against the separate store you retained for it.

## Inspect a runtime container

Per-service release images are distroless (no shell or `curl`). Run the service binary directly, or attach a read-only toolbox container:

```bash
cd /opt/zinder
docker compose --env-file /etc/zinder/env -f deploy/docker-compose.yml \
  exec zinder-query zinder-query --config /etc/zinder/config.toml --print-config
docker compose --env-file /etc/zinder/env -f deploy/docker-compose.yml \
  exec zinder-query zinder-query healthcheck
```

```bash
container="$(docker compose --env-file /etc/zinder/env \
  -f deploy/docker-compose.yml ps -q zinder-query)"
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
| Ingest `/readyz` reports `node_unavailable` while node answers | Node cookie not readable by UID 10001. | Run the node as UID 10001; do not set `user:` on Zinder services. |
| Projector `/readyz` reports `wallet_transition_limit_exceeded` | Wallet update exceeds `max_transition_logical_bytes`. | Raise the limit and restart; see [Wallet transition exceeds its logical-byte limit](deploying-single-volume.md#wallet-transition-exceeds-its-logical-byte-limit). |
| Query and compat refuse with `REORG_WINDOW_EXCEEDED` | Reorg deeper than `1000` blocks parked ingest. | Verify chain health, then rebuild on empty paths (a restart parks again). |

## Private backups

See [Backups and Edge Snapshots](private-backups.md) for coherent `zinder-projector` backup/restore and `ADR-0024` verified edge-snapshot bootstrap.

## Side-by-side wallet rebuild and cutover

When the projector requires a wallet rebuild (for example, an expired event cursor), never rebuild in place while serving. Provision lane B on the same host reading the same canonical store with distinct wallet path, projector/reader secondary paths, `ZINDER_PROJECTOR_BUILD_OWNER_HEX`, and gRPC/ops ports. Wait for lane B's projector, query, and compat `/readyz`, verify reads, switch the proxy upstream, then stop lane A after in-flight requests drain.

## Run the release binaries without Docker

Releases publish GNU/Linux archives (`x86_64-v3-unknown-linux-gnu` and `aarch64-unknown-linux-gnu`; baseline glibc 2.34+, `GLIBCXX_3.4.30`, Debian 12 tested). Verify with GitHub CLI 2.94+ and Cosign:

```bash
release=vX.Y.Z
platform=x86_64-v3-unknown-linux-gnu   # or aarch64-unknown-linux-gnu
gh release download "$release" --repo ZcashFoundation/zinder \
  --pattern "zinder-${release#v}-${platform}.tar.gz" \
  --pattern "zinder-${release#v}-${platform}.spdx.json" \
  --pattern SHA256SUMS --pattern SHA256SUMS.sigstore.json
sha256sum --ignore-missing --check SHA256SUMS
cosign verify-blob --bundle SHA256SUMS.sigstore.json \
  --certificate-identity "https://github.com/ZcashFoundation/zinder/.github/workflows/release.yml@refs/tags/${release}" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  SHA256SUMS
tar -xzf "zinder-${release#v}-${platform}.tar.gz"
sudo install -m 0755 "zinder-${release#v}-${platform}"/bin/zinder-* /usr/local/bin/
```

When supervising directly (for example with systemd units using `deploy/config/*.toml` via `--config`), provide:

- One filesystem for all stores, with matching `storage.path` / `wallet.path` and unique per-reader `secondary_path` directories.
- The two control tokens, node endpoint/network/projector owner env vars, and read access to the node's mode-0600 cookie (run as the node's UID).
- Startup order: `zinder-ingest`, then `zinder-projector`, then readers after each reports ready.
- `LimitNOFILE=65536` in each Zinder runtime's `[Service]` unit to match Compose, because readers keep up to 2,048 SST handles open per store generation alongside segment files and sockets.
- A loopback listen address in `compat-lightwalletd.toml` (`127.0.0.1:9067`) if running behind a host proxy (the shipped file binds `0.0.0.0:9067` for Compose).
