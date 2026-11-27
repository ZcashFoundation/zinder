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

### 2. Optional: prepare a signed shielded-history key generation

If serving `CompactTxStreamer` ranges from signed `ZSHRNG01` segments (`history-publisher`), provision an Ed25519 key generation on `ext4 -O verity` storage (substitute `testnet` or `regtest` as needed):

```bash
sudo install -d -m 0700 /srv/zinder-history/mainnet
sudo /opt/zinder/scripts/prepare-shielded-history.sh /srv/zinder-history/mainnet/key-1
```

Reuse the same key generation directory across restarts; `prepare-shielded-history.sh` refuses to overwrite an existing generation.

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

Check these core variables in `/etc/zinder/env`:

| Variable | Meaning |
| --- | --- |
| `ZINDER_DEPLOY_NETWORK` | `mainnet`, `testnet`, or `regtest` (names the Compose project and volumes). |
| `ZINDER_PRESET` | Deployment preset: `light-wallet`, `wallet` (default), or `explorer`. |
| `ZINDER_SOURCE_NETWORK_NAME` | Node's external Docker network. |
| `ZINDER_SOURCE_COOKIE_VOLUME_NAME` | Node's external `.cookie` volume. |
| `ZINDER_NETWORK__NAME` | `zcash-mainnet`, `zcash-testnet`, or `zcash-regtest`. |
| `ZINDER_NODE__JSON_RPC_ADDR` | Node's JSON-RPC URL on that network. |
| `ZINDER_NODE__INDEXER_GRPC_ADDR` | Node's indexer gRPC URL (empty polls mempool over JSON-RPC). |

- **Presets and raw-blob retention:** `deploy/docker-compose.yml` defaults `ZINDER_PRESET` to `wallet` (`raw_blob_policy = "none"`). Set `ZINDER_PRESET=light-wallet` for minimal shielded-only serving, `ZINDER_PRESET=explorer` to add `ExplorerQuery`, `explorer_views`, and `zinder-compat-cipherscan`, and add `ZINDER_STORAGE__FULL_BLOCKS=true` (`raw_blob_policy = "all"`) to retain full raw block and transaction blobs. Changing persisted retention requires rebuilding on fresh volumes; for existing pre-profile volumes created with `raw_blob_policy = "transactions"`, leave `ZINDER_DEPLOYMENT__PROFILE` unset and set `ZINDER_STORAGE__RAW_BLOB_POLICY=transactions`.
- **Streaming mempool & binary block fetch:** Set `ZEBRA_RPC__INDEXER_LISTEN_ADDR=0.0.0.0:8155` in Z3's `.env`, restart Z3, and set `ZINDER_NODE__INDEXER_GRPC_ADDR=http://zebra:8155` and `ZINDER_INGEST__SOURCE=zebra-indexer-grpc` in `/etc/zinder/env` (or export `ZINDER_INGEST__SOURCE=zebra-indexer-grpc` for standalone binaries).
- **High-concurrency Mainnet endpoints:** On 64+ GiB hosts, size `zinder-serve`'s memory ceiling above 32 GiB (`ZINDER_SERVE_MEMORY_LIMIT`) for the ~21+ GB signed catalog page cache; see [Size your host](deploying.md#what-you-need).
- Leave `ZINDER_VERSION` unset to build `:local` from the checkout instead of pulling the release image, or pin `ghcr.io/zcashfoundation/zinder@sha256:...` via `ZINDER_IMAGE`.

### 4. Start the runtimes

```bash
cd /opt/zinder
docker compose --env-file /etc/zinder/env -f deploy/docker-compose.yml config -q
docker compose --env-file /etc/zinder/env -f deploy/docker-compose.yml pull
docker compose --env-file /etc/zinder/env -f deploy/docker-compose.yml up -d --no-build
```

If `ZINDER_VERSION` is unset, skip `pull` and use `up -d --build` here and in later commands. Wait for `zinder-index` (`:9105/readyz`) and `zinder-serve` (`:9108/readyz`) to finish initial construction without restarting them; see [Initial sync](initial-sync.md).

### 5. Verify

Check `/readyz` (`9105` for `zinder-index`, `9108` for `zinder-serve` on Mainnet; `+10000` Testnet, `+20000` Regtest) and run the `ServerInfo` and `GetLightdInfo` smoke calls from steps 4–5 of the [quickstart](deploying.md#quickstart-with-docker-compose).

### 6. Supervise with systemd

Install the unit **after** the first sync finishes:

```bash
sudo cp /opt/zinder/deploy/systemd/zinder.service /etc/systemd/system/zinder.service
sudo systemctl daemon-reload
sudo systemctl enable --now zinder
sudo journalctl -u zinder.service -f
```

`deploy/systemd/zinder.service` runs `docker compose --env-file /etc/zinder/env -f deploy/docker-compose.yml up --no-build` with `Restart=on-failure` and tears down only on explicit `ExecStop` so failed restarts do not discard an active sync or build images unintentionally. Override with `sudo systemctl edit zinder` if you mount custom files or attach extra Compose overlays (e.g., `--profile observability`).

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
- An external Docker network shared with Zinder where the node resolves as `zakura` (`ZINDER_SOURCE_NETWORK_NAME`).
- `ZAKURA_RPC__COOKIE_DIR=/var/run/auth` on a dedicated named volume (`ZINDER_SOURCE_COOKIE_VOLUME_NAME`; never mount Zakura's state volume into Zinder).
- An explicit `ZAKURA_NETWORK__ZAKURA__LISTEN_ADDR` (otherwise its QUIC listener binds `0.0.0.0:8234`).

Zakura refuses plaintext indexer listeners off loopback; see [Stream mempool events over indexer TLS](#stream-mempool-events-over-indexer-tls) to stream its mempool.

## Fail over to a second node

Ingest switches to a fallback node when `[node]` fails, and readers fail over broadcasts ([Node endpoint failover](../architecture/chain-ingestion.md#node-endpoint-failover)). Ensure the container can resolve and authenticate to the fallback (for example via a small local `docker-compose.override.yml` attaching the external fallback network/cookie volume if hosted in another Compose stack) and add to `/etc/zinder/env`:

```text
ZINDER_NODE_FALLBACKS__1__JSON_RPC_ADDR=http://zakura:8232
ZINDER_NODE_FALLBACKS__1__AUTH__METHOD=basic
ZINDER_NODE_FALLBACKS__1__AUTH__USERNAME=__cookie__
ZINDER_NODE_FALLBACKS__1__AUTH__PASSWORD=...
ZINDER_NODE_FALLBACKS__1__INDEXER_GRPC_ADDR=
ZINDER_NODE_FALLBACKS__1__BROADCAST_TIMEOUT_SECS=7
```

Apply the change with `docker compose`:

```bash
docker compose --env-file /etc/zinder/env \
  -f deploy/docker-compose.yml up -d --no-build
```

Ingest `/readyz` reports `active_node.position`, and failovers log `node_endpoint_switched`.

## Stream mempool events over indexer TLS

When a node requires mutual TLS on its indexer gRPC port (such as Zakura off loopback), configure the node with `ZAKURA_RPC__INDEXER_LISTEN_ADDR=0.0.0.0:8230`, `ZAKURA_RPC__INDEXER_TLS__CERT_FILE`, `ZAKURA_RPC__INDEXER_TLS__KEY_FILE`, and `ZAKURA_RPC__INDEXER_TLS__CLIENT_CA_FILE` (with the server cert naming `zakura`).

Either pass the certificates inline in `/etc/zinder/env` via `*_PEM` variables (12-factor secret injection):

```text
ZINDER_NODE__INDEXER_GRPC_ADDR=https://zakura:8230
ZINDER_NODE__INDEXER_TLS__CA_CERT_PEM="-----BEGIN CERTIFICATE-----..."
ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PEM="-----BEGIN CERTIFICATE-----..."
ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PEM="-----BEGIN PRIVATE KEY-----..."
```

Or mount PEM files readable by UID 10001 and point `ZINDER_NODE__INDEXER_TLS__CA_CERT_PATH`, `CLIENT_CERT_PATH`, and `CLIENT_KEY_PATH` at them (set `ZINDER_NODE__INDEXER_TLS__SERVER_NAME` if the cert names another host). File-backed PEMs are reread on every indexer reconnect without restarting.

## Publish signed wallet history

When `ZINDER_HISTORY_PUBLISHER__DIRECTORY` and `ZINDER_HISTORY_PUBLISHER__SIGNING_KEY` (and matching `ZINDER_COMPAT__SHIELDED_HISTORY__*`) are configured, `zinder serve` supervises `history-publisher` automatically (or run `zinder run history-publisher` explicitly). It opens the canonical store read-only, builds initial 4,096-block settled segments using `min(available CPUs, 4)` workers by default, enables `fs-verity` on Linux `ext4 -O verity` filesystems, and commits new settled segments every 60 seconds ([Signed settled history](../reference/lightwalletd-compatibility.md#signed-settled-history)).

- **Mount & permissions:** Keep `publisher.pk8` restricted to UID 10001 (`0700` directory / private volume) while keeping `publisher.pub` and `catalog/` readable by the reader process, matching `reorg_window_blocks` with the canonical store.
- **Back up and restore:** Stop `zinder-serve`, copy `publisher.pk8`, `publisher.pub`, and all of `catalog/` (including `.publisher-head.json`) together, then start `zinder-serve`.
- **Serve canonical ranges during an incident or rotate keys:** Unset or temporarily clear `ZINDER_COMPAT__SHIELDED_HISTORY__*` and `ZINDER_HISTORY_PUBLISHER__*` in `/etc/zinder/env`, recreate `zinder-serve`, provision a fresh generation (`scripts/prepare-shielded-history.sh /srv/zinder-history/<network>/key-2`), update the env vars once ready, and recreate `zinder-serve`.

## Add the Explorer

Set `ZINDER_PRESET=explorer` (or `ZINDER_DEPLOYMENT__PROFILE=explorer`) in `/etc/zinder/env` and update the Compose deployment:

```bash
cd /opt/zinder
docker compose --env-file /etc/zinder/env \
  -f deploy/docker-compose.yml up -d --no-build
```

`zinder-index` enables `explorer_views = true`, and `zinder-serve` adds `zinder-explorer` (`:9068`) and `zinder-compat-cipherscan` (`:9070`). Views live under `<data_dir>/canonical/materialized-views` and pause automatically on low free space ([Explorer plane](../architecture/explorer-plane.md#what-it-needs)). Core canonical Explorer RPCs begin serving as soon as `zinder-index` reaches READY. If an upgrade changes view schemas (`materialized_view_store_open_failed`):

1. Record capabilities from `ExplorerQuery.ServerInfo`.
2. Stop `zinder-serve` and `zinder-index` (or restart cleanly after moving views).
3. Move `/var/lib/zinder/canonical/materialized-views` out of the canonical path on the same volume.
4. Start `zinder-index` and `zinder-serve` (ingest rebuilds views while wallet serving continues).
5. Delete the moved directory once `ServerInfo` reports `lag_blocks: 0` and all expected capabilities.

## Upgrade, restart, and roll back

Check release notes for store layout changes before upgrading. A layout change requires fresh empty store paths ([Schema admission](../architecture/storage-backend.md#schema-admission)). Keep previous binaries/images and their separate store if you need rollback.

**Upgrading an existing `transactions`-retaining store.** Volumes created by earlier releases with `raw_blob_policy = "transactions"` fail admission under `raw_blob_policy = "none"` or `"all"`, because changing persisted retention requires rebuilding the store. Either:

- Keep the existing store in place by leaving `ZINDER_DEPLOYMENT__PROFILE` / `--preset` unset and setting `ZINDER_STORAGE__RAW_BLOB_POLICY=transactions`; or
- Reclaim ~251.1 GiB on Mainnet by stopping the stack, clearing `/var/lib/zinder`, and resyncing under `--preset wallet`.

1. Fetch tags, check out the release, and update `ZINDER_VERSION` in `/etc/zinder/env`:

   ```bash
   sudo git -C /opt/zinder fetch --tags
   sudo git -C /opt/zinder checkout vX.Y.Z
   sudo sed -i 's/^ZINDER_VERSION=.*/ZINDER_VERSION=vX.Y.Z/' /etc/zinder/env
   ```

2. Pull the new image before restarting:

   ```bash
   cd /opt/zinder
   docker compose --env-file /etc/zinder/env -f deploy/docker-compose.yml pull
   ```

3. Restart `sudo systemctl restart zinder` (or restart `zinder-index` then `zinder-serve` cleanly; readers stop serving 5 minutes after `zinder-index` stops).

To roll back, start the earlier release against the separate store you retained for it.

## Inspect a runtime container

The unified release image (`ghcr.io/zcashfoundation/zinder`) is distroless (no shell or `curl`). Invoke `zinder` directly with `docker compose exec`, or attach a read-only toolbox container:

```bash
cd /opt/zinder
docker compose --env-file /etc/zinder/env -f deploy/docker-compose.yml \
  exec zinder-serve zinder config print
docker compose --env-file /etc/zinder/env -f deploy/docker-compose.yml \
  exec zinder-serve zinder healthcheck --ready
```

```bash
container="$(docker compose --env-file /etc/zinder/env \
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

Releases publish GNU/Linux archives (`x86_64-v3-unknown-linux-gnu` and `aarch64-unknown-linux-gnu`; baseline glibc 2.34+, `GLIBCXX_3.4.30`, Debian 12 tested) containing the unified `bin/zinder` binary. Verify with GitHub CLI 2.94+ and Cosign:

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
sudo install -m 0755 "zinder-${release#v}-${platform}"/bin/zinder /usr/local/bin/zinder
```

Run directly with `zinder serve --preset wallet --data-dir /var/lib/zinder` (or split `zinder index` + `zinder serve --readers-only`, or invoke individual roles via `zinder run <role>`) configured through `ZINDER_*` environment variables (or an optional combined `--config` TOML file). When supervising native units under `systemd`:

- Keep one local filesystem (`--data-dir`) for all stores, and give separate `zinder serve` instances distinct `--data-dir` or per-reader `secondary_path` directories if running multiple readers.
- Run under an account that can read the node's mode-0600 cookie (or supply `ZINDER_NODE__AUTH__COOKIE` / `basic` auth).
- Set `LimitNOFILE=65536` in each Zinder `[Service]` unit, because readers keep up to 2,048 SST handles open per store generation alongside segment files and sockets.
- Keep serving listeners bound to loopback (`127.0.0.1:...`, the default outside Docker) behind a host reverse proxy.
