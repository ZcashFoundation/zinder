# Deploying on a VM

This runbook sets up the complete Zinder topology on one Linux VM: ingest,
projector, native query, and lightwalletd compatibility, run by Docker Compose
and kept running by systemd. Read [Deploying Zinder](deploying.md) first for
host requirements and a shorter quickstart.

## Before you start

- A Linux VM with Docker Engine and the Docker Compose v2 plugin.
- An archive Zebra or Zakura node on the same Docker host. Zinder joins the
  node's Docker network and reads its RPC cookie from a named volume. The
  [Z3 stack](https://github.com/ZcashFoundation/z3) provides both for Zebra.
  For Zakura, see [Attach a Zakura node](#attach-a-zakura-node).
- Disk and memory for your network, from
  [What you need](deploying.md#what-you-need).
- A reverse proxy for TLS, authentication, rate limits, and quotas.

All stores live on one Docker volume. Do not split the processes across hosts:
the readers open the stores as RocksDB secondaries, which works only on the
same filesystem.

## Install

### 1. Get Zinder at a release tag

```bash
sudo git clone https://github.com/ZcashFoundation/zinder.git /opt/zinder
sudo git -C /opt/zinder checkout vX.Y.Z
```

The Compose file builds the images from this checkout. To run the released
images instead, set these in the environment file from step 3, preferably
pinned by digest:

```bash
ZINDER_INGEST_IMAGE=ghcr.io/zcashfoundation/zinder-ingest:vX.Y.Z
ZINDER_PROJECTOR_IMAGE=ghcr.io/zcashfoundation/zinder-projector:vX.Y.Z
ZINDER_QUERY_IMAGE=ghcr.io/zcashfoundation/zinder-query:vX.Y.Z
ZINDER_COMPAT_IMAGE=ghcr.io/zcashfoundation/zinder-compat-lightwalletd:vX.Y.Z
```

Then run `docker compose ... pull` and start with `--no-build`. Do not pass
`--build` while these name released images, because Compose then builds from
source and tags the result with the release name. [Releasing](releasing.md)
shows how to verify a release's images.

### 2. Create the control secrets

Ingest's private control API, on loopback port 9100, needs two bearer tokens.
Keep them outside the data volume, owned by root, and readable by the
containers' group, GID 1000:

```bash
sudo install -d -o root -g 1000 -m 0750 /etc/zinder/control-secrets
openssl rand -hex 32 | sudo tee /etc/zinder/control-secrets/ingest.token >/dev/null
openssl rand -hex 32 | sudo tee /etc/zinder/control-secrets/checkpoint.token >/dev/null
sudo chown root:1000 /etc/zinder/control-secrets/*.token
sudo chmod 0440 /etc/zinder/control-secrets/*.token
```

Compose mounts `ingest.token` into all four processes and `checkpoint.token`
into ingest and the projector only.

### 3. Write the environment file

Copy the env file for your node and network to `/etc/zinder/env`:

| Node | Mainnet | Testnet | Regtest |
| --- | --- | --- | --- |
| Zebra through Z3 | `.env.mainnet` | `.env.testnet` | `.env.regtest` |
| Zakura | `.env.zakura-mainnet` | `.env.zakura-testnet` | not provided |

```bash
sudo install -d -o root -g root -m 0755 /etc/zinder
sudo cp /opt/zinder/deploy/.env.mainnet /etc/zinder/env
```

Check these values. Compose refuses to start when any of the first five, or
the last two, is unset:

| Variable | Meaning |
| --- | --- |
| `ZINDER_DEPLOY_NETWORK` | `mainnet`, `testnet`, or `regtest`. It names the Compose project and the volumes. |
| `ZINDER_SOURCE_NETWORK_NAME` | The node's external Docker network. |
| `ZINDER_SOURCE_COOKIE_VOLUME_NAME` | The external volume that holds the node's `.cookie`. |
| `ZINDER_NETWORK__NAME` | `zcash-mainnet`, `zcash-testnet`, or `zcash-regtest`. |
| `ZINDER_NODE__JSON_RPC_ADDR` | The node's JSON-RPC URL on that network. |
| `ZINDER_NODE__INDEXER_GRPC_ADDR` | The node's indexer gRPC URL. Empty makes ingest poll the mempool over JSON-RPC. |
| `ZINDER_PROJECTOR_BUILD_OWNER_HEX` | A stable 32-character hexadecimal projector identity. |
| `ZINDER_CONTROL_SECRETS_DIR` | The directory from step 2. |

The Zebra and Zakura files for one network share the project name, volumes,
ports, and projector identity. Switching between them changes the node and
keeps the stores. The files also set host ports and container memory limits.

Every shipped file leaves `ZINDER_NODE__INDEXER_GRPC_ADDR` empty. To stream
mempool events from a Z3 Zebra node instead, set
`ZEBRA_RPC__INDEXER_LISTEN_ADDR=0.0.0.0:8155` in the Z3 `.env` file, restart
Z3, and set `ZINDER_NODE__INDEXER_GRPC_ADDR=http://zebra:8155`.

The [configuration reference](../reference/configuration.md) lists every
other setting.

### 4. Start the runtimes

Validate the resolved Compose file, then start it:

```bash
cd /opt/zinder
docker compose --env-file /etc/zinder/env -f deploy/docker-compose.yml config -q
docker compose --env-file /etc/zinder/env -f deploy/docker-compose.yml up -d --build
```

Compose starts ingest, then the projector once ingest is healthy, then query
and compatibility once the projector is healthy. On a first sync, the command
waits while the stores are built. The health checks give ingest 3 hours and the
projector 2 hours before they count as unhealthy. A Mainnet build can take
longer. If the command then exits because a container is unhealthy, leave the
running containers alone and run the same command again once that process's
`/readyz` returns 200. [Initial sync](initial-sync.md) describes the phases.

Do not restart ingest or the projector during their first build. A restart
discards the unfinished build and starts it over.

### 5. Verify

Verify as in steps 5 and 6 of the
[quickstart](deploying.md#quickstart-with-docker-compose), using the ports for
your network from [What you deploy](deploying.md#what-you-deploy).

### 6. Supervise with systemd

Install the unit after the first sync has finished. Each start of the unit
runs `docker compose down` first, which would discard a build in progress.

```bash
sudo cp /opt/zinder/deploy/systemd/zinder.service /etc/systemd/system/zinder.service
sudo systemctl daemon-reload
sudo systemctl enable --now zinder
sudo journalctl -u zinder.service -f
```

The unit expects the checkout at `/opt/zinder` and the env file at
`/etc/zinder/env`. It runs only `deploy/docker-compose.yml`. If you merge an
overlay, such as the Explorer or indexer TLS overlay, add it to every command
with `sudo systemctl edit zinder`, or the next start drops it:

```ini
[Service]
ExecStartPre=
ExecStartPre=-/usr/bin/docker compose --env-file /etc/zinder/env -f docker-compose.yml -f docker-compose.indexer-tls.yml down --remove-orphans
ExecStart=
ExecStart=/usr/bin/docker compose --env-file /etc/zinder/env -f docker-compose.yml -f docker-compose.indexer-tls.yml up
ExecStop=
ExecStop=/usr/bin/docker compose --env-file /etc/zinder/env -f docker-compose.yml -f docker-compose.indexer-tls.yml down
```

### 7. Put TLS in front

Expose the wallet ports only through your reverse proxy, and keep every other
port private, as [Exposing over TLS](exposing-over-tls.md) sets up.

## Before you take traffic

- Step 5 passes.
- From another host, the wallet ports answer only through the proxy, and no
  other Zinder port answers.
- The proxy stops sending new requests when compatibility `/readyz` returns
  503.
- Monitoring alerts on readiness and on `zinder_readiness_served_epoch_age_seconds`,
  which grows while the served chain stops advancing. See
  [Metrics](../reference/metrics.md).
- The systemd unit is enabled and names every overlay you use.

## Attach a Zakura node

Start the Zakura node with these settings:

- `ZAKURA_RPC__LISTEN_ADDR=0.0.0.0:8232` on Mainnet, or `0.0.0.0:18232` on
  Testnet.
- An external Docker network shared with Zinder, on which the node is
  reachable as `zakura`. Name it in `ZINDER_SOURCE_NETWORK_NAME`.
- `ZAKURA_RPC__COOKIE_DIR=/var/run/auth` on a dedicated named volume. Name it
  in `ZINDER_SOURCE_COOKIE_VOLUME_NAME`. Do not give Zinder the node's state
  volume.
- An explicit `ZAKURA_NETWORK__ZAKURA__LISTEN_ADDR`. When it is unset, Zakura's
  QUIC peer listener binds `0.0.0.0:8234`.

The node writes its cookie as UID 10001 with mode 0600, and Zinder runs as
UID 1000. Run the cookie-permissions project beside the node with the same
env file. It resets the cookie's mode to 0644 every 5 seconds:

```bash
docker compose --env-file /etc/zinder/env -f deploy/node-cookie-permissions.yml up -d
```

Z3 already does this for Zebra. Zakura refuses a plaintext indexer listener
off loopback, so to stream its mempool, follow
[Stream mempool events over indexer TLS](#stream-mempool-events-over-indexer-tls).

## Fail over to a second node

Ingest can switch to a second node when the first fails, as
[Node endpoint failover](../architecture/chain-ingestion.md#node-endpoint-failover)
describes.

Add the fallback to `/etc/zinder/env`:

```text
ZINDER_FALLBACK_SOURCE_NETWORK_NAME=zakura-mainnet
ZINDER_FALLBACK_SOURCE_COOKIE_VOLUME_NAME=zakura-mainnet-cookie
ZINDER_NODE_FALLBACKS__1__JSON_RPC_ADDR=http://zakura:8232
ZINDER_NODE_FALLBACKS__1__INDEXER_GRPC_ADDR=
```

Run the cookie-permissions project for the fallback's volume, then merge the
overlay:

```bash
ZINDER_SOURCE_COOKIE_VOLUME_NAME=zakura-mainnet-cookie \
  docker compose -p zinder-mainnet-fallback-node-cookie \
  --env-file /etc/zinder/env -f deploy/node-cookie-permissions.yml up -d
docker compose --env-file /etc/zinder/env \
  -f deploy/docker-compose.yml -f deploy/docker-compose.node-fallback.yml up -d
```

The overlay sets a 7-second broadcast timeout for the fallback. Ingest
`/readyz` reports `active_node.position`, and a switch logs
`node_endpoint_switched`.

## Stream mempool events over indexer TLS

A node that serves its indexer gRPC listener over TLS needs the
`deploy/docker-compose.indexer-tls.yml` overlay. Zakura requires mutual TLS on
any indexer listener off loopback. Configure Zakura with
`ZAKURA_RPC__INDEXER_LISTEN_ADDR=0.0.0.0:8230`, its certificate and key in
`ZAKURA_RPC__INDEXER_TLS__CERT_FILE` and `ZAKURA_RPC__INDEXER_TLS__KEY_FILE`,
and the CA that signs Zinder's client certificate in
`ZAKURA_RPC__INDEXER_TLS__CLIENT_CA_FILE`. The node certificate must name the
host Zinder dials, such as `zakura`.

Install Zinder's three PEM files outside the data volume:

```bash
sudo install -d -o root -g 1000 -m 0750 /etc/zinder/indexer-tls
sudo install -o root -g 1000 -m 0440 node-indexer-ca.pem /etc/zinder/indexer-tls/ca.pem
sudo install -o root -g 1000 -m 0440 zinder-client.pem /etc/zinder/indexer-tls/client.pem
sudo install -o root -g 1000 -m 0440 zinder-client-key.pem /etc/zinder/indexer-tls/client-key.pem
```

Set the URL and directory in `/etc/zinder/env`, and add
`ZINDER_NODE__INDEXER_TLS__SERVER_NAME` if the certificate names a different
host:

```bash
ZINDER_NODE__INDEXER_GRPC_ADDR=https://zakura:8230
ZINDER_INDEXER_TLS_DIR=/etc/zinder/indexer-tls
```

Start with the overlay, and add it to the systemd unit as in step 6:

```bash
docker compose --env-file /etc/zinder/env \
  -f deploy/docker-compose.yml -f deploy/docker-compose.indexer-tls.yml up -d
```

Ingest exits with a configuration error when a file is missing or holds no
certificate or key, or when the URL is not `https://`. It rereads the files on
each indexer reconnect, so you can replace them without a restart.

## Add the Explorer

Merge the Explorer overlay. It turns on Explorer views in ingest and starts
`zinder-explorer` on host ports 9068 and 9069 on Mainnet, 19068 and 19069 on
Testnet, and 29068 and 29069 on Regtest:

```bash
docker compose --env-file /etc/zinder/env \
  -f deploy/docker-compose.yml -f deploy/docker-compose.explorer.yml up -d --build
```

To use the released image, set
`ZINDER_EXPLORER_IMAGE=ghcr.io/zcashfoundation/zinder-explorer:vX.Y.Z`, pull,
and start with `--no-build`. Inside the node network, consumers reach the
Explorer at `zinder-${ZINDER_DEPLOY_NETWORK}-explorer:9068`.

The views live in the data volume, under
`/var/lib/zinder/canonical/materialized-views`. Ingest pauses them when the
volume runs low on space, as
[What it costs](../architecture/explorer-plane.md#what-it-costs) describes.

A release that adds, removes, or changes an Explorer view refuses a view store
built by another release. Ingest logs `materialized_view_store_open_failed`
and keeps serving wallets without the views, and `zinder-explorer` exits at
startup. To rebuild the views:

1. Record the capabilities that `ExplorerQuery.ServerInfo` advertised before
   the upgrade.
2. Stop `zinder-explorer`.
3. Move `/var/lib/zinder/canonical/materialized-views` out of the canonical
   path, on the same volume. Ingest opens the view store only at startup and
   does not hold one it refused, so it can keep running.
4. Restart `zinder-ingest`, then start `zinder-explorer` once ingest is ready.
   Ingest rebuilds the views from stored chain history while wallet serving
   continues.
5. Delete the moved directory once `ServerInfo` reports zero `lag_blocks` and
   advertises every capability from step 1.

## Restart and roll back

Restart one process at a time, in order, and wait for each `/readyz` before
the next:

```bash
docker compose --env-file /etc/zinder/env -f /opt/zinder/deploy/docker-compose.yml restart zinder-ingest
```

Repeat for `zinder-projector`, `zinder-query`, and
`zinder-compat-lightwalletd`. Readers stop serving five minutes after ingest
stops, so keep ingest's downtime short.

To roll back a release, route traffic to a lane that already runs the earlier
release. Do not start an earlier release on stores a later release wrote,
unless that release states the stores are compatible.

## Troubleshooting

| Symptom | Cause | Action |
| --- | --- | --- |
| Ingest logs `node_endpoints_unavailable` with `the node's block 0 is not the configured network's genesis block` | The node is on a different network than `ZINDER_NETWORK__NAME`. | Point `ZINDER_NODE__JSON_RPC_ADDR` at a node on the configured network. |
| Ingest exits; `/readyz` reported `node_capability_missing` for `historical_block_bodies` | The node is pruned. | Use an archive node. |
| Ingest `/readyz` reports `node_unavailable` after a node restart | The node replaced its cookie, and Zinder cannot read it yet. | Wait one poll interval. For Zakura, check that the cookie-permissions project runs. |
| Projector `/readyz` reports `wallet_transition_limit_exceeded` | One wallet update is larger than its limit, and the projector parked. | Raise `max_transition_logical_bytes` under `[projector.follow]` in `deploy/config/projector.toml` and restart the projector, as [Wallet transition exceeds its logical-byte limit](deploying-single-volume.md#wallet-transition-exceeds-its-logical-byte-limit) describes. |
| Query and compatibility refuse requests with `REORG_WINDOW_EXCEEDED` | The node replaced more blocks than the 1000-block reorg window, so ingest stopped following. A correctly working node does not do this. | Look for a replaced, resynced, or misbehaving node. Confirm the correct chain, then rebuild the stores on empty paths. Restarting only parks ingest again. |

## Side-by-side wallet rebuild and cutover

The projector refuses some failures with a request to rebuild the wallet in a
separate lane, for example when its cursor is no longer retained. Never
rebuild a wallet store in place while readers serve it. The repository ships
no Compose file for a second lane, so you provision lane B yourself, with all
of these different from lane A:

- the wallet store path and the projector's canonical secondary path;
- the native query and compatibility secondary paths;
- `ZINDER_PROJECTOR_BUILD_OWNER_HEX`;
- the native query and compatibility gRPC and ops ports.

Both lanes read the same canonical store. Start lane B, wait for its
projector, query, and compatibility `/readyz`, and test it with your clients.
Then switch the proxy upstream to lane B. Stop lane A only after its in-flight
requests drain. If lane B reports an expired event cursor, leave lane A
serving and start lane B again on empty paths.

## Run the release binaries without Docker

Each release also publishes GNU/Linux archives of the five runtime binaries
for x86-64-v3 and AArch64 CPUs. They need glibc 2.34 or newer and
`libstdc++.so.6` with `GLIBCXX_3.4.30`. Debian 12 is the tested baseline.
Download and verify an archive with GitHub CLI 2.94 or newer and Cosign:

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

The repository ships no systemd units for the binaries and does not test this
path. If you run them directly, start from the files in `deploy/config/`,
which each binary reads with `--config`, and keep what Compose provides:

- one filesystem for every store, with the paths in those files;
- the two control tokens at the configured paths;
- the node endpoint, network, and projector identity from step 3, as
  environment variables;
- the start order ingest, projector, then the readers, each after the previous
  one is ready;
- loopback listen addresses. `compat-lightwalletd.toml` binds `0.0.0.0:9067`,
  which is safe only inside the Compose network namespace.
