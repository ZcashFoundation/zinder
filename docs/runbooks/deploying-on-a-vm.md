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
- The node running as UID 10001, as the Zebra and Zakura images do, so the
  Zinder containers, which also run as UID 10001, can read its mode-0600
  cookie. If you set another user on the node container, remove that setting.
  [Runtime images](../architecture/service-operations.md#runtime-images)
  explains why.
- Disk and memory for your network, from
  [What you need](deploying.md#what-you-need).
- A reverse proxy for TLS, authentication, rate limits, and quotas.

All stores live on one Docker volume. Run every process on this host, because
the readers open the stores as RocksDB secondaries on the writers'
filesystem.

## Install

### 1. Get Zinder at a release tag

```bash
sudo git clone https://github.com/ZcashFoundation/zinder.git /opt/zinder
sudo git -C /opt/zinder checkout vX.Y.Z
```

Step 3 selects the release images of the same tag. Each Compose file matches
the images of its own release. [Releasing](releasing.md) shows how to verify
a release's image signatures.

### 2. Create the control secrets

Ingest's private control API, on loopback port 9100, needs two bearer tokens.
Keep them outside the data volume, owned by root, and readable by the
containers' group, GID 10001:

```bash
sudo install -d -o root -g 10001 -m 0750 /etc/zinder/control-secrets
openssl rand -hex 32 | sudo tee /etc/zinder/control-secrets/ingest.token >/dev/null
openssl rand -hex 32 | sudo tee /etc/zinder/control-secrets/checkpoint.token >/dev/null
sudo chown root:10001 /etc/zinder/control-secrets/*.token
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
echo 'ZINDER_VERSION=vX.Y.Z' | sudo tee -a /etc/zinder/env >/dev/null
```

`ZINDER_VERSION` selects the signed release images
`ghcr.io/zcashfoundation/zinder-<runtime>:vX.Y.Z`. Set it to the tag from
step 1. To build every image from the checkout instead, leave it unset.
`ZINDER_INGEST_IMAGE`, `ZINDER_PROJECTOR_IMAGE`, `ZINDER_QUERY_IMAGE`,
`ZINDER_COMPAT_IMAGE`, and `ZINDER_EXPLORER_IMAGE` each replace one whole
image reference, for example with one pinned by digest.

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

Validate the resolved Compose file, pull the release images, and start them:

```bash
cd /opt/zinder
docker compose --env-file /etc/zinder/env -f deploy/docker-compose.yml config -q
docker compose --env-file /etc/zinder/env -f deploy/docker-compose.yml pull
docker compose --env-file /etc/zinder/env -f deploy/docker-compose.yml up -d --no-build
```

`pull` and `--no-build` work as in step 4 of the
[quickstart](deploying.md#quickstart-with-docker-compose). If you left
`ZINDER_VERSION` unset, skip `pull` and run `up -d --build` instead. Do the same wherever a later command in this
runbook passes `--no-build`.

Compose starts ingest, then the projector once ingest is healthy, then query
and compatibility once the projector is healthy. Each health check runs the
service's own binary, for example `zinder-query healthcheck`, which requests
that process's `/readyz` and passes only on HTTP 200. `docker compose ps`
shows each container's health state.

On a first sync, the command waits while the stores are built. The health
checks give ingest 3 hours and the projector 2 hours before they count as
unhealthy. A Mainnet build can take longer. If the command then exits because
a container is unhealthy, leave the running containers alone and run the same
command again once that process's `/readyz` returns 200.
[Initial sync](initial-sync.md) describes the phases.

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
  503. A gRPC-aware proxy can instead call `grpc.health.v1.Health` on the
  wallet port, as [gRPC health](../reference/readiness.md#grpc-health)
  describes.
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

Zakura refuses a plaintext indexer listener off loopback, so to stream its
mempool, follow
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

Then merge the overlay:

```bash
docker compose --env-file /etc/zinder/env \
  -f deploy/docker-compose.yml -f deploy/docker-compose.node-fallback.yml up -d --no-build
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
sudo install -d -o root -g 10001 -m 0750 /etc/zinder/indexer-tls
sudo install -o root -g 10001 -m 0440 node-indexer-ca.pem /etc/zinder/indexer-tls/ca.pem
sudo install -o root -g 10001 -m 0440 zinder-client.pem /etc/zinder/indexer-tls/client.pem
sudo install -o root -g 10001 -m 0440 zinder-client-key.pem /etc/zinder/indexer-tls/client-key.pem
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
  -f deploy/docker-compose.yml -f deploy/docker-compose.indexer-tls.yml up -d --no-build
```

Ingest exits with a configuration error when a file is missing or holds no
certificate or key, or when the URL is not `https://`. It rereads the files on
each indexer reconnect, so you can replace them without a restart.

## Publish signed wallet history

The optional Compose publisher builds settled shielded ranges from a read-only
canonical secondary. Wallets keep using the existing lightwalletd endpoint.
Start with the [base deployment](#install) serving wallets. Allow disk space for
the segments and memory for the publisher and reader. The publisher's container
memory limit defaults to the compatibility reader's limit. Set
`ZINDER_HISTORY_PUBLISHER_MEMORY_LIMIT` in the env file to size it separately.
Full Mainnet publication size, admission time, and file descriptor use have
not been qualified.

1. Create a generation on a durable host filesystem outside the Docker data
   volume. Use a root-owned parent directory specific to this network. Run
   the remaining commands from the release checkout.

   ```bash
   sudo install -d -m 0700 /srv/zinder-history/testnet
   export ZINDER_HISTORY_DIR=/srv/zinder-history/testnet/key-1
   sudo scripts/prepare-shielded-history.sh "$ZINDER_HISTORY_DIR"
   ```

   The command creates an Ed25519 key pair and a writable `catalog/`
   directory. It refuses to overwrite an existing generation. Compose mounts
   the private key only in the publisher. The reader gets a separate
   read-only mount of the trusted public key and a read-only catalog mount.

2. Start only the publisher. Include any other overlays already in use in
   these commands. If you customized the canonical store's retention or
   reorg window, match them in `deploy/config/history-publisher.toml`.

   ```bash
   base=(docker compose --env-file /etc/zinder/env -f deploy/docker-compose.yml)
   history=("${base[@]}" -f deploy/docker-compose.shielded-history.yml)
   "${history[@]}" up -d --no-build --no-deps zinder-history-publisher
   "${history[@]}" logs --tail 20 zinder-history-publisher
   sudo test -s "$ZINDER_HISTORY_DIR/catalog/current.json"
   ```

   Wait for the final check to succeed. Publication starts at the first
   retained block and needs 4,096 settled blocks for its first segment.
   The base reader continues serving from canonical storage during that wait.
   The publisher checks for new settlement every 60 seconds and keeps its
   checkpoint in `catalog/.publisher-head.json`.

3. Recreate the compatibility reader with the new mounts. Check its
   readiness on the port from your env file (19107 on Testnet).

   ```bash
   "${history[@]}" up -d --no-build --no-deps --force-recreate zinder-compat-lightwalletd
   curl -fsS http://127.0.0.1:19107/readyz
   ```

   The reader verifies the signature, segments, and settled anchor before
   binding. It admits later extensions while running. The live tail and
   chunks crossing a coverage boundary use the canonical reader. The overlay
   uses per-payload digest verification. Strict fs-verity requires separate
   segment staging as described in [signed settled history](../reference/lightwalletd-compatibility.md#signed-settled-history).

4. Save `ZINDER_HISTORY_DIR` in `/etc/zinder/env` and add the history overlay
   to every [systemd Compose command](#6-supervise-with-systemd) for subsequent
   starts. Reuse this directory on restart. For a backup, stop the publisher,
   copy the private key, the independently trusted public key, and the entire
   catalog including hidden files, then start the publisher. Restore them together.
   A missing checkpoint beside an existing catalog stops the publisher.

**Rotate the publisher key.** Create a fresh generation at a new path in
step 1. Select it with `ZINDER_HISTORY_DIR`, then repeat steps 2 and 3 in
order. Compose recreates the publisher with the new private key and an empty
catalog. The running reader keeps its old mounts until you recreate it.
After the new reader passes readiness, persist the new path in step 4 and
retire the old generation according to your key retention policy.

**Return to canonical reads.** If a key is compromised, a generation cannot
be restored, or reader admission fails, use the base configuration to
recreate the compatibility reader and stop the publisher.

```bash
"${base[@]}" up -d --no-build --no-deps --force-recreate zinder-compat-lightwalletd
"${history[@]}" stop zinder-history-publisher
```

Remove the history overlay from your systemd commands for subsequent starts.
Provision a fresh trusted generation before enabling signed history again
after a key compromise.

## Add the Explorer

Merge the Explorer overlay. It turns on Explorer views in ingest and starts
`zinder-explorer` on host ports 9068 and 9069 on Mainnet, 19068 and 19069 on
Testnet, and 29068 and 29069 on Regtest. `ZINDER_VERSION` selects the
released `zinder-explorer` image too:

```bash
docker compose --env-file /etc/zinder/env \
  -f deploy/docker-compose.yml -f deploy/docker-compose.explorer.yml pull
docker compose --env-file /etc/zinder/env \
  -f deploy/docker-compose.yml -f deploy/docker-compose.explorer.yml up -d --no-build
```

Inside the node network, consumers reach the Explorer at
`zinder-${ZINDER_DEPLOY_NETWORK}-explorer:9068`.

The views live in the data volume, under
`/var/lib/zinder/canonical/materialized-views`. Ingest pauses them when the
volume runs low on space, as
[What it needs](../architecture/explorer-plane.md#what-it-needs) describes.

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

## Upgrade, restart, and roll back

To upgrade, change the release tag:

1. Read the new release's notes for any step the upgrade needs.
2. Check out the new tag and point `ZINDER_VERSION` at it:

   ```bash
   sudo git -C /opt/zinder fetch --tags
   sudo git -C /opt/zinder checkout vX.Y.Z
   sudo sed -i 's/^ZINDER_VERSION=.*/ZINDER_VERSION=vX.Y.Z/' /etc/zinder/env
   ```

3. Pull the new images, adding every overlay you use:

   ```bash
   cd /opt/zinder
   docker compose --env-file /etc/zinder/env -f deploy/docker-compose.yml pull
   ```

   Pull before the restart. The unit's `up` builds any image that is missing
   from the host.

4. Restart the unit with `sudo systemctl restart zinder`. It stops every
   runtime and starts them again in order, so wallets are not served until
   the readers are healthy again.

To restart without upgrading, restart one process at a time, in order, and
wait for each `/readyz` before the next:

```bash
docker compose --env-file /etc/zinder/env -f /opt/zinder/deploy/docker-compose.yml restart zinder-ingest
```

Repeat for `zinder-projector`, `zinder-query`, and
`zinder-compat-lightwalletd`. Readers stop serving five minutes after ingest
stops, so keep ingest's downtime short.

To roll back, repeat the upgrade steps with the earlier tag only when that
release states it can open stores the later release wrote. Otherwise, route
traffic to a lane that already runs the earlier release.

## Inspect a runtime container

The runtime images hold the service binary and the C and C++ runtime
libraries it links, without a shell, `curl`, or package manager. Inspect a
container by running the binary itself, not `docker compose exec zinder-query sh`:

```bash
cd /opt/zinder
# The effective configuration, with secrets redacted.
docker compose --env-file /etc/zinder/env -f deploy/docker-compose.yml \
  exec zinder-query zinder-query --config /etc/zinder/config.toml --print-config
# One readiness probe, which prints the reason when it fails.
docker compose --env-file /etc/zinder/env -f deploy/docker-compose.yml \
  exec zinder-query zinder-query healthcheck
```

A custom health check must call the binary too, for example
`["CMD", "zinder-query", "healthcheck", "--path", "/healthz"]` for liveness
only.

To use ordinary tools on the container's files, processes, or loopback
listeners, attach a throwaway toolbox container to its namespaces, with its
volumes read-only:

```bash
container="$(docker compose --env-file /etc/zinder/env \
  -f deploy/docker-compose.yml ps -q zinder-query)"
docker run --rm -it \
  --network "container:${container}" --pid "container:${container}" \
  --volumes-from "${container}:ro" \
  busybox:1.37 sh
```

The toolbox leaves the running container unchanged and stops when you exit
the shell.

## Troubleshooting

| Symptom | Cause | Action |
| --- | --- | --- |
| Ingest logs `node_endpoints_unavailable` with `the node's block 0 is not the configured network's genesis block` | The node is on a different network than `ZINDER_NETWORK__NAME`. | Point `ZINDER_NODE__JSON_RPC_ADDR` at a node on the configured network. |
| Ingest exits; `/readyz` reported `node_capability_missing` for `historical_block_bodies` | The node is pruned. | Use an archive node. |
| Ingest `/readyz` reports `node_unavailable` while the node answers | Zinder cannot read the node's cookie, because the node runs under a UID other than 10001. | Run the node as UID 10001, the default of the Zebra and Zakura images. Do not set `user:` on the Zinder services; [Runtime images](../architecture/service-operations.md#runtime-images) explains why. |
| Projector `/readyz` reports `wallet_transition_limit_exceeded` | One wallet update is larger than its limit, and the projector parked. | Raise `max_transition_logical_bytes` under `[projector.follow]` in `deploy/config/projector.toml` and restart the projector, as [Wallet transition exceeds its logical-byte limit](deploying-single-volume.md#wallet-transition-exceeds-its-logical-byte-limit) describes. |
| Query and compatibility refuse requests with `REORG_WINDOW_EXCEEDED` | The node replaced more blocks than the 1000-block reorg window, so ingest stopped following. A correctly working node does not do this. | Look for a replaced, resynced, or misbehaving node. Confirm the correct chain, then rebuild the stores on empty paths. A restart parks ingest again. |

## Private backups

[Back up and restore a wallet-serving indexer](private-backups.md) covers
coherent capture, off-host storage, and replacement on a fresh volume.

## Side-by-side wallet rebuild and cutover

The projector refuses some failures with a request to rebuild the wallet in a
separate lane, for example when its cursor is no longer retained. Never
rebuild a wallet store in place while readers serve it. Provision lane B with
your own Compose file, with all of these different from lane A:

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

When you run the binaries directly, you provide their process supervision,
such as your own systemd units. Start from the files in `deploy/config/`,
which each binary reads with `--config`, and provide what Compose provides:

- one filesystem for every store, with the paths in those files;
- the two control tokens at the configured paths;
- the node endpoint, network, and projector identity from step 3, as
  environment variables;
- read access to the node's mode-0600 cookie, which means running as the
  node's user;
- the start order ingest, projector, then the readers, each after the previous
  one is ready;
- loopback listen addresses. `compat-lightwalletd.toml` binds `0.0.0.0:9067`
  for the Compose network namespace, so set a loopback address in its place.
