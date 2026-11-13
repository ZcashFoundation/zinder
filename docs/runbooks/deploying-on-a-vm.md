# Deploying the wallet service on one VM

This runbook operates the supported `rocksdb-single-host` wallet-serving
topology. Four independent processes share one host filesystem:
`zinder-ingest` owns canonical storage, `zinder-projector` owns the wallet
projection, and `zinder-query` plus `zinder-compat-lightwalletd` independently
serve immutable canonical and wallet secondary pairs. The optional explorer
overlay builds a local `zinder-explorer` image for named explorer consumers;
it is not part of the published release-image catalog. The single-volume host
images that a release also publishes are separate deployment shapes, described
in [Wallet-serving deployment (single-volume hosts)](deploying-wallet-serving.md),
and are not this topology.

A successful deployment is a canary until the release's mainnet construction,
wallet-build, coherent-restore, capacity, replacement, and independent-client
gates have current evidence. See
[ADR-0035](../adrs/0035-canonical-storage-topologies.md) and the
[testing runbook](testing.md).

## Prerequisites

- A Linux host with Docker Engine 24 or newer and Docker Compose v2.
- A pinned Zinder release checkout at `/opt/zinder`.
- A Zebra or Zakura node on the same Docker host. Zinder attaches to the
  node's external Docker network and reads the node's RPC cookie from a
  dedicated named volume. A [Z3](https://github.com/ZcashFoundation/z3) stack
  provides both for Zebra. For Zakura, see
  [Attach a Zakura node](#attach-a-zakura-node).
- Storage sized for canonical data, the wallet projection, a coherent
  checkpoint, compaction/restore workspace, and chain-growth reserve.
- An HTTP/2-capable reverse proxy for TLS, authentication, rate limits, and
  quotas.
- GitHub CLI 2.94 or newer and Cosign with Sigstore bundle support when
  installing a native release bundle.
- For native binary installation, a GNU/Linux system with glibc 2.34 or newer,
  dynamic `libstdc++.so.6` providing `GLIBCXX_3.4.30`, and either an
  x86-64-v3 CPU or an AArch64 Armv8-A CPU. Debian 12 Bookworm is the certified
  runtime baseline; the downloadable bundles are not musl artifacts.

Do not place the two storage owners on hosts with independent filesystems.
RocksDB secondary replication in this lifecycle is a same-filesystem boundary,
not a network replication protocol.

## Install

### Download native binaries

Choose the archive that matches the host CPU. Preserve prerelease suffixes in
both the tag and filename:

```bash
release=vX.Y.Z
platform=x86_64-v3-unknown-linux-gnu
gh release download "$release" \
  --repo ZcashFoundation/zinder \
  --pattern "zinder-${release#v}-${platform}.tar.gz" \
  --pattern "zinder-${release#v}-${platform}.spdx.json" \
  --pattern SHA256SUMS \
  --pattern SHA256SUMS.sigstore.json
sha256sum --check SHA256SUMS
gh release verify "$release" --repo ZcashFoundation/zinder
gh release verify-asset "$release" \
  "zinder-${release#v}-${platform}.tar.gz" \
  --repo ZcashFoundation/zinder
cosign verify-blob \
  --bundle SHA256SUMS.sigstore.json \
  --certificate-identity \
    "https://github.com/ZcashFoundation/zinder/.github/workflows/release.yml@refs/tags/${release}" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  SHA256SUMS
tar -xzf "zinder-${release#v}-${platform}.tar.gz"
sudo install -m 0755 \
  "zinder-${release#v}-${platform}"/bin/zinder-* \
  /usr/local/bin/
```

Releases before `v0.8.0` were published from `gustavovalverde/zinder`. To
verify one of them, use that repository in the Cosign certificate identity
and the `ghcr.io/gustavovalverde/` image namespace.

Use `aarch64-unknown-linux-gnu` on AArch64. Inspect `BUILD-INFO.json`, the
internal `SHA256SUMS`, and the accompanying SPDX SBOM before installation. The
4 executables still require separate service configuration, storage paths,
control secrets, process supervision, and the ownership order documented below;
downloading the bundle does not create a mixed single-process deployment.

### Deploy with Compose

Clone and pin the intended release:

```bash
sudo git clone https://github.com/ZcashFoundation/zinder.git /opt/zinder
sudo git -C /opt/zinder checkout vX.Y.Z
sudo install -d -o root -g root -m 0755 /etc/zinder
sudo cp /opt/zinder/deploy/.env.mainnet /etc/zinder/env
```

Pick the env file for your node and network:

| Node | Mainnet | Testnet | Regtest |
| --- | --- | --- | --- |
| Zebra through Z3 | `.env.mainnet` | `.env.testnet` | `.env.regtest` |
| Zakura | `.env.zakura-mainnet` | `.env.zakura-testnet` | not provided |

The Compose file has no node address or network default. The env file
supplies every node-specific value:

| Variable | Meaning |
| --- | --- |
| `ZINDER_DEPLOY_NETWORK` | `mainnet`, `testnet`, or `regtest`. It names the Compose project, the Zinder volumes, and the network aliases. |
| `ZINDER_SOURCE_NETWORK_NAME` | The node's external Docker network. |
| `ZINDER_SOURCE_COOKIE_VOLUME_NAME` | The external volume that holds the node's `.cookie`. |
| `ZINDER_NODE__JSON_RPC_ADDR` | The node's JSON-RPC URL on that network. |
| `ZINDER_NODE__INDEXER_GRPC_ADDR` | The node's indexer gRPC URL. Empty selects JSON-RPC mempool polling. |

Compose refuses to start when `ZINDER_NETWORK__NAME` or any of the first four
is unset. The Zebra and Zakura files for one network share
`ZINDER_DEPLOY_NETWORK`, the host ports, and the projector owner, so they
describe the same deployment. Switching the file switches the node and keeps
the existing stores.

Every checked-in file leaves `ZINDER_NODE__INDEXER_GRPC_ADDR` empty, so ingest
polls the node's mempool over JSON-RPC. To stream mempool events from a Z3
Zebra node instead, set `ZEBRA_RPC__INDEXER_LISTEN_ADDR=0.0.0.0:8155` in the
Z3 `.env` file, restart Z3, and set
`ZINDER_NODE__INDEXER_GRPC_ADDR=http://zebra:8155` in `/etc/zinder/env`.

Each file contains a stable, non-secret `ZINDER_PROJECTOR_BUILD_OWNER_HEX`;
assign a different 32-character hexadecimal value to every side-by-side
projector lane.

### Upgrade an existing env file

Earlier releases derived the project, network, and cookie volume names from
`Z3_NETWORK_LOWER` and defaulted the node endpoints to `zebra`. Compose now
ignores `Z3_NETWORK_LOWER` and requires the new variables. Before you start
an upgraded checkout, edit `/etc/zinder/env`:

1. Add `ZINDER_DEPLOY_NETWORK=<network>` next to `Z3_NETWORK_LOWER=<network>`.
   Use the same value: it keeps the `zinder-<network>` project and volume
   names, so the existing stores stay attached.
2. Add `ZINDER_SOURCE_NETWORK_NAME=z3-<network>` and
   `ZINDER_SOURCE_COOKIE_VOLUME_NAME=z3-<network>-cookie`.
3. Check that `ZINDER_NETWORK__NAME` and `ZINDER_NODE__JSON_RPC_ADDR` are set.
4. Set `ZINDER_NODE__INDEXER_GRPC_ADDR` to an empty value unless the node
   serves its indexer listener on the Docker network. The earlier
   `http://zebra:8155` value works only after the Z3 opt-in above.

Comparing the file with the current `deploy/.env.<network>` shows any other
missing value.

Keep the old `Z3_NETWORK_LOWER=<network>` line until the upgrade is final.
An earlier checkout ignores the new variables and falls back to
`zinder-mainnet` without it, so a rollback on a testnet or regtest host would
attach to the wrong project and volumes.

Create the root-owned file-backed control-secret directory before validating
Compose. The ordinary ingest token is mounted into all four runtimes; the
checkpoint token is mounted only into ingest and projector.
The group must be the container service group (the release images use GID
1000), while the files remain owned by root and are not stored in the shared
RocksDB volume.

```bash
sudo install -d -o root -g 1000 -m 0750 /etc/zinder/control-secrets
openssl rand -hex 32 | sudo tee /etc/zinder/control-secrets/ingest.token >/dev/null
openssl rand -hex 32 | sudo tee /etc/zinder/control-secrets/checkpoint.token >/dev/null
sudo chown root:1000 /etc/zinder/control-secrets/*.token
sudo chmod 0440 /etc/zinder/control-secrets/*.token
```

The checked-in network env templates already set
`ZINDER_CONTROL_SECRETS_DIR=/etc/zinder/control-secrets`. If the host uses a
different path, update the copied `/etc/zinder/env` value before resolving
Compose. Query and compatibility mount only `ingest.token`; they cannot read
`checkpoint.token` even though the files share one host directory.

Validate the resolved topology before starting it:

```bash
cd /opt/zinder
docker compose --env-file /etc/zinder/env \
  -f deploy/docker-compose.yml config -q
```

Build and start the four release runtimes:

```bash
docker compose --env-file /etc/zinder/env \
  -f deploy/docker-compose.yml up -d --build
```

The Compose topology gives projector, query, and compatibility containers the
ingest container's network namespace. This keeps `CanonicalControl` and
`IngestControl` on `127.0.0.1:9100` without weakening the non-loopback
bearer-token rule. Both wallet-facing gRPC ports are published on host loopback
only. Readiness is also the dependency gate: expected fresh construction stays
in Docker's `starting` state for the documented three-hour canonical and
two-hour wallet-build hard gates instead of being reported unhealthy after one
minute. Query and compatibility receive five-minute startup grace periods.

### Attach a Zakura node

Zakura's own Compose file does not share a network or a cookie volume with
other projects. Start the node with these settings:

- `ZAKURA_RPC__LISTEN_ADDR=0.0.0.0:8232` on Mainnet, or `0.0.0.0:18232` on
  Testnet.
- An external Docker network shared with Zinder, on which the node is
  reachable as `zakura`. Name it in `ZINDER_SOURCE_NETWORK_NAME`.
- `ZAKURA_RPC__COOKIE_DIR=/var/run/auth`, backed by a dedicated named volume.
  Name it in `ZINDER_SOURCE_COOKIE_VOLUME_NAME`. Stock Zakura keeps the cookie
  in its chain-state volume, which Zinder must not mount.
- An explicit `network.zakura.listen_addr`
  (`ZAKURA_NETWORK__ZAKURA__LISTEN_ADDR`). When it is unset, Zakura's QUIC
  peer listener binds `0.0.0.0:8234`.

Zakura refuses to start a plaintext indexer gRPC listener on a non-loopback
address. The Zakura env files therefore leave
`ZINDER_NODE__INDEXER_GRPC_ADDR` empty, and ingest polls the mempool.

Zebra and Zakura write the cookie as UID 10001 with mode 0600, and replace it
on every start. The Zinder runtimes run as UID 1000 and cannot read it. Run
the cookie-permissions project beside the node with the same env file:

```bash
docker compose --env-file /etc/zinder/env \
  -f deploy/node-cookie-permissions.yml up -d
```

It runs as UID 10001 with no capabilities and no network. Every 5 seconds it
sets the cookie's mode to 0644 in place; it never copies the cookie. It is a
separate Compose project, so the Zinder systemd unit does not stop it. After a
node restart, ingest can find the cookie unreadable for up to one poll
interval. Ingest reports the node as unavailable and retries. Z3 already runs
an equivalent service, so Z3 deployments do not need this project.

### Attach an explorer consumer

When the same host serves Zexplorer or another named `ExplorerQuery` consumer,
merge the explorer overlay:

```bash
docker compose --env-file /etc/zinder/env \
  -f deploy/docker-compose.yml \
  -f deploy/docker-compose.explorer.yml \
  up -d --build
```

The overlay builds `zinder-explorer`, opens a process-owned materialized-view
secondary, and federates `WalletQuery` over the shared loopback namespace. The
runtime does not open a canonical secondary. It publishes gRPC and ops
endpoints on host loopback using `ZINDER_EXPLORER_HOST_PORT` and
`ZINDER_EXPLORER_OPS_HOST_PORT`. Inside the node network, consumers use the
stable `zinder-${ZINDER_DEPLOY_NETWORK}-explorer:9068` alias.

Create two file-backed control secrets outside the shared data volume: the
ordinary `ingest.token`, mounted into all four runtimes, and the separate
`checkpoint.token`, mounted only into ingest and projector. Set
`ZINDER_CONTROL_SECRETS_DIR` to the directory containing both files. Compose
mounts individual files, so query and compatibility receive only
`ingest.token`. `checkpoint.token` authorizes both the loopback
`ProjectorControl` capture request and the method-level canonical checkpoint
capability; reader runtimes must never mount or read it.

The root-owned `state-init` service initializes the shared data volume and the
separate checkpoint-staging volume for UID/GID 1000 before ingest starts.
Checkpoint staging is mounted only into ingest and projector, never into query
or compatibility. Canonical checkpoint requests accept only an opaque candidate
identifier beneath that root; the projector prepares the candidate directory,
ingest creates the fixed `canonical.rocksdb` child through its owner queue, and
cold admission runs off the writer queue. The current capture foundation can add
the exact-fence wallet checkpoint and publish a format-3 `state-bundle.json`
manifest last. It can also create and byte-admit a fixed-layout sealed recovery
directory whose outer manifest is published last and binds every payload file,
both checkpoint identities, and the canonical construction-manifest sidecar.
The configured local archive root is not physical WORM storage. Instance-state
reset, immutable external publication, inactive-lane restore, and the required
10000-block tail proof remain absent, so neither captured nor locally sealed
manifests satisfy the restore production gate.

### Rebuild Explorer views after a manifest change

Each release admits only a materialized-view store whose recorded consumers
match its bundled Explorer views exactly. A release that adds or retires an
Explorer view therefore refuses stores built by earlier releases. For example,
a store that still records the retired
`transparent_address_transaction_history` view fails admission after the
upgrade:

- `zinder-ingest` logs `materialized_view_store_open_failed` with a
  `ConsumerNotDeclared` error. It keeps writing canonical data and serving
  wallet readers without Explorer views.
- `zinder-explorer` exits at startup with the same error.

A store that records only part of the manifest fails with
`ExplorerViewManifestRequiresFreshStore` instead. Neither failure changes the
store, and no Zinder service deletes it.

To rebuild the Explorer views:

1. Record the capabilities `ExplorerQuery.ServerInfo` advertises today. They
   are the coverage the rebuilt store must reach again.
2. Stop `zinder-explorer` and `zinder-ingest`.
3. Move the `materialized-views` directory out of the canonical store path. In
   the Compose topology it is `/var/lib/zinder/canonical/materialized-views` in
   the `data` volume. Keep it until step 6.
4. Start `zinder-ingest`. It creates a store with the current manifest and
   replays retained canonical history, which must be genesis-complete or start
   from the height-0 checkpoint. Views that own a historical backfill, such as
   fee distributions, value-pool flows, and transaction components, fill their
   settled history from separate backfill workers after the live tail joins.
5. Start `zinder-explorer` once ingest reports ready.
6. Delete the moved directory only after `ExplorerQuery.ServerInfo` reports
   zero `lag_blocks` and advertises every capability recorded in step 1. Zero
   lag covers only the shared replay cursor. A backfill-owned view advertises
   its capability once its backfill completes. The transparent-address ranking
   stays withheld until its canonical snapshot is published, and no release
   process publishes one, so a deployment that served
   `explorer.transparent_address.ranking_v1` keeps the old directory until
   that capability returns.

### Stream mempool events over indexer TLS

Ingest streams mempool events and chain-tip wakeups from the node's indexer
gRPC listener when `ZINDER_NODE__INDEXER_GRPC_ADDR` is set, and polls the node
over JSON-RPC otherwise. A node that serves its indexer listener over TLS
needs the indexer TLS overlay, `deploy/docker-compose.indexer-tls.yml`.
Zakura requires mutual TLS on any indexer listener that is not on loopback, so
a Zakura node in another container streams to Zinder only this way.

Configure the node's listener first. For Zakura, set:

- `ZAKURA_RPC__INDEXER_LISTEN_ADDR=0.0.0.0:8230`;
- `ZAKURA_RPC__INDEXER_TLS__CERT_FILE` and `ZAKURA_RPC__INDEXER_TLS__KEY_FILE`
  to the node's certificate and key. The certificate must name the host that
  Zinder dials, such as `zakura` on the shared Docker network;
- `ZAKURA_RPC__INDEXER_TLS__CLIENT_CA_FILE` to the CA that signs Zinder's
  client certificate.

Zinder needs three PEM files:

| File | Contents |
| --- | --- |
| `ca.pem` | The CA certificates that verify the node's indexer certificate. |
| `client.pem` | Zinder's client certificate chain. |
| `client-key.pem` | The private key for that certificate. |

Install them in a root-owned directory that the container service group can
read, outside the shared RocksDB volume:

```bash
sudo install -d -o root -g 1000 -m 0750 /etc/zinder/indexer-tls
sudo install -o root -g 1000 -m 0440 node-indexer-ca.pem /etc/zinder/indexer-tls/ca.pem
sudo install -o root -g 1000 -m 0440 zinder-client.pem /etc/zinder/indexer-tls/client.pem
sudo install -o root -g 1000 -m 0440 zinder-client-key.pem /etc/zinder/indexer-tls/client-key.pem
```

Then set the indexer URL and the directory in `/etc/zinder/env`:

```bash
ZINDER_NODE__INDEXER_GRPC_ADDR=https://zakura:8230
ZINDER_INDEXER_TLS_DIR=/etc/zinder/indexer-tls
```

When the node's certificate names a different host than the URL, also set
`ZINDER_NODE__INDEXER_TLS__SERVER_NAME` to the name it carries.

Validate and start the topology with the overlay:

```bash
docker compose --env-file /etc/zinder/env \
  -f deploy/docker-compose.yml \
  -f deploy/docker-compose.indexer-tls.yml \
  config -q
docker compose --env-file /etc/zinder/env \
  -f deploy/docker-compose.yml \
  -f deploy/docker-compose.indexer-tls.yml \
  up -d --build
```

The overlay mounts the directory read-only into `zinder-ingest` only, because
no other runtime reads the indexer, and points `[node.indexer_tls]` at the
three files. Compose refuses to resolve the overlay without an indexer URL or
`ZINDER_INDEXER_TLS_DIR`. Ingest checks the files at startup and exits with a
configuration error when one is missing or holds no certificate or key, or
when the indexer URL is not `https://`.

Ingest reads the files again each time it reconnects to the indexer. The
overlay mounts the directory rather than individual files, so a replaced file,
including one renamed into place, takes effect on the next reconnect without a
restart. A running stream keeps the session it opened with the old material.

## Verify the ownership chain

Use the host ports from the selected env file. Mainnet uses 9105, 9110, 9102,
9106, 9107, and 9067; testnet adds 10000; regtest adds 20000.

```bash
curl -fsS http://127.0.0.1:9105/healthz
curl -fsS http://127.0.0.1:9105/readyz
curl -fsS http://127.0.0.1:9110/readyz
curl -fsS http://127.0.0.1:9106/readyz
curl -fsS http://127.0.0.1:9107/readyz

grpcurl -plaintext -d '{}' 127.0.0.1:9102 \
  zinder.v1.wallet.WalletQuery/ServerInfo
grpcurl -plaintext -d '{}' 127.0.0.1:9067 \
  cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLightdInfo
```

Interpret the probes in order:

1. Ingest is ready only when canonical following is within the configured lag
   boundary and the current mempool generation has completed hydration.
2. Projector is ready only when its admitted wallet store has reached an
   authenticated canonical event fence.
3. Native query is ready only when its inactive canonical and wallet
   secondaries converge to one exact network, epoch, event sequence, visible
   tip, settled tip, and digest before publication.
4. Compatibility is ready only when its inactive canonical and wallet
   secondaries converge to one exact network, epoch, event sequence, visible
   tip, settled tip, and digest before publication.

A healthy process with a non-ready dependency must continue returning 503. Do
not route wallet traffic based on `/healthz`.

## Put TLS in front of compatibility

The published host ports remain loopback-only. Expose only the compatibility
route through an operator-owned trusted-TLS edge; do not publish the native
query, compatibility plaintext listener, ops, control, observability, or
storage ports. The [Trusted TLS and ZODL compatibility admission
runbook](zodl-trusted-tls-certification.md) owns the parameterized Caddy
template, certificate and DNS ownership, `/readyz` routing gate, external
unreachability proof, endpoint attribution, and rollback evidence. An already
accepted compatibility request may drain against its immutable canonical/wallet
generation, but new traffic must stop when compatibility `/readyz` returns 503.

## Install systemd supervision

The repository unit expects the checkout at `/opt/zinder` and the selected
env file at `/etc/zinder/env`:

```bash
sudo cp /opt/zinder/deploy/systemd/zinder.service \
  /etc/systemd/system/zinder.service
sudo systemctl daemon-reload
sudo systemctl enable --now zinder
sudo journalctl -u zinder.service -f
```

The unit runs only `deploy/docker-compose.yml`. A deployment that merges an
overlay, such as the explorer or indexer TLS overlay, must name it in the
unit's commands too, or the next start drops it. Add it with a drop-in from
`sudo systemctl edit zinder`, for example for the indexer TLS overlay:

```ini
[Service]
ExecStartPre=
ExecStartPre=-/usr/bin/docker compose --env-file /etc/zinder/env -f docker-compose.yml -f docker-compose.indexer-tls.yml down --remove-orphans
ExecStart=
ExecStart=/usr/bin/docker compose --env-file /etc/zinder/env -f docker-compose.yml -f docker-compose.indexer-tls.yml up
ExecStop=
ExecStop=/usr/bin/docker compose --env-file /etc/zinder/env -f docker-compose.yml -f docker-compose.indexer-tls.yml down
```

## Side-by-side wallet rebuild and cutover

Never rebuild a wallet store in place while readers serve it. Provision lane B
with all of the following distinct from lane A:

- `wallet.path`;
- projector `storage.canonical_secondary_path`;
- native-query canonical and wallet secondary roots;
- compatibility canonical and wallet secondary roots;
- projector build-owner identity;
- native-query gRPC and ops ports;
- compatibility gRPC and ops ports.

Both projector lanes may read the same canonical primary and hold independent
retention leases. Start lane B, wait for its projector, native query, and
compatibility `/readyz` endpoints, run parity and client probes against lane B,
then move the reverse-proxy upstream atomically. Keep lane A running until
in-flight requests drain. Only then stop lane A and reclaim its wallet and
secondary paths.

If lane B reports an expired event cursor or a source-fence mismatch, leave
lane A serving and rebuild lane B from a newly captured canonical fence. Do not
copy READY markers, rename a live RocksDB directory, or reuse a drained
secondary path before every request holding its generation has released it.

## Restart and rollback

Restart one owner at a time in dependency order:

```bash
docker compose --env-file /etc/zinder/env \
  -f /opt/zinder/deploy/docker-compose.yml restart zinder-ingest
docker compose --env-file /etc/zinder/env \
  -f /opt/zinder/deploy/docker-compose.yml restart zinder-projector
docker compose --env-file /etc/zinder/env \
  -f /opt/zinder/deploy/docker-compose.yml restart zinder-query
docker compose --env-file /etc/zinder/env \
  -f /opt/zinder/deploy/docker-compose.yml restart zinder-compat-lightwalletd
```

After each restart, require the complete readiness chain again. For a release
rollback, route traffic to a previously certified side-by-side lane. Do not
start a binary on existing canonical or wallet paths unless that release
explicitly declares their physical layouts compatible.

## Production admission checklist

Before routing mainnet traffic, attach evidence for every item:

- fresh canonical construction within the 3-hour hard gate;
- wallet construction within the 2-hour hard gate and complete wallet-ready
  lifecycle within 4 hours;
- verified coherent canonical-plus-wallet restore and 10000-block tail within
  15 minutes;
- at most 2 blocks of canonical lag and at most 2 canonical epochs of wallet
  lag under sustained following;
- maximum-depth replacement, restart, and secondary-generation race tests;
- storage capacity including checkpoint, compaction, restore, and growth
  reserve;
- fresh create, known-seed restore, non-empty transparent funds, send,
  mempool, confirmation, restart, projection lag, and reorg on the pinned
  independent client.

A local deployment is suitable for validation and controlled canaries. It is
not a production certification claim without release-specific evidence for
every gate above.

## References

- [Public environment-variable contract](../architecture/public-interfaces.md#environment-variable-mapping)
- [Initial sync](initial-sync.md)
- [Testing](testing.md)
- [ADR-0035](../adrs/0035-canonical-storage-topologies.md)
- [ADR-0036](../adrs/0036-gnu-linux-binary-release-bundles.md)
- [ADR-0037](../adrs/0037-release-artifact-provenance-and-sboms.md)
