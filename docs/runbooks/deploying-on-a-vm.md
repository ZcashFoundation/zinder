# Deploying the wallet service on one VM

This runbook operates the supported `rocksdb-single-host` wallet-serving
topology. Four independent processes share one host filesystem:
`zinder-ingest` owns canonical storage, `zinder-projector` owns the wallet
projection, and `zinder-query` plus `zinder-compat-lightwalletd` independently
serve immutable canonical and wallet secondary pairs. The optional explorer
overlay builds a local `zinder-explorer` image for named explorer consumers;
it is not part of the published release-image catalog. Mixed single-container
images are not release paths.

A successful deployment is a canary until the release's mainnet construction,
wallet-build, coherent-restore, capacity, replacement, and independent-client
gates have current evidence. See
[ADR-0035](../adrs/0035-canonical-storage-topologies.md) and the
[testing runbook](testing.md).

## Prerequisites

- A Linux host with Docker Engine 24 or newer and Docker Compose v2.
- A pinned Zinder release checkout at `/opt/zinder`.
- A running [Z3](https://github.com/ZcashFoundation/z3) stack on the same
  Docker host. Zinder attaches to its network and cookie volume.
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
  --repo gustavovalverde/zinder \
  --pattern "zinder-${release#v}-${platform}.tar.gz" \
  --pattern "zinder-${release#v}-${platform}.spdx.json" \
  --pattern SHA256SUMS \
  --pattern SHA256SUMS.sigstore.json
sha256sum --check SHA256SUMS
gh release verify "$release" --repo gustavovalverde/zinder
gh release verify-asset "$release" \
  "zinder-${release#v}-${platform}.tar.gz" \
  --repo gustavovalverde/zinder
cosign verify-blob \
  --bundle SHA256SUMS.sigstore.json \
  --certificate-identity \
    "https://github.com/gustavovalverde/zinder/.github/workflows/release.yml@refs/tags/${release}" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  SHA256SUMS
tar -xzf "zinder-${release#v}-${platform}.tar.gz"
sudo install -m 0755 \
  "zinder-${release#v}-${platform}"/bin/zinder-* \
  /usr/local/bin/
```

Use `aarch64-unknown-linux-gnu` on AArch64. Inspect `BUILD-INFO.json`, the
internal `SHA256SUMS`, and the accompanying SPDX SBOM before installation. The
4 executables still require separate service configuration, storage paths,
control secrets, process supervision, and the ownership order documented below;
downloading the bundle does not create a mixed single-process deployment.

### Deploy with Compose

Clone and pin the intended release:

```bash
sudo git clone https://github.com/gustavovalverde/zinder.git /opt/zinder
sudo git -C /opt/zinder checkout vX.Y.Z
sudo install -d -o root -g root -m 0755 /etc/zinder
sudo cp /opt/zinder/deploy/.env.mainnet /etc/zinder/env
```

Use `.env.testnet` or `.env.regtest` for another network. Each file contains
a stable, non-secret `ZINDER_PROJECTOR_BUILD_OWNER_HEX`; assign a different
32-character hexadecimal value to every side-by-side projector lane.

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
`ZINDER_EXPLORER_OPS_HOST_PORT`. Inside the Z3 network, consumers use the stable
`zinder-${Z3_NETWORK_LOWER}-explorer:9068` alias.

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
