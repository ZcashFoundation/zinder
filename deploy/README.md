# Deploying Zinder

Zinder ships as a single non-root distroless OCI image (`ghcr.io/zcashfoundation/zinder`, `USER 10001:10001`) built from [Dockerfile](Dockerfile) (`--target zinder`) and configured purely through CLI flags and `ZINDER_*` environment variables (no TOML config files are baked into the image or mounted by Compose).

## Presets and modes

Pass `--preset <preset>` (`light-wallet`, `wallet` default, or `explorer`) or set `ZINDER_DEPLOYMENT__PROFILE`:

| Preset | Roles started | Ports |
| --- | --- | --- |
| `light-wallet` | `ingest`, `compat-lightwalletd` | `9067` (compat), `9105`/`9108` (ops) |
| `wallet` | `ingest`, `projector`, `query`, `compat-lightwalletd` | `9067` (compat), `9102` (query), `9105`/`9108` (ops) |
| `explorer` | `wallet` roles + `explorer`, `compat-cipherscan` | `9067`, `9068` (explorer), `9070` (cipherscan), `9102`, `9105`/`9108` |

Single-container deployments run `zinder serve --preset <preset>` (writer + readers supervised together). Two-container Compose deployments split writer lock ownership (`zinder index`) from readers (`zinder serve --readers-only`) over one shared `data` volume.

## Personal-node Compose

[personal/compose.yaml](personal/compose.yaml) runs a bundled archive Zebra node
alongside `zinder serve` with `ZINDER_DEPLOYMENT__PROFILE=light-wallet`. It
limits Zinder to 8 GiB by default, gives Zinder one persistent volume, and
publishes wallet and ops ports on host loopback. Download
`zinder-personal-compose.yaml` from the release for a signed manifest with both
images pinned by digest. Follow the
[personal-node runbook](../docs/runbooks/deploying.md#personal-node-compose)
to size, start, verify, and stop the stack.

Releases also provide [native archives](../docs/adrs/0022-release-artifacts.md#decision)
for Apple Silicon, ARM64 Linux, optimized x86-64 Linux, and baseline x86-64 Linux.
Use the baseline archive for Linux CPUs without AVX2 or ADX.

## Docker Compose

1. Create the required host control tokens in `${DEPLOY_CONTROL_SECRETS_DIR:-/etc/zinder/control-secrets}` (`ingest.token` and `checkpoint.token`).
2. Start the stack with the `deploy.env` file for your network and upstream node (which selects its matching `zinder.env` runtime file via `DEPLOY_ZINDER_ENV`):

```bash
docker compose --env-file deploy/profiles/mainnet/deploy.env -f deploy/docker-compose.yml up -d
```

Optional profiles:

- Observability (`zinder-prometheus` and `zinder-grafana`): add `--profile observability`.

Stop or restart the stack with the same `--env-file` and `-f` arguments. Check readiness via `curl -sf http://127.0.0.1:9108/readyz` (or `:19108` on testnet, `:29108` on regtest).

## Tor onion service

The [Tor overlay](docker-compose.tor.yml) adds a Tor v3 sidecar on the shared Zinder loopback. Follow [Serve wallets over Tor](../docs/runbooks/tor-onion-service.md) to configure access and verify the wallet endpoints.

## Kubernetes

[kubernetes/statefulset.yaml](kubernetes/statefulset.yaml) includes a `zinder-config` `ConfigMap` (`ZINDER_DEPLOYMENT__PROFILE=wallet`) and runs `zinder serve` in a single non-root `StatefulSet` (`UID 10001`, `fsGroup: 10001`, read-only root filesystem, ReadWriteOnce PVC at `/var/lib/zinder`, `startupProbe` and `livenessProbe` on `/healthz`, `readinessProbe` on `/readyz`).

## Systemd

The GNU/Linux archives include the [native units and loopback environment
file](systemd/native/). On a host with systemd 254 or later, install them with
the archive's `bin/zinder` to run the wallet services directly on the host,
with optional Explorer and Cipherscan services. Follow the
[native systemd runbook](../docs/runbooks/deploying-on-a-vm.md#run-the-release-binary-without-docker).

[systemd/zinder.service](systemd/zinder.service) wraps the Compose topology on
Linux hosts using `/etc/zinder/deploy.env`. Set
`DEPLOY_ZINDER_ENV=/etc/zinder/zinder.env` for the shared runtime settings and
optionally `DEPLOY_INDEX_ENV=/etc/zinder/index.env` for writer-only settings.
Add `COMPOSE_PROFILES=observability` to `/etc/zinder/deploy.env` to include
Prometheus and Grafana.

## TLS reverse proxy

See [compat-lightwalletd.Caddyfile](compat-lightwalletd.Caddyfile) and [Exposing over TLS](../docs/runbooks/exposing-over-tls.md) for terminating TLS with Caddy in front of loopback gRPC listeners.

## Benchmark and test harnesses

Maintainer-only storage benchmarks, RocksDB lifecycle tests, live test topologies, and local observability stacks live under [bench/](bench/). See [storage-benchmark.md](storage-benchmark.md), [rocksdb-storage-lifecycle.md](rocksdb-storage-lifecycle.md), and the [Stateless-Topology Testing Ground](bench/postgres-testing-ground/README.md).
