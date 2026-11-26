# Deploying Zinder

This page takes an operator from an empty host to a running, verified Zinder deployment that serves wallets from your own Zcash node. It covers choosing a profile, choosing how to run it, host sizing, the canonical port matrix, and a Testnet Docker Compose quickstart.

Each deployment runs on one host (`rocksdb` topology) and builds its stores from your node. Pin an exact release tag, and read each release's notes before upgrading.

<a id="choose-a-profile"></a>

## Step 1: Choose a profile

Select the profile that matches what your clients read. All profiles build complete canonical history from genesis unless configured with a checkpoint override.

| Profile | Processes run | Mainnet disk | First sync (8 writer cores) | Transaction bodies & transparent reads |
| --- | --- | --- | --- | --- |
| **`wallet`** *(recommended production default)* | `zinder-ingest`, `zinder-projector`, `zinder-history-publisher`, `zinder-query`, `zinder-compat-lightwalletd` | ~123 GB | ~76 min | Local transparent index (`wallet` store); verified mined transaction bodies forwarded from your primary `[node]` and cached. |
| **`light-wallet`** | `zinder-ingest`, `zinder-compat-lightwalletd` (plus optional `zinder-history-publisher`) | ~72 GB | ~49–60 min | No projector or `zinder-query`. Forwards transparent address reads and omitted transaction bodies to one archive node with transparent address indexes. |
| **`wallet-archive`** | Same as `wallet` + all raw block & tx blobs | >392 GB (`wallet` + raw blocks + ~270 GB / ~251.1 GiB raw tx blobs) | ~76+ min | Sets `raw_blob_policy = "all"` for self-hosted raw transaction bodies and `FullBlocksInRange`. Omit `deployment.profile` and set `raw_blob_policy = "transactions"` (+~270 GB / ~251.1 GiB, ~392 GB total) if you only want raw transaction bodies. |

To add the optional **Explorer** (`zinder-explorer` + `ingest.explorer_views = true` via `deploy/docker-compose.explorer.yml`), omit `deployment.profile` and configure `storage`, `ingest`, `wallet`, and `compat` explicitly (`zinder-ingest` rejects `ingest.explorer_views = true` when any `deployment.profile` is set).

> [!WARNING]
> **Check `raw_blob_policy` in shipped configs before your first sync.** The shipped `deploy/config/*.toml` files (`ingest.toml`, `projector.toml`, `query.toml`, `compat-lightwalletd.toml`, and `deploy/config/wallet-serving/ingest.toml`; `history-publisher.toml` also sets it for older binaries, though `publish-shielded-history` ignores it) still set `raw_blob_policy = "transactions"`, which adds **+251.1 GiB** on Mainnet. Because `ingest.toml` also sets `coverage = "wallet-serving"` (which rejects `raw_blob_policy = "none"` without a profile) and `deployment.profile` rejects conflicting explicit `raw_blob_policy` / `coverage` / `serving` keys, passing `ZINDER_DEPLOYMENT__PROFILE=wallet` or `ZINDER_STORAGE__RAW_BLOB_POLICY=none` over the unmodified shipped Compose TOMLs fails startup. To run the `wallet` profile without raw blobs in Compose or standalone binaries, set `deployment.profile = "wallet"` in your TOML files and remove `storage.raw_blob_policy`, `ingest.run_overrides.coverage`, and `compat.serving` before initial sync (note: `zinder-wallet-serving` currently strips `ZINDER_DEPLOYMENT__*` and retains `transactions` blobs unless custom configs are mounted). Changing raw-blob retention on an existing store requires a rebuild.

See [Deployment profiles](../reference/configuration.md#deployment-profiles) for setting conflicts and [ADR-0023](../adrs/0023-deployment-profiles.md) for the design contract.

<a id="choose-a-deployment"></a>

## Step 2: Choose how to run it

| Option | Pick it when | Guide |
| --- | --- | --- |
| **Docker Compose + systemd on a Linux VM** *(recommended production)* | You run a long-lived multi-process service on a VM you manage, or alongside a Z3/Zakura container stack. | [Testnet quickstart](#quickstart-with-docker-compose) below; [Deploying on a VM](deploying-on-a-vm.md) for systemd and production hardening |
| **Single-volume container (`zinder-wallet-serving`)** | Your PaaS attaches one persistent volume to one container (such as Railway). | [Deploying the single-volume container](deploying-single-volume.md) |
| **Standalone binaries** | You manage systemd units and OS packages directly without Docker. | [Run the release binaries without Docker](deploying-on-a-vm.md#run-the-release-binaries-without-docker) |
| **Public Foundation endpoints** | You want to evaluate or integrate a client without operating an indexer. | [Integration surfaces](../reference/integration-surfaces.md#endpoints) |

<a id="what-you-need"></a>

## Step 3: Size your host

You need an archive Zebra or Zakura node on the same network. Ingest reads every historical block and refuses a pruned node at startup. The certified node versions are **Zebra 7.1.0** and **Zakura 1.8.6**. For a Mainnet first sync, use a Zakura node that was not built with fast sync, because fast sync omits per-height commitment trees below its last checkpoint. Give the node its own disk on top of the Zinder storage below.

### Private or standard Mainnet VM

- **CPU:** 8+ vCPUs (`x86-64-v3` or `AArch64`). Wallet accumulators select AVX-512F/VL at run time when available and AVX2 otherwise; both produce identical store bytes. Hosts with fewer than 8 logical CPUs take longer to sync; see [First sync on fewer than 8 logical CPUs](bulk-catchup-resource-tuning.md#first-sync-on-fewer-than-8-logical-cpus).
- **RAM:** 32 GiB RAM. Keep the shipped `4g` `ZINDER_COMPAT_MEMORY_LIMIT` as an OOM guardrail. The shipped Mainnet Compose env files set container ceilings of `14g` (ingest), `10g` (projector), `4g` (query), `4g` (history-publisher), `4g` (compat), and `4g` (optional explorer). Give single-volume containers at least 8 GB.
- **Disk:** Fast local SSD/NVMe formatted **`ext4 -O verity`** (so `zinder-history-publisher` can protect settled segments with Linux fs-verity). Reserve **260+ GB free** for `wallet` (~123 GB steady-state total, which already includes the ~20.9 GB / ~19.5 GiB signed `ZSHRNG01` catalog across 877 segments at Mainnet height 3,593,347 and growing, plus ~130 GB temporary sort spill headroom during initial wallet construction), or **550+ GB free** when retaining raw transaction blobs (+251.1 GiB; `wallet-archive` with `raw_blob_policy = "all"` needs additional space for raw blocks).

### Public high-concurrency Mainnet endpoint

- **CPU:** 8–16+ vCPUs. On Broadwell+/Zen+ `x86-64` (BMI2 + ADX) or `AArch64`, build images with `--build-arg ZINDER_PASTA_ASM=1` (or `ZINDER_PASTA_ASM=1` with Compose, or `--features pasta_curves/asm` with Cargo) for the faster Pasta field-arithmetic backend (~2–7% lower CPU and elapsed time on measured 2,000-block windows; the binary checks ADX at startup).
- **RAM:** **64+ GiB RAM** and raise `ZINDER_COMPAT_MEMORY_LIMIT=32g` (or higher). Under cgroup v2, the `ZSHRNG01` signed-history catalog page cache (~20–21 GiB on Mainnet and growing) counts against the cgroup that first reads each page (`zinder-compat-lightwalletd` or `zinder-history-publisher`). Keeping compat's ceiling above the catalog plus working set avoids 5–7x page-cache thrashing and `wait_timeout` refusals under concurrent range syncs. Leave `ZINDER_QUERY_MEMORY_LIMIT` and `ZINDER_HISTORY_PUBLISHER_MEMORY_LIMIT` at `4g` (`zinder-query` uses `O_DIRECT` RocksDB and does not mount the catalog); validate cache residency with publisher activity included.
- **Node feed:** Set `ZINDER_NODE__INDEXER_GRPC_ADDR` and set `source = "zebra-indexer-grpc"` in `deploy/config/ingest.toml` (`deploy/docker-compose.yml` does not forward `ZINDER_INGEST__SOURCE` from the environment; for standalone binaries, pass `ZINDER_INGEST__SOURCE=zebra-indexer-grpc`) to stream mempool events and fetch blocks as binary protobufs instead of polling the full mempool over JSON-RPC on every block.
- **Edge proxy:** Put a TLS proxy with per-client connection, request, and stream limits in front of the listeners ([Exposing over TLS](exposing-over-tls.md)).

### Testnet and Regtest baselines

| Network | Compose memory ceilings (ingest / projector / query / publisher / compat) | First sync notes |
| --- | --- | --- |
| **Testnet** | `8g` / `6g` / `2g` / `2g` / `2g` | Fits the default construction deadlines (3 h canonical, 2 h wallet). |
| **Regtest** | `2g` / `2g` / `1g` / `1g` / `1g` | Minutes from genesis. |

<a id="what-you-deploy"></a>

## Processes and canonical ports

`zinder-ingest` is the only writer of the canonical store (and Explorer views when enabled), and `zinder-projector` is the only writer of the wallet store. `zinder-query`, `zinder-compat-lightwalletd`, and optional `zinder-explorer` open read-only RocksDB secondaries on the same filesystem. See the [architecture overview](../architecture/overview.md) for data flow.

| Process | Role / serves | Serving port | Ops port |
| --- | --- | --- | --- |
| `zinder-ingest` | Reads the node, writes the canonical store and optional Explorer views. | none | 9105 |
| `zinder-projector` | Builds and updates the wallet store. | none | 9110 |
| `zinder-history-publisher` | Writes Ed25519-signed settled shielded ranges (`ZSHRNG01`) for lightwalletd. | none | none |
| `zinder-query` | Native `WalletQuery` gRPC | 9102 | 9106 |
| `zinder-compat-lightwalletd` | lightwalletd `CompactTxStreamer` gRPC (and optional gRPC-Web) | 9067 | 9107 |
| `zinder-explorer` *(optional)* | `ExplorerQuery` gRPC | 9068 | 9069 |

Each ops port serves `/healthz`, `/readyz`, and `/metrics`. The serving gRPC ports also serve `grpc.health.v1.Health` ([gRPC health](../reference/readiness.md#grpc-health)).

These are the **Mainnet** ports. The shipped Compose env files add `+10000` on Testnet (for example, `19102` for `zinder-query`) and `+20000` on Regtest, and bind every published port to host loopback (`127.0.0.1`) only.

Compose also exposes these internal/observability ports:

| Port | Use | Host publication |
| --- | --- | --- |
| 9100 | Ingest control API (projector and readers) | Loopback inside the shared container network only (same on all networks). |
| 9101 | Projector control API | Loopback inside the shared container network only (same on all networks). |
| 9095 | Prometheus | Loopback (`19095` Testnet, `29095` Regtest). |
| 3002 | Grafana (`--profile observability`) | Loopback (`13002` Testnet, `23002` Regtest). |

<a id="quickstart-with-docker-compose"></a>

## Testnet Docker Compose quickstart

This quickstart runs Zinder on Testnet beside a Zebra node from the [Z3 stack](https://github.com/ZcashFoundation/z3) using Docker Engine, the Compose v2 plugin, `git`, `openssl`, `curl`, and `grpcurl`. For Zakura, use `deploy/.env.zakura-testnet` and follow [Attach a Zakura node](deploying-on-a-vm.md#attach-a-zakura-node) for step 1.

1. Start a Zebra Testnet node with Z3 and wait until it is synced (this creates the `z3-testnet` Docker network and `z3-testnet-cookie` volume):

   ```bash
   Z3_NETWORK=testnet docker compose up -d
   ```

2. Check out a Zinder release tag:

   ```bash
   git clone https://github.com/ZcashFoundation/zinder.git
   cd zinder
   git checkout vX.Y.Z
   ```

3. Create the two control tokens and the shielded-history publisher key generation on the host:

   ```bash
   sudo install -d -o root -g 10001 -m 0750 /etc/zinder/control-secrets
   openssl rand -hex 32 | sudo tee /etc/zinder/control-secrets/ingest.token >/dev/null
   openssl rand -hex 32 | sudo tee /etc/zinder/control-secrets/checkpoint.token >/dev/null
   sudo chown root:10001 /etc/zinder/control-secrets/*.token
   sudo chmod 0440 /etc/zinder/control-secrets/*.token
   sudo install -d -m 0700 /srv/zinder-history/testnet
   sudo scripts/prepare-shielded-history.sh /srv/zinder-history/testnet/key-1
   ```

4. Pull the signed release images for your tag and start Zinder (keep `--no-build` so Compose does not compile local images under the release tag; add `--profile observability` to also start Grafana on `13002`):

   ```bash
   export ZINDER_VERSION=vX.Y.Z
   docker compose --env-file deploy/.env.testnet \
     -f deploy/docker-compose.yml pull
   docker compose --env-file deploy/.env.testnet \
     -f deploy/docker-compose.yml up -d --no-build
   ```

   Compose starts `zinder-ingest`, then `zinder-projector` and `zinder-history-publisher` once ingest is healthy, and then `zinder-query` and `zinder-compat-lightwalletd` once the projector is healthy.

5. Check `/readyz` on each runtime (each prints `{"status":"ready",...}` once serving, or HTTP 503 while syncing):

   ```bash
   curl -fsS http://127.0.0.1:19105/readyz   # ingest
   curl -fsS http://127.0.0.1:19110/readyz   # projector
   curl -fsS http://127.0.0.1:19106/readyz   # native query
   curl -fsS http://127.0.0.1:19107/readyz   # lightwalletd compatibility
   ```

6. Verify one native RPC and one lightwalletd RPC:

   ```bash
   grpcurl -plaintext -d '{}' 127.0.0.1:19102 \
     zinder.v1.wallet.WalletQuery/ServerInfo
   grpcurl -plaintext -d '{}' 127.0.0.1:19067 \
     cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLightdInfo
   ```

   `ServerInfo` reports `network: "zcash-testnet"` and `zinder-query`; `GetLightdInfo` reports `chainName: "test"` and a `blockHeight` near your node's tip.

<a id="build-from-source"></a>

### Build from source

To run an unreleased checkout instead of published images, leave `ZINDER_VERSION` unset (optionally export `ZINDER_PASTA_ASM=1` on supported CPUs) and run:

```bash
docker compose --env-file deploy/.env.testnet \
  -f deploy/docker-compose.yml up -d --build
```

<a id="before-you-expose-it"></a>

## Before you expose it

Zinder listeners serve plaintext gRPC. Put TLS termination, authentication, and per-client rate/connection limits in front of them using [Exposing over TLS](exposing-over-tls.md). Keep operational, control, and observability ports private as listed in the [security boundary](../architecture/service-operations.md#security-boundary).

## What happens next

- Do not restart `zinder-ingest` or `zinder-projector` while they are constructing their stores; a restart discards the unfinished build and starts over.
- Follow construction progress and phases in [Initial sync](initial-sync.md) and `/readyz` causes in [Readiness](../reference/readiness.md).
- For production VM setup with systemd, failover nodes, indexer mTLS, signed-history key rotation, or Explorer, continue to [Deploying on a VM](deploying-on-a-vm.md).
