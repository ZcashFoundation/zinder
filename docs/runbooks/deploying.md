# Deploying Zinder

This page takes an operator from an empty host to a running, verified Zinder deployment that serves wallets from your own Zcash node. It covers choosing a preset, choosing a runtime target (VM Compose, single-volume PaaS, or Kubernetes `StatefulSet`), host sizing, the canonical port matrix, and a zero-TOML Testnet Docker Compose quickstart.

Each supported deployment runs against persistent storage on one host (`rocksdb` topology, including a single-replica Kubernetes `StatefulSet` on `ReadWriteOnce` storage) and builds its stores from your node. Pin an exact release tag, and read each release's notes before upgrading.

<a id="choose-a-profile"></a>

## Step 1: Choose a preset

Select the preset (`--preset <light-wallet|wallet|explorer>` on `zinder serve` / `zinder index`, or `ZINDER_PRESET` in Compose / `ZINDER_DEPLOYMENT__PROFILE` in env) that matches what your clients read. All presets build complete canonical history from genesis unless configured with a checkpoint override.

| Preset (`deployment.profile`) | Roles run | Mainnet disk | First sync (8 writer cores) | Transaction bodies & transparent reads |
| --- | --- | --- | --- | --- |
| **`wallet`** *(recommended production default)* | `zinder-ingest`, `zinder-projector`, `zinder-query`, `zinder-compat-lightwalletd` | ~102 GB | ~76 min | Local transparent index (`wallet` store); verified mined transaction bodies forwarded from your primary `[node]` and cached. |
| **`light-wallet`** (`lightwalletd`) | `zinder-ingest`, `zinder-compat-lightwalletd` | ~51 GB | ~49–60 min | No projector or `zinder-query`. Forwards transparent address reads and omitted transaction bodies to one archive node with transparent address indexes. |
| **`explorer`** | Everything in `wallet`, plus `ingest.explorer_views = true`, `zinder-explorer`, and `zinder-compat-cipherscan` | Additional view disk & build time | ~76+ min + view backfill | Canonical-only displaced-block RPCs (`DisplacedBlockHistory`, `DisplacedBlockDetail`) read `RocksDbCanonicalSecondary` once canonical ingest is READY; `BlockDetail`, block-production, `BlockTransactions`, `CommitmentRootSearch`, fee/flow/ranking analytics, and Cipherscan routes additionally require `explorer_views` catch-up/backfill coverage and/or `WalletQuery`. |
| **Add `--full-blocks`** (`storage.full_blocks = true`) | Combinable with any preset above | Adds single-copy full consensus block storage (`ZSBLKB01` + `live_tail_raw_block`) | Same + raw block I/O | Sets `raw_blob_policy = "all"` to retain full serialized blocks and indexed transaction slices locally (`FullBlocksInRange` + local tx bodies). |

Canonical Schema `v2` removes `storage.raw_blob_policy = "transactions"` and refuses Schema `v1` stores. To retain local raw transaction and block bytes, enable `--full-blocks` (`storage.full_blocks = true` / `raw_blob_policy = "all"`) and build on fresh storage. See [Deployment profiles](../reference/configuration.md#deployment-profiles) for setting conflicts and [ADR-0023](../adrs/0023-deployment-profiles.md) for the design contract.

<a id="choose-a-deployment"></a>

## Step 2: Choose how to run it

| Option | Status | Pick it when | Guide |
| --- | --- | --- | --- |
| **Docker Compose + systemd on a Linux VM** *(recommended production)* | Supported | You run a long-lived two-container deployment (`zinder-index` writer + `zinder-serve --readers-only`) on a VM you manage, or alongside a Z3/Zakura stack. | [Testnet quickstart](#quickstart-with-docker-compose) below; [Deploying on a VM](deploying-on-a-vm.md) and [deploy/README.md](../../deploy/README.md) |
| **Personal-node Compose** | Supported | You want a private light-wallet endpoint and a bundled Zebra node. | [Personal-node Compose](#personal-node-compose) |
| **Single-volume container (`ghcr.io/zcashfoundation/zinder`)** | Supported | Your PaaS attaches one persistent volume to one container (`zinder serve`), such as Railway. | [Deploying the single-volume container](deploying-single-volume.md) |
| **Kubernetes (`StatefulSet`)** | Supported *(Single-host RWO)* | You run single-replica pods on a cluster with a `ReadWriteOnce` `PersistentVolumeClaim` (`deploy/kubernetes/statefulset.yaml`, `fsGroup: 10001`). | [Kubernetes StatefulSet](deploying-single-volume.md#kubernetes-statefulset) |
| **Standalone `zinder` binary** | Supported | You run Zinder natively on Linux or Apple Silicon macOS. | [Run the release binary without Docker](deploying-on-a-vm.md#run-the-release-binaries-without-docker) |
| **Stateless serverless (Cloud Run, Cloudflare Containers)** | Not supported *(Planned M3)* | You expect stateless scale-to-zero containers without attached RWO disks. | See [Cloud and serverless boundaries](#cloud-and-serverless-boundaries) below |
| **Public Foundation endpoints** | Supported | You want to evaluate or integrate a client without operating an indexer. | [Integration surfaces](../reference/integration-surfaces.md#endpoints) |

<a id="cloud-and-serverless-boundaries"></a>

### Cloud and serverless boundaries

- **Kubernetes (`StatefulSet` vs. horizontal `Deployment`):** The shipped [deploy/kubernetes/statefulset.yaml](../../deploy/kubernetes/statefulset.yaml) runs `replicas: 1` against a single `ReadWriteOnce` PVC mounted at `/var/lib/zinder` with `fsGroup: 10001`, so writer primaries and RocksDB secondaries share one local POSIX filesystem inside the pod. Scaling readers across multiple Kubernetes nodes with a shared `Deployment` is **not** supported on `rocksdb` ([ADR-0004](../adrs/0004-storage-and-deployment-topologies.md)).
- **Stateless serverless (Google Cloud Run, Cloudflare Containers):** Neither single-volume RocksDB nor long-lived stateful sync streams fit request-scoped or diskless serverless runtimes today. Horizontal stateless readers (`Deployment`, Cloud Run, and Cloudflare Containers + Hyperdrive) wait on Milestone M3's self-contained PostgreSQL storage backend ([ADR-0004](../adrs/0004-storage-and-deployment-topologies.md)).

<a id="what-you-need"></a>

## Step 3: Size your host

You need an archive Zebra or Zakura node on the same network. Ingest reads every historical block and refuses a pruned node at startup. The certified node versions are **Zebra 7.1.0** and **Zakura 1.8.6**. For a Mainnet first sync, use a Zakura node that was not built with fast sync, because fast sync omits per-height commitment trees below its last checkpoint. Give the node its own disk on top of the Zinder storage below.

### Private or standard Mainnet VM

- **CPU:** 8+ vCPUs (Broadwell+/Zen+ `x86-64` supporting `x86-64-v3` + `ADX`, or `AArch64`). The AMD64 image uses the optimized archive. Choose the baseline GNU/Linux archive if your CPU lacks the optimized build’s features. Optimized wallet accumulators can use AVX-512F/VL when available. Hosts with fewer than 8 logical CPUs take longer to sync; see [First sync on fewer than 8 logical CPUs](bulk-catchup-resource-tuning.md#first-sync-on-fewer-than-8-logical-cpus).
- **RAM:** 32 GiB RAM. Give `zinder-index` enough memory for canonical + wallet construction (`14g`–`24g` on Mainnet), keep `4g`+ on `zinder-serve` for private endpoints (raise for high-concurrency range reads), and give single-volume containers (`zinder serve`) at least 8–16 GB.
- **Disk:** Fast local SSD/NVMe formatted **`ext4 -O verity`** (when protecting canonical settled segments with optional Linux fs-verity). Reserve **240+ GB free** for `wallet` (pre-v2 Schema `v1` steady-state was ~102 GB canonical + wallet at Mainnet height 3,593,347 and growing, plus ~130 GB temporary sort spill headroom during initial wallet construction). Enabling `--full-blocks` (`storage.full_blocks = true`, `raw_blob_policy = "all"`) stores a single Zstd-compressed copy of full consensus blocks plus 40-byte transaction slice descriptors (`ZSBLKB01` + `live_tail_raw_block`), so budget additional free NVMe capacity beyond the ~250+ GiB raw chain data and wallet sort spill headroom.

### Public high-concurrency Mainnet endpoint

- **CPU:** 8–16+ vCPUs (Broadwell+/Zen+ `x86-64` with `BMI2 + ADX`, or `AArch64`). Release images and optimized binary archives enable `pasta_curves/asm` (`ZINDER_PASTA_ASM=1`; pass `--features pasta_curves/asm` when compiling directly with Cargo), saving ~6.7–8.2% on Orchard Merkle hashing and canonical construction benchmarks while failing closed at startup if `ADX` is missing.
- **RAM:** **64+ GiB RAM** and size `zinder-serve`'s cgroup memory limit above **32 GiB** (`ZINDER_SERVE_MEMORY_LIMIT`). Allow room for the active settled-segment working set and the rest of the process. If range-stream `wait_timeout` refusals rise alongside disk read saturation, check the [settled-segment memory gauges](../reference/metrics.md#storage) and raise the limit to accommodate the serving workload.
- **Node feed:** Set `ZINDER_NODE__INDEXER_GRPC_ADDR` and `ZINDER_INGEST__SOURCE=zebra-indexer-grpc` (in your Compose env file or exported environment) to stream mempool events and fetch blocks as binary protobufs instead of polling the full mempool over JSON-RPC on every block.
- **Edge proxy:** Put a TLS proxy with per-client connection, request, and stream limits in front of the listeners ([Exposing over TLS](exposing-over-tls.md)).

The two-container Compose manifest defaults `ZINDER_SERVE_MEMORY_LIMIT` to
`8g`. For a native systemd deployment, set the equivalent ceiling with
`MemoryMax`. Buffered reads of settled segments can charge the Linux page
cache to that limit even when RocksDB uses direct I/O. If the admitted corpus
exceeds the detected budget at open, Zinder logs a warning and requests
disabled speculative readahead on segment files.

### Testnet and Regtest baselines

| Network | Suggested container memory (`zinder-index` / `zinder-serve`) | First sync notes |
| --- | --- | --- |
| **Testnet** | `12g` / `4g` | Fits within a few hours on SSD/NVMe. |
| **Regtest** | `4g` / `2g` | Minutes from genesis. |

## Personal-node Compose

Use Docker Engine with the Compose plugin on AMD64 Linux that supports the
optimized CPU baseline, ARM64 Linux, or Apple Silicon with Docker Desktop.
The bundle starts an archive Zebra node and `zinder serve --preset light-wallet`.
Give the stack at least 12 GiB of container memory plus headroom for the host
or Docker VM. The manifest caps Zebra at 4 GiB and Zinder at 8 GiB by default.
Use `ZINDER_SERVE_MEMORY_LIMIT=4g` for a small Regtest stack. For Mainnet,
reserve the [Zinder disk space above](#choose-a-profile), plus initial-sync
headroom and [Zebra's separate disk budget](https://zebra.zfnd.org/user/requirements.html).

1. Download the manifest from a release that includes it. Verify the downloaded
   asset with GitHub CLI before you start it.

   ```bash
   release=vX.Y.Z
   gh release download "$release" --repo ZcashFoundation/zinder \
     --pattern zinder-personal-compose.yaml
   gh release verify-asset "$release" zinder-personal-compose.yaml \
     --repo ZcashFoundation/zinder
   ```

2. Start the bundle. The release manifest pins both images by digest.

   ```bash
   docker compose -f zinder-personal-compose.yaml up -d
   ```

   The defaults use Mainnet and `http://zebra:8232`. For Testnet, export
   `ZINDER_NETWORK__NAME=zcash-testnet` and `ZEBRA_NETWORK__NETWORK=Testnet`
   before starting. Keep these variables set for subsequent commands. If the
   bundled node needs a different RPC service address, set
   `ZINDER_NODE__JSON_RPC_ADDR`. Zinder authenticates with the bundled
   node's cookie and generates its own internal control tokens. The two
   published host ports bind only to `127.0.0.1`. Keep the default loopback
   mappings when using a proxy.

3. Check progress and readiness. The container health check reports liveness
   while the node syncs and Zinder builds its canonical store.

   ```bash
   docker compose -f zinder-personal-compose.yaml ps
   curl -fsS http://127.0.0.1:9108/readyz
   ```

   When readiness returns HTTP 200, point a local lightwalletd-compatible
   client at `127.0.0.1:9067`. For another device, follow
   [Exposing over TLS](exposing-over-tls.md) and proxy only the wallet port.

4. Stop the bundle and keep its state. The Zinder store and Zebra chain state
   live in separate named volumes.

   ```bash
   docker compose -f zinder-personal-compose.yaml down
   ```

<a id="what-you-deploy"></a>

## Processes and canonical ports

In `deploy/docker-compose.yml`, `zinder-index` (`zinder index`) writes the canonical and wallet stores while `zinder-serve` (`zinder serve --readers-only`) supervises the reader processes sharing the same volume and network namespace (`network_mode: "service:zinder-index"`). In a single-volume PaaS or `StatefulSet`, `zinder serve` supervises both writers and readers together. See the [architecture overview](../architecture/overview.md) for data flow.

| Surface / Supervisor | Mode / Role | Mainnet port | Notes |
| --- | --- | --- | --- |
| `zinder serve` ops | Supervisor `/healthz`, `/readyz`, `/metrics` | `9108` | Default ops listener for `zinder serve` |
| `zinder index` ops | Writer supervisor `/healthz`, `/readyz`, `/metrics` | `9105` | Default ops listener for `zinder index` (and standalone `zinder-ingest`) |
| `zinder-compat-lightwalletd` | lightwalletd `CompactTxStreamer` gRPC (and optional gRPC-Web) | `9067` | All presets |
| `zinder-query` | Native `WalletQuery` gRPC | `9102` | `wallet` and `explorer` presets |
| `zinder-explorer` | `ExplorerQuery` gRPC | `9068` | `explorer` preset |
| `zinder-compat-cipherscan` | Cipherscan-compatible REST & WebSocket | `9070` | `explorer` preset |

When running individual roles directly via `zinder run <role>`, per-role standalone ops defaults remain `9105` (`ingest`), `9110` (`projector`), `9106` (`query`), `9107` (`compat-lightwalletd`), `9069` (`explorer`), and `9108` (`compat-cipherscan`). The serving gRPC ports also serve `grpc.health.v1.Health` ([gRPC health](../reference/readiness.md#grpc-health)).

These are the **Mainnet** ports. The shipped Compose env files add `+10000` on Testnet (for example, `19067`, `19068`, `19070`, `19102`, `19105`, and `19108`) and `+20000` on Regtest, and bind all six published ports to host loopback (`127.0.0.1`) only.

Compose also exposes these internal/observability ports:

| Port | Use | Host publication |
| --- | --- | --- |
| 9100 | Ingest control API (projector and readers) | Loopback inside the shared container network only (same on all networks). |
| 9101 | Projector control API | Loopback inside the shared container network only (same on all networks). |
| 9095 | Prometheus (`--profile observability`) | Loopback (`19095` Testnet, `29095` Regtest). |
| 3002 | Grafana (`--profile observability`) | Loopback (`13002` Testnet, `23002` Regtest). |

<a id="quickstart-with-docker-compose"></a>

## Testnet Docker Compose quickstart

This quickstart runs Zinder on Testnet beside a Zebra node from the [Z3 stack](https://github.com/ZcashFoundation/z3) with zero TOML configuration files, using Docker Engine, the Compose v2 plugin, `git`, `curl`, and `grpcurl`. For Zakura, use `deploy/.env.zakura-testnet` and follow [Attach a Zakura node](deploying-on-a-vm.md#attach-a-zakura-node) for step 1.

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

3. Provision the required host control tokens (`ingest.token` and `checkpoint.token`):

   ```bash
   sudo install -d -o root -g 10001 -m 0750 /etc/zinder/control-secrets
   head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' | sudo install -o root -g 10001 -m 0440 /dev/stdin /etc/zinder/control-secrets/ingest.token
   head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' | sudo install -o root -g 10001 -m 0440 /dev/stdin /etc/zinder/control-secrets/checkpoint.token
   ```

4. Pull the signed release image for your tag and start Zinder (select `--preset` via `ZINDER_PRESET=light-wallet|wallet|explorer`, defaulting to `wallet`; keep `--no-build` so Compose uses the published image; add `--profile observability` to also start Prometheus on `19095` and Grafana on `13002`):

   ```bash
   export ZINDER_VERSION=vX.Y.Z
   docker compose --env-file deploy/.env.testnet \
     -f deploy/docker-compose.yml pull
   docker compose --env-file deploy/.env.testnet \
     -f deploy/docker-compose.yml up -d --no-build
   ```

   Compose runs `state-init` (setting `/var/lib/zinder` ownership to `10001:10001` while keeping `/var/lib/zinder/canonical` and `/var/lib/zinder/wallet` absent until writers publish them), starts `zinder-index`, and starts `zinder-serve --readers-only` sharing its volume and network namespace.

5. Check `/healthz` and `/readyz` on `zinder-index` and `zinder-serve` (`{"status":"ready",...}` once serving, or HTTP 503 while syncing):

   ```bash
   curl -fsS http://127.0.0.1:19105/readyz   # zinder-index (ingest + projector)
   curl -fsS http://127.0.0.1:19108/readyz   # zinder-serve (readers)
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

To run an unreleased checkout, leave `ZINDER_VERSION` unset and build the images:

```bash
docker compose --env-file deploy/.env.testnet \
  -f deploy/docker-compose.yml up -d --build
```

<a id="before-you-expose-it"></a>

## Before you expose it

Zinder listeners serve plaintext gRPC and REST. Put TLS termination, authentication, and per-client rate/connection limits in front of them using [Exposing over TLS](exposing-over-tls.md). Keep operational, control, and observability ports private as listed in the [security boundary](../architecture/service-operations.md#security-boundary).

## What happens next

- Do not restart `zinder-index` (or `zinder-ingest` / `zinder-projector`) while stores are constructing; a restart discards the unfinished build and starts over.
- Follow construction progress and phases in [Initial sync](initial-sync.md) and `/readyz` causes in [Readiness](../reference/readiness.md).
- For production VM setup with systemd, failover nodes, indexer mTLS, or Explorer, continue to [Deploying on a VM](deploying-on-a-vm.md).
