# Deploying Zinder

This page takes an operator from an empty host to a running, verified Zinder
deployment that serves wallets from your own node. It covers what you deploy,
what the host needs, which deployment to choose, and a Docker Compose
quickstart.

A deployment runs on one host and builds its stores from your node. Pin an
exact release tag, and read each release's notes before you upgrade.

## What you deploy

Zinder reads the chain from your Zcash node, stores it once, and serves it to
wallets and applications. A deployment is four processes on one host
filesystem, plus an optional fifth. `zinder-ingest` is the only process that
writes the chain store, and `zinder-projector` is the only one that writes the
wallet store. Native query and lightwalletd compatibility read both stores.
The optional Explorer reads the chain store and the Explorer views that ingest
writes, and it gets wallet data through native query. The
[architecture overview](../architecture/overview.md) explains how they fit
together.

| Process | Serves | Port | Ops port |
| --- | --- | --- | --- |
| `zinder-ingest` | Nothing to clients. Reads the node and writes the chain store. | none | 9105 |
| `zinder-projector` | Nothing to clients. Builds the wallet store. | none | 9110 |
| `zinder-query` | Native `WalletQuery` gRPC | 9102 | 9106 |
| `zinder-compat-lightwalletd` | lightwalletd `CompactTxStreamer` gRPC | 9067 | 9107 |
| `zinder-explorer` (optional) | `ExplorerQuery` gRPC | 9068 | 9069 |

Each ops port serves `/healthz`, `/readyz`, and `/metrics`. These are the
Mainnet ports. The Compose files add 10000 on Testnet and 20000 on Regtest, so
the Testnet native query port is 19102. Compose publishes every port on host
loopback only.

Compose also runs these private ports:

| Port | Use | Published on the host |
| --- | --- | --- |
| 9100 | Ingest control API, used by the projector and the readers | No. Loopback inside the shared container network, same number on every network. |
| 9101 | Projector control API | No. Loopback inside the shared container network, same number on every network. |
| 9095 | Prometheus | Yes, on loopback. Testnet 19095, Regtest 29095. |
| 3002 | Grafana, under the `observability` profile | Yes, on loopback. Testnet 13002, Regtest 23002. |

## What you need

You need an archive Zebra or Zakura node on the same network. Ingest reads
every historical block, so it refuses a pruned node at startup. The certified
node versions are Zebra 7.1.0 and Zakura 1.8.6. For a Mainnet first sync, use
a Zakura node that was not built by fast sync, because fast sync does not
store the per-height commitment trees below its last checkpoint. Give the node
its own disk on top of the figures below.

| Network | Disk | Memory | First sync |
| --- | --- | --- | --- |
| Mainnet | About 465 GB in use, including Explorer views. Wallet construction also needs about 130 GB free for temporary files when it starts. | Compose container limits: ingest 14 GiB, projector 10 GiB, query 4 GiB, compatibility 4 GiB | Between about 6 and 16 hours. See the note below. |
| Testnet | Not measured | Compose container limits: ingest 8 GiB, projector 6 GiB, query 2 GiB, compatibility 2 GiB | Not measured. It fits the default construction deadlines of 3 hours for the chain store and 2 hours for the wallet store. |
| Regtest | Not measured | Compose container limits: 2, 2, 1, and 1 GiB | Not measured |

The memory figures are the limits the shipped Compose env files set. They are
ceilings, not measured use. Give the single-volume container at least 8 GB.
The release binaries need an x86-64-v3 or AArch64 CPU. CPU use is not
measured.

The Mainnet disk figures come from separate observations, so do not add them
up to get a peak:

- About 465 GB is the volume in use on one deployment on v0.16.3, after both
  stores were built.
- About 310 GB is the staging size of one measured canonical build. Ingest
  builds the chain store in a staging directory on the same volume and renames
  it into place when it publishes.
- About 130 GB is the free space that the projector's temporary sort files
  need on the same volume when wallet construction starts.

Peak disk use during a first sync has not been measured.

Plan for up to about 16 hours for a Mainnet first sync. One measured build
took about four hours for the chain store and more than two hours for the
wallet store. Builds on earlier releases took about 16 hours in total. No full
build has been timed on the current release.

## Choose a deployment

First decide what to serve. Each deployment below runs a different set of
processes. [Use cases](../use-cases.md) explains which clients fit each
surface.

| Deployment | What runs | What it serves | What it requires |
| --- | --- | --- | --- |
| Wallet endpoint | Ingest, projector, and `zinder-compat-lightwalletd`. Compose and the single-volume image also run `zinder-query`. | lightwalletd `CompactTxStreamer` on port 9067, to existing light wallets | The chain store and the wallet store, with `storage.raw_blob_policy` set to `transactions` or `all`. The shipped configurations set `transactions`. |
| Native wallet backend | Ingest, projector, and `zinder-query` | `WalletQuery` on port 9102, with reads pinned to one chain snapshot and typed errors, to clients built on `zinder-client` or the native API | The chain store and the wallet store, with `storage.raw_blob_policy` set to `transactions` or `all`, as for the wallet endpoint. |
| Wallet plus explorer | The wallet processes plus `zinder-explorer` | Both wallet surfaces, and `ExplorerQuery` on port 9068 | Set `ingest.explorer_views = true` on a chain store whose history starts at genesis or at the authenticated `checkpoint_height = 0`. Ingest refuses the setting on a store built from a later checkpoint, and otherwise also builds the Explorer view store. |
| Block feed | Ingest and `zinder-compat-lightwalletd` in compact-blocks mode | `GetLightdInfo`, `GetLatestBlock`, `GetBlock`, and `GetBlockRange` from the chain store, to consumers that read compact blocks | The chain store only. Start `zinder-compat-lightwalletd` with `ZINDER_COMPAT__SERVING=compact-blocks`. That process refuses to start with any `[node]`, `[node_fallbacks.N]`, or `[wallet]` setting, any `ZINDER_NODE__*`, `ZINDER_NODE_FALLBACKS__*`, or `ZINDER_WALLET__*` variable, or `compat.tree_state_replay_wait_ms`. Ingest keeps its node settings. |

Every process reads `storage.raw_blob_policy` as an exact expectation of the store it opens. The first commit fixes the policy, so a store built with `none` must be rebuilt to serve transactions.

Then choose how to package it:

| Deployment | Pick it when | Runbook |
| --- | --- | --- |
| Docker Compose beside a node | You want to try Zinder, or run it on a host that already runs your node in Docker. | [Quickstart](#quickstart-with-docker-compose) below |
| Docker Compose under systemd on a VM | You run a long-lived service on a VM you manage. This is the complete topology. | [Deploying on a VM](deploying-on-a-vm.md) |
| Single-volume container | Your platform attaches one volume to one container, such as Railway. It runs the same four processes in one container and serves both native `WalletQuery` and lightwalletd. | [Deploying the single-volume container](deploying-single-volume.md) |

## Quickstart with Docker Compose

This path runs Zinder on Testnet beside a Zebra node from the
[Z3 stack](https://github.com/ZcashFoundation/z3). For a Zakura node, use
`deploy/.env.zakura-testnet` and follow
[Attach a Zakura node](deploying-on-a-vm.md#attach-a-zakura-node) instead of
step 1.

The steps use Docker Engine with the Compose v2 plugin, `git`, `openssl`,
`curl`, and `grpcurl`. Step 4 runs the published release images. To build the
images from source instead, see [Build from source](#build-from-source).

1. Start a Zebra Testnet node with Z3, from your Z3 checkout, and wait until it
   has synchronized:

   ```bash
   Z3_NETWORK=testnet docker compose up -d
   ```

   Z3 creates the `z3-testnet` Docker network and the `z3-testnet-cookie`
   volume. `deploy/.env.testnet` names both.

2. Check out a Zinder release:

   ```bash
   git clone https://github.com/ZcashFoundation/zinder.git
   cd zinder
   git checkout vX.Y.Z
   ```

3. Create the two control tokens that the Zinder processes use to talk to
   each other. `deploy/.env.testnet` expects them in
   `/etc/zinder/control-secrets`, readable by the container group, GID 10001:

   ```bash
   sudo install -d -o root -g 10001 -m 0750 /etc/zinder/control-secrets
   openssl rand -hex 32 | sudo tee /etc/zinder/control-secrets/ingest.token >/dev/null
   openssl rand -hex 32 | sudo tee /etc/zinder/control-secrets/checkpoint.token >/dev/null
   sudo chown root:10001 /etc/zinder/control-secrets/*.token
   sudo chmod 0440 /etc/zinder/control-secrets/*.token
   ```

4. Pull the release images for the tag you checked out, and start Zinder:

   ```bash
   export ZINDER_VERSION=vX.Y.Z
   docker compose --env-file deploy/.env.testnet \
     -f deploy/docker-compose.yml pull
   docker compose --env-file deploy/.env.testnet \
     -f deploy/docker-compose.yml up -d --no-build
   ```

   `ZINDER_VERSION` selects the signed release image of every runtime, such
   as `ghcr.io/zcashfoundation/zinder-ingest:vX.Y.Z`. Use the tag you checked out, because each Compose file
   matches the images of its own release. `pull` fails on a tag that was never
   published. Keep `--no-build`: without it, Compose would build missing
   images from source and label the result as the release.

   Compose starts ingest first. It starts the projector only once ingest is
   healthy, and the two readers only once the projector is healthy, so on a
   first sync the command keeps waiting while the stores are built.
   [Initial sync](initial-sync.md) describes each phase.

5. Check readiness. Each command prints a JSON body that starts with
   `{"status":"ready"` once that process serves traffic. Until then, `/readyz`
   answers 503 with the reason, and `curl -f` exits non-zero:

   ```bash
   curl -fsS http://127.0.0.1:19105/readyz   # ingest
   curl -fsS http://127.0.0.1:19110/readyz   # projector
   curl -fsS http://127.0.0.1:19106/readyz   # native query
   curl -fsS http://127.0.0.1:19107/readyz   # lightwalletd compatibility
   ```

6. Make one native call and one lightwalletd call:

   ```bash
   grpcurl -plaintext -d '{}' 127.0.0.1:19102 \
     zinder.v1.wallet.WalletQuery/ServerInfo
   grpcurl -plaintext -d '{}' 127.0.0.1:19067 \
     cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLightdInfo
   ```

   `ServerInfo` reports the network `zcash-testnet` and the service
   `zinder-query`. `GetLightdInfo` reports `chainName` `test` and a
   `blockHeight` close to your node's tip.

The same Compose file also starts Prometheus on host port 19095. Add
`--profile observability` to the commands in step 4 to start Grafana on 13002.

### Build from source

To run a checkout that has no published images, leave `ZINDER_VERSION` unset
and run this command instead of step 4:

```bash
docker compose --env-file deploy/.env.testnet \
  -f deploy/docker-compose.yml up -d --build
```

Compose builds every image from the checkout and tags it `:local`.

## Before you expose it

Wallet listeners serve plaintext gRPC, and TLS termination and client
authentication belong in your proxy. Expose them only through that proxy, as
[Exposing over TLS](exposing-over-tls.md) sets up. Compose
publishes ports on host loopback, but any container on the node's Docker
network can still reach the wallet and ops listeners. The
[security boundary](../architecture/service-operations.md#security-boundary)
lists what must stay private.

## Optional pieces

- **Fallback nodes.** Ingest can switch to a second node when the first stops
  answering, and broadcasts move to the next node. See
  [Fail over to a second node](deploying-on-a-vm.md#fail-over-to-a-second-node)
  and [node endpoint failover](../architecture/chain-ingestion.md#node-endpoint-failover).
- **Explorer.** `zinder-explorer` serves block, transaction, address, and
  mempool views for explorer front ends. See
  [Add the Explorer](deploying-on-a-vm.md#add-the-explorer) and the
  [Explorer plane](../architecture/explorer-plane.md).
- **Full blocks.** Wallets that scan whole blocks need
  `storage.raw_blob_policy = "all"` on ingest and every reader before the
  first sync. The shipped configuration stores transactions only, and
  changing an existing store requires a rebuild. See the
  [wallet data plane](../architecture/wallet-data-plane.md).

## What happens next

The first sync builds the chain store and then the wallet store, and Zinder
serves wallets once both finish. Do not restart a process during that build,
because a restart starts it over.
[Initial sync](initial-sync.md) describes each phase and what to watch.
[Readiness](../reference/readiness.md) explains every `/readyz` state.
