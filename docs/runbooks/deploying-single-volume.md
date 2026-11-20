# Deploying the single-volume container

This runbook deploys Zinder on a platform that attaches one persistent volume
to one container, such as Railway. The `zinder-wallet-serving` image runs
`zinder-ingest`, `zinder-projector`, `zinder-query`, and
`zinder-compat-lightwalletd` as supervised processes in one container. It
serves native `WalletQuery` on port 9102 and lightwalletd `CompactTxStreamer`
on port 9067. Read [Deploying Zinder](deploying.md) first for host
requirements.

## When to use it

Use this image when your platform attaches one volume to one container. The
readers open the stores on the writers' filesystem, so the four processes
share a container. When your host shares a volume between containers,
[Deploying on a VM](deploying-on-a-vm.md) runs each process in its own
container.

## Get the image

Each release publishes the image for Linux amd64 and arm64. The package is
public, so pulling needs no credentials. Pin the release tag or its digest:

```text
ghcr.io/zcashfoundation/zinder-wallet-serving:vX.Y.Z
```

To run an unreleased commit, build the `zinder-wallet-serving` target and push
it to a registry your platform can read:

```bash
docker build -f deploy/Dockerfile --target zinder-wallet-serving \
  -t <registry>/zinder-wallet-serving:<commit> .
```

Unlike the per-service images, this image is built on a digest-pinned Debian
slim base with `sh` and `curl`, so you can open a shell in it. It starts as
root, hands the volume to the `zinder` user (UID and GID 10001) on every
start, and runs each process as that user.

## Configure the container

Attach one persistent volume at `/var/lib/zinder`, sized as
[What you need](deploying.md#what-you-need) describes. Give the container at
least 8 GB of memory. All four processes share one memory limit. In a
smaller container, the kernel kills a process during the first build. The
whole container then restarts, and the build starts over and fails again.

Lightwalletd compatibility starts after both builds, so it does not raise the
build peak that sets this minimum. Its memory while serving has not been
measured in this image. Size its block-range streams as
[lightwalletd compatibility](../reference/lightwalletd-compatibility.md#getblockrange-and-getblockrangenullifiers)
describes. Before you rely on 8 GB, record the container's peak memory under
the block ranges your wallets request. Lower
`ZINDER_COMPAT__MAX_CONCURRENT_BLOCK_RANGE_STREAMS` or add memory if the peak
leaves too little headroom. To run without compatibility, set
`ZINDER_WALLET_SERVING__LIGHTWALLETD=false`.

Set these variables:

| Variable | Value |
| --- | --- |
| `ZINDER_NETWORK__NAME` | `zcash-mainnet` or `zcash-testnet` |
| `ZINDER_NODE__JSON_RPC_ADDR` | The node's JSON-RPC URL, such as `http://zebra.railway.internal:18232` |
| `ZINDER_NODE__AUTH__METHOD` | `cookie` |
| `ZINDER_NODE__AUTH__COOKIE` | The node's cookie, as the inline `user:password` string |
| `ZINDER_NODE__INDEXER_GRPC_ADDR` | The node's indexer gRPC URL, such as `http://zebra.railway.internal:8155`. An `https://` URL uses TLS; see [Indexer TLS](#indexer-tls). |
| `ZINDER_PROJECTOR__BUILD_OWNER_HEX` | A stable 32-character hexadecimal value |

The container requires `ZINDER_NODE__INDEXER_GRPC_ADDR` and stops at startup
without it. Without the indexer stream, ingest would reread the whole node
mempool after every block, and withdraw readiness whenever that takes longer
than 30 seconds.

These variables are optional:

| Variable | Effect |
| --- | --- |
| `ZINDER_WALLET_SERVING__CANONICAL_CONSTRUCTION_DEADLINE_SECONDS` | How long the chain-store build may take before the container stops. Default `10800`, 3 hours. |
| `ZINDER_WALLET_SERVING__WALLET_CONSTRUCTION_DEADLINE_SECONDS` | How long the wallet-store build may take before the container stops. Default `7200`, 2 hours. |
| `ZINDER_WALLET_SERVING__LIGHTWALLETD` | `false` leaves lightwalletd compatibility stopped and port 9067 closed. Default `true`. Letter case does not matter, and any other value stops the container at startup. Changing it takes a redeploy; the stores need no rebuild. |
| `INGEST_CONTROL_BEARER_TOKEN`, `CHECKPOINT_BEARER_TOKEN` | The internal control tokens. The container generates them at startup when unset. |
| `RUST_LOG` | Log filter. Default `info`. A value that does not parse stops the container. |

The default deadlines fit Testnet. For Mainnet, set at least `86400` (24
hours) and `43200` (12 hours) before the first deployment, and more on slower
disks or a slower node, because the defaults end a Mainnet build from an
empty volume.

Any other variable from the
[configuration reference](../reference/configuration.md#environment-variables)
works if it applies to ingest, projector, query, or lightwalletd
compatibility. `ZINDER_COMPAT__*` variables reach compatibility alone.
Compatibility serves in `wallet` mode here, so
`ZINDER_COMPAT__SERVING=compact-blocks` stops the container when
compatibility starts. The
entrypoint passes each process only the variables its binary accepts. At
startup it logs the names of `ZINDER_*` variables that no process reads, so
check that line after you add one. A misspelled `ZINDER_WALLET_SERVING__*`
name stops the container.

Do not override storage paths, listen addresses, control endpoints, or token
paths. They are fixed in `deploy/config/wallet-serving/`, and a path variable
such as `ZINDER_STORAGE__PATH` would reach more than one process. Unless
compatibility is turned off, `ZINDER_STORAGE__SECONDARY_PATH` or
`ZINDER_WALLET__SECONDARY_PATH` stops the container at startup, because it
would give both readers the same secondary directory.
`ZINDER_WALLET__ROCKSDB__BLOCK_CACHE_BYTES` sizes the wallet cache of the
projector, query, and compatibility, so count it three times against the
memory limit.

### Indexer TLS

Zakura requires mutual TLS on an indexer listener that is not on loopback.
Set `ZINDER_NODE__INDEXER_GRPC_ADDR` to the `https://` URL. If your platform
can mount files, mount the PEM files and set their paths. If it delivers
secrets only as variables, as Railway does, set the PEM text with its line
breaks intact:

| Item | Path variable | Text variable |
| --- | --- | --- |
| CA that verifies the node. Omit to use system roots. | `ZINDER_NODE__INDEXER_TLS__CA_CERT_PATH` | `ZINDER_NODE__INDEXER_TLS__CA_CERT_PEM` |
| Client certificate chain | `ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PATH` | `ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PEM` |
| Client private key | `ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH` | `ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PEM` |

Set `ZINDER_NODE__INDEXER_TLS__SERVER_NAME` when the node certificate names a
different host than the URL.

The entrypoint writes each PEM value to a file only the `zinder` user can
read. The deploy log names the location: `/dev/shm` is in memory, and `/run`
may be on disk. The container stops when a value lacks a `-----BEGIN` line,
has no line breaks, or is set as both text and path. To rotate the material,
change the variables and redeploy.

### Fallback node endpoints

Ingest can switch to a second node, and both readers broadcast through it; see
[node endpoint failover](../architecture/chain-ingestion.md#node-endpoint-failover).
Name each fallback with the `[node]` variables under a position, counting from
1:

| Variable | Value |
| --- | --- |
| `ZINDER_NODE_FALLBACKS__1__JSON_RPC_ADDR` | `http://zakura.railway.internal:18232` |
| `ZINDER_NODE_FALLBACKS__1__AUTH__METHOD` | `cookie` |
| `ZINDER_NODE_FALLBACKS__1__AUTH__COOKIE` | That node's cookie |
| `ZINDER_NODE_FALLBACKS__1__INDEXER_GRPC_ADDR` | That node's indexer gRPC URL. Required. |
| `ZINDER_NODE_FALLBACKS__1__BROADCAST_TIMEOUT_SECS` | `7` |

Set the broadcast timeout on every fallback, because
[a fallback does not inherit it](../reference/configuration.md#the-node-sections).
Indexer TLS takes the same variables under the
position, such as `ZINDER_NODE_FALLBACKS__1__INDEXER_TLS__CLIENT_KEY_PEM`.

## Ports

| Port | Serves | Reachable from |
| --- | --- | --- |
| 9067 | lightwalletd `CompactTxStreamer` gRPC, plaintext | The platform's private network |
| 9102 | Native `WalletQuery` gRPC, plaintext | The platform's private network |
| 9106 | Query ops: `/healthz`, `/readyz`, `/metrics` | The private network and the platform health check |
| 9107 | Compatibility ops: `/healthz`, `/readyz`, `/metrics` | The private network |
| 9100, 9101 | Ingest and projector control | Container loopback only |
| 9105, 9110 | Ingest and projector ops | Container loopback only |

Do not attach a public domain to 9067 or 9102. Put TLS, authentication, rate
limits, and quotas in a proxy in front of them, as
[Before you expose it](deploying.md#before-you-expose-it) describes.

## Deploy on Railway

1. Create the service from a Docker image source, and name the image by its
   exact tag. Railway pulls it. To upgrade, change the tag.
2. Attach a volume at `/var/lib/zinder`, and set the variables above.
3. Set `PORT=9106`, the query ops port that Railway health-checks, and
   `RAILWAY_DEPLOYMENT_DRAINING_SECONDS=30`. The entrypoint gives the
   processes 20 seconds to flush their stores on shutdown, which a shorter
   drain window cuts off.
4. Set the restart policy to Always.
5. On a first deployment to an empty volume, leave the health-check path
   unset. Port 9106 does not listen until both builds finish, which can take
   longer than Railway's maximum health-check timeout of 3600 seconds.
6. When the deploy log shows `wallet serving is ready`, set the health-check
   path to `/healthz` and the timeout to `3600`, then deploy again. A changed
   setting applies to a new deployment, not to Redeploy of an existing one.

Deploy from the published image. Railway builds only the last stage of a
Dockerfile, so it cannot build this image from the repository. Clients reach
the service at `<service>.railway.internal:9067` and
`<service>.railway.internal:9102` over Railway's IPv6 private network.

The Railway health check confirms that query listens, and it does not read
compatibility. Also monitor `/readyz` on 9106 and 9107, and
`zinder_readiness_served_epoch_age_seconds`, which is the only signal of a
[node outage](../reference/readiness.md#during-a-node-outage). A compatibility
process that exits, or is not ready within the serving deadline, stops the
container.

For private scrape placement, lifecycle evidence, and coverage checks, follow
[Measure a wallet-serving deployment](serving-census.md).

## What a fresh sync looks like

A fresh volume builds the chain store, then the wallet store, and serves
wallets once both finish. Expect several hours without a serving port.
The deploy log names each phase:

| Log line | Phase |
| --- | --- |
| `starting canonical construction` | Ingest is building the chain store. The line names the deadline. |
| `starting wallet construction` | The projector is building the wallet store. |
| `starting wallet serving with zinder-query and zinder-compat-lightwalletd` | Both readers are opening the stores. They share one fixed 30-minute deadline. With compatibility turned off, the line names `zinder-query` alone. |
| `wallet serving is ready` | Native `WalletQuery` answers, and so does lightwalletd unless it is turned off. |

While a phase waits, the log repeats that process's `/readyz` body every five
minutes. A restart during a build discards it and starts over. Once both
stores are built, a restart resumes from them. [Initial sync](initial-sync.md)
describes each phase.

From a shell in the container, such as `railway ssh`, check each process:

```bash
curl --fail-with-body -sS http://127.0.0.1:9105/readyz   # ingest
curl --fail-with-body -sS http://127.0.0.1:9110/readyz   # projector
curl --fail-with-body -sS http://127.0.0.1:9106/readyz   # query
curl --fail-with-body -sS http://127.0.0.1:9107/readyz   # lightwalletd compatibility
```

## Private backups

[Back up and restore a wallet-serving indexer](private-backups.md) covers
coherent capture, off-host storage, and replacement on a fresh volume.

## Failure handling

The entrypoint watches every process and its `/readyz`. When an enabled
wallet reader answers HTTP 200, the entrypoint also calls `grpc.health.v1.Health/Check`
on that reader's serving socket. A non-serving reply counts as an answered
unready poll. A connection failure or a missed reply counts as an unreachable
listener. When a budget expires, the entrypoint exits non-zero, and the
platform's restart policy starts a clean set.

| Symptom | Cause | What the container does | Action |
| --- | --- | --- | --- |
| A build phase outlives its deadline | The build is slower than the deadline | Logs the last `/readyz` body and exits | Raise the deadline variable. A restarted build starts over. |
| A process exits | Crash or configuration error | Exits | Read the process's error in the log. |
| `/readyz` reports `schema_mismatch` or `node_capability_missing` | Incompatible store, or a pruned or incapable node | Exits at once | Use empty paths for a new schema, or an archive node. |
| A process stays unready for an hour | Lag or a fault that did not clear | Exits | Read the logged `/readyz` body. |
| A process flaps between ready and unready | Repeated short faults | Exits after six unready episodes, or 900 seconds unready in total, without 10 ready minutes in a row | Read the logged episodes. |
| Ingest cannot reach its node | Node outage | Keeps serving stored data. Logs the outage once when it starts and once when it ends. Exits after 24 hours of active outage time. | Restore the node. After a restart, the container serves nothing until a node answers, because ingest needs one to start. |
| Ingest reports `upstream_not_ready`, for example while the node resyncs | The node answers but is not synchronized | Keeps serving stored data with no time limit | Wait for the node. |
| Query and compatibility refuse wallets with `REORG_WINDOW_EXCEEDED` | The node replaced more blocks than the reorg window, `ZINDER_INGEST__REORG_WINDOW_BLOCKS`, default 1000. Ingest parks. | Stays up and keeps the typed refusal | Look for a replaced or misbehaving node, then rebuild. Do not redeploy otherwise: ingest parks again on the same reorg. |
| Ingest, query, and compatibility report `canonical_store_damaged` | Ingest found a stored row of the canonical store that fails verification and logged it as `tree_state_verification_mismatch`. | Stays up, and query and compatibility refuse wallets with `SERVICE_NOT_READY` | Rebuild the canonical store. Do not redeploy otherwise: ingest reports the recorded damage again and confirms it on the next start. |
| Query and compatibility report `canonical_store_damaged` without a height | Ingest cannot read its verification record and logged `tree_state_verification_record_unreadable`. | Stays up, refuses wallets, and verifies every settled row at once | Wait for the cycle to pass, which clears the cause, or rebuild the canonical store. |
| Projector reports `wallet_transition_limit_exceeded` | See [below](#wallet-transition-exceeds-its-logical-byte-limit) | Stays up | Raise the limit and redeploy. |
| Query logs `wallet_serving_pair_table_file_missing` | Compaction deleted a file under an open read view. Query replaced the view and retried. | Stays up | None for an occasional warning. If it follows most blocks, check for `wallet_serving_pair_publisher_refresh_failed` and for bulk compactions. |

If ingest reports `syncing`, check the `ingest_control` entry in query's
`/readyz`. A relayed `upstream_not_ready` means the node is answering and
syncing. For a relayed `node_unavailable`, read `cause.syncing.wait_reason`
on ingest. With `mempool_hydration_at_tip`, check the writer's connection
to the node's mempool feed.
With `canonical_progress`, inspect ingest's progress even if the reported
heights match. Lag and block preparation use the one-hour tolerance and the
recurrence budgets. The 24-hour clock pauses until ingest is back at the tip
if the same mempool outage continues.

The image `HEALTHCHECK` runs `zinder-wallet-serving-healthcheck`. It passes
when query and compatibility each return 200 from `/readyz` or serve the
`reorg_window_exceeded` or `canonical_store_damaged` refusal. Both readers
serve the same data, so either
one failing fails the check. With compatibility turned off, the check reads
query alone. It also passes while the projector is parked on its transition
limit. A host that restarts unhealthy containers therefore does not restart
into the same park. Its start period is 6 hours, which covers only the default
deadlines. Lengthen it for a Mainnet build.

On `SIGTERM`, the entrypoint signals compatibility, query, the projector, and
ingest in that order, and they stop in parallel. Query and compatibility
end gRPC calls still open after 10 seconds with `UNAVAILABLE`, and allow 2
more seconds to deliver that status, so a slow block-range client cannot hold
the shutdown open. The entrypoint waits 20 seconds, then exits non-zero and
names any process still holding a store open.

### Wallet transition exceeds its logical-byte limit

The projector applies wallet changes as atomic transitions, and refuses a
transition that touches more wallet data than
`projector.follow.max_transition_logical_bytes` allows. A deep reorg that
rolls back and replays many blocks of a busy address can exceed it. The
projector logs `wallet_projection_following_parked` once, with an error that
begins:

```text
wallet following stopped planning a transition that already needs <n> logical bytes, more than projector.follow.max_transition_logical_bytes (ZINDER_PROJECTOR__FOLLOW__MAX_TRANSITION_LOGICAL_BYTES) allows (<limit>)
```

The wallet is unchanged, and `<n>` is a lower bound. The projector stays up,
parked, and its `/readyz` reports `wallet_transition_limit_exceeded` with
`required_bytes` and `configured_bytes`. It keeps its place in the chain, so
it resumes without a rebuild once the limit allows the transition. A running
query or compatibility process keeps answering from its last view for a few
blocks, then refuses wallets with `SERVICE_NOT_READY`. After a restart, the
entrypoint starts neither reader behind the parked projector and logs
`wallet readers not started`.

To recover, set `ZINDER_PROJECTOR__FOLLOW__MAX_TRANSITION_LOGICAL_BYTES` well
above `<n>`, at most `1073741824`, and redeploy. The default is `536870912`.
While it applies one transition, the projector's memory grows by up to about
1.5 times the bytes the transition counts. Add at least 1.5 times the bytes
you add to the limit to the container's memory. On Testnet, one 1,000-block reorg needed 796 MB, which the default
refuses.

If the limit is still too small, the projector parks again and the readers
never start. If `<n>` already exceeds `1073741824`, no setting admits the
transition. Rebuild the wallet in a second service with its own volume, move
traffic to it once its `/readyz` returns 200, and retire this one.

## Serve the Explorer from the same volume

The `zinder-wallet-serving-explorer` image adds `zinder-explorer`, which
serves `ExplorerQuery` for explorer front ends. Ingest in this image also
builds the Explorer views. Configure it exactly like `zinder-wallet-serving`,
with the same volume, variables, and Railway settings:

```text
ghcr.io/zcashfoundation/zinder-wallet-serving-explorer:vX.Y.Z
```

| Port | Serves | Reachable from |
| --- | --- | --- |
| 9068 | `ExplorerQuery` gRPC, plaintext | The private network |
| 9069 | Explorer ops | The private network |

The Explorer reads `ZINDER_NETWORK__*`, `ZINDER_NODE__*`, and
`ZINDER_EXPLORER__*` variables. It reads from `[node]` and not from the
fallbacks. Give the container more than the 8 GB minimum and watch the
volume, because memory and disk for the Explorer and its views are not
measured.

The container stops at startup if the Explorer configuration is invalid. The
Explorer starts once wallet serving passes its health check and ingest admits
the view store, or 10 minutes after that check passes. An Explorer failure
does not stop wallet serving: the entrypoint restarts the Explorer after 15
seconds, doubling the delay up to 5 minutes. The image `HEALTHCHECK` and the
Railway health check cover wallet serving, so monitor the Explorer's `/readyz`
on port 9069.

Ingest pauses the views when the volume runs low on space, as
[What it needs](../architecture/explorer-plane.md#what-it-needs) describes. Set
`ZINDER_INGEST__EXPLORER_VIEWS=false` to stop the views from growing while you
grow the volume, and the Explorer serves them as they last stood.

To rebuild the views after a release that changes them, follow
[Add the Explorer](deploying-on-a-vm.md#add-the-explorer). From a shell in the
container, move the store with
`mv /var/lib/zinder/canonical/materialized-views /var/lib/zinder/materialized-views.previous`,
then restart the deployment.

## Chain-store-only diagnostic image

Releases also publish `zinder-canonical-runtime`, which runs `zinder-ingest`
alone. It builds and follows the chain store but serves no clients. Use it to
check chain construction on a platform, not to serve wallets. The image takes
every ingest setting, including the storage path under `/var/lib/zinder`,
from `ZINDER_*` variables. Its ops port is
9105, so set `PORT=9105` on Railway. Indexer TLS PEM variables work as above.
