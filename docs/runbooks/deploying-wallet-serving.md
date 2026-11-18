# Wallet-serving deployment (single-volume hosts)

This target serves the native `WalletQuery` protocol from one container
against one volume. `zinder-ingest`, `zinder-projector`, and `zinder-query`
run as three supervised processes over the volume mounted at
`/var/lib/zinder`, because the readers open secondaries over paths the
writers own. The canonical and wallet primaries keep one owner each and the
reader opens process-unique secondary generations, as
[ADR-0035](../adrs/0035-canonical-storage-topologies.md) requires.

Any Docker host that attaches one persistent volume to one container can run
this image. Hosts that share a volume across containers use
[docker-compose](../../deploy/docker-compose.yml) instead. Railway is the
reference host; the sections marked Railway are its adapter, and other
platforms substitute their own health-check, restart-policy, and variable
plumbing around the same image.

This target does not serve the lightwalletd compatibility protocol and is not
the certified complete release topology. Use
[Deploying on a VM](deploying-on-a-vm.md) for the four-runtime topology, and
[Railway canonical-writer validation](deploying-on-railway.md) for isolated
canonical evidence.

## Getting the image

Each release publishes the image for Linux amd64 and arm64:

```text
ghcr.io/zcashfoundation/zinder-wallet-serving:vX.Y.Z
```

Pin the exact release tag or its digest. The package is public, so pulling
needs no registry credentials.

The image is the `zinder-wallet-serving` target of `deploy/Dockerfile`. Build
it from source when you need an unreleased commit:

```bash
docker build -f deploy/Dockerfile --target zinder-wallet-serving \
    -t zinder-wallet-serving:local .
```

## Service configuration

Attach one persistent volume at `/var/lib/zinder`. Size it for canonical data,
the wallet projection, checkpoint staging, the reader secondaries, and
chain-growth reserve. Fresh wallet construction also spills its sorters to a
staging directory beside the wallet store. On Mainnet, keep about 130 GB free
when wallet construction starts; see
[Wallet construction](initial-sync.md#wallet-construction).

Provision at least 8 GB of memory. All three processes share one memory cgroup,
so the baked configs pin every budget that would otherwise be derived from it:

| Process | Bounded budget | Largest term |
| --- | --- | --- |
| `zinder-ingest` | ~2.6 GiB | canonical and materialized-view writer stores |
| `zinder-projector` | ~2.7 GiB while building, ~1.2 GiB while following | wallet-build sorters |
| `zinder-query` | ~0.35 GiB with one open serving pair, up to ~0.45 GiB with four | canonical secondary block cache, shared by every pair |

Query keeps up to four serving pairs open: the current pair and three more,
each the candidate it is catching up, a replaced pair that still answers
pinned reads, or one a long request holds. All of them read through one
canonical and one wallet block cache, so the configured cache sizes, 256 MiB
and 64 MiB here, bound the process however many pairs are open. Each open pair
adds its own memtables, up to 32 MiB, and the table readers of the files it
holds open. A generation stays open after its pair drains so a later block can
reuse it, so once three blocks arrive within one minute, or a long request
holds a released pair, all four stay open until query restarts. Query catches
drained generations up in turn, so a primary file that compaction replaces
stays on disk for only a few blocks while a drained generation still
references it. The wallet reader's 64 MiB default is likewise the total for
every pair. A Mainnet wallet's index and filter blocks need far more; size
`ZINDER_WALLET__ROCKSDB__BLOCK_CACHE_BYTES` from the wallet store's filter
working set as
[ADR-0020](../adrs/0020-bounded-rocksdb-resource-budget.md#locked-rocksdb-invariants)
describes, and count it twice, because in this container the variable also
sizes the projector's wallet writer cache. The remainder covers
allocator overhead, gRPC buffers, and task stacks. In a
smaller container the cgroup OOM-killer takes one process during construction.
That terminates the whole container. The restart discards the unpublished
canonical or wallet build and constructs it again from the start
([Initial sync](initial-sync.md)), only to be killed again.

Set these variables on the service (node addresses shown for Railway's
private network; substitute your host's):

| Variable | Value |
| --- | --- |
| `PORT` | `9106` |
| `ZINDER_NETWORK__NAME` | `zcash-testnet` or `zcash-mainnet` |
| `ZINDER_NODE__JSON_RPC_ADDR` | `http://zebra.railway.internal:18232` |
| `ZINDER_NODE__AUTH__METHOD` | `cookie` |
| `ZINDER_NODE__AUTH__COOKIE` | the inline `user:password` cookie string |
| `ZINDER_NODE__INDEXER_GRPC_ADDR` | `http://zebra.railway.internal:8155`, the node's indexer gRPC URL. An `https://` URL negotiates TLS; see [Indexer TLS](#indexer-tls). |
| `ZINDER_PROJECTOR__BUILD_OWNER_HEX` | a stable 32-character hexadecimal value |

The entrypoint stops the container at startup when
`ZINDER_NODE__INDEXER_GRPC_ADDR` is unset or empty. Without the indexer
stream, ingest follows the node mempool by polling JSON-RPC and re-reads the
whole mempool after every block. The mempool view for each new tip then
arrives a poll interval plus a full re-read late, and ingest withdraws
readiness whenever that takes longer than 30 seconds.

These are optional:

| Variable | Effect |
| --- | --- |
| `ZINDER_WALLET_SERVING__CANONICAL_CONSTRUCTION_DEADLINE_SECONDS` | Seconds canonical construction may take before the container stops. Defaults to `10800` (three hours). See [Construction deadlines](#construction-deadlines). |
| `ZINDER_WALLET_SERVING__WALLET_CONSTRUCTION_DEADLINE_SECONDS` | Seconds wallet construction may take before the container stops. Defaults to `7200` (two hours). See [Construction deadlines](#construction-deadlines). |
| `ZINDER_INGEST__REORG_WINDOW_BLOCKS` | Sets the canonical replacement depth, applied identically to the projector and the query surface. Defaults to 1000, Zebra's finalization depth. Raising it on an existing deployment, including by upgrading to this default, raises the stores in place and irreversibly; capture a state bundle first if you need a rollback path. Lowering it requires a rebuild. |
| `RUST_LOG` | Log filter, such as `info,zinder=info`. Defaults to `info`. A value that does not parse stops the container at startup; see [Structured logs](../architecture/service-operations.md#structured-logs). |
| `INGEST_CONTROL_BEARER_TOKEN` | Ingest-control bearer secret. Generated at boot when unset. |
| `CHECKPOINT_BEARER_TOKEN` | Checkpoint bearer secret. Generated at boot when unset. |

You can also set any variable in the
[environment-variable reference](../architecture/public-interfaces.md#environment-variable-reference)
whose "Used by" column names `zinder-ingest`, `zinder-projector`, or
`zinder-query`. Each process rejects a `ZINDER_*` variable that its binary
does not define. For that reason, the entrypoint gives each process only the
variables its binary accepts:

| Variables | `zinder-ingest` | `zinder-projector` | `zinder-query` |
| --- | --- | --- | --- |
| `ZINDER_NETWORK__*`, `ZINDER_OPS__*`, `ZINDER_SECURITY__*`, `ZINDER_INGEST_CONTROL__*` | yes | yes | yes |
| `ZINDER_NODE__*` | yes | | yes |
| `ZINDER_NODE_FALLBACKS__*` | yes | | yes |
| `ZINDER_STORAGE__CANONICAL__*`, `ZINDER_STORAGE__RAW_BLOB_POLICY` | yes | yes | yes |
| `ZINDER_INGEST__*`, `ZINDER_RETENTION__*`, `ZINDER_STORAGE__MATERIALIZED_VIEWS__*` | yes | | |
| `ZINDER_PROJECTOR__*`, `ZINDER_PROJECTOR_CONTROL__*` | | yes | |
| `ZINDER_WALLET__ROCKSDB__*` | | yes | yes |
| `ZINDER_QUERY__*`, `ZINDER_STORAGE__INITIAL_CATCHUP_TIMEOUT_MS`, `ZINDER_STORAGE__SECONDARY_CATCHUP_INTERVAL_MS`, `ZINDER_STORAGE__SECONDARY_REPLICA_LAG_THRESHOLD_CHAIN_EPOCHS`, `ZINDER_STORAGE__SERVING_PAIR_STALENESS_CEILING_MS` | | | yes |

For example, `ZINDER_PROJECTOR__BUILD__MAX_TEMPORARY_FILE_BYTES_PER_SORTER`
and `ZINDER_STORAGE__SERVING_PAIR_STALENESS_CEILING_MS` each reach one
process, while `ZINDER_STORAGE__CANONICAL__ROCKSDB__MAX_OPEN_FILES` applies to
all three. `zinder-projector` uses no node, so it never receives
`ZINDER_NODE__*` and fails at startup if a node setting reaches it some other
way.

No process receives the `ZINDER_WALLET_SERVING__*` variables, which configure
the entrypoint itself, or the two bearer secrets. Any other
`ZINDER_WALLET_SERVING__*` name, such as a misspelled deadline, stops the
container at startup. At startup the entrypoint
logs the names of any other `ZINDER_*` variables that no process reads, such
as a misspelled section or storage key. Check that line after you add a
variable.

Storage paths, listen addresses, control endpoints, and bearer token paths are
fixed in `deploy/config/wallet-serving/`. Do not override them: each process
uses its own path, and a path variable such as `ZINDER_STORAGE__PATH` reaches
more than one process.

The wallet-serving ingest configuration sets
`ingest.explorer_views = false` explicitly. Wallet projection and
wallet-serving readiness do not depend on the optional materialized-view plane;
an existing materialized-view directory is left untouched. To serve Explorer
views from the same volume, use the
[`zinder-wallet-serving-explorer` image](#serving-the-explorer-from-the-same-volume).

`PORT` names the port Railway healthchecks. It must stay `9106`, the
operational endpoint of the query process.

### Indexer TLS

A node whose indexer listens on a network address serves it over TLS. Zakura
also requires a client certificate there. Set `ZINDER_NODE__INDEXER_GRPC_ADDR`
to the `https://` URL, then supply the certificate material in one of two ways.

If the platform can mount files, mount the PEM files and set their paths:

| Variable | Value |
| --- | --- |
| `ZINDER_NODE__INDEXER_TLS__CA_CERT_PATH` | CA bundle that verifies the node. Omit it to use the system roots. |
| `ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PATH` | Client certificate chain. Set it together with the key. |
| `ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH` | Client private key. |
| `ZINDER_NODE__INDEXER_TLS__SERVER_NAME` | Name to verify when it differs from the URL host. Optional. |

If the platform delivers secrets only as variables, as Railway does, set the
PEM text instead, with its line breaks intact:

| Variable | Value |
| --- | --- |
| `ZINDER_NODE__INDEXER_TLS__CA_CERT_PEM` | CA bundle PEM text. |
| `ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PEM` | Client certificate chain PEM text. |
| `ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PEM` | Client private key PEM text. |

The entrypoint writes each value to a file that only the `zinder` user can
read, and it passes the processes the matching `*_PATH` variable. No runtime
process receives the PEM text. The entrypoint stops the container with an
error in these cases:

- a value lacks a `-----BEGIN` line;
- a value has no line breaks, which happens when an editor collapses a
  multi-line value;
- both the `*_PEM` and the `*_PATH` variable are set for the same item; or
- neither `/dev/shm` nor `/run` is writable.

The deploy log names where the files went. `writing indexer TLS material to
tmpfs at /dev/shm/zinder-indexer-tls` means they are in memory. A line naming
`/run/zinder-indexer-tls` means `/dev/shm` is not a writable tmpfs, and the
files may be on the container's disk. Whether Railway provides a tmpfs at
`/dev/shm` has not been verified; check this line on the first deployment.

The PEM variables stay visible in the platform's variable editor and in any
shell you open in the container. Use mounted files when the platform supports
them. To rotate the material, change the variables and redeploy.

`zinder-ingest` checks the files at startup. A missing file, or one with no
certificate or private key, stops the container with a configuration error.

### Fallback node endpoints

Ingest can switch to a second node when the first stops answering or stays
unsynchronized; see
[Chain ingestion §Node endpoint failover](../architecture/chain-ingestion.md#node-endpoint-failover).
Name each fallback with the `[node]` variables under a position, counting from
1 without a gap:

| Variable | Value |
| --- | --- |
| `ZINDER_NODE_FALLBACKS__1__JSON_RPC_ADDR` | `http://zakura.railway.internal:18232` |
| `ZINDER_NODE_FALLBACKS__1__AUTH__METHOD` | `cookie` |
| `ZINDER_NODE_FALLBACKS__1__AUTH__COOKIE` | that node's inline cookie string |
| `ZINDER_NODE_FALLBACKS__1__INDEXER_GRPC_ADDR` | that node's indexer gRPC URL |
| `ZINDER_NODE_FALLBACKS__1__BROADCAST_TIMEOUT_SECS` | `7` |

The entrypoint stops the container when a configured fallback has no
`INDEXER_GRPC_ADDR`, for the reason it requires one for `[node]`: ingest
follows the mempool of the endpoint it switched to. A fallback's indexer TLS
takes the same `*_PATH` or `*_PEM` variables under its position, such as
`ZINDER_NODE_FALLBACKS__1__INDEXER_TLS__CLIENT_KEY_PEM`; the entrypoint writes
those files as `node-fallback-1-client-key.pem` beside the `[node]` files.

Ingest and query receive the list. Query broadcasts through `[node]` and then
each verified fallback, moving on when an endpoint is unreachable; the
projector reads no node list. Set `BROADCAST_TIMEOUT_SECS` for every fallback: a
fallback does not inherit the 7-second broadcast timeout of `[node]`, and
without its own value a hung fallback holds each broadcast for its full
request timeout. Ingest's `/readyz` reports
`active_node.position` once a fallback is configured.

## Deploy on Railway

Create the service from a Docker image source and name the released image by
its exact tag, such as
`ghcr.io/zcashfoundation/zinder-wallet-serving:vX.Y.Z`. Railway pulls the
image; it does not build Zinder. To deploy another release, change the tag on
the service.

Set these in the service settings:

| Setting | Value |
| --- | --- |
| Healthcheck path | `/healthz` |
| Healthcheck timeout | `3600` seconds |
| Restart policy | Always |

Railway rejects a healthcheck timeout above 3600 seconds, so `3600` is the
longest window available.

On the first deployment to an empty volume, leave the healthcheck path unset
until the deploy logs show `wallet serving is ready`, then set it. The query
operational endpoint on port 9106 does not listen until canonical and wallet
construction finish, so every healthcheck path on that port fails until then,
including `/healthz`. Construction on an empty volume can outlast the one-hour
window.

On Mainnet, set the
[construction deadlines](#construction-deadlines) before the first deployment.
The defaults end a Mainnet build from an empty volume before it finishes.

If you set the healthcheck path before construction finishes, Railway marks the
deployment failed when the window expires and stops the container. The
entrypoint forwards `SIGTERM` and the processes exit cleanly. The volume keeps
the stores built so far, and the next deployment resumes from them.

A changed service setting, such as the healthcheck path, takes effect on a new
deployment. Railway's Redeploy action on an existing deployment keeps that
deployment's earlier healthcheck setting.

`/healthz` passes as soon as the query endpoint listens. It does not show
whether the native query surface serves. Watch query `/readyz` on port 9106 for
that, and alert on it in monitoring. It is not the deploy-time healthcheck
because it answers 503 in states a deployment should survive. A parked writer
serves the typed refusal with `reorg_window_exceeded`, and after a restart
query refuses with `canonical_tip_unconfirmed` until ingest confirms its
stored tip against the node.

Query `/readyz` stays 200 while the Zebra node is unavailable or behind its
estimated network tip. Query keeps serving stored reads, fails broadcasts
with `NODE_UNAVAILABLE`, and lists the node under `degraded` in its `/readyz`
body. Alert on `zinder_readiness_dependency_state` with a cause that does not
admit traffic, other than the `reorg_window_exceeded` halt that query already
refuses traffic for, and on `zinder_readiness_served_epoch_age_seconds`, which grows while
the node or ingest stops advancing the served chain.

Also set the `RAILWAY_DEPLOYMENT_DRAINING_SECONDS=30` service variable. The
entrypoint gives the three RocksDB openers twenty seconds to flush, so a
shorter drain window lets the platform kill them mid-write.

Railway cannot build this image from the repository. It builds the last stage
of a Dockerfile and has no target selection, and the last stage of
`deploy/Dockerfile` is a different target. To run an unreleased commit, build
the target as shown under [Getting the image](#getting-the-image), push it to a
registry the service can read, and point the service at that image.

## Ports

| Port | Surface | Reachability |
| --- | --- | --- |
| 9102 | native `WalletQuery` gRPC | Railway private network |
| 9106 | query operational HTTP | Railway private network and healthcheck |
| 9100 | ingest control gRPC | container loopback |
| 9101 | projector control gRPC | container loopback |
| 9105 | ingest operational HTTP | container loopback |
| 9110 | projector operational HTTP | container loopback |

The private network is IPv6-only, so clients reach
`<service>.railway.internal:9102`. Only the query process binds beyond
loopback; the two control planes stay private to the container.

## Fresh-sync behaviour

A fresh volume constructs canonical storage before the wallet projection, and
serves nothing until both complete. Several hours without a serving surface is
the expected first-deploy shape, not a stall.

A cause that no amount of waiting clears (`schema_mismatch`,
`node_capability_missing`) ends the container as soon as a plane reports it,
instead of consuming the phase deadline. A plane that reports
`reorg_window_exceeded` ends its phase instead: the canonical writer is parked,
and the remaining processes start so wallets receive the typed refusal
described under [Failure handling](#failure-handling).

Tell the three phases apart from the deploy logs:

| Log line | Phase |
| --- | --- |
| `starting canonical construction` | `zinder-ingest` is building or following canonical storage. |
| `starting wallet construction` | Canonical storage is ready and `zinder-projector` is building or following the wallet projection. |
| `starting wallet serving` | Both writers are ready and `zinder-query` is admitting a serving pair. |
| `wallet serving is ready` | The native `WalletQuery` surface answers. |

While a phase waits, the entrypoint repeats the plane's whole `/readyz` body
every five minutes, which carries the cause, the ingest phase, and the heights.

A plane that answers logs `http <status>: <body>`. A plane that does not answer
has no body, so the entrypoint logs `unreachable: <class> (curl exit <code>)`
instead. The class separates a stalled process from one that refuses traffic:
`connect_timeout` and `total_timeout` mean the process accepted or failed to
accept within the probe bounds and never completed the response,
`connection_refused` means no listener is bound, and `connection_reset` and
`empty_reply` mean the listener dropped the exchange.

From a shell in the container (`railway ssh`), each plane reports separately:

```bash
curl -fsS http://127.0.0.1:9105/readyz
curl -fsS http://127.0.0.1:9110/readyz
curl -fsS http://127.0.0.1:9106/readyz
```

Canonical readiness proves the writer and mempool contract only. Wallet
readiness proves the projection. Serving readiness proves an admitted
canonical and wallet pair at an exact fence, and it is the only one that
proves a client can be answered.

Once serving is ready, each new block reaches wallets within about 1.25
seconds of the canonical commit: one query refresh
(`ZINDER_STORAGE__SECONDARY_CATCHUP_INTERVAL_MS`, 1,000 ms by default) plus
one 250 ms projector poll. Each refresh publishes the writer's latest fence,
so blocks that commit closer together than that share one publication at the
later height. That happens while ingest catches up after a stall, when a reorg
replaces several blocks, and on Regtest. To measure the delay, match each
`canonical_live_append_committed` in the ingest log with the first later
`wallet_serving_pair_publisher_published` in the query log whose
`event_sequence` is at least the commit's `chain_event_sequence`. A height
with no publication of its own is expected.

Query keeps each replaced epoch answering pinned reads for one minute, so a
wallet that pins an epoch for a multi-request walk finishes it across new
blocks. When more than three blocks arrive within one minute, no generation is
free for the next one. That block waits until the oldest replaced epoch's
minute ends, and it is then published together with every block that arrived
during the wait. On Testnet this happens during bursts of blocks seconds
apart, and on any network while ingest catches up after a stall. The wait
lasts at most a minute, or about 90 seconds while a long request holds a
released pair. Readiness keeps admitting traffic, and
`zinder_wallet_serving_pair_publisher_generation_wait_total{reason="retention_window"}`
counts each refresh that waited.

A block waits longer when query must first open and admit a serving-pair
generation, which takes about 25 seconds on Testnet. Query opens the first of
its four generations at startup and the second on the first block after it.
It opens the third the first time a block arrives within one minute of the
previous publication, and the fourth the first time three publications fall
within one minute or a long request holds a released pair. With 75-second
blocks, these opens can come long after the start. Each logs
`wallet_serving_pair_publisher_generation_opened` with a `generation` that has
not opened before. An event that repeats an earlier `generation` means a
failed refresh closed it, and the `wallet_serving_pair_publisher_refresh_failed`
warning before it names the error.

### Construction deadlines

Each phase carries its own deadline:

| Phase | Default | Variable |
| --- | --- | --- |
| Canonical construction | 3 hours (`10800`) | `ZINDER_WALLET_SERVING__CANONICAL_CONSTRUCTION_DEADLINE_SECONDS` |
| Wallet construction | 2 hours (`7200`) | `ZINDER_WALLET_SERVING__WALLET_CONSTRUCTION_DEADLINE_SECONDS` |
| Serving pair | 30 minutes | fixed |

A phase that outlives its deadline logs the plane's last `/readyz` body and
exits non-zero, so the reason reaches the deploy logs. The `starting canonical
construction` and `starting wallet construction` lines name the deadline in
effect. A value that is not a positive whole number of seconds stops the
container at startup.

The defaults fit Testnet. They are too short for Mainnet from an empty volume.
One measured Mainnet build from genesis took about four hours for canonical
construction, with staging near 310 GB, and then more than two hours for
wallet construction. A restart discards unpublished construction staging, so
a phase that hits its deadline starts over and never finishes. For a Mainnet
build from an empty volume, set at least:

| Variable | Mainnet value |
| --- | --- |
| `ZINDER_WALLET_SERVING__CANONICAL_CONSTRUCTION_DEADLINE_SECONDS` | `86400` (24 hours) |
| `ZINDER_WALLET_SERVING__WALLET_CONSTRUCTION_DEADLINE_SECONDS` | `43200` (12 hours) |

Raise them further on slower disks or a slower node. Once construction has
published, a restart resumes from the durable fence and the defaults are
enough. The image `HEALTHCHECK` start period covers only the default
deadlines. A Docker host that restarts unhealthy containers needs a longer
start period for a Mainnet build.

## Failure handling

The container terminates with a non-zero status in these cases:

- any process it started exits;
- a plane reports `schema_mismatch` or `node_capability_missing`;
- a plane stops admitting traffic for an hour, except while ingest reports a
  node outage;
- ingest reports `node_unavailable` for 24 hours;
- a plane keeps flapping, as described below; or
- the projector leaves a park that kept query from starting, as described in
  [Wallet transition exceeds its logical-byte limit](#wallet-transition-exceeds-its-logical-byte-limit).

The restart policy is `ALWAYS` with no retry cap, so a surviving subset never
keeps answering requests it cannot satisfy and the platform never stops
restarting a crash loop.

Liveness alone does not prove the topology advances, so the supervisor polls
all three `/readyz` endpoints as well. A canonical writer that meets a reorg
deeper than `ZINDER_INGEST__REORG_WINDOW_BLOCKS` stops following and parks
without exiting, reporting `reorg_window_exceeded`. It keeps serving its
frozen fence to the projector and query. Native query reads the halt from the
writer, drains its own readiness with `reorg_window_exceeded`, and refuses
every wallet request and open `ChainEvents` stream with the typed
`REORG_WINDOW_EXCEEDED` status. A lightwalletd compatibility runtime pointed
at the same writer refuses wallet requests the same way. The container stays up in this state and logs
the halt once: restarting would park the writer on the same reorg and replace
the typed refusal with refused connections. With the default 1000-block window,
the node itself cannot produce such a reorg, so look for a replaced, resynced,
or misbehaving node. Confirm the authoritative branch, then rebuild or restore a
coherent state bundle before returning the service to traffic.

Health checks follow the same rule. The image `HEALTHCHECK` runs
`zinder-wallet-serving-healthcheck`, which passes when query `/readyz` answers
200 or reports `reorg_window_exceeded`, so a Docker or Compose host that
restarts unhealthy containers keeps the typed refusal up instead of restarting
into the same park. It also passes while the projector reports the
`wallet_transition_limit_exceeded` park described in
[Wallet transition exceeds its logical-byte limit](#wallet-transition-exceeds-its-logical-byte-limit),
whether query answers behind it or never started.

Railway's deploy-time healthcheck is different. It uses `/healthz`, which
passes once the query endpoint listens, so a parked writer does not fail a
deployment. Query `/readyz` still reports `reorg_window_exceeded`, which is why
monitoring watches it. Do not redeploy while the writer is parked except to
ship the reconciliation, such as a restored state bundle or a rebuild. A
redeploy restarts all three processes, and ingest parks again on the same
reorg. Because the service owns a volume, Railway stops the running deployment
before it starts the new one, so the redeploy also ends the deployment that was
serving the typed refusal. A plain restart or a crash restart under the
`ALWAYS` policy comes back serving the halt.

Sync lag and transient faults drain readiness but clear on their own, so a
plane that stays unready is tolerated for an hour before the container
terminates.

A node outage is tolerated much longer. While ingest reports
`node_unavailable` or `upstream_not_ready`, query keeps serving the stored
chain, and a restart cannot help: query and the projector start without the
node, but ingest needs it to start, so the restarted container would serve
nothing. The supervisor keeps the outage out of the one-hour tolerance and the
recurrence budgets below. While Zebra resyncs, or while only the node's
mempool feed fails, ingest can report `syncing` instead, because it trails the
node or its mempool cannot hydrate. The supervisor treats that as the same
outage when query's `/readyz` lists `ingest_control` with
`upstream_not_ready`, which is the writer's own observation of its node. An
`upstream_not_ready` node answers, so a resync of any length never restarts
the container. When query lists `ingest_control` with `node_unavailable`
instead, the writer's mempool is waiting for a node snapshot. The supervisor
counts that as an unreachable node only while ingest's `syncing` cause carries
`"lag_blocks": null` and ingest reports a `current_height` at or above its
`target_height`, which means canonical ingest has caught up and only the
mempool is behind. A numeric `lag_blocks`, or a `current_height` below
`target_height`, means the writer trails a node it can reach, and its mempool
waits for it, so the one-hour tolerance and the recurrence budgets still
apply. An unreachable node, including one that only
the writer's mempool cannot reach, is logged once when the outage begins and
once when it ends, and terminates the container only after it has lasted 24
hours, in case ingest is stuck in its own node client.

A plane that flaps between ready and unready never stays down for an hour, so
the supervisor also tracks its unready stretches. It terminates the container
after six episodes, or after 900 seconds unready in total, unless the plane
first stays ready for ten minutes in a row. An unready stretch counts as an
episode in two cases:

- the plane's `/readyz` stops answering, which counts at once; or
- the plane answers with an unready cause for at least 60 seconds.

A shorter answered drain is not an episode, but its time still counts toward
the 900 seconds and it restarts the ten-minute clock. An occasional brief
drain therefore never restarts the container, while a plane that is unready
most of the time in short bursts still does. The supervisor logs each short
drain as `recovered after <n>s unready, too brief to count as an episode`,
with the unready total so far.

For Testnet, configure Zebra with `ready_max_blocks_behind = 4` and
`ready_max_tip_age = "5m"`. The four-block limit is the bounded correction for
an observed `lag=3` transition during a canonically current 225-second block
gap, leaving one estimator block of margin. It is not an arithmetic equivalent
of the five-minute limit: Zebra derives estimated lag from the best tip's
header timestamp, while tip age measures elapsed time since the local height
increased.

Before release, record the local time when each new `bestblockhash` is first
observed, its `getblockheader` `time`, and every `/ready` transition through an
ordinary gap. Reject the four-block calibration if the measured header-time
offset makes estimated lag exceed it before the local five-minute tip-age
boundary while `blocks == headers`; select a new bounded threshold and repeat
the failure matrix. Keep peer, sync-status, tip-age, and lag checks enabled; do
not make `/ready` unconditional on Testnet.

Railway sends `SIGTERM` on redeploy and stop. The entrypoint forwards it to the
query, projector, and ingest processes in that order and waits twenty seconds,
inside Railway's thirty-second drain window. It exits zero once all three stop,
and non-zero naming the survivors if any still holds a RocksDB instance open
when the window expires.

### Serving pair missing a table file

Query logs `wallet_serving_pair_table_file_missing` at warning level when a
read finds that the writer's or projector's compaction deleted a table file
of its serving pair. The warning's `error` field includes the RocksDB status,
which names the missing `.sst` path under the canonical or the wallet store.
You do not need to act on an occasional warning. Query has already handled it:

- it marked the pair and published a replacement at the writer's fence;
- the read ran once more on the replacement; and
- `zinder_wallet_serving_missing_table_file_pairs_total` counted the pair
  once.

The container does not restart, and readiness stays ready unless the refresh
that prepares the replacement fails. When that candidate hits another deleted
file, during its RocksDB open or catch-up or during the admission checks that
follow them, readiness reports `serving_pair_stale`, which still admits
traffic. Any other candidate failure refuses traffic until a later refresh
succeeds. Both log `wallet_serving_pair_publisher_refresh_failed`.

Investigate when the warnings follow most publications, or when clients keep
receiving `STORAGE_UNAVAILABLE` for minutes after one. Both mean the primary
replaces files faster than query replaces pairs, or the replacement cannot be
published. Check the query log for `wallet_serving_pair_publisher_refresh_failed`
warnings, and check
`zinder_wallet_serving_pair_publisher_generation_wait_total` for refreshes
that found no free generation. Then look for anything that rewrites the
primary's files in bulk, such as a manual compaction. Restarting the service
also clears the condition, because query reopens every generation, but query
replaces the pair on its own once a generation is free.

### Wallet transition exceeds its logical-byte limit

The projector logs `wallet_projection_following_parked` once, at error level,
with an error that begins:

```text
wallet following stopped planning a transition that already needs <n> logical bytes, more than projector.follow.max_transition_logical_bytes (ZINDER_PROJECTOR__FOLLOW__MAX_TRANSITION_LOGICAL_BYTES) allows (<limit>)
```

One wallet transition, typically a deep reorg that rolls back and replays many
blocks of a busy address, touches more wallet rows than the limit allows. The
limit counts each row the transition touches twice, once for the plan in memory
and once for the write batch that publishes it. The projector refuses the
transition before it writes anything, so the wallet is unchanged, and `<n>` is
a lower bound on what the whole transition needs.

The projector stays up, parked, and its `/readyz` reports both numbers:

```text
{"status":"not_ready","cause":{"wallet_transition_limit_exceeded":{"required_bytes":<n>,"configured_bytes":<limit>}},"current_height":<wallet height>,"target_height":<writer height>}
```

While it is parked:

- it keeps renewing its one canonical retention lease, so the canonical writer
  keeps every retained event from the wallet cursor on, and the wallet later
  resumes from that cursor without a rebuild;
- it plans the transition again only after the writer commits, and never
  repeats the refused transition's canonical replay and wallet reads;
- it resumes on its own only if a later reorg replaces the refused blocks, and
  that reconciliation is also one transition under the same limit; and
- a query that was already running keeps answering wallets from its last
  serving pair for a few blocks, reports `serving_pair_stale`, and once its
  staleness ceiling passes refuses them with `SERVICE_NOT_READY` and readiness
  cause `replica_lagging`.

The container stays up in this state, and the supervisor logs the park once.

A restart with the same setting, such as a crash restart, a plain restart, or
a host reboot, parks the projector on the same transition again. Query cannot
publish its first serving pair behind the parked wallet, so its startup would
fail, and the supervisor does not start it. The supervisor logs
`zinder-query was not started`, and the container stays up with ingest and the
parked projector. Wallets get refused connections until the projector
recovers, and the image `HEALTHCHECK` still passes. If the projector later
leaves the park on its own, the supervisor terminates the container, and the
restart starts query behind the moving wallet.

To recover, set `ZINDER_PROJECTOR__FOLLOW__MAX_TRANSITION_LOGICAL_BYTES` on
the service well above `<n>`, up to the maximum of `1073741824`, and redeploy.
The default is `536870912`. While the projector plans and publishes one
transition, its resident memory grows by about 0.7 to 1.5 times the bytes the
transition accounts, depending on the blocks it replaces. Raise the
container's memory by at least 1.5 times the bytes you add to the limit.

Measured on Testnet with `zinder-bench rocksdb-tip-reorg`, a 1,000-block reorg
needs:

- 350 MB of the limit over heights 4,617,272 to 4,618,271, which spend about
  480,000 transparent outputs, and the transition grows resident memory by
  0.24 to 0.33 GB, 0.7 to 0.9 times the accounted bytes;
- 796 MB over heights 4,601,674 to 4,602,673, which create about one million
  transparent outputs, so the default refuses it, and the transition grows
  resident memory by 1.09 to 1.18 GB, 1.4 to 1.5 times the accounted bytes.

If the new limit is still too small, the projector parks again and query never
starts, so `/healthz` never answers. Railway then marks the deployment failed
when the healthcheck window expires and stops the container. Raise the limit
further and redeploy.

If `<n>` already exceeds `1073741824`, or the failure repeats at the maximum,
no setting admits the transition. Rebuild the wallet in a second wallet-serving
service with its own volume, wait for its `/readyz`, move traffic to it, and
then retire this service. Do not attach the second service to this volume.

## Serving the Explorer from the same volume

The `zinder-wallet-serving-explorer` image runs this topology plus
`zinder-explorer`, which serves the native `ExplorerQuery` protocol for
explorer consumers such as Zexplorer. The Explorer opens the materialized-view
store that `zinder-ingest` writes as a RocksDB secondary, so it must run on the
writer's volume. On a host that attaches one volume to one container, it must
run in the same container.

Each release publishes the image for Linux amd64 and arm64:

```text
ghcr.io/zcashfoundation/zinder-wallet-serving-explorer:vX.Y.Z
```

The image is the `zinder-wallet-serving-explorer` target of
`deploy/Dockerfile`. It differs from `zinder-wallet-serving` in two ways:

- Its ingest configuration sets `ingest.explorer_views = true`, so ingest also
  writes the Explorer views.
- Its entrypoint starts `zinder-explorer` with
  `deploy/config/wallet-serving/explorer.toml` after wallet serving is up.

Its `HEALTHCHECK` is the wallet-serving check alone and does not cover the
Explorer.

Configure the service exactly as described above, with the same volume,
variables, and Railway settings. `PORT` stays `9106`, and the Railway
healthcheck path stays `/healthz` on the query process. Provision memory
beyond the 8 GB minimum for the Explorer process. Its budget has not been
measured.

The Explorer views live on the same volume, beneath
`/var/lib/zinder/canonical/materialized-views`, so size the volume for them on
top of the wallet-serving needs. Their disk budget has not been measured.
Ingest pauses view replay while the volume has less available space than
`ZINDER_INGEST__EXPLORER_VIEWS_MIN_FREE_BYTES`, 10 GiB by default, and logs
`materialized_view_replay_volume_space_low`. Wallet serving keeps running, but
the Explorer views stop advancing until the volume grows or space is freed.
Watch for that event, and see
[Keep Explorer views from filling the volume](deploying-on-a-vm.md#keep-explorer-views-from-filling-the-volume).

The Explorer reads only these variables:

- `ZINDER_NETWORK__*`, `ZINDER_NODE__*`, and `ZINDER_NODE_FALLBACKS__*`, which
  it shares with the wallet-serving processes. The Explorer validates the
  fallback list but reads only `[node]`.
- `ZINDER_EXPLORER__*`, such as `ZINDER_EXPLORER__BEARER_TOKEN_PATH`. The
  entrypoint delivers these variables to the Explorer alone.

Its storage paths, listen addresses, and bind policy are fixed in its
configuration file. Indexer TLS PEM variables become files before either side
starts, so the Explorer receives the same `*_PATH` variables as the other
processes.

### Explorer startup

The entrypoint first checks the Explorer configuration with
`zinder-explorer --print-config`. A rejected configuration, such as an
unreadable `ZINDER_EXPLORER__BEARER_TOKEN_PATH`, ends the container before
wallet serving starts, so the deployment fails at once instead of hours later.

The entrypoint then starts the wallet-serving entrypoint and waits until
`zinder-wallet-serving-healthcheck` passes, which happens when query `/readyz`
answers 200 or reports `reorg_window_exceeded`, or while the projector reports
the `wallet_transition_limit_exceeded` park. Behind a park that kept query from
starting, the Explorer's reads that compose wallet query fail until query
starts. What happens next depends on whether ingest writes the Explorer views:

- With the views enabled, as this image configures them, the entrypoint waits
  until ingest `/readyz` on port 9105 lists `materialized_view_identities`,
  which ingest reports once it admits the Explorer view store. The Explorer
  opens that store only when it starts, so it would otherwise serve without
  its view-backed capabilities. After 10 minutes without admission, the
  entrypoint logs the wait and starts the Explorer anyway.
- With `ZINDER_INGEST__EXPLORER_VIEWS=false`, ingest never admits the view
  store, so the entrypoint logs `ingest Explorer views are disabled` and starts
  the Explorer at once. The Explorer serves an existing view store as it last
  stood, and `ExplorerFreshness` reports how far it trails the chain. Disable
  the views this way to stop them from growing, for example while the volume
  is grown.

The deploy log then shows `starting zinder-explorer`. The Explorer composes
transaction, block, search, and mempool reads through the container's own
query endpoint at `127.0.0.1:9102`.

The Explorer views need genesis-complete canonical history. The
wallet-serving coverage provides it. A volume built by `zinder-wallet-serving`
has no Explorer view store yet. On such a volume, ingest creates the store
beneath the canonical path and builds the views from canonical heights while it
keeps following the chain. Each Explorer response reports how far the views
trail the chain in `ExplorerFreshness`.

### Rebuild the Explorer views

A view store that another release built can fail admission. Ingest then logs
`materialized_view_store_open_failed` with `ConsumerNotDeclared` or
`ExplorerViewManifestRequiresFreshStore` and keeps serving wallets without
Explorer views. The Explorer exits with the same error on every start, and
the entrypoint keeps restarting it. Neither process changes the store.

This image fixes the store path at
`/var/lib/zinder/canonical/materialized-views`. To rebuild the views:

1. Record the capabilities `ExplorerQuery.ServerInfo` advertised before the
   upgrade. They are the coverage the rebuilt store must reach again.
2. Open a shell in the running container, for example with `railway ssh`.
   Neither process holds the refused store open.
3. Move the store aside on the same volume:
   `mv /var/lib/zinder/canonical/materialized-views /var/lib/zinder/materialized-views.previous`.
4. Restart the deployment. Ingest creates a store with the current manifest and
   rebuilds the views from canonical history while wallet serving continues.
5. Delete `/var/lib/zinder/materialized-views.previous` only after
   `ExplorerQuery.ServerInfo` reports zero `lag_blocks` and advertises every
   capability recorded in step 1, as
   [Rebuild Explorer views after a manifest change](deploying-on-a-vm.md#rebuild-explorer-views-after-a-manifest-change)
   describes.

### Explorer ports

| Port | Surface | Reachability |
| --- | --- | --- |
| 9068 | native `ExplorerQuery` gRPC | Railway private network |
| 9069 | Explorer operational HTTP | Railway private network |

Both bind `[::]` because the Railway private network is IPv6-only. Clients reach
`<service>.railway.internal:9068`. Do not attach a public domain to 9068. Like
9102, the surface is plaintext.

### Explorer failure handling

Watch Explorer `/readyz` on port 9069 in monitoring. Neither the Railway
deploy-time healthcheck nor the container's `HEALTHCHECK` covers it, so a host
that restarts unhealthy containers never restarts wallet serving because the
Explorer is down.

An Explorer failure does not stop wallet serving. When `zinder-explorer`
exits, the entrypoint logs its status and restarts it after 15 seconds. The
delay doubles on each further exit, up to 5 minutes, and returns to 15 seconds
after a run that lasts 5 minutes.

The container terminates with a non-zero status when the wallet-serving
entrypoint exits for any reason in [Failure handling](#failure-handling). The
entrypoint stops the Explorer first, and the restart policy then restarts the
complete set.

On `SIGTERM`, the entrypoint forwards the signal to the Explorer and to the
wallet-serving entrypoint. It waits up to twenty-five seconds for both to stop,
inside Railway's thirty-second drain window. It exits non-zero when either
stops with a failure status, such as a wallet-serving process that outlived
its own drain window.

## Boundaries

- Do not label this target as the complete version-1 production topology; it
  omits `zinder-compat-lightwalletd`.
- Do not attach a second service to this volume. The canonical and wallet
  primaries have exactly one owner process each.
- Do not override storage paths, listen addresses, control endpoints, or
  bearer token paths through `ZINDER_*` variables.
- Do not expose 9100, 9101, 9105, or 9110 beyond the container.
- Do not attach a public domain to 9102 or 9068. The native surfaces are
  plaintext; public exposure requires TLS, authentication, and quotas at a
  reverse proxy.

See [Service boundaries](../architecture/service-boundaries.md) and
[Public interface configuration](../architecture/public-interfaces.md#configuration).
