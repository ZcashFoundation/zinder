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
staging directory beside the wallet store, so leave headroom for that.

Provision at least 8 GB of memory. All three processes share one memory cgroup,
so the baked configs pin every budget that would otherwise be derived from it:

| Process | Bounded budget | Largest term |
| --- | --- | --- |
| `zinder-ingest` | ~2.6 GiB | canonical and materialized-view writer stores |
| `zinder-projector` | ~2.7 GiB while building, ~1.2 GiB while following | wallet-build sorters |
| `zinder-query` | ~0.4 GiB per open serving pair, up to ~1.4 GiB | canonical secondary block cache |

Query keeps up to four serving pairs open, each with its own block caches: the
current pair, the candidate it is catching up, and two replaced pairs that
either still answer pinned reads or are held by a long request. The remainder covers allocator
overhead, gRPC buffers, and task stacks. In a
smaller container the cgroup OOM-killer takes one process during construction.
That terminates the whole container, and the restart resumes construction from
the last durable fence only to be killed again.

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
stream, ingest follows the node mempool by polling JSON-RPC. It then rebuilds
its live mempool index from a full re-read after every block, and its
readiness drops until the rebuild finishes. At the tip, that drain repeats
with every block.

These are optional:

| Variable | Effect |
| --- | --- |
| `ZINDER_WALLET_SERVING__CANONICAL_CONSTRUCTION_DEADLINE_SECONDS` | Seconds canonical construction may take before the container stops. Defaults to `10800` (three hours). See [Construction deadlines](#construction-deadlines). |
| `ZINDER_WALLET_SERVING__WALLET_CONSTRUCTION_DEADLINE_SECONDS` | Seconds wallet construction may take before the container stops. Defaults to `7200` (two hours). See [Construction deadlines](#construction-deadlines). |
| `ZINDER_INGEST__REORG_WINDOW_BLOCKS` | Sets the canonical replacement depth, applied identically to the projector and the query surface. Defaults to 1000, Zebra's finalization depth. Raising it on an existing deployment, including by upgrading to this default, raises the stores in place and irreversibly; capture a state bundle first if you need a rollback path. Lowering it requires a rebuild. |
| `RUST_LOG` | Log filter, such as `info,zinder=info`. |
| `INGEST_CONTROL_BEARER_TOKEN` | Ingest-control bearer secret. Generated at boot when unset. |
| `CHECKPOINT_BEARER_TOKEN` | Checkpoint bearer secret. Generated at boot when unset. |

You can also set any variable in the
[environment-variable reference](../architecture/public-interfaces.md#environment-variable-reference)
whose "Used by" column names `zinder-ingest`, `zinder-projector`, or
`zinder-query`. Each process rejects a `ZINDER_*` section that its binary does
not define. For that reason, the entrypoint gives each process only the
sections its binary defines:

| Section | `zinder-ingest` | `zinder-projector` | `zinder-query` |
| --- | --- | --- | --- |
| `ZINDER_NETWORK__*`, `ZINDER_NODE__*`, `ZINDER_OPS__*`, `ZINDER_SECURITY__*` | yes | yes | yes |
| `ZINDER_STORAGE__*`, `ZINDER_INGEST_CONTROL__*` | yes | yes | yes |
| `ZINDER_INGEST__*`, `ZINDER_RETENTION__*` | yes | | |
| `ZINDER_PROJECTOR__*`, `ZINDER_PROJECTOR_CONTROL__*` | | yes | |
| `ZINDER_WALLET__*` | | yes | yes |
| `ZINDER_QUERY__*` | | | yes |

For example, `ZINDER_PROJECTOR__BUILD__MAX_TEMPORARY_FILE_BYTES_PER_SORTER`
reaches only `zinder-projector`. A variable in a shared section reaches every
process that defines it, so
`ZINDER_STORAGE__CANONICAL__ROCKSDB__MAX_OPEN_FILES` applies to all three.

No process receives the `ZINDER_WALLET_SERVING__*` variables, which configure
the entrypoint itself, or the two bearer secrets. At startup the entrypoint
logs the names of any other `ZINDER_*` variables that no process reads, such
as a misspelled section. Check that line after you add a variable.

Storage paths, listen addresses, control endpoints, and bearer token paths are
fixed in `deploy/config/wallet-serving/`. Do not override them: the shared
sections would apply one value to all three processes.

The wallet-serving ingest configuration sets
`ingest.explorer_views = false` explicitly. Wallet projection and
wallet-serving readiness do not depend on the optional materialized-view plane;
an existing materialized-view directory is left untouched. To serve Explorer
views on the same host, use the documented Explorer Compose overlay, which
explicitly overrides Explorer views to `true` and requires a materialized-view
store that records this release's exact Explorer view manifest.

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
because it returns 503 with cause `upstream_not_ready` while the Zebra node is
behind its estimated network tip, even when Zinder has indexed everything the
node has committed. A lagging node would then fail the deployment without any
Zinder fault.

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

- any of the three processes exits;
- a plane reports `schema_mismatch` or `node_capability_missing`;
- a plane stops admitting traffic for an hour; or
- a plane keeps flapping, as described below.

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
into the same park.

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

Upstream outages and sync lag drain readiness the same way but clear on their
own, so they are tolerated for an hour before the container terminates.

A plane that flaps between ready and unready never stays down for an hour, so
the supervisor also counts flap episodes. It terminates the container after
six episodes, or after 900 seconds unready across episodes, unless the plane
first stays ready for ten minutes. An unready stretch counts as an episode in
two cases:

- the plane's `/readyz` stops answering, which counts at once; or
- the plane answers with an unready cause for at least 60 seconds.

A shorter answered drain, such as the brief one at a new block, does not
count. The supervisor logs each one as `recovered after <n>s unready, too
brief to count toward the recurrence budget`.

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

## Boundaries

- Do not label this target as the complete version-1 production topology; it
  omits `zinder-compat-lightwalletd`.
- Do not attach a second service to this volume. The canonical and wallet
  primaries have exactly one owner process each.
- Do not override storage paths, listen addresses, control endpoints, or
  bearer token paths through `ZINDER_*` variables.
- Do not expose 9100, 9101, 9105, or 9110 beyond the container.
- Do not attach a public domain to 9102. The native surface is plaintext;
  public exposure requires TLS, authentication, and quotas at a reverse proxy.

See [Service boundaries](../architecture/service-boundaries.md) and
[Public interface configuration](../architecture/public-interfaces.md#configuration).
