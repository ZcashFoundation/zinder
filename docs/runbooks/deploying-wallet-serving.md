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

## Building the image

Hosts with stage selection build the runtime stage directly:

```bash
docker build -f deploy/Dockerfile.wallet-serving \
    --target zinder-wallet-serving-runtime .
```

Hosts that always build a file's final stage land on an admission guard
instead and must present the target as a build argument:

```text
RAILWAY_DOCKER_TARGET_STAGE=zinder-wallet-serving-runtime
```

On Railway, `deploy/railway.wallet-serving-runtime.toml` builds
`deploy/Dockerfile.wallet-serving`; see [Deploy on Railway](#deploy-on-railway)
for how the service selects it. The closed default target fails the build.

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
| `zinder-query` | ~0.4 GiB | canonical secondary block cache |

The remainder covers allocator overhead, gRPC buffers, and task stacks. In a
smaller container the cgroup OOM-killer takes one process during construction.
That terminates the whole container, and the restart resumes construction from
the last durable fence only to be killed again.

Set these variables on the service (node addresses shown for Railway's
private network; substitute your host's):

| Variable | Value |
| --- | --- |
| `RAILWAY_DOCKER_TARGET_STAGE` | `zinder-wallet-serving-runtime` |
| `PORT` | `9106` |
| `ZINDER_NETWORK__NAME` | `zcash-testnet` or `zcash-mainnet` |
| `ZINDER_NODE__JSON_RPC_ADDR` | `http://zebra.railway.internal:18232` |
| `ZINDER_NODE__AUTH__METHOD` | `cookie` |
| `ZINDER_NODE__AUTH__COOKIE` | the inline `user:password` cookie string |
| `ZINDER_PROJECTOR__BUILD_OWNER_HEX` | a stable 32-character hexadecimal value |
| `ZINDER_INGEST__EXPLORER_VIEWS` | `false` |

These are optional:

| Variable | Effect |
| --- | --- |
| `ZINDER_NODE__INDEXER_GRPC_ADDR` | Enables the streaming mempool source and chain-tip wakeups, such as `http://zebra.railway.internal:8155`. |
| `ZINDER_INGEST__REORG_WINDOW_BLOCKS` | Sets the canonical replacement depth, applied identically to the projector and the query surface. Defaults to 1000, Zebra's finalization depth. Raising it on an existing deployment raises the stores in place; lowering it requires a rebuild. |
| `RUST_LOG` | Log filter, such as `info,zinder=info`. |
| `INGEST_CONTROL_BEARER_TOKEN` | Ingest-control bearer secret. Generated at boot when unset. |
| `CHECKPOINT_BEARER_TOKEN` | Checkpoint bearer secret. Generated at boot when unset. |

Only the `[network]` and `[node]` fields are shared by all three processes.
Every process rejects a `ZINDER_*` variable naming a section it does not own,
and the entrypoint delivers the two role-scoped variables above to their owner
alone. Add no other role-scoped variable. Storage paths, listen addresses,
control endpoints, and bearer token paths are fixed in
`deploy/config/wallet-serving/`.

The wallet-serving ingest configuration sets
`ingest.explorer_views = false` explicitly. Wallet projection and
wallet-serving readiness do not depend on the optional materialized-view plane;
an existing materialized-view directory is left untouched. To serve Explorer
views on the same host, use the documented Explorer Compose overlay, which
explicitly overrides Explorer views to `true` and requires the matching
persisted preset.

`PORT` names the port Railway healthchecks. It must stay `9106`, the
operational endpoint of the query process.

## Deploy on Railway

Railway reads `railway.toml` at the repository root unless the service names
another file. That default builds the ingest-only canary image, so set the
config-as-code path once on the service settings page:

```text
/deploy/railway.wallet-serving-runtime.toml
```

The same setting is `railwayConfigFile` on the service API. Until it is set,
every deploy of this service builds `deploy/Dockerfile.railway-nocache` and
fails its target-admission guard.

Then deploy the exact reviewed revision and name it in the message:

```bash
railway up \
  --service zinder-wallet-serving \
  --environment production \
  --detach \
  --json \
  --message "<subject> (<commit>)"
```

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
serves nothing until both complete. Canonical construction has a three-hour
production gate and wallet construction has a two-hour gate, so several hours
without a serving surface is the expected first-deploy shape, not a stall.

Each phase carries its own deadline: three hours for canonical construction,
two for wallet construction, thirty minutes for the serving pair. An expired
phase logs the plane's last `/readyz` body and exits non-zero, so the reason
reaches the deploy logs before Railway's six-hour healthcheck window closes.
The volume is retained and the next attempt resumes from the durable fence
rather than starting over.

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

## Failure handling

The container terminates with a non-zero status when any of the three processes
exits, when a plane reports `schema_mismatch` or `node_capability_missing`, or
when a plane stops admitting traffic for an hour. The restart policy is `ALWAYS` with no
retry cap, so a surviving subset never keeps answering requests it cannot
satisfy and the platform never stops restarting a crash loop.

Liveness alone does not prove the topology advances, so the supervisor polls
all three `/readyz` endpoints as well. A canonical writer that meets a reorg
deeper than `ZINDER_INGEST__REORG_WINDOW_BLOCKS` stops following and parks
without exiting, reporting `reorg_window_exceeded`. It keeps serving its
frozen fence to the projector and query. Native query reads the halt from the
writer, drains its own readiness with `reorg_window_exceeded`, and refuses
every wallet request and open `ChainEvents` stream with the typed
`REORG_WINDOW_EXCEEDED` status. The container stays up in this state and logs
the halt once: restarting would park the writer on the same reorg and replace
the typed refusal with refused connections. With the default 1000-block window,
the node itself cannot produce such a reorg, so look for a replaced, resynced,
or misbehaving node. Confirm the authoritative branch, then rebuild or restore a
coherent state bundle before returning the service to traffic.

Upstream outages and sync lag drain readiness the same way but clear on their
own, so they are tolerated for an hour before the container terminates.

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
- Do not set role-scoped `ZINDER_*` variables beyond the two the entrypoint
  scopes to their owner.
- Do not expose 9100, 9101, 9105, or 9110 beyond the container.
- Do not attach a public domain to 9102. The native surface is plaintext;
  public exposure requires TLS, authentication, and quotas at a reverse proxy.

See [Service boundaries](../architecture/service-boundaries.md) and
[Public interface configuration](../architecture/public-interfaces.md#configuration).
