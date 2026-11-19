# Service operations

This page explains how Zinder runtimes start and stop, what you must keep
private, how a deployment recovers, and which deployment shapes are
supported. It is for operators planning a deployment and for contributors
changing runtime lifecycle code. Readiness causes are listed in
[Readiness](../reference/readiness.md), and metrics in
[Metrics](../reference/metrics.md).

## Startup and shutdown

### Startup phases

Every runtime except `zinder-projector` reports its startup through the same
named phases and skips the phases that do not apply to it. The projector
reports no startup phases, so use its `/readyz` instead. Each phase logs an
entry event and an exit event under the `zinder::startup` target, with `phase`,
`phase_state`, and an `outcome` of `ok`, `failed`, or `aborted`. The
`zinder_startup_phase_duration_seconds` histogram records the same phases.

| Phase | What happens |
| --- | --- |
| `load_config` | Merge defaults, the config file, environment variables, and flags. |
| `connect_node` | Ingest only: build the node clients from configuration. |
| `check_schema` | Ingest only: check each node's capabilities, archive storage, genesis block, and network-upgrade table. |
| `admit_ingest_control` | Readers that need it: authenticate to ingest control and check its identity, network, and contract revision. |
| `open_storage` | Readers only: open their read-only store copies. Ingest opens its store after `ready`. |
| `recover_state` | Replay or rebuild in-flight state. |
| `start_api` | Bind the gRPC and operational listeners. |
| `ready` | Startup is complete. |

Only ingest contacts a node during startup. The projector, `zinder-query`,
and `zinder-compat-lightwalletd` open storage with the network-upgrade
schedule that ingest admitted. They therefore start while the node is down or
before it has written its cookie file. Ingest needs a reachable node to start.

### Startup order

Ingest must run first, because it is the only writer of the canonical store
and the only source of writer status. The reference Compose topology starts
the runtimes in this order:

1. `zinder-ingest`.
2. `zinder-projector`, once ingest is healthy.
3. `zinder-query` and `zinder-compat-lightwalletd`, once the projector is
   healthy and the ingest container has started.

A query or compatibility runtime that starts behind a parked projector cannot
publish its first serving pair, so it fails before it listens. The
single-volume entrypoint therefore starts neither reader while the projector
reports that park.

A restart during a first sync starts the interrupted build over; see
[Initial sync](../runbooks/initial-sync.md#restarts-during-the-first-sync).

### Shutdown

On a termination signal, a runtime reports `shutting_down` at once and stops
accepting new requests. It then cancels background tasks, waits for its
servers and owned tasks, and closes its operational endpoint. Only the owner
of a store closes it, and readers make no final writes.

`zinder-query` and `zinder-compat-lightwalletd` publish `shutting_down`
before their gRPC listener starts to drain, so `/readyz` and their
[gRPC health service](../reference/readiness.md#grpc-health) report
`NOT_SERVING` together, and gRPC load balancers and Kubernetes `grpc` probes
stop routing to them while the drain runs. Open health `Watch` streams then
end, so they do not hold the drain open.

`zinder-query` ends open server streams, such as `ChainEvents` and
`MempoolEvents`, with `UNAVAILABLE` and `SERVICE_NOT_READY`, because event
streams never end on their own. `zinder-query` and
`zinder-compat-lightwalletd` each bound their gRPC drain at 10 seconds. Calls
still open then, such as a block-range stream to a client that reads slowly,
are abandoned. The single-volume entrypoint allows its processes 20 seconds to
exit.

### Background task failure

Each background task of `zinder-query` and `zinder-compat-lightwalletd` runs
until shutdown cancels it, so a task that ends earlier has failed. A task that
ended quietly would leave the server answering from that task's last state:
readiness would stop tracking its dependency, and a serving pair published as
ready would stay ready past `storage.serving_pair_staleness_ceiling_ms`.

Each runtime therefore watches its tasks next to its gRPC server. When one
exits, or the server stops without a shutdown request, the runtime reports
`shutting_down`, cancels the other tasks, drains the server, and exits with an
error that names the task. It leaves the restart to the platform. The server
drains before the serving generations are released, so a retired generation
is never torn down under a live reader. A requested shutdown follows the same
order and exits successfully.

| Runtime | Watched tasks |
| --- | --- |
| `zinder-query` | Serving-pair publisher, node-readiness probe, ingest-control readiness probe, and operational endpoint |
| `zinder-compat-lightwalletd`, wallet serving | Serving-pair publisher, node-readiness probe, ingest-control readiness probe, tip-change publisher, and node-tip witness |
| `zinder-compat-lightwalletd`, compact-blocks serving | Compact-block publisher and node-tip witness |

The compatibility runtime closes its operational endpoint after the server
stops, and does not watch it.

## Memory allocator

On Linux, every service binary allocates through jemalloc, and jemalloc also
serves the C `malloc` that RocksDB calls. Tune it with `MALLOC_CONF`. Glibc
variables such as `MALLOC_ARENA_MAX` have no effect.
`MALLOC_CONF=stats_print:true` prints allocator statistics when the process
exits, and `MALLOC_CONF=background_thread:true` returns freed memory to the
kernel while the process is idle. [ADR-0005](../adrs/0005-bounded-process-memory.md) records why.

## Security boundary

Zinder protects the integrity of the chain data it stores and serves.
Transport security belongs to the proxy in front of it. Every Zinder listener
serves plaintext, and your proxy terminates TLS and enforces authentication,
rate limits, and quotas. Certificate provisioning, rotation, and client
policy therefore stay in your proxy, as
[ADR-0011](../adrs/0011-ingest-control-transport-security.md) records. Before
you expose a deployment, do three things:

1. **Put the wallet listeners behind your proxy.** The native wallet gRPC
   listener (`zinder-query`), the lightwalletd listener
   (`zinder-compat-lightwalletd`), and the Cipherscan REST adapter serve
   plaintext without client authentication. The Explorer listener serves
   plaintext and can require one shared bearer token
   (`explorer.bearer_token_path`).
   [Exposing over TLS](../runbooks/exposing-over-tls.md) shows one setup.
2. **Keep the operational endpoint private.** `/healthz`, `/readyz`, and
   `/metrics` serve without authentication. The endpoint accepts at most 64
   connections, so any client that reaches it can make your health probes
   and metric scrapes fail. Bind `ops.listen_addr` to loopback or to a
   private network that your orchestrator and metrics scraper alone reach.
3. **Keep the control endpoints private.** The ingest control endpoint serves
   writer status, chain events, and the mempool to readers. Ingest refuses a
   non-loopback `ingest_control.listen_addr` without
   `ingest_control.bearer_token_path`. The token travels in plaintext, so keep
   the endpoint on loopback, as the reference topology does. The projector
   control endpoint binds to loopback and always requires a bearer token.

Zinder refuses to bind any listener to `0.0.0.0`, `::`, or a public address
unless you set `security.allow_public_bind = true`. Loopback and private
ranges are always allowed. Setting the flag logs a warning. The flag permits
the bind, and the protection comes from your proxy and network.

Zinder's own listener bounds, listed in
[Configuration](../reference/configuration.md#listener-and-request-limits),
count all clients together, so enforce per-client limits in your proxy.

Bearer tokens are read from files. Configuration output, such as
`--print-config`, shows redaction markers in place of secrets, and errors
name only the field or the path. Log events that carry a node URL replace its
user information, query, and fragment with `[REDACTED]`.

### Runtime images

Each runtime image runs its one binary as UID and GID 10001 on a
digest-pinned `gcr.io/distroless/cc-debian12:nonroot` base. The image has no
shell, package manager, or HTTP client. The
single-volume images keep a Debian base with a shell, because their
entrypoint scripts need one.
[ADR-0022](../adrs/0022-release-artifacts.md) records why.

The Zebra and Zakura images also run as UID 10001 and write the node's RPC
cookie with mode 0600, so a Zinder container that mounts the cookie reads it
directly. The reference Compose file requires the node to run as UID 10001.
Its `state-init` service gives the data volume to UID 10001 on every start, so
a Zinder container under another UID cannot write its stores. In an
orchestrator other than Compose, give mounted volumes to UID 10001 yourself,
for example with Kubernetes `fsGroup: 10001`.

The runtime images listen on every container interface. Their default
configuration sets each serving and operational listener to `[::]` and sets
`security.allow_public_bind = true`, as
[Configuration](../reference/configuration.md) lists. Publishing a port is
therefore not the only way in:

- On a Docker bridge network, every container on that network can connect to
  the listeners, whether or not you publish their ports. On Linux, the host
  can too, through the container's address.
- With host networking, `[::]` binds every interface of the host.

To restrict who can connect, run the containers on a dedicated network that
holds only the services that need them, and do not use host networking.
Publish ports on `127.0.0.1` unless a proxy that terminates TLS and
authorization sits in front of them. To keep a listener on container
loopback, set its address, for example
`ZINDER_OPS__LISTEN_ADDR=127.0.0.1:9106`. The ingest control listener keeps
its `127.0.0.1:9100` default.

To inspect a running container, run its binary with `docker exec`, as
[Inspect a runtime container](../runbooks/deploying-on-a-vm.md#inspect-a-runtime-container)
shows.

## Recovery

Node failures, ingest outages shorter than the serving staleness ceiling, and
ordinary restarts recover on their own. A restart during a first sync starts
that build over. Causes that need an operator, with what to do, are in
[Readiness](../reference/readiness.md#readiness-causes).

### Restore by rebuilding

Zinder restores a deployment by rebuilding it from the node. A rebuild is a
fresh first sync: start a new deployment on empty storage, let ingest and the
projector build from the node, move traffic once its readers report ready,
and then retire the old deployment. A Mainnet first sync takes many hours; see
[Deploying](../runbooks/deploying.md) for measured times. Plan for that time
in your recovery objective.

The projector's private control API can capture a state bundle. A bundle
holds a canonical checkpoint and a wallet checkpoint bound to the same
canonical position, with a manifest. Recover by rebuilding, not from a
bundle.

Do not use a filesystem copy of the canonical and wallet directories as a
backup. Copies taken at different moments describe different chain states,
and a running store changes while you copy it.

## Supported deployments

A Zinder deployment runs on one host. Ingest is the only writer of the
canonical store, and the projector is the only writer of the wallet store.
Every reader opens read-only copies of those stores on the same filesystem.
Each deployment builds its stores from its own node.
[ADR-0004](../adrs/0004-storage-and-deployment-topologies.md) records why.

[Deploying Zinder](../runbooks/deploying.md#choose-a-deployment) lists the
deployments and when to pick each.

`deploy/release-images.json` lists the runtime images, and
`deploy/release-single-volume-images.json` lists the single-volume images.
The Cipherscan adapter and PostgreSQL are not release artifacts.

Releases also publish the runtime binaries. The images are the tested way to
run them. To run the binaries under your own service manager, reproduce what
[Deploying on a VM](../runbooks/deploying-on-a-vm.md) lists.

### More than one reader on a host

You can run more than one `zinder-query` or `zinder-compat-lightwalletd`
process over the same stores on one host. Each process needs its own
`storage.secondary_path`, and its own `wallet.secondary_path` when it reads
the wallet store, because a secondary's metadata directory belongs to one
process. Each also needs its own listen and
ops addresses. The shipped deployments run one of each. Every reader process
runs on the host that holds the stores.
