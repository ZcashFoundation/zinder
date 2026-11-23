# Service operations

This page explains how Zinder runtimes start and stop, what you must keep
private, how a deployment recovers, and which deployment shapes are
supported. It is for operators planning a deployment and for contributors
changing runtime lifecycle code. Readiness causes are listed in
[Readiness](../reference/readiness.md), and metrics in
[Metrics](../reference/metrics.md).

## Startup and shutdown

### Startup phases

Each runtime reports the startup phases it performs. Each phase logs an entry
and an exit under the `zinder::startup` target. Both events carry `phase` and
`phase_state`. The exit carries an `outcome` of `ok`, `failed`, or `aborted`. The
`zinder_startup_phase_duration_seconds` histogram records the same phases.

| Phase | What happens |
| --- | --- |
| `load_config` | Merge defaults, the config file, environment variables, and flags, and validate the result. |
| `connect_node` | Ingest only: build the node clients from configuration. |
| `check_schema` | Ingest only: check each node's capabilities, archive storage, genesis block, and network-upgrade table. |
| `admit_ingest_control` | Authenticate to ingest control. Wallet-serving readers check its identity, network, and contract revision. The projector checks its canonical secondary against the writer's fence. |
| `open_storage` | Open a read-only store copy, including the projector's canonical secondary. Ingest opens its store after `ready`. |
| `recover_state` | Replay or rebuild in-flight state. |
| `start_api` | Bind the gRPC and operational listeners. |
| `ready` | Startup is complete. |

The projector reports `start_api` when it binds each enabled listener, first
for operations and later for owner control. During `recover_state`, it builds
a wallet or resumes the persisted wallet at a retained event. If it must
bootstrap from the next retained event, recovery covers that first transition.
The projector reports `ready` once, when the wallet first catches up to the
attested writer fence. Use `/readyz` for its current state.

Wallet recovery and ingest admission overlap during startup. An error opening
the wallet fails `recover_state`. An error checking the initial writer fence
fails `admit_ingest_control`. The other unfinished phase reports `aborted`.

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
accepting new requests. It then stops its background tasks and its servers,
including the operational endpoint, and waits for them. Only the owner of a
store closes it, and readers make no final writes.

When shutdown interrupts an initial wallet build, the projector asks ingest
to release its canonical retention lease. If the release returns
`UNAVAILABLE`, the projector logs the lease expiry and continues shutdown.
Any remaining lease stops protecting its anchor at expiry. A refused renewal
still fails the build, even if shutdown begins while the renewal is in flight.

`zinder-query` ends open server streams, such as `ChainEvents` and
`MempoolEvents`, with `UNAVAILABLE` and `SERVICE_NOT_READY`, because event
streams never end on their own.

`zinder-query`, `zinder-compat-lightwalletd`, and `zinder-explorer` share the
following shutdown order.

1. They report `shutting_down`, so `/readyz`, their
   [gRPC health service](../reference/readiness.md#grpc-health), and new
   requests see it before anything stops. gRPC load balancers and
   Kubernetes `grpc` probes then stop routing to them while the drain runs.
2. They stop their background tasks and their operational endpoint, and
   start the gRPC server's graceful drain at the same time. Open health
   `Watch` streams end as the drain starts, so they do not hold it open.
3. They wait up to 10 seconds for the drain.
4. They end every call still open, such as a block-range stream to a client
   that reads slowly, with `UNAVAILABLE`, and refuse any later call the same
   way before its service runs. Both carry `SERVICE_NOT_READY` with the
   `shutting_down` readiness cause.
5. If the drain timed out, they wait up to 2 more seconds for the server to
   send those calls their status and close its connections.
6. Wallet-serving publishers release their serving generations.
7. They wait for their background tasks to stop.

When step 4 completes, no call handler or response of the gRPC server
remains, and each ended response sends its status before any more of its
data. A client-streaming call's request stream also ends, so work its handler
handed the stream to cannot keep the connection open. Other work a handler
already handed off finishes on its own and holds the generation it reads
until then: a storage read running on a blocking thread completes and its
result is discarded, and a block-range reader finishes the chunk it is
reading and stops when it tries to send it, because the response it fed has
ended.

A client that has stopped reading cannot receive its status, so step 5 gives
up on its connection after 2 seconds; a client that reads gets its status
within milliseconds. These limits bound the gRPC drain. The single-volume
wallet-serving entrypoint gives its processes 20 seconds to exit.

In `compact-blocks` and `light-wallet` modes, the publisher interrupts pending writer-status
reads and retry delays when shutdown begins, including during candidate
convergence. It keeps the published reader and skips refresh-failure
reporting for the interrupted refresh. RocksDB work already running finishes,
and the publisher closes its serving generations after the drain. Slow
storage can extend process shutdown beyond the entrypoint's grace window.

### Background task failure

The supervised tasks below run until shutdown cancels them. A task that ends
earlier has failed. If the Explorer's upstream-observation probe ended
quietly, it could keep serving the last node tip it observed. A wallet
serving-pair publisher that ended quietly could leave a stale pair ready
past `storage.serving_pair_staleness_ceiling_ms`.

Each runtime therefore watches its tasks next to its gRPC server. When one
exits, or the server stops without a shutdown request, the runtime stops in
the order above and leaves the restart to the platform. A termination signal
is a shutdown request of its own: the server and tasks see only the runtime
stopping, so a signal never looks like the server stopping on its own.

| Runtime | Watched tasks |
| --- | --- |
| `zinder-query` | Serving-pair publisher, node-readiness probe, ingest-control readiness probe, and operational endpoint |
| `zinder-compat-lightwalletd`, wallet serving | Serving-pair publisher, node-readiness probe, ingest-control readiness probe, tip-change publisher, node-tip witness, and operational endpoint |
| `zinder-compat-lightwalletd`, compact-blocks serving | Compact-block publisher, node-tip witness, and operational endpoint |
| `zinder-compat-lightwalletd`, light-wallet serving | Canonical publisher, forwarding-node readiness probe, ingest-control readiness probe, tip-change publisher, node-tip witness, and operational endpoint |
| `zinder-explorer` | Materialized-view catch-up when a view store is open, upstream-observation probe when a node is configured, and operational endpoint when enabled |

The light-wallet probe can report a failed dependency while the publisher
continues serving local data. See
[During a node outage](../reference/readiness.md#during-a-node-outage).

A task that had already ended when the runtime started stopping counts as
failed, even when the runtime stopped for a shutdown request. A publisher
fails if closing its serving generations fails, which happens after shutdown
starts.

The runtime logs every task that fails, with its name, as
`runtime_task_failed`, or as `runtime_task_stopped` for a task that ended
without an error. It exits with status 0 only after a requested shutdown in
which the gRPC server reported no error and every task stopped cleanly after
the runtime started stopping. It exits with status 1 in every other case,
with an error that names the task or server that stopped the runtime, or, for
a requested shutdown, the first task that failed.

## Wallet traffic

For traffic by protocol method, use the [wallet RPC listener metrics](../reference/metrics.md#wallet-rpcs-at-the-listener).
They include admission refusals and follow streaming calls through their last
status or cancellation. The query engine's operation counters measure the work
inside a call. A long lightwalletd stream can perform several engine reads.

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
   plaintext and can require one shared bearer token for `ExplorerQuery`
   (`explorer.bearer_token_path`). Health and reflection accept calls without a token.
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

During wallet serving, the single-volume supervisor gives ingest 24 hours
when it reports `node_unavailable`. A syncing ingest also qualifies when its
current follow iteration is waiting for mempool hydration at the node's tip
and native query relays `node_unavailable` under `ingest_control`. When
the node answers a retry with a different tip, ingest clears its direct
outage before it fetches the next block or prepares a reorg. If the
writer starts preparing a block or falls behind while the relay continues,
the outage clock pauses. The one-hour unready tolerance and recurrence
budgets apply during that work. If the mempool is still waiting when ingest
reaches the tip again, the clock resumes with its original start time and
accumulated outage time. Recovery clears the clock. A direct or relayed
`upstream_not_ready` means the node answers
and can finish syncing without a restart.
[Single-volume failure handling](../runbooks/deploying-single-volume.md#failure-handling)
describes how to distinguish these cases.

### Restore by rebuilding

An operator with no verified private backup can rebuild from the node. A rebuild is a
fresh first sync: start a new deployment on empty storage, let ingest and the
projector build from the node, move traffic once its readers report ready,
and then retire the old deployment. A Mainnet first sync takes many hours; see
[Deploying](../runbooks/deploying.md) for measured times. Plan for that time
in your recovery objective.

### Restore from a private backup

The projector can capture and seal a coherent pair of checkpoints. Offline
restore verifies the independently recorded manifest digest and installs both
stores under one fresh directory before ordinary owner and reader admission.
Use [Back up and restore a wallet-serving indexer](../runbooks/private-backups.md)
for capture, off-host transfer, verification, and cutover.

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
