# Service operations

This page explains how Zinder runtimes start and stop, what you must keep
private, what you can recover from today, and which deployment shapes are
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
before it has written its cookie file. Ingest cannot start without a node.

### Startup order

Ingest must run first, because it is the only writer of the canonical store
and the only source of writer status. The reference Compose topology starts
the runtimes in this order:

1. `zinder-ingest`.
2. `zinder-projector`, once ingest is healthy.
3. `zinder-query` and `zinder-compat-lightwalletd`, once the projector is
   healthy and the ingest container has started.

A query runtime that starts behind a parked projector cannot publish its
first serving pair, so it fails before it listens. The single-volume
entrypoint therefore does not start query while the projector reports that
park.

A restart during a first sync does not resume; see
[Initial sync](../runbooks/initial-sync.md#restarts-during-the-first-sync).

### Shutdown

On a termination signal, a runtime reports `shutting_down` at once and stops
accepting new requests. It then cancels background tasks, waits for its
servers and owned tasks, and closes its operational endpoint. Only the owner
of a store closes it, and readers make no final writes.

`zinder-query` ends open server streams, such as `ChainEvents` and
`MempoolEvents`, with `UNAVAILABLE` and `SERVICE_NOT_READY`, because event
streams never end on their own. Its gRPC drain is bounded at 10 seconds. The
single-volume entrypoint allows its processes 20 seconds to exit.

## Memory allocator

On Linux, every service binary allocates through jemalloc, and jemalloc also
serves the C `malloc` that RocksDB calls. Tune it with `MALLOC_CONF`. Glibc
variables such as `MALLOC_ARENA_MAX` have no effect.
`MALLOC_CONF=stats_print:true` prints allocator statistics when the process
exits, and `MALLOC_CONF=background_thread:true` returns freed memory to the
kernel while the process is idle. [ADR-0005](../adrs/0005-bounded-process-memory.md) records why.

## Security boundary

Zinder protects chain data integrity. It does not protect its listeners from
the network. Every Zinder listener is plaintext. You are responsible for
three things:

1. **Put the wallet listeners behind your own proxy.** The native wallet gRPC
   listener (`zinder-query`), the lightwalletd listener
   (`zinder-compat-lightwalletd`), and the Cipherscan REST adapter have no TLS
   and no client authentication. The Explorer listener has no TLS and can
   require only one shared bearer token (`explorer.bearer_token_path`). Your
   proxy terminates TLS and enforces authentication, rate limits, and quotas.
   [Exposing over TLS](../runbooks/exposing-over-tls.md) shows one setup.
2. **Keep the operational endpoint private.** `/healthz`, `/readyz`, and
   `/metrics` have no authentication. The endpoint also accepts only 64
   connections, so any client that reaches it can make your health probes
   and metric scrapes fail. Bind `ops.listen_addr` to loopback or to a
   private network that only your orchestrator and metrics scraper reach.
3. **Keep the control endpoints private.** The ingest control endpoint serves
   writer status, chain events, and the mempool to readers. Ingest refuses a
   non-loopback `ingest_control.listen_addr` without
   `ingest_control.bearer_token_path`, but the token travels in plaintext.
   Keep the endpoint on loopback, as the reference topology does. The
   projector control endpoint accepts only a loopback address and always
   requires a bearer token.

Zinder refuses to bind any listener to `0.0.0.0`, `::`, or a public address
unless you set `security.allow_public_bind = true`. Loopback and private
ranges are always allowed. Setting the flag logs a warning; it adds no
protection.

Zinder's own listener bounds, listed in
[Configuration](../reference/configuration.md#listener-and-request-limits),
are not per client, so your proxy must enforce per-client limits.

Bearer tokens are read from files. Configuration output, such as
`--print-config`, shows redaction markers in place of secrets, and errors
name only the field or the path. Log events that carry a node URL replace its
user information, query, and fragment with `[REDACTED]`.

## Recovery

Node failures, ingest outages shorter than the serving staleness ceiling, and
ordinary restarts recover on their own, except that a first sync starts over.
Causes that need an operator, with what to do, are in
[Readiness](../reference/readiness.md#readiness-causes).

### Rebuilding is the only restore

Zinder has no restore today. A rebuild means a fresh first sync: start a new
deployment on empty storage, let ingest and the projector build from the
node, move traffic once its readers report ready, and then retire the old
deployment. A Mainnet first sync takes many hours; see
[Deploying](../runbooks/deploying.md) for measured times. Plan for that time
in your recovery objective.

The projector's private control API can capture a state bundle. A bundle
holds a canonical checkpoint and a wallet checkpoint bound to the same
canonical position, with a manifest. No shipped command restores a bundle or
moves it to another host. Do not treat bundles as backups yet.

A filesystem copy of the canonical and wallet directories is not a backup
either. Copies taken at different moments describe different chain states,
and a running store changes while you copy it.

## Supported deployments

Zinder supports one host. Ingest is the only writer of the canonical store,
and the projector is the only writer of the wallet store. Every reader opens
read-only copies of those stores on the same filesystem. There are no read
replicas on other hosts, no snapshot bootstrap, and no shared database.
[ADR-0004](../adrs/0004-storage-and-deployment-topologies.md) records why.

[Deploying Zinder](../runbooks/deploying.md#choose-a-deployment) lists the
deployments and when to pick each.

`deploy/release-images.json` lists the runtime images, and
`deploy/release-single-volume-images.json` lists the single-volume images.
The Cipherscan adapter and PostgreSQL are not release artifacts.

Releases also publish the runtime binaries. The repository ships no systemd
units for them and does not test running them outside Docker.
[Deploying on a VM](../runbooks/deploying-on-a-vm.md) lists what such a
deployment must reproduce.

### More than one reader on a host

You can run more than one `zinder-query` or `zinder-compat-lightwalletd`
process over the same stores on one host. Each process needs its own
`storage.secondary_path`, and its own `wallet.secondary_path` when it reads
the wallet store, because a secondary's metadata directory belongs to one
process. Each also needs its own listen and
ops addresses. The shipped deployments run one of each, and no layout with
several copies of a reader is shipped or tested. Processes on another host
cannot open the stores.
