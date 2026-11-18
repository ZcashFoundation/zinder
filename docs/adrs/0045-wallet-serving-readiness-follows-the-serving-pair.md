# ADR-0045: Wallet-serving readiness follows the serving pair

| Field | Value |
| --- | --- |
| Status | Accepted |
| Product | Zinder |
| Domain | Traffic readiness of native wallet query and lightwalletd compatibility, dependency reporting, single-host restart policy |
| Related | [ADR-0038](0038-wallet-runtime-composition-and-capability-discovery.md#support-readiness-and-request-outcomes), [ADR-0013](0013-source-failure-recovery-topology.md), [ADR-0041](0041-reorg-window-follows-the-node-bound.md), [Service operations](../architecture/service-operations.md#runtime-specific-readiness), [Wallet-serving deployment](../runbooks/deploying-wallet-serving.md#failure-handling) |

## Context

A wallet-serving reader answers almost every method from its published
canonical-and-wallet serving pair. Only a few methods call anything else:

- Transaction broadcast calls the node.
- Mempool snapshots, mempool events, and lookups of unmined transactions call
  ingest control.
- Compact-blocks compatibility calls the node from no method.

Readiness used to be the conjunction of the serving pair and probes of those
dependencies. Native query required the pair, the node source, and ingest
control. Compatibility wallet serving required the pair and the node source,
and compact-blocks serving required its canonical generation and the node
source. Any refusing input closed the traffic gate, so every request failed
with `SERVICE_NOT_READY`. A node outage or a syncing node therefore refused
every stored read without making any request succeed. The same happened when
the canonical writer reported `upstream_not_ready` in its `WriterStatus`,
because the ingest-control probe treated that report as a failure.

The outage spread further through deployment tooling. A Caddy `forward_auth`
gate calls `/readyz` before each compatibility request, so it closed the
public route. The single-host entrypoint counted an ingest `node_unavailable`
or `upstream_not_ready` toward its restart budgets and restarted the container
after at most an hour of contiguous outage, or sooner when the node flapped.
A restart during a node outage turns it into a full outage, because ingest and
the readers need the node to start.

Explorer and the Cipherscan adapter already keep serving through upstream
outages, for the same reason: reflecting an upstream outage in readiness
drains the runtime without making any request succeed.

## Decision

The serving pair alone admits wallet-serving traffic. For native query and
compatibility wallet serving, the admission is the published
canonical-and-wallet pair. For compact-blocks compatibility, it is the
published canonical generation. Every admission rule that guards the served
chain view stays as it was:

- storage, schema, and fence-identity failures refuse traffic;
- replica lag beyond `storage.secondary_replica_lag_threshold_chain_epochs`
  reports `serving_pair_stale` until `storage.serving_pair_staleness_ceiling_ms`
  passes, and then refuses traffic;
- a writer that has not confirmed its tip since it reopened refuses traffic with
  `canonical_tip_unconfirmed`; and
- a writer parked on a reorg deeper than its window refuses traffic with
  `reorg_window_exceeded`.

### Dependencies degrade instead of refusing

Native query declares the node source and ingest control as dependency
inputs. Compatibility wallet serving declares the node source.
Compact-blocks serving declares none and runs no node probe. Each dependency
starts as `starting` and then carries its probe's latest cause.

A dependency whose cause does not admit traffic never closes the traffic gate.
`ComposedReadiness::with_dependency_inputs` lists it in the report's `degraded`
dependencies instead. The one exception is a `reorg_window_exceeded` halt, which
refuses traffic from any input because it causes the admission failures that
follow it. A halt is the runtime's own cause and is never a degraded entry.
Each input still owns only its own slot, so no probe can clear another probe's
observation.

Because an ingest-control failure no longer refuses traffic, native query's
ingest-control probe keeps a halt it observed through later failures and
through answers without the halt. It clears the halt only when
`CanonicalControl.WriterStatus` shows a writer that follows its node with a
confirmed tip, the evidence on which the serving-pair publisher clears the
halt it remembers. A control outage therefore cannot reopen traffic on a halt
that the publisher has not seen yet.

When the canonical writer reports `upstream_not_ready` in
`IngestControl.WriterStatus`, native query relays that observation, with the
writer's heights and health source, as the `upstream_not_ready` cause of its
ingest-control dependency. When the writer's live mempool, which waits on the
node, refuses `MempoolSnapshot` with `NODE_UNAVAILABLE`, native query reports
`node_unavailable` as that dependency's cause, so `/readyz` shows that mempool
calls fail even while the reader's own node probe succeeds. Every other
ingest-control health failure reports `ingest_control_unavailable`.

### Each request fails on its own

A node-backed request always calls the node, whatever the node probe last
observed. A failed call returns `UNAVAILABLE` with `NODE_UNAVAILABLE`, which
clients retry with backoff. The probe polls every 30 seconds by default. A
request therefore must not fail on an observation that may be 30 seconds old,
and `node.request_timeout_secs` and `node.broadcast_timeout_secs` already bound
the call. Mempool requests and unmined transaction lookups fail per request
too when ingest control does not answer: native query returns `UNAVAILABLE`
with `UPSTREAM_UNREACHABLE` for a call or stream the writer never answered,
and passes through every status the writer itself returns.

Losing ingest control entirely stays bounded. Native query reads
`CanonicalControl.WriterStatus` through the same admitted channel, so the pair
reports `serving_pair_stale` and then refuses traffic with
`writer_status_unavailable` once the staleness ceiling passes. An unattested
fence is a question about the identity of the served chain, not about a
dependency, so the ceiling keeps its meaning.

### Readiness reports the degradation and the staleness

The report gains two additive fields on `/readyz` and on the
`zinder.v1.ops.ReadinessReport` message:

- `degraded` lists each degraded dependency as `input` and `dependency_cause`.
  The proto message is `ReadinessDependency` with `input`, `cause`, and
  `detail`. The JSON list follows the top-level `cause`, and its entries do not
  use a `cause` key. Shell parsers that take the first `"cause":` key of the
  body therefore keep reading the runtime's own cause.
- `served_epoch_age_seconds` is the time since the canonical writer committed
  the chain epoch the reader serves.

Metrics gain `zinder_readiness_dependency_state{input, cause}`, which is 1 for
each dependency's current cause and 0 for every other cause label, and
`zinder_readiness_served_epoch_age_seconds`. The gauge carries a dependency's
halt too, so an alert on degraded dependencies excludes
`reorg_window_exceeded`, which the runtime's own cause already reports.
`/readyz` keeps answering 200 while the runtime admits traffic, whatever its
dependencies report.

During a node outage the canonical writer stops committing, and the reader
serves its last chain view as ready. `GetLightdInfo.estimated_height` keeps
equal to the served tip, as it is during normal operation, so a lightwalletd
client cannot tell from it that the chain has stopped advancing. The
served-epoch age is the signal. Operators alert on it and on the dependency
gauge instead of on a reader's 503.

### Deployments stop restarting on node outages

The single-host wallet-serving entrypoint treats an ingest plane that reports
`node_unavailable` or `upstream_not_ready` as serving for its restart budgets.
Such an outage spends neither the contiguous-unready tolerance nor the
recurrence budgets. An ingest that reports `upstream_not_ready` reaches its
node, which proves its node client works, so the entrypoint never restarts the
container for it, however long the node takes to synchronize. An ingest that
reports `node_unavailable` for 24 hours ends the container, so an ingest stuck
in its own node client still restarts eventually. The container healthcheck is unchanged, because query now answers
200 through the outage.

Ingest's own cause hides its node probe while its admission state refuses
traffic. While Zebra resyncs, ingest reports `syncing`, because it trails the
node or because its mempool cannot hydrate. `IngestControl.WriterStatus`
reports the writer's node observation independently of that admission state,
and native query relays it as the `upstream_not_ready` cause of its
`ingest_control` dependency. The entrypoint therefore also treats an ingest
that reports `syncing` as an upstream outage when query's report carries that
relay. It reads only the writer's own observation, never query's view of the
node, and it reclassifies no other ingest cause.

The reference Docker Compose topology starts the readers once the ingest
container has started, instead of once it is healthy. Readers share the ingest
container's network namespace, which needs only a started container. The first
start still waits for a healthy projector, which itself waits for a healthy
ingest.

## Consequences

- Stored reads keep answering through a node outage, a syncing node, and an
  ingest-control outage within the staleness ceiling. This includes
  compact-block sync, block and transaction reads, transparent address reads,
  subtree roots, tree state at any height, chain events, and
  `GetLightdInfo`.
- Broadcast fails per request while the node is unavailable. It is the only
  wallet-serving request that calls the node, so a wallet that is catching up
  keeps scanning through a node outage.
- A load balancer keeps routing to a reader whose broadcasts fail. With several
  readers on different nodes, nothing steers a broadcast to a healthy node
  until reader failover exists.
- A wallet that applies the ZIP 203 default expiry delta of 40 blocks to a
  served tip that is more than about 40 blocks behind the network builds a
  transaction that the network rejects as expired. The served-epoch age alert
  must therefore fire well within that window on Mainnet.
- Readiness no longer enforces the coherence of mempool snapshots with writer
  status. The ingest-control probe still checks that a snapshot is not older
  than the writer's epoch and agrees with its fence, but a mismatch only
  degrades the dependency. The writer's live mempool owner rejects incoherent
  epochs before it serves them.
- Native query no longer reports `node_unavailable`, `upstream_not_ready`,
  `node_capability_missing`, or `ingest_control_unavailable` as its own cause,
  and compatibility no longer reports the node causes. These causes appear
  only under `degraded`.
- A reader restart during a node outage still fails, because the readers probe
  the node at startup and ingest needs the node to open its store and confirm
  its tip. This decision does not change startup.

## Alternatives considered

- **A new traffic-admitting cause such as `serving_degraded`.** Rejected. It
  would compete with `serving_pair_stale` and `cursor_at_risk` for the single
  cause slot and hide the pair's own warning, and it would add a proto enum
  value that consumers must learn. A separate list composes with every
  existing cause.
- **Fail node-backed requests fast while the probe reports an outage.**
  Rejected. The probe's observation can be 30 seconds old, and the request
  timeout already bounds a call to a node that is down.
- **Serve indefinitely without ingest control.** Rejected. The staleness
  ceiling bounds how long a reader serves a fence the writer has not attested,
  and that bound does not depend on why the writer is unreachable.
- **Never restart the container on an ingest node outage.** Rejected. An ingest
  stuck in its own node client, with a healthy node, would then never restart.
  Restarting only when query's own node probe reports the node healthy would
  separate the two cases, but it ties the restart to query's view of the node.
  A 24-hour bound on `node_unavailable` handles both without that coupling.
- **Bound `upstream_not_ready` the same way.** Rejected. A node that answers
  with its sync state rules out a stuck node client, so a restart during a
  long resync only turns a degraded but serving container into a full outage.
- **Keep readiness conjunctive.** Rejected for the reasons in
  [Context](#context): it refuses every stored read without making any request
  succeed.
