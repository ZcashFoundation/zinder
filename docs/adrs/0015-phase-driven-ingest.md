# ADR-0015: Phase-Driven Ingest

## Status

Accepted.

## Context

Canonical ingest has two distinct workloads. A store far behind the upstream
tip needs bounded, pipelined construction, while a near-tip store needs serial
append and replacement handling with low observation latency. Requiring an
operator or deployment system to select one workload creates two startup
contracts and permits a cold store to run the near-tip path indefinitely.

The writer already owns the information needed to choose correctly: whether a
published canonical store exists, its authenticated fence, the upstream tip, and
upstream health. The choice therefore belongs inside `zinder-ingest`, not in
service wrappers or deployment scripts.

## Decision

`zinder-ingest` runs one long-lived canonical writer and selects its workload
from the canonical store lifecycle at startup:

- A ready store at `storage.path` is reopened and followed.
- A published store left in the writer-owned staging path is installed at
  `storage.path`, reopened, and followed.
- Otherwise the writer constructs a fresh store, publishes it, and then follows
  it.

Construction reports the `BulkCatchup` phase. Following reports `FollowingTip`.
After downtime, the writer reopens its published store and follows from the
persisted fence, so no restart, phase switch, or operator action is needed to
catch up. Following readiness reports `syncing` while canonical lag exceeds
`ingest.follow.lag_threshold_blocks`.

Construction fetches bounded source segments, prepares blocks concurrently,
and publishes an authenticated baseline whose settled tip stays
`ingest.reorg_window_blocks` below the construction tip. Following observes one
coherent source update at a time and publishes either an append or a
replacement. Both paths use the same canonical store, error classification,
readiness state, and recovery policy.

`classify_phase` applies the gap rules only for the diagnostic
`zinder-ingest probe` command:

- `AwaitingUpstream` when the upstream tip is height zero.
- `BulkCatchup` when the upstream-to-store gap is greater than
  `ingest.phase_classification.catchup_threshold_blocks`.
- `FollowingTip` otherwise.

The probe treats an empty store as height zero, and a store ahead of the
observed upstream tip saturates the gap to zero.

The writer exposes the phase independently from readiness cause. A process can,
for example, be in `following_tip` while reporting `node_unavailable`; clients
must not infer one dimension from the other. `IngestControl.WriterStatus`
reports phase, source position, gap, and upstream-health observations.

The configuration is grouped by responsibility:

- `[ingest.phase_classification]` owns the probe's gap boundary.
- `[ingest.construction]` owns historical construction limits.
- `[ingest.follow]` owns near-tip polling and lag limits.
- `[ingest.run_overrides]` owns bounded one-run overrides.
- `[node.health]` owns the upstream readiness probe because health is a source
  property, not an ingest-phase property.

The writer opens the primary canonical store once. The mempool owner, retention
worker, and control service share the same process lifetime and do not restart
when construction hands off to following.

### Upstream sync detection

When `[node.health].addr` is configured, the writer polls Zebra's readiness
endpoint and treats a non-ready upstream as `upstream_not_ready`. The resolved
`NodeHealthConfig` owns the poll interval, verification-progress floor, and
estimated-gap threshold. The source adapter returns a typed
`UpstreamHealthSnapshot`; the writer owns how that observation affects
readiness and recovery. Missing, malformed, or unreachable health responses do
not become successful observations.

Without an explicit readiness endpoint, the writer relies on the source
observations available through Zebra JSON-RPC. Upstream health remains
independent from the local ingest phase in both cases. The progress and
estimated-gap thresholds apply to Mainnet and Testnet. Regtest has no external
network tip, so a successful JSON-RPC tip and health observation is ready even
when Zebra reports a public-network-shaped estimated height.

## Consequences

- Operators run one ingest command for empty, recovering, and near-tip stores.
- Deployment configuration does not encode the writer's current phase.
- Historical throughput limits and near-tip latency limits remain separate and
  can be tuned without creating separate execution modes.
- Readiness and control-plane clients receive explicit phase and upstream-health
  data.
- Phase handlers must preserve one canonical publication and recovery contract.

## References

- [Chain ingestion](../architecture/chain-ingestion.md)
- [Service operations](../architecture/service-operations.md)
- [ADR-0013: Source failure recovery topology](0013-source-failure-recovery-topology.md)
- [ADR-0014: Shared configuration sections](0014-shared-configuration-sections.md)
- [ADR-0022: Resource-budgeted bulk catch-up](0022-resource-budgeted-bulk-catchup.md)
- [ADR-0035: Canonical storage topologies](0035-canonical-storage-topologies.md)
