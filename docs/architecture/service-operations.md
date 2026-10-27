# Service operations

Every Zinder runtime exposes the same operational HTTP contract through
`zinder-runtime`:

| Route | Purpose |
| --- | --- |
| `/healthz` | Process liveness. Returns success while the runtime can answer HTTP. |
| `/readyz` | Typed traffic readiness for the runtime's owned contract. |
| `/metrics` | Prometheus metrics with bounded label sets. |

Health does not imply readiness. A writer may be alive while synchronizing or
recovering its source, and a reader may be alive while its secondaries cannot
form an exact serving pair.

`/healthz` identifies the service, product version, build Git commit, network,
and advertised capabilities. Local builds use `unknown` when no commit was
injected, while release images carry the full tag-target commit. The same
version and commit appear in the `zinder_build_info` metric and native
`ServerInfo`; lightwalletd compatibility exposes them through `LightdInfo`.

## Readiness vocabulary

`ReadinessCause` is shared by JSON and protobuf surfaces:

| Cause | Operator meaning |
| --- | --- |
| `starting` | Initialization has not completed. |
| `syncing` | Owned state is healthy but behind its target. |
| `ready` | The runtime can serve its contract. |
| `node_unavailable` | Zebra or its transport is temporarily unavailable. |
| `node_capability_missing` | Zebra cannot provide a required capability. |
| `upstream_not_ready` | Zebra is reachable but not sufficiently synchronized. |
| `storage_unavailable` | Required local storage cannot be opened or read. |
| `schema_mismatch` | Persisted identity or schema differs from the binary. |
| `reorg_window_exceeded` | A replacement crosses the persisted reorg policy. |
| `replica_lagging` | A RocksDB secondary exceeds the admitted epoch lag. |
| `serving_pair_stale` | A wallet-serving reader cannot refresh its published pair, which is still within its staleness ceiling. |
| `writer_status_unavailable` | A trusted reader cannot reach the writer control API. |
| `ingest_control_unavailable` | Native wallet live-state health failed on the admitted ingest-control channel. |
| `cursor_at_risk` | Canonical event retention is approaching an active cursor. |
| `shutting_down` | New traffic has been drained for termination. |

`ready`, `cursor_at_risk`, and `serving_pair_stale` admit traffic; every other
cause refuses it.

Payload-bearing causes include structured detail. `node_unavailable` carries a
bounded failure class, sanitized reason, consecutive-failure count, and outage
duration. `upstream_not_ready` carries source heights, verification progress,
and the health-probe source. Reorg, replica, serving-pair, and
canonical-retention causes carry the numeric boundary that failed.

Readiness detail is operational data, not a place for raw node responses,
authorization material, filesystem paths, transaction identifiers, or other
secrets.

## Runtime-specific readiness

### Ingest

Ingest is ready when its canonical fence is within the configured source lag
and the current mempool source generation has published a complete snapshot
certified at that exact canonical tip.
Fresh construction, source recovery, schema admission, and reorg refusal keep
it unready. The canonical follower owns the one process-readiness state; the
mempool owner communicates only through its hydration prerequisite, so source
and retention tasks cannot overwrite a canonical failure. The private control
server, live mempool owner, and mempool retention task are supervised with the
writer; an unexpected exit from any of them terminates the runtime.

### Projector

Projector is ready only after it has admitted the canonical secondary,
acquired the required leases, opened or built the wallet primary, and reached
continuous following at an authenticated source position. An individually
healthy canonical or wallet store is insufficient if their source identities
do not agree.

### Native wallet query

Native wallet query is ready only when all three independently retained inputs
are healthy: the admitted canonical-and-wallet serving pair, the exact node
source used by every advertised node-backed operation, and the authenticated
ingest-control channel used by pair publication and live wallet operations.
No input can erase another input's failure. Capability discovery remains
immutable while any admitted dependency is temporarily unhealthy.

Before storage is opened or traffic is bound, `AdmittedIngestControl` connects
once and validates `ServerInfo` service identity, network, contract revision,
and the seven methods required by the native composition. The accepted
identity and channel are then immutable. Health checks do not repeat structural
admission: they call `WriterStatus` and a bounded
`MempoolSnapshot(max_entries = 1)` through that same authenticated channel and
validate coherent network and tip evidence. A transition into failure emits
one classified warning and drains readiness with
`ingest_control_unavailable`; recovery emits one event and restores only the
ingest-control input. A `WriterStatus` that reports a parked writer instead
drains readiness with `reorg_window_exceeded`, which takes precedence over the
serving-pair failures it causes. New native requests then fail with
`REORG_WINDOW_EXCEEDED`, and open `ChainEvents` streams end with the same
status. The health probe intentionally shares the configured
serving-pair refresh cadence because both observe the same control plane.

The node probe exercises `tip_id` and upstream synchronization health through
the same shared source handle installed in the query. It does not repeat
structural capability discovery. Any node-backed composition must admit
`TipId` before binding so this liveness prerequisite cannot fail later as a
configuration surprise. The serving-pair publisher, node-readiness probe, and
ingest-control-readiness probe are supervised with the gRPC server; an
unexpected exit drains readiness and terminates the runtime.

When the shared traffic gate is closed, new gRPC calls return
`UNAVAILABLE` with `ErrorReason = SERVICE_NOT_READY` and
`ErrorInfo.metadata["readiness_cause"]` set to the stable readiness label.
`/readyz` carries the corresponding structured detail. This keeps a
readiness outage distinguishable from poisoned transport without mutating the
endpoint's immutable capability set.

### Lightwalletd compatibility

Compatibility is ready only while `WalletServingPairPublisher` can reach writer
status, catch canonical and wallet secondaries up, and publish a pair that
passes exact-fence admission, and while the admitted node source needed by its
compatibility methods is healthy. Traffic uses a readiness interceptor, so a
process that has drained readiness does not accept new gRPC requests.
Compatibility bootstraps its serving pair through the writer-status endpoint
and uses separate ingest-control channels for mempool and tip-change traffic.
It does not instantiate the native query's admitted ingest type or advertise
native capabilities. Consumer support additionally requires the selected
lightwalletd client to pass its complete lifecycle through a trusted edge.

Writer-status transport failures and replica or projection lag report
`serving_pair_stale` and keep serving the published pair, because that pair is
still exactly what the writer attested. `storage.serving_pair_staleness_ceiling_ms`
bounds how long that lasts, measured from the last writer attestation and
defaulting to five minutes; past it the underlying cause takes over and traffic
stops. A writer fence that disagrees with the pair, a schema mismatch, and an
absent pair slot fail closed immediately: the reader would otherwise present a
chain view the system of record no longer attests.

The ceiling governs admission on its own. `storage.secondary_replica_lag_threshold_chain_epochs`
decides when lag stops being routine and becomes a reported warning, not how
long a warned reader may keep answering.

## Startup and shutdown

Startup phases use the shared `StartupPhase` vocabulary, including load config,
connect node, admit ingest control, check schema, recover state, open storage,
start API, and ready. Phase duration and failure are metrics and structured
logs.

On termination, a runtime sets `shutting_down`, stops accepting new traffic,
cancels background tasks, waits for owned tasks and servers, and closes the
operational endpoint. Primary stores are closed only by their owner. Readers do
not attempt a final write or migration.

## Metrics

Every runtime exports build information and readiness state.
`zinder_build_info` is a constant gauge labeled by service, product version,
build Git commit, and network. Important runtime metrics include:

### Canonical writer

- `zinder_ingest_canonical_tip_height`
- `zinder_ingest_canonical_lag_blocks`
- `zinder_ingest_canonical_chain_epoch`
- `zinder_ingest_canonical_chain_event_sequence`
- `zinder_ingest_canonical_live_appends_total`
- `zinder_ingest_canonical_live_replacements_total`
- `zinder_ingest_canonical_live_commit_seconds`
- source fetch queue, reservation, response-size, and reassembly gauges
- construction watermark and persistence measurements
- `zinder_ingest_canonical_historical_prevout_reads_total`
- `zinder_ingest_canonical_cross_block_wallet_reads_total`

The last two counters are expected to remain zero and protect the canonical
block-local boundary.

### Mempool owner

- `zinder_mempool_lifecycle_state{status}` is a one-hot gauge over the closed
  `hydrating`, `serving`, and `rebuild_required` states.
- `zinder_mempool_entries` reports the current process-local live entry count.
- `zinder_mempool_source_errors_total{kind,failure_class}` and
  `zinder_mempool_hydration_failures_total{reason}` diagnose bounded source
  and hydration failure classes.
- `zinder_mempool_snapshot_age_seconds` samples the age of the certified
  source-generation snapshot when a page is served.
- `zinder_mempool_events_retained`,
  `zinder_mempool_event_retention_oldest_sequence`, and
  `zinder_mempool_event_retention_oldest_age_seconds` report the durable event
  floor after each retention pass.
- `zinder_mempool_events_pruned_total{kind}` and
  `zinder_mempool_event_pruning_duration_seconds{status}` report per-kind
  deletion counts and step outcomes. The closed status set is
  `budget_exhausted`, `reached_head`, `reached_unexpired_event`,
  `retention_disabled`, and `error`.
- `zinder_mempool_event_retention_examined_events` and
  `zinder_mempool_event_retention_examined_encoded_bytes` report bounded work
  performed by each step.

Mempool retention metrics describe stored history, not active consumer
cursors. Retention is contiguous-prefix based, and active `Added` replay
anchors can keep later rows beyond their individual minimum residence window.
Budget-exhausted steps resume without the normal interval delay, with one
canonical source turn between consecutive maintenance commands.

### Projector and wallet store

Projector metrics report build phase, lease renewal, canonical event position,
wallet source position, projection lag, transition bytes, digest validation,
and checkpoint operations. Store metrics use explicit canonical or wallet role
labels.

### Compatibility reader

Compatibility metrics report writer-status availability, catch-up duration,
retired-pair teardown duration, pair convergence attempts, published
generation, replica lag, pair admission failure, and wallet-serving pair
replacement. gRPC request metrics remain separate from pair-maintenance
metrics.

### RocksDB

`zinder-store` exports bounded role-labelled metrics for block-cache capacity
and use, memtables, WAL bytes and limits, pending compaction, active
compactions, write stops, bytes read and written, MultiGet behavior, Bloom
filters, read latency, and startup open duration.

Metric labels must be closed enumerations. Never use paths, addresses, hashes,
transaction identifiers, exception text, or user input as labels.

## Structured logs

Logs use a stable `event` field and the owning tracing target. Current canonical
events include:

- `canonical_writer_started`
- `canonical_ready_store_reopened`
- `canonical_empty_construction_staging_removed`
- `canonical_unpublished_construction_restarted`
- `canonical_live_append_committed`
- `canonical_live_replacement_committed`

Messages explain the event for humans; automation keys on structured fields.
Logs may include bounded heights, epoch numbers, sequence numbers, durations,
and enum labels. They must not include raw authorization headers, bearer tokens,
cookie contents, viewing keys, or unbounded payloads.

## Security boundary

Public listeners require explicit `security.allow_public_bind = true`. The
release service does not provide public TLS termination, authentication, rate
limits, or quota accounting. Put public compatibility traffic behind an
operator-controlled HTTP/2 proxy.

Private ingest and projector control endpoints use bearer tokens loaded from
files. Configuration output shows redaction markers, and errors identify only
the configuration field or unreadable path, never token contents.

Node JSON-RPC authentication uses `none`, `basic`, or `cookie` according to the
shared `[node.auth]` section. Cookie and bearer material must not enter metrics,
logs, reports, or test snapshots.

## Recovery

Recoverable source errors drain readiness and retry from durable state. Store
identity, corruption, schema, and reorg-policy failures fail closed and require
operator action.

Production wallet recovery is one coherent state-bundle operation:

1. coordinate canonical and wallet owners through their private control APIs;
2. capture physical checkpoints and owner admission evidence;
3. bind both checkpoints to one canonical event fence, wallet digest, and the
   canonical build plan including raw-blob retention;
4. restore into fresh paths;
5. cold-admit each checkpoint under bounded resources; and
6. start ingest, projector, native query, then compatibility and require normal
   wallet-serving admission before traffic.

An independently timed copy of the canonical and wallet directories is not a
coherent backup. Physical checkpoint success also does not prove query serving,
continuous following, reorg recovery, or client compatibility.

State-bundle manifest format 3 carries the checkpoint build plan's exact
raw-blob retention and the canonical writer's admitted network-upgrade schedule
revision and fingerprint. Missing or unknown retention values fail admission
instead of being inferred from workload or available rows. Format 2 manifests,
which predate schedule admission, remain readable.

## Deployment support

The complete supported composition is the same-host RocksDB deployment in
`deploy/docker-compose.yml` and `deploy/systemd/`. The published release-image
catalog is limited to ingest, projector, native query, and lightwalletd
compatibility. `deploy/Dockerfile.wallet-serving` is a self-built native-only
target for hosts that attach one volume to one container; it is not a published
release image and omits lightwalletd compatibility. Explorer, Cipherscan, and
PostgreSQL are not release classes.
