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
| `node_capability_missing` | The node cannot provide a required capability. |
| `upstream_not_ready` | Zebra is reachable but not sufficiently synchronized. |
| `storage_unavailable` | Required local storage cannot be opened or read. |
| `schema_mismatch` | Persisted identity or schema differs from the binary. |
| `reorg_window_exceeded` | A replacement crosses the persisted reorg policy. The detail carries the replacement depth, the window, and the depth the window covered, which is smaller than the window while a raised window is still growing. |
| `replica_lagging` | A RocksDB secondary exceeds the admitted epoch lag. |
| `serving_pair_stale` | A wallet-serving reader cannot refresh its published pair, which is still within its staleness ceiling. |
| `writer_status_unavailable` | A trusted reader cannot reach the writer control API. |
| `ingest_control_unavailable` | Native wallet live-state health failed on the admitted ingest-control channel. |
| `canonical_tip_unconfirmed` | The canonical writer has not confirmed its store's visible tip against its node since it opened the store, so wallet-serving readers do not serve its fence yet. |
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

Each runtime decides which cause each of its readiness owners reports. A
runtime whose readiness depends on more than one owner composes them through
`zinder_runtime::ComposedReadiness`, which applies one fixed precedence:

1. `shutting_down` once shutdown has started, irreversibly;
2. an input that reports a parked canonical writer's `reorg_window_exceeded`
   halt, which is the root cause of the admission failures that follow it;
3. the admission state (storage, serving pair, or canonical writer) when it
   refuses traffic;
4. the first refusing named input, in the order node source, ingest control,
   materialized-view catch-up;
5. otherwise the admission state, including a warning that admits traffic.

The admission state supplies the reported heights. When shutdown or an input
supplies the cause, the reported target height is cleared. Every owner
replaces only its own input, so no owner can clear another owner's failure.
Ingest, native wallet query, lightwalletd compatibility, and Explorer use this
composition. Projector and Cipherscan compatibility each keep one owner of
their readiness state.

### Canonical fence attestation

Projector, native wallet query, and lightwalletd compatibility each read
canonical storage through a process-owned RocksDB secondary. Before one of them
uses that view, `CanonicalControl.WriterStatus` must attest it. All three use
the same comparison and catch-up loop from `zinder-runtime`:

- The view matches only when the writer reports the reader's network and every
  fence field is equal: chain epoch, event sequence, visible tip height, visible
  tip hash, visible block count, and canonical sequence digest.
- The secondary is behind when the writer is strictly ahead in both event
  sequence and chain epoch. The writer answers status reads in order with its
  commits, so this is ordinary replication lag. The loop catches up again with
  capped backoff until the runtime's deadline or attempt bound expires.
- Any other difference is divergence: a missing fence, another network, a
  secondary ahead of the writer, or a fence at the same event sequence that
  differs in any field. Divergence fails on the round that observes it.

Projector and native wallet query also require the secondary to carry the
network-upgrade schedule revision the writer reports. A writer can admit a
revision without moving its fence, so a matching fence with an older revision
is retried like lag. Compatibility accepts the matching fence and opens its
next generation with the schedule the writer reports.

The runtimes differ in their bounds. Projector retries until
`projector.fence_convergence_timeout_seconds`, starting at 50 ms and doubling
to 1 s, and reports `syncing` once lag lasts longer than one second, except
that a ready projector trailing by at most one block stays `ready` as described
in [Projector](#projector). Query and
compatibility stop at their configured pair-convergence attempts or timeout,
whichever comes first, and wait the smaller of the catch-up interval and
100 ms between attempts.

A parked writer and a writer that has not confirmed its tip since it reopened
keep attesting their frozen fence, so convergence still succeeds. The
projector keeps following that fence. Native wallet query and compatibility
pass every attesting status through the shared `WriterAttestation` rules: a
`reorg_window_exceeded` halt refuses traffic with `reorg_window_exceeded` and
is remembered through writer-status outages until a valid status carries
neither the halt nor `tip_unconfirmed`; `tip_unconfirmed` alone refuses
traffic with `canonical_tip_unconfirmed`; and only a following writer with a
confirmed tip earns the staleness allowance that admits a lagging reader.

On refresh, a status from another network or without a fence fails as
divergence first. A parked writer's halt is checked next, so a halt reported
with a mismatched fence keeps the published pair refusing traffic with
`reorg_window_exceeded` rather than failing the refresh as a fence mismatch:
the halt is the cause an operator must resolve.

The attestation reads only the writer-status response. It never handles the
canonical checkpoint capability, which stays with the projector's checkpoint
client.

### Ingest

Ingest is ready when its canonical fence is within the configured source lag
and a mempool source generation has published a complete snapshot certified at
the canonical tip. A block does not withdraw readiness: the owner keeps the
previous generation certified while its replacement hydrates, and the follower
admits a certified tip that has trailed the canonical tip for less than 30
seconds. A certified tip that trails for longer reports `syncing`.
Fresh construction, source recovery, schema admission, and reorg refusal keep
it unready.

The canonical writer owns the admission state. Store construction, the
canonical follower, and the reorg-window park publish it; the mempool owner
communicates only through its hydration prerequisite, so source and retention
tasks cannot overwrite a canonical failure. The park is terminal: it fixes the
`reorg_window_exceeded` admission state and its ingest phase, later admission
writes are ignored, inputs keep recording but cannot replace the halt, and only
shutdown replaces it. The materialized-view tailer raises
and clears the `cursor_at_risk` warning on the same admission state, and a
lag-derived `ready` from the follower keeps an active warning while advancing
the reported heights.

When `[node.health].addr` is configured, the upstream health probe owns the
node-source input, and ingest stays `starting` until the probe's first
successful observation. Each successful probe publishes `upstream_not_ready`
while Zebra reports that it is not ready for queries and `ready` otherwise. A
failed probe keeps the last observation. The follower cannot clear the probe's
`upstream_not_ready`, so ingest stays unready until the probe observes a ready
Zebra. Without a health endpoint, ingest has no node-source input.

On `/readyz`, a refusing admission state, such as `syncing` or
`node_unavailable`, takes precedence over the probe. `IngestControl.WriterStatus`
reports the probe's latest observation independently of the admission state:
`upstream_not_ready` and Zebra's heights stay set throughout fresh construction
or source recovery while the probe reports Zebra not ready.

During fresh construction, the writer has no published chain epoch and does not
service its command queue, so `WriterStatus` answers from readiness alone. It
reports the `bulk_catchup` phase and `upstream_not_ready` without `chain_view`
or `gap_blocks`, and Zebra's heights appear only in `upstream_not_ready`.
Native query treats a missing `chain_view` as an invalid writer status, so it
stays unready until the writer publishes its first epoch.

Ingest reports `shutting_down` as soon as termination is requested, before the
writer and its workers drain. Termination during fresh construction stops the
writer without admitting a store. While blocks stream in, ingest exits within
the wallet-serving supervisor's 20-second grace window. SST finalization and
publication cannot be interrupted, and ingest waits for them before it exits,
which on a long chain can outlast the grace window. The next start discards the unpublished staging or installs a staged READY store,
as described in
[Chain ingestion §Fresh construction](chain-ingestion.md#fresh-construction).

The private control server, live mempool owner, and mempool retention task are
supervised with the writer; an unexpected exit from any of them terminates the
runtime. The mempool owner and retention task start after the writer admits a
READY store, so fresh construction logs no `mempool_durable_replay_failed` or
`mempool_event_retention_failed` warnings.

### Projector

Projector is ready only after it has admitted the canonical secondary,
acquired the required leases, opened or built the wallet primary, and reached
continuous following at an authenticated source position. An individually
healthy canonical or wallet store is insufficient if their source identities
do not agree.

A block does not withdraw readiness. Each new block leaves the wallet one block
behind the canonical fence until the projector applies it, so a ready projector
that trails the fence by at most one block, or follows a replacement at the
same height, stays `ready` for up to 30 seconds and reports the fence height as
its target. A projector that trails by more than one block, trails for longer
than 30 seconds, or was not ready when the trail began reports `syncing`. Each
block it applies starts a new grace.

### Native wallet query

Native wallet query is ready only when all three independently retained inputs
are healthy: the admitted canonical-and-wallet serving pair, the exact node
source used by every advertised node-backed operation, and the authenticated
ingest-control channel used by pair publication and live wallet operations.
No input can erase another input's failure. Capability discovery remains
immutable while any admitted dependency is temporarily unhealthy.

Before storage is opened or traffic is bound, `AdmittedIngestControl` connects
once and validates `ServerInfo` service identity, network, contract revision,
and the five methods required by the native composition. The accepted
identity and channel are then immutable. Health checks do not repeat structural
admission: they call `WriterStatus` and a bounded
`MempoolSnapshot(max_entries = 1)` through that same authenticated channel and
validate coherent network and tip evidence. Block arrival does not fail the
check: the writer answers a mempool read at a new tip once its replacement
generation is certified, waiting up to five seconds, and a snapshot that is
still not coherent returns `CHAIN_EPOCH_PIN_UNAVAILABLE`, which the probe
accepts as healthy. A transition into failure emits
one classified warning and drains readiness with
`ingest_control_unavailable`; recovery emits one event and restores only the
ingest-control input. A `WriterStatus` that reports a parked writer instead
drains readiness with `reorg_window_exceeded`, which takes precedence over the
serving-pair failures it causes. New native requests then fail with
`REORG_WINDOW_EXCEEDED`, and open `ChainEvents` streams end with the same
status. The serving-pair publisher reads the same halt from
`CanonicalControl.WriterStatus`, which is how lightwalletd compatibility,
which has no ingest-control probe, refuses traffic in both serving modes. The
health probe intentionally shares the configured serving-pair refresh cadence
because both observe the same control plane.

The node probe exercises `tip_id` and upstream synchronization health through
the same shared source handle installed in the query. It does not repeat
structural capability discovery. Any node-backed composition must admit
`TipId` before binding so this liveness prerequisite cannot fail later as a
configuration surprise. The serving-pair publisher, node-readiness probe, and
ingest-control-readiness probe are supervised with the gRPC server; an
unexpected exit drains readiness and terminates the runtime.

Native query reports `shutting_down` as soon as termination is requested and
ends every open server stream, including `ChainEvents` and `MempoolEvents`,
with `UNAVAILABLE`, `ErrorReason = SERVICE_NOT_READY`, and readiness cause
`shutting_down`. Event streams never end on their own, so without this the
graceful gRPC drain would wait for them indefinitely. The drain is also bounded
at 10 seconds, after which calls still open are abandoned, so query exits
within the wallet-serving supervisor's 20-second grace window.

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

### Explorer

Explorer is ready after its API has started and, when a materialized-view store
is open, while the latest catch-up of its materialized-view secondary
succeeded. The secondary catches up once per second and starts in `starting`
until the first catch-up succeeds. A failed catch-up refuses readiness with
`schema_mismatch` when the primary's store format, consumer schemas, or
column-family identity no longer match the binary, and with
`storage_unavailable` for any other failure. Validation runs after the
secondary has already advanced, so after a `schema_mismatch` the served view
may no longer match the binary. Explorer keeps answering gRPC in either case;
only `/readyz` refuses traffic. The next successful catch-up restores
readiness. The store already retries the known transient missing-file race
with primary compaction before it reports a failure.

Because a successful catch-up brings the secondary to the primary's latest
durable state, the secondary's lag behind its primary is bounded by the
catch-up interval while catch-up succeeds. How far the materialized views
trail the chain is reported per response in `ExplorerFreshness`, not through
readiness. Without a materialized-view store, Explorer has no catch-up input.
Explorer reports `shutting_down` as soon as termination is requested, before
its gRPC server drains.

### Cipherscan compatibility

`zinder-compat-cipherscan` owns no storage and no secondary. It connects to its
Explorer and wallet query endpoints before it reports ready, and a failed
connection fails startup. After that, readiness stays `ready` until
termination is requested, when it reports `shutting_down` before the REST
server drains.
Each upstream runtime reports its own outage through its own `/readyz`, and an
adapter request that reaches an unavailable upstream returns HTTP 503 with
`upstream_unavailable`. Reflecting
upstream readiness in the adapter would drain every adapter instance whenever
one upstream runtime is briefly unready, without making any request succeed.

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
  `hydrating`, `serving`, `rebuild_required`, and `halted` states. A normal
  tip change stays `serving`; `hydrating` means no generation has been
  certified yet. `halted` means the canonical writer parked on a reorg deeper
  than its window and the owner stopped following the node mempool until the
  process restarts.
- `zinder_mempool_source_tip_changes_total` counts source generations that
  ended at a tip boundary, including a `Mined` change for a block other than
  the serving tip.
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

### Wallet query requests

`zinder-query` and `zinder-compat-lightwalletd` both serve wallet reads through
`WalletServingQuery`, and both export one request series for them:

- `zinder_query_request_total{operation,status,error_class}` counts requests.
- `zinder_query_request_duration_seconds{operation,status,error_class}` is a
  histogram of request latency.
- `zinder_query_compact_block_range_block_count{status}` samples the block
  count each `compact_blocks_in_range` request asked for.

`status` is `ok` or `error`. `error_class` is `none` on success. On error it is
the snake-case name of the `QueryError` variant, such as
`block_range_too_large`, `artifact_unavailable`, or `chain_epoch_pin_unavailable`.
`operation` is the domain read, such as `visible_tip_block`,
`compact_blocks_in_range`, `transaction_by_id`, `tree_state_at`, or
`subtree_roots_in_range`.

Each `WalletServingQuery` request records exactly one sample when it returns.
The sample's duration covers argument validation, the storage read, and any
upstream node call, such as the tree-state fill or transaction broadcast. A
request rejected by validation is still recorded with its error class. A read
the serving pair cannot answer still records, with an error class such as
`endpoint_capability_unavailable` or `materialized_view_unavailable`, and so
does `network_upgrade_activations`, which reads no storage. A streaming read
records when the stream is established. Later sub-reads of the stream do not
record. One protocol request can produce several samples when it composes
several domain reads. For example, a compatibility request that pins the
visible tip first records a `visible_tip_block` sample.

The chain-event reads are the exception. `chain_events` page reads and
`resolve_chain_events_start` serve a stream that the adapter records, so they
record nothing themselves. The stream counts once and idle polling does not
inflate the request rate.

The native gRPC adapter records the operations it owns under the same metric
names, with the `tonic::Code` name as `error_class`:

- `chain_events` stream establishment.
- The ingest-control proxies `mempool_transaction`, `mempool_snapshot`, and
  `mempool_events`.

The native endpoint has no transparent balance method. A direct
`WalletServingQuery` balance read, such as the lightwalletd compatibility
balance request, records under `transparent_address_balance`.

The `ZinderWalletQueryP95High` alert and the overview dashboard read these
series.

### Serving-pair publisher

Native query and compatibility share the serving-pair publisher. Its metrics
report writer-status availability, catch-up duration, retired-pair teardown
duration, pair convergence attempts, published generation, replica lag, pair
admission failure, wallet-serving pair replacement, and the number of replaced
pairs retained for pinned reads
(`zinder_wallet_serving_pair_publisher_retained_pairs`). gRPC request metrics
remain separate from pair-maintenance metrics.

A request keeps the pair it captured open until it finishes, which pins that
pair's secondary generation. The publisher rotates through four generation
slots: the current pair, the candidate, and two for replaced pairs, which are
either retained for pinned reads or retired but still held by a request. When
a held retired pair leaves no slot free, the publisher releases a retained
pair that no request holds, so publication continues while up to two retired
pairs are pinned.
`zinder_wallet_serving_pair_publisher_pinned_retired_pairs` reports how many
retired pairs requests still hold, and
`zinder_wallet_serving_pair_publisher_generation_wait_total{reason="in_flight_requests"}`
counts refreshes that found every unpublished slot pinned and published
nothing. A steadily rising wait count means a request is holding pairs for
minutes, and the reader stops advancing until it ends.

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
`deploy/docker-compose.yml` and `deploy/systemd/`. It attaches to an external
Zebra or Zakura node selected entirely by a `deploy/.env.*` file, and
`deploy/node-cookie-permissions.yml` makes a non-Z3 node's cookie readable to
the Zinder runtimes. The runtime release-image catalog,
`deploy/release-images.json`, is limited to ingest, projector, native query,
and lightwalletd compatibility. A release also publishes the two
single-volume host images in `deploy/release-single-volume-images.json` for
hosts that attach one volume to one container: `zinder-wallet-serving`, which
is native-only and omits lightwalletd compatibility, and
`zinder-canonical-runtime`, an ingest-only diagnostic or canary runtime.
Neither is the complete supported composition. Every image is a target of the
single `deploy/Dockerfile`. Explorer, Cipherscan, and
PostgreSQL are not release classes.
