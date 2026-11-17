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
| `wallet_transition_limit_exceeded` | The projector refused its next wallet transition because it needs more logical bytes than `projector.follow.max_transition_logical_bytes` allows, and stays parked. The detail carries the bytes the transition had accounted when it was refused, a lower bound on what it needs, and the configured limit. |
| `replica_lagging` | A RocksDB secondary exceeds the admitted epoch lag. |
| `serving_pair_stale` | A wallet-serving reader cannot refresh its published pair, which is still within its staleness ceiling. |
| `writer_status_unavailable` | A trusted reader cannot reach the writer control API. |
| `ingest_control_unavailable` | Native wallet live-state health failed on the admitted ingest-control channel. |
| `canonical_tip_unconfirmed` | The canonical writer has not confirmed its store's visible tip against its node since it opened the store, so wallet-serving readers do not serve its fence yet. |
| `cursor_at_risk` | Canonical event retention is approaching an active cursor. |
| `shutting_down` | New traffic has been drained for termination. |

`ready`, `cursor_at_risk`, and `serving_pair_stale` admit traffic; every other
cause refuses it.

A runtime can also report dependencies it keeps serving without. Native wallet
query reports its node source and ingest control this way, and lightwalletd
compatibility in wallet mode reports its node source. A dependency whose cause
does not admit traffic appears in the report's `degraded` list with its
`input` label (`node_source`, `ingest_control`, or
`materialized_view_catchup`) and its cause. The runtime's own `cause` can
still be `ready`, because only the requests that call the dependency fail. On
`/readyz`, `degraded` follows the top-level `cause`, and each entry names its
cause `dependency_cause`, so a parser that reads the first `"cause"` key gets
the runtime's own cause:

```json
{
  "status": "ready",
  "cause": "ready",
  "current_height": 3412870,
  "served_epoch_age_seconds": 420,
  "degraded": [
    {
      "input": "node_source",
      "dependency_cause": {
        "node_unavailable": {
          "failure_class": "node_unreachable",
          "last_reason": "upstream node is unavailable: connection refused",
          "consecutive_failures": 14,
          "outage_seconds": 390
        }
      }
    }
  ]
}
```

A wallet-serving reader also reports `served_epoch_age_seconds`, the time since
the canonical writer committed the chain epoch the reader serves. While the
node or the writer stops advancing, the reader keeps serving its last chain
view as ready, and this age is the signal that the view is stale.

Payload-bearing causes include structured detail. `node_unavailable` carries a
bounded failure class, sanitized reason, consecutive-failure count, and outage
duration. `upstream_not_ready` carries source heights, verification progress,
and the health-probe source. Reorg, wallet-transition, replica, serving-pair,
and canonical-retention causes carry the numeric boundary that failed.

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
4. the first refusing required input, in the order node source, ingest
   control, materialized-view catch-up;
5. otherwise the admission state, including a warning that admits traffic.

Each input is either required or a dependency, as the runtime declares. A
required input that refuses traffic supplies the cause at step 4. A dependency
never does. Its refusing cause is listed under `degraded` instead, unless it
is the writer halt of step 2, which refuses traffic from either kind of input.

The admission state supplies the reported heights. When shutdown or an input
supplies the cause, the reported target height is cleared. Every owner
replaces only its own input, so no owner can clear another owner's
observation. Ingest, native wallet query, lightwalletd compatibility, and
Explorer use this composition. Ingest's node-source input and Explorer's
materialized-view catch-up input are required inputs. Native query's node
source and ingest control, and compatibility's node source, are dependencies
([ADR-0045](../adrs/0045-wallet-serving-readiness-follows-the-serving-pair.md)).
Compact-blocks compatibility composes no input. Projector and Cipherscan
compatibility each keep one owner of their readiness state.

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

The runtimes differ in their bounds. Query and compatibility need the
writer's exact fence for every serving pair they publish. They stop at their
configured pair-convergence attempts or timeout, whichever comes first, and
wait the smaller of the catch-up interval and 100 ms between attempts.

The projector needs only the retained events its wallet has not applied yet.
Once its caught-up secondary holds such events, it stops converging even while
the writer is ahead. A writer that keeps committing, as it does during a burst
of blocks, would otherwise keep the wallet from advancing until the burst ends.
This applies from startup whenever a published wallet resumes following, so a
restart during a burst resumes at once. Only a fresh wallet build waits for the
writer to attest the secondary's exact fence, because it binds its build plan
to that fence.
The writer's retained-event page still authenticates every event before the
wallet applies it: the page's bounding fence must equal the secondary's fence
or be strictly ahead of it in both event sequence and chain epoch, and the
secondary's events must match the first events of the page field for field.

While the secondary holds nothing new for the wallet, the projector retries
with backoff from 50 ms, doubling to 1 s, and treats lag that lasts longer than
one second as trailing the writer, as described in [Projector](#projector).
An attempt that lasts `projector.fence_convergence_timeout_seconds` without
converging logs `canonical_secondary_behind_writer`, reports `replica_lagging`
with the last lag it observed, or `writer_status_unavailable` when no writer
status classified the secondary in that attempt, and starts another attempt.
Each such attempt also renews the wallet's canonical retention lease to its
full duration, so a long wait neither lets the lease expire nor lets the
writer prune the events the wallet still needs. Lag never stops the projector;
divergence still does.

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

Block arrival does not withdraw readiness. Each new block leaves the wallet
behind the writer until the projector applies it, and a burst of blocks keeps
it behind until the projector has applied the last of them. A ready projector
whose wallet trails the writer, by any number of blocks or by a replacement at
the same height, stays `ready` and reports the writer's height as its target
while both of these hold:

- the wallet applies a block at least every 30 seconds; and
- less than 2 minutes have passed since the wallet fell behind.

The projector reports `syncing` once either bound passes, or when it was not
ready as the trail began, and stays unready until its wallet matches a fence
the writer attests. Catching up ends the trail, so the next block starts a new
one. Both bounds expire on their own deadlines, even while the projector waits
on a slow request or a large transition. A canonical secondary that cannot
reach the writer within a convergence attempt reports `replica_lagging`
instead, until it gives the wallet a block to apply, as described in
[Canonical fence attestation](#canonical-fence-attestation). Every height the
projector reports is the wallet's authenticated height, never the canonical
secondary's.

A wallet transition that needs more logical bytes than
`projector.follow.max_transition_logical_bytes` allows leaves the wallet
unchanged, and the projector parks instead of exiting: the same wallet cursor
would plan the same transition again, and the same limit would refuse it at
the same point. A parked projector reports `wallet_transition_limit_exceeded`
with the wallet's height and the writer's, keeps and renews the one canonical
retention lease it holds, and refuses owner-control captures with
`FAILED_PRECONDITION`. It plans the next transition again only after the writer
commits, and resumes following only when that plan ends at a different fence,
as it does after a later reorg replaces the refused blocks. Otherwise it stays
parked until it restarts with a larger limit, which resumes from the
persisted cursor without a rebuild because the lease kept it retained.

### Native wallet query

Native wallet query is ready when its admitted canonical-and-wallet serving
pair admits traffic. Two other observations are retained independently as
dependencies:

- the exact node source used by every advertised node-backed operation; and
- the authenticated ingest-control channel used by pair publication and live
  wallet operations.

A dependency outage never closes the traffic gate. It appears under
`degraded`, and only the requests that call the dependency fail. No input can
erase another input's observation. Capability discovery remains immutable
while any admitted dependency is temporarily unhealthy.

| Request | Node unavailable or not synchronized | Ingest unreachable |
| --- | --- | --- |
| Stored reads: tips, blocks, compact blocks, mined transactions, transparent address reads, subtree roots, tree state at any height, the latest tree-state checkpoint, chain events, activations, and `ServerInfo` | Served | Served until `storage.serving_pair_staleness_ceiling_ms` passes |
| `BroadcastTransaction` | `UNAVAILABLE`, `NODE_UNAVAILABLE` | Served until the ceiling passes |
| `MempoolSnapshot`, `MempoolEvents`, and unmined `Transaction` lookups | Passed to ingest, which answers or fails them from its own mempool state; while that mempool hydrates or rebuilds from the node, `UNAVAILABLE`, `NODE_UNAVAILABLE` | `UNAVAILABLE`, `UPSTREAM_UNREACHABLE` |

A node-backed request always calls the node, whatever the node probe last
observed, so a node that recovers between probes serves it. The staleness
ceiling starts only when the publisher cannot read `WriterStatus`. An
ingest-control health failure that leaves `WriterStatus` answering, such as a
failing mempool snapshot, degrades the dependency without starting it.

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
accepts as healthy. A writer mempool that is hydrating or rebuilding from the
node answers `NODE_UNAVAILABLE`, which the probe also accepts: ingest readiness
and the reader's own node probe report that outage, so ingest control is not
blamed for it. A transition into failure emits one classified warning and
degrades the ingest-control dependency with `ingest_control_unavailable`;
recovery emits one event and clears only that dependency. A `WriterStatus`
that reports `upstream_not_ready` degrades the dependency with the writer's
`upstream_not_ready` detail instead, so `/readyz` names the syncing node as the
root cause. A `WriterStatus` that reports a parked writer refuses traffic with
`reorg_window_exceeded`, which takes precedence over the serving-pair failures
it causes. New native requests then fail with
`REORG_WINDOW_EXCEEDED`, and open `ChainEvents` streams end with the same
status. The serving-pair publisher reads the same halt from
`CanonicalControl.WriterStatus`, which is how lightwalletd compatibility,
which has no ingest-control probe, refuses traffic in both serving modes. The
health probe intentionally shares the configured serving-pair refresh cadence
because both observe the same control plane.

Losing ingest control entirely also stops pair refresh, because the publisher
reads `CanonicalControl.WriterStatus` through the same channel. The pair then
reports `serving_pair_stale` and refuses traffic with
`writer_status_unavailable` once `storage.serving_pair_staleness_ceiling_ms`
passes, as it does for any writer-status outage.

The node probe exercises `tip_id` and upstream synchronization health through
the same shared source handle installed in the query, and publishes
`node_unavailable`, `upstream_not_ready`, or `node_capability_missing` as the
node-source dependency's cause. It does not repeat structural capability
discovery. Transaction broadcast is the only node-backed operation; tree state
at any height is served from the serving pair. A query that does not advertise
broadcast therefore runs no node probe, and its node-source dependency is
never degraded. Any node-backed composition must admit `TipId` before binding
so this liveness prerequisite cannot fail later as a configuration surprise.
The serving-pair publisher, node-readiness probe, and
ingest-control-readiness probe are supervised with the gRPC server; an
unexpected exit drains readiness and terminates the runtime.

Native query reports `shutting_down` as soon as termination is requested and
ends every open server stream, including `ChainEvents` and `MempoolEvents`,
with `UNAVAILABLE`, `ErrorReason = SERVICE_NOT_READY`, and readiness cause
`shutting_down`. Event streams never end on their own, so without this the
graceful gRPC drain would wait for them indefinitely. The drain is also bounded
at 10 seconds, after which calls still open are abandoned, and the readiness
probes stop without waiting for a node or ingest-control request in flight, so
query exits within the wallet-serving supervisor's 20-second grace window.

When the shared traffic gate is closed, which only the serving pair, a parked
writer, or shutdown can cause, new gRPC calls return
`UNAVAILABLE` with `ErrorReason = SERVICE_NOT_READY` and
`ErrorInfo.metadata["readiness_cause"]` set to the stable readiness label.
`/readyz` carries the corresponding structured detail. This keeps a
readiness outage distinguishable from poisoned transport without mutating the
endpoint's immutable capability set.

Behind a parked projector, the wallet stops advancing, so query cannot publish
a newer pair. It keeps answering from its last pair while the writer stays
within `storage.secondary_replica_lag_threshold_chain_epochs`, reports
`serving_pair_stale` until `storage.serving_pair_staleness_ceiling_ms` passes,
and then refuses wallets with `SERVICE_NOT_READY` and readiness cause
`replica_lagging`. A query runtime that starts behind a parked projector cannot
converge its first pair, so its startup fails before its endpoints listen. The
single-host wallet-serving supervisor therefore does not start query while the
projector reports the park.

### Lightwalletd compatibility

Compatibility is ready only while `WalletServingPairPublisher` can reach writer
status, catch canonical and wallet secondaries up, and publish a pair that
passes exact-fence admission. In wallet mode, the node source behind
`SendTransaction` is a dependency. Its outage appears under `degraded`, and
`SendTransaction` fails with `UNAVAILABLE` while every other method, including
`GetTreeState` at any height, keeps answering. Compact-blocks
mode calls the node from no method and runs no node probe. Traffic uses a
readiness interceptor, so a process whose serving pair refuses traffic does
not accept new gRPC requests.
Compatibility bootstraps its serving pair through the writer-status endpoint
and uses separate ingest-control channels for mempool and tip-change traffic.
It does not instantiate the native query's admitted ingest type or advertise
native capabilities. Consumer support additionally requires the selected
lightwalletd client to pass its complete lifecycle through a trusted edge.

Writer-status transport failures, replica or projection lag, and a candidate
whose RocksDB open or catch-up fails on a table file the primary just deleted
report `serving_pair_stale` and keep serving the published pair, because that
pair is still exactly what the writer attested. `storage.serving_pair_staleness_ceiling_ms`
bounds how long that lasts, measured from the last writer attestation and
defaulting to five minutes; past it the underlying cause takes over and traffic
stops. A writer fence that disagrees with the pair, a schema mismatch, and an
absent pair slot fail closed immediately: the reader would otherwise present a
chain view the system of record no longer attests.

The ceiling governs admission on its own. `storage.secondary_replica_lag_threshold_chain_epochs`
decides when lag stops being routine and becomes a reported warning, not how
long a warned reader may keep answering.

The projector applies each block after the writer commits it, so a refresh
regularly finds the candidate's canonical and wallet secondaries at different
epochs and retries on the next interval. While the writer lag stays within that
threshold and the published pair still admits traffic, the publisher logs such
a `wallet_serving_pair_publisher_refresh_failed` event at debug level. It logs
every other refresh failure as a warning. Both increment
`zinder_wallet_serving_pair_publisher_refresh_total{status="error"}` with their
`error_class`, such as `projection_behind`.

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

## Memory allocator

On Linux, every service binary allocates through jemalloc, which also replaces
the C library `malloc` that RocksDB uses. Concurrent storage reads therefore do
not wait on shared allocator locks. Tune the allocator with `MALLOC_CONF`.
Glibc malloc variables, such as `MALLOC_ARENA_MAX`, have no effect.
`MALLOC_CONF=stats_print:true` prints allocator statistics when the process
exits. [ADR-0044](../adrs/0044-service-binaries-allocate-through-jemalloc.md)
records the decision.

## Metrics

Every runtime exports build information and readiness state.
`zinder_build_info` is a constant gauge labeled by service, product version,
build Git commit, and network. `zinder_readiness_state{cause}` is 1 for the
runtime's current readiness cause. A runtime with dependency inputs also
exports `zinder_readiness_dependency_state{input, cause}`, which is 1 for each
dependency's current cause and 0 for every other cause label. The gauge
reports the cause the dependency published, so a writer's
`reorg_window_exceeded` halt read through ingest control appears on it even
though it is the runtime's own cause and not a `degraded` entry. A
wallet-serving reader exports `zinder_readiness_served_epoch_age_seconds`.
Alert on a dependency gauge whose cause does not admit traffic and is not
`reorg_window_exceeded`, and on a served-epoch age that has grown past several
block intervals, because a reader keeps answering 200 through both. Important runtime metrics include:

### Canonical writer

- `zinder_ingest_canonical_tip_height`
- `zinder_ingest_canonical_lag_blocks`
- `zinder_ingest_canonical_chain_epoch`
- `zinder_ingest_canonical_chain_event_sequence`
- `zinder_ingest_canonical_live_appends_total`
- `zinder_ingest_canonical_live_replacements_total`
- `zinder_ingest_canonical_live_commit_seconds`
- `zinder_ingest_canonical_source_recoveries_total{failure_class}`
- source fetch queue, reservation, response-size, and reassembly gauges
- construction watermark and persistence measurements
- `zinder_ingest_canonical_historical_prevout_reads_total`
- `zinder_ingest_canonical_cross_block_wallet_reads_total`

`zinder_ingest_canonical_source_recoveries_total` counts follower iterations
that failed on a source error, kept the fence, and retried after a backoff. Its
`failure_class` label uses the readiness class names, such as
`upstream_view_changed` or `protocol_mismatch`. A short outage can end before a
readiness probe sees it, so this counter and the `canonical_source_recovery`
warning are the lasting record.

The last two counters in the list are expected to remain zero and protect the
canonical block-local boundary.

### Explorer view replay

- `zinder_materialized_view_replay_height` and
  `zinder_materialized_view_replay_lag_blocks` report the shared replay
  position and its distance from the canonical tip.
- `zinder_materialized_view_replay_paused` is `1` while replay is paused for
  any reason. `zinder_materialized_view_replay_phase_gate` and
  `zinder_materialized_view_replay_volume_space_low` show whether canonical
  bulk catch-up or the free-space guard holds it.
- `zinder_materialized_view_replay_volume_available_bytes` and
  `zinder_materialized_view_replay_volume_min_free_bytes` report the last
  sample of the store volume and the configured
  `ingest.explorer_views_min_free_bytes`.
- `zinder_materialized_view_prevout_resolutions_total{source,cache}` counts
  the distinct spent outputs each hydrated range resolved, by where they were
  found: `range`, `cache`, or `canonical`. `cache` names the walker: `replay`,
  `ranking_build`, or `none` for a backfill that hydrates without a prevout
  cache.
- `zinder_materialized_view_prevout_block_reads_total{cache}` counts the
  producing blocks each walker read from canonical storage to resolve its
  `canonical` share.
- In ingest, the `zinder_store_*` gauges with
  `store_role="canonical_secondary"` describe the canonical secondary that
  hydration reads through. Producing replay rows
  bypass its block cache, so `rocksdb.block.cache.data.miss` against
  `rocksdb.block.cache.data.hit` in `zinder_store_rocksdb_ticker` shows how
  often a transaction-location lookup reached the volume.
- `zinder_materialized_view_prevout_cache_outputs{cache}` reports how many
  unspent outputs the `replay` and `ranking_build` prevout caches hold, and
  `zinder_materialized_view_prevout_cache_evicted_outputs_total{cache}` counts
  the outputs each dropped to stay within its bound.
- `zinder_materialized_view_transparent_address_ranking_build_pending` is `1`
  while an inactive transparent-address ranking build is pending.
  `zinder_materialized_view_transparent_address_ranking_build_height` reports
  the height through which it has applied history, and
  `zinder_materialized_view_transparent_address_ranking_build_target_height`
  the height it is walking toward: the other views' materialized height,
  capped at the settled tip.
- `zinder_materialized_view_replay_waiting_for_ranking_build` is `1` while
  replay dispatches no page because a ranking build trails views that rebuild
  without a shared cursor. Replay stops waiting when the build joins, or when it
  applies no history for 5 minutes and logs
  `materialized_view_replay_ranking_build_stalled`.

A replay or ranking build that resumes partway through history warms its empty
prevout cache first and logs `materialized_view_prevout_cache_warmed` with the
walked range and the time its last call took. When a ranking build joins
replay, replay takes over the build's cache instead and logs
`materialized_view_prevout_cache_handed_to_replay`. A steady `canonical` share after
the warm-up means the cache is evicting outputs that are still spent later,
which the eviction counter confirms, and the producing-block reads set the
replay speed. A walker that paused after its cache held outputs resumes cold
and does not warm again until the process restarts, so its `canonical` share
stays high until the cache refills.

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
wallet source position, projection lag, digest validation, and checkpoint
operations. Store metrics use explicit canonical or wallet role labels.

### Wallet query requests

`zinder-query` and `zinder-compat-lightwalletd` both serve wallet reads through
`WalletServingQuery`, and both export one request series for them:

- `zinder_query_request_total{operation,status,error_class}` counts requests.
- `zinder_query_request_duration_seconds{operation,status,error_class}` is a
  histogram of request latency.
- `zinder_query_compact_block_range_block_count{status}` samples the block
  count each `compact_blocks_in_range` request asked for. A compatibility
  block-range chunk asks for up to 1,000 blocks and can keep fewer under its
  byte cap; the sample counts what it asked for.
- `zinder_query_tree_state_replayed_blocks` samples the compact blocks each
  `tree_state_at` reconstruction replayed, from 1 to 99. A height with a
  stored checkpoint records no sample.
- `zinder_query_tree_state_replay_refusals_total` counts `tree_state_at`
  requests refused with `tree_state_replay_limit_reached` because the query
  already ran one reconstruction per available CPU. A sustained rate means
  wallets anchor on construction-built history faster than the host replays
  it.

`status` is `ok` or `error`. `error_class` is `none` on success. On error it is
the snake-case name of the `QueryError` variant, such as
`block_range_too_large`, `artifact_unavailable`, or `chain_epoch_pin_unavailable`.
`operation` is the domain read, such as `visible_tip_block`,
`compact_blocks_in_range`, `transaction_by_id`, `tree_state_at`, or
`subtree_roots_in_range`.

Each `WalletServingQuery` request records exactly one sample when it returns.
The sample's duration covers argument validation, the storage read, including
any tree-state reconstruction, and the upstream node call of a transaction
broadcast. A request rejected by validation is still recorded with its error
class. A read
the serving pair cannot answer still records, with an error class such as
`endpoint_capability_unavailable` or `materialized_view_unavailable`, and so
does `network_upgrade_activations`, which reads no storage. A streaming read,
such as `full_blocks_in_range` or `transparent_address_unspent_outputs`,
records when the stream is established. Later sub-reads of the stream do not
record, and neither does a failure that ends the stream later. One protocol request can produce several samples when it composes
several domain reads. For example, a compatibility request that pins the
visible tip first records a `visible_tip_block` sample. A compatibility
`GetBlockRange` or `GetBlockRangeNullifiers` stream records one
`visible_tip_block` and one `compact_blocks_in_range` sample for each chunk
after the first. A chunk holds at most 1,000 blocks and 2 MiB of stored
compact blocks, so a range of large blocks records more samples than its
width alone implies.

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

A request keeps the pair it captured open while it reads, which pins that
pair's secondary generation. The publisher rotates through four generation
slots. The current pair takes one. Each of the other three holds the
candidate, a replaced pair retained for pinned reads, or a retired pair that a
request still holds. A replaced pair stays retained for one minute after it
was replaced, so a pin on its epoch is answered for that minute however fast
blocks arrive. While no slot is free, the publisher publishes nothing.

The publisher opens a generation slot cold the first time it uses it: it opens
both secondaries and runs their full admission, which takes about 25 seconds
on a Testnet store. Once a slot's pair drains, the publisher catches the same
open secondaries up in place for a later block, which takes milliseconds. It
uses a slot it has not opened yet only when no opened slot has drained. Startup
opens the first slot, and the first block after it opens the second. The third
opens on the first block that arrives within one minute of the previous
publication, because the pair that publication replaced is still retained. The
fourth opens the first time three publications fall within one minute, or when
a request still holds a released pair. With 75-second blocks, these opens can
come long after startup. Once all four are open, the publisher never opens
another slot to keep up with blocks. A slot whose catch-up or publication
fails is closed and opened cold again when it is next used.

An opened slot stays open after its pair drains, and its secondaries keep
open the primary files their table caches hold, including files the primaries
have since compacted away. The publisher takes drained slots in turn, starting
after the slot it last published, so each opened slot is caught up again
within a few publications and releases the files it no longer references.
The disk a drained slot holds beyond the primaries is therefore limited to
what the last few publications compacted away.

A secondary holds at most `max_open_files` table files open, so a published
pair can name a file it does not hold. When the writer's or the projector's
compaction deletes that file, a read that needs it fails with an I/O error
that names the missing `.sst` path. Query then handles the read this way:

1. It marks the pair, which leaves retention at once.
2. The read that marks the pair wakes the publisher, which publishes a
   replacement at the writer's fence, even when the writer has not moved. A
   recycled slot makes the replacement in about the time of one catch-up.
   Later reads that hit the marked pair wait for the same publication without
   waking the publisher again. When the replacement cannot be published yet,
   the next periodic refresh, every `storage.secondary_catchup_interval_ms`,
   tries again.
3. It waits up to five seconds for the replacement and reads once more there
   when that pair answers its epoch.

The client sees one of three outcomes:

- an answer from the replacement;
- `CHAIN_EPOCH_PIN_UNAVAILABLE` when the read was pinned to an epoch that only
  the marked pair served; or
- `STORAGE_UNAVAILABLE` when no replacement was published within five
  seconds, for example while every slot is taken or the replacement needs a
  cold open.

Marking the pair does not change readiness. The refresh that prepares the
replacement reports readiness as every refresh does, even when the writer's
fence has not moved: a candidate failure other than those that report
`serving_pair_stale` fails closed until a later refresh succeeds. Corruption
and every other storage failure fail the read as before and leave the pair
published.

`zinder_wallet_serving_missing_table_file_pairs_total` counts the pairs marked
this way, once per pair however many reads hit the file. Each mark also logs
one `wallet_serving_pair_table_file_missing` warning with the pair's
`chain_epoch` and an `error` that includes the RocksDB status naming the
missing file. The path shows which primary deleted it: the canonical store or
the wallet store. An occasional increment is routine compaction overtaking a
published pair, and the replacement publication follows it. A count that
rises with most
publications means files are replaced faster than pairs are, such as during a
manual compaction or a bulk rewrite of the primary. If query still logs
`STORAGE_UNAVAILABLE` for those reads, compare the warnings with
`zinder_wallet_serving_pair_publisher_generation_wait_total` and the
`wallet_serving_pair_publisher_refresh_failed` warnings: a replacement that
cannot find a free slot, or keeps failing, leaves the marked pair current.

A candidate can also fail with the same missing-file error when the primary
deletes a file between listing and opening it. That happens during the
candidate's RocksDB open or catch-up, during the canonical store-control check
that scans the store's default family right after each of them, or during the
canonical read that admits the candidate as a pair. That refresh failure
reports `serving_pair_stale` and keeps the published pair serving until the
staleness ceiling, as a writer-status outage does. It counts under
`zinder_wallet_serving_pair_publisher_refresh_total` with the
`canonical_secondary` or `wallet_secondary` `error_class`, and the next refresh
tries again. Any other failure of those checks fails closed as before.

`zinder_wallet_serving_pair_publisher_candidate_prepared_total{kind}` counts
candidates by `kind`: `cold` for a slot opened from the primaries, and
`recycled` for a drained generation caught up in place.
`zinder_wallet_serving_pair_publisher_cold_open_duration_seconds{status}`
records each cold open, and each successful one logs
`wallet_serving_pair_publisher_generation_opened` with its `generation` and
`elapsed_ms`. Without refresh failures, the `cold` count stays at four or
fewer, one for each slot, and every later candidate is `recycled`. A
`generation_opened` event that repeats a `generation` already opened since
startup means a failed refresh closed that slot, and the
`wallet_serving_pair_publisher_refresh_failed` warning before it names the
error. A `cold` count that keeps rising means refreshes keep failing and
closing their generations.

`zinder_wallet_serving_pair_publisher_open_generations` reports how many slots
hold open secondaries, at most four. Every open generation reads through the
publisher's one canonical and one wallet block cache, so the
`canonical_secondary` and `wallet_secondary` block-cache gauges describe one
cache each however many generations are open, and the memtable gauges grow
with this count. [RocksDB](#rocksdb) lists them.

Each refresh publishes the writer's fence at the moment its candidate
converges, not one pair for each height. Blocks that commit closer together
than one refresh share one publication at the later height, and the earlier
heights get no publication of their own. This happens while ingest catches up
after a stall, when a reorg replaces several blocks, and on Regtest. The
publisher refreshes every `storage.secondary_catchup_interval_ms` (1,000 ms by
default), and the projector applies a block within one 250 ms follow poll. A
block therefore reaches wallet serving within about 1.25 seconds plus catch-up
of its canonical commit. To measure that delay, match each
`canonical_live_append_committed` with the first later
`wallet_serving_pair_publisher_published` whose `event_sequence` is at least
the commit's `chain_event_sequence`. A block that needs a slot not yet opened
waits for that cold open instead.

Blocks also share a publication when more than three arrive within one
minute. The three replaced pairs are then all inside their retention minute,
so no slot is free, and the next block waits until the oldest pair's minute
ends. The first refresh after that publishes the writer's latest fence, and
every block committed during the wait shares that publication. Each such
refresh counts as a wait with `reason="retention_window"`. With 75-second
blocks this happens only in bursts. It also happens while ingest catches up
after a stall, when the publisher would otherwise publish once per refresh.
Readiness keeps admitting traffic during the wait, and reports
`serving_pair_stale` once the writer is more than
`storage.secondary_replica_lag_threshold_chain_epochs` ahead. A wait ends
within about 90 seconds of the previous publication, well inside the
five-minute default of `storage.serving_pair_staleness_ceiling_ms`. A ceiling
set below 90 seconds can stop traffic during a long burst.

A request may keep reading a retired pair for at most 30 seconds after the
publisher retired it. Past that read limit the publisher revokes reads on the
pair. The `TransparentAddressUnspentOutputs` and `FullBlocksInRange` streams,
the only reads that keep a pair across several page reads, stop at their next
page or while they wait for a slow client, end with
`CHAIN_EPOCH_PIN_UNAVAILABLE`, and drop the pair. Every other read is one
bounded blocking read that finishes on its own. A stream whose client cancels or disconnects stops after
the page in progress. A slow or abandoned stream can therefore hold
publication for at most the read limit plus one page read after its pair
leaves retention. Retention and slow streams together hold a publication for
at most about 90 seconds. A lightwalletd `GetBlockRange` stream holds a pair
only while it reads one chunk, so it takes no part in the read limit or in
this bound.

`zinder_wallet_serving_pair_publisher_pinned_retired_pairs` reports how many
retired pairs requests still hold.
`zinder_wallet_serving_pair_publisher_generation_wait_total{reason}` counts
refreshes that found every unpublished slot taken and published nothing:
`retention_window` while a retained pair takes a slot, and
`in_flight_requests` while only retired pairs that requests hold take them.
`zinder_wallet_serving_pair_publisher_revoked_pairs_total` counts the
retired pairs whose reads the publisher revoked, and each revocation logs a
`wallet_serving_pair_reads_revoked` warning with the pair's chain epoch.
`in_flight_requests` waits that keep rising while revocations stay flat point
at reads that never check
for revocation; revocations that keep rising point at clients whose streams
outlast the read limit, such as a client that stops consuming an unspent-output
stream without cancelling it.

### Compatibility block-range streams

`GetBlockRange`, and `GetBlockRangeNullifiers` in wallet serving, read a range
one chunk at a time. A chunk holds at most 1,000 blocks and 2 MiB of stored
compact blocks, or one larger block. Each chunk after the first reads
the chain view current when it is read and holds its serving pair, or its
compact-blocks generation, only for that read. A slow client therefore holds
no pair or generation while the server waits for it: in wallet serving its
stream takes no part in retention or the read limit, and in compact-blocks
serving the retired generation drains within one chunk read. A long stream
should not coincide with `zinder_compat_compact_refresh_failures_total{cause="replica_lagging"}`
or `compact_serving_refresh_failed` warnings.

A later chunk continues a stream only when it links by hash to the last block
the stream sent
([ADR-0038](../adrs/0038-wallet-runtime-composition-and-capability-discovery.md#compatibility-block-range-streams)).
`zinder_compat_block_range_reorg_endings_total{cause}` counts streams that
ended early at a reorg:

- `chain_replaced`: the next chunk did not link, because a reorg replaced the
  block it had to link to.
- `tip_lowered`: a reorg left the visible tip below a height the stream still
  had to send.

Each ending also logs `compat_block_range_ended_by_reorg` at `info` under the
`zinder::compat_lightwalletd` target, with `cause`, `last_sent_height`,
`sent_chain_epoch`, `current_chain_epoch`, `next_chunk_start`, and
`next_chunk_end`. The client receives `ABORTED` for `chain_replaced` or
`OUT_OF_RANGE` for `tip_lowered`, as lightwalletd returns, and requests again.
An ending is therefore not an error on its own. Endings that keep rising while
ingest logs no `canonical_live_replacement_committed` events point at a fault
in the chain view the compatibility runtime serves.

A chunk that links at a newer chain view than the chunk before it logs
`compat_block_range_continued_at_newer_epoch` at `debug` under the same
target, with `last_sent_height`, `sent_chain_epoch`, `current_chain_epoch`,
`next_chunk_start`, and `next_chunk_end`. Enable `debug` for that target to
see whether long streams cross publications. The event has no metric.

`zinder_compat_block_range_streams_open` reports how many block-range
streams hold a slot of `compat.max_concurrent_block_range_streams`, which the
`compat_started` log records as `max_concurrent_block_range_streams`. Compare
its peaks with the limit to see how close wallets come to it.
`zinder_compat_block_range_stream_refusals_total` counts block-range requests
refused with `RESOURCE_EXHAUSTED` because every slot was taken. A count that
keeps rising means wallets wait for slots: raise the limit if memory allows,
or add a compatibility replica. Refusals while few wallets sync can mean one
client holds many streams without reading them. The compatibility server
cannot tell clients apart, so limit concurrent requests per client at the
edge. The
[compatibility reference](../reference/lightwalletd-compatibility.md#block-range-resource-limits)
sizes one stream.

### RocksDB

`zinder-store` exports bounded role-labelled metrics for block-cache capacity
and use, memtables, WAL bytes and limits, pending compaction, active
compactions, write stops, bytes read and written, MultiGet behavior, filter
tickers, read latency, and startup open duration.

Ingest exports these series for the canonical secondary that hydration reads
through (`canonical_secondary`) and for its materialized-view primary
(`materialized_view_primary`), and Explorer exports them for its
materialized-view secondary (`materialized_view_secondary`). None of those
stores configures a filter, so their `rocksdb.bloom.filter.*` tickers stay at
zero.

Query and the compatibility runtime keep several secondaries of a store open
and share one block cache per store among them. They export a smaller set for
each store, labelled `canonical_secondary` and, in wallet serving,
`wallet_secondary`:

- `zinder_store_block_cache_capacity_bytes`,
  `zinder_store_block_cache_usage_bytes`, and
  `zinder_store_block_cache_pinned_usage_bytes` describe the one shared cache
  and update after each refresh. Usage can pass capacity by the index and
  filter blocks that open table files pin.
- `zinder_store_memtable_budget_bytes` and
  `zinder_store_memtable_budget_usage_bytes` sum the memtable budget and usage
  of every open generation.
- `zinder_store_table_reader_memory_bytes` sums the table-reader memory of
  every open generation that the block cache does not hold.

The memtable and table-reader sums update at most once every 15 seconds,
because sampling table-reader memory looks up every table file's reader. A sum
can therefore trail a generation that opened or closed since the last sample.

They export no per-generation properties or tickers. A `canonical_secondary`
series therefore describes the hydration secondary in ingest and the shared
serving cache in query or the compatibility runtime; tell them apart by the
scrape target, not by `store_role`. To tell cache fill from other memory
growth in a serving process, subtract its cache usage, memtable usage, and
table-reader memory from its anonymous resident memory. Once the caches stop
growing, a remainder that keeps rising is not cache.

The projector's wallet primary, whose four hash-keyed families carry filters,
does not export `zinder_store_*` series, and the serving wallet secondaries
export only the set above. While `wallet.rocksdb.statistics_level` is
`tickers` or `full`, RocksDB writes the projector's wallet primary statistics
to the `LOG` file in the wallet store directory every 10 minutes; wallet
secondaries do not write them. Read the filters there:

- The false-positive rate is `(full.positive - full.true.positive) /
  (full.positive - full.true.positive + useful)`, from the
  `rocksdb.bloom.filter.*` counters. Expect about 1% for the wallet filters.
- `rocksdb.block.cache.filter.miss` shows whether the filter working set fits
  the wallet block cache. A count that keeps rising against
  `rocksdb.block.cache.filter.hit` means filter partitions are evicted and
  re-read; size `wallet.rocksdb.block_cache_bytes` as
  [ADR-0020](../adrs/0020-bounded-rocksdb-resource-budget.md) describes.

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

Explorer view replay logs `materialized_view_replay_volume_space_low` once
when the free-space guard pauses replay and
`materialized_view_replay_volume_space_recovered` once when it resumes. Both
carry `available_bytes` and `min_free_bytes`.

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

Recoverable source errors drain ingest readiness and retry from durable state.
Wallet-serving readers report the same errors as degraded dependencies and keep
serving stored reads. Store identity, corruption, schema, and reorg-policy
failures fail closed and require operator action.

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
`deploy/release-images.json`, holds ingest, projector, native query,
lightwalletd compatibility, and the optional Explorer, which the
`deploy/docker-compose.explorer.yml` overlay adds to that composition. A
release also publishes the three single-volume host images in
`deploy/release-single-volume-images.json` for hosts that attach one volume to
one container: `zinder-wallet-serving`, which is native-only and omits
lightwalletd compatibility; `zinder-wallet-serving-explorer`, which adds the
Explorer to it; and `zinder-canonical-runtime`, an ingest-only diagnostic or
canary runtime. None is the complete supported composition. Every image is a
target of the single `deploy/Dockerfile`. Cipherscan and PostgreSQL are not
release classes.
