# Measure a wallet-serving deployment

Use a retained interval to report availability, wallet traffic, publication
delay, and memory by deployment. A successful scrape establishes what that
process reported at that instant. A successful wallet read establishes that
the serving path answered.

## Set up private collection

1. Keep an inventory outside the application. Give each expected target a
   Railway service, network, runtime role, and replica. Record when each role
   becomes expected and when it retires. In a single-volume container expect
   ingest on 9105, projector on 9110, and query on 9106. Add compatibility on
   9107 when enabled and Explorer on 9069 for the Explorer image. Record
   construction separately from steady-state serving. Each independent reader
   needs its own target.
2. Scrape `/metrics` every 15 seconds with a 10-second timeout as a starting
   point. The single-volume image binds ingest and projector ops to loopback.
   Place the scraper in the same network namespace to reach those listeners.
   A separate Railway service can scrape a privately bound listener after
   verifying reachability in the same environment. A service DNS name shared
   by multiple replicas needs per-replica discovery to preserve identity.
   Leave ops ports out of public routing. [Private networking](https://docs.railway.com/networking/private-networking)
3. Remote-write over an authenticated private or TLS connection to a backend
   outside the application volume. Provision 30 days of retention for daily
   and seven-day reports. Supply credentials from a mounted secret. Size the
   collector's write-ahead log for the tolerated outage and test interrupted
   delivery and recovery. Alert at the receiver on a missing collector
   heartbeat, stale last-received timestamp, increasing send failures, and
   a growing unsent backlog. [Prometheus remote write](https://prometheus.io/docs/practices/remote_write/)
4. Evaluate expected coverage at the retained backend, independent of target
   discovery. Match the full stable target identity. Distinguish a failed
   scrape (`up == 0`), a vanished target, an absent metric on a healthy target,
   and a collector outage. The reference Compose and smoke setups include
   [target rules](../reference/metrics.md#signals-worth-alerting-on). Configure
   a receiver and exercise delivery to the on-call operator.
5. Probe a safe read from the consumer network, for example
   `WalletQuery.VisibleTipBlock` or lightwalletd `GetLatestBlock`. Where
   Explorer is enabled, probe a block-detail read that traverses its wallet
   dependency. Record synthetic traffic separately at the caller. For each
   node, observe chain height, headers, peers, sync/verification progress,
   estimated gap, version, and last **successful** observation time. Give
   each probe its own availability series.

For a collector beside one Testnet wallet backend, this Prometheus agent
configuration scrapes the three mandatory processes. Replace the example
service, replica, and receiver with the identities from your inventory. Use
one static target per additional enabled role. The remote-write receiver
owns the retention setting.

```yaml
global:
  scrape_interval: 15s
  scrape_timeout: 10s
  external_labels:
    railway_service: wallet-backend
    replica: replica-a
    network: zcash-testnet
scrape_configs:
  - job_name: wallet-serving
    static_configs:
      - targets: ["127.0.0.1:9105"]
        labels: {service: zinder-ingest}
      - targets: ["127.0.0.1:9110"]
        labels: {service: zinder-projector}
      - targets: ["127.0.0.1:9106"]
        labels: {service: zinder-query}
    relabel_configs:
      - source_labels: [service]
        target_label: job
remote_write:
  - url: https://metrics.example.invalid/api/v1/write
    authorization:
      credentials_file: /run/secrets/metrics-write-token
```

Use stable target labels for joins. Keep deployment IDs, process IDs, store
lineage, block identity, and individual event observations in the inventory
or event store. Collect the shared cgroup only once per container. A separate
Railway service cannot inspect a sibling's process or loopback listeners.

## Retain lifecycle and error events

Keep platform deployment history and per-replica platform start, stop, and
termination events with UTC timestamps, cause when available, and deployment
ID. A webhook about a failed deployment can arrive after several restart
attempts. Reconcile provider events with actual container starts. Checkpoint
the last acknowledged event, allow overlapping reads, and deduplicate by
provider event ID or a verified source identity. Monitor event lag, delivery
failures, and gaps. Keep the retained copy independent of the terminating
container. [Railway deployment actions](https://docs.railway.com/deployments/deployment-actions)

Keep application tracing level, stable event name, source process, and
timestamp. Parse the tracing severity inside text logs. Railway can classify
all stderr output as errors. Keep the original incident evidence under
operator access controls. Record skipped or rate-limited log intervals.
[Railway logs](https://docs.railway.com/observability/logs)

Separate a child exit, a supervisor decision, and a subsequent platform
restart. On the Explorer image, `supervisor_child_exited` with
`role=zinder-explorer` and `action=restart_child` records an unexpected exit
and scheduled delay. A changed process start time confirms an observed new
process. Planned construction completion and forwarded shutdown have a
different meaning. Keep brief readiness drains even when they never qualify
as recurrence episodes. Record elapsed time per drain and the supervisor's
cumulative completed seconds separately. Treat a missing termination cause
as unknown.

Count fatal events, warnings, storage errors, failed epoch pins, and source
outages at their owners. `canonical_source_recovery` records onset or a
change of failure class. The recovery counter counts retries. A typed RPC
outcome and a failed inner store read can describe the same request.

## Report one interval

Record UTC start (inclusive), end (exclusive), release, commit,
deployment and replica lifetimes, network, enabled roles, scrape interval,
and each source's last observation. Show expected samples from scheduled
target time, received samples, successful scrapes, and the longest gap.
Include the number of observations behind every zero and percentile. Split
at deploys and process restarts. Keep earlier report windows and their raw
evidence when changing methods or scrape cadence.

| Question | Measurement |
| --- | --- |
| Did the service keep up and admit requests? | Use readiness, dependency causes, served-epoch age, ingest tip/lag, fresh independent node tip, and synthetic reads. Count transitions and elapsed refusal intervals from timestamped events where available. Polls alone can miss a flap. |
| How many wallet or Explorer requests failed? | Use completed wallet RPCs by surface, method, gRPC code, and typed reason. Use the readiness refusal counter for calls rejected at the gate. A mid-stream shutdown can return the same typed error. Compare with the user's HTTP or gRPC outcome and identify retry/probe traffic there. Use Explorer's operation errors to diagnose affected details. Record HTTP route templates and status at the HTTP API. |
| How much traffic and concurrency did readers serve? | Compute per-hour request increases per instance before aggregation. Report typical and peak hourly counts and sampled peak/p95 concurrency. Report concurrent response streams, balance uploads, and per-range block counts/durations, including cancellation. Short peaks can fall between scrapes. State the long-stream threshold used. |
| Did following survive reorgs and verify roots? | Count committed replacements per deployment. Old suffix depth is previous tip height minus common parent height. New suffix length is replacement tip height minus common parent height. Deduplicate shared network forks by chain identity. A successful subtree RPC proves transport success. Claim root agreement only with comparison evidence (first match, match on recheck, or persistent mismatch, per pool). |
| Did readers refresh, open more stores, or wait? | Use refresh errors by class, successful catch-up histogram count and buckets, cold opens after initial population in the same process, open/pinned generations, and generation waits by reason. A refresh can succeed without publishing. A `retention_window` wait counts a polling attempt. To measure elapsed slot waiting, time contiguous waits at their owner. |
| Which memory grew? | Graph per-process RSS and anonymous RSS alongside ingest cgroup current/max/anon/file. Compare 6/12/24-hour slopes and minima over multiple release cycles at comparable process ages. Correlate per-role cache, pinned cache, table readers, and memtable usage/budget with generations, traffic, primary flush/WAL activity, and block volume. The reported serving-role memtable budget already sums open generations. |
| Were upstream nodes healthy and supported? | Use node-native metrics, independent read-only probes, platform lifecycle/memory, and the node's named stall/eviction events. Compare the installed version to its upstream support condition. Keep the separate list of releases under compatibility qualification. |

For a release with listener accounting, this query estimates daily refusals
at the readiness gate, grouped by the affected method. Each method's counter
starts at zero when the listener is assembled. Check for a baseline scrape
and coverage through the end of the window.

```promql
sum by (railway_service, surface, method) (
  increase(zinder_wallet_rpc_readiness_refusals_total[24h])
)
```

Calculate daily successful catch-up p99 alongside its sample count. Scope
both queries to one deployment interval using your collector's target labels.

```promql
histogram_quantile(0.99, sum by (railway_service, job, le) (
  increase(zinder_wallet_serving_pair_publisher_catchup_duration_seconds_bucket{status="ok"}[24h])
))
```

```promql
sum by (railway_service, job) (
  increase(zinder_wallet_serving_pair_publisher_catchup_duration_seconds_count{status="ok"}[24h])
)
```

Counter increases are extrapolated. Classic bucket quantiles are interpolated.
A summary's published quantiles cannot be subtracted across scrapes. A lazy
counter's first event can precede its first sample. Record unknown coverage
when a process reset, missing baseline, or scrape gap prevents a count.
Check the release's metric definitions. Releases with failure-only publisher
refresh counts have no completed-attempt denominator. Scraping can also
affect older releases that drain histogram observations only on render.

## Measure publication delay

A publication measures when a reader installed a fence. A caller-side read
is needed to measure when a wallet actually observed it. Use one cohort of
commits per deployment, store lineage, and reader. Retain events after the
commit interval so late publications can resolve pending observations.

1. Collect committed append and replacement events from the writer, and
   publication events from each reader. Index by epoch **and** event sequence.
   Compare visible tip identity and sequence digest when the release supplies
   them. Attach process lifetime and reader role before combining logs. When
   two publishers share a log with no way to tell them apart, per-reader
   coverage is unknown.
2. For an **exact** observation, match the published fence to the committed
   fence. For **first covering**, find the earliest publication on a proven
   descendant whose visible height reaches the commit. Follow every intervening
   replacement. A block above its common parent has been superseded. Match a
   discovery event to the actual committed replacement when dealing with an
   older release that omits the common parent on commit. A prepared fork can
   fail to commit. Equal heights on different branches are distinct states.
3. Sort by source event time within each lineage, deduplicate replay, and
   checkpoint by source identity. Retain a watermark for out-of-order delivery.
   Mark a missing transition, conflicting fence, ambiguous fork, clock reversal,
   or expired history as unmatched. Leave an unresolved live tail pending.
   Never infer ancestry across a gap or store replacement.
4. Publish committed, exact, coalesced (first seen through a descendant),
   superseded before publication, unmatched, and pending counts. The five
   disjoint outcomes sum to committed. First covering includes exact and
   coalesced observations. Report each latency distribution with its own
   denominator, and save the timestamped observations for exact maxima and
   quantiles. If exporting histograms, include buckets beyond 10 seconds,
   including 30, 45, and 60 seconds, and the freshness objective.

Before switching a daily report, reconcile an overlapping day with the
archived process. Exercise a refused request during a short flap, a reader
restart, a disappeared target, and a same-height replacement. Record the
first deployment and time for which each new observation has coverage.
