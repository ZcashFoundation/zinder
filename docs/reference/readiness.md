# Readiness

This page lists what each Zinder runtime reports on its health and readiness
endpoints, what every readiness cause means, and what you should do about it.
It is for operators who wire probes and alerts or who triage an incident.

Each runtime serves three routes on its operational endpoint,
`ops.listen_addr`:

| Route | Answers | Status code |
| --- | --- | --- |
| `/healthz` | Whether the process is alive, with its identity. | Always 200 while the process answers HTTP. |
| `/readyz` | Whether the runtime should receive traffic, and why not. | 200 when the cause admits traffic, 503 otherwise. |
| `/metrics` | Prometheus metrics. See [Metrics](metrics.md). | 200 |

A live process is not always ready. A writer can be alive while it catches
up, and a reader can be alive while it cannot attest the chain view it would
serve. Point liveness probes at `/healthz` and load balancers at `/readyz`.

The operational endpoint serves without authentication. Keep it private, as
[Service operations](../architecture/service-operations.md#security-boundary)
describes.

`zinder-query`, `zinder-compat-lightwalletd`, and `zinder-explorer` also report
readiness on their gRPC listeners, as [gRPC health](#grpc-health) describes.

## Response bodies

`/healthz` returns `status` (`alive`), `service`, `version`, `git_commit`,
`network`, and `capabilities`, the capability strings the runtime advertises.
The Explorer snapshots these at startup. Query `ExplorerQuery.ServerInfo` for
its current [capabilities](capabilities.md#explorer-capabilities).
A local build reports `git_commit` as `unknown`.

`/readyz` returns these fields. Fields without a value are omitted.

| Field | Meaning |
| --- | --- |
| `status` | `ready` or `not_ready`. |
| `cause` | The runtime's own readiness cause, a string or an object with detail. |
| `phase` | Ingest only: `bulk_catchup` or `following_tip`. |
| `current_height`, `target_height` | The height the runtime serves or has written, and the height it is moving toward. |
| `served_epoch_age_seconds` | Readers only (wallet-serving readers and compact-blocks compatibility): seconds since the writer committed the chain state this reader serves. |
| `degraded` | Dependencies the runtime serves without, whose cause refuses traffic. Each entry has `input` and `dependency_cause`. |
| `active_node` | Ingest only, with fallback nodes configured: `{ "position": N }`, where 0 is `[node]`. |
| `materialized_view_identities` | The Explorer view stores the runtime admitted. |

A wallet-serving reader during a node outage answers like this:

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

`degraded` entries use the key `dependency_cause`, and the list follows
`cause`. A script that reads the first `"cause"` key therefore gets the
runtime's own cause.

## Supervisor endpoint (`zinder serve` / `zinder index`)

When running under the unified `zinder serve` (default `:9108`) or `zinder index` (default `:9105`) supervisor, the parent process binds its operational HTTP endpoint at **T=0** before child stores finish constructing:

- `/healthz`: returns HTTP 200 immediately with `{"status":"alive","service":"zinder","version":...,"git_commit":...,"network":...}` so container liveness and startup probes succeed while initial sync runs.
- `/readyz`: aggregates every supervised role into `{"status":"ready"|"not_ready","mode":...,"preset":...,"network":...,"phase":"bootstrapping_canonical"|"catching_up_wallet_projection"|"waiting_for_explorer_views"|"waiting_for_serving_listeners"|"ready"|"shutting_down"|"failed","roles":[...]}` and returns HTTP 200 only once every required role reports ready.
- `/metrics`: exports `zinder_supervisor_ready` and `zinder_supervisor_role_ready` alongside scraped child role metrics.

## Container healthcheck

The unified distroless image has no shell or HTTP client, so probes invoke `zinder healthcheck` directly:

```bash
zinder healthcheck
zinder healthcheck --ready --addr 127.0.0.1:9108
zinder healthcheck --addr 127.0.0.1:9105 --path /healthz
```

The probe sends one HTTP/1.1 `GET` and exits 0 only when the endpoint answers 200. Otherwise it exits 1 and prints the reason to stderr, which Docker records as the failure reason.

The flags and defaults below apply to `zinder healthcheck`:

| Flag | Default (`zinder healthcheck`) |
| --- | --- |
| `--ready` | Probes `/readyz` instead of `/healthz` (unless `--path` is passed). |
| `--path` | `/healthz`. Override with `--path /readyz` or pass `--ready`. |
| `--addr` | `ZINDER_OPS__LISTEN_ADDR`, then `[ops] listen_addr` in the `--config` file, then `127.0.0.1:9108`. |

Standalone role binaries have two CLI differences:
- `zinder-ingest`, `zinder-projector`, `zinder-query`, `zinder-compat-lightwalletd`, and `zinder-explorer` accept `healthcheck --addr <ADDR> --path <PATH> --config <PATH>`, default `--path` to `/readyz` (with no `--ready` flag; pass `--path /healthz` for liveness), and default `--addr` to each role's own ops port.
- `zinder-compat-cipherscan` has no `healthcheck` subcommand.

- The probe reads only `[ops] listen_addr` from the config file. It skips a
  file that is missing, unreadable, or not TOML.
- It ignores `--ops-listen-addr`. Pass a non-default address with `--addr`.
- An unspecified host probes loopback. `0.0.0.0` probes `127.0.0.1`, and
  `[::]` probes `[::1]`, then `127.0.0.1`.
- An empty address disables the endpoint, so the probe fails without
  connecting.
- The whole probe takes at most 3 seconds, below the 5-second timeout of the
  image and Compose healthchecks.

In `deploy/docker-compose.yml`, `zinder-index` (`:9105`) and `zinder-serve` (`:9108`) both run `zinder healthcheck` without `--ready`, probing `/healthz` liveness so Docker can keep containers marked healthy while initial construction and catch-up run. Always check readiness separately via `/readyz` (`http://127.0.0.1:9108/readyz`, `zinder healthcheck --ready --addr 127.0.0.1:9108`, or `grpc.health.v1.Health`) before routing client traffic. The single OCI image sets `HEALTHCHECK ... CMD ["/usr/local/bin/zinder", "healthcheck"]` for `/healthz`, while Kubernetes readiness probes query `/readyz` on `:9108` (see [Deploying the single-volume container](../runbooks/deploying-single-volume.md)).

## gRPC health

`zinder-query`, `zinder-explorer`, and all three serving modes of
`zinder-compat-lightwalletd` serve [`grpc.health.v1.Health`](https://github.com/grpc/grpc/blob/master/doc/health-checking.md)
(`Check` and `Watch`) on their public gRPC listeners. gRPC-aware load
balancers, Envoy, and Kubernetes `grpc` probes can then ask the serving port
directly. Each gRPC listener reads the same readiness state as its own
`/readyz` endpoint.

| Status | When |
| --- | --- |
| `SERVING` | `/readyz` answers 200. Causes that admit traffic with a warning, such as `serving_pair_stale`, and a dependency that is degraded but not required to serve, stay `SERVING`. |
| `NOT_SERVING` | `/readyz` answers 503, including `shutting_down` once shutdown starts. |

An empty service name asks about the whole listener. The name of the service
the listener serves (`zinder.v1.wallet.WalletQuery`,
`cash.z.wallet.sdk.rpc.CompactTxStreamer`, or `zinder.v1.explorer.ExplorerQuery`)
returns the same status. Any other name fails `Check` with `NOT_FOUND`, and
`Watch` reports `SERVICE_UNKNOWN`.
There is no per-method or per-capability status.

Use `zinder-query grpc-healthcheck <ADDR>` to probe any of these plaintext gRPC
listeners. For example, `zinder-query grpc-healthcheck '[::1]:9067'`
checks the single-volume lightwalletd listener. The command exits 0 for
`SERVING`, 3 for a non-serving health response, and 1 if the call fails.
Connecting and receiving the reply share a three-second deadline.

The health service and gRPC reflection stay reachable while the runtime
refuses wallet or Explorer calls with `SERVICE_NOT_READY`. Both accept calls
without an Explorer bearer token. Reflection describes `grpc.health.v1.Health`,
so `grpcurl` needs no proto files to call it.

`Watch` sends the current status at once and again on every change, as soon
as readiness publishes it. Changes that land before the stream can send them
are not lost: it sends each status readiness passed through, in order, ending
with the current one, so a client sees an outage or a recovery however
quickly it reversed. A run of more than three such changes is shortened to
two or three messages, which still send both statuses and end with the
current one.

The process bounds how many `Watch` streams stay open across all
connections, as [Public gRPC listeners](configuration.md#public-grpc-listeners)
lists; `Check` has no such bound.

On a termination signal the runtime publishes `shutting_down` before its
listener drains, so `Check`, `Watch`, and `/readyz` report it together. When
the drain starts, every open `Watch` stream ends. A stream on the listener's
status first sends `NOT_SERVING` unless that was its last message, and
drops any recovery it had not yet sent, so a balancer stops routing to the
listener while it closes. A `SERVING` the stream was already sending as the
drain began can still arrive just before that final `NOT_SERVING`. A stream that reported `SERVICE_UNKNOWN` ends
without another message.

A Kubernetes `grpc` probe needs no `service` field. This example probes the
lightwalletd port directly.

```yaml
readinessProbe:
  grpc:
    port: 9102
  periodSeconds: 5
livenessProbe:
  httpGet:
    path: /healthz
    port: 9106
```

Keep liveness on `/healthz`. A readiness failure should only remove the pod
from the load balancer, not restart it. The ports are the Mainnet defaults
from [Deploying Zinder](../runbooks/deploying.md#what-you-deploy), and
`grpcurl` can run the same check:

```bash
grpcurl -plaintext -d '{}' 127.0.0.1:9102 grpc.health.v1.Health/Check
```

## Readiness causes

The same causes appear in `/readyz`, in the `zinder.v1.ops.ReadinessReport`
protobuf message, and in the `cause` label of `zinder_readiness_state`. Only
`ready`, `cursor_at_risk`, and `serving_pair_stale` admit traffic.

| Cause | Traffic | Meaning | What you do |
| --- | --- | --- | --- |
| `starting` | Refused | Initialization has not finished. | If ingest stays here with `node.health.addr` set, check the health endpoint and its JSON-RPC fallback. |
| `syncing` | Refused | The runtime is catching up. Ingest reports `lag_blocks` and can also be waiting for a mempool snapshot at the node's tip. | Check [ingest's wait reason](#ingest). A first sync takes hours. See [Initial sync](../runbooks/initial-sync.md). |
| `ready` | Admitted | The runtime serves its contract. | Nothing. |
| `node_unavailable` | Refused | The node or its transport failed. Detail: `failure_class`, `last_reason`, `consecutive_failures`, `outage_seconds`. | Check the node, its address, and its credentials. |
| `node_capability_missing` | Refused | The node lacks a capability the runtime needs. Detail: `capability`. | Configure or upgrade the node to provide it. |
| `upstream_not_ready` | Refused | The node answers but is not synchronized. Detail: node heights, verification progress, and the health source and reason. | Wait for the node to sync. |
| `storage_unavailable` | Refused | A required store cannot be opened or read. | Check the volume, free space, and permissions. If the store is damaged, rebuild it; see [Recovery](../architecture/service-operations.md#recovery). After an in-place canonical store replacement, restart the reader; see [Canonical store replacement](#canonical-store-replacement). |
| `canonical_store_damaged` | Refused | Ingest found a stored row of the canonical store that fails verification. Ingest and the store's wallet-serving readers report it. Detail: `height`, absent when ingest cannot read its verification record, and, when one pool's frontier failed, `pool`. | Rebuild the canonical store; see [Recovery](../architecture/service-operations.md#recovery). Restarting does not help: ingest reports the recorded damage again until it checks it. |
| `schema_mismatch` | Refused | A store's identity or schema differs from what the binary expects. | Run a binary that matches the store, or rebuild the store. See [Recovery](../architecture/service-operations.md#recovery). After an in-place canonical store replacement, restart the reader; see [Canonical store replacement](#canonical-store-replacement). |
| `reorg_window_exceeded` | Refused | Ingest refused a reorg deeper than its reorg window and stopped following. Detail: `depth`, `configured`, and `covered`. | Look for a replaced, resynced, or misbehaving node. Confirm the correct chain, then rebuild the stores; see [Recovery](../architecture/service-operations.md#recovery). Restarting ingest parks it again. |
| `wallet_transition_limit_exceeded` | Refused | The projector parked because its next wallet update needs more memory than `projector.follow.max_transition_logical_bytes` allows. Detail: `required_bytes`, `configured_bytes`. | Restart the projector with a larger limit, as in [Wallet transition exceeds its logical-byte limit](../runbooks/deploying-single-volume.md#wallet-transition-exceeds-its-logical-byte-limit). It resumes without a rebuild. |
| `replica_lagging` | Refused | A reader's copy of a store is more chain epochs behind the writer than allowed. Detail: `lag_chain_epochs`. | Check the writer and projector, then disk and CPU pressure. |
| `serving_pair_stale` | Admitted | A wallet-serving reader cannot refresh and keeps serving the last chain view the writer attested. Detail: `lag_chain_epochs`, `staleness_seconds`. | Find why the refresh fails before the ceiling passes. |
| `writer_status_unavailable` | Refused | A reader cannot reach the writer's private status RPC. | Check that ingest runs and its control endpoint is reachable. |
| `ingest_control_unavailable` | Degraded only | The private ingest-control channel or one of its health calls failed. Appears only under `degraded`. | Check ingest and its control token. |
| `canonical_tip_unconfirmed` | Refused | Ingest reopened its store and has not yet confirmed the store's tip against a node. | Get ingest's node reachable. |
| `cursor_at_risk` | Admitted | Retained chain events are nearing the age at which a consumer's cursor expires. Detail: `oldest_retained_age_hours`, `retention_hours`. | Check slow event consumers. |
| `shutting_down` | Refused | The runtime is stopping. | Nothing. |

The refusing causes of a wallet-serving reader's node or ingest-control
dependency appear under `degraded` and leave the reader ready. Readiness
detail never carries raw node responses, credentials, file paths, or
transaction IDs.

### When several things fail

A runtime with more than one readiness input picks one cause in this order:

1. `shutting_down`.
2. A parked writer's `reorg_window_exceeded`, from any input, because it
   causes the failures that follow it.
3. The runtime's own state (storage, serving pair, or writer) when it refuses
   traffic.
4. A required input that refuses traffic: node source, then ingest control,
   then materialized-view catch-up.
5. Otherwise the runtime's own state, including a warning that admits traffic.

A dependency input never supplies the cause, except for step 2.

A writer that finds damage in its store while it is parked on a reorg keeps
reporting `reorg_window_exceeded`, and so do its readers: the halt's typed
refusal outranks the damage, which ingest still logs and reports to readers.

## Per-runtime readiness

### Ingest

Ingest is ready when its store is within `ingest.follow.lag_threshold_blocks`
of the node's tip and its mempool owner has loaded a snapshot at that tip. A
new block does not withdraw readiness: the mempool owner keeps the previous
snapshot for up to 30 seconds while it loads the next one.

| Cause | When |
| --- | --- |
| `starting` | Until startup finishes. With `node.health.addr` set, also until the first successful health probe. |
| `syncing` | During fresh construction, while trailing the node, when the certified mempool snapshot trails the tip for 30 seconds, or when a replacement has waited 30 seconds for certification. |
| `node_unavailable` | The active node fails. Ingest keeps its state and retries. |
| `upstream_not_ready` | The `[node.health]` probe reports the node not synchronized. |
| `reorg_window_exceeded` | A reorg was deeper than the window. Only shutdown replaces it. |
| `canonical_store_damaged` | The tree-state verification pass found a damaged checkpoint row, logged as `tree_state_verification_mismatch`. Ingest keeps following but stays not ready until it stops. Every `CanonicalControl.WriterStatus` answer reports the damage, so readers refuse traffic on their next refresh. After a restart, writer status reports the recorded damage from its first answer, and ingest reports this cause again once the verification pass confirms it, which it checks as soon as the store is published. A verification record ingest cannot read is damage without a height until a full verification cycle passes. Rebuild the canonical store. See [Chain ingestion](../architecture/chain-ingestion.md#tree-state-verification). |
| `cursor_at_risk` | Explorer view replay falls near the event-retention limit. |

During fresh construction, `/readyz` stays `not_ready` (`503`) with
`phase: "bulk_catchup"` and `cause: { "syncing": { "lag_blocks": ... } }`:

- While resolving the construction range from the node, `current_height`,
  `target_height`, and `cause.syncing.lag_blocks` are absent or `null`.
- Once the fixed build tip is resolved, `target_height` holds that build tip,
  `current_height` starts at the build predecessor height (`0` for a genesis
  build, or the configured checkpoint height), and `lag_blocks` starts at
  `target_height - current_height`.
- As block loading stages blocks into the builder, ingest updates
  `current_height`, `lag_blocks`, and `zinder_readiness_sync_lag_blocks` at a
  bounded cadence until `current_height` reaches `target_height` and
  `lag_blocks` reaches `0`.
- Ingest stays `not_ready` in `bulk_catchup` through subtree verification,
  baseline publication, and store installation, then transitions to
  `following_tip`. See [Fresh construction](metrics.md#fresh-construction) for
  the matching Prometheus gauges.

While ingest follows the node, its `/readyz` can include the optional field
`cause.syncing.wait_reason`. It has two values.

- `mempool_hydration_at_tip` means canonical ingest has reached the node's
  tip and is waiting for a mempool snapshot there.
- `canonical_progress` means the current iteration is checking or advancing
  the canonical chain, including preparing the next block. The last reported
  heights can still match during preparation.

`IngestControl.WriterStatus` reports `upstream_not_ready` when the latest
health probe says the node is syncing, even while ingest's own `/readyz`
shows `syncing`. If a later probe fails to obtain a health response, its
input becomes `node_unavailable` and writer status clears `upstream_not_ready`.
Ingest then keeps retrying until the node synchronizes.
With fallback nodes, a switch resets the probe's input to `starting` until
the first successful observation of the new endpoint. See
[Chain ingestion](../architecture/chain-ingestion.md#node-endpoint-failover).

### Projector

The projector is ready after it has opened or built the wallet store and is
following the canonical store at a position the writer attests.

| Cause | When |
| --- | --- |
| `ready` | Also while the wallet trails the writer, as long as it applies a block at least every 30 seconds and has trailed for less than 2 minutes. |
| `syncing` | Building the wallet, or trailing past either bound above. |
| `replica_lagging` | Its canonical copy could not reach the writer's state within `projector.fence_convergence_timeout_seconds`. It keeps retrying and does not exit. |
| `writer_status_unavailable` | The writer did not answer during that attempt. |
| `wallet_transition_limit_exceeded` | Parked on an oversized update, until a restart with a larger limit or a later reorg that changes the update. |

The heights the projector reports are the wallet's, not the canonical
store's. A parked projector stops wallet updates, so wallet-serving readers
fall behind and stop serving once their staleness ceiling passes. The
projector keeps following a writer that reports damage in its store, as it
follows a parked writer's fence; only the wallet-serving readers refuse
traffic over it.

### Wallet-serving readers

`zinder-query` and `zinder-compat-lightwalletd` in wallet mode serve from a
published pair of read-only store copies: one canonical and one wallet.
Before publishing a pair, the reader checks it against the writer's private
status RPC. The pair alone decides readiness. The reasons are recorded in
[ADR-0016](../adrs/0016-wallet-serving-readiness.md).

| Cause | When |
| --- | --- |
| `ready` | The published pair matches what the writer attests. |
| `serving_pair_stale` | A refresh fails for a transient reason, such as lag, a status-RPC outage, or a store file compacted away. Traffic continues until `storage.serving_pair_staleness_ceiling_ms` (5 minutes by default) passes since the last attestation. The reader refreshes at least every half ceiling, even if `storage.secondary_catchup_interval_ms` is longer, so a writer that answers attests it again first. Traffic stops when the ceiling passes, even while a status read is still waiting for the writer. |
| `writer_status_unavailable`, `replica_lagging` | The ceiling passed. A stale view that trailed the writer reports `replica_lagging`; otherwise `writer_status_unavailable` holds until a refresh's own cause takes over. Traffic stops. |
| `canonical_store_damaged` | Ingest reports damage in the canonical store. Traffic stops at the first writer status that reports it, including one read while a replacement pair converges, with no staleness allowance, and stays stopped through a writer-status outage until ingest reports no damage. New requests fail with `SERVICE_NOT_READY`. |
| `canonical_tip_unconfirmed` | Ingest restarted and has not confirmed its tip. |
| `reorg_window_exceeded` | Ingest is parked. New requests fail with `REORG_WINDOW_EXCEEDED` and open event streams end. |
| `storage_unavailable`, `schema_mismatch` | Fail closed at once, as does a writer fence that disagrees with the pair. |

`storage.secondary_replica_lag_threshold_chain_epochs` (4 by default) decides
when lag becomes a reported warning. The staleness ceiling alone decides when
traffic stops. When the reader refuses traffic, new gRPC calls fail with
`UNAVAILABLE`, `SERVICE_NOT_READY`, and the cause in
`ErrorInfo.metadata["readiness_cause"]`.

### Reader dependency health

The readiness report lists reader dependency failures under `degraded`.

| Input | Used by | When degraded |
| --- | --- | --- |
| `node_source` | Transaction broadcast, `GetStandardFee` in wallet-mode compatibility, and cold transaction bytes in lean wallet serving | No verified node endpoint answers (`node_unavailable`), the node reports it is not synchronized (`upstream_not_ready`), or it lacks a required method (`node_capability_missing`). Lean wallet readiness checks the primary node because it supplies cold bytes. A healthy broadcast fallback can still accept broadcasts during a primary outage. |
| `node_source` in light-wallet serving | [Forwarded wallet calls](lightwalletd-compatibility.md#runtime-serving-selection) | The configured node fails the dependency probe. The report carries its cause under `degraded`. |
| `ingest_control` | Mempool snapshots and events, unmined transaction lookups | Ingest control fails (`ingest_control_unavailable`), the writer's mempool waits on its node (`node_unavailable`), or the writer reports `upstream_not_ready`. |

See [Node source boundary](../architecture/node-source-boundary.md#capabilities)
for endpoint verification.

### Canonical-only compatibility readers

Compact-blocks and light-wallet readiness follow the published canonical
generation and its writer attestation. At admission, both check the writer's
canonical fence. Light-wallet also checks the writer's admitted upgrade
schedule and reports the `node_source` and `ingest_control` dependencies under
`degraded`. Compact-blocks declares neither dependency. See
[During a node outage](#during-a-node-outage) for how degradation affects calls.

### Canonical store replacement

Readers pin the canonical stored set they admit at startup. A replacement
with a different workload, raw-blob retention, first retained block height,
subtree-root coverage mode, or first retained subtree-root index for any
shielded pool (Sapling, Orchard, or Ironwood) cannot take over while the
reader is running. Compact-blocks and
light-wallet compatibility report `schema_mismatch` when a replacement
passes normal admission and then fails the pin. Native query and wallet-mode
compatibility report `storage_unavailable` for that refusal.

After a stored-set refusal, the reader keeps checking writer status on its
normal cadence and waits at least 30 seconds before trying another cold open.
Restart the reader to adopt the new stored set. The replacement still has to
meet the reader's configured minimum, writer attestation, and, for paired
readers, wallet projection admission.

### Explorer

Explorer is ready once its API starts and, with a materialized-view store,
while the latest catch-up of its read-only view copy succeeded. It catches up
once per second.

| Cause | When |
| --- | --- |
| `starting` | Before the first successful catch-up. |
| `schema_mismatch` | The view store's format or consumer schemas no longer match the binary. |
| `storage_unavailable` | Any other catch-up failure. |

On a catch-up failure, `/readyz` returns 503, gRPC health reports
`NOT_SERVING`, and new `ExplorerQuery` calls fail with `UNAVAILABLE` and
the readiness cause. The next successful catch-up reopens the gate.
`ExplorerFreshness` reports how far the views trail the chain in each response.

### Cipherscan adapter

`zinder-compat-cipherscan` stores nothing. It fails startup if it cannot
connect to Explorer and wallet query, and then stays `ready` until shutdown.
A request that reaches an unavailable upstream returns HTTP 503 with
`upstream_unavailable`. Check the upstream runtimes' own `/readyz`.

## During a node outage

This table shows what each runtime does while every node it uses is
unreachable.

| Runtime | Readiness | Keeps serving | Stops |
| --- | --- | --- | --- |
| `zinder-ingest` | `node_unavailable` | Its private control API, including writer status. | Committing blocks. The chain tip stops advancing. |
| `zinder-projector` | `ready` | Its last wallet state. | Nothing new to apply. |
| `zinder-query`, wallet-mode compatibility | `ready` while writer attestation holds, with configured `node_source` under `degraded` | Stored blocks, retained transactions, transparent address queries, tree states, and subtree roots. Native query also serves chain events and server info. | Broadcasts and wallet-mode `GetStandardFee` fail with `UNAVAILABLE` and `NODE_UNAVAILABLE`. |
| Light-wallet compatibility | `ready` while writer attestation holds, with `node_source` under `degraded` after the node probe fails | Local block ranges, tree states, subtree roots, retained transaction bodies, and cached verified transaction bodies. | Calls that need the forwarding node fail with `UNAVAILABLE`, including broadcast. A history stream can fail after delivering rows. See the [method contracts](lightwalletd-compatibility.md#method-behavior). |
| Compact-blocks compatibility | `ready` while writer attestation holds | Canonical block and range reads. | The tip calls follow the stale-tip rule below. |
| `zinder-explorer` | `ready` | Stored views. | Observing the node's tip for `ExplorerFreshness`. |

When ingest loses its live mempool because its node is unavailable, wallet
and light-wallet compatibility refuse new mempool calls through ingest
control. Losing only the forwarding node does not withdraw ingest's mempool.

In all three compatibility modes, `GetLatestBlock` and `GetLightdInfo`
can fail with `UNAVAILABLE` while `/readyz` returns 200. The
[stale-tip rule](lightwalletd-compatibility.md#stale-tip) applies when the tip
is too old and the writer's node cannot vouch for it. Watch
`served_epoch_age_seconds` while the chain stands still.

Readers stop serving in these cases:

- **Ingest stops.** Readers lose the writer's status RPC and keep serving for
  `storage.serving_pair_staleness_ceiling_ms`, 5 minutes by default. Then
  they report `writer_status_unavailable` and refuse traffic (and under
  `zinder serve`, an unexpected child exit triggers fail-fast shutdown so the
  orchestrator restarts cleanly).
- **Ingest restarts while the node is down.** Ingest needs a node to start and
  to confirm its tip. Readers report `canonical_tip_unconfirmed` until it
  does.

A reader that restarts during a node outage serves local data once its pair
or canonical generation converges and the writer attests it. Readers can open
their local stores without contacting their configured node.
