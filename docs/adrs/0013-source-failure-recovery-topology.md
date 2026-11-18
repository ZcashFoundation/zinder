# ADR-0013: Source failure recovery topology

## Status

Accepted.

## Context

`zinder-ingest` observes upstream state while constructing and following the
canonical chain. Source failures must drain readiness and retry without
discarding the ingest runtime. Storage, reorg-boundary, and internal integrity
failures remain process exits because retrying them could hide an unsafe state.

Source adapters describe failures; they do not decide whether the ingest loop
continues. A boolean retry classification cannot express that boundary or
provide operators with the actionable reason and recovery cadence.

## Decision

1. **Ingest owns recovery lifecycle.** `services/zinder-ingest/src/source_recovery.rs::decide_recovery(&IngestError, SourceRecoveryBackoff) -> SourceRecoveryDecision` returns `Recover { failure_class, last_reason, backoff }` or `Exit`; the canonical follower applies that decision.
2. **Source errors are loop-recoverable.** Every `SourceError` variant returns `Recover`. Storage failures, reorg-window violations, and internal integrity errors return `Exit`.
3. **Unknown upstream responses recover by source identity.** Adapters preserve an actionable `SourceError` and classify it rather than treating an unrecognized upstream response as a fatal loop decision.
4. **Source errors describe observations.** `SourceFailureClass` (`NodeUnreachable`, `UpstreamViewChanged`, `StreamDisconnected`, `CapabilityMissing`, `ProtocolMismatch`, `Malformed`, `Configuration`) is the operator-facing label returned by `SourceError::upstream_classification()`.
5. **Readiness carries operator detail.** `ReadinessCause::NodeUnavailable` carries `NodeUnavailableDetail` with `{ failure_class, last_reason, consecutive_failures, outage_seconds }`. `/readyz`, the proto report, and `zinder_readiness_node_failure_class{class=...}` surface that detail.
6. **Per-call retry handles narrow transport classes.** `retry_source_request` retries only `NodeUnreachable` and `StreamDisconnected` failures during fresh construction. The canonical follower makes one attempt per call; other source failures, and every follow-mode failure, return to the loop, which re-observes upstream state before issuing dependent requests.
7. **Only the writer drains on a source failure.** Wallet-serving readers report their node probe's `node_unavailable` or `upstream_not_ready`, and the writer's `upstream_not_ready` relayed through ingest control, as degraded dependencies with the same detail. They keep serving stored reads, and a node-backed request fails on its own with `NODE_UNAVAILABLE` ([ADR-0045](0045-wallet-serving-readiness-follows-the-serving-pair.md)).
8. **Ingest owns the node endpoint switch.** When `[node_fallbacks.N]` endpoints are configured, the canonical follower reads through the active endpoint of a `NodeSet`, `[node]` being position 0. It switches only between follow iterations, so every read that prepares one block reaches one endpoint:
   - after `NodeFailoverPolicy::transport_failures_before_switch` (3) consecutive recoveries of one class, `node_unreachable` or `stream_disconnected`, counted per class;
   - or at the top of an iteration, after the active endpoint has reported not ready for `upstream_not_ready_before_switch` (300 seconds), which outlasts the brief synchronization flaps nodes report.

   `upstream_view_changed`, `protocol_mismatch`, `malformed`, `capability_missing`, and `configuration` never switch: switching on them would pick whichever node's answer parses and could hide a Zinder defect.

   A standby monitor evaluates the other endpoints in the background. It bounds each endpoint's evaluation by the evaluation cadence, or by five seconds when the cadence is shorter, and records each evaluation as soon as it completes, so an active endpoint that hangs never makes the others' evaluations stale. A candidate qualifies when it advertises the required ingest capabilities and the startup endpoint's value-pool capabilities, reports ready, holds the configured network's genesis block, advertises the writer-admitted activation table ([ADR-0039](0039-writer-admitted-network-upgrade-schedules.md)), and has a tip at or above the visible tip. Candidates holding the visible tip are tried first, in position order. A candidate that shares only the settled tip is tried only when none holds the visible tip, and the switch then replaces the diverged suffix within the reorg window. A candidate that differs at or below the settled tip never qualifies, so a switch cannot park the writer. At switch time the follower rechecks only each candidate's readiness, its tip, and the block it holds at the visible tip, or at the settled tip for a diverged candidate. It rechecks every candidate at once, so the whole attempt takes at most one five-second bound however many candidates hang, and switches to the first candidate in that order that passes; a candidate that no longer reports ready is skipped. Shutdown interrupts the attempt. The bound keeps the writer's control plane responsive during a failover. When a trigger finds no candidate, or every candidate fails its recheck, the follower attempts the switch again only after the monitor records another evaluation.

   A switch to a candidate holding the visible tip keeps the tip confirmed: the final recheck found the visible tip block on the new endpoint, which is the evidence an append gives, so wallet-serving readers keep serving while the new endpoint's first iteration is pending or failing ([ADR-0045](0045-wallet-serving-readiness-follows-the-serving-pair.md)). A switch to a diverged candidate resets tip confirmation, so readers see `tip_unconfirmed` until the replacement of the diverged suffix commits. Either switch skips the recovery backoff, resets the `node_source` readiness input to `starting` when `[node.health]` gates readiness, stops admitting the mempool certification made from the previous endpoint, and ends the live mempool stream so the owner rebuilds from the new endpoint. Ingest readiness therefore waits for a generation certified after the switch, even when the owner is still busy with the previous endpoint. There is no timed failback: the next switch walks positions from 0 again, and a restart starts at position 0. Fresh construction does not switch; a node failure there exits as before, and the restart admits the first reachable endpoint.

   At startup, a single-endpoint deployment is checked as before. With fallbacks, every endpoint is checked concurrently, including its genesis block, and the first admitted endpoint in position order becomes active as soon as every lower position has a verdict. Startup does not wait for higher positions; an endpoint still being checked then is deferred to the standby monitor, so an unresponsive fallback never delays a healthy `[node]`. An endpoint is excluded until restart only on positive evidence: pruned storage, a capability its capability discovery omits, a genesis block of another network, or an activation table other than the active endpoint's. A fallback that would become active with a table other than the store's admitted revision is excluded the same way, so only `[node]` can bring a new table ([ADR-0039](0039-writer-admitted-network-upgrade-schedules.md)); that comparison waits until every lower position has a verdict, so a fallback that already advertises the table `[node]` brings stays a standby. A timeout, a failed call, or a storage mode the node did not report leaves the endpoint unreachable, and the monitor evaluates it again.

## Consequences

- Canonical following stays alive through source outages while ingest `/readyz` reports `node_unavailable` with the source-failure class.
- Wallet-serving readers stay ready through the same outages, so an operator watches ingest readiness and the readers' `zinder_readiness_dependency_state` gauge rather than reader 503s.
- With fallback endpoints, ingest keeps committing through an outage of its active node. Expect a switch about 4 seconds after connections start being refused, or up to three request timeouts when requests hang. Each switch costs one mempool rehydration, during which ingest readiness is withdrawn.
- Operators can see the failure class, latest reason, retry count, and outage duration without consulting logs.
- New `SourceError` variants select a `SourceFailureClass`; recovery posture remains owned by ingest.

## Related

- `docs/architecture/node-source-boundary.md` — Capability Model section: `SourceFailureClass` taxonomy and the operator-action table (`failure_class` → action).
- `docs/architecture/service-operations.md` — Health and Readiness section: the `NodeUnavailableDetail` payload contract.
- `docs/architecture/chain-ingestion.md` — Continuous following: loop-owned recovery posture and endpoint switching.

## Revision history

- 2026-11-18: Ingest switches between the endpoints of the ordered node list (Decision 8), and Decision 6 now states that the follower makes one attempt per call.
- 2026-11-17: Wallet-serving readers no longer drain readiness on a source
  failure. They report it as a degraded dependency and fail only node-backed
  requests ([ADR-0045](0045-wallet-serving-readiness-follows-the-serving-pair.md)).
