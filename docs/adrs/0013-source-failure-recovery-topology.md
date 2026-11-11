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
6. **Per-call retry handles narrow transport classes.** `retry_source_request` retries only `NodeUnreachable` and `StreamDisconnected` failures. Other source failures return to the loop, which re-observes upstream state before issuing dependent requests.

## Consequences

- Canonical following stays alive through source outages while `/readyz` reports `node_unavailable` with the source-failure class.
- Operators can see the failure class, latest reason, retry count, and outage duration without consulting logs.
- New `SourceError` variants select a `SourceFailureClass`; recovery posture remains owned by ingest.

## Related

- `docs/architecture/node-source-boundary.md` — Capability Model section: `SourceFailureClass` taxonomy and the operator-action table (`failure_class` → action).
- `docs/architecture/service-operations.md` — Health and Readiness section: the `NodeUnavailableDetail` payload contract.
- `docs/architecture/chain-ingestion.md` — Continuous following: loop-owned recovery posture.
