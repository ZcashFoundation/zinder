# ADR-0003: Epoch-bound canonical reads and RocksDB secondaries

| Field | Value |
| --- | --- |
| Status | Accepted |
| Related | [Storage backend](../architecture/storage-backend.md), [Chain events](../architecture/chain-events.md), [ADR-0035](0035-canonical-storage-topologies.md) |

## Context

Wallet synchronization and explorer requests often read several artifacts for
the same chain range. A reorg or tip advance between those reads must not make a
single response combine different best-chain states. Separate service processes
also need fresh canonical data without sharing an in-process RocksDB handle or
opening a second writer.

## Decision

Every chain-dependent read is bound to one `ChainEpoch` or one
`CanonicalEventFence` before it reads artifacts. The bound reader may finish
against that immutable visible state even when a newer epoch is published.

Canonical storage has one primary owner. Other processes open process-owned
RocksDB secondaries with unique metadata paths. A secondary catches up before
admission, validates store identity and schema after catch-up, and never mutates
the primary. Service-to-service notifications may trigger catch-up, but the
notification is not the data plane and does not replace persisted evidence.

The canonical store exposes role-specific handles:

- `RocksDbCanonicalStore` owns construction, publication, following, reorgs,
  retained events, and leases.
- `RocksDbCanonicalSecondary` exposes immutable read operations and explicit
  catch-up.
- `CanonicalReader` is the query-layer contract consumed inside an admitted
  `WalletServingReadPair`.

Readers fail closed when the requested epoch or fence is unavailable, store
identity differs, schema is unsupported, or a secondary cannot converge within
its configured boundary. They do not fall back to unpinned reads or fetch
missing canonical artifacts directly from the node.

## Consequences

- Multi-artifact responses have one explicit chain identity.
- One process owns every RocksDB write path.
- Reader freshness is observable and can participate in readiness.
- Each reader needs a unique secondary metadata directory. A writer process may
  host reader-plane secondaries of the store it writes, as `zinder-ingest` does
  for materialized-view hydration, because the linear ownership rule binds the
  primary handle rather than the process.
- Long-lived readers require explicit retention contracts for epochs and events
  they may still reference.
