# ADR-0026: Serve single-server PIR from an external sidecar

Status: Accepted

## Context

Private information retrieval (PIR) lets a wallet query a prepared dataset
without revealing the requested record to the serving process. The choice
of scheme affects record packing, resource requirements, and the wallet
protocol. [#708](https://github.com/ZcashFoundation/zinder/issues/708).

## Decision

1. **Keep PIR outside Zinder runtimes.** A future standalone, read-only PIR
   service and exporter own shard generation and serving. `bin/zinder`,
   `zinder-query`, `zinder-compat-lightwalletd`, and `zinder-ingest` carry
   zero PIR serving state, lattice or RLWE cryptography, or GPU runtimes.
   The exporter stays outside `zinder-materialized-views`, whose closed
   registry contains ingest-owned Explorer views
   ([ADR-0020](0020-materialized-views.md)).
2. **Consume published sealed segments.** The exporter reads published
   immutable Schema v2 segments through the pool in
   [ADR-0024](0024-verified-edge-snapshot-bootstrap.md). Dataset coverage
   ends at or below `sealed_tip` and excludes the unsettled live tail
   and mempool. Encrypted memo datasets require `full_blocks` and published
   `ZSBLKB01` objects.
3. **Defer the in-repo exporter and discover it out of band.** A concrete
   open-source Zcash PIR scheme and a committed wallet consumer must
   agree on shard record formats before exporter implementation.
   Discovery stays out of band until Zinder serves or verifies PIR
   metadata directly. Carry PIR inside
   [OHTTP](0025-oblivious-http-gateway-boundary.md) or through
   [Tor](https://github.com/ZcashFoundation/zinder/issues/706) when both
   lookup and client IP unlinkability are required.

## Evidence

- Decision 1: Judgment. PIR compute and serving state belong to the
  standalone service. Reopen placement if a qualified scheme cannot
  consume immutable exports within its serving budget.
- Decision 2: Judgment. Sealed coverage avoids reorg-driven shard
  replacement, and compact blocks omit the full ciphertext needed for
  encrypted memos. Reopen coverage if a committed consumer demonstrates
  a requirement for private lookups above `sealed_tip` and can safely
  handle shard replacement.
- Decision 3: Judgment. A concrete scheme and wallet integration can
  establish record sizes and encoding. A qualified pairing that needs
  discovery inside the Zinder trust boundary would reopen discovery.

## Consequences

- The PIR operator owns dataset builds, resource provisioning, and
  publication. A wallet must account for the sealed dataset's height.
- The scheme, shard format, and implementation remain open.

## Alternatives rejected

- **Embed PIR in a Zinder serving role.** It would make every deployment
  carry scheme-specific compute and serving state.
- **Add PIR shards as an Explorer materialized view.** This would couple
  sidecar datasets to the canonical writer's replay, recovery, and schema
  admission.
- **Export live-tail or mempool records.** Their changing membership would
  require a different freshness and replacement contract.
