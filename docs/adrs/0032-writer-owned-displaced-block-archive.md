# ADR-0032: Writer-owned displaced-block archive

| Field | Value |
| --- | --- |
| Status | Accepted |
| Product | Zinder |
| Domain | Reorg storage and explorer reads |
| Related | [Chain events](../architecture/chain-events.md), [Storage backend](../architecture/storage-backend.md) |

## Context

After a replacement commits, the canonical store cannot reconstruct the displaced branch from its current best-chain indexes. Capturing later in an explorer process would race the atomic replacement and could mix epochs.

## Decision

The canonical writer captures displaced header facts, ordered transaction ids, transparent coinbase payout scripts and values, optional already-retained raw bytes, and available final roots in the same write batch that commits the `CanonicalLiveReplacement`. Block hash is identity; event sequence and former height define observation order. A durable activation record defines the archive's coverage boundary.

Archive retention is permanent in this release. There is no pruning or retention option. Public methods expose the activation boundary and never claim to reconstruct earlier displaced branches. Product labels, miner branding, external reports, and compatibility terminology remain outside the native artifact.

## Consequences

- Reorg acceptance and archive capture are atomic.
- Storage and checkpoint size grow with accepted replacements and require capacity monitoring.
- Any future bounded retention policy needs a separate ADR covering cursor invalidation, coverage contraction, and secondary-reader safety.
