# ADR-0004: Separate Node Sources from Protocol Surfaces

## Status

Accepted.

## Context

Upstream-node integration and Zinder's service protocols are adjacent but
independent boundaries. If they share types or ownership, a transport DTO can
leak into canonical storage, query handlers can acquire hidden node fallbacks,
and compatibility schemas can shape native domain APIs.

## Decision

Zinder keeps two explicit owners:

1. `zinder-source` owns upstream-node communication behind `NodeSource`,
   `MempoolSource`, `ChainTipNotificationSource`, and
   `TransactionBroadcaster`.
2. `zinder-proto` owns service protobufs, vendored compatibility protobufs, and
   generated Rust protocol modules.

Source adapters return normalized Zinder values and `SourceError`, never
transport DTOs. Capability discovery is behavior-based through
`NodeCapabilities`; an upstream version string is diagnostic information, not
the compatibility contract. Consensus-critical bytes are parsed with
Zebra-compatible primitives inside the source boundary.

`zinder-proto` owns these wire families:

- `zinder_proto::v1::wallet` for the native `WalletQuery` contract;
- `zinder_proto::v1::explorer` for the optional `ExplorerQuery` runtime;
- `zinder_proto::v1::ingest` for canonical-writer, projector, and ingest
  control services;
- `zinder_proto::v1::ops` for shared operational messages; and
- `zinder_proto::compat::lightwalletd` for the vendored
  `CompactTxStreamer` compatibility contract.

Domain and storage crates do not expose generated protobuf types in their
public APIs. Storage-private control encodings may live in `zinder-store` when
they are not service contracts.

`zinder-compat-lightwalletd` translates `WalletQueryApi` semantics into
the vendored lightwalletd contract. It does not build missing canonical
artifacts or mutate canonical storage. Its explicitly configured edge call
may broadcast a transaction through `zinder-source`; it cannot substitute for
indexed history. Activations come from the writer-admitted schedule. Tree state
is served from stored checkpoints and compact blocks, never from the node.

## Consequences

- Upstream transports can change without changing wallet or explorer APIs.
- Native protocol vocabulary is not copied from compatibility protocols unless
  the concepts are identical.
- Query and adapter services fail explicitly when required canonical data is
  unavailable; they do not query the node as a fallback.
- Each new protocol message and source capability has one clear owner.

## References

- [Node source boundary](../architecture/node-source-boundary.md)
- [Protocol boundary](../architecture/protocol-boundary.md)
- [Service boundaries](../architecture/service-boundaries.md)
- [Public interfaces](../architecture/public-interfaces.md)

## Revision history

- 2026-11-18: The compatibility adapter no longer discovers network-upgrade
  activations from the node; it reads the writer-admitted schedule (see
  [ADR-0039](0039-writer-admitted-network-upgrade-schedules.md)). Broadcast is
  its only node call.
- 2026-11-17: The compatibility adapter no longer fills sparse tree state
  from the node; tree state is served from stored checkpoints and compact
  blocks (see ADR-0005).
