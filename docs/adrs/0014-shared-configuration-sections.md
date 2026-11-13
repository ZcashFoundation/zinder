# ADR-0014: Shared configuration sections

| Field | Value |
| --- | --- |
| Status | Accepted |
| Related | [Public interfaces](../architecture/public-interfaces.md), [Service operations](../architecture/service-operations.md), [ADR-0035](0035-canonical-storage-topologies.md) |

## Context

Zinder binaries share network, node, storage, security, and operational
concepts. Private copies of those schemas drift in field names, defaults,
environment mappings, redaction, and validation. At the same time, forcing
every binary to accept every field makes ownership unclear and allows a reader
to appear capable of configuring a writer.

## Decision

`zinder-runtime` owns reusable configuration section types and the layered
loader. Each binary composes only the sections it needs and owns its
service-specific validation.

Configuration precedence is:

1. compiled defaults;
2. the optional TOML file;
3. `ZINDER_*` environment variables; and
4. explicit CLI overrides.

Shared names keep the same meaning across services:

- `[network]` selects the Zinder-native network identity;
- `[node]` owns upstream endpoints, authentication, transport limits, indexer
  TLS (`[node.indexer_tls]`), and health probing;
- `[storage]` owns canonical paths, secondary paths, catch-up behavior, and
  role-scoped RocksDB resource budgets;
- `[security]` owns public-bind refusal and transport policy; and
- service sections such as `[ingest]`, `[projector]`, and `[compat]` own only
  behavior for that runtime.

`zinder-ingest` further groups runtime behavior by domain:

- `[ingest.phase_classification]` selects construction versus following;
- `[ingest.construction]` bounds source and canonical construction work;
- `[ingest.follow]` controls steady-state polling and lag readiness;
- `[ingest.run_overrides]` contains one-run targeting and checkpoint inputs.

`--print-config` renders the resolved service configuration using the same
serialization contract as the loader. Secret values and raw authorization
material are replaced by explicit redaction markers. Unknown fields are
rejected and invalid cross-field combinations fail before storage opens. A
service does not accept a section for a role it does not own, but a shared
section type accepts every field of its role pair: for example, `[node]`
accepts the ingest-only `indexer_grpc_addr` and `[node.indexer_tls]` in every
binary that reads `[node]`, and `[ingest_control]` accepts both the writer
fields and the reader fields. A binary ignores the fields it does not consume. The environment-variable
reference names the binaries that read a variable into their configuration, and
a description states when a listed binary does not apply it, such as RocksDB
background job caps on secondary opens.

## Consequences

- Shared fields have one spelling, default, environment mapping, and redaction
  policy.
- Service configuration remains narrow enough to communicate ownership.
- New shared fields belong in `zinder-runtime`; service-only fields remain in
  that service's config module.
- Configuration changes must update `--print-config`, environment-variable
  documentation, examples, tests, and the public vocabulary in the same change.
- TLS key material enters configuration only as a file path. `--print-config`
  shows the path; the file is read by the transport that uses it. See
  [ADR-0019](0019-transport-policy-ownership.md#credential-delivery).
