# ADR-0028: Per-Consumer Materialized-View Schema Versioning

## Status

Accepted.

## Context

Materialized-view consumers own disjoint rows, cursors, and recovery sources.
A payload or key change in one consumer must not invalidate every other view.
Rebuild safety also cannot be inferred from retained chain events alone: a row
may depend on canonical inputs whose retention differs from the event log.

The versioning unit therefore needs to match the ownership and recovery unit.

## Decision

Each consumer registers one `MaterializedViewConsumerSchema` containing:

- a stable `MaterializedViewConsumerName`;
- a monotonic `schema_version`;
- the exact column families it owns.

`MaterializedViewStoreOptions::consumers` is a closed declaration set for the
opened store. Consumer names and column-family ownership must be unique. The
`consumer_metadata` family persists one manifest row per consumer with its
writer version and owned families.

### Exact schema admission

The format version and every declared manifest row are written atomically only
when creating a fresh store. Every subsequent primary open, secondary open,
and secondary catch-up require:

- the exact `MATERIALIZED_VIEW_STORE_FORMAT_VERSION`;
- an exact physical column-family set, including the shared families;
- every manifest consumer to be declared; and
- identical consumer names, column-family ownership, and schema versions.

Any lower, higher, missing, renamed, or otherwise mismatched consumer identity
fails closed before the primary opens RocksDB for mutation or any consumer row
is decoded. An operator must select a fresh materialized-view path and rebuild
it from a certified recovery source. Store open never clears consumer rows,
resets cursors, rewrites an existing manifest, or creates a missing consumer
column family.

### Container format

`MATERIALIZED_VIEW_STORE_FORMAT_VERSION` versions only the shared container:
manifest layout, cursor encoding, and shared metadata families. The running
reader defines the only admitted format.

Every opener rejects a container format different from the running format with
`SchemaMismatch`, without mutation. The operator must select a fresh path and
rebuild it from a certified recovery source; a store open never deletes an
existing materialized-view directory. Consumer key or payload changes increment
that consumer's schema version and require a fresh store, except for the
additive payload fields below.

### Additive payload fields

A consumer may add a field to its stored payload without changing its schema
version when the running reader still decodes every row an earlier writer of
that version produced. New rows set the new field, existing rows keep their
bytes, and the store opens in place. The reader accepts both forms for as long
as the version lasts, and a future version change drops the older form along
with the rows a fresh store no longer holds.

The transparent-address delta and activity rows use this to store their
transaction id as 32 bytes in internal byte order in `internal_transaction_id`.
Rows written before that field existed carry the 64-character RPC hex form in
`transaction_id`, and the consumers' `stored_transaction_id` reads either.

Such a field is forward-only. An earlier release decodes a new row without the
field, so it would serve the row without its value, here an empty transaction
id. Rolling back past an additive field therefore requires rebuilding the
affected views on a fresh path.

## Consequences

- Consumer schema changes are explicit store-replacement operations rather
  than in-place migrations.
- Removing, renaming, or adding a consumer requires a fresh store; absence is
  never interpreted as permission to delete or create state.
- Recovery must use a certified source that covers the required history before
  a replacement store becomes active.
- Rollback cannot reinterpret, erase, or create any incompatible consumer or
  container format.
- Rollback past an additive payload field leaves rows the earlier release
  reads incompletely; the affected views are rebuilt after such a rollback.

## Revision history

- 2026-11-15: Added additive payload fields, which a consumer adds without a
  schema version change while its reader still decodes the earlier rows.

## References

- [Materialized-view plane](../architecture/materialized-view-plane.md)
- [ADR-0003: Canonical storage access boundary](0003-canonical-storage-access-boundary.md)
- [ADR-0017: Materialized-view consumer and key codec](0017-materialized-view-consumer-and-key-codec.md)
- [ADR-0029: Durable transparent outpoint-spend projection](0029-durable-transparent-outpoint-spend-projection.md)
