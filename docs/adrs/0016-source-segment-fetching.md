# ADR-0016: Source Segment Fetching

## Status

Accepted.

## Context

Historical construction needs more than a one-block source call. Per-block
round trips leave the writer idle, while an unbounded range call can exceed
source, network, or process memory limits. The source boundary also has to
detect a chain change between the caller's cursor and the returned blocks.

The canonical writer must not depend on one Zebra transport shape. It needs a
bounded sequence of connected source observations with the same semantics
whether an adapter obtains them through JSON-RPC batches or an indexer gRPC
endpoint.

## Decision

`NodeSource::fetch_chain_segment` is the bounded historical-fetch boundary. The
request carries a cursor, a maximum connected-block count, and a maximum
response size. The response contains ordered `SourceChainUpdate` values and
source payload statistics. Every adapter must:

- observe the upstream tip before selecting its range;
- return a revert update when the cursor block is no longer the selected-chain
  block at that height;
- validate that returned blocks form one parent-linked sequence;
- enforce the requested count and response-size limits; and
- return typed source errors for unavailable blocks, incoherent observations,
  protocol mismatches, and oversized responses.

`ZebraJsonRpcSource` implements the boundary with one JSON-RPC batch of
height-keyed `getblock` requests, one entry per requested block. The adapter
does not cap the batch: Zakura refuses batches above 1,000 entries, so ingest
rejects `source_segment_max_blocks` above 1000 at config load, and other
callers must keep `SourceChainSegmentLimits` within that bound. It decodes each raw block locally, validates
linkage, records observed payload bytes, and splits requests when the source or
transport rejects the response size. It does not make a preceding
`getblockhash` call: the decoded block hash and parent hash provide the required
identity and continuity checks.

`ZebraIndexerBlockSource` implements the same boundary with bounded concurrent
indexer `GetBlock` calls. It still uses the JSON-RPC control plane for tip,
tree-state, subtree-root, health, and broadcast operations that the indexer
protocol does not provide. This adapter is available as a library boundary;
the `zinder-ingest` configuration currently selects only `zebra-json-rpc` for
canonical construction.

The writer adapts segment size below
`ingest.construction.source_segment_max_blocks` using observed response bytes.
It separately bounds in-flight request count, in-flight bytes, block preparation
concurrency, and commit reassembly memory. Consensus branch changes reset the
response-density estimate.

`NodeSource::fetch_block_at` remains the one-block primitive and the default
implementation used by sources that do not override `fetch_chain_segment`.
Following-tip ingestion uses the coherent one-update path rather than assuming
that a historical segment is a live event stream.

## Consequences

- Canonical construction has one transport-neutral, resource-bounded source
  contract.
- JSON-RPC historical fetches avoid a redundant hash lookup and amortize
  request overhead across a bounded segment.
- Reorg detection is expressed as source updates and typed errors, not inferred
  from partial batches.
- Adding a configured source kind requires explicit runtime wiring and
  validation; the existence of an adapter type alone does not make it a
  supported deployment option.

## References

- [Node source boundary](../architecture/node-source-boundary.md)
- [Chain ingestion](../architecture/chain-ingestion.md)
- [ADR-0015: Phase-driven ingest](0015-phase-driven-ingest.md)
- [ADR-0022: Resource-budgeted bulk catch-up](0022-resource-budgeted-bulk-catchup.md)
