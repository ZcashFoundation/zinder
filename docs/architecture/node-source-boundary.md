# Node Source Boundary

`zinder-source` is Zinder's only boundary to upstream nodes. It owns transport,
authentication, upstream response DTOs, capability discovery, and consensus
parsing. Ingest receives normalized source-domain values; query and adapter
services do not reach around this boundary.

Protocol schemas live in [Protocol boundary](protocol-boundary.md). Canonical
publication semantics live in [Chain ingestion](chain-ingestion.md).

## Ownership rule

Allowed inside `zinder-source`:

- Zebra JSON-RPC and indexer gRPC clients.
- Upstream authentication and transport errors.
- Zebra and Zcash consensus primitives used to parse upstream bytes.
- Capability discovery and upstream readiness probes.
- Transaction broadcast I/O.

Forbidden outside `zinder-source`:

- Upstream response DTOs or client-library errors in public signatures.
- Direct upstream calls from query handlers or storage code.
- Hand-written parsing of consensus-critical block or transaction bytes.
- Hidden fallback from a missing canonical artifact to an upstream query.

## Source contracts

`NodeSource` is the canonical observation boundary. Its operations cover:

- bounded connected-chain segments for historical construction;
- one-block random access for following and reorg traversal;
- upstream tip identity;
- tree-state checkpoints and shielded subtree roots;
- chain-wide and block-bound value-pool observations; and
- upstream readiness.

Every operation returns Zinder types and `SourceError`. Historical construction
uses `fetch_chain_segment` with explicit block-count and response-byte limits.
The response carries ordered `SourceChainUpdate` values plus payload statistics
used by the writer's resource controller. See
[ADR-0016](../adrs/0016-source-segment-fetching.md).

`ZebraJsonRpcSource` fetches a segment as one JSON-RPC batch with one
`getblock` entry per block. Two node limits shape that batch:

- Zakura 1.8.6 refuses a batch with more than 1,000 entries. Zebra 7.0.0 has
  no entry limit. Ingest therefore rejects a configured segment size above
  1000 blocks at startup.
- Zebra 7.0.0 and Zakura 1.8.6 each refuse a response larger than 50 MiB by
  default. Either node answers an oversized batch with a single error object
  instead of a response array. The source treats that answer like an
  oversized response and splits the segment.

`node.max_response_bytes`, 64 MiB by default, is a ceiling on what Zinder
accepts, not a request size. The default segment target of 32 MiB stays below
both node limits, so a split is the exception rather than the normal path.

`tip_id` returns height and hash from one source observation. Height alone is
not a chain identity and must not be used to accept a value across a reorg.

`TransactionBroadcaster` is separate because broadcasting is a command, not a
chain observation. The unit implementation returns
`TransactionBroadcastDisabled`, so a read-only composition fails explicitly.

## Implementations and runtime selection

The current implementations are:

| Type | Role | Runtime status |
| --- | --- | --- |
| `ZebraJsonRpcSource` | Canonical blocks, source segments, tip, tree state, subtree roots, health, value pools, broadcast | The only `ingest.source` option; configured as `zebra-json-rpc` |
| `ZebraIndexerBlockSource` | Block and segment reads over Zebra indexer gRPC with JSON-RPC control-plane hydration | Library implementation; not selectable through `zinder-ingest` configuration |
| `ZebraIndexerMempoolSource` | Streaming mempool events with JSON-RPC transaction hydration | Selected when `node.indexer_grpc_addr` is configured |
| `JsonRpcMempoolSource` | Polling mempool fallback | Selected when no indexer gRPC endpoint is configured |
| `ZebraIndexerChainTipSource` | Indexer chain-tip notifications | Library implementation |

`ingest.source` belongs to the writer because it selects an adapter
implementation. `[node]` describes the endpoint itself. The relevant
environment variables therefore include:

```text
ZINDER_INGEST__SOURCE=zebra-json-rpc
ZINDER_NODE__JSON_RPC_ADDR=http://127.0.0.1:8232
ZINDER_NODE__INDEXER_GRPC_ADDR=http://127.0.0.1:8234
ZINDER_NODE__AUTH__METHOD=cookie
ZINDER_NODE__AUTH__PATH=/var/lib/zebra/.cookie
```

The indexer endpoint is optional and does not change the canonical source
selection. It changes only the components that are explicitly wired to indexer
gRPC.

## Capability model

`NodeCapabilities` is the source diagnostic contract. The current capability
names are:

- `best_chain_blocks`
- `source_chain_segments`
- `tip_id`
- `tree_state`
- `subtree_roots`
- `settled_tip_height`
- `readiness_probe`
- `transaction_broadcast`
- `json_rpc`
- `openrpc_discovery`
- `chain_value_pools`
- `block_value_pool_balances`

The Zebra JSON-RPC source probes `rpc.discover` and validates the methods needed
by canonical ingest before mutation begins. Optional methods add capabilities
without changing the required canonical contract. Missing or contradictory
capabilities produce typed failures rather than version-based guesses.

Source errors describe the upstream observation. `SourceFailureClass` groups
them for writer recovery and readiness:

| Class | Meaning |
| --- | --- |
| `node_unreachable` | The endpoint cannot be reached |
| `upstream_view_changed` | A best-chain observation changed during the operation |
| `stream_disconnected` | An indexer subscription disconnected |
| `capability_missing` | A required upstream operation is unavailable |
| `protocol_mismatch` | A response violates the expected wire contract |
| `malformed` | Upstream bytes cannot be parsed |
| `configuration` | The adapter configuration is invalid |

Recovery policy belongs to `zinder-ingest`; adapters do not decide whether a
process retries or exits.

## Authentication and health

`NodeAuth` represents valid authentication states: none, cookie file, or basic
credentials. Invalid combinations are rejected before connection. Secrets are
redacted from resolved configuration and logs.

When `[node.health].addr` is configured, `ZebraJsonRpcSource` polls Zebra's
readiness endpoint using the configured cadence and thresholds. Without that
explicit endpoint, source status comes from the JSON-RPC observations available
to the writer. Upstream readiness caps Zinder readiness even when local storage
is healthy.

## Consensus parsing

Raw block and transaction bytes are parsed with Zebra-compatible primitives.
`zinder-source` produces `SourceBlock` and related source types;
`zinder-ingest` turns those values into canonical artifacts. The separation
keeps upstream transport types out of storage while avoiding duplicate
consensus parsers.

## Review checklist

A source-boundary change must satisfy all of these conditions:

- Upstream-specific types stop inside `zinder-source`.
- Public failures are typed `SourceError` values.
- A capability is added only with a real consumer and runtime wiring.
- Historical reads preserve segment bounds and parent-link validation.
- Query paths do not call upstream nodes directly.
- Tests use deterministic source fixtures or controlled upstream endpoints.
