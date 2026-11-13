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

## Indexer transport security

The indexer URL scheme selects the transport. `http://` opens plaintext HTTP/2,
which suits a loopback indexer listener. `https://` negotiates TLS, which a
node needs when its indexer listens on a network address. Zakura refuses a
non-loopback indexer listener without mutual TLS.

`[node.indexer_tls]` configures an `https://` indexer:

```text
ZINDER_NODE__INDEXER_GRPC_ADDR=https://zakura.internal:8230
ZINDER_NODE__INDEXER_TLS__CA_CERT_PATH=/run/secrets/indexer-ca.pem
ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PATH=/run/secrets/indexer-client.pem
ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH=/run/secrets/indexer-client.key
```

Without `ca_cert_path`, the node's certificate is verified against the system
roots. The client certificate and key are optional, and they must be set
together. `server_name` overrides the name verified against the node's
certificate. Configuration loading rejects the section for an `http://`
indexer URL or when no indexer URL is set.

`ZebraIndexerSourceTarget::from_node_target` carries the URL and these settings
to the indexer sources. The transport reads the PEM files whenever it builds a
channel, so a streaming source uses rotated files on its next reconnect.
`zinder-ingest` validates the files at startup and exits with a configuration
error when one is missing or holds no certificate or key. Binaries accept the
material only as file paths; see
[ADR-0019](../adrs/0019-transport-policy-ownership.md#credential-delivery) for
delivery on platforms that provide secrets only as environment variables. The
Compose topology mounts the files into ingest with
`deploy/docker-compose.indexer-tls.yml`; see
[Deploying on one VM](../runbooks/deploying-on-a-vm.md#stream-mempool-events-over-indexer-tls).

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
