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
- chain-wide and block-bound value-pool observations;
- upstream readiness; and
- the node's own description: `probe_capabilities`, which also refreshes
  `capabilities`, `fetch_network_upgrade_activations`, and
  `fetch_storage_mode`.

Ingest reaches these through the trait, and a wallet reader's
`BroadcastNodeProbe` delegates to the same methods.
`discover_network_upgrade_activations` wraps
`fetch_network_upgrade_activations` with the shared discovery log event,
and `MempoolSourceFactory` builds an endpoint's live mempool source: the
streaming indexer source when the endpoint names an indexer, polling
otherwise. `ZebraJsonRpcSource::from_node_target` builds the source one
resolved `NodeTarget` describes, including its indexer endpoint.

Every operation returns Zinder types and `SourceError`. Historical construction
uses `fetch_chain_segment` with explicit block-count and response-byte limits.
The response carries ordered `SourceChainUpdate` values plus payload statistics
used by the writer's resource controller. See
[ADR-0016](../adrs/0016-source-segment-fetching.md).

`ZebraJsonRpcSource` fetches a segment as one JSON-RPC batch with one
`getblock` entry per block. Two node limits shape that batch:

- Zakura 1.8.6 refuses a batch with more than 1,000 entries. Zebra 7.1.0 has
  no entry limit. Ingest therefore rejects a configured segment size above
  1000 blocks at startup.
- Zebra 7.1.0 and Zakura 1.8.6 each refuse a response larger than 50 MiB by
  default. Either node answers an oversized batch with a single error object
  instead of a response array. The source treats that answer like an
  oversized response and splits the segment.

`node.max_response_bytes`, 64 MiB by default, is a ceiling on what Zinder
accepts, not a request size. The default segment target of 32 MiB stays below
both node limits, so a split is the exception rather than the normal path.

`tip_id` returns height and hash from one source observation. Height alone is
not a chain identity and must not be used to accept a value across a reorg.

`fetch_chain_checkpoint_for_block` applies the same rule to tree state. It
returns the checkpoint of a block that the caller has already identified.
`ZebraJsonRpcSource` sends that block's hash to `z_gettreestate`, which Zebra
7.1.0 and Zakura 1.8.6 accept in place of a height. The node then answers for
that block, or refuses with "the requested block is not in the main chain" when
the block has left its best chain. The source maps that refusal to
`BlockUnavailable`, and a response that names another block to
`SourceProtocolMismatch`. Two refusals keep the class they have on a
height-keyed read: a fast-synced Zakura node's refusal of a skipped tree, and
Zebra's refusal to reassemble a damaged stored block
(`HistoricalBlockBodyUnavailable`). Any other refusal of the hash is also
`SourceProtocolMismatch`, because the hash came from a block that the node
itself served. The source logs
the node's code and message as `tree_state_block_hash_refused`. A source
without a hash-keyed read checks the identity
of a height-keyed answer instead, and returns `BlockReorgDuringFetch` when the
answer names another block. `fetch_chain_checkpoint` stays height-keyed for a
caller that knows only a height, such as a configured construction checkpoint.

`fetch_subtree_root_range` has no block-keyed form. `z_getsubtreesbyindex` is
keyed by pool and subtree index, and each root it returns carries only the
height of the block that completed it. Ingest therefore stores the roots it
derives from the completing blocks' note commitments and uses the node's roots
only as a cross-check, as [Chain ingestion](chain-ingestion.md) describes.

`TransactionBroadcaster` is separate because broadcasting is a command, not a
chain observation. The unit implementation returns
`TransactionBroadcastDisabled`, so a read-only composition fails explicitly.

## Ordered node list

Every node-reading runtime reads one ordered list of endpoints: `[node]` is
position 0, and `[node_fallbacks.N]` is position `N`, from 1 without a gap.
Each fallback table has the fields of `[node]`. Positions are integer table
keys in TOML and in the environment
(`ZINDER_NODE_FALLBACKS__1__JSON_RPC_ADDR`), so a file and the environment
merge field by field; a TOML array of tables is refused, because the loader
would replace the whole array as soon as one environment variable named an
entry. `zinder-ingest`, `zinder-query`, `zinder-compat-lightwalletd`, and
`zinder-explorer` accept the list, and `--print-config` renders it with
credentials redacted. Compatibility accepts it only with
`compat.serving = "wallet"`. `zinder-projector` and compact-blocks
compatibility reject both `[node]` and the list.

`NodeSet` holds the list and implements `NodeSource` by delegating every call
to its active endpoint, so code written against one node is unchanged.
`NodeSet::connect(primary, fallbacks)` builds every endpoint's
`ZebraJsonRpcSource`. Ingest builds its endpoints this way; the readers still
build one `ZebraJsonRpcSource` from `[node]`. Only the `NodeSetSwitch` returned
with the set changes the active endpoint, and `NodeSet::mempool_source` follows
the active endpoint's mempool: a switch ends the open event stream with one
`MempoolStreamUnavailable` item, so the consumer rebuilds its snapshot from the
new endpoint as after any stream failure.

Only ingest switches, and only between follow iterations; see
[Chain ingestion §Node endpoint failover](chain-ingestion.md#node-endpoint-failover)
and [ADR-0013](../adrs/0013-source-failure-recovery-topology.md). The readers
validate the list and keep reading `[node]`.

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
ZINDER_NODE__INDEXER_GRPC_ADDR=http://127.0.0.1:8230
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
- `historical_block_bodies`

The Zebra JSON-RPC source derives capabilities from two observations. Methods
advertised by `rpc.discover` grant the method-backed capabilities. When the
node advertises `getblockchaininfo`, the probe also reads its storage mode
through `fetch_storage_mode` and grants `historical_block_bodies` only for
`pruned: false`. A failed call or a response without `pruned` withholds the
capability, so an unreported storage mode fails closed. Zebra always reports
`pruned: false`; Zakura reports `true` in pruned storage mode.
`fetch_storage_mode` itself reports `archive`, `pruned`, or `unreported`, and
fails when the call fails, so ingest can exclude a fallback endpoint only when
it reports `pruned: true`.

Ingest validates the capabilities it needs before mutation begins and requires
`historical_block_bodies`, so it refuses a pruned node at startup with readiness
cause `node_capability_missing`. Optional methods add capabilities without
changing the required canonical contract. Missing or contradictory
capabilities produce typed failures rather than version-based guesses.

Ingest is the only runtime that needs its node to start. Query and
lightwalletd compatibility use a node only to broadcast transactions, accept a
pruned node, and start without contacting it; the projector and compact-blocks
compatibility use no node at all. A reader wraps its configured endpoint in a
`NodeBroadcast` and verifies it on first contact through the source's
`BroadcastNodeProbe`: `rpc.discover` must list `sendrawtransaction`, the
`getblockchaininfo` activation table must agree with the serving pair's
writer-admitted schedule through the visible tip, and the node's best chain
must hold the pair's visible tip block, read with `getblockhash`. A broadcast
before that verification passes fails with `NodeUnavailable` and sends
nothing, so a transaction never reaches a node on another network or chain.
A reader also starts before its node writes a cookie file, as described under
[Authentication and health](#authentication-and-health).

Some nodes refuse historical reads permanently. A Zakura node built by
verified-commitment-trees fast sync never writes per-height trees or subtree
roots below its last checkpoint. Zebra 7.1.0 and later refuse a stored block
whose transactions are incomplete or whose metadata is inconsistent, instead of
returning a partial block; on a node that keeps every block body, that means
its store is damaged. The source maps those refusals to dedicated errors
classed `capability_missing`:

| Error | Raised for |
| --- | --- |
| `HistoricalTreeStateUnavailable` | `z_gettreestate` or verbose `getblock` at a height whose tree the node never wrote |
| `HistoricalBlockBodyUnavailable` | `getblock` (single or batched), `z_gettreestate`, or indexer `GetBlock` for a stored block the node cannot reassemble |
| `HistoricalSubtreeRootsUnavailable` | `z_getsubtreesbyindex` for subtrees the node never recorded and will not restore |

A subtree refusal that may resolve once the node passes its last checkpoint
stays `SubtreeRootsUnavailable`, classed `upstream_view_changed`. The match is
on the node's error text, so a reworded refusal falls back to the transient
classes rather than to a wrong answer. Only ingest makes these reads. Query
serves tree states, block bodies, and subtree roots from the store, so none of
the three reaches a query client.

Source errors describe the upstream observation. `SourceFailureClass` groups
them for writer recovery and readiness:

| Class | Meaning |
| --- | --- |
| `node_unreachable` | The endpoint cannot be reached |
| `upstream_view_changed` | A best-chain observation changed during the operation, for example a block left the best chain before its tree state was read |
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

Zebra 7.1.0 and later refuse missing or stale credentials with HTTP 401 and a
JSON-RPC error body. The source reports the refusal as `NodeUnavailable`,
classed `node_unreachable`, with a reason that names HTTP 401. Three
consecutive failures of that class rebuild the JSON-RPC client, and the
rebuild reads a cookie file again, so ingest recovers after a node restart
rotates its cookie. Basic credentials and an inline cookie do not change on a
rebuild, so their refusal continues until `node.auth` is corrected.

Building a source reads no cookie file it cannot read yet. The node writes the
file when it starts, so a missing, unreadable, or empty file leaves the source
waiting for it: each request reads the file first and, until it holds
credentials, fails with `NodeUnavailable` without reaching the node. A reader
therefore starts before its node, and its first-contact verification passes
once the file appears. Ingest's startup capability probe fails on the same
error, so ingest still exits when it starts without a readable cookie for
`[node]`. With `[node_fallbacks.N]`, an endpoint whose cookie file is not
readable yet fails its startup check as unreachable: ingest starts on the
first endpoint that passes and checks the waiting one again while it runs.
Empty inline cookie credentials and a `node.json_rpc_addr` that is not a
usable URL come from configuration and still fail at startup.

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
to the writer. Upstream readiness caps ingest readiness even when local storage
is healthy. Wallet-serving readers use their node only for broadcast, so they
report its readiness as a degraded dependency and keep serving stored reads
([ADR-0045](../adrs/0045-wallet-serving-readiness-follows-the-serving-pair.md)).

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
