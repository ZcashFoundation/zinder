# Node source boundary

This page explains how Zinder talks to a Zcash node: what it needs from the
node, how it authenticates, how it learns what the node supports, and how it
reports node failures. It is for operators choosing and configuring a node and
for contributors changing the `zinder-source` crate.

`zinder-source` is the only code that talks to a node. It owns the transport,
authentication, node response types, capability discovery, and the parsing of
block and transaction bytes with Zebra's consensus primitives. It returns
Zinder types and typed `SourceError` values. The caller decides whether to
retry, switch nodes, or exit. Storage code reads local stores. Light-wallet
handlers use `zinder-source` for the node-backed methods described in
[Lightwalletd compatibility](../reference/lightwalletd-compatibility.md#runtime-serving-selection). See
[ADR-0006](../adrs/0006-node-source-boundary.md).

## Node adapters

Zinder certifies Zebra and Zakura. Zakura speaks Zebra's JSON-RPC and indexer
gRPC protocols, so one set of adapters serves both, and recognizes each node's
own refusal messages.

| Type | Role | Selected when |
| --- | --- | --- |
| `ZebraJsonRpcSource` | Blocks, block batches, tip, tree states, subtree roots, value pools, health, broadcast, transparent-address lookups | Always; `ingest.source` accepts only `zebra-json-rpc` |
| `ZebraIndexerMempoolSource` | Streaming mempool events | `node.indexer_grpc_addr` is set |
| `JsonRpcMempoolSource` | Polling mempool | No indexer endpoint is set |
| `ZebraIndexerBlockSource`, `ZebraIndexerChainTipSource` | Block reads and tip notifications over indexer gRPC | Library only; no runtime selects them |

JSON-RPC is required. The indexer gRPC endpoint is optional and changes only
how ingest follows the mempool.

## The `NodeSource` contract

`NodeSource` is the trait ingest reads the chain through. It provides:

- bounded batches of connected blocks for construction;
- one block by height, for following and reorg walks;
- the tip as height and hash from one observation (`tip_id`);
- tree states and shielded subtree roots;
- chain-wide and per-block value-pool balances;
- the node's readiness, capabilities, activation table, and storage mode.

A height alone never identifies a block across a reorg, so callers pair it with
the hash. `fetch_chain_checkpoint_for_block` asks for a block's tree state by
that block's hash. A node that no longer has the block on its best chain
refuses, and the source reports `BlockUnavailable` instead of a tree state for
another block. Subtree roots have no hash-keyed request, so ingest stores roots
it derives itself and uses the node's only as a cross-check.

`TransactionBroadcaster` is a separate trait, because broadcasting is a
command, not an observation. A runtime built without one returns
`TransactionBroadcastDisabled`.

`ZebraJsonRpcSource` fetches blocks for construction as one JSON-RPC batch.
Zakura refuses more than 1,000 entries per batch, so ingest rejects
`ingest.construction.source_segment_max_blocks` above 1,000. When a batch
response exceeds `node.max_response_bytes`, or the node answers the batch with
a single error, the source splits the height range in half and retries each
half. It logs `source_chain_segment_split`.

## Transparent address lookups

Light-wallet serving uses the following `zinder-source` methods to query
the node's transparent address index.

| Method | Node RPC | Returns |
| --- | --- | --- |
| `fetch_transparent_address_balance` | `getaddressbalance` | Unspent balance and total received |
| `fetch_transparent_address_transaction_ids` | `getaddresstxids` | Transaction ids in a height range, in chain order |
| `fetch_transparent_address_utxos` | `getaddressutxos` | Every unspent output at or above a height, in chain order, and the tip the node answered at |
| `fetch_transparent_address_utxo_prefix` | `getaddressutxos` | The first `max_rows` unspent outputs at or above a height, in chain order |
| `fetch_transparent_address_utxo_page` | `getaddressutxos` | A chain-ordered prefix, including extra rows the node supplied, whether it covers every match, and the tip the node answered at |

Every method takes a `TransparentAddressLookup`: 1 to 10,000 distinct
addresses of the source's network. TEX addresses are refused, because the
index holds the P2PKH address a TEX address encodes; Zebra refuses one and
Zakura matches nothing for it. Zebra 7.1.0 and Zakura 1.8.6 answer
differently, and the source gives both the same result:

- **Row limit.** One answer holds at most 100,000 rows. Zebra refuses a
  larger `getaddresstxids` answer, and a larger `getaddressutxos` answer when
  `maxEntries` is zero. Zakura has no limit, so the source counts the rows
  that match and refuses more than 100,000. Both are reported as
  `TransparentAddressRowLimitExceeded`, and the caller narrows the lookup: a
  shorter height range for transaction ids, or fewer addresses for unspent
  outputs.
- **Height range.** Both nodes clamp `getaddresstxids` heights to the tip they
  see when they answer, and read an end height of zero as the tip. The source
  reads the tip first, returns nothing for a range above it, and sends no
  request for a range that ends at genesis, which neither node indexes. It
  reads the tip again after the request and keeps the answer only when both
  reads name the same block, because a tip that fell below the range in
  between makes the node answer with its new tip's transactions. It treats a
  refusal of the answer's size the same way, because that refusal may be for
  the new tip's range. After three attempts whose tip changed, it reports
  `ChainTipChangedDuringFetch`, classed `upstream_view_changed`.
- **Answer chain.** Neither node reports the chain a `getaddresstxids` answer
  describes. The caller names a block the answer's chain must hold, and the
  source reads the node's hash at that block's height immediately before and
  after each request. A hash read that names another block, or a tip read
  below that block's height, reports `ChainTipChangedDuringFetch`, so a
  rollback the node recovers from is never answered as an empty range.
- **Complete unspent outputs.** `fetch_transparent_address_utxos` sends
  `maxEntries` zero, the only value for which Zebra refuses a lookup over the
  row limit. With a positive `maxEntries`, Zebra returns that many outputs and
  does not say whether more match.
- **Prefix of unspent outputs.** `fetch_transparent_address_utxo_prefix`
  sends `startHeight` and `maxEntries`, which Zebra applies to its index scan.
  Zakura ignores both and returns every unspent output, so the source drops
  the rows below the start height and truncates the rest. A full prefix may
  have more outputs after it, and no later lookup resumes losslessly: the node
  pages only by height, and one block can hold more matching outputs than one
  prefix. The page method retains extra matching rows up to the same
  100,000-row cap, so callers can use rows Zakura has already sent. A page
  that reaches the local cap with more matching rows is incomplete.
- **Answer tip.** Every `getaddressutxos` request sets `chainInfo`, and both
  nodes then return the hash and height of the tip the outputs describe. The
  source returns that tip with the outputs, so a caller can bind the answer
  to a chain view. `getaddressbalance` has no such field on either node.
- **Response size.** An answer above `node.max_response_bytes` is reported as
  `SourceResponseTooLarge`. Zakura's unspent-output answer counts in full,
  even for a prefix or a high start height. Either node answers JSON-RPC error
  -32008 instead of an answer above its own `rpc.max_response_body_size`. The
  source reports that refusal as `NodeResponseTooLarge`, classed
  `configuration`, because the same request to the same node fails the same
  way.

The source checks every unspent-output row the node returns, including the
rows it drops below the start height or past a prefix. A row must name an
address of the lookup and carry a P2PKH or P2SH script that pays that
address, the address both nodes' indexes derive from the script. Rows must
be in chain order: by height, with each transaction's outputs together and
in increasing index order, so no outpoint repeats. Any other row is reported
as `SourceProtocolMismatch`, as is an answer of another shape, such as a
balance without `received`. That class is `protocol_mismatch`, not
`node_unreachable`, so such an answer does not count toward the client
rebuild described under [Authentication](#authentication).

Both nodes refuse an address lookup with code -1 and a message containing
`state was committing a block` when they commit a block while reading their
index. The source reports that refusal as `BlockCommittedDuringRead`,
classed `upstream_view_changed`, and the caller retries the same node.

A Zakura node in pruned storage mode with checkpoint sync stops maintaining its
transparent indexes and refuses `getaddressbalance`, `getaddresstxids`, and
`getaddressutxos`. The source reports that refusal, and a node without the
methods, as `TransparentAddressIndexesUnavailable`.

For address history, `fetch_mined_transaction_bytes` reads the serialized bytes
and mined block from one verbose `getrawtransaction` reply. It checks the
returned transaction ID against the request. A missing, mempool, or side-chain
transaction has no best-chain location.

## Capabilities

Ingest learns what a node supports from the methods it lists in `rpc.discover`,
not from its version. A node that does not answer `rpc.discover` fails the
check. The capabilities are `best_chain_blocks`,
`source_chain_segments`, `tip_id`, `tree_state`, `subtree_roots`,
`settled_tip_height`, `readiness_probe`, `transaction_broadcast`, `json_rpc`,
`openrpc_discovery`, `chain_value_pools`, `block_value_pool_balances`,
`historical_block_bodies`, and `standard_fee`. They are distinct from the client-facing strings in
[Capabilities](../reference/capabilities.md).

`historical_block_bodies` means the node keeps every block. The source grants
it only when `getblockchaininfo` reports `pruned: false`. A failed call or a
missing field withholds it. Zebra always reports `pruned: false`; Zakura
reports `pruned: true` in its pruned mode.

Ingest requires `json_rpc`, `best_chain_blocks`, `tip_id`, `tree_state`,
`subtree_roots`, and `historical_block_bodies`. Without one, ingest does not
start, and readiness reports `node_capability_missing`.

A reader that broadcasts verifies each node on first contact instead:

- `rpc.discover` must list `sendrawtransaction`;
- the node's activation table must agree with the store's schedule through the
  height a new transaction targets, one above the store's tip;
- the node's best chain must hold the store's tip block.

A node that fails one of the first two checks is excluded and logs
`wallet_node_excluded`. It is verified again after 30 seconds, and each later
exclusion doubles the wait, up to 5 minutes. Any other failure is retried at
the next attempt. A broadcast goes only to a verified node. While none is verified, it fails with
`NodeUnavailable` and sends nothing, so a transaction never reaches a node on
another network or chain. See
[ADR-0015](../adrs/0015-wallet-runtime-composition.md).

## Errors

`SourceFailureClass` groups node errors for recovery and readiness:

| Class | Meaning |
| --- | --- |
| `node_unreachable` | The endpoint cannot be reached or refused the credentials |
| `upstream_view_changed` | The node's best chain changed during the operation |
| `stream_disconnected` | An indexer subscription disconnected |
| `capability_missing` | The node cannot provide a required operation |
| `protocol_mismatch` | A response violates the expected wire contract |
| `malformed` | Node bytes cannot be parsed |
| `configuration` | The adapter configuration or a request limit refused the operation |

Some refusals are permanent: a fast-synced Zakura node lacks old trees and
subtree roots, and Zebra refuses a damaged stored block. The source reports
them as `HistoricalTreeStateUnavailable`, `HistoricalSubtreeRootsUnavailable`,
and `HistoricalBlockBodyUnavailable`, classed `capability_missing`, as is
`TransparentAddressIndexesUnavailable`. It matches the node's error text. A reworded refusal of a read by height falls back to
`upstream_view_changed`. For a tree-state read by block hash, the source
reports an unrecognized refusal as `protocol_mismatch` and logs
`tree_state_block_hash_refused`.

`getstandardfee` reports a zero fee, or an answer of another shape, as
`SourceProtocolMismatch`, as the transparent-address lookups report an answer
of another shape. The answer must be a JSON object with a fee and an optional
estimator version, so an array of the two numbers is another shape. A node
without the method is `NodeCapabilityMissing` for `standard_fee`.

## Authentication

`node.auth.method` selects one mode:

| Method | Fields |
| --- | --- |
| `none` (default) | None |
| `cookie` | `node.auth.path` (a cookie file) or `node.auth.cookie` (inline), not both |
| `basic` | `node.auth.username` and `node.auth.password` |

With `cookie` or `basic`, a field of the other method fails configuration.
With `none`, extra fields are ignored with a warning. Logs and `--print-config`
redact credentials, including those in node URLs.

A cookie file is read when a request needs it. Until the file holds
credentials, each request fails with `NodeUnavailable` without reaching the
node. Readers can therefore start before their node. Ingest with only `[node]`
exits instead, because its startup capability check fails. With fallbacks, an
endpoint without a readable cookie counts as unreachable, and ingest checks it
again while it runs.

Zebra refuses missing or stale credentials with HTTP 401, which the source
reports as `node_unreachable` with a reason that names HTTP 401. After three
consecutive failures of that class, the source rebuilds its client and rereads
the cookie file, so ingest recovers when a node restart rotates the cookie.
Basic credentials and an inline cookie do not change, so their refusal
continues until `node.auth` is fixed.

## Ordered node list

Every runtime that uses a node reads one ordered list, `[node]` followed by
`[node_fallbacks.N]`. [Configuration](../reference/configuration.md#the-node-sections)
lists which runtime uses which node section and the fallback format.

`NodeSet` holds the list and forwards each `NodeSource` call to the active
endpoint. When the active endpoint changes, it ends the open mempool stream, and
the consumer rebuilds its snapshot from the new endpoint.

- Ingest switches its chain reads between endpoints. See
  [Chain ingestion](chain-ingestion.md#node-endpoint-failover).
- `zinder-query` and wallet-mode compatibility use the list only to broadcast
  and, in compatibility, to read the recommended fee with `getstandardfee`.
  They try the active endpoint first and move to the next verified one. They
  read only the JSON-RPC and `[node.health]` fields.
- `zinder-explorer` validates the list but reads only `[node]`.

## Indexer gRPC and TLS

The scheme of `node.indexer_grpc_addr` selects the transport. `http://` opens
plaintext HTTP/2, which suits a loopback listener. `https://` negotiates TLS,
which a node needs when its indexer listens on a network address. Zakura
refuses a non-loopback indexer listener without mutual TLS.

`[node.indexer_tls]` configures an `https://` indexer:

```text
ZINDER_NODE__INDEXER_GRPC_ADDR=https://zakura.internal:8230
ZINDER_NODE__INDEXER_TLS__CA_CERT_PATH=/run/secrets/indexer-ca.pem
ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PATH=/run/secrets/indexer-client.pem
ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH=/run/secrets/indexer-client.key
```

Without `ca_cert_path`, the node's certificate is checked against the system
roots. The client certificate and key, which mutual TLS needs, must be set
together. `server_name` overrides the name checked against the certificate.
The section is rejected without an indexer URL or with an `http://` one.

The transport rereads the PEM files whenever it opens a channel, so a stream
picks up rotated files on its next reconnect. `zinder-ingest` validates the
files at startup and exits when one is missing or holds no certificate or key.
Binaries accept key material only as file paths;
[ADR-0007](../adrs/0007-node-failure-recovery-and-transport-policy.md) covers
platforms that deliver secrets as environment variables.
[Deploying on one VM](../runbooks/deploying-on-a-vm.md) shows the Compose
overlay that mounts the files.

## Upstream health probe

When `[node.health].addr` is set, the source polls the node's readiness
endpoint on the configured cadence. Each probe waits at most two seconds, or the
cadence if shorter, and falls back to `getblockchaininfo` in the same poll when
it fails. A node that accepts connections but never answers therefore cannot
stall a caller. Without `[node.health].addr`, readiness comes from JSON-RPC
observations. Both JSON-RPC paths apply `verification_progress_floor` and
`estimated_gap_floor_blocks` from the active node's health settings.

Ingest starts its periodic readiness probe when the primary node has a health
address. Setting only the fallback floors changes how JSON-RPC observations
are classified. A reader with a configured node polls it on either path.

The node's readiness caps ingest's readiness. Readers use their node only to
broadcast and, in wallet-mode compatibility, to read the recommended fee, so
they report it as a degraded dependency and keep serving stored data. See
[Readiness](../reference/readiness.md) and
[ADR-0016](../adrs/0016-wallet-serving-readiness.md).
