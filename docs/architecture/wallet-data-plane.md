# Wallet Data Plane

The wallet data plane is the part of Zinder that wallets and wallet-like applications call. It is not a wallet and must not become one by accident.

## Responsibility

The `zinder-query` crate owns the wallet request, adapter, and error contract,
and its binary serves the native `zinder.v1.wallet.WalletQuery` protocol.
`zinder-projector` owns the durable wallet state.
`zinder-compat-lightwalletd` separately serves wallets that require the
lightwalletd protocol. Both wallet-facing runtimes are deployed and admit
independent secondary generations at the same canonical fence contract.

Per [ADR-0005](../adrs/0005-consumer-neutral-wallet-data-plane.md),
this plane is consumer-neutral. Mobile SDKs, lightwalletd clients, native
full-node wallet adapters, and Rust libraries exercise different public
contracts, but they all depend on the same canonical artifact families. Compatibility adapters may
preserve lightwalletd wire names; the core vocabulary stays on artifact coverage,
tree-state anchors, chain epochs, and typed errors.

It should provide:

- Compact block range APIs.
- Visible-tip block and chain metadata APIs.
- Transaction lookup APIs where compatible with Zcash wallet expectations.
- Tree state APIs required for wallet sync.
- Sapling and Orchard subtree root APIs required for batched wallet scanning.
- Transparent-address output APIs required by lightwalletd/Zodl compatibility,
  backed by the stored transparent output artifact family.
- Transaction broadcast.
- Chain-event subscription per [Wallet data plane §Chain-Event Subscription](wallet-data-plane.md#chain-event-subscription).
- Mempool snapshot and mempool-event subscription per [ADR-0007](../adrs/0007-mempool-topology-and-retention.md).
- `ServerInfo` capability descriptor per [Public interfaces §Capability Discovery](public-interfaces.md#capability-discovery).
- Optional compatibility endpoints for lightwalletd clients.

It should not provide:

- Spending-key custody.
- Viewing-key custody.
- Seed phrase storage.
- Server-side shielded wallet scanning.
- Address ownership inference for shielded users.
- Compliance or identity logic.

## Privacy Boundary

A wallet-facing indexer can still leak metadata. Zinder must treat wallet API design as a privacy boundary.

Required privacy rules:

- Do not require shielded users to reveal spending keys or viewing keys.
- Do not add server-side address scanning as a convenience feature.
- Keep request logs free of sensitive wallet identifiers where possible.
- Document which API calls can reveal interest in a height range, transaction, or address-like value.
- Support deployment behind privacy-preserving transport where operators need it.

## Compact Blocks

The compact block builder belongs to ingestion. The wallet data plane serves compact block artifacts through the fence-bound `CanonicalReader` of an admitted `WalletServingReadPair`, or through the compact-serving canonical secondary in `zinder-compat-lightwalletd`.

Compact blocks are stored as the structured, consumer-neutral
`CompactBlockArtifact` defined by `zinder-core`. Native WalletQuery response
builders and the lightwalletd adapter translate that one semantic artifact at
their protocol boundaries. The durable format is versioned independently from
either RPC schema, so compatibility message changes do not redefine canonical
wallet facts.

This avoids two problems:

- Query-time construction can mix chain views under concurrent reorgs.
- Wallet traffic can force expensive upstream node reads or artifact derivation.

If a requested compact block is outside the store's retained history, `zinder-query` returns `ArtifactUnavailable` for the compact-block family and the first absent height. A range read that reaches above the visible tip or below the retained floor therefore reports the same `NOT_FOUND` outcome as the single-block read. A compact block missing inside retained history is store damage: the range read fails closed as a canonical storage error (`UNAVAILABLE` with `STORAGE_UNAVAILABLE`) so operators see the integrity failure. `zinder-query` does not fetch the block from the upstream node and build a one-off response.

The native wallet protocol slices expose latest block metadata, compact block ranges, checkpoint tree-state reads, latest checkpoint tree-state reads, subtree roots, lightd-compatible network metadata, and the chain-event subscription described below as generated `zinder_proto::v1::wallet` responses. Each unary response carries the cross-plane `ChainView` at field tag 1; wallet responses fill `chain_view.chain_epoch` with the epoch used to answer the read and leave the materialized-view axes unset (see [ADR-0011](../adrs/0011-explorer-freshness-envelope.md)). Native gRPC streams compact block ranges as `CompactBlocksInRangeChunk` messages so range size is bounded by request limits and not by a single gRPC response message. `WalletQueryGrpcAdapter` serves the generated native `WalletQuery` tonic service over `WalletQueryApi` through `grpc/native.rs` response builders and preserves the same epoch binding, unavailable-artifact, and range limit behavior.

A request pins a chain snapshot with `optional uint64 at_epoch_id`: absent resolves to the visible epoch at request time; present resolves the canonical epoch by id. The store keys the epoch by id, so a pinned read either resolves it or returns `CHAIN_EPOCH_PIN_UNAVAILABLE` when the id is no longer retained. `ChainEpoch` is a response-only descriptor nested in `ChainView`; the request never echoes the epoch body.

### Compact block range stream

`CompactBlocksInRange` uses the one-shot header shape of the transparent
address streams:

1. The server sends one leading `ChainView` header (`chunk.body.header`). Its
   `chain_epoch` is the single epoch that answers the whole range.
2. The server then sends one `CompactBlock` per requested height, in ascending
   order (`chunk.body.item`). Blocks carry no per-block epoch.

`zinder-client` decodes this stream behind `ChainIndex::compact_blocks_in_range`
and `ChainSnapshot::compact_blocks_in_range`, which yield `CompactBlockArtifact`
values. The stream fails with `IndexerError::MalformedResponse` and then ends
when the server does any of the following:

- sends a block before the header;
- sends a second header;
- ends the stream without a header;
- sends a header for a different network than the client's, or whose epoch
  differs from the request's `at_epoch_id`;
- skips, repeats, or exceeds a requested height, or stops before the last one;
- sends a block above the header epoch's visible tip.

Contract revision 7 introduced this shape. In revision 6 and earlier, every
chunk repeated the full `ChainView` beside its block. `zinder-client` requires
revision 8, which keeps this shape, so it rejects an older server at preflight
instead of misreading its stream. The removed fields map directly to the new ones:

| Revision 6 field | Revision 7 replacement |
| --- | --- |
| `CompactBlocksInRangeChunk.chain_view`, repeated on every chunk | `CompactBlocksInRangeChunk.body.header`, sent once before any block |
| `CompactBlocksInRangeChunk.compact_block` | `CompactBlocksInRangeChunk.body.item` |

The field tags stay 1 and 2, and the capability string stays
`wallet.read.compact_block_range_v2`.

## Full Blocks

Full-block-scanning wallets parse whole serialized blocks for inline transparent
detection and shielded trial-decryption, so the native wallet protocol defines
consensus-serialized block reads alongside the compact-block surface.
`FullBlock(height, at_epoch_id)` returns one serialized block, and
`FullBlocksInRange(start, end, at_epoch_id)` streams
`FullBlocksInRangeChunk` messages. The admitted serving-pair query implements
both operations directly over canonical `BlockBlobArtifact` rows.

The range pins one admitted pair for the entire stream and repeats its
`ChainView` at field tag 1 on every chunk. Pair rotation cannot change or close
that in-flight read because the driver retains the pair's `Arc`; a later
request using an expired epoch receives `CHAIN_EPOCH_PIN_UNAVAILABLE` and must
reacquire its whole workflow snapshot. The request is bounded to 1,000 blocks.
The driver walks it in 16-block multi-get sub-reads and forwards through a
four-item channel as the client drains, so per-stream memory tracks one
sub-read plus bounded channel depth rather than the whole window.

Full-block support is derived from authenticated persisted retention, not a
runtime profile. Ingest writes block blobs only when `storage.raw_blob_policy =
"all"`, and query must declare the same value when admitting its canonical
secondary. An admitted `All` endpoint advertises
`wallet.read.full_block_at_v1` and `wallet.read.full_block_range_v1`; `None`
and `Transactions` endpoints omit both, and direct calls fail with the precise
missing-capability precondition. A missing blob inside an `All` store is an
`ArtifactUnavailable` integrity or coverage outcome, not an invitation to
fetch history from the upstream node. Changing a non-empty store to `All`
requires a rebuilt store and blue-green cutover.

Tree-state storage keeps typed commitment-tree checkpoints: the validated
frontier of each shielded pool at every 100-block cadence height, at the latest
committed tip, and, for checkpointed history, at the history predecessor. Reads
derive the tree-state payload, one `commitments.finalState` per pool, from
those frontiers; the store does not keep an upstream node's JSON. A
checkpointed store therefore serves its floor's tree state although it retains
no block at the floor. Every store serves a latest tree state, because every
build retains at least one block and checkpoints its tip.

The native read surface is
`tree_state_at(height, at_epoch_id)` and `latest_tree_state_checkpoint(at_epoch_id)`.
`tree_state_at` serves the tree state at exactly `height`: a stored checkpoint
when one exists there, otherwise a cache-fill from the configured upstream node
(mirroring lightwalletd's `GetTreeState`), so the returned height always equals
the requested height. The fill is the one query path permitted to contact an
upstream node, gated on an explicitly supplied source (see ADR-0005). An
endpoint advertises `wallet.read.tree_state_at_height_v2` only after probing a
node that supports tree-state fills. Without one, `tree_state_at` fails with
that missing-capability precondition, and `latest_tree_state_checkpoint` still
serves the stored tip checkpoint. Lightwalletd compatibility returns a
`TreeState` response for `GetLatestTreeState`; `GetTreeState(BlockID)` is a clean
passthrough to `tree_state_at` and serves any height the upstream can answer.

Subtree roots are also wallet-sync artifacts. Query and compatibility services
must serve them from epoch-bound indexed state and must distinguish a valid empty
range from a not-yet-available upstream node subtree index.

The native query path makes that distinction without upstream node repair or
query-time compact-block decoding: it reads `ChainTipMetadata` from the same
`ChainEpoch` to decide whether a subtree index can exist, then reads stored
`SubtreeRootArtifact` values for completed roots. Ranges beyond the completed
subtree count return an empty response. A store holds every root its retained
blocks complete. A checkpointed store whose construction loaded retained-range
coverage has no row for a root its history predecessor had already completed,
and a read of that root returns `ArtifactUnavailable` for the subtree-root
family and index (`NOT_FOUND`). A store whose construction loaded the complete
prefix holds every completed root from index zero. Any other absent completed
root, including any absent root in a complete-prefix store, is a canonical
integrity failure.

## Block Identity and Hash-Keyed Reads

Height remains the primary key for compact-block reads. Wallet scanning is
height-ordered, compact-block ranges are height-bounded, and the canonical store
keeps those reads cheap and predictable. Do not turn compact-block range APIs
into hash-driven scans.

Zinder ships a canonical best-chain hash-to-height resolver through the typed
[`zinder_core::BlockSelector`] enum (`Height(BlockHeight) | Hash(BlockHash)`).
The resolver is backed by the `block_hash_index` column family in `zinder-store`:
every committed block writes a hash-to-height entry, and read paths treat that
entry as a hint until the canonical header at the recorded height proves the
same hash is still visible. A missing, displaced, or stale entry never resolves
to the replacement block at that height; the native API reports that the
requested block is not in the best chain.

The native surface is `WalletQuery.BlockIdBySelector` (capability
`wallet.read.block_id_by_selector_v1`) returning `BlockIdResponse { chain_view,
block_id }`. Compat hash-only requests (`GetBlock`, `GetTreeState`,
`GetTransaction`-by-block-hash) call the resolver before reaching the existing
height-keyed read; height-only callers get a normalized `BlockId` with the
resolved hash. Both the gRPC adapter and `zinder-client::ChainIndex` expose
`block_id_by_selector(BlockSelector)`.

The same resolver backs the typed block-header read model:
`WalletQuery.BlockHeaderBySelector` (capability
`wallet.read.block_header_by_selector_v1`) returns `BlockHeaderResponse {
chain_view, block_header }` where `BlockHeader` is the Zinder-native
header shape (block identity, previous hash, merkle root, commitment bytes,
block time, bits, nonce, version). The shape does not re-export Zebra's
JSON-RPC `getblockheader` object or the lightwalletd compact block header. The
header is read at request time from the typed `BlockHeaderArtifact`; raw block
bytes are not part of the normal wallet read path. If repeated reads become
the larger cost, the implementation should improve the typed header row rather
than reintroducing raw-block parsing.

The release serving-pair query resolves height and hash selectors. Height selectors read canonical headers;
hash selectors use the canonical hash index and verify the header at the
indexed height. The release composition therefore advertises
`wallet.read.block_id_by_selector_v1`.

The release composition does not advertise
`wallet.read.block_header_by_selector_v1`. The current native `BlockHeader`
message omits the Equihash solution and is not a consensus-complete header
contract. Consumers that require the complete serialized header read a
retained full block and decode its header instead; method presence alone does
not justify a capability claim.

`BlockSelector` is `#[non_exhaustive]`. Non-best-chain `(txid, block_hash)`
lookup is a *separate method*, not a third selector arm. That form is a
non-best-chain transaction lookup and has different retention, secondary-reader,
and explorer semantics than ordinary wallet sync. `Transaction` /
`TransactionStatus` answers against the visible canonical chain plus the live
mempool; a non-best-chain lookup must add its own named API surface.

## Transaction Status and Enrichment

Native transaction lookup returns typed transaction status. The public Rust shape is
`zinder_core::TxStatus` and the native gRPC surface mirrors it through
`WalletQuery.Transaction(TransactionRequest) returns (TransactionStatusResponse)`
under capability `wallet.read.transaction_by_id_v2`. The response carries the
shared `TransactionLocation` oneof on its `location` field; the oneof has two
arms. `NotFound` is not an oneof slot; it is gRPC `NOT_FOUND` with reason
`TRANSACTION_NOT_FOUND`, described under [Transaction absence](#transaction-absence):

- `mined`: `MinedTransaction { MinedBlockLocation location; MinedTransactionChainContext chain_context; bytes raw_transaction_bytes }`.
- `in_mempool`: `MempoolEntry { string transaction_id; string auth_digest; bytes raw_transaction_bytes; CompactTransactionData compact_transaction_data; uint64 first_seen_unix_millis; ChainEpoch first_seen_chain_epoch; repeated TransparentMempoolOutput transparent_outputs; repeated TransparentMempoolSpend transparent_spends }`.

`TransactionLocation` is one message defined in `wallet.proto` and embedded by
every read surface that answers "where does this transaction live", including
`ExplorerQuery.TransactionDetail`, so a consumer writes one match shape for both
planes.

The mined variant carries epoch-bound `MinedTransactionChainContext {
consensus_branch_id, block_time, confirmations }`. These fields are
response/read-model values, not persisted transaction-artifact fields.
`MinedTransactionChainContext::from_response_epoch(epoch, mined_height, consensus_branch_id,
block_time)` is the **only** public constructor in `zinder-core`. Callers
cannot construct `MinedTransactionChainContext` without the response's `ChainEpoch` in scope,
so the racy `tip_height - block_height` confirmations computation is
prevented by construction. `consensus_branch_id` is resolved from the
network-upgrade schedule the canonical writer admitted, as captured with the
serving pair: callers invoke `activations.consensus_branch_id_at(height)`,
which returns `PRE_OVERWINTER_BRANCH_ID` for heights below the earliest
activation. Before the writer's first admission, the pair carries no schedule
and the query uses the table it discovered at startup through
`ZebraJsonRpcSource::discover_network_upgrade_activations()`; reader admission
requires that table to equal the store's construction table. The service
binary refuses to start when `[node]` is not configured, so the field always
reflects a real node-discovered table; see
[ADR-0008](../adrs/0008-network-parameter-discovery.md) and
[ADR-0039](../adrs/0039-writer-admitted-network-upgrade-schedules.md).
`block_time` is read from the stored `BlockHeaderArtifact` and falls back to
`0` only when the typed header row is unavailable.

`WalletQuery.NetworkUpgradeActivations` exposes that same writer-admitted
schedule to native wallet backends under capability
`wallet.read.network_upgrade_activations_v1`. Each row carries the consensus
branch id, node-reported name, and activation height. Wallets compare it with
their configured consensus parameters before opening or migrating wallet state;
they derive active versus pending status against the epoch-pinned latest block
rather than trusting a second node query.

The mined arm may also carry `raw_transaction_bytes`: the serialized consensus
transaction bytes, symmetric with the mempool arm's live bytes. The field is
gated separately by `wallet.read.transaction_bytes_v1` because it is not
unconditionally present. Ingest writes canonical transaction blobs only when
`raw_blob_policy` is `transactions` or `all`; under `none`, typed transaction
location and enrichment remain available while mined bytes are absent. The
bytes come from the same `TransactionBlobArtifact` the canonical reader
resolves and are not a separate RPC. A consumer requiring the serialized mined
form preflights both `wallet.read.transaction_by_id_v2` and
`wallet.read.transaction_bytes_v1`.

A response builder must not call the upstream node or latest tip again during
response construction.

The epoch rule is stricter for mempool. An `at_epoch_id` transaction lookup is a
canonical chain read and never consults live mempool state. A non-epoch-pinned
lookup falls through to the writer-owned mempool index after the canonical
chain misses.

### Transaction absence

A transaction absence is an answer, not a failure, and it is reported only when
it is proven ([ADR-0042](../adrs/0042-proven-transaction-absence.md)). The
server returns gRPC `NOT_FOUND` with reason `TRANSACTION_NOT_FOUND` when:

- the canonical chain of the answering epoch has no record of the transaction,
  searched from height 1 through the epoch's visible tip; and
- the store retains complete canonical history; and
- for a lookup without `at_epoch_id`, the writer's live mempool, at the same
  epoch id, has no record either.

The writer's mempool view answers for exactly one source tip. Only a `Mined`
change for the tip block removes an entry; a `Mined` change for any other block
ends the mempool generation, so point reads fail with `UNAVAILABLE` until the
next generation is certified. The mempool half of an unpinned absence is still
best effort around reorgs and block boundaries
([ADR-0042](../adrs/0042-proven-transaction-absence.md)); the canonical half is
exact.

`ErrorInfo.metadata` carries `chain_epoch_id`, `first_covered_height` (always
`1`), `visible_tip_height`, and `covers_mempool`. A caller learns the covered
range from the absence itself; a store that cannot cover height 1 never issues
one. Zinder never indexes the genesis block at height 0, so its coinbase is
reported absent like any unknown id.

A store whose history starts after a checkpoint cannot prove an absence, but a
pending transaction needs no such proof. An unpinned lookup on that store still
consults the writer's live mempool and reports a hit as `in_mempool`; only a
mempool miss becomes `CANONICAL_HISTORY_UNAVAILABLE`. A pinned lookup never
consults the mempool, so every pinned miss on that store is
`CANONICAL_HISTORY_UNAVAILABLE`.

Every other outcome is an error:

| Condition | Status and reason |
| --- | --- |
| Store history starts after a checkpoint; an unpinned lookup reaches this only after a mempool miss | `FAILED_PRECONDITION`, `CANONICAL_HISTORY_UNAVAILABLE`, `metadata["first_available_height"]` |
| Pinned epoch is not served, or, for an unpinned lookup, the writer's mempool view is not yet coherent with the writer's epoch | `FAILED_PRECONDITION`, `CHAIN_EPOCH_PIN_UNAVAILABLE` |
| Storage fault on a visible transaction | `NOT_FOUND`, `ARTIFACT_UNAVAILABLE` with `ResourceInfo`, or another storage reason |
| Mined block is not in the best chain | `NOT_FOUND`, `BLOCK_NOT_IN_BEST_CHAIN` |
| Mempool answered at another epoch than the canonical read | `NOT_FOUND`, `CHAIN_EPOCH_MISSING` naming the writer epoch |
| Writer answered the mempool miss without the typed absence | `NOT_FOUND`, `ARTIFACT_UNAVAILABLE` without `ResourceInfo` |
| Mempool view hydrating, rebuilding, or unreachable | `UNAVAILABLE` |

To judge a transaction against a height, pin the lookup to an epoch whose
visible tip covers that height, for example through a `zinder-client`
`ChainSnapshot`, and confirm the epoch is still current afterwards when the
judgment depends on it. A pinned absence says nothing about the mempool; make
an unpinned lookup to learn whether the transaction is pending.

Each native wire response shape pairs to one capability string. The capability records the semantic shape; a change to the response shape requires a new capability string, even when the RPC name stays the same.

## Chain-Event Subscription

Wallet sync needs durable chain-state notifications. `WalletQuery.ChainEvents` is the native subscription that delivers `ChainEventEnvelope` messages to wallet clients in `event_sequence` order, settled by [Wallet data plane §Chain-Event Subscription](wallet-data-plane.md#chain-event-subscription). Chain ingestion already produces these envelopes at every canonical commit; this RPC is the wire boundary that exposes them to wallet clients without requiring them to poll latest-block metadata or infer tip changes from unrelated stream lifecycles.

The contract:

- Each `ChainEventEnvelope` carries the cross-plane `ChainView` at field tag 3 (`chain_view`). `chain_view.chain_epoch` is the epoch visible after the event, and `chain_view.chain_epoch.settled_tip.height` is the settled tip height that was true for the event. The envelope carries no separate `settled_tip_height` field.
- The cursor is the `StreamCursorTokenV1` bytes documented in [Chain events](chain-events.md). Clients persist the exact bytes returned in the previous envelope and resume strictly after that cursor.
- The request carries a required `EventStreamStart start` oneof per [ADR-0027](../adrs/0027-event-stream-start-positions.md): `after_cursor` resumes strictly after the supplied cursor, `earliest_retained` replays from the retention floor (the bootstrap path for a fresh wallet install), and `live_tail` resolves once at subscribe time to a server-minted head cursor so only events applied after subscription are delivered. An unset start is `INVALID_ARGUMENT`.
- With `after_cursor`, the cursor's encoded family is authoritative; a non-default request `family` that disagrees is `INVALID_ARGUMENT`. `earliest_retained` and `live_tail` resolve within the request's `family` field.
- The server emits historical events first (replay phase) and then continues with live events in one ordered sequence; clients see no transition.
- Stream end means the consumer disconnected or the server is shutting down. It never means a new block arrived. Clients must distinguish stream end from stream error and reconnect with their last persisted cursor in both cases.
- An expired cursor returns the typed `EventCursorExpired` error and does not silently restart from the current tip.

`ChainCommitted` and `ChainReorged` are the two event variants. `ChainReorged` carries both the reverted range and the replacement range, so a wallet receiving a reorg event truncates its local view at the reverted boundary and resumes from the replacement range without making additional indexer calls. If a client reconnects with a cursor whose branch was reorged out, the server resolves the fork point from the cursor's back-spaced locator against the canonical block index and delivers a `ChainReorged` envelope before resuming, synthesizing it when the real reorg event has aged out of retention. A wallet recovers from a reconnect reorg without a full re-derive and never observes silent branch changes. A divergence deeper than the locator cap degrades to the typed `EventCursorExpired`. See [ADR-0025](../adrs/0025-chain-event-reconnect-reorg-locator.md).

Two cursor varieties are advertised under capability string `wallet.events.chain_v1`: `Visible` and `Settled`. Visible consumers receive every envelope including reorgs. Settled consumers receive only non-reorg commits entirely at or below the settled tip. The settled cursor family is represented in the cursor body, not by a separate RPC.

The lightwalletd compatibility shim does not expose this subscription. The vendored `CompactTxStreamer` proto has no equivalent method, and ADR-0004 forbids inventing parallel surfaces in the compat layer. Wallet clients on the lightwalletd contract use `GetLatestBlock` polling, while native Zinder clients use the subscription contract.

## Mempool Snapshot and Subscription

Mempool surfaces are owned by [ADR-0007](../adrs/0007-mempool-topology-and-retention.md), which records the source, live index, event log, API, compatibility, retention windows, metrics-only diagnostics, and exact-tip readiness prerequisite.

The unconfirmed-transaction contract serves several Zcash ecosystem products,
but each product consumes a different boundary. This table is the canonical
product map; reference documents carry the line-numbered source evidence and
observed wallet-run details.

| Consumer shape | Zinder relationship | What the mempool surface enables | Required boundary |
| ----------------- | ------------------- | ---------- | ----------------- |
| Native full-node wallet adapter | Native `WalletQuery` consumer | Typed transaction lifecycle, rebroadcast decisions, transparent unmined UTXO updates, and chain-tip notifications separate from mempool stream lifecycle | Generated `WalletQuery` client or `RemoteChainIndex`, depending on whether the consumer can link `zinder-client` |
| Rust wallet library or application | Native typed client | Chain events, mempool state, transaction status, and broadcast behind the application's own wallet abstractions | Public `RemoteChainIndex` plus `EndpointBackedIndex` over `zinder-query` |
| Lightwalletd-compatible wallet | Compatibility consumer | Existing SDK mempool observation, pending-send feedback, shielded mempool scanning, and transaction submission behavior | `zinder-compat-lightwalletd` mapping `GetMempoolStream` and `GetMempoolTx` over the native mempool index and event log |
| Block explorers and analytics | Application or `zinder-explorer` consumers | Live mempool pages, pending transaction lifecycle, pending transparent address/outpoint overlays, and "mempool in sync" status | Native `WalletQuery` or replayable `zinder-explorer` views; full explorer parity also needs transparent history and balance |
| Zebra | Upstream node source, not a Zinder client | Keeps wallet and explorer indexing outside the node while reusing Zebra's verified mempool observations | `zinder-source` consumes Zebra `MempoolChange` when available, or falls back to `getrawmempool` polling |

Three architectural consequences follow from that map:

- The canonical path is `NodeSource -> MempoolSourceEvent -> MempoolIndex + durable mempool event history -> WalletQuery -> adapters`. Compatibility methods translate over that path; they do not own their own mempool cache.
- Source observations must become hydrated `MempoolEntry` records before they reach public APIs. Zebra's streaming mempool event carries transaction hash and auth digest, so raw transaction fetching and compact-transaction construction belong in the source/ingest path, not in `zinder-compat-lightwalletd`.
- The native mempool surface does not inherit lightwalletd's stream-close lifecycle. `MempoolEvents` stream end means disconnect or shutdown. Chain-tip changes are delivered through `ChainEvents`.
- The public server-observed type is `MempoolEntry`, not `PendingTransaction`. A pending transaction is a wallet-local UX state: it can include a transaction that was created locally but never accepted by the network.
- Product readiness claims are boundary-specific. Native method coverage proves an adapter can be written, not that a wallet integration has landed. Lightwalletd readiness requires compatible methods plus SDK or app validation, while explorer readiness requires the mempool surface plus the transparent history and balance surfaces needed for address-oriented views.

The native protocol exposes two complementary mempool methods:

- **`WalletQuery.MempoolSnapshot`** returns a bounded, pageable point-in-time view of the live mempool index, bound to the visible `ChainEpoch` at call time. The response's certified `source_tip` exactly equals `chain_view.chain_epoch.visible_tip` by height and hash. While canonical ingest and the live index move to a new tip, the writer waits up to five seconds for the replacement source generation and then answers at the new epoch. It never returns a stale or misleadingly empty answer: a view still not coherent with the canonical fence at that bound returns `CHAIN_EPOCH_PIN_UNAVAILABLE`, and a view with no certified generation yet, or one that is rebuilding, returns `UNAVAILABLE`. The response also carries `snapshot_age_millis`, measured from certification of the current source generation even when the certified mempool is empty, so clients with strict freshness needs can choose to subscribe to `MempoolEvents` when the age exceeds a threshold. It also carries `events_resume_cursor`, an opaque `MempoolEvents` `after_cursor` value anchored at the last mempool event the writer had applied when the walk began ([ADR-0027](../adrs/0027-event-stream-start-positions.md)). The resume cursor is identical on every page of one paged walk and empty when the writer had applied no event yet (consumers then subscribe with `earliest_retained`). Replaying from it is at-least-once; consumers apply events idempotently. Paging uses the standard opaque, HMAC-authenticated `StreamCursorTokenV1` under its `SnapshotPage` family (offset-49 nibble `0x5`): the next-page `bytes` carry the walk's anchor event position and the last yielded transaction id. A tampered cursor returns `SNAPSHOT_PAGE_CURSOR_INVALID`; a cursor anchored ahead of the mempool-event sequence the writer has applied returns `SNAPSHOT_PAGE_CURSOR_EXPIRED`. There is no separate snapshot-cursor codec.
- **`WalletQuery.MempoolEvents`** is a server-streaming subscription that mirrors Zebra's `MempoolChange` semantics: typed `Added`, `Invalidated`, `Mined` envelopes with cursor-resume via the `StreamCursorTokenV1` mempool-event family. The request carries the same required `EventStreamStart start` oneof as `ChainEvents`; `live_tail` resolves to the newest retained envelope's cursor at subscribe time.

`Invalidated` is not optional. If the polling backend observes a txid disappear
from `getrawmempool` without a corresponding block commit, it emits
`Invalidated { reason: Unknown }` or a more specific reason when the source can
prove one. Silently dropping a txid would make the mempool cache insert-only and
break rebroadcast and pending-transaction views.

Mempool retention is two-tier (60 minutes mined / 24 hours invalidated by default, both configurable). Expired cursors return `MempoolEventCursorExpired` with `oldest_retained_sequence` in `PreconditionFailure` detail.

### Mempool Point Lookups

`MempoolSnapshot` is the bootstrap and bounded-enumeration surface, and
`MempoolEvents` is the live follow. The one focused point lookup is
`WalletQuery.Transaction`. It returns the typed `TransactionStatusResponse`
carrying the shared `TransactionLocation` oneof (`mined`/`in_mempool`); a
proven absence is gRPC `NOT_FOUND` with reason `TRANSACTION_NOT_FOUND` (see
[Transaction absence](#transaction-absence)). Its mempool half reads the
writer-owned `MempoolIndex` through the `IngestControl` proxy path settled by
[ADR-0007](../adrs/0007-mempool-topology-and-retention.md); `zinder-query` does
not open a second mempool source or reconstruct a local live index.

There are no transparent by-address or by-outpoint mempool lookups. Every
`MempoolEntry` in the snapshot and in `MempoolEvents` carries its
`transparent_outputs` and `transparent_spends`, so a client that watches
transparent addresses filters those entries locally. Clients scan mempool
compact-transaction payloads locally for shielded interest as well, because the
privacy boundary forbids by-address shielded queries.

`MempoolMinedEvent` carries `transaction_id`, `mined_height`, and `block_hash`.
Block hash is source-driven enrichment: the source backends extract the mined
block hash from the upstream node's observation (Zebra streaming
`MempoolChange::Mined` plus `getrawtransaction`, JSON-RPC polling
`getrawtransaction`'s `blockhash` field) so the live mempool owner publishes
authoritative bytes without a chain-store-not-yet-caught-up race. Consumers
that track a pending transaction through mining receive the full mined block
identity in one cursor delivery.

The lightwalletd compat shim maps `GetMempoolStream` and `GetMempoolTx` over
`MempoolEvents` and `MempoolSnapshot` when the adapter is configured with the
mempool surface. `GetMempoolStream` streams the current snapshot walk's
contents first, then subscribes `MempoolEvents` with
`after_cursor = events_resume_cursor` for live delivery; the composition is
at-least-once, matching lightwalletd-go semantics. The shim also races the
stream against retained `ChainEvents` after the first page's `chain_epoch.id`;
a change already observed during stream startup closes the stream immediately
instead of being discarded. A compatibility runtime without that concrete
surface returns a typed unavailable response from the lightwalletd methods.
It does not publish or interpret the native Wallet capability descriptor;
native discovery remains owned by `zinder-query`.

## Transparent Address Outputs

Transparent-address output queries are not shielded scanning. They reveal
transparent addresses that are already public on chain, but they still must be
served from epoch-bound indexed artifacts. `GetAddressUtxos` and
`GetAddressUtxosStream` map over
`WalletQueryApi::transparent_address_unspent_outputs`,
backed by the canonical transparent address output and transparent spend artifact
families in `zinder-store`.

The compatibility shim must not answer these methods by scanning compact blocks
on demand, calling upstream nodes, returning synthetic empty results for unknown
indexed state, or materializing an unbounded address result before truncating
it. Missing indexed state is a readiness or artifact-availability failure; it
is not a reason to bypass the wallet data plane.

`GetLightdInfo.taddr_support` is derived once by the admitted compatibility
constructor. It is `true` only when the canonical pair retains transaction
blobs with `raw_blob_policy = "transactions"` or `"all"` and the exact
wallet-serving pair provides the transparent reads. A `none` composition fails
before the compatibility listener binds; later pair lag or provider outage
drains readiness without rewriting the immutable claim. Transparent output
reads themselves remain available from canonical artifacts when raw payload
retention is `none`, but lightwalletd transparent transaction history returns
raw transaction bytes and therefore requires `transactions` or `all`. The flag
is a product contract for lightwalletd clients, not a way to silence Android
SDK logs.

The remaining `GetLightdInfo` and `GetLatestBlock` fields, and which of them
are comparable with another lightwalletd server, are defined in
[Lightwalletd compatibility §Identity and live-tip responses](../reference/lightwalletd-compatibility.md#identity-and-live-tip-responses).

The native `WalletQuery` proto exposes the same artifact-backed read through
one server-streaming RPC, `TransparentAddressUnspentOutputs`, which streams
`TransparentUnspentOutputsChunk` messages. The server walks the address-output
projection once at a single pinned chain epoch and emits the `ChainView` as one
leading header message (`chunk.body.header`), then one `TransparentUnspentOutput`
per unspent output (`chunk.body.item`); `start_height` is the wallet-birthday
floor. The single header makes the stream-wide single-epoch guarantee
structural: the epoch is carried once, never repeated on every item. There is
no cursor and no entry cap on the wire: a stream that is always complete cannot
be truncated by a client that ignores pagination. The request consumes the shared `AddressLookup` oneof
that accepts either a 32-byte `script_hash` (typed clients) or a base58
transparent `address` (CLI, tests, debug callers); the native adapter parses
string addresses through `ZebraTransparentAddress`, validates the network,
and SHA-256-hashes the `scriptPubKey` before any in-process call. The request
also carries the standard `optional uint64 at_epoch_id`: absent resolves
against the live visible tip; present pins the unspent read to a specific
chain epoch, so the stream is snapshot-consistent with the other canonical
reads. The Rust `ChainIndex` trait carries the same surface:
`transparent_address_unspent_outputs(query)` keyed by the typed
`TransparentAddressScriptHash`, with `query.at_epoch_id:
Option<ChainEpochId>` threading the pin. The release `zinder-query` composition
advertises `wallet.address.transparent_unspent_outputs_v1`: its admitted wallet
projection serves the complete ascending set from the same immutable
`WalletServingReadPair` as the canonical epoch header. The current query
materializes that complete set before gRPC emission; end-to-end incremental
delivery remains tracked in
[#62](https://github.com/ZcashFoundation/zinder/issues/62). Compatibility
runtimes own their separate lightwalletd admission and resource bounds.

`TransparentAddressTxIdsInRange` streams `TransparentAddressTxIdsChunk`
messages with the same one-shot header shape: one leading `ChainView` header
(`chunk.body.header`), then one `TransparentAddressTxId` per indexed
transaction (`chunk.body.item`). The chain epoch is carried once on the header,
not on every item.

Cursor cadence on the tx-history streaming surface: the wire emits the resume
cursor only on the terminal item of a server stream. Non-terminal
`TransparentAddressTxId` items carry empty `cursor` bytes; the last item
carries either the next-page cursor (when more entries may be available) or
empty bytes (stream fully drained). Clients that lose the connection mid-stream
must restart the page rather than resume from a non-terminal item; bounded
history pages keep that restart cost small.

Operators must publish only a `zinder-compat-lightwalletd` process whose
constructor admitted `taddr_support=true` from transaction retention and the
exact transparent serving pair. It is not an operator opt-in. The
wallet-serving coverage profile (`ingest.run_overrides.coverage =
"wallet-serving"` or `zinder-ingest --wallet-serving`) is the supported way to
select transaction retention and complete non-genesis canonical history. A
recent-checkpoint or tip-bootstrapped store may have the address-output index
family enabled but still lack the historical rows needed by wallet birthdays and
resync anchors; that deployment posture is not wallet-serving.

`GetAddressUtxos.maxEntries` is an aggregate response budget across the
requested address set. The compatibility adapter may make several internal
artifact reads to satisfy the request, but the public response and stream must
be deterministically ordered and capped as one result set. `maxEntries = 0`
means every matching output, as in lightwalletd. A response that would exceed
the adapter's configured bound fails with `RESOURCE_EXHAUSTED` instead of
dropping rows; see
[Transparent address UTXOs](../reference/lightwalletd-compatibility.md#transparent-address-utxos).

## Transparent Address Tx History

Transaction-history reads return the txids that touch a given transparent
address within an inclusive height range. The native surface is
`WalletQuery.TransparentAddressTxIdsInRange`, a server-streamed page-bounded
read; the matching Rust API is
`ChainIndex::transparent_address_tx_ids_in_range`. Its `at_epoch_id` is a
client-side response expectation rather than a wire field. The client rejects
a mandatory stream header from another epoch with
`ChainEpochPinUnavailable`; snapshots set the expectation from their captured
epoch, including for empty pages.

The release query advertises `wallet.address.transparent_history_v1` for its
ascending, page-bounded wallet-projection read. Its opaque cursor carries the
exact `WalletCanonicalSourceIdentity` that issued the page, so pair replacement
fails with `FAILED_PRECONDITION`. History is ascending only: the request has no
direction field, and field 6, which once carried a newest-first flag, is
reserved. Newest-first address history is `ExplorerQuery.TransparentAddressActivity`.
Only the terminal chunk carries a non-empty resume cursor.

The compatibility adapter implements `GetTaddressTxids` and
`GetTaddressTransactions` through `WalletServingQuery`. That path reads
the transparent history already present in the admitted wallet projection,
alongside the canonical half of the same `WalletServingReadPair`; it does not
depend on the optional materialized-view store. Its ascending page cursor binds
the wallet row key to the exact `WalletCanonicalSourceIdentity` that issued the
page. If the publisher installs another serving pair between pages, resumption
fails with `FAILED_PRECONDITION` instead of combining rows from two sources.
An absent, zero, or above-tip `range.end` reads to the visible tip; see
[Transparent address history](../reference/lightwalletd-compatibility.md#transparent-address-history).

The full projection path is the canonical worked example in
[Extending artifacts §A worked example: transparent address transaction history](extending-artifacts.md#a-worked-example-transparent-address-transaction-history).

## Transparent Outpoint Spend Status

`WalletQuery.TransparentOutpointSpendStatus` answers, for each requested
transparent outpoint, whether a mined transaction spends it. A wallet uses it
to resolve the spends of outputs it already tracks in one request per batch,
without downloading each output's address history. Zallet's `spend-index`
chain contract (`ChainView::outpoint_spend_status`) and librustzcash's
`TransactionDataRequest::GetSpendingTx` are the consumers; see
[ADR-0043](../adrs/0043-owned-transparent-wallet-query-surface.md).

The request reveals which outpoints the wallet tracks. The wallet projection
stores each output with its address, so the server also learns the address
that owns every found outpoint. That is the same set of addresses an
address-history scan reveals, plus the exact outputs, so the method is not a
privacy improvement over the scan.

The request carries 1 to 1,024 outpoints and the standard
`optional uint64 at_epoch_id`. The response carries the `ChainView` it was
read at and one `TransparentOutpointSpendStatusEntry` per requested outpoint,
in request order and including duplicates. Each entry's `status` oneof is one
of:

| Status | Meaning |
| --- | --- |
| `unspent` | A mined transaction created the output, and no mined transaction spends it. |
| `spent_by` | A mined transaction spends the output. The entry names the `spending_transaction_id`, the `input_index` within it, and the `block` that mined it. |
| `not_found` | No mined transaction created an output at this outpoint. This includes the coinbase input sentinel and the genesis coinbase output, which is unspendable and never indexed. |

The status covers mined transactions from height 1 through the epoch's visible
tip. A spend that is only in the mempool leaves the output `unspent`; pending
spends arrive through `MempoolSnapshot` and `MempoolEvents` on each entry's
`transparent_spends`. Zinder never reports a spent output without its spender,
because the wallet projection stores the spending input in the same row that
records the spend.

The release query answers from the wallet projection's
`transparent_unspent_output` and `transparent_spent_output` families. Each
request captures one `WalletServingReadPair`. Its wallet reader is frozen at
the same source identity as its canonical reader, so both families answer at
the pair's epoch. A pin naming any other epoch fails with
`FAILED_PRECONDITION` and `CHAIN_EPOCH_PIN_UNAVAILABLE`, as every other pinned
wallet read does; the query never combines families from two epochs. The
projector moves an output into the spent family in the transition that mines
its spend and moves it back only when a reorg reverts that block, so an
outpoint is in at most one family.

The projection covers exactly the canonical history the pair retains, so
`not_found` is a proven absence only on a store whose history starts at height
1, as a wallet-serving store's does. On a store whose history starts after a
checkpoint, a request that misses any outpoint fails with
`FAILED_PRECONDITION` and `CANONICAL_HISTORY_UNAVAILABLE`, carrying
`metadata["first_available_height"]`, as a transaction lookup does. A request
whose outpoints are all found still answers there.

A request with no outpoints or more than 1,024 fails with `INVALID_ARGUMENT`
and `TRANSPARENT_OUTPOINT_COUNT_OUT_OF_RANGE` before any storage read.
`zinder-client` hides the cap: `ChainSnapshot::transparent_outpoint_spend_status`
and `ChainIndex::transparent_outpoint_spend_status` split a longer slice into
requests of at most `MAX_OUTPOINTS_PER_SPEND_STATUS_REQUEST` outpoints and pin
every later request to the first response's epoch. The release query
advertises `wallet.read.transparent_outpoint_spend_status_v1`.

## Transparent Surface Ownership

The native transparent surface contains only the methods a packaged consumer
calls. [ADR-0043](../adrs/0043-owned-transparent-wallet-query-surface.md)
records the consumer call sites, the serving reader for each method, and the
mapping for removed methods.

| Method | Capability | Owning consumers | Serving reader |
| --- | --- | --- | --- |
| `TransparentAddressUnspentOutputs` | `wallet.address.transparent_unspent_outputs_v1` | Zallet, Zally, Zpay through Zally; ZODL through `GetAddressUtxos` in the compatibility runtime | Wallet projection unspent rows |
| `TransparentAddressTxIdsInRange` | `wallet.address.transparent_history_v1` | Zallet; ZODL through `GetTaddressTxids` in the compatibility runtime | Wallet projection history rows |
| `TransparentOutpointSpendStatus` | `wallet.read.transparent_outpoint_spend_status_v1` | Zallet `spend-index` contract; librustzcash `GetSpendingTx` | Wallet projection `transparent_unspent_output` and `transparent_spent_output` families |

Prevout resolution, mempool spend lookups by outpoint, the UTXO-set summary,
and the native transparent balance are not part of the protocol. Wallet spend
status comes from the wallet projection. The `transparent_outpoint_spend`
Explorer view stays the source of `TransactionDetail.spent_by` for
`ExplorerQuery.TransactionDetail`
([ADR-0029](../adrs/0029-durable-transparent-outpoint-spend-projection.md)).

## Chain Value Pools

The native schema contains `WalletQuery.ChainValuePoolsAtTip(ChainValuePoolsAtTipRequest) returns (ChainValuePoolsAtTipResponse)`, but the release composition does not advertise `wallet.read.chain_value_pools_at_tip_v1`.

The response binds to the query's admitted visible `ChainEpoch`, carries `source_tip: BlockTip`, and preserves `repeated ChainValuePool pools` in upstream order. `source_tip` is the `blocks` plus `bestblockhash` pair returned by the same `getblockchaininfo` response as the pool totals. A caller accepting the snapshot as canonical compares both its height and hash with the intended canonical tip; height equality alone is not sufficient across a reorg. Each pool entry carries `id`, `monitored`, and optional `chain_value_zat`. The list-shaped contract is intentional: consumers can render known pools by id without forcing Zinder to drop or rename future consensus pools.

OpenRPC method-name discovery does not prove that `getblockchaininfo` returns
the required `valuePools` payload, and the current readiness loop does not
retain a semantic value-pool probe. A later explorer slice may admit the method
only after a successful startup value-pool read on the exact installed source
and a matching retained readiness check. Until then, direct native calls fail
with `FAILED_PRECONDITION` and `ENDPOINT_CAPABILITY_UNAVAILABLE` before node
I/O.

This live snapshot is a prerequisite fact, not a value-pool history or value-flow projection. Historical pool totals and per-period movements remain separate future consumers with independent coverage and replay semantics.

## Transparent Address Balance

The native endpoint does not serve a transparent balance. Native wallets sum
the `TransparentAddressUnspentOutputs` they already read to spend, and
integrators that need balance, received, and sent totals use
`ExplorerQuery.TransparentAddressActivity.summary`. The lightwalletd
compatibility runtime answers `GetTaddressBalance` and
`GetTaddressBalanceStream` from the in-process
`WalletQueryApi::transparent_address_balance` domain method. That method sums
the wallet projection's confirmed unspent outputs for up to 256 addresses
(`MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES`) at one pinned chain epoch. An
over-cap list is rejected with `INVALID_ARGUMENT` carrying
`TRANSPARENT_BALANCE_ADDRESS_COUNT_EXCEEDED`, and an empty list is rejected
with `INVALID_ARGUMENT`.

The compatibility runtime reports the confirmed total as the lightwalletd
`int64 value_zat`. A total above `i64::MAX` is rejected with `OUT_OF_RANGE`.
Live mempool state is not part of the balance. `GetTaddressBalanceStream`
collects the streamed addresses and returns the same value as the unary call.

## Capability Discovery

`WalletQuery.ServerInfo` returns a `ServerCapabilities` descriptor per [Public interfaces §Capability Discovery](public-interfaces.md#capability-discovery). Capability strings are exact-match; clients gate features on capability strings such as `wallet.events.chain_v1` rather than on Zinder version. The admitted query derives one immutable set from persisted storage evidence, implemented methods, and the exact probed providers installed in the composition. The native descriptor and operations endpoint share that exact immutable set without operator overrides or a copied support list. The descriptor's `node` field reports the upstream-node probe snapshot; an adapter constructed without source-probe results leaves it empty.

## Native Rust API

Rust integrations can call the remote-first `zinder-client` surface per [Public interfaces §Rust API Shape](public-interfaces.md#rust-api-shape). The `ChainIndex` trait exposes immutable network metadata and canonical reads with typed Rust values (`BlockHeight`, `ChainEpoch`, `TxStatus`, and `IndexerError`); `RemoteChainIndex` also implements `EndpointBackedIndex` for broadcast, subscriptions, and live state. Zinder's serving runtimes compose service-internal reads through `WalletServingQuery` and an admitted `WalletServingReadPair`, while consumers with dependency or toolchain conflicts can generate stubs from the native `WalletQuery` protocol instead of linking the SDK.

### Event stream progression

`RemoteChainIndex::chain_events_for_family` and `RemoteChainIndex::mempool_events` apply the same progression checks to the stream they return, so a remote cannot make a consumer apply duplicated or reordered state changes. Each stream keeps its own state, and the chain and mempool sequence spaces never mix.

- Every envelope must carry a non-empty cursor and an `event_sequence` greater than zero.
- `event_sequence` must strictly increase. Gaps are valid because retention pruning leaves them; a repeated or decreasing value fails the stream.
- A cursor may not repeat anywhere in the stream, not only on adjacent events.
- The client retains accepted cursors to detect repeats, bounded to 65,536 cursors and 8 MiB of cursor bytes per stream. Reaching either bound fails the stream with `MalformedResponse` before the next event is accepted. The consumer reconnects with `EventStreamStart::AfterCursor` from the last accepted cursor, which starts with fresh progression state.
- A failing event is never yielded. `MalformedResponse` names the offending field (`event_sequence` and `cursor` for chain events, `mempool_event_envelope.event_sequence` and `mempool_event_envelope.cursor` for mempool events).

These checks are client-side only. Server-side cursor validation, snapshot-to-stream handoff through `events_resume_cursor`, and resume semantics are unchanged. Mempool resume is at-least-once, so a resumed stream restarts progression state and can re-deliver events after the resume cursor.

The `ChainIndex` trait does not duplicate the `WalletQueryApi` Rust trait inside `services/zinder-query`. They serve different boundaries: `WalletQueryApi` is the gRPC server's internal trait; `ChainIndex` is the public Rust API for consumers. Both share types from `zinder-core`. A compatibility test asserts that every advertised `ServerCapabilities` capability string has a corresponding `ChainIndex` method.

## Transaction Broadcast

Transaction broadcast is a network operation, not a canonical chain commit.

`zinder-query` may expose transaction broadcast because wallets expect a single endpoint. This is wallet operation parity, not part of the minimum read-only shielded sync surface. The broadcast path must:

- Forward the raw transaction to a configured network or upstream node path.
- Advertise `wallet.broadcast.transaction_v1` only after the configured source probe reports `transaction_broadcast`.
- Return `TransactionBroadcastOutcome` with accepted, duplicate, invalid-encoding, queued, rejected, or unknown outcomes.
- Avoid writing canonical chain state.
- Rely on ingestion to observe the transaction later in mempool or block data.

`zinder-source` owns source-specific broadcast I/O through `TransactionBroadcaster`. `zinder-query` may delegate to that boundary, but it must not embed Zebra or zcashd JSON-RPC behavior in query logic.

`zinder-compat-lightwalletd` exposes the same broadcast path as the
lightwalletd-compatible `SendTransaction`. The adapter only carries raw
transaction bytes through `WalletQueryApi::broadcast_transaction` and maps
typed outcomes to `lightwalletd::SendResponse` error codes. No viewing keys,
spending keys, or scanning material cross the Zinder boundary on this path; the
privacy contract from the read-sync surface applies unchanged.

Regtest can prove forwarding, typed rejection mapping, and the no-storage-mutation boundary. Testnet validation is required before Zinder promises public relay, stable fee-policy taxonomy, rebroadcast behavior, or accepted-transaction propagation semantics.

## Compatibility

`zinder-compat-lightwalletd` serves the vendored `CompactTxStreamer` proto from `zinder_proto::compat::lightwalletd` by translating `WalletQueryApi`. The full responsibility list, allowed request shapes, error mapping, and test surface are owned by [Protocol boundary §Lightwalletd Compatibility](protocol-boundary.md#lightwalletd-compatibility); native `WalletQueryApi` remains the primary API and new functionality lands there first.

v1 wallet APIs target self-hosted, single-operator deployments backed by a configured upstream node. The v1 binaries do not implement TLS termination, authentication, rate limiting, or quota accounting; an operator who needs any of those terminates them at a load balancer or reverse proxy in front of Zinder. Public-internet hosting requirements are out of v1 scope.

### External Wallet Compatibility Claims

This section is the canonical Zinder contract for external wallet serving
claims. Per [ADR-0005](../adrs/0005-consumer-neutral-wallet-data-plane.md),
the contract is consumer-neutral: client-specific validation belongs in test
evidence, while this document owns the durable serving criteria.

Zinder compatibility claims use these levels:

| Level | Meaning | Required evidence |
| --- | --- | --- |
| `protocol-compatible` | Zinder builds and serves the pinned `lightwallet-protocol` messages without local schema drift. | Vendored proto drift check, generated client smoke tests, and protocol field golden tests. |
| `reference-parity-compatible` | Zinder behavior matches reference `lightwalletd` for the claimed RPC set, with documented allow-listed operator differences. | Live parity suite against pinned reference inputs. |
| `client-compatible` | Real lightwalletd-compatible wallet clients can create, restore, resync, send, and observe mempool behavior through Zinder. | Android SDK and Zodl evidence for the claimed network and complete wallet-serving coverage. |
| `public-operator-compatible` | A public deployment can safely expose the compatible endpoint. | TLS/proxy, bind, rate-limit, redaction, readiness, metrics, and operational runbook evidence. |

A release or deployment may advertise only the highest level whose required
evidence is current. Do not use unqualified "full compatibility" or "drop-in
replacement" language without naming the protocol pin, claimed RPC set, and
certification level.

A deployment may claim Android SDK or Zodl compatibility only when:

- The serving store contains the subtree-root history required by fresh wallet
  bootstrap and tree-state history for every anchor height a supported wallet
  flow can request, including create, resync, and restore/import flows. Use
  `zinder-ingest --wallet-serving` (or `ingest.run_overrides.coverage = "wallet-serving"`)
  for this serving profile; recent checkpoints are validation fixtures, not
  wallet-serving stores.
- The transparent output surface in [§Transparent Address Outputs](#transparent-address-outputs)
  is implemented end-to-end.
- The transport requirement in [Service operations](service-operations.md#deployment-guidance)
  is satisfied for real Zodl endpoint tests.

The upstream Go `lightwalletd/testclient` remains smoke coverage for the basic
compat surface. It is not a substitute for an Android SDK or Zodl bootstrap
test when the release claim names those clients.

A deployment may claim compatibility with the current Android SDK or Zodl
mempool flow only when `GetMempoolStream` is mapped over the native mempool
index and event log, and the exact SDK or app observes its submitted raw
transaction through that stream before mining. A sync-only Zodl proof does not
establish pending-transaction UX.

`GetMempoolTx` remains part of Zinder's broader lightwalletd-compatible surface,
but the current SDK does not call it. Claims for that method therefore require
separate protocol or consumer evidence and must not be presented as current
Zodl certification.

## Query Consistency

Wallet sync APIs must read from one `ChainEpoch`. Primary in-process reads may also be backed by one RocksDB snapshot; secondary reads are snapshotless and rely on epoch-bound visibility retention per [ADR-0003](../adrs/0003-canonical-storage-access-boundary.md).

Rust consumers capture that boundary with `ChainSnapshot` for a borrowed
handle or `OwnedChainSnapshot` for an `Arc`-owned, cloneable chain view. Every
pinnable canonical method on either view sends `Some(captured_epoch.id)`,
including transaction lookup; snapshot transaction reads therefore never fall
back to the live mempool. The snapshot types do not wrap current-only address
history or balance reads or any `EndpointBackedIndex` operation.

For example, a compact block range response should bind:

- Start height.
- End height.
- Tip hash.
- Tip height.
- Settled tip height.
- Artifact schema version.
- Subtree-root range or cursor when subtree data is returned.

If the chain tip changes while the request is executing, the response should still finish from the epoch it started with or restart from a new epoch. It should not mix both.

The production `WalletServingQuery` publishes one current exact read pair and
does not promise historical pair retention. After publication advances, an old
snapshot receives `CHAIN_EPOCH_PIN_UNAVAILABLE` and captures a new snapshot;
it must not silently read replacement-branch artifacts.

## Performance and Pagination

`lightwalletd` compatibility is the floor for wallet-sync performance, not the target. Zinder publishes P50, P99, and worst-case budgets for hot wallet endpoints and gates `GetBlockRange`-equivalent behavior in CI.

### Enforced Regression Budgets

The `ci-perf` profile runs deterministic regression checks from
`services/zinder-query/tests/perf/perf_smoke.rs`. A 1,000-block compact range
must complete within 2 seconds, a 1,000-block full-block stream within 5
seconds, and a visible-tip read within 250 milliseconds. The full-block stream
also caps its channel at 4 chunks, so peak buffering cannot scale with the
requested range. These are deliberately generous CI ceilings, not percentile
claims about production hardware.

Live latency and mainnet catch-up tests measure real Zebra and RocksDB behavior
without publishing machine-specific observations as durable architecture.
Operators collect current P50, P99, worst-case, readiness, source-RPC, store,
and RocksDB evidence through the commands in the testing runbook before making
a release-performance claim.

Every range or list endpoint defines:

- A maximum response size.
- A cursor or explicit closed range.
- Stable ordering.
- The epoch or source timestamp that bounds the response.

The release native compact-block range API rejects requests above the fixed
1,000-block `DEFAULT_MAX_COMPACT_BLOCK_RANGE` before opening a reader.
Visible-tip reads use the epoch-bound pair directly. Tree-state reads first use
the same pinned pair, then fill a missing exact-height checkpoint through the
admitted upstream tree-state provider for the already-resolved canonical block.
Batched storage reads remain bounded by the range limit; native gRPC streams
one compact block per message instead of packing the whole range into a single
response.

The lightwalletd compatibility adapter must also keep unbounded upstream
semantics bounded at Zinder's boundary. For example, `GetSubtreeRoots` treats
`maxEntries = 0` as a bounded compatibility request, not permission to
materialize every retained subtree root.
`GetBlockRange` consumes the bounded native range result and streams decoded
lightwalletd blocks without a second full-range allocation. True per-item store
back-pressure requires an owned snapshot streaming read API; until then the
range cap is the memory bound.
`GetAddressUtxos` and `GetAddressUtxosStream` are not bounded at the read yet.
They read each address through `TransparentAddressUnspentOutputs`, which
returns the complete unspent set in memory with no row bound, and then apply
`maxEntries` and the response bound. The bound limits the response size but
not the read. A paged unspent-output read that stops at the response bound is
follow-up work.

Every other compatibility read keeps the bound at the read: it does not
materialize an unbounded list and truncate it after the fact, and its storage
and protocol signatures make that shape impossible.
