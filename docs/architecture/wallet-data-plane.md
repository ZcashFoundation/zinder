# Wallet data plane

This page describes what the native `zinder.v1.wallet.WalletQuery` gRPC service returns and the rules a client can rely on: which reads agree with each other, how ranges and pages are bounded, and which errors to handle. It is for developers who build a wallet backend or SDK against `zinder-query`. Clients of the lightwalletd protocol use `zinder-compat-lightwalletd` instead, described in [Lightwalletd compatibility](../reference/lightwalletd-compatibility.md).

Zinder serves public chain data. Keys, trial decryption, note and account state, and transaction building stay in the wallet, as [Overview](overview.md) explains. The reasons for this API shape are recorded in [ADR-0014](../adrs/0014-consumer-neutral-wallet-data-plane.md) and [ADR-0015](../adrs/0015-wallet-runtime-composition.md). Capability strings are listed in [Capabilities](../reference/capabilities.md), and error reasons with their retry rules in [Error vocabulary](../reference/error-vocabulary.md).

## How reads stay consistent

Zinder publishes the chain as a sequence of numbered, immutable snapshots called chain epochs. Each time ingest commits new blocks or a reorg, it creates a new epoch, and `zinder-query` starts serving it. An epoch names one visible tip, one settled tip, and the stored data that belongs to that chain.

Each stored chain lookup answers from exactly one epoch. A unary response carries it in `chain_view.chain_epoch`. A range or transparent-address stream sends it once in a leading header message. `FullBlocksInRange` repeats it on every chunk. One response therefore never mixes data from two branches of a reorg.

To make several reads agree, pin them to one epoch:

1. Make the first read without `at_epoch_id`. The server answers from the current epoch.
2. Copy `chain_view.chain_epoch.id` from that response.
3. Send it as `at_epoch_id` on every later read in the same workflow.

A pinned read answers from that epoch or fails. It never answers from a newer chain instead.

The settled tip is the height at or below which ingest never replaces a block. The gap between the visible and settled tips is the reorg window, reported as `reorg_window_blocks` in `ServerInfo` (1,000 blocks by default). Wallets scan to the visible tip and use the settled tip for decisions that must not be reversed.

### How long an epoch stays readable

A replaced epoch that a response exposed stays readable for one minute after a newer one replaces it, however many blocks arrive in that minute. An epoch that no response exposed has nothing to pin, so the server releases it as soon as a newer one replaces it. `GetLatestBlock` and `GetLightdInfo` on the compatibility listener expose no epoch, so polling them never extends retention. A workflow that finishes within that minute completes across new blocks. After it, a pinned read fails with `FAILED_PRECONDITION` and `CHAIN_EPOCH_PIN_UNAVAILABLE`. The client takes a new snapshot and restarts the whole workflow, not only the failed read. `zinder-client` reports this as `IndexerError::ChainEpochPinUnavailable` with `RetryPolicy::RefreshChainEpoch`.

A retained epoch still answers after a reorg replaces its blocks, because a pin promises that epoch's view, not the current best chain.

`TransparentAddressUnspentOutputs`, `CompactBlocksInRange`, and `FullBlocksInRange` hold their epoch across many storage reads. If one of these streams is still running 30 seconds after its epoch leaves retention, the server ends it with `CHAIN_EPOCH_PIN_UNAVAILABLE` after the items it already sent. Those items are not a complete answer. Discard them and read again at a newer epoch. Every other read is one bounded storage read and finishes on its own.

A reader keeps at most three replaced epochs open. While retained epochs and slow streams hold all of them, it postpones publishing new blocks. Together they delay a new block by at most about 90 seconds. A burst of blocks that no workflow reads by epoch does not wait on retention.

## Methods

The service advertises a method's capability string only when this deployment can answer it. Call `ServerInfo` and check the capability before you rely on a method. A method that is not advertised fails with `FAILED_PRECONDITION` and `ENDPOINT_CAPABILITY_UNAVAILABLE` before it reads anything.

| Method | Returns | Advertised when |
| --- | --- | --- |
| `VisibleTipBlock`, `SettledTipBlock` | Block identity at the visible or settled tip | Always |
| `BlockIdBySelector` | Height and hash for a height or hash selector | Always |
| `BlockHeaderBySelector` | Zinder header read model | Never |
| `CompactBlock`, `CompactBlocksInRange` | Compact blocks | Always |
| `FullBlock`, `FullBlocksInRange` | Consensus-serialized blocks | Ingest retains block blobs (`storage.raw_blob_policy = "all"`) |
| `TreeStateAtHeight`, `LatestTreeStateCheckpoint` | Sapling, Orchard, and Ironwood tree states | Always |
| `SubtreeRoots` | Completed shielded subtree roots | Always |
| `NetworkUpgradeActivations` | Network-upgrade schedule | Always |
| `Transaction` | Mined or mempool status of one transaction | Always; mined bytes need transaction retention |
| `ChainEvents` | Replayable chain events | Always |
| `MempoolSnapshot`, `MempoolEvents` | Live mempool contents and changes | Always |
| `TransparentAddressUnspentOutputs` | Unspent outputs of one transparent address | Always |
| `TransparentAddressTxIdsInRange` | Transaction ids that touch one transparent address | Always |
| `TransparentOutpointSpendStatus` | Spend status of up to 1,024 outpoints | Always |
| `ChainValuePoolsAtTip` | Chain value-pool totals | Never |
| `BroadcastTransaction` | Typed broadcast outcome | A `[node]` endpoint is configured |
| `ServerInfo` | Network, contract revision, capabilities, broadcast node | Always, not gated |

"Always" means in the release `zinder-query` binary, which requires ingest control and a wallet-serving store. `Transaction`, `MempoolSnapshot`, and `MempoolEvents` also need ingest control to serve mempool lookups, which a release `zinder-ingest` always does; [Capabilities](../reference/capabilities.md) lists each condition. Hashes in native messages are 64-character hex strings in RPC byte order. Compact transaction ids are 32 bytes in protocol byte order. [Public interfaces](public-interfaces.md#wire-conventions) owns these conventions.

## Compact blocks

`CompactBlock(height, at_epoch_id)` returns one stored compact block. `CompactBlocksInRange(start_height, end_height, at_epoch_id)` streams an inclusive ascending range of at most 1,000 blocks: first one header message (`body.header`) with the `ChainView` that answers the whole range, then one `CompactBlock` per height (`body.item`). The server reads about 2 MiB of stored blocks at a time and queues at most 16 blocks beyond that read, so a slow client does not make it hold the whole range. A range outside retained history or an unavailable epoch pin fails before the header. One process serves at most 100 of these streams at once and refuses more with `RESOURCE_EXHAUSTED` and `COMPACT_BLOCK_STREAM_LIMIT_REACHED`; retry after a short delay. A stream the client cancels or drops keeps its place until the read it has already started ends.

Ingest builds compact blocks when it commits a block. Query never fetches a block from the node or builds one on demand.

| Condition | Status and reason |
| --- | --- |
| Range wider than 1,000 blocks | `INVALID_ARGUMENT`, `BLOCK_RANGE_TOO_LARGE` |
| Start above end | `INVALID_ARGUMENT`, `INVALID_BLOCK_RANGE` |
| Height above the visible tip or below the retained history | `NOT_FOUND`, `ARTIFACT_UNAVAILABLE` naming the first missing height |
| Block missing inside retained history | `UNAVAILABLE`, `STORAGE_UNAVAILABLE` (store damage) |

Canonical history never contains height 0. A complete store retains blocks from height 1.

`zinder-client` validates the range stream. It fails with `IndexerError::MalformedResponse` when the server sends a block before the header, a second header, no header, a header for another network or another pinned epoch, a skipped, repeated, or extra height, a short stream, or a block above the header's visible tip. The client requires contract revision 8 or later and refuses an older server.

## Full blocks

`FullBlock(height, at_epoch_id)` returns one consensus-serialized block. `FullBlocksInRange(start_height, end_height, at_epoch_id)` streams an inclusive range of at most 1,000 blocks and repeats the `ChainView` on every chunk. The server reads 16 blocks at a time and buffers at most 4 chunks, so a slow client does not make it hold the whole range.

Full blocks exist only when ingest writes raw block segments, which requires `storage.raw_blob_policy = "all"` (`storage.full_blocks = true`). Under `none` the methods are not advertised. A missing block inside an `all` store returns `NOT_FOUND` with `ARTIFACT_UNAVAILABLE`. Changing an existing store to `all` requires a rebuilt store.

## Block identity and tips

`VisibleTipBlock` and `SettledTipBlock` return the block at the epoch's visible or settled tip.

`BlockIdBySelector` resolves a height or a block hash against the best chain of the answering epoch. A hash that is unknown or belongs to a displaced block returns `NOT_FOUND` with `BLOCK_NOT_IN_BEST_CHAIN`. So does a height above the visible tip. A hash never resolves to the replacement block at the same height.

`BlockHeaderBySelector` is in the protocol but never advertised. Its `BlockHeader` message omits the Equihash solution, so it is not a complete header. A client that needs the full header reads a full block and decodes it.

## Tree states and subtree roots

`TreeStateAtHeight(height, at_epoch_id)` returns the Sapling, Orchard, and Ironwood note commitment tree states after the block at exactly `height`. The response height always equals the requested height. The server never asks the node.

A wallet-serving first sync stores a tree-state checkpoint at every height by default, so reads at every constructed height use a stored checkpoint as soon as the store reaches READY. Explorer and `explicit` coverage default to sparse construction, which stores one every 100 blocks and at the build tip. Live following stores one per block, and the background pass fills missing settled checkpoints according to the selected policy. [Chain ingestion](chain-ingestion.md#tree-state-densification) describes the settings and tradeoff.

A height with a checkpoint is read directly: the stored frontier bytes are served as written, with only their framing checked, no hashing, and the stored roots left unread. Every checkpoint is validated when it is written: each frontier is a canonical encoding, derives its stored root, and belongs to the pool it is stored for. The bytes and root cannot show that last binding on their own, because every empty tree encodes as the same bytes and Orchard and Ironwood trees share a hash function. RocksDB block checksums detect corruption at rest, and ingest checks every settled row again in the background, as [Chain ingestion](chain-ingestion.md#tree-state-verification) describes. Any other height is rebuilt from the nearest checkpoint below it and the stored compact blocks in between, which replays at most 99 blocks. A rebuild parses that seed checkpoint in full and re-derives its roots before it appends to it.

Two reads that replay the same number of blocks can use different amounts of CPU. Each shielded note commitment updates its pool's Merkle tree, so blocks with more commitments take more hashing. Before serving the result, the reader decodes each canonical frontier and compares its position, leaf, and other frontier nodes with the tree it just rebuilt. Wallet responses carry only the frontier bytes, so this check requires no final root computation. Concurrent requests for the same height on one reader generation can share this work, including verification of the seed checkpoint. A replacement reader rebuilds against its own storage. The reader can also reuse up to 1,024 settled answers within an 8 MiB budget via a settled-chain LRU cache that survives tip appends with one canonical header check on settled-tip advance and resets on forks, schedule revisions, or retention changes.

The query hands each tree state to its adapters as typed per-pool frontier bytes. Native `TreeStateAtHeight` and `LatestTreeStateCheckpoint` encode them as the `z_gettreestate`-shaped JSON `payload_bytes`, with an entry only for each pool active at that height. The lightwalletd adapter hex-encodes the same bytes into its per-pool strings and returns an empty string for an inactive pool.

| Condition | Status and reason |
| --- | --- |
| Height above the visible tip, or below the parent of the store's first retained block | `NOT_FOUND`, `BLOCK_NOT_IN_BEST_CHAIN` |
| A request needs reconstruction and its admission deadline expires, or every waiting place is taken | `RESOURCE_EXHAUSTED`, `TREE_STATE_REPLAY_LIMIT_REACHED` |
| A seed checkpoint is missing, or replayed history does not link or reach the recorded tree sizes | `UNAVAILABLE`, `STORAGE_UNAVAILABLE` (store damage) |

Stored checkpoints and settled cache hits bypass rebuild admission. Each process has one rebuild slot per available CPU. The default wait for a slot is 2 seconds, with a maximum setting of 5 seconds. The same deadline applies across reader replacements. Set `query.tree_state_replay_wait_ms` for native query or `compat.tree_state_replay_wait_ms` for wallet-serving lightwalletd.

Up to 64 requests can wait per slot by default. A request joining an existing rebuild uses one waiting place, including while the rebuild is running. Set `query.tree_state_replay_waiters_per_slot` or `compat.tree_state_replay_waiters_per_slot` to a value from 1 to 256. A disconnect releases the caller's place. Other callers keep their place in the queue and can still receive the answer. When the last caller leaves, the worker can stop at its next cancellation check. The slot becomes available when the worker exits.

At 30 ms per rebuild, about the 99th percentile measured on Testnet, one slot can process 64 distinct rebuilds in 1.92 seconds. A single rebuild in dense history can exceed the default 2-second wait. Retry a refused request after a short delay. `zinder_query_tree_state_replay_refusals_total` counts refusals by reason.

`LatestTreeStateCheckpoint(at_epoch_id)` returns the newest stored checkpoint at or below the visible tip.

`SubtreeRoots(shielded_protocol, start_index, max_entries, at_epoch_id)` returns completed subtree roots in ascending index order. `max_entries` must be 1 to 1,024. Zero fails with `INVALID_ARGUMENT`, and a larger value with `INVALID_ARGUMENT` and `SUBTREE_ROOT_RANGE_TOO_LARGE`. A start index at or past the number of completed subtrees returns an empty list. A checkpointed store can serve roots from before its checkpoint when it has complete-prefix coverage. Requesting an earlier root that the store lacks returns `NOT_FOUND` with `ARTIFACT_UNAVAILABLE`. See [Schema admission](storage-backend.md#schema-admission). `root_hash` carries raw bytes.

## Network-upgrade schedule

`NetworkUpgradeActivations` returns the consensus branch id, name, and activation height of each network upgrade, in ascending height order. This is the schedule ingest admitted from its node, served with the current epoch. Wallets compare it with their own consensus parameters before they open or migrate wallet state. [ADR-0008](../adrs/0008-consensus-parameters-from-the-node.md) explains where the schedule comes from.

## Transactions and status

`Transaction(transaction_id, at_epoch_id)` returns where a transaction is, as a `TransactionLocation` with one of two arms:

- `mined`: block height, hash, and position; `chain_context` with the consensus branch id, block time, and confirmations computed against the answering epoch's visible tip; and `raw_transaction_bytes` when the server advertises `wallet.read.transaction_bytes_v1`. The server reads retained transaction blobs or retrieves the exact bytes from the verified cold-byte source and checks them against the indexed digest.
- `in_mempool`: the live `MempoolEntry`, including raw bytes and transparent outputs and spends.

Without `at_epoch_id`, the server reads the canonical chain first. On a miss, it asks the writer's live mempool through ingest control, with a 15-second timeout. With `at_epoch_id`, the lookup locates the transaction in that epoch's canonical chain and skips the mempool. To learn whether a transaction is pending, make an unpinned lookup.

### Absence

"Not found" is an answer, and the server returns it only when it can prove it. The response is `NOT_FOUND` with `TRANSACTION_NOT_FOUND` when all of these hold:

- the answering epoch's canonical chain has no record of the transaction, searched from height 1 through its visible tip;
- the store retains complete history from height 1;
- for an unpinned lookup, the writer's mempool, answering at the same epoch, has no record either.

`ErrorInfo.metadata` carries `chain_epoch_id`, `first_covered_height` (always `1`), `visible_tip_height`, and `covers_mempool`. The genesis coinbase at height 0 is never indexed and reads as absent. The mempool half of an unpinned absence is best effort around block boundaries; the canonical half is exact. [ADR-0018](../adrs/0018-transaction-facts-status-and-broadcast.md) records why.

Every other miss is an error. Treat none of these as "the transaction does not exist":

| Condition | Status and reason |
| --- | --- |
| Store history starts after a checkpoint; unpinned lookups reach this only after a mempool miss | `FAILED_PRECONDITION`, `CANONICAL_HISTORY_UNAVAILABLE`, `metadata["first_available_height"]` |
| Pinned epoch no longer served, or the writer's mempool is not yet coherent with its epoch | `FAILED_PRECONDITION`, `CHAIN_EPOCH_PIN_UNAVAILABLE` |
| The mempool answered at a different epoch than the canonical read | `NOT_FOUND`, `CHAIN_EPOCH_MISSING`; retry |
| The writer answered the mempool miss without a typed absence | `NOT_FOUND`, `ARTIFACT_UNAVAILABLE` without `ResourceInfo` |
| Storage fault on a visible transaction | `NOT_FOUND`, `ARTIFACT_UNAVAILABLE` with `ResourceInfo`, or another storage reason |
| Mempool view hydrating, rebuilding, or unreachable | `UNAVAILABLE` |

On a store whose history starts after a checkpoint, an unpinned lookup still reports a pending transaction as `in_mempool`. Only a mempool miss there becomes `CANONICAL_HISTORY_UNAVAILABLE`.

## Chain events

`ChainEvents` streams commits and reorgs with resumable cursors, so a wallet learns about new blocks without polling. Start positions, families, the address filter, retention, and errors are in [Chain events](chain-events.md).

## Mempool

The writer, `zinder-ingest`, owns the live mempool view. `zinder-query` forwards mempool reads to it through ingest control and keeps no mempool copy of its own. [ADR-0012](../adrs/0012-mempool-topology-and-retention.md) describes the topology and retention.

`MempoolSnapshot(max_entries, from_cursor)` returns one page of the current mempool:

- A page holds at most `max_entries` entries (clamped to 1,024; zero means 256) and at most 3.5 MiB (`3,670,016` bytes) of encoded protobuf response, staying within a default 4 MiB gRPC message limit.
- `next_cursor` is set when more entries remain, whether bounded by `max_entries` or by the response byte budget. Pass it as `from_cursor` for the next page; a multi-page walk serves from the point-in-time mempool membership anchored when the walk began (retaining up to 64 concurrent multi-page walks for 30 seconds at the active chain epoch).
- A single mempool entry whose one-entry response exceeds the 3.5 MiB budget fails with `UNAVAILABLE` and `UNSUPPORTED_WALLET_ENCODING`.
- `source_tip` equals `chain_view.chain_epoch.visible_tip` by height and hash. When the chain tip moves, the writer waits up to five seconds for the mempool to catch up. A view still not coherent then fails with `CHAIN_EPOCH_PIN_UNAVAILABLE`. A view with no certified generation yet, or one being rebuilt, fails with `UNAVAILABLE`. The server never returns a stale or falsely empty page.
- `snapshot_age_millis` is the time since the current mempool view was certified.
- `events_resume_cursor` is the same on every page of one walk. Pass it as `after_cursor` to `MempoolEvents` to follow changes after the walk. Replay from it is at-least-once, so apply events idempotently. It is empty when the writer has applied no event yet; then subscribe with `earliest_retained`.
- A tampered page cursor fails with `SNAPSHOT_PAGE_CURSOR_INVALID`. A cursor ahead of the writer's applied events, or one whose pinned multi-page snapshot is no longer retained after the writer's applied events moved on, fails with `SNAPSHOT_PAGE_CURSOR_EXPIRED`.

`MempoolEvents(start)` streams `Added`, `Invalidated`, and `Mined` envelopes with cursors. `start` works as in `ChainEvents`. `Mined` carries the mined height and block hash as the node reported them. A new block keeps the live stream open while the replacement mempool certifies, for up to 30 seconds. If the replacement misses that bound, or its node endpoint becomes inactive, snapshot and transaction reads return `NODE_UNAVAILABLE`. While the writer awaits a certified view, the stream replays retained events and then ends with `UNAVAILABLE` and `NODE_UNAVAILABLE`, so silence never looks like an unchanged mempool. Resubscribe from the last delivered cursor with backoff. Follow `ChainEvents` for tip changes.

Mempool events are retained for 60 minutes after mining and 24 hours after invalidation by default. An expired cursor fails with `MEMPOOL_EVENT_CURSOR_EXPIRED`.

A client that watches transparent addresses filters mempool entries itself, using the `transparent_outputs` and `transparent_spends` each `MempoolEntry` carries. The mempool has no lookup by address or outpoint. Shielded mempool interest is found the same way, by scanning `compact_transaction_data` locally.

## Transparent address data

Transparent addresses and their outputs are public on chain. These reads still reveal which addresses a client cares about, so a privacy-sensitive wallet should send them over a channel it trusts. [ADR-0017](../adrs/0017-transparent-wallet-queries.md) explains which consumers each method serves.

An address is given as an `AddressLookup`: either the 32-byte SHA-256 of its `scriptPubKey`, or the base58 address text. Text longer than 64 bytes, an invalid address, or an address for another network fails with `INVALID_ARGUMENT` and `INVALID_ADDRESS`.

These reads come from the wallet store that `zinder-projector` writes. Its history matches the canonical store's. A wallet-serving store starts at height 1. Wallets that restore from an old birthday need a store with complete history, because a store built from a recent checkpoint lacks older rows.

### Unspent outputs

`TransparentAddressUnspentOutputs(address, start_height, at_epoch_id)` streams every unspent output of one address mined at or above `start_height`: first a `ChainView` header, then one `TransparentUnspentOutput` per output. There is no cursor and no entry cap, so a client cannot truncate the set by ignoring pagination.

The server reads the set in pages of 1,024 outputs and reads the next page only when the client has taken the previous one. It holds at most three pages per stream. A client that stops reading stalls the storage reads. Cancelling the stream releases them. A stream ends without an error only after the address's last output.

An in-process caller that opens many of these streams for one request can use `WalletQueryApi::transparent_address_unspent_outputs_on_demand` instead. It returns the same outputs in the same order at the same pinned epoch, but reads nothing until the stream is first polled and never reads ahead: it reads the next page only when the caller has taken every output of the previous one. The caller chooses the first page's size, at most 1,024 outputs, from how many streams it holds. Later pages double up to 1,024 as the caller consumes them, so such a stream holds at most the page its caller is reading, and a stream not yet polled holds none.

The lightwalletd adapter's `GetAddressUtxos` and `GetAddressUtxosStream` merge uses these on-demand streams, one for each distinct address, as [Lightwalletd compatibility](../reference/lightwalletd-compatibility.md#getaddressutxos-and-getaddressutxosstream) describes.

### Transaction history

`TransparentAddressTxIdsInRange(address, start_height, end_height, max_entries, from_cursor)` streams the transactions that touch an address within an inclusive height range, in ascending mined order: a `ChainView` header, then one `TransparentAddressTxId` per transaction.

- One call returns one page. `max_entries` of 0 means 1,000, and larger values are clamped to 1,000.
- Only the last item of a page carries a cursor. It is non-empty when more entries may follow. Pass it as `from_cursor` to read the next page.
- If the connection drops mid-page, request the page again from its start.
- A cursor resumes on the epoch that issued it. Once that epoch is released, the next page fails with `CHAIN_EPOCH_PIN_UNAVAILABLE`. Restart the history from its first page.
- The request has no `at_epoch_id`. `zinder-client` checks the header epoch against its snapshot instead and reports a mismatch as `ChainEpochPinUnavailable`.

History is in ascending order. For newest-first address history, use `ExplorerQuery.TransparentAddressActivity`.

### Outpoint spend status

`TransparentOutpointSpendStatus(outpoints, at_epoch_id)` reports, for each of 1 to 1,024 outpoints, whether a mined transaction spends it. A wallet uses it to resolve spends of outputs it already tracks without reading each address's history. The response has one entry per requested outpoint, in request order, including duplicates:

| Status | Meaning |
| --- | --- |
| `unspent` | A mined transaction created the output, and no mined transaction spends it. |
| `spent_by` | A mined transaction spends it. The entry names the spending transaction, input index, and block. |
| `not_found` | No mined transaction created this outpoint, including the coinbase input sentinel and the unspendable genesis coinbase output. |

The status covers mined transactions from height 1 through the epoch's visible tip. A spend that is only in the mempool leaves the output `unspent`; pending spends appear on mempool entries.

The request reveals which outpoints the wallet tracks, and the server can link them to the addresses that own them. It is not a privacy improvement over reading address history.

A request with no outpoints or more than 1,024 fails with `INVALID_ARGUMENT` and `TRANSPARENT_OUTPOINT_COUNT_OUT_OF_RANGE`. On a store whose history starts after a checkpoint, a request in which any outpoint is not found fails with `FAILED_PRECONDITION` and `CANONICAL_HISTORY_UNAVAILABLE`, because absence cannot be proven there. `zinder-client` splits a longer list into requests of at most 1,024 and pins them all to the first response's epoch.

### Balance

The wallet store keeps a balance for each transparent address. The compatibility runtime serves those balances through the lightwalletd `GetTaddressBalance` methods, as [Lightwalletd compatibility](../reference/lightwalletd-compatibility.md) describes. Native wallets sum the unspent outputs they read, because the native service has no balance method. Explorer clients use `ExplorerQuery.TransparentAddressActivity`.

## Value pools

`ChainValuePoolsAtTip` is in the protocol but not advertised, because Zinder does not verify that its node returns the value-pool payload this method needs. A call fails with `FAILED_PRECONDITION` and `ENDPOINT_CAPABILITY_UNAVAILABLE` without contacting the node.

## Broadcast

`BroadcastTransaction(raw_transaction)` forwards a serialized transaction to the node and returns a typed outcome: `accepted`, `duplicate`, `invalid_encoding`, `rejected` with a `BroadcastRejectionReason`, `queued`, or `unknown`. Broadcast never writes chain state. The transaction appears in mempool and chain reads once ingest observes it from the node.

- The method is advertised when `[node]` is configured. The query does not contact its nodes at startup. It verifies each endpoint on first contact, and a broadcast fails with the retryable `NODE_UNAVAILABLE` until one passes.
- With `[node_fallbacks.N]` configured, a broadcast tries the verified endpoints in order and moves to the next one when an endpoint is unreachable or rejects the transaction's consensus branch. If an earlier endpoint was unreachable, a later `duplicate` or `queued` answer is reported as `accepted`, because the unreachable endpoint may have accepted the transaction.
- A transaction larger than 2,000,000 bytes fails with `INVALID_ARGUMENT` and `BROADCAST_TRANSACTION_TOO_LARGE`.
- The process forwards at most 16 MiB of transactions at once. A broadcast that does not fit fails at once with `UNAVAILABLE` and `BROADCAST_BUDGET_EXHAUSTED` and reaches no node. `zinder_query_broadcast_budget_refusals_total` counts refusals.

A node verdict is a successful response, not a gRPC error:

| Outcome | Meaning | Send the same bytes again? |
| --- | --- | --- |
| `accepted` | The node accepted the transaction. `transaction_id` names it. | No |
| `duplicate` | The node already has the transaction in its mempool or chain. | No |
| `invalid_encoding` | The node could not decode the bytes. | No. Fix the encoding. |
| `queued` | An earlier submission of the same bytes is still queued for download or verification. The node accepts or rejects it later. | No. Watch `Transaction` or the mempool for it. |
| `rejected` | The node refused the transaction for the `kind` below. | Only for `MEMPOOL_FULL` |
| `unknown` | The node returned an error without an error code, so Zinder could not classify it. `message` carries the node's text. | Not determined. Look the transaction up before you submit it again. |

| `BroadcastRejectionReason` | Meaning |
| --- | --- |
| `INVALID_SIGNATURE` | The node rejected one or more signatures. |
| `BAD_EXPIRY_HEIGHT` | `nExpiryHeight` is at or below the visible tip. |
| `BAD_CONSENSUS_BRANCH` | The consensus branch ID does not match the current network upgrade. The query tries the next verified endpoint before it reports this. |
| `MEMPOOL_FULL` | The node's mempool is full. The only reason to retry with the same bytes. |
| `UNSUPPORTED_TRANSACTION_VERSION` | The next block's network upgrade does not allow this transaction version. Rebuild the transaction; re-signing cannot fix it. |
| `EXCEEDS_BLOCK_ACTION_LIMIT` | The transaction alone exceeds a ZIP 218 per-block shielded action limit, so no block can include it. Split the payment. |
| `UNKNOWN` | The node's message matched no known reason. A client that decodes an unrecognized value treats it the same way. |

Without `[node]`, `wallet.broadcast.transaction_v1` is not advertised, and a broadcast fails with `FAILED_PRECONDITION` and `ENDPOINT_CAPABILITY_UNAVAILABLE`.

`ServerInfo.node` describes the first verified endpoint in preference order and is empty until one is verified. It can change while the query runs, so read it from a fresh `ServerInfo`. [Node endpoint failover](chain-ingestion.md#node-endpoint-failover) covers endpoint selection.

## Readiness and node outages

A reader that is not ready rejects new requests with `UNAVAILABLE` and `SERVICE_NOT_READY`; `ErrorInfo.metadata["readiness_cause"]` names the cause. If the canonical writer stopped because a reorg exceeded its window, requests fail with `REORG_WINDOW_EXCEEDED` instead until an operator recovers the writer. During a node outage, stored reads keep working while the tip stops advancing. [Readiness](../reference/readiness.md) lists the causes and what stays ready.
