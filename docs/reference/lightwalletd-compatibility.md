# Lightwalletd compatibility

This page is the reference for how `zinder-compat-lightwalletd` answers the lightwalletd `CompactTxStreamer` protocol, and where its behavior differs from Go lightwalletd. It is for wallet developers who point an existing lightwalletd client at Zinder, and for operators who run the compatibility runtime.

The runtime serves the same stored data as the native `WalletQuery` service, described in [Wallet data plane](../architecture/wallet-data-plane.md). It uses the node only to broadcast transactions and to read the recommended fee.

## Claim boundary

Zinder replaces the network-facing lightwalletd server. Wallet-local scanning, key custody, transaction construction, and wallet UX stay in the client.

Name one of these levels in release notes and operator docs, and claim the highest level whose evidence is current:

| Level | Evidence |
| --- | --- |
| `protocol-compatible` | The vendored protocol is pinned, the drift check passes, and generated-client integration tests pass. |
| `reference-parity-compatible` | The live parity suite matches the pinned reference lightwalletd for the claimed methods. |
| `client-compatible` | A named lightwalletd client completes bootstrap, restore, transparent discovery, shielded-note discovery, send, and pending-transaction observation. |
| `public-operator-compatible` | The deployed endpoint has TLS, access control, readiness, rate limits, and log redaction in place. |

A named wallet is supported once it passes end-to-end certification against a release on the claimed network. Method coverage by itself is not a support claim. When you call the adapter a drop-in replacement, name the protocol pin, the claimed methods, and the level.

## Runtime serving selection

The operator selects one of two serving modes with `[compat].serving`, the `ZINDER_COMPAT__SERVING` environment variable, or `--serving`. The default is `wallet`.

- `wallet` serves every method in the table below. It reads the canonical store and the wallet store as one consistent pair and needs ingest to retain transaction blobs (`raw_blob_policy` of `transactions` or `all`); with `none`, the process fails before it binds its listener. It uses `[node]` and `[node_fallbacks.N]` only for `SendTransaction` and `GetStandardFee`, and connects to ingest control for the mempool methods.
- `compact-blocks` serves `GetLightdInfo`, `GetLatestBlock`, `GetBlock`, and `GetBlockRange` from the canonical store and answers them as wallet serving does, except that `taddrSupport` is `false`. Every other method except `Ping` returns `UNIMPLEMENTED`. It never contacts a node, so it refuses to start with a `[wallet]` section, a `[node]` or `[node_fallbacks.N]` section, `--node-json-rpc-addr`, or any `ZINDER_NODE__*` or `ZINDER_NODE_FALLBACKS__*` variable.

In both modes, `Ping` is refused with `FAILED_PRECONDITION`, as reference lightwalletd refuses it. Setting `compat.ping_very_insecure = true` (`ZINDER_COMPAT__PING_VERY_INSECURE`, or `--ping-very-insecure`) makes it answer as `lightwalletd --ping-very-insecure` does. The response's `entry` and `exit` report the calls in flight, and the call sleeps `interval_us` microseconds, wrapping on overflow as Go's `time.Duration` does. Each call holds a server task as long as the client asks, so enable it only on a server under load test. A call still sleeping at shutdown ends with `UNAVAILABLE`.

## Methods

| Method | Wallet serving | Compact-blocks serving | Notes |
| --- | --- | --- | --- |
| `GetLightdInfo` | Served | Served | `taddrSupport` is `true` only in wallet serving |
| `GetLatestBlock` | Served | Served | Hash in internal byte order |
| `GetBlock` | Served | Served | Height 0 with an empty hash is `INVALID_ARGUMENT` |
| `GetBlockNullifiers` | Served | `UNIMPLEMENTED` | Shielded pools, nullifiers only |
| `GetBlockRange` | Served | Served | Ascending or descending; see below |
| `GetBlockRangeNullifiers` | Served | `UNIMPLEMENTED` | Ignores `poolTypes`; shielded pools only |
| `GetTransaction` | Served | `UNIMPLEMENTED` | Pending transactions at height 0 |
| `SendTransaction` | Served when `[node]` is set | `UNIMPLEMENTED` | |
| `GetTaddressTxids`, `GetTaddressTransactions` | Served | `UNIMPLEMENTED` | Same behavior for both |
| `GetTaddressBalance`, `GetTaddressBalanceStream` | Served | `UNIMPLEMENTED` | At most 256 distinct addresses |
| `GetMempoolTx` | Served | `UNIMPLEMENTED` | |
| `GetMempoolStream` | Served | `UNIMPLEMENTED` | |
| `GetTreeState` | Served | `UNIMPLEMENTED` | Every height from Zinder's store |
| `GetLatestTreeState` | Served | `UNIMPLEMENTED` | |
| `GetSubtreeRoots` | Served | `UNIMPLEMENTED` | `maxEntries = 0` streams every remaining root |
| `GetAddressUtxos`, `GetAddressUtxosStream` | Served | `UNIMPLEMENTED` | At most 100,000 outputs per response |
| `GetStandardFee` | Served when a configured node implements `getstandardfee` | `UNIMPLEMENTED` | See [`GetStandardFee`](#getstandardfee) |
| `Ping` | Refused unless enabled | Refused unless enabled | See above |

A handler that makes several storage reads pins them to the chain epoch of the tip it reads first, so one response never mixes two branches of a reorg. Block-range streams instead follow the chain, as described below. A runtime that is not ready rejects new requests with `UNAVAILABLE` and `SERVICE_NOT_READY`, as [Readiness](readiness.md) describes.

## Method behavior

### `GetLightdInfo` and `GetLatestBlock`

Both methods describe the serving process and its current tip. Whole messages reflect build metadata and tip timing, so comparing them, or their digests, across servers is not parity evidence. Compare only the comparable fields, at one agreed tip.

| `GetLightdInfo` fields | Rule |
| --- | --- |
| `chainName`, `saplingActivationHeight`, `consensusBranchId`, `upgradeName`, `upgradeHeight` | Comparable with lightwalletd at the same `blockHeight`. `chainName` is `main` or `test`; Regtest reports `test`. `consensusBranchId` is the lowercase hex branch active at the tip. `upgradeName` and `upgradeHeight` name the next pending upgrade and are empty and 0 when none is pending. |
| `lightwalletProtocolVersion` | The vendored `lightwallet-protocol` release tag, `v0.6.0`. Wallets parse it as a version; zingolib refuses to sync below 0.5.0. |
| `blockHeight` | The served visible tip. Compare with a tolerance of one block. |
| `vendor`, `version`, `gitCommit` | `Zinder`, the crate version, and the build commit. Never compare. |
| `branch`, `buildDate`, `buildUser`, `zcashdBuild`, `zcashdSubversion`, `donationAddress` | Always empty. Never compare. |
| `estimatedHeight` | The network height estimated from the served tip, as [Stale tip](#stale-tip) describes. Never below `blockHeight`, and at most a few blocks above it while the tip advances. Never compare. |
| `taddrSupport` | `true` in wallet serving, `false` in compact-blocks serving. Fixed when the process starts. |

The upgrade fields come from the network-upgrade schedule that ingest admitted, never from a node. A newly admitted upgrade shows as pending until the tip reaches it. A store without an admitted schedule is not served: the process retries until `storage.initial_catchup_timeout_ms` passes, then exits.

`GetLatestBlock` returns the visible tip height and its hash in internal byte order, the reverse of the hex Zebra's JSON-RPC prints. Compare hashes across servers only at equal heights.

#### Stale tip

`estimatedHeight` is the height Zebra would report as `estimatedheight` for the served tip: the tip, plus the blocks the target spacing would have produced since the tip's block time. Each era uses the spacing of its network upgrade in the admitted schedule: 150 seconds before Blossom, 75 seconds from Blossom, and 25 seconds from NU7. Go lightwalletd forwards Zebra's value, which Zebra computes the same way from its own tip. During a node outage the served tip stands still and `estimatedHeight` keeps rising.

An old tip is not always a stale one: the network may simply not have produced a block. So `GetLatestBlock` and `GetLightdInfo` fail with `UNAVAILABLE` only when both hold:

- The served tip is older than `compat.stale_tip_limit_ms` (`ZINDER_COMPAT__STALE_TIP_LIMIT_MS`). Its age counts from its block time, or from when this process first served it since it started if that is earlier, so a block time ahead of the local clock, which consensus accepts, delays a refusal only until the first tip call after the tip changes or the process restarts. The age is taken after any read of the tip's header the call makes, so it is the age the tip has when the call answers.
- The writer cannot vouch that the served tip is the network's tip: its node is unreachable, the node reports that its peers show it behind the network, the node has a block above the served tip because the writer is behind it (catching up or restored from an old snapshot), the writer is starting or parked, or ingest control does not answer.

While the writer's node is reachable, reports itself in sync, and has no block above the served tip, the tip calls answer however old that tip is, and `estimatedHeight` tells wallets the chain is quiet. Go lightwalletd fails these calls only while its node is down, and keeps answering while its node is reachable but behind the network.

A node counts as behind the network when Zebra's `/ready` reports `syncing`, `insufficient peers`, or `no tip`, which come from its peers and its syncer. Zebra's other not-ready reports, `tip_age=…` and `lag=…`, and both reports of the JSON-RPC health fallback are computed from how old the node's tip is. A quiet chain raises them too, so they do not stop the node vouching, and a node that has stalled but keeps one peer, which reports only `tip_age=…`, looks the same as a quiet chain. On Testnet and Regtest, Zebra's `/ready` reports ready unless `enforce_on_test_networks` is set. Without a node health probe, only the node's reachability and its tip count.

The process reads the writer's status from ingest control every `storage.secondary_catchup_interval_ms`, 1 second by default. A writer that reaches its node reports the node's committed tip there and reports none while the node is unreachable. A reading vouches for 30 seconds or three reading intervals, whichever is longer, so a stopped ingest control stops vouching within that time. Readings that fail or report no node tip stop the vouching once they have lasted 5 seconds or two reading intervals, whichever is longer, and never more than a minute, so one failed reading does not refuse a quiet chain's tip unless readings are a minute or more apart. Once the node reports a block above a tip, that tip keeps vouching for 10 seconds or three reading intervals, whichever is longer, while the reader catches up, and further blocks in that time do not shorten it. The tip calls answer again as soon as the node reports the served tip or a newer tip is served. Every other method keeps serving stored data. `zinder_compat_stale_tip_refusals_total{method}` counts the refusals.

- The limit defaults to 25 minutes on every network. On a quiet Regtest chain, which mines on demand, the reachable node vouches for the tip.
- 0 turns the limit off, for a private deployment that wants its wallets to keep syncing through a long outage. Its wallets then build transactions that expire on arrival once the tip is about 50 minutes old. Any other value must be at least 5 minutes.
- Every wallet engine below expires a transaction 40 blocks after the tip it was told, plus one, and 120 blocks from NU7: 50 minutes of target time either way. The default limit is half that window, so a wallet stops on the stale tip well before a transaction built on it would expire.

How each engine uses the tip calls:

| Engine | Reads `estimatedHeight` | When `GetLatestBlock` or `GetLightdInfo` fails | Transaction expiry |
| --- | --- | --- | --- |
| Slipstream, in the ZODL Android and iOS apps (`zodl-inc/slipstream` `245a2d2`, `zodl-android-wallet-sdk` `ea01b96`, `zodl-swift-wallet-sdk` `0571cb4`) | Server ranking only: a server whose tip is more than 6 hours of target time behind its `estimatedHeight` is left out. Sync never reads it. | Each sync pass stops at `GetLatestBlock` and retries. The follow loop polls every 10 to 30 seconds and, after 5 failures, reports the endpoint unreachable while it keeps polling. Ranking leaves out a server whose tip calls fail. | Wallet tip plus 1 plus 40 blocks, or 120 from NU7. |
| Zcash Android SDK (`zcash/zcash-android-wallet-sdk` `a95780a`) | Server ranking only: a server whose `estimatedHeight` is 288 or more blocks above `blockHeight` is left out. | After 3 attempts the synchronizer reports `DISCONNECTED` and reconnects to the same server about every 5 seconds, without giving up. A failing `GetLightdInfo` at sync setup ends in a `ClassCastException` in `CompactBlockProcessor.verifySetup`, as it does against lightwalletd with its node down. | Rust backend default: tip plus 1 plus 40 blocks. |
| Vizor (`chainapsis/vizor-wallet` `a69183b`) | Never. | Retries after 2, 4, and 8 seconds, then shows "Network connection lost. We'll keep trying automatically." In Automatic mode it sets the server aside for a minute and switches; a custom endpoint retries every 10 seconds. A send fails while the tip call fails. Automatic mode also leaves a server whose tip has not advanced for 5 minutes when another is higher. | Server tip plus 1 plus 40 blocks, or 120 from NU7. |
| Zingo (`zingolabs/zingolib` `658dea0`, pepper-sync) | Never. | The sync pass ends with an error it classes as recoverable; a failing `GetLightdInfo` stops a sync before it starts. | Wallet tip plus 1 plus 40 blocks, or 120 from NU7. |

### `GetBlock` and `GetBlockNullifiers`

A `BlockID` with a height reads that height. Height 0 with a 32-byte hash is a hash lookup. Height 0 with an empty hash is an unspecified identifier and returns `INVALID_ARGUMENT`. A height above the visible tip returns `OUT_OF_RANGE`. A hash that is not in the best chain, or a hash that does not match the block at the given height, returns `NOT_FOUND`. A best-chain block whose compact block the store does not hold also returns `NOT_FOUND`, but with the [reason](error-vocabulary.md) `ARTIFACT_UNAVAILABLE`, where a hash that is not in the best chain carries `BLOCK_NOT_IN_BEST_CHAIN`. Both serving modes answer this way, whether the request names the block by height or by hash. `GetBlock` in compact-blocks serving returns the stored block without pool filtering.

### `GetBlockRange` and `GetBlockRangeNullifiers`

`GetBlockRange` applies the requested `poolTypes` to every transaction and omits any transaction left with no spends, outputs, actions, or transparent inputs and outputs, as lightwalletd does. With no `poolTypes`, it returns the shielded pools, so a transparent-only coinbase is absent unless the request names the transparent pool. Each remaining transaction keeps its block position in `CompactTx.index`, so a gap marks an omitted transaction. A range with start above end is sent in descending order.

`GetBlockRangeNullifiers` ignores `poolTypes` and sends the nullifiers of every shielded pool. This differs from lightwalletd v0.6.2, which drops the transparent pool from `poolTypes` and honors the pools that remain, so a request that names only some shielded pools gets only their nullifiers from lightwalletd.

These outcomes fail the call before any block is sent:

| Condition | Status |
| --- | --- |
| Malformed request, including a missing bound or an invalid pool type | `INVALID_ARGUMENT` |
| No block-range stream slot returned within 10 seconds, or the queue for one is full | `UNAVAILABLE` |
| Higher bound above the visible tip | `OUT_OF_RANGE` |
| One of the first 1,000 heights the stream would send is outside retained history, including height 0 | `NOT_FOUND` |
| A block missing inside retained history | `UNAVAILABLE` |
| A stored block in the first byte-bounded chunk that a lightwalletd message cannot carry, such as a transaction fee above `uint32` | `DATA_LOSS` |

Once a stream has started, a client can rely on the following:

1. **Order and coverage.** Blocks arrive at consecutive heights in the requested direction. A stream that ends `OK` sent every requested height exactly once.
2. **Every link holds.** Ascending, each block's `prevHash` equals the hash of the block sent before it. Descending, each block's hash equals the `prevHash` of the block sent before it.
3. **One chain as read.** An ascending stream that crosses a reorg above its last sent block continues on the new branch. A descending stream continues on the chain it started on. An `OK` stream does not promise its blocks are still the best chain on arrival.
4. **Reorg ending.** A stream ends with `ABORTED` when a reorg replaces a block it already linked to (ascending) or the next height it must send (descending), as lightwalletd v0.5.5 and later do. It ends with `OUT_OF_RANGE` when a reorg leaves the tip below a height it still must send. To recover, request again from the height the wallet needs.
5. **Settled history never ends a stream.** Ingest never replaces a block at or below the settled tip, so only the last reorg window (1,000 blocks by default) can end a stream early.
6. **Other mid-stream endings.** A storage failure ends the stream with `UNAVAILABLE`. A chunk with a stored block that a lightwalletd message cannot carry ends it with `DATA_LOSS`, before any block of that chunk is sent. A descending range that runs below retained history, or, in wallet serving, a reorg that lowers the tip during a chunk read, ends it with `NOT_FOUND`.
7. **No time limit.** Elapsed time, new blocks, and slow reading never end a stream. Shutdown, unanswered keepalive pings, and the listener's send-stall deadline do.

A chunk holds at most 1,000 blocks and at most 2 MiB of stored compact blocks, counted before pool filtering; a single larger block forms its own chunk. Until the server's gRPC layer has taken every block of the first chunk from the stream to send it, the server reads only the first two chunks, however few blocks they hold. The gRPC layer takes blocks as it fills its send buffer, so this bounds the reads ahead of that layer, not ahead of what the client has received. After that, the server reads ahead by one chunk plus 16 queued blocks in wallet serving or 8 in compact-blocks serving. A waiting stream holds no chain epoch, so a slow client never delays new blocks.

`compat.max_concurrent_block_range_streams` (`ZINDER_COMPAT__MAX_CONCURRENT_BLOCK_RANGE_STREAMS`) limits how many `GetBlockRange` and `GetBlockRangeNullifiers` streams the process serves at once. It defaults to 100 and accepts 1 to 65,536.

- A stream holds its slot from validation until it ends, fails, or is cancelled.
- A valid request past the limit waits for a slot, first come first served, for up to 10 seconds, and reads nothing while it waits. If no slot returns in time, it fails with `UNAVAILABLE`. lightwalletd waits and fails the same way.
- At most as many requests wait as the limit has slots, 100 by default. A request that finds the queue full fails with `UNAVAILABLE` at once. A client that cancels, disconnects, or reaches its deadline while it waits gives up its place.
- The Zcash Android SDK, Slipstream, and Vizor retry a failed block-range request after a short backoff, so an occasional refusal delays a sync. Repeated refusals can still end a sync attempt.
- `zinder_compat_block_range_streams_open` reports open streams, `zinder_compat_block_range_slot_wait_duration_seconds` how long requests waited, and `zinder_compat_block_range_stream_refusals_total` the refusals, as [Metrics](metrics.md#zinder-compat-lightwalletd) lists.
- Every client shares the limit and the queue. A client that opens the limit's worth of streams and stops reading holds every slot until it disconnects, and one that also sends as many more requests fills the queue. Set a per-client limit on concurrent requests at your proxy. The shipped Caddy template does not include one.

The memory a stream uses beyond its stored bytes has no enforced maximum and has not been measured. Size the limit to the memory left after the block caches in [Storage backend](../architecture/storage-backend.md), and watch container memory while `zinder_compat_block_range_streams_open` is near it. lightwalletd v0.6.2 limits block-range requests with `--max-concurrent-blockrange`, 200 by default.

### `GetTreeState` and `GetLatestTreeState`

`GetTreeState` answers every retained height from Zinder's store, where lightwalletd asks its node. It returns the same statuses as native `TreeStateAtHeight`, listed in [Wallet data plane](../architecture/wallet-data-plane.md#tree-states-and-subtree-roots), except that a height above the visible tip is `INVALID_ARGUMENT`, as lightwalletd returns, and a refused rebuild is `UNAVAILABLE`.

Zinder bounds concurrent tree-state rebuilds, where lightwalletd has no rebuild limit. A rebuild that finds every slot taken waits up to `compat.tree_state_replay_wait_ms` (`ZINDER_COMPAT__TREE_STATE_REPLAY_WAIT_MS`, 0 to 5,000 ms, 2,000 by default), then fails with `UNAVAILABLE`, the code a full block-range limit returns, so a wallet retries both limits the same way. It fails at once when every waiting place is taken. `compat.tree_state_replay_waiters_per_slot` (`ZINDER_COMPAT__TREE_STATE_REPLAY_WAITERS_PER_SLOT`, 1 to 256, 64 by default) sets the waiting places per slot. Native `TreeStateAtHeight` refuses the same rebuild with `RESOURCE_EXHAUSTED` and `TREE_STATE_REPLAY_LIMIT_REACHED`. `GetLatestTreeState` returns the newest stored checkpoint at or below the visible tip.

### `GetSubtreeRoots`

lightwalletd treats `maxEntries = 0` as "every root", and Zinder does the same: it streams every root from `startIndex` to the last completed subtree. A positive `maxEntries` streams that many roots, or fewer when the store holds fewer. Zinder honors the full `uint32` range of `maxEntries`, where lightwalletd with Zebra rejects a value above 65,535 with `INVALID_ARGUMENT`, and native `SubtreeRoots` has its own cap. Zinder reads the roots in pages of at most 1,000, all at the chain epoch the call started on, so a long stream never mixes two chains.

If that epoch is retired before the last page, the stream ends with `UNAVAILABLE` after the roots already sent. A stream that has already sent its last completed root ends with `OK` without another read. To retry after `UNAVAILABLE`, restart from the original `startIndex`, or resume after a root whose `completingBlockHash` the wallet has verified on the new chain. Resuming at the index after the last delivered root is unsafe, because a reorg that replaced that root's completing block also replaces the root, and the resumed stream would skip its replacement and combine roots from two forks.

An absent root under retained-range coverage is `NOT_FOUND`, which includes a root below the history floor of a checkpointed store that was not loaded with its roots. A store loaded with complete-prefix subtree-root coverage retains the roots below its floor and serves them. `completingBlockHash` is in RPC byte order, as lightwalletd sends it.

### `GetTransaction`

The request must carry a transaction id; an empty hash is `INVALID_ARGUMENT`. Zinder reads the canonical chain first. On a miss, it asks the writer's live mempool through ingest control, with a 15-second timeout:

- A pending transaction is returned at height 0, the protocol's "in the mempool" marker, as lightwalletd returns it from Zebra.
- A transaction in neither place is `NOT_FOUND`, but only when the writer answered at the same chain epoch as the canonical read. A miss at another epoch is a retryable `UNAVAILABLE`.
- On a store whose history starts at a checkpoint, a mempool miss is `FAILED_PRECONDITION` with `CANONICAL_HISTORY_UNAVAILABLE`, because the transaction may be mined below the checkpoint.
- A failed or timed-out mempool read is `UNAVAILABLE`, not `NOT_FOUND`, because the transaction may be pending.

Wallet SDKs treat height 0 as pending and `NOT_FOUND` as an unknown id, so a just-broadcast transaction reads as pending.

### `SendTransaction`

`SendTransaction` uses the same broadcast path as native `BroadcastTransaction`, including endpoint verification on first contact, failover across `[node_fallbacks.N]`, and the 16 MiB in-flight byte budget, described in [Wallet data plane](../architecture/wallet-data-plane.md). Until an endpoint passes verification, a send fails with `UNAVAILABLE` and reaches no node. Without `[node]`, a send fails with `FAILED_PRECONDITION` and `ENDPOINT_CAPABILITY_UNAVAILABLE`.

The typed outcome maps to `SendResponse.errorCode`. When the node supplied a code, Zinder forwards it; otherwise it uses these defaults:

| Outcome | `errorCode` | `errorMessage` |
| --- | --- | --- |
| Accepted | 0 | Transaction id hex |
| Invalid encoding | -22 | Node message |
| Rejected | -26 | Node message |
| Duplicate | -27 | Node message |
| Queued | -25 | Node message |
| Unknown | -1 | Node message |

When an earlier endpoint was unreachable, a later duplicate or queued answer returns `errorCode` 0 with the transaction id, because the unreachable endpoint may have accepted the transaction.

### `GetStandardFee`

`GetStandardFee` returns the ZIP 317 marginal fee per logical action and the estimator version, both as the node's `getstandardfee` RPC reports them. An omitted estimator version is `0`. Zinder does not compute a fee itself, and it does not pass on a zero fee, which a node that follows ZIP 317 never reports.

The call uses the same verified endpoints and failover order as `SendTransaction`, and applies the same admission check against the pair being served, so it never asks an endpoint that has not passed first-contact verification or whose network-upgrade table disagrees with the served schedule at the next block. It moves to the next endpoint when one is unreachable or does not implement the RPC, and it does not change the preferred broadcast endpoint.

| Situation | Status |
| --- | --- |
| A verified node answers with a fee | The fee and estimator version |
| A verified node answers with a zero fee, or with a reply that is not a JSON object with a fee and estimator version, such as an array of the two numbers | `INTERNAL` |
| No endpoint is verified yet, an endpoint is still awaiting verification and none answered, or every verified endpoint is unreachable or answers with an error other than method-not-found | `UNAVAILABLE` |
| Every endpoint that can still be admitted is verified and lacks `getstandardfee`, or `[node]` is not set | `UNIMPLEMENTED` |

An unreachable endpoint, or one still awaiting verification, takes precedence over one that lacks the method, so an outage is not reported as a permanent absence. A zero fee or a reply of another shape is a node bug, not an outage, so it ends the call with `INTERNAL` without asking the next endpoint, as Go lightwalletd answers it.

### `GetTaddressTxids` and `GetTaddressTransactions`

Both methods resolve `range` the way lightwalletd does on Zebra:

- `range` and `range.start` are required; a missing one is `INVALID_ARGUMENT`.
- An absent `range.end`, or an end of 0, means the visible tip.
- Both bounds are clamped to the visible tip. A start of 0 reads from genesis.
- An end below the start is `INVALID_ARGUMENT`.

lightwalletd rejects a span wider than 10,000,000 blocks. Zinder clamps the end to the tip and serves it, because wallets such as WebZjs send `end = u64::MAX`.

All reads of one request are pinned to one epoch. The server reads the history 1,000 entries at a time and sends each transaction as it reads it, at most 8 transactions ahead of the client, so memory does not grow with the address's history.

A failure after the first transaction ends the stream with its status, after the transactions already sent, as lightwalletd does when a transaction read fails. A stream still reading one minute after a new block replaces its epoch ends with `CHAIN_EPOCH_PIN_UNAVAILABLE`, which a client that reads a long history slowly can meet. A client must treat a stream that ends with an error as incomplete and request the range again.

### `GetTaddressBalance` and `GetTaddressBalanceStream`

Both methods return the confirmed balance, the sum of unspent outputs, of up to 256 distinct addresses at one chain epoch. A repeated address counts once. Mempool state is not included.

- An empty list is `INVALID_ARGUMENT`.
- The 257th distinct address is `INVALID_ARGUMENT` with `TRANSPARENT_BALANCE_ADDRESS_COUNT_EXCEEDED`. The streaming method refuses as soon as that address arrives, without waiting for the client to finish. lightwalletd bounds the streaming method at 10,000 addresses.
- An invalid address, an address for another network, or address text longer than 64 bytes is `INVALID_ARGUMENT`.
- A total above the protocol's `int64` field is `OUT_OF_RANGE`.

### `GetAddressUtxos` and `GetAddressUtxosStream`

Both methods follow lightwalletd's `maxEntries` rule: 0 returns every unspent output at or above `startHeight`, and a positive value returns at most that many, oldest first. Within one block, Zinder orders rows by transaction id and output index, where Zebra uses block position; this matters only when `maxEntries` splits a block.

A request that lists an address more than once gets that address's rows once, as lightwalletd v0.6.2 does. A wallet that adds up the rows counts each output once, and duplicate rows never use up `maxEntries`.

One response returns at most 100,000 outputs, the same cap Zebra puts on `getaddressutxos`. Zinder never drops rows silently, because wallets add these rows up as the address balance. lightwalletd on Zebra instead returns `OK` with an empty list in this case.

- `GetAddressUtxos` reads every row before it answers, so a request that would return more fails with `RESOURCE_EXHAUSTED` and no rows.
- `GetAddressUtxosStream` sends rows as it merges them, at most 64 ahead of the client. A request that would return more sends the first 100,000 rows and then ends with `RESOURCE_EXHAUSTED`. Any other failure after the first row also ends the stream with its status after the rows already sent, such as `CHAIN_EPOCH_PIN_UNAVAILABLE` for a client still reading one minute after a new block replaces the request's epoch. A wallet must treat a stream that ends with an error as incomplete and must not add up its rows.

Both methods read each distinct address once, at one chain epoch, and merge the addresses' outputs in response order as they send them. Each address is read in pages that double only as its rows are sent. Its first page holds an even share of 8,192 outputs, capped at 1,024 and at a positive `maxEntries`, but never fewer than 16. A request that lists up to eight distinct addresses therefore reads up to 1,024 outputs of each in one page.

A request holds at most about twice the rows it has sent, plus 2 outputs and one first page for each distinct address. Its first pages together hold at most 8,192 outputs, or 16 for each distinct address when it lists more than 512. At the limits of 10,000 addresses and 100,000 rows, that is about 380,000 outputs.

The protocol has no lossless way to page past the cap. A single block with more matching outputs than the cap always fails with `RESOURCE_EXHAUSTED`.

### `GetMempoolTx`

`GetMempoolTx` returns the compact form of every current mempool transaction, filtered by `poolTypes` as `GetBlockRange` filters blocks. `exclude_txid_suffixes` accepts at most 1,024 suffixes of at most 32 bytes each. A suffix excludes a transaction only when it matches exactly one mempool transaction. Suffixes are compared against transaction ids in internal byte order.

### `GetMempoolStream`

`GetMempoolStream` sends every current mempool transaction, then each new one, at height 0, and each transaction id at most once per stream, as lightwalletd does.

- The stream ends with `OK` when the chain tip changes, including a change already observed while the stream was starting. Clients reconnect, as with lightwalletd.
- If the tip-change signal itself closes, for example during shutdown, the stream ends with `UNAVAILABLE`, so a client does not mistake it for a new block.
- One stream sends at most 100,000 distinct transactions between tip changes. The next new one ends it with `UNAVAILABLE`, which clients retry.

## Request limits

Limits shared by every public Zinder gRPC listener are listed in [Configuration](configuration.md#listener-and-request-limits). These are the compatibility runtime's list-field bounds, which follow the counting and refusal rules in [Request list bounds](configuration.md#request-list-bounds):

| Method | Field | Limit | Handler cap |
| --- | --- | --- | --- |
| `GetTaddressBalance` | `addresses` | 512 | 256 distinct addresses |
| `GetAddressUtxos`, `GetAddressUtxosStream` | `addresses` | 10,000 | none |
| `GetMempoolTx` | `exclude_txid_suffixes` | 2,048 | 1,024 |
| `GetMempoolTx` | `poolTypes` | 1,000 | none |
| `GetBlockRange`, `GetBlockRangeNullifiers` | `poolTypes` | 1,000 | none |
| `GetTaddressTxids`, `GetTaddressTransactions` | `range.poolTypes` | 1,000 | none |

lightwalletd reports an over-cap list refusal as `INTERNAL`.

## Reference inputs

The vendored protocol and the images the parity suite runs are:

- `lightwallet-protocol` `v0.6.0` (`299805c19011a027d2c48c5e912dd05041f76156`);
- `lightwalletd` `v0.6.2` (`f8fe2cca7ede03b1f76b1d637a2ab467d151b79d`) as the source reference, which parses the version-6 transactions that NU6.3 introduced;
- `electriccoinco/lightwalletd:v0.6.2@sha256:75e5742df686094556abef66e69c37bc114de23a0706712aee324f332ca52951`;
- `docker.io/zfnd/zebra:7.1.0@sha256:4f9536c8061e407f9f2ec81b7e0dd2f1762f81deec316abb1530dca8796a33ba`, started on Regtest with `deploy/zebra/regtest.toml`, which activates every upgrade through NU7 (see [Regtest through NU7](../runbooks/testing.md#regtest-through-nu7));
- `docker.io/zakuracore/zakura:1.8.6@sha256:e514c89b751b685686690098928507aaa994311e6b90c79373c55b0afaa45d50`, started on Regtest with `deploy/zakura/regtest.toml`, which uses the same schedule.

A release needs the parity suite to pass with both node images. The test setup is in [Testing](../runbooks/testing.md).
