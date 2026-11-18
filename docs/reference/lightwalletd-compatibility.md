# Lightwalletd compatibility

This page is the reference for how `zinder-compat-lightwalletd` answers the lightwalletd `CompactTxStreamer` protocol, and where its behavior differs from Go lightwalletd. It is for wallet developers who point an existing lightwalletd client at Zinder, and for operators who run the compatibility runtime.

The runtime serves the same stored data as the native `WalletQuery` service, described in [Wallet data plane](../architecture/wallet-data-plane.md). It uses the node only to broadcast transactions.

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

- `wallet` serves every method in the table below. It reads the canonical store and the wallet store as one consistent pair and needs ingest to retain transaction blobs (`raw_blob_policy` of `transactions` or `all`); with `none`, the process fails before it binds its listener. It uses `[node]` and `[node_fallbacks.N]` only for `SendTransaction`, and connects to ingest control for the mempool methods.
- `compact-blocks` serves `GetLightdInfo`, `GetLatestBlock`, `GetBlock`, and `GetBlockRange` from the canonical store. Every other method except `Ping` returns `UNIMPLEMENTED`. It never contacts a node, so it refuses to start with a `[wallet]` section, a `[node]` or `[node_fallbacks.N]` section, `--node-json-rpc-addr`, or any `ZINDER_NODE__*` or `ZINDER_NODE_FALLBACKS__*` variable.

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
| `GetSubtreeRoots` | Served | `UNIMPLEMENTED` | `maxEntries = 0` returns at most 1,000 |
| `GetAddressUtxos`, `GetAddressUtxosStream` | Served | `UNIMPLEMENTED` | At most 100,000 outputs per response |
| `Ping` | Refused unless enabled | Refused unless enabled | See above |

`GetStandardFee` is not part of the vendored protocol, `lightwallet-protocol` v0.5.0, so a call to it returns `UNIMPLEMENTED`.

A handler that makes several storage reads pins them to the chain epoch of the tip it reads first, so one response never mixes two branches of a reorg. Block-range streams instead follow the chain, as described below. A runtime that is not ready rejects new requests with `UNAVAILABLE` and `SERVICE_NOT_READY`, as [Readiness](readiness.md) describes.

## Method behavior

### `GetLightdInfo` and `GetLatestBlock`

Both methods describe the serving process and its current tip. Whole messages reflect build metadata and tip timing, so comparing them, or their digests, across servers is not parity evidence. Compare only the comparable fields, at one agreed tip.

| `GetLightdInfo` fields | Rule |
| --- | --- |
| `chainName`, `saplingActivationHeight`, `consensusBranchId`, `upgradeName`, `upgradeHeight` | Comparable with lightwalletd at the same `blockHeight`. `chainName` is `main` or `test`; Regtest reports `test`. `consensusBranchId` is the lowercase hex branch active at the tip. `upgradeName` and `upgradeHeight` name the next pending upgrade and are empty and 0 when none is pending. |
| `lightwalletProtocolVersion` | The vendored `lightwallet-protocol` release tag, `v0.5.0`. Wallets parse it as a version; zingolib refuses to sync below 0.5.0. |
| `blockHeight` | The served visible tip. Compare with a tolerance of one block. |
| `vendor`, `version`, `gitCommit` | `Zinder`, the crate version, and the build commit. Never compare. |
| `branch`, `buildDate`, `buildUser`, `zcashdBuild`, `zcashdSubversion`, `donationAddress` | Always empty. Never compare. |
| `estimatedHeight` | Always equal to `blockHeight`, even during a node outage. Operators read `served_epoch_age_seconds` on `/readyz` instead. |
| `taddrSupport` | `true` in wallet serving, `false` in compact-blocks serving. Fixed when the process starts. |

The upgrade fields come from the network-upgrade schedule that ingest admitted, never from a node. A newly admitted upgrade shows as pending until the tip reaches it. A store without an admitted schedule is not served: the process retries until `storage.initial_catchup_timeout_ms` passes, then exits.

`GetLatestBlock` returns the visible tip height and its hash in internal byte order, the reverse of the hex Zebra's JSON-RPC prints. Compare hashes across servers only at equal heights.

### `GetBlock` and `GetBlockNullifiers`

A `BlockID` with a height reads that height. Height 0 with a 32-byte hash is a hash lookup. Height 0 with an empty hash is an unspecified identifier and returns `INVALID_ARGUMENT`. A height above the visible tip returns `OUT_OF_RANGE`. A hash that is not in the best chain, or a hash that does not match the block at the given height, returns `NOT_FOUND`. `GetBlock` in compact-blocks serving returns the stored block without pool filtering.

### `GetBlockRange` and `GetBlockRangeNullifiers`

`GetBlockRange` applies the requested `poolTypes` to every transaction and omits any transaction left with no spends, outputs, actions, or transparent inputs and outputs, as lightwalletd does. With no `poolTypes`, it returns the shielded pools, so a transparent-only coinbase is absent unless the request names the transparent pool. Each remaining transaction keeps its block position in `CompactTx.index`, so a gap marks an omitted transaction. A range with start above end is sent in descending order.

These outcomes fail the call before any block is sent:

| Condition | Status |
| --- | --- |
| Malformed request, including a missing bound or an invalid pool type | `INVALID_ARGUMENT` |
| The process already serves its limit of block-range streams | `RESOURCE_EXHAUSTED` |
| Higher bound above the visible tip | `OUT_OF_RANGE` |
| One of the first 1,000 heights the stream would send is outside retained history, including height 0 | `NOT_FOUND` |
| A block missing inside retained history | `UNAVAILABLE` |

Once a stream has started, a client can rely on the following:

1. **Order and coverage.** Blocks arrive at consecutive heights in the requested direction. A stream that ends `OK` sent every requested height exactly once.
2. **Every link holds.** Ascending, each block's `prevHash` equals the hash of the block sent before it. Descending, each block's hash equals the `prevHash` of the block sent before it.
3. **One chain as read.** An ascending stream that crosses a reorg above its last sent block continues on the new branch. A descending stream continues on the chain it started on. An `OK` stream does not promise its blocks are still the best chain on arrival.
4. **Reorg ending.** A stream ends with `ABORTED` when a reorg replaces a block it already linked to (ascending) or the next height it must send (descending), as lightwalletd v0.5.5 and later do. It ends with `OUT_OF_RANGE` when a reorg leaves the tip below a height it still must send. To recover, request again from the height the wallet needs.
5. **Settled history never ends a stream.** Ingest never replaces a block at or below the settled tip, so only the last reorg window (1,000 blocks by default) can end a stream early.
6. **Other mid-stream endings.** A storage failure ends the stream with `UNAVAILABLE`. A descending range that runs below retained history, or, in wallet serving, a reorg that lowers the tip during a chunk read, ends it with `NOT_FOUND`.
7. **No time limit.** Elapsed time, new blocks, and slow reading never end a stream. Shutdown, unanswered keepalive pings, and the listener's send-stall deadline do.

A chunk holds at most 1,000 blocks and at most 2 MiB of stored compact blocks, counted before pool filtering; a single larger block forms its own chunk. The server reads ahead by up to two chunks at the start of a stream, and afterwards by one chunk plus 16 queued blocks in wallet serving or 8 in compact-blocks serving. A waiting stream holds no chain epoch, so a slow client never delays new blocks.

`compat.max_concurrent_block_range_streams` (`ZINDER_COMPAT__MAX_CONCURRENT_BLOCK_RANGE_STREAMS`) limits how many `GetBlockRange` and `GetBlockRangeNullifiers` streams the process serves at once. It defaults to 100 and accepts 1 to 65,536.

- A stream holds its slot from validation until it ends, fails, or is cancelled. A valid request past the limit fails with `RESOURCE_EXHAUSTED` before any read. `zinder_compat_block_range_stream_refusals_total` counts refusals, and `zinder_compat_block_range_streams_open` reports open streams.
- The Zcash Android SDK retries a refused batch. Other clients should retry `RESOURCE_EXHAUSTED` with backoff.
- Every client shares the limit. A client that opens the limit's worth of streams and stops reading holds every slot until it disconnects. Set a per-client limit on concurrent requests at your proxy. The shipped Caddy template does not include one.

The memory a stream uses beyond its stored bytes has no enforced maximum and has not been measured. Size the limit to the memory left after the block caches in [Storage backend](../architecture/storage-backend.md), and watch container memory while `zinder_compat_block_range_streams_open` is near it. lightwalletd sets no stream limit.

### `GetTreeState` and `GetLatestTreeState`

`GetTreeState` answers every retained height from Zinder's store, where lightwalletd asks its node. It returns the same statuses as native `TreeStateAtHeight`, listed in [Wallet data plane](../architecture/wallet-data-plane.md#tree-states-and-subtree-roots), except that a height above the visible tip is `INVALID_ARGUMENT`, as lightwalletd returns.

Zinder bounds concurrent tree-state rebuilds, where lightwalletd has no rebuild limit. Retry a `RESOURCE_EXHAUSTED` from `GetTreeState` with backoff. `GetLatestTreeState` returns the newest stored checkpoint at or below the visible tip.

### `GetSubtreeRoots`

lightwalletd treats `maxEntries = 0` as "every root". Zinder returns at most 1,000 roots for `maxEntries = 0`, and a client reads the rest by paging with `startIndex`. A positive `maxEntries` above 1,024 fails with `INVALID_ARGUMENT`. `completingBlockHash` is in RPC byte order, as lightwalletd sends it.

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

### `GetTaddressTxids` and `GetTaddressTransactions`

Both methods resolve `range` the way lightwalletd does on Zebra:

- `range` and `range.start` are required; a missing one is `INVALID_ARGUMENT`.
- An absent `range.end`, or an end of 0, means the visible tip.
- Both bounds are clamped to the visible tip. A start of 0 reads from genesis.
- An end below the start is `INVALID_ARGUMENT`.

lightwalletd rejects a span wider than 10,000,000 blocks. Zinder clamps the end to the tip and serves it, because wallets such as WebZjs send `end = u64::MAX`.

All reads of one request are pinned to one epoch. If the request is still reading one minute after a new block replaces that epoch, it fails with `CHAIN_EPOCH_PIN_UNAVAILABLE`. The server reads the whole history in the range, with every transaction's bytes, before it sends the first one, so memory grows with the address's history.

### `GetTaddressBalance` and `GetTaddressBalanceStream`

Both methods return the confirmed balance, the sum of unspent outputs, of up to 256 distinct addresses at one chain epoch. A repeated address counts once. Mempool state is not included.

- An empty list is `INVALID_ARGUMENT`.
- The 257th distinct address is `INVALID_ARGUMENT` with `TRANSPARENT_BALANCE_ADDRESS_COUNT_EXCEEDED`. The streaming method refuses as soon as that address arrives, without waiting for the client to finish. lightwalletd bounds the streaming method at 10,000 addresses.
- An invalid address, an address for another network, or address text longer than 64 bytes is `INVALID_ARGUMENT`.
- A total above the protocol's `int64` field is `OUT_OF_RANGE`.

### `GetAddressUtxos` and `GetAddressUtxosStream`

Both methods follow lightwalletd's `maxEntries` rule: 0 returns every unspent output at or above `startHeight`, and a positive value returns at most that many, oldest first. Within one block, Zinder orders rows by transaction id and output index, where Zebra uses block position; this matters only when `maxEntries` splits a block.

One response returns at most 100,000 outputs, the same cap Zebra puts on `getaddressutxos`. A request that would return more fails with `RESOURCE_EXHAUSTED`. Zinder never drops rows silently, because wallets add these rows up as the address balance. lightwalletd on Zebra instead returns `OK` with an empty list in this case.

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

- `lightwallet-protocol` `v0.5.0` (`ac7cee052a1bf5d430985a478d39e8b513fc4bd4`);
- `lightwalletd` `v0.6.0` (`e7436d2adb9ab5828eb8a27de534e16218841fc2`) as the source reference, which parses the version-6 transactions that NU6.3 introduced;
- `electriccoinco/lightwalletd:v0.6.0@sha256:abc8199dae800e050415a3459f71c7850aeefeb1ca6a681e9b9eda7249ce8613`;
- `docker.io/zfnd/zebra:7.1.0@sha256:4f9536c8061e407f9f2ec81b7e0dd2f1762f81deec316abb1530dca8796a33ba`, started on Regtest with `deploy/zebra/regtest.toml`, which activates every upgrade through NU7 (see [Regtest through NU7](../runbooks/testing.md#regtest-through-nu7));
- `docker.io/zakuracore/zakura:1.8.6@sha256:e514c89b751b685686690098928507aaa994311e6b90c79373c55b0afaa45d50`, started on Regtest with `deploy/zakura/regtest.toml`, which uses the same schedule.

A release needs the parity suite to pass with both node images. The test setup is in [Testing](../runbooks/testing.md).
