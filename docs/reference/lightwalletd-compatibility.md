# Lightwalletd compatibility

This page is the reference for how `zinder-compat-lightwalletd` answers the lightwalletd `CompactTxStreamer` protocol, and where its behavior differs from Go lightwalletd. It is for wallet developers who point an existing lightwalletd client at Zinder, and for operators who run the compatibility runtime.

Wallet serving reads the stored data described in [Wallet data plane](../architecture/wallet-data-plane.md). The light-wallet profile combines canonical data with node-backed reads.

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

Select the [light-wallet profile](configuration.md#light-wallet-profile) to configure ingest and compatibility together, or select a serving mode with `compat.serving`, `ZINDER_COMPAT__SERVING`, or `--serving`. The default without a profile is `wallet`.

- `wallet` serves every method in the table below. It reads the canonical store and the wallet store as one consistent pair. It requires retained transaction blobs or a verified primary node byte source. Balance, UTXOs, and transparent history selection use the local wallet projection. In lean wallet mode, `GetTransaction` and the raw bodies in compatibility history responses use digest-verified mined bytes from the node or its in-memory cache. Mempool methods use ingest control.
- `compact-blocks` serves `GetLightdInfo`, `GetLatestBlock`, `GetBlock`, and `GetBlockRange` from the canonical store and answers them as wallet serving does, except that `taddrSupport` is `false`. Every other method except `Ping` returns `UNIMPLEMENTED`. It never contacts a node, so it refuses to start with a `[wallet]` section, a `[node]` or `[node_fallbacks.N]` section, `--node-json-rpc-addr`, or any `ZINDER_NODE__*` or `ZINDER_NODE_FALLBACKS__*` variable.
- `light-wallet` serves the wallet methods from the canonical store, ingest control, and one configured node. The node handles transparent address reads, supplies omitted transaction bodies, accepts broadcasts, and provides the recommended fee. See [During a node outage](readiness.md#during-a-node-outage) for the behavior of the listener and its dependencies.

By default, `Ping` is refused with `FAILED_PRECONDITION`, as reference lightwalletd refuses it. Setting `compat.ping_very_insecure = true` (`ZINDER_COMPAT__PING_VERY_INSECURE`, or `--ping-very-insecure`) makes it answer as `lightwalletd --ping-very-insecure` does. The response's `entry` and `exit` report the calls in flight, and the call sleeps `interval_us` microseconds, wrapping on overflow as Go's `time.Duration` does. Each call holds a server task as long as the client asks, so enable it only on a server under load test. A call still sleeping at shutdown ends with `UNAVAILABLE`.

## Methods

Light-wallet serving supports the methods in the Wallet serving column, with node-backed reads as described above.

| Method | Wallet serving | Compact-blocks serving | Notes |
| --- | --- | --- | --- |
| `GetLightdInfo` | Served | Served | `taddrSupport` is `true` in wallet and light-wallet serving |
| `GetLatestBlock` | Served | Served | Hash in internal byte order |
| `GetBlock` | Served | Served | Height 0 with an empty hash is `INVALID_ARGUMENT` |
| `GetBlockNullifiers` | Served | `UNIMPLEMENTED` | Shielded pools, nullifiers only |
| `GetBlockRange` | Served | Served | Ascending or descending; see below |
| `GetBlockRangeNullifiers` | Served | `UNIMPLEMENTED` | Returns nullifiers for the requested shielded pools |
| `GetTransaction` | Served | `UNIMPLEMENTED` | Pending transactions at height 0 |
| `SendTransaction` | Served when `[node]` is set | `UNIMPLEMENTED` | |
| `GetTaddressTxids`, `GetTaddressTransactions` | Served | `UNIMPLEMENTED` | Same behavior for both |
| `GetTaddressBalance`, `GetTaddressBalanceStream` | Served | `UNIMPLEMENTED` | At most 256 distinct addresses. [Browser wallets](integration-surfaces.md#browser-wallets) use the unary `GetTaddressBalance`. |
| `GetMempoolTx` | Served | `UNIMPLEMENTED` | |
| `GetMempoolStream` | Served | `UNIMPLEMENTED` | |
| `GetTreeState` | Served | `UNIMPLEMENTED` | Retained heights at or after Sapling activation |
| `GetLatestTreeState` | Served | `UNIMPLEMENTED` | |
| `GetSubtreeRoots` | Served | `UNIMPLEMENTED` | `maxEntries = 0` streams every remaining root |
| `GetAddressUtxos`, `GetAddressUtxosStream` | Served | `UNIMPLEMENTED` | At most 100,000 outputs per response |
| `GetStandardFee` | Served when a configured node implements `getstandardfee` | `UNIMPLEMENTED` | See [`GetStandardFee`](#getstandardfee) |
| `Ping` | Refused unless enabled | Refused unless enabled | See above |

Local wallet reads that span several lookups use the chain epoch captured by the handler. Block-range streams follow the chain under the continuity rules below. Light-wallet's forwarded reads follow the method-specific checks below and the admission rules in [Node-forwarded reads](#node-forwarded-reads). A runtime that is not ready rejects new requests with `UNAVAILABLE` and `SERVICE_NOT_READY`, as [Readiness](readiness.md) describes.

## Node-forwarded reads

Some methods read data the store does not hold from the configured `[node]`. Which methods do this depends on the serving mode and on whether the store retains transaction bodies.

| Method | `wallet` with retained bodies | `wallet` without bodies | `light-wallet` without bodies |
| --- | --- | --- | --- |
| `GetLatestBlock`, `GetLightdInfo`, `GetBlock`, `GetBlockNullifiers`, `GetBlockRange`, `GetBlockRangeNullifiers`, `GetTreeState`, `GetLatestTreeState`, `GetSubtreeRoots` | Local | Local | Local |
| `GetMempoolTx`, `GetMempoolStream` | Ingest control | Ingest control | Ingest control |
| `GetTransaction` for a mined transaction | Local | Forwarded body | Forwarded body |
| `GetTaddressTxids`, `GetTaddressTransactions` | Local | Local rows, forwarded bodies | Forwarded IDs and bodies |
| `GetTaddressBalance`, `GetTaddressBalanceStream`, `GetAddressUtxos`, `GetAddressUtxosStream` | Local | Local | Forwarded |
| `SendTransaction`, `GetStandardFee` | Node | Node | Node |

A `light-wallet` store that retains transaction bodies serves mined `GetTransaction` locally and forwards only the address methods.

### Admission

Each process bounds its forwarded reads in two layers.

The first layer is a budget of node calls, `compat.forwarded_read_max_concurrent` (32 by default). A forwarded read holds a slot only while one node call runs. It never holds a slot while a client reads its response. Each kind of read has a reserved floor and a cap, and the remaining slots are shared:

| Kind | Reserved at 32 slots | Cap at 32 slots |
| --- | --- | --- |
| Transaction bodies, for `GetTransaction` | 12 | 24 |
| Address UTXO and balance lookups | 4 | 8 |
| Address history pages and history row bodies | 4 | 16 |

Twelve slots are shared at the default. Other budgets scale the same proportions, and every kind keeps at least one reserved slot from three slots up. Address history therefore cannot take the slots that transaction lookups reserve.

The second layer bounds what a forwarded response holds while the client reads it. At most 16 unspent-output responses (with 1,024 waiting places) and 32 address-history streams (with 128 waiting places) are open at once. A request takes its response slot before any node-call slot.

A request that finds its kind's slots taken waits in arrival order for up to `compat.forwarded_read_wait_ms` (10,000 ms by default). Each kind has `compat.forwarded_read_waiters_per_slot` waiting places per slot of its cap (64 by default). When the wait ends, or when every waiting place is taken, the request fails with `UNAVAILABLE`, which wallets retry. This is the same code a full block-range or tree-state rebuild limit returns. `RESOURCE_EXHAUSTED` stays reserved for size and row limits that a retry cannot pass. A client deadline shorter than the wait ends the wait at that deadline. A request whose client leaves gives up its place at once. Native `WalletQuery` refuses the same reads with `RESOURCE_EXHAUSTED` and `FORWARDED_READ_LIMIT_REACHED`.

`compact-blocks` serving forwards nothing and refuses these settings. The admission metrics are listed in [Metrics](metrics.md#forwarded-reads).

### Verified transaction bytes

Mined transaction bodies read from the node pass two checks. The node must report the transaction in the block the local `transaction_location` record names. The bytes must match the digest in that record. Verified bodies go into a cache of `compat.forwarded_transaction_cache_bytes` (64 MiB by default, 0 disables it), shared by `GetTransaction` and address history rows. A cached body is served without a node call or a slot. Concurrent lookups of the same transaction and admission kind share one node call, and a waiting lookup takes verified bytes another kind caches while it waits. A lookup that leaves does not cancel the call for the others. Failed lookups are never cached.

### Served-tip checks

Every forwarded read first confirms that the node's network-upgrade schedule matches the served one. That check is shared by concurrent requests and kept for up to a minute. Whether the read also checks the served tip with `getblockhash` depends on whether the node's reply proves which chain it came from:

| Method | Separate tip check |
| --- | --- |
| `GetTransaction` for a mined transaction | None. The reply names the mining block, and the bytes match the recorded digest. |
| `GetTransaction` for a transaction the served chain does not hold | Before the mempool answers |
| `GetAddressUtxos`, `GetAddressUtxosStream`, `GetTaddressBalance`, `GetTaddressBalanceStream` | None. Each `getaddressutxos` page reports the tip it answered at, and it must be the served tip. If the node answers ahead of the served tip, the listener releases any response slot and [waits briefly](#waiting-for-a-height-just-past-the-tip) before retrying once; if it still mismatches or pages disagree, it refuses with `UNAVAILABLE`, `SERVED_TIP_MISMATCH`, and `grpc-retry-pushback-ms: 1000`. |
| `GetTaddressTxids`, `GetTaddressTransactions` | Before and after each page of IDs, and once at the end of the stream |
| `SendTransaction`, `GetStandardFee` | Before the call |

Concurrent checks of the same tip share one `getblockhash` call, and its answer is not reused after it returns. A failed tip check fails only the requests that waited on it. [ADR-0003](../adrs/0003-epoch-bound-reads-and-storage-roles.md) Decision 7 records why these checks keep forwarded answers inside the served chain view.

### Retaining transaction bodies locally

`GetTransaction` and address history rows are the forwarded reads with the highest volume, because wallets fetch every transaction they find while scanning. A public deployment with many concurrent wallets, or a node that is remote, pruning, or shared with other workloads, can serve them locally instead. Build the canonical store with `storage.raw_blob_policy = "transactions"` and select `compat.serving = "light-wallet"` or `"wallet"` explicitly, or use the `wallet-archive` profile. The bodies take about 251 GiB on Mainnet. The policy is fixed when the store is built, so an existing store must be rebuilt to retain them. See [Configuration](configuration.md#deployment-profiles).

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
| `taddrSupport` | `true` in wallet and light-wallet serving, `false` in compact-blocks serving. Fixed when the process starts. |

The upgrade fields come from the network-upgrade schedule that ingest admitted, never from a node. A newly admitted upgrade shows as pending until the tip reaches it. A store without an admitted schedule is not served: the process retries until `storage.initial_catchup_timeout_ms` passes, then exits.

`GetLatestBlock` returns the visible tip height and its hash in internal byte order, the reverse of the hex Zebra's JSON-RPC prints. Compare hashes across servers only at equal heights.

#### Stale tip

`estimatedHeight` is the height Zebra would report as `estimatedheight` for the served tip: the tip, plus the blocks the target spacing would have produced since the tip's block time. Each era uses the spacing of its network upgrade in the admitted schedule: 150 seconds before Blossom, 75 seconds from Blossom, and 25 seconds from NU7. Go lightwalletd forwards Zebra's value, which Zebra computes the same way from its own tip. During a node outage the served tip stands still and `estimatedHeight` keeps rising.

An old tip is not always a stale one: the network may simply not have produced a block. So `GetLatestBlock` and `GetLightdInfo` fail with `UNAVAILABLE` only when both hold:

- The served tip is older than `compat.stale_tip_limit_ms` (`ZINDER_COMPAT__STALE_TIP_LIMIT_MS`). Its age counts from its block time, or from when this process first served it since it started if that is earlier, so a block time ahead of the local clock, which consensus accepts, delays a refusal only until the first tip call after the tip changes or the process restarts. The age is taken after any read of the tip's header the call makes, so it is the age the tip has when the call answers.
- The writer cannot vouch that the served tip is the network's tip: its node is unreachable, the node reports that its peers show it behind the network, the node has a block above the served tip because the writer is behind it (catching up or restored from an old snapshot), the writer is starting or parked, or ingest control does not answer.

While the writer's node is reachable, reports itself in sync, and has no block above the served tip, the tip calls answer however old that tip is, and `estimatedHeight` tells wallets the chain is quiet. Go lightwalletd fails these calls only while its node is down, and keeps answering while its node is reachable but behind the network.

A node counts as behind the network when Zebra's `/ready` reports `syncing`, `insufficient peers`, or `no tip`, which come from its peers and its syncer. Zebra's other not-ready reports, `tip_age=…` and `lag=…`, and both reports of the JSON-RPC health fallback are computed from how old the node's tip is. A quiet chain raises them too, so they do not stop the node vouching, and a node that has stalled but keeps one peer, which reports only `tip_age=…`, looks the same as a quiet chain. On Testnet and Regtest, Zebra's `/ready` reports ready unless `enforce_on_test_networks` is set. Without a node health probe, only the node's reachability and its tip count.

The process reads the writer's status from ingest control every `storage.secondary_catchup_interval_ms`, 1 second by default. A writer that reaches its node reports the node's committed tip there and reports none while the node is unreachable. A reading vouches for 30 seconds or three reading intervals, whichever is longer, so a stopped ingest control stops vouching within that time. Readings that fail or report no node tip stop the vouching once they have lasted 5 seconds or two reading intervals, whichever is longer, and never more than a minute, so one failed reading does not refuse a quiet chain's tip unless readings are a minute or more apart. Once the node reports a block above a tip, that tip keeps vouching for 10 seconds or three reading intervals, whichever is longer, while the reader catches up, and further blocks in that time do not shorten it. The tip calls answer again as soon as the node reports the served tip or a newer tip is served. A stale-tip refusal can occur while `/readyz` returns 200. `zinder_compat_stale_tip_refusals_total{method}` counts the refusals.

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

### Waiting for a height just past the tip

Readers behind a load balancer publish each block at slightly different times, and the balancer routes every request on its own. A wallet can learn tip H+1 from one reader, or a forwarded node can advance to H+1, while a reader is still at H. Instead of failing at once, Zinder waits briefly for the height to arrive. For how this fits a multi-reader deployment, see [More than one reader on a host](../architecture/service-operations.md#more-than-one-reader-on-a-host).

These methods wait when they select a height:

- `GetBlock` and `GetBlockNullifiers`, by height or by height and hash.
- `GetBlockRange` and `GetBlockRangeNullifiers`, and the encoded block range methods `GetEncodedBlockRange` and `GetEncodedBlockRangeNullifiers`. A range waits on its higher end, whichever end the request names first.
- `GetTreeState` by height, in the serving modes that answer it: wallet and light-wallet serving.

In light-wallet serving, forwarded `GetAddressUtxos`, `GetAddressUtxosStream`, `GetTaddressBalance`, and `GetTaddressBalanceStream` calls also wait when the node answers at a tip height above the served tip. A waiting call drops any held forwarded-read permits first, waits up to the shorter of `compat.tip_wait_ms` and the request's remaining forwarded-read wait budget, then recaptures the served view and retries the node lookup at most once. Multi-page UTXO disagreement at the same tip and non-advancing forks fail at once without waiting.

A hash-only selector never waits and keeps its immediate `NOT_FOUND`. `GetLatestBlock`, `GetLightdInfo`, `GetLatestTreeState`, `GetTransaction`, `SendTransaction`, the mempool methods, `GetTaddressTxids`, `GetTaddressTransactions`, wallet-serving transparent-address reads, `GetSubtreeRoots`, and native `WalletQuery` never wait.

A request waits only when its height is above the visible tip by at most `compat.tip_wait_max_blocks` blocks (default 2). It waits up to `compat.tip_wait_ms` milliseconds (default 3000, and 0 turns the wait off). At most `compat.tip_wait_max_waiters` requests (default 256) wait at once, and a request that finds them all taken answers at once. [Configuration](configuration.md) lists the settings and their limits. The default wait is provisional until cross-region skew has been measured, and operators can raise it.

A waiting request wakes each time the reader publishes a new tip and checks the fresh tip, with no polling:

- When the height arrives, the method runs as if it had been there from the start.
- When the wait expires, block and tree-state methods answer with their usual status against the fresh tip: `OUT_OF_RANGE` with `block N is newer than latest block M` for block reads, and `INVALID_ARGUMENT` for `GetTreeState`. There is no new status or error reason for those methods. Forwarded UTXO and balance reads refuse with `UNAVAILABLE`, `SERVED_TIP_MISMATCH`, and `grpc-retry-pushback-ms: 1000` without calling the node again (and refuse the same way if the single retry still mismatches).
- When a publication moves the tip down so that the height is outside the window, the wait ends at once.
- When the reader adopts a different block at the awaited height, a request by height alone serves the local block. A request by height and hash gets the usual `NOT_FOUND` with `requested block hash does not match indexed block`.
- When shutdown begins, every wait ends at once.
- When the client sets a `grpc-timeout` shorter than the wait, the request ends with `DEADLINE_EXCEEDED`. A public listener answers any expired `grpc-timeout` whose response has not started with `DEADLINE_EXCEEDED`, as the gRPC status-code specification and grpc-go servers such as lightwalletd do.

A waiting request holds no reader generation, serving pair, forwarded-read slot, or block-range stream slot, so waiting requests never use the stream or forwarded-read slots below. [Metrics](metrics.md#zinder-compat-lightwalletd) lists `zinder_compat_tip_wait_total`, `zinder_compat_tip_wait_duration_seconds`, and `zinder_compat_tip_waiters`.

### `GetBlock` and `GetBlockNullifiers`

A `BlockID` with a height reads that height. Zinder also accepts a 32-byte hash in internal byte order for these two methods. Height 0 with a hash selects the block by hash. Height 0 with an empty hash is an unspecified identifier and returns `INVALID_ARGUMENT`. A height above the visible tip returns `OUT_OF_RANGE` once the [tip wait](#waiting-for-a-height-just-past-the-tip) ends. A hash that is not in the best chain, or a hash that does not match the block at the given height, returns `NOT_FOUND`. A best-chain block whose compact block the store does not hold also returns `NOT_FOUND`, but with the [reason](error-vocabulary.md) `ARTIFACT_UNAVAILABLE`, where a hash that is not in the best chain carries `BLOCK_NOT_IN_BEST_CHAIN`.

`GetBlock` supports these lookups in every serving mode. In compact-blocks serving it returns the stored block without pool filtering. `GetBlockNullifiers` is available in wallet and light-wallet serving and returns `UNIMPLEMENTED` in compact-blocks serving.

Like Go lightwalletd v0.6.2, `GetBlockNullifiers` retains every stored transaction after clearing non-nullifier payloads. Transparent-only transactions, including a transparent-only coinbase, retain just `index`, `txid`, and `fee`. `GetBlockRangeNullifiers` selects transactions by shielded pool before clearing non-nullifier payloads. Both methods send a present `chainMetadata` submessage with all three commitment-tree sizes set to zero.

### `GetBlockRange` and `GetBlockRangeNullifiers`

`GetBlockRange` applies the requested `poolTypes` to every transaction and omits any transaction left with no spends, outputs, actions, or transparent inputs and outputs, as lightwalletd does. With no `poolTypes`, it returns the shielded pools, so a transparent-only coinbase is absent unless the request names the transparent pool. Each remaining transaction keeps its block position in `CompactTx.index`, so a gap marks an omitted transaction. A range with start above end is sent in descending order.

`GetBlockRangeNullifiers` returns nullifiers for the shielded pools listed in `poolTypes`. It removes `TRANSPARENT` first. If the list is then empty, it selects all shielded pools.

All three serving modes serve `GetBlockRange` pool selections that omit `TRANSPARENT`, and wallet and light-wallet serving serve every `GetBlockRangeNullifiers` pool selection, from [signed settled history and the standalone live-tail buffer](#signed-settled-history) when those cover the chunk, in both directions. Default shielded and nullifier selections, and `SAPLING`-and-`ORCHARD` selections whose highest requested height is proven below Ironwood activation on the admitted schedule, serve the stored frames directly without protobuf decoding or re-encoding; all other non-transparent pool subsets project protobuf wire fields from the authenticated stored frame (`Slab 1` for Sapling-omitting nullifier subsets, `Slab 0` otherwise) while sharing the same 16-read catalog budget, chunk sizing, 512 KiB sequential slab reads, and uncompressed 8 KiB / 32 KiB gRPC batching. Only `GetBlockRange` requests that include `TRANSPARENT` bypass the signed-read budget and read canonical storage with the normal chunk budget.

For both range methods, repeated pool values select a pool once, and unrecognized numeric values select no transactions. A list containing only unrecognized values still returns every requested block with an empty `vtx`. A mixed list selects the recognized pools.

A Sapling-only request includes transactions with Sapling outputs even when they have no Sapling spends. For those transactions, the response contains only the transaction’s position in the block, its ID, and its fee.

Vizor v0.0.65 requests only Ironwood nullifiers when it scans for Gift Card spends. Its ordinary wallet sync uses `GetBlockRange`, as does Slipstream in the ZODL 8.1.0 wallet SDKs. [Vizor Gift Card request](https://github.com/chainapsis/vizor-wallet/blob/d0edb5b13890dd33d9bb93b5c4e50fd9973fa4e6/rust/src/wallet/sync_engine/lwd.rs#L1813-L1837) · [Vizor sync request](https://github.com/chainapsis/vizor-wallet/blob/d0edb5b13890dd33d9bb93b5c4e50fd9973fa4e6/rust/src/wallet/sync_engine/lwd.rs#L1052-L1068) · [Slipstream sync request](https://github.com/zodl-inc/slipstream/blob/80f95741896543a22262182ab37a5a05d484d2e7/core/src/fetch.rs#L1125-L1145)

These outcomes fail the call before any block is sent:

| Condition | Status |
| --- | --- |
| Missing bound or height above `uint32` | `INVALID_ARGUMENT` |
| No block-range stream slot returned within 10 seconds, or the queue for one is full | `UNAVAILABLE` |
| An admitted request includes `POOL_TYPE_INVALID` (0) | `UNKNOWN` |
| Higher bound above the visible tip, once the [tip wait](#waiting-for-a-height-just-past-the-tip) ends | `OUT_OF_RANGE` |
| One of the first 1,000 heights the stream would send is outside retained history, including height 0 | `NOT_FOUND` |
| A block missing inside retained history, where the read of the first chunk reaches it | `UNAVAILABLE` |
| A stored block of the first chunk that a lightwalletd message cannot carry, such as a transaction fee above `uint32` | `DATA_LOSS` |

The tip wait happens before the first header is sent and before the request takes a stream slot, so a stream never sends a prefix and then waits. The longest delay before a range response's headers is the tip wait plus the 10-second slot wait below.

Once a stream has started, a client can rely on the following:

1. **Order and coverage.** Blocks arrive at consecutive heights in the requested direction. A stream that ends `OK` sent every requested height exactly once.
2. **Every link holds.** Ascending, each block's `prevHash` equals the hash of the block sent before it. Descending, each block's hash equals the `prevHash` of the block sent before it.
3. **One chain as read.** An ascending stream that crosses a reorg above its last sent block continues on the new branch. A descending stream continues on the chain it started on. An `OK` stream does not promise its blocks are still the best chain on arrival.
4. **Reorg ending.** A stream ends with `ABORTED` when a reorg replaces a block it already linked to (ascending) or the next height it must send (descending), as lightwalletd v0.5.5 and later do. It ends with `OUT_OF_RANGE` when a reorg leaves the tip below a height it still must send. To recover, request again from the height the wallet needs.
5. **Settled history never ends a stream.** Ingest never replaces a block at or below the settled tip, so only the last reorg window (1,000 blocks by default) can end a stream early.
6. **Other mid-stream endings.** A storage failure, including a block missing inside retained history that only a later read reaches, ends the stream with `UNAVAILABLE`. A chunk with a stored block that a lightwalletd message cannot carry ends it with `DATA_LOSS`, before any block of that chunk is sent. A descending range that runs below retained history ends it with `NOT_FOUND`, and so can, in wallet serving, a reorg that lowers the tip during a chunk read.
7. **No time limit.** Elapsed time and new blocks never end a stream. Shutdown and unanswered keepalive pings do, and so does the send-stall deadline. The deadline falls once one piece of the stream has waited 60 seconds to go out: for the wallet's window to cover everything queued ahead of it plus one byte, which in the worst case takes a whole piece, 64 KiB, of new window, or for the server to write out the data ahead of it on a slow connection or one shared with other busy streams. Slow reading ends a stream only this way. The deadline also applies to a finished stream whose last blocks still wait to go out. At that deadline the stream's work stops, its range-stream slot returns once a read already in flight finishes, and the server resets its HTTP/2 stream. The server discards the data it holds for the stream the next time it writes to the connection. A wallet that reads again receives the blocks that reached it, then the reset: clients built on grpc-go, grpc-java, and tonic with its `server` feature report `CANCELLED`, and gRPC Swift on SwiftNIO reports `UNAVAILABLE`. Other clients, or a proxy in front of Zinder, may report it differently. To recover, request again from the height the wallet needs ([Public gRPC listeners](configuration.md#public-grpc-listeners)).

A chunk holds at most 1,000 blocks and at most 2 MiB of stored compact blocks, counted before pool filtering; a single larger block forms its own chunk. The first chunk holds at most 256 KiB, so the stream opens after a small read. A thousand blocks of up to 262 stored bytes each fit in it, so a request for such small blocks is read in one chunk, and a denser range opens after its first few blocks.

The read of the first chunk checks all of the first 1,000 heights against retained history. It reaches the blocks of the first chunk and, unless that chunk holds all of those heights or at least 256 KiB, the block after it, which the read looks at to learn that it does not fit. A first chunk holds that much when its blocks fill exactly 256 KiB or when it holds a single larger block. It converts only the blocks of the first chunk. That is why the table above covers retained history for the first 1,000 heights, a missing block only where the first read reaches it, and an unconvertible block only in the first chunk. Past them, these end the stream as item 6 describes.

Each later read is offered up to 1,000 heights from the next height to send, so every chunk can fill its 2 MiB. A read checks every height it is offered against retained history before it keeps any. When a read finds a height outside retained history while heights an earlier read checked are unsent, the server reads those heights alone, so the stream sends every height a read has checked before it ends with `NOT_FOUND`.

The adapter holds at most two chunks of a stream: the one it is handing to the gRPC server and the next, which it reads meanwhile. It reads a third only once the gRPC server has taken every block of the first. The gRPC server takes blocks while the connection can send them, so encoded blocks the wallet has not received can also wait in the server's send buffers and the connection's HTTP/2 flow-control window. A waiting stream holds no chain epoch, so a slow client never delays new blocks. When the client goes away, the server finishes the read in flight and starts no other.

`compat.max_concurrent_block_range_streams` (`ZINDER_COMPAT__MAX_CONCURRENT_BLOCK_RANGE_STREAMS`) limits how many `GetBlockRange` and `GetBlockRangeNullifiers` streams the process serves at once. It defaults to 100 and accepts 1 to 65,536.

- A stream holds its slot from validation until it ends (after yielding its last message or a terminal error and finishing any chunk read in flight), fails, or is cancelled—before the transport drops the response body.
- A valid request past the limit waits for a slot, first come first served, for up to 10 seconds, and reads nothing while it waits. If no slot returns in time, it fails with `UNAVAILABLE`. lightwalletd waits and fails the same way.
- At most three times as many requests wait as the limit has slots, 300 by default (holding no chunk buffers while queued). A request that finds the queue full fails with `UNAVAILABLE` at once. A client that cancels, disconnects, or reaches its deadline while it waits gives up its place.
- The Zcash Android SDK, Slipstream, and Vizor retry a failed block-range request after a short backoff, so an occasional refusal delays a sync. Repeated refusals can still end a sync attempt.
- `zinder_compat_block_range_streams_open` reports open streams, `zinder_compat_block_range_slot_wait_duration_seconds` how long requests waited, and `zinder_compat_block_range_stream_refusals_total` the refusals, as [Metrics](metrics.md#zinder-compat-lightwalletd) lists.
- Every client shares the limit and the queue. A client that opens the limit's worth of streams and stops reading holds every slot until the send-stall deadline ends those streams, and one that also sends three times as many more requests fills the queue. Set a per-client limit on concurrent requests at your proxy. The shipped Caddy template does not include one.

With the default of 100 active slots and 300 queued waiters, a listener admits up to 400 concurrent block-range clients (100 active plus 300 queued) without immediate `waiters_full` refusals while bounding active chunk-buffer memory to 100 streams. That 400-client figure is admission capacity rather than a throughput guarantee: queued requests still fail with `UNAVAILABLE` (`wait_timeout`) if they wait longer than the 10-second admission timeout while the 100 active slots stay occupied. For public endpoints with sustained higher concurrency or rising `zinder_compat_block_range_slot_wait_duration_seconds` or `zinder_compat_block_range_stream_refusals_total`, raise `compat.max_concurrent_block_range_streams` (for example to 200) within available container memory, or scale out horizontally behind a load balancer. Under cgroup v2, `zinder-compat-lightwalletd` charges OS page cache for `ZSHRNG01` segments against `ZINDER_COMPAT_MEMORY_LIMIT`, so on 64+ GiB Mainnet hosts with sustained high-concurrency range syncs, raise `ZINDER_COMPAT_MEMORY_LIMIT` above the catalog size plus working set (see [What you need](../runbooks/deploying.md#what-you-need)) to avoid disk re-read thrashing and `wait_timeout` refusals.

A stalled stream keeps its range-stream slot until the server ends it at the send-stall deadline, unless its server side has already finished. Data the server still holds for finished streams counts against the per-connection bound in [Public gRPC listeners](configuration.md#public-grpc-listeners), not this limit, so this limit does not bound that data across the process. Other memory a stream uses beyond its stored bytes has no enforced maximum and has not been measured. Size the limit to the memory left after the block caches in [Storage backend](../architecture/storage-backend.md), and watch container memory while `zinder_compat_block_range_streams_open` is near it. lightwalletd v0.6.2 limits block-range requests with `--max-concurrent-blockrange`, 200 by default.

<a id="signed-settled-history"></a>

#### Signed settled history and standalone live-tail buffer

All three serving modes can answer every non-transparent `GetBlockRange` pool selection, and wallet and light-wallet serving can answer every `GetBlockRangeNullifiers` pool selection, from signed segments and an in-memory tail, including before the first signed segment is available. Each cached tail row keeps only its two pre-encoded default frames, the shielded and the nullifier one. Default shielded and nullifier selections, and `SAPLING`-and-`ORCHARD` selections whose highest requested height sits below Ironwood activation, use those stored frames directly. Other non-transparent pool subsets project protobuf wire fields on the fly from the authenticated stored frame: Sapling-omitting nullifier subsets (such as `[ORCHARD]`) project the nullifier frame (`Slab 1`), while full-payload subsets and Sapling-selecting nullifier subsets project the shielded frame (`Slab 0`) so Sapling output-only deposit shells are preserved. A chunk that reaches the visible tip can fill the tail. Ascending fills and prepends read at most the chunk’s ordinary byte budget, including the single oversized first-block exception. If the returned rows stop before the tip or the cached suffix, the listener sends them directly and leaves the shared tail unchanged. A chunk that reaches the lowest cached row can extend the tail downward by its own missing heights. Other chunks read canonical storage without changing the tail. A short request below the tail reads only the heights it asks for. A stream can cross from signed segments to the tail; a direct tail hit runs without blocking conversion, while wire-projected tail reads run on the blocking pool. `GetBlockRange` requests that include `TRANSPARENT` read canonical storage with the normal chunk budget and bypass the signed-segment permit queue.

Each tail belongs to the admitted generation that supplied its rows. The epoch, canonical sequence digest, tip, and retention bounds must also match. Two stores can agree on block hashes and still hold different compact bytes. Republishing the serving pair during a read discards the rows, even if its epoch identifier stays the same. A miss on a stale view reads canonical storage. A request waiting for another fill can wait up to the listener’s block-range admission timeout, then reads canonical storage. A zero timeout falls back immediately on contention. A view that becomes stale during a fill can finish its own request, while only the current view can update the shared tail. If the fill fails, canonical reads report any fault at the affected chunk.

The payload budget is 64 MiB per tail, with a maximum of 5,096 blocks, and counts each row's shielded and nullifier frames together. A single newest block larger than the byte budget is retained on its own. One request fills a given buffer at a time. During replacement, a full prior tail and a full new tail can together retain about 128 MiB of encoded payloads. The fill also processes one storage chunk at a time, up to 2 MiB of stored bytes or one larger block. Decoded objects, conversion buffers, and older payloads held by active streams add to process memory. A disconnect after a storage read skips conversion, including conversion still waiting for a blocking worker.

Up to 16 chunks can read signed segments concurrently per listener. A chunk counts as a signed read when its lowest height lies inside the catalog, or when a descending chunk spans the whole catalog. A later chunk also counts when the stream may retry only the heights it already checked and that retry would count. Other chunks use the live tail or ordinary canonical reader and keep their normal byte budget, even when all 16 signed reads are busy. The listener rejects a range above the visible tip with `OUT_OF_RANGE` before waiting for a chunk read. Additional reads wait in arrival order. If the first read waits, the listener checks the tip again when the read is admitted. Above 16 active streams the listener divides a 32 MiB native-byte allowance among them for each chunk, retaining the first-block exception for oversized rows. The listener reads short catalog frames sequentially into independent slabs of up to 512 KiB, while each frame of 32 KiB or more occupies its own slab. On uncompressed published and tail reads—both direct default selections and wire-projected shielded pool subsets—frames under 32 KiB are coalesced during the chunk read into 8 KiB opening and 32 KiB subsequent gRPC wire batches so the temporary slab buffer drops immediately, while a direct or passthrough frame of 32 KiB or more turns its stored length field into its gRPC message prefix and leaves the generated server as a slice of its slab (and a rewritten projected frame of 32 KiB or more leaves in its own buffer) instead of copying into tonic's encode buffer; single-message uncompressed streams batch shorter messages the same way. Behind a gateway that forwards whole HTTP/2 frames, the 8 KiB opening cap lets an idle connection deliver the first block in its first round trip unless the message that crosses 8 KiB is longer than about 4.6 KB. The send-stall bound hands each response chunk to HTTP/2 in pieces of at most 64 KiB. For a `GetBlockRange` or `GetBlockRangeNullifiers` response these pieces are slices, not copies, because every chunk of that response keeps at most 512 KiB, and at most twice its own bytes, alive: a batch of short messages owns an allocation of under 64 KiB, a large message is a slice of its slab, and a row whose slab would exceed 512 KiB is copied into 64 KiB pieces. The largest Mainnet row so far is about 260 KiB. Binary gRPC-Web passes those slices on unchanged, and base64 gRPC-Web re-encodes each into a buffer of its own. Every other method keeps copied pieces. The send-stall bound keeps the unsent rest of the current chunk, and with it that chunk's slab, until it hands out the last piece, the stream is abandoned at the send-stall timeout, or its response is dropped.

A stalled stream's pieces stay with the transport until the send-stall deadline resets the stream. Before the reset, the stream's own queues hold at most about 1,216 KiB of its response data: up to HTTP/2's 1 MiB send buffer, one piece queued past it, one piece waiting for capacity, and the next piece the listener has read ahead. These figures leave out the frame HTTP/2 has already handed to its write buffer, which is shared by the connection's streams and holds one frame per connection rather than per stream, and the blocks the range producer holds before the listener reads them. With copied pieces a stalled stream pins those bytes in up to about 1,280 KiB of buffers, because a partly sent piece still owns its whole 64 KiB buffer. A batch of short messages is copied into an allocation of exactly its length. With sliced range pieces it pins the allocations behind them: messages fully inside that window pin at most twice their bytes, and the two messages at its edges pin at most 512 KiB each, so a stalled range stream pins at most about 3,456 KiB, plus allocator size-class rounding, and one connection's 200 streams about 675 MiB. The reset releases the stream's range-stream slot and its pieces: HTTP/2 discards the stream's queue the next time it writes to the connection, and a connection that cannot write is closed. A finished or paused stream is covered too, because the listener ends a response, or waits for more blocks, only once HTTP/2 has encoded everything it sent. Settled-tip anchor checks share work through a 16-tip LRU cache. A disconnect signals cooperative cancellation for queued blocking reads.

To publish, configure the network, canonical storage paths, and reorg window for the READY canonical store, and supply an Ed25519 private key in DER PKCS#8 format. The publisher uses compact history and accepts every raw-blob retention policy. It can run with the light-wallet profile without a node or ingest-control endpoint. Keep the signing key outside the catalog directory. The publisher starts at the first retained height, which is 1 for a complete store or one above the checkpoint for a checkpointed store. To start later, pass the same `--first-height` on every run.

```sh
zinder-compat-lightwalletd --config reader.toml publish-shielded-history \
  --directory /srv/zinder/published-history --signing-key /etc/zinder/publisher.pk8 \
  --segment-blocks 4096 --workers 4
```

Add `--print-config` to this invocation to inspect the publisher's effective
network, storage paths, RocksDB budget, and reorg window without opening storage.

`--workers` defaults to `min(available CPUs, 4)` (accepted range `1..=16`). Initial bulk catchup builds complete settled segments across those worker threads and commits signed catalog updates in bounded batches, while subsequent `--follow-interval-secs` polls commit each newly settled segment incrementally. Pass `--workers 1` on constrained shared-disk hosts that need sequential disk I/O; every worker setting produces identical segments and signed manifests.

The command reserves one directory under `storage.secondary_path` for each catalog and canonical store pair. On restart it replaces the previous secondary under that reservation, including after a forced stop. It publishes complete segments through its captured settled tip and exits. Add `--follow-interval-secs 60` to keep the publisher running and catch up its secondary every minute. If writer compaction removes a table file the publisher still needs, the next interval opens a fresh secondary and resumes from the last published segment under the same catalog lock. The listener can serve the recent unpublished suffix from its encoded tail while the chain view matches. It reads other uncovered heights from canonical storage. The [single-host Compose runbook](../runbooks/deploying-on-a-vm.md#publish-signed-wallet-history) covers installation and key rotation.

One publisher holds the directory lock. On restart it reclaims its temporary files, `.publisher-write.tmp`, `.publisher-segment.tmp`, and `.publisher-nullifiers.tmp` (including per-worker `.tmp.<slot>` suffixes), and verifies the existing segments and links before appending. The publisher stages each segment's nullifier slab in `.publisher-nullifiers.tmp` (or `.publisher-nullifiers.tmp.<slot>`) and streams it into the segment when the segment completes, so its memory does not grow with `--segment-blocks`. Keep its signed `.publisher-head.json` with the publication directory when restarting or restoring it. A missing checkpoint beside an existing catalog, a regressed prefix, or an incompatible chain stops publication.

Single-host Compose points `compat.shielded_history.directory` at the publisher's read-only catalog and mounts its raw 32-byte Ed25519 public key separately. The reader authenticates the signed publisher checkpoint before binding. An empty checkpoint lets it serve ranges from canonical storage and the standalone live tail until the first catalog arrives. A populated checkpoint requires a signed catalog containing that prefix. The listener adopts the first verified catalog and later extensions on the same wallet connection.

For a separately distributed snapshot, install the public key through a trusted channel. Copy the referenced `segment-*.bin` files to the reader before atomically installing their signed `current.json`. Select the existing snapshot by its manifest:

```toml
[compat.shielded_history]
manifest = "/srv/zinder/wallet-history/current.json"
publisher_key = "/etc/zinder/publisher.pub"
require_fs_verity = false
```

Each segment file starts with a fixed 128-byte `ZSHRNG01` header: the magic, the network tag, the artifact schema version, a flags word whose bit 0 marks the nullifier slab present and whose remaining bits are reserved, the start height and block count, the parent hash of the first block and the hash of the last one in internal byte order, the file offsets of the two slabs and the index, and reserved zero bytes. The header is followed by two slabs of framed payloads, the shielded `GetBlockRange` frames and then the nullifier `GetBlockRangeNullifiers` frames, each frame carrying its native and wire lengths and one pre-encoded `CompactBlock`. One slab holds at most 4 GiB of frames. The index holds one `[u32]` relative-offset table per slab with one entry per block plus the slab's total length, then one metadata row per block: the block and parent hashes, the native length, and the payload SHA-256 of each slab's frame. The file ends with the SHA-256 of every byte before it, which the signed catalog pins.

For a portable segment, admission streams the exact file once, compares the trailing digest with the signed SHA-256, and checks both slabs' framing, payload hashes, and block links against the index. With a matching kernel-measured digest and signed fs-verity pin, admission reads only the header and index. Both paths check the terminal block against the canonical settled chain and retain the admitted file descriptor.

Before serving a row for the first time, the reader checks its frame lengths and payload digest against the index for the requested slab, decodes the complete lightwalletd `CompactBlock`, and compares its height, hash, and parent with the indexed block. Wallets share the protobuf validation result for that admitted row and slab; a damaged shielded frame leaves the nullifier frame of the same row serving, and the other way around. The reader still checks the digest on every portable read. On a protected file, the kernel authenticates subsequent reads. The publisher remains responsible for deriving the transaction contents and tree metadata from canonical storage. Republish pre-existing `ZSHRNG02` catalogs into a fresh catalog directory before switching readers; the formats share no compatibility promise.

At each secondary catch-up interval, a snapshot reader checks the manifest for changes. A reader following the publisher checks both the manifest and the signed checkpoint, even when only one file changes. It reuses the verified prefix and admits new segments only after their files and chain anchor validate. A rejected update, including a missing or invalid publisher checkpoint, leaves the previous catalog active and is retried. A restarted snapshot reader accepts any correctly signed catalog that matches its retained settled chain. An older valid snapshot gives it less coverage. Restart the listener to change the trusted key.

On Linux, the publisher automatically enables SHA-256 fs-verity with 4,096-byte blocks on each new segment when the underlying ext4, F2FS, or Btrfs filesystem supports it. It verifies the kernel digest before publishing the signed catalog. If an interrupted run left a matching portable segment at the destination, the publisher verifies its contents and replaces that directory entry with the new protected segment. Other hard links to the portable file remain readable and writable under their existing permissions. To require kernel verification on the reader, set `require_fs_verity = true`. For a copied catalog, enable protection on each destination segment with `fsverity enable --hash-alg sha256 --block-size 4096 FILE` before installing the manifest. The reader uses `FS_IOC_MEASURE_VERITY` on the serving descriptor and compares the digest with the signed `fs_verity_sha256` field. Strict mode requires both kernel protection and this pin. An unprotected segment fails strict admission. A failed row check or protected page read ends the affected covered stream with `DATA_LOSS`. [Linux fs-verity](https://docs.kernel.org/filesystems/fsverity.html)

### `GetTreeState` and `GetLatestTreeState`

`GetTreeState` answers retained heights at or after Sapling activation from Zinder's store, where lightwalletd asks its node. Before activation it returns `INVALID_ARGUMENT`, matching Go lightwalletd v0.6.2, for both height and hash requests. Native WalletQuery tree-state reads still return pre-activation states with inactive pools absent. A positive `height` takes precedence and ignores `hash`. An empty hash at height zero is an unspecified selector and returns `INVALID_ARGUMENT`. The genesis hash returns the pre-Sapling error even when retained history starts at a later checkpoint. With `height = 0`, send the 32 bytes obtained by hex-decoding `TreeState.hash` in RPC byte order. A wallet starting from `CompactBlock.prevHash` reverses those bytes once to request the predecessor’s tree state. A hash outside the best chain and a height above the visible tip, once the [tip wait](#waiting-for-a-height-just-past-the-tip) ends, both return `INVALID_ARGUMENT`, matching lightwalletd's status for a failed `z_gettreestate` lookup. A refused rebuild returns `UNAVAILABLE`. Other height lookup statuses follow [Wallet data plane](../architecture/wallet-data-plane.md#tree-states-and-subtree-roots).

Zinder bounds concurrent tree-state rebuilds, where lightwalletd has no rebuild limit. A rebuild that finds every slot taken waits up to `compat.tree_state_replay_wait_ms` (`ZINDER_COMPAT__TREE_STATE_REPLAY_WAIT_MS`, 0 to 5,000 ms, 2,000 by default), then fails with `UNAVAILABLE`, the code a full block-range limit returns, so a wallet retries both limits the same way. It fails at once when every waiting place is taken. `compat.tree_state_replay_waiters_per_slot` (`ZINDER_COMPAT__TREE_STATE_REPLAY_WAITERS_PER_SLOT`, 1 to 256, 64 by default) sets the waiting places per slot. Native `TreeStateAtHeight` refuses the same rebuild with `RESOURCE_EXHAUSTED` and `TREE_STATE_REPLAY_LIMIT_REACHED`. `GetLatestTreeState` returns the newest stored checkpoint at or below the visible tip. It returns `INVALID_ARGUMENT` when that checkpoint precedes Sapling activation.

### `GetSubtreeRoots`

lightwalletd treats `maxEntries = 0` as "every root", and Zinder does the same: it streams every root from `startIndex` to the last completed subtree. A positive `maxEntries` streams that many roots, or fewer when the store holds fewer. Zinder honors the full `uint32` range of `maxEntries`, where lightwalletd with Zebra rejects a value above 65,535 with `INVALID_ARGUMENT`, and native `SubtreeRoots` has its own cap. Zinder reads the roots in pages of at most 1,000, bounded by the tip captured at the start of the call.

Wallet serving reads each page at the captured epoch. The stream ends with `UNAVAILABLE` if that epoch is retired before the last page. Light-wallet serving reads each page from the current generation and checks that it still holds the captured tip. Appends above that tip preserve the stream's bounds. A replacement that removes the captured tip ends the stream with `UNAVAILABLE`. A stream that has already sent its last completed root ends with `OK` without another read.

To retry after `UNAVAILABLE`, restart from the original `startIndex`, or resume after a root whose `completingBlockHash` the wallet has verified on the new chain. A reorg that replaced the last delivered root's completing block also replaces the root. Resuming at the next index without checking the completing block can skip that replacement and combine roots from two forks.

Light-wallet serving requires every completed root from index zero in each pool at admission. Unprofiled wallet serving can open a checkpointed pair with retained-range coverage. A request for a root completed before the retained block history returns `NOT_FOUND` with `ARTIFACT_UNAVAILABLE` if the store lacks that root. A complete-prefix build also serves those earlier roots. See [Schema admission](../architecture/storage-backend.md#schema-admission). `completingBlockHash` is in RPC byte order.

### `GetTransaction`

The request must carry a transaction id; an empty hash is `INVALID_ARGUMENT`. Zinder reads the canonical chain first. On a miss, it asks the writer's live mempool through ingest control, with a 15-second timeout:

- A pending transaction is returned at height 0, the protocol's "in the mempool" marker, as lightwalletd returns it from Zebra.
- A transaction in neither place is `NOT_FOUND`, but only when the writer answered at the same chain epoch as the canonical read. A miss at another epoch is a retryable `UNAVAILABLE`.
- On a store whose history starts at a checkpoint, a mempool miss is `FAILED_PRECONDITION` with `CANONICAL_HISTORY_UNAVAILABLE`, because the transaction may be mined below the checkpoint.
- A failed or timed-out mempool read is `UNAVAILABLE`, not `NOT_FOUND`, because the transaction may be pending.

Wallet SDKs treat height 0 as pending and `NOT_FOUND` as an unknown id, so a just-broadcast transaction reads as pending.

Light-wallet serving uses the admitted store's persisted retention. A missing body in a store that retains transaction bodies returns `NOT_FOUND` with `ARTIFACT_UNAVAILABLE`. If the store omits transaction bodies, Zinder looks up the transaction's canonical location and reads the body as [Verified transaction bytes](#verified-transaction-bytes) describes. A digest mismatch returns `DATA_LOSS`. A transaction absent from the captured canonical view goes through the ingest mempool lookup after a served-tip check when the store omits transaction bodies.

Forwarded lookups wait for a slot as [Admission](#admission) describes. Reads of retained transaction bodies use the local path.

### `SendTransaction`

Wallet serving uses the native `BroadcastTransaction` path, including endpoint verification on first contact, failover across `[node_fallbacks.N]`, and the 16 MiB in-flight byte budget. A wallet listener without `[node]` returns `FAILED_PRECONDITION` with `ENDPOINT_CAPABILITY_UNAVAILABLE`. See [Wallet data plane](../architecture/wallet-data-plane.md).

Light-wallet serving uses the same byte budget and sends through its one configured node after verification against the current reader. If the served fence or schedule changes during verification, the send fails with `UNAVAILABLE`. During a node outage, the call fails with `UNAVAILABLE`. Zinder has no queue for later submission.

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

Wallet serving uses the same verified endpoints and failover order as `SendTransaction`. It moves to the next endpoint when one is unreachable or lacks the RPC, without changing the preferred broadcast endpoint. Light-wallet serving verifies its single node against the captured canonical view and asks that node for the fee.

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

Wallet serving pins the history to one epoch. It reads 1,000 entries at a time and streams transactions in canonical order, overlapping up to 8 transaction reads ahead of the client (capped at the `history` forwarded-read allowance when bodies are fetched from the node). A stream still reading one minute after a new block replaces its epoch ends with `CHAIN_EPOCH_PIN_UNAVAILABLE`.

Light-wallet serving bounds forwarded history to the captured tip and releases the reader before the node calls. It checks the node's hash at that tip's height before and after each ID lookup. For each returned transaction, it reads the indexed `transaction_location` record first. The location must fall inside the page's height range and follow the previous row in canonical order. The body then comes from the verified cache or the node, checked against the record's block and digest. Rows need no tip check of their own, because each one is bound to the served view through its local record. The stream checks the tip once more before it ends. These checks validate returned rows. Completeness still depends on the node's address index, and a fork the node leaves and returns to between the two hash checks can go undetected. The stream is best-effort.

Forwarded history waits for a stream slot and then for node-call slots as [Admission](#admission) describes. Each admitted request has 60 seconds from its arrival, including time spent waiting for a slot or for the client to read. At that deadline the stream ends with `DEADLINE_EXCEEDED` after any rows it has already sent. Treat those rows as an incomplete result and request the range again. When a stream is dropped while it prepares a row, the stream slot stays taken until that row's work ends. A stream dropped while it waits for the client frees its slot at once.

A failure can end either kind of stream after it has delivered rows. For forwarded history, a lost tip or unreachable node can return `UNAVAILABLE`, and a digest mismatch returns `DATA_LOSS`. Treat any stream that ends with an error as incomplete and request the range again.

### `GetTaddressBalance` and `GetTaddressBalanceStream`

Both methods return the confirmed balance, the sum of unspent outputs, of up to 256 distinct addresses. Wallet serving reads at one chain epoch. Light-wallet serving sums the outputs reported by its node at the captured canonical tip. A repeated address counts once. Mempool state is excluded. When light-wallet serving observes a node tip different from the captured served tip, it performs any configured [tip wait](#waiting-for-a-height-just-past-the-tip) and single retry and otherwise refuses with `UNAVAILABLE`, reason `SERVED_TIP_MISMATCH`, and `grpc-retry-pushback-ms: 1000` instead of `NODE_UNAVAILABLE`.

- An empty list is `INVALID_ARGUMENT`.
- The 257th distinct address is `INVALID_ARGUMENT` with `TRANSPARENT_BALANCE_ADDRESS_COUNT_EXCEEDED`. The streaming method refuses as soon as that address arrives, without waiting for the client to finish. lightwalletd bounds the streaming method at 10,000 addresses.
- An invalid address, an address for another network, or address text longer than 64 bytes is `INVALID_ARGUMENT`.
- A total above the protocol's `int64` field is `OUT_OF_RANGE`.

### `GetAddressUtxos` and `GetAddressUtxosStream`

Both methods follow lightwalletd's `maxEntries` rule: 0 returns every unspent output at or above `startHeight`, and a positive value returns at most that many, oldest first. Within one block, Zinder orders rows by transaction id and output index, where Zebra uses block position; this matters only when `maxEntries` splits a block.

A request that lists an address more than once gets that address's rows once, as lightwalletd v0.6.2 does. A wallet that adds up the rows counts each output once, and duplicate rows never use up `maxEntries`.

One response returns at most 100,000 outputs. The handling of an oversized response depends on the serving mode.

- In wallet serving, `GetAddressUtxos` reads every row before it answers. A request that would return more fails with `RESOURCE_EXHAUSTED` and no rows.
- In wallet serving, `GetAddressUtxosStream` sends rows as it merges them, at most 64 ahead of the client. A request that would return more sends the first 100,000 rows and then ends with `RESOURCE_EXHAUSTED`. A later failure such as `CHAIN_EPOCH_PIN_UNAVAILABLE` also ends the stream after the rows already sent. Treat a stream that ends with an error as incomplete.
- In light-wallet serving, both methods fetch the node results and check the response limit before sending any rows. A response that exceeds the limit returns `RESOURCE_EXHAUSTED` without a partial output list. Each node page must name the captured canonical tip. For a bounded prefix, Zinder verifies the outputs at the cutoff height before selecting rows in wallet order. A tip mismatch (after any [tip wait](#waiting-for-a-height-just-past-the-tip) and single retry) or disagreeing pages at the served tip fail with `UNAVAILABLE`, reason `SERVED_TIP_MISMATCH`, and `grpc-retry-pushback-ms: 1000` instead of `NODE_UNAVAILABLE`.

In light-wallet serving, the unspent-output and balance methods share the UTXO kind of node-call slots, and each node page holds a slot only while it is read. The unspent-output methods also take one of the unspent-output response slots before their first node call, as [Admission](#admission) describes. An empty reply releases its response slot before the stream opens, and a streamed reply holds its response slot only until its last row is yielded or the stream is dropped. A unary reply holds its response slot until its body ends or fails after the send-stall layer hands the buffered data to the transport, or until the handler body is dropped on disconnect or a send stall. The bound covers the listener’s buffered reply. Bytes already handed to the transport are outside this bound.

In wallet serving, both methods read each distinct address at one chain epoch and merge the addresses' outputs in response order as they send them. Each address is read in pages that double only as its rows are sent. Its first page holds an even share of 8,192 outputs, capped at 1,024 and at a positive `maxEntries`, but never fewer than 16. A request that lists up to eight distinct addresses therefore reads up to 1,024 outputs of each in one page.

A request holds at most about twice the rows it has sent, plus 2 outputs and one first page for each distinct address. Its first pages together hold at most 8,192 outputs, or 16 for each distinct address when it lists more than 512. At the limits of 10,000 addresses and 100,000 rows, that is about 380,000 outputs.

The protocol has no lossless way to page past the cap. A single block with more matching outputs than the cap always fails with `RESOURCE_EXHAUSTED`.

### `GetMempoolTx`

`GetMempoolTx` returns the compact form of every current mempool transaction, filtered by `poolTypes` as `GetBlockRange` filters blocks. Zinder and Go lightwalletd v0.6.2 accept up to 20,000 entries in `exclude_txid_suffixes`, each up to 32 bytes long. A suffix excludes a transaction only if it matches exactly one ID in the entire snapshot, including transactions outside the requested pools. Send suffixes in internal byte order.

When suffixes overlap, the shortest matching suffix takes precedence, as in Go lightwalletd v0.6.2. A list containing an empty suffix therefore excludes the only transaction in a singleton snapshot. An empty list excludes nothing. [Go mempool filter](https://github.com/zcash/lightwalletd/blob/v0.6.2/frontend/service.go#L1019-L1058)

A pool list containing only unrecognized numeric values yields an empty stream. A request containing `POOL_TYPE_INVALID` (0) fails with `INVALID_ARGUMENT` before any transaction is sent. Snapshot reads page through the live mempool within the 3.5 MiB unary response budget; an admitted entry that cannot fit in a single snapshot page fails the call with `UNAVAILABLE` and `UNSUPPORTED_WALLET_ENCODING`.

### `GetMempoolStream`

`GetMempoolStream` sends every current mempool transaction, then each new one, at height 0, and each transaction id at most once per stream, as lightwalletd does.

- The stream ends with `OK` when the chain tip changes, including a change already observed while the stream was starting. Clients reconnect, as with lightwalletd.
- If the tip-change signal itself closes, for example during shutdown, the stream ends with `UNAVAILABLE`, so a client does not mistake it for a new block.
- An admitted mempool entry that exceeds the 3.5 MiB unary snapshot response budget fails the stream with `UNAVAILABLE` and `UNSUPPORTED_WALLET_ENCODING` (after any earlier snapshot pages already sent).
- One stream sends at most 100,000 distinct transactions between tip changes. The next new one ends it with `UNAVAILABLE`, which clients retry.

## Request limits

Limits shared by every public Zinder gRPC listener are listed in [Configuration](configuration.md#listener-and-request-limits). These are the compatibility runtime's list-field bounds, which follow the counting and refusal rules in [Request list bounds](configuration.md#request-list-bounds):

| Method | Field | Limit | Handler cap |
| --- | --- | --- | --- |
| `GetTaddressBalance` | `addresses` | 512 | 256 distinct addresses |
| `GetAddressUtxos`, `GetAddressUtxosStream` | `addresses` | 10,000 | none |
| `GetMempoolTx` | `exclude_txid_suffixes` | 40,000 | 20,000 |
| `GetMempoolTx` | `poolTypes` | 1,000 | none |
| `GetBlockRange`, `GetBlockRangeNullifiers` | `poolTypes` | 1,000 | none |
| `GetTaddressTxids`, `GetTaddressTransactions` | `range.poolTypes` | 1,000 | none |

For `GetMempoolTx`, both servers return `INVALID_ARGUMENT` for 20,001 through 40,000 suffixes. Above the predecode limit, Zinder returns `INVALID_ARGUMENT` and Go lightwalletd v0.6.2 returns `INTERNAL`. [Go handler](https://github.com/zcash/lightwalletd/blob/v0.6.2/frontend/service.go#L810-L829) and [Go predecode limit](https://github.com/zcash/lightwalletd/blob/v0.6.2/frontend/codec.go#L17-L40).

## Reference inputs

The vendored protocol and the images the parity suite runs are:

- `lightwallet-protocol` `v0.6.0` (`299805c19011a027d2c48c5e912dd05041f76156`);
- `lightwalletd` `v0.6.2` (`f8fe2cca7ede03b1f06b1d637a2ab467d151b79d`) as the source reference, which parses the version-6 transactions that NU6.3 introduced;
- `electriccoinco/lightwalletd:v0.6.2@sha256:75e5742df686094556abef66e69c37bc114de23a0706712aee324f332ca52951`;
- `docker.io/zfnd/zebra:7.1.0@sha256:4f9536c8061e407f9f2ec81b7e0dd2f1762f81deec316abb1530dca8796a33ba`, started on Regtest with `deploy/zebra/regtest.toml`, which activates every upgrade through NU7 (see [Regtest through NU7](../runbooks/testing.md#regtest-through-nu7));
- `docker.io/zakuracore/zakura:1.8.6@sha256:e514c89b751b685686690098928507aaa994311e6b90c79373c55b0afaa45d50`, started on Regtest with `deploy/zakura/regtest.toml`, which uses the same schedule.

A release needs the parity suite to pass with both node images. The test setup is in [Testing](../runbooks/testing.md).
