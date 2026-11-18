# Lightwalletd Compatibility Certification

Zinder can replace the server role of lightwalletd only for clients that speak
the vendored `CompactTxStreamer` protocol. It remains an adapter over
`WalletQueryApi`; it must not introduce a second chain-data model, wallet
state, or key-management surface.

## Claim boundary

The replacement claim applies to the network-facing server only. It excludes
wallet-local scanning, key custody, transaction construction, and user-facing
wallet UX.

Use a named level in release notes and operator docs:

| Level | Evidence |
| --- | --- |
| `protocol-compatible` | Pinned vendored proto, drift check, and generated-client integration coverage. |
| `reference-parity-compatible` | Live comparison against the pinned reference lightwalletd for the RPCs claimed. |
| `client-compatible` | A separately versioned lightwalletd client completes bootstrap, restore, transparent discovery, shielded-note discovery, send, and pending-transaction observation. |
| `public-operator-compatible` | The deployed endpoint has the required TLS/proxy, access, readiness, rate-limit, and redaction controls. |

Do not call the adapter a full or drop-in replacement without the protocol pin,
the claimed RPC set, and the highest evidence-backed level.

## Architecture

- `zinder-proto` owns the vendored protocol and its provenance.
- `zinder-query` owns the native `WalletQueryApi`, `WalletServingQuery`, and the
  shared exact-fence serving-pair publisher.
- `zinder-compat-lightwalletd` translates protocol messages, query values, and
  errors over an admitted exact-fence canonical/wallet-projection reader pair.
- `zinder-ingest` and `zinder-store` own compact blocks, tree states, subtree
  roots, and transaction bytes; `zinder-projector` owns the wallet projection
  that provides transparent UTXOs and transaction history.

In particular, `GetLightdInfo.taddrSupport` is an immutable serving-process
claim derived by the compatibility constructor. Construction admits it only
when the canonical pair retains transaction blobs (`transactions` or `all`)
and the exact wallet-serving pair provides the transparent reads; insufficient
retention fails before the listener binds. Later pair lag changes readiness,
and a node or ingest-control outage degrades that dependency, without
rewriting `GetLightdInfo`.

`GetLightdInfo` derives its upgrade fields from one activation table and the
served tip on every request:

- `consensusBranchId` is the branch of the upgrade active at the tip.
- `upgradeName` and `upgradeHeight` describe the next pending upgrade, the
  first one that activates above the tip. They are empty and 0 when no upgrade
  is pending, as the lightwalletd protocol defines and lightwalletd reports.
- `saplingActivationHeight` is the Sapling activation height.

In both serving modes that table is the schedule the canonical writer
admitted, and neither mode reads a table from the node. A running
compatibility process therefore reports a newly admitted upgrade, such as NU7,
as pending while its tip is below the activation height, and as the active
`consensusBranchId` once the tip reaches it. A store whose writer has not
admitted a schedule yet cannot be served: the process retries it at startup
until `storage.initial_catchup_timeout_ms` passes, and exits if ingest has
still not admitted one. See
[ADR-0039 §Readers](../adrs/0039-writer-admitted-network-upgrade-schedules.md#readers).

## Identity and live-tip responses

`GetLightdInfo` and `GetLatestBlock` describe the serving process and its
current tip. Their responses are snapshots, so comparing a whole message, or a
digest of its bytes, across Zinder, lightwalletd, and any other server
measures build metadata and tip timing, not chain data. A full-message digest
of either method is not parity evidence. Compare only the fields named
comparable below, at one agreed tip.

Both serving modes return the same fields under the same rules. The only
field that differs by mode is `taddrSupport`.

### `GetLightdInfo`

| Class | Fields | Rule |
| --- | --- | --- |
| Comparable with lightwalletd | `chainName`, `saplingActivationHeight`, `consensusBranchId`, `upgradeName`, `upgradeHeight` | Derived from the chain and the activation table, so two servers on the same node agree at the same tip. `chainName` is the BIP70 name (`main` or `test`; Regtest reports `test`). `consensusBranchId` is the lowercase hex branch active at the tip. `upgradeName` and `upgradeHeight` name the next pending upgrade, as described above. Compare them only after both servers report the same `blockHeight`, because an activation height moves them. |
| Protocol release | `lightwalletProtocolVersion` | The `lightwallet-protocol` release tag the server implements, such as `v0.5.0`. Two servers agree when they pin the same protocol release, whatever their build. |
| Live tip | `blockHeight` | The served visible tip. Two servers agree only when they have both applied the same block, so a harness compares it with a tolerance of one block or compares the other fields only when the heights are equal. |
| Zinder-specific | `version`, `vendor`, `gitCommit`, `branch`, `buildDate`, `buildUser`, `zcashdBuild`, `zcashdSubversion`, `donationAddress`, `estimatedHeight`, `taddrSupport` | Identify or configure this process. Never compare them with another server. |

The Zinder-specific values are fixed:

- `vendor` is `Zinder`. `version` is the crate version and `gitCommit` is the
  build commit, both from the shared build identity.
- `branch`, `buildDate`, `buildUser`, `zcashdBuild`, `zcashdSubversion`, and
  `donationAddress` are empty. Zinder is not zcashd, has no build-host
  metadata, and has no donation configuration, so it leaves them unset rather
  than impersonating another build.
- `estimatedHeight` equals `blockHeight`. Zinder serves its own visible tip and
  does not report the node's estimate, so it stays at the served tip while the
  node syncs or is unavailable. Compatibility keeps serving that tip through a
  node outage, so `estimatedHeight` does not show that the chain stopped
  advancing. Operators read `served_epoch_age_seconds` on `/readyz` for that.
- `taddrSupport` is `true` in wallet-serving mode, where construction admitted
  transparent reads, and `false` in compact-blocks mode and in a generic
  adapter built without the transparent constructor.

### `lightwalletProtocolVersion`

`lightwalletProtocolVersion` is the release tag of the vendored
`lightwallet-protocol` schemas, `v0.5.0` today, read from the `VERSION` file
next to the pinned `COMMIT` in `zinder-proto` and exported as
`LIGHTWALLETD_PROTOCOL_VERSION`. lightwalletd reports the same form: the pinned
reference v0.6.0 sets the field to its protocol tag, `v0.5.0`
(`common/common.go:51` at `e7436d2`).

Wallets read the field as a version, not as an identifier. zingolib parses it
as `vMAJOR.MINOR.PATCH` and refuses to sync below 0.5.0, the release that
added the Ironwood pool (zingolib `f930d80`,
`pepper-sync/src/client.rs:104-143`, checked before every sync at
`sync.rs:464`). A commit hash fails that parse, so Zingo cannot sync against a
server that reports one.

The vendored commit stays available to Rust code as
`LIGHTWALLETD_PROTOCOL_COMMIT`. lightwalletd reports no protocol commit on the
wire, and `gitCommit` names the server build, so Zinder does not add one.

### `GetLatestBlock`

`GetLatestBlock` returns the visible tip as a `BlockID`: `height` is the tip
height and `hash` is the 32-byte block hash in internal byte order, the same
order as `CompactBlock.hash`. It is the reverse of the display hex that Zebra
JSON-RPC returns.

The pair is comparable with lightwalletd only at one height. When two servers
report different heights, one is mid-commit and the hashes are expected to
differ. A harness compares `hash` only when `height` is equal, or reads
`GetBlock` at a height both servers serve and compares that block instead. The
height itself is live-tip data and is not comparable beyond the one-block
tolerance used for `blockHeight`.

## Runtime serving selection

The compatibility binary selects its serving plane at runtime through the
operator configuration. Set `[compat].serving` in TOML, use the
`ZINDER_COMPAT__SERVING` environment variable, or pass `--serving` on the
command line. The only accepted values are `wallet` and `compact-blocks`;
omitting the setting preserves the migration-safe `wallet` default.

`wallet` admits the canonical and wallet projection pair and retains the
existing wallet-compatible behavior. `compact-blocks` admits only the
canonical secondary and rejects any `[wallet]` section or wallet settings;
its resolved configuration omits the wallet section. This selector is a
runtime operator contract, not a new Rust library export or a storage/proto
layout change.

Compact-block serving admits exactly `GetLightdInfo`, `GetLatestBlock`,
`GetBlock`, and `GetBlockRange`. Every other `CompactTxStreamer` RPC except
`Ping` returns `UNIMPLEMENTED`, including wallet queries, mempool methods,
tree/subtree methods, transparent-address methods, and transaction
submission. Its `GetLightdInfo` reports the admitted network and tip with
`taddrSupport = false`. Compact-blocks serving never contacts a node, so it
rejects a `[node]` section, `[node_fallbacks.N]`, `--node-json-rpc-addr`,
every `ZINDER_NODE__*` variable, and every `ZINDER_NODE_FALLBACKS__*` variable
at startup. Its fields follow
[Identity and live-tip responses](#identity-and-live-tip-responses).

`Ping` is lightwalletd's load-testing RPC. Both serving modes refuse it with
`FAILED_PRECONDITION` by default, as reference lightwalletd does. Setting
`compat.ping_very_insecure = true` (`ZINDER_COMPAT__PING_VERY_INSECURE`, or
`--ping-very-insecure` as on lightwalletd) makes it answer the way
`lightwalletd --ping-very-insecure` does: `entry` is the number of `Ping`
calls in flight once this one starts, the call sleeps `interval_us`
microseconds, and `exit` is the number still in flight when it returns. The
interval converts to nanoseconds with wrapping arithmetic, as lightwalletd's
Go `time.Duration` does, so an interval too large to represent returns at
once. Each call holds a server task for as long as the client asks, so enable
it only on a server you are load testing. A call still sleeping when the
server starts shutting down ends with `UNAVAILABLE`, so it does not delay
shutdown. The `compat_started` log records the setting as
`ping_very_insecure`.

Wallet serving uses `[node]` and the `[node_fallbacks.N]` endpoints only for
`SendTransaction`. It starts without contacting any node and verifies each
endpoint on first contact, as
[ADR-0038](../adrs/0038-wallet-runtime-composition-and-capability-discovery.md)
describes. Until one passes, `SendTransaction` fails with `UNAVAILABLE` and
sends nothing. A send moves to the next verified endpoint when one is
unreachable or rejects the consensus branch. When an earlier endpoint was
unreachable, a later duplicate or queued answer returns `errorCode` 0 with the
transaction id instead of -27 or -25, because the unreachable endpoint may
have accepted the transaction. Without `[node]`, `SendTransaction` is
unsupported.

A compact-blocks `GetBlockRange` stream holds a canonical generation only
while it reads one chunk, never while it waits for its client. A slow stream
therefore does not keep the publisher from reopening the inactive generation,
so publication and readiness do not depend on any client's pace.

## Block range transaction contents

`GetBlockRange` applies the requested pool types to every compact transaction
and then omits any transaction left with no spends, outputs, actions, inputs,
or outputs, as lightwalletd does. A transparent-only coinbase is therefore
absent under the default shielded selection and present when the request names
the transparent pool. Transactions that remain keep their block position in
`CompactTx.index`, so a gap in the indexes marks an omitted transaction. Wallet
and compact-blocks serving apply the same payload rule. In compact-blocks
serving, `GetBlock` returns the stored compact block without pool pruning and
is unchanged by this rule.

The fixture-backed range tests pin a SHA-256 digest of the length-delimited
stream for ascending, descending, and transparent-pool ranges, plus the
`GetBlock` bytes. A deliberate serialization change updates those digests in
the same change.

## Block range status codes

`GetBlockRange` keeps the lightwalletd status contract in both serving modes.
A range whose higher bound is above the visible tip returns `OUT_OF_RANGE`. A
range that touches a height the serving store does not hold, such as height 0
or a height below the retained floor, returns `NOT_FOUND`. Neither case returns
`UNAVAILABLE`, which reports a storage or dependency outage. A compact block
missing inside retained history is store damage and returns `UNAVAILABLE`.

Canonical history never retains height 0. A complete build takes the network's
genesis block as its authenticated predecessor and retains blocks from height
1, so no compact block exists at height 0, and a `GetBlockRange` read that
touches height 0 returns `NOT_FOUND`.

These codes describe the request as it arrives and fail the call before any
block is sent. A stream that has started can also end with `ABORTED` or
`OUT_OF_RANGE` after the blocks it sent, when a reorg reaches the chain it is
sending; see [Block range consistency](#block-range-consistency).

A `GetBlockRange` or `GetBlockRangeNullifiers` request that arrives while the
process already serves its limit of concurrent block-range streams fails with
`RESOURCE_EXHAUSTED` before any read. Retry it later. lightwalletd sets no such
limit; see [Block range resource limits](#block-range-resource-limits).

Unary block lookups validate the selector before reading. Height 0 with a
32-byte hash is a hash lookup. Height 0 with an empty hash is an unspecified
identifier, so `GetBlock` returns `INVALID_ARGUMENT` for it in both serving
modes, not `NOT_FOUND`. `GetBlockNullifiers` applies the same rule where it is
served.

## Tree state status codes

In wallet serving, `GetTreeState` answers every retained height from storage
and never contacts the node. A height with a stored checkpoint is read
directly. Any other height is rebuilt from the nearest stored checkpoint below
it and the stored compact blocks up to it. lightwalletd instead asks its node
for every height.

| Request | Status |
| --- | --- |
| Height above the visible tip | `INVALID_ARGUMENT`, as lightwalletd returns |
| Height below the store's history predecessor | `NOT_FOUND` with reason `BLOCK_NOT_IN_BEST_CHAIN` |
| A checkpoint the rebuild seeds from is missing, or stored history between it and the height is damaged | `UNAVAILABLE` with reason `STORAGE_UNAVAILABLE` |
| Rebuild requested while the process already runs one rebuild per available CPU | `RESOURCE_EXHAUSTED` with reason `TREE_STATE_REPLAY_LIMIT_REACHED` |

A height with a stored checkpoint is never refused this way. Retry a refused
request later.
`zinder_query_tree_state_replay_refusals_total` counts refusals. lightwalletd
sets no such limit, and how clients handle `RESOURCE_EXHAUSTED` on this method
is not verified.

Compact-block serving returns `UNIMPLEMENTED` for `GetTreeState`, as
[Runtime serving selection](#runtime-serving-selection) describes.

## Block range consistency

`GetBlockRange` in both serving modes, and `GetBlockRangeNullifiers` in wallet
serving, read a range in chunks of at most 1,000 blocks and at most 2 MiB of
compact blocks, as [Chunk size](#chunk-size) describes. A range of large
blocks therefore takes several chunks even when it spans fewer than 1,000
heights. The first chunk is read at the chain view current when the request
arrives, before the response starts. Each later chunk is read at the view
current when the server reads it, and continues the stream only when it links
by hash to the last block the stream sent
([ADR-0038](../adrs/0038-wallet-runtime-composition-and-capability-discovery.md#compatibility-block-range-streams)).
Every chunk edge carries the same hash check, whatever ended the chunk before
it. A client can rely on the following:

1. **Request-time outcomes.** These fail the call before any block is sent:
   - `INVALID_ARGUMENT` for a malformed request, whatever the server's load.
   - `RESOURCE_EXHAUSTED` when the server already serves its limit of
     block-range streams, as [Concurrent streams](#concurrent-streams)
     describes.
   - `OUT_OF_RANGE` when the higher bound is above the visible tip when the
     request arrives.
   - `NOT_FOUND` when one of the first 1,000 heights the stream would send is
     outside the store's retained history, even when the first chunk stops
     before that height.
2. **Order and coverage.** Blocks arrive at consecutive heights in the
   requested direction. A stream that ends `OK` has sent every requested
   height exactly once.
3. **Every link holds.** In ascending order each block's `prev_hash` equals
   the `hash` of the block sent just before it. In descending order each
   block's `hash` equals the `prev_hash` of the block sent just before it. This
   holds across chunk edges and across new blocks.
4. **One chain as read.** The blocks one stream sends form one chain as the
   server read it. An ascending stream that crosses a reorg above the last
   block it sent continues on the new branch. A descending stream that crosses
   a reorg at or above the last block it sent continues on the chain it
   started on. An `OK` stream does not promise that its blocks are still the
   best chain when the client receives them.
5. **No time limit.** The server does not end a stream because of elapsed
   time, new blocks, or how slowly the client reads. Process shutdown and
   drain still end open streams, and so does a connection that stops
   answering HTTP/2 keepalive pings. A client that stops reading holds no
   serving pair or generation, only the blocks already queued for it, plus
   transport buffers:
   - Until the client has received the whole first chunk, the rest of that
     chunk and the next chunk are queued, at most two chunks.
   - After that, at most one chunk is queued, plus a buffer of 16 blocks in
     wallet serving or 8 in compact-blocks serving.
   - Beyond the server, the client's HTTP/2 stream window holds the blocks
     already sent but not yet read by the wallet.
     [Block range resource limits](#block-range-resource-limits) sizes each
     term.
6. **Reorg ending.** An ascending stream ends early when a reorg replaces the
   last block it sent or any block below it. A descending stream ends early
   when a reorg replaces the next height it has to send. Either ending comes
   after the blocks already sent, with `ABORTED`. lightwalletd v0.5.5 and
   later check the same link and end the stream with the same code. A stream
   also ends early, with `OUT_OF_RANGE`, when a reorg leaves the visible tip
   below a height it still has to send. Neither ending carries a Zinder
   `ErrorInfo` reason. To recover, request again from the height the wallet
   needs. A wallet that kept the partial blocks finds the fork with its own
   parent check on the next block.
7. **Settled history.** The canonical writer never replaces a block at or
   below the settled tip, so a stream over settled history never ends this
   way. Only the unsettled suffix, at most the reorg window of 1,000 blocks by
   default, can end a stream.
8. **Read-ahead.** The server reads ahead of the client by the amounts in
   point 5: up to two chunks at the start of a stream, and one chunk plus the
   buffer after the first chunk. The view a block was read at can therefore be
   older than the view current when the client receives it.
9. **Mid-stream storage outcomes.** A storage failure ends the stream with
   `UNAVAILABLE`, and a descending range that runs below the retained floor
   ends it with `NOT_FOUND`. In wallet serving, a reorg that lowers the tip in
   the moment between a chunk's tip check and its read also ends the stream
   with `NOT_FOUND`.

## Block range resource limits

`GetBlockRange` and `GetBlockRangeNullifiers` streams are the compatibility
runtime's largest memory term after its block caches. Three mechanisms limit
what the streams hold:

- The chunk cap limits the stored blocks that one chunk read keeps.
- A process-wide limit bounds how many streams run at once.
- HTTP/2 keepalive ends the streams of clients that vanished.

None of them sets a byte maximum on that memory, and the total is not
measured.

### Chunk size

A chunk holds at most 1,000 blocks and at most 2 MiB of compact blocks. The
2 MiB counts each block as the store holds it, the whole compact block before
pool filtering, because that is what the read materializes. So
`GetBlockRangeNullifiers` reads the same chunks as `GetBlockRange` although it
sends smaller messages.

- The read walks the range in the order the stream sends it and stops before
  the block that would take the chunk past 2 MiB. A range of small blocks
  keeps 1,000-block chunks, and a range of large blocks is read and sent in
  smaller ones.
- A chunk always holds at least one block, so a single block larger than
  2 MiB is read and sent alone.
- Before it reads, a chunk checks that every height it could cover, up to
  1,000, is in retained history. A range that reaches outside it ends with
  `NOT_FOUND` exactly where a 1,000-block chunk would end.
- The store fetches rows in batches. The first batch is one row. Each later
  batch fetches as many rows as the budget left holds at the size of the
  largest row the read has seen, up to 16. While block sizes stay steady, the
  rows a read fetches past the point where it stops therefore fit in the
  budget it had left. A read that meets blocks larger than any it has seen
  can fetch up to 16 of them in one batch. The read releases the rows it does
  not keep before it returns.

The cap is fixed. On Testnet, 1,000-block stretches of heavy blocks measured
2.1 to 8.8 MB on the wire, so such a stretch is read in two to five chunks.

### One stream

The server holds, for one stream:

- The chunk it is reading: at most 2 MiB of stored blocks, or one block that
  is larger. The read decodes each block into values that it allocates
  separately. The encoding then builds lightwalletd messages with byte
  vectors of their own while the decoded chunk is still held. After that, the
  encoded chunk waits for the client.
- During a read, the rows it fetched but does not keep: within the chunk's
  2 MiB of stored bytes while block sizes stay steady, and up to 16 blocks
  when the read meets blocks larger than any before them.
- Up to 16 queued blocks in wallet serving, or 8 in compact-blocks serving.
  These are whole blocks, so their size follows the blocks in the range, not
  the chunk cap.
- Up to 400 KiB in the stream's HTTP/2 send buffer and about 32 KiB of gRPC
  encoding buffer. The encoding buffer can exceed 32 KiB by one block.

At the start of a stream, the first chunk and the second can both be held.
Later, the stream holds the chunk it is reading and the blocks queued from
the chunk before it.

The server enforces these limits for one stream:

- The stored bytes that one chunk read keeps, and the 1,000-block maximum of
  a chunk.
- The number of chunks the stream holds at once.
- The number of queued blocks.

It does not limit the bytes behind them:

- A chunk takes more memory than its stored bytes, because the decoded
  blocks and the encoded messages each allocate their own values. How much
  more depends on the shape of the blocks and is not measured.
- A block larger than 2 MiB is read alone, so its own size sets the size of
  its chunk.
- Queued blocks are whole blocks of any size.

The memory one stream holds therefore has no maximum that the server
enforces, and it is not measured.

The client holds one more term. Its HTTP/2 stream window bounds how far the
server can send ahead of what the wallet has read, and those blocks sit in
the client's memory, not the server's. tonic and hyper clients default to a
2 MiB stream window and a 5 MiB connection window; other gRPC stacks use
other windows. A tonic client that stopped reading on Testnet held about
2 MiB of unread blocks while the server's socket send queue stayed under
3 KB.

### Concurrent streams

`compat.max_concurrent_block_range_streams`
(`ZINDER_COMPAT__MAX_CONCURRENT_BLOCK_RANGE_STREAMS`) limits how many
`GetBlockRange` and `GetBlockRangeNullifiers` streams the process serves at
once. It defaults to 100 and accepts 1 to 65,536.

- A stream takes a slot once its request is validated, so a malformed
  request gets `INVALID_ARGUMENT` whatever the load. It returns the slot when
  the stream ends or fails, or when the client cancels it or disconnects,
  once no chunk read of the stream is still running. A chunk read that has
  started finishes even after the client has gone, so the slot stays taken
  for at most one more chunk read. This holds for the first chunk, which the
  server reads before it answers, and for the chunks its background reader
  reads later.
- A valid request past the limit fails with `RESOURCE_EXHAUSTED` before any
  read, and `zinder_compat_block_range_stream_refusals_total` counts it.
  `zinder_compat_block_range_streams_open` reports how many streams hold a
  slot.
- The Zcash Android SDK retries a failed batch, so a refusal delays its sync
  rather than failing it. How other clients handle `RESOURCE_EXHAUSTED` is not
  verified.

A syncing wallet holds few block-range streams at once, so the default
serves many syncing wallets. The limit bounds how many streams hold the
terms in [One stream](#one-stream), not how many bytes they hold together.
That total is not measured, at the default or at any other limit.

Size the limit to the memory left after the block caches described below.
Then check the container's memory while
`zinder_compat_block_range_streams_open` is near the limit, with the blocks
your wallets request. Freed memory can stay with the allocator after the
streams end.

Every client shares the limit, and the server cannot tell clients apart.
Behind a proxy, every wallet arrives over the proxy's connections. A stream
has no time limit (point 5 of
[Block range consistency](#block-range-consistency)), and a client that stops
reading still answers keepalive pings. One client that opens as many streams
as the limit and stops reading therefore holds every slot, and the server
refuses other wallets until that client disconnects. A public endpoint should
limit concurrent requests per client at its edge. The shipped Caddy template
sets no such limit.

### Connections

The compatibility server, like every public Zinder gRPC server, limits one
HTTP/2 connection to 200 concurrent streams, which is hyper's own server
default. A client at that limit queues
further streams or opens another connection, depending on its HTTP/2 stack.

The server pings a connection after one minute without a frame from it and
closes the connection when a ping goes unanswered for 20 seconds. Closing it
ends its streams and returns their slots, so a wallet that vanished without
closing its connection holds its slot for about 80 seconds. A client that is
connected but reads slowly still answers pings and keeps its stream.

Behind a TLS proxy or managed edge, the server talks to the proxy, not to the
wallets:

- The pings reach the proxy, so the proxy's own timeouts decide when a
  vanished wallet's stream is cancelled. The shipped Caddy template,
  `deploy/compat-lightwalletd.Caddyfile`, sets no timeouts, so Caddy's
  defaults apply. How long they let a vanished wallet hold a slot is not
  measured.
- The proxy carries every wallet's streams over its own connections to the
  server, so the 200-stream limit applies to each proxy connection, not to
  each wallet. Whether Caddy opens another connection or queues streams at
  that limit is not verified.

These connection settings are fixed. lightwalletd sets no stream limit per
connection or per process. It keeps grpc-go's server defaults, which advertise
no stream limit and ping a connection only after two hours without activity,
and it never returns `RESOURCE_EXHAUSTED`. It reads and sends one block at a
time through an unbuffered channel, so its server holds about one block per
stream.

### Requests being received

The server reads a whole request message into memory before the handler runs,
so it bounds how many requests it receives at once across every connection.
The bound is fixed and shared with native query and Explorer
([ADR-0019](../adrs/0019-transport-policy-ownership.md#public-grpc-listeners)):

- A request message larger than 2 MiB fails with `OUT_OF_RANGE` before the
  server buffers it. A `SendTransaction` of the largest valid transaction,
  2,000,000 bytes, fits.
- At most 32 requests are being received at once. A unary or server-streaming
  call holds its slot only until its request has arrived, so an open
  `GetBlockRange` or `GetMempoolStream` holds none. A request that has not
  arrived 20 seconds after its stream opened fails with `DEADLINE_EXCEEDED`.
- `GetTaddressBalanceStream` and gRPC reflection draw on a separate pool of
  32 slots and keep theirs until the client half-closes or the call ends. A
  stream that completes no message for 20 seconds fails with
  `DEADLINE_EXCEEDED`. gRPC reflection messages are also bounded at 2 MiB.
- When a pool is full, a request from a connection holding fewer slots takes
  the oldest slot of the connection holding the most, and the request that
  loses it fails with `UNAVAILABLE`. Otherwise the new request fails with
  `UNAVAILABLE` at once. `zinder_grpc_request_receive_refusals_total{pool}`
  counts refusals, `zinder_grpc_request_receive_evictions_total{pool}` counts
  evictions, and `zinder_grpc_request_receive_timeouts_total{pool}` counts
  timeouts.

One connection that opens many streams therefore cannot lock other clients
out, but clients spread over many connections still can keep a pool full.
Limit connections per client address at the edge. lightwalletd's fix for
GHSA-xrx5-jc6q-9h8r sets the same values and eviction rule.

### Block caches

In wallet serving, the compatibility runtime keeps up to four reader
generations open, as native query does. Each generation is a canonical and a
wallet secondary. Every canonical secondary reads through one canonical block
cache and every wallet secondary through one wallet block cache, so the
configured sizes bound the process: by default 128 MiB for the canonical
reader, or one eighth of the container memory limit up to 512 MiB, and 64 MiB
for the wallet reader. Under the Compose default memory limit of 4 GiB, which
derives a 512 MiB canonical cache, the two caches take 576 MiB. Index and
filter blocks that open table files pin can take a cache somewhat past its
size. Each open generation also keeps its own memtables, up to 16 MiB per
store by default, and the table readers of the files it holds open.
Compact-blocks serving keeps two canonical generations on one canonical block
cache.

## Request list limits

The server counts the elements of every list in a request before it decodes
the request, and refuses a request over the limit with `INVALID_ARGUMENT` and
the message `<Method> request field <field> carries <count> elements; at most
<limit> are accepted`. An empty address or txid suffix takes 2 bytes on the
wire but at least 24 once decoded, so without this check one 4 MiB request
could cost tens of MiB before a handler's own cap refused it.

| Method | Field | Limit | Handler cap |
| --- | --- | --- | --- |
| `GetTaddressBalance` | `addresses` | 512 | 256 addresses per balance |
| `GetAddressUtxos`, `GetAddressUtxosStream` | `addresses` | 10,000 | none |
| `GetMempoolTx` | `exclude_txid_suffixes` | 2,048 | 1,024 |
| `GetMempoolTx` | `poolTypes` | 1,000 | none |
| `GetBlockRange`, `GetBlockRangeNullifiers` | `poolTypes` | 1,000 | none |
| `GetTaddressTxids`, `GetTaddressTransactions` | `range.poolTypes` | 1,000 | none |

A limit is twice the handler's cap where one exists, so a request just over
the cap still gets the handler's error. `poolTypes` names at most five pools,
and the server reduces it to a set once per request, so 1,000 entries leave
room for any client while stopping a list that only inflates decoding.

lightwalletd checks the same fields before decoding, also at twice its handler
caps (lightwalletd `78c8476`), but reports the refusal as `INTERNAL`, because
grpc-go turns every codec error into that code. lightwalletd does not check
`range.poolTypes` in `GetTaddressTxids` and `GetTaddressTransactions`.

## Transaction lookup

`GetTransaction` returns a pending transaction the way lightwalletd does.
lightwalletd calls Zebra's `getrawtransaction`, which checks the mempool first
and omits `height` for a mempool transaction (Zebra 7.1.0,
`zebra-rpc/src/methods.rs:2114-2144` and
`zebra-rpc/src/methods/types/transaction.rs:313-317`), so lightwalletd answers
with the raw bytes at height 0 (lightwalletd `e7436d2`,
`frontend/service.go:474-485` and `common/common.go:765-779`). The protocol
defines height 0 as "in the mempool".

Zinder reads the canonical chain at the request's epoch first. When that read
misses, it asks the writer's live mempool through
`IngestControl.MempoolTransaction`:

- A pending transaction is returned with its raw bytes at height 0.
- A transaction in neither is `NOT_FOUND`, as in lightwalletd. The mempool
  miss counts only when the writer answered it at the same chain epoch as the
  canonical read
  ([ADR-0042](../adrs/0042-proven-transaction-absence.md)).
- A mempool miss at another chain epoch is `UNAVAILABLE`. The transaction may
  have been mined into a block this replica has not applied yet, so the lookup
  is retryable; a later lookup returns it mined or `NOT_FOUND`.
- On a store whose history starts at a checkpoint, a mempool miss is
  `CANONICAL_HISTORY_UNAVAILABLE` instead of `NOT_FOUND`, because the
  transaction may be mined below the checkpoint.
- A failed or timed-out mempool read is `UNAVAILABLE` rather than
  `NOT_FOUND`, because the transaction may be pending. So is a writer miss
  that names no chain epoch. The mempool read has the same 15-second timeout
  as the native wallet query's `IngestControl` calls, and the timeout covers
  the first connection to the writer as well as the call itself.
- The wallet-serving binary always connects the mempool surface to the
  writer's `IngestControl` endpoint. A `LightwalletdGrpcAdapter` built in Rust
  without `with_mempool_surface` cannot see pending transactions and answers
  `NOT_FOUND` for them.

Wallet SDKs depend on the distinction. The Swift SDK treats height 0 as
unmined and `NOT_FOUND` as an unknown transaction id
(zcash-swift-wallet-sdk `ebd0add`,
`Sources/ZcashLightClientKit/Modules/Service/GRPC/LightWalletGRPCService.swift:282-297`),
and the Android SDK marks a transaction it cannot fetch as `TxidNotRecognized`
(zcash-android-wallet-sdk `a95780a`, `CompactBlockProcessor.kt:2404-2430`).
A just-broadcast transaction therefore reads as pending, not unknown.

## Transparent address UTXOs

`GetAddressUtxos` and `GetAddressUtxosStream` follow lightwalletd's
`maxEntries` rule (lightwalletd `e7436d2`, `frontend/service.go:954`):

- `maxEntries = 0` returns every unspent output at or above `startHeight` for
  the requested addresses.
- A positive `maxEntries` returns at most that many outputs, the oldest first.

Rows are ordered by height across the whole address set, as Zebra's
`getaddressutxos` orders them for lightwalletd. Within one block, Zinder orders
rows by transaction id and then output index, where Zebra uses the
transaction's position in the block. The order differs only when a positive
`maxEntries` cuts a block's outputs in two.

One response returns at most `LightwalletdResponseLimits::max_address_utxos`
outputs, 100,000 by default, the same cap Zebra puts on `getaddressutxos`
(Zebra 7.1.0, `zebra-rpc/src/methods.rs:4142`). A request that would return
more fails with `RESOURCE_EXHAUSTED`. Zinder never drops rows silently, because
librustzcash, the Android SDK, Brave, and Zingo add these rows up as the
address balance. `GetAddressUtxosStream` has the same bound.

The protocol has no lossless way to page past the bound. A positive
`maxEntries` at or below the bound returns the oldest outputs, and a client can
continue from the next height only when that page ends at a block boundary. If
the page ends inside a block, repeating that block's height returns the same
first outputs again, and starting at the next height skips the rest of that
block. A single block that holds more outputs for the requested addresses than
the bound cannot be served.

lightwalletd on Zebra handles the same overflow differently. Zebra rejects the
query, and lightwalletd maps the unrecognized error to status `OK`, so the
client receives an empty UTXO list (`frontend/service.go:931-939`). Zinder
reports the failure instead of that empty success.

## Transparent address history

`GetTaddressTxids` and `GetTaddressTransactions` resolve `range` the way
lightwalletd resolves it on Zebra, so a request that works there works here:

- `range` and `range.start` are required; a missing one is
  `INVALID_ARGUMENT`.
- An absent `range.end`, or an end of 0, means the visible tip. lightwalletd
  defaults both to the chain tip (lightwalletd `e7436d2`,
  `frontend/service.go:128-137`).
- Both bounds are clamped to the visible tip, as Zebra's `getaddresstxids`
  clamps them (Zebra 7.1.0, `zebra-rpc/src/methods.rs:5163-5187`). An end
  above the tip reads to the tip, and a start above the tip reads the tip
  block. A start of 0 reads from genesis.
- An explicit end below the start is `INVALID_ARGUMENT`.

One case differs on purpose. lightwalletd rejects a span wider than
10,000,000 blocks with `INVALID_ARGUMENT` (`frontend/service.go:98` and
`:138-142`), which protects zcashd from an unbounded index scan. Zinder clamps
the end to the visible tip before reading, so the read is already bounded, and
it serves such a request to the tip. WebZjs birthday detection sends
`end = u64::MAX` (WebZjs `a50df94`,
`crates/webzjs-wallet/src/bindgen/wallet.rs:638-641`), and the Swift and
Android SDK Tor paths send no end at all (zcash-swift-wallet-sdk `ebd0add`,
`rust/src/tor.rs:274`; zcash-android-wallet-sdk `a95780a`,
`backend-lib/src/main/rust/tor.rs:246`).

## Certification work

1. Keep the protocol pin and generated-client coverage green. Updating the
   vendored proto is a protocol change, not an adapter-only change.
2. Run the live parity suite against the pinned reference-lightwalletd image,
   once with the pinned Zebra image and once with the pinned Zakura image as
   the node. It compares observable values and status codes; intentional
   operator metadata differences remain allow-listed. A release needs both
   legs green.
3. Keep the wallet-serving live tests focused on retained compact blocks, tree
   states, subtree roots, transaction bytes, transparent history and UTXOs, and
   strict below-floor failures.
4. Run the current Zcash Android SDK and ZODL lifecycle against the real
   Android-trusted TLS route. This is independent-client proof, not a
   substitute for protocol parity.
5. Before a public claim, validate the actual TLS/proxy deployment and record
   the exact commands, image digests, client version, network, and store floor
   with the release. No repository-side manifest schema is required.

## Reference inputs

The vendored protocol pin and parity workflow image digests are:

- `lightwallet-protocol` `v0.5.0`
  (`ac7cee052a1bf5d430985a478d39e8b513fc4bd4`);
- `lightwalletd` `v0.6.0`
  (`e7436d2adb9ab5828eb8a27de534e16218841fc2`) as the source reference, which
  parses the version-6 transactions that NU6.3 introduced;
- `electriccoinco/lightwalletd:v0.6.0@sha256:abc8199dae800e050415a3459f71c7850aeefeb1ca6a681e9b9eda7249ce8613`;
- `docker.io/zfnd/zebra:7.1.0@sha256:4f9536c8061e407f9f2ec81b7e0dd2f1762f81deec316abb1530dca8796a33ba`,
  started on Regtest with `deploy/zebra/regtest.toml`, which activates every
  upgrade through NU7 (see
  [Regtest through NU7](../runbooks/testing.md#regtest-through-nu7));
- `docker.io/zakuracore/zakura:1.8.6@sha256:e514c89b751b685686690098928507aaa994311e6b90c79373c55b0afaa45d50`,
  started on Regtest with `deploy/zakura/regtest.toml`, which uses the same
  schedule.

The pin is reproducibility, not evidence by itself. It makes a failing parity
run explainable and a passing one repeatable.
