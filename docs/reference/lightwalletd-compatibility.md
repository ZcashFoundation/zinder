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
retention fails before the listener binds. Later pair lag or provider outage
changes readiness without rewriting `GetLightdInfo`.

`GetLightdInfo` derives its upgrade fields from one activation table and the
served tip on every request:

- `consensusBranchId` is the branch of the upgrade active at the tip.
- `upgradeName` and `upgradeHeight` describe the next pending upgrade, the
  first one that activates above the tip. They are empty and 0 when no upgrade
  is pending, as the lightwalletd protocol defines and lightwalletd reports.
- `saplingActivationHeight` is the Sapling activation height.

In both serving modes that table is the schedule the canonical writer
admitted. A running compatibility process therefore reports a newly admitted
upgrade, such as NU7, as pending while its tip is below the activation height,
and as the active `consensusBranchId` once the tip reaches it. Before the
writer's first admission, it is the table discovered from the node at
startup, which reader admission requires to equal the store's construction
table. See
[ADR-0039](../adrs/0039-writer-admitted-network-upgrade-schedules.md).

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
  node syncs.
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
`GetBlock`, and `GetBlockRange`. Every other `CompactTxStreamer` RPC returns
`UNIMPLEMENTED`, including wallet queries, mempool methods, tree/subtree
methods, transparent-address methods, transaction submission, and ping.
`GetLightdInfo` reports the admitted network and tip with
`taddrSupport = false`; node discovery and liveness are still required for
that identity response. Its fields follow
[Identity and live-tip responses](#identity-and-live-tip-responses).

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

Unary block lookups validate the selector before reading. Height 0 with a
32-byte hash is a hash lookup. Height 0 with an empty hash is an unspecified
identifier, so `GetBlock` returns `INVALID_ARGUMENT` for it in both serving
modes, not `NOT_FOUND`. `GetBlockNullifiers` applies the same rule where it is
served.

## Transaction lookup

`GetTransaction` returns a pending transaction the way lightwalletd does.
lightwalletd calls Zebra's `getrawtransaction`, which checks the mempool first
and omits `height` for a mempool transaction (Zebra 7.0.0,
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
  as the native wallet query's `IngestControl` calls.
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
(Zebra 7.0.0, `zebra-rpc/src/methods.rs:4142`). A request that would return
more fails with `RESOURCE_EXHAUSTED` and asks the client to page with
`startHeight` and `maxEntries`. Zinder never drops rows silently, because
librustzcash, the Android SDK, Brave, and Zingo add these rows up as the
address balance.

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
  clamps them (Zebra 7.0.0, `zebra-rpc/src/methods.rs:5163-5187`). An end
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
2. Run the live parity suite against pinned Zebra and reference-lightwalletd
   images. It compares observable values and status codes; intentional
   operator metadata differences remain allow-listed.
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
- `docker.io/zfnd/zebra:7.0.0@sha256:a70d2be8ffef3a4b6e37355724e36a0e6c1838e46baa28bfb216739f00cf2d34`,
  started on Regtest with `deploy/zebra/regtest.toml`, which activates every
  upgrade through NU7 (see
  [Regtest through NU7](../runbooks/testing.md#regtest-through-nu7)).

The pin is reproducibility, not evidence by itself. It makes a failing parity
run explainable and a passing one repeatable.
