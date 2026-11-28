# ADR-0002: Choose serialization per boundary and use RPC byte order on the wire

Status: Accepted

## Context

Zinder turns data into bytes at several boundaries, and each one fails in a different way:

- RocksDB keys must sort in the same order as network and height, or range reads return the wrong blocks.
- Stored chain values must decode within explicit bounds and refuse truncated, oversized, or unknown-version bytes.
- Compact blocks must serve both the native `WalletQuery` protocol and lightwalletd compatibility without making either wire schema the storage format.
- Some storage records change shape over time and need a schema-evolution story.
- Stream cursors must be small, opaque to clients, and refuse tampering or reuse against another store.

One serializer cannot serve all of these. A general serializer does not control key byte order, and a fixed layout cannot evolve the way protobuf can.

Hashes add a second problem. A Zcash 32-byte hash exists in two byte orders. The protocol specification calls the raw SHA-256d output **internal byte order**; consensus serialization and Zinder's storage use it. It calls the byte-reversed form **RPC byte order**; `zcash-cli`, wallets, block explorers, and the specification's own printed hashes use it, and ZIP 308 uses the same term for transaction ids. When Zinder's native protocol carried hashes in internal byte order, a transaction id copied from a wallet missed on lookup, because the consumer passed the form people see. Fixing that in each consumer would leave the same trap for the next one.

## Decision

1. **Ordered keys** use fixed big-endian layouts owned by the storage crates, built from the `zinder-core` wire codecs. No general serializer produces key bytes.
2. **Canonical chain values and control records** use fixed, versioned binary layouts. Decoders check exact lengths and refuse truncated input, trailing input, and unknown versions.
3. **Compact blocks** are stored natively in Canonical Schema v2 as canonical settled shielded segments (`ZSHRNG02`, carrying pre-encoded `DEFAULT_RANGE` lightwalletd `CompactBlock` frames in `Slab 0`), paired `ZSTSUP01` supplement segments (Tag-1 compact transparent supplement and Tag-2 extensible block and transaction metadata), and versioned `ZSLVTL01` records in `live_tail_block` for unsealed heights above `sealed_tip` (covering settled-but-unsealed blocks up to `settled_tip` as well as the live reorg suffix). Construction, appends, and reorg replacements check each compact block and supplement against its canonical header before writing, and segment readers verify checksums, row digests, and height linkage. The compatibility adapter streams shielded and nullifier range frames directly from `ZSHRNG02` and `live_tail_block` (projecting narrowed pool subsets and nullifier payloads on the fly without duplicating settled history in an external signed projection; see [ADR-0015](0015-wallet-runtime-composition.md)), while regenerating full `CompactBlock` records when transparent fields are requested. Changing either the stored wire payload format or the versioned segment and live-tail framing is therefore a storage schema change. `GetBlockNullifiers` clears non-nullifier payloads and retains every stored transaction. `GetBlockRangeNullifiers` first selects transactions by shielded pool. An empty or transparent-only pool list selects all shielded pools.
4. **Variable-shape storage records**, such as retained mempool events and materialized-view rows, use storage-specific protobuf messages that are separate from RPC messages.
5. **Stream cursors** use a versioned binary layout authenticated with HMAC-SHA256 under a per-store key. The native protocols carry a cursor as those raw bytes, and a client sends the bytes back unchanged. Unpadded base64url is the form for contexts that need text and is never applied on the wire. A cursor from another store fails authentication.
6. **Hash byte order.** Storage and keys keep internal byte order. In the native protocols, every hash a person reads, copies, or searches for, such as a block hash or transaction id, is a lowercase hex string in RPC byte order. Hashes inside the compact-block scanning payload and shielded subtree roots stay raw bytes, as lightwalletd clients expect. Lightwalletd uses RPC byte order for the `SubtreeRoot.completingBlockHash` response and the `GetTreeState(BlockID { height: 0, hash })` request. Its remaining `bytes` hash fields use internal byte order.
7. **One translation site.** Conversion between the two byte orders happens only in the `zinder-core` and `zinder-proto` wire modules. "RPC byte order" and "internal byte order" are the only terms Zinder uses for the two forms.

[Public interfaces](../architecture/public-interfaces.md#wire-conventions) lists the wire conventions, and [Storage backend](../architecture/storage-backend.md) describes the stored formats.

## Evidence

- Decision 3, pre-encoded settled compact-block frames: measured (historical Schema v1 `ZSHRNG01` prototype motivating Canonical Schema v2 `ZSHRNG02`). On a frozen Testnet store, a warm lightwalletd `GetBlockRange` cost 1.67 to 2.08 µs of reader CPU per block when the reader fetched, validated, converted, and re-encoded canonical rows, and 0.48 to 0.53 µs per block when it sent pre-encoded wire frames from signed segments, at 1 and 16 clients. About half of the row-decode path's CPU was decoding, validation, conversion, and re-encoding ([#372](https://github.com/ZcashFoundation/zinder/issues/372)). Canonical Schema v2 moves that pre-encoded shielded representation directly into canonical `ZSHRNG02` settled segments and `live_tail_block`, eliminating the duplicate second copy ([ADR-0014](0014-consumer-neutral-wallet-data-plane.md)).

## Consequences

- Contributors work with more than one binary format, and the storage crates hand-write and test fixed-layout parsers with golden fixtures and malformed-input cases.
- Store keys and cursors stay small, explicit, and stable across library upgrades.
- A change to a stored protobuf message, including a change of a hash field's byte order, changes the on-disk format. The owning store refuses the old layout, and the operator builds a fresh store.
- A hex hash field is about twice the size of raw bytes. That cost is negligible for lookup responses. The compact-block stream, where size matters, keeps raw bytes.
- A structural test fails the build when code outside the wire modules converts hash bytes inline.

## Alternatives rejected

- **One serializer everywhere.** Protobuf cannot control key byte order and adds framing ambiguity to cursors. Fixed layouts cannot replace protobuf's schema evolution.
- **A compact Rust serializer, such as `postcard`, for durable records.** It is not self-describing, so durable records would lose the evolution and repair story protobuf gives.
- **`rkyv` or another zero-copy format for canonical storage.** Alignment, validation, and migration constraints are too risky for chain data. A materialized-view cache may adopt one privately only after profiling shows at least a 30 percent P99 improvement on a real workload, including validation cost.
- **FlatBuffers or Cap'n Proto.** Either would add a second IDL beside the protobuf that lightwalletd compatibility already requires.
- **Internal byte order on the native wire.** It keeps storage and wire identical but pushes byte reversal onto every consumer, which is the defect this decision removes.
- **Both forms as parallel fields.** Zinder's consumers are its own clients, so one breaking change costs less than a deprecation cycle.

## Revision history

- 2026-11-27. Reconciled decision 3 and evidence for Canonical Schema v2 (`ZSHRNG02`, `ZSTSUP01`, and `ZSLVTL01`), retiring the separate derived signed projection copy while keeping direct wire streaming.
- 2026-11-20. Decision 3 assigns pool selection to the compatibility adapter, including narrowed nullifier requests from wallets. [Lightwalletd compatibility](../reference/lightwalletd-compatibility.md#getblockrange-and-getblockrangenullifiers).
- 2026-11-20. Decision 3 records which transactions unary nullifier requests retain. Decision 6 records the RPC-order hash in `GetTreeState` requests. [Lightwalletd compatibility](../reference/lightwalletd-compatibility.md#gettreestate-and-getlatesttreestate).
- 2026-11-20. Decision 3 specifies when writers check header identity and what readers validate.
- 2026-11-23. Decision 3 separates canonical storage formats from the adapter's derived, signed projection of settled history, and records the reader CPU it saves.
