# Public interfaces

This page defines the names Zinder uses across Rust types, protocols,
configuration, errors, metrics, and documentation, and the rules for adding a
new one. Read it before you name or rename anything a caller, operator, or
client can see. The values themselves live in reference pages:
[configuration](../reference/configuration.md),
[capabilities](../reference/capabilities.md), and
[error vocabulary](../reference/error-vocabulary.md).

## Core vocabulary

Zinder keeps three kinds of state apart. Canonical state is the chain as
Zinder indexed it from the node. The wallet projection is query state that
`zinder-projector` derives from canonical state for wallets. Materialized views
are optional explorer aggregates. Use `canonical`, `wallet projection`, and
`materialized view` for these three things and for nothing else.

### Chain state

| Term | Plain meaning | Where it lives |
| --- | --- | --- |
| `ChainEpoch` | One immutable snapshot of the visible best chain. Its id starts at 1 and grows by one with each durable canonical commit, so epoch ids and chain-event sequences share one identity space. | `zinder-core` |
| `visible_tip` | The best block in a chain epoch. | `ChainEpoch` |
| `settled_tip` | The highest block buried at least `reorg_window_blocks` deep. | `ChainEpoch` |
| `artifact_schema_version` | The durable artifact schema version of the serving binary, the version of the contract for compact blocks and scan data. It equals `schema_version` in `WalletServerInfo`; it is not the store layout version. Every served epoch carries it, including the first-seen epoch of a mempool entry. | `ChainEpoch` |
| `ChainView` | Response envelope: a `ChainEpoch` plus optional `indexed_tip`, `upstream_tip`, and `materialized_views` status. | `zinder.v1.wallet` |
| `CanonicalEventFence` | Proof of one writer position: chain epoch, event sequence, visible tip, and sequence digest. A reader compares fences to prove it sees exactly what the writer committed. | `zinder-store` |
| `CanonicalWriterFence` | The wire form of the writer's fence, returned by `CanonicalControl.WriterStatus`. Writer and readers both encode it with `zinder_proto::wire::canonical_writer_fence_message`. | `zinder.v1.ingest` |
| `CanonicalActivationSchedule` | The network-upgrade activation table the writer admitted, with its revision and admission tip. | `zinder-store` |
| `CanonicalBlockFacts` | Everything about one block that replay and materialized views need. | `zinder-core` |
| `ChainEvent` | A durable canonical append or replacement. | `zinder-store`, `zinder-client` |
| `MempoolEvent` | A live-pool change: added, invalidated, or mined. | `zinder-store`, `zinder-client` |

### Serving

| Term | Plain meaning | Where it lives |
| --- | --- | --- |
| `CanonicalReader` | The asynchronous, engine-neutral interface to canonical data held at one admitted fence. | `zinder-query` |
| `WalletProjectionReader` | The asynchronous, engine-neutral interface to wallet projection state held at one admitted source identity. | `zinder-query` |
| `StorageReadError` | Why a canonical or wallet read returned no value. `QueryError` converts it without changing the public outcome. | `zinder-query` |
| `RocksDbCanonicalReader`, `RocksDbWalletReader` | The `RocksDB` implementations of the two readers, over admitted secondaries. | `zinder-query` |
| `WalletServingReadPair` | A canonical reader and a wallet reader proven to describe the same fence, called a serving pair. A request that reads both captures one pair when it starts. | `zinder-query` |
| `WalletServingPairPublisher` | The reader-process owner that catches up, admits, and publishes serving pairs. | `zinder-query` |
| `SupervisedRuntime` | The supervision that stops a wallet-serving runtime's gRPC listener and background tasks together, and ends the calls still open at its drain deadline. | `zinder-runtime` |
| `WalletQueryApi` | The wallet read and broadcast trait that the gRPC adapters call. | `zinder-query` |
| `WalletServingQuery` | The `WalletQueryApi` implementation over a replaceable serving pair. The native and lightwalletd-compatible runtimes share it. | `zinder-query` |
| `TreeState` | A tree-state read bound to one chain epoch. It carries typed per-pool `finalState` frontier bytes, `None` for a pool not active at that height. Native gRPC encodes them as the `z_gettreestate`-shaped JSON payload. The lightwalletd adapter hex-encodes each pool and rejects `GetTreeState` before Sapling activation with `INVALID_ARGUMENT`, matching Go lightwalletd. Native WalletQuery reads still return inactive pools as `None`. | `zinder-query` |
| `NativeWalletEndpointCapabilities` | The immutable capability set a native query derives once storage and providers are admitted. | `zinder-query` |
| `MaterializedViewConsumer` | An explorer view builder that applies retained chain events. | `zinder-materialized-views` |
| `MaterializedViewStore` | The separate store for materialized-view rows, cursors, coverage, and schemas. | `zinder-materialized-views` |

### Node

| Term | Plain meaning | Where it lives |
| --- | --- | --- |
| `NodeSource` | Typed reads and block fetches from the upstream node. | `zinder-source` |
| `NodeSet` | The ordered node list, `[node]` then `[node_fallbacks.N]`, read through one active endpoint. | `zinder-source` |
| `ActiveNodeEndpoint` | The list position a runtime currently reads from, reported as `active_node` on `/readyz`. | `zinder-runtime` |
| `BroadcastNodeProbe` | What a reader checks on first contact with a broadcast endpoint: its methods, its activation table, and whether its best chain holds a given block. | `zinder-source` |
| `OrderedBroadcaster` | The ordered, verified endpoints a wallet query broadcasts through. | `zinder-query` |

### Client SDK

| Term | Plain meaning | Where it lives |
| --- | --- | --- |
| `ChainIndex` | The consumer trait for stored chain reads. Calls that need a live endpoint, such as server info, broadcast, chain events, and mempool reads, are on `EndpointBackedIndex`. | `zinder-client` |
| `ChainSnapshot` | A borrowed view whose canonical reads all use one captured `ChainEpoch`. It has no mempool surface. | `zinder-client` |
| `OwnedChainSnapshot` | The cloneable, `Arc`-backed form of `ChainSnapshot` for callers that keep a `'static` view. | `zinder-client` |
| `MempoolSnapshotView` | One page of the live pool with a resume cursor, the canonical epoch captured before the read, and a source tip equal to that epoch's visible tip. | `zinder-client` |
| `ServerInfo`, `Capability`, `CapabilityDescriptor` | Client-owned server identity and capability types. | `zinder-client` |
| `IndexerError`, `ErrorReason`, `RetryPolicy` | Client-owned error types. | `zinder-client` |

Readiness names (`ReadinessCause`, `ReadinessInput`, `ReadinessDependency`)
are defined in [Readiness](../reference/readiness.md).

## Naming rules

Names optimize for honesty first, then domain specificity, the vocabulary
above, and brevity.

- Do not put temporal labels such as `new`, `legacy`, `next`, or `v2` in source
  identifiers. Version numbers belong only to persisted and wire contracts
  that carry a version, such as capability strings and proto packages.
- "Service" means a deployable runtime. Do not name types `*Service`,
  `*Manager`, `*Handler`, or `*Helper`, and do not name modules `utils`,
  `common`, or `helpers`. A name required by a protocol contract, such as the
  vendored `CompactTxStreamer`, is the exception.
- Name a module for the domain behavior it contains, for example
  `wallet_serving_pair`, `wallet_serving_pair_publisher`,
  `materialized_view_replay`, or `canonical_block_context`.
- Name a crate or module for a technology only when it holds that technology's
  boundary, as `zinder-rocksdb-bulk-load` and `zinder-wallet-rocksdb` do.
- Prefix wallet projection types with `Wallet` and reusable materialized-view
  framework types with `MaterializedView`.
- Reserve `fact` for semantic source records such as `CanonicalBlockFacts`,
  `TransactionPublicFacts`, and `TransparentSpendFact`. It is not a runtime,
  topology, migration, or backend label.
- Use `settled_tip` for the reorg-window boundary. Do not use `safe`, which is
  ambiguous, or `finalized`, which collides with Zcash consensus finality.
- `construction` and `follow` name the writer's two durable modes: building a
  fresh store, then following the node's tip. `BulkCatchup` and
  `FollowingTip` are writer phase labels only, not names of a store, backend,
  or ingestion path.
- Metric labels are bounded enumerations. Never put addresses, transaction
  ids, paths, URLs, or error text in a label. A node endpoint is labeled by its
  list position. Log events use stable snake-case action names such as
  `canonical_live_append_committed`. [Metrics](../reference/metrics.md) lists
  the names.
- When you add or rename a public name, change the protocol, configuration,
  error, metric, and documentation names together. Do not keep an alias unless
  a current external contract requires it.

## Runtimes and crates

| Runtime | Role |
| --- | --- |
| `zinder-ingest` | Only canonical writer and owner of the live mempool |
| `zinder-projector` | Only wallet projection writer |
| `zinder-query` | Native `WalletQuery` gRPC server |
| `zinder-compat-lightwalletd` | Lightwalletd-compatible `CompactTxStreamer` server |
| `zinder-explorer` | Optional `ExplorerQuery` server |
| `zinder-compat-cipherscan` | Optional Cipherscan-compatible REST and WebSocket adapter |

[Architecture overview](overview.md) explains how they fit together.

Three crates are published: `zinder-core` (domain values), `zinder-proto`
(generated protocol modules), and `zinder-client` (the typed Rust client).
Every other crate is internal: `zinder-store`, `zinder-source`,
`zinder-runtime`, `zinder-wallet-projection`, `zinder-wallet-rocksdb`,
`zinder-materialized-views`, `zinder-rocksdb-bulk-load`, and
`zinder-testkit`.

## Rust API shape

- Public operations accept and return domain types, not RocksDB handles,
  column families, or untyped byte maps.
- Writer and reader roles use different types. A read-only process cannot get
  a primary handle through a flag on a generic store.
- A fallible constructor validates immutable identity before it returns a
  handle.
- Range requests use typed bounds and enforce explicit maximum sizes.
- Pagination cursors bind to the filters, order, network, and source fence
  that produced them.
- Public traits describe what a consumer can do. Concrete RocksDB types stay
  in composition roots and storage adapters.
- Serving runtimes read stored data only through `CanonicalReader` and
  `WalletProjectionReader`. Their signatures name no storage engine, they fail
  with `StorageReadError`, and each implementation keeps blocking storage work
  off the async runtime. A method answers one serving question, so a request
  that needs several lookups makes one read. The compact-blocks mode, whose
  generation lifecycle is `RocksDB` code, reads its range chunks through
  `CanonicalReader`, driving each read with `read_on_blocking_thread` on the
  blocking thread that holds the generation.
  [Storage backend](storage-backend.md#read-interfaces) describes them and
  their error vocabulary.
- Library crates expose per-boundary `thiserror` enums. Public domain crates
  never expose `tonic::Status`, `rocksdb` types, or transport client errors.
  Each server boundary error (`QueryError`, `StoreError`, `ExplorerError`)
  implements `zinder_proto::BoundaryError` to name its `ErrorReason`.

### Client SDK rules

- `zinder-client` enables its remote adapter by default and has no normal
  dependency on storage or RocksDB crates.
- Before discovery, the remote client checks the server's network, its
  `contract_revision` against `MIN_SUPPORTED_CONTRACT_REVISION`, and that it
  advertises `wallet.read.network_upgrade_activations_v1`.
- Network-upgrade activations live on `ChainIndex` itself, not on a
  snapshot.
- A snapshot removes the epoch argument from every pinnable canonical read and
  always forwards its captured epoch id. Mempool state, event subscriptions,
  broadcast, and current address history stay on their own traits and are
  never presented as epoch-pinned.
- Transport metadata and errors reach callers only as client-owned
  `ServerInfo`, `Capability`, and `ErrorReason` types. Unknown capability and
  reason strings stay available as `Unknown(String)` variants. Every wallet
  operation an external client preflights has a `Capability` variant, so
  callers never copy wire strings. Generated protobuf types are not public SDK
  vocabulary.
- `ChainSnapshot::transparent_outpoint_spend_status` returns one
  `TransparentOutpointSpendStatus` per requested outpoint, in request order:
  `Unspent`, `SpentBy`, or `NotFound`. The client splits a list longer than
  `MAX_OUTPOINTS_PER_SPEND_STATUS_REQUEST` (1,024) into requests pinned to one
  epoch.
- `NotFound` from `ChainIndex::transaction_by_id` or a spend-status read is a
  proven absence. When the server cannot prove an absence, the client returns
  `IndexerError::TransactionAbsenceUnverified` or
  `IndexerError::CanonicalHistoryUnavailable` instead.
  [Wallet data plane](wallet-data-plane.md) describes both reads.

Inside Zinder, the serving runtimes read through `WalletServingQuery` and an
admitted `WalletServingReadPair`. They do not expose a storage-backed client
adapter.

## Protocol ownership

`zinder-proto` owns every protobuf Zinder serves or calls. Domain and storage
crates use their own types and convert at the service adapter.

| Surface | Rust module | Services | Served by |
| --- | --- | --- | --- |
| Native wallet API | `zinder_proto::v1::wallet` | `WalletQuery` | `zinder-query` |
| Native explorer API | `zinder_proto::v1::explorer` | `ExplorerQuery` | `zinder-explorer` |
| Private control APIs | `zinder_proto::v1::ingest` | `CanonicalControl`, `ProjectorControl`, `IngestControl` | `zinder-ingest`, `zinder-projector` |
| Shared operations messages | `zinder_proto::v1::ops` | `ServerInfo`, `ErrorReason`, readiness messages | Embedded in the surfaces above |
| Lightwalletd compatibility | `zinder_proto::compat::lightwalletd` | `CompactTxStreamer` | `zinder-compat-lightwalletd` |
| Zebra indexer client | `zinder_proto::external::zebra_indexer_rpc` | `Indexer` (client only) | Called by `zinder-source` |

- `WalletQuery` is the native contract for wallets and applications.
  [Wallet data plane](wallet-data-plane.md) describes it.
- `ExplorerQuery` serves explorer, dashboard, and analytics shapes.
  [Explorer plane](explorer-plane.md) describes it.
- The control APIs are private and must never be exposed to clients.
  `IngestControl` requires a bearer token when it listens outside loopback,
  and an enabled `ProjectorControl` always requires one. See
  [ADR-0011](../adrs/0011-ingest-control-transport-security.md) and
  [Service operations](service-operations.md#security-boundary).
- The lightwalletd schemas are vendored unchanged from
  `zcash/lightwallet-protocol`. The pin is in
  `crates/zinder-proto/proto/compat/lightwalletd/COMMIT`, with provenance in
  `UPSTREAM.md` beside it. Compatibility names stay identical to the external
  contract even where native names differ. Both serving modes expose those
  schemas through native gRPC reflection v1 and v1alpha.
  [Lightwalletd compatibility](../reference/lightwalletd-compatibility.md)
  covers method behavior.
- Upstream node protocols belong to
  [Node source boundary](node-source-boundary.md), and storage encodings to
  [Storage backend](storage-backend.md).

Generated Rust modules and descriptor sets are checked in under
`crates/zinder-proto/generated/`, so building Zinder or a crate that depends on
it needs neither `protoc` nor a build script. Only the private
`zinder-proto-codegen` tool regenerates them. Service crates never hand-write
`prost::Message` types or generate private copies of a shared protocol. Domain
crates never expose generated types in their public APIs.

### Protocol evolution

- An additive field gets a new tag. A removed field reserves its tag and name.
- An in-place change to the meaning of an existing wire surface increments
  `zinder_proto::CONTRACT_REVISION`, which servers report as
  `contract_revision` in `ServerInfo`.
- New or changed optional behavior gets a new capability string, with its own
  `_vN` suffix.
- A new `ErrorReason` gets a new scalar value, a policy entry, a client
  variant, and a round-trip test. Renaming, removing, or repurposing a reason
  is a breaking change.
- Vendored lightwalletd and external Zebra schemas change only with an
  explicit update to their pinned source and provenance record.

## Wire conventions

The generated lightwalletd server accepts an optional `EncodedBlockRange` implementation for `GetBlockRange`. Each stream element contains one complete `CompactBlock` protobuf payload. Tonic applies the configured compression, framing, and message limits. Existing server implementations and generated clients use their ordinary typed interfaces.

These rules apply at every protocol boundary.
[ADR-0002](../adrs/0002-serialization-and-byte-order.md) records why.

- Native hash strings use RPC byte order as 64 lowercase hex characters.
- Inside Zinder, hashes are typed values in internal byte order. Each protocol
  adapter converts at its boundary. Lightwalletd-compatible `bytes` hash
  fields carry internal byte order except for the hash in a
  `GetTreeState(BlockID { height: 0, hash })` request and the
  `SubtreeRoot.completingBlockHash` response field. Both use RPC byte order.
- Protobuf bytes are checked for exact length before they become typed hashes,
  outpoints, or commitments.
- Wire translations of an identifier live in one place:
  `crates/zinder-core/src/wire/` for pure domain encodings and
  `crates/zinder-proto/src/wire/` for proto enum mappings. To add a wire
  field, find or add the function for that concept there and call it from the
  boundary. Inline `transaction_id.as_bytes()`, inline
  `format!("{:08x}", ...)`, hard-coded capability strings, and duplicate
  network-name tables elsewhere fail the `wire_invariants.rs` and
  `capability_string_uniqueness.rs` tests.
- Persisted keys for heights, positions, timestamps, outpoints, and address
  script hashes use the `zinder_core::wire` codecs. Storage encodings and
  protobuf encodings never share a serializer just because they hold the same
  value.
- An optional field means a genuine absence or an unavailable capability. It
  is never filled with a zero value to simplify clients.
- An integer field names its unit when the type alone does not, as in
  `observed_at_millis` or `minimum_amounts_zat`.
- Enum scalar values and field tags are never renumbered. An unknown enum
  value fails or degrades as the method's contract says, never by mapping to a
  valid domain value.
- Cursors are opaque. Clients never parse or construct them.
- Every repeated request field has an element bound that the server checks
  before decoding. [Configuration](../reference/configuration.md) lists the
  bounds.
- An error that a handler raises from a typed boundary error carries
  `google.rpc.ErrorInfo` with `domain = "zinder.dev"` and one stable reason.
  Transport refusals and some request-shape checks, including many
  lightwalletd-compatible ones, carry only a status code and message. Do not
  encode retry policy in error text. [Error vocabulary](../reference/error-vocabulary.md) lists the
  reasons.
