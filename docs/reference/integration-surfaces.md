# Integration Surfaces

This page starts after a client has chosen Zinder. It maps each client shape to
the smallest suitable contract and records the boundary between protocol
coverage and consumer certification. For the architectural choice between
direct node access, an embedded indexer, a shared Zinder deployment, and
lightwalletd-compatible serving, see
[Indexer/wallet boundary](../architecture/indexer-wallet-boundary.md).

## Pick a surface

| Client shape | Integration surface | Constraint |
| --- | --- | --- |
| Existing lightwalletd client | `zinder-compat-lightwalletd` | The client already speaks `CompactTxStreamer`; changing the endpoint should not require a protocol rewrite. |
| Rust wallet or service that needs live events or broadcast | `zinder-client::RemoteChainIndex` | The consumer can link the Zinder Rust crates and reach a `zinder-query` deployment over gRPC. |
| Client that cannot link `zinder-client` | Vendored `WalletQuery` protos | The consumer generates a native gRPC client with its own language and toolchain. |
| Explorer or analytics application | `ExplorerQuery`, with `WalletQuery` where required | The consumer needs derived views rather than only wallet-sync artifacts. |

## Current consumer boundaries

Method coverage proves that Zinder has an appropriate public primitive. A
support claim additionally requires the consumer's create or import, sync,
recovery, send, mempool, and reorg flows against a selected release and
network.

| Consumer | Contract | Current claim |
| --- | --- | --- |
| [Zallet](https://github.com/zcash/zallet) | Native `WalletQuery` through the backend-neutral `Chain` and snapshot-scoped `ChainView` traits | The current source-built default Zinder backend is Regtest-certified for server metadata, network-upgrade activations, visible tip, block selectors and headers, full blocks, tree state, Sapling, Orchard, and Ironwood subtree roots, transaction lookup, transparent-address UTXOs and ascending history, chain events, mempool snapshot and events, and broadcast. Official Zallet packaging remains tracked in [Zallet #696](https://github.com/zcash/zallet/issues/696). |
| [ZODL](https://github.com/zodl-inc/zodl-android) | Zcash Android SDK `LightWalletEndpoint` over `CompactTxStreamer` | The current source-built ZODL and SDK pair has passed fresh-wallet, restore, transparent-history, send, mempool, mining, continuous-follow, and restart flows through the candidate's Android-trusted Testnet TLS route. This local certification does not claim a public operator deployment or reorg behavior. |

## Lightwalletd compatibility

`zinder-compat-lightwalletd` serves the vendored lightwalletd
`CompactTxStreamer` protocol by translating requests onto `WalletQueryApi`.
It serves indexed reads from an admitted exact-fence pair of canonical and
wallet-projection secondaries. It does not write canonical storage, build
artifacts independently, or use Zebra as a fallback for indexed history.

The compatibility runtime uses `zinder-source` only for explicit edge
capabilities: transaction broadcast, network-upgrade activation discovery, and
sparse tree-state fill where the query contract delegates that read upstream.
Those calls never substitute for indexed history or change the pinned serving
pair.

For a compatible wallet, Zinder is an endpoint-level replacement for
lightwalletd: the wallet keeps the `CompactTxStreamer` contract and changes the
server address. It is not an operator-level binary or configuration replacement.
Zinder has its own ingest, storage, query, readiness, and deployment model.

Public deployments terminate TLS, authentication, rate limiting, and quota controls before traffic reaches Zinder. The compatibility process speaks plaintext h2c by default and should be bound behind the operator's proxy boundary.
For the isolated `transactions` retention route, trusted TLS, compatibility
`/readyz` admission, external plaintext-unreachability, and ZODL endpoint
attribution are owned by the [Trusted TLS and ZODL compatibility admission
runbook](../runbooks/zodl-trusted-tls-certification.md).

## Native Rust clients

`zinder-client` is the canonical Rust integration crate:

- `RemoteChainIndex` is enabled by default and connects to a `WalletQuery` gRPC endpoint without a RocksDB dependency.
- `ServerInfo`, `Capability`, `CapabilityDescriptor`, and `ErrorReason` are
  client-owned public types. Generated protobuf messages and `tonic::Status`
  remain behind the SDK boundary, while unknown capability and error-reason
  strings are preserved for forward compatibility.
- `ChainSnapshot<'_, I>` captures one epoch from a borrowed `ChainIndex` and
  exposes its pinnable canonical reads without a repeated epoch parameter.
- `OwnedChainSnapshot<I>` provides the same surface over `Arc<I>`, including
  `Arc<dyn ChainIndex>`, for wallet adapters whose chain view must be cloneable
  and `'static`.

The contract is split across two async traits so the compiler expresses which calls a handle can serve:

- `ChainIndex` carries immutable network metadata plus canonical and wallet-projection reads. `RemoteChainIndex` implements the typed client contract; consumers preflight advertised capabilities before relying on optional reads.
- `EndpointBackedIndex` carries operations that need live endpoint-owned
  collaborators: transaction broadcast and chain value-pools require an
  admitted upstream source; the chain-event stream and live-mempool
  snapshot/events/overlays require the writer control boundary; and the
  wallet-plane server descriptor describes the endpoint itself. Only
  `RemoteChainIndex` implements it. The release query currently omits the
  chain-value-pools capability because method discovery alone does not prove
  the required payload or retained liveness semantics.

A consumer that broadcasts or subscribes bounds its handle `T: ChainIndex + EndpointBackedIndex`. Typed capability discovery (`CapabilityDescriptor::supports(Capability::…)`) probes the advertised set without matching raw strings.

The public traits and protocol include optional full-block and
transparent-outpoint methods. The admitted wallet-serving query advertises
full-block reads only when authenticated canonical retention is `all`.
Transparent-outpoint reads remain omitted until their concrete serving-pair
resolvers are implemented. Method presence is not a deployment support claim;
consumers must preflight exact capability strings.

When the canonical writer parks because a replacement crossed its reorg
window, new reads and open chain-event streams return
`IndexerError::ReorgWindowExceeded`, with the replacement depth and window when
the server reports them. Its retry policy is `OperatorActionRequired`: the
indexer serves a chain that no longer advances, so a wallet should stop
creating spends against it until an operator reconciles the indexer.

Capture calls `current_epoch` once. A remote serving pair that has advanced
returns `IndexerError::ChainEpochPinUnavailable`; its
retry policy is `RefreshChainEpoch`. The released wallet-serving runtime keeps
only its current exact read pair, so consumers must refresh rather than assume
historical epoch retention. Implementations may retain older epochs, but may
serve a pin only when they can answer that exact canonical epoch.

Native clients get typed errors, capability discovery, epoch-pinned reads,
chain-event cursors, transaction broadcast outcomes, mempool reads, and
transparent-address artifacts without depending on the lightwalletd
compatibility layer.

Inside Zinder, `zinder-query` and `zinder-compat-lightwalletd` read storage
through `WalletServingQuery` over an admitted `WalletServingReadPair`. That
service-internal composition is not a public SDK adapter.

The registry-ready Rust SDK consists of `zinder-core`, `zinder-proto`, and
`zinder-client`, all at the lockstep product version and workspace Rust 1.95
MSRV. Package CI builds each feature mode, checks documentation without
`protoc`, and compiles a standalone consumer from the extracted crate archives.
The packages become installable from crates.io only after a release publishes
that lockstep version; local repository consumers continue to resolve the same
edges by path.

## Vendoring the protocol

Consumers that cannot depend on `zinder-client` vendor the `.proto` files and
generate their own stubs. This includes non-Rust stacks and Rust consumers with
toolchain or native-dependency clashes. The protocol packages are
self-contained; no `googleapis` or third-party proto is imported.

### File set

The import closure under `crates/zinder-proto/proto/` is:

| Vendor when the consumer speaks | Files |
| --- | --- |
| `WalletQuery` | `zinder/v1/wallet/wallet.proto`, `zinder/v1/ops/server_info.proto` |
| `IngestControl` (adds to the above) | `zinder/v1/ingest/ingest.proto`, `zinder/v1/ops/readiness.proto` |
| `ExplorerQuery` (adds to the wallet set) | `zinder/v1/explorer/explorer.proto` |

`zinder/v1/ops/error.proto` sits outside every import closure but defines the `ErrorReason` enum; vendor it alongside so error handling compiles against the same vocabulary the wire carries.

### Pin and drift guard

Mirror the pattern Zinder itself uses for the lightwalletd protos (`crates/zinder-proto/proto/compat/lightwalletd/COMMIT` plus the `vendored-proto` job in `.github/workflows/ci-pr.yml`):

1. Copy the files preserving the `zinder/v1/...` directory layout and record the pinned Zinder commit hash in a `COMMIT` file next to them.
2. Generate stubs with the consumer's own toolchain (`tonic-prost-build` for Rust; any `protoc` plugin elsewhere). Do not edit the vendored files.
3. Add a CI job that fetches each file at the pinned commit and diffs it against the vendored copy, so local edits and upstream drift both fail loudly.

### Contract surfaces outside the protos

Two vocabularies bind the integration but do not appear in any `.proto` message definition:

- Capability strings. The authoritative table lives in `crates/zinder-proto/src/capabilities.rs`; servers advertise them through `ServerInfo.capabilities` and clients gate features on exact string match.
- Error reasons. `google.rpc.ErrorInfo` with `domain = "zinder.dev"` carries the string-form `ErrorReason` name on every failure; the [Error vocabulary](error-vocabulary.md) page owns the reason-to-code-to-retry semantics.

These two vocabularies plus `ServerInfo.contract_revision` are the machine-checkable integration keys. A vendored consumer asserts a minimum `contract_revision` at connect time and treats a lower value as an incompatible server.

### Feature detection

At connect time, call `ServerInfo` and check:

1. `network` matches the expected wire string exactly (`zcash-mainnet`, `zcash-testnet`, or `zcash-regtest`).
2. `contract_revision` meets the consumer's minimum.
3. Every capability the consumer requires is present in `capabilities`.

The release `zinder-query` composition admits one authenticated
`IngestControl` identity before opening storage or binding traffic. Admission
validates the exact service name, network, contract revision, and the seven
control methods required for pair publication, transaction lookup, mempool
snapshot and events, and the two admitted transparent mempool primitives. The query,
serving-pair publisher, live wallet handlers, and readiness probe all clone the
same admitted channel. The probe checks `WriterStatus` plus a bounded,
tip-coherent `MempoolSnapshot`; it does not repeat structural `ServerInfo`
discovery.

The native endpoint therefore advertises transaction lookup, mempool snapshot
and events, transparent mempool outputs-by-address and
spends-by-outpoint from that concrete composition. It omits
`wallet.address.transparent_balance_v1`: the legacy composite performs multiple
canonical and live calls without one authenticated mempool snapshot, so
provider presence alone is not sufficient admission evidence. Transaction-byte
support additionally requires authenticated transaction-blob retention.
Temporary ingest-control failure drains readiness without rewriting the
immutable capability set. Methods with no admitted provider or coherent
snapshot, including transparent mempool outputs-by-outpoint and transparent
balance, remain omitted and fail their capability guard before provider
access. These Zinder contract claims do not replace current Zallet or ZODL
consumer certification.
`wallet.events.chain_v1` remains independently derived from the admitted
serving pair.

## Server-side wallets

Server-side wallets can pair `zinder-client` with a higher-level wallet
library. Zinder owns consumer-neutral chain reads and broadcast; the wallet
process owns keys, trial decryption, note state, account state, transaction
building, and proving. Keep that boundary explicit instead of embedding a
wallet model in the chain-data service.

## Explorer and analytics views

Explorer-shaped reads use `WalletQuery` for canonical wallet-plane data and
`ExplorerQuery` for derived views. A consumer should call `WalletQuery` directly
for canonical blocks, transactions, tree state, broadcast, and wallet events.
It should call `ExplorerQuery` for summaries, search, history, distributions,
rankings, and other derived projections. The materialized-view plane consumes canonical
artifacts and event streams, owns its own storage, and can be rebuilt without
touching canonical chain state.

## References

- [Indexer/wallet boundary](../architecture/indexer-wallet-boundary.md)
- [Wallet data plane](../architecture/wallet-data-plane.md)
- [Materialized-view plane](../architecture/materialized-view-plane.md)
- [Protocol boundary](../architecture/protocol-boundary.md)
