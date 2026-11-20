# Integration surfaces

This page tells you how to connect a client to Zinder: which endpoint to use, the smallest working call, and what the client must handle. It is for wallet, SDK, and backend developers. To match Zinder to your product, read [Use cases](../use-cases.md) and [Indexer comparison](indexer-comparison.md).

## Choose a path

| You have | Use | Service | Compose port |
| --- | --- | --- | --- |
| An existing lightwalletd client or SDK | `CompactTxStreamer` | `zinder-compat-lightwalletd` | 9067 |
| A browser wallet using lightwalletd | gRPC-Web on the same `CompactTxStreamer` endpoint | `zinder-compat-lightwalletd` | 9067 |
| A new native application or backend | Native `zinder.v1.wallet.WalletQuery` gRPC | `zinder-query` | 9102 |
| A Rust application | The `zinder-client` crate over `WalletQuery` | `zinder-query` | 9102 |
| An explorer or analytics backend | `zinder.v1.explorer.ExplorerQuery`, plus `WalletQuery` | `zinder-explorer` | 9068 |
| A server-side wallet | `zinder-client` plus a wallet library such as librustzcash | `zinder-query` | 9102 |

The ports are the defaults in `deploy/docker-compose.yml` and `deploy/docker-compose.explorer.yml`, bound to host loopback. Your operator may publish different ones.

## What every client must handle

**Transport.** Native gRPC uses plaintext HTTP/2 (h2c) behind the operator's proxy. TLS termination and client authentication belong in the operator's proxy, which the [security boundary](../architecture/service-operations.md#security-boundary) requires and [Exposing over TLS](../runbooks/exposing-over-tls.md) sets up. Connect to the proxy's `https://` address. `zinder-explorer` can also require a shared bearer token (`explorer.bearer_token_path`), which clients send as `authorization: Bearer <token>`; the other listeners rely on the proxy for authentication.

**Readiness.** A runtime that is not ready rejects new requests with `UNAVAILABLE` and `SERVICE_NOT_READY`. Retry with backoff. A request that fails with `REORG_WINDOW_EXCEEDED` cannot succeed until an operator repairs the indexer; stop creating spends against it. [Readiness](readiness.md) lists the causes.

**Capability discovery.** On connect, call `ServerInfo` and check three things:

1. `network` is exactly `zcash-mainnet`, `zcash-testnet`, or `zcash-regtest`, matching what you expect.
2. `contract_revision` is at least the revision your client was built for. `zinder-client` requires 8.
3. Every capability string your client depends on is present. Strings match exactly. [Capabilities](capabilities.md) lists them.

On `zinder-query`, a method whose capability is absent fails with `FAILED_PRECONDITION` and `ENDPOINT_CAPABILITY_UNAVAILABLE`. On `zinder-explorer`, a method whose backing store or `WalletQuery` endpoint is not configured fails with `FAILED_PRECONDITION` and `DEPENDENCY_NOT_CONFIGURED`. The `node` field describes the node that broadcasts transactions and can change while the server runs, so read it from a fresh `ServerInfo` instead of caching it.

**Errors.** Most native failures carry a `google.rpc.ErrorInfo` with `domain = "zinder.dev"` and a stable reason string. Branch on the reason, not the message. Some rejections carry only a gRPC status code. Listener overload returns `UNAVAILABLE`; retry it with backoff. Malformed request fields, such as a missing block selector or an oversized repeated field, return `INVALID_ARGUMENT`. Handle a status without `ErrorInfo` by its code. [Error vocabulary](error-vocabulary.md) gives each reason's status code and retry rule.

**Consistent reads.** Native responses carry the chain epoch they read. Pin related reads to one epoch with `at_epoch_id`, and restart the workflow when a pin fails with `CHAIN_EPOCH_PIN_UNAVAILABLE`. [Wallet data plane](../architecture/wallet-data-plane.md) explains the rules.

## Lightwalletd clients

Point the client's server address at the compatibility endpoint. Both wallet and compact-blocks modes support native gRPC reflection v1 and v1alpha. Clients using either version can discover `CompactTxStreamer` and its message types without local proto files. For example, use `grpcurl` to request server information.

```bash
grpcurl -plaintext -d '{}' 127.0.0.1:9067 \
  cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLightdInfo
```

A wallet-serving endpoint answers with `"taddrSupport": true`. An endpoint in compact-blocks mode answers `false` and serves block reads. Method-by-method behavior, limits, and differences from Go lightwalletd are in [Lightwalletd compatibility](lightwalletd-compatibility.md). A named wallet is supported once it passes end-to-end certification, as the [claim boundary](lightwalletd-compatibility.md#claim-boundary) states.

### Browser wallets

Give your browser wallet the same HTTPS lightwalletd URL as a native wallet. The operator [enables your wallet site's origin](../runbooks/exposing-over-tls.md#enable-browser-wallets) on that endpoint. For example, once your application has initialized WebZjs, read the Testnet tip using your operator's Testnet URL:

```javascript
const wallet = new WebWallet("test", "https://lightwalletd.example.org", 1, 1);
const tip = await wallet.get_latest_block();
```

Use a wallet-serving endpoint for sync. A compact-blocks endpoint can stream blocks, but wallet sync also needs subtree roots and tree state. Browser clients can call unary and server-streaming RPCs. For transparent balances, use `GetTaddressBalance`, which takes the address list in one request. Read a stream through its terminal gRPC status before treating the result as complete.

WebZjs requests binary gRPC-Web by default. If your SDK supports binary server streaming, use `application/grpc-web+proto` for block ranges. Text mode base64-encodes the same frames, adding about a third to the response body before HTTP compression.

## Native gRPC clients

`zinder-query` serves `WalletQuery` and also exposes gRPC reflection:

```bash
grpcurl -plaintext -d '{}' 127.0.0.1:9102 \
  zinder.v1.wallet.WalletQuery/ServerInfo
grpcurl -plaintext -d '{"start_height": 1, "end_height": 10}' 127.0.0.1:9102 \
  zinder.v1.wallet.WalletQuery/CompactBlocksInRange
```

It also serves `grpc.health.v1.Health`, which agrees with `/readyz`; see [gRPC health](readiness.md#grpc-health).

To generate a client, vendor the `.proto` files from `crates/zinder-proto/proto/` and keep the `zinder/v1/...` layout. The files import nothing outside this set.

| If you call | Vendor |
| --- | --- |
| `WalletQuery` | `zinder/v1/wallet/wallet.proto`, `zinder/v1/ops/server_info.proto` |
| `ExplorerQuery` | the wallet set, plus `zinder/v1/explorer/explorer.proto` |
| any service | `zinder/v1/ops/error.proto`, which defines the `ErrorReason` names that `ErrorInfo` carries |

Then:

1. Record the Zinder commit you copied from in a `COMMIT` file next to the protos.
2. Generate stubs with your own toolchain. Do not edit the vendored files.
3. Add a CI check that fetches each file at the pinned commit and diffs it against your copy, so local edits and upstream changes both fail loudly.

## Rust applications

`zinder-client` is the typed Rust SDK. It hides generated protobuf types and `tonic::Status` behind `ChainIndex`, `EndpointBackedIndex`, `ServerInfo`, and `IndexerError`. Releases publish `zinder-core`, `zinder-proto`, and `zinder-client` to crates.io at the product version. They need Rust 1.100 or later.

Add the client and an async runtime to your crate:

```bash
cargo add zinder-client
cargo add tokio --features macros,rt-multi-thread
```

This program prints nothing. It connects to a native `WalletQuery` endpoint, checks one capability, and reads the visible tip at a pinned epoch:

```rust
use zinder_client::{
    Capability, CapabilityDescriptor, ChainIndex, EndpointBackedIndex, IndexerError, Network,
    RemoteChainIndex, RemoteOpenOptions,
};

async fn read_visible_tip(endpoint: String) -> Result<(), IndexerError> {
    let index = RemoteChainIndex::connect(RemoteOpenOptions {
        endpoint,
        network: Network::ZcashMainnet,
    })?;
    let info = index.server_info().await?;
    if !info.supports(Capability::TransparentAddressUnspentOutputs) {
        // Fall back or report that this endpoint cannot serve the feature.
    }
    let snapshot = index.snapshot().await?;
    let visible_tip = snapshot.visible_tip_block().await?;
    let _ = visible_tip;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), IndexerError> {
    read_visible_tip("http://127.0.0.1:9102".to_owned()).await
}
```

An `https://` endpoint makes the client use TLS with the system's root certificates. The connection opens on the first call.

- `ChainIndex` holds stored reads. `snapshot()` captures one chain epoch and pins every later read through the snapshot to it. `OwnedChainSnapshot` does the same over an `Arc` for a view that must be cloneable and `'static`.
- `EndpointBackedIndex` holds calls that need a live endpoint: `server_info`, broadcast, chain events, and mempool reads. Bound a generic handle as `T: ChainIndex + EndpointBackedIndex` when you need both.
- Every `IndexerError` has a `retry_policy()`: `RefreshChainEpoch`, `RestartFromEarliestRetained`, `RetryWithBackoff`, `OperatorActionRequired`, or `ClientError`. Act on the policy rather than on the error text.
- The client checks the server's streams. A stream that breaks its contract, such as a repeated event cursor or a missing range header, fails with `IndexerError::MalformedResponse`. Reconnect from the last accepted cursor.

The crate's examples in `crates/zinder-client/examples/`, `observe_transparent_address.rs` and `observe_mempool_to_confirmed.rs`, show the snapshot, subscribe, and reconnect pattern end to end. Run one from a Zinder checkout against a running query endpoint, with the endpoint, the network, and a transparent address to watch:

```bash
cargo run -p zinder-client --example observe_transparent_address -- \
    http://127.0.0.1:9102 zcash-regtest <t-address>
```

## Explorer and analytics backends

Use `ExplorerQuery` on `zinder-explorer` for derived views: block and transaction pages, address activity, search, fees, mempool summaries, and value-pool history. Use `WalletQuery` directly for canonical blocks, transactions, tree states, broadcast, and chain events. [Explorer plane](../architecture/explorer-plane.md) covers freshness and privacy refusals, and [Chain data catalog](chain-data-catalog.md) maps data to methods.

## Server-side wallets

Pair `zinder-client` with a wallet library. Zinder serves chain reads, chain events, mempool state, and broadcast. Your process keeps keys, trial decryption, note and account state, transaction building, and proving. Zinder never receives viewing or spending keys.
