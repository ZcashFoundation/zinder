# Integration surfaces

This page tells you how to connect a client to Zinder: which endpoint to use, the smallest working call, and what the client must handle. It is for wallet, SDK, and backend developers. To match Zinder to your product, read [Use cases](../use-cases.md) and [Indexer comparison](indexer-comparison.md).

## Choose a path

You make two independent choices: which protocol your client speaks, and which Zcash network it reads.

### Choose a protocol

Zinder serves two wallet protocols on separate endpoints. A client speaks one of them.

- **You have a lightwalletd wallet or SDK.** Keep it. It speaks `CompactTxStreamer`, and `zinder-compat-lightwalletd` serves that protocol unchanged.
- **You are writing a new client.** Use the native `zinder.v1.wallet.WalletQuery` API, served by `zinder-query`. It adds reads pinned to one chain snapshot and a resumable event stream, which [Use cases](../use-cases.md) describes for wallets, backends, and payment tracking. The Rust `zinder-client` crate is a typed wrapper over this same API, so it needs a native endpoint.

| You have | Use | Service | Compose port |
| --- | --- | --- | --- |
| An existing lightwalletd client or SDK | `CompactTxStreamer` | `zinder-compat-lightwalletd` | 9067 |
| A browser wallet using lightwalletd | gRPC-Web on the same `CompactTxStreamer` endpoint | `zinder-compat-lightwalletd` | 9067 |
| A new native application or backend | Native `zinder.v1.wallet.WalletQuery` gRPC | `zinder-query` | 9102 |
| A Rust application | The `zinder-client` crate over `WalletQuery` | `zinder-query` | 9102 |
| An explorer or analytics backend | `zinder.v1.explorer.ExplorerQuery`, plus `WalletQuery` | `zinder-explorer` | 9068 |
| A server-side wallet | `zinder-client` plus a wallet library such as librustzcash | `zinder-query` | 9102 |

The ports are the defaults in `deploy/docker-compose.yml` and `deploy/docker-compose.explorer.yml`, bound to host loopback. Your operator may publish different ones.

### Choose a network

Each endpoint serves one Zcash network, and the two networks are separate chains with separate funds.

- **Mainnet** carries real ZEC. Use it for production chain data.
- **Testnet** is its own chain with test coins that have no value. Use it to build and test an integration.

Regtest is a private chain for local development and exists only on a Zinder you run yourself.

### Endpoints

Use the endpoint that matches both choices. On your own deployment, that is the Compose port from the protocol table. Compose adds 10000 to each port on Testnet and 20000 on Regtest. [Deploying Zinder](../runbooks/deploying.md) starts a deployment beside your own node.

The Zcash Foundation runs these public endpoints, which any client can use:

| Protocol | Mainnet | Testnet |
| --- | --- | --- |
| lightwalletd `CompactTxStreamer` | `https://lwd.zfnd.org:443` | `https://testnet.lwd.zfnd.org:443` |
| Native `WalletQuery`, including `zinder-client` | `https://zinder.zfnd.org:443` | `https://testnet.zinder.zfnd.org:443` |

Each address is a gRPC endpoint over TLS, not a website. Give it to a gRPC client, such as a wallet, `grpcurl`, or `zinder-client`. A browser that opens it gets no page.

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

Point the client's server address at the compatibility endpoint. Wallet, light-wallet, and compact-blocks serving support native gRPC reflection v1 and v1alpha. Clients using either version can discover `CompactTxStreamer` and its message types without local proto files. Use `grpcurl` to request server information.

```bash
grpcurl -plaintext -d '{}' 127.0.0.1:9067 \
  cash.z.wallet.sdk.rpc.CompactTxStreamer/GetLightdInfo
```

Wallet and light-wallet endpoints answer with `"taddrSupport": true`. Compact-blocks endpoints answer `false` and serve block reads. Light-wallet forwards selected wallet calls, so a node outage can interrupt those calls while the endpoint still serves local data. See [Lightwalletd compatibility](lightwalletd-compatibility.md) for method behavior and [During a node outage](readiness.md#during-a-node-outage) for retry conditions. A named wallet is supported once it passes end-to-end certification, as the [claim boundary](lightwalletd-compatibility.md#claim-boundary) states.

### Browser wallets

Give your browser wallet the same HTTPS lightwalletd URL as a native wallet. The operator [enables your wallet site's origin](../runbooks/exposing-over-tls.md#enable-browser-wallets) on that endpoint. For example, once your application has initialized WebZjs, read the Testnet tip using your operator's Testnet URL:

```javascript
const wallet = new WebWallet("test", "https://lightwalletd.example.org", 1, 1);
const tip = await wallet.get_latest_block();
```

Use a wallet or light-wallet endpoint for sync, which needs subtree roots and tree state as well as blocks. Browser clients can call unary and server-streaming RPCs. For transparent balances, use `GetTaddressBalance`, which takes the address list in one request. Read a stream through its terminal gRPC status before treating the result as complete.

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

`zinder-client` is the typed Rust SDK. It hides generated protobuf types and `tonic::Status` behind `ChainIndex`, `EndpointBackedIndex`, `ServerInfo`, and `IndexerError`. Releases publish `zinder-core`, `zinder-proto`, and `zinder-client` to crates.io at the product version.

### Quickstart

This quickstart builds a small program that connects to a native endpoint, confirms the server and its network, and prints the chain tip from one pinned snapshot. You need Rust 1.100 or later and the address of a `zinder-query` endpoint.

1. Create the project and add the client and an async runtime:

   ```bash
   cargo new zinder-quickstart
   cd zinder-quickstart
   cargo add 'zinder-client@=0.23.0'
   cargo add tokio --features macros,rt-multi-thread
   ```

2. Replace `src/main.rs` with this program:

   ```rust
   use std::env;
   use std::error::Error;

   use zinder_client::{
       ChainIndex, EndpointBackedIndex, Network, RemoteChainIndex, RemoteOpenOptions,
   };

   #[tokio::main]
   async fn main() -> Result<(), Box<dyn Error>> {
       let mut args = env::args().skip(1);
       let (Some(endpoint), Some(network_name)) = (args.next(), args.next()) else {
           return Err("usage: zinder-quickstart <endpoint> <mainnet|testnet|regtest>".into());
       };
       let network = match network_name.as_str() {
           "mainnet" => Network::ZcashMainnet,
           "testnet" => Network::ZcashTestnet,
           "regtest" => Network::ZcashRegtest,
           other => return Err(format!("unknown network: {other}").into()),
       };

       // No connection opens here. The first call below connects.
       let index = RemoteChainIndex::connect(RemoteOpenOptions { endpoint, network })?;

       // Fails with `IndexerError::NetworkMismatch` if the server is on another network.
       let info = index.server_info().await?;
       println!(
           "{} {} on {:?}, contract revision {}",
           info.service_name, info.service_version, info.network, info.contract_revision
       );

       // Every read through `snapshot` comes from this one chain epoch.
       let snapshot = index.snapshot().await?;
       let epoch = snapshot.chain_epoch();
       let tip = snapshot.visible_tip_block().await?;
       println!("chain epoch {}", epoch.id.value());
       println!("visible tip height {}", tip.height.value());
       println!("settled tip height {}", epoch.settled_tip_height.value());
       Ok(())
   }
   ```

3. Run it with the endpoint and the network you expect it to serve. Against the Foundation's public Testnet endpoint:

   ```bash
   cargo run -- https://testnet.zinder.zfnd.org:443 testnet
   ```

   Against a Zinder you run with the [Compose quickstart](../runbooks/deploying.md#quickstart-with-docker-compose) on Testnet:

   ```bash
   cargo run -- http://127.0.0.1:19102 testnet
   ```

   The program prints the server identity and three values from one snapshot. This output is from a Mainnet endpoint:

   ```text
   zinder-query 0.23.0 on ZcashMainnet, contract revision 14
   chain epoch 1499
   visible tip height 3604138
   settled tip height 3603138
   ```

### Endpoint and network

The endpoint and the network are separate arguments, and the client checks that they agree. Pass the network the endpoint serves. When they differ, the first call fails before your program reads any chain data:

```text
Error: NetworkMismatch { expected: ZcashTestnet, actual: "zcash-mainnet" }
```

For a local endpoint, pass the network of your own deployment. For a Foundation endpoint, pass `Network::ZcashMainnet` or `Network::ZcashTestnet` to match the column you took the address from in [Endpoints](#endpoints).

### TLS

The URL scheme selects the transport. An `http://` endpoint uses plaintext HTTP/2, which fits a Zinder on the same host or private network. An `https://` endpoint uses TLS, and the client verifies the server certificate against the operating system's trusted root certificates. Install your system's CA certificate package in a minimal container image. An endpoint whose certificate comes from a private certificate authority needs that authority in the system trust store.

### A native endpoint is not a lightwalletd endpoint

`zinder-client` speaks the native `zinder.v1.wallet.WalletQuery` protocol, which `zinder-query` serves. It does not speak lightwalletd's `CompactTxStreamer` protocol, which `zinder-compat-lightwalletd` serves on a different address. A lightwalletd URL does not work with `zinder-client`, and a lightwalletd client does not work against a native endpoint. If you have an existing lightwalletd client, keep it and follow [Lightwalletd clients](#lightwalletd-clients).

### Beyond the quickstart

- `ChainIndex` holds stored reads. `snapshot()` captures one chain epoch and pins every later read through the snapshot to it. `OwnedChainSnapshot` does the same over an `Arc` for a view that must be cloneable and `'static`.
- `EndpointBackedIndex` holds calls that need a live endpoint: `server_info`, broadcast, chain events, and mempool reads. Bound a generic handle as `T: ChainIndex + EndpointBackedIndex` when you need both.
- Every `IndexerError` has a `retry_policy()`: `RefreshChainEpoch`, `RestartFromEarliestRetained`, `RetryWithBackoff`, `OperatorActionRequired`, or `ClientError`. Act on the policy rather than on the error text.
- The client checks the server's streams. A stream that breaks its contract, such as a repeated event cursor or a missing range header, fails with `IndexerError::MalformedResponse`. Reconnect from the last accepted cursor.

To follow the chain, open a `ChainEvents` stream, store the cursor from each event, and resume from it after a disconnect. [Chain events](../architecture/chain-events.md#resume-with-a-cursor) describes the contract. The crate's examples in `crates/zinder-client/examples/`, `observe_transparent_address.rs` and `observe_mempool_to_confirmed.rs`, show the snapshot, subscribe, and reconnect pattern end to end. Run one from a Zinder checkout against a running query endpoint. It takes the endpoint, the network as `zcash-mainnet`, `zcash-testnet`, or `zcash-regtest`, and a transparent address to watch. This command uses the Compose Regtest port. Set `ADDRESS` to your own address first:

```bash
ADDRESS='your-transparent-address'
cargo run -p zinder-client --example observe_transparent_address -- \
    http://127.0.0.1:29102 zcash-regtest "$ADDRESS"
```

## Explorer and analytics backends

Use `ExplorerQuery` on `zinder-explorer` for derived views: block and transaction pages, address activity, search, fees, mempool summaries, and value-pool history. Use `WalletQuery` directly for canonical blocks, transactions, tree states, broadcast, and chain events. [Explorer plane](../architecture/explorer-plane.md) covers freshness and privacy refusals, and [Chain data catalog](chain-data-catalog.md) maps data to methods.

## Server-side wallets

Pair `zinder-client` with a wallet library. Zinder serves chain reads, chain events, mempool state, and broadcast. Your process keeps keys, trial decryption, note and account state, transaction building, and proving. Zinder never receives viewing or spending keys.
