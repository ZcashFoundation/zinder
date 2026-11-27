# Use cases

This page maps each Zcash consumer pattern to the Zinder surface and deployment pieces it needs. For capability details, reliability guarantees, and deployment profiles, see [Features and roadmap](features.md); for a comparison against other Zcash indexers, see [Indexer comparison](reference/indexer-comparison.md).

In every pattern, Zinder serves public chain data from your configured node. Keys, trial decryption, shielded balances, transaction building, and signing stay in your application.

## At a glance

| Consumer | Zinder surface | Enabled by default | Pattern summary |
| --- | --- | --- | --- |
| Light wallet | `CompactTxStreamer` on `zinder-compat-lightwalletd` | Yes | Existing lightwalletd client with no protocol changes |
| Browser wallet | gRPC-Web on `zinder-compat-lightwalletd` | Optional (CORS origins) | Browser SDK (e.g., WebZjs) over the same HTTPS endpoint |
| Native wallet or backend | `WalletQuery` on `zinder-query`, or `zinder-client` | Yes | Multi-call epoch-pinned reads, typed retries, capability checks |
| Payment or deposit tracker | `ChainEvents` + transparent queries on `zinder-query` | Yes | Resumable reorg-aware cursor + outpoint spend checks |
| Faucet or transaction sender | `BroadcastTransaction` + `Transaction` on `zinder-query` | Yes | Typed broadcast verdicts with multi-node failover |
| Block explorer | `ExplorerQuery` on `zinder-explorer` (plus optional `zinder-compat-cipherscan`) | Optional (`--preset explorer`) | Block/tx/address queries from canonical store; search, fees, mempool, and pools with optional views |
| Multi-product operator | All of the above | Explorer optional | Several products sharing one canonical index |

## Light wallets

Point an existing `CompactTxStreamer` wallet or SDK at `zinder-compat-lightwalletd`. No client code change is required.

- Choose the `wallet` preset for local transparent indexes or `light-wallet` for a smaller shielded-focused footprint ([Features and roadmap](features.md#deployment-profiles)).
- `Ping` stays disabled unless explicitly enabled for load testing, and `GetAddressUtxos` and transparent address history enforce Zinder-specific limits. See [Lightwalletd compatibility](reference/lightwalletd-compatibility.md).
- A named wallet release is supported once it passes end-to-end certification for the target network.

## Browser wallets

Point a browser SDK such as WebZjs at the same HTTPS URL native light wallets use, after enabling your wallet's browser origin on `zinder-compat-lightwalletd` ([Exposing over TLS](runbooks/exposing-over-tls.md#enable-browser-wallets)).

- Use a `wallet` or `light-wallet` endpoint so subtree roots and tree states are available alongside compact blocks.
- Prefer binary gRPC-Web (`application/grpc-web+proto`) for block ranges to avoid base64 payload expansion, and use `GetTaddressBalance` for unary transparent balances. See [Browser wallets](reference/integration-surfaces.md#browser-wallets).

## Native wallets and backends

Build new wallets, SDKs, and server-side services on `WalletQuery` (or the typed `zinder-client` Rust crate) rather than `CompactTxStreamer`:

1. Call `ServerInfo` on connect to confirm `network`, `contract_revision`, and required [capabilities](reference/capabilities.md).
2. Capture a chain snapshot (`snapshot()` / `at_epoch_id`) so block reads, tree states, and transparent queries agree on one chain epoch.
3. Branch on `google.rpc.ErrorInfo` reasons (or `IndexerError::retry_policy()` in Rust) instead of message text.

A server-side wallet pairs `zinder-client` with a wallet library such as `librustzcash` while keeping keys and scanning local. Start from the [Rust quickstart](reference/integration-surfaces.md#quickstart).

## Payment and deposit tracking

Backends that credit deposits or reconcile settlements combine three `WalletQuery` contracts:

- **`ChainEvents`:** Persist the opaque `cursor` from each `ChainCommitted` and `ChainReorged` event and pass it back on reconnect. Events are retained for 7 days by default; older gaps rebuild from block and address reads. See [Chain events](architecture/chain-events.md).
- **Transparent queries:** Read address UTXOs, height-bounded transaction IDs, and mined spend status for up to 1,024 outpoints per call via `TransparentOutpointSpendStatus`.
- **Mempool:** Observe unconfirmed activity through mempool snapshots and events, and detect shielded deposits by scanning compact blocks with your own viewing keys.

See [Wallet data plane](architecture/wallet-data-plane.md) for range and epoch rules.

## Faucets and transaction senders

Build and sign transactions in your service, submit them with `BroadcastTransaction`, and confirm status with `Transaction`:

- `BroadcastTransaction` returns typed verdicts (accepted, duplicate, or rejected with reason) and fails over across configured nodes on transport or consensus-branch failure.
- `Transaction` reports mined, in-mempool, or proven-absent status at one snapshot.

Block templates and mined-block submission stay on the node; miners and mining faucets call the node's JSON-RPC directly.

## Block explorers

Run with `--preset explorer` (or set `deployment.profile = "explorer"`) to start `zinder-explorer` and `zinder-compat-cipherscan` alongside the wallet plane. Core block, transaction, address history, address UTXO, and network statistics RPCs read `RocksDbCanonicalSecondary` directly without materialized views, while `explorer_views` (enabled automatically by the `explorer` preset) adds rebuildable views for commitment-root search, block production, and mempool analytics:

- Explorer views live in a separate, rebuildable store and never block wallet serving.
- Shielded addresses return a typed privacy refusal; the Explorer never scans shielded data.
- `zinder-compat-cipherscan` fronts an unchanged Cipherscan UI ([Cipherscan adapter](architecture/cipherscan-adapter.md)).

See [Explorer plane](architecture/explorer-plane.md) for setup and method contracts.

## Multi-product operators

Run one Zinder deployment to serve lightwalletd wallets, browser wallets, native backends, payment trackers, and an optional explorer simultaneously. Separate reader processes share one canonical index on the host, observe the same chain epochs, and avoid operating a separate indexer per product.

To deploy, start with [Deploying Zinder](runbooks/deploying.md).
