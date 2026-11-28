# Cipherscan adapter

This page explains what `zinder-compat-cipherscan` does, where its responsibilities end, and what each Cipherscan route returns. It is for operators deciding whether to point Cipherscan at Zinder and for developers changing the adapter.

## What it is

Cipherscan is a Zcash block explorer with its own REST and WebSocket API. `zinder-compat-cipherscan` keeps Cipherscan's routes and JSON shapes and answers them from Zinder's native `ExplorerQuery` and `WalletQuery` services. Cipherscan can switch its API base URL to the adapter without frontend changes.

```text
Cipherscan UI
    -> zinder-compat-cipherscan     REST and WebSocket routes, Cipherscan JSON
        -> ExplorerQuery            zinder-explorer
        -> WalletQuery              zinder-query
        -> market-price provider    CoinGecko by default
```

The adapter is optional. Enable it via `zinder serve --preset explorer` (or `zinder run compat-cipherscan`) from the unified `zinder` binary and `ghcr.io/zcashfoundation/zinder` OCI image, or run the `zinder-compat-cipherscan` binary directly from the workspace. It pairs with [Explorer](explorer-plane.md) and `zinder-query`, opens no storage of its own, and keeps only bounded in-memory caches, so it restarts without a backfill. The [service README](../../services/zinder-compat-cipherscan/README.md) documents standalone flags and environment variables.

## Boundaries

- **Zinder** owns chain facts, views, coverage, epoch-consistent reads, and explicit absence. Native names stay product-neutral.
- **The adapter** owns HTTP routing, request coercion, Cipherscan field names, address encoding, display units, bounded product calculations, and caches. It never turns a missing value into zero or an empty successful result.
- **Sidecars**, which are services Cipherscan would run beside the adapter, own data that is not a Zcash chain fact: labels, prices beyond the built-in provider, mining-pool branding, node monitoring, names, bridges, and identity or risk analytics. The adapter answers those routes with a stable unavailable or degraded response.

When a page needs a reusable chain fact that the native APIs cannot supply, the fact is added to Zinder as a narrow native surface. The adapter never calls the node directly and never stores chain data.

## How failures reach Cipherscan

`zinder-explorer` serves the canonical-backed explorer methods (`BlockProductionSeries`, `BlockProductionInTimeRange`, `BlockTransactions`, `CommitmentRootSearch`, `DisplacedBlockHistory`, and `DisplacedBlockDetail`), plus `intrinsic_value_balances` and `explorer.transaction.history_v2` once its canonical secondary and view coverage match the canonical tip ([Explorer plane](explorer-plane.md)). `PaidFeeDistribution`, `ValuePoolBalanceHistory`, and `MempoolEventCounts` remain unadvertised. When a route's required capability is unadvertised or the canonical/view coverage is not ready yet, it responds in one of three ways:

- If the adapter calls the method directly, a native `FAILED_PRECONDITION` becomes HTTP 502 with code `upstream_error`.
- If the adapter checks the capability first, it returns HTTP 503 with code `upstream_field_unavailable`.
- If a weaker native source exists, the route answers HTTP 200 with `degraded: true` and an `unavailable` list.

## Coverage matrix

Status is what each route family does against the current Explorer runtime when both the materialized-view store and the canonical secondary are open. Any `/api/*` route not listed returns 404 with a "not implemented" error.

| Routes | Status | Native source |
| --- | --- | --- |
| `/api/info`, `/api/blockchain-info`, `/api/network/health` | Works | `VisibleTipBlock`, `ServerInfo`, readiness |
| `/api/blocks`, `/api/blocks/list`, `/api/network/blocks/recent`, `/api/block/:heightOrHash` | Works (`BlockDetail` / `BlockTransactions` / `DisplacedBlockDetail` for orphaned hashes) | `BlockProductionSeries`, `BlockTransactions`, `DisplacedBlockDetail` |
| `/api/tx/:txid`, `/api/tx/:txid/raw`, `/api/tx/raw/batch`, `/api/tx/:txid/verbose` | Works for retained bytes. Verbose decoding is degraded. Output `spent` is `null` while the spend view does not cover the response epoch | `TransactionDetail`, `WalletQuery.Transaction` |
| `/api/tx/broadcast` | Works | `WalletQuery.BroadcastTransaction` |
| `/api/transactions/list`, `/api/tx/shielded`, `/api/scan/orchard` | Works once transaction history covers the canonical tip (`explorer.transaction.history_v2`) | `TransactionHistory` |
| `/api/search/anchor/:root` | Works, including retained displaced matches since archive activation | `CommitmentRootSearch` |
| `/api/mempool`, `/api/mempool/tx/:txid` | Works | `MempoolSnapshot`, `TransactionDetail` |
| Root WebSocket | Works (`mempool_tx`, `mempool_removed`, and `new_block` frames) | `WalletQuery.ChainEvents`, `WalletQuery.MempoolEvents`, `BlockProductionSeries` |
| `/api/address/:address`, `/api/rich-list`, `/api/supply/transparent-breakdown` | Works | `TransparentAddressActivity`, `TransparentAddressRanking` |
| `/api/labels`, `/api/label/:address` | Degraded: needs a label sidecar | None |
| `/api/supply` | Requires a wallet endpoint that serves chain value pools | `ValuePoolSummary` |
| `/api/circulating-supply`, `/api/network/halving` | Works | Visible tip height and local consensus rules |
| `/api/network/emission`, `/api/pools/overview`, `/api/network/pool-history` | 503 while the Explorer omits `explorer.value_pool.balance_history_v1` | `ValuePoolSummary`, `ValuePoolBalanceHistory` |
| `/api/network/stats` | Works | `BlockProductionSeries` |
| `/api/network/fees` | Works | `FeeSummary` |
| `/api/network/fee-distribution` | Degraded to conventional fees, because `PaidFeeDistribution` is never advertised | `ConventionalFeeDistribution` |
| `/api/shielded/list`, `/api/pools/flows` | Works | `ValuePoolFlowHistory`, `ValuePoolFlowSummary` |
| `/api/mining/rewards`, `/api/network/mining-metrics` | Works | `BlockProductionSeries` |
| `/api/mining/pool-distribution` | Works once block production time view has coverage | `BlockProductionInTimeRange` |
| `/api/mining/pool-ranking`, `/api/mining/hashrate-share`, `/api/mining/miner-behavior`, `/api/mining/zodl-leaderboard` | Degraded: needs a sidecar | None |
| `/api/network/protocol-stats`, `/api/stats/shielded-count`, `/api/stats/shielded-daily`, `/api/analytics/usage-clock`, `/api/privacy-stats`, `/api/analytics/anonymity-set`, `/api/analytics/shielding-distribution` | Works once the underlying views have coverage | `TransactionComponentSummary`, `BlockActivityDistribution`, value-pool methods |
| `/api/blend-check`, `/api/blend-check/split`, `/api/privacy/common-amounts`, `/api/tx/:txid/linkability` | Works once coverage is complete. Results are heuristics, not identity or safety claims | Value-pool flow summaries, `TransactionDetail` |
| Other `/api/privacy/*` graph, cluster, and risk routes | Degraded: needs a sidecar | None |
| `/api/pools/turnstile` | 503 "rebuilding" response, as Cipherscan returns | None |
| `/api/price`, `/api/price/at` | Works through the configured price provider | None |
| `/api/crosschain/*`, `/api/name/*` | Unavailable: needs a sidecar | None |
| `/api/uncles/stats`, `/api/uncles/forks` | Degraded: observed reorgs without explicit external uncle counts | `ChainReorgHistory` |
| `/api/uncles`, `/api/uncle/:hash` | Works | `DisplacedBlockHistory`, `DisplacedBlockDetail` |
| `/api/uncle/report`, `/api/uncles/nodes`, `/api/network/peers`, `/api/network/nodes*`, `/api/crosslink/*`, `/api/finalizers*` | Degraded or unavailable: needs crawler, fork-monitor, or Crosslink data | None |
| `/api/migration/*` | Supply audit works. Migration analytics work once transaction history covers canonical tip with intrinsic value balances | `ValuePoolSummary`, `TransactionHistory` |
| `/api/lightwalletd/scan` | 404: not implemented. Compact-block scans belong to `zinder-compat-lightwalletd` | None |
| `/api/network/chain-size-history` | Degraded: needs operational telemetry | None |

## Consensus economics

The adapter computes consensus economics itself instead of reading them from the node: block subsidy, funding-stream and lockbox amounts, halving height, emission, target spacing, and difficulty. These appear on `/api/network/stats`, `/api/network/halving`, `/api/network/emission`, and `/api/circulating-supply`.

It takes them from the `zebra-chain` release Zinder is built with. Mainnet and Testnet use that release's built-in parameters. Regtest uses its default Regtest parameters, not the activation heights the node reports. The values can therefore differ from what the node pays when:

- the node runs Regtest or a test network with its own activation heights, funding streams, or lockbox disbursements; or
- the node release, such as a Zakura release, applies consensus or fee rules that this `zebra-chain` release does not.

## Rules for changing the adapter

1. Cipherscan route names and presentation stop at the adapter. Native contracts carry only chain facts, freshness, coverage, and absence.
2. A response that cannot prove its data exposes the capability, coverage, or unavailable state. It never invents rows, zeroes, finality, prices, labels, or risk conclusions.
3. A reusable durable fact belongs in a Zinder view, with reorg and restart semantics. A product-only join stays in the adapter or a sidecar.
4. Sidecar and unavailable rows are deliberate scope boundaries. A new Cipherscan route does not by itself justify new Zinder storage or a product-specific native API.
