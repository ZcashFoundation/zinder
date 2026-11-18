# Cipherscan adapter

This page explains what `zinder-compat-cipherscan` does, where its responsibilities end, and which Cipherscan routes it can serve today. It is for operators deciding whether to point Cipherscan at Zinder and for developers changing the adapter.

## What it is

Cipherscan is a Zcash block explorer with its own REST and WebSocket API. `zinder-compat-cipherscan` keeps Cipherscan's routes and JSON shapes and answers them from Zinder's native `ExplorerQuery` and `WalletQuery` services. Cipherscan can switch its API base URL to the adapter without frontend changes.

```text
Cipherscan UI
    -> zinder-compat-cipherscan     REST and WebSocket routes, Cipherscan JSON
        -> ExplorerQuery            zinder-explorer
        -> WalletQuery              zinder-query
        -> market-price provider    CoinGecko by default
```

The adapter is optional and is not a release image. It builds from the workspace as the `zinder-compat-cipherscan` binary. It needs a running [Explorer](explorer-plane.md), which in turn needs `[ingest].explorer_views = true`, and a `zinder-query` endpoint. It opens no storage and keeps only bounded in-memory caches, so it restarts without a backfill. The [service README](../../services/zinder-compat-cipherscan/README.md) shows how to run it.

## Boundaries

- **Zinder** owns chain facts, views, coverage, epoch-consistent reads, and explicit absence. Native names stay product-neutral.
- **The adapter** owns HTTP routing, request coercion, Cipherscan field names, address encoding, display units, bounded product calculations, and caches. It never turns a missing value into zero or an empty successful result.
- **Sidecars**, which are services Cipherscan would run beside the adapter, own data that is not a Zcash chain fact: labels, prices beyond the built-in provider, mining-pool branding, node monitoring, names, bridges, and identity or risk analytics. The adapter answers those routes with a stable unavailable or degraded response.

When a page needs a reusable chain fact that the native APIs cannot supply, the fact is added to Zinder as a narrow native surface. The adapter never calls the node directly and never stores chain data.

## How failures reach Cipherscan

The Explorer runtime cannot serve seven methods that need canonical-store reads, including `BlockProductionSeries`, `BlockTransactions`, and `CommitmentRootSearch` ([Explorer plane](explorer-plane.md)). It also never advertises `explorer.transaction.history_v2` or `explorer.transaction.intrinsic_value_balances_v1`. A route that depends on one of these fails in one of three ways:

- If the adapter calls the method directly, the native `FAILED_PRECONDITION` becomes HTTP 502 with code `upstream_error`.
- If the adapter checks the capability first, it returns HTTP 503 with code `upstream_field_unavailable`.
- If a weaker native source exists, the route answers HTTP 200 with `degraded: true` and an `unavailable` list.

## Coverage matrix

Status is what each route family does against the current Explorer runtime. Any `/api/*` route not listed returns 404 with a "not implemented" error.

| Routes | Status | Native source |
| --- | --- | --- |
| `/api/info`, `/api/blockchain-info`, `/api/network/health` | Works | `VisibleTipBlock`, `ServerInfo`, readiness |
| `/api/blocks`, `/api/blocks/list`, `/api/network/blocks/recent`, `/api/block/:heightOrHash` | 502: needs `BlockProductionSeries` or `BlockTransactions` | Explorer block methods |
| `/api/tx/:txid`, `/api/tx/:txid/raw`, `/api/tx/raw/batch`, `/api/tx/:txid/verbose` | Works for retained bytes. Verbose decoding is degraded. Output `spent` is `null` while the spend view does not cover the response epoch | `TransactionDetail`, `WalletQuery.Transaction` |
| `/api/tx/broadcast` | Works | `WalletQuery.BroadcastTransaction` |
| `/api/transactions/list`, `/api/tx/shielded`, `/api/scan/orchard` | 503: needs `explorer.transaction.history_v2` | `TransactionHistory` |
| `/api/search/anchor/:root` | 502: needs `CommitmentRootSearch` | Explorer |
| `/api/mempool`, `/api/mempool/tx/:txid` | Works | `MempoolSnapshot`, `TransactionDetail` |
| Root WebSocket | `mempool_tx` and `mempool_removed` frames work. At the next block, `new_block` hydration fails and subscribers are closed with code 1013 | `WalletQuery.ChainEvents`, `WalletQuery.MempoolEvents`, `BlockProductionSeries` |
| `/api/address/:address`, `/api/rich-list`, `/api/supply/transparent-breakdown` | Works | `TransparentAddressActivity`, `TransparentAddressRanking` |
| `/api/labels`, `/api/label/:address` | Degraded: needs a label sidecar | None |
| `/api/supply`, `/api/circulating-supply`, `/api/network/halving` | Works | `ValuePoolSummary` plus local consensus rules |
| `/api/network/emission`, `/api/pools/overview`, `/api/network/pool-history` | Current values work. History and deltas are degraded because `ValuePoolBalanceHistory` has no rows | `ValuePoolSummary`, `ValuePoolBalanceHistory` |
| `/api/network/stats` | 502: needs `BlockProductionSeries` | Explorer |
| `/api/network/fees` | Works | `FeeSummary` |
| `/api/network/fee-distribution` | Degraded to conventional fees, because `PaidFeeDistribution` is never advertised | `ConventionalFeeDistribution` |
| `/api/shielded/list`, `/api/pools/flows` | Works | `ValuePoolFlowHistory`, `ValuePoolFlowSummary` |
| `/api/mining/rewards`, `/api/network/mining-metrics` | 502: needs `BlockProductionSeries` | Explorer |
| `/api/mining/pool-distribution` | 503: needs `explorer.block.production_time_range_v1` | `BlockProductionInTimeRange` |
| `/api/mining/pool-ranking`, `/api/mining/hashrate-share`, `/api/mining/miner-behavior`, `/api/mining/zodl-leaderboard` | Degraded: needs a sidecar | None |
| `/api/network/protocol-stats`, `/api/stats/shielded-count`, `/api/stats/shielded-daily`, `/api/analytics/usage-clock`, `/api/privacy-stats`, `/api/analytics/anonymity-set`, `/api/analytics/shielding-distribution` | Works once the underlying views have coverage | `TransactionComponentSummary`, `BlockActivityDistribution`, value-pool methods |
| `/api/blend-check`, `/api/blend-check/split`, `/api/privacy/common-amounts`, `/api/tx/:txid/linkability` | Works once coverage is complete. Results are heuristics, not identity or safety claims | Value-pool flow summaries, `TransactionDetail` |
| Other `/api/privacy/*` graph, cluster, and risk routes | Degraded: needs a sidecar | None |
| `/api/pools/turnstile` | 503 "rebuilding" response, as Cipherscan returns | None |
| `/api/price`, `/api/price/at` | Works through the configured price provider | None |
| `/api/crosschain/*`, `/api/name/*` | Unavailable: needs a sidecar | None |
| `/api/uncles/stats`, `/api/uncles/forks` | Degraded: observed reorgs only, without displaced-block rows | `ChainReorgHistory` |
| `/api/uncles`, `/api/uncle/:hash` | 503: needs the displaced-block archive | `DisplacedBlockHistory`, `DisplacedBlockDetail` |
| `/api/uncle/report`, `/api/uncles/nodes`, `/api/network/peers`, `/api/network/nodes*`, `/api/crosslink/*`, `/api/finalizers*` | Degraded or unavailable: needs crawler, fork-monitor, or Crosslink data | None |
| `/api/migration/*` | Supply audit works. Migration analytics are degraded because they need `explorer.transaction.intrinsic_value_balances_v1` | `ValuePoolSummary`, `TransactionHistory` |
| `/api/lightwalletd/scan` | 404: not implemented. Compact-block scans belong to `zinder-compat-lightwalletd` | None |
| `/api/network/chain-size-history` | Degraded: needs operational telemetry | None |

## Known limitations

The adapter computes consensus economics itself instead of reading them from the node: block subsidy, funding-stream and lockbox amounts, halving height, emission, target spacing, and difficulty. These appear on `/api/network/stats`, `/api/network/halving`, `/api/network/emission`, and `/api/circulating-supply`.

It takes them from the `zebra-chain` release Zinder is built with. Mainnet and Testnet use that release's built-in parameters. Regtest uses its default Regtest parameters, not the activation heights the node reports. The values can therefore differ from what the node pays when:

- the node runs Regtest or a test network with its own activation heights, funding streams, or lockbox disbursements; or
- the node release, such as a Zakura release, applies consensus or fee rules that this `zebra-chain` release does not.

## Rules for changing the adapter

1. Cipherscan route names and presentation stop at the adapter. Native contracts carry only chain facts, freshness, coverage, and absence.
2. A response that cannot prove its data exposes the capability, coverage, or unavailable state. It never invents rows, zeroes, finality, prices, labels, or risk conclusions.
3. A reusable durable fact belongs in a Zinder view, with reorg and restart semantics. A product-only join stays in the adapter or a sidecar.
4. Sidecar and unavailable rows are deliberate scope limits. A new Cipherscan route does not by itself justify new Zinder storage or a product-specific native API.
