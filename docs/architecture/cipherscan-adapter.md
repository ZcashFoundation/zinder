# Cipherscan Adapter Architecture

`zinder-compat-cipherscan` preserves the Cipherscan REST paths and JSON shapes
while reading chain data from Zinder's native APIs. Cipherscan can therefore
move its API base URL without a frontend migration. The adapter is an edge
translation service, not a second chain indexer and not a Cipherscan-shaped
Zinder core API.

```text
Cipherscan UI
    |
    v
zinder-compat-cipherscan
    |  REST path, query compatibility, JSON, product calculations
    v
ExplorerQuery / WalletQuery / operations endpoints
    |
    v
canonical store and replayable materialized views
```

## Boundaries

- **Zinder** owns canonical data, durable reusable materialized views, capability and
  coverage metadata, epoch-coherent reads, and explicit absence. Native names
  remain product-neutral.
- **The adapter** owns HTTP routing, request coercion, Cipherscan response
  shapes, address encoding, display units, bounded product calculations,
  caches, and stable degraded responses. It does not infer unavailable facts
  or turn missing values into zeroes or empty successful results.
- **Sidecars** own product enrichments that are not Zcash chain facts, including
  labels, market data, mining attribution, external-node monitoring, names,
  bridges, and identity-oriented risk analytics. They do not require Zinder
  schema or materialized-view changes.
- **Unavailable** marks an intentional boundary: the adapter returns an
  explicit unavailable or degraded response until an owner provides the source
  fact. It is not a request to synthesize an approximation in Zinder.

The adapter prefers one Zinder snapshot or epoch for a page response. When a
page needs a reusable chain fact that native queries cannot supply truthfully,
that fact is added as a narrow Zinder surface rather than joined from direct
Zebra calls or copied into adapter storage.

For arbitrary time-window block-production reads, materialized-view identity, frozen
continuations, reorg handling, and completeness are defined by
[ADR 0033](../adrs/0033-time-indexed-block-production.md). This document only
records how the adapter consumes that contract. The executable acceptance
procedure lives in the
[Cipherscan adapter verification runbook](../runbooks/cipherscan-adapter-verification.md).

## Coverage Matrix

| Route family | Current status | Zinder source | Adapter responsibility | Remaining requirement | Ownership (Zinder, adapter, sidecar, unavailable) |
| --- | --- | --- | --- | --- | --- |
| Chain identity and health: `/api/info`, `/api/blockchain-info`, `/api/network/health` | Complete | `WalletQuery.VisibleTipBlock`, `ExplorerQuery.ServerInfo`, `WalletQuery.ServerInfo`, operations readiness | Preserve Cipherscan envelopes and distinguish query-plane health from direct-node availability | None for current fields | Zinder, adapter |
| Recent blocks and block detail: `/api/blocks`, `/api/blocks/list`, `/api/network/blocks/recent`, `/api/block/:heightOrHash` | Complete | `ExplorerQuery.BlockProductionSeries`, `ExplorerQuery.BlockTransactions`, `WalletQuery.Transaction`, final-note commitment-root artifacts | Pagination, difficulty and display units, standard-script address decoding, coinbase presentation, Cipherscan row names; `finality_status` is `Finalized` only at or below the response epoch's settled tip | Pool branding is external; nonstandard or absent facts remain unavailable | Zinder, adapter, sidecar |
| Mined transaction detail and bytes: `/api/tx/:txid`, `/api/tx/:txid/raw`, `/api/tx/raw/batch`, `/api/tx/:txid/verbose` | Complete for retained bytes and mined detail; verbose decoding is deliberately degraded | `ExplorerQuery.TransactionDetail`, `WalletQuery.Transaction`, transaction-byte retention | Map inputs, outputs, spent state, fee fields, pool balances, and raw-byte envelopes | Historical bytes outside retention and a reusable decoded-transaction contract are unavailable | Zinder, adapter, unavailable |
| Broadcast and transaction browsing: `/api/tx/broadcast`, `/api/transactions/list`, `/api/tx/shielded` | Complete | `WalletQuery.BroadcastTransaction`, `ExplorerQuery.TransactionHistory` v2 | Map typed broadcast outcomes; translate Cipherscan paging to filter- and fence-bound native cursors | None for the supported confirmed-history contract | Zinder, adapter |
| Commitment-root search: `/api/search/anchor/:root` | Complete for canonical matches; displaced matches are activation-limited | `ExplorerQuery.CommitmentRootSearch`, final-note commitment-root artifacts, displaced-root archive capability | Map canonical and retained displaced positives into Cipherscan arrays; retain indeterminate state for empty displaced results | Pre-activation displaced history cannot prove absence; displaced miner identity remains unavailable without a retained standard payout | Zinder, adapter, unavailable |
| Mempool and realtime: `/api/mempool`, `/api/mempool/tx/:txid`, root WebSocket | Complete | `MempoolSnapshot`, `IngestControl.MempoolTransaction`, `ExplorerQuery.TransactionDetail`, `WalletQuery.ChainEvents`, `WalletQuery.MempoolEvents` | Preserve mempool fallback behavior and `new_block`, `mempool_tx`, and `mempool_removed` frames; manage reconnect cursors internally | Parent-input enrichment, shielded balances, and paid fees for pending transactions remain unavailable | Zinder, adapter, unavailable |
| Transparent addresses and ranking: `/api/address/:address`, `/api/rich-list`, `/api/labels`, `/api/label/:address` | Address and ranking complete; label routes are degraded | `ExplorerQuery.TransparentAddressActivity`, `ExplorerQuery.TransparentAddressRanking`, retained transaction facts | Address encoding, page coercion, Cipherscan units, field names, and typed degradation | Labels and counterparty annotations require a registry sidecar | Zinder, adapter, sidecar |
| Supply, activity, and economics: `/api/network/stats`, `/api/supply`, `/api/circulating-supply`, `/api/supply/transparent-breakdown`, `/api/network/halving`, `/api/network/emission` | Complete for current supply, bounded activity, script breakdown, consensus subsidy, and observed issuance | `ValuePoolSummary`, `BlockProductionSeries`, `TransparentAddressRanking`, `UtxoSetSummary`, `ValuePoolBalanceHistory` | Formatting, Cipherscan periods, supply presentation, and consensus-backed calculations | Category labels are sidecar data; historical subsidy allocation needs a proven cross-product requirement before a new materialized view | Zinder, adapter, sidecar |
| Fees: `/api/network/fees`, `/api/network/fee-distribution` | Complete | `FeeSummary`, conventional-fee distribution, `PaidFeeDistribution`, intrinsic value balances | Expose Cipherscan periods, percentiles, units, and the distinction between conventional and actual paid fees | Conventional fees are only a degraded fallback when paid-fee coverage is unavailable | Zinder, adapter |
| Value pools and shielded flows: `/api/shielded/list`, `/api/pools/overview`, `/api/pools/flows`, `/api/network/pool-history` | Complete | `ValuePoolSummary`, `ValuePoolBalanceHistory`, `ValuePoolFlowHistory`, `ValuePoolFlowSummary` | Cipherscan period rules, fixed pool fields, ZEC/zatoshi output, labels, and pagination | Address attribution remains unavailable; product labels remain sidecar-owned | Zinder, adapter, sidecar, unavailable |
| Mining production: `/api/mining/rewards`, `/api/network/mining-metrics`, `/api/mining/pool-distribution` | Complete for the current Cipherscan contract | `BlockProductionSeries`, `BlockProductionInTimeRange`, `PaidFeeDistribution` | Period coercion, rolling formulas, solrate units, adapter-owned pool registry, grouping, response shape, and caching | Longer reward history needs a daily reward materialized view only when a product requirement justifies it | Zinder, adapter |
| Mining attribution products: `/api/mining/pool-ranking`, `/api/mining/hashrate-share`, `/api/mining/miner-behavior`, `/api/mining/zodl-leaderboard` | Degraded | None | Return stable unavailable responses | Pool branding, destination classification, behavior scoring, and leaderboard data require a sidecar | Sidecar |
| Protocol, activity, and privacy summaries: `/api/network/protocol-stats`, `/api/stats/shielded-count`, `/api/stats/shielded-daily`, `/api/analytics/usage-clock`, `/api/privacy-stats`, `/api/analytics/anonymity-set`, `/api/analytics/shielding-distribution` | Complete | `TransactionComponentSummary`, `BlockActivityDistribution`, `ValuePoolSummary`, `ValuePoolBalanceHistory`, `ValuePoolFlowHistory`, amount-threshold summaries | Product scores, thresholds, buckets, trends, cache policy, WebSocket refresh, and JSON | None for the implemented chain-derived views | Zinder, adapter |
| Blend, common amounts, and transaction linkability: `/api/blend-check`, `/api/blend-check/split`, `/api/privacy/common-amounts`, `/api/tx/:txid/linkability` | Complete when bounded native coverage is complete | Value-pool amount-threshold and rounded-amount summaries, `TransactionDetail`, `ValuePoolFlowEventsInRange` | Candidate policy, scoring, split rules, labels, coercion, cache, warnings, and JSON | Results are product heuristics, not identity or safety claims; unavailable native coverage produces a truthful degraded result | Zinder, adapter |
| Privacy graph and risk products: graph, linkage, cluster, batch-risk, pattern, recommendation endpoints under `/api/privacy/*` | Degraded | None | Preserve stable unavailable shapes | Graph relationships and identity-oriented risk analytics require a Cipherscan sidecar and are out of scope for Zinder | Sidecar |
| Turnstile: `/api/pools/turnstile` | Explicit rebuilding `503` | None | Preserve the existing rebuilding response | Destination classification is sidecar analytics and is out of scope for Zinder | Sidecar |
| Prices: `/api/price`, `/api/price/at` | Current and bounded historical pricing complete through external transport | None | Query, validate, and map external market responses without gating Zinder readiness | Deeper history and provider coverage remain external sidecar concerns | Adapter, sidecar |
| Cross-chain and names: `/api/crosschain/*`, `/api/name/:name`, `/api/name/:name/events` | Explicit unavailable or degraded | None | Preserve Cipherscan-compatible unavailable responses | Bridge and name-service data require dedicated sidecars and are out of scope for Zinder | Sidecar |
| Reorg and displaced blocks: `/api/uncles`, `/api/uncles/forks`, `/api/uncles/stats`, `/api/uncle/:hash`, canonical-miss block fallback | Complete for retained post-activation displaced preimages | `ChainReorgHistory`, `ExplorerQuery.DisplacedBlockHistory`, `ExplorerQuery.DisplacedBlockDetail` | Map reorg and displaced-detail shapes; keep archive counts distinct from observed reorg incidents | Pre-activation history, reports, monitored nodes, and miner branding are not supplied by the archive | Zinder, adapter, sidecar, unavailable |
| Fork reports, node inventory, Crosslink, and finalizers: `/api/uncle/report`, `/api/uncles/nodes`, `/api/network/peers`, `/api/network/nodes*`, `/api/crosslink/*`, `/api/finalizers*` | Node and finality data are degraded or explicitly unavailable; local anchor reads remain limited | `BlockIdBySelector` only for local anchors | Preserve degraded shapes without presenting a PoW settled tip as Crosslink finality | Crawler, registry, fork-monitor, BFT, finalizer, and Crosslink-consensus data require sidecars or a future native consensus surface | Sidecar, unavailable |
| Migration dashboard: `/api/migration/*` | Complete for chain-derived overview, cohorts, denominations, and supply audit | `ValuePoolSummary`, NU6.3 activation height, `TransactionHistory` intrinsic balances | Migration predicates, cohort boundaries, denominations, cache, and Cipherscan JSON | Reference-node height and observed timing remain sidecar data | Zinder, adapter, sidecar |
| Client scanning helpers: `/api/scan/orchard`, `/api/lightwalletd/scan` | Recent Orchard candidate discovery complete; birthday compact-block scans rejected | `TransactionHistory` v2, retained bytes, `zinder-compat-lightwalletd` streaming | Require same-epoch retained bytes; keep viewing keys and decryption in the client | Server-side viewing-key scans and unbounded compact-block REST documents are unavailable | Zinder, adapter, unavailable |
| Physical chain size: `/api/network/chain-size-history` | Degraded | None | Preserve chart metadata and an unavailable response | Physical storage telemetry is operational data and requires a sidecar | Sidecar |

## Review Invariants

1. Native Zinder contracts carry chain facts, freshness, coverage, and absence;
   Cipherscan route names and presentation policy stop at the adapter boundary.
2. A response that cannot prove its requested data must expose the relevant
   capability, coverage, or unavailable state. It must not fabricate rows,
   zeroes, finality, prices, labels, or risk conclusions.
3. Reusable durable facts are materialized-view-owned with reorg, checkpoint, and
   restart semantics. Product-only joins stay in the adapter or sidecar.
4. Displaced-block and displaced-root results are positive evidence within the
   archive activation range, not proof of complete historical fork coverage.
5. Sidecar and unavailable rows in the matrix are intentional scope boundaries;
   adding a Cipherscan route does not authorize new Zinder storage or a
   product-specific native API.
