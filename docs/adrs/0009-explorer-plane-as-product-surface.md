# ADR-0009: Explorer Plane As First-Class Product Surface

| Field | Value |
| ----- | ----- |
| Status | Accepted |
| Product | Zinder |
| Domain | Service topology, explorer wire surface, capability namespace |
| Related | [ADR-0003](0003-canonical-storage-access-boundary.md), [ADR-0005](0005-consumer-neutral-wallet-data-plane.md), [ADR-0006](0006-ingest-control-transport-security.md), [ADR-0007](0007-mempool-topology-and-retention.md), [Explorer plane](../architecture/explorer-plane.md), [Materialized-view plane](../architecture/materialized-view-plane.md), [Service boundaries](../architecture/service-boundaries.md), [Public interfaces](../architecture/public-interfaces.md) |

## Context

Explorer support is a product surface, not an implementation pattern. The
product answer is `zinder-explorer`: operators reading `ps aux`, contributors
reading `services/`, and integrators reading the capability list should all see
the same word.

At the same time, the SDK that powers the explorer is reusable.
`MaterializedViewConsumer`, `MaterializedViewStore`, and the
`MaterializedViewStore::write_*` dispatch entry points describe a pattern
(chain and mempool events flowing into stateful consumers with atomic cursor
persistence), not the explorer product specifically. Additional consumers link
the same SDK and ship their own service binaries.

The decisions to record here are:

1. The service binary, config namespace, capability namespace, and Prometheus prefix use `explorer`.
2. The SDK names (`MaterializedViewConsumer`, `MaterializedViewStore`, etc.) stay because they describe the pattern, not the product.
3. Canonical chain facts come from canonical artifacts and replayable events; explorer-local source use is limited to parsing and optional upstream-health observation.

## Decision

### The service is `zinder-explorer`

Production processes, config keys, capability strings, and Prometheus metrics use one namespace:

| Surface | Value |
| ------- | ----- |
| Service crate | `services/zinder-explorer/` |
| Cargo package | `zinder-explorer` |
| Binary | `zinder-explorer` |
| Config TOML namespace | `[explorer]` |
| Environment-variable prefix | `ZINDER_EXPLORER__*` |
| Capability prefix | `explorer.*` |
| Prometheus metric prefix | `zinder_explorer_*` |
| CLI flag namespace | `--explorer-*` |

The explorer plane exposes this baseline capability string:

| Capability | Meaning |
| ------ | ----- |
| `explorer.server_info_v1` | Explorer server-info descriptor |

Transparent-address balance remains a wallet-plane protocol primitive, not an
explorer capability. The current release wallet endpoint omits
`wallet.address.transparent_balance_v1` because no admitted composition owns
one coherent canonical-and-mempool snapshot; Explorer must not infer support
from the presence of separate live primitives. The contract is documented in
[Wallet data plane](../architecture/wallet-data-plane.md).

### The SDK uses materialized-view vocabulary

The reusable SDK abstractions name the materialized-view pattern rather than
the explorer product:

- `MaterializedViewConsumer` (trait), `MaterializedViewMempoolConsumer` (trait)
- `MaterializedViewStore` (RocksDB wrapper), `MaterializedViewStoreTable`, `MaterializedViewConsumerName`
- `MaterializedViewConsumerCtx` (per-event context with `&MaterializedViewStore` + `&mut WriteBatch`)
- `MaterializedViewStore::write_chain_event`, `MaterializedViewStore::write_mempool_event`

The boundary is "Zinder process or library entity belonging to the running
explorer service" -> `explorer` namespace; "reusable projection abstraction"
-> `MaterializedView*` name.

### Capability namespace structure

The explorer plane's capability namespace mirrors the wallet plane's:

- `explorer.server_info_v1`
- `explorer.<noun>.<capability>_v{N}` for read methods.

The noun is a domain category (`transaction`, `block`, `mempool`, `transparent_address`, `fee`, `value_pool`, `search`), and the capability names the operation (`detail_v1`, `summary_v1`, `activity_v1`). Examples:

- `explorer.transaction.detail_v4`
- `explorer.block.summary_v1`
- `explorer.mempool.summary_v1`
- `explorer.value_pool.summary_v1`
- `explorer.search.v1`

The `domain.noun.capability_v{N}` shape is identical to the wallet plane's `wallet.subdomain.capability_v{N}` pattern so the namespace stays predictable across surfaces.

### Explorer source boundary

The explorer plane consumes canonical artifacts through `WalletQuery` over gRPC and through materialized views that ingest derives from retained canonical events. It may use `zinder-source` to parse transaction bytes and to poll an optional upstream-health observation for its freshness envelope. Those uses do not supply authoritative chain facts, change a response's pinned chain view, or provide a fallback when canonical data is unavailable.

An explorer view that needs a new authoritative chain fact extends the source and canonical boundaries first, then consumes the resulting canonical artifact or event. `zinder-explorer` must not become a parallel chain follower or substitute direct node reads for a missing canonical fact. This preserves canonical state as the authority and keeps rebuildable views reproducible from retained canonical inputs.

## Consequences

### Operational

- Operators configure the explorer through `[explorer]` and `ZINDER_EXPLORER__*`.
- Prometheus scrapes use the `zinder_explorer_*` metric prefix.
- A deployment that does not run `zinder-explorer` advertises only the
  structurally admitted `wallet.*` capabilities. It currently omits
  `wallet.address.transparent_balance_v1`; a missing or unhealthy
  ingest-control dependency must not be disguised as a zero mempool delta.

## Alternatives Considered

### Use an implementation-pattern service name

Rejected. Explorer is a first-class product surface. The deployable name should
match what operators run and what integrators discover in capability strings.
Reusable SDK abstractions already carry the materialized-view pattern name.

### Use explorer names for reusable SDK types

Rejected. The SDK describes a pattern, not a product. Naming the trait
`ExplorerConsumer` would force every non-explorer consumer through a product
name it does not own. The boundary is service product name -> `explorer`;
reusable SDK abstraction -> `MaterializedView*`.

### Use a capability namespace that differs from the binary

Rejected. The capability namespace is the durable contract; if the binary is
`zinder-explorer`, the capabilities are `explorer.*`. A capability prefix that
does not match the service name is harder to grep and pushes operators to
context-switch between two vocabularies for the same thing.

## Out of Scope

- Splitting the SDK into another crate. The reusable consumer traits, store wrapper, and dispatch entry points live in `crates/zinder-materialized-views`; another split waits until a second consumer justifies the crate boundary.
- Renaming the chain-event subscription endpoint (`WalletQuery.ChainEvents`). The wallet-plane RPC stays in place; explorer consumers subscribe to it through `WalletQuery`, not through a parallel `ExplorerQuery.ChainEvents`.
- The explorer plane's full wire vocabulary is owned by [Explorer plane](../architecture/explorer-plane.md). This ADR records the topology and namespace decision.
