# Error vocabulary

This page lists every reason a Zinder gRPC error can carry, the status code it
comes with, and whether a client should retry. It is for client developers who
map Zinder errors to retries, alerts, and operator actions.

## How an error appears on the wire

A failed Zinder gRPC call carries:

- a standard gRPC status code, which is the general retry signal;
- a `google.rpc.ErrorInfo` detail with `domain = "zinder.dev"` and `reason`
  set to an `ErrorReason` name, such as `"CHAIN_EPOCH_PIN_UNAVAILABLE"`; and
- for some reasons, `ErrorInfo.metadata` keys or a second detail:
  `BadRequest.field_violations`, `PreconditionFailure.violations`, or
  `ResourceInfo`.

Check the domain before you trust the reason, so another service's
`ErrorInfo` is not mistaken for Zinder's. The status code always matches the
reason, because one table, `reason_policy` in
`crates/zinder-proto/src/error_policy.rs`, assigns both. The enum is defined in
`crates/zinder-proto/proto/zinder/v1/ops/error.proto`.

Transport refusals, which happen before any handler runs, carry no
`ErrorInfo`. [Configuration](configuration.md#public-grpc-listeners) lists
them with their status codes.

A response whose client stops reading it ends at the send-stall deadline with
an HTTP/2 stream reset (`RST_STREAM` with `CANCEL`) instead of a status.
Clients built on grpc-go, grpc-java, and tonic with its `server` feature
report it as `CANCELLED` with no `ErrorInfo`. gRPC Swift on SwiftNIO reports `UNAVAILABLE`, and other
clients, or a proxy in front of Zinder, may report it differently.
`zinder-client` classifies it as `RetryPolicy::RetryWithBackoff`.

A node that rejects a broadcast transaction is not an error. The response
carries a `BroadcastRejectionReason` verdict instead, as
[ADR-0018](../adrs/0018-transaction-facts-status-and-broadcast.md) describes.

## Retry dispositions

Each reason has one retry disposition:

| Disposition | Meaning |
| --- | --- |
| `RetryAfterBackoff` | A dependency is temporarily unavailable, or the resource may appear after a later commit. Retry with exponential backoff. |
| `NonRetryable` | The same request fails until the request or the deployment changes. |
| `Retryable` | Safe to retry without backoff. No reason uses it today. |

The Rust client maps each error to a `RetryPolicy`, which is more specific
than the wire disposition:

| `RetryPolicy` | What to do |
| --- | --- |
| `RetryWithBackoff` | Retry with exponential backoff. |
| `RefreshChainEpoch` | Read the current epoch again and restart the whole epoch-bound operation. Retrying the same pinned request cannot succeed. |
| `RestartFromEarliestRetained` | Resubscribe to chain events from the earliest retained event and rebuild derived state from there. |
| `RefreshMempoolSnapshot` | Discard the expired cursor and any partial snapshot. Start a new mempool snapshot and subscribe from its events resume cursor. |
| `OperatorActionRequired` | Alert an operator. Do not retry until the deployment changes. |
| `ClientError` | Fix the request. |

```rust
use zinder_client::RetryPolicy;

match error.retry_policy() {
    RetryPolicy::RetryWithBackoff => retry_with_backoff(),
    RetryPolicy::RefreshChainEpoch => restart_at_current_epoch(),
    RetryPolicy::RestartFromEarliestRetained => resubscribe_from_earliest(),
    RetryPolicy::RefreshMempoolSnapshot => restart_mempool_snapshot(),
    RetryPolicy::ClientError => fix_request(),
    _ => alert_operator(),
}
```

The client converts known reason strings to `ErrorReason` variants and keeps
unknown ones as `ErrorReason::Unknown(String)`. For an unknown reason, it
derives the policy from the status code.

## Reasons

Each section below covers one status code. Its first line gives the wire
disposition and the Rust client's policy.

### `INVALID_ARGUMENT`

`NonRetryable`. Client policy: `ClientError`. When one field is at fault, a
`BadRequest.field_violations` entry names it.

| Reason | Meaning |
| --- | --- |
| `INVALID_BLOCK_RANGE` | `start_height` is above `end_height`. |
| `BLOCK_RANGE_TOO_LARGE` | A compact-block or full-block range asks for more than 1,000 blocks. |
| `SUBTREE_ROOT_RANGE_TOO_LARGE` | `max_entries` is above 1,024. |
| `TRANSPARENT_OUTPOINT_COUNT_OUT_OF_RANGE` | A `TransparentOutpointSpendStatus` request has no outpoints or more than 1,024. |
| `TRANSPARENT_BALANCE_ADDRESS_COUNT_EXCEEDED` | A lightwalletd transparent balance request has no addresses or more than 256. |
| `CHAIN_EVENT_CURSOR_INVALID` | The cursor does not parse, belongs to another network, store, or stream family, or disagrees with the requested family. |
| `ADDRESS_OUTPUT_CURSOR_INVALID` | The same, for address-output streams. |
| `TRANSPARENT_HISTORY_CURSOR_INVALID` | The same, for transparent-history streams. |
| `SNAPSHOT_PAGE_CURSOR_INVALID` | The same, for `MempoolSnapshot` page cursors. |
| `INVALID_ADDRESS` | The address is empty, malformed, or for another network, or a `ChainEvents` address filter has more than 256 entries. |
| `UNSUPPORTED_SHIELDED_PROTOCOL` | The wallet protocol does not support the requested shielded protocol. |
| `BROADCAST_TRANSACTION_TOO_LARGE` | The raw transaction is larger than 2,000,000 bytes. |
| `INVALID_CHAIN_STORE_OPTIONS` | `[storage]` is misconfigured. |
| `ARTIFACT_PAYLOAD_TOO_LARGE` | A stored artifact exceeds a size limit. |
| `INVALID_CHAIN_EPOCH_ARTIFACTS` | A commit batch failed structural validation. |

### `FAILED_PRECONDITION`

`NonRetryable`. Client policy: `OperatorActionRequired`, except where the
table says otherwise. Some reasons carry a `PreconditionFailure` violation
with a `type` and `subject`.

| Reason | Meaning |
| --- | --- |
| `CHAIN_EVENT_CURSOR_EXPIRED` | The cursor is older than retained chain events. Client policy: `RestartFromEarliestRetained`. |
| `MEMPOOL_EVENT_CURSOR_EXPIRED` | The event cursor is older than retained mempool events. The client reports `RefreshMempoolSnapshot`. |
| `SNAPSHOT_PAGE_CURSOR_EXPIRED` | The snapshot page cursor is ahead of the writer’s applied events, or its pinned multi-page snapshot is no longer retained after the writer’s applied events moved on. The client reports `RefreshMempoolSnapshot`. |
| `CHAIN_EPOCH_PIN_UNAVAILABLE` | The pinned epoch is no longer retained. A `TransparentAddressUnspentOutputs` or `FullBlocksInRange` stream can also end with it after sending items, when its serving pair is released or replaced. Discard the partial result and read again at a newer epoch. Client policy: `RefreshChainEpoch`. |
| `SCHEMA_MISMATCH` | The store's schema does not match the running binary. |
| `SCHEMA_TOO_NEW` | A newer Zinder wrote the store. Rolling back is unsafe. |
| `REORG_WINDOW_EXCEEDED` | A reorg went deeper than the reorg window, and the writer is parked until an operator reconciles it. Native query also sends it as the last item of open `ChainEvents` and `MempoolEvents` streams. Metadata: `readiness_cause`, `replacement_depth_blocks`, `reorg_window_blocks`, `covered_depth_blocks`. |
| `CHAIN_EPOCH_CONFLICT` | The writer and a reader disagree about a chain epoch. |
| `CHAIN_EPOCH_NETWORK_MISMATCH` | The store belongs to a different network. |
| `MATERIALIZED_VIEW_UNAVAILABLE` | A required wallet projection or explorer view is not configured. Violation subject: the capability. |
| `ENDPOINT_CAPABILITY_UNAVAILABLE` | This wallet endpoint does not advertise the capability the method needs. Violation subject: the capability. |
| `DEPENDENCY_NOT_CONFIGURED` | An explorer request needs a dependency that is not configured: the canonical store, a materialized view, or a wallet query endpoint. Waiting never clears it. |
| `NODE_CAPABILITY_MISSING` | The node lacks a capability the request needs. Only broadcast returns it. |
| `EXPLORER_PRECONDITION_UNSATISFIED` | An explorer read cannot serve the requested transaction state. |
| `CANONICAL_HISTORY_UNAVAILABLE` | The store's history starts at a checkpoint, so a transaction lookup or `TransparentOutpointSpendStatus` miss cannot prove an absence. An unpinned lookup checks the live mempool first. Metadata: `first_available_height`. Client policy: `OperatorActionRequired`. A store with complete history answers with a proven absence instead. |

### `NOT_FOUND`

`RetryAfterBackoff`, because the resource may appear after a later commit.
Client policy: `RetryWithBackoff`. Some reasons carry `ResourceInfo`, whose
`resource_type` is an artifact family label such as `compact_block` or
`chain_epoch`.

| Reason | Meaning |
| --- | --- |
| `ARTIFACT_UNAVAILABLE` | An artifact is missing at the visible epoch. A block-range read names the first height above the visible tip or below the retained floor. A subtree-root read names the first completed root the store lacks. Explorer reads return it for a view row not yet materialized. `ResourceInfo`: family and key. |
| `CHAIN_EPOCH_MISSING` | The epoch is not retained, or, for an unpinned `Transaction` miss, the writer's mempool answered at a different epoch than this reader. `ResourceInfo`: `chain_epoch`. |
| `BLOCK_NOT_IN_BEST_CHAIN` | No visible best-chain block matches the selector. Unknown and reorged-out selectors look the same. |
| `TRANSACTION_NOT_FOUND` | A proven absence from `WalletQuery.Transaction`. The transaction is in neither the answering epoch's chain nor, for an unpinned lookup, the writer's mempool at that epoch, and the store has complete history. It is an answer, not a failure. Metadata: `chain_epoch_id`, `first_covered_height` (always `1`), `visible_tip_height`, and `covers_mempool`. |

On `WalletQuery.Transaction`, only `TRANSACTION_NOT_FOUND` reports an
absence. Any other `NOT_FOUND` reason on that method is a failure and says
nothing about whether the transaction exists.

### `DATA_LOSS`

`NonRetryable`. Client policy: `OperatorActionRequired`. Page on-call: stored
data may be corrupt.

| Reason | Meaning |
| --- | --- |
| `COMPACT_BLOCK_PAYLOAD_MALFORMED` | A stored compact block failed to decode. |
| `ARTIFACT_CORRUPT` | A stored artifact has an invalid checksum or shape, or a federated response carried an unusable value, such as a missing outpoint or an out-of-range amount. |

### `RESOURCE_EXHAUSTED`

`RetryAfterBackoff`. Client policy: `RetryWithBackoff`.

| Reason | Meaning |
| --- | --- |
| `TREE_STATE_REPLAY_LIMIT_REACHED` | `TreeStateAtHeight` named a height without a stored checkpoint while the server was already rebuilding one tree state per CPU, and either no rebuild slot freed within the wait (2 seconds by default) or every waiting place was already taken (64 per slot by default), in which case the request is refused at once. A height with a stored checkpoint is never refused this way. |
| `COMPACT_BLOCK_STREAM_LIMIT_REACHED` | Native `CompactBlocksInRange` was refused because the server already serves its limit of open streams (100 by default). A slot frees when an open stream ends or the client drops it and its outstanding read finishes. The lightwalletd `GetBlockRange` has its own limit and does not carry this reason. |
| `FORWARDED_READ_LIMIT_REACHED` | A wallet read the server forwards to its node, such as a mined transaction body the store omits, found every slot of its kind taken, and either no slot freed within `query.forwarded_read_wait_ms` (10 seconds by default) or every waiting place was already taken (64 per slot by default), in which case the request is refused at once. The lightwalletd surface refuses the same reads with `UNAVAILABLE` and no reason. |

### `UNAVAILABLE`

`RetryAfterBackoff`. Client policy: `RetryWithBackoff`.

| Reason | Meaning |
| --- | --- |
| `NODE_UNAVAILABLE` | The node is unreachable or failing. A broadcast also returns it, without sending, until one configured endpoint passes first-contact verification and whenever every verified endpoint is unreachable. The writer's mempool returns it while it hydrates from the node, and query forwards that for mempool calls. |
| `NODE_RATE_LIMITED` | The node refused a forwarded call with HTTP 429 because it already holds its JSON-RPC connection limit, 100 by default on Zebra. The node is up. A broadcast tries the next verified endpoint first and returns this reason only when no endpoint accepted the transaction. |
| `SERVED_TIP_MISMATCH` | A forwarded transparent UTXO or balance lookup in light-wallet serving answered at a different tip than the reader's served tip, or consecutive UTXO pages disagreed at that tip. The node is up. The response attaches `grpc-retry-pushback-ms: 1000`; retry after that delay. |
| `STORAGE_UNAVAILABLE` | Local storage cannot serve the request. Native query forwards this reason for mempool reads and stream establishment when the writer’s store owner stalls or stops. The writer reserves up to one second of the caller’s deadline to return the refusal. Reads without a deadline and owner writes retain the 60-second bound. |
| `UPSTREAM_UNREACHABLE` | A configured federated endpoint, such as the explorer's wallet query endpoint or native query's ingest control, is temporarily unreachable. |
| `SERVICE_NOT_READY` | The process is alive but not ready. Metadata: `readiness_cause`, a label from [Readiness](readiness.md#readiness-causes). With `canonical_store_damaged`, retrying does not succeed until an operator rebuilds the canonical store. |
| `NO_VISIBLE_CHAIN_EPOCH` | No chain epoch has been committed yet. |
| `BROADCAST_BUDGET_EXHAUSTED` | The process is already broadcasting its 16 MiB budget of transaction bytes. Covers lightwalletd `SendTransaction` too. The broadcast never reached a node. |
| `BLOCKING_TASK_FAILED` | An internal background task failed. |
| `UNSUPPORTED_CHAIN_EVENT` | This binary cannot represent the requested event variant. |
| `UNSUPPORTED_BLOCK_SELECTOR` | This binary cannot represent the requested selector. |
| `UNSUPPORTED_TRANSACTION_STATUS` | A transaction status cannot be encoded on the wire. |
| `UNSUPPORTED_WALLET_ENCODING` | A value has no representation in the wallet protocol, or a single mempool entry exceeds the `MempoolSnapshot` response budget. |

### `UNIMPLEMENTED`

`NonRetryable`. Client policy: `OperatorActionRequired`.

| Reason | Meaning |
| --- | --- |
| `EXPLORER_METHOD_DISABLED` | The configured explorer views omit the view this method needs. |

### `INTERNAL`

`NonRetryable`. Client policy: `OperatorActionRequired`.

| Reason | Meaning |
| --- | --- |
| `ENTROPY_UNAVAILABLE` | The process cannot read operating-system entropy. |
| `EXPLORER_INTERNAL` | An explorer read hit an unexpected condition, such as a stored view record that fails to decode or a wallet response missing a required field. |
| `ERROR_REASON_UNSPECIFIED` | The default value, never sent on purpose. Report it as a Zinder bug. |

The Rust client keeps an explicitly received `ERROR_REASON_UNSPECIFIED` in
`IndexerError::RemoteFailure`. Do not read `IndexerError::reason() == None` as
a missing wire reason: coarser variants such as `IndexerError::NotFound` do
not keep their original reason.

## Stability

A new reason gets a new scalar value. Renaming, removing, or changing the
meaning of a reason is a breaking change: it increments `contract_revision`
and reserves the removed name and value.
