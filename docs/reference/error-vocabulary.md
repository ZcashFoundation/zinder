# Error Vocabulary

This page lists every `zinder.v1.ops.ErrorReason` value, the gRPC `Status` code it pairs with, the recommended retry disposition, and the auxiliary detail clients should expect alongside it. Use it as the reference when you map Zinder errors to your local retry, alert, and operator-action decisions.

The proto enum is defined in [`crates/zinder-proto/proto/zinder/v1/ops/error.proto`](../../crates/zinder-proto/proto/zinder/v1/ops/error.proto). The reason-to-(code, retry) mapping is authored once in [`crates/zinder-proto/src/error_policy.rs`](../../crates/zinder-proto/src/error_policy.rs) as the `reason_policy` table; every Zinder surface builds its `Status` through `status_with_reason`/`status_for_reason`, so the code on the wire and the typed reason can never disagree. Each library boundary error enum (`QueryError`, `StoreError`, `ExplorerError`) implements `BoundaryError::error_reason` next to its own definition. This page owns the wire semantics clients should rely on; the policy table and the docs are kept in agreement by the `error_reason_policy_drift` CI guard and the per-boundary "no variant maps to unspecified" tests.

`BroadcastRejectionReason` stays a separate payload verdict per [ADR-0023](../adrs/0023-typed-broadcast-rejection-reasons.md) and is never folded into `ErrorReason`.

## How to read the wire

Every Zinder gRPC failure ships:

- A `tonic::Status` carrying a standard gRPC `Code` (one of 16).
- A `google.rpc.ErrorInfo` detail with `domain = "zinder.dev"` and `reason = <ErrorReason name>` (the string-form name of the enum value, e.g. `"BROADCAST_DISABLED"`).
- For some reasons, an additional structured detail (`BadRequest.field_violations`, `PreconditionFailure.violations`, `ResourceInfo`).

Clients consult `ErrorInfo.reason` for the typed category and the auxiliary detail for the failure shape. Match on the domain first to avoid mistaking a non-Zinder service's `ErrorInfo` for a Zinder one. The Rust SDK converts recognized strings to its client-owned `ErrorReason` variants and preserves unrecognized Zinder reasons as `ErrorReason::Unknown(String)`, so a newer server does not erase diagnostic identity at an older client.

The Rust client crate `zinder-client` exposes the typed accessors:

```rust
use zinder_client::{IndexerError, RetryPolicy};

match indexer_error {
    err if err.retry_policy() == RetryPolicy::RefreshChainEpoch => restart_with_fresh_epoch(&err),
    err if err.retry_policy() == RetryPolicy::OperatorActionRequired => alert_oncall(&err),
    err if err.retry_policy() == RetryPolicy::ClientError => fix_request(&err),
    err => retry_with_backoff(&err),
}
```

## Retry disposition semantics

The authored `reason_policy` table pairs each reason with a `RetryDisposition`:

| Disposition | Meaning |
| --- | --- |
| `RetryAfterBackoff` | A transient dependency is unavailable or the resource may appear after a future commit. Retry with exponential backoff. |
| `Retryable` | The failure is a momentary contention signal; retry is safe without backoff. |
| `NonRetryable` | Retrying the same request is futile until the request shape or deployment state changes (malformed request, schema mismatch, broadcast disabled, internal/data-loss conditions). |

The Rust client crate `zinder-client` exposes `RetryPolicy::RetryWithBackoff`, `RefreshChainEpoch`, `OperatorActionRequired`, and `ClientError`. `RefreshChainEpoch` means the same pinned request cannot succeed: acquire `current_epoch` again and restart the whole epoch-bound operation. The `RetryDisposition` above remains the authoritative wire-level disposition.

## Reason table

### `INVALID_ARGUMENT` family

The request shape failed validation. Retry disposition: **NonRetryable**. Carries `BadRequest.field_violations` naming the offending field and a human-readable reason.

| Reason | What it means | Example metadata |
| --- | --- | --- |
| `INVALID_BLOCK_RANGE` | `start_height` exceeds `end_height` | `field_violations[start_height, end_height]` |
| `BLOCK_RANGE_TOO_LARGE` | Requested compact- or full-block range exceeds the per-deployment cap | `field_violations[end_height]` |
| `SUBTREE_ROOT_RANGE_TOO_LARGE` | Requested subtree-root count exceeds the public 1,024-entry per-request cap | `field_violations[max_entries]` |
| `CHAIN_EVENT_CURSOR_INVALID` | Cursor bytes failed to parse, are for a different network / store / stream family, or a non-default request family disagrees with the cursor's encoded family | `field_violations[from_cursor]` |
| `ADDRESS_OUTPUT_CURSOR_INVALID` | Same as above for address-output streams | `field_violations[from_cursor]` |
| `TRANSPARENT_HISTORY_CURSOR_INVALID` | Same as above for transparent-history streams | `field_violations[from_cursor]` |
| `SNAPSHOT_PAGE_CURSOR_INVALID` | Same as above for `MempoolSnapshot` paging cursors | `field_violations[from_cursor]` |
| `INVALID_ADDRESS` | Address selector is empty, malformed, or targets a different network | `field_violations[address]` |
| `UNSUPPORTED_SHIELDED_PROTOCOL` | Shielded protocol value is not supported by the wallet protocol | `field_violations[shielded_protocol]` |
| `BROADCAST_TRANSACTION_TOO_LARGE` | Submitted raw transaction exceeds the maximum serialized transaction size | `field_violations[raw_transaction]` |
| `INVALID_CHAIN_STORE_OPTIONS` | Operator misconfiguration in `[storage]` | — |
| `ARTIFACT_PAYLOAD_TOO_LARGE` | Stored artifact exceeds size limits (deployment data issue) | — |
| `INVALID_CHAIN_EPOCH_ARTIFACTS` | A future commit batch failed structural validation | — |

### `FAILED_PRECONDITION` family

The request shape is valid but the deployment is in a state that cannot serve it. Retry disposition: **NonRetryable**. Most reasons carry `PreconditionFailure.violations` with a typed `type` + `subject` + `description`.

| Reason | What it means | Example metadata |
| --- | --- | --- |
| `BROADCAST_DISABLED` | The `broadcaster` is the no-op `()` implementation; deployment is read-only | `type=TRANSACTION_BROADCAST_DISABLED, subject=wallet.broadcast.transaction_v1` |
| `CHAIN_EVENT_CURSOR_EXPIRED` | Cursor sequence is older than retained history (consumer fell behind) | `type=CHAIN_EVENT_CURSOR_EXPIRED, subject=chain_event:<seq>` |
| `MEMPOOL_EVENT_CURSOR_EXPIRED` | Same as above for the mempool event stream | `type=MEMPOOL_EVENT_CURSOR_EXPIRED, subject=mempool_event:<seq>` |
| `SNAPSHOT_PAGE_CURSOR_EXPIRED` | `MempoolSnapshot` paging cursor is anchored ahead of the mempool-event sequence the writer has applied | `type=SNAPSHOT_PAGE_CURSOR_EXPIRED, subject=snapshot_page:<anchor_seq>` |
| `CHAIN_EPOCH_PIN_UNAVAILABLE` | Pinned chain epoch is no longer retained | `subject=chain_epoch:<id>` |
| `SCHEMA_MISMATCH` | Persistent store schema is incompatible with the running binary | — |
| `SCHEMA_TOO_NEW` | Store was opened by a newer Zinder; rolling back is unsafe | — |
| `REORG_WINDOW_EXCEEDED` | A reorg crossed the configured reorg window and the canonical writer is parked until an operator reconciles it. Native query and lightwalletd compatibility return it on new requests, and native query also as the final item of open `ChainEvents` and `MempoolEvents` streams | `readiness_cause=reorg_window_exceeded, replacement_depth_blocks=<blocks>, reorg_window_blocks=<blocks>, covered_depth_blocks=<blocks>` |
| `CHAIN_EPOCH_CONFLICT` | Detected chain-epoch contention between writer and reader | — |
| `CHAIN_EPOCH_NETWORK_MISMATCH` | Store opened against the wrong `network` | — |
| `MATERIALIZED_VIEW_UNAVAILABLE` | A required wallet projection or explorer materialized view is not configured for this deployment | `type=MATERIALIZED_VIEW_UNAVAILABLE, subject=<capability>` |
| `ENDPOINT_CAPABILITY_UNAVAILABLE` | The admitted wallet endpoint does not expose the capability required by this operation | `type=ENDPOINT_CAPABILITY_UNAVAILABLE, subject=<capability>` |
| `DEPENDENCY_NOT_CONFIGURED` | A federated dependency the explorer request needs (the canonical store, a materialized view, or a wallet-query endpoint) is not wired on this deployment | — |
| `NODE_CAPABILITY_MISSING` | The upstream Zcash node does not advertise a capability the request requires; the operator must reconfigure the node | — |
| `EXPLORER_PRECONDITION_UNSATISFIED` | An explorer read cannot serve the requested transaction state | — |

### `NOT_FOUND` family

The requested resource is not available in the visible query state. Retry disposition: **RetryAfterBackoff** (the resource may appear after a future commit). Some reasons carry `ResourceInfo` naming the family/key. The `resource_type` is the on-wire artifact-family label (the `zinder_core::artifact_family` constants such as `compact_block`, `chain_epoch`); clients read it directly without a Rust-side translation table.

| Reason | What it means | Example metadata |
| --- | --- | --- |
| `ARTIFACT_UNAVAILABLE` | A specific artifact is missing from the named family at the visible epoch (also raised by explorer reads for a not-yet-materialized materialized-view row) | `resource_type=<family>, resource_name=<key>` |
| `CHAIN_EPOCH_MISSING` | Chain epoch is not retained (often: pruned) | `resource_type=chain_epoch, resource_name=chain_epoch:<id>` |
| `BLOCK_NOT_IN_BEST_CHAIN` | No block matching the requested selector is visible in the best chain; unknown and reorged-out selectors share this reason | — |

### `DATA_LOSS` family

Persistent or upstream data could not be decoded. Retry disposition: **NonRetryable**. Pages on-call; this is the strongest signal that storage corruption needs to be investigated.

| Reason | What it means |
| --- | --- |
| `COMPACT_BLOCK_PAYLOAD_MALFORMED` | A persisted compact block failed to deserialize |
| `ARTIFACT_CORRUPT` | A persisted artifact's checksum or shape is invalid, or a federated response carried an unusable value (for example a missing outpoint or an out-of-range Zatoshi amount) |

### `UNAVAILABLE` family

Service is reachable but a dependency is not, or the operation is not yet supported. Retry disposition: **RetryAfterBackoff**.

| Reason | What it means |
| --- | --- |
| `NODE_UNAVAILABLE` | Upstream node (Zebra) is unreachable or returning errors |
| `STORAGE_UNAVAILABLE` | Local storage cannot serve the request (fall-through case) |
| `BLOCKING_TASK_FAILED` | An internal background task failed unexpectedly |
| `UNSUPPORTED_CHAIN_EVENT` | The requested event variant cannot be represented by this binary |
| `UNSUPPORTED_BLOCK_SELECTOR` | The requested selector shape cannot be represented |
| `UNSUPPORTED_TRANSACTION_STATUS` | The received transaction-status variant cannot be decoded |
| `UPSTREAM_UNREACHABLE` | A configured federated endpoint (for example the explorer's wallet-query endpoint) is temporarily unreachable |
| `SERVICE_NOT_READY` | The process is alive but its shared readiness gate is closed; `ErrorInfo.metadata["readiness_cause"]` contains the stable readiness label and `/readyz` carries any structured detail |
| `NO_VISIBLE_CHAIN_EPOCH` | No visible chain epoch has been committed yet (the store is empty or still bootstrapping) |

### `UNIMPLEMENTED` family

The selected deployment cannot serve this method. Retry disposition: **NonRetryable**.

| Reason | What it means |
| --- | --- |
| `EXPLORER_METHOD_DISABLED` | The configured explorer materialized view omits the method's required product view |

### `INTERNAL` family

A self-inflicted failure that needs investigation. Retry disposition: **NonRetryable**.

| Reason | What it means |
| --- | --- |
| `ENTROPY_UNAVAILABLE` | The deployment cannot read OS-level entropy (very rare) |
| `EXPLORER_INTERNAL` | An explorer read hit an unexpected condition: a stored materialized-view record failed to decode, a federated wallet response omitted a required field, or an index key had the wrong shape |

### Sentinel

`ERROR_REASON_UNSPECIFIED = 0` is the default scalar. It is never emitted intentionally; if a client receives it, treat it as a Zinder bug and report it. The Rust client preserves an explicitly received `Unspecified` reason through `IndexerError::RemoteFailure`. Do not infer a missing wire reason from `IndexerError::reason() == None`: deliberately coarser variants such as `IndexerError::NotFound` do not retain their originating reason.

## Stability

The set above is the v1 contract. Existing reasons' semantics are stable within a major version. Removing or repurposing a reason requires a major-version boundary because clients make retry and alerting decisions from this vocabulary.

## References

- [`crates/zinder-proto/proto/zinder/v1/ops/error.proto`](../../crates/zinder-proto/proto/zinder/v1/ops/error.proto)
- [`crates/zinder-client/src/error.rs`](../../crates/zinder-client/src/error.rs)
- [Public interfaces](../architecture/public-interfaces.md)
