//! Pre-decode bounds on the repeated fields of `WalletQuery` requests.
//!
//! Each bound is twice the handler's own cap, so a request just over a cap
//! still decodes and gets the handler's typed refusal; only element counts no
//! client sends are refused before decoding.

use zinder_core::MAX_OUTPOINTS_PER_SPEND_STATUS_REQUEST;
use zinder_runtime::{MethodFieldLimits, RepeatedFieldLimit};

use super::chain_events::MAX_CHAIN_EVENTS_ADDRESS_FILTER;

/// Repeated-field bounds for every `WalletQuery` request that carries a list,
/// applied by [`zinder_runtime::RepeatedFieldGuard`].
pub const WALLET_QUERY_FIELD_LIMITS: &[MethodFieldLimits] = &[
    MethodFieldLimits::new(
        "ChainEvents",
        &[RepeatedFieldLimit::length_delimited(
            "address_filter",
            &[3],
            2 * MAX_CHAIN_EVENTS_ADDRESS_FILTER,
        )],
    ),
    MethodFieldLimits::new(
        "TransparentOutpointSpendStatus",
        &[RepeatedFieldLimit::length_delimited(
            "outpoints",
            &[1],
            2 * MAX_OUTPOINTS_PER_SPEND_STATUS_REQUEST,
        )],
    ),
];
