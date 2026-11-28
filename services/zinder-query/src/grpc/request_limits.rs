//! Pre-decode bounds on the repeated fields of `WalletQuery` requests.
//!
//! Each bound is twice the handler's own cap, so a request just over a cap
//! still decodes and gets the handler's typed refusal; only element counts no
//! client sends are refused before decoding.

use zinder_core::{MAX_OUTPOINTS_PER_SPEND_STATUS_REQUEST, MAX_TRANSPARENT_ADDRESSES_PER_BATCH};
use zinder_runtime::{MethodFieldLimits, RepeatedFieldLimit};

use super::chain_events::MAX_CHAIN_EVENTS_ADDRESS_FILTER;

pub(super) const MAX_COMPACT_RANGE_POOLS: usize = 4;

/// Repeated-field bounds for every `WalletQuery` request that carries a list,
/// applied by [`zinder_runtime::RepeatedFieldGuard`].
pub const WALLET_QUERY_FIELD_LIMITS: &[MethodFieldLimits] = &[
    MethodFieldLimits::new("CompactBlocksInRange", &POOL_TYPES_LIMIT),
    MethodFieldLimits::new("CompactNullifiersInRange", &POOL_TYPES_LIMIT),
    MethodFieldLimits::new("TransparentAddressBalances", &ADDRESSES_LIMIT),
    MethodFieldLimits::new("TransparentAddressesUnspentOutputs", &ADDRESSES_LIMIT),
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

const POOL_TYPES_LIMIT: [RepeatedFieldLimit; 1] = [RepeatedFieldLimit::varint(
    "pool_types",
    &[4],
    2 * MAX_COMPACT_RANGE_POOLS,
)];
const ADDRESSES_LIMIT: [RepeatedFieldLimit; 1] = [RepeatedFieldLimit::length_delimited(
    "addresses",
    &[1],
    2 * MAX_TRANSPARENT_ADDRESSES_PER_BATCH,
)];
