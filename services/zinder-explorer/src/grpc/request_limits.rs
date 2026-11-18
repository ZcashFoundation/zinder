//! Pre-decode bounds on the repeated fields of `ExplorerQuery` requests.
//!
//! A bound on a field the handler caps is twice that cap, so a request just
//! over the cap still decodes and gets the handler's specific refusal. The
//! enum filter lists have no handler cap and get
//! [`MAX_UNCAPPED_ENUM_LIST_ELEMENTS`].

use zinder_runtime::{MAX_UNCAPPED_ENUM_LIST_ELEMENTS, MethodFieldLimits, RepeatedFieldLimit};

use super::value_pool_flow::MAX_AMOUNT_THRESHOLDS;

const fn enum_list(field_name: &'static str, field_path: &'static [u32]) -> RepeatedFieldLimit {
    RepeatedFieldLimit::varint(field_name, field_path, MAX_UNCAPPED_ENUM_LIST_ELEMENTS)
}

/// Repeated-field bounds for every `ExplorerQuery` request that carries a
/// list, applied by [`zinder_runtime::RepeatedFieldGuard`].
pub const EXPLORER_QUERY_FIELD_LIMITS: &[MethodFieldLimits] = &[
    MethodFieldLimits::new(
        "ValuePoolFlowHistory",
        &[
            enum_list("filter.directions", &[3, 1]),
            enum_list("filter.pools", &[3, 2]),
        ],
    ),
    MethodFieldLimits::new(
        "ValuePoolFlowEventsInRange",
        &[enum_list("directions", &[3]), enum_list("pools", &[4])],
    ),
    MethodFieldLimits::new("ValuePoolFlowSummary", &[enum_list("pools", &[3])]),
    MethodFieldLimits::new(
        "ValuePoolFlowAmountThresholdSummary",
        &[
            enum_list("pools", &[3]),
            RepeatedFieldLimit::varint("minimum_amounts_zat", &[4], 2 * MAX_AMOUNT_THRESHOLDS),
        ],
    ),
    MethodFieldLimits::new(
        "ValuePoolFlowRoundedAmountSummary",
        &[enum_list("pools", &[3])],
    ),
    MethodFieldLimits::new(
        "TransactionHistory",
        &[
            enum_list("filter.privacy_shapes", &[5, 2]),
            enum_list("filter.contains_any_protocol", &[5, 3]),
        ],
    ),
];
