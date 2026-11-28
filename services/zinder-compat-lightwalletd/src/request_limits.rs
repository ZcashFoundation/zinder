//! Pre-decode bounds on the repeated fields of `CompactTxStreamer` requests.
//!
//! Each bound is twice the handler's own cap, so a request just over a cap
//! still decodes and gets the handler's specific refusal; only element counts
//! no client sends are refused before decoding. A field the handler does not
//! cap gets a flat bound far above what any client sends.

use zinder_runtime::{MAX_UNCAPPED_ENUM_LIST_ELEMENTS, MethodFieldLimits, RepeatedFieldLimit};

use crate::grpc::u32_to_usize;
use crate::mempool::MAX_EXCLUDED_TXID_SUFFIXES_PER_REQUEST;

/// Addresses one `GetAddressUtxos` or `GetAddressUtxosStream` request may
/// list. The handler caps the returned outputs, not the addresses, so this
/// only stops a list of addresses no wallet holds.
const MAX_ADDRESS_UTXOS_ADDRESSES: usize = 10_000;

const BALANCE_ADDRESSES: RepeatedFieldLimit = RepeatedFieldLimit::length_delimited(
    "addresses",
    &[1],
    2 * u32_to_usize(zinder_query::MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES),
);
const UTXO_ADDRESSES: RepeatedFieldLimit =
    RepeatedFieldLimit::length_delimited("addresses", &[1], MAX_ADDRESS_UTXOS_ADDRESSES);
const BLOCK_RANGE_POOL_TYPES: RepeatedFieldLimit =
    RepeatedFieldLimit::varint("poolTypes", &[3], MAX_UNCAPPED_ENUM_LIST_ELEMENTS);
const ADDRESS_FILTER_POOL_TYPES: RepeatedFieldLimit =
    RepeatedFieldLimit::varint("range.poolTypes", &[2, 3], MAX_UNCAPPED_ENUM_LIST_ELEMENTS);

/// Repeated-field bounds for every `CompactTxStreamer` request that carries a
/// list, applied by [`zinder_runtime::RepeatedFieldGuard`].
pub const COMPACT_TX_STREAMER_FIELD_LIMITS: &[MethodFieldLimits] = &[
    MethodFieldLimits::new("GetBlockRange", &[BLOCK_RANGE_POOL_TYPES]),
    MethodFieldLimits::new("GetBlockRangeNullifiers", &[BLOCK_RANGE_POOL_TYPES]),
    MethodFieldLimits::new("GetTaddressTxids", &[ADDRESS_FILTER_POOL_TYPES]),
    MethodFieldLimits::new("GetTaddressTransactions", &[ADDRESS_FILTER_POOL_TYPES]),
    MethodFieldLimits::new("GetTaddressBalance", &[BALANCE_ADDRESSES]),
    MethodFieldLimits::new(
        "GetMempoolTx",
        &[
            RepeatedFieldLimit::length_delimited(
                "exclude_txid_suffixes",
                &[1],
                2 * MAX_EXCLUDED_TXID_SUFFIXES_PER_REQUEST,
            ),
            RepeatedFieldLimit::varint("poolTypes", &[3], MAX_UNCAPPED_ENUM_LIST_ELEMENTS),
        ],
    ),
    MethodFieldLimits::new("GetAddressUtxos", &[UTXO_ADDRESSES]),
    MethodFieldLimits::new("GetAddressUtxosStream", &[UTXO_ADDRESSES]),
];
