//! Transparent-input projection for explorer transaction responses.

use std::collections::HashMap;

use zinder_core::TransparentInputFact;
use zinder_core::wire::encode_rpc_transaction_id_hex;
use zinder_proto::v1::explorer::{TransactionFeesRecord, TransparentInput};
use zinder_proto::v1::wallet;

/// Encodes transient inputs when only their transaction-local outpoints exist.
pub(super) fn encode_unresolved_transparent_inputs(
    inputs: &[TransparentInputFact],
    fees: Option<&TransactionFeesRecord>,
) -> Vec<TransparentInput> {
    let projected_values: HashMap<u32, u64> = fees
        .into_iter()
        .flat_map(|record| record.transparent_inputs.iter())
        .filter_map(|input| {
            input
                .value_zat
                .map(|value_zat| (input.input_index, value_zat))
        })
        .collect();
    inputs
        .iter()
        .filter(|input| !input.spent_outpoint.is_coinbase_sentinel())
        .map(|input| TransparentInput {
            input_index: input.input_index,
            spent_outpoint: Some(wallet::OutPoint {
                transaction_id: encode_rpc_transaction_id_hex(input.spent_outpoint.transaction_id),
                output_index: input.spent_outpoint.output_index,
            }),
            value_zat: projected_values.get(&input.input_index).copied(),
            script_pub_key: None,
        })
        .collect()
}
