//! Lightwalletd `CompactBlock` and `CompactTx` messages built from stored
//! compact blocks and mempool entries, and the pool filtering both serving
//! modes apply to them.

use tonic::Status;
use zinder_core::{
    CompactBlockArtifact, CompactShieldedAction, CompactTransaction, CompactTransactionData,
    TransactionId,
    wire::{encode_internal_block_hash, encode_internal_transaction_id},
};
use zinder_proto::compat::lightwalletd;

/// The pools a request keeps, parsed from its `poolTypes`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "Each bool is an independent per-pool inclusion gate parsed from a request's poolTypes, not a state machine."
)]
pub(crate) struct CompactBlockPoolSelection {
    pub(crate) sapling: bool,
    pub(crate) orchard: bool,
    pub(crate) ironwood: bool,
    pub(crate) transparent: bool,
}

impl CompactBlockPoolSelection {
    /// Every shielded pool, which a request that names no pool receives.
    pub(crate) const fn shielded() -> Self {
        Self {
            sapling: true,
            orchard: true,
            ironwood: true,
            transparent: false,
        }
    }

    /// Parses a request's `poolTypes`, where an empty list selects the
    /// shielded pools.
    pub(crate) fn from_request(pool_types: &[i32]) -> Result<Self, Status> {
        if pool_types.is_empty() {
            return Ok(Self::shielded());
        }

        let mut pool_selection = Self {
            sapling: false,
            orchard: false,
            ironwood: false,
            transparent: false,
        };
        for pool_type in pool_types {
            match lightwalletd::PoolType::try_from(*pool_type) {
                Ok(lightwalletd::PoolType::Sapling) => pool_selection.sapling = true,
                Ok(lightwalletd::PoolType::Orchard) => pool_selection.orchard = true,
                Ok(lightwalletd::PoolType::Ironwood) => pool_selection.ironwood = true,
                Ok(lightwalletd::PoolType::Transparent) => pool_selection.transparent = true,
                Ok(lightwalletd::PoolType::Invalid) | Err(_) => {
                    return Err(Status::invalid_argument("invalid pool type requested"));
                }
            }
        }

        Ok(pool_selection)
    }
}

/// How much of each kept pool a message retains.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompactBlockPayloadMode {
    /// Every field of the kept pools.
    Full,
    /// Nullifiers alone, as the `*Nullifiers` methods send them.
    NullifiersOnly,
}

/// How a response shapes each compact block it sends.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CompactBlockShape {
    /// Pools the sent blocks keep.
    pub(crate) pool_selection: CompactBlockPoolSelection,
    /// Payload depth the sent blocks keep.
    pub(crate) payload_mode: CompactBlockPayloadMode,
}

impl CompactBlockShape {
    /// The shielded pools, nullifiers only, as `GetBlockNullifiers` and
    /// `GetBlockRangeNullifiers` send them whatever `poolTypes` asks.
    pub(crate) const NULLIFIERS: Self = Self {
        pool_selection: CompactBlockPoolSelection::shielded(),
        payload_mode: CompactBlockPayloadMode::NullifiersOnly,
    };

    /// Every field of `pool_selection`, as `GetBlockRange` sends it.
    pub(crate) const fn full(pool_selection: CompactBlockPoolSelection) -> Self {
        Self {
            pool_selection,
            payload_mode: CompactBlockPayloadMode::Full,
        }
    }

    /// Converts `compact_block` and keeps what this shape selects.
    pub(crate) fn message(
        self,
        compact_block: &CompactBlockArtifact,
    ) -> Result<lightwalletd::CompactBlock, Status> {
        compact_block_to_lightwalletd(compact_block)
            .map(|message| prune_compact_block(message, self.pool_selection, self.payload_mode))
    }
}

/// Converts a stored compact block to the lightwalletd message, with every
/// pool and field it holds.
pub(crate) fn compact_block_to_lightwalletd(
    compact_block: &CompactBlockArtifact,
) -> Result<lightwalletd::CompactBlock, Status> {
    let transactions = compact_block
        .transactions()
        .iter()
        .map(compact_transaction_to_lightwalletd)
        .collect::<Result<Vec<_>, _>>()?;
    let metadata = compact_block.chain_metadata();
    Ok(lightwalletd::CompactBlock {
        height: u64::from(compact_block.height().value()),
        hash: encode_internal_block_hash(compact_block.block_hash()).to_vec(),
        prev_hash: encode_internal_block_hash(compact_block.previous_block_hash()).to_vec(),
        time: compact_block.time(),
        header: Vec::new(),
        vtx: transactions,
        chain_metadata: Some(lightwalletd::ChainMetadata {
            sapling_commitment_tree_size: metadata.sapling_commitment_tree_size,
            orchard_commitment_tree_size: metadata.orchard_commitment_tree_size,
            ironwood_commitment_tree_size: metadata.ironwood_commitment_tree_size,
        }),
    })
}

fn compact_transaction_to_lightwalletd(
    transaction: &CompactTransaction,
) -> Result<lightwalletd::CompactTx, Status> {
    compact_transaction_data_to_lightwalletd(
        transaction.index,
        transaction.transaction_id,
        &transaction.data,
    )
}

/// Converts one transaction's compact data to the lightwalletd message at
/// block position `index`.
pub(crate) fn compact_transaction_data_to_lightwalletd(
    index: u64,
    transaction_id: TransactionId,
    transaction_data: &CompactTransactionData,
) -> Result<lightwalletd::CompactTx, Status> {
    let fee = transaction_data
        .fee_zat
        .map(u32::try_from)
        .transpose()
        .map_err(|_| Status::data_loss("compact transaction fee exceeds u32"))?
        .unwrap_or_default();
    Ok(lightwalletd::CompactTx {
        index,
        txid: encode_internal_transaction_id(transaction_id).to_vec(),
        fee,
        spends: transaction_data
            .sapling_spends
            .iter()
            .map(|spend| lightwalletd::CompactSaplingSpend {
                nf: spend.nullifier.to_vec(),
            })
            .collect(),
        outputs: transaction_data
            .sapling_outputs
            .iter()
            .map(|output| lightwalletd::CompactSaplingOutput {
                cmu: output.commitment.to_vec(),
                ephemeral_key: output.ephemeral_key.to_vec(),
                ciphertext: output.ciphertext.to_vec(),
            })
            .collect(),
        actions: transaction_data
            .orchard_actions
            .iter()
            .map(compact_shielded_action_to_lightwalletd)
            .collect(),
        vin: transaction_data
            .transparent_inputs
            .iter()
            .map(|input| lightwalletd::CompactTxIn {
                prevout_txid: encode_internal_transaction_id(input.previous_transaction_id)
                    .to_vec(),
                prevout_index: input.previous_output_index,
            })
            .collect(),
        vout: transaction_data
            .transparent_outputs
            .iter()
            .map(|output| lightwalletd::TxOut {
                value: output.value_zat,
                script_pub_key: output.script_pub_key.clone(),
            })
            .collect(),
        ironwood_actions: transaction_data
            .ironwood_actions
            .iter()
            .map(compact_shielded_action_to_lightwalletd)
            .collect(),
    })
}

fn compact_shielded_action_to_lightwalletd(
    action: &CompactShieldedAction,
) -> lightwalletd::CompactOrchardAction {
    lightwalletd::CompactOrchardAction {
        nullifier: action.nullifier.to_vec(),
        cmx: action.commitment.to_vec(),
        ephemeral_key: action.ephemeral_key.to_vec(),
        ciphertext: action.ciphertext.to_vec(),
    }
}

fn prune_compact_transaction(
    mut transaction: lightwalletd::CompactTx,
    pool_selection: CompactBlockPoolSelection,
    payload_mode: CompactBlockPayloadMode,
) -> lightwalletd::CompactTx {
    if !pool_selection.sapling || payload_mode == CompactBlockPayloadMode::NullifiersOnly {
        transaction.outputs.clear();
    }
    if !pool_selection.sapling {
        transaction.spends.clear();
    }
    if !pool_selection.orchard {
        transaction.actions.clear();
    } else if payload_mode == CompactBlockPayloadMode::NullifiersOnly {
        for action in &mut transaction.actions {
            action.cmx.clear();
            action.ephemeral_key.clear();
            action.ciphertext.clear();
        }
    }
    if !pool_selection.ironwood {
        transaction.ironwood_actions.clear();
    } else if payload_mode == CompactBlockPayloadMode::NullifiersOnly {
        for action in &mut transaction.ironwood_actions {
            action.cmx.clear();
            action.ephemeral_key.clear();
            action.ciphertext.clear();
        }
    }
    if !pool_selection.transparent || payload_mode == CompactBlockPayloadMode::NullifiersOnly {
        transaction.vin.clear();
        transaction.vout.clear();
    }
    transaction
}

/// Keeps what `pool_selection` and `payload_mode` select of a pending
/// transaction's message.
pub(crate) fn prune_mempool_compact_transaction(
    transaction: lightwalletd::CompactTx,
    pool_selection: CompactBlockPoolSelection,
    payload_mode: CompactBlockPayloadMode,
) -> lightwalletd::CompactTx {
    let mut transaction = prune_compact_transaction(transaction, pool_selection, payload_mode);
    // Reference lightwalletd exposes transparent outputs for pending mempool
    // transactions but omits transparent inputs from GetMempoolTx.
    transaction.vin.clear();
    transaction
}

/// Keeps what `pool_selection` and `payload_mode` select of a block's
/// message.
pub(crate) fn prune_compact_block(
    mut compact_block: lightwalletd::CompactBlock,
    pool_selection: CompactBlockPoolSelection,
    payload_mode: CompactBlockPayloadMode,
) -> lightwalletd::CompactBlock {
    // The nullifiers-only contract excludes commitment tree sizes: the
    // GetBlockNullifiers / GetBlockRangeNullifiers responses must not leak the
    // witness-construction tree sizes carried in chain_metadata.
    if payload_mode == CompactBlockPayloadMode::NullifiersOnly {
        compact_block.chain_metadata = None;
    }
    compact_block.vtx = compact_block
        .vtx
        .into_iter()
        .map(|transaction| prune_compact_transaction(transaction, pool_selection, payload_mode))
        .filter(compact_transaction_has_payload)
        .collect();
    compact_block
}

/// Returns whether a pruned compact transaction still carries wallet-relevant
/// data.
///
/// Both serving modes drop transactions that this rejects, as lightwalletd
/// does, and keep the original `CompactTx.index` of the transactions that
/// remain.
pub(crate) fn compact_transaction_has_payload(transaction: &lightwalletd::CompactTx) -> bool {
    !transaction.spends.is_empty()
        || !transaction.outputs.is_empty()
        || !transaction.actions.is_empty()
        || !transaction.ironwood_actions.is_empty()
        || !transaction.vin.is_empty()
        || !transaction.vout.is_empty()
}

#[cfg(test)]
mod tests {
    use tonic::Status;
    use zinder_core::{
        CompactSaplingOutput, CompactSaplingSpend, CompactShieldedAction, CompactTransactionData,
        CompactTransparentInput, CompactTransparentOutput, TransactionId,
        wire::encode_internal_transaction_id,
    };
    use zinder_proto::compat::lightwalletd;

    use super::{
        CompactBlockPayloadMode, CompactBlockPoolSelection,
        compact_transaction_data_to_lightwalletd, prune_compact_block, prune_compact_transaction,
    };

    #[test]
    fn empty_pool_request_is_shielded_only() -> Result<(), Status> {
        assert_eq!(
            CompactBlockPoolSelection::from_request(&[])?,
            CompactBlockPoolSelection::shielded()
        );
        Ok(())
    }

    #[test]
    fn invalid_pool_request_fails_before_storage_read() {
        assert_eq!(
            CompactBlockPoolSelection::from_request(&[99])
                .err()
                .map(|status| status.code()),
            Some(tonic::Code::InvalidArgument)
        );
    }

    fn transparent_only_transaction(index: u64) -> lightwalletd::CompactTx {
        lightwalletd::CompactTx {
            index,
            vin: vec![lightwalletd::CompactTxIn {
                prevout_txid: vec![0; 32],
                prevout_index: u32::MAX,
            }],
            vout: vec![lightwalletd::TxOut {
                value: 625_000_000,
                script_pub_key: vec![0x76, 0xa9],
            }],
            ..lightwalletd::CompactTx::default()
        }
    }

    fn sapling_transaction(index: u64) -> lightwalletd::CompactTx {
        lightwalletd::CompactTx {
            index,
            spends: vec![lightwalletd::CompactSaplingSpend { nf: vec![7; 32] }],
            ..lightwalletd::CompactTx::default()
        }
    }

    fn block_with_transactions(vtx: Vec<lightwalletd::CompactTx>) -> lightwalletd::CompactBlock {
        lightwalletd::CompactBlock {
            height: 139,
            vtx,
            ..lightwalletd::CompactBlock::default()
        }
    }

    fn transaction_indexes(block: &lightwalletd::CompactBlock) -> Vec<u64> {
        block
            .vtx
            .iter()
            .map(|transaction| transaction.index)
            .collect()
    }

    #[test]
    fn default_selection_drops_transparent_only_transactions_and_keeps_indexes() {
        let pruned = prune_compact_block(
            block_with_transactions(vec![
                transparent_only_transaction(0),
                sapling_transaction(1),
                transparent_only_transaction(2),
                sapling_transaction(3),
            ]),
            CompactBlockPoolSelection::shielded(),
            CompactBlockPayloadMode::Full,
        );

        assert_eq!(transaction_indexes(&pruned), vec![1, 3]);
    }

    #[test]
    fn transparent_selection_keeps_transparent_only_transactions() {
        let pruned = prune_compact_block(
            block_with_transactions(vec![
                transparent_only_transaction(0),
                sapling_transaction(1),
            ]),
            CompactBlockPoolSelection {
                sapling: false,
                orchard: false,
                ironwood: false,
                transparent: true,
            },
            CompactBlockPayloadMode::Full,
        );

        assert_eq!(transaction_indexes(&pruned), vec![0]);
        assert_eq!(
            pruned.vtx[0].spends,
            [] as [lightwalletd::CompactSaplingSpend; 0]
        );
        assert_ne!(pruned.vtx[0].vout, [] as [lightwalletd::TxOut; 0]);
    }

    fn compact_tx_with_ironwood() -> lightwalletd::CompactTx {
        lightwalletd::CompactTx {
            index: 0,
            txid: vec![1; 32],
            fee: 0,
            spends: Vec::new(),
            outputs: Vec::new(),
            actions: vec![lightwalletd::CompactOrchardAction {
                nullifier: vec![9; 32],
                cmx: vec![10; 32],
                ephemeral_key: vec![11; 32],
                ciphertext: vec![12; 52],
            }],
            ironwood_actions: vec![lightwalletd::CompactOrchardAction {
                nullifier: vec![20; 32],
                cmx: vec![21; 32],
                ephemeral_key: vec![22; 32],
                ciphertext: vec![23; 52],
            }],
            vin: Vec::new(),
            vout: Vec::new(),
        }
    }

    #[test]
    fn nullifiers_only_keeps_the_ironwood_nullifier_and_clears_the_rest() {
        let pruned = prune_compact_transaction(
            compact_tx_with_ironwood(),
            CompactBlockPoolSelection::shielded(),
            CompactBlockPayloadMode::NullifiersOnly,
        );
        assert_eq!(pruned.ironwood_actions.len(), 1);
        let action = &pruned.ironwood_actions[0];
        assert_eq!(action.nullifier, vec![20; 32]);
        assert_eq!(action.cmx, [] as [u8; 0]);
        assert_eq!(action.ephemeral_key, [] as [u8; 0]);
        assert_eq!(action.ciphertext, [] as [u8; 0]);
    }

    #[test]
    fn deselecting_the_ironwood_pool_drops_ironwood_actions() {
        let pruned = prune_compact_transaction(
            compact_tx_with_ironwood(),
            CompactBlockPoolSelection {
                sapling: true,
                orchard: true,
                ironwood: false,
                transparent: true,
            },
            CompactBlockPayloadMode::Full,
        );
        assert_eq!(
            pruned.ironwood_actions,
            [] as [lightwalletd::CompactOrchardAction; 0]
        );
        assert_eq!(pruned.actions.len(), 1);
    }

    #[test]
    fn structured_scan_data_maps_every_pool_exactly() -> Result<(), Status> {
        let transaction_id = TransactionId::from_bytes([0x41; 32]);
        let previous_transaction_id = TransactionId::from_bytes([0x42; 32]);
        let transaction_data = CompactTransactionData {
            fee_zat: Some(23),
            sapling_spends: vec![CompactSaplingSpend {
                nullifier: [0x11; 32],
            }],
            sapling_outputs: vec![CompactSaplingOutput {
                commitment: [0x12; 32],
                ephemeral_key: [0x13; 32],
                ciphertext: [0x14; 52],
            }],
            orchard_actions: vec![CompactShieldedAction {
                nullifier: [0x21; 32],
                commitment: [0x22; 32],
                ephemeral_key: [0x23; 32],
                ciphertext: [0x24; 52],
            }],
            ironwood_actions: vec![CompactShieldedAction {
                nullifier: [0x31; 32],
                commitment: [0x32; 32],
                ephemeral_key: [0x33; 32],
                ciphertext: [0x34; 52],
            }],
            transparent_inputs: vec![CompactTransparentInput {
                previous_transaction_id,
                previous_output_index: 7,
            }],
            transparent_outputs: vec![CompactTransparentOutput {
                value_zat: 99,
                script_pub_key: vec![0x51, 0x21],
            }],
        };

        let mapped =
            compact_transaction_data_to_lightwalletd(5, transaction_id, &transaction_data)?;

        assert_eq!(mapped.index, 5);
        assert_eq!(mapped.txid, encode_internal_transaction_id(transaction_id));
        assert_eq!(mapped.fee, 23);
        assert_eq!(mapped.spends[0].nf, vec![0x11; 32]);
        assert_eq!(mapped.outputs[0].cmu, vec![0x12; 32]);
        assert_eq!(mapped.outputs[0].ephemeral_key, vec![0x13; 32]);
        assert_eq!(mapped.outputs[0].ciphertext, vec![0x14; 52]);
        assert_eq!(mapped.actions[0].nullifier, vec![0x21; 32]);
        assert_eq!(mapped.actions[0].cmx, vec![0x22; 32]);
        assert_eq!(mapped.actions[0].ephemeral_key, vec![0x23; 32]);
        assert_eq!(mapped.actions[0].ciphertext, vec![0x24; 52]);
        assert_eq!(mapped.ironwood_actions[0].nullifier, vec![0x31; 32]);
        assert_eq!(mapped.ironwood_actions[0].cmx, vec![0x32; 32]);
        assert_eq!(mapped.ironwood_actions[0].ephemeral_key, vec![0x33; 32]);
        assert_eq!(mapped.ironwood_actions[0].ciphertext, vec![0x34; 52]);
        assert_eq!(
            mapped.vin[0].prevout_txid,
            encode_internal_transaction_id(previous_transaction_id)
        );
        assert_eq!(mapped.vin[0].prevout_index, 7);
        assert_eq!(mapped.vout[0].value, 99);
        assert_eq!(mapped.vout[0].script_pub_key, vec![0x51, 0x21]);
        Ok(())
    }
}
