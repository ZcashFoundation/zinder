//! Projection of canonical compact blocks onto the lightwalletd wire format.

use tonic::Status;
use zinder_core::{
    CompactBlockArtifact, CompactShieldedAction, CompactTransaction, CompactTransactionData,
    wire::{encode_internal_block_hash, encode_internal_transaction_id},
};
use zinder_proto::compat::lightwalletd::{self, EncodedBlock};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Payload {
    FullBlock,
    SelectedPools,
    /// Unary `GetBlockNullifiers`: retains every stored transaction (including
    /// transparent-only shells) with only shielded nullifiers kept.
    BlockNullifiers,
    /// Range `GetBlockRangeNullifiers`: filters transactions by selected
    /// shielded pools before redacting them to nullifiers only.
    Nullifiers,
    Mempool,
}

impl Payload {
    const fn is_nullifiers_only(self) -> bool {
        matches!(self, Self::BlockNullifiers | Self::Nullifiers)
    }

    const fn retains_all_transactions(self) -> bool {
        matches!(self, Self::FullBlock | Self::BlockNullifiers)
    }
}

/// The fields and transactions a lightwalletd compact-block method returns.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "Each pool is independently selected on the wire."
)]
pub(crate) struct CompactBlockShape {
    sapling: bool,
    orchard: bool,
    ironwood: bool,
    transparent: bool,
    payload: Payload,
}

impl CompactBlockShape {
    /// `GetBlock` retains every stored transaction and all of its pools.
    pub(crate) const FULL_BLOCK: Self = Self {
        sapling: true,
        orchard: true,
        ironwood: true,
        transparent: true,
        payload: Payload::FullBlock,
    };

    /// `GetBlockNullifiers` retains every stored transaction in nullifier-only
    /// form across every shielded pool, keeping transparent-only transactions
    /// as `{index, txid, fee}` shells.
    pub(crate) const BLOCK_NULLIFIERS: Self = Self {
        transparent: false,
        payload: Payload::BlockNullifiers,
        ..Self::FULL_BLOCK
    };

    pub(crate) fn for_range(pool_types: &[i32]) -> Result<Self, Status> {
        Self::select(pool_types, Payload::SelectedPools)
    }

    pub(crate) fn for_nullifier_range(pool_types: &[i32]) -> Result<Self, Status> {
        Self::select(pool_types, Payload::Nullifiers)
    }

    pub(crate) fn for_mempool(pool_types: &[i32]) -> Result<Self, Status> {
        Self::select(pool_types, Payload::Mempool)
    }

    fn select(pool_types: &[i32], payload: Payload) -> Result<Self, Status> {
        let mut selected = Self {
            sapling: false,
            orchard: false,
            ironwood: false,
            transparent: false,
            payload,
        };
        let mut use_default = true;
        for &pool_type in pool_types {
            match lightwalletd::PoolType::try_from(pool_type) {
                Ok(lightwalletd::PoolType::Sapling) => selected.sapling = true,
                Ok(lightwalletd::PoolType::Orchard) => selected.orchard = true,
                Ok(lightwalletd::PoolType::Ironwood) => selected.ironwood = true,
                Ok(lightwalletd::PoolType::Transparent) if payload == Payload::Nullifiers => {
                    continue;
                }
                Ok(lightwalletd::PoolType::Transparent) => selected.transparent = true,
                Ok(lightwalletd::PoolType::Invalid) => {
                    return Err(if payload == Payload::Mempool {
                        Status::invalid_argument("invalid pool type requested")
                    } else {
                        Status::unknown("GetBlockRange: invalid pool type requested")
                    });
                }
                Err(_) => {}
            }
            // An unknown number selects no pool but still makes the selection explicit.
            use_default = false;
        }
        if use_default {
            selected.sapling = true;
            selected.orchard = true;
            selected.ironwood = true;
        }
        Ok(selected)
    }

    /// Converts a stored block, as [`Self::message`] does, into a gRPC-framed message.
    pub(crate) fn encoded(self, block: CompactBlockArtifact) -> Result<EncodedBlock, Status> {
        EncodedBlock::encode(&self.message(block)?)
            .map_err(|error| Status::internal(error.to_string()))
    }

    /// Converts a stored block and preserves the original indices of selected transactions.
    pub(crate) fn message(
        self,
        block: CompactBlockArtifact,
    ) -> Result<lightwalletd::CompactBlock, Status> {
        let block = block.into_parts();
        let mut vtx = if self.payload.retains_all_transactions() {
            Vec::with_capacity(block.transactions.len())
        } else {
            Vec::new()
        };
        for transaction in block.transactions {
            if let Some(selected) = self.encode_transaction(transaction)? {
                vtx.push(selected);
            }
        }
        let metadata = block.chain_metadata;
        Ok(lightwalletd::CompactBlock {
            height: u64::from(block.block_id.height.value()),
            hash: encode_internal_block_hash(block.block_id.hash).to_vec(),
            prev_hash: encode_internal_block_hash(block.previous_block_hash).to_vec(),
            time: block.time,
            header: Vec::new(),
            vtx,
            chain_metadata: Some(if self.payload.is_nullifiers_only() {
                lightwalletd::ChainMetadata::default()
            } else {
                lightwalletd::ChainMetadata {
                    sapling_commitment_tree_size: metadata.sapling_commitment_tree_size,
                    orchard_commitment_tree_size: metadata.orchard_commitment_tree_size,
                    ironwood_commitment_tree_size: metadata.ironwood_commitment_tree_size,
                }
            }),
        })
    }

    pub(crate) fn encode_transaction(
        self,
        transaction: CompactTransaction,
    ) -> Result<Option<lightwalletd::CompactTx>, Status> {
        let scan = transaction.data;
        // An unrepresentable fee is storage damage even when a pool filter would omit the transaction.
        let fee = scan
            .fee_zat
            .map(u32::try_from)
            .transpose()
            .map_err(|_| Status::data_loss("compact transaction fee exceeds u32"))?
            .unwrap_or_default();
        if !self.includes_transaction(&scan) {
            return Ok(None);
        }
        let nullifiers_only = self.payload.is_nullifiers_only();
        let mut message = lightwalletd::CompactTx {
            index: transaction.index,
            txid: encode_internal_transaction_id(transaction.transaction_id).to_vec(),
            fee,
            ..lightwalletd::CompactTx::default()
        };
        if self.sapling {
            message.spends = scan
                .sapling_spends
                .into_iter()
                .map(|spend| lightwalletd::CompactSaplingSpend {
                    nf: spend.nullifier.to_vec(),
                })
                .collect();
            if !nullifiers_only {
                message.outputs = scan
                    .sapling_outputs
                    .into_iter()
                    .map(|output| lightwalletd::CompactSaplingOutput {
                        cmu: output.commitment.to_vec(),
                        ephemeral_key: output.ephemeral_key.to_vec(),
                        ciphertext: output.ciphertext.to_vec(),
                    })
                    .collect();
            }
        }
        if self.orchard {
            message.actions = encode_actions(scan.orchard_actions, nullifiers_only);
        }
        if self.ironwood {
            message.ironwood_actions = encode_actions(scan.ironwood_actions, nullifiers_only);
        }
        if self.transparent {
            if self.payload != Payload::Mempool {
                message.vin = scan
                    .transparent_inputs
                    .into_iter()
                    .map(|input| lightwalletd::CompactTxIn {
                        prevout_txid: encode_internal_transaction_id(input.previous_transaction_id)
                            .to_vec(),
                        prevout_index: input.previous_output_index,
                    })
                    .collect();
            }
            message.vout = scan
                .transparent_outputs
                .into_iter()
                .map(|output| lightwalletd::TxOut {
                    value: output.value_zat,
                    script_pub_key: output.script_pub_key,
                })
                .collect();
        }
        Ok(Some(message))
    }

    fn includes_transaction(self, scan: &CompactTransactionData) -> bool {
        // A Sapling output selects the transaction even when the nullifier RPC omits the output.
        self.payload.retains_all_transactions()
            || (self.sapling
                && (!scan.sapling_spends.is_empty() || !scan.sapling_outputs.is_empty()))
            || (self.orchard && !scan.orchard_actions.is_empty())
            || (self.ironwood && !scan.ironwood_actions.is_empty())
            || (self.transparent
                && (!scan.transparent_outputs.is_empty()
                    || (self.payload != Payload::Mempool && !scan.transparent_inputs.is_empty())))
    }
}

fn encode_actions(
    actions: Vec<CompactShieldedAction>,
    nullifiers_only: bool,
) -> Vec<lightwalletd::CompactOrchardAction> {
    actions
        .into_iter()
        .map(|action| {
            if nullifiers_only {
                lightwalletd::CompactOrchardAction {
                    nullifier: action.nullifier.to_vec(),
                    ..lightwalletd::CompactOrchardAction::default()
                }
            } else {
                lightwalletd::CompactOrchardAction {
                    nullifier: action.nullifier.to_vec(),
                    cmx: action.commitment.to_vec(),
                    ephemeral_key: action.ephemeral_key.to_vec(),
                    ciphertext: action.ciphertext.to_vec(),
                }
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use zinder_core::{
        BlockHash, BlockHeight, BlockId, CompactChainMetadata, CompactSaplingOutput,
        CompactSaplingSpend, CompactTransactionData, CompactTransparentInput,
        CompactTransparentOutput, TransactionId,
    };

    fn transaction(index: u64, scan: CompactTransactionData) -> CompactTransaction {
        CompactTransaction {
            index,
            transaction_id: TransactionId::from_bytes([1; 32]),
            data: scan,
        }
    }

    fn mixed_block() -> Result<CompactBlockArtifact, zinder_core::CompactBlockArtifactError> {
        let transparent = CompactTransactionData {
            transparent_outputs: vec![CompactTransparentOutput {
                value_zat: 20,
                script_pub_key: vec![0x51],
            }],
            ..CompactTransactionData::default()
        };
        let sapling = CompactTransactionData {
            sapling_spends: vec![CompactSaplingSpend { nullifier: [7; 32] }],
            ..CompactTransactionData::default()
        };
        CompactBlockArtifact::new(
            BlockId::new(BlockHeight::new(139), BlockHash::from_bytes([1; 32])),
            BlockHash::from_bytes([2; 32]),
            0,
            vec![
                transaction(0, transparent.clone()),
                transaction(1, sapling.clone()),
                transaction(2, transparent),
                transaction(3, sapling),
                transaction(4, CompactTransactionData::default()),
            ],
            CompactChainMetadata {
                sapling_commitment_tree_size: 31,
                orchard_commitment_tree_size: 47,
                ironwood_commitment_tree_size: 53,
            },
        )
    }

    #[test]
    fn pool_selection_retains_original_positions_and_full_blocks_keep_empty_transactions()
    -> eyre::Result<()> {
        let block = mixed_block()?;
        let all = CompactBlockShape::FULL_BLOCK.message(block.clone())?;
        let shielded = CompactBlockShape::for_range(&[])?.message(block.clone())?;
        let transparent = CompactBlockShape::for_range(&[
            99,
            lightwalletd::PoolType::Transparent as i32,
            lightwalletd::PoolType::Transparent as i32,
        ])?
        .message(block)?;
        let mut shielded_expected = all.clone();
        shielded_expected.vtx.retain(|tx| matches!(tx.index, 1 | 3));
        let mut transparent_expected = all.clone();
        transparent_expected
            .vtx
            .retain(|tx| matches!(tx.index, 0 | 2));
        assert_eq!(
            all.vtx.iter().map(|tx| tx.index).collect::<Vec<_>>(),
            [0, 1, 2, 3, 4]
        );
        let nullifiers = CompactBlockShape::for_nullifier_range(&[])?.message(mixed_block()?)?;
        assert_eq!(
            nullifiers.chain_metadata,
            Some(lightwalletd::ChainMetadata::default())
        );
        assert_eq!(nullifiers.vtx, shielded_expected.vtx);
        let block_nullifiers = CompactBlockShape::BLOCK_NULLIFIERS.message(mixed_block()?)?;
        assert_eq!(
            block_nullifiers.chain_metadata,
            Some(lightwalletd::ChainMetadata::default())
        );
        assert_eq!(
            block_nullifiers
                .vtx
                .iter()
                .map(|tx| tx.index)
                .collect::<Vec<_>>(),
            [0, 1, 2, 3, 4]
        );
        for index in [0_usize, 2, 4] {
            let expected_index = u64::try_from(index)?;
            assert_eq!(
                block_nullifiers.vtx[index],
                lightwalletd::CompactTx {
                    index: expected_index,
                    txid: vec![1; 32],
                    fee: 0,
                    ..lightwalletd::CompactTx::default()
                }
            );
        }
        assert_eq!(block_nullifiers.vtx[1], nullifiers.vtx[0]);
        assert_eq!(block_nullifiers.vtx[3], nullifiers.vtx[1]);
        assert_eq!(shielded, shielded_expected);
        assert_eq!(transparent, transparent_expected);
        Ok(())
    }

    #[test]
    fn fees_outside_the_wire_range_fail_even_for_an_omitted_transaction() -> eyre::Result<()> {
        let scan = CompactTransactionData {
            fee_zat: Some(u64::from(u32::MAX) + 1),
            ..CompactTransactionData::default()
        };
        let projected = CompactBlockShape::for_range(&[])?.encode_transaction(transaction(0, scan));
        assert!(matches!(projected, Err(status) if status.code() == tonic::Code::DataLoss));
        Ok(())
    }

    #[test]
    fn nullifiers_and_explicit_pools_select_orchard_and_ironwood_independently() -> eyre::Result<()>
    {
        let action = |nf| CompactShieldedAction {
            nullifier: [nf; 32],
            commitment: [10; 32],
            ephemeral_key: [11; 32],
            ciphertext: [12; 52],
        };
        let scan = CompactTransactionData {
            orchard_actions: vec![action(9)],
            ironwood_actions: vec![action(20)],
            ..CompactTransactionData::default()
        };
        let nullifiers = CompactBlockShape::for_nullifier_range(&[])?
            .encode_transaction(transaction(0, scan.clone()))?
            .ok_or_else(|| eyre::eyre!("missing nullifier transaction"))?;
        assert_eq!(
            nullifiers.ironwood_actions,
            [lightwalletd::CompactOrchardAction {
                nullifier: vec![20; 32],
                ..lightwalletd::CompactOrchardAction::default()
            }]
        );
        assert_eq!(
            nullifiers.actions,
            [lightwalletd::CompactOrchardAction {
                nullifier: vec![9; 32],
                ..lightwalletd::CompactOrchardAction::default()
            }]
        );
        let orchard = CompactBlockShape::for_range(&[lightwalletd::PoolType::Orchard as i32])?
            .encode_transaction(transaction(0, scan))?
            .ok_or_else(|| eyre::eyre!("missing Orchard transaction"))?;
        assert_eq!(
            orchard.ironwood_actions,
            [] as [lightwalletd::CompactOrchardAction; 0]
        );
        assert_eq!(orchard.actions.len(), 1);
        Ok(())
    }

    #[test]
    fn mempool_transparent_selection_exposes_outputs_only() -> eyre::Result<()> {
        let input_only = CompactTransactionData {
            transparent_inputs: vec![CompactTransparentInput {
                previous_transaction_id: TransactionId::from_bytes([5; 32]),
                previous_output_index: 7,
            }],
            ..CompactTransactionData::default()
        };
        let selection =
            CompactBlockShape::for_mempool(&[lightwalletd::PoolType::Transparent as i32])?;
        assert!(
            selection
                .encode_transaction(transaction(0, input_only.clone()))?
                .is_none()
        );
        let scan = CompactTransactionData {
            transparent_outputs: vec![CompactTransparentOutput {
                value_zat: 11,
                script_pub_key: vec![0x51],
            }],
            ..input_only
        };
        let tx = selection
            .encode_transaction(transaction(0, scan))?
            .ok_or_else(|| eyre::eyre!("missing mempool output"))?;
        assert_eq!(tx.vin, [] as [lightwalletd::CompactTxIn; 0]);
        assert_eq!(
            tx.vout,
            [lightwalletd::TxOut {
                value: 11,
                script_pub_key: vec![0x51]
            }]
        );
        Ok(())
    }

    #[test]
    fn structured_scan_data_maps_every_pool_exactly() -> Result<(), Status> {
        let transaction_id = zinder_core::TransactionId::from_bytes([0x41; 32]);
        let previous_transaction_id = zinder_core::TransactionId::from_bytes([0x42; 32]);
        let data = CompactTransactionData {
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

        let mapped = CompactBlockShape::FULL_BLOCK
            .encode_transaction(CompactTransaction {
                index: 5,
                transaction_id,
                data,
            })?
            .ok_or_else(|| Status::internal("full block omitted a transaction"))?;

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
