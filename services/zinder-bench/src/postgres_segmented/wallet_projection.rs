//! Provisional PostgreSQL wallet UTXO and address-activity projection builder.
//!
//! Splits block validation (`stage_block`) from in-memory mutation (`commit_staged`)
//! so rejected blocks or aborted PostgreSQL transactions never leave partial state behind.

use std::collections::{HashMap, HashSet};

use zinder_core::{
    BlockHeight, CanonicalBlockFacts, CanonicalTransactionFacts, TransactionId,
    TransparentAddressScriptHash, TransparentOutPoint,
    wire::{encode_address_script_hash, encode_internal_transaction_id},
};

use crate::error::BenchError;

use super::database::candidate_error;

/// Persisted row representation for `{schema}.wallet_utxo`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalletUtxoRow {
    /// Transparent script-hash owning the output.
    pub script_hash: TransparentAddressScriptHash,
    /// Creating transaction identifier.
    pub txid: TransactionId,
    /// Creating output index within `txid`.
    pub output_index: u32,
    /// Height of the block that created this output.
    pub block_height: BlockHeight,
    /// Output amount in zatoshis.
    pub zatoshis: i64,
    /// Optional spending transaction identifier once spent.
    pub spent_by_txid: Option<TransactionId>,
    /// Optional spending block height once spent.
    pub spent_at_height: Option<BlockHeight>,
}

/// Persisted row representation for `{schema}.wallet_address_tx`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalletAddressTxRow {
    /// Transparent script-hash affected by this transaction.
    pub script_hash: TransparentAddressScriptHash,
    /// Height of the containing block.
    pub block_height: BlockHeight,
    /// Canonical zero-based transaction index inside the block.
    pub tx_index: u32,
    /// Transaction identifier.
    pub txid: TransactionId,
    /// Net balance change in zatoshis contributed by this transaction to `script_hash`.
    pub net_delta_zatoshis: i64,
}

/// One transparent output spend applied incrementally during tip-follow.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalletOutpointSpend {
    /// Outpoint consumed by the tip transaction.
    pub outpoint: TransparentOutPoint,
    /// Spending transaction identifier.
    pub spent_by_txid: TransactionId,
    /// Height of the spending block.
    pub spent_at_height: BlockHeight,
}

/// Atomic SQL delta produced for a single newly mined tip block.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WalletBlockDelta {
    /// Newly created UTXO rows to insert into `{schema}.wallet_utxo`.
    pub created_utxos: Vec<WalletUtxoRow>,
    /// Existing UTXOs marked spent at this tip height.
    pub spent_utxos: Vec<WalletOutpointSpend>,
    /// Address-activity rows to insert into `{schema}.wallet_address_tx`.
    pub address_tx_rows: Vec<WalletAddressTxRow>,
}

/// Validated wallet block delta ready to apply atomically after PostgreSQL commit.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StagedWalletBlock {
    /// SQL mutations derived for the block.
    pub delta: WalletBlockDelta,
}

/// Deterministic in-memory provisional transparent wallet projection across canonical blocks.
#[derive(Clone, Debug, Default)]
pub struct ProvisionalWalletProjection {
    utxos: HashMap<TransparentOutPoint, WalletUtxoRow>,
    address_txs: Vec<WalletAddressTxRow>,
    address_tx_count: u64,
}

struct StageBuffers {
    created_utxos: Vec<WalletUtxoRow>,
    spent_utxos: Vec<WalletOutpointSpend>,
    address_tx_rows: Vec<WalletAddressTxRow>,
    staged_outputs: HashMap<TransparentOutPoint, WalletUtxoRow>,
    staged_spends: HashSet<TransparentOutPoint>,
}

impl ProvisionalWalletProjection {
    /// Creates an empty projection state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns all tracked UTXO rows in deterministic insertion/outpoint order.
    #[must_use]
    pub fn sorted_utxo_rows(&self) -> Vec<WalletUtxoRow> {
        let mut rows: Vec<WalletUtxoRow> = self.utxos.values().cloned().collect();
        rows.sort_by_key(|row| {
            (
                row.block_height,
                encode_internal_transaction_id(row.txid),
                row.output_index,
            )
        });
        rows
    }

    /// Returns all accumulated address-transaction rows in block/tx order.
    #[must_use]
    pub fn address_tx_rows(&self) -> &[WalletAddressTxRow] {
        &self.address_txs
    }

    /// Drains accumulated pending address-transaction rows after batch `COPY` persistence.
    pub fn drain_address_tx_rows(&mut self) -> Vec<WalletAddressTxRow> {
        std::mem::take(&mut self.address_txs)
    }

    /// Returns total UTXO rows tracked across bulk and tip phases without re-sorting.
    #[must_use]
    pub fn utxo_count(&self) -> u64 {
        u64::try_from(self.utxos.len()).unwrap_or(0)
    }

    /// Returns total address-transaction rows emitted across bulk and tip phases.
    #[must_use]
    pub const fn address_tx_count(&self) -> u64 {
        self.address_tx_count
    }

    /// Purely validates `facts` and produces [`StagedWalletBlock`] without mutating `self`.
    pub fn stage_block(
        &self,
        facts: &CanonicalBlockFacts,
    ) -> Result<StagedWalletBlock, BenchError> {
        let block_height = facts.block_header.height;
        let mut buffers = StageBuffers {
            created_utxos: Vec::new(),
            spent_utxos: Vec::new(),
            address_tx_rows: Vec::new(),
            staged_outputs: HashMap::new(),
            staged_spends: HashSet::new(),
        };

        for (tx_position, tx_facts) in facts.transactions.iter().enumerate() {
            let tx_index = u32::try_from(tx_position)
                .map_err(|_| candidate_error("transaction index overflow in wallet projection"))?;
            let mut per_script_delta = HashMap::<TransparentAddressScriptHash, i128>::new();
            self.collect_tx_spends(block_height, tx_facts, &mut per_script_delta, &mut buffers)?;
            self.collect_tx_outputs(block_height, tx_facts, &mut per_script_delta, &mut buffers)?;
            append_address_tx_rows(
                block_height,
                tx_index,
                tx_facts.public_facts.transaction_id,
                per_script_delta,
                &mut buffers.address_tx_rows,
            )?;
        }

        Ok(StagedWalletBlock {
            delta: WalletBlockDelta {
                created_utxos: buffers.created_utxos,
                spent_utxos: buffers.spent_utxos,
                address_tx_rows: buffers.address_tx_rows,
            },
        })
    }

    /// Commits a previously staged block delta into `self` (infallible once staged).
    pub fn commit_staged(&mut self, staged: StagedWalletBlock) {
        let delta = staged.delta;
        for utxo in delta.created_utxos {
            self.utxos
                .insert(TransparentOutPoint::new(utxo.txid, utxo.output_index), utxo);
        }
        for spend in delta.spent_utxos {
            if let Some(target) = self.utxos.get_mut(&spend.outpoint) {
                target.spent_by_txid = Some(spend.spent_by_txid);
                target.spent_at_height = Some(spend.spent_at_height);
            }
        }
        let added_addr_rows = u64::try_from(delta.address_tx_rows.len()).unwrap_or(0);
        self.address_tx_count = self.address_tx_count.saturating_add(added_addr_rows);
        self.address_txs.extend(delta.address_tx_rows);
    }

    /// Validates `facts`, commits its delta into `self`, and returns the delta.
    pub fn apply_block(
        &mut self,
        facts: &CanonicalBlockFacts,
    ) -> Result<WalletBlockDelta, BenchError> {
        let staged = self.stage_block(facts)?;
        let delta = staged.delta.clone();
        self.commit_staged(staged);
        Ok(delta)
    }

    fn collect_tx_spends(
        &self,
        block_height: BlockHeight,
        tx_facts: &CanonicalTransactionFacts,
        per_script_delta: &mut HashMap<TransparentAddressScriptHash, i128>,
        buffers: &mut StageBuffers,
    ) -> Result<(), BenchError> {
        let txid = tx_facts.public_facts.transaction_id;
        for spent in &tx_facts.transparent_inputs {
            let outpoint = spent.spent_outpoint;
            if buffers.staged_spends.contains(&outpoint) {
                return Err(candidate_error(format!(
                    "intra-block double-spend detected for outpoint {:?}:{}",
                    outpoint.transaction_id, outpoint.output_index
                )));
            }
            if let Some(prev) = self
                .utxos
                .get(&outpoint)
                .or_else(|| buffers.staged_outputs.get(&outpoint))
            {
                if prev.spent_at_height.is_some() {
                    return Err(candidate_error(format!(
                        "double-spend detected for outpoint {:?}:{}",
                        outpoint.transaction_id, outpoint.output_index
                    )));
                }
                buffers.staged_spends.insert(outpoint);
                let delta = per_script_delta.entry(prev.script_hash).or_insert(0);
                *delta -= i128::from(prev.zatoshis);
                buffers.spent_utxos.push(WalletOutpointSpend {
                    outpoint,
                    spent_by_txid: txid,
                    spent_at_height: block_height,
                });
            }
        }
        Ok(())
    }

    fn collect_tx_outputs(
        &self,
        block_height: BlockHeight,
        tx_facts: &CanonicalTransactionFacts,
        per_script_delta: &mut HashMap<TransparentAddressScriptHash, i128>,
        buffers: &mut StageBuffers,
    ) -> Result<(), BenchError> {
        let txid = tx_facts.public_facts.transaction_id;
        for output in &tx_facts.transparent_outputs {
            let zatoshis = i64::try_from(output.value_zat)
                .map_err(|_| candidate_error("transparent output zatoshis exceed SQL BIGINT"))?;
            let outpoint = TransparentOutPoint::new(txid, output.output_index);
            if self.utxos.contains_key(&outpoint) || buffers.staged_outputs.contains_key(&outpoint)
            {
                return Err(candidate_error(format!(
                    "duplicate transparent outpoint {:?}:{}",
                    txid, output.output_index
                )));
            }
            let utxo = WalletUtxoRow {
                script_hash: output.address_script_hash,
                txid,
                output_index: output.output_index,
                block_height,
                zatoshis,
                spent_by_txid: None,
                spent_at_height: None,
            };
            let delta = per_script_delta
                .entry(output.address_script_hash)
                .or_insert(0);
            *delta += i128::from(zatoshis);
            buffers.staged_outputs.insert(outpoint, utxo.clone());
            buffers.created_utxos.push(utxo);
        }
        Ok(())
    }
}

fn append_address_tx_rows(
    block_height: BlockHeight,
    tx_index: u32,
    txid: TransactionId,
    per_script_delta: HashMap<TransparentAddressScriptHash, i128>,
    address_tx_rows: &mut Vec<WalletAddressTxRow>,
) -> Result<(), BenchError> {
    let mut entries: Vec<(TransparentAddressScriptHash, i128)> =
        per_script_delta.into_iter().collect();
    entries.sort_by_key(|(script_hash, _)| encode_address_script_hash(*script_hash));
    for (script_hash, net_delta) in entries {
        let net_delta_zatoshis = i64::try_from(net_delta)
            .map_err(|_| candidate_error("address net delta overflows SQL BIGINT"))?;
        address_tx_rows.push(WalletAddressTxRow {
            script_hash,
            block_height,
            tx_index,
            txid,
            net_delta_zatoshis,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use zinder_core::{
        BlockHash, BlockHeaderArtifact, SerializedBytesDigest, TransactionComponentCounts,
        TransactionIntrinsicValueBalances, TransactionPublicFacts, TransactionVersion,
        TransparentInputFact, TransparentOutputFact,
    };

    fn make_tx(
        txid_byte: u8,
        inputs: Vec<TransparentOutPoint>,
        outputs: Vec<(u32, u64, u8)>,
    ) -> Result<CanonicalTransactionFacts, BenchError> {
        let txid = TransactionId::from_bytes([txid_byte; 32]);
        let mut transparent_inputs = Vec::with_capacity(inputs.len());
        for (idx, outpoint) in inputs.into_iter().enumerate() {
            let input_index =
                u32::try_from(idx).map_err(|_| candidate_error("test input index exceeds u32"))?;
            transparent_inputs.push(TransparentInputFact::new(input_index, outpoint));
        }
        let transparent_outputs = outputs
            .into_iter()
            .map(|(idx, zats, script_byte)| {
                TransparentOutputFact::new(
                    idx,
                    zats,
                    vec![0x76, script_byte],
                    TransparentAddressScriptHash::from_bytes([script_byte; 32]),
                )
            })
            .collect();
        Ok(CanonicalTransactionFacts {
            public_facts: TransactionPublicFacts {
                transaction_id: txid,
                auth_digest: None,
                wtxid: None,
                version: TransactionVersion::V4,
                consensus_branch_id: None,
                lock_time: zinder_core::LockTime::Unlocked,
                expiry_height: None,
                size_bytes: 100,
                counts: TransactionComponentCounts {
                    transparent_input_count: 0,
                    transparent_output_count: 0,
                    sapling_spend_count: 0,
                    sapling_output_count: 0,
                    orchard_action_count: 0,
                    ironwood_action_count: 0,
                    sprout_joinsplit_count: 0,
                },
                orchard_value_balance_zat: None,
                orchard_anchor: None,
                ironwood_value_balance_zat: None,
                privacy_shape: zinder_core::PrivacyShape::TransparentOnly,
                is_coinbase: false,
                unsupported_sections: Vec::new(),
            },
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&[txid_byte]),
            intrinsic_value_balances: TransactionIntrinsicValueBalances {
                sprout_zat: 0,
                sapling_zat: 0,
                orchard_zat: 0,
                ironwood_zat: 0,
            },
            transparent_inputs,
            transparent_outputs,
        })
    }

    fn make_block(height: u32, txs: Vec<CanonicalTransactionFacts>) -> CanonicalBlockFacts {
        let seed = u8::try_from(height % 250).unwrap_or(1);
        CanonicalBlockFacts {
            block_header: BlockHeaderArtifact::new(
                BlockHeight::new(height),
                BlockHash::from_bytes([seed; 32]),
                BlockHash::from_bytes([0_u8; 32]),
                [seed; 32],
                [0_u8; 32],
                1_700_000_000 + i64::from(height),
                0x2007_ffff,
                [0_u8; 32],
                4,
                200,
            ),
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&[seed]),
            transactions: txs,
        }
    }

    #[test]
    fn stage_and_apply_reject_intra_and_inter_block_double_spends_cleanly() -> Result<(), BenchError>
    {
        let mut projection = ProvisionalWalletProjection::new();
        let tx1 = make_tx(1, vec![], vec![(0, 50_000, 0x11)])?;
        let block1 = make_block(1, vec![tx1]);
        let delta1 = projection.apply_block(&block1)?;
        assert_eq!(delta1.created_utxos.len(), 1);
        assert_eq!(delta1.address_tx_rows[0].net_delta_zatoshis, 50_000);

        let outpoint = TransparentOutPoint::new(TransactionId::from_bytes([1_u8; 32]), 0);
        let spend_a = make_tx(2, vec![outpoint], vec![(0, 40_000, 0x22)])?;
        let spend_b = make_tx(3, vec![outpoint], vec![(0, 40_000, 0x33)])?;
        assert!(
            projection
                .stage_block(&make_block(2, vec![spend_a, spend_b]))
                .is_err()
        );

        let dup_out = make_tx(1, vec![], vec![(0, 10_000, 0x44)])?;
        let valid_spend = make_tx(2, vec![outpoint], vec![(0, 40_000, 0x22)])?;
        assert!(
            projection
                .apply_block(&make_block(2, vec![valid_spend, dup_out]))
                .is_err()
        );

        // Neither rejected attempt mutated projection state.
        assert_eq!(projection.sorted_utxo_rows().len(), 1);
        assert_eq!(projection.sorted_utxo_rows()[0].spent_at_height, None);
        assert_eq!(projection.address_tx_rows().len(), 1);
        Ok(())
    }
}
