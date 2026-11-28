//! Deterministic serial correctness oracle for wallet projection builders.

use std::collections::{BTreeMap, BTreeSet};

use zinder_core::{
    BlockHash, BlockHeight, BlockId, CanonicalBlockFacts, CanonicalBlockFactsDigestVersion,
    CanonicalBlockFactsSequenceDigest, CanonicalBlockFactsSequenceDigestBuilder,
    CanonicalBlockFactsSequenceDigestVersion, Network, TransparentAddressScriptHash,
    TransparentOutPoint, TransparentUtxoSetCommitment,
};

use crate::{
    WalletAddressTransaction, WalletAddressTransactionKey, WalletAddressUnspentOutputKey,
    WalletBlockHash, WalletOutpointKey, WalletProjectionAccumulator, WalletProjectionContractError,
    WalletProjectionDigest, WalletProjectionDigestBuilder, WalletProjectionFamilyRowCounts,
    WalletProjectionRowFamily, WalletReorgUndo, WalletSpentOutput, WalletTransactionPosition,
    WalletUnspentOutput, WalletUtxoSetSummary,
};

/// Small deterministic reference model for complete-history wallet projection.
///
/// This oracle deliberately favors obvious serial transitions over throughput.
/// Bulk `RocksDB` and Postgres builders can compare their logical rows, balances,
/// UTXO aggregate, and projection digest against this model on bounded fixtures.
#[derive(Clone, Debug)]
pub struct WalletProjectionSerialOracle {
    network: Network,
    last_projected_block: Option<BlockId>,
    source_sequence_digest: CanonicalBlockFactsSequenceDigest,
    unspent_outputs: BTreeMap<WalletOutpointKey, WalletUnspentOutput>,
    spent_outputs: BTreeMap<WalletOutpointKey, WalletSpentOutput>,
    address_transactions: BTreeMap<WalletAddressTransactionKey, WalletAddressTransaction>,
    balance_by_address: BTreeMap<[u8; 32], u64>,
    supported_reorg_depth: u32,
    reorg_undo: BTreeMap<BlockHeight, WalletReorgUndo>,
    block_hashes: BTreeMap<BlockHeight, BlockHash>,
    utxo_count: u64,
    total_utxo_value_zat: u64,
    utxo_commitment: TransparentUtxoSetCommitment,
}

impl WalletProjectionSerialOracle {
    /// Starts an empty complete-history oracle for `network`.
    #[must_use]
    pub fn new(network: Network) -> Self {
        Self::with_supported_reorg_depth(network, 0)
    }

    /// Starts an oracle that retains exact inverse deltas for a bounded tip window.
    #[must_use]
    pub fn with_supported_reorg_depth(network: Network, supported_reorg_depth: u32) -> Self {
        Self {
            network,
            last_projected_block: None,
            source_sequence_digest: CanonicalBlockFactsSequenceDigestBuilder::new(
                CanonicalBlockFactsSequenceDigestVersion::V1,
            )
            .finish(),
            unspent_outputs: BTreeMap::new(),
            spent_outputs: BTreeMap::new(),
            address_transactions: BTreeMap::new(),
            balance_by_address: BTreeMap::new(),
            supported_reorg_depth,
            reorg_undo: BTreeMap::new(),
            block_hashes: BTreeMap::new(),
            utxo_count: 0,
            total_utxo_value_zat: 0,
            utxo_commitment: TransparentUtxoSetCommitment::empty(),
        }
    }

    /// Applies one contiguous canonical block atomically and returns its undo record.
    pub fn apply_block(
        &mut self,
        facts: &CanonicalBlockFacts,
    ) -> Result<WalletReorgUndo, WalletProjectionContractError> {
        let mut candidate = self.clone();
        let undo = candidate.apply_block_in_place(facts)?;
        *self = candidate;
        Ok(undo)
    }

    /// Returns the last projected block, or `None` before height one is applied.
    #[must_use]
    pub const fn last_projected_block(&self) -> Option<BlockId> {
        self.last_projected_block
    }

    /// Returns the exact ordered source digest through the current tip.
    #[must_use]
    pub const fn source_sequence_digest(&self) -> CanonicalBlockFactsSequenceDigest {
        self.source_sequence_digest
    }

    /// Finds one currently unspent output.
    #[must_use]
    pub fn find_unspent_output(
        &self,
        outpoint: TransparentOutPoint,
    ) -> Option<&WalletUnspentOutput> {
        self.unspent_outputs.get(&WalletOutpointKey::new(outpoint))
    }

    /// Finds one historical spent output.
    #[must_use]
    pub fn find_spent_output(&self, outpoint: TransparentOutPoint) -> Option<&WalletSpentOutput> {
        self.spent_outputs.get(&WalletOutpointKey::new(outpoint))
    }

    /// Returns one address's current balance, with absent rows represented as zero.
    #[must_use]
    pub fn address_balance(&self, address_script_hash: TransparentAddressScriptHash) -> u64 {
        self.balance_by_address
            .get(&address_script_hash.as_bytes())
            .copied()
            .unwrap_or_default()
    }

    /// Returns one projected canonical block hash by height.
    #[must_use]
    pub fn find_block_hash(&self, height: BlockHeight) -> Option<BlockHash> {
        self.block_hashes.get(&height).copied()
    }

    /// Iterates address transactions in exact durable key order.
    pub fn address_transactions(&self) -> impl ExactSizeIterator<Item = &WalletAddressTransaction> {
        self.address_transactions.values()
    }

    /// Iterates retained undo rows in ascending block-height order.
    pub fn reorg_undo(&self) -> impl ExactSizeIterator<Item = &WalletReorgUndo> {
        self.reorg_undo.values()
    }

    /// Returns the logical row counts represented by this reference model.
    #[must_use]
    pub fn row_counts(&self) -> WalletProjectionFamilyRowCounts {
        WalletProjectionFamilyRowCounts {
            transparent_unspent_output_count: count_rows(self.unspent_outputs.len()),
            transparent_unspent_output_by_address_count: count_rows(self.unspent_outputs.len()),
            transparent_spent_output_count: count_rows(self.spent_outputs.len()),
            transparent_address_transaction_count: count_rows(self.address_transactions.len()),
            transparent_address_balance_count: count_rows(self.balance_by_address.len()),
            reorg_undo_count: count_rows(self.reorg_undo.len()),
            block_hash_by_height_count: count_rows(self.block_hashes.len()),
        }
    }

    /// Returns the complete current UTXO aggregate.
    #[must_use]
    pub fn utxo_summary(&self) -> WalletUtxoSetSummary {
        WalletUtxoSetSummary {
            utxo_count: self.utxo_count,
            total_value_zat: self.total_utxo_value_zat,
            commitment: self.utxo_commitment.clone(),
        }
    }

    /// Rebuilds the full row accumulator from every logical wallet row.
    pub fn projection_accumulator(
        &self,
    ) -> Result<WalletProjectionAccumulator, WalletProjectionContractError> {
        let (accumulator, _digest) = self.projection_builder()?.finish_with_accumulator();
        Ok(accumulator)
    }

    /// Derives the display digest from the same full accumulator contract.
    pub fn projection_digest(
        &self,
    ) -> Result<WalletProjectionDigest, WalletProjectionContractError> {
        Ok(self.projection_builder()?.finish())
    }

    fn projection_builder(
        &self,
    ) -> Result<WalletProjectionDigestBuilder, WalletProjectionContractError> {
        let mut digest = WalletProjectionDigestBuilder::new();

        for (key, output) in &self.unspent_outputs {
            digest.append_row(
                WalletProjectionRowFamily::TransparentUnspentOutput,
                key.as_bytes(),
                &output.encode_value()?,
            )?;
        }

        let unspent_output_address_keys: BTreeSet<_> = self
            .unspent_outputs
            .values()
            .map(WalletAddressUnspentOutputKey::new)
            .collect();
        for address_key in unspent_output_address_keys {
            digest.append_row(
                WalletProjectionRowFamily::TransparentUnspentOutputByAddress,
                address_key.as_bytes(),
                &[],
            )?;
        }

        for (key, output) in &self.spent_outputs {
            digest.append_row(
                WalletProjectionRowFamily::TransparentSpentOutput,
                key.as_bytes(),
                &output.encode_value()?,
            )?;
        }

        for (key, entry) in &self.address_transactions {
            digest.append_row(
                WalletProjectionRowFamily::TransparentAddressTransaction,
                key.as_bytes(),
                &entry.encode_value(),
            )?;
        }

        for (address_script_hash, balance_zat) in &self.balance_by_address {
            digest.append_row(
                WalletProjectionRowFamily::TransparentAddressBalance,
                address_script_hash,
                &balance_zat.to_be_bytes(),
            )?;
        }

        for (height, undo) in &self.reorg_undo {
            digest.append_row(
                WalletProjectionRowFamily::ReorgUndo,
                &height.value().to_be_bytes(),
                &undo.encode_value()?,
            )?;
        }

        for (height, hash) in &self.block_hashes {
            let block_hash_row = WalletBlockHash::new(BlockId::new(*height, *hash));
            digest.append_row(
                WalletProjectionRowFamily::BlockHashByHeight,
                &block_hash_row.encode_key(),
                &block_hash_row.encode_value(),
            )?;
        }
        Ok(digest)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the serial oracle keeps one top-to-bottom block transition visible"
    )]
    fn apply_block_in_place(
        &mut self,
        facts: &CanonicalBlockFacts,
    ) -> Result<WalletReorgUndo, WalletProjectionContractError> {
        let block = BlockId::new(facts.block_header.height, facts.block_header.block_hash);
        self.validate_next_block(block, facts.block_header.parent_hash)?;
        let source_sequence_digest_before = self.source_sequence_digest;
        let mut source_sequence_digest =
            CanonicalBlockFactsSequenceDigestBuilder::resume_from_prefix(
                source_sequence_digest_before,
            );
        source_sequence_digest.try_append(facts.digest(CanonicalBlockFactsDigestVersion::V1))?;
        let source_sequence_digest_after = source_sequence_digest.finish();
        let mut created_outpoints = Vec::new();
        let mut created_outpoint_keys = BTreeSet::new();
        let mut spent_outpoints = Vec::new();
        let mut address_transaction_keys = Vec::new();

        for (transaction_index, transaction) in facts.transactions.iter().enumerate() {
            let tx_index_in_block = u32::try_from(transaction_index)
                .map_err(|_| WalletProjectionContractError::FactIndexOverflow)?;
            let transaction_id = transaction.public_facts.transaction_id;
            let transaction_position =
                WalletTransactionPosition::new(transaction_id, tx_index_in_block, block);
            let mut touched_addresses = BTreeSet::new();

            for (input_position, input) in transaction.transparent_inputs.iter().enumerate() {
                let expected_input_index = u32::try_from(input_position)
                    .map_err(|_| WalletProjectionContractError::FactIndexOverflow)?;
                if input.input_index != expected_input_index {
                    return Err(WalletProjectionContractError::FactIndexMismatch);
                }
                if input.spent_outpoint.is_coinbase_sentinel() {
                    continue;
                }
                let key = WalletOutpointKey::new(input.spent_outpoint);
                if self.spent_outputs.contains_key(&key) {
                    return Err(WalletProjectionContractError::DuplicateSpend);
                }
                let output = self
                    .unspent_outputs
                    .remove(&key)
                    .ok_or(WalletProjectionContractError::MissingTransparentPredecessor)?;
                self.subtract_unspent_output(&output)?;
                touched_addresses.insert(output.address_script_hash.as_bytes());
                self.spent_outputs.insert(
                    key,
                    WalletSpentOutput::new(output, transaction_position, input.input_index),
                );
                if !created_outpoint_keys.contains(&key) {
                    spent_outpoints.push(key);
                }
            }

            for (output_position, output) in transaction.transparent_outputs.iter().enumerate() {
                let expected_output_index = u32::try_from(output_position)
                    .map_err(|_| WalletProjectionContractError::FactIndexOverflow)?;
                if output.output_index != expected_output_index {
                    return Err(WalletProjectionContractError::FactIndexMismatch);
                }
                let outpoint = TransparentOutPoint::new(transaction_id, output.output_index);
                let key = WalletOutpointKey::new(outpoint);
                if self.unspent_outputs.contains_key(&key) || self.spent_outputs.contains_key(&key)
                {
                    return Err(WalletProjectionContractError::DuplicateOutput);
                }
                let unspent_output = WalletUnspentOutput::new(
                    outpoint,
                    output.address_script_hash,
                    output.value_zat,
                    output.script_pub_key.clone(),
                    transaction_position,
                )?;
                self.add_unspent_output(&unspent_output)?;
                touched_addresses.insert(output.address_script_hash.as_bytes());
                self.unspent_outputs.insert(key, unspent_output);
                created_outpoints.push(key);
                created_outpoint_keys.insert(key);
            }

            for address_bytes in touched_addresses {
                let address_script_hash = TransparentAddressScriptHash::from_bytes(address_bytes);
                let key = WalletAddressTransactionKey::new(
                    address_script_hash,
                    block.height,
                    tx_index_in_block,
                );
                self.address_transactions.insert(
                    key,
                    WalletAddressTransaction::new(key, transaction_id, block.hash),
                );
                address_transaction_keys.push(key);
            }
        }

        self.last_projected_block = Some(block);
        self.block_hashes.insert(block.height, block.hash);
        self.source_sequence_digest = source_sequence_digest_after;
        created_outpoints.sort_unstable();
        spent_outpoints.sort_unstable();
        address_transaction_keys.sort_unstable();
        let undo = WalletReorgUndo {
            block,
            parent_hash: facts.block_header.parent_hash,
            source_sequence_digest_before,
            source_sequence_digest_after,
            created_outpoints,
            spent_outpoints,
            address_transaction_keys,
        };
        if self.supported_reorg_depth > 0 {
            self.reorg_undo.insert(block.height, undo.clone());
            let first_retained_height = block
                .height
                .value()
                .saturating_sub(self.supported_reorg_depth.saturating_sub(1));
            self.reorg_undo
                .retain(|height, _| height.value() >= first_retained_height);
        }
        Ok(undo)
    }

    fn validate_next_block(
        &self,
        block: BlockId,
        parent_hash: BlockHash,
    ) -> Result<(), WalletProjectionContractError> {
        match self.last_projected_block {
            None if block.height == BlockHeight::new(1) => Ok(()),
            Some(previous)
                if previous.height.next() == Some(block.height) && previous.hash == parent_hash =>
            {
                Ok(())
            }
            None | Some(_) => Err(WalletProjectionContractError::NonContiguousBlock),
        }
    }

    fn add_unspent_output(
        &mut self,
        output: &WalletUnspentOutput,
    ) -> Result<(), WalletProjectionContractError> {
        if output.value_zat > 0 {
            let address = output.address_script_hash.as_bytes();
            let balance = self.balance_by_address.entry(address).or_default();
            *balance = balance
                .checked_add(output.value_zat)
                .ok_or(WalletProjectionContractError::AddressBalanceOverflow)?;
        }
        self.utxo_count = self
            .utxo_count
            .checked_add(1)
            .ok_or(WalletProjectionContractError::UtxoCountOverflow)?;
        self.total_utxo_value_zat = self
            .total_utxo_value_zat
            .checked_add(output.value_zat)
            .ok_or(WalletProjectionContractError::UtxoValueOverflow)?;
        self.utxo_commitment
            .insert(&output.commitment_element(self.network));
        Ok(())
    }

    fn subtract_unspent_output(
        &mut self,
        output: &WalletUnspentOutput,
    ) -> Result<(), WalletProjectionContractError> {
        if output.value_zat > 0 {
            let address = output.address_script_hash.as_bytes();
            let balance = self
                .balance_by_address
                .get_mut(&address)
                .ok_or(WalletProjectionContractError::AddressBalanceUnderflow)?;
            *balance = balance
                .checked_sub(output.value_zat)
                .ok_or(WalletProjectionContractError::AddressBalanceUnderflow)?;
            if *balance == 0 {
                self.balance_by_address.remove(&address);
            }
        }
        self.utxo_count = self
            .utxo_count
            .checked_sub(1)
            .ok_or(WalletProjectionContractError::UtxoCountUnderflow)?;
        self.total_utxo_value_zat = self
            .total_utxo_value_zat
            .checked_sub(output.value_zat)
            .ok_or(WalletProjectionContractError::UtxoValueUnderflow)?;
        self.utxo_commitment
            .subtract(&output.commitment_element(self.network));
        Ok(())
    }
}

fn count_rows(len: usize) -> u64 {
    u64::try_from(len).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use zinder_core::{
        BlockHeaderArtifact, CanonicalTransactionFacts, LockTime, PrivacyShape,
        SerializedBytesDigest, TransactionComponentCounts, TransactionId,
        TransactionIntrinsicValueBalances, TransactionPublicFacts, TransactionVersion,
        TransparentInputFact, TransparentOutputFact,
    };

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "one end-to-end oracle test keeps the create, spend, and atomic refusal proof together"
    )]
    fn serial_oracle_projects_create_then_spend() {
        let address_one = TransparentAddressScriptHash::from_bytes([0xa1; 32]);
        let address_two = TransparentAddressScriptHash::from_bytes([0xa2; 32]);
        let transaction_one = TransactionId::from_bytes([0xb1; 32]);
        let transaction_two = TransactionId::from_bytes([0xb2; 32]);
        let outpoint_one = TransparentOutPoint::new(transaction_one, 0);
        let outpoint_two = TransparentOutPoint::new(transaction_two, 0);
        let block_one = block_facts(
            1,
            [0x00; 32],
            [0xc1; 32],
            transaction_facts(
                transaction_one,
                Vec::new(),
                vec![TransparentOutputFact::new(0, 7, [0x51], address_one)],
            ),
        );
        let block_two = block_facts(
            2,
            [0xc1; 32],
            [0xc2; 32],
            transaction_facts(
                transaction_two,
                vec![TransparentInputFact::new(0, outpoint_one)],
                vec![TransparentOutputFact::new(0, 4, [0x52], address_two)],
            ),
        );

        let mut oracle = WalletProjectionSerialOracle::new(Network::ZcashRegtest);
        let first_undo = oracle
            .apply_block(&block_one)
            .unwrap_or_else(|error| unreachable!("valid first block: {error}"));
        let second_undo = oracle
            .apply_block(&block_two)
            .unwrap_or_else(|error| unreachable!("valid second block: {error}"));

        assert_eq!(first_undo.created_outpoints.len(), 1);
        assert_eq!(second_undo.created_outpoints.len(), 1);
        assert_eq!(second_undo.spent_outpoints.len(), 1);
        assert_eq!(second_undo.address_transaction_keys.len(), 2);
        assert!(oracle.find_unspent_output(outpoint_one).is_none());
        assert!(oracle.find_spent_output(outpoint_one).is_some());
        assert!(oracle.find_unspent_output(outpoint_two).is_some());
        assert_eq!(oracle.address_balance(address_one), 0);
        assert_eq!(oracle.address_balance(address_two), 4);
        assert_eq!(oracle.address_transactions().len(), 3);
        assert_eq!(
            oracle.find_block_hash(BlockHeight::new(1)),
            Some(block_one.block_header.block_hash)
        );
        assert_eq!(
            oracle.find_block_hash(BlockHeight::new(2)),
            Some(block_two.block_header.block_hash)
        );
        assert_eq!(
            oracle.row_counts(),
            WalletProjectionFamilyRowCounts {
                transparent_unspent_output_count: 1,
                transparent_unspent_output_by_address_count: 1,
                transparent_spent_output_count: 1,
                transparent_address_transaction_count: 3,
                transparent_address_balance_count: 1,
                reorg_undo_count: 0,
                block_hash_by_height_count: 2,
            }
        );
        assert_eq!(oracle.utxo_summary().utxo_count, 1);
        assert_eq!(oracle.utxo_summary().total_value_zat, 4);

        let digest_before_error = oracle
            .projection_digest()
            .unwrap_or_else(|error| unreachable!("valid oracle digest: {error}"));
        let missing_outpoint = TransparentOutPoint::new(TransactionId::from_bytes([0xdd; 32]), 0);
        let invalid_block = block_facts(
            3,
            [0xc2; 32],
            [0xc3; 32],
            transaction_facts(
                TransactionId::from_bytes([0xb3; 32]),
                vec![TransparentInputFact::new(0, missing_outpoint)],
                Vec::new(),
            ),
        );
        assert_eq!(
            oracle.apply_block(&invalid_block),
            Err(WalletProjectionContractError::MissingTransparentPredecessor)
        );
        assert_eq!(
            oracle.last_projected_block(),
            Some(block_two.block_header.into_header().block_id)
        );
        assert_eq!(
            oracle
                .projection_digest()
                .unwrap_or_else(|error| unreachable!("valid oracle digest: {error}")),
            digest_before_error
        );
    }

    #[test]
    fn serial_oracle_retains_only_the_configured_reorg_window() {
        let address = TransparentAddressScriptHash::from_bytes([0xa1; 32]);
        let block_one = block_facts(
            1,
            [0x00; 32],
            [0xc1; 32],
            transaction_facts(
                TransactionId::from_bytes([0xb1; 32]),
                Vec::new(),
                vec![TransparentOutputFact::new(0, 7, [0x51], address)],
            ),
        );
        let block_two = block_facts(
            2,
            [0xc1; 32],
            [0xc2; 32],
            transaction_facts(
                TransactionId::from_bytes([0xb2; 32]),
                Vec::new(),
                vec![TransparentOutputFact::new(0, 4, [0x52], address)],
            ),
        );
        let mut oracle =
            WalletProjectionSerialOracle::with_supported_reorg_depth(Network::ZcashRegtest, 1);
        assert!(oracle.apply_block(&block_one).is_ok());
        assert!(oracle.apply_block(&block_two).is_ok());

        assert_eq!(oracle.reorg_undo().len(), 1);
        assert_eq!(
            oracle.reorg_undo().next().map(|undo| undo.block),
            Some(BlockId::new(
                BlockHeight::new(2),
                BlockHash::from_bytes([0xc2; 32])
            ))
        );
        assert_eq!(oracle.row_counts().reorg_undo_count, 1);
        assert_eq!(oracle.row_counts().block_hash_by_height_count, 2);
        assert!(oracle.projection_digest().is_ok());
    }

    #[test]
    fn zero_value_utxo_does_not_create_an_address_balance_row() {
        let address = TransparentAddressScriptHash::from_bytes([0xa1; 32]);
        let transaction = TransactionId::from_bytes([0xb1; 32]);
        let outpoint = TransparentOutPoint::new(transaction, 0);
        let block = block_facts(
            1,
            [0x00; 32],
            [0xc1; 32],
            transaction_facts(
                transaction,
                Vec::new(),
                vec![TransparentOutputFact::new(0, 0, [0x51], address)],
            ),
        );
        let mut oracle = WalletProjectionSerialOracle::new(Network::ZcashRegtest);

        oracle
            .apply_block(&block)
            .unwrap_or_else(|error| unreachable!("valid zero-value output: {error}"));

        assert!(oracle.find_unspent_output(outpoint).is_some());
        assert_eq!(oracle.address_balance(address), 0);
        assert_eq!(oracle.row_counts().transparent_unspent_output_count, 1);
        assert_eq!(
            oracle
                .row_counts()
                .transparent_unspent_output_by_address_count,
            1
        );
        assert_eq!(oracle.row_counts().transparent_address_balance_count, 0);
        assert_eq!(oracle.utxo_summary().utxo_count, 1);
        assert_eq!(oracle.utxo_summary().total_value_zat, 0);
    }

    #[test]
    fn mixed_zero_and_positive_utxos_create_one_positive_balance_row() {
        let address = TransparentAddressScriptHash::from_bytes([0xa1; 32]);
        let transaction = TransactionId::from_bytes([0xb1; 32]);
        let zero_outpoint = TransparentOutPoint::new(transaction, 0);
        let positive_outpoint = TransparentOutPoint::new(transaction, 1);
        let block = block_facts(
            1,
            [0x00; 32],
            [0xc1; 32],
            transaction_facts(
                transaction,
                Vec::new(),
                vec![
                    TransparentOutputFact::new(0, 0, [0x51], address),
                    TransparentOutputFact::new(1, 7, [0x52], address),
                ],
            ),
        );
        let mut oracle = WalletProjectionSerialOracle::new(Network::ZcashRegtest);

        oracle
            .apply_block(&block)
            .unwrap_or_else(|error| unreachable!("valid mixed-value outputs: {error}"));

        assert!(oracle.find_unspent_output(zero_outpoint).is_some());
        assert!(oracle.find_unspent_output(positive_outpoint).is_some());
        assert_eq!(oracle.address_balance(address), 7);
        assert_eq!(oracle.row_counts().transparent_unspent_output_count, 2);
        assert_eq!(oracle.row_counts().transparent_address_balance_count, 1);
        assert_eq!(oracle.utxo_summary().utxo_count, 2);
        assert_eq!(oracle.utxo_summary().total_value_zat, 7);
    }

    #[test]
    fn same_block_spend_is_deleted_but_not_restored_by_reorg_undo() {
        let address = TransparentAddressScriptHash::from_bytes([0xa1; 32]);
        let creating_transaction = TransactionId::from_bytes([0xb1; 32]);
        let spending_transaction = TransactionId::from_bytes([0xb2; 32]);
        let same_block_outpoint = TransparentOutPoint::new(creating_transaction, 0);
        let block = block_facts_with_transactions(
            1,
            [0x00; 32],
            [0xc1; 32],
            vec![
                transaction_facts(
                    creating_transaction,
                    Vec::new(),
                    vec![TransparentOutputFact::new(0, 7, [0x51], address)],
                ),
                transaction_facts(
                    spending_transaction,
                    vec![TransparentInputFact::new(0, same_block_outpoint)],
                    Vec::new(),
                ),
            ],
        );
        let mut oracle =
            WalletProjectionSerialOracle::with_supported_reorg_depth(Network::ZcashRegtest, 1);

        let undo = oracle
            .apply_block(&block)
            .unwrap_or_else(|error| unreachable!("valid same-block spend: {error}"));

        assert_eq!(
            undo.created_outpoints,
            vec![WalletOutpointKey::new(same_block_outpoint)]
        );
        assert_eq!(undo.spent_outpoints, []);
        assert!(oracle.find_unspent_output(same_block_outpoint).is_none());
        assert!(oracle.find_spent_output(same_block_outpoint).is_some());
    }

    #[test]
    fn serial_oracle_accumulates_address_index_rows() {
        let first_transaction = TransactionId::from_bytes([0x01; 32]);
        let second_transaction = TransactionId::from_bytes([0x02; 32]);
        let block = block_facts_with_transactions(
            1,
            [0x00; 32],
            [0xc1; 32],
            vec![
                transaction_facts(
                    first_transaction,
                    Vec::new(),
                    vec![TransparentOutputFact::new(
                        0,
                        1,
                        [0x51],
                        TransparentAddressScriptHash::from_bytes([0xff; 32]),
                    )],
                ),
                transaction_facts(
                    second_transaction,
                    Vec::new(),
                    vec![TransparentOutputFact::new(
                        0,
                        2,
                        [0x52],
                        TransparentAddressScriptHash::from_bytes([0x00; 32]),
                    )],
                ),
            ],
        );
        let mut oracle = WalletProjectionSerialOracle::new(Network::ZcashRegtest);
        oracle
            .apply_block(&block)
            .unwrap_or_else(|error| unreachable!("valid ordering fixture: {error}"));

        let outpoint_ordered_address_keys: Vec<_> = oracle
            .unspent_outputs
            .values()
            .map(WalletAddressUnspentOutputKey::new)
            .collect();
        let address_ordered_keys: Vec<_> = outpoint_ordered_address_keys
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        assert_ne!(outpoint_ordered_address_keys, address_ordered_keys);
        assert_eq!(
            hex::encode(
                oracle
                    .projection_digest()
                    .unwrap_or_else(|error| unreachable!("valid ordering digest: {error}"))
                    .as_bytes()
            ),
            "b5600032eb51ad99a3d24a44901a48ae0b2a57255a21a763dc1b9c9f6c7b6be7"
        );
    }

    fn block_facts(
        height: u32,
        parent_hash: [u8; 32],
        block_hash: [u8; 32],
        transaction: CanonicalTransactionFacts,
    ) -> CanonicalBlockFacts {
        block_facts_with_transactions(height, parent_hash, block_hash, vec![transaction])
    }

    fn block_facts_with_transactions(
        height: u32,
        parent_hash: [u8; 32],
        block_hash: [u8; 32],
        transactions: Vec<CanonicalTransactionFacts>,
    ) -> CanonicalBlockFacts {
        CanonicalBlockFacts {
            block_header: BlockHeaderArtifact::new(
                BlockHeight::new(height),
                BlockHash::from_bytes(block_hash),
                BlockHash::from_bytes(parent_hash),
                [0; 32],
                [0; 32],
                0,
                0,
                [0; 32],
                0,
                0,
            ),
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&[0]),
            transactions,
        }
    }

    fn transaction_facts(
        transaction_id: TransactionId,
        transparent_inputs: Vec<TransparentInputFact>,
        transparent_outputs: Vec<TransparentOutputFact>,
    ) -> CanonicalTransactionFacts {
        CanonicalTransactionFacts {
            public_facts: TransactionPublicFacts {
                transaction_id,
                auth_digest: None,
                wtxid: None,
                version: TransactionVersion::Unsupported {
                    effective_version: 0,
                    version_group_id: None,
                },
                consensus_branch_id: None,
                lock_time: LockTime::Unlocked,
                expiry_height: None,
                size_bytes: 0,
                counts: TransactionComponentCounts::EMPTY,
                orchard_value_balance_zat: None,
                orchard_anchor: None,
                ironwood_value_balance_zat: None,
                privacy_shape: PrivacyShape::Unclassified,
                is_coinbase: false,
                unsupported_sections: Vec::new(),
            },
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&[0]),
            intrinsic_value_balances: TransactionIntrinsicValueBalances::default(),
            transparent_inputs,
            transparent_outputs,
        }
    }
}
