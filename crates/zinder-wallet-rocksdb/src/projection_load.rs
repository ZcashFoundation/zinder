//! Single-scan external construction of wallet projection SSTs.

use std::{
    collections::{BTreeSet, VecDeque},
    mem::size_of,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use rust_rocksdb::Options;
use zinder_core::wire::UtxoSetCommitmentElement;
use zinder_core::{
    BlockHash, BlockHeight, BlockId, CanonicalBlockFactsSequenceDigest,
    CanonicalBlockFactsSequenceDigestBuilder, CanonicalBlockFactsSequenceDigestVersion, Network,
    TransactionId, TransparentUtxoSetCommitment, ValidatedCanonicalBlockReplay,
};
use zinder_rocksdb_bulk_load::{
    OrderedSstWriter, SortedVariableValues, SstFileSet, VariableValueSortEvidence,
    VariableValueSorter,
};
use zinder_store::CanonicalStoreError;
use zinder_wallet_projection::{
    WalletAddressBalance, WalletAddressTransaction, WalletAddressTransactionKey,
    WalletAddressUnspentOutputKey, WalletOutpointKey, WalletProjectionAccumulator,
    WalletProjectionContractError, WalletProjectionDigest, WalletProjectionDigestBuilder,
    WalletProjectionFamilyRowCounts, WalletProjectionRowFamily, WalletReorgUndo, WalletSpentOutput,
    WalletTransactionPosition, WalletUnspentOutput, WalletUtxoSetSummary,
};

use crate::{
    RocksDbWalletError,
    store::{
        REORG_UNDO_COLUMN_FAMILY, TRANSPARENT_ADDRESS_BALANCE_COLUMN_FAMILY,
        TRANSPARENT_ADDRESS_TRANSACTION_COLUMN_FAMILY, TRANSPARENT_SPENT_OUTPUT_COLUMN_FAMILY,
        TRANSPARENT_UNSPENT_OUTPUT_BY_ADDRESS_COLUMN_FAMILY,
        TRANSPARENT_UNSPENT_OUTPUT_COLUMN_FAMILY,
    },
};

const OUTPOINT_EVENT_KEY_BYTES: usize = 37;
const ADDRESS_UNSPENT_KEY_BYTES: usize = 72;
const ADDRESS_TRANSACTION_KEY_BYTES: usize = 40;
const SPEND_TRAILER_BYTES: usize = 76;
const OUTPUT_EVENT_TAG: u8 = 0;
const SPEND_EVENT_TAG: u8 = 1;

/// Resource limits for one external projection construction.
#[derive(Clone, Copy)]
pub(crate) struct WalletProjectionLoadConfig<'options> {
    pub(crate) staging_path: &'options Path,
    pub(crate) options: &'options Options,
    pub(crate) network: Network,
    pub(crate) first_retained_block: BlockId,
    pub(crate) settled_tip: BlockId,
    pub(crate) supported_reorg_depth: u32,
    pub(crate) max_outpoint_sort_memory_bytes: u64,
    pub(crate) max_secondary_sort_memory_bytes_per_sorter: u64,
    pub(crate) max_temporary_file_bytes_per_sorter: u64,
    pub(crate) sst_target_logical_bytes: u64,
    pub(crate) max_accounted_reorg_undo_bytes: u64,
}

/// Counts proving the build used one authenticated scan and no prevout reads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalletProjectionLoadCounters {
    pub(crate) scanned_block_count: u64,
    pub(crate) scanned_transaction_count: u64,
    pub(crate) staged_output_count: u64,
    pub(crate) staged_spend_count: u64,
    pub(crate) historical_prevout_read_count: u64,
    pub(crate) peak_accounted_reorg_undo_bytes: u64,
    pub(crate) max_accounted_reorg_undo_bytes: u64,
}

/// Phase timings exposed by the fixed-tip build report.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct WalletProjectionLoadPhaseDurations {
    pub(crate) canonical_scan: Duration,
    pub(crate) outpoint_sort: Duration,
    pub(crate) outpoint_merge: Duration,
    pub(crate) secondary_row_derivation: Duration,
    pub(crate) logical_evidence: Duration,
}

/// One prepared physical family ready for external-file ingestion.
pub(crate) struct PreparedWalletColumnFamily {
    pub(crate) name: &'static str,
    pub(crate) paths: Vec<PathBuf>,
}

/// Complete version-1 SST artifacts and the evidence derived while writing them.
pub(crate) struct PreparedWalletProjectionLoad {
    pub(crate) network: Network,
    pub(crate) supported_reorg_depth: u32,
    pub(crate) first_block: BlockId,
    pub(crate) tip: BlockId,
    pub(crate) source_sequence_digest: CanonicalBlockFactsSequenceDigest,
    pub(crate) projection_digest: WalletProjectionDigest,
    pub(crate) projection_accumulator: WalletProjectionAccumulator,
    pub(crate) row_counts: WalletProjectionFamilyRowCounts,
    pub(crate) utxo_summary: WalletUtxoSetSummary,
    pub(crate) counters: WalletProjectionLoadCounters,
    pub(crate) phase_durations: WalletProjectionLoadPhaseDurations,
    pub(crate) outpoint_sort_evidence: VariableValueSortEvidence,
    pub(crate) address_index_sort_evidence: VariableValueSortEvidence,
    pub(crate) address_transaction_sort_evidence: VariableValueSortEvidence,
    pub(crate) logical_row_bytes: u64,
    pub(crate) sst_file_bytes: u64,
    pub(crate) sst_file_count: u64,
    pub(crate) families: Vec<PreparedWalletColumnFamily>,
}

#[derive(Clone, Copy)]
struct StagedSpend {
    spent_at: WalletTransactionPosition,
    input_index: u32,
}

#[derive(Default)]
struct WalletProjectionSstEvidence {
    logical_row_bytes: u64,
}

impl WalletProjectionSstEvidence {
    fn add_row(&mut self, key: &[u8], encoded_value: &[u8]) -> Result<(), RocksDbWalletError> {
        let row_bytes = u64::try_from(key.len())
            .ok()
            .and_then(|key_bytes| {
                u64::try_from(encoded_value.len())
                    .ok()
                    .and_then(|value_bytes| key_bytes.checked_add(value_bytes))
            })
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
        self.logical_row_bytes = self
            .logical_row_bytes
            .checked_add(row_bytes)
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
        Ok(())
    }
}

struct RetainedUndo {
    records: VecDeque<WalletReorgUndo>,
    supported_depth: usize,
    settled_tip: BlockId,
    tracking_current_block: bool,
    accounted_bytes: u64,
    peak_accounted_bytes: u64,
    max_accounted_bytes: u64,
}

impl RetainedUndo {
    fn new(supported_depth: u32, settled_tip: BlockId, max_accounted_bytes: u64) -> Self {
        Self {
            records: VecDeque::new(),
            supported_depth: usize::try_from(supported_depth).unwrap_or(usize::MAX),
            settled_tip,
            tracking_current_block: false,
            accounted_bytes: 0,
            peak_accounted_bytes: 0,
            max_accounted_bytes,
        }
    }

    fn begin_block(
        &mut self,
        block: BlockId,
        parent_hash: BlockHash,
        source_sequence_digest_before: CanonicalBlockFactsSequenceDigest,
        source_sequence_digest_after: CanonicalBlockFactsSequenceDigest,
    ) -> Result<(), RocksDbWalletError> {
        self.tracking_current_block = false;
        if block.height < self.settled_tip.height {
            return Ok(());
        }
        if block.height == self.settled_tip.height {
            if block != self.settled_tip {
                return Err(RocksDbWalletError::CanonicalSourceFenceMismatch {
                    reason: "canonical replay settled-height block differs from the admitted settled tip",
                });
            }
            return Ok(());
        }
        if self.supported_depth == 0 || self.records.len() >= self.supported_depth {
            return Err(RocksDbWalletError::WalletProjectionRebuildRequired {
                reason: "canonical unsettled suffix exceeds the wallet's configured undo capacity",
            });
        }
        self.reserve(
            u64::try_from(size_of::<WalletReorgUndo>())
                .map_err(|_| RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?,
        )?;
        self.records.push_back(WalletReorgUndo {
            block,
            parent_hash,
            source_sequence_digest_before,
            source_sequence_digest_after,
            created_outpoints: Vec::new(),
            spent_outpoints: Vec::new(),
            address_transaction_keys: Vec::new(),
        });
        self.tracking_current_block = true;
        Ok(())
    }

    fn add_created_outpoint(&mut self, key: WalletOutpointKey) -> Result<(), RocksDbWalletError> {
        if !self.tracking_current_block {
            return Ok(());
        }
        self.reserve(
            u64::try_from(size_of::<WalletOutpointKey>())
                .map_err(|_| RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?,
        )?;
        self.records
            .back_mut()
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?
            .created_outpoints
            .push(key);
        Ok(())
    }

    fn add_spent_outpoint(&mut self, key: WalletOutpointKey) -> Result<(), RocksDbWalletError> {
        if !self.tracking_current_block {
            return Ok(());
        }
        self.reserve(
            u64::try_from(size_of::<WalletOutpointKey>())
                .map_err(|_| RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?,
        )?;
        self.records
            .back_mut()
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?
            .spent_outpoints
            .push(key);
        Ok(())
    }

    fn finish_block(&mut self) -> Result<(), RocksDbWalletError> {
        if !self.tracking_current_block {
            return Ok(());
        }
        let undo = self
            .records
            .back_mut()
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
        undo.created_outpoints.sort_unstable();
        undo.spent_outpoints.sort_unstable();
        Ok(())
    }

    fn reserve_transient(&mut self, additional_bytes: u64) -> Result<(), RocksDbWalletError> {
        if !self.tracking_current_block {
            return Ok(());
        }
        self.reserve(additional_bytes)
    }

    fn release_transient(&mut self, bytes: u64) -> Result<(), RocksDbWalletError> {
        if !self.tracking_current_block {
            return Ok(());
        }
        self.accounted_bytes = self
            .accounted_bytes
            .checked_sub(bytes)
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
        Ok(())
    }

    fn add_address_transaction(
        &mut self,
        key: WalletAddressTransactionKey,
    ) -> Result<(), RocksDbWalletError> {
        let Some(undo_index) = self
            .records
            .iter()
            .position(|undo| undo.block.height == key.block_height())
        else {
            return Ok(());
        };
        self.reserve(
            u64::try_from(size_of::<WalletAddressTransactionKey>())
                .map_err(|_| RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?,
        )?;
        self.records[undo_index].address_transaction_keys.push(key);
        Ok(())
    }

    fn reserve(&mut self, additional_bytes: u64) -> Result<(), RocksDbWalletError> {
        let required_bytes = self
            .accounted_bytes
            .checked_add(additional_bytes)
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
        if required_bytes > self.max_accounted_bytes {
            return Err(RocksDbWalletError::AccountedReorgUndoMemoryLimit {
                limit_bytes: self.max_accounted_bytes,
                required_bytes,
            });
        }
        self.accounted_bytes = required_bytes;
        self.peak_accounted_bytes = self.peak_accounted_bytes.max(required_bytes);
        Ok(())
    }
}

/// Writes all six wallet families from one authenticated canonical replay scan.
#[allow(
    clippy::too_many_lines,
    reason = "the loader keeps the source scan, external drains, and evidence fence in visible order"
)]
pub(crate) fn write_projection_ssts(
    config: WalletProjectionLoadConfig<'_>,
    blocks: impl IntoIterator<Item = Result<ValidatedCanonicalBlockReplay, CanonicalStoreError>>,
) -> Result<PreparedWalletProjectionLoad, RocksDbWalletError> {
    let scan_started = Instant::now();
    let mut outpoint_sorter = VariableValueSorter::<OUTPOINT_EVENT_KEY_BYTES>::new(
        config.staging_path,
        "wallet-outpoint-events",
        config.max_outpoint_sort_memory_bytes,
        config.max_temporary_file_bytes_per_sorter,
    )?;
    let mut retained_undo = RetainedUndo::new(
        config.supported_reorg_depth,
        config.settled_tip,
        config.max_accounted_reorg_undo_bytes,
    );
    let mut sequence_digest =
        CanonicalBlockFactsSequenceDigestBuilder::new(CanonicalBlockFactsSequenceDigestVersion::V1);
    let mut first_block = None;
    let mut previous_block = None;
    let mut scanned_block_count = 0_u64;
    let mut scanned_transaction_count = 0_u64;
    let mut staged_output_count = 0_u64;
    let mut staged_spend_count = 0_u64;

    for replay in blocks {
        let replay = replay.map_err(|source| RocksDbWalletError::CanonicalReplay { source })?;
        let reference_digest = replay.reference_digest();
        let facts = replay.into_facts();
        let block = BlockId::new(facts.block_header.height, facts.block_header.block_hash);
        validate_next_block(
            config.first_retained_block,
            previous_block,
            block,
            facts.block_header.parent_hash,
        )?;
        first_block.get_or_insert(block);
        let source_sequence_digest_before = sequence_digest.clone().finish();
        sequence_digest.try_append(reference_digest)?;
        let source_sequence_digest_after = sequence_digest.clone().finish();
        scanned_block_count = increment(scanned_block_count)?;

        retained_undo.begin_block(
            block,
            facts.block_header.parent_hash,
            source_sequence_digest_before,
            source_sequence_digest_after,
        )?;
        let mut block_created_outpoints = BTreeSet::new();
        for (transaction_index, transaction) in facts.transactions.iter().enumerate() {
            let tx_index_in_block = u32::try_from(transaction_index)
                .map_err(|_| WalletProjectionContractError::FactIndexOverflow)?;
            scanned_transaction_count = increment(scanned_transaction_count)?;
            let transaction_position = WalletTransactionPosition::new(
                transaction.public_facts.transaction_id,
                tx_index_in_block,
                block,
            );
            for (input_position, input) in transaction.transparent_inputs.iter().enumerate() {
                let expected_input_index = u32::try_from(input_position)
                    .map_err(|_| WalletProjectionContractError::FactIndexOverflow)?;
                if input.input_index != expected_input_index {
                    return Err(WalletProjectionContractError::FactIndexMismatch.into());
                }
                if input.spent_outpoint.is_coinbase_sentinel() {
                    continue;
                }
                let outpoint_key = WalletOutpointKey::new(input.spent_outpoint);
                outpoint_sorter.push(
                    outpoint_event_key(outpoint_key, SPEND_EVENT_TAG),
                    &encode_spend_trailer(StagedSpend {
                        spent_at: transaction_position,
                        input_index: input.input_index,
                    }),
                )?;
                staged_spend_count = increment(staged_spend_count)?;
                if !block_created_outpoints.contains(&outpoint_key) {
                    retained_undo.add_spent_outpoint(outpoint_key)?;
                }
            }
            for (output_position, output) in transaction.transparent_outputs.iter().enumerate() {
                let expected_output_index = u32::try_from(output_position)
                    .map_err(|_| WalletProjectionContractError::FactIndexOverflow)?;
                if output.output_index != expected_output_index {
                    return Err(WalletProjectionContractError::FactIndexMismatch.into());
                }
                let outpoint = zinder_core::TransparentOutPoint::new(
                    transaction.public_facts.transaction_id,
                    output.output_index,
                );
                let outpoint_key = WalletOutpointKey::new(outpoint);
                let wallet_output = WalletUnspentOutput::new(
                    outpoint,
                    output.address_script_hash,
                    output.value_zat,
                    output.script_pub_key.clone(),
                    transaction_position,
                )?;
                outpoint_sorter.push(
                    outpoint_event_key(outpoint_key, OUTPUT_EVENT_TAG),
                    &wallet_output.encode_value()?,
                )?;
                staged_output_count = increment(staged_output_count)?;
                if config.supported_reorg_depth != 0
                    && !block_created_outpoints.contains(&outpoint_key)
                {
                    retained_undo.reserve_transient(
                        u64::try_from(size_of::<WalletOutpointKey>()).map_err(|_| {
                            RocksDbWalletError::WalletProjectionLoadAccountingOverflow
                        })?,
                    )?;
                    block_created_outpoints.insert(outpoint_key);
                }
                retained_undo.add_created_outpoint(outpoint_key)?;
            }
        }
        retained_undo.release_transient(
            u64::try_from(block_created_outpoints.len())
                .ok()
                .and_then(|count| {
                    u64::try_from(size_of::<WalletOutpointKey>())
                        .ok()
                        .and_then(|bytes| count.checked_mul(bytes))
                })
                .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?,
        )?;
        retained_undo.finish_block()?;
        previous_block = Some(block);
    }
    let first_block = first_block.ok_or(RocksDbWalletError::EmptyCanonicalHistory)?;
    let tip = previous_block.ok_or(RocksDbWalletError::EmptyCanonicalHistory)?;
    let canonical_scan = scan_started.elapsed();

    let phase_started = Instant::now();
    let mut sorted_outpoints = outpoint_sorter.finish()?;
    let outpoint_sort_evidence = sorted_outpoints.evidence();
    let outpoint_sort = phase_started.elapsed();

    let phase_started = Instant::now();
    let mut address_index_sorter = VariableValueSorter::<ADDRESS_UNSPENT_KEY_BYTES>::new(
        config.staging_path,
        "wallet-address-unspent",
        config.max_secondary_sort_memory_bytes_per_sorter,
        config.max_temporary_file_bytes_per_sorter,
    )?;
    let mut address_transaction_sorter = VariableValueSorter::<ADDRESS_TRANSACTION_KEY_BYTES>::new(
        config.staging_path,
        "wallet-address-transactions",
        config.max_secondary_sort_memory_bytes_per_sorter,
        config.max_temporary_file_bytes_per_sorter,
    )?;
    let mut unspent_writer = ordered_writer(config, "wallet-unspent-output")?;
    let mut spent_writer = ordered_writer(config, "wallet-spent-output")?;
    let mut digest = WalletProjectionDigestBuilder::new();
    let mut sst_evidence = WalletProjectionSstEvidence::default();
    let mut utxo_count = 0_u64;
    let mut total_value_zat = 0_u64;
    let mut commitment = TransparentUtxoSetCommitment::empty();
    drain_outpoint_events(
        config.network,
        &mut sorted_outpoints,
        &mut unspent_writer,
        &mut spent_writer,
        &mut address_index_sorter,
        &mut address_transaction_sorter,
        &mut digest,
        &mut sst_evidence,
        &mut utxo_count,
        &mut total_value_zat,
        &mut commitment,
    )?;
    let unspent_files = unspent_writer.finish()?;
    let spent_files = spent_writer.finish()?;
    let outpoint_merge = phase_started.elapsed();

    let phase_started = Instant::now();
    let mut sorted_address_index = address_index_sorter.finish()?;
    let address_index_sort_evidence = sorted_address_index.evidence();
    let mut address_index_writer = ordered_writer(config, "wallet-address-unspent-index")?;
    let mut address_balance_writer = ordered_writer(config, "wallet-address-balance")?;
    drain_address_index(
        &mut sorted_address_index,
        &mut address_index_writer,
        &mut address_balance_writer,
        &mut digest,
        &mut sst_evidence,
    )?;
    let address_index_files = address_index_writer.finish()?;
    let address_balance_files = address_balance_writer.finish()?;

    let mut sorted_address_transactions = address_transaction_sorter.finish()?;
    let address_transaction_sort_evidence = sorted_address_transactions.evidence();
    let mut address_transaction_writer = ordered_writer(config, "wallet-address-transaction")?;
    drain_address_transactions(
        &mut sorted_address_transactions,
        &mut address_transaction_writer,
        &mut retained_undo,
        &mut digest,
        &mut sst_evidence,
    )?;
    let address_transaction_files = address_transaction_writer.finish()?;

    let mut reorg_undo_writer = ordered_writer(config, "wallet-reorg-undo")?;
    for mut undo in retained_undo.records {
        undo.address_transaction_keys.sort_unstable();
        let key = undo.encode_key();
        let encoded_value = undo.encode_value()?;
        append_row(
            &mut reorg_undo_writer,
            &mut digest,
            &mut sst_evidence,
            WalletProjectionRowFamily::ReorgUndo,
            &key,
            &encoded_value,
        )?;
    }
    let reorg_undo_files = reorg_undo_writer.finish()?;
    let secondary_row_derivation = phase_started.elapsed();

    let phase_started = Instant::now();
    let row_counts = digest.row_counts();
    let (projection_accumulator, projection_digest) = digest.finish_with_accumulator();
    let logical_evidence = phase_started.elapsed();
    let artifacts = [
        (TRANSPARENT_UNSPENT_OUTPUT_COLUMN_FAMILY, unspent_files),
        (
            TRANSPARENT_UNSPENT_OUTPUT_BY_ADDRESS_COLUMN_FAMILY,
            address_index_files,
        ),
        (TRANSPARENT_SPENT_OUTPUT_COLUMN_FAMILY, spent_files),
        (
            TRANSPARENT_ADDRESS_TRANSACTION_COLUMN_FAMILY,
            address_transaction_files,
        ),
        (
            TRANSPARENT_ADDRESS_BALANCE_COLUMN_FAMILY,
            address_balance_files,
        ),
        (REORG_UNDO_COLUMN_FAMILY, reorg_undo_files),
    ];
    let (families, sst_file_bytes, sst_file_count) = prepare_families(artifacts)?;

    Ok(PreparedWalletProjectionLoad {
        network: config.network,
        supported_reorg_depth: config.supported_reorg_depth,
        first_block,
        tip,
        source_sequence_digest: sequence_digest.finish(),
        projection_digest,
        projection_accumulator,
        row_counts,
        utxo_summary: WalletUtxoSetSummary {
            utxo_count,
            total_value_zat,
            commitment,
        },
        counters: WalletProjectionLoadCounters {
            scanned_block_count,
            scanned_transaction_count,
            staged_output_count,
            staged_spend_count,
            historical_prevout_read_count: 0,
            peak_accounted_reorg_undo_bytes: retained_undo.peak_accounted_bytes,
            max_accounted_reorg_undo_bytes: retained_undo.max_accounted_bytes,
        },
        phase_durations: WalletProjectionLoadPhaseDurations {
            canonical_scan,
            outpoint_sort,
            outpoint_merge,
            secondary_row_derivation,
            logical_evidence,
        },
        outpoint_sort_evidence,
        address_index_sort_evidence,
        address_transaction_sort_evidence,
        logical_row_bytes: sst_evidence.logical_row_bytes,
        sst_file_bytes,
        sst_file_count,
        families,
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "the streaming drain owns three output sinks and exact evidence accumulators"
)]
fn drain_outpoint_events(
    network: Network,
    sorted: &mut SortedVariableValues<OUTPOINT_EVENT_KEY_BYTES>,
    unspent_writer: &mut OrderedSstWriter<'_>,
    spent_writer: &mut OrderedSstWriter<'_>,
    address_index_sorter: &mut VariableValueSorter<ADDRESS_UNSPENT_KEY_BYTES>,
    address_transaction_sorter: &mut VariableValueSorter<ADDRESS_TRANSACTION_KEY_BYTES>,
    digest: &mut WalletProjectionDigestBuilder,
    sst_evidence: &mut WalletProjectionSstEvidence,
    utxo_count: &mut u64,
    total_value_zat: &mut u64,
    commitment: &mut TransparentUtxoSetCommitment,
) -> Result<(), RocksDbWalletError> {
    let mut pending = sorted.next_record()?;
    while let Some(first) = pending.take() {
        let outpoint_key = WalletOutpointKey::decode(&first.key[..36])?;
        let mut output = None;
        let mut spend = None;
        accept_outpoint_event(&first.key, &first.encoded_value, &mut output, &mut spend)?;
        loop {
            let next = sorted.next_record()?;
            let Some(next_record) = next else {
                pending = None;
                break;
            };
            if next_record.key[..36] != first.key[..36] {
                pending = Some(next_record);
                break;
            }
            accept_outpoint_event(
                &next_record.key,
                &next_record.encoded_value,
                &mut output,
                &mut spend,
            )?;
        }
        let output = output.ok_or(WalletProjectionContractError::MissingTransparentPredecessor)?;
        stage_address_transaction(&output, output.created_at, address_transaction_sorter)?;
        if let Some(spend) = spend {
            if !precedes(output.created_at, spend.spent_at) {
                return Err(WalletProjectionContractError::MissingTransparentPredecessor.into());
            }
            let spent_output = WalletSpentOutput::new(output, spend.spent_at, spend.input_index);
            stage_address_transaction(
                &spent_output.output,
                spent_output.spent_at,
                address_transaction_sorter,
            )?;
            let encoded_value = spent_output.encode_value()?;
            append_row(
                spent_writer,
                digest,
                sst_evidence,
                WalletProjectionRowFamily::TransparentSpentOutput,
                outpoint_key.as_bytes(),
                &encoded_value,
            )?;
        } else {
            let address_key = WalletAddressUnspentOutputKey::new(&output);
            address_index_sorter.push(*address_key.as_bytes(), &output.value_zat.to_be_bytes())?;
            *utxo_count = increment(*utxo_count)?;
            *total_value_zat = total_value_zat
                .checked_add(output.value_zat)
                .ok_or(WalletProjectionContractError::UtxoValueOverflow)?;
            commitment.insert(&UtxoSetCommitmentElement {
                network_id: network.id(),
                outpoint: output.outpoint,
                value_zat: output.value_zat,
                script_pub_key: &output.script_pub_key,
                block_height: output.created_at.block.height,
            });
            let encoded_value = output.encode_value()?;
            append_row(
                unspent_writer,
                digest,
                sst_evidence,
                WalletProjectionRowFamily::TransparentUnspentOutput,
                outpoint_key.as_bytes(),
                &encoded_value,
            )?;
        }
    }
    Ok(())
}

fn accept_outpoint_event(
    key: &[u8; OUTPOINT_EVENT_KEY_BYTES],
    encoded_value: &[u8],
    output: &mut Option<WalletUnspentOutput>,
    spend: &mut Option<StagedSpend>,
) -> Result<(), RocksDbWalletError> {
    let outpoint_key = WalletOutpointKey::decode(&key[..36])?;
    match key[36] {
        OUTPUT_EVENT_TAG => {
            if output.is_some() {
                return Err(WalletProjectionContractError::DuplicateOutput.into());
            }
            *output = Some(WalletUnspentOutput::decode_value(
                outpoint_key,
                encoded_value,
            )?);
        }
        SPEND_EVENT_TAG => {
            if spend.is_some() {
                return Err(WalletProjectionContractError::DuplicateSpend.into());
            }
            *spend = Some(decode_spend_trailer(encoded_value)?);
        }
        _ => {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "outpoint event sort contains an unsupported tag",
            });
        }
    }
    Ok(())
}

fn drain_address_index(
    sorted: &mut SortedVariableValues<ADDRESS_UNSPENT_KEY_BYTES>,
    index_writer: &mut OrderedSstWriter<'_>,
    balance_writer: &mut OrderedSstWriter<'_>,
    digest: &mut WalletProjectionDigestBuilder,
    evidence: &mut WalletProjectionSstEvidence,
) -> Result<(), RocksDbWalletError> {
    let mut current_address = None;
    let mut current_balance = 0_u64;
    while let Some(record) = sorted.next_record()? {
        if record.encoded_value.len() != 8 {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "address-index staging value is not an exact u64",
            });
        }
        let address_key = WalletAddressUnspentOutputKey::decode(&record.key)?;
        let address = address_key.address_script_hash();
        if current_address.is_some_and(|current| current != address) {
            write_balance(
                current_address.ok_or(WalletProjectionContractError::AddressBalanceUnderflow)?,
                current_balance,
                balance_writer,
                digest,
                evidence,
            )?;
            current_balance = 0;
        }
        current_address = Some(address);
        let value_zat = u64::from_be_bytes(record.encoded_value.try_into().map_err(|_| {
            RocksDbWalletError::AdmissionChanged {
                reason: "address-index staging value changed after length validation",
            }
        })?);
        current_balance = current_balance
            .checked_add(value_zat)
            .ok_or(WalletProjectionContractError::AddressBalanceOverflow)?;
        append_row(
            index_writer,
            digest,
            evidence,
            WalletProjectionRowFamily::TransparentUnspentOutputByAddress,
            &record.key,
            &[],
        )?;
    }
    if let Some(address) = current_address {
        write_balance(address, current_balance, balance_writer, digest, evidence)?;
    }
    Ok(())
}

fn write_balance(
    address: zinder_core::TransparentAddressScriptHash,
    balance_zat: u64,
    writer: &mut OrderedSstWriter<'_>,
    digest: &mut WalletProjectionDigestBuilder,
    evidence: &mut WalletProjectionSstEvidence,
) -> Result<(), RocksDbWalletError> {
    if balance_zat == 0 {
        return Ok(());
    }
    let balance = WalletAddressBalance {
        address_script_hash: address,
        balance_zat,
    };
    append_row(
        writer,
        digest,
        evidence,
        WalletProjectionRowFamily::TransparentAddressBalance,
        &balance.encode_key(),
        &balance.encode_value(),
    )
}

fn drain_address_transactions(
    sorted: &mut SortedVariableValues<ADDRESS_TRANSACTION_KEY_BYTES>,
    writer: &mut OrderedSstWriter<'_>,
    retained_undo: &mut RetainedUndo,
    digest: &mut WalletProjectionDigestBuilder,
    evidence: &mut WalletProjectionSstEvidence,
) -> Result<(), RocksDbWalletError> {
    let mut pending = sorted.next_record()?;
    while let Some(first) = pending.take() {
        if first.encoded_value.len() != 64 {
            return Err(RocksDbWalletError::AdmissionChanged {
                reason: "address-transaction staging value is not exact64",
            });
        }
        let mut canonical_value = first.encoded_value;
        loop {
            let next = sorted.next_record()?;
            let Some(next_record) = next else {
                pending = None;
                break;
            };
            if next_record.key != first.key {
                pending = Some(next_record);
                break;
            }
            if next_record.encoded_value != canonical_value {
                return Err(RocksDbWalletError::AddressTransactionConflict);
            }
            canonical_value = next_record.encoded_value;
        }
        let key = WalletAddressTransactionKey::decode(&first.key)?;
        WalletAddressTransaction::decode_value(key, &canonical_value)?;
        retained_undo.add_address_transaction(key)?;
        append_row(
            writer,
            digest,
            evidence,
            WalletProjectionRowFamily::TransparentAddressTransaction,
            &first.key,
            &canonical_value,
        )?;
    }
    Ok(())
}

fn stage_address_transaction(
    output: &WalletUnspentOutput,
    position: WalletTransactionPosition,
    sorter: &mut VariableValueSorter<ADDRESS_TRANSACTION_KEY_BYTES>,
) -> Result<(), RocksDbWalletError> {
    let key = WalletAddressTransactionKey::new(
        output.address_script_hash,
        position.block.height,
        position.tx_index_in_block,
    );
    let transaction =
        WalletAddressTransaction::new(key, position.transaction_id, position.block.hash);
    sorter.push(*key.as_bytes(), &transaction.encode_value())?;
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "one row must update its physical writer and both pieces of logical evidence together"
)]
fn append_row(
    writer: &mut OrderedSstWriter<'_>,
    digest: &mut WalletProjectionDigestBuilder,
    evidence: &mut WalletProjectionSstEvidence,
    family: WalletProjectionRowFamily,
    key: &[u8],
    encoded_value: &[u8],
) -> Result<(), RocksDbWalletError> {
    writer.put(key, encoded_value)?;
    digest.append_row(family, key, encoded_value)?;
    evidence.add_row(key, encoded_value)
}

fn ordered_writer<'options>(
    config: WalletProjectionLoadConfig<'options>,
    artifact_label: &'static str,
) -> Result<OrderedSstWriter<'options>, RocksDbWalletError> {
    Ok(OrderedSstWriter::new(
        config.staging_path,
        artifact_label,
        config.options,
        config.sst_target_logical_bytes,
    )?)
}

fn prepare_families(
    artifacts: [(&'static str, SstFileSet); 6],
) -> Result<(Vec<PreparedWalletColumnFamily>, u64, u64), RocksDbWalletError> {
    let mut families = Vec::with_capacity(artifacts.len());
    let mut sst_file_bytes = 0_u64;
    let mut sst_file_count = 0_u64;
    for (name, files) in artifacts {
        sst_file_bytes = sst_file_bytes
            .checked_add(files.file_bytes)
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
        sst_file_count = sst_file_count
            .checked_add(
                u64::try_from(files.paths.len())
                    .map_err(|_| RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?,
            )
            .ok_or(RocksDbWalletError::WalletProjectionLoadAccountingOverflow)?;
        families.push(PreparedWalletColumnFamily {
            name,
            paths: files.paths,
        });
    }
    Ok((families, sst_file_bytes, sst_file_count))
}

fn validate_next_block(
    first_retained_block: BlockId,
    previous: Option<BlockId>,
    block: BlockId,
    parent_hash: BlockHash,
) -> Result<(), WalletProjectionContractError> {
    match previous {
        None if block == first_retained_block => Ok(()),
        Some(previous)
            if previous.height.next() == Some(block.height) && previous.hash == parent_hash =>
        {
            Ok(())
        }
        None | Some(_) => Err(WalletProjectionContractError::NonContiguousBlock),
    }
}

fn precedes(created_at: WalletTransactionPosition, spent_at: WalletTransactionPosition) -> bool {
    if created_at.block.height < spent_at.block.height {
        return true;
    }
    created_at.block.height == spent_at.block.height
        && created_at.block.hash == spent_at.block.hash
        && created_at.tx_index_in_block < spent_at.tx_index_in_block
}

fn outpoint_event_key(key: WalletOutpointKey, tag: u8) -> [u8; OUTPOINT_EVENT_KEY_BYTES] {
    let mut encoded = [0; OUTPOINT_EVENT_KEY_BYTES];
    encoded[..36].copy_from_slice(key.as_bytes());
    encoded[36] = tag;
    encoded
}

fn encode_spend_trailer(spend: StagedSpend) -> [u8; SPEND_TRAILER_BYTES] {
    let mut encoded = [0; SPEND_TRAILER_BYTES];
    encoded[..32].copy_from_slice(&spend.spent_at.transaction_id.as_bytes());
    encoded[32..36].copy_from_slice(&spend.input_index.to_be_bytes());
    encoded[36..40].copy_from_slice(&spend.spent_at.tx_index_in_block.to_be_bytes());
    encoded[40..44].copy_from_slice(&spend.spent_at.block.height.value().to_be_bytes());
    encoded[44..].copy_from_slice(&spend.spent_at.block.hash.as_bytes());
    encoded
}

fn decode_spend_trailer(encoded: &[u8]) -> Result<StagedSpend, RocksDbWalletError> {
    let encoded: [u8; SPEND_TRAILER_BYTES] =
        encoded
            .try_into()
            .map_err(|_| RocksDbWalletError::AdmissionChanged {
                reason: "spend staging value is not the exact version-1 76-byte trailer",
            })?;
    let mut transaction_id = [0; 32];
    transaction_id.copy_from_slice(&encoded[..32]);
    let mut input_index = [0; 4];
    input_index.copy_from_slice(&encoded[32..36]);
    let mut tx_index = [0; 4];
    tx_index.copy_from_slice(&encoded[36..40]);
    let mut height = [0; 4];
    height.copy_from_slice(&encoded[40..44]);
    let mut block_hash = [0; 32];
    block_hash.copy_from_slice(&encoded[44..]);
    Ok(StagedSpend {
        spent_at: WalletTransactionPosition::new(
            TransactionId::from_bytes(transaction_id),
            u32::from_be_bytes(tx_index),
            BlockId::new(
                BlockHeight::new(u32::from_be_bytes(height)),
                BlockHash::from_bytes(block_hash),
            ),
        ),
        input_index: u32::from_be_bytes(input_index),
    })
}

fn increment(counter: u64) -> Result<u64, RocksDbWalletError> {
    counter
        .checked_add(1)
        .ok_or(RocksDbWalletError::BuildCounterOverflow)
}

#[cfg(test)]
mod tests {
    use std::{error::Error, path::Path};

    use tempfile::TempDir;
    use zinder_core::{
        BlockHeaderArtifact, CanonicalBlockFacts, CanonicalBlockFactsDigestVersion,
        CanonicalBlockReplayFormatVersion, CanonicalTransactionFacts, LockTime, PrivacyShape,
        SerializedBytesDigest, TransactionComponentCounts, TransactionIntrinsicValueBalances,
        TransactionPublicFacts, TransactionVersion, TransparentAddressScriptHash,
        TransparentInputFact, TransparentOutPoint, TransparentOutputFact,
        decode_canonical_block_replay, encode_canonical_block_replay,
    };
    use zinder_rocksdb_bulk_load::BulkLoadError;
    use zinder_wallet_projection::WalletProjectionSerialOracle;

    use super::*;

    const TEST_SORT_MEMORY_BYTES: u64 = 1024 * 1024;
    const TEST_TEMPORARY_FILE_BYTES: u64 = 16 * 1024 * 1024;
    const TEST_SST_LOGICAL_BYTES: u64 = 1024 * 1024;
    const TEST_REORG_UNDO_BYTES: u64 = 1024 * 1024;

    #[derive(Clone, Copy)]
    struct TestByteLimits {
        outpoint_sort: u64,
        secondary_sort_per_sorter: u64,
        temporary_files_per_sorter: u64,
    }

    impl Default for TestByteLimits {
        fn default() -> Self {
            Self {
                outpoint_sort: TEST_SORT_MEMORY_BYTES,
                secondary_sort_per_sorter: TEST_SORT_MEMORY_BYTES,
                temporary_files_per_sorter: TEST_TEMPORARY_FILE_BYTES,
            }
        }
    }

    #[test]
    fn retained_undo_accounting_bounds_a_thousand_block_unsettled_suffix()
    -> Result<(), Box<dyn Error>> {
        const DEPTH: u32 = 1_000;
        // The projector's default `build.max_accounted_reorg_undo_bytes`.
        const DEFAULT_UNDO_CAP_BYTES: u64 = 512 * 1024 * 1024;
        // Created and spent outpoints per block in a transparent-heavy suffix.
        const OUTPOINTS_PER_BLOCK: u32 = 64;
        let settled_tip = BlockId::new(BlockHeight::new(10_000), BlockHash::from_bytes([1; 32]));
        let digest = CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
            CanonicalBlockFactsSequenceDigestVersion::V1,
            1,
            [2; 32],
        );
        let fill = |undo: &mut RetainedUndo, block_count: u32| {
            for offset in 1..=block_count {
                let height = BlockHeight::new(settled_tip.height.value() + offset);
                let hash = BlockHash::from_bytes([u8::try_from(offset % 251).unwrap_or(0); 32]);
                undo.begin_block(BlockId::new(height, hash), hash, digest, digest)?;
                for output_index in 0..OUTPOINTS_PER_BLOCK {
                    let mut transaction_id = [0; 32];
                    transaction_id[..4].copy_from_slice(&offset.to_be_bytes());
                    undo.add_created_outpoint(WalletOutpointKey::new(TransparentOutPoint::new(
                        TransactionId::from_bytes(transaction_id),
                        output_index,
                    )))?;
                    transaction_id[4] = 1;
                    undo.add_spent_outpoint(WalletOutpointKey::new(TransparentOutPoint::new(
                        TransactionId::from_bytes(transaction_id),
                        output_index,
                    )))?;
                }
                undo.finish_block()?;
            }
            Ok::<_, RocksDbWalletError>(())
        };
        let per_block_bytes = u64::try_from(
            size_of::<WalletReorgUndo>()
                + 2 * usize::try_from(OUTPOINTS_PER_BLOCK)? * size_of::<WalletOutpointKey>(),
        )?;
        let suffix_bytes = u64::from(DEPTH) * per_block_bytes;

        let mut undo = RetainedUndo::new(DEPTH, settled_tip, DEFAULT_UNDO_CAP_BYTES);
        fill(&mut undo, DEPTH)?;
        assert_eq!(undo.records.len(), usize::try_from(DEPTH)?);
        assert_eq!(undo.accounted_bytes, suffix_bytes);
        assert_eq!(undo.peak_accounted_bytes, suffix_bytes);
        assert!(suffix_bytes < DEFAULT_UNDO_CAP_BYTES / 64);
        let next_height = BlockHeight::new(settled_tip.height.value() + DEPTH + 1);
        assert!(matches!(
            undo.begin_block(
                BlockId::new(next_height, BlockHash::from_bytes([0xfe; 32])),
                BlockHash::from_bytes([0xfd; 32]),
                digest,
                digest,
            ),
            Err(RocksDbWalletError::WalletProjectionRebuildRequired { .. })
        ));

        // A cap below the complete suffix fails closed instead of growing.
        let mut capped = RetainedUndo::new(DEPTH, settled_tip, suffix_bytes - 1);
        let error = fill(&mut capped, DEPTH)
            .err()
            .ok_or("a suffix above the accounting cap must be refused")?;
        assert!(
            matches!(
                error,
                RocksDbWalletError::AccountedReorgUndoMemoryLimit {
                    limit_bytes,
                    required_bytes,
                } if limit_bytes == suffix_bytes - 1 && required_bytes == suffix_bytes
            ),
            "{error:?}"
        );
        Ok(())
    }

    #[test]
    fn external_loader_accepts_an_authenticated_checkpoint_successor_as_its_first_block()
    -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let options = Options::default();
        let first_retained = BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([0xc2; 32]));
        let tip = BlockId::new(BlockHeight::new(3), BlockHash::from_bytes([0xc3; 32]));
        let blocks = vec![
            block_facts(2, [0xc1; 32], [0xc2; 32], Vec::new()),
            block_facts(3, [0xc2; 32], [0xc3; 32], Vec::new()),
        ];

        let prepared = write_projection_ssts(
            test_config_with_bounds(
                temporary.path(),
                &options,
                TestByteLimits::default(),
                first_retained,
                tip,
            ),
            canonical_scan(validated_replays(&blocks)?),
        )?;

        assert_eq!(prepared.first_block, first_retained);
        assert_eq!(prepared.tip, tip);
        assert_eq!(prepared.counters.scanned_block_count, 2);
        Ok(())
    }

    #[test]
    fn external_loader_rejects_a_first_block_outside_the_authenticated_retained_anchor()
    -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let options = Options::default();
        let expected_first = BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([0xd2; 32]));
        let observed_first = BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([0xc2; 32]));
        let blocks = vec![block_facts(2, [0xc1; 32], [0xc2; 32], Vec::new())];

        let result = write_projection_ssts(
            test_config_with_bounds(
                temporary.path(),
                &options,
                TestByteLimits::default(),
                expected_first,
                observed_first,
            ),
            canonical_scan(validated_replays(&blocks)?),
        );

        assert!(matches!(
            result,
            Err(RocksDbWalletError::Contract(
                WalletProjectionContractError::NonContiguousBlock
            ))
        ));
        Ok(())
    }

    #[test]
    fn external_loader_rejects_duplicate_output_and_spend_keys() -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let options = Options::default();
        let address = TransparentAddressScriptHash::from_bytes([0xa1; 32]);
        let transaction = TransactionId::from_bytes([0xb1; 32]);
        let outpoint = TransparentOutPoint::new(transaction, 0);
        let duplicate_outputs = vec![block_facts(
            1,
            [0; 32],
            [0xc1; 32],
            vec![
                transaction_facts(
                    transaction,
                    Vec::new(),
                    vec![TransparentOutputFact::new(0, 1, [0x51], address)],
                ),
                transaction_facts(
                    transaction,
                    Vec::new(),
                    vec![TransparentOutputFact::new(0, 1, [0x51], address)],
                ),
            ],
        )];
        let duplicate_output_replays = validated_replays(&duplicate_outputs)?;
        let duplicate_output_result = write_projection_ssts(
            test_config(temporary.path(), &options, TestByteLimits::default()),
            canonical_scan(duplicate_output_replays),
        );
        assert!(matches!(
            duplicate_output_result,
            Err(RocksDbWalletError::Contract(
                WalletProjectionContractError::DuplicateOutput
            ))
        ));

        let duplicate_spends = vec![
            block_facts(
                1,
                [0; 32],
                [0xc1; 32],
                vec![transaction_facts(
                    transaction,
                    Vec::new(),
                    vec![TransparentOutputFact::new(0, 1, [0x51], address)],
                )],
            ),
            block_facts(
                2,
                [0xc1; 32],
                [0xc2; 32],
                vec![transaction_facts(
                    TransactionId::from_bytes([0xb2; 32]),
                    vec![
                        TransparentInputFact::new(0, outpoint),
                        TransparentInputFact::new(1, outpoint),
                    ],
                    Vec::new(),
                )],
            ),
        ];
        let duplicate_spend_replays = validated_replays(&duplicate_spends)?;
        let duplicate_spend_result = write_projection_ssts(
            test_config_with_settled_tip(
                temporary.path(),
                &options,
                TestByteLimits::default(),
                BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([0xc2; 32])),
            ),
            canonical_scan(duplicate_spend_replays),
        );
        assert!(matches!(
            duplicate_spend_result,
            Err(RocksDbWalletError::Contract(
                WalletProjectionContractError::DuplicateSpend
            ))
        ));
        Ok(())
    }

    #[test]
    fn external_loader_rejects_missing_and_future_predecessors() -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let options = Options::default();
        let missing = TransparentOutPoint::new(TransactionId::from_bytes([0xee; 32]), 0);
        let missing_blocks = vec![block_facts(
            1,
            [0; 32],
            [0xc1; 32],
            vec![transaction_facts(
                TransactionId::from_bytes([0xb1; 32]),
                vec![TransparentInputFact::new(0, missing)],
                Vec::new(),
            )],
        )];
        assert_missing_predecessor(
            temporary.path(),
            &options,
            validated_replays(&missing_blocks)?,
        );

        let later_transaction = TransactionId::from_bytes([0xb3; 32]);
        let future = TransparentOutPoint::new(later_transaction, 0);
        let future_blocks = vec![block_facts(
            1,
            [0; 32],
            [0xc1; 32],
            vec![
                transaction_facts(
                    TransactionId::from_bytes([0xb2; 32]),
                    vec![TransparentInputFact::new(0, future)],
                    Vec::new(),
                ),
                transaction_facts(
                    later_transaction,
                    Vec::new(),
                    vec![TransparentOutputFact::new(
                        0,
                        1,
                        [0x51],
                        TransparentAddressScriptHash::from_bytes([0xa1; 32]),
                    )],
                ),
            ],
        )];
        assert_missing_predecessor(
            temporary.path(),
            &options,
            validated_replays(&future_blocks)?,
        );
        Ok(())
    }

    #[test]
    fn external_loader_preserves_zero_only_and_mixed_zero_balances() -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let options = Options::default();
        let address = TransparentAddressScriptHash::from_bytes([0xa1; 32]);
        let zero_blocks = vec![block_facts(
            1,
            [0; 32],
            [0xc1; 32],
            vec![transaction_facts(
                TransactionId::from_bytes([0xb1; 32]),
                Vec::new(),
                vec![TransparentOutputFact::new(0, 0, [0x51], address)],
            )],
        )];
        let zero = write_projection_ssts(
            test_config(temporary.path(), &options, TestByteLimits::default()),
            canonical_scan(validated_replays(&zero_blocks)?),
        )?;
        let zero_oracle = oracle_for(&zero_blocks)?;
        assert_eq!(zero.row_counts, zero_oracle.row_counts());
        assert_eq!(zero.utxo_summary, zero_oracle.utxo_summary());
        assert_eq!(zero.projection_digest, zero_oracle.projection_digest()?);
        assert_eq!(
            zero.projection_accumulator,
            zero_oracle.projection_accumulator()?
        );
        assert_eq!(zero.row_counts.transparent_unspent_output_count, 1);
        assert_eq!(zero.row_counts.transparent_address_balance_count, 0);

        let mixed_blocks = vec![block_facts(
            1,
            [0; 32],
            [0xd1; 32],
            vec![transaction_facts(
                TransactionId::from_bytes([0xb2; 32]),
                Vec::new(),
                vec![
                    TransparentOutputFact::new(0, 0, [0x51], address),
                    TransparentOutputFact::new(1, 7, [0x52], address),
                ],
            )],
        )];
        let mixed_tip = BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([0xd1; 32]));
        let mixed = write_projection_ssts(
            test_config_with_bounds(
                temporary.path(),
                &options,
                TestByteLimits::default(),
                mixed_tip,
                mixed_tip,
            ),
            canonical_scan(validated_replays(&mixed_blocks)?),
        )?;
        let mixed_oracle = oracle_for(&mixed_blocks)?;
        assert_eq!(mixed.row_counts, mixed_oracle.row_counts());
        assert_eq!(mixed.utxo_summary, mixed_oracle.utxo_summary());
        assert_eq!(mixed.projection_digest, mixed_oracle.projection_digest()?);
        assert_eq!(
            mixed.projection_accumulator,
            mixed_oracle.projection_accumulator()?
        );
        assert_eq!(mixed.row_counts.transparent_unspent_output_count, 2);
        assert_eq!(mixed.row_counts.transparent_address_balance_count, 1);
        assert_eq!(mixed.utxo_summary.total_value_zat, 7);
        Ok(())
    }

    #[test]
    fn external_loader_merges_multiple_outpoint_runs() -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let options = Options::default();
        let address = TransparentAddressScriptHash::from_bytes([0xa1; 32]);
        let blocks = vec![block_facts(
            1,
            [0; 32],
            [0xc1; 32],
            vec![transaction_facts(
                TransactionId::from_bytes([0xb1; 32]),
                Vec::new(),
                vec![
                    TransparentOutputFact::new(0, 1, [0x51], address),
                    TransparentOutputFact::new(1, 2, [0x52], address),
                    TransparentOutputFact::new(2, 3, [0x53], address),
                ],
            )],
        )];
        let prepared = write_projection_ssts(
            test_config(
                temporary.path(),
                &options,
                TestByteLimits {
                    outpoint_sort: 160,
                    ..TestByteLimits::default()
                },
            ),
            canonical_scan(validated_replays(&blocks)?),
        )?;

        assert!(prepared.outpoint_sort_evidence.initial_run_count > 1);
        assert!(prepared.outpoint_sort_evidence.merge_pass_count > 0);
        assert_eq!(prepared.row_counts.transparent_unspent_output_count, 3);
        assert_eq!(prepared.utxo_summary.total_value_zat, 6);
        Ok(())
    }

    #[test]
    fn external_loader_refuses_memory_and_temporary_file_limit_crossings()
    -> Result<(), Box<dyn Error>> {
        let temporary = TempDir::new()?;
        let options = Options::default();
        let blocks = vec![block_facts(
            1,
            [0; 32],
            [0xc1; 32],
            vec![transaction_facts(
                TransactionId::from_bytes([0xb1; 32]),
                Vec::new(),
                vec![TransparentOutputFact::new(
                    0,
                    1,
                    [0x51],
                    TransparentAddressScriptHash::from_bytes([0xa1; 32]),
                )],
            )],
        )];
        let memory_result = write_projection_ssts(
            test_config(
                temporary.path(),
                &options,
                TestByteLimits {
                    outpoint_sort: 1,
                    ..TestByteLimits::default()
                },
            ),
            canonical_scan(validated_replays(&blocks)?),
        );
        assert!(matches!(
            memory_result,
            Err(RocksDbWalletError::BulkLoad(
                BulkLoadError::AccountedMemoryLimit {
                    limit_bytes: 1,
                    required_bytes: _
                }
            ))
        ));

        let disk_result = write_projection_ssts(
            test_config(
                temporary.path(),
                &options,
                TestByteLimits {
                    temporary_files_per_sorter: 1,
                    ..TestByteLimits::default()
                },
            ),
            canonical_scan(validated_replays(&blocks)?),
        );
        assert!(matches!(
            disk_result,
            Err(RocksDbWalletError::BulkLoad(
                BulkLoadError::TemporaryFileLimit {
                    limit_bytes: 1,
                    required_bytes: _
                }
            ))
        ));
        Ok(())
    }

    fn test_config<'options>(
        staging_path: &'options Path,
        options: &'options Options,
        limits: TestByteLimits,
    ) -> WalletProjectionLoadConfig<'options> {
        test_config_with_settled_tip(
            staging_path,
            options,
            limits,
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([0xc1; 32])),
        )
    }

    fn test_config_with_settled_tip<'options>(
        staging_path: &'options Path,
        options: &'options Options,
        limits: TestByteLimits,
        settled_tip: BlockId,
    ) -> WalletProjectionLoadConfig<'options> {
        test_config_with_bounds(
            staging_path,
            options,
            limits,
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([0xc1; 32])),
            settled_tip,
        )
    }

    fn test_config_with_bounds<'options>(
        staging_path: &'options Path,
        options: &'options Options,
        limits: TestByteLimits,
        first_retained_block: BlockId,
        settled_tip: BlockId,
    ) -> WalletProjectionLoadConfig<'options> {
        WalletProjectionLoadConfig {
            staging_path,
            options,
            network: Network::ZcashRegtest,
            first_retained_block,
            settled_tip,
            supported_reorg_depth: 0,
            max_outpoint_sort_memory_bytes: limits.outpoint_sort,
            max_secondary_sort_memory_bytes_per_sorter: limits.secondary_sort_per_sorter,
            max_temporary_file_bytes_per_sorter: limits.temporary_files_per_sorter,
            sst_target_logical_bytes: TEST_SST_LOGICAL_BYTES,
            max_accounted_reorg_undo_bytes: TEST_REORG_UNDO_BYTES,
        }
    }

    fn canonical_scan(
        replays: Vec<ValidatedCanonicalBlockReplay>,
    ) -> impl Iterator<Item = Result<ValidatedCanonicalBlockReplay, CanonicalStoreError>> {
        replays.into_iter().map(Ok)
    }

    fn validated_replays(
        blocks: &[CanonicalBlockFacts],
    ) -> Result<Vec<ValidatedCanonicalBlockReplay>, Box<dyn Error>> {
        blocks
            .iter()
            .map(|facts| {
                let envelope = encode_canonical_block_replay(
                    facts,
                    CanonicalBlockReplayFormatVersion::V1,
                    CanonicalBlockFactsDigestVersion::V1,
                );
                decode_canonical_block_replay(envelope.as_bytes())
                    .map_err(|error| Box::new(error) as Box<dyn Error>)
            })
            .collect()
    }

    fn oracle_for(
        blocks: &[CanonicalBlockFacts],
    ) -> Result<WalletProjectionSerialOracle, WalletProjectionContractError> {
        let mut oracle = WalletProjectionSerialOracle::new(Network::ZcashRegtest);
        for block in blocks {
            oracle.apply_block(block)?;
        }
        Ok(oracle)
    }

    fn assert_missing_predecessor(
        staging_path: &Path,
        options: &Options,
        replays: Vec<ValidatedCanonicalBlockReplay>,
    ) {
        assert!(matches!(
            write_projection_ssts(
                test_config(staging_path, options, TestByteLimits::default()),
                canonical_scan(replays),
            ),
            Err(RocksDbWalletError::Contract(
                WalletProjectionContractError::MissingTransparentPredecessor
            ))
        ));
    }

    fn block_facts(
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
                i64::from(height),
                0,
                [0; 32],
                0,
                0,
            ),
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&block_hash),
            transactions,
        }
    }

    fn transaction_facts(
        transaction_id: TransactionId,
        transparent_inputs: Vec<TransparentInputFact>,
        transparent_outputs: Vec<TransparentOutputFact>,
    ) -> CanonicalTransactionFacts {
        let counts = TransactionComponentCounts {
            transparent_input_count: u32::try_from(transparent_inputs.len()).unwrap_or(u32::MAX),
            transparent_output_count: u32::try_from(transparent_outputs.len()).unwrap_or(u32::MAX),
            ..TransactionComponentCounts::EMPTY
        };
        CanonicalTransactionFacts {
            public_facts: TransactionPublicFacts {
                transaction_id,
                auth_digest: None,
                wtxid: None,
                version: TransactionVersion::V4,
                consensus_branch_id: None,
                lock_time: LockTime::Unlocked,
                expiry_height: None,
                size_bytes: 32,
                counts,
                orchard_value_balance_zat: None,
                orchard_anchor: None,
                ironwood_value_balance_zat: None,
                privacy_shape: PrivacyShape::Unclassified,
                is_coinbase: false,
                unsupported_sections: Vec::new(),
            },
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(
                &transaction_id.as_bytes(),
            ),
            intrinsic_value_balances: TransactionIntrinsicValueBalances::default(),
            transparent_inputs,
            transparent_outputs,
        }
    }
}
