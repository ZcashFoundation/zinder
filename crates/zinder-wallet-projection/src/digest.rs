//! Order-independent, deletion-capable wallet projection row accumulator.

use blake2b_simd::Params;
use blake2b_simd::many::{HashManyJob, hash_many};

use crate::contract_error::encoded_len;
use crate::{
    WALLET_PROJECTION_VALUE_ENCODING_VERSION, WalletProjectionContractError,
    WalletProjectionDigest, WalletProjectionFamilyRowCounts,
};

/// Version of the full wallet-row accumulator durable encoding.
pub const WALLET_PROJECTION_ACCUMULATOR_VERSION: u16 = 1;

/// Number of `u16` lanes in the wallet-row `LtHash16` accumulator.
const ACCUMULATOR_LANE_COUNT: usize = 1024;

/// Length in bytes of the full wallet-row accumulator.
pub const WALLET_PROJECTION_ACCUMULATOR_LEN: usize = ACCUMULATOR_LANE_COUNT * 2;

/// Length in bytes of the derived display digest.
const DISPLAY_DIGEST_LEN: usize = 32;

/// Fixed BLAKE2X personalization for wallet projection rows.
const PROJECTION_ACCUMULATOR_PERSONAL: [u8; 16] = *b"ZinderWalletProj";

/// Domain-separated preimage prefix for one durable wallet row.
const PROJECTION_ROW_DOMAIN: &[u8] = b"zinder:wallet-projection:row:v2\0";

/// A `BLAKE2b` output block is 64 bytes.
const BLAKE2B_OUTPUT_LEN: usize = 64;

/// A `BLAKE2b` output block is 64 bytes in the XOF parameter encoding.
const BLAKE2B_OUTPUT_LEN_U32: u32 = 64;

/// Full accumulator length in the BLAKE2X parameter encoding.
const WALLET_PROJECTION_ACCUMULATOR_LEN_U32: u32 = 2048;

/// Number of 64-byte BLAKE2X blocks in one row expansion.
const EXPANSION_BLOCK_COUNT: usize = WALLET_PROJECTION_ACCUMULATOR_LEN / BLAKE2B_OUTPUT_LEN;

/// Durable row families committed by the wallet projection accumulator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum WalletProjectionRowFamily {
    /// Primary rows for currently unspent outputs.
    TransparentUnspentOutput = 1,
    /// Address-ordered secondary keys for unspent outputs.
    TransparentUnspentOutputByAddress = 2,
    /// Historical outputs paired with their consuming inputs.
    TransparentSpentOutput = 3,
    /// Address-ordered transactions touching transparent state.
    TransparentAddressTransaction = 4,
    /// Current non-zero transparent address balances.
    TransparentAddressBalance = 5,
    /// Bounded inverse deltas used for canonical reorgs.
    ReorgUndo = 6,
}

impl WalletProjectionRowFamily {
    const fn tag(self) -> u8 {
        self as u8
    }
}

/// Full, order-independent `LtHash16` accumulator over every durable row.
///
/// Each row expands through `BLAKE2X` into 1024 little-endian `u16` lanes. The
/// accumulator adds or subtracts those lanes modulo `2^16`, so an insert and
/// exact delete are inverse operations without rescanning historic rows. The
/// full 2048-byte state is durable; [`Self::display_digest`] derives the
/// compact 32-byte value exposed by readiness evidence.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct WalletProjectionAccumulator(Box<[u8; WALLET_PROJECTION_ACCUMULATOR_LEN]>);

impl Default for WalletProjectionAccumulator {
    fn default() -> Self {
        Self::empty()
    }
}

impl WalletProjectionAccumulator {
    /// Returns the all-zero accumulator for an empty wallet projection.
    #[must_use]
    pub fn empty() -> Self {
        Self(Box::new([0; WALLET_PROJECTION_ACCUMULATOR_LEN]))
    }

    /// Reconstructs an exact durable full accumulator.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, WalletProjectionContractError> {
        let accumulator = bytes.try_into().map_err(|_| {
            WalletProjectionContractError::ProjectionAccumulatorLengthMismatch {
                expected: WALLET_PROJECTION_ACCUMULATOR_LEN,
                actual: bytes.len(),
            }
        })?;
        Ok(Self(Box::new(accumulator)))
    }

    /// Returns the exact full durable accumulator bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; WALLET_PROJECTION_ACCUMULATOR_LEN] {
        &self.0
    }

    /// Returns the compact BLAKE2b-256 display digest of the full accumulator.
    #[must_use]
    pub fn display_digest(&self) -> WalletProjectionDigest {
        let hash = Params::new()
            .hash_length(DISPLAY_DIGEST_LEN)
            .hash(self.0.as_slice());
        let mut digest = [0; DISPLAY_DIGEST_LEN];
        digest.copy_from_slice(hash.as_bytes());
        WalletProjectionDigest::from_bytes(digest)
    }

    fn add_accumulator(&mut self, other: &Self) {
        self.fold_lanes(&other.0, u16::wrapping_add);
    }

    fn fold_lanes(
        &mut self,
        lanes: &[u8; WALLET_PROJECTION_ACCUMULATOR_LEN],
        fold: impl Fn(u16, u16) -> u16,
    ) {
        let (accumulated_lanes, _) = self.0.as_chunks_mut::<2>();
        let (other_lanes, _) = lanes.as_chunks::<2>();
        for (accumulated, other) in accumulated_lanes.iter_mut().zip(other_lanes) {
            *accumulated =
                fold(u16::from_le_bytes(*accumulated), u16::from_le_bytes(*other)).to_le_bytes();
        }
    }
}

/// Incremental row accumulator and exact family counts.
///
/// The builder accepts rows in any order. Its removal operation is intended
/// for a row that was read and validated from durable storage; family-count
/// underflow is rejected before any caller can publish an invalid READY fence.
pub struct WalletProjectionDigestBuilder {
    accumulator: WalletProjectionAccumulator,
    row_counts: WalletProjectionFamilyRowCounts,
}

impl WalletProjectionDigestBuilder {
    /// Starts one empty wallet projection accumulator.
    #[must_use]
    pub fn new() -> Self {
        Self {
            accumulator: WalletProjectionAccumulator::empty(),
            row_counts: WalletProjectionFamilyRowCounts::default(),
        }
    }

    /// Resumes incremental accumulation from durable READY evidence.
    #[must_use]
    pub const fn from_parts(
        accumulator: WalletProjectionAccumulator,
        row_counts: WalletProjectionFamilyRowCounts,
    ) -> Self {
        Self {
            accumulator,
            row_counts,
        }
    }

    /// Adds one durable row to the full accumulator and family count.
    pub fn append_row(
        &mut self,
        family: WalletProjectionRowFamily,
        key: &[u8],
        encoded_value: &[u8],
    ) -> Result<(), WalletProjectionContractError> {
        // Construct the exact input and validate the counter before touching
        // any lane. Callers use this property to abandon a rejected atomic
        // transition without reconstructing the accumulator from history.
        let preimage = row_preimage(family, key, encoded_value)?;
        let next_count = self
            .row_count(family)
            .checked_add(1)
            .ok_or(WalletProjectionContractError::ProjectionDigestRowCountOverflow)?;
        self.accumulator
            .fold_lanes(&blake2x_expand(&preimage), u16::wrapping_add);
        *self.row_count_mut(family) = next_count;
        Ok(())
    }

    /// Removes one exact durable row from the full accumulator and family count.
    pub fn remove_row(
        &mut self,
        family: WalletProjectionRowFamily,
        key: &[u8],
        encoded_value: &[u8],
    ) -> Result<(), WalletProjectionContractError> {
        // As above, make every rejecting condition observable before the
        // inverse lane operation or count mutation.
        let preimage = row_preimage(family, key, encoded_value)?;
        let next_count = self
            .row_count(family)
            .checked_sub(1)
            .ok_or(WalletProjectionContractError::ProjectionDigestRowCountUnderflow)?;
        self.accumulator
            .fold_lanes(&blake2x_expand(&preimage), u16::wrapping_sub);
        *self.row_count_mut(family) = next_count;
        Ok(())
    }

    /// Folds the rows accumulated by `other` into this builder.
    ///
    /// The accumulator is order independent, so independent readers may each
    /// accumulate a disjoint subset of the durable rows and combine the results
    /// without changing the final accumulator or counts. A row counter overflow
    /// is rejected before either the lanes or the counts change.
    pub fn combine(&mut self, other: &Self) -> Result<(), WalletProjectionContractError> {
        let combined_counts = WalletProjectionFamilyRowCounts {
            transparent_unspent_output_count: combine_count(
                self.row_counts.transparent_unspent_output_count,
                other.row_counts.transparent_unspent_output_count,
            )?,
            transparent_unspent_output_by_address_count: combine_count(
                self.row_counts.transparent_unspent_output_by_address_count,
                other.row_counts.transparent_unspent_output_by_address_count,
            )?,
            transparent_spent_output_count: combine_count(
                self.row_counts.transparent_spent_output_count,
                other.row_counts.transparent_spent_output_count,
            )?,
            transparent_address_transaction_count: combine_count(
                self.row_counts.transparent_address_transaction_count,
                other.row_counts.transparent_address_transaction_count,
            )?,
            transparent_address_balance_count: combine_count(
                self.row_counts.transparent_address_balance_count,
                other.row_counts.transparent_address_balance_count,
            )?,
            reorg_undo_count: combine_count(
                self.row_counts.reorg_undo_count,
                other.row_counts.reorg_undo_count,
            )?,
        };
        self.accumulator.add_accumulator(&other.accumulator);
        self.row_counts = combined_counts;
        Ok(())
    }

    /// Returns the row counts observed by all six family accumulators.
    #[must_use]
    pub const fn row_counts(&self) -> WalletProjectionFamilyRowCounts {
        self.row_counts
    }

    /// Returns the full accumulated state without consuming the builder.
    #[must_use]
    pub fn accumulator(&self) -> &WalletProjectionAccumulator {
        &self.accumulator
    }

    /// Returns the compact display digest derived from the full accumulator.
    #[must_use]
    pub fn finish(self) -> WalletProjectionDigest {
        self.accumulator.display_digest()
    }

    /// Returns the full accumulator and its derived display digest together.
    #[must_use]
    pub fn finish_with_accumulator(self) -> (WalletProjectionAccumulator, WalletProjectionDigest) {
        let digest = self.accumulator.display_digest();
        (self.accumulator, digest)
    }

    fn row_count_mut(&mut self, family: WalletProjectionRowFamily) -> &mut u64 {
        match family {
            WalletProjectionRowFamily::TransparentUnspentOutput => {
                &mut self.row_counts.transparent_unspent_output_count
            }
            WalletProjectionRowFamily::TransparentUnspentOutputByAddress => {
                &mut self.row_counts.transparent_unspent_output_by_address_count
            }
            WalletProjectionRowFamily::TransparentSpentOutput => {
                &mut self.row_counts.transparent_spent_output_count
            }
            WalletProjectionRowFamily::TransparentAddressTransaction => {
                &mut self.row_counts.transparent_address_transaction_count
            }
            WalletProjectionRowFamily::TransparentAddressBalance => {
                &mut self.row_counts.transparent_address_balance_count
            }
            WalletProjectionRowFamily::ReorgUndo => &mut self.row_counts.reorg_undo_count,
        }
    }

    fn row_count(&self, family: WalletProjectionRowFamily) -> u64 {
        match family {
            WalletProjectionRowFamily::TransparentUnspentOutput => {
                self.row_counts.transparent_unspent_output_count
            }
            WalletProjectionRowFamily::TransparentUnspentOutputByAddress => {
                self.row_counts.transparent_unspent_output_by_address_count
            }
            WalletProjectionRowFamily::TransparentSpentOutput => {
                self.row_counts.transparent_spent_output_count
            }
            WalletProjectionRowFamily::TransparentAddressTransaction => {
                self.row_counts.transparent_address_transaction_count
            }
            WalletProjectionRowFamily::TransparentAddressBalance => {
                self.row_counts.transparent_address_balance_count
            }
            WalletProjectionRowFamily::ReorgUndo => self.row_counts.reorg_undo_count,
        }
    }
}

fn combine_count(left: u64, right: u64) -> Result<u64, WalletProjectionContractError> {
    left.checked_add(right)
        .ok_or(WalletProjectionContractError::ProjectionDigestRowCountOverflow)
}

impl Default for WalletProjectionDigestBuilder {
    fn default() -> Self {
        Self::new()
    }
}

fn row_preimage(
    family: WalletProjectionRowFamily,
    key: &[u8],
    encoded_value: &[u8],
) -> Result<Vec<u8>, WalletProjectionContractError> {
    let key_len = encoded_len(key.len(), "projection accumulator key")?;
    let value_len = encoded_len(encoded_value.len(), "projection accumulator value")?;
    let mut preimage = Vec::new();
    preimage.extend_from_slice(PROJECTION_ROW_DOMAIN);
    preimage.extend_from_slice(&WALLET_PROJECTION_ACCUMULATOR_VERSION.to_be_bytes());
    preimage.extend_from_slice(&WALLET_PROJECTION_VALUE_ENCODING_VERSION.to_be_bytes());
    preimage.push(family.tag());
    preimage.extend_from_slice(&key_len.to_be_bytes());
    preimage.extend_from_slice(key);
    preimage.extend_from_slice(&value_len.to_be_bytes());
    preimage.extend_from_slice(encoded_value);
    Ok(preimage)
}

fn blake2x_expand(message: &[u8]) -> [u8; WALLET_PROJECTION_ACCUMULATOR_LEN] {
    let xof_length = WALLET_PROJECTION_ACCUMULATOR_LEN_U32;
    let root = Params::new()
        .hash_length(BLAKE2B_OUTPUT_LEN)
        .node_offset(u64::from(xof_length) << 32)
        .personal(&PROJECTION_ACCUMULATOR_PERSONAL)
        .hash(message);

    // Each expansion block hashes the same root under its own node offset, so
    // the blocks are independent and hash together as one SIMD batch.
    let mut blocks: [HashManyJob<'_>; EXPANSION_BLOCK_COUNT] =
        core::array::from_fn(|block_index| {
            let block_index = u64::try_from(block_index).unwrap_or(u64::MAX);
            HashManyJob::new(
                Params::new()
                    .hash_length(BLAKE2B_OUTPUT_LEN)
                    .fanout(0)
                    .max_depth(0)
                    .max_leaf_length(BLAKE2B_OUTPUT_LEN_U32)
                    .node_offset(block_index | (u64::from(xof_length) << 32))
                    .inner_hash_length(BLAKE2B_OUTPUT_LEN)
                    .personal(&PROJECTION_ACCUMULATOR_PERSONAL),
                root.as_bytes(),
            )
        });
    hash_many(blocks.iter_mut());

    let mut output = [0; WALLET_PROJECTION_ACCUMULATOR_LEN];
    for (output_block, block) in output.chunks_exact_mut(BLAKE2B_OUTPUT_LEN).zip(&blocks) {
        output_block.copy_from_slice(block.to_hash().as_bytes());
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accumulator_accepts_interleaved_families_and_counts_rows() {
        let mut digest = WalletProjectionDigestBuilder::new();
        digest
            .append_row(
                WalletProjectionRowFamily::TransparentSpentOutput,
                b"a",
                b"spent",
            )
            .unwrap_or_else(|error| unreachable!("valid spent row: {error}"));
        digest
            .append_row(
                WalletProjectionRowFamily::TransparentUnspentOutput,
                b"a",
                b"one",
            )
            .unwrap_or_else(|error| unreachable!("valid first unspent row: {error}"));
        digest
            .append_row(
                WalletProjectionRowFamily::TransparentAddressBalance,
                b"address",
                b"balance",
            )
            .unwrap_or_else(|error| unreachable!("valid balance row: {error}"));
        digest
            .append_row(
                WalletProjectionRowFamily::TransparentUnspentOutput,
                b"b",
                b"two",
            )
            .unwrap_or_else(|error| unreachable!("valid second unspent row: {error}"));

        assert_eq!(
            digest.row_counts(),
            WalletProjectionFamilyRowCounts {
                transparent_unspent_output_count: 2,
                transparent_unspent_output_by_address_count: 0,
                transparent_spent_output_count: 1,
                transparent_address_transaction_count: 0,
                transparent_address_balance_count: 1,
                reorg_undo_count: 0,
            }
        );
    }

    #[test]
    fn accumulator_is_independent_of_row_order_within_a_family() {
        let mut forward = WalletProjectionDigestBuilder::new();
        forward
            .append_row(
                WalletProjectionRowFamily::TransparentUnspentOutput,
                b"a",
                b"one",
            )
            .unwrap_or_else(|error| unreachable!("valid first row: {error}"));
        forward
            .append_row(
                WalletProjectionRowFamily::TransparentUnspentOutput,
                b"b",
                b"two",
            )
            .unwrap_or_else(|error| unreachable!("valid second row: {error}"));

        let mut reverse = WalletProjectionDigestBuilder::new();
        reverse
            .append_row(
                WalletProjectionRowFamily::TransparentUnspentOutput,
                b"b",
                b"two",
            )
            .unwrap_or_else(|error| unreachable!("valid second row: {error}"));
        reverse
            .append_row(
                WalletProjectionRowFamily::TransparentUnspentOutput,
                b"a",
                b"one",
            )
            .unwrap_or_else(|error| unreachable!("valid first row: {error}"));

        assert_eq!(forward.row_counts(), reverse.row_counts());
        assert_eq!(forward.finish(), reverse.finish());
    }

    #[test]
    fn combined_partial_builders_equal_one_sequential_builder() {
        let rows: [(WalletProjectionRowFamily, &[u8], &[u8]); 4] = [
            (
                WalletProjectionRowFamily::TransparentUnspentOutput,
                b"a",
                b"one",
            ),
            (
                WalletProjectionRowFamily::TransparentAddressTransaction,
                b"b",
                b"two",
            ),
            (
                WalletProjectionRowFamily::TransparentAddressBalance,
                b"c",
                b"three",
            ),
            (WalletProjectionRowFamily::ReorgUndo, b"d", b"four"),
        ];
        let mut sequential = WalletProjectionDigestBuilder::new();
        let mut first = WalletProjectionDigestBuilder::new();
        let mut second = WalletProjectionDigestBuilder::new();
        for (index, (family, key, value)) in rows.into_iter().enumerate() {
            sequential
                .append_row(family, key, value)
                .unwrap_or_else(|error| unreachable!("valid sequential row: {error}"));
            let partial = if index % 2 == 0 {
                &mut first
            } else {
                &mut second
            };
            partial
                .append_row(family, key, value)
                .unwrap_or_else(|error| unreachable!("valid partial row: {error}"));
        }

        first
            .combine(&second)
            .unwrap_or_else(|error| unreachable!("disjoint partial counts fit: {error}"));

        assert_eq!(first.row_counts(), sequential.row_counts());
        assert_eq!(first.accumulator(), sequential.accumulator());
    }

    #[test]
    fn combine_rejects_row_count_overflow_without_changing_the_builder() {
        let saturated = WalletProjectionFamilyRowCounts {
            reorg_undo_count: u64::MAX,
            ..WalletProjectionFamilyRowCounts::default()
        };
        let mut target = WalletProjectionDigestBuilder::from_parts(
            WalletProjectionAccumulator::empty(),
            saturated,
        );
        let mut other = WalletProjectionDigestBuilder::new();
        other
            .append_row(
                WalletProjectionRowFamily::TransparentUnspentOutput,
                b"a",
                b"one",
            )
            .unwrap_or_else(|error| unreachable!("valid row: {error}"));
        other
            .append_row(WalletProjectionRowFamily::ReorgUndo, b"b", b"two")
            .unwrap_or_else(|error| unreachable!("valid undo row: {error}"));

        assert!(matches!(
            target.combine(&other),
            Err(WalletProjectionContractError::ProjectionDigestRowCountOverflow)
        ));
        assert_eq!(target.row_counts(), saturated);
        assert_eq!(target.accumulator(), &WalletProjectionAccumulator::empty());
    }

    #[test]
    fn digest_is_independent_of_family_interleaving() {
        let mut family_order = WalletProjectionDigestBuilder::new();
        family_order
            .append_row(
                WalletProjectionRowFamily::TransparentUnspentOutput,
                b"a",
                b"one",
            )
            .unwrap_or_else(|error| unreachable!("valid unspent row: {error}"));
        family_order
            .append_row(
                WalletProjectionRowFamily::TransparentSpentOutput,
                b"b",
                b"two",
            )
            .unwrap_or_else(|error| unreachable!("valid spent row: {error}"));

        let mut reverse_order = WalletProjectionDigestBuilder::new();
        reverse_order
            .append_row(
                WalletProjectionRowFamily::TransparentSpentOutput,
                b"b",
                b"two",
            )
            .unwrap_or_else(|error| unreachable!("valid spent row: {error}"));
        reverse_order
            .append_row(
                WalletProjectionRowFamily::TransparentUnspentOutput,
                b"a",
                b"one",
            )
            .unwrap_or_else(|error| unreachable!("valid unspent row: {error}"));

        assert_eq!(family_order.finish(), reverse_order.finish());
    }

    #[test]
    fn accumulator_delete_restores_the_prior_full_state() {
        let mut accumulator = WalletProjectionDigestBuilder::new();
        accumulator
            .append_row(
                WalletProjectionRowFamily::TransparentSpentOutput,
                b"outpoint",
                b"spent-row",
            )
            .unwrap_or_else(|error| unreachable!("valid accumulator insertion: {error}"));
        let populated = accumulator.accumulator().clone();
        accumulator
            .remove_row(
                WalletProjectionRowFamily::TransparentSpentOutput,
                b"outpoint",
                b"spent-row",
            )
            .unwrap_or_else(|error| unreachable!("exact accumulator removal: {error}"));

        assert_ne!(populated, *accumulator.accumulator());
        assert_eq!(
            accumulator.accumulator(),
            &WalletProjectionAccumulator::empty()
        );
        assert_eq!(
            accumulator.remove_row(
                WalletProjectionRowFamily::TransparentSpentOutput,
                b"outpoint",
                b"spent-row",
            ),
            Err(WalletProjectionContractError::ProjectionDigestRowCountUnderflow)
        );
    }

    #[test]
    fn rejected_count_changes_leave_the_accumulator_and_counts_unchanged() {
        let mut overflow = WalletProjectionDigestBuilder::from_parts(
            WalletProjectionAccumulator::empty(),
            WalletProjectionFamilyRowCounts {
                transparent_unspent_output_count: u64::MAX,
                ..WalletProjectionFamilyRowCounts::default()
            },
        );
        let overflow_accumulator = overflow.accumulator().clone();
        let overflow_counts = overflow.row_counts();
        assert_eq!(
            overflow.append_row(
                WalletProjectionRowFamily::TransparentUnspentOutput,
                b"outpoint",
                b"row",
            ),
            Err(WalletProjectionContractError::ProjectionDigestRowCountOverflow)
        );
        assert_eq!(overflow.accumulator(), &overflow_accumulator);
        assert_eq!(overflow.row_counts(), overflow_counts);

        let mut underflow = WalletProjectionDigestBuilder::new();
        let underflow_accumulator = underflow.accumulator().clone();
        let underflow_counts = underflow.row_counts();
        assert_eq!(
            underflow.remove_row(
                WalletProjectionRowFamily::TransparentUnspentOutput,
                b"outpoint",
                b"row",
            ),
            Err(WalletProjectionContractError::ProjectionDigestRowCountUnderflow)
        );
        assert_eq!(underflow.accumulator(), &underflow_accumulator);
        assert_eq!(underflow.row_counts(), underflow_counts);
    }

    /// Serial BLAKE2X expansion that every faster path must reproduce byte
    /// for byte: one `Params::hash` call per 64-byte block, in block order.
    fn serial_reference_expand(message: &[u8]) -> [u8; WALLET_PROJECTION_ACCUMULATOR_LEN] {
        let mut output = [0; WALLET_PROJECTION_ACCUMULATOR_LEN];
        let xof_length = WALLET_PROJECTION_ACCUMULATOR_LEN_U32;
        let root = Params::new()
            .hash_length(BLAKE2B_OUTPUT_LEN)
            .node_offset(u64::from(xof_length) << 32)
            .personal(&PROJECTION_ACCUMULATOR_PERSONAL)
            .hash(message);

        let mut produced = 0usize;
        let mut block_index = 0u32;
        while produced < output.len() {
            let block_length = (output.len() - produced).min(BLAKE2B_OUTPUT_LEN);
            let block = Params::new()
                .hash_length(block_length)
                .fanout(0)
                .max_depth(0)
                .max_leaf_length(BLAKE2B_OUTPUT_LEN_U32)
                .node_offset(u64::from(block_index) | (u64::from(xof_length) << 32))
                .inner_hash_length(BLAKE2B_OUTPUT_LEN)
                .personal(&PROJECTION_ACCUMULATOR_PERSONAL)
                .hash(root.as_bytes());
            output[produced..produced + block_length].copy_from_slice(block.as_bytes());
            produced += block_length;
            block_index = block_index.wrapping_add(1);
        }
        output
    }

    /// Deterministic bytes of `len`, distinct for each `seed`.
    fn patterned_bytes(len: usize, seed: u8) -> Vec<u8> {
        (0..=u8::MAX)
            .cycle()
            .take(len)
            .map(|byte| byte ^ seed)
            .collect()
    }

    /// BLAKE2b-256 fingerprint of a full 2048-byte expansion.
    fn expansion_fingerprint(expansion: &[u8; WALLET_PROJECTION_ACCUMULATOR_LEN]) -> String {
        hex::encode(Params::new().hash_length(32).hash(expansion).as_bytes())
    }

    /// Row sequence over all six families that mixes inserts with exact
    /// removals. Every family keeps at least one row at the end, so the final
    /// accumulator depends on each family tag and on both lane folds.
    fn multi_family_builder() -> Result<WalletProjectionDigestBuilder, WalletProjectionContractError>
    {
        use WalletProjectionRowFamily::{
            ReorgUndo, TransparentAddressBalance, TransparentAddressTransaction,
            TransparentSpentOutput, TransparentUnspentOutput, TransparentUnspentOutputByAddress,
        };

        let address = [0xa1; 32];
        let spent_outpoint = patterned_bytes(36, 0x11);
        let spent_by_address = patterned_bytes(72, 0x12);
        let spent_unspent_value = patterned_bytes(109, 0x13);
        let live_outpoint = patterned_bytes(36, 0x21);
        let live_by_address = patterned_bytes(72, 0x22);
        let live_unspent_value = patterned_bytes(109, 0x23);
        let pruned_undo_key = 41u32.to_be_bytes();
        let live_undo_key = 42u32.to_be_bytes();

        let mut builder = WalletProjectionDigestBuilder::new();
        builder.append_row(TransparentAddressBalance, &address, &5u64.to_be_bytes())?;
        builder.remove_row(TransparentAddressBalance, &address, &5u64.to_be_bytes())?;
        builder.append_row(TransparentAddressBalance, &address, &9u64.to_be_bytes())?;

        builder.append_row(
            TransparentUnspentOutput,
            &spent_outpoint,
            &spent_unspent_value,
        )?;
        builder.append_row(TransparentUnspentOutputByAddress, &spent_by_address, &[])?;
        builder.remove_row(
            TransparentUnspentOutput,
            &spent_outpoint,
            &spent_unspent_value,
        )?;
        builder.remove_row(TransparentUnspentOutputByAddress, &spent_by_address, &[])?;

        builder.append_row(
            TransparentSpentOutput,
            &spent_outpoint,
            &patterned_bytes(185, 0x14),
        )?;
        builder.append_row(
            TransparentAddressTransaction,
            &patterned_bytes(40, 0x15),
            &patterned_bytes(64, 0x16),
        )?;
        builder.append_row(ReorgUndo, &pruned_undo_key, &patterned_bytes(236, 0x17))?;
        builder.remove_row(ReorgUndo, &pruned_undo_key, &patterned_bytes(236, 0x17))?;

        builder.append_row(
            TransparentUnspentOutput,
            &live_outpoint,
            &live_unspent_value,
        )?;
        builder.append_row(TransparentUnspentOutputByAddress, &live_by_address, &[])?;
        builder.append_row(ReorgUndo, &live_undo_key, &patterned_bytes(196, 0x27))?;
        Ok(builder)
    }

    #[test]
    fn multi_family_sequence_keeps_its_pinned_accumulator()
    -> Result<(), WalletProjectionContractError> {
        let builder = multi_family_builder()?;
        assert_eq!(
            builder.row_counts(),
            WalletProjectionFamilyRowCounts {
                transparent_unspent_output_count: 1,
                transparent_unspent_output_by_address_count: 1,
                transparent_spent_output_count: 1,
                transparent_address_transaction_count: 1,
                transparent_address_balance_count: 1,
                reorg_undo_count: 1,
            }
        );
        let lanes = builder.accumulator().as_bytes();
        assert_eq!(
            hex::encode(&lanes[..16]),
            "745de52265dda526441abcc6f84841c3"
        );
        assert_eq!(
            hex::encode(&lanes[1024..1040]),
            "6c4855879a2042465d65e374bd12b90d"
        );
        assert_eq!(
            hex::encode(&lanes[2032..]),
            "a37252d1b2f379e9c0b1bfdad6e582ac"
        );
        assert_eq!(
            hex::encode(builder.finish().as_bytes()),
            "14fb5a39e6323a2d5805d6ea38c35b1f05b85773ffd0fd9833003e621ad1f788"
        );
        Ok(())
    }

    #[test]
    fn row_expansions_keep_their_pinned_bytes() {
        for (message, expected) in [
            (
                Vec::new(),
                "1b4f11d7c46d8024164a76418031ff38e46567e2d44cb9ed774f02992ba0c7e9",
            ),
            (
                patterned_bytes(64, 0x00),
                "41339bca56f01af9e60b2013f7c3b324f91fd7bae8d196264dbaecc7d9a2a3ff",
            ),
            (
                patterned_bytes(300, 0x5a),
                "2c829dab647cf52acd7e841d2ec033cf9e18e90a8dd837c4798dfc217f51d916",
            ),
        ] {
            assert_eq!(
                expansion_fingerprint(&blake2x_expand(&message)),
                expected,
                "{}-byte preimage",
                message.len()
            );
        }
    }

    #[test]
    fn row_expansion_matches_the_serial_reference_in_every_block() {
        let messages = [
            Vec::new(),
            patterned_bytes(1, 0x01),
            patterned_bytes(64, 0x02),
            patterned_bytes(128, 0x03),
            patterned_bytes(190, 0x04),
            patterned_bytes(300, 0x05),
        ];
        for message in &messages {
            let expanded = blake2x_expand(message);
            let reference = serial_reference_expand(message);
            for (block_index, (block, reference_block)) in expanded
                .chunks_exact(BLAKE2B_OUTPUT_LEN)
                .zip(reference.chunks_exact(BLAKE2B_OUTPUT_LEN))
                .enumerate()
            {
                assert_eq!(
                    block,
                    reference_block,
                    "block {block_index} of a {}-byte preimage",
                    message.len()
                );
            }
        }
    }
}
