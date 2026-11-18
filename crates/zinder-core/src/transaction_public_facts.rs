//! Public-facts read-model for explorer transaction views.
//!
//! Single typed value the explorer plane returns when rendering a
//! transaction page. Parsed once at the source boundary
//! (`zinder_source::parse_transaction_public_fact_set`) from raw serialized
//! transaction bytes plus the node-discovered network upgrade table.
//! Carries everything the wallet plane already publishes plus the
//! ZIP-aware version, lock-time, expiry-height, component counts, and
//! privacy classification the explorer needs.
//!
//! The full design contract lives in
//! [ADR-0018](../../../docs/adrs/0018-transaction-facts-status-and-broadcast.md).

use crate::{AuthDigest, BlockHeight, ConsensusBranchId, TransactionId};

/// Wallet-transaction identifier carrying the witness-data commitment.
///
/// Per [ZIP-239](https://zips.z.cash/zip-0239), v5 transactions are relayed
/// under `MSG_WTX` with `wtxid = txid || auth_digest` (64 bytes total).
/// Pre-v5 transactions have no distinct wtxid because their txid already
/// covers their witness data; `Wtxid` is therefore only populated for v5+
/// transactions and `None` otherwise.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Wtxid([u8; 64]);

impl Wtxid {
    /// Creates a wtxid from canonical 64-byte material (`txid || auth_digest`).
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 64]) -> Self {
        Self(bytes)
    }

    /// Returns the wtxid bytes.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 64] {
        self.0
    }

    /// Returns the leading 32 bytes (`txid` half) of the wtxid.
    #[must_use]
    pub const fn txid_bytes(self) -> [u8; 32] {
        let mut out = [0_u8; 32];
        let mut idx = 0;
        while idx < 32 {
            out[idx] = self.0[idx];
            idx += 1;
        }
        out
    }
}

/// Coarse transaction-format identifier surfaced on the explorer wire.
///
/// Carries the integer `effective_version` from [ZIP-225](https://zips.z.cash/zip-0225)
/// for every variant plus the `version_group_id` from the v3/v4/v5 header so
/// future variants stay distinguishable without inventing fresh enum values.
/// `Unsupported` covers transactions whose effective version the parser
/// does not yet model (hypothetical v7, etc.); the explorer surface degrades
/// to `unsupported_sections` and `PrivacyShape::Unclassified` for those rows
/// instead of failing outright.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionVersion {
    /// Sprout v1 transaction (pre-Overwinter).
    V1,
    /// Sprout v2 transaction (pre-Overwinter).
    V2,
    /// Overwinter-activated v3 transaction.
    V3,
    /// Sapling-activated v4 transaction.
    V4,
    /// NU5-activated v5 transaction.
    V5,
    /// NU6.3/Ironwood-activated v6 transaction.
    V6,
    /// Transaction format the parser does not model end-to-end.
    Unsupported {
        /// Raw effective-version field from the transaction header.
        effective_version: u32,
        /// Raw `version_group_id` when one is present in the header.
        version_group_id: Option<u32>,
    },
}

impl TransactionVersion {
    /// Returns the effective-version integer the wire surface advertises.
    #[must_use]
    pub const fn effective_version(self) -> u32 {
        match self {
            Self::V1 => 1,
            Self::V2 => 2,
            Self::V3 => 3,
            Self::V4 => 4,
            Self::V5 => 5,
            Self::V6 => 6,
            Self::Unsupported {
                effective_version, ..
            } => effective_version,
        }
    }

    /// Returns `true` for any version Zinder fully models.
    #[must_use]
    pub const fn is_supported(self) -> bool {
        !matches!(self, Self::Unsupported { .. })
    }
}

/// Lock-time classification kept thin so the wire can carry it as a oneof.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LockTime {
    /// No lock-time constraint applies (header value zero or all sequences max).
    Unlocked,
    /// Lock time interpreted as a block height.
    Height(BlockHeight),
    /// Lock time interpreted as a Unix epoch seconds value.
    UnixSeconds(u64),
}

/// Coarse privacy classification surfaced verbatim on the explorer wire.
///
/// Computed by [`classify_privacy_shape`] from the component counts and
/// `is_coinbase` flag; the classifier does not look at scripts, values, or
/// shielded encryption blobs. Every transaction the explorer renders falls
/// into exactly one shape; `Unclassified` covers `TransactionVersion::Unsupported`
/// rows where the parser could not extract reliable component counts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrivacyShape {
    /// Transparent inputs and outputs only.
    TransparentOnly,
    /// Transparent inputs, shielded outputs, no shielded inputs.
    Shielding,
    /// Shielded inputs, transparent outputs, no shielded outputs.
    Deshielding,
    /// Shielded inputs and outputs only, no transparent components.
    ShieldedOnly,
    /// Mixed transparent and shielded components on both sides.
    Mixed,
    /// Coinbase transaction with transparent-only outputs.
    Coinbase,
    /// Coinbase transaction with shielded outputs ([ZIP-213](https://zips.z.cash/zip-0213)).
    ShieldedCoinbase,
    /// Parser could not classify because the transaction version is unsupported.
    Unclassified,
}

/// Forward-compatibility marker for transaction sections the parser does not model.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum UnsupportedSection {
    /// Future transaction-version header the parser does not decode.
    FutureVersionHeader,
    /// Future shielded protocol the parser does not iterate.
    FutureShieldedProtocol,
}

/// Component counts feeding [`classify_privacy_shape`] and the wire surface.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransactionComponentCounts {
    /// Transparent input count.
    pub transparent_input_count: u32,
    /// Transparent output count.
    pub transparent_output_count: u32,
    /// Sapling spend count.
    pub sapling_spend_count: u32,
    /// Sapling output count.
    pub sapling_output_count: u32,
    /// Orchard action count.
    pub orchard_action_count: u32,
    /// Ironwood action count.
    pub ironwood_action_count: u32,
    /// Sprout `JoinSplit` count.
    pub sprout_joinsplit_count: u32,
}

impl TransactionComponentCounts {
    /// All-zero counts marker, useful as a default for unsupported transactions.
    pub const EMPTY: Self = Self {
        transparent_input_count: 0,
        transparent_output_count: 0,
        sapling_spend_count: 0,
        sapling_output_count: 0,
        orchard_action_count: 0,
        ironwood_action_count: 0,
        sprout_joinsplit_count: 0,
    };

    /// Returns `true` when the transaction has any transparent inputs.
    #[must_use]
    pub const fn has_transparent_input(self) -> bool {
        self.transparent_input_count > 0
    }

    /// Returns `true` when the transaction has any transparent outputs.
    #[must_use]
    pub const fn has_transparent_output(self) -> bool {
        self.transparent_output_count > 0
    }

    /// Returns `true` when the transaction has any shielded inputs
    /// (Sapling spend, Orchard or Ironwood action acting as spend, or Sprout
    /// `JoinSplit`).
    #[must_use]
    pub const fn has_shielded_input(self) -> bool {
        self.sapling_spend_count > 0
            || self.orchard_action_count > 0
            || self.ironwood_action_count > 0
            || self.sprout_joinsplit_count > 0
    }

    /// Returns the [ZIP-317](https://zips.z.cash/zip-0317) logical-action
    /// count for this component shape.
    ///
    /// The count is the sum of each pool's contribution:
    /// `max(transparent_input_count, transparent_output_count) +
    /// max(sapling_spend_count, sapling_output_count) + orchard_action_count +
    /// ironwood_action_count`. The `max` folds inputs against outputs *within*
    /// the transparent and Sapling pools; the pools themselves add.
    ///
    /// The transparent term approximates ZIP-317's byte-size formula
    /// (`max(ceil(tx_in_size / 150), ceil(tx_out_size / 34))`) with input and
    /// output counts, which is exact for standard P2PKH scripts.
    ///
    /// The grace-actions floor (a fee-compute concept) is **not** applied
    /// here; this is the raw shape-derived count consumers surface on
    /// per-transaction views.
    ///
    /// Sprout `JoinSplit`s are intentionally excluded: ZIP-317 was specified
    /// after the Sprout pool was effectively frozen, and the spec scopes
    /// logical actions to the Sapling/Orchard/Ironwood side.
    #[must_use]
    pub const fn logical_actions(self) -> u32 {
        let transparent_logical = if self.transparent_input_count > self.transparent_output_count {
            self.transparent_input_count
        } else {
            self.transparent_output_count
        };
        let sapling_logical = if self.sapling_spend_count > self.sapling_output_count {
            self.sapling_spend_count
        } else {
            self.sapling_output_count
        };
        transparent_logical
            .saturating_add(sapling_logical)
            .saturating_add(self.orchard_action_count)
            .saturating_add(self.ironwood_action_count)
    }

    /// Returns the [ZIP-317](https://zips.z.cash/zip-0317) conventional
    /// fee floor in zatoshi computed from the component counts alone.
    ///
    /// The conventional fee is `MARGINAL_FEE * max(logical_actions,
    /// GRACE_ACTIONS)`, where `MARGINAL_FEE = 5_000` and `GRACE_ACTIONS = 2`.
    /// The result is the minimum fee a wallet should attach to a
    /// transaction with this shape; the actual fee a miner collected may
    /// differ and requires prevout resolution to compute.
    ///
    /// Used by `ExplorerQuery.FeeSummary` to aggregate fee floors across
    /// a block range without resolving every transparent input.
    #[must_use]
    pub const fn zip317_conventional_fee_zat(self) -> u64 {
        const MARGINAL_FEE_ZAT: u64 = 5_000;
        const GRACE_ACTIONS: u32 = 2;
        let logical_actions = self.logical_actions();
        let billable_actions = if logical_actions < GRACE_ACTIONS {
            GRACE_ACTIONS
        } else {
            logical_actions
        };
        MARGINAL_FEE_ZAT * billable_actions as u64
    }

    /// Returns `true` when the transaction has any shielded outputs
    /// (Sapling output, Orchard or Ironwood action acting as output, or Sprout
    /// `JoinSplit`).
    #[must_use]
    pub const fn has_shielded_output(self) -> bool {
        self.sapling_output_count > 0
            || self.orchard_action_count > 0
            || self.ironwood_action_count > 0
            || self.sprout_joinsplit_count > 0
    }
}

/// Read-model carrying every public fact the explorer renders for a transaction.
///
/// Constructed once by `zinder_source::parse_transaction_public_fact_set` and
/// surfaced verbatim on `ExplorerQuery.TransactionDetail`. Wallet ingest and
/// the mempool entry hydration path will adopt the same struct in subsequent
/// slices so all three call sites share one parse. Adding a new field is a
/// breaking change for external constructors; the struct stays passive on
/// purpose so callers spell out every field at construction time.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionPublicFacts {
    /// Canonical transaction identifier.
    pub transaction_id: TransactionId,
    /// ZIP-244 authorization digest (v5+; `None` for v1-v4).
    pub auth_digest: Option<AuthDigest>,
    /// ZIP-239 witness-data identifier (v5+; `None` for v1-v4).
    pub wtxid: Option<Wtxid>,
    /// Coarse transaction-format identifier.
    pub version: TransactionVersion,
    /// Consensus branch ID that authorized the transaction.
    ///
    /// Pulled from the transaction header for v5+; resolved from the node
    /// activation table for v3/v4 transactions; `None` for v1/v2 (no
    /// `nVersionGroupId` field on those legacy headers).
    pub consensus_branch_id: Option<ConsensusBranchId>,
    /// Lock-time classification.
    pub lock_time: LockTime,
    /// [ZIP-203](https://zips.z.cash/zip-0203) `nExpiryHeight` when present and
    /// non-sentinel. `None` for v1/v2 (no expiry field) and for the
    /// "no expiry" sentinel value 0.
    pub expiry_height: Option<BlockHeight>,
    /// Raw transaction byte length.
    pub size_bytes: u32,
    /// Component counts feeding the privacy classifier and wire surface.
    pub counts: TransactionComponentCounts,
    /// Signed Orchard value balance in zatoshis; `Some` only when the
    /// transaction carries an Orchard bundle.
    ///
    /// Positive means net value leaves the chain-wide Orchard pool through this
    /// transaction (its bundle's spends exceed its outputs); negative means net
    /// value enters the pool. A legitimate bundle can balance to `Some(0)`.
    pub orchard_value_balance_zat: Option<i64>,
    /// `anchorOrchard`: the Orchard bundle's shared note-commitment-tree root
    /// that its spends prove membership against; `Some` only when the
    /// transaction carries an Orchard bundle.
    ///
    /// Conformant Orchard-to-Ironwood migration transactions from many
    /// different wallets share this exact root when broadcast in the same
    /// network-wide anchor-height bucket (per the draft ZIP "Orchard to
    /// Ironwood Migration"), which lets a later consumer group them into
    /// privacy cohorts.
    pub orchard_anchor: Option<[u8; 32]>,
    /// Signed Ironwood value balance in zatoshis; `Some` only when the
    /// transaction carries an Ironwood bundle.
    ///
    /// Positive means net value leaves the chain-wide Ironwood pool through this
    /// transaction (its bundle's spends exceed its outputs); negative means net
    /// value enters the pool. A legitimate bundle can balance to `Some(0)`.
    pub ironwood_value_balance_zat: Option<i64>,
    /// Coarse privacy classification.
    pub privacy_shape: PrivacyShape,
    /// `true` when the transaction is a coinbase row.
    pub is_coinbase: bool,
    /// Forward-compatibility markers for sections the parser does not decode.
    pub unsupported_sections: Vec<UnsupportedSection>,
}

/// Classifies a transaction's privacy shape from its component counts.
///
/// Pure function on the counts struct and `is_coinbase` flag; no I/O, no
/// transaction bytes. Coinbase transactions classify as
/// [`PrivacyShape::Coinbase`] when their outputs are transparent-only and
/// [`PrivacyShape::ShieldedCoinbase`] when they contain Sapling outputs or
/// Orchard or Ironwood actions per [ZIP-213](https://zips.z.cash/zip-0213).
/// Non-coinbase transactions classify into the four pure shapes
/// ([`PrivacyShape::TransparentOnly`], [`Shielding`](PrivacyShape::Shielding),
/// [`Deshielding`](PrivacyShape::Deshielding),
/// [`ShieldedOnly`](PrivacyShape::ShieldedOnly)) when the counts split
/// cleanly, [`PrivacyShape::Mixed`] when both sides carry both kinds, and
/// [`PrivacyShape::Unclassified`] when no rule fires (e.g. zero-input,
/// zero-output transactions or unsupported versions).
#[must_use]
pub fn classify_privacy_shape(
    counts: TransactionComponentCounts,
    is_coinbase: bool,
    version: TransactionVersion,
) -> PrivacyShape {
    if !version.is_supported() {
        return PrivacyShape::Unclassified;
    }
    if is_coinbase {
        return if counts.sapling_output_count > 0
            || counts.orchard_action_count > 0
            || counts.ironwood_action_count > 0
        {
            PrivacyShape::ShieldedCoinbase
        } else {
            PrivacyShape::Coinbase
        };
    }
    classify_non_coinbase(counts)
}

fn classify_non_coinbase(counts: TransactionComponentCounts) -> PrivacyShape {
    let has_transparent_in = counts.has_transparent_input();
    let has_transparent_out = counts.has_transparent_output();
    let has_shielded_in = counts.has_shielded_input();
    let has_shielded_out = counts.has_shielded_output();
    match (
        has_transparent_in,
        has_transparent_out,
        has_shielded_in,
        has_shielded_out,
    ) {
        (true, true, false, false) => PrivacyShape::TransparentOnly,
        (true, _, false, true) if !has_shielded_in => PrivacyShape::Shielding,
        (_, true, true, false) if !has_shielded_out => PrivacyShape::Deshielding,
        (false, false, true, true) => PrivacyShape::ShieldedOnly,
        (true, true, true, true) | (true, _, true, _) | (_, true, _, true) => PrivacyShape::Mixed,
        _ => PrivacyShape::Unclassified,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds counts from a positional array.
    ///
    /// Indices: `[transparent_in, transparent_out, sapling_spend, sapling_out, orchard_action, ironwood_action, sprout_joinsplit]`.
    fn counts(values: [u32; 7]) -> TransactionComponentCounts {
        TransactionComponentCounts {
            transparent_input_count: values[0],
            transparent_output_count: values[1],
            sapling_spend_count: values[2],
            sapling_output_count: values[3],
            orchard_action_count: values[4],
            ironwood_action_count: values[5],
            sprout_joinsplit_count: values[6],
        }
    }

    #[test]
    fn transparent_only_v5_classifies_as_transparent_only() {
        let shape =
            classify_privacy_shape(counts([1, 1, 0, 0, 0, 0, 0]), false, TransactionVersion::V5);
        assert_eq!(shape, PrivacyShape::TransparentOnly);
    }

    #[test]
    fn shielding_v5_classifies_as_shielding() {
        let shape =
            classify_privacy_shape(counts([1, 0, 0, 1, 0, 0, 0]), false, TransactionVersion::V5);
        assert_eq!(shape, PrivacyShape::Shielding);
    }

    #[test]
    fn deshielding_v5_classifies_as_deshielding() {
        let shape =
            classify_privacy_shape(counts([0, 1, 1, 0, 0, 0, 0]), false, TransactionVersion::V5);
        assert_eq!(shape, PrivacyShape::Deshielding);
    }

    #[test]
    fn shielded_only_orchard_classifies_as_shielded_only() {
        let shape =
            classify_privacy_shape(counts([0, 0, 0, 0, 2, 0, 0]), false, TransactionVersion::V5);
        assert_eq!(shape, PrivacyShape::ShieldedOnly);
    }

    #[test]
    fn shielded_only_ironwood_classifies_as_shielded_only() {
        let shape =
            classify_privacy_shape(counts([0, 0, 0, 0, 0, 2, 0]), false, TransactionVersion::V6);
        assert_eq!(shape, PrivacyShape::ShieldedOnly);
    }

    #[test]
    fn mixed_components_classify_as_mixed() {
        let shape =
            classify_privacy_shape(counts([1, 1, 1, 1, 0, 0, 0]), false, TransactionVersion::V5);
        assert_eq!(shape, PrivacyShape::Mixed);
    }

    #[test]
    fn coinbase_transparent_only_classifies_as_coinbase() {
        let shape =
            classify_privacy_shape(counts([1, 1, 0, 0, 0, 0, 0]), true, TransactionVersion::V5);
        assert_eq!(shape, PrivacyShape::Coinbase);
    }

    #[test]
    fn coinbase_with_sapling_outputs_classifies_as_shielded_coinbase() {
        let shape =
            classify_privacy_shape(counts([1, 0, 0, 1, 0, 0, 0]), true, TransactionVersion::V5);
        assert_eq!(shape, PrivacyShape::ShieldedCoinbase);
    }

    #[test]
    fn unsupported_version_classifies_as_unclassified() {
        let unsupported = TransactionVersion::Unsupported {
            effective_version: 6,
            version_group_id: Some(0x0123_4567),
        };
        let shape = classify_privacy_shape(counts([1, 1, 0, 0, 0, 0, 0]), false, unsupported);
        assert_eq!(shape, PrivacyShape::Unclassified);
    }

    #[test]
    fn v6_classifies_as_a_supported_shape_not_unclassified() {
        let shape =
            classify_privacy_shape(counts([1, 1, 1, 1, 0, 0, 0]), false, TransactionVersion::V6);
        assert_eq!(shape, PrivacyShape::Mixed);
    }

    #[test]
    fn zip317_conventional_fee_matches_grace_floor() {
        // Empty counts: logical_actions = 0, clamped up to GRACE_ACTIONS = 2;
        // conventional fee = 5_000 * 2 = 10_000 zatoshi.
        assert_eq!(
            TransactionComponentCounts::EMPTY.zip317_conventional_fee_zat(),
            10_000,
        );
    }

    #[test]
    fn zip317_conventional_fee_sums_pool_contributions() {
        // 1 transparent_in, 2 transparent_out, 1 sapling_spend, 0 sapling_out,
        // 3 orchard_action -> logical_actions = max(1, 2) + max(1, 0) + 3 = 6.
        // Conventional fee = 5_000 * 6 = 30_000 zatoshi.
        let payload = counts([1, 2, 1, 0, 3, 0, 0]);
        assert_eq!(payload.logical_actions(), 6);
        assert_eq!(payload.zip317_conventional_fee_zat(), 30_000);
    }

    #[test]
    fn logical_actions_sums_pools_rather_than_max_folding() {
        // 2 transparent_in, 2 orchard_action across two pools: the summed
        // contributions give 2 + 2 = 4, not max(2, 2) = 2.
        let payload = counts([2, 0, 0, 0, 2, 0, 0]);
        assert_eq!(payload.logical_actions(), 4);
    }

    #[test]
    fn zip317_conventional_fee_uses_max_of_sapling_spends_and_outputs() {
        // 4 sapling_spend, 7 sapling_out -> sapling_logical = 7, dominates.
        // Conventional fee = 5_000 * 7 = 35_000 zatoshi.
        let payload = counts([0, 0, 4, 7, 0, 0, 0]);
        assert_eq!(payload.zip317_conventional_fee_zat(), 35_000);
    }

    #[test]
    fn zip317_conventional_fee_floored_to_grace_actions() {
        // 1 transparent_in, 1 transparent_out -> logical_actions = 1, floored
        // up to GRACE_ACTIONS = 2; conventional fee = 5_000 * 2 = 10_000.
        let payload = counts([1, 1, 0, 0, 0, 0, 0]);
        assert_eq!(payload.zip317_conventional_fee_zat(), 10_000);
    }

    #[test]
    fn zip317_conventional_fee_adds_ironwood_actions_marginally() {
        let ironwood_only = counts([0, 0, 0, 0, 0, 5, 0]);
        assert_eq!(ironwood_only.logical_actions(), 5);
        assert_eq!(ironwood_only.zip317_conventional_fee_zat(), 25_000);

        let combined_with_orchard = counts([0, 0, 0, 0, 3, 2, 0]);
        assert_eq!(combined_with_orchard.logical_actions(), 5);
        assert_eq!(combined_with_orchard.zip317_conventional_fee_zat(), 25_000);
    }

    #[test]
    fn wtxid_txid_bytes_matches_leading_half() {
        let mut bytes = [0_u8; 64];
        for (idx, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::try_from(idx % 256).unwrap_or(0);
        }
        let wtxid = Wtxid::from_bytes(bytes);
        let txid = wtxid.txid_bytes();
        for idx in 0..32 {
            assert_eq!(txid[idx], bytes[idx]);
        }
    }
}
