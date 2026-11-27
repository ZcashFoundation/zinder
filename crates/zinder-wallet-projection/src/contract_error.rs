//! Fail-closed errors shared by wallet projection contracts.

use thiserror::Error;

/// Contract construction or serial-oracle failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum WalletProjectionContractError {
    /// An output's outpoint and creation position name different transactions.
    #[error("wallet output creator does not match its outpoint transaction")]
    OutputCreatorMismatch,
    /// A variable-length field exceeds the version-1 `u32` length prefix.
    #[error("{field} exceeds the version-1 u32 length limit")]
    FieldTooLong {
        /// Field whose length could not be represented.
        field: &'static str,
    },
    /// A fixed-length durable field has the wrong number of bytes.
    #[error("{field} requires exactly {expected} bytes, observed {actual}")]
    DurableFieldLengthMismatch {
        /// Durable field being decoded.
        field: &'static str,
        /// Exact version-1 byte length.
        expected: usize,
        /// Observed byte length.
        actual: usize,
    },
    /// A variable-length durable value is shorter than its fixed prefix.
    #[error("{field} requires at least {minimum} bytes, observed {actual}")]
    DurableValueTooShort {
        /// Durable value being decoded.
        field: &'static str,
        /// Minimum version-1 byte length.
        minimum: usize,
        /// Observed byte length.
        actual: usize,
    },
    /// A durable value's length prefix does not consume the complete value.
    #[error("{field} length prefix does not match its encoded bytes")]
    DurableLengthPrefixMismatch {
        /// Durable value being decoded.
        field: &'static str,
    },
    /// A durable list is not in strict canonical key order.
    #[error("{field} keys must be strictly increasing")]
    DurableKeyOrder {
        /// Durable list whose ordering was invalid.
        field: &'static str,
    },
    /// A durable record does not begin with the wallet store identity.
    #[error("{field} does not begin with the wallet store identity \"wallet\"")]
    DurableIdentityMismatch {
        /// Durable contract being decoded.
        field: &'static str,
    },
    /// A wallet store was written under a projection schema this release does not read.
    #[error(
        "wallet store uses projection schema version {encoded}, and this release reads only \
         version {supported}; rebuild the wallet store from the canonical store"
    )]
    UnsupportedWalletProjectionSchemaVersion {
        /// Schema version the store records.
        encoded: u64,
        /// The only schema version this release reads.
        supported: u16,
    },
    /// A durable numeric discriminator is unsupported by version 1.
    #[error("{field} has unsupported encoded value {encoded}")]
    UnsupportedEncodedValue {
        /// Durable discriminator being decoded.
        field: &'static str,
        /// Unsupported numeric value.
        encoded: u64,
    },
    /// A durable record contains bytes after its complete version-1 value.
    #[error("{field} contains trailing bytes")]
    DurableTrailingBytes {
        /// Durable record being decoded.
        field: &'static str,
    },
    /// A parsed durable record does not reproduce its exact input bytes.
    #[error("{field} is not canonically encoded")]
    DurableNonCanonicalEncoding {
        /// Durable record being decoded.
        field: &'static str,
    },
    /// A projection accumulator family contains more than `u64::MAX` rows.
    #[error("wallet projection accumulator family row count exceeds u64::MAX")]
    ProjectionDigestRowCountOverflow,
    /// A projection accumulator removal was attempted from an empty family.
    #[error("wallet projection accumulator family row count underflow")]
    ProjectionDigestRowCountUnderflow,
    /// A persisted projection accumulator uses an unsupported algorithm version.
    #[error("wallet projection accumulator has unsupported version {encoded}")]
    UnsupportedProjectionAccumulatorVersion {
        /// Unsupported durable accumulator version.
        encoded: u64,
    },
    /// A persisted projection accumulator has the wrong exact byte length.
    #[error(
        "wallet projection accumulator has invalid byte length; expected {expected}, observed {actual}"
    )]
    ProjectionAccumulatorLengthMismatch {
        /// Required accumulator byte length.
        expected: usize,
        /// Observed durable byte length.
        actual: usize,
    },
    /// READY evidence's display digest is not derived from its full accumulator.
    #[error("wallet projection display digest does not match its full accumulator")]
    ProjectionAccumulatorDigestMismatch,
    /// A persisted wallet event cursor uses an unsupported version.
    #[error("wallet projection event cursor has unsupported version {encoded}")]
    UnsupportedWalletProjectionEventCursorVersion {
        /// Unsupported durable cursor version.
        encoded: u64,
    },
    /// A wallet event cursor must name a nonzero retained event sequence.
    #[error("wallet projection event cursor must name a nonzero event sequence")]
    WalletProjectionEventCursorZeroSequence,
    /// A source position's event cursor and event sequence differ.
    #[error("wallet projection source position event cursor does not match its event sequence")]
    WalletProjectionEventCursorSequenceMismatch,
    /// A reorg undo record has an unsupported source-sequence digest version.
    #[error("wallet reorg undo source sequence digest must use version 1")]
    ReorgUndoSourceSequenceVersionMismatch,
    /// A reorg undo record's source sequence does not advance exactly once.
    #[error("wallet reorg undo source sequence digests do not differ by one block")]
    ReorgUndoSourceSequenceLengthMismatch,
    /// The wallet projection source sequence exceeded its `u64` count domain.
    #[error(transparent)]
    SourceSequenceLength(#[from] zinder_core::CanonicalBlockFactsSequenceLengthOverflow),
    /// A canonical block does not extend the oracle's current tip.
    #[error("canonical block does not extend the wallet projection serial oracle")]
    NonContiguousBlock,
    /// Canonical facts repeated an already-created outpoint.
    #[error("canonical facts contain a duplicate transparent output")]
    DuplicateOutput,
    /// A transparent input references an output absent from complete history.
    #[error("transparent input predecessor is absent from complete history")]
    MissingTransparentPredecessor,
    /// Canonical facts repeat a transparent spend.
    #[error("canonical facts contain a duplicate transparent spend")]
    DuplicateSpend,
    /// Canonical transaction or transparent fact order cannot fit its `u32` contract.
    #[error("canonical fact position exceeds the version-1 u32 index limit")]
    FactIndexOverflow,
    /// Canonical transparent fact index disagrees with its ordered vector position.
    #[error("canonical transparent fact index disagrees with vector order")]
    FactIndexMismatch,
    /// An address balance addition exceeded `u64::MAX`.
    #[error("wallet address balance exceeds u64::MAX zatoshi")]
    AddressBalanceOverflow,
    /// An address balance could not cover a spent output.
    #[error("wallet address balance is below a spent output value")]
    AddressBalanceUnderflow,
    /// The UTXO count exceeded `u64::MAX`.
    #[error("wallet UTXO count exceeds u64::MAX")]
    UtxoCountOverflow,
    /// The UTXO value total exceeded `u64::MAX`.
    #[error("wallet UTXO value total exceeds u64::MAX zatoshi")]
    UtxoValueOverflow,
    /// The oracle could not remove one known UTXO from its count.
    #[error("wallet UTXO count underflow")]
    UtxoCountUnderflow,
    /// The oracle could not remove one known UTXO from its value total.
    #[error("wallet UTXO value underflow")]
    UtxoValueUnderflow,
    /// Every unspent output must have exactly one address secondary-index row.
    #[error("wallet unspent-output and address-index row counts do not match")]
    ReadyUnspentOutputIndexCountMismatch,
    /// The ready UTXO count must equal the unspent-output row count.
    #[error("wallet UTXO count does not match the unspent-output row count")]
    ReadyUtxoCountMismatch,
    /// The ready undo family must cover the unsettled canonical suffix exactly.
    #[error("wallet reorg-undo count does not match the unsettled canonical suffix")]
    ReadyReorgUndoCountMismatch,
    /// The ready block-hash table must cover every retained canonical block exactly.
    #[error("wallet block-hash row count does not match the retained canonical sequence")]
    ReadyBlockHashCountMismatch,
    /// The canonical settled tip is not an ancestor of the ready visible tip.
    #[error("wallet settled tip lies outside the ready canonical source range")]
    ReadySettledTipOutsideSourceRange,
    /// The source sequence does not contain the retained tip block.
    #[error("wallet source sequence must contain at least one retained tip block")]
    ReadySourceSequenceLengthMismatch,
    /// Version 1 accepts only canonical sequence digest version 1.
    #[error("wallet source sequence digest must use version 1")]
    ReadySourceSequenceVersionMismatch,
    /// Readiness requires the `LtHash16` UTXO commitment with `ChaCha12` expansion.
    #[error("wallet readiness requires UTXO commitment scheme 1 (LtHash16, ChaCha12 expansion)")]
    ReadyUtxoCommitmentSchemeMismatch,
    /// A persisted build lease uses an unsupported lease encoding version.
    #[error("wallet projection build lease has unsupported version {encoded}")]
    UnsupportedWalletProjectionBuildLeaseVersion {
        /// Unsupported durable lease version.
        encoded: u64,
    },
    /// A persisted lease does not name the wallet projection schema that contains it.
    #[error("wallet projection build lease schema does not match store schema")]
    WalletProjectionBuildLeaseSchemaMismatch,
    /// A persisted lease belongs to a different network than its store control.
    #[error("wallet projection build lease network does not match store control")]
    WalletProjectionBuildLeaseNetworkMismatch,
    /// A persisted lease and singleton control record disagree about writer generation.
    #[error("wallet projection build lease generation does not match store control")]
    WalletProjectionBuildLeaseGenerationMismatch,
    /// A persisted lease is not anchored at the BUILDING plan's canonical target.
    #[error("wallet projection build lease canonical anchor does not match the BUILDING plan")]
    WalletProjectionBuildLeaseCanonicalAnchorMismatch,
    /// A retained event anchor follows the canonical anchor it is meant to resume from.
    #[error("wallet projection retained event anchor follows its canonical anchor")]
    WalletProjectionBuildLeaseRetainedEventAnchorMismatch,
    /// READY state must not retain a build lease after atomic promotion.
    #[error("READY wallet projection control must not retain a build lease")]
    ReadyControlRetainsBuildLease,
}

pub(crate) fn encoded_len(
    len: usize,
    field: &'static str,
) -> Result<u32, WalletProjectionContractError> {
    u32::try_from(len).map_err(|_| WalletProjectionContractError::FieldTooLong { field })
}
