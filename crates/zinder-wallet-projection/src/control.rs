//! Wallet projection build, readiness, and control contracts.

use zinder_core::{
    BlockHash, BlockHeight, BlockId, CanonicalBlockFactsSequenceDigest,
    CanonicalBlockFactsSequenceDigestVersion, ChainEpochId, Network, TransparentUtxoSetCommitment,
    UTXO_SET_COMMITMENT_LEN, UnixTimestampMillis, UtxoSetCommitmentScheme,
};

use crate::{
    REQUIRED_CANONICAL_FACTS_DIGEST_VERSION, REQUIRED_CANONICAL_REPLAY_FORMAT_VERSION,
    REQUIRED_CANONICAL_SEQUENCE_DIGEST_VERSION, REQUIRED_CANONICAL_STORE_SCHEMA_VERSION,
    WALLET_PROJECTION_ACCUMULATOR_LEN, WALLET_PROJECTION_ACCUMULATOR_VERSION,
    WALLET_PROJECTION_BUILD_LEASE_VERSION, WALLET_PROJECTION_SCHEMA_VERSION,
    WALLET_PROJECTION_STORE_IDENTITY, WALLET_PROJECTION_VALUE_ENCODING_VERSION,
    WalletProjectionAccumulator, WalletProjectionContractError,
};

const WALLET_PROJECTION_EVENT_CURSOR_VERSION: u8 = 1;
const WALLET_PROJECTION_EVENT_CURSOR_LEN: usize = 1 + size_of::<u64>();

/// Exact portable encoding of the canonical retained-event cursor.
///
/// The wallet contract does not depend on a canonical-store implementation,
/// but it persists the same version byte and big-endian event sequence so a
/// READY fence always carries both its cursor bytes and decoded sequence.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct WalletProjectionEventCursor([u8; WALLET_PROJECTION_EVENT_CURSOR_LEN]);

impl WalletProjectionEventCursor {
    /// Creates the current cursor encoding for one event sequence.
    #[must_use]
    pub const fn for_sequence(event_sequence: u64) -> Self {
        let sequence = event_sequence.to_be_bytes();
        Self([
            WALLET_PROJECTION_EVENT_CURSOR_VERSION,
            sequence[0],
            sequence[1],
            sequence[2],
            sequence[3],
            sequence[4],
            sequence[5],
            sequence[6],
            sequence[7],
        ])
    }

    /// Decodes and validates exact durable cursor bytes.
    pub fn from_bytes(
        bytes: [u8; WALLET_PROJECTION_EVENT_CURSOR_LEN],
    ) -> Result<Self, WalletProjectionContractError> {
        if bytes[0] != WALLET_PROJECTION_EVENT_CURSOR_VERSION {
            return Err(
                WalletProjectionContractError::UnsupportedWalletProjectionEventCursorVersion {
                    encoded: u64::from(bytes[0]),
                },
            );
        }
        let cursor = Self(bytes);
        if cursor.event_sequence() == 0 {
            return Err(WalletProjectionContractError::WalletProjectionEventCursorZeroSequence);
        }
        Ok(cursor)
    }

    /// Returns the exact durable cursor bytes.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; WALLET_PROJECTION_EVENT_CURSOR_LEN] {
        self.0
    }

    /// Returns the event sequence encoded by this cursor.
    #[must_use]
    pub const fn event_sequence(self) -> u64 {
        u64::from_be_bytes([
            self.0[1], self.0[2], self.0[3], self.0[4], self.0[5], self.0[6], self.0[7], self.0[8],
        ])
    }
}

/// Exact canonical source position represented by wallet projection state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalletProjectionSourcePosition {
    /// Monotonic canonical epoch visible at this position.
    pub chain_epoch_id: ChainEpochId,
    /// Canonical tip projected into wallet state.
    pub tip: BlockId,
    /// Monotonic event-stream sequence projected through this position.
    pub event_sequence: u64,
    /// Exact versioned cursor encoding of `event_sequence`.
    pub event_cursor: WalletProjectionEventCursor,
}

impl WalletProjectionSourcePosition {
    /// Creates an exact wallet projection source position.
    #[must_use]
    pub const fn new(chain_epoch_id: ChainEpochId, tip: BlockId, event_sequence: u64) -> Self {
        Self {
            chain_epoch_id,
            tip,
            event_sequence,
            event_cursor: WalletProjectionEventCursor::for_sequence(event_sequence),
        }
    }

    /// Creates a source position from independently persisted cursor bytes.
    pub fn with_event_cursor(
        chain_epoch_id: ChainEpochId,
        tip: BlockId,
        event_sequence: u64,
        event_cursor: WalletProjectionEventCursor,
    ) -> Result<Self, WalletProjectionContractError> {
        let source_position = Self {
            chain_epoch_id,
            tip,
            event_sequence,
            event_cursor,
        };
        source_position.validate_event_cursor()?;
        Ok(source_position)
    }

    fn validate_event_cursor(self) -> Result<(), WalletProjectionContractError> {
        if self.event_cursor.event_sequence() != self.event_sequence {
            return Err(WalletProjectionContractError::WalletProjectionEventCursorSequenceMismatch);
        }
        if self.event_sequence == 0 {
            return Err(WalletProjectionContractError::WalletProjectionEventCursorZeroSequence);
        }
        Ok(())
    }
}

/// Exact authenticated canonical source a READY wallet projection represents.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalletCanonicalSourceIdentity {
    source_position: WalletProjectionSourcePosition,
    source_sequence_digest: CanonicalBlockFactsSequenceDigest,
    settled_tip: BlockId,
}

impl WalletCanonicalSourceIdentity {
    /// Creates an expected source identity from an authenticated canonical fence.
    #[must_use]
    pub const fn new(
        source_position: WalletProjectionSourcePosition,
        source_sequence_digest: CanonicalBlockFactsSequenceDigest,
        settled_tip: BlockId,
    ) -> Self {
        Self {
            source_position,
            source_sequence_digest,
            settled_tip,
        }
    }

    /// Extracts the serving identity committed by READY evidence.
    #[must_use]
    pub const fn from_ready_evidence(evidence: &WalletProjectionReadyEvidence) -> Self {
        Self::new(
            evidence.source_position,
            evidence.source_sequence_digest,
            evidence.settled_tip,
        )
    }

    /// Returns the exact epoch, tip, and event sequence represented by the source.
    #[must_use]
    pub const fn source_position(self) -> WalletProjectionSourcePosition {
        self.source_position
    }

    /// Returns the authenticated ordered canonical-facts digest through the source tip.
    #[must_use]
    pub const fn source_sequence_digest(self) -> CanonicalBlockFactsSequenceDigest {
        self.source_sequence_digest
    }

    /// Returns the canonical settlement boundary represented by this source.
    #[must_use]
    pub const fn settled_tip(self) -> BlockId {
        self.settled_tip
    }
}

/// Opaque, durable identity of one projection-build owner.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct WalletProjectionBuildOwner([u8; 16]);

impl WalletProjectionBuildOwner {
    /// Creates an owner identity from its exact durable bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the exact durable owner bytes.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 16] {
        self.0
    }
}

/// Formats the owner as the 32 lowercase hexadecimal characters that
/// `projector.build_owner_hex` configures, so an operator can match a
/// reported lease holder to its configuration.
impl core::fmt::Display for WalletProjectionBuildOwner {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.0
            .iter()
            .try_for_each(|byte| write!(formatter, "{byte:02x}"))
    }
}

/// Exact retained chain-event position from which projection following may resume.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalletProjectionRetainedEventAnchor {
    earliest_retained_event_sequence: u64,
}

impl WalletProjectionRetainedEventAnchor {
    /// Creates an exact retained chain-event sequence anchor.
    #[must_use]
    pub const fn new(earliest_retained_event_sequence: u64) -> Self {
        Self {
            earliest_retained_event_sequence,
        }
    }

    /// Returns the earliest event sequence the builder observed as retained.
    #[must_use]
    pub const fn earliest_retained_event_sequence(self) -> u64 {
        self.earliest_retained_event_sequence
    }
}

/// Requested durable ownership for one fixed-tip projection build.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalletProjectionBuildLeaseRequest {
    owner: WalletProjectionBuildOwner,
    pinned_canonical_anchor: WalletCanonicalSourceIdentity,
    retained_event_anchor: WalletProjectionRetainedEventAnchor,
    expires_at: UnixTimestampMillis,
}

impl WalletProjectionBuildLeaseRequest {
    /// Creates a request to own one fixed-tip projection build until `expires_at`.
    #[must_use]
    pub const fn new(
        owner: WalletProjectionBuildOwner,
        pinned_canonical_anchor: WalletCanonicalSourceIdentity,
        retained_event_anchor: WalletProjectionRetainedEventAnchor,
        expires_at: UnixTimestampMillis,
    ) -> Self {
        Self {
            owner,
            pinned_canonical_anchor,
            retained_event_anchor,
            expires_at,
        }
    }

    /// Returns the requested owner identity.
    #[must_use]
    pub const fn owner(self) -> WalletProjectionBuildOwner {
        self.owner
    }

    /// Returns the exact canonical source the builder must reproduce before promotion.
    #[must_use]
    pub const fn pinned_canonical_anchor(self) -> WalletCanonicalSourceIdentity {
        self.pinned_canonical_anchor
    }

    /// Returns the retained event-history anchor observed before the build started.
    #[must_use]
    pub const fn retained_event_anchor(self) -> WalletProjectionRetainedEventAnchor {
        self.retained_event_anchor
    }

    /// Returns the requested lease expiry.
    #[must_use]
    pub const fn expires_at(self) -> UnixTimestampMillis {
        self.expires_at
    }
}

/// Versioned durable capability that exclusively owns one projection build.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalletProjectionBuildLease {
    version: u16,
    owner: WalletProjectionBuildOwner,
    generation: u64,
    network: Network,
    projection_schema_version: u16,
    pinned_canonical_anchor: WalletCanonicalSourceIdentity,
    retained_event_anchor: WalletProjectionRetainedEventAnchor,
    expires_at: UnixTimestampMillis,
}

impl WalletProjectionBuildLease {
    /// Creates a current-version lease from one accepted ownership request.
    #[must_use]
    pub const fn from_request(
        request: WalletProjectionBuildLeaseRequest,
        generation: u64,
        network: Network,
    ) -> Self {
        Self {
            version: WALLET_PROJECTION_BUILD_LEASE_VERSION,
            owner: request.owner,
            generation,
            network,
            projection_schema_version: WALLET_PROJECTION_SCHEMA_VERSION,
            pinned_canonical_anchor: request.pinned_canonical_anchor,
            retained_event_anchor: request.retained_event_anchor,
            expires_at: request.expires_at,
        }
    }

    /// Returns the durable lease encoding version.
    #[must_use]
    pub const fn version(self) -> u16 {
        self.version
    }

    /// Returns the owner authorized by this lease.
    #[must_use]
    pub const fn owner(self) -> WalletProjectionBuildOwner {
        self.owner
    }

    /// Returns the monotonic ownership generation.
    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }

    /// Returns the network bound to this lease.
    #[must_use]
    pub const fn network(self) -> Network {
        self.network
    }

    /// Returns the wallet projection schema admitted by this lease.
    #[must_use]
    pub const fn projection_schema_version(self) -> u16 {
        self.projection_schema_version
    }

    /// Returns the exact canonical source that must be reproduced before promotion.
    #[must_use]
    pub const fn pinned_canonical_anchor(self) -> WalletCanonicalSourceIdentity {
        self.pinned_canonical_anchor
    }

    /// Returns the retained event-history anchor observed by the owner.
    #[must_use]
    pub const fn retained_event_anchor(self) -> WalletProjectionRetainedEventAnchor {
        self.retained_event_anchor
    }

    /// Returns the exclusive ownership expiry.
    #[must_use]
    pub const fn expires_at(self) -> UnixTimestampMillis {
        self.expires_at
    }

    /// Returns a renewal candidate preserving every lease identity field.
    ///
    /// A candidate becomes active only when the wallet store atomically
    /// validates and persists it against the current durable lease.
    #[must_use]
    pub const fn renewed(self, expires_at: UnixTimestampMillis) -> Self {
        Self { expires_at, ..self }
    }
}

/// Immutable plan recorded before a wallet projection build starts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalletProjectionBuildPlan {
    /// Canonical source position the build must reach before readiness publication.
    pub target_source_position: WalletProjectionSourcePosition,
}

impl WalletProjectionBuildPlan {
    /// Creates a complete-history build plan.
    #[must_use]
    pub fn complete_history(target_source_position: WalletProjectionSourcePosition) -> Self {
        Self {
            target_source_position,
        }
    }
}

/// Compact BLAKE2b-256 display digest of the full wallet-row accumulator.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct WalletProjectionDigest([u8; 32]);

impl WalletProjectionDigest {
    /// Reconstructs a digest from exact committed bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the exact digest bytes.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Row counts published with wallet readiness evidence.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WalletProjectionFamilyRowCounts {
    /// Rows in `transparent_unspent_output`.
    pub transparent_unspent_output_count: u64,
    /// Rows in `transparent_unspent_output_by_address`.
    pub transparent_unspent_output_by_address_count: u64,
    /// Rows in `transparent_spent_output`.
    pub transparent_spent_output_count: u64,
    /// Rows in `transparent_address_transaction`.
    pub transparent_address_transaction_count: u64,
    /// Non-zero rows in `transparent_address_balance`.
    pub transparent_address_balance_count: u64,
    /// Rows in the contiguous retained `reorg_undo` suffix.
    pub reorg_undo_count: u64,
}

/// Complete UTXO aggregate published with wallet readiness.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalletUtxoSetSummary {
    /// Number of currently unspent transparent outputs.
    pub utxo_count: u64,
    /// Sum of all currently unspent transparent output values.
    pub total_value_zat: u64,
    /// Full order-independent `LtHash16` accumulator.
    pub commitment: TransparentUtxoSetCommitment,
}

/// Evidence required before wallet queries may serve a projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalletProjectionReadyEvidence {
    /// Exact source position of the ready projection.
    pub source_position: WalletProjectionSourcePosition,
    /// Ordered digest of every canonical block-facts record through `source_position`.
    pub source_sequence_digest: CanonicalBlockFactsSequenceDigest,
    /// Exact canonical settlement boundary; undo rows exist only above it.
    pub settled_tip: BlockId,
    /// Digest of every logical projection row.
    pub projection_digest: WalletProjectionDigest,
    /// Full order-independent accumulator from which `projection_digest` is derived.
    pub projection_accumulator: WalletProjectionAccumulator,
    /// Exact logical row counts by family.
    pub row_counts: WalletProjectionFamilyRowCounts,
    /// Complete current UTXO aggregate.
    pub utxo_summary: WalletUtxoSetSummary,
}

/// Durable wallet store lifecycle state.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WalletProjectionBuildState {
    /// Rows are incomplete and query admission must refuse the store.
    Building(WalletProjectionBuildPlan),
    /// Evidence is complete and query admission may validate the source fence.
    Ready(WalletProjectionReadyEvidence),
}

/// Singleton durable control record for one wallet projection store.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalletStoreControlRecord {
    /// Network whose canonical facts produced the projection.
    pub network: Network,
    /// Maximum rollback depth represented by `reorg_undo`.
    pub supported_reorg_depth: u32,
    /// Monotonic single-writer generation.
    pub writer_generation: u64,
    /// Expiring exclusive ownership while the store remains BUILDING.
    pub build_lease: Option<WalletProjectionBuildLease>,
    /// Build or ready lifecycle state.
    pub build_state: WalletProjectionBuildState,
}

impl WalletStoreControlRecord {
    /// Encodes the exact, single-version wallet control record.
    pub fn encode(&self) -> Result<Vec<u8>, WalletProjectionContractError> {
        if let WalletProjectionBuildState::Building(plan) = &self.build_state {
            plan.target_source_position.validate_event_cursor()?;
        }
        validate_build_lease(self)?;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(WALLET_PROJECTION_STORE_IDENTITY);
        bytes.extend_from_slice(&WALLET_PROJECTION_SCHEMA_VERSION.to_be_bytes());
        bytes.extend_from_slice(&WALLET_PROJECTION_VALUE_ENCODING_VERSION.to_be_bytes());
        bytes.extend_from_slice(&self.network.id().to_be_bytes());
        bytes.extend_from_slice(&REQUIRED_CANONICAL_STORE_SCHEMA_VERSION.to_be_bytes());
        bytes.extend_from_slice(&REQUIRED_CANONICAL_REPLAY_FORMAT_VERSION.to_be_bytes());
        bytes.extend_from_slice(&REQUIRED_CANONICAL_FACTS_DIGEST_VERSION.to_be_bytes());
        bytes.extend_from_slice(&REQUIRED_CANONICAL_SEQUENCE_DIGEST_VERSION.to_be_bytes());
        bytes.extend_from_slice(&self.supported_reorg_depth.to_be_bytes());
        bytes.extend_from_slice(&self.writer_generation.to_be_bytes());
        match self.build_lease {
            None => bytes.push(0),
            Some(lease) => {
                bytes.push(1);
                encode_build_lease(lease, &mut bytes);
            }
        }
        match &self.build_state {
            WalletProjectionBuildState::Building(plan) => {
                bytes.push(1);
                encode_build_plan(plan, &mut bytes);
            }
            WalletProjectionBuildState::Ready(evidence) => {
                bytes.push(2);
                encode_ready_evidence(evidence, self.supported_reorg_depth, &mut bytes)?;
            }
        }
        Ok(bytes)
    }

    /// Decodes and validates one exact wallet control record.
    pub fn decode(encoded: &[u8]) -> Result<Self, WalletProjectionContractError> {
        let mut decoder = WalletControlDecoder::new(encoded);
        let identity = decoder.read_bytes(WALLET_PROJECTION_STORE_IDENTITY.len())?;
        if identity != WALLET_PROJECTION_STORE_IDENTITY {
            return Err(WalletProjectionContractError::DurableIdentityMismatch {
                field: "wallet store control",
            });
        }
        decoder.require_u16(
            "wallet projection schema version",
            WALLET_PROJECTION_SCHEMA_VERSION,
        )?;
        decoder.require_u16(
            "wallet projection value encoding version",
            WALLET_PROJECTION_VALUE_ENCODING_VERSION,
        )?;
        let network_id = decoder.read_u32()?;
        let network = Network::from_id(network_id).ok_or_else(|| {
            WalletProjectionContractError::UnsupportedEncodedValue {
                field: "wallet projection network",
                encoded: u64::from(network_id),
            }
        })?;
        decoder.require_u16(
            "required canonical store schema version",
            REQUIRED_CANONICAL_STORE_SCHEMA_VERSION,
        )?;
        decoder.require_u32(
            "required canonical replay format version",
            REQUIRED_CANONICAL_REPLAY_FORMAT_VERSION,
        )?;
        decoder.require_u16(
            "required canonical facts digest version",
            REQUIRED_CANONICAL_FACTS_DIGEST_VERSION,
        )?;
        decoder.require_u16(
            "required canonical sequence digest version",
            REQUIRED_CANONICAL_SEQUENCE_DIGEST_VERSION,
        )?;
        let supported_reorg_depth = decoder.read_u32()?;
        let writer_generation = decoder.read_u64()?;
        let build_lease = match decoder.read_u8()? {
            0 => None,
            1 => Some(decoder.read_build_lease()?),
            encoded_presence => {
                return Err(WalletProjectionContractError::UnsupportedEncodedValue {
                    field: "wallet projection build lease presence",
                    encoded: u64::from(encoded_presence),
                });
            }
        };
        let build_state = match decoder.read_u8()? {
            1 => WalletProjectionBuildState::Building(WalletProjectionBuildPlan::complete_history(
                decoder.read_source_position()?,
            )),
            2 => WalletProjectionBuildState::Ready(
                decoder.read_ready_evidence(supported_reorg_depth)?,
            ),
            encoded_state => {
                return Err(WalletProjectionContractError::UnsupportedEncodedValue {
                    field: "wallet projection build state",
                    encoded: u64::from(encoded_state),
                });
            }
        };
        decoder.finish()?;
        let control = Self {
            network,
            supported_reorg_depth,
            writer_generation,
            build_lease,
            build_state,
        };
        validate_build_lease(&control)?;
        if control.encode()? != encoded {
            return Err(WalletProjectionContractError::DurableNonCanonicalEncoding {
                field: "wallet store control canonical encoding",
            });
        }
        Ok(control)
    }
}

struct WalletControlDecoder<'a> {
    encoded: &'a [u8],
    offset: usize,
}

impl<'a> WalletControlDecoder<'a> {
    const fn new(encoded: &'a [u8]) -> Self {
        Self { encoded, offset: 0 }
    }

    fn read_build_lease(
        &mut self,
    ) -> Result<WalletProjectionBuildLease, WalletProjectionContractError> {
        let version = self.read_u16()?;
        if version != WALLET_PROJECTION_BUILD_LEASE_VERSION {
            return Err(
                WalletProjectionContractError::UnsupportedWalletProjectionBuildLeaseVersion {
                    encoded: u64::from(version),
                },
            );
        }
        let owner = WalletProjectionBuildOwner::from_bytes(self.read_array::<16>()?);
        let generation = self.read_u64()?;
        let network_id = self.read_u32()?;
        let network = Network::from_id(network_id).ok_or_else(|| {
            WalletProjectionContractError::UnsupportedEncodedValue {
                field: "wallet projection build lease network",
                encoded: u64::from(network_id),
            }
        })?;
        let projection_schema_version = self.read_u16()?;
        let pinned_canonical_anchor = WalletCanonicalSourceIdentity::new(
            self.read_source_position()?,
            self.read_source_sequence_digest()?,
            self.read_block_id()?,
        );
        let retained_event_anchor = WalletProjectionRetainedEventAnchor::new(self.read_u64()?);
        let expires_at = UnixTimestampMillis::new(self.read_u64()?);
        Ok(WalletProjectionBuildLease {
            version,
            owner,
            generation,
            network,
            projection_schema_version,
            pinned_canonical_anchor,
            retained_event_anchor,
            expires_at,
        })
    }

    fn read_ready_evidence(
        &mut self,
        supported_reorg_depth: u32,
    ) -> Result<WalletProjectionReadyEvidence, WalletProjectionContractError> {
        let source_position = self.read_source_position()?;
        let source_sequence_digest = self.read_source_sequence_digest()?;
        let settled_tip = self.read_block_id()?;
        self.require_u16(
            "wallet projection accumulator version",
            WALLET_PROJECTION_ACCUMULATOR_VERSION,
        )?;
        let projection_accumulator = WalletProjectionAccumulator::from_bytes(
            self.read_bytes(WALLET_PROJECTION_ACCUMULATOR_LEN)?,
        )?;
        let projection_digest = projection_accumulator.display_digest();
        let row_counts = WalletProjectionFamilyRowCounts {
            transparent_unspent_output_count: self.read_u64()?,
            transparent_unspent_output_by_address_count: self.read_u64()?,
            transparent_spent_output_count: self.read_u64()?,
            transparent_address_transaction_count: self.read_u64()?,
            transparent_address_balance_count: self.read_u64()?,
            reorg_undo_count: self.read_u64()?,
        };
        let utxo_count = self.read_u64()?;
        let total_value_zat = self.read_u64()?;
        let scheme_id = self.read_u32()?;
        let commitment_scheme = UtxoSetCommitmentScheme::from_id(scheme_id).ok_or_else(|| {
            WalletProjectionContractError::UnsupportedEncodedValue {
                field: "wallet UTXO commitment scheme",
                encoded: u64::from(scheme_id),
            }
        })?;
        let commitment_bytes = self.read_bytes(UTXO_SET_COMMITMENT_LEN)?;
        let commitment =
            TransparentUtxoSetCommitment::from_parts(commitment_scheme, commitment_bytes).ok_or(
                WalletProjectionContractError::DurableFieldLengthMismatch {
                    field: "wallet UTXO commitment accumulator",
                    expected: UTXO_SET_COMMITMENT_LEN,
                    actual: commitment_bytes.len(),
                },
            )?;
        let evidence = WalletProjectionReadyEvidence {
            source_position,
            source_sequence_digest,
            settled_tip,
            projection_digest,
            projection_accumulator,
            row_counts,
            utxo_summary: WalletUtxoSetSummary {
                utxo_count,
                total_value_zat,
                commitment,
            },
        };
        validate_ready_evidence(&evidence, supported_reorg_depth)?;
        Ok(evidence)
    }

    fn read_source_position(
        &mut self,
    ) -> Result<WalletProjectionSourcePosition, WalletProjectionContractError> {
        WalletProjectionSourcePosition::with_event_cursor(
            ChainEpochId::new(self.read_u64()?),
            BlockId::new(
                BlockHeight::new(self.read_u32()?),
                BlockHash::from_bytes(self.read_array::<32>()?),
            ),
            self.read_u64()?,
            WalletProjectionEventCursor::from_bytes(
                self.read_array::<WALLET_PROJECTION_EVENT_CURSOR_LEN>()?,
            )?,
        )
    }

    fn read_source_sequence_digest(
        &mut self,
    ) -> Result<CanonicalBlockFactsSequenceDigest, WalletProjectionContractError> {
        self.require_u16(
            "wallet source sequence digest version",
            REQUIRED_CANONICAL_SEQUENCE_DIGEST_VERSION,
        )?;
        let sequence_block_count = self.read_u64()?;
        let sequence_digest = self.read_array::<32>()?;
        Ok(
            CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
                CanonicalBlockFactsSequenceDigestVersion::V1,
                sequence_block_count,
                sequence_digest,
            ),
        )
    }

    fn read_block_id(&mut self) -> Result<BlockId, WalletProjectionContractError> {
        Ok(BlockId::new(
            BlockHeight::new(self.read_u32()?),
            BlockHash::from_bytes(self.read_array::<32>()?),
        ))
    }

    fn require_u16(
        &mut self,
        field: &'static str,
        expected: u16,
    ) -> Result<(), WalletProjectionContractError> {
        let encoded = self.read_u16()?;
        if encoded != expected {
            return Err(WalletProjectionContractError::UnsupportedEncodedValue {
                field,
                encoded: u64::from(encoded),
            });
        }
        Ok(())
    }

    fn require_u32(
        &mut self,
        field: &'static str,
        expected: u32,
    ) -> Result<(), WalletProjectionContractError> {
        let encoded = self.read_u32()?;
        if encoded != expected {
            return Err(WalletProjectionContractError::UnsupportedEncodedValue {
                field,
                encoded: u64::from(encoded),
            });
        }
        Ok(())
    }

    fn read_u8(&mut self) -> Result<u8, WalletProjectionContractError> {
        Ok(self.read_array::<1>()?[0])
    }

    fn read_u16(&mut self) -> Result<u16, WalletProjectionContractError> {
        Ok(u16::from_be_bytes(self.read_array::<2>()?))
    }

    fn read_u32(&mut self) -> Result<u32, WalletProjectionContractError> {
        Ok(u32::from_be_bytes(self.read_array::<4>()?))
    }

    fn read_u64(&mut self) -> Result<u64, WalletProjectionContractError> {
        Ok(u64::from_be_bytes(self.read_array::<8>()?))
    }

    fn read_array<const LEN: usize>(&mut self) -> Result<[u8; LEN], WalletProjectionContractError> {
        self.read_bytes(LEN)?.try_into().map_err(|_| {
            WalletProjectionContractError::DurableFieldLengthMismatch {
                field: "wallet store control field",
                expected: LEN,
                actual: self.encoded.len().saturating_sub(self.offset),
            }
        })
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8], WalletProjectionContractError> {
        let end = self.offset.checked_add(len).ok_or(
            WalletProjectionContractError::DurableLengthPrefixMismatch {
                field: "wallet store control",
            },
        )?;
        let Some(bytes) = self.encoded.get(self.offset..end) else {
            return Err(WalletProjectionContractError::DurableValueTooShort {
                field: "wallet store control",
                minimum: end,
                actual: self.encoded.len(),
            });
        };
        self.offset = end;
        Ok(bytes)
    }

    fn finish(self) -> Result<(), WalletProjectionContractError> {
        if self.offset != self.encoded.len() {
            return Err(WalletProjectionContractError::DurableTrailingBytes {
                field: "wallet store control",
            });
        }
        Ok(())
    }
}

fn encode_build_plan(plan: &WalletProjectionBuildPlan, bytes: &mut Vec<u8>) {
    encode_source_position(&plan.target_source_position, bytes);
}

fn encode_build_lease(lease: WalletProjectionBuildLease, bytes: &mut Vec<u8>) {
    bytes.extend_from_slice(&lease.version.to_be_bytes());
    bytes.extend_from_slice(&lease.owner.as_bytes());
    bytes.extend_from_slice(&lease.generation.to_be_bytes());
    bytes.extend_from_slice(&lease.network.id().to_be_bytes());
    bytes.extend_from_slice(&lease.projection_schema_version.to_be_bytes());
    encode_source_position(&lease.pinned_canonical_anchor.source_position(), bytes);
    encode_source_sequence_digest(
        lease.pinned_canonical_anchor.source_sequence_digest(),
        bytes,
    );
    encode_block_id(lease.pinned_canonical_anchor.settled_tip(), bytes);
    bytes.extend_from_slice(
        &lease
            .retained_event_anchor
            .earliest_retained_event_sequence()
            .to_be_bytes(),
    );
    bytes.extend_from_slice(&lease.expires_at.value().to_be_bytes());
}

fn encode_source_position(source_position: &WalletProjectionSourcePosition, bytes: &mut Vec<u8>) {
    bytes.extend_from_slice(&source_position.chain_epoch_id.value().to_be_bytes());
    bytes.extend_from_slice(&source_position.tip.height.value().to_be_bytes());
    bytes.extend_from_slice(&source_position.tip.hash.as_bytes());
    bytes.extend_from_slice(&source_position.event_sequence.to_be_bytes());
    bytes.extend_from_slice(&source_position.event_cursor.as_bytes());
}

fn encode_source_sequence_digest(digest: CanonicalBlockFactsSequenceDigest, bytes: &mut Vec<u8>) {
    bytes.extend_from_slice(&digest.version().value().to_be_bytes());
    bytes.extend_from_slice(&digest.block_count().to_be_bytes());
    bytes.extend_from_slice(&digest.as_bytes());
}

fn encode_block_id(block: BlockId, bytes: &mut Vec<u8>) {
    bytes.extend_from_slice(&block.height.value().to_be_bytes());
    bytes.extend_from_slice(&block.hash.as_bytes());
}

fn validate_build_lease(
    control: &WalletStoreControlRecord,
) -> Result<(), WalletProjectionContractError> {
    let Some(lease) = control.build_lease else {
        if matches!(control.build_state, WalletProjectionBuildState::Ready(_)) {
            return Ok(());
        }
        return Ok(());
    };
    if lease.version != WALLET_PROJECTION_BUILD_LEASE_VERSION {
        return Err(
            WalletProjectionContractError::UnsupportedWalletProjectionBuildLeaseVersion {
                encoded: u64::from(lease.version),
            },
        );
    }
    if lease.projection_schema_version != WALLET_PROJECTION_SCHEMA_VERSION {
        return Err(WalletProjectionContractError::WalletProjectionBuildLeaseSchemaMismatch);
    }
    if lease.network != control.network {
        return Err(WalletProjectionContractError::WalletProjectionBuildLeaseNetworkMismatch);
    }
    if lease.generation != control.writer_generation {
        return Err(WalletProjectionContractError::WalletProjectionBuildLeaseGenerationMismatch);
    }
    let WalletProjectionBuildState::Building(plan) = &control.build_state else {
        return Err(WalletProjectionContractError::ReadyControlRetainsBuildLease);
    };
    if lease.pinned_canonical_anchor.source_position() != plan.target_source_position {
        return Err(
            WalletProjectionContractError::WalletProjectionBuildLeaseCanonicalAnchorMismatch,
        );
    }
    if lease
        .retained_event_anchor
        .earliest_retained_event_sequence()
        > lease
            .pinned_canonical_anchor
            .source_position()
            .event_sequence
    {
        return Err(
            WalletProjectionContractError::WalletProjectionBuildLeaseRetainedEventAnchorMismatch,
        );
    }
    Ok(())
}

fn encode_ready_evidence(
    evidence: &WalletProjectionReadyEvidence,
    supported_reorg_depth: u32,
    bytes: &mut Vec<u8>,
) -> Result<(), WalletProjectionContractError> {
    validate_ready_evidence(evidence, supported_reorg_depth)?;
    encode_source_position(&evidence.source_position, bytes);
    encode_source_sequence_digest(evidence.source_sequence_digest, bytes);
    encode_block_id(evidence.settled_tip, bytes);
    bytes.extend_from_slice(&WALLET_PROJECTION_ACCUMULATOR_VERSION.to_be_bytes());
    bytes.extend_from_slice(evidence.projection_accumulator.as_bytes());
    let counts = evidence.row_counts;
    bytes.extend_from_slice(&counts.transparent_unspent_output_count.to_be_bytes());
    bytes.extend_from_slice(
        &counts
            .transparent_unspent_output_by_address_count
            .to_be_bytes(),
    );
    bytes.extend_from_slice(&counts.transparent_spent_output_count.to_be_bytes());
    bytes.extend_from_slice(&counts.transparent_address_transaction_count.to_be_bytes());
    bytes.extend_from_slice(&counts.transparent_address_balance_count.to_be_bytes());
    bytes.extend_from_slice(&counts.reorg_undo_count.to_be_bytes());
    bytes.extend_from_slice(&evidence.utxo_summary.utxo_count.to_be_bytes());
    bytes.extend_from_slice(&evidence.utxo_summary.total_value_zat.to_be_bytes());
    bytes.extend_from_slice(&evidence.utxo_summary.commitment.scheme().id().to_be_bytes());
    bytes.extend_from_slice(evidence.utxo_summary.commitment.accumulator());
    Ok(())
}

fn validate_ready_evidence(
    evidence: &WalletProjectionReadyEvidence,
    supported_reorg_depth: u32,
) -> Result<(), WalletProjectionContractError> {
    evidence.source_position.validate_event_cursor()?;
    if evidence.projection_digest != evidence.projection_accumulator.display_digest() {
        return Err(WalletProjectionContractError::ProjectionAccumulatorDigestMismatch);
    }
    if evidence.row_counts.transparent_unspent_output_count
        != evidence
            .row_counts
            .transparent_unspent_output_by_address_count
    {
        return Err(WalletProjectionContractError::ReadyUnspentOutputIndexCountMismatch);
    }
    if evidence.row_counts.transparent_unspent_output_count != evidence.utxo_summary.utxo_count {
        return Err(WalletProjectionContractError::ReadyUtxoCountMismatch);
    }
    if evidence.source_sequence_digest.version().value()
        != REQUIRED_CANONICAL_SEQUENCE_DIGEST_VERSION
    {
        return Err(WalletProjectionContractError::ReadySourceSequenceVersionMismatch);
    }
    // A canonical restore may retain a checkpointed suffix whose sequence
    // count starts after the chain's height-zero origin. The authenticated
    // fence binds that prefix; READY only requires that it contains the tip's
    // retained block, not that its count numerically equals block height.
    if evidence.source_sequence_digest.block_count() == 0 {
        return Err(WalletProjectionContractError::ReadySourceSequenceLengthMismatch);
    }
    if evidence.settled_tip.height > evidence.source_position.tip.height
        || (evidence.settled_tip.height == evidence.source_position.tip.height
            && evidence.settled_tip.hash != evidence.source_position.tip.hash)
    {
        return Err(WalletProjectionContractError::ReadySettledTipOutsideSourceRange);
    }
    let required_reorg_undo_count = u64::from(evidence.source_position.tip.height.value())
        .checked_sub(u64::from(evidence.settled_tip.height.value()))
        .ok_or(WalletProjectionContractError::ReadySettledTipOutsideSourceRange)?;
    if required_reorg_undo_count > u64::from(supported_reorg_depth)
        || evidence.row_counts.reorg_undo_count != required_reorg_undo_count
    {
        return Err(WalletProjectionContractError::ReadyReorgUndoCountMismatch);
    }
    if evidence.utxo_summary.commitment.scheme() != UtxoSetCommitmentScheme::LtHash16 {
        return Err(WalletProjectionContractError::ReadyUtxoCommitmentSchemeMismatch);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use zinder_core::{
        BlockHash, BlockHeight, CanonicalBlockFactsDigest, CanonicalBlockFactsDigestVersion,
        CanonicalBlockFactsSequenceDigestBuilder, CanonicalBlockFactsSequenceDigestVersion,
    };

    fn sample_source_position(height: u32) -> WalletProjectionSourcePosition {
        WalletProjectionSourcePosition::new(
            ChainEpochId::new(0x1112_1314_1516_1718),
            BlockId::new(BlockHeight::new(height), BlockHash::from_bytes([0x33; 32])),
            0x2122_2324_2526_2728,
        )
    }

    fn sequence_digest(block_count: u32) -> CanonicalBlockFactsSequenceDigest {
        let mut builder = CanonicalBlockFactsSequenceDigestBuilder::new(
            CanonicalBlockFactsSequenceDigestVersion::V1,
        );
        for index in 0..block_count {
            let digest = CanonicalBlockFactsDigest::from_reference_encoding(
                CanonicalBlockFactsDigestVersion::V1,
                &index.to_be_bytes(),
            );
            assert!(builder.try_append(digest).is_ok());
        }
        builder.finish()
    }

    fn ready_evidence() -> WalletProjectionReadyEvidence {
        let source_position = sample_source_position(1);
        let projection_accumulator = WalletProjectionAccumulator::empty();
        WalletProjectionReadyEvidence {
            source_position,
            source_sequence_digest: sequence_digest(1),
            settled_tip: BlockId::new(BlockHeight::new(0), BlockHash::from_bytes([0x22; 32])),
            projection_digest: projection_accumulator.display_digest(),
            projection_accumulator,
            row_counts: WalletProjectionFamilyRowCounts {
                transparent_unspent_output_count: 1,
                transparent_unspent_output_by_address_count: 1,
                transparent_spent_output_count: 2,
                transparent_address_transaction_count: 3,
                transparent_address_balance_count: 1,
                reorg_undo_count: 1,
            },
            utxo_summary: WalletUtxoSetSummary {
                utxo_count: 1,
                total_value_zat: 9,
                commitment: TransparentUtxoSetCommitment::empty(),
            },
        }
    }

    fn sample_control(build_state: WalletProjectionBuildState) -> WalletStoreControlRecord {
        WalletStoreControlRecord {
            network: Network::ZcashRegtest,
            supported_reorg_depth: 100,
            writer_generation: 0x0102_0304_0506_0708,
            build_lease: None,
            build_state,
        }
    }

    #[test]
    fn control_decode_rejects_the_pre_reset_wallet_projection_identity() {
        let control = sample_control(WalletProjectionBuildState::Building(
            WalletProjectionBuildPlan::complete_history(sample_source_position(1)),
        ));
        let current = control
            .encode()
            .unwrap_or_else(|error| unreachable!("valid building control: {error}"));
        let mut pre_reset = b"wallet-projection".to_vec();
        pre_reset.extend_from_slice(&current[WALLET_PROJECTION_STORE_IDENTITY.len()..]);

        assert!(matches!(
            WalletStoreControlRecord::decode(&pre_reset),
            Err(WalletProjectionContractError::UnsupportedEncodedValue {
                field: "wallet projection schema version",
                encoded: 0x2d70,
            })
        ));
    }

    #[test]
    fn building_control_has_exact_version_one_bytes() {
        let control = sample_control(WalletProjectionBuildState::Building(
            WalletProjectionBuildPlan::complete_history(sample_source_position(0x0a0b_0c0d)),
        ));
        let encoded = control
            .encode()
            .unwrap_or_else(|error| unreachable!("valid building control: {error}"));
        assert_eq!(
            hex::encode(&encoded),
            concat!(
                "77616c6c6574",
                "0001",
                "0002",
                "00000003",
                "0006",
                "00000001",
                "0001",
                "0001",
                "00000064",
                "0102030405060708",
                "00",
                "01",
                "1112131415161718",
                "0a0b0c0d",
                "3333333333333333333333333333333333333333333333333333333333333333",
                "2122232425262728",
                "012122232425262728"
            )
        );
        assert_eq!(WalletStoreControlRecord::decode(&encoded), Ok(control));
    }

    #[test]
    fn ready_control_has_exact_version_one_bytes() {
        let evidence = ready_evidence();
        let control = sample_control(WalletProjectionBuildState::Ready(evidence.clone()));
        let mut expected = Vec::new();
        expected.extend_from_slice(WALLET_PROJECTION_STORE_IDENTITY);
        expected.extend_from_slice(&WALLET_PROJECTION_SCHEMA_VERSION.to_be_bytes());
        expected.extend_from_slice(&WALLET_PROJECTION_VALUE_ENCODING_VERSION.to_be_bytes());
        expected.extend_from_slice(&Network::ZcashRegtest.id().to_be_bytes());
        expected.extend_from_slice(&REQUIRED_CANONICAL_STORE_SCHEMA_VERSION.to_be_bytes());
        expected.extend_from_slice(&REQUIRED_CANONICAL_REPLAY_FORMAT_VERSION.to_be_bytes());
        expected.extend_from_slice(&REQUIRED_CANONICAL_FACTS_DIGEST_VERSION.to_be_bytes());
        expected.extend_from_slice(&REQUIRED_CANONICAL_SEQUENCE_DIGEST_VERSION.to_be_bytes());
        expected.extend_from_slice(&100u32.to_be_bytes());
        expected.extend_from_slice(&0x0102_0304_0506_0708u64.to_be_bytes());
        expected.push(0);
        expected.push(2);
        expected.extend_from_slice(
            &evidence
                .source_position
                .chain_epoch_id
                .value()
                .to_be_bytes(),
        );
        expected.extend_from_slice(&evidence.source_position.tip.height.value().to_be_bytes());
        expected.extend_from_slice(&evidence.source_position.tip.hash.as_bytes());
        expected.extend_from_slice(&evidence.source_position.event_sequence.to_be_bytes());
        expected.extend_from_slice(&evidence.source_position.event_cursor.as_bytes());
        encode_source_sequence_digest(evidence.source_sequence_digest, &mut expected);
        encode_block_id(evidence.settled_tip, &mut expected);
        expected.extend_from_slice(&WALLET_PROJECTION_ACCUMULATOR_VERSION.to_be_bytes());
        expected.extend_from_slice(evidence.projection_accumulator.as_bytes());
        let counts = evidence.row_counts;
        expected.extend_from_slice(&counts.transparent_unspent_output_count.to_be_bytes());
        expected.extend_from_slice(
            &counts
                .transparent_unspent_output_by_address_count
                .to_be_bytes(),
        );
        expected.extend_from_slice(&counts.transparent_spent_output_count.to_be_bytes());
        expected.extend_from_slice(&counts.transparent_address_transaction_count.to_be_bytes());
        expected.extend_from_slice(&counts.transparent_address_balance_count.to_be_bytes());
        expected.extend_from_slice(&counts.reorg_undo_count.to_be_bytes());
        expected.extend_from_slice(&evidence.utxo_summary.utxo_count.to_be_bytes());
        expected.extend_from_slice(&evidence.utxo_summary.total_value_zat.to_be_bytes());
        expected.extend_from_slice(&evidence.utxo_summary.commitment.scheme().id().to_be_bytes());
        expected.extend_from_slice(evidence.utxo_summary.commitment.accumulator());

        assert_eq!(control.encode(), Ok(expected.clone()));
        assert_eq!(WalletStoreControlRecord::decode(&expected), Ok(control));
    }

    #[test]
    fn build_owner_displays_as_its_configured_hexadecimal_value() {
        let owner = WalletProjectionBuildOwner::from_bytes(
            0x0011_2233_4455_6677_8899_aabb_ccdd_eeff_u128.to_be_bytes(),
        );
        assert_eq!(owner.to_string(), "00112233445566778899aabbccddeeff");
    }

    #[test]
    fn building_control_round_trips_a_versioned_projection_build_lease() {
        let source = sample_source_position(1);
        let source_identity = WalletCanonicalSourceIdentity::new(
            source,
            sequence_digest(1),
            BlockId::new(BlockHeight::new(0), BlockHash::from_bytes([0x22; 32])),
        );
        let lease = WalletProjectionBuildLease::from_request(
            WalletProjectionBuildLeaseRequest::new(
                WalletProjectionBuildOwner::from_bytes([0x55; 16]),
                source_identity,
                WalletProjectionRetainedEventAnchor::new(1),
                UnixTimestampMillis::new(900),
            ),
            7,
            Network::ZcashRegtest,
        );
        let control = WalletStoreControlRecord {
            network: Network::ZcashRegtest,
            supported_reorg_depth: 0,
            writer_generation: 7,
            build_lease: Some(lease),
            build_state: WalletProjectionBuildState::Building(
                WalletProjectionBuildPlan::complete_history(source),
            ),
        };
        let encoded = control
            .encode()
            .unwrap_or_else(|error| unreachable!("valid leased control: {error}"));
        let decoded = WalletStoreControlRecord::decode(&encoded)
            .unwrap_or_else(|error| unreachable!("leased control decode: {error}"));
        assert_eq!(decoded, control);
        assert_eq!(lease.version(), WALLET_PROJECTION_BUILD_LEASE_VERSION);
        assert_eq!(
            lease.projection_schema_version(),
            WALLET_PROJECTION_SCHEMA_VERSION
        );
        assert_eq!(lease.pinned_canonical_anchor(), source_identity);
    }

    #[test]
    fn control_decode_rejects_unknown_versions_states_and_trailing_bytes() {
        let control = sample_control(WalletProjectionBuildState::Building(
            WalletProjectionBuildPlan::complete_history(sample_source_position(1)),
        ));
        let encoded = control
            .encode()
            .unwrap_or_else(|error| unreachable!("valid building control: {error}"));

        let mut wrong_schema = encoded.clone();
        let schema_offset = WALLET_PROJECTION_STORE_IDENTITY.len();
        wrong_schema[schema_offset..schema_offset + 2].copy_from_slice(&5_u16.to_be_bytes());
        assert!(matches!(
            WalletStoreControlRecord::decode(&wrong_schema),
            Err(WalletProjectionContractError::UnsupportedEncodedValue { .. })
        ));

        let mut unknown_state = encoded.clone();
        let state_offset =
            WALLET_PROJECTION_STORE_IDENTITY.len() + 2 + 2 + 4 + 2 + 4 + 2 + 2 + 4 + 8;
        unknown_state[state_offset] = 9;
        assert!(matches!(
            WalletStoreControlRecord::decode(&unknown_state),
            Err(WalletProjectionContractError::UnsupportedEncodedValue { .. })
        ));

        let mut trailing = encoded;
        trailing.push(0);
        assert!(matches!(
            WalletStoreControlRecord::decode(&trailing),
            Err(WalletProjectionContractError::DurableTrailingBytes { .. })
        ));
    }

    #[test]

    fn ready_control_rejects_inconsistent_evidence() {
        let mut cases = Vec::new();

        let mut evidence = ready_evidence();
        evidence
            .row_counts
            .transparent_unspent_output_by_address_count = 2;
        cases.push((
            evidence,
            WalletProjectionContractError::ReadyUnspentOutputIndexCountMismatch,
        ));

        let mut evidence = ready_evidence();
        evidence.utxo_summary.utxo_count = 2;
        cases.push((
            evidence,
            WalletProjectionContractError::ReadyUtxoCountMismatch,
        ));

        let mut evidence = ready_evidence();
        evidence.row_counts.reorg_undo_count = 0;
        cases.push((
            evidence,
            WalletProjectionContractError::ReadyReorgUndoCountMismatch,
        ));

        let mut evidence = ready_evidence();
        evidence.source_sequence_digest = sequence_digest(0);
        cases.push((
            evidence,
            WalletProjectionContractError::ReadySourceSequenceLengthMismatch,
        ));

        for (evidence, expected_error) in cases {
            let control = sample_control(WalletProjectionBuildState::Ready(evidence));
            assert_eq!(control.encode(), Err(expected_error));
        }
    }

    #[test]
    fn ready_control_accepts_a_checkpointed_source_sequence() {
        let mut evidence = ready_evidence();
        evidence.source_position = WalletProjectionSourcePosition::new(
            evidence.source_position.chain_epoch_id,
            BlockId::new(BlockHeight::new(100), BlockHash::from_bytes([0x44; 32])),
            evidence.source_position.event_sequence,
        );
        evidence.source_sequence_digest = sequence_digest(1);
        evidence.settled_tip =
            BlockId::new(BlockHeight::new(99), BlockHash::from_bytes([0x43; 32]));
        let control = WalletStoreControlRecord {
            network: Network::ZcashRegtest,
            supported_reorg_depth: 1,
            writer_generation: 1,
            build_lease: None,
            build_state: WalletProjectionBuildState::Ready(evidence),
        };

        assert!(control.encode().is_ok());
    }

    #[test]
    fn ready_control_accepts_an_unsettled_suffix_bounded_by_the_settled_tip() {
        let mut evidence = ready_evidence();
        evidence.source_position = sample_source_position(3);
        evidence.source_sequence_digest = sequence_digest(3);
        evidence.settled_tip = BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([0x32; 32]));
        evidence.row_counts.reorg_undo_count = 1;
        let control = WalletStoreControlRecord {
            network: Network::ZcashRegtest,
            supported_reorg_depth: 2,
            writer_generation: 1,
            build_lease: None,
            build_state: WalletProjectionBuildState::Ready(evidence),
        };

        assert!(control.encode().is_ok());
    }
}
