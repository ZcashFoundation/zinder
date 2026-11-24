use zinder_core::{
    BlockFinalNoteCommitmentRoots, BlockHash, BlockHeaderArtifact, BlockHeight,
    CommitmentTreeFrontier, CommitmentTreeFrontierValidationError, CommitmentTreeFrontiers,
    CommitmentTreeFrontiersError, FinalNoteCommitmentRoot,
    MAX_COMMITMENT_TREE_FRONTIER_FINAL_STATE_BYTES, ShieldedProtocol, TransactionLocation,
    wire::encode_height_key_ascending,
};

use thiserror::Error;

use crate::canonical_store::served_tree_state::{
    ServedFrontier, ServedFrontiers, framed_tree_size,
};

pub(in crate::canonical_store) const BLOCK_HEADER_VALUE_LEN: usize = 184;
pub(super) const BLOCK_HASH_INDEX_RECORD_LEN: usize = 36;
pub(super) const TRANSACTION_LOCATION_RECORD_LEN: usize = 32 + TRANSACTION_LOCATION_VALUE_LEN;
pub(in crate::canonical_store) const TRANSACTION_LOCATION_VALUE_LEN: usize = 72;

const SAPLING_PRESENCE_BIT: u8 = 1 << 0;
const ORCHARD_PRESENCE_BIT: u8 = 1 << 1;
const IRONWOOD_PRESENCE_BIT: u8 = 1 << 2;
const SHIELDED_PROTOCOL_PRESENCE_MASK: u8 =
    SAPLING_PRESENCE_BIT | ORCHARD_PRESENCE_BIT | IRONWOOD_PRESENCE_BIT;
const TREE_STATE_CHECKPOINT_FIXED_VALUE_LEN: usize = 4 + 1;

#[derive(Clone, Debug, Eq, Error, PartialEq)]
/// Rejection reason for one malformed clean-v1 canonical-family value.
pub(in crate::canonical_store) enum CanonicalFamilyValueDecodeError {
    /// The value ended before one fixed or length-delimited field was complete.
    #[error(
        "{family} value ended while decoding {field}: required {required_bytes} bytes, found {remaining_bytes}"
    )]
    Truncated {
        /// Canonical column family being decoded.
        family: &'static str,
        /// Field that could not be read completely.
        field: &'static str,
        /// Bytes required for the field.
        required_bytes: usize,
        /// Bytes still available in the value.
        remaining_bytes: usize,
    },
    /// Reserved bits were set in the shielded-pool presence bitmap.
    #[error("{family} value has unknown shielded-pool presence bits {presence_bitmap:#010b}")]
    UnknownPresenceBits {
        /// Canonical column family being decoded.
        family: &'static str,
        /// Invalid presence bitmap.
        presence_bitmap: u8,
    },
    /// A frontier length exceeded the v1 domain admission bound.
    #[error(
        "tree_state_checkpoint {protocol:?} frontier is {encoded_bytes} bytes; maximum is {MAX_COMMITMENT_TREE_FRONTIER_FINAL_STATE_BYTES}"
    )]
    FrontierTooLarge {
        /// Shielded pool whose frontier was rejected.
        protocol: ShieldedProtocol,
        /// Frontier length declared by the encoded value.
        encoded_bytes: usize,
    },
    /// A frontier's encoding was rejected, or, by a decode that revalidates
    /// frontiers, its claimed root.
    #[error("tree_state_checkpoint {protocol:?} frontier is invalid: {source}")]
    InvalidFrontier {
        /// Shielded pool whose frontier was rejected.
        protocol: ShieldedProtocol,
        #[source]
        source: CommitmentTreeFrontierValidationError,
    },
    /// A decoded frontier was validated for a pool other than the one whose
    /// position holds it.
    #[error("tree_state_checkpoint frontiers are misplaced: {source}")]
    MisplacedFrontier {
        #[source]
        source: CommitmentTreeFrontiersError,
    },
    /// Bytes remained after every field selected by the presence bitmap.
    #[error("{family} value has {trailing_bytes} trailing bytes")]
    TrailingBytes {
        /// Canonical column family being decoded.
        family: &'static str,
        /// Bytes remaining after the exact value layout.
        trailing_bytes: usize,
    },
}

impl CanonicalFamilyValueDecodeError {
    /// Returns the pool whose frontier was rejected, or `None` when the
    /// value failed outside any one frontier.
    pub(in crate::canonical_store) const fn protocol(&self) -> Option<ShieldedProtocol> {
        match self {
            Self::FrontierTooLarge { protocol, .. } | Self::InvalidFrontier { protocol, .. } => {
                Some(*protocol)
            }
            Self::MisplacedFrontier {
                source: CommitmentTreeFrontiersError::PoolMismatch { pool, .. },
            } => Some(*pool),
            Self::Truncated { .. }
            | Self::UnknownPresenceBits { .. }
            | Self::TrailingBytes { .. } => None,
        }
    }
}

pub(in crate::canonical_store) fn encode_block_header(
    header: &BlockHeaderArtifact,
) -> [u8; BLOCK_HEADER_VALUE_LEN] {
    let mut encoded_header = [0_u8; BLOCK_HEADER_VALUE_LEN];
    encoded_header[..32].copy_from_slice(&header.block_hash.as_bytes());
    encoded_header[32..64].copy_from_slice(&header.parent_hash.as_bytes());
    encoded_header[64..96].copy_from_slice(&header.merkle_root_hash);
    encoded_header[96..128].copy_from_slice(&header.commitment_bytes);
    encoded_header[128..136].copy_from_slice(&header.block_time.to_le_bytes());
    encoded_header[136..140].copy_from_slice(&header.bits.to_le_bytes());
    encoded_header[140..172].copy_from_slice(&header.nonce);
    encoded_header[172..176].copy_from_slice(&header.version.to_le_bytes());
    encoded_header[176..184].copy_from_slice(&header.block_size_bytes.to_le_bytes());
    encoded_header
}

pub(in crate::canonical_store) fn encode_transaction_position(
    height: BlockHeight,
    transaction_index: u32,
) -> [u8; 8] {
    let mut key = [0_u8; 8];
    key[..4].copy_from_slice(&encode_height_key_ascending(height));
    key[4..].copy_from_slice(&transaction_index.to_be_bytes());
    key
}

pub(in crate::canonical_store) fn encode_block_position(height: BlockHeight) -> [u8; 4] {
    encode_height_key_ascending(height)
}

pub(in crate::canonical_store) fn encode_block_hash_location(
    block_hash: BlockHash,
    height: BlockHeight,
) -> [u8; 36] {
    let mut row = [0_u8; BLOCK_HASH_INDEX_RECORD_LEN];
    row[..32].copy_from_slice(&block_hash.as_bytes());
    row[32..].copy_from_slice(&encode_block_position(height));
    row
}

pub(in crate::canonical_store) fn encode_transaction_location(
    location: TransactionLocation,
    serialized_bytes_sha256: [u8; 32],
) -> [u8; TRANSACTION_LOCATION_RECORD_LEN] {
    let mut row = [0_u8; TRANSACTION_LOCATION_RECORD_LEN];
    row[..32].copy_from_slice(&location.transaction_id.as_bytes());
    row[32..36].copy_from_slice(&encode_block_position(location.block_height));
    row[36..68].copy_from_slice(&location.block_hash.as_bytes());
    row[68..72].copy_from_slice(&location.tx_index_in_block.to_be_bytes());
    row[72..].copy_from_slice(&serialized_bytes_sha256);
    row
}

/// Encodes one v1 tree-state checkpoint value without key-owned block identity.
pub(in crate::canonical_store) fn encode_tree_state_checkpoint(
    block_time_seconds: u32,
    frontiers: &CommitmentTreeFrontiers,
) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(
        TREE_STATE_CHECKPOINT_FIXED_VALUE_LEN + encoded_frontiers_len(frontiers),
    );
    encoded.extend_from_slice(&block_time_seconds.to_le_bytes());
    encoded.push(frontier_presence_bitmap(frontiers));
    for protocol in shielded_protocols() {
        let Some(frontier) = frontiers.get(protocol) else {
            continue;
        };
        encoded.extend_from_slice(&frontier.final_root().as_bytes());
        let final_state_len = u16::try_from(frontier.final_state_bytes().len()).unwrap_or(u16::MAX);
        encoded.extend_from_slice(&final_state_len.to_le_bytes());
        encoded.extend_from_slice(frontier.final_state_bytes());
    }
    encoded
}

/// Stored final note-commitment roots extracted from one v1
/// `tree_state_checkpoint` row after checking frontier framing alone.
///
/// Carries no frontier bytes, so nothing that seeds a commitment-tree
/// accumulator or writes canonical tree state can take it as input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::canonical_store) struct StoredTreeStateRoots {
    /// Block timestamp in Unix seconds recorded by the checkpoint row.
    pub(in crate::canonical_store) block_time_seconds: u32,
    /// Stored Sapling final root, when the Sapling presence bit is set.
    pub(in crate::canonical_store) sapling: Option<FinalNoteCommitmentRoot>,
    /// Stored Orchard final root, when the Orchard presence bit is set.
    pub(in crate::canonical_store) orchard: Option<FinalNoteCommitmentRoot>,
    /// Stored Ironwood final root, when the Ironwood presence bit is set.
    pub(in crate::canonical_store) ironwood: Option<FinalNoteCommitmentRoot>,
}

impl StoredTreeStateRoots {
    /// Reports whether the checkpoint row carried a frontier for `protocol`.
    pub(in crate::canonical_store) const fn has_pool(self, protocol: ShieldedProtocol) -> bool {
        match protocol {
            ShieldedProtocol::Sapling => self.sapling.is_some(),
            ShieldedProtocol::Orchard => self.orchard.is_some(),
            ShieldedProtocol::Ironwood => self.ironwood.is_some(),
            // A store admits only these three pools.
            _ => false,
        }
    }

    /// Pairs the stored roots with the canonical header at the checkpoint height.
    pub(in crate::canonical_store) const fn into_block_roots(
        self,
        height: BlockHeight,
        block_hash: BlockHash,
    ) -> BlockFinalNoteCommitmentRoots {
        BlockFinalNoteCommitmentRoots::new(
            height,
            block_hash,
            self.sapling,
            self.orchard,
            self.ironwood,
        )
    }
}

/// Decodes and revalidates one exact v1 tree-state checkpoint value.
///
/// Each frontier is parsed with the official Zcash codec, must be its
/// canonical encoding, and must derive its stored root, so the result can seed
/// further tree state or certify stored rows.
pub(in crate::canonical_store) fn decode_tree_state_checkpoint(
    encoded: &[u8],
) -> Result<(u32, CommitmentTreeFrontiers), CanonicalFamilyValueDecodeError> {
    let (block_time_seconds, [sapling, orchard, ironwood]) =
        decode_checkpoint_frontiers(encoded, |protocol, final_root, final_state_bytes| {
            CommitmentTreeFrontier::from_canonical_final_state(
                protocol,
                final_root,
                final_state_bytes.to_vec(),
            )
        })?;
    let frontiers = CommitmentTreeFrontiers::from_validated_parts(sapling, orchard, ironwood)
        .map_err(|source| CanonicalFamilyValueDecodeError::MisplacedFrontier { source })?;
    Ok((block_time_seconds, frontiers))
}

/// Decodes one exact v1 tree-state checkpoint value for serving.
///
/// Each frontier's framing is checked and its stored root skipped, without
/// decoding or hashing tree nodes.
pub(in crate::canonical_store) fn decode_served_tree_state_checkpoint(
    encoded: &[u8],
) -> Result<(u32, ServedFrontiers), CanonicalFamilyValueDecodeError> {
    let (block_time_seconds, [sapling, orchard, ironwood]) =
        decode_checkpoint_frontiers(encoded, |_, _, final_state_bytes| {
            ServedFrontier::from_stored_final_state(final_state_bytes.to_vec())
        })?;
    Ok((
        block_time_seconds,
        ServedFrontiers::new(sapling, orchard, ironwood),
    ))
}

/// Decodes the stored final note-commitment roots and block time from one
/// exact v1 `tree_state_checkpoint` value without decoding or hashing tree
/// nodes.
///
/// Every present frontier's framing is checked with [`framed_tree_size`],
/// rejecting truncated, oversized, non-canonical, or trailing-byte encodings
/// without allocating frontier buffers or re-deriving Merkle roots.
pub(in crate::canonical_store) fn decode_stored_tree_state_roots(
    encoded: &[u8],
) -> Result<StoredTreeStateRoots, CanonicalFamilyValueDecodeError> {
    let (block_time_seconds, [sapling, orchard, ironwood]) =
        decode_checkpoint_frontiers(encoded, |_, final_root, final_state_bytes| {
            let _tree_size = framed_tree_size(final_state_bytes)?;
            Ok(final_root)
        })?;
    Ok(StoredTreeStateRoots {
        block_time_seconds,
        sapling,
        orchard,
        ironwood,
    })
}

/// Decodes the exact v1 layout of a tree-state checkpoint value, building
/// each present frontier from its pool, stored root, and `finalState` bytes
/// with `frontier`.
fn decode_checkpoint_frontiers<Frontier>(
    encoded: &[u8],
    mut frontier: impl FnMut(
        ShieldedProtocol,
        FinalNoteCommitmentRoot,
        &[u8],
    ) -> Result<Frontier, CommitmentTreeFrontierValidationError>,
) -> Result<(u32, [Option<Frontier>; 3]), CanonicalFamilyValueDecodeError> {
    let mut decoder = ValueDecoder::new("tree_state_checkpoint", encoded);
    let block_time_seconds = decoder.read_u32("block time")?;
    let presence_bitmap = decoder.read_presence_bitmap()?;
    let mut decode =
        |protocol| decode_frontier(&mut decoder, presence_bitmap, protocol, &mut frontier);
    let sapling = decode(ShieldedProtocol::Sapling)?;
    let orchard = decode(ShieldedProtocol::Orchard)?;
    let ironwood = decode(ShieldedProtocol::Ironwood)?;
    decoder.reject_trailing_bytes()?;
    Ok((block_time_seconds, [sapling, orchard, ironwood]))
}

/// Encodes one v1 block-root value without key/header-owned block identity.
pub(in crate::canonical_store) fn encode_block_final_note_commitment_roots(
    roots: &BlockFinalNoteCommitmentRoots,
) -> Vec<u8> {
    let presence_bitmap = optional_root_presence_bitmap(roots);
    let mut encoded = Vec::with_capacity(1 + 3 * 32);
    encoded.push(presence_bitmap);
    for root in [roots.sapling, roots.orchard, roots.ironwood]
        .into_iter()
        .flatten()
    {
        encoded.extend_from_slice(&root.as_bytes());
    }
    encoded
}

/// Decodes one exact v1 block-root value and restores key/header-owned identity.
pub(in crate::canonical_store) fn decode_block_final_note_commitment_roots(
    height: BlockHeight,
    block_hash: BlockHash,
    encoded: &[u8],
) -> Result<BlockFinalNoteCommitmentRoots, CanonicalFamilyValueDecodeError> {
    let mut decoder = ValueDecoder::new("block_final_note_commitment_roots", encoded);
    let presence_bitmap = decoder.read_presence_bitmap()?;
    let sapling = decoder.read_optional_root(presence_bitmap, ShieldedProtocol::Sapling)?;
    let orchard = decoder.read_optional_root(presence_bitmap, ShieldedProtocol::Orchard)?;
    let ironwood = decoder.read_optional_root(presence_bitmap, ShieldedProtocol::Ironwood)?;
    decoder.reject_trailing_bytes()?;
    Ok(BlockFinalNoteCommitmentRoots::new(
        height, block_hash, sapling, orchard, ironwood,
    ))
}

fn encoded_frontiers_len(frontiers: &CommitmentTreeFrontiers) -> usize {
    shielded_protocols()
        .into_iter()
        .filter_map(|protocol| frontiers.get(protocol))
        .map(|frontier| 32 + 2 + frontier.final_state_bytes().len())
        .sum()
}

fn frontier_presence_bitmap(frontiers: &CommitmentTreeFrontiers) -> u8 {
    shielded_protocols()
        .into_iter()
        .filter(|protocol| frontiers.get(*protocol).is_some())
        .fold(0, |bitmap, protocol| {
            bitmap | protocol_presence_bit(protocol)
        })
}

const fn optional_root_presence_bitmap(roots: &BlockFinalNoteCommitmentRoots) -> u8 {
    let mut bitmap = 0;
    if roots.sapling.is_some() {
        bitmap |= SAPLING_PRESENCE_BIT;
    }
    if roots.orchard.is_some() {
        bitmap |= ORCHARD_PRESENCE_BIT;
    }
    if roots.ironwood.is_some() {
        bitmap |= IRONWOOD_PRESENCE_BIT;
    }
    bitmap
}

const fn protocol_presence_bit(protocol: ShieldedProtocol) -> u8 {
    match protocol {
        ShieldedProtocol::Sapling => SAPLING_PRESENCE_BIT,
        ShieldedProtocol::Orchard => ORCHARD_PRESENCE_BIT,
        ShieldedProtocol::Ironwood => IRONWOOD_PRESENCE_BIT,
        // Every caller supplies one value from `shielded_protocols`; future
        // protocols require an explicit v1 schema decision before admission.
        _ => 0,
    }
}

const fn shielded_protocols() -> [ShieldedProtocol; 3] {
    [
        ShieldedProtocol::Sapling,
        ShieldedProtocol::Orchard,
        ShieldedProtocol::Ironwood,
    ]
}

fn decode_frontier<Frontier>(
    decoder: &mut ValueDecoder<'_>,
    presence_bitmap: u8,
    protocol: ShieldedProtocol,
    frontier: &mut impl FnMut(
        ShieldedProtocol,
        FinalNoteCommitmentRoot,
        &[u8],
    ) -> Result<Frontier, CommitmentTreeFrontierValidationError>,
) -> Result<Option<Frontier>, CanonicalFamilyValueDecodeError> {
    if presence_bitmap & protocol_presence_bit(protocol) == 0 {
        return Ok(None);
    }
    let final_root = FinalNoteCommitmentRoot::from_bytes(
        decoder.read_array::<32>("commitment-tree frontier root")?,
    );
    let final_state_len = usize::from(decoder.read_u16("commitment-tree frontier length")?);
    if final_state_len > MAX_COMMITMENT_TREE_FRONTIER_FINAL_STATE_BYTES {
        return Err(CanonicalFamilyValueDecodeError::FrontierTooLarge {
            protocol,
            encoded_bytes: final_state_len,
        });
    }
    let final_state_bytes =
        decoder.read_bytes(final_state_len, "commitment-tree frontier bytes")?;
    frontier(protocol, final_root, final_state_bytes)
        .map(Some)
        .map_err(|source| CanonicalFamilyValueDecodeError::InvalidFrontier { protocol, source })
}

struct ValueDecoder<'encoded> {
    family: &'static str,
    encoded: &'encoded [u8],
    position: usize,
}

impl<'encoded> ValueDecoder<'encoded> {
    const fn new(family: &'static str, encoded: &'encoded [u8]) -> Self {
        Self {
            family,
            encoded,
            position: 0,
        }
    }

    fn read_presence_bitmap(&mut self) -> Result<u8, CanonicalFamilyValueDecodeError> {
        let presence_bitmap = self.read_u8("shielded-pool presence bitmap")?;
        if presence_bitmap & !SHIELDED_PROTOCOL_PRESENCE_MASK != 0 {
            return Err(CanonicalFamilyValueDecodeError::UnknownPresenceBits {
                family: self.family,
                presence_bitmap,
            });
        }
        Ok(presence_bitmap)
    }

    fn read_optional_root(
        &mut self,
        presence_bitmap: u8,
        protocol: ShieldedProtocol,
    ) -> Result<Option<FinalNoteCommitmentRoot>, CanonicalFamilyValueDecodeError> {
        if presence_bitmap & protocol_presence_bit(protocol) == 0 {
            return Ok(None);
        }
        Ok(Some(FinalNoteCommitmentRoot::from_bytes(
            self.read_array::<32>("final note-commitment root")?,
        )))
    }

    fn read_u8(&mut self, field: &'static str) -> Result<u8, CanonicalFamilyValueDecodeError> {
        Ok(self.read_array::<1>(field)?[0])
    }

    fn read_u16(&mut self, field: &'static str) -> Result<u16, CanonicalFamilyValueDecodeError> {
        Ok(u16::from_le_bytes(self.read_array(field)?))
    }

    fn read_u32(&mut self, field: &'static str) -> Result<u32, CanonicalFamilyValueDecodeError> {
        Ok(u32::from_le_bytes(self.read_array(field)?))
    }

    fn read_array<const LENGTH: usize>(
        &mut self,
        field: &'static str,
    ) -> Result<[u8; LENGTH], CanonicalFamilyValueDecodeError> {
        let bytes = self.read_bytes(LENGTH, field)?;
        let mut array = [0; LENGTH];
        array.copy_from_slice(bytes);
        Ok(array)
    }

    fn read_bytes(
        &mut self,
        byte_count: usize,
        field: &'static str,
    ) -> Result<&'encoded [u8], CanonicalFamilyValueDecodeError> {
        let remaining_bytes = self.encoded.len().saturating_sub(self.position);
        if remaining_bytes < byte_count {
            return Err(CanonicalFamilyValueDecodeError::Truncated {
                family: self.family,
                field,
                required_bytes: byte_count,
                remaining_bytes,
            });
        }
        let end = self.position + byte_count;
        let bytes = &self.encoded[self.position..end];
        self.position = end;
        Ok(bytes)
    }

    fn reject_trailing_bytes(self) -> Result<(), CanonicalFamilyValueDecodeError> {
        let trailing_bytes = self.encoded.len().saturating_sub(self.position);
        if trailing_bytes != 0 {
            return Err(CanonicalFamilyValueDecodeError::TrailingBytes {
                family: self.family,
                trailing_bytes,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use zinder_core::{
        BlockHash, BlockHeaderArtifact, BlockHeight, CommitmentTreeFrontier,
        CommitmentTreeFrontierValidationError, CommitmentTreeFrontiers, FinalNoteCommitmentRoot,
        ShieldedProtocol,
    };

    use super::*;

    #[test]
    fn header_v1_encoding_has_known_field_offsets() {
        let encoded = encode_block_header(&BlockHeaderArtifact::new(
            BlockHeight::new(7),
            BlockHash::from_bytes([1; 32]),
            BlockHash::from_bytes([2; 32]),
            [3; 32],
            [4; 32],
            0x0102_0304_0506_0708,
            0x1112_1314,
            [5; 32],
            0x2122_2324,
            0x3132_3334_3536_3738,
        ));

        assert_eq!(&encoded[..32], &[1; 32]);
        assert_eq!(&encoded[32..64], &[2; 32]);
        assert_eq!(&encoded[64..96], &[3; 32]);
        assert_eq!(&encoded[96..128], &[4; 32]);
        assert_eq!(&encoded[128..136], &[8, 7, 6, 5, 4, 3, 2, 1]);
        assert_eq!(&encoded[136..140], &[0x14, 0x13, 0x12, 0x11]);
        assert_eq!(&encoded[140..172], &[5; 32]);
        assert_eq!(&encoded[172..176], &[0x24, 0x23, 0x22, 0x21]);
        assert_eq!(
            &encoded[176..184],
            &[0x38, 0x37, 0x36, 0x35, 0x34, 0x33, 0x32, 0x31]
        );
    }

    #[test]
    fn transaction_position_v1_encoding_sorts_by_height_then_index() {
        assert_eq!(
            encode_transaction_position(BlockHeight::new(0x0102_0304), 0x1112_1314),
            [1, 2, 3, 4, 0x11, 0x12, 0x13, 0x14]
        );
    }

    #[test]
    fn direct_index_and_height_rows_have_known_v1_bytes() {
        let height = BlockHeight::new(0x0102_0304);
        let block_hash = BlockHash::from_bytes([7; 32]);
        let location = zinder_core::TransactionLocation::new(
            zinder_core::TransactionId::from_bytes([8; 32]),
            height,
            block_hash,
            0x1112_1314,
        );

        assert_eq!(encode_block_position(height), [1, 2, 3, 4]);
        let block_hash_location = encode_block_hash_location(block_hash, height);
        assert_eq!(&block_hash_location[..32], &[7; 32]);
        assert_eq!(&block_hash_location[32..], &[1, 2, 3, 4]);
        let encoded_location = encode_transaction_location(location, [9; 32]);
        assert_eq!(&encoded_location[..32], &[8; 32]);
        assert_eq!(&encoded_location[32..36], &[1, 2, 3, 4]);
        assert_eq!(&encoded_location[36..68], &[7; 32]);
        assert_eq!(&encoded_location[68..72], &[0x11, 0x12, 0x13, 0x14]);
        assert_eq!(&encoded_location[72..], &[9; 32]);
    }

    #[test]
    fn tree_state_checkpoint_v1_has_exact_known_bytes_and_round_trips()
    -> Result<(), Box<dyn std::error::Error>> {
        let sapling = CommitmentTreeFrontier::empty(ShieldedProtocol::Sapling);
        let orchard = CommitmentTreeFrontier::empty(ShieldedProtocol::Orchard);
        let frontiers = CommitmentTreeFrontiers::from_validated_parts(
            Some(sapling.clone()),
            Some(orchard.clone()),
            None,
        )?;

        let encoded = encode_tree_state_checkpoint(0x0102_0304, &frontiers);
        let mut expected = vec![4, 3, 2, 1, SAPLING_PRESENCE_BIT | ORCHARD_PRESENCE_BIT];
        expected.extend_from_slice(&sapling.final_root().as_bytes());
        expected.extend_from_slice(&[3, 0]);
        expected.extend_from_slice(&[0, 0, 0]);
        expected.extend_from_slice(&orchard.final_root().as_bytes());
        expected.extend_from_slice(&[3, 0]);
        expected.extend_from_slice(&[0, 0, 0]);

        assert_eq!(encoded, expected);
        assert_eq!(
            decode_tree_state_checkpoint(&encoded),
            Ok((0x0102_0304, frontiers))
        );
        assert_eq!(
            decode_served_tree_state_checkpoint(&encoded),
            Ok((
                0x0102_0304,
                ServedFrontiers::new(
                    Some(ServedFrontier::from(&sapling)),
                    Some(ServedFrontier::from(&orchard)),
                    None,
                )
            ))
        );
        assert_eq!(
            decode_stored_tree_state_roots(&encoded),
            Ok(StoredTreeStateRoots {
                block_time_seconds: 0x0102_0304,
                sapling: Some(sapling.final_root()),
                orchard: Some(orchard.final_root()),
                ironwood: None,
            })
        );
        assert_eq!(
            encode_tree_state_checkpoint(0x0102_0304, &CommitmentTreeFrontiers::default()),
            [4, 3, 2, 1, 0]
        );
        assert_eq!(
            decode_stored_tree_state_roots(&[4, 3, 2, 1, 0]),
            Ok(StoredTreeStateRoots {
                block_time_seconds: 0x0102_0304,
                sapling: None,
                orchard: None,
                ironwood: None,
            })
        );
        Ok(())
    }

    #[test]
    fn block_final_note_commitment_roots_v1_has_exact_known_bytes_and_round_trips() {
        let height = BlockHeight::new(0x0102_0304);
        let block_hash = BlockHash::from_bytes([9; 32]);
        let roots = BlockFinalNoteCommitmentRoots::new(
            height,
            block_hash,
            Some(FinalNoteCommitmentRoot::from_bytes([1; 32])),
            None,
            Some(FinalNoteCommitmentRoot::from_bytes([3; 32])),
        );
        let mut expected = vec![SAPLING_PRESENCE_BIT | IRONWOOD_PRESENCE_BIT];
        expected.extend_from_slice(&[1; 32]);
        expected.extend_from_slice(&[3; 32]);

        let encoded = encode_block_final_note_commitment_roots(&roots);
        assert_eq!(encoded, expected);
        assert_eq!(
            decode_block_final_note_commitment_roots(height, block_hash, &encoded),
            Ok(roots)
        );
    }

    /// A checkpoint decode reduced to the block time it reads.
    type BlockTimeDecode = fn(&[u8]) -> Result<u32, CanonicalFamilyValueDecodeError>;

    #[test]
    fn tree_state_checkpoint_v1_rejects_non_exact_values_in_every_decode()
    -> Result<(), Box<dyn std::error::Error>> {
        let decodes: [BlockTimeDecode; 3] = [
            |encoded| decode_tree_state_checkpoint(encoded).map(|(block_time, _)| block_time),
            |encoded| {
                decode_served_tree_state_checkpoint(encoded).map(|(block_time, _)| block_time)
            },
            |encoded| decode_stored_tree_state_roots(encoded).map(|roots| roots.block_time_seconds),
        ];
        let sapling = CommitmentTreeFrontier::empty(ShieldedProtocol::Sapling);
        let frontiers = CommitmentTreeFrontiers::from_validated_parts(Some(sapling), None, None)?;
        let mut broken_frontier = encode_tree_state_checkpoint(0, &frontiers);
        // The frontier's first presence flag follows the root and its length.
        broken_frontier[5 + 32 + 2] = 2;
        let oversized_len = MAX_COMMITMENT_TREE_FRONTIER_FINAL_STATE_BYTES + 1;
        let mut oversized = vec![0, 0, 0, 0, SAPLING_PRESENCE_BIT];
        oversized.extend_from_slice(&[0; 32]);
        oversized.extend_from_slice(&u16::try_from(oversized_len)?.to_le_bytes());
        let mut trailing = encode_tree_state_checkpoint(0, &CommitmentTreeFrontiers::default());
        trailing.push(0);
        // Framing the canonical encoder never writes: an empty tree with a
        // parent slot, and one leaf without the 31 parent slots.
        let mut one_leaf = vec![1, 1];
        one_leaf.extend_from_slice(&[0; 31]);
        one_leaf.extend_from_slice(&[0, 0]);
        let non_canonical_frontiers = [vec![0, 0, 1, 0], one_leaf]
            .iter()
            .map(|final_state| sapling_row(final_state))
            .collect::<Result<Vec<_>, _>>()?;

        for decode in decodes {
            assert!(matches!(
                decode(&[0, 0, 0]),
                Err(CanonicalFamilyValueDecodeError::Truncated { .. })
            ));
            assert_eq!(
                decode(&[0, 0, 0, 0, 0x80]),
                Err(CanonicalFamilyValueDecodeError::UnknownPresenceBits {
                    family: "tree_state_checkpoint",
                    presence_bitmap: 0x80,
                })
            );
            assert!(matches!(
                decode(&broken_frontier),
                Err(CanonicalFamilyValueDecodeError::InvalidFrontier {
                    protocol: ShieldedProtocol::Sapling,
                    source: CommitmentTreeFrontierValidationError::InvalidEncoding { .. },
                })
            ));
            assert_eq!(
                decode(&oversized),
                Err(CanonicalFamilyValueDecodeError::FrontierTooLarge {
                    protocol: ShieldedProtocol::Sapling,
                    encoded_bytes: oversized_len,
                })
            );
            assert_eq!(
                decode(&trailing),
                Err(CanonicalFamilyValueDecodeError::TrailingBytes {
                    family: "tree_state_checkpoint",
                    trailing_bytes: 1,
                })
            );
            for row in &non_canonical_frontiers {
                assert!(
                    matches!(
                        decode(row),
                        Err(CanonicalFamilyValueDecodeError::InvalidFrontier {
                            protocol: ShieldedProtocol::Sapling,
                            source: CommitmentTreeFrontierValidationError::InvalidEncoding { .. },
                        })
                    ),
                    "row {row:?}"
                );
            }
        }
        Ok(())
    }

    /// One checkpoint value whose only frontier is a Sapling frontier with
    /// `final_state` bytes and an all-zero root.
    fn sapling_row(final_state: &[u8]) -> Result<Vec<u8>, std::num::TryFromIntError> {
        let mut row = vec![0, 0, 0, 0, SAPLING_PRESENCE_BIT];
        row.extend_from_slice(&[0; 32]);
        row.extend_from_slice(&u16::try_from(final_state.len())?.to_le_bytes());
        row.extend_from_slice(final_state);
        Ok(row)
    }

    #[test]
    fn only_the_revalidating_decode_checks_the_stored_root()
    -> Result<(), Box<dyn std::error::Error>> {
        let sapling = CommitmentTreeFrontier::empty(ShieldedProtocol::Sapling);
        let frontiers =
            CommitmentTreeFrontiers::from_validated_parts(Some(sapling.clone()), None, None)?;
        let mut wrong_root = encode_tree_state_checkpoint(7, &frontiers);
        wrong_root[5] ^= 0xff;

        assert_eq!(
            decode_tree_state_checkpoint(&wrong_root),
            Err(CanonicalFamilyValueDecodeError::InvalidFrontier {
                protocol: ShieldedProtocol::Sapling,
                source: CommitmentTreeFrontierValidationError::RootMismatch,
            })
        );
        assert_eq!(
            decode_served_tree_state_checkpoint(&wrong_root),
            Ok((
                7,
                ServedFrontiers::new(Some(ServedFrontier::from(&sapling)), None, None)
            )),
            "a served decode skips the root and keeps the stored frontier"
        );
        let mut flipped_root_bytes = sapling.final_root().as_bytes();
        flipped_root_bytes[0] ^= 0xff;
        assert_eq!(
            decode_stored_tree_state_roots(&wrong_root),
            Ok(StoredTreeStateRoots {
                block_time_seconds: 7,
                sapling: Some(FinalNoteCommitmentRoot::from_bytes(flipped_root_bytes)),
                orchard: None,
                ironwood: None,
            }),
            "a stored-roots decode checks frontier framing and returns the stored root without hashing"
        );
        Ok(())
    }

    #[test]
    fn block_final_note_commitment_roots_v1_rejects_non_exact_values() {
        let height = BlockHeight::new(7);
        let block_hash = BlockHash::from_bytes([7; 32]);
        assert!(matches!(
            decode_block_final_note_commitment_roots(height, block_hash, &[]),
            Err(CanonicalFamilyValueDecodeError::Truncated { .. })
        ));
        assert_eq!(
            decode_block_final_note_commitment_roots(height, block_hash, &[0x80]),
            Err(CanonicalFamilyValueDecodeError::UnknownPresenceBits {
                family: "block_final_note_commitment_roots",
                presence_bitmap: 0x80,
            })
        );
        assert!(matches!(
            decode_block_final_note_commitment_roots(height, block_hash, &[SAPLING_PRESENCE_BIT]),
            Err(CanonicalFamilyValueDecodeError::Truncated { .. })
        ));
        assert_eq!(
            decode_block_final_note_commitment_roots(height, block_hash, &[0, 0]),
            Err(CanonicalFamilyValueDecodeError::TrailingBytes {
                family: "block_final_note_commitment_roots",
                trailing_bytes: 1,
            })
        );
    }
}
