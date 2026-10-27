use zinder_core::{
    BlockHash, BlockHeight, BlockId, CanonicalBlockFactsDigestVersion,
    CanonicalBlockFactsSequenceDigest, CanonicalBlockFactsSequenceDigestVersion,
    CanonicalBlockReplayFormatVersion, CanonicalHistoryBounds, ChainEpochId,
    CommitmentTreeCheckpoint, CommitmentTreeFrontier, CommitmentTreeFrontiers,
    FinalNoteCommitmentRoot, MAX_COMMITMENT_TREE_FRONTIER_FINAL_STATE_BYTES, Network,
    NetworkUpgradeActivationsFingerprint, NetworkUpgradeActivationsFingerprintVersion,
    ShieldedProtocol,
};

use super::{
    CANONICAL_STORE_IDENTITY, CANONICAL_STORE_SCHEMA_VERSION, CanonicalSequenceCheckpoint,
    CanonicalStoreBuildPlan, CanonicalStoreBuildPlanError, CanonicalStoreBuildState,
    CanonicalStoreError, CanonicalStoreReadyEvidence, CanonicalStoreWorkload,
};
use crate::RawBlobRetention;

const BUILDING_STATE: u8 = 1;
const READY_STATE: u8 = 2;
const ACTIVATION_SCHEDULE_TRAILER_MAGIC: &[u8] = b"ASCH";
const ACTIVATION_SCHEDULE_TRAILER_VERSION: u16 = 1;
const COMPLETE_HISTORY: u8 = 0;
const CHECKPOINTED_HISTORY: u8 = 1;
const WALLET_WORKLOAD: u8 = 1;
const EXPLORER_WORKLOAD: u8 = 2;
const NO_RAW_BLOB_RETENTION: u8 = 0;
const TRANSACTION_RAW_BLOB_RETENTION: u8 = 1;
const ALL_RAW_BLOB_RETENTION: u8 = 2;
const FRONTIER_ABSENT: u8 = 0;
const FRONTIER_PRESENT: u8 = 1;
const ACTIVATIONS_FINGERPRINT_FIELDS_LENGTH: usize = 2 + 32;
const HISTORY_FIXED_FIELDS_LENGTH: usize = 1 + 4 + 32 + 4 + 3;
const BUILD_TIP_FIELDS_LENGTH: usize = 4 + 32;
const READY_FIELDS_LENGTH: usize =
    4 + 32 + 4 + 32 + 8 + 8 + 8 + 2 + 4 + 2 + 32 + 8 + 4 + 32 + 8 + 2 + 32 + 8 + 2 + 32;
const STORE_CONTROL_MINIMUM_LENGTH: usize = CANONICAL_STORE_IDENTITY.len()
    + 2
    + 4
    + ACTIVATIONS_FINGERPRINT_FIELDS_LENGTH
    + 1
    + 1
    + 4
    + HISTORY_FIXED_FIELDS_LENGTH
    + BUILD_TIP_FIELDS_LENGTH
    + 32
    + 1
    + READY_FIELDS_LENGTH;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct DecodedStoreControl {
    pub(super) network: Network,
    pub(super) workload: CanonicalStoreWorkload,
    pub(super) build_plan: CanonicalStoreBuildPlan,
    pub(super) cursor_auth_key: [u8; 32],
    pub(super) build_state: CanonicalStoreBuildState,
    pub(super) activation_schedule_binding: Option<ActivationScheduleBinding>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ActivationScheduleBinding {
    pub(super) revision: u64,
    pub(super) digest: [u8; 32],
}

pub(super) fn encode_building_store_control(
    workload: CanonicalStoreWorkload,
    build_plan: &CanonicalStoreBuildPlan,
    cursor_auth_key: [u8; 32],
) -> Result<Vec<u8>, CanonicalStoreBuildPlanError> {
    let mut encoded = encode_store_control_prefix(workload, build_plan, cursor_auth_key)?;
    encoded.push(BUILDING_STATE);
    encoded.resize(encoded.len() + READY_FIELDS_LENGTH, 0);
    Ok(encoded)
}

pub(super) fn encode_ready_store_control(
    workload: CanonicalStoreWorkload,
    build_plan: &CanonicalStoreBuildPlan,
    cursor_auth_key: [u8; 32],
    ready_evidence: &CanonicalStoreReadyEvidence,
) -> Result<Vec<u8>, CanonicalStoreBuildPlanError> {
    encode_ready_store_control_with_schedule(
        workload,
        build_plan,
        cursor_auth_key,
        ready_evidence,
        None,
    )
}

pub(super) fn encode_ready_store_control_with_schedule(
    workload: CanonicalStoreWorkload,
    build_plan: &CanonicalStoreBuildPlan,
    cursor_auth_key: [u8; 32],
    ready_evidence: &CanonicalStoreReadyEvidence,
    activation_schedule_binding: Option<ActivationScheduleBinding>,
) -> Result<Vec<u8>, CanonicalStoreBuildPlanError> {
    let mut encoded = encode_store_control_prefix(workload, build_plan, cursor_auth_key)?;
    encoded.push(READY_STATE);
    encoded.extend_from_slice(
        &ready_evidence
            .first_retained_block
            .height
            .value()
            .to_le_bytes(),
    );
    encoded.extend_from_slice(&ready_evidence.first_retained_block.hash.as_bytes());
    encoded.extend_from_slice(&ready_evidence.visible_tip.height.value().to_le_bytes());
    encoded.extend_from_slice(&ready_evidence.visible_tip.hash.as_bytes());
    encoded.extend_from_slice(&ready_evidence.visible_epoch.value().to_le_bytes());
    encoded.extend_from_slice(&ready_evidence.visible_event_sequence.to_le_bytes());
    encoded.extend_from_slice(&ready_evidence.visible_block_count.to_le_bytes());
    encoded.extend_from_slice(&ready_evidence.block_digest_version.value().to_le_bytes());
    encoded.extend_from_slice(&ready_evidence.replay_format_version.value().to_le_bytes());
    encoded.extend_from_slice(&ready_evidence.sequence_digest_version.value().to_le_bytes());
    encoded.extend_from_slice(&ready_evidence.visible_sequence_digest);
    encoded.extend_from_slice(&ready_evidence.visible_logical_replay_bytes.to_le_bytes());
    let checkpoint = ready_evidence.sequence_checkpoint;
    encoded.extend_from_slice(&checkpoint.through().height.value().to_le_bytes());
    encoded.extend_from_slice(&checkpoint.through().hash.as_bytes());
    encoded.extend_from_slice(&checkpoint.retained_block_count().to_le_bytes());
    encoded.extend_from_slice(&checkpoint.sequence_digest().version().value().to_le_bytes());
    encoded.extend_from_slice(&checkpoint.sequence_digest().as_bytes());
    encoded.extend_from_slice(&checkpoint.logical_replay_bytes().to_le_bytes());
    encoded.extend_from_slice(&ready_evidence.construction_manifest_version.to_le_bytes());
    encoded.extend_from_slice(&ready_evidence.construction_manifest_sha256);
    if let Some(binding) = activation_schedule_binding {
        encoded.extend_from_slice(ACTIVATION_SCHEDULE_TRAILER_MAGIC);
        encoded.extend_from_slice(&ACTIVATION_SCHEDULE_TRAILER_VERSION.to_le_bytes());
        encoded.extend_from_slice(&binding.revision.to_le_bytes());
        encoded.extend_from_slice(&binding.digest);
    }
    Ok(encoded)
}

fn encode_store_control_prefix(
    workload: CanonicalStoreWorkload,
    build_plan: &CanonicalStoreBuildPlan,
    cursor_auth_key: [u8; 32],
) -> Result<Vec<u8>, CanonicalStoreBuildPlanError> {
    let mut encoded = Vec::with_capacity(STORE_CONTROL_MINIMUM_LENGTH);
    encoded.extend_from_slice(CANONICAL_STORE_IDENTITY.as_bytes());
    encoded.extend_from_slice(&CANONICAL_STORE_SCHEMA_VERSION.to_le_bytes());
    encoded.extend_from_slice(&build_plan.network().id().to_le_bytes());
    let activations_fingerprint = build_plan.network_upgrade_activations_fingerprint();
    encoded.extend_from_slice(&activations_fingerprint.version().value().to_le_bytes());
    encoded.extend_from_slice(&activations_fingerprint.as_bytes());
    encoded.push(match workload {
        CanonicalStoreWorkload::Wallet => WALLET_WORKLOAD,
        CanonicalStoreWorkload::Explorer => EXPLORER_WORKLOAD,
    });
    encoded.push(match build_plan.raw_blob_retention() {
        RawBlobRetention::None => NO_RAW_BLOB_RETENTION,
        RawBlobRetention::Transactions => TRANSACTION_RAW_BLOB_RETENTION,
        RawBlobRetention::All => ALL_RAW_BLOB_RETENTION,
    });
    encoded.extend_from_slice(
        &build_plan
            .reorg_policy()
            .reorg_window_blocks()
            .to_le_bytes(),
    );
    match build_plan.history_bounds().preceding_checkpoint() {
        None => {
            encoded.push(COMPLETE_HISTORY);
            encoded.extend_from_slice(
                &build_plan
                    .history_predecessor()
                    .block_id
                    .height
                    .value()
                    .to_le_bytes(),
            );
            encoded.extend_from_slice(&build_plan.history_predecessor().block_id.hash.as_bytes());
        }
        Some(checkpoint) => {
            encoded.push(CHECKPOINTED_HISTORY);
            encoded.extend_from_slice(&checkpoint.height.value().to_le_bytes());
            encoded.extend_from_slice(&checkpoint.hash.as_bytes());
        }
    }
    encoded.extend_from_slice(
        &build_plan
            .history_predecessor()
            .block_time_seconds
            .to_le_bytes(),
    );
    encode_predecessor_frontiers(&mut encoded, &build_plan.history_predecessor().frontiers)?;
    encoded.extend_from_slice(&build_plan.build_tip().height.value().to_le_bytes());
    encoded.extend_from_slice(&build_plan.build_tip().hash.as_bytes());
    encoded.extend_from_slice(&cursor_auth_key);
    Ok(encoded)
}

fn encode_predecessor_frontiers(
    encoded: &mut Vec<u8>,
    frontiers: &CommitmentTreeFrontiers,
) -> Result<(), CanonicalStoreBuildPlanError> {
    for protocol in [
        ShieldedProtocol::Sapling,
        ShieldedProtocol::Orchard,
        ShieldedProtocol::Ironwood,
    ] {
        let Some(frontier) = frontiers.get(protocol) else {
            encoded.push(FRONTIER_ABSENT);
            continue;
        };
        let final_state_length =
            u16::try_from(frontier.final_state_bytes().len()).map_err(|_| {
                CanonicalStoreBuildPlanError::PredecessorFrontierTooLarge {
                    protocol,
                    encoded_bytes: frontier.final_state_bytes().len(),
                }
            })?;
        encoded.push(FRONTIER_PRESENT);
        encoded.extend_from_slice(&frontier.final_root().as_bytes());
        encoded.extend_from_slice(&final_state_length.to_le_bytes());
        encoded.extend_from_slice(frontier.final_state_bytes());
    }
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "decodes the ordered, versioned control contract without partial admission"
)]
pub(super) fn decode_store_control(
    path: &std::path::Path,
    encoded: &[u8],
) -> Result<DecodedStoreControl, CanonicalStoreError> {
    let mut decoder = Decoder::new(path, encoded);
    let identity = decoder.read_bytes(CANONICAL_STORE_IDENTITY.len(), "identity")?;
    if identity != CANONICAL_STORE_IDENTITY.as_bytes() {
        return Err(CanonicalStoreError::admission(
            path,
            format!("store identity must be exactly {CANONICAL_STORE_IDENTITY:?}"),
        ));
    }
    let schema_version = decoder.read_u16("schema version")?;
    if schema_version != CANONICAL_STORE_SCHEMA_VERSION {
        return Err(CanonicalStoreError::admission(
            path,
            format!(
                "store schema version {schema_version} does not equal required version {CANONICAL_STORE_SCHEMA_VERSION}"
            ),
        ));
    }
    let network_id = decoder.read_u32("network")?;
    let network = Network::from_id(network_id).ok_or_else(|| {
        CanonicalStoreError::admission(
            path,
            format!("store control contains unknown network id {network_id}"),
        )
    })?;
    let network_upgrade_activations_fingerprint =
        decode_network_upgrade_activations_fingerprint(&mut decoder)?;
    let workload = match decoder.read_u8("workload")? {
        WALLET_WORKLOAD => CanonicalStoreWorkload::Wallet,
        EXPLORER_WORKLOAD => CanonicalStoreWorkload::Explorer,
        workload => {
            return Err(CanonicalStoreError::admission(
                path,
                format!("store control contains unknown workload {workload}"),
            ));
        }
    };
    let raw_blob_retention = decode_raw_blob_retention(&mut decoder)?;
    let reorg_policy =
        super::CanonicalReorgPolicy::new(decoder.read_u32("reorg window blocks")?)
            .map_err(|source| CanonicalStoreError::admission(path, source.to_string()))?;
    let (history_bounds, history_predecessor) = decode_history_bounds(&mut decoder)?;
    let build_tip = BlockId::new(
        BlockHeight::new(decoder.read_u32("build tip height")?),
        BlockHash::from_bytes(decoder.read_array::<32>("build tip hash")?),
    );
    let build_plan = CanonicalStoreBuildPlan {
        network,
        network_upgrade_activations_fingerprint,
        raw_blob_retention,
        reorg_policy,
        history_bounds,
        history_predecessor,
        build_tip,
    }
    .validate()
    .map_err(|source| CanonicalStoreError::admission(path, source.to_string()))?;
    let cursor_auth_key = decoder.read_array::<32>("cursor authentication key")?;
    let build_state = match decoder.read_u8("build state")? {
        BUILDING_STATE => {
            decoder.reject_nonzero_building_fields()?;
            CanonicalStoreBuildState::Building
        }
        READY_STATE => CanonicalStoreBuildState::Ready(
            decoder.decode_ready_evidence(history_bounds, build_tip)?,
        ),
        state => {
            return Err(CanonicalStoreError::admission(
                path,
                format!("store control contains unknown build state {state}"),
            ));
        }
    };
    let activation_schedule_binding = if decoder.remaining() == 0 {
        None
    } else {
        if build_state == CanonicalStoreBuildState::Building {
            return Err(CanonicalStoreError::admission(
                path,
                "BUILDING store control must not contain an activation-schedule trailer",
            ));
        }
        if decoder.read_bytes(
            ACTIVATION_SCHEDULE_TRAILER_MAGIC.len(),
            "schedule trailer identity",
        )? != ACTIVATION_SCHEDULE_TRAILER_MAGIC
        {
            return Err(CanonicalStoreError::admission(
                path,
                "store control contains an unknown trailing extension",
            ));
        }
        let version = decoder.read_u16("activation schedule trailer version")?;
        if version != ACTIVATION_SCHEDULE_TRAILER_VERSION {
            return Err(CanonicalStoreError::admission(
                path,
                format!("unsupported activation schedule trailer version {version}"),
            ));
        }
        let revision = decoder.read_u64("activation schedule revision")?;
        if revision == 0 {
            return Err(CanonicalStoreError::admission(
                path,
                "activation schedule revision must be nonzero",
            ));
        }
        Some(ActivationScheduleBinding {
            revision,
            digest: decoder.read_array("activation schedule digest")?,
        })
    };
    decoder.reject_trailing_bytes()?;
    Ok(DecodedStoreControl {
        network,
        workload,
        build_plan,
        cursor_auth_key,
        build_state,
        activation_schedule_binding,
    })
}

fn decode_raw_blob_retention(
    decoder: &mut Decoder<'_>,
) -> Result<RawBlobRetention, CanonicalStoreError> {
    Ok(match decoder.read_u8("raw blob retention")? {
        NO_RAW_BLOB_RETENTION => RawBlobRetention::None,
        TRANSACTION_RAW_BLOB_RETENTION => RawBlobRetention::Transactions,
        ALL_RAW_BLOB_RETENTION => RawBlobRetention::All,
        retention => {
            return Err(CanonicalStoreError::admission(
                decoder.path,
                format!("store control contains unknown raw blob retention {retention}"),
            ));
        }
    })
}

fn decode_network_upgrade_activations_fingerprint(
    decoder: &mut Decoder<'_>,
) -> Result<NetworkUpgradeActivationsFingerprint, CanonicalStoreError> {
    let version = NetworkUpgradeActivationsFingerprintVersion::try_from(
        decoder.read_u16("network upgrade activations fingerprint version")?,
    )
    .map_err(|source| CanonicalStoreError::admission(decoder.path, source.to_string()))?;
    if version != NetworkUpgradeActivationsFingerprintVersion::V1 {
        return Err(CanonicalStoreError::admission(
            decoder.path,
            "network upgrade activations fingerprint contract must be version 1",
        ));
    }
    Ok(NetworkUpgradeActivationsFingerprint::from_bytes(
        version,
        decoder.read_array::<32>("network upgrade activations fingerprint")?,
    ))
}

fn decode_history_bounds(
    decoder: &mut Decoder<'_>,
) -> Result<(CanonicalHistoryBounds, CommitmentTreeCheckpoint), CanonicalStoreError> {
    let kind = decoder.read_u8("history kind")?;
    let predecessor_height = decoder.read_u32("history predecessor height")?;
    let predecessor_hash = decoder.read_array::<32>("history predecessor hash")?;
    let history_predecessor = BlockId::new(
        BlockHeight::new(predecessor_height),
        BlockHash::from_bytes(predecessor_hash),
    );
    let history_bounds = match kind {
        COMPLETE_HISTORY if predecessor_height == 0 => Ok(CanonicalHistoryBounds::complete()),
        COMPLETE_HISTORY => Err(CanonicalStoreError::admission(
            decoder.path,
            "complete history predecessor must be the height-zero block",
        )),
        CHECKPOINTED_HISTORY => CanonicalHistoryBounds::checkpointed(history_predecessor)
            .map_err(|source| CanonicalStoreError::admission(decoder.path, source.to_string())),
        _ => Err(CanonicalStoreError::admission(
            decoder.path,
            format!("store control contains unknown history kind {kind}"),
        )),
    }?;
    let block_time_seconds = decoder.read_u32("history predecessor block time")?;
    let frontiers = CommitmentTreeFrontiers::from_validated_parts(
        decode_predecessor_frontier(decoder, ShieldedProtocol::Sapling)?,
        decode_predecessor_frontier(decoder, ShieldedProtocol::Orchard)?,
        decode_predecessor_frontier(decoder, ShieldedProtocol::Ironwood)?,
    );
    Ok((
        history_bounds,
        CommitmentTreeCheckpoint::new(history_predecessor, block_time_seconds, frontiers),
    ))
}

fn decode_predecessor_frontier(
    decoder: &mut Decoder<'_>,
    protocol: ShieldedProtocol,
) -> Result<Option<CommitmentTreeFrontier>, CanonicalStoreError> {
    match decoder.read_u8("history predecessor frontier presence")? {
        FRONTIER_ABSENT => Ok(None),
        FRONTIER_PRESENT => {
            let final_root = FinalNoteCommitmentRoot::from_bytes(
                decoder.read_array::<32>("history predecessor frontier root")?,
            );
            let final_state_length =
                usize::from(decoder.read_u16("history predecessor frontier finalState length")?);
            if final_state_length > MAX_COMMITMENT_TREE_FRONTIER_FINAL_STATE_BYTES {
                return Err(CanonicalStoreError::admission(
                    decoder.path,
                    format!(
                        "store control {protocol:?} predecessor frontier finalState is {final_state_length} bytes; maximum is {MAX_COMMITMENT_TREE_FRONTIER_FINAL_STATE_BYTES}"
                    ),
                ));
            }
            let final_state_bytes = decoder
                .read_bytes(
                    final_state_length,
                    "history predecessor frontier finalState bytes",
                )?
                .to_vec();
            CommitmentTreeFrontier::from_canonical_final_state(
                protocol,
                final_root,
                final_state_bytes,
            )
            .map(Some)
            .map_err(|source| {
                CanonicalStoreError::admission(
                    decoder.path,
                    format!("store control {protocol:?} predecessor frontier is invalid: {source}"),
                )
            })
        }
        presence => Err(CanonicalStoreError::admission(
            decoder.path,
            format!(
                "store control contains unknown {protocol:?} predecessor frontier presence {presence}"
            ),
        )),
    }
}

struct Decoder<'encoded> {
    path: &'encoded std::path::Path,
    encoded: &'encoded [u8],
    position: usize,
}

impl<'encoded> Decoder<'encoded> {
    fn remaining(&self) -> usize {
        self.encoded.len().saturating_sub(self.position)
    }

    const fn new(path: &'encoded std::path::Path, encoded: &'encoded [u8]) -> Self {
        Self {
            path,
            encoded,
            position: 0,
        }
    }

    fn decode_ready_evidence(
        &mut self,
        history_bounds: CanonicalHistoryBounds,
        build_tip: BlockId,
    ) -> Result<CanonicalStoreReadyEvidence, CanonicalStoreError> {
        let first_height = self.read_u32("first height")?;
        let first_hash = self.read_array::<32>("first hash")?;
        let visible_tip_height = self.read_u32("visible tip height")?;
        let visible_tip_hash = self.read_array::<32>("visible tip hash")?;
        let visible_epoch = self.read_u64("visible epoch")?;
        let visible_event_sequence = self.read_u64("visible event sequence")?;
        let visible_block_count = self.read_u64("visible block count")?;
        let block_digest_version =
            CanonicalBlockFactsDigestVersion::try_from(self.read_u16("block digest version")?)
                .map_err(|source| CanonicalStoreError::admission(self.path, source.to_string()))?;
        let replay_format_version =
            CanonicalBlockReplayFormatVersion::try_from(self.read_u32("replay format version")?)
                .map_err(|source| CanonicalStoreError::admission(self.path, source.to_string()))?;
        let sequence_digest_version = CanonicalBlockFactsSequenceDigestVersion::try_from(
            self.read_u16("sequence digest version")?,
        )
        .map_err(|source| CanonicalStoreError::admission(self.path, source.to_string()))?;
        let visible_sequence_digest = self.read_array::<32>("visible sequence digest")?;
        let visible_logical_replay_bytes = self.read_u64("visible logical replay bytes")?;
        let sequence_checkpoint = self.decode_sequence_checkpoint(
            first_height,
            visible_tip_height,
            visible_logical_replay_bytes,
        )?;
        let construction_manifest_version = self.read_u16("construction manifest version")?;
        let construction_manifest_sha256 = self.read_array::<32>("construction manifest digest")?;

        let expected_block_count = visible_tip_height
            .checked_sub(first_height)
            .map(u64::from)
            .and_then(|height_span| height_span.checked_add(1));
        let baseline_pointer_is_exact = visible_epoch == 1
            && visible_event_sequence == 1
            && visible_tip_height == build_tip.height.value()
            && visible_tip_hash == build_tip.hash.as_bytes();
        let live_pointer_is_exact = visible_epoch > 1 && visible_event_sequence == visible_epoch;
        if first_height != history_bounds.first_available_height().value()
            || !(baseline_pointer_is_exact || live_pointer_is_exact)
            || expected_block_count != Some(visible_block_count)
        {
            return Err(CanonicalStoreError::admission(
                self.path,
                "ready store control has an invalid chain position",
            ));
        }
        if visible_logical_replay_bytes == 0 {
            return Err(CanonicalStoreError::admission(
                self.path,
                "ready store control is missing validation evidence",
            ));
        }
        let ready_evidence = CanonicalStoreReadyEvidence {
            first_retained_block: BlockId::new(
                BlockHeight::new(first_height),
                BlockHash::from_bytes(first_hash),
            ),
            visible_tip: BlockId::new(
                BlockHeight::new(visible_tip_height),
                BlockHash::from_bytes(visible_tip_hash),
            ),
            visible_epoch: ChainEpochId::new(visible_epoch),
            visible_event_sequence,
            visible_block_count,
            block_digest_version,
            replay_format_version,
            sequence_digest_version,
            visible_sequence_digest,
            visible_logical_replay_bytes,
            sequence_checkpoint,
            construction_manifest_version,
            construction_manifest_sha256,
        };
        self.validate_ready_contract_versions(&ready_evidence)?;
        Ok(ready_evidence)
    }

    fn validate_ready_contract_versions(
        &self,
        ready_evidence: &CanonicalStoreReadyEvidence,
    ) -> Result<(), CanonicalStoreError> {
        if ready_evidence.block_digest_version != CanonicalBlockFactsDigestVersion::V1
            || ready_evidence.replay_format_version != CanonicalBlockReplayFormatVersion::V1
            || ready_evidence.sequence_digest_version
                != CanonicalBlockFactsSequenceDigestVersion::V1
            || ready_evidence.construction_manifest_version
                != super::CANONICAL_CONSTRUCTION_MANIFEST_FORMAT_VERSION
            || ready_evidence
                .construction_manifest_sha256
                .iter()
                .all(|byte| *byte == 0)
        {
            return Err(CanonicalStoreError::admission(
                self.path,
                "ready store control has an unsupported contract version",
            ));
        }
        Ok(())
    }

    fn decode_sequence_checkpoint(
        &mut self,
        first_height: u32,
        visible_tip_height: u32,
        visible_logical_replay_bytes: u64,
    ) -> Result<CanonicalSequenceCheckpoint, CanonicalStoreError> {
        let through_height = self.read_u32("checkpoint through height")?;
        let through_hash = self.read_array::<32>("checkpoint through hash")?;
        let retained_block_count = self.read_u64("checkpoint retained block count")?;
        let digest_version = CanonicalBlockFactsSequenceDigestVersion::try_from(
            self.read_u16("checkpoint sequence digest version")?,
        )
        .map_err(|source| CanonicalStoreError::admission(self.path, source.to_string()))?;
        let digest = self.read_array::<32>("checkpoint sequence digest")?;
        let logical_replay_bytes = self.read_u64("checkpoint logical replay bytes")?;
        let expected_count = through_height
            .checked_sub(first_height)
            .map(u64::from)
            .and_then(|height_span| height_span.checked_add(1));
        if through_height > visible_tip_height
            || expected_count != Some(retained_block_count)
            || logical_replay_bytes == 0
            || logical_replay_bytes > visible_logical_replay_bytes
            || digest_version != CanonicalBlockFactsSequenceDigestVersion::V1
        {
            return Err(CanonicalStoreError::admission(
                self.path,
                "ready store control has an invalid sequence checkpoint",
            ));
        }
        Ok(CanonicalSequenceCheckpoint::from_admitted_parts(
            BlockId::new(
                BlockHeight::new(through_height),
                BlockHash::from_bytes(through_hash),
            ),
            retained_block_count,
            CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
                digest_version,
                retained_block_count,
                digest,
            ),
            logical_replay_bytes,
        ))
    }

    fn reject_nonzero_building_fields(&mut self) -> Result<(), CanonicalStoreError> {
        let ready_fields = self.read_bytes(READY_FIELDS_LENGTH, "reserved ready fields")?;
        if ready_fields.iter().any(|byte| *byte != 0) {
            return Err(CanonicalStoreError::admission(
                self.path,
                "building store control contains ready-state evidence",
            ));
        }
        Ok(())
    }

    fn read_u8(&mut self, field: &'static str) -> Result<u8, CanonicalStoreError> {
        Ok(self.read_bytes(1, field)?[0])
    }

    fn read_u16(&mut self, field: &'static str) -> Result<u16, CanonicalStoreError> {
        Ok(u16::from_le_bytes(self.read_array(field)?))
    }

    fn read_u32(&mut self, field: &'static str) -> Result<u32, CanonicalStoreError> {
        Ok(u32::from_le_bytes(self.read_array(field)?))
    }

    fn read_u64(&mut self, field: &'static str) -> Result<u64, CanonicalStoreError> {
        Ok(u64::from_le_bytes(self.read_array(field)?))
    }

    fn read_array<const LENGTH: usize>(
        &mut self,
        field: &'static str,
    ) -> Result<[u8; LENGTH], CanonicalStoreError> {
        let mut array = [0; LENGTH];
        array.copy_from_slice(self.read_bytes(LENGTH, field)?);
        Ok(array)
    }

    fn read_bytes(
        &mut self,
        length: usize,
        field: &'static str,
    ) -> Result<&'encoded [u8], CanonicalStoreError> {
        let end = self.position.checked_add(length).ok_or_else(|| {
            CanonicalStoreError::admission(self.path, "store control offset overflow")
        })?;
        let bytes = self.encoded.get(self.position..end).ok_or_else(|| {
            CanonicalStoreError::admission(self.path, format!("store control {field} is truncated"))
        })?;
        self.position = end;
        Ok(bytes)
    }

    fn reject_trailing_bytes(&self) -> Result<(), CanonicalStoreError> {
        if self.position != self.encoded.len() {
            return Err(CanonicalStoreError::admission(
                self.path,
                format!(
                    "store control contains {} trailing bytes",
                    self.encoded.len().saturating_sub(self.position)
                ),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU32, path::Path};

    use super::*;
    use crate::CanonicalReorgPolicy;

    const CURSOR_AUTH_KEY: [u8; 32] = [7; 32];
    const ACTIVATIONS_FINGERPRINT: NetworkUpgradeActivationsFingerprint =
        NetworkUpgradeActivationsFingerprint::from_bytes(
            NetworkUpgradeActivationsFingerprintVersion::V1,
            [11; 32],
        );

    #[test]
    fn building_control_round_trips_every_network() -> Result<(), Box<dyn std::error::Error>> {
        for network in [
            Network::ZcashMainnet,
            Network::ZcashTestnet,
            Network::ZcashRegtest,
        ] {
            for workload in [
                CanonicalStoreWorkload::Wallet,
                CanonicalStoreWorkload::Explorer,
            ] {
                let build_plan = complete_build_plan(network);
                let encoded =
                    encode_building_store_control(workload, &build_plan, CURSOR_AUTH_KEY)?;
                assert_eq!(
                    decode_store_control(Path::new("canonical"), &encoded)?,
                    DecodedStoreControl {
                        network,
                        workload,
                        build_plan,
                        cursor_auth_key: CURSOR_AUTH_KEY,
                        build_state: CanonicalStoreBuildState::Building,
                        activation_schedule_binding: None,
                    }
                );
                assert_eq!(
                    decode_store_control(Path::new("canonical"), &encoded)?
                        .build_plan
                        .reorg_policy()
                        .reorg_window_blocks(),
                    1
                );
            }
        }

        let reorg_window_offset =
            CANONICAL_STORE_IDENTITY.len() + 2 + 4 + ACTIVATIONS_FINGERPRINT_FIELDS_LENGTH + 1 + 1;
        let mut zero_reorg_window = encode_building_store_control(
            CanonicalStoreWorkload::Wallet,
            &complete_build_plan(Network::ZcashTestnet),
            CURSOR_AUTH_KEY,
        )?;
        zero_reorg_window[reorg_window_offset..reorg_window_offset + 4].fill(0);
        let zero_error = decode_store_control(Path::new("canonical"), &zero_reorg_window)
            .err()
            .ok_or("zero persisted reorg window must fail closed")?;
        assert!(zero_error.to_string().contains("greater than zero"));

        let checkpoint = BlockId::new(BlockHeight::new(99), BlockHash::from_bytes([9; 32]));
        let history_bounds = CanonicalHistoryBounds::checkpointed(checkpoint)?;
        let build_plan = checkpointed_build_plan(checkpoint, history_bounds);
        let encoded = encode_building_store_control(
            CanonicalStoreWorkload::Wallet,
            &build_plan,
            CURSOR_AUTH_KEY,
        )?;
        assert_eq!(
            decode_store_control(Path::new("canonical"), &encoded)?
                .build_plan
                .history_bounds(),
            history_bounds
        );
        assert_eq!(
            decode_store_control(Path::new("canonical"), &encoded)?
                .build_plan
                .history_predecessor()
                .frontiers,
            build_plan.history_predecessor().frontiers
        );
        assert_eq!(
            decode_store_control(Path::new("canonical"), &encoded)?
                .build_plan
                .history_predecessor()
                .block_time_seconds,
            build_plan.history_predecessor().block_time_seconds
        );
        Ok(())
    }

    #[test]
    fn ready_control_requires_complete_current_contract_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let encoded = valid_ready_control();
        let decoded_ready = decode_store_control(Path::new("canonical"), &encoded)?;
        assert_eq!(decoded_ready, expected_ready_control());
        let CanonicalStoreBuildState::Ready(ready_evidence) = decoded_ready.build_state else {
            return Err("ready fixture decoded as BUILDING".into());
        };
        assert_eq!(
            encode_ready_store_control(
                decoded_ready.workload,
                &decoded_ready.build_plan,
                decoded_ready.cursor_auth_key,
                &ready_evidence,
            )?,
            encoded
        );

        let building_plan = complete_build_plan(Network::ZcashTestnet);
        let building_control = encode_building_store_control(
            CanonicalStoreWorkload::Explorer,
            &building_plan,
            CURSOR_AUTH_KEY,
        )?;
        let decoded_building = decode_store_control(Path::new("canonical"), &building_control)?;
        assert_ne!(decoded_building, decoded_ready);

        let ready_fields = CANONICAL_STORE_IDENTITY.len()
            + 2
            + 4
            + ACTIVATIONS_FINGERPRINT_FIELDS_LENGTH
            + 1
            + 1
            + 4
            + HISTORY_FIXED_FIELDS_LENGTH
            + BUILD_TIP_FIELDS_LENGTH
            + 32
            + 1;
        let visible_epoch = ready_fields + 4 + 32 + 4 + 32;
        let mut changed_epoch = encoded.clone();
        changed_epoch[visible_epoch..visible_epoch + 8].copy_from_slice(&2_u64.to_le_bytes());
        assert!(decode_store_control(Path::new("canonical"), &changed_epoch).is_err());

        let visible_event_sequence = visible_epoch + 8;
        let mut missing_event_sequence = encoded.clone();
        missing_event_sequence[visible_event_sequence..visible_event_sequence + 8].fill(0);
        assert!(decode_store_control(Path::new("canonical"), &missing_event_sequence).is_err());

        let block_count = visible_event_sequence + 8;
        let mut wrong_count = encoded.clone();
        wrong_count[block_count..block_count + 8].copy_from_slice(&1_u64.to_le_bytes());
        let error = decode_store_control(Path::new("canonical"), &wrong_count)
            .err()
            .ok_or_else(|| {
                CanonicalStoreError::admission(Path::new("canonical"), "expected error")
            })?;
        assert!(error.to_string().contains("invalid chain position"));

        let mut wrong_version = encoded;
        let block_digest_version = block_count + 8;
        wrong_version[block_digest_version..block_digest_version + 2]
            .copy_from_slice(&2_u16.to_le_bytes());
        let error = decode_store_control(Path::new("canonical"), &wrong_version)
            .err()
            .ok_or_else(|| {
                CanonicalStoreError::admission(Path::new("canonical"), "expected error")
            })?;
        assert!(
            error
                .to_string()
                .contains("unsupported canonical block facts digest version 2")
        );
        Ok(())
    }

    #[test]
    fn ready_control_binds_an_additive_versioned_activation_schedule_trailer()
    -> Result<(), Box<dyn std::error::Error>> {
        let expected = expected_ready_control();
        let CanonicalStoreBuildState::Ready(ready) = expected.build_state else {
            return Err("expected READY test control".into());
        };
        let binding = ActivationScheduleBinding {
            revision: 3,
            digest: [0x7a; 32],
        };
        let encoded = encode_ready_store_control_with_schedule(
            expected.workload,
            &expected.build_plan,
            expected.cursor_auth_key,
            &ready,
            Some(binding),
        )?;
        assert_eq!(
            decode_store_control(Path::new("canonical"), &encoded)?.activation_schedule_binding,
            Some(binding)
        );
        // Releases before schedule admission decode exactly the legacy READY
        // encoding and reject any trailing byte, so they also refuse this
        // control.
        let legacy = encode_ready_store_control(
            expected.workload,
            &expected.build_plan,
            expected.cursor_auth_key,
            &ready,
        )?;
        assert!(encoded.len() > legacy.len() && encoded.starts_with(&legacy));
        let mut unknown_extension = legacy;
        unknown_extension.extend_from_slice(b"NEXT");
        assert!(
            decode_store_control(Path::new("canonical"), &unknown_extension)
                .err()
                .ok_or("an unknown READY extension must be rejected")?
                .to_string()
                .contains("unknown trailing extension")
        );
        let mut unsupported_version = encoded;
        let version_offset = unsupported_version.len() - 42;
        unsupported_version[version_offset] = b'X';
        let error = decode_store_control(Path::new("canonical"), &unsupported_version)
            .err()
            .ok_or("unknown trailer should be rejected")?;
        assert!(
            error
                .to_string()
                .contains("unsupported activation schedule trailer version")
        );
        Ok(())
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "the table enumerates each current control field that must fail closed"
    )]
    fn malformed_control_variants_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
        let build_plan = complete_build_plan(Network::ZcashTestnet);
        let base = encode_building_store_control(
            CanonicalStoreWorkload::Explorer,
            &build_plan,
            CURSOR_AUTH_KEY,
        )?;
        let identity_end = CANONICAL_STORE_IDENTITY.len();
        let network_start = identity_end + 2;
        let activations_fingerprint_version = network_start + 4;
        let workload_offset =
            activations_fingerprint_version + ACTIVATIONS_FINGERPRINT_FIELDS_LENGTH;
        let raw_blob_retention_offset = workload_offset + 1;
        let history_kind = raw_blob_retention_offset + 1 + 4;
        let sapling_frontier_presence = history_kind + 1 + 4 + 32 + 4;
        let build_tip = history_kind + HISTORY_FIXED_FIELDS_LENGTH;
        let cursor_auth_key = build_tip + BUILD_TIP_FIELDS_LENGTH;
        let state_offset = cursor_auth_key + 32;
        let ready_fields_start = state_offset + 1;
        let cases = [
            {
                let mut encoded = base.clone();
                encoded[0] ^= 0xff;
                (encoded, "identity")
            },
            {
                let mut encoded = base.clone();
                encoded[identity_end..identity_end + 2].copy_from_slice(&1_u16.to_le_bytes());
                (
                    encoded,
                    "store schema version 1 does not equal required version 7",
                )
            },
            {
                let mut encoded = base.clone();
                encoded[network_start..network_start + 4].copy_from_slice(&99_u32.to_le_bytes());
                (encoded, "unknown network")
            },
            {
                let mut encoded = base.clone();
                encoded[network_start..network_start + 4]
                    .copy_from_slice(&Network::ZcashMainnet.id().to_le_bytes());
                (encoded, "history predecessor")
            },
            {
                let mut encoded = base.clone();
                encoded[activations_fingerprint_version..activations_fingerprint_version + 2]
                    .copy_from_slice(&2_u16.to_le_bytes());
                (encoded, "activations fingerprint version 2")
            },
            {
                let mut encoded = base.clone();
                encoded[workload_offset] = 99;
                (encoded, "unknown workload")
            },
            {
                let mut encoded = base.clone();
                encoded[raw_blob_retention_offset] = 99;
                (encoded, "unknown raw blob retention")
            },
            {
                let mut encoded = base.clone();
                encoded[history_kind] = 99;
                (encoded, "unknown history kind")
            },
            {
                let mut encoded = base.clone();
                encoded[sapling_frontier_presence] = 99;
                (encoded, "unknown Sapling predecessor frontier presence")
            },
            {
                let mut encoded = base.clone();
                encoded[state_offset] = 99;
                (encoded, "unknown build state")
            },
            {
                let mut encoded = base.clone();
                encoded[ready_fields_start] = 1;
                (encoded, "ready-state evidence")
            },
            {
                let mut encoded = base.clone();
                encoded.pop();
                (encoded, "truncated")
            },
            {
                let mut encoded = base;
                encoded.push(0);
                (
                    encoded,
                    "BUILDING store control must not contain an activation-schedule trailer",
                )
            },
        ];

        for (encoded, expected_reason) in cases {
            let error = decode_store_control(Path::new("canonical"), &encoded)
                .err()
                .ok_or_else(|| {
                    CanonicalStoreError::admission(Path::new("canonical"), "expected error")
                })?;
            assert!(error.to_string().contains(expected_reason), "{error}");
        }
        Ok(())
    }

    #[test]
    fn persisted_checkpoint_frontiers_are_intrinsically_revalidated()
    -> Result<(), Box<dyn std::error::Error>> {
        let checkpoint = BlockId::new(BlockHeight::new(99), BlockHash::from_bytes([9; 32]));
        let history_bounds = CanonicalHistoryBounds::checkpointed(checkpoint)?;
        let build_plan = checkpointed_build_plan(checkpoint, history_bounds);
        let encoded = encode_building_store_control(
            CanonicalStoreWorkload::Wallet,
            &build_plan,
            CURSOR_AUTH_KEY,
        )?;
        let history_kind = CANONICAL_STORE_IDENTITY.len()
            + 2
            + 4
            + ACTIVATIONS_FINGERPRINT_FIELDS_LENGTH
            + 1
            + 1
            + 4;
        let sapling_presence = history_kind + 1 + 4 + 32 + 4;
        assert_eq!(encoded[sapling_presence], FRONTIER_PRESENT);
        let sapling_root = sapling_presence + 1;
        let sapling_state_length = sapling_root + 32;

        let mut wrong_root = encoded.clone();
        wrong_root[sapling_root] ^= 0xff;
        let root_error = decode_store_control(Path::new("canonical"), &wrong_root)
            .err()
            .ok_or_else(|| CanonicalStoreError::admission(Path::new("canonical"), "expected"))?;
        assert!(root_error.to_string().contains("root does not match"));

        let mut oversized = encoded;
        oversized[sapling_state_length..sapling_state_length + 2].copy_from_slice(
            &u16::try_from(MAX_COMMITMENT_TREE_FRONTIER_FINAL_STATE_BYTES + 1)?.to_le_bytes(),
        );
        let oversize_error = decode_store_control(Path::new("canonical"), &oversized)
            .err()
            .ok_or_else(|| CanonicalStoreError::admission(Path::new("canonical"), "expected"))?;
        assert!(oversize_error.to_string().contains("maximum is 1090"));
        Ok(())
    }

    fn valid_ready_control() -> Vec<u8> {
        let mut encoded = Vec::with_capacity(STORE_CONTROL_MINIMUM_LENGTH);
        encoded.extend_from_slice(CANONICAL_STORE_IDENTITY.as_bytes());
        encoded.extend_from_slice(&CANONICAL_STORE_SCHEMA_VERSION.to_le_bytes());
        encoded.extend_from_slice(&Network::ZcashTestnet.id().to_le_bytes());
        encoded.extend_from_slice(&ACTIVATIONS_FINGERPRINT.version().value().to_le_bytes());
        encoded.extend_from_slice(&ACTIVATIONS_FINGERPRINT.as_bytes());
        encoded.push(EXPLORER_WORKLOAD);
        encoded.push(TRANSACTION_RAW_BLOB_RETENTION);
        encoded.extend_from_slice(&1_u32.to_le_bytes());
        encoded.push(COMPLETE_HISTORY);
        encoded.extend_from_slice(&0_u32.to_le_bytes());
        encoded.extend_from_slice(&Network::ZcashTestnet.genesis_hash().as_bytes());
        encoded.extend_from_slice(&1_234_u32.to_le_bytes());
        encoded.push(FRONTIER_ABSENT);
        encoded.push(FRONTIER_ABSENT);
        encoded.push(FRONTIER_ABSENT);
        encoded.extend_from_slice(&2_u32.to_le_bytes());
        encoded.extend_from_slice(&[2; 32]);
        encoded.extend_from_slice(&CURSOR_AUTH_KEY);
        encoded.push(READY_STATE);
        encoded.extend_from_slice(&1_u32.to_le_bytes());
        encoded.extend_from_slice(&[1; 32]);
        encoded.extend_from_slice(&2_u32.to_le_bytes());
        encoded.extend_from_slice(&[2; 32]);
        encoded.extend_from_slice(&1_u64.to_le_bytes());
        encoded.extend_from_slice(&1_u64.to_le_bytes());
        encoded.extend_from_slice(&2_u64.to_le_bytes());
        encoded.extend_from_slice(&CanonicalBlockFactsDigestVersion::V1.value().to_le_bytes());
        encoded.extend_from_slice(&CanonicalBlockReplayFormatVersion::V1.value().to_le_bytes());
        encoded.extend_from_slice(
            &CanonicalBlockFactsSequenceDigestVersion::V1
                .value()
                .to_le_bytes(),
        );
        encoded.extend_from_slice(&[3; 32]);
        encoded.extend_from_slice(&1_u64.to_le_bytes());
        encoded.extend_from_slice(&1_u32.to_le_bytes());
        encoded.extend_from_slice(&[1; 32]);
        encoded.extend_from_slice(&1_u64.to_le_bytes());
        encoded.extend_from_slice(
            &CanonicalBlockFactsSequenceDigestVersion::V1
                .value()
                .to_le_bytes(),
        );
        encoded.extend_from_slice(&[5; 32]);
        encoded.extend_from_slice(&1_u64.to_le_bytes());
        encoded.extend_from_slice(
            &crate::CANONICAL_CONSTRUCTION_MANIFEST_FORMAT_VERSION.to_le_bytes(),
        );
        encoded.extend_from_slice(&[6; 32]);
        assert_eq!(encoded.len(), STORE_CONTROL_MINIMUM_LENGTH);
        encoded
    }

    fn expected_ready_control() -> DecodedStoreControl {
        DecodedStoreControl {
            network: Network::ZcashTestnet,
            workload: CanonicalStoreWorkload::Explorer,
            build_plan: complete_build_plan(Network::ZcashTestnet),
            cursor_auth_key: CURSOR_AUTH_KEY,
            activation_schedule_binding: None,
            build_state: CanonicalStoreBuildState::Ready(CanonicalStoreReadyEvidence {
                first_retained_block: BlockId::new(
                    BlockHeight::new(1),
                    BlockHash::from_bytes([1; 32]),
                ),
                visible_tip: BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([2; 32])),
                visible_epoch: ChainEpochId::new(1),
                visible_event_sequence: 1,
                visible_block_count: 2,
                block_digest_version: CanonicalBlockFactsDigestVersion::V1,
                replay_format_version: CanonicalBlockReplayFormatVersion::V1,
                sequence_digest_version: CanonicalBlockFactsSequenceDigestVersion::V1,
                visible_sequence_digest: [3; 32],
                visible_logical_replay_bytes: 1,
                sequence_checkpoint: CanonicalSequenceCheckpoint::from_admitted_parts(
                    BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([1; 32])),
                    1,
                    CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
                        CanonicalBlockFactsSequenceDigestVersion::V1,
                        1,
                        [5; 32],
                    ),
                    1,
                ),
                construction_manifest_version:
                    crate::CANONICAL_CONSTRUCTION_MANIFEST_FORMAT_VERSION,
                construction_manifest_sha256: [6; 32],
            }),
        }
    }

    fn complete_build_plan(network: Network) -> CanonicalStoreBuildPlan {
        CanonicalStoreBuildPlan {
            network,
            network_upgrade_activations_fingerprint: ACTIVATIONS_FINGERPRINT,
            raw_blob_retention: RawBlobRetention::Transactions,
            reorg_policy: CanonicalReorgPolicy {
                reorg_window_blocks: NonZeroU32::MIN,
            },
            history_bounds: CanonicalHistoryBounds::complete(),
            history_predecessor: CommitmentTreeCheckpoint::new(
                BlockId::new(BlockHeight::new(0), network.genesis_hash()),
                1_234,
                CommitmentTreeFrontiers::default(),
            ),
            build_tip: BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([2; 32])),
        }
    }

    fn checkpointed_build_plan(
        checkpoint: BlockId,
        history_bounds: CanonicalHistoryBounds,
    ) -> CanonicalStoreBuildPlan {
        CanonicalStoreBuildPlan {
            network: Network::ZcashTestnet,
            network_upgrade_activations_fingerprint: ACTIVATIONS_FINGERPRINT,
            raw_blob_retention: RawBlobRetention::Transactions,
            reorg_policy: CanonicalReorgPolicy {
                reorg_window_blocks: NonZeroU32::MIN,
            },
            history_bounds,
            history_predecessor: CommitmentTreeCheckpoint::new(
                checkpoint,
                1_234,
                CommitmentTreeFrontiers::from_validated_parts(
                    Some(CommitmentTreeFrontier::empty(ShieldedProtocol::Sapling)),
                    Some(CommitmentTreeFrontier::empty(ShieldedProtocol::Orchard)),
                    Some(CommitmentTreeFrontier::empty(ShieldedProtocol::Ironwood)),
                ),
            ),
            build_tip: BlockId::new(BlockHeight::new(100), BlockHash::from_bytes([10; 32])),
        }
    }
}
