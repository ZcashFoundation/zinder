//! `ExplorerQuery.CommitmentRootSearch` handler.

use std::{collections::HashSet, num::NonZeroU32};

use tonic::{Request, Response, Status};
use zinder_core::{
    BlockHash, BlockHeight, ChainEpoch, FinalNoteCommitmentRoot,
    ShieldedProtocol as CoreShieldedProtocol, wire::encode_rpc_block_hash_hex,
};
use zinder_materialized_views::{
    COMMITMENT_ROOT_SEARCH_CONSUMER_NAME, CommitmentRootBackfillCoverage,
    CommitmentRootSearchConsumer, MaterializedViewState, MaterializedViewStore,
};
use zinder_proto::capabilities::EXPLORER_COMMITMENT_ROOT_SEARCH_V1;
use zinder_proto::v1::explorer::{
    CommitmentRootMatch, CommitmentRootSearchCoverage, CommitmentRootSearchDisplacedCoverage,
    CommitmentRootSearchRequest, CommitmentRootSearchResponse, ShieldedProtocol,
};
use zinder_store::{RocksDbCanonicalSecondary, chain_epoch_message};

use super::canonical_read::{
    SharedCanonicalSecondary, internal_canonical_error, read_with_pinned_canonical_epoch,
};
use super::clamp_max_entries;
use super::error::ExplorerError;
use super::freshness::{
    UpstreamObservationCache, attach_upstream_observation, build_explorer_freshness,
};

const DEFAULT_COMMITMENT_ROOT_SEARCH_MATCHES: u32 = 64;
const MAX_COMMITMENT_ROOT_SEARCH_MATCHES: u32 = 1024;

/// Shared dependencies for `CommitmentRootSearch`.
pub(crate) struct CommitmentRootSearchContext<'store> {
    pub(crate) canonical_store: &'store SharedCanonicalSecondary,
    pub(crate) materialized_view_store: &'store MaterializedViewStore,
    pub(crate) upstream_observation_cache: &'store UpstreamObservationCache,
}

struct CommitmentRootSearchOutcome {
    epoch: ChainEpoch,
    matches: Vec<CommitmentRootMatch>,
    coverage: CommitmentRootSearchCoverage,
    displaced_matches: Vec<CommitmentRootMatch>,
    displaced_coverage: CommitmentRootSearchDisplacedCoverage,
}

/// Serves `ExplorerQuery.CommitmentRootSearch`.
pub(crate) async fn query_commitment_root_search(
    context: CommitmentRootSearchContext<'_>,
    request: Request<CommitmentRootSearchRequest>,
) -> Result<Response<CommitmentRootSearchResponse>, Status> {
    let inner = request.into_inner();
    let root_bytes: [u8; 32] = inner.root.as_slice().try_into().map_err(|_| {
        ExplorerError::invalid_request("CommitmentRootSearchRequest.root must be exactly 32 bytes")
    })?;
    let root = FinalNoteCommitmentRoot::from_bytes(root_bytes);
    let max_matches = clamp_max_entries(
        inner.max_matches,
        DEFAULT_COMMITMENT_ROOT_SEARCH_MATCHES,
        MAX_COMMITMENT_ROOT_SEARCH_MATCHES,
    );

    let mv_store = context.materialized_view_store.clone();
    let outcome =
        read_with_pinned_canonical_epoch(context.canonical_store, None, move |secondary, epoch| {
            read_commitment_root_search_sync(secondary, &mv_store, epoch, root, max_matches)
        })
        .await?;

    let freshness = attach_upstream_observation(
        context.upstream_observation_cache,
        build_explorer_freshness(
            Some(context.materialized_view_store),
            EXPLORER_COMMITMENT_ROOT_SEARCH_V1,
            Some(chain_epoch_message(outcome.epoch)),
            0,
        )?,
    )
    .await;

    Ok(Response::new(CommitmentRootSearchResponse {
        freshness: Some(freshness),
        matches: outcome.matches,
        coverage: Some(outcome.coverage),
        displaced_matches: outcome.displaced_matches,
        displaced_coverage: Some(outcome.displaced_coverage),
    }))
}

fn read_commitment_root_search_sync(
    secondary: &RocksDbCanonicalSecondary,
    mv_store: &MaterializedViewStore,
    epoch: ChainEpoch,
    root: FinalNoteCommitmentRoot,
    max_matches: u32,
) -> Result<CommitmentRootSearchOutcome, Status> {
    if !mv_store.has_consumer(COMMITMENT_ROOT_SEARCH_CONSUMER_NAME) {
        return Err(ExplorerError::unsupported(
            "CommitmentRootSearch is omitted by the selected materialized-view workload",
        )
        .into());
    }
    let limit = max_matches as usize;
    let snapshot = mv_store.read_snapshot();
    let backfill = CommitmentRootSearchConsumer::backfill_coverage_snapshot(&snapshot)
        .map_err(|error| ExplorerError::internal(error.to_string()))?;
    let consumer_state = snapshot
        .consumer_state(COMMITMENT_ROOT_SEARCH_CONSUMER_NAME)
        .map_err(|error| ExplorerError::internal(error.to_string()))?;
    let matches = collect_canonical_matches(secondary, &snapshot, epoch, root, limit)?;
    drop(snapshot);

    let canonical_tip_hash = secondary
        .block_header_at(epoch.visible_tip_height)
        .map_err(internal_canonical_error)?
        .map(|header| header.block_hash);
    let coverage = build_canonical_coverage(
        secondary,
        epoch,
        backfill,
        consumer_state,
        canonical_tip_hash,
    );
    let (displaced_matches, displaced_coverage) =
        read_displaced_matches(secondary, epoch, root, limit)?;

    Ok(CommitmentRootSearchOutcome {
        epoch,
        matches,
        coverage,
        displaced_matches,
        displaced_coverage,
    })
}

fn collect_canonical_matches(
    secondary: &RocksDbCanonicalSecondary,
    snapshot: &zinder_materialized_views::MaterializedViewStoreReadSnapshot<'_>,
    epoch: ChainEpoch,
    root: FinalNoteCommitmentRoot,
    limit: usize,
) -> Result<Vec<CommitmentRootMatch>, Status> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let page_size = limit.clamp(64, 256);
    let mut matches = Vec::with_capacity(limit);
    let mut after = None;
    loop {
        let batch = CommitmentRootSearchConsumer::search_snapshot_page(
            snapshot,
            root,
            after.as_ref(),
            page_size,
        )
        .map_err(|error| ExplorerError::internal(error.to_string()))?;
        if batch.is_empty() {
            break;
        }
        let exhausted = batch.len() < page_size;
        after = batch.last().copied();
        let remaining = limit.saturating_sub(matches.len());
        let accepted = filter_canonical_matches(secondary, epoch, &batch, remaining)?;
        matches.extend(accepted);
        if matches.len() >= limit || exhausted {
            break;
        }
    }
    Ok(matches)
}

fn filter_canonical_matches(
    secondary: &RocksDbCanonicalSecondary,
    epoch: ChainEpoch,
    candidates: &[zinder_materialized_views::CommitmentRootIndexEntry],
    limit: usize,
) -> Result<Vec<CommitmentRootMatch>, Status> {
    let heights: Vec<BlockHeight> = candidates
        .iter()
        .map(|candidate| candidate.block_height)
        .collect();
    let headers = secondary
        .block_headers_at(&heights)
        .map_err(internal_canonical_error)?;
    let mut matches = Vec::with_capacity(candidates.len().min(limit));
    for (candidate, maybe_header) in candidates.iter().zip(headers) {
        if matches.len() >= limit {
            break;
        }
        let Some(header) = maybe_header else {
            continue;
        };
        let within_epoch = candidate.block_height <= epoch.visible_tip_height;
        let hash_matches = header.block_hash == candidate.block_hash;
        if within_epoch && hash_matches {
            matches.push(CommitmentRootMatch {
                block_height: candidate.block_height.value(),
                block_hash: encode_rpc_block_hash_hex(candidate.block_hash),
                block_time_unix_seconds: candidate.block_time_unix_seconds,
                protocol: encode_shielded_protocol(candidate.protocol)? as i32,
            });
        }
    }
    Ok(matches)
}

fn build_canonical_coverage(
    secondary: &RocksDbCanonicalSecondary,
    epoch: ChainEpoch,
    backfill: Option<CommitmentRootBackfillCoverage>,
    consumer_state: Option<MaterializedViewState>,
    canonical_tip_hash: Option<BlockHash>,
) -> CommitmentRootSearchCoverage {
    let latest_indexed_height = consumer_state
        .map(|state| {
            state
                .tip_height
                .value()
                .min(epoch.visible_tip_height.value())
        })
        .or_else(|| backfill.map(|cov| cov.complete_through_height.value()));
    let mv_matches_canonical_epoch = consumer_state.is_some_and(|state| {
        state.chain_epoch_id == epoch.id
            && state.tip_height == epoch.visible_tip_height
            && canonical_tip_hash == Some(state.tip_hash)
    });
    let first_retained = secondary.history_bounds().first_available_height();
    let earliest_root_height = earliest_shielded_activation_height(secondary);
    let canonical_history_complete = first_retained == BlockHeight::new(1)
        && mv_matches_canonical_epoch
        && earliest_root_height.is_some_and(|earliest_root| {
            epoch.visible_tip_height < earliest_root
                || backfill.is_some_and(|cov| {
                    cov.complete_from_height <= earliest_root
                        && cov.complete_through_height >= epoch.visible_tip_height
                })
        });

    CommitmentRootSearchCoverage {
        complete_from_height: backfill.map(|cov| cov.complete_from_height.value()),
        complete_through_height: backfill.map(|cov| cov.complete_through_height.value()),
        latest_indexed_height,
        canonical_history_complete,
    }
}

fn earliest_shielded_activation_height(
    secondary: &RocksDbCanonicalSecondary,
) -> Option<BlockHeight> {
    let activations = secondary.network_upgrade_activations()?;
    let min_height = ["Sapling", "Orchard", "Ironwood"]
        .into_iter()
        .filter_map(|name| activations.activation_height_by_name(name))
        .min()
        .unwrap_or(BlockHeight::new(1));
    Some(min_height.max(BlockHeight::new(1)))
}

struct DisplacedPageBatch<Cursor> {
    matches: Vec<zinder_core::DisplacedRootCandidate>,
    has_more: bool,
    next_cursor: Option<Cursor>,
    coverage: Option<zinder_core::DisplacedRootArchiveCoverage>,
    scan_complete: bool,
}

fn read_displaced_matches(
    secondary: &RocksDbCanonicalSecondary,
    epoch: ChainEpoch,
    root: FinalNoteCommitmentRoot,
    limit: usize,
) -> Result<
    (
        Vec<CommitmentRootMatch>,
        CommitmentRootSearchDisplacedCoverage,
    ),
    Status,
> {
    let page_limit_u32 = u32::try_from(limit.clamp(64, 256)).unwrap_or(256);
    let page_limit = NonZeroU32::new(page_limit_u32).unwrap_or(NonZeroU32::MIN);
    collect_displaced_matches_with(
        (limit, page_limit),
        epoch,
        |cursor, batch_limit| {
            let page = secondary
                .search_displaced_final_note_commitment_roots_page(root, cursor, batch_limit)
                .map_err(internal_canonical_error)?;
            Ok(DisplacedPageBatch {
                matches: page.matches,
                has_more: page.has_more,
                next_cursor: page.next_cursor,
                coverage: page.coverage,
                scan_complete: page.scan_complete,
            })
        },
        |heights| {
            secondary
                .block_headers_at(heights)
                .map_err(internal_canonical_error)
        },
    )
}

fn collect_displaced_matches_with<Cursor, FetchPage, FetchHeaders>(
    limits: (usize, NonZeroU32),
    epoch: ChainEpoch,
    mut fetch_page: FetchPage,
    mut fetch_headers: FetchHeaders,
) -> Result<
    (
        Vec<CommitmentRootMatch>,
        CommitmentRootSearchDisplacedCoverage,
    ),
    Status,
>
where
    FetchPage: FnMut(Option<&Cursor>, NonZeroU32) -> Result<DisplacedPageBatch<Cursor>, Status>,
    FetchHeaders:
        FnMut(&[BlockHeight]) -> Result<Vec<Option<zinder_core::BlockHeaderArtifact>>, Status>,
{
    let (limit, page_limit) = limits;
    let mut cursor = None;
    let mut seen = HashSet::with_capacity(limit);
    let mut matches = Vec::with_capacity(limit);
    let mut all_pages_complete = true;

    let archive_coverage = loop {
        let page = fetch_page(cursor.as_ref(), page_limit)?;
        if !page.scan_complete {
            all_pages_complete = false;
        }
        if limit > 0 && !page.matches.is_empty() {
            let heights: Vec<BlockHeight> = page
                .matches
                .iter()
                .map(|candidate| candidate.block_id.height)
                .collect();
            let headers = fetch_headers(&heights)?;
            append_filtered_displaced_page(
                epoch,
                &page.matches,
                &headers,
                limit,
                (&mut seen, &mut matches),
            )?;
        }
        if limit == 0 || matches.len() >= limit || !page.has_more {
            break page.coverage;
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            break page.coverage;
        }
    };

    let coverage = encode_displaced_coverage(archive_coverage, all_pages_complete);
    Ok((matches, coverage))
}

fn append_filtered_displaced_page(
    epoch: ChainEpoch,
    candidates: &[zinder_core::DisplacedRootCandidate],
    headers: &[Option<zinder_core::BlockHeaderArtifact>],
    limit: usize,
    output: (
        &mut HashSet<(BlockHeight, BlockHash, CoreShieldedProtocol)>,
        &mut Vec<CommitmentRootMatch>,
    ),
) -> Result<(), Status> {
    let (seen, matches) = output;
    for (candidate, maybe_header) in candidates.iter().zip(headers) {
        if matches.len() >= limit {
            break;
        }
        let still_canonical = candidate.block_id.height <= epoch.visible_tip_height
            && maybe_header
                .as_ref()
                .is_some_and(|header| header.block_hash == candidate.block_id.hash);
        if still_canonical {
            continue;
        }
        let dedup_key = (
            candidate.block_id.height,
            candidate.block_id.hash,
            candidate.protocol,
        );
        if !seen.insert(dedup_key) {
            continue;
        }
        matches.push(CommitmentRootMatch {
            block_height: candidate.block_id.height.value(),
            block_hash: encode_rpc_block_hash_hex(candidate.block_id.hash),
            block_time_unix_seconds: candidate.block_time_unix_seconds,
            protocol: encode_shielded_protocol(candidate.protocol)? as i32,
        });
    }
    Ok(())
}

const fn encode_displaced_coverage(
    coverage: Option<zinder_core::DisplacedRootArchiveCoverage>,
    scan_complete: bool,
) -> CommitmentRootSearchDisplacedCoverage {
    match coverage {
        Some(cov) => CommitmentRootSearchDisplacedCoverage {
            activation_event_sequence: Some(cov.activation_event_sequence),
            activation_epoch_id: Some(cov.activation_epoch.value()),
            activated_at_millis: Some(cov.activated_at.value()),
            captured_block_count: cov.captured_block_count,
            root_artifact_unavailable_count: cov.root_artifact_unavailable_count,
            captured_range_complete: scan_complete && cov.root_artifact_unavailable_count == 0,
        },
        None => CommitmentRootSearchDisplacedCoverage {
            activation_event_sequence: None,
            activation_epoch_id: None,
            activated_at_millis: None,
            captured_block_count: 0,
            root_artifact_unavailable_count: 0,
            captured_range_complete: false,
        },
    }
}

fn encode_shielded_protocol(protocol: CoreShieldedProtocol) -> Result<ShieldedProtocol, Status> {
    if protocol == CoreShieldedProtocol::Sapling {
        Ok(ShieldedProtocol::Sapling)
    } else if protocol == CoreShieldedProtocol::Orchard {
        Ok(ShieldedProtocol::Orchard)
    } else if protocol == CoreShieldedProtocol::Ironwood {
        Ok(ShieldedProtocol::Ironwood)
    } else {
        Err(ExplorerError::internal(format!(
            "unsupported shielded protocol variant: {protocol:?}"
        ))
        .into())
    }
}

#[cfg(test)]
mod tests {
    use zinder_core::{
        ArtifactSchemaVersion, BlockHeaderArtifact, BlockId, ChainEpochId, ChainTipMetadata,
        DisplacedRootArchiveCoverage, DisplacedRootCandidate, Network, UnixTimestampMillis,
    };

    use super::*;

    fn test_candidate(height: u32, hash_byte: u8) -> DisplacedRootCandidate {
        DisplacedRootCandidate {
            displacement_event_sequence: u64::from(height),
            displacement_epoch: ChainEpochId::new(1),
            root: FinalNoteCommitmentRoot::from_bytes([0xAA; 32]),
            block_id: BlockId::new(
                BlockHeight::new(height),
                BlockHash::from_bytes([hash_byte; 32]),
            ),
            block_time_unix_seconds: 1_750_000_000 + i64::from(height),
            protocol: CoreShieldedProtocol::Sapling,
        }
    }

    fn test_header(height: u32, hash_byte: u8) -> BlockHeaderArtifact {
        BlockHeaderArtifact {
            height: BlockHeight::new(height),
            block_hash: BlockHash::from_bytes([hash_byte; 32]),
            parent_hash: BlockHash::from_bytes([0x01; 32]),
            merkle_root_hash: [0x02; 32],
            commitment_bytes: [0x03; 32],
            block_time: 1_750_000_000 + i64::from(height),
            bits: 0x200f_0f0f,
            nonce: [0x04; 32],
            version: 4,
            block_size_bytes: 96,
        }
    }

    fn test_epoch(tip_height: u32) -> ChainEpoch {
        ChainEpoch {
            id: ChainEpochId::new(1),
            network: Network::ZcashRegtest,
            visible_tip_height: BlockHeight::new(tip_height),
            visible_tip_hash: BlockHash::from_bytes([0x33; 32]),
            settled_tip_height: BlockHeight::new(1),
            settled_tip_hash: BlockHash::from_bytes([0x11; 32]),
            artifact_schema_version: ArtifactSchemaVersion::new(1),
            tip_metadata: ChainTipMetadata::new(0, 0, 0),
            created_at: UnixTimestampMillis::new(1_750_000_000_000),
        }
    }

    fn sample_coverage() -> DisplacedRootArchiveCoverage {
        DisplacedRootArchiveCoverage {
            activation_event_sequence: 2,
            activation_epoch: ChainEpochId::new(1),
            activated_at: UnixTimestampMillis::new(1_750_000_000_000),
            captured_block_count: 5,
            root_artifact_unavailable_count: 0,
        }
    }

    #[test]
    fn displaced_paging_skips_readopted_and_duplicate_matches_across_pages()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let cov = sample_coverage();
        let page_limit = NonZeroU32::new(2).ok_or("nonzero limit")?;

        let (matches, displaced_coverage) = collect_displaced_matches_with(
            (2, page_limit),
            test_epoch(3),
            |cursor, _| match cursor {
                None => Ok(DisplacedPageBatch {
                    matches: vec![test_candidate(2, 0x23), test_candidate(4, 0x44)],
                    has_more: true,
                    next_cursor: Some(1_u32),
                    coverage: Some(cov),
                    scan_complete: true,
                }),
                Some(1) => Ok(DisplacedPageBatch {
                    matches: vec![test_candidate(4, 0x44), test_candidate(5, 0x55)],
                    has_more: true,
                    next_cursor: Some(2_u32),
                    coverage: Some(cov),
                    scan_complete: true,
                }),
                Some(_) => Err(Status::internal("unexpected page beyond limit")),
            },
            |heights| {
                Ok(heights
                    .iter()
                    .map(|height| (height.value() == 2).then(|| test_header(2, 0x23)))
                    .collect())
            },
        )?;

        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0].block_height, 4);
        assert_eq!(matches[1].block_height, 5);
        assert!(displaced_coverage.captured_range_complete);
        Ok(())
    }

    #[test]
    fn displaced_paging_marks_incomplete_when_fallback_scan_truncates()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let page_limit = NonZeroU32::new(2).ok_or("nonzero limit")?;
        let cov = sample_coverage();
        let (matches, displaced_coverage) = collect_displaced_matches_with(
            (4, page_limit),
            test_epoch(3),
            |_: Option<&u32>, _| {
                Ok(DisplacedPageBatch {
                    matches: vec![test_candidate(4, 0x44)],
                    has_more: false,
                    next_cursor: None,
                    coverage: Some(cov),
                    scan_complete: false,
                })
            },
            |heights| Ok(vec![None; heights.len()]),
        )?;
        assert_eq!(matches.len(), 1);
        assert!(!displaced_coverage.captured_range_complete);
        Ok(())
    }
}
