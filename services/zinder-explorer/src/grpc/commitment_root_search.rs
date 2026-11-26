//! `ExplorerQuery.CommitmentRootSearch` handler.

use std::collections::HashSet;

use tonic::{Request, Response, Status};
use zinder_core::{
    BlockHeight, ChainEpoch, FinalNoteCommitmentRoot, ShieldedProtocol as CoreShieldedProtocol,
    wire::encode_rpc_block_hash_hex,
};
use zinder_materialized_views::{
    COMMITMENT_ROOT_SEARCH_CONSUMER_NAME, CommitmentRootBackfillCoverage,
    CommitmentRootSearchConsumer, MaterializedViewStore,
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
    let canonical_candidates = CommitmentRootSearchConsumer::search(mv_store, root, limit)
        .map_err(|error| ExplorerError::internal(error.to_string()))?;
    let backfill = CommitmentRootSearchConsumer::backfill_coverage(mv_store)
        .map_err(|error| ExplorerError::internal(error.to_string()))?;
    let consumer_state = mv_store
        .consumer_state(COMMITMENT_ROOT_SEARCH_CONSUMER_NAME)
        .map_err(|error| ExplorerError::internal(error.to_string()))?;

    let matches = filter_canonical_matches(secondary, epoch, &canonical_candidates, limit)?;
    let coverage = build_canonical_coverage(
        secondary,
        epoch,
        backfill,
        consumer_state.map(|state| state.tip_height),
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
    state_tip_height: Option<BlockHeight>,
) -> CommitmentRootSearchCoverage {
    let latest_indexed_height = state_tip_height
        .map(|height| height.value().min(epoch.visible_tip_height.value()))
        .or_else(|| backfill.map(|cov| cov.complete_through_height.value()));
    let first_retained = secondary.history_bounds().first_available_height();
    let earliest_root_height = earliest_shielded_activation_height(secondary);
    let canonical_history_complete = first_retained == BlockHeight::new(1)
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
    let displaced = secondary
        .search_displaced_final_note_commitment_roots(root, limit)
        .map_err(internal_canonical_error)?;
    let heights: Vec<BlockHeight> = displaced
        .matches
        .iter()
        .map(|candidate| candidate.block_id.height)
        .collect();
    let headers = secondary
        .block_headers_at(&heights)
        .map_err(internal_canonical_error)?;

    let mut seen = HashSet::with_capacity(displaced.matches.len());
    let mut matches = Vec::with_capacity(displaced.matches.len().min(limit));
    for (candidate, maybe_header) in displaced.matches.into_iter().zip(headers) {
        if matches.len() >= limit {
            break;
        }
        let still_canonical = candidate.block_id.height <= epoch.visible_tip_height
            && maybe_header.is_some_and(|header| header.block_hash == candidate.block_id.hash);
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

    let coverage = encode_displaced_coverage(displaced.coverage);
    Ok((matches, coverage))
}

const fn encode_displaced_coverage(
    coverage: Option<zinder_core::DisplacedRootArchiveCoverage>,
) -> CommitmentRootSearchDisplacedCoverage {
    match coverage {
        Some(cov) => CommitmentRootSearchDisplacedCoverage {
            activation_event_sequence: Some(cov.activation_event_sequence),
            activation_epoch_id: Some(cov.activation_epoch.value()),
            activated_at_millis: Some(cov.activated_at.value()),
            captured_block_count: cov.captured_block_count,
            root_artifact_unavailable_count: cov.root_artifact_unavailable_count,
            captured_range_complete: cov.root_artifact_unavailable_count == 0,
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
