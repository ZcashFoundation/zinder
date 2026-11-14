//! `ExplorerQuery.TransparentAddressActivity` handler.
//!
//! Reads the confirmed-activity feed materialized by
//! [`zinder_materialized_views::TransparentAddressActivityConsumer`]
//! out of the consumer-owned `transparent_address_activity` column family.
//! The storage layout sorts newest-first per address, so the handler
//! serves pages in that order; clients that want oldest-first reverse
//! client-side.

use prost::Message as _;
use tonic::{Request, Response, Status};
use zebra_chain::transparent::Address as ZebraTransparentAddress;
use zinder_core::wire::{
    decode_address_script_hash, decode_height_key_descending, decode_rpc_block_hash_hex,
    encode_address_script_hash,
};
use zinder_core::{BlockHeight, BlockId, Network, TransparentAddressScriptHash};
use zinder_proto::capabilities::EXPLORER_TRANSPARENT_ADDRESS_ACTIVITY_V2;
use zinder_proto::v1::explorer::{
    TransparentAddressActivityEntry, TransparentAddressActivityRecord,
    TransparentAddressActivityRequest, TransparentAddressActivityResponse,
    TransparentAddressRankingCoverage, TransparentAddressSummary as WireTransparentAddressSummary,
};
use zinder_proto::v1::wallet::{
    self, AddressLookup, BlockSelector, VisibleTipBlockRequest,
    address_lookup::Selector as AddressSelector, block_selector,
    wallet_query_client::WalletQueryClient,
};
use zinder_runtime::AuthenticatedChannel;

use super::clamp_max_entries;
use super::error::ExplorerError;
use super::freshness::{
    UpstreamObservationCache, attach_upstream_observation, build_explorer_freshness,
};
use zinder_materialized_views::{
    MaterializedViewStore, MaterializedViewStoreReadSnapshot,
    TRANSPARENT_ADDRESS_ACTIVITY_COLUMN_FAMILY, TRANSPARENT_ADDRESS_ACTIVITY_KEY_LEN,
    TransparentAddressRankingConsumer, TransparentAddressRankingConsumerError,
    TransparentAddressSummary as ProjectedTransparentAddressSummary,
};

/// Hard cap on the activity rows one page returns.
const MAX_TRANSPARENT_ADDRESS_ACTIVITY_ENTRIES_PER_REQUEST: u32 = 256;

/// Default entries when the caller passes `max_entries = 0`.
const DEFAULT_TRANSPARENT_ADDRESS_ACTIVITY_ENTRIES: u32 = 64;

/// Hard cap on offset work for page-number compatibility adapters.
const MAX_TRANSPARENT_ADDRESS_ACTIVITY_OFFSET: u64 = 100_000;

const ADDRESS_HASH_LEN: usize = 32;
const HEIGHT_KEY_OFFSET: usize = ADDRESS_HASH_LEN;
const HEIGHT_KEY_END: usize = HEIGHT_KEY_OFFSET + 4;

/// Executes one `ExplorerQuery.TransparentAddressActivity` request.
pub(crate) struct TransparentAddressActivityContext<'store> {
    pub(crate) materialized_view_store: &'store MaterializedViewStore,
    pub(crate) network: Network,
    pub(crate) upstream_observation_cache: &'store UpstreamObservationCache,
}

pub(crate) async fn query_transparent_address_activity(
    context: TransparentAddressActivityContext<'_>,
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    request: Request<TransparentAddressActivityRequest>,
) -> Result<Response<TransparentAddressActivityResponse>, Status> {
    let TransparentAddressActivityContext {
        materialized_view_store,
        network,
        upstream_observation_cache,
    } = context;
    let inner = request.into_inner();
    validate_activity_pagination(inner.offset, &inner.from_cursor)?;
    let address = inner
        .address
        .ok_or_else(|| ExplorerError::invalid_request("address selector is required"))?;
    let resolved_address = resolve_address_lookup(&address, network)?;
    let max_entries = clamp_max_entries(
        inner.max_entries,
        DEFAULT_TRANSPARENT_ADDRESS_ACTIVITY_ENTRIES,
        MAX_TRANSPARENT_ADDRESS_ACTIVITY_ENTRIES_PER_REQUEST,
    );
    let chain_epoch = resolve_activity_chain_epoch(wallet_client, inner.at_epoch_id).await?;
    let block = visible_tip_block(&chain_epoch)?;
    let store = materialized_view_store.clone();
    let address_read = tokio::task::spawn_blocking(move || {
        read_address_at_block(
            &store,
            &resolved_address,
            block,
            &ActivityScanParameters {
                script_hash: resolved_address.script_hash,
                offset: inner.offset,
                start_height: inner.start_height,
                end_height: inner.end_height,
                max_entries,
                from_cursor: inner.from_cursor.as_slice(),
            },
        )
    })
    .await
    .map_err(|error| {
        ExplorerError::internal(format!("transparent-address activity read failed: {error}"))
    })??;
    if let Some(trailing_tip) = address_read.trailing_tip {
        let epoch_block =
            epoch_block_at_height(wallet_client, &chain_epoch, trailing_tip.height).await?;
        require_trailing_tip_on_epoch(trailing_tip, epoch_block)?;
    }
    let freshness = attach_upstream_observation(
        upstream_observation_cache,
        build_explorer_freshness(
            Some(materialized_view_store),
            EXPLORER_TRANSPARENT_ADDRESS_ACTIVITY_V2,
            Some(chain_epoch),
            0,
        )?,
    )
    .await;
    Ok(Response::new(TransparentAddressActivityResponse {
        freshness: Some(freshness),
        entries: address_read.entries,
        next_cursor: address_read.next_cursor,
        summary: Some(address_read.summary),
        coverage: Some(address_read.coverage),
    }))
}

async fn resolve_activity_chain_epoch(
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    at_epoch_id: Option<u64>,
) -> Result<wallet::ChainEpoch, Status> {
    wallet_client
        .visible_tip_block(Request::new(VisibleTipBlockRequest { at_epoch_id }))
        .await?
        .into_inner()
        .chain_view
        .and_then(|chain_view| chain_view.chain_epoch)
        .ok_or_else(|| {
            ExplorerError::internal("VisibleTipBlockResponse.chain_view.chain_epoch missing").into()
        })
}

fn visible_tip_block(chain_epoch: &wallet::ChainEpoch) -> Result<BlockId, Status> {
    let visible_tip = chain_epoch.visible_tip.as_ref().ok_or_else(|| {
        ExplorerError::internal("VisibleTipBlockResponse chain epoch visible_tip missing")
    })?;
    let hash = decode_rpc_block_hash_hex(&visible_tip.hash).map_err(|error| {
        ExplorerError::internal(format!(
            "VisibleTipBlockResponse chain epoch visible_tip hash is invalid: {error}"
        ))
    })?;
    Ok(BlockId::new(BlockHeight::new(visible_tip.height), hash))
}

async fn epoch_block_at_height(
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    chain_epoch: &wallet::ChainEpoch,
    height: BlockHeight,
) -> Result<BlockId, Status> {
    let block_id = wallet_client
        .block_id_by_selector(Request::new(wallet::BlockSelectorRequest {
            selector: Some(BlockSelector {
                selector: Some(block_selector::Selector::Height(height.value())),
            }),
            at_epoch_id: Some(chain_epoch.chain_epoch_id),
        }))
        .await?
        .into_inner()
        .block_id
        .ok_or_else(|| ExplorerError::internal("BlockIdResponse.block_id missing"))?;
    let hash = decode_rpc_block_hash_hex(&block_id.block_hash).map_err(|error| {
        ExplorerError::internal(format!("BlockIdResponse.block_id hash is invalid: {error}"))
    })?;
    Ok(BlockId::new(BlockHeight::new(block_id.height), hash))
}

/// Refuses an answer from a ranking that trails the epoch on another branch,
/// as when a reorg reached the wallet query before the views replayed it.
fn require_trailing_tip_on_epoch(
    trailing_tip: BlockId,
    epoch_block: BlockId,
) -> Result<(), Status> {
    if trailing_tip == epoch_block {
        return Ok(());
    }
    Err(ExplorerError::unsatisfied_precondition(format!(
        "transparent-address ranking applied a different block at height {} than the requested chain epoch",
        trailing_tip.height.value()
    ))
    .into())
}

/// Activity rows, summary, and ranking coverage for one address at one block.
struct AddressReadAtBlock {
    entries: Vec<TransparentAddressActivityEntry>,
    next_cursor: Vec<u8>,
    summary: WireTransparentAddressSummary,
    coverage: TransparentAddressRankingCoverage,
    /// Ranking tip to find on the epoch's chain before serving the answer.
    trailing_tip: Option<BlockId>,
}

/// Reads one address as the Explorer views stood at `block`.
///
/// The ranking summary and the activity rows come from one store snapshot, so
/// a block the views apply meanwhile cannot split them. Rows above `block` are
/// left out because the summary does not include them.
fn read_address_at_block(
    materialized_view_store: &MaterializedViewStore,
    resolved_address: &ResolvedAddressLookup,
    block: BlockId,
    scan: &ActivityScanParameters<'_>,
) -> Result<AddressReadAtBlock, Status> {
    let snapshot = materialized_view_store.read_snapshot();
    let summary_at_block = TransparentAddressRankingConsumer::summary_at_block(
        &snapshot,
        resolved_address.script_hash,
        block,
    )
    .map_err(|error| ranking_read_status(&error))?
    .ok_or_else(|| {
        ExplorerError::not_materialized(
            "transparent-address ranking has no active materialized generation",
        )
    })?;
    let (entries, next_cursor) = scan_address_activity(
        &snapshot,
        &ActivityScanParameters {
            end_height: scan.end_height.min(block.height.value()),
            ..*scan
        },
    )?;
    drop(snapshot);
    Ok(AddressReadAtBlock {
        entries,
        next_cursor,
        summary: encode_address_summary(
            summary_at_block.summary,
            resolved_address.script_pub_key.as_deref(),
        ),
        coverage: encode_ranking_coverage(summary_at_block.coverage),
        trailing_tip: summary_at_block.trailing_tip,
    })
}

fn ranking_read_status(error: &TransparentAddressRankingConsumerError) -> Status {
    if error.is_read_block_unavailable() {
        ExplorerError::unsatisfied_precondition(format!(
            "transparent-address ranking cannot answer at the requested chain epoch: {error}"
        ))
        .into()
    } else {
        ExplorerError::internal(error.to_string()).into()
    }
}

fn validate_activity_pagination(offset: u64, from_cursor: &[u8]) -> Result<(), Status> {
    if offset != 0 && !from_cursor.is_empty() {
        return Err(ExplorerError::invalid_request(
            "from_cursor and nonzero offset are mutually exclusive",
        )
        .into());
    }
    if offset > MAX_TRANSPARENT_ADDRESS_ACTIVITY_OFFSET {
        return Err(ExplorerError::invalid_request(format!(
            "offset exceeds maximum {MAX_TRANSPARENT_ADDRESS_ACTIVITY_OFFSET}",
        ))
        .into());
    }
    Ok(())
}

/// Bundled inputs to [`scan_address_activity`].
#[derive(Clone, Copy)]
struct ActivityScanParameters<'a> {
    script_hash: TransparentAddressScriptHash,
    offset: u64,
    start_height: u32,
    end_height: u32,
    max_entries: u32,
    from_cursor: &'a [u8],
}

fn scan_address_activity(
    snapshot: &MaterializedViewStoreReadSnapshot<'_>,
    parameters: &ActivityScanParameters<'_>,
) -> Result<(Vec<TransparentAddressActivityEntry>, Vec<u8>), Status> {
    let prefix = encode_address_script_hash(parameters.script_hash);
    let ScanKeys {
        mut start_key,
        end_key,
        mut resume_cursor,
    } = build_scan_keys(prefix, parameters.from_cursor)?;
    let scan_cap = usize::try_from(MAX_TRANSPARENT_ADDRESS_ACTIVITY_ENTRIES_PER_REQUEST)
        .unwrap_or(usize::MAX)
        .saturating_add(1);
    let mut remaining_offset = parameters.offset;
    let mut entries =
        Vec::with_capacity(usize::try_from(parameters.max_entries).unwrap_or(usize::MAX));
    let mut last_key: Option<RowKey> = None;
    'scan: loop {
        let rows = snapshot
            .range_iterate_consumer(
                TRANSPARENT_ADDRESS_ACTIVITY_COLUMN_FAMILY,
                &start_key,
                &end_key,
                scan_cap,
            )
            .map_err(|error| ExplorerError::internal(error.to_string()))?;
        let row_count = rows.len();
        let mut last_scanned_key = None;
        for (key, payload) in rows {
            let key_array: RowKey = key
                .as_slice()
                .try_into()
                .map_err(|_| ExplorerError::internal("activity row key not 40 bytes"))?;
            if resume_cursor.is_some_and(|cursor| key_array == cursor) {
                continue;
            }
            let key_address = decode_address_script_hash(&key_array[0..ADDRESS_HASH_LEN])
                .map_err(|error| ExplorerError::internal(error.to_string()))?;
            if key_address != parameters.script_hash {
                break 'scan;
            }
            last_scanned_key = Some(key_array);
            let height = decode_row_height(&key_array)?;
            if height < parameters.start_height || height > parameters.end_height {
                continue;
            }
            if remaining_offset != 0 {
                remaining_offset -= 1;
                continue;
            }
            let record = TransparentAddressActivityRecord::decode(payload.as_slice())
                .map_err(|error| ExplorerError::internal(error.to_string()))?;
            entries.push(activity_entry_from_record(record, height));
            last_key = Some(key_array);
            if u32::try_from(entries.len()).unwrap_or(u32::MAX) >= parameters.max_entries {
                break 'scan;
            }
        }
        if row_count < scan_cap {
            break;
        }
        let Some(last_scanned_key) = last_scanned_key else {
            break;
        };
        start_key = last_scanned_key;
        resume_cursor = Some(last_scanned_key);
    }

    let next_cursor = if u32::try_from(entries.len()).unwrap_or(u32::MAX) >= parameters.max_entries
    {
        last_key.map_or_else(Vec::new, |key| key.to_vec())
    } else {
        Vec::new()
    };
    Ok((entries, next_cursor))
}

fn activity_entry_from_record(
    record: TransparentAddressActivityRecord,
    block_height: u32,
) -> TransparentAddressActivityEntry {
    TransparentAddressActivityEntry {
        transaction_id: record.transaction_id,
        block_height,
        block_time_unix_seconds: record.block_time_unix_seconds,
        net_value_zat: record.net_value_zat,
        input_count: record.input_count,
        output_count: record.output_count,
        prevout_resolution_status: record.prevout_resolution_status,
        transaction_index: None,
        size_bytes: None,
        component_counts: None,
        input_value_zat: None,
        output_value_zat: None,
        other_input_script_pub_keys: Vec::new(),
        other_output_script_pub_keys: Vec::new(),
        input_facts_complete: false,
    }
}

fn encode_address_summary(
    summary: Option<ProjectedTransparentAddressSummary>,
    requested_script_pub_key: Option<&[u8]>,
) -> WireTransparentAddressSummary {
    let Some(summary) = summary else {
        return WireTransparentAddressSummary {
            script_pub_key: requested_script_pub_key.map(<[u8]>::to_vec),
            balance_zat: 0,
            total_received_zat: None,
            total_sent_zat: None,
            distinct_transaction_count: None,
            first_seen_unix_seconds: None,
            last_seen_unix_seconds: None,
        };
    };
    WireTransparentAddressSummary {
        script_pub_key: summary
            .script_pub_key
            .or_else(|| requested_script_pub_key.map(<[u8]>::to_vec)),
        balance_zat: summary.balance_zat,
        total_received_zat: Some(summary.total_received_zat),
        total_sent_zat: Some(summary.total_sent_zat),
        distinct_transaction_count: Some(summary.distinct_transaction_count),
        first_seen_unix_seconds: minimum_optional(
            summary.first_seen_unix_seconds,
            summary.snapshot_first_seen_unix_seconds,
        ),
        last_seen_unix_seconds: maximum_optional(
            summary.last_seen_unix_seconds,
            summary.snapshot_last_seen_unix_seconds,
        ),
    }
}

fn encode_ranking_coverage(
    coverage: zinder_materialized_views::TransparentAddressRankingCoverage,
) -> TransparentAddressRankingCoverage {
    TransparentAddressRankingCoverage {
        balance_complete_through_height: coverage.balance_complete_through_height.value(),
        history_complete_from_height: coverage
            .history_complete_from_height
            .map(BlockHeight::value),
        history_complete_through_height: coverage
            .history_complete_through_height
            .map(BlockHeight::value),
        lifetime_statistics_complete: coverage.lifetime_statistics_complete,
    }
}

fn minimum_optional(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(timestamp), None) | (None, Some(timestamp)) => Some(timestamp),
        (None, None) => None,
    }
}

fn maximum_optional(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(timestamp), None) | (None, Some(timestamp)) => Some(timestamp),
        (None, None) => None,
    }
}

/// Owned row-key array, repeated three times in [`ScanKeys`].
type RowKey = [u8; TRANSPARENT_ADDRESS_ACTIVITY_KEY_LEN];

/// Iterator bounds and optional resume cursor returned by [`build_scan_keys`].
struct ScanKeys {
    start_key: RowKey,
    end_key: RowKey,
    resume_cursor: Option<RowKey>,
}

/// Computes the iterator bounds and the optional resume-cursor row.
fn build_scan_keys(prefix: [u8; ADDRESS_HASH_LEN], from_cursor: &[u8]) -> Result<ScanKeys, Status> {
    let mut start_key = [0u8; TRANSPARENT_ADDRESS_ACTIVITY_KEY_LEN];
    start_key[0..ADDRESS_HASH_LEN].copy_from_slice(&prefix);
    let mut end_key = [0xFFu8; TRANSPARENT_ADDRESS_ACTIVITY_KEY_LEN];
    end_key[0..ADDRESS_HASH_LEN].copy_from_slice(&prefix);
    if from_cursor.is_empty() {
        return Ok(ScanKeys {
            start_key,
            end_key,
            resume_cursor: None,
        });
    }
    let bytes: [u8; TRANSPARENT_ADDRESS_ACTIVITY_KEY_LEN] = from_cursor
        .try_into()
        .map_err(|_| ExplorerError::invalid_request("from_cursor must be 40 bytes"))?;
    if bytes[0..ADDRESS_HASH_LEN] != prefix {
        return Err(ExplorerError::invalid_request(
            "from_cursor address prefix does not match request address",
        )
        .into());
    }
    start_key = bytes;
    Ok(ScanKeys {
        start_key,
        end_key,
        resume_cursor: Some(bytes),
    })
}

fn decode_row_height(key: &RowKey) -> Result<u32, Status> {
    let height_bytes: [u8; 4] = key[HEIGHT_KEY_OFFSET..HEIGHT_KEY_END]
        .try_into()
        .map_err(|_| ExplorerError::internal("height segment not 4 bytes"))?;
    decode_height_key_descending(&height_bytes)
        .map(BlockHeight::value)
        .map_err(|error| ExplorerError::internal(error.to_string()).into())
}

/// Resolves an [`AddressLookup`] selector to a transparent script hash,
/// rejecting addresses whose network does not match the server network.
pub(crate) fn address_lookup_to_script_hash(
    address: &AddressLookup,
    network: Network,
) -> Result<TransparentAddressScriptHash, Status> {
    resolve_address_lookup(address, network).map(|resolved| resolved.script_hash)
}

struct ResolvedAddressLookup {
    script_hash: TransparentAddressScriptHash,
    script_pub_key: Option<Vec<u8>>,
}

fn resolve_address_lookup(
    address: &AddressLookup,
    network: Network,
) -> Result<ResolvedAddressLookup, Status> {
    let selector = address
        .selector
        .as_ref()
        .ok_or_else(|| ExplorerError::invalid_request("address selector arm is required"))?;
    match selector {
        AddressSelector::ScriptHash(bytes) => {
            let hash_bytes: [u8; 32] = bytes
                .as_slice()
                .try_into()
                .map_err(|_| ExplorerError::invalid_request("script_hash must be 32 bytes"))?;
            Ok(ResolvedAddressLookup {
                script_hash: TransparentAddressScriptHash::from_bytes(hash_bytes),
                script_pub_key: None,
            })
        }
        AddressSelector::Address(text) => {
            let parsed = text.parse::<ZebraTransparentAddress>().map_err(|_| {
                ExplorerError::invalid_request("transparent address could not be parsed")
            })?;
            if !network_matches(parsed.network_kind(), network) {
                return Err(ExplorerError::invalid_request(
                    "transparent address network does not match server network",
                )
                .into());
            }
            let script_pub_key = parsed.script().as_raw_bytes().to_vec();
            if script_pub_key.is_empty() {
                return Err(ExplorerError::invalid_request(
                    "transparent address resolved to empty scriptPubKey",
                )
                .into());
            }
            Ok(ResolvedAddressLookup {
                script_hash: TransparentAddressScriptHash::of_script_pub_key(&script_pub_key),
                script_pub_key: Some(script_pub_key),
            })
        }
    }
}

fn network_matches(
    address_network: zebra_chain::parameters::NetworkKind,
    server_network: Network,
) -> bool {
    use zebra_chain::parameters::NetworkKind;
    matches!(
        (address_network, server_network),
        (NetworkKind::Mainnet, Network::ZcashMainnet)
            | (
                NetworkKind::Testnet | NetworkKind::Regtest,
                Network::ZcashTestnet | Network::ZcashRegtest
            )
    )
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, error::Error, sync::Arc};

    use prost::Message as _;
    use tempfile::{TempDir, tempdir};
    use tonic::Code;
    use zinder_core::wire::encode_rpc_block_hash_hex;
    use zinder_core::{
        ArtifactSchemaVersion, BlockHash, BlockHeightRange, ChainEpoch, ChainEpochId,
        ChainTipMetadata, TransactionFactsArtifact, TransactionId, TransactionLocation,
        TransactionPublicFacts, TransparentOutputFact, UnixTimestampMillis,
    };
    use zinder_materialized_views::{
        BlockCommitContext, BlockCommitInput, BlockKeyedConsumer, ChainEventDispatchInputs,
        MaterializedViewStoreOptions, TRANSPARENT_ADDRESS_ACTIVITY_SCHEMA,
        TRANSPARENT_ADDRESS_RANKING_SCHEMA, TransparentAddressActivityConsumer,
        TransparentSpendFacts,
    };
    use zinder_store::{ChainEpochCommitted, ChainEvent, RocksDbResourceBudget};
    use zinder_testkit::synthetic_transaction_public_facts;

    use super::*;

    type TestResult<T = ()> = Result<T, Box<dyn Error>>;

    #[test]
    fn offset_and_cursor_are_mutually_exclusive() {
        let outcome = validate_activity_pagination(1, &[0xAA]);
        assert!(matches!(outcome, Err(error) if error.code() == Code::InvalidArgument));
    }

    #[test]
    fn address_read_answers_at_an_epoch_the_ranking_advanced_past() -> TestResult {
        let (_tempdir, store) = open_address_views()?;
        let address = receiving_address();
        commit_receipts(&store, &address, &[(1, 100), (2, 200)])?;
        let epoch_tip = BlockId::new(BlockHeight::new(2), test_block_hash(2));
        commit_receipts(&store, &address, &[(3, 400)])?;

        let read = read_address_at_block(&store, &address, epoch_tip, &full_scan(&address))?;

        assert_eq!(read.summary.balance_zat, 300);
        assert_eq!(read.summary.total_received_zat, Some(300));
        assert_eq!(read.summary.distinct_transaction_count, Some(2));
        assert_eq!(read.coverage.balance_complete_through_height, 2);
        assert_eq!(read.coverage.history_complete_through_height, Some(2));
        let heights = read
            .entries
            .iter()
            .map(|entry| entry.block_height)
            .collect::<Vec<_>>();
        assert_eq!(heights, [2, 1]);
        assert_eq!(read.trailing_tip, None);
        Ok(())
    }

    #[test]
    fn address_read_names_the_ranking_tip_while_the_ranking_trails_the_epoch() -> TestResult {
        let (_tempdir, store) = open_address_views()?;
        let address = receiving_address();
        commit_receipts(&store, &address, &[(1, 100), (2, 200)])?;
        let epoch_tip = BlockId::new(BlockHeight::new(4), test_block_hash(4));

        let read = read_address_at_block(&store, &address, epoch_tip, &full_scan(&address))?;

        assert_eq!(read.summary.balance_zat, 300);
        assert_eq!(read.coverage.balance_complete_through_height, 2);
        assert_eq!(
            read.trailing_tip,
            Some(BlockId::new(BlockHeight::new(2), test_block_hash(2)))
        );
        Ok(())
    }

    #[test]
    fn trailing_ranking_tip_must_be_on_the_epoch_chain() -> TestResult {
        let trailing_tip = BlockId::new(BlockHeight::new(2), test_block_hash(2));

        require_trailing_tip_on_epoch(trailing_tip, trailing_tip)?;
        let replaced = BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([0xEE; 32]));
        let outcome = require_trailing_tip_on_epoch(trailing_tip, replaced);

        assert!(matches!(outcome, Err(error) if error.code() == Code::FailedPrecondition));
        Ok(())
    }

    #[test]
    fn address_read_at_the_ranking_tip_includes_every_block() -> TestResult {
        let (_tempdir, store) = open_address_views()?;
        let address = receiving_address();
        commit_receipts(&store, &address, &[(1, 100), (2, 200), (3, 400)])?;
        let epoch_tip = BlockId::new(BlockHeight::new(3), test_block_hash(3));

        let read = read_address_at_block(&store, &address, epoch_tip, &full_scan(&address))?;

        assert_eq!(read.summary.balance_zat, 700);
        assert_eq!(read.coverage.balance_complete_through_height, 3);
        assert_eq!(read.entries.len(), 3);
        Ok(())
    }

    #[test]
    fn address_read_refuses_an_epoch_on_another_branch() -> TestResult {
        let (_tempdir, store) = open_address_views()?;
        let address = receiving_address();
        commit_receipts(&store, &address, &[(1, 100), (2, 200), (3, 400)])?;
        let replaced_tip = BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([0xEE; 32]));

        let outcome = read_address_at_block(&store, &address, replaced_tip, &full_scan(&address));

        assert!(matches!(outcome, Err(error) if error.code() == Code::FailedPrecondition));
        Ok(())
    }

    #[test]
    fn visible_tip_block_decodes_the_epoch_tip_identity() -> TestResult {
        let hash = test_block_hash(9);
        let chain_epoch = wallet::ChainEpoch {
            visible_tip: Some(wallet::BlockTip {
                height: 9,
                hash: encode_rpc_block_hash_hex(hash),
            }),
            ..Default::default()
        };

        assert_eq!(
            visible_tip_block(&chain_epoch)?,
            BlockId::new(BlockHeight::new(9), hash)
        );
        Ok(())
    }

    #[test]
    fn offset_skips_matching_rows_without_changing_cursor_order() -> TestResult {
        let tempdir = tempdir()?;
        let store = MaterializedViewStore::open(
            tempdir.path(),
            MaterializedViewStoreOptions {
                consumers: &[TRANSPARENT_ADDRESS_ACTIVITY_SCHEMA],
                rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
                sync_writes: false,
            },
        )?;
        let script_hash = TransparentAddressScriptHash::from_bytes([0x11; 32]);
        for (height, seed) in [(103, 3_u8), (102, 2_u8), (101, 1_u8)] {
            let record = TransparentAddressActivityRecord {
                transaction_id: format!("{seed:02x}").repeat(32),
                block_time_unix_seconds: i64::from(height),
                net_value_zat: Some(i64::from(seed)),
                input_count: 0,
                output_count: 1,
                prevout_resolution_status:
                    zinder_proto::v1::explorer::PrevoutResolutionStatus::Resolved as i32,
            };
            store.put_consumer(
                TRANSPARENT_ADDRESS_ACTIVITY_COLUMN_FAMILY,
                &TransparentAddressActivityConsumer::key_for_row(
                    script_hash,
                    BlockHeight::new(height),
                    0,
                ),
                &record.encode_to_vec(),
            )?;
        }

        let (entries, next_cursor) = scan_address_activity(
            &store.read_snapshot(),
            &ActivityScanParameters {
                script_hash,
                offset: 1,
                start_height: 0,
                end_height: u32::MAX,
                max_entries: 1,
                from_cursor: &[],
            },
        )?;

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].block_height, 102);
        assert_eq!(next_cursor.len(), TRANSPARENT_ADDRESS_ACTIVITY_KEY_LEN);
        Ok(())
    }

    #[test]
    fn missing_summary_maps_to_valid_zero_state_with_requested_script() {
        let script = vec![0x76, 0xa9, 0x14, 0x88, 0xac];
        let summary = encode_address_summary(None, Some(&script));

        assert_eq!(summary.script_pub_key, Some(script));
        assert_eq!(summary.balance_zat, 0);
        assert_eq!(summary.total_received_zat, None);
        assert_eq!(summary.total_sent_zat, None);
        assert_eq!(summary.distinct_transaction_count, None);
        assert_eq!(summary.first_seen_unix_seconds, None);
        assert_eq!(summary.last_seen_unix_seconds, None);
    }

    #[test]
    fn summary_uses_snapshot_and_tail_timestamp_extrema() {
        let summary = encode_address_summary(
            Some(ProjectedTransparentAddressSummary {
                script_pub_key: None,
                balance_zat: 900,
                total_received_zat: 1_200,
                total_sent_zat: 300,
                distinct_transaction_count: 7,
                first_seen_unix_seconds: Some(120),
                last_seen_unix_seconds: Some(180),
                snapshot_first_seen_unix_seconds: Some(100),
                snapshot_last_seen_unix_seconds: Some(200),
            }),
            Some(&[0x51]),
        );

        assert_eq!(summary.script_pub_key, Some(vec![0x51]));
        assert_eq!(summary.first_seen_unix_seconds, Some(100));
        assert_eq!(summary.last_seen_unix_seconds, Some(200));
        assert_eq!(summary.total_received_zat, Some(1_200));
    }

    fn open_address_views() -> TestResult<(TempDir, MaterializedViewStore)> {
        let tempdir = tempdir()?;
        let store = MaterializedViewStore::open(
            tempdir.path(),
            MaterializedViewStoreOptions {
                consumers: &[
                    TRANSPARENT_ADDRESS_ACTIVITY_SCHEMA,
                    TRANSPARENT_ADDRESS_RANKING_SCHEMA,
                ],
                rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
                sync_writes: false,
            },
        )?;
        TransparentAddressRankingConsumer::activate_genesis_generation(&store)?;
        Ok((tempdir, store))
    }

    fn receiving_address() -> ResolvedAddressLookup {
        let mut script_pub_key = vec![0x76, 0xa9, 0x14];
        script_pub_key.extend_from_slice(&[0x42; 20]);
        script_pub_key.extend_from_slice(&[0x88, 0xac]);
        ResolvedAddressLookup {
            script_hash: TransparentAddressScriptHash::of_script_pub_key(&script_pub_key),
            script_pub_key: Some(script_pub_key),
        }
    }

    fn full_scan(address: &ResolvedAddressLookup) -> ActivityScanParameters<'static> {
        ActivityScanParameters {
            script_hash: address.script_hash,
            offset: 0,
            start_height: 0,
            end_height: u32::MAX,
            max_entries: DEFAULT_TRANSPARENT_ADDRESS_ACTIVITY_ENTRIES,
            from_cursor: &[],
        }
    }

    fn test_block_hash(height: u32) -> BlockHash {
        let mut bytes = [0; 32];
        bytes[..4].copy_from_slice(&height.to_be_bytes());
        BlockHash::from_bytes(bytes)
    }

    /// Commits one block per `(height, value)` receipt through the replay
    /// dispatch, so the ranking and activity views advance together.
    fn commit_receipts(
        store: &MaterializedViewStore,
        address: &ResolvedAddressLookup,
        receipts: &[(u32, u64)],
    ) -> TestResult {
        let (Some(&(start, _)), Some(&(end, _))) = (receipts.first(), receipts.last()) else {
            return Ok(());
        };
        let mut blocks = HashMap::new();
        for &(height, value_zat) in receipts {
            blocks.insert(
                BlockHeight::new(height),
                Arc::new(receipt_block(height, address, value_zat)?),
            );
        }
        let chain_epoch = ChainEpoch {
            id: ChainEpochId::new(u64::from(end)),
            network: Network::ZcashRegtest,
            visible_tip_height: BlockHeight::new(end),
            visible_tip_hash: test_block_hash(end),
            settled_tip_height: BlockHeight::new(0),
            settled_tip_hash: Network::ZcashRegtest.genesis_hash(),
            artifact_schema_version: ArtifactSchemaVersion::new(1),
            tip_metadata: ChainTipMetadata::empty(),
            created_at: UnixTimestampMillis::new(u64::from(end)),
        };
        let chain_event = ChainEvent::ChainCommitted {
            committed: ChainEpochCommitted {
                chain_epoch,
                block_range: BlockHeightRange::inclusive(
                    BlockHeight::new(start),
                    BlockHeight::new(end),
                ),
            },
        };
        let cursor = end.to_be_bytes();
        let mut ranking = TransparentAddressRankingConsumer::new();
        let mut activity = TransparentAddressActivityConsumer::new();
        let mut consumers: [&mut dyn BlockKeyedConsumer; 2] = [&mut ranking, &mut activity];
        store.write_chain_event_chunk(
            &mut consumers,
            ChainEventDispatchInputs {
                chain_epoch,
                chain_event: &chain_event,
                chain_cursor: &cursor,
                event_sequence: u64::from(end),
                settled_tip_height: BlockHeight::new(0),
            },
            &blocks,
            true,
        )?;
        Ok(())
    }

    fn receipt_block(
        height: u32,
        address: &ResolvedAddressLookup,
        value_zat: u64,
    ) -> TestResult<BlockCommitContext> {
        let block_height = BlockHeight::new(height);
        let block_hash = test_block_hash(height);
        let transaction_id = TransactionId::from_bytes(block_hash.as_bytes());
        let script_pub_key = address
            .script_pub_key
            .clone()
            .ok_or("receiving address has no script")?;
        let transaction = TransactionFactsArtifact::new(
            TransactionLocation::new(transaction_id, block_height, block_hash, 0),
            TransactionPublicFacts {
                is_coinbase: true,
                ..synthetic_transaction_public_facts(transaction_id, 0)
            },
        )
        .with_transparent_facts(
            Vec::new(),
            vec![TransparentOutputFact::new(
                0,
                value_zat,
                script_pub_key,
                address.script_hash,
            )],
        );
        Ok(BlockCommitContext::new(
            BlockCommitInput {
                height: block_height,
                block_hash,
                previous_block_hash: test_block_hash(height.saturating_sub(1)),
                block_time_unix_seconds: 1_700_000_000 + i64::from(height),
                block_size_bytes: 0,
                transactions: vec![transaction],
                final_note_commitment_roots: None,
            },
            TransparentSpendFacts::Offline,
        ))
    }
}
