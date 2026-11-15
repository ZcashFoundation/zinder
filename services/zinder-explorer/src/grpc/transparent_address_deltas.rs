//! `ExplorerQuery.TransparentAddressDeltas` handler.
//!
//! Reads the per-event signed value series materialized by
//! [`zinder_materialized_views::TransparentAddressDeltasConsumer`] out of the
//! consumer-owned `transparent_address_deltas` column family. The storage
//! layout sorts ascending by height under each address prefix, so the handler
//! serves pages oldest-first and clients consume the zcashd
//! `getaddressdeltas` order directly.

use prost::Message as _;
use tonic::{Request, Response, Status};
use zinder_core::wire::{
    decode_address_script_hash, decode_height_key_ascending, encode_address_script_hash,
    encode_rpc_transaction_id_hex,
};
use zinder_core::{Network, TransparentAddressScriptHash};
use zinder_proto::capabilities::EXPLORER_TRANSPARENT_ADDRESS_DELTAS_V1;
use zinder_proto::v1::explorer::{
    TransparentAddressDeltasEntry, TransparentAddressDeltasRecord, TransparentAddressDeltasRequest,
    TransparentAddressDeltasResponse,
};
use zinder_proto::v1::wallet::{VisibleTipBlockRequest, wallet_query_client::WalletQueryClient};
use zinder_proto::wire::decode_transparent_delta_kind;
use zinder_runtime::AuthenticatedChannel;

use super::clamp_max_entries;
use super::error::ExplorerError;
use super::freshness::{
    UpstreamObservationCache, attach_upstream_observation, build_explorer_freshness,
};
use super::transparent_address_activity::address_lookup_to_script_hash;
use zinder_materialized_views::{
    MaterializedViewStore, TRANSPARENT_ADDRESS_DELTAS_COLUMN_FAMILY,
    TransparentAddressDeltasConsumer,
};

/// Hard cap on the delta rows one page returns.
const MAX_TRANSPARENT_ADDRESS_DELTAS_ENTRIES_PER_REQUEST: u32 = 256;

/// Default entries when the caller passes `max_entries = 0`.
const DEFAULT_TRANSPARENT_ADDRESS_DELTAS_ENTRIES: u32 = 64;

const ADDRESS_HASH_LEN: usize = 32;
const HEIGHT_KEY_OFFSET: usize = ADDRESS_HASH_LEN;
const HEIGHT_KEY_END: usize = HEIGHT_KEY_OFFSET + 4;
const KIND_OFFSET: usize = HEIGHT_KEY_END + 4;

/// Length of one primary storage key:
/// 32 address + 4 ascending-height + 4 position + 1 kind + 4 event-index.
const DELTAS_KEY_LEN: usize = zinder_materialized_views::TRANSPARENT_ADDRESS_DELTAS_KEY_LEN;

/// Executes one `ExplorerQuery.TransparentAddressDeltas` request.
pub(crate) async fn query_transparent_address_deltas(
    materialized_view_store: &MaterializedViewStore,
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    network: Network,
    upstream_observation_cache: &UpstreamObservationCache,
    request: Request<TransparentAddressDeltasRequest>,
) -> Result<Response<TransparentAddressDeltasResponse>, Status> {
    let inner = request.into_inner();
    let address = inner
        .address
        .ok_or_else(|| ExplorerError::invalid_request("address selector is required"))?;
    let script_hash = address_lookup_to_script_hash(&address, network)?;
    let max_entries = clamp_max_entries(
        inner.max_entries,
        DEFAULT_TRANSPARENT_ADDRESS_DELTAS_ENTRIES,
        MAX_TRANSPARENT_ADDRESS_DELTAS_ENTRIES_PER_REQUEST,
    );
    let (entries, next_cursor) = scan_address_deltas(
        materialized_view_store,
        &DeltaScanParameters {
            script_hash,
            start_height: inner.start_height,
            end_height: inner.end_height,
            max_entries,
            from_cursor: inner.from_cursor.as_slice(),
        },
    )?;
    let latest = wallet_client
        .visible_tip_block(Request::new(VisibleTipBlockRequest { at_epoch_id: None }))
        .await?
        .into_inner();
    let chain_epoch = latest
        .chain_view
        .and_then(|chain_view| chain_view.chain_epoch)
        .ok_or_else(|| {
            ExplorerError::internal("VisibleTipBlockResponse.chain_view.chain_epoch missing")
        })?;
    let freshness = attach_upstream_observation(
        upstream_observation_cache,
        build_explorer_freshness(
            Some(materialized_view_store),
            EXPLORER_TRANSPARENT_ADDRESS_DELTAS_V1,
            Some(chain_epoch),
            0,
        )?,
    )
    .await;
    Ok(Response::new(TransparentAddressDeltasResponse {
        freshness: Some(freshness),
        entries,
        next_cursor,
    }))
}

/// Bundled inputs to [`scan_address_deltas`].
struct DeltaScanParameters<'a> {
    script_hash: TransparentAddressScriptHash,
    start_height: u32,
    end_height: u32,
    max_entries: u32,
    from_cursor: &'a [u8],
}

fn scan_address_deltas(
    materialized_view_store: &MaterializedViewStore,
    parameters: &DeltaScanParameters<'_>,
) -> Result<(Vec<TransparentAddressDeltasEntry>, Vec<u8>), Status> {
    let prefix = encode_address_script_hash(parameters.script_hash);
    let ScanKeys {
        start_key,
        end_key,
        resume_cursor,
    } = build_scan_keys(prefix, parameters.from_cursor)?;

    let scan_cap =
        (parameters.max_entries as usize).saturating_add(usize::from(resume_cursor.is_some()));
    let rows = materialized_view_store
        .range_iterate_consumer(
            TRANSPARENT_ADDRESS_DELTAS_COLUMN_FAMILY,
            &start_key,
            &end_key,
            scan_cap,
        )
        .map_err(|error| ExplorerError::internal(error.to_string()))?;

    let mut entries: Vec<TransparentAddressDeltasEntry> = Vec::with_capacity(rows.len());
    let mut last_key: Option<RowKey> = None;
    for (key, payload) in rows {
        let key_array: RowKey = key
            .as_slice()
            .try_into()
            .map_err(|_| ExplorerError::internal("delta row key not the expected length"))?;
        if resume_cursor.is_some_and(|cursor| key_array == cursor) {
            continue;
        }
        let key_address = decode_address_script_hash(&key_array[0..ADDRESS_HASH_LEN])
            .map_err(|error| ExplorerError::internal(error.to_string()))?;
        if key_address != parameters.script_hash {
            break;
        }
        let height = decode_row_height(&key_array)?;
        if height < parameters.start_height || height > parameters.end_height {
            continue;
        }
        let kind = decode_transparent_delta_kind(key_array[KIND_OFFSET])
            .map_err(|error| ExplorerError::internal(error.to_string()))?;
        let event_index = decode_event_index(&key_array)?;
        let record = TransparentAddressDeltasRecord::decode(payload.as_slice())
            .map_err(|error| ExplorerError::internal(error.to_string()))?;
        let transaction_id = TransparentAddressDeltasConsumer::stored_transaction_id(&record)
            .map_err(|error| ExplorerError::internal(error.to_string()))?;
        entries.push(TransparentAddressDeltasEntry {
            transaction_id: encode_rpc_transaction_id_hex(transaction_id),
            block_height: height,
            block_time_unix_seconds: record.block_time_unix_seconds,
            index: event_index,
            value_zat: record.value_zat,
            kind: kind as i32,
        });
        last_key = Some(key_array);
        if u32::try_from(entries.len()).unwrap_or(u32::MAX) >= parameters.max_entries {
            break;
        }
    }

    let next_cursor = if u32::try_from(entries.len()).unwrap_or(u32::MAX) >= parameters.max_entries
    {
        last_key.map_or_else(Vec::new, |key| key.to_vec())
    } else {
        Vec::new()
    };
    Ok((entries, next_cursor))
}

/// Owned row-key array, repeated three times in [`ScanKeys`].
type RowKey = [u8; DELTAS_KEY_LEN];

/// Iterator bounds and optional resume cursor returned by [`build_scan_keys`].
struct ScanKeys {
    start_key: RowKey,
    end_key: RowKey,
    resume_cursor: Option<RowKey>,
}

fn build_scan_keys(prefix: [u8; ADDRESS_HASH_LEN], from_cursor: &[u8]) -> Result<ScanKeys, Status> {
    let mut start_key = [0u8; DELTAS_KEY_LEN];
    start_key[0..ADDRESS_HASH_LEN].copy_from_slice(&prefix);
    let mut end_key = [0xFFu8; DELTAS_KEY_LEN];
    end_key[0..ADDRESS_HASH_LEN].copy_from_slice(&prefix);
    if from_cursor.is_empty() {
        return Ok(ScanKeys {
            start_key,
            end_key,
            resume_cursor: None,
        });
    }
    let bytes: [u8; DELTAS_KEY_LEN] = from_cursor
        .try_into()
        .map_err(|_| ExplorerError::invalid_request("from_cursor must be the delta key length"))?;
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
    decode_height_key_ascending(&height_bytes)
        .map(zinder_core::BlockHeight::value)
        .map_err(|error| ExplorerError::internal(error.to_string()).into())
}

fn decode_event_index(key: &RowKey) -> Result<u32, Status> {
    let index_bytes: [u8; 4] = key[KIND_OFFSET + 1..]
        .try_into()
        .map_err(|_| ExplorerError::internal("event-index segment not 4 bytes"))?;
    Ok(u32::from_be_bytes(index_bytes))
}
