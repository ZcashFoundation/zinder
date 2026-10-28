//! `WalletQuery.ChainEvents` server-side filter logic.
//!
//! Owns the `address_filter` semantics defined in the chain-events
//! architecture: clients pass a
//! list of transparent addresses they care about; the server narrows
//! delivered envelopes to those whose committed block range touches at
//! least one of the supplied addresses. Reorged envelopes always pass
//! through because consumers must invalidate cached derivations after a
//! reorg regardless of which addresses they watch.
//!
//! Cursor opacity is preserved: every emitted envelope carries the cursor
//! the server would have emitted without a filter. A resuming client with
//! a different filter receives an envelope set consistent with the new
//! filter applied from the cursor forward.

use std::num::NonZeroU32;

use tokio::sync::mpsc;
use zinder_core::{BlockHeight, Network, TransparentAddressScriptHash};
use zinder_proto::v1::wallet;
use zinder_runtime::{Readiness, ReadinessCause, not_ready_status};
use zinder_store::{ChainEventStreamResume, run_event_stream};

use crate::{
    QueryError, TransparentAddressTxIdsInRangeRequest, WalletQueryApi,
    grpc::native::{address_lookup_to_script_hash, chain_events_response},
};

/// Maximum addresses a single `chain_events.address_filter` may carry.
///
/// Each request fans out to at most this many M4 transparent-address
/// tx-history index probes per envelope; capping the filter bounds the
/// per-envelope work without restricting realistic faucet/payment-receiver
/// workloads (which watch tens, not thousands, of addresses).
pub(super) const MAX_CHAIN_EVENTS_ADDRESS_FILTER: usize = 256;

/// Decodes the raw address strings in [`wallet::ChainEventsRequest::address_filter`]
/// into typed script hashes.
///
/// Reuses [`address_lookup_to_script_hash`] so the parsing rules match every
/// other transparent-address-accepting RPC (Base58 t-address with network
/// validation, network-mismatch rejection).
///
/// # Errors
///
/// Returns [`QueryError::InvalidAddress`] when an entry fails to parse or
/// targets a different network, and when the filter exceeds
/// [`MAX_CHAIN_EVENTS_ADDRESS_FILTER`].
pub(super) fn decode_address_filter(
    raw_addresses: Vec<String>,
    network: Network,
) -> Result<Vec<TransparentAddressScriptHash>, QueryError> {
    if raw_addresses.len() > MAX_CHAIN_EVENTS_ADDRESS_FILTER {
        return Err(QueryError::InvalidAddress {
            reason: "chain_events address_filter exceeds the per-request cap",
        });
    }
    let mut out = Vec::with_capacity(raw_addresses.len());
    for raw in raw_addresses {
        let lookup = wallet::AddressLookup {
            selector: Some(wallet::address_lookup::Selector::Address(raw)),
        };
        out.push(address_lookup_to_script_hash(Some(lookup), network)?);
    }
    Ok(out)
}

/// Spawns the server task that drives one client's chain-events stream and
/// applies the address-filter semantics from `docs/architecture/chain-events.md`
/// when the filter is
/// non-empty.
///
/// The cursor and family follow the same shape as the unfiltered stream;
/// the only behavioral change is post-fetch envelope filtering.
///
/// When `traffic_readiness` reports a parked canonical writer, the next page
/// read ends the stream with the typed halt status instead of polling a
/// chain that no longer advances.
pub(super) fn spawn_filtered_stream<Q>(
    query_api: Q,
    resume: ChainEventStreamResume,
    address_filter: Vec<TransparentAddressScriptHash>,
    traffic_readiness: Option<Readiness>,
    event_sender: mpsc::Sender<Result<wallet::ChainEventEnvelope, tonic::Status>>,
) where
    Q: WalletQueryApi + Clone + 'static,
{
    let filter = std::sync::Arc::new(address_filter);
    let family = resume.family;
    tokio::spawn(run_event_stream(
        resume.cursor,
        move |cursor| {
            let query_api = query_api.clone();
            let filter = filter.clone();
            let writer_halt = traffic_readiness.as_ref().and_then(writer_halt_status);
            async move {
                if let Some(status) = writer_halt {
                    return Err(status);
                }
                let envelopes = chain_events_response(&query_api, cursor, family)
                    .await
                    .map_err(|error| crate::grpc::status_from_query_error(&error))?;
                if filter.is_empty() {
                    return Ok(envelopes);
                }
                filter_envelopes(&query_api, &filter, envelopes).await
            }
        },
        event_sender,
    ));
}

fn writer_halt_status(readiness: &Readiness) -> Option<tonic::Status> {
    readiness
        .reorg_window_exceeded()
        .map(|halt| not_ready_status(&ReadinessCause::from(halt)))
}

/// Filters `envelopes` against `filter`, preserving every reorg envelope
/// and emitting commits only when their committed block range touches at
/// least one of the filter's addresses.
async fn filter_envelopes<Q: WalletQueryApi + ?Sized>(
    query_api: &Q,
    filter: &[TransparentAddressScriptHash],
    envelopes: Vec<wallet::ChainEventEnvelope>,
) -> Result<Vec<wallet::ChainEventEnvelope>, tonic::Status> {
    let mut out = Vec::with_capacity(envelopes.len());
    for envelope in envelopes {
        if envelope_passes_filter(query_api, filter, &envelope)
            .await
            .map_err(|error| crate::grpc::status_from_query_error(&error))?
        {
            out.push(envelope);
        }
    }
    Ok(out)
}

/// Returns true when `envelope` should be delivered to a client whose
/// `address_filter` is `filter`.
///
/// Reorg envelopes always pass. Commit envelopes pass when at least one
/// filter address has activity in the committed block range, as observed
/// through the M4 transparent-address tx-history index.
async fn envelope_passes_filter<Q: WalletQueryApi + ?Sized>(
    query_api: &Q,
    filter: &[TransparentAddressScriptHash],
    envelope: &wallet::ChainEventEnvelope,
) -> Result<bool, QueryError> {
    let Some(event) = envelope.event.as_ref() else {
        // No event payload means an empty advertisement; let it through so
        // the cursor advances on the client side.
        return Ok(true);
    };
    let committed = match event {
        wallet::chain_event_envelope::Event::ChainReorged(_) => {
            // Reorgs always pass; clients invalidate cached derivations
            // regardless of which addresses they were watching.
            return Ok(true);
        }
        wallet::chain_event_envelope::Event::ChainCommitted(commit) => commit.committed.as_ref(),
    };
    let Some(committed) = committed else {
        return Ok(true);
    };
    let start_height = BlockHeight::new(committed.start_height);
    let end_height = BlockHeight::new(committed.end_height);
    for address in filter {
        if address_touched_in_range(query_api, *address, start_height, end_height).await? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// One-page touch probe against the M4 transparent-address tx-history
/// index. Returns true on first artifact; bounded to a single-entry page.
async fn address_touched_in_range<Q: WalletQueryApi + ?Sized>(
    query_api: &Q,
    address: TransparentAddressScriptHash,
    start_height: BlockHeight,
    end_height: BlockHeight,
) -> Result<bool, QueryError> {
    let request = TransparentAddressTxIdsInRangeRequest {
        address_script_hash: address,
        start_height,
        end_height,
        max_entries: NonZeroU32::MIN,
        from_cursor: None,
        descending: false,
    };
    let page = query_api
        .transparent_address_tx_ids_in_range(request)
        .await?;
    Ok(!page.artifacts.is_empty())
}

#[cfg(test)]
mod tests {
    use super::{MAX_CHAIN_EVENTS_ADDRESS_FILTER, decode_address_filter};
    use crate::QueryError;
    use zinder_core::Network;

    #[test]
    fn empty_filter_decodes_to_empty_vec() -> Result<(), QueryError> {
        let filter = decode_address_filter(Vec::new(), Network::ZcashRegtest)?;
        assert!(filter.is_empty());
        Ok(())
    }

    #[test]
    fn unparseable_address_is_rejected() {
        let outcome =
            decode_address_filter(vec!["not-an-address".to_owned()], Network::ZcashRegtest);
        assert!(matches!(outcome, Err(QueryError::InvalidAddress { .. })));
    }

    #[test]
    fn oversized_filter_is_rejected() {
        let oversized = vec![
            "t1tBLB1xQbCJtYDFRm3HsKpAaGfocrSsELT".to_owned();
            MAX_CHAIN_EVENTS_ADDRESS_FILTER + 1
        ];
        let outcome = decode_address_filter(oversized, Network::ZcashRegtest);
        assert!(matches!(
            outcome,
            Err(QueryError::InvalidAddress {
                reason: "chain_events address_filter exceeds the per-request cap"
            })
        ));
    }
}
