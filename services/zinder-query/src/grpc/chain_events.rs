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

use futures_util::{Stream, future::Either, stream::try_unfold};
use tokio_stream::wrappers::ReceiverStream;

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

/// Reads every event into a bounded channel so the scan cursor advances
/// across quiet pages. The address filter controls only what the client receives.
pub(super) fn chain_event_stream<Q>(
    query_api: Q,
    resume: ChainEventStreamResume,
    address_filter: Vec<TransparentAddressScriptHash>,
    traffic_readiness: Option<Readiness>,
) -> impl Stream<Item = Result<wallet::ChainEventEnvelope, tonic::Status>>
where
    Q: WalletQueryApi + Clone + 'static,
{
    let reader = query_api.clone();
    let (sender, receiver) = mpsc::channel(16);
    tokio::spawn(run_event_stream(
        resume.cursor,
        move |cursor| {
            let reader = reader.clone();
            let writer_halt = traffic_readiness.as_ref().and_then(writer_halt_status);
            async move {
                if let Some(status) = writer_halt {
                    return Err(status);
                }
                chain_events_response(&reader, cursor, resume.family)
                    .await
                    .map_err(|error| crate::grpc::status_from_query_error(&error))
            }
        },
        sender,
    ));

    if address_filter.is_empty() {
        return Either::Left(ReceiverStream::new(receiver));
    }
    Either::Right(try_unfold(
        (receiver, query_api, address_filter),
        |(mut receiver, query, addresses)| async move {
            while let Some(envelope) = receiver.recv().await {
                let envelope = envelope?;
                if envelope_passes_filter(&query, &addresses, &envelope)
                    .await
                    .map_err(|error| crate::grpc::status_from_query_error(&error))?
                {
                    return Ok(Some((envelope, (receiver, query, addresses))));
                }
            }
            Ok(None)
        },
    ))
}

fn writer_halt_status(readiness: &Readiness) -> Option<tonic::Status> {
    readiness
        .reorg_window_exceeded()
        .map(|halt| not_ready_status(&ReadinessCause::from(halt)))
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
    };
    let page = query_api
        .transparent_address_tx_ids_in_range(request, None)
        .await?;
    Ok(!page.artifacts.is_empty())
}

#[cfg(test)]
mod filter_progress_tests;

#[cfg(test)]
mod tests {
    use super::{MAX_CHAIN_EVENTS_ADDRESS_FILTER, decode_address_filter};
    use crate::QueryError;
    use zinder_core::Network;

    #[test]
    fn empty_filter_decodes_to_empty_vec() -> Result<(), QueryError> {
        let filter = decode_address_filter(Vec::new(), Network::ZcashRegtest)?;
        assert_eq!(filter, [] as [zinder_core::TransparentAddressScriptHash; 0]);
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
