//! The native filtered subscription advances across quiet blocks and preserves resume cursors.

use std::{sync::Arc, time::Duration};

use eyre::eyre;
use tokio_stream::StreamExt as _;
use tonic::Request;
use zinder_core::{
    BlockHeight, Network, TransactionId, TransparentAddressScriptHash, TransparentOutPoint,
    TransparentUnspentOutput,
};
use zinder_proto::v1::wallet::{self, wallet_query_server::WalletQuery as _};
use zinder_store::{
    DEFAULT_MAX_CHAIN_EVENT_HISTORY_EVENTS, EventStreamStartPosition, StreamCursorTokenV1,
    event_stream_start_message,
};
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, IngestControlFixture,
    TransparentTestKey, sample_regtest_upgrade_activations,
};

use crate::{
    AdmittedIngestControl, RocksDbCanonicalReader, RocksDbWalletReader, WalletEndpointMetadata,
    WalletQueryGrpcAdapter, WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
};

fn pair(writer: &mut CanonicalWriterFixture) -> eyre::Result<Arc<WalletServingReadPair>> {
    let (canonical, wallet) = writer.serving_store_fixture()?.take_readers()?;
    Ok(Arc::new(WalletServingReadPair::new(
        Arc::new(RocksDbCanonicalReader::new(canonical)?),
        Arc::new(RocksDbWalletReader::new(wallet)),
    )?))
}

fn request(
    start: &EventStreamStartPosition,
    address: Option<String>,
) -> Request<wallet::ChainEventsRequest> {
    Request::new(wallet::ChainEventsRequest {
        start: Some(event_stream_start_message(start)),
        family: wallet::ChainEventStreamFamily::Visible as i32,
        address_filter: address.into_iter().collect(),
    })
}

fn credited_chain(key: &TransparentTestKey, first_match: u32) -> eyre::Result<ChainFixture> {
    let mut chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(first_match + 1);
    let script = key.address_script_bytes();
    for tag in [0, 1] {
        let block = chain
            .block_at(BlockHeight::new(first_match + u32::from(tag)))
            .ok_or_else(|| eyre!("missing credit block"))?;
        let credit = TransparentUnspentOutput::new(
            TransparentAddressScriptHash::of_script_pub_key(&script),
            script.clone(),
            TransparentOutPoint::new(TransactionId::from_bytes([tag + 1; 32]), 0),
            10,
            block.height,
            block.hash,
        );
        chain = chain.with_address_output_index(credit);
    }
    Ok(chain)
}

#[tokio::test]
async fn filtered_subscription_crosses_a_quiet_page_and_resumes_its_delivered_cursor()
-> eyre::Result<()> {
    let first_match = DEFAULT_MAX_CHAIN_EVENT_HISTORY_EVENTS.get() + 3;
    let network = Network::ZcashRegtest;
    let key = TransparentTestKey::from_seed(&[42; 32])?;
    let chain = credited_chain(&key, first_match)?;
    let mut writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).through(BlockHeight::new(1)),
        &sample_regtest_upgrade_activations(),
    )?;
    let slot = WalletServingPairSlot::new(pair(&mut writer)?);
    let control = IngestControlFixture::spawn(network).await?;
    let ingest = AdmittedIngestControl::connect(control.endpoint(), None, network).await?;
    let adapter = WalletQueryGrpcAdapter::new(
        WalletServingQuery::from_admitted_native_serving_pair(slot.clone(), ingest),
        WalletEndpointMetadata::default(),
    );
    let tail = EventStreamStartPosition::LiveTail;
    let mut filtered = adapter
        .chain_events(request(&tail, Some(key.address_base58())))
        .await?
        .into_inner();
    let mut unfiltered = adapter
        .chain_events(request(&tail, None))
        .await?
        .into_inner();

    writer.append_settled_blocks(&chain)?;
    slot.publish_for_test(pair(&mut writer)?);
    let matched = tokio::time::timeout(Duration::from_secs(5), filtered.next())
        .await?
        .ok_or_else(|| eyre!("filtered stream ended before the credit"))??;
    assert_eq!(matched.event_sequence, u64::from(first_match));
    let resume = EventStreamStartPosition::AfterCursor(StreamCursorTokenV1::from_bytes(
        matched.cursor.clone(),
    ));
    drop(filtered);
    let mut resumed = adapter
        .chain_events(request(&resume, Some(key.address_base58())))
        .await?
        .into_inner();
    let next_match = tokio::time::timeout(Duration::from_secs(5), resumed.next())
        .await?
        .ok_or_else(|| eyre!("resumed stream ended before the second credit"))??;
    assert_eq!(next_match.event_sequence, matched.event_sequence + 1);
    for sequence in 2..=next_match.event_sequence {
        let raw = unfiltered
            .next()
            .await
            .ok_or_else(|| eyre!("missing unfiltered event"))??;
        assert_eq!(raw.event_sequence, sequence);
        if sequence == matched.event_sequence {
            assert_eq!(raw, matched);
        } else if sequence == next_match.event_sequence {
            assert_eq!(raw, next_match);
        }
    }
    Ok(())
}
