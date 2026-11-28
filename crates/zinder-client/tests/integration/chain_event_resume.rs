//! Durable SDK cursors across retained and pruned canonical reorganizations.

use std::{sync::Arc, time::Duration};

use eyre::eyre;
use tokio_stream::StreamExt as _;
use zinder_client::{
    BlockHeight, BlockHeightRange, BlockId, ChainEpochId, ChainEvent, ChainEventCursor,
    ChainEventEnvelope, ChainEventStream, ChainEventStreamFamily, ChainIndex, EndpointBackedIndex,
    EventStreamStart, Network, RemoteChainIndex, RemoteOpenOptions, TransactionId,
};
use zinder_query::{
    AdmittedIngestControl, RocksDbCanonicalReader, RocksDbWalletReader, WalletEndpointMetadata,
    WalletQueryGrpcAdapter, WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
};
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, FixtureTransactionRows,
    IngestControlFixture, WalletServingStoreFixture, sample_regtest_upgrade_activations,
};

use super::remote_chain_index::spawn_wallet_query;

async fn connect_to_fixture(
    fixture: &mut WalletServingStoreFixture,
    control: &IngestControlFixture,
) -> eyre::Result<RemoteChainIndex> {
    let (canonical, wallet) = fixture.take_readers()?;
    let pair = WalletServingReadPair::new(
        Arc::new(RocksDbCanonicalReader::new(canonical)?),
        Arc::new(RocksDbWalletReader::new(wallet)),
    )?;
    let network = Network::ZcashRegtest;
    let ingest = AdmittedIngestControl::connect(control.endpoint(), None, network).await?;
    let query = WalletServingQuery::from_admitted_native_serving_pair(
        WalletServingPairSlot::new(Arc::new(pair)),
        ingest,
    );
    let endpoint = spawn_wallet_query(WalletQueryGrpcAdapter::new(
        query,
        WalletEndpointMetadata::default(),
    ))
    .await?;
    Ok(RemoteChainIndex::connect(RemoteOpenOptions {
        endpoint,
        network,
    })?)
}

async fn first_event(
    writer: &mut CanonicalWriterFixture,
    control: &IngestControlFixture,
    start: EventStreamStart<ChainEventCursor>,
) -> eyre::Result<ChainEventEnvelope> {
    let mut fixture = writer.serving_store_fixture()?;
    let index = connect_to_fixture(&mut fixture, control).await?;
    next_event(&mut index.chain_events(start).await?).await
}

async fn next_event(stream: &mut ChainEventStream) -> eyre::Result<ChainEventEnvelope> {
    Ok(tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await?
        .ok_or_else(|| eyre!("chain stream ended before the next event"))??)
}

fn with_tip_coinbase(chain: ChainFixture, tag: u8) -> eyre::Result<ChainFixture> {
    let tip = chain.blocks().last().ok_or_else(|| eyre!("missing tip"))?;
    let coinbase = FixtureTransactionRows::coinbase(
        TransactionId::from_bytes([tag; 32]),
        tip.height,
        tip.hash,
        vec![tag],
    );
    Ok(chain.with_transaction_rows(coinbase))
}

#[tokio::test]
async fn visible_cursor_recovers_after_pruned_reorgs_and_resumes_the_next_live_append()
-> eyre::Result<()> {
    let control = IngestControlFixture::spawn(Network::ZcashRegtest).await?;
    let fork = BlockHeight::new(5);
    let chain = with_tip_coinbase(ChainFixture::new(Network::ZcashRegtest).extend_blocks(5), 1)?;
    let mut writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).with_settled_tip(BlockHeight::new(3)),
        &sample_regtest_upgrade_activations(),
    )?;
    let baseline = first_event(&mut writer, &control, EventStreamStart::EarliestRetained).await?;
    let resume = EventStreamStart::AfterCursor(baseline.cursor);
    let first_branch = with_tip_coinbase(chain.fork_at(fork)?.extend_blocks(1), 2)?;
    writer.replace_live_blocks(&first_branch, fork)?;
    writer.prune_canonical_events_before(2)?;
    let retained = first_event(&mut writer, &control, resume.clone()).await?;
    assert_eq!(retained.event_sequence, 2);
    assert!(
        matches!(retained.event, ChainEvent::ChainReorged { reverted, .. }
        if reverted.block_range.start == fork)
    );

    let second_branch = first_branch.fork_at(fork)?.extend_blocks(1);
    writer.replace_live_blocks(&second_branch, fork)?;
    let branch = second_branch.extend_blocks(2);
    let settled = branch
        .block_at(BlockHeight::new(6))
        .ok_or_else(|| eyre!("missing settled block on the replacement chain"))?;
    let common = branch
        .block_at(BlockHeight::new(4))
        .ok_or_else(|| eyre!("missing fork parent"))?;
    writer.append_live_block_with_settled_tip(&branch, BlockId::new(common.height, common.hash))?;
    writer.prune_canonical_events_before(3)?;
    let first_recovery = first_event(&mut writer, &control, resume.clone()).await?;
    assert_eq!(first_recovery.event_sequence, 4);
    assert_eq!(first_recovery.settled_tip_height, common.height);
    assert!(matches!(
        first_recovery.event,
        ChainEvent::ChainReorged { .. }
    ));

    writer
        .append_live_block_with_settled_tip(&branch, BlockId::new(settled.height, settled.hash))?;
    let appended = first_event(
        &mut writer,
        &control,
        EventStreamStart::AfterCursor(first_recovery.cursor),
    )
    .await?;
    assert_eq!(appended.event_sequence, 5);
    assert!(matches!(appended.event, ChainEvent::ChainCommitted { .. }));
    let recovered = first_event(&mut writer, &control, resume).await?;
    assert_eq!(recovered.cursor, appended.cursor);
    assert_eq!(recovered.chain_epoch.id, ChainEpochId::new(5));
    assert_eq!(recovered.settled_tip_height, settled.height);
    assert!(
        matches!(recovered.event, ChainEvent::ChainReorged { reverted, committed }
        if reverted.chain_epoch == baseline.chain_epoch
        && reverted.block_range.start == fork
        && committed.block_range.end == BlockHeight::new(7))
    );
    Ok(())
}

#[tokio::test]
async fn settled_cursor_resumes_newly_settled_ranges_with_the_original_visible_epoch()
-> eyre::Result<()> {
    let control = IngestControlFixture::spawn(Network::ZcashRegtest).await?;
    let chain = with_tip_coinbase(ChainFixture::new(Network::ZcashRegtest).extend_blocks(4), 1)?;
    let mut writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).with_settled_tip(BlockHeight::new(2)),
        &sample_regtest_upgrade_activations(),
    )?;
    let extended = chain.extend_blocks(1);
    let settled = extended
        .block_at(BlockHeight::new(3))
        .ok_or_else(|| eyre!("missing settled block"))?;
    writer.append_live_block_with_settled_tip(
        &extended,
        BlockId::new(settled.height, settled.hash),
    )?;
    let mut fixture = writer.serving_store_fixture()?;
    let index = connect_to_fixture(&mut fixture, &control).await?;
    let mut stream = index
        .chain_events_for_family(
            EventStreamStart::EarliestRetained,
            ChainEventStreamFamily::Settled,
        )
        .await?;
    let baseline = next_event(&mut stream).await?;
    assert!(
        matches!(baseline.event, ChainEvent::ChainCommitted { committed }
        if committed.block_range == BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(2))
        && committed.chain_epoch == baseline.chain_epoch)
    );
    assert_eq!(baseline.chain_epoch.visible_tip_height, BlockHeight::new(4));
    let cursor = ChainEventCursor::from_bytes(baseline.cursor.as_bytes());
    let mut resumed = index
        .chain_events(EventStreamStart::AfterCursor(cursor))
        .await?;
    let appended = next_event(&mut resumed).await?;
    assert_eq!(appended, next_event(&mut stream).await?);
    assert_eq!(appended.chain_epoch, index.current_epoch().await?);
    assert!(
        matches!(appended.event, ChainEvent::ChainCommitted { committed }
        if committed.block_range == BlockHeightRange::inclusive(settled.height, settled.height)
        && committed.chain_epoch == appended.chain_epoch)
    );
    Ok(())
}
