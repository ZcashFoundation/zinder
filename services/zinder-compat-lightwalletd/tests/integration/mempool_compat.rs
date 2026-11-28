#![allow(
    missing_docs,
    reason = "Integration test names describe the lightwalletd compat behavior under test."
)]

use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

use async_trait::async_trait;
use eyre::eyre;
use parking_lot::Mutex;
use tokio::sync::{Notify, mpsc, oneshot, watch};
use tokio_stream::{Stream, StreamExt as _, wrappers::UnboundedReceiverStream};
use tonic::{Code, Request};
use zinder_compat_lightwalletd::{
    IngestControlMempoolSurface, LightwalletdGrpcAdapter, MempoolEventEnvelopeStream,
    MempoolSnapshotPage, MempoolSurface, MempoolSurfaceError, MempoolTransactionLookup,
    TipChangeWatcher, TipChangeWatcherError,
};
use zinder_core::{
    AuthDigest, BlockHash, BlockHeight, ChainEpoch, ChainEpochId, ChainTipMetadata,
    CompactSaplingOutput, CompactSaplingSpend, CompactShieldedAction, CompactTransactionData,
    CompactTransparentInput, CompactTransparentOutput, MempoolEntry, MempoolEvictionReason,
    MempoolObservation, Network, RawTransactionBytes, TransactionId, UnixTimestampMillis,
    wire::encode_internal_transaction_id,
};
use zinder_proto::compat::lightwalletd::{self, compact_tx_streamer_server::CompactTxStreamer};
use zinder_proto::wire::TransactionAbsence;
use zinder_query::{
    RocksDbCanonicalReader, RocksDbWalletReader, WalletQueryApi as _, WalletServingPairSlot,
    WalletServingQuery, WalletServingReadPair,
};
use zinder_store::{
    CURRENT_ARTIFACT_SCHEMA_VERSION, MempoolEvent, MempoolEventEnvelope, StreamCursorTokenV1,
};
use zinder_testkit::{
    ChainFixture, IngestControlFixture, WalletServingStoreFixture,
    sample_regtest_upgrade_activations,
};

/// Builds the release serving query over one published block. The mempool
/// surface under test is injected separately, so the chain content is fixed.
fn single_block_serving_query() -> eyre::Result<(WalletServingQuery<()>, WalletServingStoreFixture)>
{
    let activations = Arc::new(sample_regtest_upgrade_activations());
    let mut store_fixture = WalletServingStoreFixture::from_chain(
        &ChainFixture::new(Network::ZcashRegtest).extend_blocks(1),
        &activations,
    )?;
    let (canonical, wallet) = store_fixture.take_readers()?;
    let query = WalletServingQuery::from_serving_pair_slot(WalletServingPairSlot::new(Arc::new(
        WalletServingReadPair::new(
            Arc::new(RocksDbCanonicalReader::new(canonical)?),
            Arc::new(RocksDbWalletReader::new(wallet)),
        )?,
    )));
    Ok((query, store_fixture))
}

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_tx_returns_unavailable_without_surface() -> eyre::Result<()> {
    let (wallet_query, _store_fixture) = single_block_serving_query()?;
    let adapter = LightwalletdGrpcAdapter::new(wallet_query);
    let outcome = adapter
        .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest {
            exclude_txid_suffixes: Vec::new(),
            pool_types: Vec::new(),
        }))
        .await;
    let status = outcome.err().ok_or_else(|| eyre!("expected unavailable"))?;
    assert_eq!(status.code(), tonic::Code::Unavailable);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_tx_rejects_oversized_excluded_txid_suffixes_before_surface_lookup()
-> eyre::Result<()> {
    let (wallet_query, _store_fixture) = single_block_serving_query()?;
    let adapter = LightwalletdGrpcAdapter::new(wallet_query);

    let mut suffixes: Vec<_> = (0..19_999u32).map(|i| i.to_be_bytes().to_vec()).collect();
    suffixes.push(vec![0; 33]);
    let outcome = adapter
        .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest {
            exclude_txid_suffixes: suffixes,
            pool_types: Vec::new(),
        }))
        .await;
    let status = outcome
        .err()
        .ok_or_else(|| eyre!("expected invalid excluded txid suffix"))?;
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(
        status.message(),
        "exclude txid 19999 is larger than 32 bytes"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_tx_rejects_invalid_pool_type_before_surface_lookup()
-> eyre::Result<()> {
    let (wallet_query, _store_fixture) = single_block_serving_query()?;
    let adapter = LightwalletdGrpcAdapter::new(wallet_query);

    let outcome = adapter
        .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest {
            exclude_txid_suffixes: Vec::new(),
            pool_types: vec![lightwalletd::PoolType::Invalid as i32],
        }))
        .await;
    let status = outcome
        .err()
        .ok_or_else(|| eyre!("expected invalid pool type"))?;
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(status.message(), "invalid pool type requested");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_tx_filters_with_twenty_thousand_suffixes() -> eyre::Result<()> {
    let (wallet_query, _store_fixture) = single_block_serving_query()?;
    let surface = ScriptedMempoolSurface::with_entries(vec![
        synthetic_entry(0xAA, synthetic_chain_epoch())?,
        synthetic_entry(0xBB, synthetic_chain_epoch())?,
    ]);
    let adapter =
        LightwalletdGrpcAdapter::new(wallet_query).with_mempool_surface(Arc::new(surface));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    tokio::spawn(
        zinder_runtime::public_grpc_server(zinder_runtime::ClientAddressSource::Connection)
            .add_service(adapter.into_server())
            .serve_with_incoming(listener),
    );
    let mut client =
        lightwalletd::compact_tx_streamer_client::CompactTxStreamerClient::connect(endpoint)
            .await?;
    let mut suffixes: Vec<_> = (0..19_999u32).map(|i| i.to_be_bytes().to_vec()).collect();
    suffixes.push(vec![0xAA; 32]);
    let response = client
        .get_mempool_tx(lightwalletd::GetMempoolTxRequest {
            exclude_txid_suffixes: suffixes,
            pool_types: Vec::new(),
        })
        .await?
        .into_inner();
    let collected = collect_compact_txids(response).await?;
    assert_eq!(collected.len(), 1);
    assert_eq!(collected[0], [0xBB; 32]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_tx_preserves_transactions_when_excluded_suffix_is_ambiguous()
-> eyre::Result<()> {
    let (wallet_query, _store_fixture) = single_block_serving_query()?;
    let mut first_transaction_id = [0x11; 32];
    first_transaction_id[31] = 0xAA;
    let mut second_transaction_id = [0x22; 32];
    second_transaction_id[31] = 0xAA;
    let surface = ScriptedMempoolSurface::with_entries(vec![
        synthetic_entry_with_transaction_id(first_transaction_id, synthetic_chain_epoch())?,
        synthetic_entry_with_transaction_id(second_transaction_id, synthetic_chain_epoch())?,
    ])
    .with_snapshot_page_size(1);
    let adapter =
        LightwalletdGrpcAdapter::new(wallet_query).with_mempool_surface(Arc::new(surface));

    let response = adapter
        .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest {
            exclude_txid_suffixes: vec![vec![0xAA]],
            pool_types: Vec::new(),
        }))
        .await?
        .into_inner();
    let collected = collect_compact_txids(response).await?;
    assert_eq!(collected, vec![first_transaction_id, second_transaction_id]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_tx_treats_suffix_colliding_outside_requested_pools_as_ambiguous()
-> eyre::Result<()> {
    let (wallet_query, _store_fixture) = single_block_serving_query()?;
    let mut streamable_transaction_id = [0x11; 32];
    streamable_transaction_id[31] = 0xAA;
    let mut pruned_transaction_id = [0x22; 32];
    pruned_transaction_id[31] = 0xAA;
    let mut unique_streamable_id = [0x33; 32];
    unique_streamable_id[31] = 0xBB;
    let surface = ScriptedMempoolSurface::with_entries(vec![
        synthetic_entry_with_transaction_id(streamable_transaction_id, synthetic_chain_epoch())?,
        ironwood_only_entry_with_transaction_id(pruned_transaction_id, synthetic_chain_epoch())?,
        synthetic_entry_with_transaction_id(unique_streamable_id, synthetic_chain_epoch())?,
    ])
    .with_snapshot_page_size(1);
    let adapter =
        LightwalletdGrpcAdapter::new(wallet_query).with_mempool_surface(Arc::new(surface));

    let response = adapter
        .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest {
            // `0xAA` matches both the Sapling tx and the Ironwood-only tx in the
            // mempool, so it is ambiguous and excludes neither. `0xBB` uniquely
            // matches `unique_streamable_id` and excludes it.
            exclude_txid_suffixes: vec![vec![0xAA], vec![0x11, 0xAA], vec![0xBB]],
            pool_types: vec![lightwalletd::PoolType::Sapling as i32],
        }))
        .await?
        .into_inner();
    let collected = collect_compact_txids(response).await?;
    assert_eq!(collected, vec![streamable_transaction_id.to_vec()]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_tx_drops_transactions_outside_requested_pool_types()
-> eyre::Result<()> {
    let (wallet_query, _store_fixture) = single_block_serving_query()?;
    let surface = ScriptedMempoolSurface::with_entries(vec![
        synthetic_entry(0xC1, synthetic_chain_epoch())?,
        transparent_only_entry(0xC2, synthetic_chain_epoch())?,
    ]);
    let adapter =
        LightwalletdGrpcAdapter::new(wallet_query).with_mempool_surface(Arc::new(surface));

    let response = adapter
        .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest {
            exclude_txid_suffixes: Vec::new(),
            pool_types: vec![lightwalletd::PoolType::Transparent as i32],
        }))
        .await?
        .into_inner();
    let mut transactions = response;
    let transaction = transactions
        .next()
        .await
        .ok_or_else(|| eyre!("expected transparent mempool transaction"))??;
    assert_eq!(transaction.txid, [0xC2; 32]);
    assert!(
        transaction.vin.is_empty(),
        "reference lightwalletd omits transparent mempool inputs"
    );
    assert_eq!(transaction.vout.len(), 1);
    assert!(
        transactions.next().await.is_none(),
        "expected exactly one transparent mempool transaction"
    );
    Ok(())
}

#[tokio::test]
async fn mempool_unknown_pool_values_select_only_recognized_components() -> eyre::Result<()> {
    let (wallet_query, _store_fixture) = single_block_serving_query()?;
    let surface = ScriptedMempoolSurface::with_entries(vec![
        synthetic_entry(0xC1, synthetic_chain_epoch())?,
        transparent_only_entry(0xC2, synthetic_chain_epoch())?,
    ]);
    let adapter =
        LightwalletdGrpcAdapter::new(wallet_query).with_mempool_surface(Arc::new(surface));
    let transparent = lightwalletd::PoolType::Transparent as i32;
    for (pool_types, expected_ids) in [
        (vec![], vec![vec![0xC1; 32]]),
        (vec![-1, 99], vec![]),
        (vec![99, transparent, transparent], vec![vec![0xC2; 32]]),
    ] {
        let stream = adapter
            .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest {
                pool_types,
                exclude_txid_suffixes: Vec::new(),
            }))
            .await?
            .into_inner();
        assert_eq!(collect_compact_txids(stream).await?, expected_ids);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_tx_drops_ironwood_entries_outside_requested_pool_types()
-> eyre::Result<()> {
    let (wallet_query, _store_fixture) = single_block_serving_query()?;
    let surface = ScriptedMempoolSurface::with_entries(vec![
        synthetic_entry(0xD1, synthetic_chain_epoch())?,
        ironwood_only_entry(0xD2, synthetic_chain_epoch())?,
    ]);
    let adapter =
        LightwalletdGrpcAdapter::new(wallet_query).with_mempool_surface(Arc::new(surface));

    let response = adapter
        .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest {
            exclude_txid_suffixes: Vec::new(),
            pool_types: vec![lightwalletd::PoolType::Sapling as i32],
        }))
        .await?
        .into_inner();
    let collected = collect_compact_txids(response).await?;
    assert_eq!(collected, vec![[0xD1; 32].to_vec()]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_tx_reads_all_snapshot_pages() -> eyre::Result<()> {
    let (wallet_query, _store_fixture) = single_block_serving_query()?;
    let surface = ScriptedMempoolSurface::with_entries(vec![
        synthetic_entry(0xA1, synthetic_chain_epoch())?,
        synthetic_entry(0xB2, synthetic_chain_epoch())?,
    ])
    .with_snapshot_page_size(1);
    let adapter =
        LightwalletdGrpcAdapter::new(wallet_query).with_mempool_surface(Arc::new(surface));

    let response = adapter
        .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest {
            exclude_txid_suffixes: Vec::new(),
            pool_types: Vec::new(),
        }))
        .await?
        .into_inner();
    let collected = collect_compact_txids(response).await?;
    assert_eq!(collected, vec![[0xA1; 32].to_vec(), [0xB2; 32].to_vec()]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_stream_projects_added_envelopes_to_raw_transactions()
-> eyre::Result<()> {
    let (wallet_query, _store_fixture) = single_block_serving_query()?;
    let surface = ScriptedMempoolSurface::with_entries(Vec::new());
    let control = surface.event_control();
    let adapter =
        LightwalletdGrpcAdapter::new(wallet_query).with_mempool_surface(Arc::new(surface));

    let response = adapter
        .get_mempool_stream(Request::new(lightwalletd::Empty {}))
        .await?
        .into_inner();
    let mut response_stream = response;

    control.push_event(MempoolEvent::Added {
        entry: synthetic_entry(0x10, synthetic_chain_epoch())?,
    })?;
    control.push_event(MempoolEvent::Invalidated {
        transaction_id: TransactionId::from_bytes([0x20; 32]),
        reason: MempoolEvictionReason::Conflict,
    })?;
    control.push_event(MempoolEvent::Added {
        entry: synthetic_entry(0x30, synthetic_chain_epoch())?,
    })?;

    let first_raw = response_stream
        .next()
        .await
        .ok_or_else(|| eyre!("expected first raw transaction"))??;
    assert_eq!(first_raw.data, vec![0x10; 16]);

    let second_raw = response_stream
        .next()
        .await
        .ok_or_else(|| eyre!("expected second raw transaction"))??;
    // Invalidated was filtered; the second observation is the next Added.
    assert_eq!(second_raw.data, vec![0x30; 16]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_stream_starts_after_retained_tail() -> eyre::Result<()> {
    let (wallet_query, _store_fixture) = single_block_serving_query()?;
    let surface = ScriptedMempoolSurface::with_entries(Vec::new());
    let control = surface.event_control();
    control.append_retained_event(MempoolEvent::Added {
        entry: synthetic_entry(0x10, synthetic_chain_epoch())?,
    })?;
    let adapter =
        LightwalletdGrpcAdapter::new(wallet_query).with_mempool_surface(Arc::new(surface));

    let response = adapter
        .get_mempool_stream(Request::new(lightwalletd::Empty {}))
        .await?
        .into_inner();
    let mut response_stream = response;

    control.push_event(MempoolEvent::Added {
        entry: synthetic_entry(0x20, synthetic_chain_epoch())?,
    })?;

    let raw = tokio::time::timeout(std::time::Duration::from_secs(2), response_stream.next())
        .await?
        .ok_or_else(|| eyre!("expected live raw transaction after retained tail"))??;
    assert_eq!(raw.data, vec![0x20; 16]);
    Ok(())
}

/// `GetMempoolStream` streams the snapshot walk's contents first.
///
/// Live events follow strictly after the walk's resume anchor; the retained
/// events behind the anchor are not re-delivered.
#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_stream_streams_snapshot_contents_before_live_events()
-> eyre::Result<()> {
    let (wallet_query, _store_fixture) = single_block_serving_query()?;
    let surface = ScriptedMempoolSurface::with_entries(vec![
        synthetic_entry(0x10, synthetic_chain_epoch())?,
        synthetic_entry(0x20, synthetic_chain_epoch())?,
    ])
    .with_snapshot_page_size(1);
    let control = surface.event_control();
    control.append_retained_event(MempoolEvent::Added {
        entry: synthetic_entry(0x10, synthetic_chain_epoch())?,
    })?;
    control.append_retained_event(MempoolEvent::Added {
        entry: synthetic_entry(0x20, synthetic_chain_epoch())?,
    })?;
    let adapter =
        LightwalletdGrpcAdapter::new(wallet_query).with_mempool_surface(Arc::new(surface));

    let mut response_stream = adapter
        .get_mempool_stream(Request::new(lightwalletd::Empty {}))
        .await?
        .into_inner();

    let first_raw = response_stream
        .next()
        .await
        .ok_or_else(|| eyre!("expected first snapshot raw transaction"))??;
    assert_eq!(first_raw.data, vec![0x10; 16]);
    assert_eq!(first_raw.height, 0);
    let second_raw = response_stream
        .next()
        .await
        .ok_or_else(|| eyre!("expected second snapshot raw transaction"))??;
    assert_eq!(second_raw.data, vec![0x20; 16]);
    assert_eq!(second_raw.height, 0);

    control.push_event(MempoolEvent::Added {
        entry: synthetic_entry(0x30, synthetic_chain_epoch())?,
    })?;
    let live_raw = tokio::time::timeout(std::time::Duration::from_secs(2), response_stream.next())
        .await?
        .ok_or_else(|| eyre!("expected live raw transaction after the snapshot contents"))??;
    assert_eq!(live_raw.data, vec![0x30; 16]);
    assert_eq!(live_raw.height, 0);

    let no_more = tokio::time::timeout(
        std::time::Duration::from_millis(300),
        response_stream.next(),
    )
    .await;
    assert!(
        no_more.is_err(),
        "retained events behind the anchor must not replay; got {no_more:?}"
    );
    Ok(())
}

/// `GetTransaction` returns a pending transaction at height 0, like lightwalletd.
///
/// lightwalletd answers `GetTransaction` through Zebra's `getrawtransaction`,
/// which checks the mempool first (Zebra 7.0.0,
/// `zebra-rpc/src/methods.rs:2114-2144`) and omits `height` for a mempool
/// transaction, so lightwalletd returns it at height 0 (lightwalletd
/// `e7436d2`, `frontend/service.go:474-485` and `common/common.go:765-779`).
/// The Swift SDK treats height 0 as unmined and `NOT_FOUND` as an unknown txid
/// (zcash-swift-wallet-sdk `ebd0add`,
/// `Sources/ZcashLightClientKit/Modules/Service/GRPC/LightWalletGRPCService.swift:282-297`).
#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_transaction_returns_a_mempool_transaction_at_height_zero()
-> eyre::Result<()> {
    let (wallet_query, _store_fixture) = single_block_serving_query()?;
    let canonical_epoch_id = wallet_query.visible_tip_block(None).await?.chain_epoch.id;
    let pending_entry = synthetic_entry(0x5a, synthetic_chain_epoch())?;
    let adapter = LightwalletdGrpcAdapter::new(wallet_query).with_mempool_surface(Arc::new(
        ScriptedMempoolSurface::with_entries(vec![pending_entry.clone()])
            .with_absence_chain_epoch_id(canonical_epoch_id),
    ));

    let pending = adapter
        .get_transaction(Request::new(transaction_filter(
            pending_entry.transaction_id(),
        )))
        .await?
        .into_inner();
    assert_eq!(pending.height, 0);
    assert_eq!(
        pending.data,
        pending_entry.raw_transaction_bytes().as_slice()
    );

    let unknown = adapter
        .get_transaction(Request::new(transaction_filter(TransactionId::from_bytes(
            [0x5b; 32],
        ))))
        .await
        .err()
        .ok_or_else(|| eyre!("a transaction outside the chain and the mempool must miss"))?;
    assert_eq!(unknown.code(), Code::NotFound);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_transaction_reports_not_found_without_a_mempool_surface()
-> eyre::Result<()> {
    let (wallet_query, _store_fixture) = single_block_serving_query()?;
    let adapter = LightwalletdGrpcAdapter::new(wallet_query);

    let outcome = adapter
        .get_transaction(Request::new(transaction_filter(TransactionId::from_bytes(
            [0x5a; 32],
        ))))
        .await
        .err()
        .ok_or_else(|| eyre!("without a mempool surface an unmined transaction must miss"))?;
    assert_eq!(outcome.code(), Code::NotFound);
    Ok(())
}

/// The production surface reads the writer's `IngestControl.MempoolTransaction`.
/// A failed mempool read is not an absence, so it is not reported as
/// `NOT_FOUND`.
#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_transaction_reads_pending_transactions_through_ingest_control()
-> eyre::Result<()> {
    let (wallet_query, _store_fixture) = single_block_serving_query()?;
    let canonical_epoch = wallet_query.visible_tip_block(None).await?.chain_epoch;
    let ingest_control = IngestControlFixture::spawn(Network::ZcashRegtest).await?;
    let pending_entry = synthetic_entry(0x5c, synthetic_chain_epoch())?;
    ingest_control.set_mempool_entries(std::slice::from_ref(&pending_entry));
    let adapter = LightwalletdGrpcAdapter::new(wallet_query).with_mempool_surface(Arc::new(
        IngestControlMempoolSurface::new(ingest_control.endpoint().to_owned()),
    ));

    let pending = adapter
        .get_transaction(Request::new(transaction_filter(
            pending_entry.transaction_id(),
        )))
        .await?
        .into_inner();
    assert_eq!(pending.height, 0);
    assert_eq!(
        pending.data,
        pending_entry.raw_transaction_bytes().as_slice()
    );

    let unknown_transaction_id = TransactionId::from_bytes([0x5d; 32]);
    // A bare NOT_FOUND names no writer epoch, so it cannot prove the absence.
    let unproven = adapter
        .get_transaction(Request::new(transaction_filter(unknown_transaction_id)))
        .await
        .err()
        .ok_or_else(|| eyre!("a mempool miss without a typed absence must fail the lookup"))?;
    assert_eq!(unproven.code(), Code::Unavailable);

    ingest_control.set_mempool_transaction_error(Some(tonic::Status::unavailable(
        "writer mempool is restarting",
    )));
    let failed = adapter
        .get_transaction(Request::new(transaction_filter(unknown_transaction_id)))
        .await
        .err()
        .ok_or_else(|| eyre!("a failed mempool read must fail the lookup"))?;
    assert_eq!(failed.code(), Code::Unavailable);

    // A typed absence at the canonical read's epoch proves the miss.
    ingest_control.set_mempool_transaction_error(Some(
        TransactionAbsence::at_epoch(canonical_epoch, true).into_status("not in the mempool"),
    ));
    let unknown = adapter
        .get_transaction(Request::new(transaction_filter(unknown_transaction_id)))
        .await
        .err()
        .ok_or_else(|| eyre!("a transaction outside the chain and the mempool must miss"))?;
    assert_eq!(unknown.code(), Code::NotFound);
    ingest_control.shutdown().await
}

/// A mempool miss at another chain epoch is retryable, not an absence.
///
/// That writer describes a different chain than the canonical read. The
/// transaction may have been mined into a block this replica has not applied,
/// so the miss is retryable `UNAVAILABLE`, not the `NOT_FOUND` that wallet
/// SDKs read as an unknown transaction id.
#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_transaction_retries_a_mempool_miss_at_another_epoch() -> eyre::Result<()>
{
    let (wallet_query, _store_fixture) = single_block_serving_query()?;
    let canonical_epoch = wallet_query.visible_tip_block(None).await?.chain_epoch;
    let ingest_control = IngestControlFixture::spawn(Network::ZcashRegtest).await?;
    let adapter = LightwalletdGrpcAdapter::new(wallet_query).with_mempool_surface(Arc::new(
        IngestControlMempoolSurface::new(ingest_control.endpoint().to_owned()),
    ));

    for writer_epoch_id in [
        canonical_epoch.id.value() + 1,
        canonical_epoch.id.value().saturating_sub(1),
    ] {
        let writer_epoch = ChainEpoch {
            id: ChainEpochId::new(writer_epoch_id),
            ..canonical_epoch
        };
        ingest_control.set_mempool_transaction_error(Some(
            TransactionAbsence::at_epoch(writer_epoch, true).into_status("not in the mempool"),
        ));
        let mismatched = adapter
            .get_transaction(Request::new(transaction_filter(TransactionId::from_bytes(
                [0x5e; 32],
            ))))
            .await
            .err()
            .ok_or_else(|| eyre!("a mempool miss at another epoch must fail the lookup"))?;
        assert_eq!(mismatched.code(), Code::Unavailable, "{mismatched:?}");
    }
    ingest_control.shutdown().await
}

fn transaction_filter(transaction_id: TransactionId) -> lightwalletd::TxFilter {
    lightwalletd::TxFilter {
        block: None,
        index: 0,
        hash: encode_internal_transaction_id(transaction_id).to_vec(),
    }
}

fn synthetic_chain_epoch() -> ChainEpoch {
    ChainEpoch {
        id: ChainEpochId::new(7),
        network: Network::ZcashRegtest,
        visible_tip_height: BlockHeight::new(123),
        visible_tip_hash: BlockHash::from_bytes([0x42; 32]),
        settled_tip_height: BlockHeight::new(123),
        settled_tip_hash: BlockHash::from_bytes([0x42; 32]),
        artifact_schema_version: CURRENT_ARTIFACT_SCHEMA_VERSION,
        tip_metadata: ChainTipMetadata::empty(),
        created_at: UnixTimestampMillis::new(1_700_000_000_000),
    }
}

fn synthetic_entry(transaction_id_byte: u8, chain_epoch: ChainEpoch) -> eyre::Result<MempoolEntry> {
    synthetic_entry_with_compact_tx(
        [transaction_id_byte; 32],
        chain_epoch,
        &lightwalletd::CompactTx {
            index: 0,
            txid: transaction_id_byte_to_txid_vec(transaction_id_byte),
            fee: 0,
            spends: Vec::new(),
            outputs: vec![lightwalletd::CompactSaplingOutput {
                cmu: vec![transaction_id_byte; 32],
                ephemeral_key: vec![transaction_id_byte; 32],
                ciphertext: vec![transaction_id_byte; 52],
            }],
            actions: Vec::new(),
            ironwood_actions: Vec::new(),
            vin: Vec::new(),
            vout: Vec::new(),
        },
    )
}

fn synthetic_entry_with_transaction_id(
    transaction_id_bytes: [u8; 32],
    chain_epoch: ChainEpoch,
) -> eyre::Result<MempoolEntry> {
    let payload_byte = transaction_id_bytes[0];
    synthetic_entry_with_compact_tx(
        transaction_id_bytes,
        chain_epoch,
        &lightwalletd::CompactTx {
            index: 0,
            txid: transaction_id_bytes.to_vec(),
            fee: 0,
            spends: Vec::new(),
            outputs: vec![lightwalletd::CompactSaplingOutput {
                cmu: vec![payload_byte; 32],
                ephemeral_key: vec![payload_byte; 32],
                ciphertext: vec![payload_byte; 52],
            }],
            actions: Vec::new(),
            ironwood_actions: Vec::new(),
            vin: Vec::new(),
            vout: Vec::new(),
        },
    )
}

fn ironwood_only_entry(
    transaction_id_byte: u8,
    chain_epoch: ChainEpoch,
) -> eyre::Result<MempoolEntry> {
    ironwood_only_entry_with_transaction_id([transaction_id_byte; 32], chain_epoch)
}

fn ironwood_only_entry_with_transaction_id(
    transaction_id_bytes: [u8; 32],
    chain_epoch: ChainEpoch,
) -> eyre::Result<MempoolEntry> {
    let payload_byte = transaction_id_bytes[0];
    synthetic_entry_with_compact_tx(
        transaction_id_bytes,
        chain_epoch,
        &lightwalletd::CompactTx {
            index: 0,
            txid: transaction_id_bytes.to_vec(),
            fee: 0,
            spends: Vec::new(),
            outputs: Vec::new(),
            actions: Vec::new(),
            ironwood_actions: vec![lightwalletd::CompactOrchardAction {
                nullifier: vec![payload_byte; 32],
                cmx: vec![payload_byte; 32],
                ephemeral_key: vec![payload_byte; 32],
                ciphertext: vec![payload_byte; 52],
            }],
            vin: Vec::new(),
            vout: Vec::new(),
        },
    )
}

fn transparent_only_entry(
    transaction_id_byte: u8,
    chain_epoch: ChainEpoch,
) -> eyre::Result<MempoolEntry> {
    synthetic_entry_with_compact_tx(
        [transaction_id_byte; 32],
        chain_epoch,
        &lightwalletd::CompactTx {
            index: 0,
            txid: transaction_id_byte_to_txid_vec(transaction_id_byte),
            fee: 0,
            spends: Vec::new(),
            outputs: Vec::new(),
            actions: Vec::new(),
            ironwood_actions: Vec::new(),
            vin: vec![lightwalletd::CompactTxIn {
                prevout_txid: vec![0x11; 32],
                prevout_index: 0,
            }],
            vout: vec![lightwalletd::TxOut {
                value: 100,
                script_pub_key: vec![0xAB; 25],
            }],
        },
    )
}

fn synthetic_entry_with_compact_tx(
    transaction_id_bytes: [u8; 32],
    chain_epoch: ChainEpoch,
    compact_tx: &lightwalletd::CompactTx,
) -> eyre::Result<MempoolEntry> {
    let transaction_id = TransactionId::from_bytes(transaction_id_bytes);
    let payload_byte = transaction_id_bytes[0];
    let compact_transaction_data = CompactTransactionData {
        fee_zat: Some(u64::from(compact_tx.fee)),
        sapling_spends: compact_tx
            .spends
            .iter()
            .map(|spend| {
                Ok(CompactSaplingSpend {
                    nullifier: fixed_bytes(&spend.nf)?,
                })
            })
            .collect::<eyre::Result<Vec<_>>>()?,
        sapling_outputs: compact_tx
            .outputs
            .iter()
            .map(|output| -> eyre::Result<_> {
                Ok(CompactSaplingOutput {
                    commitment: fixed_bytes(&output.cmu)?,
                    ephemeral_key: fixed_bytes(&output.ephemeral_key)?,
                    ciphertext: fixed_bytes(&output.ciphertext)?,
                })
            })
            .collect::<eyre::Result<Vec<_>>>()?,
        orchard_actions: compact_tx
            .actions
            .iter()
            .map(compact_action_data)
            .collect::<eyre::Result<Vec<_>>>()?,
        ironwood_actions: compact_tx
            .ironwood_actions
            .iter()
            .map(compact_action_data)
            .collect::<eyre::Result<Vec<_>>>()?,
        transparent_inputs: compact_tx
            .vin
            .iter()
            .map(|input| -> eyre::Result<_> {
                Ok(CompactTransparentInput {
                    previous_transaction_id: TransactionId::from_bytes(fixed_bytes(
                        &input.prevout_txid,
                    )?),
                    previous_output_index: input.prevout_index,
                })
            })
            .collect::<eyre::Result<Vec<_>>>()?,
        transparent_outputs: compact_tx
            .vout
            .iter()
            .map(|output| CompactTransparentOutput {
                value_zat: output.value,
                script_pub_key: output.script_pub_key.clone(),
            })
            .collect(),
    };
    MempoolEntry::new(
        transaction_id,
        Some(AuthDigest::from_bytes(transaction_id_bytes)),
        RawTransactionBytes::new(vec![payload_byte; 16]),
        compact_transaction_data,
        MempoolObservation {
            first_seen_unix_millis: UnixTimestampMillis::new(1_700_000_000_000),
            first_seen_chain_epoch: chain_epoch,
        },
    )
    .map_err(|error| eyre!("invalid synthetic mempool entry: {error}"))
}

fn compact_action_data(
    action: &lightwalletd::CompactOrchardAction,
) -> eyre::Result<CompactShieldedAction> {
    Ok(CompactShieldedAction {
        nullifier: fixed_bytes(&action.nullifier)?,
        commitment: fixed_bytes(&action.cmx)?,
        ephemeral_key: fixed_bytes(&action.ephemeral_key)?,
        ciphertext: fixed_bytes(&action.ciphertext)?,
    })
}

fn fixed_bytes<const LENGTH: usize>(bytes: &[u8]) -> eyre::Result<[u8; LENGTH]> {
    <[u8; LENGTH]>::try_from(bytes)
        .map_err(|_| eyre!("expected {LENGTH} bytes, got {}", bytes.len()))
}

fn transaction_id_byte_to_txid_vec(byte: u8) -> Vec<u8> {
    vec![byte; 32]
}

async fn collect_compact_txids<S>(mut stream: S) -> eyre::Result<Vec<Vec<u8>>>
where
    S: tokio_stream::Stream<Item = Result<lightwalletd::CompactTx, tonic::Status>> + Unpin,
{
    let mut transaction_ids = Vec::new();
    while let Some(next) = stream.next().await {
        let compact_tx = next?;
        transaction_ids.push(compact_tx.txid);
    }
    Ok(transaction_ids)
}

type SharedEventSenderSlot =
    Arc<Mutex<Option<mpsc::UnboundedSender<Result<MempoolEventEnvelope, MempoolSurfaceError>>>>>;

struct ScriptedMempoolSurface {
    entries: Mutex<Vec<MempoolEntry>>,
    absence_chain_epoch_id: Option<ChainEpochId>,
    snapshot_page_size: Option<usize>,
    retained_events: Arc<Mutex<Vec<MempoolEventEnvelope>>>,
    pending_event_sender: SharedEventSenderSlot,
    event_sequence: Arc<Mutex<u64>>,
    polled_events: Arc<AtomicUsize>,
    polled_notify: Arc<Notify>,
}

impl ScriptedMempoolSurface {
    fn with_entries(entries: Vec<MempoolEntry>) -> Self {
        Self {
            entries: Mutex::new(entries),
            absence_chain_epoch_id: None,
            snapshot_page_size: None,
            retained_events: Arc::new(Mutex::new(Vec::new())),
            pending_event_sender: Arc::new(Mutex::new(None)),
            event_sequence: Arc::new(Mutex::new(0u64)),
            polled_events: Arc::new(AtomicUsize::new(0)),
            polled_notify: Arc::new(Notify::new()),
        }
    }

    fn with_absence_chain_epoch_id(mut self, chain_epoch_id: ChainEpochId) -> Self {
        self.absence_chain_epoch_id = Some(chain_epoch_id);
        self
    }

    fn with_snapshot_page_size(mut self, snapshot_page_size: usize) -> Self {
        self.snapshot_page_size = Some(snapshot_page_size);
        self
    }

    fn event_control(&self) -> ScriptedMempoolEventControl {
        ScriptedMempoolEventControl {
            retained_events: Arc::clone(&self.retained_events),
            sender_slot: Arc::clone(&self.pending_event_sender),
            event_sequence: Arc::clone(&self.event_sequence),
            polled_events: Arc::clone(&self.polled_events),
            polled_notify: Arc::clone(&self.polled_notify),
        }
    }
}

struct ScriptedMempoolEventControl {
    retained_events: Arc<Mutex<Vec<MempoolEventEnvelope>>>,
    sender_slot: SharedEventSenderSlot,
    event_sequence: Arc<Mutex<u64>>,
    polled_events: Arc<AtomicUsize>,
    polled_notify: Arc<Notify>,
}

impl ScriptedMempoolEventControl {
    fn append_retained_event(&self, event: MempoolEvent) -> eyre::Result<()> {
        let envelope = self.next_envelope(event)?;
        self.retained_events.lock().push(envelope);
        Ok(())
    }

    fn push_event(&self, event: MempoolEvent) -> eyre::Result<()> {
        let envelope = self.next_envelope(event)?;
        self.retained_events.lock().push(envelope.clone());
        let active_sender = self
            .sender_slot
            .lock()
            .as_ref()
            .cloned()
            .ok_or_else(|| eyre!("scripted mempool surface has no open event stream"))?;
        active_sender
            .send(Ok(envelope))
            .map_err(|_| eyre!("scripted mempool surface receiver dropped"))?;
        Ok(())
    }

    fn has_open_subscription(&self) -> bool {
        self.sender_slot
            .lock()
            .as_ref()
            .is_some_and(|sender| !sender.is_closed())
    }

    async fn wait_for_polled_events(
        &self,
        expected_count: usize,
        timeout_duration: Duration,
    ) -> eyre::Result<()> {
        tokio::time::timeout(timeout_duration, async {
            loop {
                let notified = self.polled_notify.notified();
                if self.polled_events.load(Ordering::SeqCst) >= expected_count {
                    return;
                }
                notified.await;
            }
        })
        .await
        .map_err(|_| {
            eyre!(
                "timed out waiting for {expected_count} polled mempool events (observed {})",
                self.polled_events.load(Ordering::SeqCst)
            )
        })
    }

    async fn wait_for_subscription_release(&self, timeout_duration: Duration) -> eyre::Result<()> {
        let active_sender = self
            .sender_slot
            .lock()
            .as_ref()
            .cloned()
            .ok_or_else(|| eyre!("scripted mempool surface has no open event stream"))?;
        tokio::time::timeout(timeout_duration, active_sender.closed())
            .await
            .map_err(|_| eyre!("upstream mempool subscription stayed open past timeout"))
    }

    fn next_envelope(&self, event: MempoolEvent) -> eyre::Result<MempoolEventEnvelope> {
        let mut sequence_guard = self.event_sequence.lock();
        *sequence_guard = sequence_guard.saturating_add(1);
        let event_sequence = *sequence_guard;
        drop(sequence_guard);

        Ok(MempoolEventEnvelope {
            cursor: StreamCursorTokenV1::mempool_event(
                Network::ZcashRegtest,
                event_sequence,
                event.transaction_id(),
                [9; 32],
            )?,
            event_sequence,
            source_observed_unix_millis: 1_700_000_000_000,
            event,
        })
    }
}

/// Lightwalletd contract: `GetMempoolStream` closes cleanly when the writer
/// observes a best-chain tip change.
///
/// Native `MempoolEvents` must NOT close on tip change; this is a compat-only
/// behavior preserved for the Go lightwalletd contract Zallet relies on.
#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_stream_closes_on_tip_change() -> eyre::Result<()> {
    let (wallet_query, _store_fixture) = single_block_serving_query()?;
    let surface = ScriptedMempoolSurface::with_entries(Vec::new());
    let event_control = surface.event_control();
    let tip_change_watcher = ScriptedTipChangeWatcher::new();
    let tip_change_signal = tip_change_watcher.signal();
    let adapter = LightwalletdGrpcAdapter::new(wallet_query)
        .with_mempool_surface(Arc::new(surface))
        .with_tip_change_watcher(Arc::new(tip_change_watcher));

    let response = adapter
        .get_mempool_stream(Request::new(lightwalletd::Empty {}))
        .await?
        .into_inner();
    let mut response_stream = response;

    event_control.push_event(MempoolEvent::Added {
        entry: synthetic_entry(0xAA, synthetic_chain_epoch())?,
    })?;
    let raw = response_stream
        .next()
        .await
        .ok_or_else(|| eyre!("expected first raw transaction"))??;
    assert_eq!(raw.data, vec![0xAA; 16]);

    // Signal a tip change; the stream should end cleanly.
    tip_change_signal.observe_tip_change();
    let next = tokio::time::timeout(std::time::Duration::from_secs(2), response_stream.next())
        .await
        .map_err(|_| eyre!("stream did not end on tip change before timeout"))?;
    assert!(
        next.is_none(),
        "expected stream end after tip change, got: {next:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_stream_closes_when_tip_change_precedes_watcher_poll()
-> eyre::Result<()> {
    let (wallet_query, _store_fixture) = single_block_serving_query()?;
    let surface = ScriptedMempoolSurface::with_entries(Vec::new());
    let tip_change_watcher = ScriptedTipChangeWatcher::new();
    let tip_change_signal = tip_change_watcher.signal();
    let adapter = LightwalletdGrpcAdapter::new(wallet_query)
        .with_mempool_surface(Arc::new(surface))
        .with_tip_change_watcher(Arc::new(tip_change_watcher));

    // The snapshot is fenced at chain epoch 7. Retaining chain-event sequence
    // 8 before the stream task polls the watcher models the original startup
    // race.
    tip_change_signal.observe_tip_change();
    let mut response_stream = adapter
        .get_mempool_stream(Request::new(lightwalletd::Empty {}))
        .await?
        .into_inner();

    let next = tokio::time::timeout(std::time::Duration::from_secs(2), response_stream.next())
        .await
        .map_err(|_| eyre!("stream did not honor the retained tip change before timeout"))?;
    assert!(
        next.is_none(),
        "expected stream end after retained tip change, got: {next:?}"
    );
    Ok(())
}

/// A closed tip-change signal is not a tip change. Ending with OK would tell
/// the client a block arrived, so the stream fails with `UNAVAILABLE`.
#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_stream_fails_unavailable_when_tip_signal_closes()
-> eyre::Result<()> {
    let (wallet_query, _store_fixture) = single_block_serving_query()?;
    let surface = ScriptedMempoolSurface::with_entries(Vec::new());
    let adapter = LightwalletdGrpcAdapter::new(wallet_query)
        .with_mempool_surface(Arc::new(surface))
        .with_tip_change_watcher(Arc::new(ClosedTipChangeWatcher::new()));

    let mut response_stream = adapter
        .get_mempool_stream(Request::new(lightwalletd::Empty {}))
        .await?
        .into_inner();

    let closing = tokio::time::timeout(std::time::Duration::from_secs(2), response_stream.next())
        .await
        .map_err(|_| eyre!("stream did not react to the closed tip signal before timeout"))?;
    match closing {
        Some(Err(status)) => assert_eq!(status.code(), Code::Unavailable),
        other => return Err(eyre!("expected UNAVAILABLE, got: {other:?}")),
    }
    let after = tokio::time::timeout(std::time::Duration::from_secs(2), response_stream.next())
        .await
        .map_err(|_| eyre!("stream did not end after the UNAVAILABLE status"))?;
    assert!(after.is_none(), "expected stream end, got: {after:?}");
    Ok(())
}

/// lightwalletd sends each txid once per block interval. A transaction in a
/// snapshot page that is also delivered as a live `Added` event, or added
/// again after an eviction, is not sent twice.
#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_stream_sends_each_transaction_once() -> eyre::Result<()> {
    let (wallet_query, _store_fixture) = single_block_serving_query()?;
    let surface = ScriptedMempoolSurface::with_entries(vec![
        synthetic_entry(0x10, synthetic_chain_epoch())?,
        synthetic_entry(0x20, synthetic_chain_epoch())?,
    ]);
    let control = surface.event_control();
    let adapter =
        LightwalletdGrpcAdapter::new(wallet_query).with_mempool_surface(Arc::new(surface));

    let mut response_stream = adapter
        .get_mempool_stream(Request::new(lightwalletd::Empty {}))
        .await?
        .into_inner();
    for expected_byte in [0x10, 0x20] {
        let snapshot_raw = response_stream
            .next()
            .await
            .ok_or_else(|| eyre!("expected snapshot raw transaction {expected_byte:#04x}"))??;
        assert_eq!(snapshot_raw.data, vec![expected_byte; 16]);
    }

    for live_byte in [0x10, 0x20, 0x30, 0x30] {
        control.push_event(MempoolEvent::Added {
            entry: synthetic_entry(live_byte, synthetic_chain_epoch())?,
        })?;
    }
    let live_raw = tokio::time::timeout(std::time::Duration::from_secs(2), response_stream.next())
        .await?
        .ok_or_else(|| eyre!("expected the first new live raw transaction"))??;
    assert_eq!(live_raw.data, vec![0x30; 16]);

    let no_more = tokio::time::timeout(
        std::time::Duration::from_millis(300),
        response_stream.next(),
    )
    .await;
    assert!(
        no_more.is_err(),
        "already-sent transactions must not repeat; got {no_more:?}"
    );
    Ok(())
}

async fn assert_tip_change_releases_full_queue(extra_upstream_events: u8) -> eyre::Result<()> {
    const QUEUE_CAPACITY: u8 = 16;
    let (wallet_query, _store_fixture) = single_block_serving_query()?;
    let surface = ScriptedMempoolSurface::with_entries(Vec::new());
    let event_control = surface.event_control();
    let tip_change_watcher = ScriptedTipChangeWatcher::new();
    let tip_change_signal = tip_change_watcher.signal();
    let adapter = LightwalletdGrpcAdapter::new(wallet_query)
        .with_mempool_surface(Arc::new(surface))
        .with_tip_change_watcher(Arc::new(tip_change_watcher));

    let mut response_stream = adapter
        .get_mempool_stream(Request::new(lightwalletd::Empty {}))
        .await?
        .into_inner();

    let total_pushed = QUEUE_CAPACITY.saturating_add(extra_upstream_events);
    for tx_byte in 1..=total_pushed {
        event_control.push_event(MempoolEvent::Added {
            entry: synthetic_entry(tx_byte, synthetic_chain_epoch())?,
        })?;
    }
    event_control
        .wait_for_polled_events(usize::from(QUEUE_CAPACITY), Duration::from_secs(2))
        .await?;
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert!(
        event_control.has_open_subscription(),
        "upstream subscription must remain open before tip change"
    );

    // Downstream receiver remains paused with all 16 queue slots full.
    tip_change_signal.observe_tip_change();
    event_control
        .wait_for_subscription_release(Duration::from_millis(500))
        .await?;

    // Frames already buffered before the tip change drain cleanly, while no
    // 17th or post-tip-change transaction is produced.
    for expected_byte in 1..=QUEUE_CAPACITY {
        let buffered = tokio::time::timeout(Duration::from_secs(2), response_stream.next())
            .await?
            .ok_or_else(|| eyre!("expected buffered transaction {expected_byte}"))??;
        assert_eq!(buffered.data, vec![expected_byte; 16]);
    }
    let after_buffered =
        tokio::time::timeout(Duration::from_secs(2), response_stream.next()).await?;
    assert!(
        after_buffered.is_none(),
        "expected stream end after buffered frames, got: {after_buffered:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_stream_releases_subscription_on_tip_change_when_send_backpressured()
-> eyre::Result<()> {
    assert_tip_change_releases_full_queue(1).await
}

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_stream_releases_subscription_on_tip_change_with_full_idle_queue()
-> eyre::Result<()> {
    assert_tip_change_releases_full_queue(0).await
}

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_stream_releases_subscription_on_idle_receiver_drop()
-> eyre::Result<()> {
    let (wallet_query, _store_fixture) = single_block_serving_query()?;
    let surface = ScriptedMempoolSurface::with_entries(Vec::new());
    let event_control = surface.event_control();
    let tip_change_watcher = ScriptedTipChangeWatcher::new();
    let adapter = LightwalletdGrpcAdapter::new(wallet_query)
        .with_mempool_surface(Arc::new(surface))
        .with_tip_change_watcher(Arc::new(tip_change_watcher));

    let response_stream = adapter
        .get_mempool_stream(Request::new(lightwalletd::Empty {}))
        .await?
        .into_inner();
    assert!(event_control.has_open_subscription());

    drop(response_stream);

    event_control
        .wait_for_subscription_release(Duration::from_millis(500))
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_stream_releases_subscription_on_idle_grpc_client_disconnect()
-> eyre::Result<()> {
    let (wallet_query, _store_fixture) = single_block_serving_query()?;
    let surface = ScriptedMempoolSurface::with_entries(Vec::new());
    let event_control = surface.event_control();
    let tip_change_watcher = ScriptedTipChangeWatcher::new();
    let adapter = LightwalletdGrpcAdapter::new(wallet_query)
        .with_mempool_surface(Arc::new(surface))
        .with_tip_change_watcher(Arc::new(tip_change_watcher));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(
        zinder_runtime::public_grpc_server(zinder_runtime::ClientAddressSource::Connection)
            .add_service(adapter.into_server())
            .serve_with_incoming(listener),
    );

    let mut client =
        lightwalletd::compact_tx_streamer_client::CompactTxStreamerClient::connect(endpoint)
            .await?;
    let response_stream = client
        .get_mempool_stream(lightwalletd::Empty {})
        .await?
        .into_inner();
    assert!(event_control.has_open_subscription());

    drop(response_stream);

    let release = event_control
        .wait_for_subscription_release(Duration::from_millis(500))
        .await;
    server.abort();
    release
}

/// A tip-change watcher whose signal source is already gone.
struct ClosedTipChangeWatcher {
    receiver: tokio::sync::watch::Receiver<u64>,
}

impl ClosedTipChangeWatcher {
    fn new() -> Self {
        let (_sender, receiver) = tokio::sync::watch::channel(7);
        Self { receiver }
    }
}

#[async_trait]
impl TipChangeWatcher for ClosedTipChangeWatcher {
    async fn await_tip_change_after(
        &self,
        chain_epoch_id: ChainEpochId,
    ) -> Result<(), TipChangeWatcherError> {
        let mut receiver = self.receiver.clone();
        if *receiver.borrow_and_update() > chain_epoch_id.value() {
            return Ok(());
        }
        receiver
            .changed()
            .await
            .map_err(|_| TipChangeWatcherError::SignalClosed)
    }
}

struct ScriptedTipChangeWatcher {
    sender: tokio::sync::watch::Sender<u64>,
    receiver: tokio::sync::watch::Receiver<u64>,
}

impl ScriptedTipChangeWatcher {
    fn new() -> Self {
        let (sender, receiver) = tokio::sync::watch::channel(7);
        Self { sender, receiver }
    }

    fn signal(&self) -> ScriptedTipChangeSignal {
        ScriptedTipChangeSignal {
            sender: self.sender.clone(),
        }
    }
}

#[derive(Clone)]
struct ScriptedTipChangeSignal {
    sender: tokio::sync::watch::Sender<u64>,
}

impl ScriptedTipChangeSignal {
    fn observe_tip_change(&self) {
        self.sender.send_modify(|sequence| {
            *sequence = sequence.saturating_add(1);
        });
    }
}

#[async_trait]
impl TipChangeWatcher for ScriptedTipChangeWatcher {
    async fn await_tip_change_after(
        &self,
        chain_epoch_id: ChainEpochId,
    ) -> Result<(), TipChangeWatcherError> {
        let mut receiver = self.receiver.clone();
        loop {
            if *receiver.borrow_and_update() > chain_epoch_id.value() {
                return Ok(());
            }
            receiver
                .changed()
                .await
                .map_err(|_| TipChangeWatcherError::SignalClosed)?;
        }
    }
}

#[async_trait]
impl MempoolSurface for ScriptedMempoolSurface {
    async fn mempool_snapshot_page(
        &self,
        max_entries: u32,
        from_cursor: Option<Vec<u8>>,
    ) -> Result<MempoolSnapshotPage, MempoolSurfaceError> {
        let entries = self.entries.lock().clone();
        let start_index = decode_snapshot_page_index(from_cursor.as_deref())?;
        let requested_page_size = usize::try_from(max_entries).unwrap_or(usize::MAX);
        let page_size = self
            .snapshot_page_size
            .map_or(requested_page_size, |limit| limit.min(requested_page_size));
        let end_index = start_index.saturating_add(page_size).min(entries.len());
        let page_entries = entries[start_index..end_index].to_vec();
        let next_cursor = if end_index < entries.len() {
            Some(
                u64::try_from(end_index)
                    .unwrap_or(u64::MAX)
                    .to_be_bytes()
                    .to_vec(),
            )
        } else {
            None
        };
        let events_resume_cursor = self
            .retained_events
            .lock()
            .last()
            .map(|envelope| envelope.cursor.clone());
        Ok(MempoolSnapshotPage {
            chain_epoch_id: ChainEpochId::new(7),
            events_resume_cursor,
            entries: page_entries,
            next_cursor,
        })
    }

    async fn mempool_events(
        &self,
        from_cursor: Option<StreamCursorTokenV1>,
    ) -> Result<MempoolEventEnvelopeStream, MempoolSurfaceError> {
        let resume_after_sequence = match from_cursor {
            Some(cursor) => {
                cursor
                    .decode_mempool_event(Network::ZcashRegtest, [9; 32])
                    .map_err(|_| MempoolSurfaceError::CursorInvalid)?
                    .event_sequence
            }
            None => 0,
        };
        let (event_sender, event_receiver) = mpsc::unbounded_channel();
        for envelope in self
            .retained_events
            .lock()
            .iter()
            .filter(|envelope| envelope.event_sequence > resume_after_sequence)
            .cloned()
        {
            if event_sender.send(Ok(envelope)).is_err() {
                return Err(MempoolSurfaceError::Unavailable {
                    reason: "scripted retained mempool event receiver dropped".to_owned(),
                });
            }
        }
        *self.pending_event_sender.lock() = Some(event_sender);
        let polled_events = Arc::clone(&self.polled_events);
        let polled_notify = Arc::clone(&self.polled_notify);
        let counted_stream =
            UnboundedReceiverStream::new(event_receiver).map(move |envelope_outcome| {
                polled_events.fetch_add(1, Ordering::SeqCst);
                polled_notify.notify_waiters();
                envelope_outcome
            });
        let stream: Pin<
            Box<
                dyn Stream<Item = Result<MempoolEventEnvelope, MempoolSurfaceError>>
                    + Send
                    + 'static,
            >,
        > = Box::pin(counted_stream);
        Ok(stream)
    }

    async fn mempool_transaction(
        &self,
        transaction_id: TransactionId,
    ) -> Result<MempoolTransactionLookup, MempoolSurfaceError> {
        if let Some(entry) = self
            .entries
            .lock()
            .iter()
            .find(|entry| entry.transaction_id() == transaction_id)
        {
            return Ok(MempoolTransactionLookup::Pending(Box::new(entry.clone())));
        }
        self.absence_chain_epoch_id
            .map(|chain_epoch_id| MempoolTransactionLookup::Absent { chain_epoch_id })
            .ok_or_else(|| MempoolSurfaceError::Unavailable {
                reason: "scripted mempool surface has no absence epoch".to_owned(),
            })
    }
}

fn decode_snapshot_page_index(cursor: Option<&[u8]>) -> Result<usize, MempoolSurfaceError> {
    let Some(cursor_bytes) = cursor else {
        return Ok(0);
    };
    if cursor_bytes.len() != 8 {
        return Err(MempoolSurfaceError::CursorInvalid);
    }
    let mut index_bytes = [0u8; 8];
    index_bytes.copy_from_slice(cursor_bytes);
    let index = u64::from_be_bytes(index_bytes);
    usize::try_from(index).map_err(|_| MempoolSurfaceError::CursorInvalid)
}

#[derive(Clone)]
enum ScriptedSnapshotPageError {
    Unavailable(String),
    CursorExpired,
    CursorInvalid,
}

impl ScriptedSnapshotPageError {
    fn to_surface_error(&self) -> MempoolSurfaceError {
        match self {
            Self::Unavailable(reason) => MempoolSurfaceError::Unavailable {
                reason: reason.clone(),
            },
            Self::CursorExpired => MempoolSurfaceError::CursorExpired,
            Self::CursorInvalid => MempoolSurfaceError::CursorInvalid,
        }
    }

    const fn expected_code(&self) -> Code {
        match self {
            Self::Unavailable(_) => Code::Unavailable,
            Self::CursorExpired => Code::FailedPrecondition,
            Self::CursorInvalid => Code::InvalidArgument,
        }
    }
}

#[derive(Clone)]
enum ScriptedSnapshotPage {
    Entries(Vec<MempoolEntry>),
    Error(ScriptedSnapshotPageError),
}

struct ScriptedPageGate {
    page_index: usize,
    reached_flag: Arc<AtomicBool>,
    reached_notify: Arc<Notify>,
    release_rx: watch::Receiver<bool>,
    cancelled_tx: Mutex<Option<oneshot::Sender<()>>>,
}

struct ScriptedPageGateHandle {
    reached_flag: Arc<AtomicBool>,
    reached_notify: Arc<Notify>,
    release_tx: watch::Sender<bool>,
}

impl ScriptedPageGateHandle {
    async fn wait_until_reached(&self, timeout_duration: Duration) -> eyre::Result<()> {
        tokio::time::timeout(timeout_duration, async {
            loop {
                let notified = self.reached_notify.notified();
                if self.reached_flag.load(Ordering::SeqCst) {
                    return;
                }
                notified.await;
            }
        })
        .await
        .map_err(|_| eyre!("timed out waiting for gated snapshot page"))
    }

    fn release(&self) -> eyre::Result<()> {
        self.release_tx
            .send(true)
            .map_err(|_| eyre!("gated snapshot page receiver dropped"))
    }
}

struct InFlightPageDropGuard {
    cancelled_tx: Option<oneshot::Sender<()>>,
}

impl InFlightPageDropGuard {
    fn disarm(&mut self) {
        self.cancelled_tx = None;
    }
}

impl Drop for InFlightPageDropGuard {
    fn drop(&mut self) {
        if let Some(sender) = self.cancelled_tx.take() {
            let _ = sender.send(());
        }
    }
}

struct ScriptedPagedSnapshotSurface {
    pages: Vec<ScriptedSnapshotPage>,
    fetched_pages: Arc<AtomicUsize>,
    gate: Option<ScriptedPageGate>,
}

impl ScriptedPagedSnapshotSurface {
    fn new(pages: Vec<ScriptedSnapshotPage>) -> (Self, Arc<AtomicUsize>) {
        let fetched_pages = Arc::new(AtomicUsize::new(0));
        let surface = Self {
            pages,
            fetched_pages: Arc::clone(&fetched_pages),
            gate: None,
        };
        (surface, fetched_pages)
    }

    fn with_page_gate(
        mut self,
        page_index: usize,
    ) -> (Self, ScriptedPageGateHandle, oneshot::Receiver<()>) {
        let reached_flag = Arc::new(AtomicBool::new(false));
        let reached_notify = Arc::new(Notify::new());
        let (release_tx, release_rx) = watch::channel(false);
        let (cancelled_tx, cancelled_rx) = oneshot::channel();
        let handle = ScriptedPageGateHandle {
            reached_flag: Arc::clone(&reached_flag),
            reached_notify: Arc::clone(&reached_notify),
            release_tx,
        };
        self.gate = Some(ScriptedPageGate {
            page_index,
            reached_flag,
            reached_notify,
            release_rx,
            cancelled_tx: Mutex::new(Some(cancelled_tx)),
        });
        (self, handle, cancelled_rx)
    }
}

#[async_trait]
impl MempoolSurface for ScriptedPagedSnapshotSurface {
    async fn mempool_snapshot_page(
        &self,
        _max_entries: u32,
        from_cursor: Option<Vec<u8>>,
    ) -> Result<MempoolSnapshotPage, MempoolSurfaceError> {
        let page_index = decode_snapshot_page_index(from_cursor.as_deref())?;
        self.fetched_pages.fetch_add(1, Ordering::SeqCst);
        if let Some(gate) = &self.gate
            && gate.page_index == page_index
        {
            let mut guard = InFlightPageDropGuard {
                cancelled_tx: gate.cancelled_tx.lock().take(),
            };
            gate.reached_flag.store(true, Ordering::SeqCst);
            gate.reached_notify.notify_waiters();
            let mut release_rx = gate.release_rx.clone();
            while !*release_rx.borrow_and_update() {
                if release_rx.changed().await.is_err() {
                    break;
                }
            }
            guard.disarm();
        }

        let page = self
            .pages
            .get(page_index)
            .ok_or_else(|| MempoolSurfaceError::Unavailable {
                reason: format!("missing scripted snapshot page {page_index}"),
            })?;
        let entries = match page {
            ScriptedSnapshotPage::Entries(entries) => entries.clone(),
            ScriptedSnapshotPage::Error(err) => return Err(err.to_surface_error()),
        };
        let next_page_index = page_index.saturating_add(1);
        let next_cursor = if next_page_index < self.pages.len() {
            Some(
                u64::try_from(next_page_index)
                    .unwrap_or(u64::MAX)
                    .to_be_bytes()
                    .to_vec(),
            )
        } else {
            None
        };
        Ok(MempoolSnapshotPage {
            chain_epoch_id: ChainEpochId::new(7),
            events_resume_cursor: None,
            entries,
            next_cursor,
        })
    }

    async fn mempool_events(
        &self,
        _from_cursor: Option<StreamCursorTokenV1>,
    ) -> Result<MempoolEventEnvelopeStream, MempoolSurfaceError> {
        Err(MempoolSurfaceError::Unavailable {
            reason: "unused in paged snapshot test".to_owned(),
        })
    }

    async fn mempool_transaction(
        &self,
        _transaction_id: TransactionId,
    ) -> Result<MempoolTransactionLookup, MempoolSurfaceError> {
        Err(MempoolSurfaceError::Unavailable {
            reason: "unused in paged snapshot test".to_owned(),
        })
    }
}

fn orchard_only_entry_with_transaction_id(
    transaction_id_bytes: [u8; 32],
    chain_epoch: ChainEpoch,
) -> eyre::Result<MempoolEntry> {
    let payload_byte = transaction_id_bytes[0];
    synthetic_entry_with_compact_tx(
        transaction_id_bytes,
        chain_epoch,
        &lightwalletd::CompactTx {
            index: 0,
            txid: transaction_id_bytes.to_vec(),
            fee: 0,
            spends: Vec::new(),
            outputs: Vec::new(),
            actions: vec![lightwalletd::CompactOrchardAction {
                nullifier: vec![payload_byte; 32],
                cmx: vec![payload_byte; 32],
                ephemeral_key: vec![payload_byte; 32],
                ciphertext: vec![payload_byte; 52],
            }],
            ironwood_actions: Vec::new(),
            vin: Vec::new(),
            vout: Vec::new(),
        },
    )
}

fn oversized_fee_transparent_entry(
    transaction_id_byte: u8,
    chain_epoch: ChainEpoch,
) -> eyre::Result<MempoolEntry> {
    let compact_transaction_data = CompactTransactionData {
        fee_zat: Some(u64::from(u32::MAX) + 1),
        transparent_outputs: vec![CompactTransparentOutput {
            value_zat: 100,
            script_pub_key: vec![0xAB; 25],
        }],
        ..CompactTransactionData::default()
    };
    MempoolEntry::new(
        TransactionId::from_bytes([transaction_id_byte; 32]),
        Some(AuthDigest::from_bytes([transaction_id_byte; 32])),
        RawTransactionBytes::new(vec![transaction_id_byte; 16]),
        compact_transaction_data,
        MempoolObservation {
            first_seen_unix_millis: UnixTimestampMillis::new(1_700_000_000_000),
            first_seen_chain_epoch: chain_epoch,
        },
    )
    .map_err(|error| eyre!("invalid oversized-fee mempool entry: {error}"))
}

async fn collect_compact_txs<S>(mut stream: S) -> eyre::Result<Vec<lightwalletd::CompactTx>>
where
    S: tokio_stream::Stream<Item = Result<lightwalletd::CompactTx, tonic::Status>> + Unpin,
{
    let mut messages = Vec::new();
    while let Some(next_message) = stream.next().await {
        messages.push(next_message?);
    }
    Ok(messages)
}

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_tx_streams_page_one_before_page_two_completes() -> eyre::Result<()>
{
    let (wallet_query, _store_fixture) = single_block_serving_query()?;
    let epoch = synthetic_chain_epoch();
    let (surface, _fetched_pages) = ScriptedPagedSnapshotSurface::new(vec![
        ScriptedSnapshotPage::Entries(vec![synthetic_entry(0xA1, epoch)?]),
        ScriptedSnapshotPage::Entries(vec![synthetic_entry(0xB2, epoch)?]),
    ]);
    let (surface, page_two_gate, _cancelled_rx) = surface.with_page_gate(1);
    let adapter =
        LightwalletdGrpcAdapter::new(wallet_query).with_mempool_surface(Arc::new(surface));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(
        zinder_runtime::public_grpc_server(zinder_runtime::ClientAddressSource::Connection)
            .add_service(adapter.into_server())
            .serve_with_incoming(listener),
    );

    let mut client =
        lightwalletd::compact_tx_streamer_client::CompactTxStreamerClient::connect(endpoint)
            .await?;
    let mut response_stream = tokio::time::timeout(
        Duration::from_secs(2),
        client.get_mempool_tx(lightwalletd::GetMempoolTxRequest {
            exclude_txid_suffixes: Vec::new(),
            pool_types: Vec::new(),
        }),
    )
    .await??
    .into_inner();

    let first_tx = tokio::time::timeout(Duration::from_secs(2), response_stream.next())
        .await?
        .ok_or_else(|| eyre!("expected page-1 mempool transaction before page 2 completes"))??;
    assert_eq!(first_tx.txid, vec![0xA1; 32]);

    page_two_gate
        .wait_until_reached(Duration::from_secs(2))
        .await?;
    let stalled_poll =
        tokio::time::timeout(Duration::from_millis(100), response_stream.next()).await;
    assert!(
        stalled_poll.is_err(),
        "page-2 poll must wait while page 2 is gated; got {stalled_poll:?}"
    );

    page_two_gate.release()?;
    let second_tx = tokio::time::timeout(Duration::from_secs(2), response_stream.next())
        .await?
        .ok_or_else(|| eyre!("expected page-2 mempool transaction after gate release"))??;
    assert_eq!(second_tx.txid, vec![0xB2; 32]);
    let trailing = tokio::time::timeout(Duration::from_secs(2), response_stream.next()).await?;
    assert!(trailing.is_none(), "expected stream end, got {trailing:?}");
    server.abort();
    Ok(())
}

async fn assert_large_page_cancellation_bounds_read_ahead(
    wallet_query: WalletServingQuery<()>,
    epoch: ChainEpoch,
) -> eyre::Result<()> {
    let mut multi_entry_pages = Vec::with_capacity(10);
    for page_idx in 0..10_u8 {
        let mut page_entries = Vec::with_capacity(20);
        for entry_idx in 0..20_u8 {
            let mut txid = [0_u8; 32];
            txid[0] = page_idx.saturating_add(1);
            txid[1] = entry_idx;
            page_entries.push(synthetic_entry_with_transaction_id(txid, epoch)?);
        }
        multi_entry_pages.push(ScriptedSnapshotPage::Entries(page_entries));
    }
    let (large_surface, large_fetched) = ScriptedPagedSnapshotSurface::new(multi_entry_pages);
    let large_adapter =
        LightwalletdGrpcAdapter::new(wallet_query).with_mempool_surface(Arc::new(large_surface));
    let mut large_stream = large_adapter
        .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest {
            exclude_txid_suffixes: Vec::new(),
            pool_types: Vec::new(),
        }))
        .await?
        .into_inner();
    for _ in 0..20 {
        let next_tx = tokio::time::timeout(Duration::from_secs(2), large_stream.next())
            .await?
            .ok_or_else(|| eyre!("expected 20 entries on page 1"))??;
        assert_eq!(next_tx.txid[0], 1);
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(large_stream);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let large_pages_fetched = large_fetched.load(Ordering::SeqCst);
    assert!(
        large_pages_fetched <= 2,
        "expected at most 2 pages fetched for 20-entry pages, got {large_pages_fetched}"
    );
    Ok(())
}

async fn assert_single_entry_page_cancellation_bounds_read_ahead(
    wallet_query: WalletServingQuery<()>,
    epoch: ChainEpoch,
) -> eyre::Result<()> {
    let mut single_entry_pages = Vec::with_capacity(40);
    for page_idx in 0..40_u8 {
        single_entry_pages.push(ScriptedSnapshotPage::Entries(vec![synthetic_entry(
            page_idx.saturating_add(1),
            epoch,
        )?]));
    }
    let (single_surface, single_fetched) = ScriptedPagedSnapshotSurface::new(single_entry_pages);
    let single_adapter =
        LightwalletdGrpcAdapter::new(wallet_query).with_mempool_surface(Arc::new(single_surface));
    let mut single_stream = single_adapter
        .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest {
            exclude_txid_suffixes: Vec::new(),
            pool_types: Vec::new(),
        }))
        .await?
        .into_inner();
    let first_single = tokio::time::timeout(Duration::from_secs(2), single_stream.next())
        .await?
        .ok_or_else(|| eyre!("expected first single-entry page"))??;
    assert_eq!(first_single.txid, vec![1; 32]);
    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(single_stream);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let single_pages_fetched = single_fetched.load(Ordering::SeqCst);
    assert!(
        single_pages_fetched <= 17,
        "expected at most 17 single-entry pages fetched (1 initial + 16 buffered), got {single_pages_fetched}"
    );
    Ok(())
}

async fn assert_in_flight_page_cancellation_on_drop(
    wallet_query: WalletServingQuery<()>,
    epoch: ChainEpoch,
) -> eyre::Result<()> {
    let (gated_surface, gated_fetched) = ScriptedPagedSnapshotSurface::new(vec![
        ScriptedSnapshotPage::Entries(vec![synthetic_entry(0x11, epoch)?]),
        ScriptedSnapshotPage::Entries(vec![synthetic_entry(0x22, epoch)?]),
        ScriptedSnapshotPage::Entries(vec![synthetic_entry(0x33, epoch)?]),
    ]);
    let (gated_surface, page_two_gate, cancelled_rx) = gated_surface.with_page_gate(1);
    let gated_adapter =
        LightwalletdGrpcAdapter::new(wallet_query).with_mempool_surface(Arc::new(gated_surface));
    let mut gated_stream = gated_adapter
        .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest {
            exclude_txid_suffixes: Vec::new(),
            pool_types: Vec::new(),
        }))
        .await?
        .into_inner();
    let first_gated = tokio::time::timeout(Duration::from_secs(2), gated_stream.next())
        .await?
        .ok_or_else(|| eyre!("expected page-1 entry before page-2 gate"))??;
    assert_eq!(first_gated.txid, vec![0x11; 32]);
    page_two_gate
        .wait_until_reached(Duration::from_secs(2))
        .await?;
    drop(gated_stream);
    tokio::time::timeout(Duration::from_secs(2), cancelled_rx)
        .await
        .map_err(|_| eyre!("in-flight page 2 was not cancelled on stream drop"))?
        .map_err(|_| eyre!("page-2 cancellation sender disappeared"))?;
    assert_eq!(gated_fetched.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_tx_cancellation_bounds_read_ahead_and_skips_trailing_pages()
-> eyre::Result<()> {
    let (wallet_query, _store_fixture) = single_block_serving_query()?;
    let epoch = synthetic_chain_epoch();
    assert_large_page_cancellation_bounds_read_ahead(wallet_query.clone(), epoch).await?;
    assert_single_entry_page_cancellation_bounds_read_ahead(wallet_query.clone(), epoch).await?;
    assert_in_flight_page_cancellation_on_drop(wallet_query, epoch).await
}

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_tx_empty_exclusions_matches_buffered_wire_parity()
-> eyre::Result<()> {
    let (wallet_query, _store_fixture) = single_block_serving_query()?;
    let epoch = synthetic_chain_epoch();

    let empty_snapshot_pages = vec![ScriptedSnapshotPage::Entries(Vec::new())];
    let multi_pool_pages = vec![
        ScriptedSnapshotPage::Entries(Vec::new()),
        ScriptedSnapshotPage::Entries(vec![
            synthetic_entry_with_transaction_id([0x11; 32], epoch)?,
            transparent_only_entry(0x22, epoch)?,
        ]),
        ScriptedSnapshotPage::Entries(Vec::new()),
        ScriptedSnapshotPage::Entries(vec![
            orchard_only_entry_with_transaction_id([0x33; 32], epoch)?,
            ironwood_only_entry_with_transaction_id([0x44; 32], epoch)?,
        ]),
        ScriptedSnapshotPage::Entries(Vec::new()),
    ];

    let pool_filters = [
        Vec::new(),
        vec![lightwalletd::PoolType::Sapling as i32],
        vec![lightwalletd::PoolType::Orchard as i32],
        vec![lightwalletd::PoolType::Ironwood as i32],
        vec![lightwalletd::PoolType::Transparent as i32],
        vec![
            lightwalletd::PoolType::Sapling as i32,
            lightwalletd::PoolType::Transparent as i32,
        ],
        vec![-1, 99],
    ];

    for pages in [empty_snapshot_pages, multi_pool_pages] {
        let (surface, _) = ScriptedPagedSnapshotSurface::new(pages);
        let adapter = LightwalletdGrpcAdapter::new(wallet_query.clone())
            .with_mempool_surface(Arc::new(surface));
        for pool_types in &pool_filters {
            let streaming_stream = adapter
                .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest {
                    exclude_txid_suffixes: Vec::new(),
                    pool_types: pool_types.clone(),
                }))
                .await?
                .into_inner();
            let streamed = collect_compact_txs(streaming_stream).await?;

            let buffered_stream = adapter
                .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest {
                    exclude_txid_suffixes: vec![vec![0xFF; 32]],
                    pool_types: pool_types.clone(),
                }))
                .await?
                .into_inner();
            let buffered = collect_compact_txs(buffered_stream).await?;
            assert_eq!(
                streamed, buffered,
                "wire parity mismatch for {pool_types:?}"
            );
        }
    }
    Ok(())
}

async fn assert_later_page_surface_error(
    wallet_query: &WalletServingQuery<()>,
    epoch: ChainEpoch,
    page_error: ScriptedSnapshotPageError,
) -> eyre::Result<()> {
    let expected_code = page_error.expected_code();
    let pages = vec![
        ScriptedSnapshotPage::Entries(vec![synthetic_entry(0xA1, epoch)?]),
        ScriptedSnapshotPage::Error(page_error),
        ScriptedSnapshotPage::Entries(vec![synthetic_entry(0xB2, epoch)?]),
    ];

    let (streaming_surface, streaming_fetched) = ScriptedPagedSnapshotSurface::new(pages.clone());
    let streaming_adapter = LightwalletdGrpcAdapter::new(wallet_query.clone())
        .with_mempool_surface(Arc::new(streaming_surface));
    let mut streaming_response = streaming_adapter
        .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest {
            exclude_txid_suffixes: Vec::new(),
            pool_types: Vec::new(),
        }))
        .await?
        .into_inner();
    let first_tx = streaming_response
        .next()
        .await
        .ok_or_else(|| eyre!("expected page-1 entry before page-2 surface error"))??;
    assert_eq!(first_tx.txid, vec![0xA1; 32]);
    let second_outcome = streaming_response
        .next()
        .await
        .ok_or_else(|| eyre!("expected page-2 surface error"))?;
    let status = second_outcome
        .err()
        .ok_or_else(|| eyre!("expected Err status from page 2"))?;
    assert_eq!(status.code(), expected_code);
    assert!(streaming_response.next().await.is_none());
    assert_eq!(streaming_fetched.load(Ordering::SeqCst), 2);

    let (buffered_surface, buffered_fetched) = ScriptedPagedSnapshotSurface::new(pages);
    let buffered_adapter = LightwalletdGrpcAdapter::new(wallet_query.clone())
        .with_mempool_surface(Arc::new(buffered_surface));
    let buffered_outcome = buffered_adapter
        .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest {
            exclude_txid_suffixes: vec![vec![0xFF; 32]],
            pool_types: Vec::new(),
        }))
        .await;
    let buffered_status = buffered_outcome
        .err()
        .ok_or_else(|| eyre!("expected buffered path to fail on page 2"))?;
    assert_eq!(buffered_status.code(), expected_code);
    assert_eq!(buffered_fetched.load(Ordering::SeqCst), 2);
    Ok(())
}

async fn assert_later_page_encode_error(
    wallet_query: &WalletServingQuery<()>,
    epoch: ChainEpoch,
) -> eyre::Result<()> {
    let encode_error_pages = vec![
        ScriptedSnapshotPage::Entries(vec![synthetic_entry(0xA1, epoch)?]),
        ScriptedSnapshotPage::Entries(vec![oversized_fee_transparent_entry(0xC2, epoch)?]),
        ScriptedSnapshotPage::Entries(vec![synthetic_entry(0xB2, epoch)?]),
    ];
    let (encode_surface, _) = ScriptedPagedSnapshotSurface::new(encode_error_pages.clone());
    let encode_adapter = LightwalletdGrpcAdapter::new(wallet_query.clone())
        .with_mempool_surface(Arc::new(encode_surface));
    let mut encode_stream = encode_adapter
        .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest {
            exclude_txid_suffixes: Vec::new(),
            pool_types: vec![lightwalletd::PoolType::Sapling as i32],
        }))
        .await?
        .into_inner();
    let page_one_tx = encode_stream
        .next()
        .await
        .ok_or_else(|| eyre!("expected page-1 entry before encode error"))??;
    assert_eq!(page_one_tx.txid, vec![0xA1; 32]);
    let encode_failure = encode_stream
        .next()
        .await
        .ok_or_else(|| eyre!("expected page-2 fee overflow error"))?;
    let encode_status = encode_failure
        .err()
        .ok_or_else(|| eyre!("expected DataLoss error on oversized fee"))?;
    assert_eq!(encode_status.code(), Code::DataLoss);
    assert!(encode_stream.next().await.is_none());

    let (buffered_encode_surface, _) = ScriptedPagedSnapshotSurface::new(encode_error_pages);
    let buffered_encode_adapter = LightwalletdGrpcAdapter::new(wallet_query.clone())
        .with_mempool_surface(Arc::new(buffered_encode_surface));
    let buffered_encode_status = buffered_encode_adapter
        .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest {
            exclude_txid_suffixes: vec![vec![0xFF; 32]],
            pool_types: vec![lightwalletd::PoolType::Sapling as i32],
        }))
        .await
        .err()
        .ok_or_else(|| eyre!("expected buffered path to fail on page-2 fee overflow"))?;
    assert_eq!(buffered_encode_status.code(), Code::DataLoss);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_tx_propagates_later_page_errors_after_page_one()
-> eyre::Result<()> {
    let (wallet_query, _store_fixture) = single_block_serving_query()?;
    let epoch = synthetic_chain_epoch();

    for page_error in [
        ScriptedSnapshotPageError::Unavailable("upstream page 2 outage".to_owned()),
        ScriptedSnapshotPageError::CursorExpired,
        ScriptedSnapshotPageError::CursorInvalid,
    ] {
        assert_later_page_surface_error(&wallet_query, epoch, page_error).await?;
    }
    assert_later_page_encode_error(&wallet_query, epoch).await
}

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_tx_non_empty_exclusions_waits_for_full_snapshot_uniqueness()
-> eyre::Result<()> {
    let (wallet_query, _store_fixture) = single_block_serving_query()?;
    let epoch = synthetic_chain_epoch();
    let mut tx_a = [0x11; 32];
    tx_a[31] = 0xAA;
    let mut tx_b = [0x22; 32];
    tx_b[31] = 0xAA;
    let mut tx_c = [0x33; 32];
    tx_c[31] = 0xBB;

    let (surface, _fetched) = ScriptedPagedSnapshotSurface::new(vec![
        ScriptedSnapshotPage::Entries(vec![synthetic_entry_with_transaction_id(tx_a, epoch)?]),
        ScriptedSnapshotPage::Entries(vec![
            ironwood_only_entry_with_transaction_id(tx_b, epoch)?,
            synthetic_entry_with_transaction_id(tx_c, epoch)?,
        ]),
    ]);
    let (surface, page_two_gate, _cancelled_rx) = surface.with_page_gate(1);
    let adapter = Arc::new(
        LightwalletdGrpcAdapter::new(wallet_query).with_mempool_surface(Arc::new(surface)),
    );

    let adapter_task = Arc::clone(&adapter);
    let mut request_task = tokio::spawn(async move {
        adapter_task
            .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest {
                exclude_txid_suffixes: vec![vec![0xAA], vec![0xBB]],
                pool_types: vec![lightwalletd::PoolType::Sapling as i32],
            }))
            .await
    });

    page_two_gate
        .wait_until_reached(Duration::from_secs(2))
        .await?;
    let premature = tokio::time::timeout(Duration::from_millis(50), &mut request_task).await;
    assert!(
        premature.is_err(),
        "non-empty exclusions must wait for full snapshot observation before returning response"
    );

    page_two_gate.release()?;
    let response = tokio::time::timeout(Duration::from_secs(2), request_task)
        .await???
        .into_inner();
    let collected = collect_compact_txids(response).await?;
    assert_eq!(collected, vec![tx_a.to_vec()]);
    Ok(())
}
