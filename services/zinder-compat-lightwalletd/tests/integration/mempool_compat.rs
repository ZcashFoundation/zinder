#![allow(
    missing_docs,
    reason = "Integration test names describe the lightwalletd compat behavior under test."
)]

use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use eyre::eyre;
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt as _, wrappers::UnboundedReceiverStream};
use tonic::{Code, Request};
use zinder_compat_lightwalletd::{
    LightwalletdGrpcAdapter, MempoolEventEnvelopeStream, MempoolSnapshotPage, MempoolSurface,
    MempoolSurfaceError, TipChangeWatcher, TipChangeWatcherError,
};
use zinder_core::{
    AuthDigest, BlockHash, BlockHeight, ChainEpoch, ChainEpochId, ChainTipMetadata,
    CompactSaplingOutput, CompactSaplingSpend, CompactShieldedAction, CompactTransactionData,
    CompactTransparentInput, CompactTransparentOutput, MempoolEntry, MempoolEvictionReason,
    MempoolObservation, Network, RawTransactionBytes, TransactionId, UnixTimestampMillis,
};
use zinder_proto::compat::lightwalletd::{self, compact_tx_streamer_server::CompactTxStreamer};
use zinder_query::WalletQuery;
use zinder_store::{
    CURRENT_ARTIFACT_SCHEMA_VERSION, MempoolEvent, MempoolEventEnvelope, StreamCursorTokenV1,
};
use zinder_testkit::{StoreFixture, sample_regtest_upgrade_activations};

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_tx_returns_unavailable_without_surface() -> eyre::Result<()> {
    let store_fixture = StoreFixture::with_single_block(Network::ZcashRegtest)?;
    let adapter = LightwalletdGrpcAdapter::new(WalletQuery::new(
        store_fixture.chain_store().clone(),
        (),
        Arc::new(sample_regtest_upgrade_activations()),
    ));
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
    let store_fixture = StoreFixture::with_single_block(Network::ZcashRegtest)?;
    let adapter = LightwalletdGrpcAdapter::new(WalletQuery::new(
        store_fixture.chain_store().clone(),
        (),
        Arc::new(sample_regtest_upgrade_activations()),
    ));

    let outcome = adapter
        .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest {
            exclude_txid_suffixes: vec![vec![0; 33]],
            pool_types: Vec::new(),
        }))
        .await;
    let status = outcome
        .err()
        .ok_or_else(|| eyre!("expected invalid excluded txid suffix"))?;
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(status.message(), "exclude txid 0 is larger than 32 bytes");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_tx_rejects_too_many_excluded_txid_suffixes_before_surface_lookup()
-> eyre::Result<()> {
    let store_fixture = StoreFixture::with_single_block(Network::ZcashRegtest)?;
    let adapter = LightwalletdGrpcAdapter::new(WalletQuery::new(
        store_fixture.chain_store().clone(),
        (),
        Arc::new(sample_regtest_upgrade_activations()),
    ));

    let outcome = adapter
        .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest {
            exclude_txid_suffixes: vec![vec![0xAA]; 1_025],
            pool_types: Vec::new(),
        }))
        .await;
    let status = outcome
        .err()
        .ok_or_else(|| eyre!("expected too many excluded txid suffixes to be rejected"))?;
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(
        status.message(),
        "exclude_txid_suffixes contains 1025 entries; at most 1024 are allowed"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_tx_rejects_invalid_pool_type_before_surface_lookup()
-> eyre::Result<()> {
    let store_fixture = StoreFixture::with_single_block(Network::ZcashRegtest)?;
    let adapter = LightwalletdGrpcAdapter::new(WalletQuery::new(
        store_fixture.chain_store().clone(),
        (),
        Arc::new(sample_regtest_upgrade_activations()),
    ));

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
async fn lightwalletd_get_mempool_tx_filters_excluded_txid_suffixes() -> eyre::Result<()> {
    let store_fixture = StoreFixture::with_single_block(Network::ZcashRegtest)?;
    let surface = ScriptedMempoolSurface::with_entries(vec![
        synthetic_entry(0xAA, synthetic_chain_epoch())?,
        synthetic_entry(0xBB, synthetic_chain_epoch())?,
    ]);
    let adapter = LightwalletdGrpcAdapter::new(WalletQuery::new(
        store_fixture.chain_store().clone(),
        (),
        Arc::new(sample_regtest_upgrade_activations()),
    ))
    .with_mempool_surface(Arc::new(surface));

    let suffix = vec![0xAA; 4];
    let response = adapter
        .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest {
            exclude_txid_suffixes: vec![suffix],
            pool_types: Vec::new(),
        }))
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
    let store_fixture = StoreFixture::with_single_block(Network::ZcashRegtest)?;
    let mut first_transaction_id = [0x11; 32];
    first_transaction_id[31] = 0xAA;
    let mut second_transaction_id = [0x22; 32];
    second_transaction_id[31] = 0xAA;
    let surface = ScriptedMempoolSurface::with_entries(vec![
        synthetic_entry_with_transaction_id(first_transaction_id, synthetic_chain_epoch())?,
        synthetic_entry_with_transaction_id(second_transaction_id, synthetic_chain_epoch())?,
    ])
    .with_snapshot_page_size(1);
    let adapter = LightwalletdGrpcAdapter::new(WalletQuery::new(
        store_fixture.chain_store().clone(),
        (),
        Arc::new(sample_regtest_upgrade_activations()),
    ))
    .with_mempool_surface(Arc::new(surface));

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
async fn lightwalletd_get_mempool_tx_uniquely_excludes_match_with_other_match_outside_requested_pools()
-> eyre::Result<()> {
    let store_fixture = StoreFixture::with_single_block(Network::ZcashRegtest)?;
    let mut streamable_transaction_id = [0x11; 32];
    streamable_transaction_id[31] = 0xAA;
    let mut pruned_transaction_id = [0x22; 32];
    pruned_transaction_id[31] = 0xAA;
    let surface = ScriptedMempoolSurface::with_entries(vec![
        synthetic_entry_with_transaction_id(streamable_transaction_id, synthetic_chain_epoch())?,
        ironwood_only_entry_with_transaction_id(pruned_transaction_id, synthetic_chain_epoch())?,
    ]);
    let adapter = LightwalletdGrpcAdapter::new(WalletQuery::new(
        store_fixture.chain_store().clone(),
        (),
        Arc::new(sample_regtest_upgrade_activations()),
    ))
    .with_mempool_surface(Arc::new(surface));

    let response = adapter
        .get_mempool_tx(Request::new(lightwalletd::GetMempoolTxRequest {
            exclude_txid_suffixes: vec![vec![0xAA]],
            pool_types: vec![lightwalletd::PoolType::Sapling as i32],
        }))
        .await?
        .into_inner();
    let collected = collect_compact_txids(response).await?;
    assert!(collected.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_tx_drops_transactions_outside_requested_pool_types()
-> eyre::Result<()> {
    let store_fixture = StoreFixture::with_single_block(Network::ZcashRegtest)?;
    let surface = ScriptedMempoolSurface::with_entries(vec![
        synthetic_entry(0xC1, synthetic_chain_epoch())?,
        transparent_only_entry(0xC2, synthetic_chain_epoch())?,
    ]);
    let adapter = LightwalletdGrpcAdapter::new(WalletQuery::new(
        store_fixture.chain_store().clone(),
        (),
        Arc::new(sample_regtest_upgrade_activations()),
    ))
    .with_mempool_surface(Arc::new(surface));

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

#[tokio::test(flavor = "multi_thread")]
async fn lightwalletd_get_mempool_tx_drops_ironwood_entries_outside_requested_pool_types()
-> eyre::Result<()> {
    let store_fixture = StoreFixture::with_single_block(Network::ZcashRegtest)?;
    let surface = ScriptedMempoolSurface::with_entries(vec![
        synthetic_entry(0xD1, synthetic_chain_epoch())?,
        ironwood_only_entry(0xD2, synthetic_chain_epoch())?,
    ]);
    let adapter = LightwalletdGrpcAdapter::new(WalletQuery::new(
        store_fixture.chain_store().clone(),
        (),
        Arc::new(sample_regtest_upgrade_activations()),
    ))
    .with_mempool_surface(Arc::new(surface));

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
    let store_fixture = StoreFixture::with_single_block(Network::ZcashRegtest)?;
    let surface = ScriptedMempoolSurface::with_entries(vec![
        synthetic_entry(0xA1, synthetic_chain_epoch())?,
        synthetic_entry(0xB2, synthetic_chain_epoch())?,
    ])
    .with_snapshot_page_size(1);
    let adapter = LightwalletdGrpcAdapter::new(WalletQuery::new(
        store_fixture.chain_store().clone(),
        (),
        Arc::new(sample_regtest_upgrade_activations()),
    ))
    .with_mempool_surface(Arc::new(surface));

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
    let store_fixture = StoreFixture::with_single_block(Network::ZcashRegtest)?;
    let surface = ScriptedMempoolSurface::with_entries(Vec::new());
    let control = surface.event_control();
    let adapter = LightwalletdGrpcAdapter::new(WalletQuery::new(
        store_fixture.chain_store().clone(),
        (),
        Arc::new(sample_regtest_upgrade_activations()),
    ))
    .with_mempool_surface(Arc::new(surface));

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
    let store_fixture = StoreFixture::with_single_block(Network::ZcashRegtest)?;
    let surface = ScriptedMempoolSurface::with_entries(Vec::new());
    let control = surface.event_control();
    control.append_retained_event(MempoolEvent::Added {
        entry: synthetic_entry(0x10, synthetic_chain_epoch())?,
    })?;
    let adapter = LightwalletdGrpcAdapter::new(WalletQuery::new(
        store_fixture.chain_store().clone(),
        (),
        Arc::new(sample_regtest_upgrade_activations()),
    ))
    .with_mempool_surface(Arc::new(surface));

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
    let store_fixture = StoreFixture::with_single_block(Network::ZcashRegtest)?;
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
    let adapter = LightwalletdGrpcAdapter::new(WalletQuery::new(
        store_fixture.chain_store().clone(),
        (),
        Arc::new(sample_regtest_upgrade_activations()),
    ))
    .with_mempool_surface(Arc::new(surface));

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
    snapshot_page_size: Option<usize>,
    retained_events: Arc<Mutex<Vec<MempoolEventEnvelope>>>,
    pending_event_sender: SharedEventSenderSlot,
    event_sequence: Arc<Mutex<u64>>,
}

impl ScriptedMempoolSurface {
    fn with_entries(entries: Vec<MempoolEntry>) -> Self {
        Self {
            entries: Mutex::new(entries),
            snapshot_page_size: None,
            retained_events: Arc::new(Mutex::new(Vec::new())),
            pending_event_sender: Arc::new(Mutex::new(None)),
            event_sequence: Arc::new(Mutex::new(0u64)),
        }
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
        }
    }
}

struct ScriptedMempoolEventControl {
    retained_events: Arc<Mutex<Vec<MempoolEventEnvelope>>>,
    sender_slot: SharedEventSenderSlot,
    event_sequence: Arc<Mutex<u64>>,
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
    let store_fixture = StoreFixture::with_single_block(Network::ZcashRegtest)?;
    let surface = ScriptedMempoolSurface::with_entries(Vec::new());
    let event_control = surface.event_control();
    let tip_change_watcher = ScriptedTipChangeWatcher::new();
    let tip_change_signal = tip_change_watcher.signal();
    let adapter = LightwalletdGrpcAdapter::new(WalletQuery::new(
        store_fixture.chain_store().clone(),
        (),
        Arc::new(sample_regtest_upgrade_activations()),
    ))
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
    let store_fixture = StoreFixture::with_single_block(Network::ZcashRegtest)?;
    let surface = ScriptedMempoolSurface::with_entries(Vec::new());
    let tip_change_watcher = ScriptedTipChangeWatcher::new();
    let tip_change_signal = tip_change_watcher.signal();
    let adapter = LightwalletdGrpcAdapter::new(WalletQuery::new(
        store_fixture.chain_store().clone(),
        (),
        Arc::new(sample_regtest_upgrade_activations()),
    ))
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
        let stream: Pin<
            Box<
                dyn Stream<Item = Result<MempoolEventEnvelope, MempoolSurfaceError>>
                    + Send
                    + 'static,
            >,
        > = Box::pin(UnboundedReceiverStream::new(event_receiver));
        Ok(stream)
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
