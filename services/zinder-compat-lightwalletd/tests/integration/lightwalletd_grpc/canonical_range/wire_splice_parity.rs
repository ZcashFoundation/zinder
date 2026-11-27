use std::sync::Arc;

use prost::Message as _;
use rust_rocksdb::{DB, DEFAULT_COLUMN_FAMILY_NAME, Options};
use serde_json::json;
use tonic::{Code, transport::Server};
use zinder_compat_lightwalletd::{
    CanonicalReaderSlot, CompactBlocksGrpcAdapter, LightwalletdGrpcAdapter,
};
use zinder_core::{
    BlockHeight, BlockHeightRange, BlockId, CompactBlockArtifact, CompactChainMetadata,
    CompactSaplingOutput, CompactSaplingSpend, CompactShieldedAction, CompactTransaction,
    CompactTransactionData, CompactTransparentInput, CompactTransparentOutput, TransactionId,
    wire::encode_internal_block_hash,
};
use zinder_proto::compat::lightwalletd::{self, PoolType};
use zinder_query::{
    CanonicalReader as _, DEFAULT_TREE_STATE_REPLAY_WAIT,
    DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT, EncodedCompactBlockLookup, ForwardedReadLimit,
    RocksDbCanonicalReader, StorageReadError,
};
use zinder_source::{NodeAuth, ZebraJsonRpcSource};
use zinder_store::{
    CompactBlockSpliceSelection, EncodedCompactBlockOutput, EncodedCompactBlockRangeRequest,
    HeightOrder, RawBlobRetention, ShieldedProjectionSelection,
};
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, JsonRpcTestServer, RpcReply, method,
    sample_regtest_upgrade_activations,
};

use super::{
    Endpoint, EpochPinRecorder, NETWORK, ReaderFixture, attach_grpc_subset_rows,
    block_range_request, direct_recorder, fixture_block, subset_writer,
};
use crate::common::verified_node::verifiable_node;

struct ParityEndpoints {
    wallet: Endpoint,
    light_wallet: Endpoint,
    blocks_only: Endpoint,
    stock: Endpoint,
    _node: JsonRpcTestServer,
}

fn parity_action(tag: u8) -> CompactShieldedAction {
    CompactShieldedAction {
        nullifier: [tag; 32],
        commitment: [tag.wrapping_add(1); 32],
        ephemeral_key: [tag.wrapping_add(2); 32],
        ciphertext: [tag.wrapping_add(3); 52],
    }
}

fn unique_txid(height_num: u32, tx_index: u8) -> TransactionId {
    let mut raw = [0x5a_u8; 32];
    raw[..4].copy_from_slice(&height_num.to_be_bytes());
    raw[4] = tx_index;
    TransactionId::from_bytes(raw)
}

fn parity_mixed_txs(height_num: u32, seed: u8) -> Vec<CompactTransaction> {
    vec![
        CompactTransaction {
            index: 0,
            transaction_id: unique_txid(height_num, 0),
            data: CompactTransactionData::default(),
        },
        CompactTransaction {
            index: 1,
            transaction_id: unique_txid(height_num, 1),
            data: CompactTransactionData {
                fee_zat: Some(888),
                sapling_spends: vec![CompactSaplingSpend {
                    nullifier: [seed.wrapping_add(2); 32],
                }],
                orchard_actions: vec![parity_action(seed.wrapping_add(6))],
                ironwood_actions: if height_num >= 4 {
                    vec![parity_action(seed.wrapping_add(9))]
                } else {
                    Vec::new()
                },
                transparent_inputs: vec![CompactTransparentInput {
                    previous_transaction_id: TransactionId::from_bytes([seed.wrapping_add(15); 32]),
                    previous_output_index: 1,
                }],
                transparent_outputs: vec![CompactTransparentOutput {
                    value_zat: 33_000,
                    script_pub_key: vec![0x00, 0x14, seed],
                }],
                ..Default::default()
            },
        },
    ]
}

fn parity_block_txs(height_num: u32) -> Vec<CompactTransaction> {
    let seed = u8::try_from(height_num % 200)
        .unwrap_or(1)
        .saturating_add(40);
    let first_tx_data = match height_num {
        1 => CompactTransactionData {
            sapling_spends: vec![CompactSaplingSpend {
                nullifier: [seed; 32],
            }],
            sapling_outputs: vec![CompactSaplingOutput {
                commitment: [seed.wrapping_add(1); 32],
                ephemeral_key: [seed.wrapping_add(2); 32],
                ciphertext: [seed.wrapping_add(3); 52],
            }],
            ..Default::default()
        },
        2 | 6 => CompactTransactionData {
            fee_zat: Some(420),
            transparent_inputs: vec![CompactTransparentInput {
                previous_transaction_id: TransactionId::from_bytes([seed.wrapping_add(5); 32]),
                previous_output_index: height_num,
            }],
            transparent_outputs: vec![CompactTransparentOutput {
                value_zat: 75_000,
                script_pub_key: vec![0x76, 0xa9, seed, 0x88, 0xac],
            }],
            ..Default::default()
        },
        3 | 7 => return parity_mixed_txs(height_num, seed),
        4 => CompactTransactionData {
            fee_zat: Some(150),
            ironwood_actions: vec![parity_action(seed.wrapping_add(11))],
            ..Default::default()
        },
        5 => return Vec::new(),
        _ => CompactTransactionData::default(),
    };
    vec![CompactTransaction {
        index: 0,
        transaction_id: unique_txid(height_num, 0),
        data: first_tx_data,
    }]
}

fn build_mixed_parity_chain(total_blocks: u32) -> eyre::Result<ChainFixture> {
    let mut chain = ChainFixture::new(NETWORK)
        .with_raw_blob_retention(RawBlobRetention::None)
        .extend_blocks(total_blocks);
    let mut sapling_tree = 0_u32;
    let mut orchard_tree = 0_u32;
    let mut ironwood_tree = 0_u32;
    for h in 1_u32..=total_blocks {
        let blk = fixture_block(&chain, BlockHeight::new(h))?;
        let txs = parity_block_txs(h);
        for tx in &txs {
            sapling_tree += u32::try_from(tx.data.sapling_outputs.len())?;
            orchard_tree += u32::try_from(tx.data.orchard_actions.len())?;
            ironwood_tree += u32::try_from(tx.data.ironwood_actions.len())?;
        }
        chain = attach_grpc_subset_rows(chain, &blk, &txs)?;
        let parent_hash = if h == 1 {
            NETWORK.genesis_hash()
        } else {
            blk.parent_hash
        };
        let compact = CompactBlockArtifact::new(
            BlockId::new(blk.height, blk.hash),
            parent_hash,
            blk.block_time_seconds,
            txs,
            CompactChainMetadata {
                sapling_commitment_tree_size: sapling_tree,
                orchard_commitment_tree_size: orchard_tree,
                ironwood_commitment_tree_size: ironwood_tree,
            },
        )?;
        chain = chain.with_compact_block_artifact(compact);
    }
    Ok(chain)
}

#[allow(
    deprecated,
    reason = "Compatibility covers the legacy GetBlockNullifiers RPC used by existing wallets."
)]
async fn assert_unary_parity(
    endpoints: &mut ParityEndpoints,
    chain: &ChainFixture,
) -> eyre::Result<()> {
    for h in 1_u32..=8 {
        let blk = fixture_block(chain, BlockHeight::new(h))?;
        let by_height = lightwalletd::BlockId {
            height: u64::from(h),
            hash: Vec::new(),
        };
        let by_hash = lightwalletd::BlockId {
            height: 0,
            hash: encode_internal_block_hash(blk.hash).to_vec(),
        };
        for req in [by_height, by_hash] {
            let expected_block = endpoints
                .stock
                .client
                .get_block(req.clone())
                .await?
                .into_inner()
                .encode_to_vec();
            let wallet_block = endpoints
                .wallet
                .client
                .get_block(req.clone())
                .await?
                .into_inner()
                .encode_to_vec();
            let light_wallet_block = endpoints
                .light_wallet
                .client
                .get_block(req.clone())
                .await?
                .into_inner()
                .encode_to_vec();
            let blocks_only_block = endpoints
                .blocks_only
                .client
                .get_block(req.clone())
                .await?
                .into_inner()
                .encode_to_vec();
            assert_eq!(wallet_block, expected_block);
            assert_eq!(light_wallet_block, expected_block);
            assert_eq!(blocks_only_block, expected_block);

            let expected_nullifiers = endpoints
                .stock
                .client
                .get_block_nullifiers(req.clone())
                .await?
                .into_inner()
                .encode_to_vec();
            let wallet_nullifiers = endpoints
                .wallet
                .client
                .get_block_nullifiers(req.clone())
                .await?
                .into_inner()
                .encode_to_vec();
            let light_wallet_nullifiers = endpoints
                .light_wallet
                .client
                .get_block_nullifiers(req.clone())
                .await?
                .into_inner()
                .encode_to_vec();
            assert_eq!(wallet_nullifiers, expected_nullifiers);
            assert_eq!(light_wallet_nullifiers, expected_nullifiers);
            let compact_only_err = endpoints
                .blocks_only
                .client
                .get_block_nullifiers(req)
                .await
                .err()
                .map(|status| status.code());
            assert_eq!(compact_only_err, Some(Code::Unimplemented));
        }
        assert_mismatched_unary_not_found(endpoints, h).await;
    }
    Ok(())
}

#[allow(
    deprecated,
    reason = "Compatibility covers the legacy GetBlockNullifiers RPC used by existing wallets."
)]
async fn assert_mismatched_unary_not_found(endpoints: &mut ParityEndpoints, height: u32) {
    let mismatched = lightwalletd::BlockId {
        height: u64::from(height),
        hash: vec![0x99; 32],
    };
    for client in [
        &mut endpoints.wallet.client,
        &mut endpoints.light_wallet.client,
        &mut endpoints.blocks_only.client,
    ] {
        let err = client
            .get_block(mismatched.clone())
            .await
            .err()
            .map(|status| status.code());
        assert_eq!(err, Some(Code::NotFound));
    }
    for client in [
        &mut endpoints.wallet.client,
        &mut endpoints.light_wallet.client,
    ] {
        let err = client
            .get_block_nullifiers(mismatched.clone())
            .await
            .err()
            .map(|status| status.code());
        assert_eq!(err, Some(Code::NotFound));
    }
}

async fn start_parity_endpoints(
    store: &ReaderFixture,
    chain: &ChainFixture,
    recorder: &EpochPinRecorder<zinder_query::WalletServingQuery<()>>,
) -> eyre::Result<ParityEndpoints> {
    let tip = fixture_block(chain, BlockHeight::new(8))?;
    let tip_hash = tip.hash;
    let tip_height = tip.height.value();
    let served_tip = method("getbestblockheightandhash").reply_with(move |_| {
        RpcReply::result(json!({"height": tip_height, "hash": tip_hash.as_bytes()}))
    });
    let node = verifiable_node(&parity_activations(), tip_hash, [served_tip])?;
    let source = ZebraJsonRpcSource::new(
        NETWORK,
        node.url(),
        NodeAuth::None,
        std::time::Duration::from_secs(2),
    )?;
    let slot = CanonicalReaderSlot::new(Arc::clone(&store.reader));
    let light_wallet_adapter = LightwalletdGrpcAdapter::for_light_wallet(
        slot.clone(),
        source,
        DEFAULT_TREE_STATE_REPLAY_WAIT,
        DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT,
        ForwardedReadLimit::default(),
    );

    Ok(ParityEndpoints {
        wallet: Endpoint::start(
            Server::builder()
                .add_service(LightwalletdGrpcAdapter::new(recorder.clone()).into_server()),
        )
        .await?,
        light_wallet: Endpoint::start(
            Server::builder().add_service(light_wallet_adapter.into_server()),
        )
        .await?,
        blocks_only: Endpoint::start(
            Server::builder().add_service(CompactBlocksGrpcAdapter::new(slot).into_server()),
        )
        .await?,
        stock: Endpoint::start(Server::builder().add_service(
            LightwalletdGrpcAdapter::new(EpochPinRecorder::new(store.query.clone())).into_server(),
        ))
        .await?,
        _node: node,
    })
}

#[tokio::test]
async fn grpc_unary_and_transparent_range_presets_match_stock_wire_bytes() -> eyre::Result<()> {
    let chain = build_mixed_parity_chain(8)?;
    let store = ReaderFixture::new(subset_writer(&chain)?.into_serving_store_fixture()?)?;
    let recorder = direct_recorder(store.query.clone());
    let mut endpoints = start_parity_endpoints(&store, &chain, &recorder).await?;

    assert_unary_parity(&mut endpoints, &chain).await?;

    let trn = PoolType::Transparent as i32;
    let sap = PoolType::Sapling as i32;
    let orc = PoolType::Orchard as i32;
    let irw = PoolType::Ironwood as i32;
    let encode_all = |blocks: Vec<lightwalletd::CompactBlock>| -> Vec<Vec<u8>> {
        blocks
            .iter()
            .map(lightwalletd::CompactBlock::encode_to_vec)
            .collect()
    };
    for pools in [
        vec![trn],
        vec![trn, sap],
        vec![trn, orc],
        vec![trn, sap, orc],
        vec![trn, sap, orc, irw],
    ] {
        for (first, last) in [(1_u64, 8_u64), (8, 1), (4, 8), (8, 4)] {
            let mut req = block_range_request(first, last);
            req.pool_types = pools.clone();
            let expected = encode_all(endpoints.stock.blocks(req.clone()).await?);
            assert_eq!(
                encode_all(endpoints.wallet.blocks(req.clone()).await?),
                expected
            );
            assert_eq!(
                encode_all(endpoints.light_wallet.blocks(req.clone()).await?),
                expected
            );
            assert_eq!(
                encode_all(endpoints.blocks_only.blocks(req).await?),
                expected
            );
        }
    }
    assert_eq!(
        recorder.recorded_compact_block_ranges(),
        [] as [BlockHeightRange; 0]
    );
    Ok(())
}

fn parity_activations() -> zinder_core::NetworkUpgradeActivations {
    let entries = sample_regtest_upgrade_activations()
        .activations()
        .iter()
        .cloned()
        .map(|mut entry| {
            if entry.name == "NU6.3" {
                entry.activation_height = BlockHeight::new(4);
            }
            entry
        })
        .collect();
    zinder_core::NetworkUpgradeActivations::new(NETWORK, entries)
        .unwrap_or_else(|_| sample_regtest_upgrade_activations())
}

fn open_raw_primary_disabled_auto_compaction(path: &std::path::Path) -> eyre::Result<DB> {
    let cf_names = DB::list_cf(&Options::default(), path)?;
    let descriptors: Vec<_> = cf_names
        .iter()
        .map(|name| {
            let mut opts = Options::default();
            opts.set_disable_auto_compactions(true);
            rust_rocksdb::ColumnFamilyDescriptor::new(name, opts)
        })
        .collect();
    Ok(DB::open_cf_descriptors(
        &Options::default(),
        path,
        descriptors,
    )?)
}

fn prepare_unpinned_split_live_tail(canonical_path: &std::path::Path) -> eyre::Result<()> {
    let primary_db = open_raw_primary_disabled_auto_compaction(canonical_path)?;
    let default_cf = primary_db
        .cf_handle(DEFAULT_COLUMN_FAMILY_NAME)
        .ok_or_else(|| eyre::eyre!("missing default cf"))?;
    let (control_key, control_row) = primary_db
        .iterator_cf(&default_cf, rust_rocksdb::IteratorMode::Start)
        .next()
        .ok_or_else(|| eyre::eyre!("missing control row"))??;
    for _ in 0..16 {
        primary_db.put_cf(&default_cf, &control_key, &control_row)?;
        primary_db.flush_cf(&default_cf)?;
    }

    for family_name in ["block_header", "live_tail_block"] {
        let cf = primary_db
            .cf_handle(family_name)
            .ok_or_else(|| eyre::eyre!("missing {family_name} cf"))?;
        let rows: Vec<_> = primary_db
            .iterator_cf(&cf, rust_rocksdb::IteratorMode::Start)
            .collect::<Result<_, _>>()?;
        for (key, row) in rows {
            primary_db.put_cf(&cf, key, row)?;
            primary_db.flush_cf(&cf)?;
        }
    }
    Ok(())
}

async fn compact_away_live_tail_tables(canonical_path: &std::path::Path) -> eyre::Result<()> {
    let primary_db = open_raw_primary_disabled_auto_compaction(canonical_path)?;
    let live_cf = primary_db
        .cf_handle("live_tail_block")
        .ok_or_else(|| eyre::eyre!("missing live_tail_block cf"))?;
    let replaced: Vec<_> = primary_db
        .live_files()?
        .into_iter()
        .filter(|file| file.column_family_name == "live_tail_block")
        .map(|file| std::path::Path::new(&file.directory).join(file.name.trim_start_matches('/')))
        .collect();
    assert_ne!(replaced, [] as [std::path::PathBuf; 0]);
    let mut compact_opts = rust_rocksdb::CompactOptions::default();
    compact_opts.set_bottommost_level_compaction(rust_rocksdb::BottommostLevelCompaction::Force);
    primary_db.compact_range_cf_opt(&live_cf, None::<&[u8]>, None::<&[u8]>, &compact_opts);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while replaced.iter().any(|path| path.exists()) && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        replaced.iter().all(|path| !path.exists()),
        "compacted live_tail_block SST files must be deleted before secondary reads"
    );
    Ok(())
}

async fn assert_compacted_unary_reader_invalidated(
    reader: &RocksDbCanonicalReader,
    tail_end: u32,
) -> eyre::Result<()> {
    for selection in [
        CompactBlockSpliceSelection::FULL_BLOCK,
        CompactBlockSpliceSelection::BLOCK_NULLIFIERS,
    ] {
        let mut invalidated_count = 0_usize;
        for h in 6_u32..=tail_end {
            match reader
                .encoded_compact_block_at(BlockHeight::new(h), selection)
                .await
            {
                Ok(EncodedCompactBlockLookup::Block(_)) => {}
                Ok(other) => eyre::bail!("unexpected lookup variant at {h}: {other:?}"),
                Err(StorageReadError::ReaderInvalidated { .. }) => invalidated_count += 1,
                Err(other) => return Err(other.into()),
            }
        }
        assert!(
            invalidated_count > 0,
            "at least one compacted live-tail height must report ReaderInvalidated"
        );
    }
    assert!(matches!(
        reader
            .encoded_compact_block_at(
                BlockHeight::new(tail_end + 10),
                CompactBlockSpliceSelection::FULL_BLOCK,
            )
            .await?,
        EncodedCompactBlockLookup::NotFound
    ));
    Ok(())
}

#[tokio::test]
async fn rocksdb_reader_reports_reader_invalidated_when_live_tail_table_file_is_compacted()
-> eyre::Result<()> {
    const TOTAL_BLOCKS: u32 = 80;
    let chain = build_mixed_parity_chain(TOTAL_BLOCKS)?;
    let activations = parity_activations();
    let baseline = CanonicalBaseline::new(&chain).with_settled_tip(BlockHeight::new(5));
    let mut writer = CanonicalWriterFixture::publish(&baseline, &activations)?;
    let canonical_path = writer.canonical_primary_path().to_path_buf();
    let reorg_policy = writer.canonical_primary()?.reorg_policy();
    drop(writer.take_canonical_primary()?);
    prepare_unpinned_split_live_tail(&canonical_path)?;

    let sec_dir = tempfile::tempdir()?;
    let mut tight_budget = zinder_store::RocksDbResourceBudget::for_local_tests();
    tight_budget.max_open_files = zinder_store::RocksDbResourceBudget::MIN_MAX_OPEN_FILES;
    let canonical_sec =
        zinder_store::RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
            &canonical_path,
            sec_dir.path(),
            NETWORK,
            zinder_store::CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::None),
            reorg_policy,
            tight_budget,
        )?;
    let reader = RocksDbCanonicalReader::new(canonical_sec)?;
    for h in 1_u32..=TOTAL_BLOCKS {
        let _ = reader.block_header_at(BlockHeight::new(h)).await?;
    }
    compact_away_live_tail_tables(&canonical_path).await?;

    for order in [HeightOrder::Ascending, HeightOrder::Descending] {
        for include_transparent in [false, true] {
            let range_err = reader
                .encoded_compact_block_range_up_to(
                    EncodedCompactBlockRangeRequest {
                        range: BlockHeightRange::inclusive(
                            BlockHeight::new(5),
                            BlockHeight::new(TOTAL_BLOCKS),
                        ),
                        order,
                        selection: ShieldedProjectionSelection::DEFAULT_SHIELDED,
                        include_transparent,
                        output: EncodedCompactBlockOutput::Blocks,
                        max_bytes: usize::MAX,
                        enforce_wire_fee_limit: true,
                    },
                    None,
                )
                .await
                .err()
                .ok_or_else(|| eyre::eyre!("expected range ReaderInvalidated"))?;
            assert!(matches!(
                range_err,
                StorageReadError::ReaderInvalidated { .. }
            ));
        }
    }
    assert_compacted_unary_reader_invalidated(&reader, TOTAL_BLOCKS).await?;
    drop(writer);
    Ok(())
}
