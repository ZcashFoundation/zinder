#![allow(
    missing_docs,
    reason = "Integration test names describe the compatibility behavior under test."
)]

use async_trait::async_trait;
use eyre::eyre;
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Code, Request, Status, transport::Server};
use zebra_chain::{
    parameters::NetworkKind as ZebraNetworkKind, transparent::Address as ZebraTransparentAddress,
};
use zinder_compat_lightwalletd::LightwalletdGrpcAdapter;
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, BlockId, BlockSelector, BroadcastDuplicate,
    BroadcastInvalidEncoding, BroadcastRejected, BroadcastRejectionReason, BroadcastUnknown,
    ChainEpochId, CommitmentTreeFrontiers, CompactBlockArtifact, CompactChainMetadata,
    CompactSaplingOutput, CompactSaplingSpend, CompactShieldedAction, CompactTransaction,
    CompactTransactionData, Network, NetworkUpgradeActivations, RawTransactionBytes,
    SUBTREE_LEAF_COUNT, ShieldedProtocol, SubtreeRootHash, SubtreeRootIndex, SubtreeRootRange,
    TransactionBroadcastOutcome, TransactionComponentCounts, TransactionId, TransactionLocation,
    TransparentAddressBalance, TransparentAddressScriptHash, TransparentOutPoint,
    TransparentOutputsByOutpointResponse, TransparentSpendFact,
    TransparentSpendsByOutpointResponse, TransparentUnspentOutput,
    TransparentUnspentOutputsByOutpointResponse, TransparentUtxoSetSummary,
    wire::encode_internal_block_hash,
};
use zinder_proto::compat::lightwalletd::{
    self, compact_tx_streamer_client::CompactTxStreamerClient,
    compact_tx_streamer_server::CompactTxStreamer,
};

use zinder_query::{
    BlockHeaderAtEpoch, BlockIdAtEpoch, ChainEvents, CompactBlock, CompactBlockRange,
    DEFAULT_MAX_COMPACT_BLOCK_RANGE, FullBlock, FullBlockStream, QueryError, RawTransaction,
    SettledTipBlock, SubtreeRoots, Transaction, TransactionStatus, TransparentAddressTxIds,
    TransparentAddressTxIdsInRangeRequest, TransparentAddressUnspentOutputs,
    TransparentAddressUnspentOutputsRequest, TreeState, VisibleTipBlock, WalletQueryApi,
    WalletServingPairSlot, WalletServingQuery, WalletServingReadPair,
};
use zinder_source::{
    NodeCapabilities, NodeCapability, NodeSource, SourceBlock, SourceError, SourceTreeState,
    TransactionBroadcaster, TreeStateUpstream,
};
use zinder_store::{
    CanonicalBuildSubtreeRoot, ChainEventStreamFamily, ChainEventStreamResume,
    EventStreamStartPosition, RawBlobRetention, StreamCursorTokenV1,
};
use zinder_testkit::{
    CanonicalBaseline, CanonicalWriterFixture, ChainFixture, FIXTURE_NOTE_COMMITMENT, FixtureBlock,
    FixtureTransactionRows, MockTransactionBroadcaster, WalletServingStoreFixture,
    completed_subtree_frontier, one_leaf_frontier, sample_regtest_upgrade_activations,
    synthetic_transaction_public_facts,
};

const ACCEPTANCE_BLOCK_HEIGHT: BlockHeight = BlockHeight::new(1);

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "Acceptance test keeps the read-sync RPC matrix together."
)]
async fn lightwalletd_adapter_serves_read_sync_methods() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);

    let visible_tip_block = adapter
        .get_latest_block(Request::new(lightwalletd::ChainSpec {}))
        .await?
        .into_inner();
    let block = adapter
        .get_block(Request::new(lightwalletd::BlockId {
            height: 1,
            hash: Vec::new(),
        }))
        .await?
        .into_inner();
    let block_range = adapter
        .get_block_range(Request::new(lightwalletd::BlockRange {
            start: Some(lightwalletd::BlockId {
                height: 1,
                hash: Vec::new(),
            }),
            end: Some(lightwalletd::BlockId {
                height: 1,
                hash: Vec::new(),
            }),
            pool_types: Vec::new(),
        }))
        .await?
        .into_inner();
    let ranged_blocks = collect_stream(block_range).await?;
    let tree_state = adapter
        .get_tree_state(Request::new(lightwalletd::BlockId {
            height: 1,
            hash: Vec::new(),
        }))
        .await?
        .into_inner();
    let latest_tree_state_checkpoint = adapter
        .get_latest_tree_state(Request::new(lightwalletd::Empty {}))
        .await?
        .into_inner();
    let subtree_roots = adapter
        .get_subtree_roots(Request::new(lightwalletd::GetSubtreeRootsArg {
            start_index: 0,
            shielded_protocol: lightwalletd::ShieldedProtocol::Sapling as i32,
            max_entries: 1,
        }))
        .await?
        .into_inner();
    let subtree_roots = collect_stream(subtree_roots).await?;
    let lightd_info = adapter
        .get_lightd_info(Request::new(lightwalletd::Empty {}))
        .await?
        .into_inner();

    assert_eq!(visible_tip_block.height, 1);
    assert_eq!(block.height, 1);
    assert_eq!(block.vtx.len(), 1);
    assert!(
        block.chain_metadata.is_some(),
        "GetBlock must retain commitment-tree sizes"
    );
    assert_eq!(ranged_blocks.len(), 1);
    assert_eq!(ranged_blocks[0].height, 1);
    assert_eq!(ranged_blocks[0].vtx[0].vin.len(), 0);
    assert_eq!(ranged_blocks[0].vtx[0].vout.len(), 0);
    assert!(
        ranged_blocks[0].chain_metadata.is_some(),
        "GetBlockRange must retain commitment-tree sizes"
    );
    assert_eq!(tree_state.height, 1);
    assert_eq!(latest_tree_state_checkpoint.height, tree_state.height);
    assert_eq!(latest_tree_state_checkpoint.hash, tree_state.hash);
    assert_eq!(
        latest_tree_state_checkpoint.sapling_tree,
        tree_state.sapling_tree
    );
    assert!(subtree_roots.is_empty());
    assert_eq!(lightd_info.vendor, "Zinder");
    // Regtest collapses to "test" per BIP70 (Zebra's NetworkKind::bip70_network_name).
    // Wallet SDKs match `chainName` against ID_TESTNET, which is "test".
    assert_eq!(lightd_info.chain_name, "test");
    assert_eq!(lightd_info.block_height, 1);
    assert_eq!(lightd_info.estimated_height, 1);
    assert_eq!(
        lightd_info.lightwallet_protocol_version,
        lightwalletd::LIGHTWALLETD_PROTOCOL_COMMIT
    );
    assert!(
        !lightd_info.taddr_support,
        "the generic adapter must not advertise transparent-address support"
    );
    // upgradeName and upgradeHeight name the next pending upgrade: in the
    // sample regtest table, NU5 activates at height 2.
    assert_eq!(lightd_info.upgrade_name, "NU5");
    assert_eq!(lightd_info.upgrade_height, 2);

    Ok(())
}

#[tokio::test]
async fn get_block_returns_lightwalletd_error_codes_for_invalid_selectors() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);

    let status = match adapter
        .get_block(Request::new(lightwalletd::BlockId {
            height: 2,
            hash: Vec::new(),
        }))
        .await
    {
        Ok(response) => return Err(eyre!("expected unknown-height error, got {response:?}")),
        Err(status) => status,
    };
    assert_eq!(status.code(), Code::OutOfRange);

    let status = match adapter
        .get_block(Request::new(lightwalletd::BlockId {
            height: 0,
            hash: Vec::new(),
        }))
        .await
    {
        Ok(response) => return Err(eyre!("expected height-zero error, got {response:?}")),
        Err(status) => status,
    };
    assert_eq!(status.code(), Code::InvalidArgument);

    let status = match adapter
        .get_block(Request::new(lightwalletd::BlockId {
            height: 1,
            hash: vec![0xff; 32],
        }))
        .await
    {
        Ok(response) => return Err(eyre!("expected hash-mismatch error, got {response:?}")),
        Err(status) => status,
    };
    assert_eq!(status.code(), Code::NotFound);

    let status = match adapter
        .get_block(Request::new(lightwalletd::BlockId {
            height: 0,
            hash: vec![0xff; 31],
        }))
        .await
    {
        Ok(response) => return Err(eyre!("expected malformed-hash error, got {response:?}")),
        Err(status) => status,
    };
    assert_eq!(status.code(), Code::InvalidArgument);

    Ok(())
}

#[tokio::test]
async fn get_block_nullifiers_reports_future_height_like_lightwalletd() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);

    let status = match adapter
        .get_block_nullifiers(Request::new(lightwalletd::BlockId {
            height: 2,
            hash: Vec::new(),
        }))
        .await
    {
        Ok(response) => return Err(eyre!("expected future-height error, got {response:?}")),
        Err(status) => status,
    };

    assert_eq!(status.code(), Code::OutOfRange);

    Ok(())
}

#[tokio::test]
async fn get_block_range_methods_report_future_height_like_lightwalletd() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);

    let request = || lightwalletd::BlockRange {
        start: Some(lightwalletd::BlockId {
            height: 1,
            hash: Vec::new(),
        }),
        end: Some(lightwalletd::BlockId {
            height: 2,
            hash: Vec::new(),
        }),
        pool_types: Vec::new(),
    };

    let Err(status) = adapter.get_block_range(Request::new(request())).await else {
        return Err(eyre!("expected future-height error, got a block stream"));
    };
    assert_eq!(status.code(), Code::OutOfRange);

    let Err(status) = adapter
        .get_block_range_nullifiers(Request::new(request()))
        .await
    else {
        return Err(eyre!("expected future-height error, got a block stream"));
    };
    assert_eq!(status.code(), Code::OutOfRange);

    Ok(())
}

/// lightwalletd puts no width bound on `BlockRange`, and the Zcash SDK clients
/// pointed at this surface rely on that. A window wider than one store read
/// must stream in full instead of failing the call.
#[tokio::test]
async fn get_block_range_streams_windows_wider_than_one_store_read() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let visible_tip_height = BlockHeight::new(2500);
    let recorder = EpochPinRecorder::new(store_fixture.query())
        .with_synthetic_compact_blocks_to(visible_tip_height);
    let adapter = LightwalletdGrpcAdapter::new(recorder.clone());

    let request = |start: u64, end: u64| lightwalletd::BlockRange {
        start: Some(lightwalletd::BlockId {
            height: start,
            hash: Vec::new(),
        }),
        end: Some(lightwalletd::BlockId {
            height: end,
            hash: Vec::new(),
        }),
        pool_types: Vec::new(),
    };

    let ascending = collect_stream(
        adapter
            .get_block_range(Request::new(request(1, 2500)))
            .await?
            .into_inner(),
    )
    .await?;
    assert_eq!(
        ascending
            .iter()
            .map(|block| block.height)
            .collect::<Vec<_>>(),
        (1..=2500).collect::<Vec<_>>()
    );
    assert_eq!(
        recorder.recorded_compact_block_ranges(),
        vec![
            BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(1000)),
            BlockHeightRange::inclusive(BlockHeight::new(1001), BlockHeight::new(2000)),
            BlockHeightRange::inclusive(BlockHeight::new(2001), BlockHeight::new(2500)),
        ],
        "each store read must stay within the compact-block range cap"
    );

    let descending = collect_stream(
        adapter
            .get_block_range(Request::new(request(2500, 1)))
            .await?
            .into_inner(),
    )
    .await?;
    assert_eq!(
        descending
            .iter()
            .map(|block| block.height)
            .collect::<Vec<_>>(),
        (1..=2500).rev().collect::<Vec<_>>(),
        "descending windows must stay monotonic across chunk edges"
    );

    Ok(())
}

#[tokio::test]
async fn get_block_range_rejects_malformed_requests_before_streaming() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);

    let Err(status) = adapter
        .get_block_range(Request::new(lightwalletd::BlockRange {
            start: None,
            end: Some(lightwalletd::BlockId {
                height: 1,
                hash: Vec::new(),
            }),
            pool_types: Vec::new(),
        }))
        .await
    else {
        return Err(eyre!("expected missing-start error, got a block stream"));
    };
    assert_eq!(status.code(), Code::InvalidArgument);

    let Err(status) = adapter
        .get_block_range(Request::new(lightwalletd::BlockRange {
            start: Some(lightwalletd::BlockId {
                height: 1,
                hash: Vec::new(),
            }),
            end: None,
            pool_types: Vec::new(),
        }))
        .await
    else {
        return Err(eyre!("expected missing-end error, got a block stream"));
    };
    assert_eq!(status.code(), Code::InvalidArgument);

    let Err(status) = adapter
        .get_block_range(Request::new(lightwalletd::BlockRange {
            start: Some(lightwalletd::BlockId {
                height: 1,
                hash: Vec::new(),
            }),
            end: Some(lightwalletd::BlockId {
                height: 1,
                hash: Vec::new(),
            }),
            pool_types: vec![999],
        }))
        .await
    else {
        return Err(eyre!("expected unknown-pool error, got a block stream"));
    };
    assert_eq!(status.code(), Code::InvalidArgument);

    Ok(())
}

/// Both nullifiers-only RPCs must omit everything the deprecated lightwalletd
/// contract excludes.
///
/// The redacted fields are block-level commitment-tree sizes
/// (`chain_metadata`), transparent inputs and outputs, Sapling outputs, and the
/// non-nullifier Orchard and Ironwood action fields. Only the shielded
/// nullifiers survive.
#[tokio::test]
async fn block_nullifiers_omit_commitment_tree_sizes_and_redact_non_nullifier_fields()
-> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);

    let single = adapter
        .get_block_nullifiers(Request::new(lightwalletd::BlockId {
            height: 1,
            hash: Vec::new(),
        }))
        .await?
        .into_inner();
    let ranged = adapter
        .get_block_range_nullifiers(Request::new(lightwalletd::BlockRange {
            start: Some(lightwalletd::BlockId {
                height: 1,
                hash: Vec::new(),
            }),
            end: Some(lightwalletd::BlockId {
                height: 1,
                hash: Vec::new(),
            }),
            pool_types: Vec::new(),
        }))
        .await?
        .into_inner();
    let ranged = collect_stream(ranged).await?;

    let ranged_block = ranged
        .first()
        .ok_or_else(|| eyre!("GetBlockRangeNullifiers must return the indexed block"))?;
    assert_nullifiers_only_redaction(&single)?;
    assert_nullifiers_only_redaction(ranged_block)?;

    Ok(())
}

/// Asserts one nullifiers-only compact block carries only shielded nullifiers
/// and has every excluded field cleared.
fn assert_nullifiers_only_redaction(block: &lightwalletd::CompactBlock) -> eyre::Result<()> {
    assert!(
        block.chain_metadata.is_none(),
        "nullifiers-only responses must omit commitment-tree sizes"
    );
    let transaction = block
        .vtx
        .first()
        .ok_or_else(|| eyre!("nullifiers-only block must retain the nullifier-bearing tx"))?;
    assert_eq!(
        transaction.vin.len(),
        0,
        "transparent inputs must be cleared"
    );
    assert_eq!(
        transaction.vout.len(),
        0,
        "transparent outputs must be cleared"
    );
    assert_eq!(
        transaction.outputs.len(),
        0,
        "Sapling outputs must be cleared"
    );
    assert_eq!(
        transaction.spends.len(),
        1,
        "Sapling spend nullifiers must survive"
    );
    assert_eq!(transaction.spends[0].nf, vec![3; 32]);
    let action = transaction
        .actions
        .first()
        .ok_or_else(|| eyre!("Orchard action must survive in nullifier-only form"))?;
    assert_eq!(
        action.nullifier,
        vec![9; 32],
        "Orchard nullifier must survive"
    );
    assert!(action.cmx.is_empty(), "Orchard cmx must be cleared");
    assert!(
        action.ephemeral_key.is_empty(),
        "Orchard ephemeralKey must be cleared"
    );
    assert!(
        action.ciphertext.is_empty(),
        "Orchard ciphertext must be cleared"
    );
    let ironwood_action = transaction
        .ironwood_actions
        .first()
        .ok_or_else(|| eyre!("Ironwood action must survive in nullifier-only form"))?;
    assert_eq!(
        ironwood_action.nullifier,
        vec![13; 32],
        "Ironwood nullifier must survive"
    );
    assert!(
        ironwood_action.cmx.is_empty(),
        "Ironwood cmx must be cleared"
    );
    assert!(
        ironwood_action.ephemeral_key.is_empty(),
        "Ironwood ephemeralKey must be cleared"
    );
    assert!(
        ironwood_action.ciphertext.is_empty(),
        "Ironwood ciphertext must be cleared"
    );
    Ok(())
}

/// Ironwood subtree-root requests are served like the other pools; an empty
/// store yields an empty stream rather than an error.
#[tokio::test]
async fn get_subtree_roots_serves_ironwood() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);

    let stream = adapter
        .get_subtree_roots(Request::new(lightwalletd::GetSubtreeRootsArg {
            start_index: 0,
            shielded_protocol: lightwalletd::ShieldedProtocol::Ironwood as i32,
            max_entries: 1,
        }))
        .await?
        .into_inner();
    let subtree_roots = collect_stream(stream).await?;
    assert!(
        subtree_roots.is_empty(),
        "an empty store must yield an empty ironwood subtree-root stream"
    );

    Ok(())
}

/// Records the `at_epoch_id` argument of every canonical read so a test can
/// assert one handler pins all of its reads to a single chain epoch.
#[derive(Clone)]
struct EpochPinRecorder<Inner> {
    inner: Inner,
    recorded_epoch_ids: Arc<Mutex<Vec<Option<ChainEpochId>>>>,
    recorded_compact_block_ranges: Arc<Mutex<Vec<BlockHeightRange>>>,
    transparent_address_balance: Option<TransparentAddressBalance>,
    synthetic_visible_tip_height: Option<BlockHeight>,
}

impl<Inner> EpochPinRecorder<Inner> {
    fn new(inner: Inner) -> Self {
        Self {
            inner,
            recorded_epoch_ids: Arc::new(Mutex::new(Vec::new())),
            recorded_compact_block_ranges: Arc::new(Mutex::new(Vec::new())),
            transparent_address_balance: None,
            synthetic_visible_tip_height: None,
        }
    }

    fn with_transparent_address_balance(
        mut self,
        transparent_address_balance: TransparentAddressBalance,
    ) -> Self {
        self.transparent_address_balance = Some(transparent_address_balance);
        self
    }

    /// Reports `visible_tip_height` as the visible tip and answers every
    /// compact-block range from synthetic blocks, so a test can span more
    /// heights than a store fixture holds.
    fn with_synthetic_compact_blocks_to(mut self, visible_tip_height: BlockHeight) -> Self {
        self.synthetic_visible_tip_height = Some(visible_tip_height);
        self
    }

    fn record(&self, at_epoch_id: Option<ChainEpochId>) {
        self.recorded_epoch_ids.lock().push(at_epoch_id);
    }

    fn recorded_epoch_ids(&self) -> Vec<Option<ChainEpochId>> {
        self.recorded_epoch_ids.lock().clone()
    }

    fn recorded_compact_block_ranges(&self) -> Vec<BlockHeightRange> {
        self.recorded_compact_block_ranges.lock().clone()
    }
}

fn synthetic_compact_block(height: BlockHeight) -> CompactBlockArtifact {
    let block_hash = |height: u32| {
        let mut bytes = [0_u8; 32];
        bytes[..4].copy_from_slice(&height.to_le_bytes());
        BlockHash::from_bytes(bytes)
    };
    CompactBlockArtifact::empty(
        BlockId::new(height, block_hash(height.value())),
        block_hash(height.value().saturating_sub(1)),
        0,
        CompactChainMetadata {
            sapling_commitment_tree_size: 0,
            orchard_commitment_tree_size: 0,
            ironwood_commitment_tree_size: 0,
        },
    )
}

#[async_trait]
impl<Inner: WalletQueryApi + Clone> WalletQueryApi for EpochPinRecorder<Inner> {
    fn native_endpoint_capabilities(&self) -> &zinder_query::NativeWalletEndpointCapabilities {
        self.inner.native_endpoint_capabilities()
    }

    fn upstream_node_capabilities(&self) -> Option<&zinder_query::UpstreamNodeCapabilities> {
        self.inner.upstream_node_capabilities()
    }

    async fn network_upgrade_activations(&self) -> Result<NetworkUpgradeActivations, QueryError> {
        self.inner.network_upgrade_activations().await
    }

    async fn visible_tip_block(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<VisibleTipBlock, QueryError> {
        let mut visible_tip_block = self.inner.visible_tip_block(at_epoch_id).await?;
        if let Some(height) = self.synthetic_visible_tip_height {
            visible_tip_block.height = height;
            visible_tip_block.chain_epoch.visible_tip_height = height;
        }
        Ok(visible_tip_block)
    }

    async fn settled_tip_block(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<SettledTipBlock, QueryError> {
        self.record(at_epoch_id);
        self.inner.settled_tip_block(at_epoch_id).await
    }

    async fn block_id_by_selector(
        &self,
        selector: BlockSelector,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<BlockIdAtEpoch, QueryError> {
        self.record(at_epoch_id);
        self.inner.block_id_by_selector(selector, at_epoch_id).await
    }

    async fn block_header_by_selector(
        &self,
        selector: BlockSelector,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<BlockHeaderAtEpoch, QueryError> {
        self.record(at_epoch_id);
        self.inner
            .block_header_by_selector(selector, at_epoch_id)
            .await
    }

    async fn compact_block_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlock, QueryError> {
        self.record(at_epoch_id);
        self.inner.compact_block_at(height, at_epoch_id).await
    }

    async fn compact_blocks_in_range(
        &self,
        block_range: BlockHeightRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlockRange, QueryError> {
        self.record(at_epoch_id);
        self.recorded_compact_block_ranges.lock().push(block_range);
        if self.synthetic_visible_tip_height.is_some() {
            let requested = block_range.into_iter().len();
            let maximum =
                usize::try_from(DEFAULT_MAX_COMPACT_BLOCK_RANGE.get()).unwrap_or(usize::MAX);
            if requested > maximum {
                return Err(QueryError::BlockRangeTooLarge { requested, maximum });
            }
            return Ok(CompactBlockRange {
                chain_epoch: self.visible_tip_block(at_epoch_id).await?.chain_epoch,
                block_range,
                compact_blocks: block_range
                    .into_iter()
                    .map(synthetic_compact_block)
                    .collect(),
            });
        }
        self.inner
            .compact_blocks_in_range(block_range, at_epoch_id)
            .await
    }

    async fn full_block_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<FullBlock, QueryError> {
        self.record(at_epoch_id);
        self.inner.full_block_at(height, at_epoch_id).await
    }

    async fn full_blocks_in_range(
        &self,
        block_range: BlockHeightRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<FullBlockStream, QueryError> {
        self.record(at_epoch_id);
        self.inner
            .full_blocks_in_range(block_range, at_epoch_id)
            .await
    }

    async fn transaction(
        &self,
        transaction_id: TransactionId,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransactionStatus, QueryError> {
        self.record(at_epoch_id);
        self.inner.transaction(transaction_id, at_epoch_id).await
    }

    async fn transaction_at_block_index(
        &self,
        height: BlockHeight,
        tx_index: u64,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<Transaction, QueryError> {
        self.record(at_epoch_id);
        self.inner
            .transaction_at_block_index(height, tx_index, at_epoch_id)
            .await
    }

    async fn raw_transaction(
        &self,
        transaction_id: TransactionId,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<RawTransaction, QueryError> {
        self.record(at_epoch_id);
        self.inner
            .raw_transaction(transaction_id, at_epoch_id)
            .await
    }

    async fn transparent_outputs_by_outpoint(
        &self,
        outpoints: Vec<TransparentOutPoint>,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentOutputsByOutpointResponse, QueryError> {
        self.record(at_epoch_id);
        self.inner
            .transparent_outputs_by_outpoint(outpoints, at_epoch_id)
            .await
    }

    async fn transparent_spends_by_outpoint(
        &self,
        outpoints: Vec<TransparentOutPoint>,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentSpendsByOutpointResponse, QueryError> {
        self.record(at_epoch_id);
        self.inner
            .transparent_spends_by_outpoint(outpoints, at_epoch_id)
            .await
    }

    async fn transparent_unspent_outputs_by_outpoint(
        &self,
        outpoints: Vec<TransparentOutPoint>,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentUnspentOutputsByOutpointResponse, QueryError> {
        self.record(at_epoch_id);
        self.inner
            .transparent_unspent_outputs_by_outpoint(outpoints, at_epoch_id)
            .await
    }

    async fn transparent_address_unspent_outputs(
        &self,
        request: TransparentAddressUnspentOutputsRequest,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressUnspentOutputs, QueryError> {
        self.record(at_epoch_id);
        self.inner
            .transparent_address_unspent_outputs(request, at_epoch_id)
            .await
    }

    async fn transparent_address_tx_ids_in_range(
        &self,
        request: TransparentAddressTxIdsInRangeRequest,
    ) -> Result<TransparentAddressTxIds, QueryError> {
        self.inner
            .transparent_address_tx_ids_in_range(request)
            .await
    }

    async fn transparent_address_balance(
        &self,
        addresses: Vec<TransparentAddressScriptHash>,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressBalance, QueryError> {
        self.record(at_epoch_id);
        if let Some(transparent_address_balance) = self.transparent_address_balance {
            return Ok(transparent_address_balance);
        }
        self.inner
            .transparent_address_balance(addresses, at_epoch_id)
            .await
    }

    async fn transparent_utxo_set_summary(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentUtxoSetSummary, QueryError> {
        self.record(at_epoch_id);
        self.inner.transparent_utxo_set_summary(at_epoch_id).await
    }

    async fn tree_state_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TreeState, QueryError> {
        self.record(at_epoch_id);
        self.inner.tree_state_at(height, at_epoch_id).await
    }

    async fn latest_tree_state_checkpoint(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TreeState, QueryError> {
        self.record(at_epoch_id);
        self.inner.latest_tree_state_checkpoint(at_epoch_id).await
    }

    async fn subtree_roots(
        &self,
        subtree_root_range: SubtreeRootRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<SubtreeRoots, QueryError> {
        self.record(at_epoch_id);
        self.inner
            .subtree_roots(subtree_root_range, at_epoch_id)
            .await
    }

    async fn chain_events(
        &self,
        from_cursor: Option<StreamCursorTokenV1>,
        family: ChainEventStreamFamily,
    ) -> Result<ChainEvents, QueryError> {
        self.inner.chain_events(from_cursor, family).await
    }

    async fn resolve_chain_events_start(
        &self,
        start: EventStreamStartPosition,
        requested_family: ChainEventStreamFamily,
    ) -> Result<ChainEventStreamResume, QueryError> {
        self.inner
            .resolve_chain_events_start(start, requested_family)
            .await
    }

    async fn broadcast_transaction(
        &self,
        raw_transaction: RawTransactionBytes,
    ) -> Result<TransactionBroadcastOutcome, QueryError> {
        self.inner.broadcast_transaction(raw_transaction).await
    }
}

/// The by-hash `GetTransaction` path issues two canonical reads.
///
/// The status lookup and the raw-transaction fetch must pin to the same chain
/// epoch so the response cannot mix two chain states across a tip move.
#[tokio::test]
async fn get_transaction_by_hash_pins_every_read_to_one_epoch() -> eyre::Result<()> {
    let transaction_id = TransactionId::from_bytes([0x77; 32]);
    let transaction_payload = b"single-epoch-transaction-bytes".to_vec();
    let store_fixture =
        acceptance_store_fixture_with_transaction_rows(RawBlobRetention::Transactions, |block| {
            vec![FixtureTransactionRows::from_raw_transaction(
                transaction_id,
                block.height,
                block.hash,
                0,
                transaction_payload.clone(),
            )]
        })?;
    let recorder = EpochPinRecorder::new(store_fixture.query());
    let adapter = LightwalletdGrpcAdapter::new(recorder.clone());

    let transaction = adapter
        .get_transaction(Request::new(lightwalletd::TxFilter {
            block: None,
            index: 0,
            hash: transaction_id.as_bytes().to_vec(),
        }))
        .await?
        .into_inner();

    assert_eq!(transaction.data, transaction_payload);
    let recorded_pins = recorder.recorded_epoch_ids();
    assert_eq!(
        recorded_pins.len(),
        2,
        "the by-hash path issues two canonical reads"
    );
    let entry_epoch_id = recorded_pins
        .first()
        .copied()
        .flatten()
        .ok_or_else(|| eyre!("the first canonical read must be pinned to an epoch"))?;
    assert!(
        recorded_pins.iter().all(|pin| *pin == Some(entry_epoch_id)),
        "every read in one handler must pin the same epoch; recorded {recorded_pins:?}"
    );

    Ok(())
}

#[tokio::test]
async fn tree_state_reports_future_height_like_lightwalletd() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);

    let status = match adapter
        .get_tree_state(Request::new(lightwalletd::BlockId {
            height: 2,
            hash: Vec::new(),
        }))
        .await
    {
        Ok(response) => return Err(eyre!("expected future-height error, got {response:?}")),
        Err(status) => status,
    };

    assert_eq!(status.code(), Code::InvalidArgument);

    Ok(())
}

/// Block-order index of the spending transaction in the UTXO stream fixture.
/// The spend must follow both creating transactions, which the fixture places
/// at indices 0 and 1.
const SPENDING_TRANSACTION_INDEX: u32 = 2;

#[tokio::test]
async fn get_address_utxos_stream_returns_indexed_unspent_transparent_outputs() -> eyre::Result<()>
{
    let transparent_address =
        ZebraTransparentAddress::from_pub_key_hash(ZebraNetworkKind::Regtest, [0x11; 20]);
    let address = transparent_address.to_string();
    let script_pub_key = transparent_address.script().as_raw_bytes().to_vec();
    let transaction_id = TransactionId::from_bytes([0x55; 32]);
    let spent_transaction_id = TransactionId::from_bytes([0x66; 32]);
    let spending_transaction_id = TransactionId::from_bytes([0x77; 32]);
    let store_fixture = acceptance_store_fixture_with_transaction_rows_and_transparent(
        RawBlobRetention::None,
        |_| Vec::new(),
        |block| {
            let unspent_outpoint = TransparentOutPoint::new(transaction_id, 0);
            let spent_outpoint = TransparentOutPoint::new(spent_transaction_id, 0);
            (
                vec![
                    TransparentUnspentOutput::new(
                        TransparentAddressScriptHash::of_script_pub_key(&script_pub_key),
                        script_pub_key.clone(),
                        unspent_outpoint,
                        12,
                        block.height,
                        block.hash,
                    ),
                    TransparentUnspentOutput::new(
                        TransparentAddressScriptHash::of_script_pub_key(&script_pub_key),
                        script_pub_key.clone(),
                        spent_outpoint,
                        13,
                        block.height,
                        block.hash,
                    ),
                ],
                vec![TransparentSpendFact::new(
                    spent_outpoint,
                    0,
                    spending_transaction_id,
                    SPENDING_TRANSACTION_INDEX,
                    block.height,
                    block.hash,
                    13,
                    TransparentAddressScriptHash::of_script_pub_key(&script_pub_key),
                    block.height,
                    block.hash,
                )],
            )
        },
    )?;
    let adapter = acceptance_adapter(&store_fixture);

    let request = lightwalletd::GetAddressUtxosArg {
        addresses: vec![address.clone()],
        start_height: 1,
        max_entries: 10,
    };
    let list_response = adapter
        .get_address_utxos(Request::new(request.clone()))
        .await?
        .into_inner();
    let stream_response = adapter
        .get_address_utxos_stream(Request::new(request))
        .await?
        .into_inner();
    let streamed_utxos = collect_stream(stream_response).await?;

    assert_eq!(list_response.address_utxos, streamed_utxos);
    assert_eq!(streamed_utxos.len(), 1);
    assert_eq!(streamed_utxos[0].address, address);
    assert_eq!(streamed_utxos[0].txid, transaction_id.as_bytes().to_vec());
    assert_eq!(streamed_utxos[0].index, 0);
    assert_eq!(streamed_utxos[0].script, script_pub_key);
    assert_eq!(streamed_utxos[0].value_zat, 12);
    assert_eq!(streamed_utxos[0].height, 1);

    Ok(())
}

#[tokio::test]
async fn get_address_utxos_honors_start_height_floor() -> eyre::Result<()> {
    let transparent_address =
        ZebraTransparentAddress::from_pub_key_hash(ZebraNetworkKind::Regtest, [0x12; 20]);
    let address = transparent_address.to_string();
    let script_pub_key = transparent_address.script().as_raw_bytes().to_vec();
    let address_script_hash = TransparentAddressScriptHash::of_script_pub_key(&script_pub_key);
    let before_floor_transaction_id = TransactionId::from_bytes([0x12; 32]);
    let at_floor_transaction_id = TransactionId::from_bytes([0x13; 32]);
    let chain_fixture = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let before_floor_block = chain_fixture
        .block_at(BlockHeight::new(1))
        .ok_or_else(|| eyre!("start-height fixture must include height 1"))?
        .clone();
    let at_floor_block = chain_fixture
        .block_at(BlockHeight::new(2))
        .ok_or_else(|| eyre!("start-height fixture must include height 2"))?
        .clone();
    let chain_fixture = chain_fixture
        .with_address_output_index(TransparentUnspentOutput::new(
            address_script_hash,
            script_pub_key.clone(),
            TransparentOutPoint::new(before_floor_transaction_id, 0),
            12,
            before_floor_block.height,
            before_floor_block.hash,
        ))
        .with_address_output_index(TransparentUnspentOutput::new(
            address_script_hash,
            script_pub_key,
            TransparentOutPoint::new(at_floor_transaction_id, 0),
            13,
            at_floor_block.height,
            at_floor_block.hash,
        ));
    let store_fixture = acceptance_store_from_chain(&chain_fixture)?;
    let adapter = acceptance_adapter(&store_fixture);
    let request = lightwalletd::GetAddressUtxosArg {
        addresses: vec![address],
        start_height: 2,
        max_entries: 10,
    };

    let list_response = adapter
        .get_address_utxos(Request::new(request.clone()))
        .await?
        .into_inner();
    let stream_response = adapter
        .get_address_utxos_stream(Request::new(request))
        .await?
        .into_inner();
    let streamed_utxos = collect_stream(stream_response).await?;

    assert_eq!(list_response.address_utxos, streamed_utxos);
    assert_eq!(streamed_utxos.len(), 1);
    assert_eq!(streamed_utxos[0].height, 2);
    assert_eq!(streamed_utxos[0].txid, at_floor_transaction_id.as_bytes());

    Ok(())
}

/// Regression: txid bytes emitted by `GetAddressUtxos` must be accepted verbatim
/// by `GetTransaction(TxFilter { hash, ... })`.
///
/// The output and input use the same little-endian txid bytes; reversing the
/// input breaks this round trip. Lightwalletd-go documents the wire contract at
/// `frontend/service.go:792`: txid `bytes` fields are Zcash internal
/// little-endian, the same byte order [`TransactionId::as_bytes`] returns.
#[tokio::test]
async fn get_address_utxos_txid_round_trips_through_get_transaction_by_hash() -> eyre::Result<()> {
    let transparent_address =
        ZebraTransparentAddress::from_pub_key_hash(ZebraNetworkKind::Regtest, [0x44; 20]);
    let address = transparent_address.to_string();
    let script_pub_key = transparent_address.script().as_raw_bytes().to_vec();
    let transaction_id = TransactionId::from_bytes([0x77; 32]);
    let transaction_payload = b"round-trip-transaction-bytes".to_vec();
    let store_fixture = acceptance_store_fixture_with_transaction_rows_and_transparent(
        RawBlobRetention::Transactions,
        |block| {
            vec![FixtureTransactionRows::from_raw_transaction(
                transaction_id,
                block.height,
                block.hash,
                0,
                transaction_payload.clone(),
            )]
        },
        |block| {
            (
                vec![TransparentUnspentOutput::new(
                    TransparentAddressScriptHash::of_script_pub_key(&script_pub_key),
                    script_pub_key.clone(),
                    TransparentOutPoint::new(transaction_id, 0),
                    21,
                    block.height,
                    block.hash,
                )],
                Vec::new(),
            )
        },
    )?;
    let adapter = acceptance_adapter(&store_fixture);

    let utxos = adapter
        .get_address_utxos(Request::new(lightwalletd::GetAddressUtxosArg {
            addresses: vec![address],
            start_height: 1,
            max_entries: 10,
        }))
        .await?
        .into_inner();
    let utxo = utxos
        .address_utxos
        .first()
        .ok_or_else(|| eyre!("expected one UTXO in the round-trip fixture"))?;

    let transaction = adapter
        .get_transaction(Request::new(lightwalletd::TxFilter {
            block: None,
            index: 0,
            hash: utxo.txid.clone(),
        }))
        .await?
        .into_inner();

    assert_eq!(transaction.height, 1);
    assert_eq!(transaction.data, transaction_payload);

    Ok(())
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the acceptance scenario keeps multi-address ordering and truncation assertions together"
)]
async fn get_address_utxos_applies_max_entries_across_address_set() -> eyre::Result<()> {
    let transparent_address_a =
        ZebraTransparentAddress::from_pub_key_hash(ZebraNetworkKind::Regtest, [0x21; 20]);
    let transparent_address_b =
        ZebraTransparentAddress::from_pub_key_hash(ZebraNetworkKind::Regtest, [0x22; 20]);
    let address_a = transparent_address_a.to_string();
    let address_b = transparent_address_b.to_string();
    let script_pub_key_a = transparent_address_a.script().as_raw_bytes().to_vec();
    let script_pub_key_b = transparent_address_b.script().as_raw_bytes().to_vec();
    let first_transaction_id = TransactionId::from_bytes([0x10; 32]);
    let second_transaction_id = TransactionId::from_bytes([0x20; 32]);
    let truncated_transaction_id = TransactionId::from_bytes([0x30; 32]);
    let store_fixture = acceptance_store_fixture_with_transaction_rows_and_transparent(
        RawBlobRetention::None,
        |_| Vec::new(),
        |block| {
            (
                vec![
                    TransparentUnspentOutput::new(
                        TransparentAddressScriptHash::of_script_pub_key(&script_pub_key_a),
                        script_pub_key_a.clone(),
                        TransparentOutPoint::new(truncated_transaction_id, 0),
                        30,
                        block.height,
                        block.hash,
                    ),
                    TransparentUnspentOutput::new(
                        TransparentAddressScriptHash::of_script_pub_key(&script_pub_key_b),
                        script_pub_key_b.clone(),
                        TransparentOutPoint::new(first_transaction_id, 0),
                        10,
                        block.height,
                        block.hash,
                    ),
                    TransparentUnspentOutput::new(
                        TransparentAddressScriptHash::of_script_pub_key(&script_pub_key_b),
                        script_pub_key_b.clone(),
                        TransparentOutPoint::new(second_transaction_id, 0),
                        20,
                        block.height,
                        block.hash,
                    ),
                ],
                Vec::new(),
            )
        },
    )?;
    let adapter = LightwalletdGrpcAdapter::new(store_fixture.query());

    let request = lightwalletd::GetAddressUtxosArg {
        addresses: vec![address_a, address_b.clone()],
        start_height: 1,
        max_entries: 2,
    };
    let list_response = adapter
        .get_address_utxos(Request::new(request.clone()))
        .await?
        .into_inner();
    let stream_response = adapter
        .get_address_utxos_stream(Request::new(request))
        .await?
        .into_inner();
    let streamed_utxos = collect_stream(stream_response).await?;
    let returned_txids: Vec<_> = streamed_utxos
        .iter()
        .map(|utxo| utxo.txid.clone())
        .collect();

    assert_eq!(list_response.address_utxos, streamed_utxos);
    assert_eq!(streamed_utxos.len(), 2);
    assert_eq!(
        returned_txids,
        vec![
            first_transaction_id.as_bytes().to_vec(),
            second_transaction_id.as_bytes().to_vec(),
        ]
    );
    assert!(streamed_utxos.iter().all(|utxo| utxo.address == address_b));

    Ok(())
}

#[tokio::test]
async fn get_transaction_returns_not_found_when_blob_is_unretained() -> eyre::Result<()> {
    let transaction_id = TransactionId::from_bytes([0x32; 32]);
    let store_fixture =
        acceptance_store_fixture_with_transaction_rows(RawBlobRetention::None, |block| {
            vec![transaction_rows_without_blob(transaction_id, block)]
        })?;
    let adapter = acceptance_adapter(&store_fixture);

    let by_hash_status = match adapter
        .get_transaction(Request::new(lightwalletd::TxFilter {
            block: None,
            index: 0,
            hash: transaction_id.as_bytes().to_vec(),
        }))
        .await
    {
        Ok(response) => return Err(eyre!("expected missing raw blob, got {response:?}")),
        Err(status) => status,
    };
    assert_eq!(by_hash_status.code(), Code::NotFound);

    Ok(())
}

#[tokio::test]
async fn send_transaction_forwards_accepted_to_zero_error_code() -> eyre::Result<()> {
    let transaction_id = TransactionId::from_bytes([0x42; 32]);
    let broadcaster = MockTransactionBroadcaster::accepted(transaction_id);
    let (adapter, _store_fixture) = broadcasting_acceptance_adapter(broadcaster)?;

    let response = adapter
        .send_transaction(Request::new(lightwalletd::RawTransaction {
            data: vec![0xde, 0xad, 0xbe, 0xef],
            height: 0,
        }))
        .await?
        .into_inner();

    assert_eq!(response.error_code, 0);
    let mut expected_id = transaction_id.as_bytes();
    expected_id.reverse();
    assert_eq!(response.error_message, hex::encode(expected_id));

    Ok(())
}

#[tokio::test]
async fn send_transaction_maps_invalid_encoding_to_minus_22() -> eyre::Result<()> {
    let broadcaster = MockTransactionBroadcaster::returning(
        TransactionBroadcastOutcome::InvalidEncoding(BroadcastInvalidEncoding {
            error_code: None,
            message: "TX decode failed".to_owned(),
        }),
    );
    let (adapter, _store_fixture) = broadcasting_acceptance_adapter(broadcaster)?;

    let response = adapter
        .send_transaction(Request::new(lightwalletd::RawTransaction {
            data: vec![0xff],
            height: 0,
        }))
        .await?
        .into_inner();

    assert_eq!(response.error_code, -22);
    assert_eq!(response.error_message, "TX decode failed");

    Ok(())
}

#[tokio::test]
async fn send_transaction_maps_duplicate_and_rejected_codes() -> eyre::Result<()> {
    let cases = [
        (
            TransactionBroadcastOutcome::Duplicate(BroadcastDuplicate {
                error_code: None,
                message: "transaction already in mempool".to_owned(),
            }),
            -27,
            "transaction already in mempool",
        ),
        (
            TransactionBroadcastOutcome::Rejected(BroadcastRejected {
                kind: BroadcastRejectionReason::Unknown,
                error_code: None,
                message: "bad-txns-invalid".to_owned(),
            }),
            -26,
            "bad-txns-invalid",
        ),
        (
            TransactionBroadcastOutcome::Unknown(BroadcastUnknown {
                error_code: None,
                message: "node returned unclassified".to_owned(),
            }),
            -1,
            "node returned unclassified",
        ),
    ];

    for (broadcast_outcome, expected_code, expected_message) in cases {
        let broadcaster = MockTransactionBroadcaster::returning(broadcast_outcome);
        let (adapter, _store_fixture) = broadcasting_acceptance_adapter(broadcaster)?;

        let response = adapter
            .send_transaction(Request::new(lightwalletd::RawTransaction {
                data: vec![0x00],
                height: 0,
            }))
            .await?
            .into_inner();

        assert_eq!(response.error_code, expected_code);
        assert_eq!(response.error_message, expected_message);
    }

    Ok(())
}

#[tokio::test]
async fn send_transaction_forwards_node_error_code_when_present() -> eyre::Result<()> {
    let broadcaster = MockTransactionBroadcaster::returning(TransactionBroadcastOutcome::Rejected(
        BroadcastRejected {
            kind: BroadcastRejectionReason::Unknown,
            error_code: Some(-25),
            message: "missing-input".to_owned(),
        },
    ));
    let (adapter, _store_fixture) = broadcasting_acceptance_adapter(broadcaster)?;

    let response = adapter
        .send_transaction(Request::new(lightwalletd::RawTransaction {
            data: vec![0x00],
            height: 0,
        }))
        .await?
        .into_inner();

    assert_eq!(response.error_code, -25);
    assert_eq!(response.error_message, "missing-input");

    Ok(())
}

#[tokio::test]
async fn send_transaction_reports_broadcast_disabled() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);

    let status = match adapter
        .send_transaction(Request::new(lightwalletd::RawTransaction {
            data: vec![0x00],
            height: 0,
        }))
        .await
    {
        Ok(response) => {
            return Err(eyre!("expected disabled error, got {response:?}"));
        }
        Err(status) => status,
    };

    assert_eq!(status.code(), Code::FailedPrecondition);

    Ok(())
}

#[tokio::test]
async fn taddress_balance_projects_native_delta_for_generated_lightwalletd_clients()
-> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let wallet_query = store_fixture.query();
    let visible_tip_block = wallet_query.visible_tip_block(None).await?;
    let query_api = EpochPinRecorder::new(wallet_query).with_transparent_address_balance(
        TransparentAddressBalance {
            confirmed_zat: 1_000,
            unconfirmed_delta_zat: -375,
            address_count: 1,
            chain_epoch: visible_tip_block.chain_epoch,
        },
    );
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let server_addr = listener.local_addr()?;
    let adapter = LightwalletdGrpcAdapter::new(query_api).into_server();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(adapter)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _ = shutdown_rx.await;
            })
            .await
    });

    {
        let mut client = CompactTxStreamerClient::connect(format!("http://{server_addr}")).await?;
        let transparent_address =
            ZebraTransparentAddress::from_pub_key_hash(ZebraNetworkKind::Regtest, [0x41; 20]);
        let address = transparent_address.to_string();
        let unary_balance = client
            .get_taddress_balance(lightwalletd::AddressList {
                addresses: vec![address.clone()],
            })
            .await?
            .into_inner();
        let stream_balance = client
            .get_taddress_balance_stream(tokio_stream::iter(vec![lightwalletd::Address {
                address,
            }]))
            .await?
            .into_inner();

        assert_eq!(unary_balance.value_zat, 625);
        assert_eq!(unary_balance, stream_balance);
    }

    let _ = shutdown_tx.send(());
    server_handle.await??;
    Ok(())
}

#[tokio::test]
async fn generated_lightwalletd_client_streams_over_grpc_transport() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let server_addr = listener.local_addr()?;
    let adapter = acceptance_adapter(&store_fixture).into_server();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(adapter)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _ = shutdown_rx.await;
            })
            .await
    });

    {
        let mut client = CompactTxStreamerClient::connect(format!("http://{server_addr}")).await?;
        let visible_tip_block = client
            .get_latest_block(lightwalletd::ChainSpec {})
            .await?
            .into_inner();
        let mut compact_blocks = client
            .get_block_range(lightwalletd::BlockRange {
                start: Some(lightwalletd::BlockId {
                    height: visible_tip_block.height,
                    hash: Vec::new(),
                }),
                end: Some(lightwalletd::BlockId {
                    height: visible_tip_block.height,
                    hash: Vec::new(),
                }),
                pool_types: Vec::new(),
            })
            .await?
            .into_inner();
        let tree_state = client
            .get_latest_tree_state(lightwalletd::Empty {})
            .await?
            .into_inner();

        let compact_block = compact_blocks
            .message()
            .await?
            .ok_or_else(|| eyre!("missing compact block from compatibility stream"))?;

        assert!(compact_blocks.message().await?.is_none());
        assert_eq!(visible_tip_block.height, 1);
        assert_eq!(compact_block.height, visible_tip_block.height);
        assert_eq!(tree_state.height, visible_tip_block.height);
    }

    let _ = shutdown_tx.send(());
    server_handle.await??;
    Ok(())
}

#[tokio::test]
async fn tree_state_reports_missing_non_empty_final_state_as_data_loss() -> eyre::Result<()> {
    let store_fixture = sparse_tree_state_fixture(
        br#"{"hash":"010101","height":1,"time":1296694002,"sapling":{"commitments":{"size":1}},"orchard":{"commitments":{}}}"#
            .to_vec(),
    )?;
    let adapter = acceptance_adapter(&store_fixture);

    let status = match adapter
        .get_tree_state(Request::new(lightwalletd::BlockId {
            height: 1,
            hash: Vec::new(),
        }))
        .await
    {
        Ok(response) => {
            return Err(eyre!(
                "expected malformed tree-state error, got {response:?}"
            ));
        }
        Err(status) => status,
    };

    assert_eq!(status.code(), Code::DataLoss);

    Ok(())
}

#[tokio::test]
async fn tree_state_rejects_zero_empty_block_id_without_reader_fallback() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);

    let status = match adapter
        .get_tree_state(Request::new(lightwalletd::BlockId {
            height: 0,
            hash: Vec::new(),
        }))
        .await
    {
        Ok(response) => return Err(eyre!("expected block 0 error, got {response:?}")),
        Err(status) => status,
    };

    assert_eq!(status.code(), Code::InvalidArgument);

    Ok(())
}

#[tokio::test]
async fn tree_state_treats_absent_pool_and_empty_commitments_as_empty() -> eyre::Result<()> {
    let store_fixture = sparse_tree_state_fixture(
        br#"{"hash":"010101","height":1,"time":1296694002,"orchard":{"commitments":{}}}"#.to_vec(),
    )?;
    let adapter = acceptance_adapter(&store_fixture);

    let tree_state = adapter
        .get_tree_state(Request::new(lightwalletd::BlockId {
            height: 1,
            hash: Vec::new(),
        }))
        .await?
        .into_inner();

    assert_eq!(tree_state.sapling_tree, "");
    assert_eq!(tree_state.orchard_tree, "");
    assert_eq!(tree_state.ironwood_tree, "");

    Ok(())
}

#[tokio::test]
async fn tree_state_maps_ironwood_pool_final_state() -> eyre::Result<()> {
    let store_fixture = sparse_tree_state_fixture(
        br#"{"hash":"010101","height":1,"time":1296694002,"sapling":{"commitments":{}},"orchard":{"commitments":{}},"ironwood":{"commitments":{"finalState":"aabbcc"}}}"#
            .to_vec(),
    )?;
    let adapter = acceptance_adapter(&store_fixture);

    let tree_state = adapter
        .get_tree_state(Request::new(lightwalletd::BlockId {
            height: 1,
            hash: Vec::new(),
        }))
        .await?
        .into_inner();

    assert_eq!(tree_state.ironwood_tree, "aabbcc");

    Ok(())
}

#[tokio::test]
async fn tree_state_reports_wrong_pool_shape_as_data_loss() -> eyre::Result<()> {
    let store_fixture = sparse_tree_state_fixture(
        br#"{"hash":"010101","height":1,"time":1296694002,"sapling":[]}"#.to_vec(),
    )?;
    let adapter = acceptance_adapter(&store_fixture);

    let status = match adapter
        .get_tree_state(Request::new(lightwalletd::BlockId {
            height: 1,
            hash: Vec::new(),
        }))
        .await
    {
        Ok(response) => {
            return Err(eyre!("expected malformed pool error, got {response:?}"));
        }
        Err(status) => status,
    };

    assert_eq!(status.code(), Code::DataLoss);

    Ok(())
}

#[tokio::test]
async fn tree_state_reports_wrong_commitments_shape_as_data_loss() -> eyre::Result<()> {
    let store_fixture = sparse_tree_state_fixture(
        br#"{"hash":"010101","height":1,"time":1296694002,"sapling":{"commitments":[]}}"#.to_vec(),
    )?;
    let adapter = acceptance_adapter(&store_fixture);

    let status = match adapter
        .get_tree_state(Request::new(lightwalletd::BlockId {
            height: 1,
            hash: Vec::new(),
        }))
        .await
    {
        Ok(response) => {
            return Err(eyre!(
                "expected malformed commitments error, got {response:?}"
            ));
        }
        Err(status) => status,
    };

    assert_eq!(status.code(), Code::DataLoss);

    Ok(())
}

#[tokio::test]
async fn tree_state_uses_typed_time_when_payload_omits_time() -> eyre::Result<()> {
    let store_fixture = sparse_tree_state_fixture(
        br#"{"hash":"010101","height":1,"sapling":{"commitments":{}},"orchard":{"commitments":{}}}"#
            .to_vec(),
    )?;
    let adapter = acceptance_adapter(&store_fixture);

    let response = adapter
        .get_tree_state(Request::new(lightwalletd::BlockId {
            height: 1,
            hash: Vec::new(),
        }))
        .await?
        .into_inner();

    assert_eq!(response.time, 1_774_668_401);

    Ok(())
}

#[tokio::test]
async fn tree_state_ignores_non_authoritative_payload_time() -> eyre::Result<()> {
    let store_fixture = sparse_tree_state_fixture(
        br#"{"hash":"010101","height":1,"time":"1296694002","sapling":{"commitments":{}},"orchard":{"commitments":{}}}"#
            .to_vec(),
    )?;
    let adapter = acceptance_adapter(&store_fixture);

    let response = adapter
        .get_tree_state(Request::new(lightwalletd::BlockId {
            height: 1,
            hash: Vec::new(),
        }))
        .await?
        .into_inner();

    assert_eq!(response.time, 1_774_668_401);

    Ok(())
}

#[tokio::test]
async fn ping_returns_zero_entry_and_exit() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);

    let response = adapter
        .ping(Request::new(lightwalletd::Duration { interval_us: 0 }))
        .await?
        .into_inner();

    assert_eq!(response.entry, 0);
    assert_eq!(response.exit, 0);

    Ok(())
}

#[tokio::test]
async fn get_transaction_rejects_block_index_without_txid_like_lightwalletd() -> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);

    let status = match adapter
        .get_transaction(Request::new(lightwalletd::TxFilter {
            block: Some(lightwalletd::BlockId {
                height: 1,
                hash: Vec::new(),
            }),
            index: 0,
            hash: Vec::new(),
        }))
        .await
    {
        Ok(response) => return Err(eyre!("expected invalid argument, got {response:?}")),
        Err(status) => status,
    };

    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(status.message(), "GetTransaction: specify a txid");
    Ok(())
}

#[tokio::test]
async fn get_transaction_rejects_unknown_block_index_without_txid_like_lightwalletd()
-> eyre::Result<()> {
    let store_fixture = acceptance_store_fixture()?;
    let adapter = acceptance_adapter(&store_fixture);

    let status = match adapter
        .get_transaction(Request::new(lightwalletd::TxFilter {
            block: Some(lightwalletd::BlockId {
                height: 1,
                hash: Vec::new(),
            }),
            index: 99,
            hash: Vec::new(),
        }))
        .await
    {
        Ok(response) => {
            return Err(eyre!("expected not-found error, got {response:?}"));
        }
        Err(status) => status,
    };

    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(status.message(), "GetTransaction: specify a txid");

    Ok(())
}

async fn collect_stream<T, Stream>(mut stream: Stream) -> Result<Vec<T>, tonic::Status>
where
    Stream: tonic::codegen::tokio_stream::Stream<Item = Result<T, tonic::Status>> + Unpin,
{
    use tonic::codegen::tokio_stream::StreamExt;

    let mut values = Vec::new();
    while let Some(stream_item) = stream.next().await {
        values.push(stream_item?);
    }
    Ok(values)
}

fn transaction_rows_without_blob(
    transaction_id: TransactionId,
    block: &zinder_testkit::FixtureBlock,
) -> FixtureTransactionRows {
    let location = TransactionLocation::new(transaction_id, block.height, block.hash, 0);
    FixtureTransactionRows::from_public_facts(
        location,
        synthetic_transaction_public_facts(transaction_id, 128),
    )
}

// Release history that one complete baseline cannot express: completed
// subtrees, checkpointed history floors, and blocks a live replacement
// displaced. Each store is built through the production canonical writer.

const FLOOR_HEIGHT: BlockHeight = BlockHeight::new(1);
const FIRST_RETAINED_HEIGHT: BlockHeight = BlockHeight::new(2);

#[tokio::test]
async fn get_subtree_roots_serves_non_empty_orchard_and_ironwood() -> eyre::Result<()> {
    let activations = sample_regtest_upgrade_activations();
    let completing_height = activations
        .activation_height_by_name(ShieldedProtocol::Ironwood.activation_upgrade_name())
        .ok_or_else(|| eyre!("regtest activations must activate Ironwood"))?;
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(completing_height.value());
    let completing_block = fixture_block(&chain, completing_height)?;
    let chain = chain
        .with_compact_block_artifact(CompactBlockArtifact::empty(
            BlockId::new(completing_block.height, completing_block.hash),
            completing_block.parent_hash,
            completing_block.block_time_seconds,
            CompactChainMetadata {
                sapling_commitment_tree_size: SUBTREE_LEAF_COUNT,
                orchard_commitment_tree_size: SUBTREE_LEAF_COUNT,
                ironwood_commitment_tree_size: SUBTREE_LEAF_COUNT,
            },
        ))
        .with_commitment_tree_frontiers_at(
            completing_height,
            CommitmentTreeFrontiers::from_validated_parts(
                Some(completed_subtree_frontier(ShieldedProtocol::Sapling)?),
                Some(completed_subtree_frontier(ShieldedProtocol::Orchard)?),
                Some(completed_subtree_frontier(ShieldedProtocol::Ironwood)?),
            ),
        );
    let mut baseline = CanonicalBaseline::new(&chain);
    for (protocol, root_byte) in [
        (ShieldedProtocol::Sapling, 0x17),
        (ShieldedProtocol::Orchard, 0x18),
        (ShieldedProtocol::Ironwood, 0x19),
    ] {
        baseline = baseline.with_subtree_root(CanonicalBuildSubtreeRoot {
            protocol,
            subtree_index: SubtreeRootIndex::new(0),
            root_hash: SubtreeRootHash::from_bytes([root_byte; 32]),
            completing_block_height: completing_height,
        });
    }
    let store =
        acceptance_store_from_writer(CanonicalWriterFixture::publish(&baseline, &activations)?)?;
    let adapter = acceptance_adapter(&store);

    for (protocol, root_byte) in [
        (lightwalletd::ShieldedProtocol::Orchard, 0x18),
        (lightwalletd::ShieldedProtocol::Ironwood, 0x19),
    ] {
        let roots = collect_stream(
            adapter
                .get_subtree_roots(Request::new(lightwalletd::GetSubtreeRootsArg {
                    start_index: 0,
                    shielded_protocol: protocol as i32,
                    max_entries: 1,
                }))
                .await?
                .into_inner(),
        )
        .await?;

        assert_eq!(roots.len(), 1, "{protocol:?} must serve its completed root");
        let root = roots
            .first()
            .ok_or_else(|| eyre!("{protocol:?} root missing"))?;
        assert_eq!(root.root_hash, vec![root_byte; 32]);
        assert_eq!(
            root.completing_block_hash,
            encode_internal_block_hash(completing_block.hash).to_vec()
        );
        assert_eq!(
            root.completing_block_height,
            u64::from(completing_height.value())
        );
    }

    Ok(())
}

/// A checkpointed store holds nothing at or below its history floor except
/// the floor's authenticated tree state.
#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the retained-history floor contract is a cross-RPC lightwalletd compatibility claim"
)]
async fn retained_history_floor_unavailable_artifacts_return_not_found() -> eyre::Result<()> {
    let store = acceptance_store_from_writer(floor_with_completed_subtree_writer()?)?;
    let adapter = acceptance_adapter(&store);
    let floor = || lightwalletd::BlockId {
        height: u64::from(FLOOR_HEIGHT.value()),
        hash: Vec::new(),
    };
    let floor_range = || lightwalletd::BlockRange {
        start: Some(floor()),
        end: Some(floor()),
        pool_types: Vec::new(),
    };

    let mut observed = Vec::new();
    observed.push((
        "GetBlock",
        adapter.get_block(Request::new(floor())).await.err(),
    ));
    observed.push((
        "GetBlockNullifiers",
        adapter
            .get_block_nullifiers(Request::new(floor()))
            .await
            .err(),
    ));
    observed.push((
        "GetBlockRange",
        first_stream_status(adapter.get_block_range(Request::new(floor_range())).await).await,
    ));
    observed.push((
        "GetBlockRangeNullifiers",
        first_stream_status(
            adapter
                .get_block_range_nullifiers(Request::new(floor_range()))
                .await,
        )
        .await,
    ));
    observed.push((
        "GetSubtreeRoots",
        first_stream_status(
            adapter
                .get_subtree_roots(Request::new(lightwalletd::GetSubtreeRootsArg {
                    start_index: 0,
                    shielded_protocol: lightwalletd::ShieldedProtocol::Sapling as i32,
                    max_entries: 1,
                }))
                .await,
        )
        .await,
    ));
    let mismatches = observed
        .into_iter()
        .filter_map(|(method, status)| match status {
            Some(status) if status.code() == Code::NotFound => None,
            Some(status) => Some(format!(
                "{method}: expected NotFound, observed {:?} \"{}\"",
                status.code(),
                status.message()
            )),
            None => Some(format!("{method}: expected NotFound, observed a response")),
        })
        .collect::<Vec<_>>();
    if !mismatches.is_empty() {
        return Err(eyre!(mismatches.join("\n")));
    }

    let floor_tree_state = adapter
        .get_tree_state(Request::new(floor()))
        .await?
        .into_inner();
    assert_eq!(floor_tree_state.height, u64::from(FLOOR_HEIGHT.value()));
    assert_eq!(
        floor_tree_state.sapling_tree,
        hex::encode(completed_subtree_frontier(ShieldedProtocol::Sapling)?.final_state_bytes())
    );

    Ok(())
}

#[tokio::test]
async fn compact_block_methods_serve_first_block_above_retained_history_floor() -> eyre::Result<()>
{
    let (writer, chain) = first_block_above_floor_writer()?;
    let store = acceptance_store_from_writer(writer)?;
    let adapter = acceptance_adapter(&store);
    let retained_block = fixture_block(&chain, FIRST_RETAINED_HEIGHT)?;
    let retained_height = u64::from(FIRST_RETAINED_HEIGHT.value());
    let retained_id = || lightwalletd::BlockId {
        height: retained_height,
        hash: Vec::new(),
    };

    let block = adapter
        .get_block(Request::new(retained_id()))
        .await?
        .into_inner();
    let ranged_blocks = collect_stream(
        adapter
            .get_block_range(Request::new(lightwalletd::BlockRange {
                start: Some(retained_id()),
                end: Some(retained_id()),
                pool_types: Vec::new(),
            }))
            .await?
            .into_inner(),
    )
    .await?;

    assert_eq!(block.height, retained_height);
    assert_eq!(
        block.hash,
        encode_internal_block_hash(retained_block.hash).to_vec()
    );
    assert_eq!(
        block.prev_hash,
        encode_internal_block_hash(retained_block.parent_hash).to_vec()
    );
    assert_eq!(
        block.chain_metadata,
        Some(lightwalletd::ChainMetadata {
            sapling_commitment_tree_size: 1,
            orchard_commitment_tree_size: 1,
            ironwood_commitment_tree_size: 0,
        })
    );
    let transaction = block
        .vtx
        .first()
        .ok_or_else(|| eyre!("retained block must carry its shielded transaction"))?;
    assert_eq!(
        transaction
            .outputs
            .iter()
            .map(|output| output.cmu.clone())
            .collect::<Vec<_>>(),
        vec![FIXTURE_NOTE_COMMITMENT.to_vec()]
    );
    assert_eq!(
        transaction
            .actions
            .iter()
            .map(|action| action.cmx.clone())
            .collect::<Vec<_>>(),
        vec![FIXTURE_NOTE_COMMITMENT.to_vec()]
    );
    assert_eq!(ranged_blocks, vec![block]);

    Ok(())
}

/// Tree states are derived from the typed frontiers the writer accumulated
/// from the retained block's note commitments.
#[tokio::test]
async fn tree_state_methods_serve_first_checkpoint_above_retained_history_floor() -> eyre::Result<()>
{
    let (writer, _chain) = first_block_above_floor_writer()?;
    let store = acceptance_store_from_writer(writer)?;
    let adapter = acceptance_adapter(&store);
    let retained_height = u64::from(FIRST_RETAINED_HEIGHT.value());

    let tree_state = adapter
        .get_tree_state(Request::new(lightwalletd::BlockId {
            height: retained_height,
            hash: Vec::new(),
        }))
        .await?
        .into_inner();
    let latest_tree_state = adapter
        .get_latest_tree_state(Request::new(lightwalletd::Empty {}))
        .await?
        .into_inner();

    assert_eq!(tree_state.height, retained_height);
    assert_eq!(
        tree_state.sapling_tree,
        hex::encode(one_leaf_frontier(ShieldedProtocol::Sapling)?.final_state_bytes())
    );
    assert_eq!(
        tree_state.orchard_tree,
        hex::encode(one_leaf_frontier(ShieldedProtocol::Orchard)?.final_state_bytes())
    );
    assert_eq!(tree_state.ironwood_tree, "");
    assert_eq!(latest_tree_state, tree_state);

    Ok(())
}

#[tokio::test]
async fn get_transaction_returns_not_found_after_reorg_invalidates_transaction() -> eyre::Result<()>
{
    let transaction_id = TransactionId::from_bytes([0x33; 32]);
    let reorged_height = BlockHeight::new(2);
    let chain = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(RawBlobRetention::Transactions)
        .extend_blocks(2);
    let reorged_block = fixture_block(&chain, reorged_height)?;
    let chain = chain
        .with_transaction_rows(FixtureTransactionRows::coinbase(
            TransactionId::from_bytes([0x32; 32]),
            reorged_block.height,
            reorged_block.hash,
            b"raw-reorged-coinbase".to_vec(),
        ))
        .with_transaction_rows(FixtureTransactionRows::from_raw_transaction(
            transaction_id,
            reorged_block.height,
            reorged_block.hash,
            1,
            b"raw-reorged-transaction".to_vec(),
        ));
    let replacement_chain = chain.fork_at(reorged_height)?.extend_blocks(1);
    let replacement_block = fixture_block(&replacement_chain, reorged_height)?;
    let mut writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).with_settled_tip(BlockHeight::new(1)),
        &sample_regtest_upgrade_activations(),
    )?;
    let transaction_filter = || lightwalletd::TxFilter {
        block: None,
        index: 0,
        hash: transaction_id.as_bytes().to_vec(),
    };

    let before_reorg = AcceptanceStore::from_store_fixture(
        writer.serving_store_fixture()?,
        CompatNode::read_only(writer.canonical_primary()?.event_fence().visible_tip())?,
    )?;
    let served = acceptance_adapter(&before_reorg)
        .get_transaction(Request::new(transaction_filter()))
        .await?
        .into_inner();
    assert_eq!(served.height, u64::from(reorged_height.value()));
    assert_eq!(served.data, b"raw-reorged-transaction".to_vec());

    writer.replace_live_blocks(&replacement_chain, reorged_height)?;
    let after_reorg = acceptance_store_from_writer(writer)?;
    let adapter = acceptance_adapter(&after_reorg);
    let status = match adapter
        .get_transaction(Request::new(transaction_filter()))
        .await
    {
        Ok(response) => {
            return Err(eyre!(
                "expected reorged transaction to disappear, got {response:?}"
            ));
        }
        Err(status) => status,
    };
    let replacement = adapter
        .get_block(Request::new(lightwalletd::BlockId {
            height: u64::from(reorged_height.value()),
            hash: Vec::new(),
        }))
        .await?
        .into_inner();

    assert_eq!(status.code(), Code::NotFound);
    assert_eq!(
        replacement.hash,
        encode_internal_block_hash(replacement_block.hash).to_vec()
    );

    Ok(())
}

/// Checkpointed history whose floor already completed one Sapling subtree,
/// with one retained block that adds no note commitments.
fn floor_with_completed_subtree_writer() -> eyre::Result<CanonicalWriterFixture> {
    let completed_tree = CompactChainMetadata {
        sapling_commitment_tree_size: SUBTREE_LEAF_COUNT,
        orchard_commitment_tree_size: 0,
        ironwood_commitment_tree_size: 0,
    };
    let mut chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    for block in chain.blocks().to_vec() {
        chain = chain.with_compact_block_artifact(CompactBlockArtifact::empty(
            BlockId::new(block.height, block.hash),
            canonical_parent_hash(&block),
            block.block_time_seconds,
            completed_tree,
        ));
    }
    let chain = chain.with_commitment_tree_frontiers_at(
        FLOOR_HEIGHT,
        CommitmentTreeFrontiers::from_validated_parts(
            Some(completed_subtree_frontier(ShieldedProtocol::Sapling)?),
            None,
            None,
        ),
    );
    CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).with_history_floor(FLOOR_HEIGHT),
        &sample_regtest_upgrade_activations(),
    )
}

/// Checkpointed history whose first retained block appends one Sapling and
/// one Orchard note commitment to empty trees.
fn first_block_above_floor_writer() -> eyre::Result<(CanonicalWriterFixture, ChainFixture)> {
    let transaction_id = TransactionId::from_bytes([0x52; 32]);
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let retained_block = fixture_block(&chain, FIRST_RETAINED_HEIGHT)?;
    let mut transaction_rows = FixtureTransactionRows::from_raw_transaction(
        transaction_id,
        retained_block.height,
        retained_block.hash,
        0,
        b"retained-shielded-transaction".to_vec(),
    );
    transaction_rows.facts.public_facts.counts = TransactionComponentCounts {
        sapling_output_count: 1,
        orchard_action_count: 1,
        ..TransactionComponentCounts::EMPTY
    };
    let compact_block = CompactBlockArtifact::new(
        BlockId::new(retained_block.height, retained_block.hash),
        retained_block.parent_hash,
        retained_block.block_time_seconds,
        vec![CompactTransaction {
            index: 0,
            transaction_id,
            data: CompactTransactionData {
                sapling_outputs: vec![CompactSaplingOutput {
                    commitment: FIXTURE_NOTE_COMMITMENT,
                    ephemeral_key: [0x22; 32],
                    ciphertext: [0x33; 52],
                }],
                orchard_actions: vec![CompactShieldedAction {
                    nullifier: [0x44; 32],
                    commitment: FIXTURE_NOTE_COMMITMENT,
                    ephemeral_key: [0x55; 32],
                    ciphertext: [0x66; 52],
                }],
                ..CompactTransactionData::default()
            },
        }],
        CompactChainMetadata {
            sapling_commitment_tree_size: 1,
            orchard_commitment_tree_size: 1,
            ironwood_commitment_tree_size: 0,
        },
    )?;
    let chain = chain
        .with_transaction_rows(transaction_rows)
        .with_compact_block_artifact(compact_block);
    let writer = CanonicalWriterFixture::publish(
        &CanonicalBaseline::new(&chain).with_history_floor(FLOOR_HEIGHT),
        &sample_regtest_upgrade_activations(),
    )?;
    Ok((writer, chain))
}

fn fixture_block(chain: &ChainFixture, height: BlockHeight) -> eyre::Result<FixtureBlock> {
    chain
        .block_at(height)
        .cloned()
        .ok_or_else(|| eyre!("fixture must contain height {}", height.value()))
}

/// The parent the canonical store records for `block`: the network's genesis
/// block below height 1.
fn canonical_parent_hash(block: &FixtureBlock) -> zinder_core::BlockHash {
    if block.height == BlockHeight::new(1) {
        Network::ZcashRegtest.genesis_hash()
    } else {
        block.parent_hash
    }
}

/// Returns the status a streaming call reports, at the call or in its stream.
async fn first_stream_status<T, Stream>(
    outcome: Result<tonic::Response<Stream>, Status>,
) -> Option<Status>
where
    Stream: tokio_stream::Stream<Item = Result<T, Status>> + Unpin,
{
    match outcome {
        Err(status) => Some(status),
        Ok(response) => collect_stream(response.into_inner()).await.err(),
    }
}
/// Serves the writer's final fence through the compatibility composition.
fn acceptance_store_from_writer(writer: CanonicalWriterFixture) -> eyre::Result<AcceptanceStore> {
    let node = CompatNode::read_only(writer.canonical_primary()?.event_fence().visible_tip())?;
    AcceptanceStore::from_store_fixture(writer.into_serving_store_fixture()?, node)
}

/// Exact production serving pair queried through a probed node double.
struct AcceptanceStore {
    query: WalletServingQuery<CompatNode>,
    _store_fixture: WalletServingStoreFixture,
}

impl AcceptanceStore {
    fn from_chain(chain: &ChainFixture, node: CompatNode) -> eyre::Result<Self> {
        Self::from_store_fixture(
            WalletServingStoreFixture::from_chain(chain, &sample_regtest_upgrade_activations())?,
            node,
        )
    }

    fn from_store_fixture(
        mut store_fixture: WalletServingStoreFixture,
        node: CompatNode,
    ) -> eyre::Result<Self> {
        let activations = Arc::new(sample_regtest_upgrade_activations());
        let (canonical, wallet) = store_fixture.take_readers()?;
        let query = WalletServingQuery::from_probed_node_source(
            WalletServingPairSlot::new(Arc::new(WalletServingReadPair::new(
                Arc::new(canonical),
                Arc::new(wallet),
            )?)),
            node,
            activations,
        )?;
        Ok(Self {
            query,
            _store_fixture: store_fixture,
        })
    }

    fn query(&self) -> WalletServingQuery<CompatNode> {
        self.query.clone()
    }
}

fn acceptance_adapter(
    store_fixture: &AcceptanceStore,
) -> LightwalletdGrpcAdapter<WalletServingQuery<CompatNode>> {
    LightwalletdGrpcAdapter::new(store_fixture.query())
}

fn acceptance_store_from_chain(chain: &ChainFixture) -> eyre::Result<AcceptanceStore> {
    let tip = chain
        .blocks()
        .last()
        .ok_or_else(|| eyre!("acceptance chain must contain a block"))?;
    AcceptanceStore::from_chain(
        chain,
        CompatNode::read_only(BlockId::new(tip.height, tip.hash))?,
    )
}

/// Builds a compatibility adapter whose probed node admits broadcast and
/// forwards it to `broadcaster`.
fn broadcasting_acceptance_adapter(
    broadcaster: MockTransactionBroadcaster,
) -> eyre::Result<(
    LightwalletdGrpcAdapter<WalletServingQuery<CompatNode>>,
    AcceptanceStore,
)> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(1);
    let tip = chain
        .block_at(ACCEPTANCE_BLOCK_HEIGHT)
        .ok_or_else(|| eyre!("broadcast fixture must include the height 1 block"))?;
    let store_fixture = AcceptanceStore::from_chain(
        &chain,
        CompatNode::broadcasting(BlockId::new(tip.height, tip.hash), broadcaster)?,
    )?;
    Ok((acceptance_adapter(&store_fixture), store_fixture))
}

/// Builds a two-block pair whose height-1 tree state is not a canonical
/// checkpoint, so the node fills it with `tree_state_payload`.
fn sparse_tree_state_fixture(tree_state_payload: Vec<u8>) -> eyre::Result<AcceptanceStore> {
    let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
    let sparse_block = chain
        .block_at(ACCEPTANCE_BLOCK_HEIGHT)
        .ok_or_else(|| eyre!("sparse tree-state fixture must include height 1"))?;
    let sparse_block_id = BlockId::new(sparse_block.height, sparse_block.hash);
    let tip = chain
        .block_at(BlockHeight::new(2))
        .ok_or_else(|| eyre!("sparse tree-state fixture must include height 2"))?;
    AcceptanceStore::from_chain(
        &chain,
        CompatNode::read_only(BlockId::new(tip.height, tip.hash))?.with_tree_state(
            SourceTreeState::new(
                sparse_block_id,
                sparse_block.block_time_seconds,
                tree_state_payload,
            ),
        ),
    )
}

fn acceptance_store_fixture() -> eyre::Result<AcceptanceStore> {
    acceptance_store_fixture_with_transaction_rows(RawBlobRetention::None, |block| {
        let transaction_id = TransactionId::from_bytes([2; 32]);
        let mut rows = FixtureTransactionRows::from_raw_transaction(
            transaction_id,
            block.height,
            block.hash,
            0,
            b"compact-wallet-transaction",
        );
        rows.facts.public_facts.counts = TransactionComponentCounts {
            transparent_input_count: 0,
            transparent_output_count: 0,
            sapling_spend_count: 1,
            sapling_output_count: 1,
            orchard_action_count: 1,
            ironwood_action_count: 1,
            sprout_joinsplit_count: 0,
        };
        vec![rows]
    })
}

fn acceptance_store_fixture_with_transaction_rows<TransactionsFn>(
    raw_blob_retention: RawBlobRetention,
    build_transaction_rows: TransactionsFn,
) -> eyre::Result<AcceptanceStore>
where
    TransactionsFn: FnOnce(&zinder_testkit::FixtureBlock) -> Vec<FixtureTransactionRows>,
{
    acceptance_store_fixture_with_transaction_rows_and_transparent(
        raw_blob_retention,
        build_transaction_rows,
        |_| (Vec::new(), Vec::new()),
    )
}

/// Builds the one-block acceptance pair.
///
/// The production fixture publishes empty commitment-tree frontiers, so the
/// compact block keeps its shielded payload but reports zero tree sizes.
fn acceptance_store_fixture_with_transaction_rows_and_transparent<TransactionsFn, TransparentFn>(
    raw_blob_retention: RawBlobRetention,
    build_transaction_rows: TransactionsFn,
    build_transparent_artifacts: TransparentFn,
) -> eyre::Result<AcceptanceStore>
where
    TransactionsFn: FnOnce(&zinder_testkit::FixtureBlock) -> Vec<FixtureTransactionRows>,
    TransparentFn: FnOnce(
        &zinder_testkit::FixtureBlock,
    ) -> (Vec<TransparentUnspentOutput>, Vec<TransparentSpendFact>),
{
    let base_fixture = ChainFixture::new(Network::ZcashRegtest)
        .with_raw_blob_retention(raw_blob_retention)
        .extend_blocks(1);
    let acceptance_block = base_fixture
        .block_at(ACCEPTANCE_BLOCK_HEIGHT)
        .ok_or_else(|| eyre!("acceptance fixture must include the height 1 block"))?
        .clone();
    let transaction_rows = build_transaction_rows(&acceptance_block);
    let compact_block = acceptance_compact_block_artifact_at(&acceptance_block, &transaction_rows)?;
    let has_explicit_compact_transactions = !compact_block.transactions().is_empty();
    let (address_output_index, transparent_spend_facts) =
        build_transparent_artifacts(&acceptance_block);

    let mut chain_fixture = base_fixture;
    if has_explicit_compact_transactions {
        chain_fixture = chain_fixture.with_compact_block_artifact(CompactBlockArtifact::new(
            BlockId::new(acceptance_block.height, acceptance_block.hash),
            Network::ZcashRegtest.genesis_hash(),
            acceptance_block.block_time_seconds,
            compact_block.transactions().to_vec(),
            CompactChainMetadata {
                sapling_commitment_tree_size: 0,
                orchard_commitment_tree_size: 0,
                ironwood_commitment_tree_size: 0,
            },
        )?);
    }
    for transaction_rows in transaction_rows {
        chain_fixture = chain_fixture.with_transaction_rows(transaction_rows);
    }
    for address_output_index in address_output_index {
        chain_fixture = chain_fixture.with_address_output_index(address_output_index);
    }
    for transparent_spend_fact in transparent_spend_facts {
        chain_fixture = chain_fixture.with_transparent_spend_fact(transparent_spend_fact);
    }

    acceptance_store_from_chain(&chain_fixture)
}

/// Probed node double for the compatibility composition.
///
/// It always advertises tree-state fill, as production nodes do, and answers
/// a fill only for the one tree state it holds.
#[derive(Clone)]
struct CompatNode {
    capabilities: NodeCapabilities,
    tip: BlockId,
    broadcaster: MockTransactionBroadcaster,
    tree_state: Option<SourceTreeState>,
}

impl CompatNode {
    fn read_only(tip: BlockId) -> eyre::Result<Self> {
        Ok(Self {
            capabilities: NodeCapabilities::new([
                NodeCapability::OpenRpcDiscovery,
                NodeCapability::TipId,
                NodeCapability::TreeState,
            ])?,
            tip,
            broadcaster: MockTransactionBroadcaster::broadcast_disabled(),
            tree_state: None,
        })
    }

    fn broadcasting(tip: BlockId, broadcaster: MockTransactionBroadcaster) -> eyre::Result<Self> {
        Ok(Self {
            capabilities: NodeCapabilities::new([
                NodeCapability::OpenRpcDiscovery,
                NodeCapability::TipId,
                NodeCapability::TreeState,
                NodeCapability::TransactionBroadcast,
            ])?,
            tip,
            broadcaster,
            tree_state: None,
        })
    }

    fn with_tree_state(mut self, tree_state: SourceTreeState) -> Self {
        self.tree_state = Some(tree_state);
        self
    }
}

#[async_trait]
impl NodeSource for CompatNode {
    fn capabilities(&self) -> NodeCapabilities {
        self.capabilities
    }

    async fn fetch_block_at(&self, _height: BlockHeight) -> Result<SourceBlock, SourceError> {
        Err(SourceError::NodeCapabilityMissing {
            capability: NodeCapability::BestChainBlocks,
        })
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        Ok(self.tip)
    }
}

#[async_trait]
impl TreeStateUpstream for CompatNode {
    async fn fetch_tree_state_for_block(
        &self,
        block_id: BlockId,
    ) -> Result<SourceTreeState, SourceError> {
        self.tree_state
            .clone()
            .filter(|tree_state| tree_state.block_id == block_id)
            .ok_or_else(|| SourceError::NodeUnavailable {
                reason: "compatibility test node holds no tree state for this block".to_owned(),
            })
    }
}

#[async_trait]
impl TransactionBroadcaster for CompatNode {
    async fn broadcast_transaction(
        &self,
        raw_transaction: RawTransactionBytes,
    ) -> Result<TransactionBroadcastOutcome, SourceError> {
        self.broadcaster
            .broadcast_transaction(raw_transaction)
            .await
    }
}

fn acceptance_compact_block_artifact_at(
    block: &zinder_testkit::FixtureBlock,
    rows: &[FixtureTransactionRows],
) -> eyre::Result<CompactBlockArtifact> {
    let mut sapling_outputs = 0_u32;
    let mut orchard_actions = 0_u32;
    let mut ironwood_actions = 0_u32;
    let mut transactions = Vec::new();
    for rows in rows {
        let counts = rows.facts.public_facts.counts;
        if counts.sapling_spend_count == 0
            && counts.sapling_output_count == 0
            && counts.orchard_action_count == 0
            && counts.ironwood_action_count == 0
            && counts.transparent_input_count == 0
            && counts.transparent_output_count == 0
        {
            continue;
        }
        sapling_outputs = sapling_outputs.saturating_add(counts.sapling_output_count);
        orchard_actions = orchard_actions.saturating_add(counts.orchard_action_count);
        ironwood_actions = ironwood_actions.saturating_add(counts.ironwood_action_count);
        transactions.push(CompactTransaction {
            index: u64::from(rows.location.tx_index_in_block),
            transaction_id: rows.location.transaction_id,
            data: CompactTransactionData {
                fee_zat: None,
                sapling_spends: (0..counts.sapling_spend_count)
                    .map(|_| CompactSaplingSpend { nullifier: [3; 32] })
                    .collect(),
                sapling_outputs: (0..counts.sapling_output_count)
                    .map(|_| CompactSaplingOutput {
                        commitment: [4; 32],
                        ephemeral_key: [5; 32],
                        ciphertext: [6; 52],
                    })
                    .collect(),
                orchard_actions: (0..counts.orchard_action_count)
                    .map(|_| CompactShieldedAction {
                        nullifier: [9; 32],
                        commitment: [10; 32],
                        ephemeral_key: [11; 32],
                        ciphertext: [12; 52],
                    })
                    .collect(),
                ironwood_actions: (0..counts.ironwood_action_count)
                    .map(|_| CompactShieldedAction {
                        nullifier: [13; 32],
                        commitment: [14; 32],
                        ephemeral_key: [15; 32],
                        ciphertext: [16; 52],
                    })
                    .collect(),
                transparent_inputs: Vec::new(),
                transparent_outputs: Vec::new(),
            },
        });
    }
    Ok(CompactBlockArtifact::new(
        BlockId::new(block.height, block.hash),
        block.parent_hash,
        block.block_time_seconds,
        transactions,
        CompactChainMetadata {
            sapling_commitment_tree_size: sapling_outputs,
            orchard_commitment_tree_size: orchard_actions,
            ironwood_commitment_tree_size: ironwood_actions,
        },
    )?)
}
