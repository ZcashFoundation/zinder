#![allow(
    missing_docs,
    reason = "Live test names describe the behavior under test."
)]

use eyre::{Result, eyre};
use zinder_core::{
    BlockHeight, ConsensusBranchId, Network, RawTransactionBytes, ShieldedProtocol,
    SubtreeRootIndex, TransactionBroadcastOutcome, TransactionId,
};
use zinder_source::{
    JsonRpcMempoolSource, JsonRpcMempoolSourceOptions, MempoolSource, MempoolSourceBackend,
    NodeCapability, NodeSource, TransactionBroadcaster, UpstreamTransactionLookup,
    ZebraJsonRpcSource, ZebraJsonRpcSourceOptions,
};
use zinder_testkit::live::{init, require_live, require_live_for, require_live_mainnet};

#[tokio::test]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn fetch_chain_checkpoint_at_tip_reflects_supported_tree_sizes() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live_for(&[Network::ZcashRegtest])? else {
        return Ok(());
    };
    let source = zebra_source(&env)?;
    let tip = NodeSource::tip_id(&source).await?.height;
    let activations = source.fetch_network_upgrade_activations().await?;
    let checkpoint = source.fetch_chain_checkpoint(tip, &activations).await?;
    let tip_metadata = checkpoint.tip_metadata();

    assert_eq!(checkpoint.block_id.height, tip);
    // Each shielded output or action appends one note commitment, so the tree
    // sizes at the tip equal the commitments in every block up to it.
    let mut expected_sizes = (0_usize, 0_usize, 0_usize);
    for height in 1..=tip.value() {
        let block = parse_block(&source, BlockHeight::new(height)).await?;
        for transaction in &block.transactions {
            expected_sizes.0 += transaction.sapling_note_commitments().count();
            expected_sizes.1 += transaction.orchard_note_commitments().count();
            expected_sizes.2 += transaction.ironwood_note_commitments().count();
        }
    }
    assert_eq!(
        (
            usize::try_from(tip_metadata.sapling_commitment_tree_size)?,
            usize::try_from(tip_metadata.orchard_commitment_tree_size)?,
            usize::try_from(tip_metadata.ironwood_commitment_tree_size)?,
        ),
        expected_sizes,
        "(Sapling, Orchard, Ironwood) tree sizes at tip {}",
        tip.value()
    );
    Ok(())
}

async fn parse_block(
    source: &ZebraJsonRpcSource,
    height: BlockHeight,
) -> Result<zebra_chain::block::Block> {
    parse_raw_block(
        &source.fetch_block_at(height).await?.raw_block_bytes,
        height,
    )
}

fn parse_raw_block(
    raw_block_bytes: &[u8],
    height: BlockHeight,
) -> Result<zebra_chain::block::Block> {
    use zebra_chain::serialization::ZcashDeserializeInto;
    raw_block_bytes.zcash_deserialize_into().map_err(|error| {
        eyre!(
            "zebra-chain block parse failed at {}: {error}",
            height.value()
        )
    })
}

/// Every block from genesis to the tip round-trips through Zinder's parser.
///
/// Each block decodes with `zebra-chain` and re-serializes to exactly the
/// bytes the node returned. A node that serves stored bytes and a node that
/// re-serializes its own block model must both agree with the parser Zinder
/// indexes with.
#[tokio::test]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn source_blocks_reencode_to_node_bytes_across_the_chain() -> Result<()> {
    use zebra_chain::serialization::ZcashSerialize as _;

    let _guard = init();
    let Some(env) = require_live_for(&[Network::ZcashRegtest])? else {
        return Ok(());
    };
    let source = zebra_source(&env)?;
    let tip = NodeSource::tip_id(&source).await?;

    for height in 0..=tip.height.value() {
        let height = BlockHeight::new(height);
        let source_block = source.fetch_block_at(height).await?;
        let block = parse_raw_block(&source_block.raw_block_bytes, height)?;
        let reencoded = block.zcash_serialize_to_vec()?;
        if let Some(offset) = first_difference(&reencoded, &source_block.raw_block_bytes) {
            return Err(eyre!(
                "block at height {} re-encodes to different bytes than the node served: \
                 first difference at byte {offset}, re-encoded {} bytes, served {} bytes",
                height.value(),
                reencoded.len(),
                source_block.raw_block_bytes.len()
            ));
        }
        if height == tip.height {
            assert_eq!(
                block.hash().0,
                tip.hash.as_bytes(),
                "the tip block's bytes must hash to the node's best block hash"
            );
        }
    }
    Ok(())
}

fn first_difference(left: &[u8], right: &[u8]) -> Option<usize> {
    left.iter()
        .zip(right)
        .position(|(left_byte, right_byte)| left_byte != right_byte)
        .or_else(|| (left.len() != right.len()).then(|| left.len().min(right.len())))
}

/// The node advertises network upgrades in consensus order.
///
/// Zinder's activation table relies on that order when several upgrades share
/// an activation height (Regtest activates Overwinter through Canopy at height
/// 1): the last one advertised at a height is the one active there.
#[tokio::test]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn upgrade_activations_keep_advertised_order() -> Result<()> {
    use zebra_chain::parameters::NetworkUpgrade;

    let _guard = init();
    let Some(env) = require_live_for(&[
        Network::ZcashRegtest,
        Network::ZcashTestnet,
        Network::ZcashMainnet,
    ])?
    else {
        return Ok(());
    };
    let source = zebra_source(&env)?;
    let activations = source.fetch_network_upgrade_activations().await?;
    let upgrade_of = |branch_id: ConsensusBranchId, name: &str| {
        NetworkUpgrade::try_from(branch_id.value()).map_err(|_| {
            eyre!(
                "node advertised {name} with branch id {branch_id}, which zebra-chain does not know"
            )
        })
    };

    // The table is sorted by activation height and keeps the node's order
    // among upgrades that share a height, so consensus order must increase
    // strictly along it.
    let advertised = activations
        .activations()
        .iter()
        .map(|activation| {
            Ok((
                activation.activation_height,
                upgrade_of(activation.branch_id, &activation.name)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    for pair in advertised.windows(2) {
        let [(earlier_height, earlier), (later_height, later)] = pair else {
            continue;
        };
        assert!(
            earlier < later,
            "node advertised {later:?} (height {}) after {earlier:?} (height {}); \
             upgrades must be listed in consensus order",
            later_height.value(),
            earlier_height.value()
        );
    }

    let tip = NodeSource::tip_id(&source).await?.height;
    let mut heights: Vec<BlockHeight> = advertised.iter().map(|(height, _)| *height).collect();
    heights.dedup();
    for height in heights {
        let latest = advertised
            .iter()
            .filter(|(activation_height, _)| *activation_height == height)
            .map(|(_, upgrade)| *upgrade)
            .max()
            .ok_or_else(|| eyre!("no upgrade activates at height {}", height.value()))?;
        let active = activations
            .active_at(height)
            .ok_or_else(|| eyre!("no upgrade is active at height {}", height.value()))?;
        assert_eq!(
            upgrade_of(active.branch_id, &active.name)?,
            latest,
            "active upgrade at height {}",
            height.value()
        );

        // A version-5 or later coinbase names its consensus branch, so the
        // node's own block confirms the upgrade the table resolves.
        if height > tip {
            continue;
        }
        let block = parse_block(&source, height).await?;
        let coinbase = block
            .transactions
            .first()
            .ok_or_else(|| eyre!("block at height {} has no coinbase", height.value()))?;
        if let Some(coinbase_upgrade) = coinbase.network_upgrade() {
            assert_eq!(
                coinbase_upgrade,
                latest,
                "coinbase consensus branch at height {}",
                height.value()
            );
        }
    }
    Ok(())
}

#[tokio::test]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn fetch_chain_checkpoint_returns_advancing_tree_sizes_on_mainnet() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live_mainnet()? else {
        return Ok(());
    };
    let source = zebra_source(&env)?;
    let tip = NodeSource::tip_id(&source).await?.height;
    let checkpoint_height = BlockHeight::new(tip.value().saturating_sub(1_000));
    let activations = source.fetch_network_upgrade_activations().await?;
    let checkpoint = source
        .fetch_chain_checkpoint(checkpoint_height, &activations)
        .await?;
    let tip_metadata = checkpoint.tip_metadata();

    assert_eq!(checkpoint.block_id.height, checkpoint_height);
    assert!(
        tip_metadata.sapling_commitment_tree_size > 100_000,
        "mainnet sapling tree size at recent height should be well above 100k; got {}",
        tip_metadata.sapling_commitment_tree_size
    );
    assert!(
        tip_metadata.orchard_commitment_tree_size > 10_000,
        "mainnet orchard tree size at recent height should be above 10k; got {}",
        tip_metadata.orchard_commitment_tree_size
    );
    Ok(())
}

#[tokio::test]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn capability_probe_discovers_zebra_methods() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live()? else {
        return Ok(());
    };
    let source = zebra_source(&env)?;

    let probed = source.probe_capabilities().await?;

    assert!(probed.supports(NodeCapability::JsonRpc));
    assert!(probed.supports(NodeCapability::OpenRpcDiscovery));
    assert!(probed.supports(NodeCapability::BestChainBlocks));
    assert!(probed.supports(NodeCapability::TipId));
    assert!(probed.supports(NodeCapability::TreeState));
    assert!(probed.supports(NodeCapability::SubtreeRoots));
    assert!(probed.supports(NodeCapability::TransactionBroadcast));

    Ok(())
}

#[tokio::test]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn tip_id_advances_above_one_million() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live_mainnet()? else {
        return Ok(());
    };
    let source = zebra_source(&env)?;
    let tip = source.tip_id().await?;

    assert!(
        tip.height.value() > 1_000_000,
        "tip height should be well above 1,000,000; got {tip:?}"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn broadcast_classifies_invalid_transaction() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live()? else {
        return Ok(());
    };
    let source = zebra_source(&env)?;

    let subtree_roots = source
        .fetch_subtree_roots(
            ShieldedProtocol::Sapling,
            SubtreeRootIndex::new(0),
            std::num::NonZeroU32::new(1).ok_or_else(|| eyre!("invalid max entries"))?,
        )
        .await?;
    let broadcast_outcome = source
        .broadcast_transaction(RawTransactionBytes::new([0x00]))
        .await?;

    assert_eq!(subtree_roots.protocol, ShieldedProtocol::Sapling);
    assert_eq!(subtree_roots.start_index, SubtreeRootIndex::new(0));
    assert!(matches!(
        broadcast_outcome,
        TransactionBroadcastOutcome::InvalidEncoding(_)
            | TransactionBroadcastOutcome::Rejected(_)
            | TransactionBroadcastOutcome::Unknown(_)
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn fetches_tip_block_and_rejects_invalid_transaction() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live()? else {
        return Ok(());
    };
    let source = zebra_source(&env)?;

    let tip_height = source.tip_id().await?.height;
    let source_block = source.fetch_block_at(tip_height).await?;
    let subtree_roots = source
        .fetch_subtree_roots(
            ShieldedProtocol::Sapling,
            SubtreeRootIndex::new(0),
            std::num::NonZeroU32::new(1).ok_or_else(|| eyre!("invalid max entries"))?,
        )
        .await?;
    let broadcast_outcome = source
        .broadcast_transaction(RawTransactionBytes::new([0x00]))
        .await?;

    assert!(tip_height.value() > 0);
    assert_eq!(source_block.network, env.network());
    assert_eq!(source_block.height, tip_height);
    assert_eq!(subtree_roots.protocol, ShieldedProtocol::Sapling);
    assert_eq!(subtree_roots.start_index, SubtreeRootIndex::new(0));
    assert!(matches!(
        broadcast_outcome,
        TransactionBroadcastOutcome::InvalidEncoding(_)
            | TransactionBroadcastOutcome::Rejected(_)
            | TransactionBroadcastOutcome::Unknown(_)
    ));
    Ok(())
}

fn zebra_source(env: &zinder_testkit::live::LiveTestEnv) -> Result<ZebraJsonRpcSource> {
    Ok(ZebraJsonRpcSource::with_options(
        env.target.network,
        &env.target.json_rpc_addr,
        env.target.node_auth.clone(),
        ZebraJsonRpcSourceOptions {
            request_timeout: env.target.request_timeout,
            max_response_bytes: env.target.max_response_bytes,
            broadcast_timeout: None,
        },
    )?)
}

#[tokio::test]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn fetch_raw_mempool_transaction_ids_returns_typed_list() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live()? else {
        return Ok(());
    };
    let source = zebra_source(&env)?;
    // The shape of the response (Vec<TransactionId>) is the contract;
    // the regtest mempool may be empty or carry transactions from another
    // local process. Either way, the call must succeed and return a typed list.
    let mempool_ids = source.fetch_raw_mempool_transaction_ids().await?;
    for transaction_id in &mempool_ids {
        assert_eq!(transaction_id.as_bytes().len(), 32);
    }
    Ok(())
}

#[tokio::test]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn upstream_transaction_lookup_resolves_mined_coinbase() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live()? else {
        return Ok(());
    };
    let source = zebra_source(&env)?;
    let tip_height = source.tip_id().await?.height;
    let mined_transaction_id = parse_coinbase_transaction_id(&source, tip_height).await?;

    let lookup = source
        .fetch_upstream_transaction_lookup(mined_transaction_id)
        .await?;

    assert!(matches!(lookup, UpstreamTransactionLookup::Mined { .. }));
    if let UpstreamTransactionLookup::Mined {
        mined_height: observed_height,
        block_hash: observed_hash,
    } = lookup
    {
        assert_eq!(observed_height, tip_height);
        assert_ne!(
            observed_hash.as_bytes(),
            [0u8; 32],
            "blockhash must be present"
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn json_rpc_mempool_source_runs_one_poll_cycle_without_panic() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live()? else {
        return Ok(());
    };
    let json_rpc = zebra_source(&env)?;
    let mempool_source = JsonRpcMempoolSource::with_options(
        json_rpc,
        JsonRpcMempoolSourceOptions {
            poll_interval: std::time::Duration::from_millis(100),
            event_channel_capacity: 16,
            ..JsonRpcMempoolSourceOptions::default()
        },
    );
    assert_eq!(
        mempool_source.capabilities().backend,
        MempoolSourceBackend::Polling
    );

    let mut event_stream = mempool_source.events().await?;
    // The polling loop runs in a background task. Wait long enough for
    // the first iteration to complete (poll_interval=100ms with one
    // round-trip to Zebra). Empty regtest mempool yields no events; a
    // populated mempool would yield Added envelopes here. Either way,
    // the loop must remain alive and the stream must not error out.
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        tokio_stream::StreamExt::next(&mut event_stream),
    )
    .await;
    match outcome {
        Ok(Some(Ok(_event))) => {} // Mempool had a transaction; that is fine.
        Ok(Some(Err(error))) => {
            return Err(eyre!("polling source emitted error item: {error}"));
        }
        Ok(None) => {
            return Err(eyre!("polling source closed unexpectedly"));
        }
        Err(_elapsed) => {} // Empty mempool, no events; loop is alive.
    }
    Ok(())
}

async fn parse_coinbase_transaction_id(
    source: &ZebraJsonRpcSource,
    height: BlockHeight,
) -> Result<TransactionId> {
    use zebra_chain::serialization::ZcashDeserializeInto;
    let source_block = source.fetch_block_at(height).await?;
    let parsed_block: zebra_chain::block::Block = source_block
        .raw_block_bytes
        .as_slice()
        .zcash_deserialize_into()
        .map_err(|error| eyre!("zebra-chain block parse failed: {error}"))?;
    let coinbase_transaction = parsed_block
        .transactions
        .first()
        .ok_or_else(|| eyre!("block has no coinbase transaction"))?;
    let transaction_hash_bytes = coinbase_transaction.hash().0;
    Ok(TransactionId::from_bytes(transaction_hash_bytes))
}

#[tokio::test]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn upstream_transaction_lookup_returns_not_found_for_unknown_txid() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live()? else {
        return Ok(());
    };
    let source = zebra_source(&env)?;
    let unknown_transaction_id = TransactionId::from_bytes([0xAB; 32]);
    let lookup = source
        .fetch_upstream_transaction_lookup(unknown_transaction_id)
        .await?;
    assert!(matches!(lookup, UpstreamTransactionLookup::NotFound));
    Ok(())
}

#[tokio::test]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn fetch_raw_transaction_bytes_returns_none_for_unknown_txid() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live()? else {
        return Ok(());
    };
    let source = zebra_source(&env)?;
    let unknown_transaction_id = TransactionId::from_bytes([0xCD; 32]);
    let bytes = source
        .fetch_raw_transaction_bytes(unknown_transaction_id)
        .await?;
    assert!(bytes.is_none());
    Ok(())
}

#[tokio::test]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn fetch_network_upgrade_activations_matches_running_node_getblockchaininfo() -> Result<()> {
    let _guard = init();
    let Some(env) = zinder_testkit::live::require_live_for(&[
        Network::ZcashRegtest,
        Network::ZcashTestnet,
        Network::ZcashMainnet,
    ])?
    else {
        return Ok(());
    };
    let source = zebra_source(&env)?;
    let activations = source.fetch_network_upgrade_activations().await?;

    assert_eq!(
        activations.network(),
        env.network(),
        "the table must carry the same network identifier the source was built with"
    );
    let Some(sapling) = activations.activation_height_by_name("Sapling") else {
        return Err(eyre!(
            "running node did not advertise Sapling in getblockchaininfo.upgrades; \
             every Zinder-supported network activates Sapling at or above height 1"
        ));
    };
    assert!(
        sapling.value() >= 1,
        "Sapling activation height must be at least 1; got {}",
        sapling.value()
    );

    let tip = NodeSource::tip_id(&source).await?.height;
    let branch_at_tip = activations.consensus_branch_id_at(tip);
    if tip.value() >= sapling.value() {
        assert_ne!(
            branch_at_tip,
            zinder_core::ConsensusBranchId::PRE_OVERWINTER,
            "after Sapling, consensus_branch_id_at(tip) must be non-zero; \
             tip={}, sapling={}, activations={:?}",
            tip.value(),
            sapling.value(),
            activations.activations(),
        );
    }
    let Some(active) = activations.active_at(tip) else {
        return Err(eyre!(
            "tip {} is below the first activation in the table; \
             the table must cover at least pre-Overwinter through the active upgrade",
            tip.value()
        ));
    };
    assert!(
        !active.name.is_empty(),
        "active_at(tip).name must be a non-empty upgrade name; got empty string"
    );
    Ok(())
}
