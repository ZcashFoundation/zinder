//! Probe compiled against both revisions by scripts/test-canonical-v7-interop.sh.

use std::{fs, num::NonZeroU32, path::Path};

use rust_rocksdb::{DB, IteratorMode};
use sha2::{Digest, Sha256};
use zinder_core::{
    BlockHash, BlockHeight, BlockId, ChainEpochId, CommitmentTreeCheckpoint,
    CommitmentTreeFrontiers, CompactBlockArtifact, CompactChainMetadata, Network,
    NetworkUpgradeActivations, SUBTREE_LEAF_COUNT, ShieldedProtocol, SubtreeRootArtifact,
    SubtreeRootHash, SubtreeRootIndex, SubtreeRootRange, TransactionId, UnixTimestampMillis,
};

use super::live_replacement_tests::replacement_block_with_retention;
use crate::{
    BlockHashLookup, CANONICAL_STORE_SCHEMA_VERSION, CanonicalBaselinePublication,
    CanonicalBuildBlock, CanonicalEventHistoryRequest, CanonicalLiveReplacement,
    CanonicalReorgPolicy, CanonicalReplacementBlock, CanonicalStoreBuildPlan,
    CanonicalStoreWorkload, MempoolEvent, MempoolEventHistoryRequest, RawBlobRetention,
    RocksDbCanonicalBuilder, RocksDbCanonicalSecondary, RocksDbCanonicalStore,
    RocksDbResourceBudget,
};

type ProbeResult<T> = Result<T, Box<dyn std::error::Error>>;

const PREDECESSOR: BlockId = BlockId::new(BlockHeight::new(99), BlockHash::from_bytes([99; 32]));
const SETTLED: BlockId = BlockId::new(BlockHeight::new(100), BlockHash::from_bytes([100; 32]));
const BUILD_TIP: BlockId = BlockId::new(BlockHeight::new(101), BlockHash::from_bytes([101; 32]));
const REPLACED_TIP: BlockId =
    BlockId::new(BlockHeight::new(101), BlockHash::from_bytes([0x44; 32]));
const OBSERVED_AT: UnixTimestampMillis = UnixTimestampMillis::new(1_750_000_000_001);
const MINED_ID: TransactionId = TransactionId::from_bytes([0x44; 32]);

#[test]
fn v7_interop() -> ProbeResult<()> {
    assert_eq!(CANONICAL_STORE_SCHEMA_VERSION, 7);
    let root = std::path::PathBuf::from(std::env::var("ZINDER_V7_INTEROP_ROOT")?);
    let create = std::env::var("ZINDER_V7_INTEROP_CREATE")?;
    let read = std::env::var("ZINDER_V7_INTEROP_READ")?;
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    for retention in [
        RawBlobRetention::None,
        RawBlobRetention::Transactions,
        RawBlobRetention::All,
    ] {
        if !create.is_empty() {
            let fixture = root.join(format!("{create}-{retention}"));
            fs::create_dir(&fixture)?;
            let writer = create_store(&fixture.join("primary"), &activations, retention)?;
            save_or_check_rows(&writer.bounded_open.db, &fixture, retention, true)?;
        }
        if !read.is_empty() {
            read_store(
                &root.join(format!("{read}-{retention}")),
                &activations,
                retention,
            )?;
        }
    }
    Ok(())
}

fn root_zero() -> SubtreeRootArtifact {
    SubtreeRootArtifact::new(
        ShieldedProtocol::Sapling,
        SubtreeRootIndex::new(0),
        SubtreeRootHash::from_bytes([0x51; 32]),
        PREDECESSOR.height,
        PREDECESSOR.hash,
    )
}

fn fixture_block(
    block: BlockId,
    parent: BlockId,
    transaction_tag: u8,
    retention: RawBlobRetention,
    frontiers: &CommitmentTreeFrontiers,
) -> ProbeResult<CanonicalBuildBlock> {
    let mut prepared = replacement_block_with_retention(
        block.height,
        block.hash.as_bytes(),
        parent.hash.as_bytes(),
        transaction_tag,
        retention,
    )?;
    prepared.tip_metadata = frontiers.tip_metadata();
    prepared.compact_block = CompactBlockArtifact::empty(
        block,
        parent.hash,
        block.height.value(),
        CompactChainMetadata {
            sapling_commitment_tree_size: SUBTREE_LEAF_COUNT,
            orchard_commitment_tree_size: 0,
            ironwood_commitment_tree_size: 0,
        },
    );
    prepared.tree_state_checkpoint = Some(CommitmentTreeCheckpoint::new(
        block,
        block.height.value(),
        frontiers.clone(),
    ));
    Ok(prepared)
}

fn create_store(
    path: &Path,
    activations: &NetworkUpgradeActivations,
    retention: RawBlobRetention,
) -> ProbeResult<RocksDbCanonicalStore> {
    let empty = super::test_checkpoint_frontiers(activations, PREDECESSOR.height)?;
    let frontiers = CommitmentTreeFrontiers::from_validated_parts(
        Some(zinder_testkit::frontier_of_size(
            ShieldedProtocol::Sapling,
            SUBTREE_LEAF_COUNT,
        )?),
        empty.get(ShieldedProtocol::Orchard).cloned(),
        empty.get(ShieldedProtocol::Ironwood).cloned(),
    )?;
    let plan = CanonicalStoreBuildPlan::checkpointed(
        activations,
        CommitmentTreeCheckpoint::new(PREDECESSOR, PREDECESSOR.height.value(), frontiers.clone()),
        BUILD_TIP,
        retention,
        CanonicalReorgPolicy::new(2)?,
    )?;
    let mut builder = RocksDbCanonicalBuilder::create_fresh(
        path,
        CanonicalStoreWorkload::Wallet,
        plan,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    builder.bulk_load_blocks(
        activations,
        [
            Ok::<_, std::io::Error>(fixture_block(
                SETTLED,
                PREDECESSOR,
                100,
                retention,
                &frontiers,
            )?),
            Ok::<_, std::io::Error>(fixture_block(
                BUILD_TIP, SETTLED, 101, retention, &frontiers,
            )?),
        ],
    )?;
    builder.load_complete_subtree_root_prefix([root_zero()])?;
    builder.confirm_source_tip_checkpoint(&CommitmentTreeCheckpoint::new(
        BUILD_TIP,
        BUILD_TIP.height.value(),
        frontiers.clone(),
    ))?;
    let validated = builder.prepare_cold_certified_publication()?;
    let baseline =
        validated.prepare_baseline(CanonicalBaselinePublication::new(SETTLED, OBSERVED_AT))?;
    let mut writer = validated.publish_baseline(baseline)?;
    writer.admit_network_upgrade_schedule(activations)?;
    let replacement = fixture_block(REPLACED_TIP, SETTLED, 0x44, retention, &frontiers)?;
    let update = CanonicalLiveReplacement::new(
        writer.event_fence(),
        vec![CanonicalReplacementBlock::new(replacement, Vec::new())],
        UnixTimestampMillis::new(OBSERVED_AT.value() + 1),
    );
    let (writer, _) = writer.commit_live_replacement(update, activations)?;
    writer.append_mempool_event(
        MempoolEvent::Mined {
            transaction_id: MINED_ID,
            mined_height: REPLACED_TIP.height,
            block_hash: REPLACED_TIP.hash,
        },
        OBSERVED_AT,
    )?;
    Ok(writer)
}

fn save_or_check_rows(
    db: &DB,
    fixture: &Path,
    retention: RawBlobRetention,
    save: bool,
) -> ProbeResult<()> {
    let families = db.cf_names();
    assert_eq!(families.len(), 16);
    for name in families {
        let family = db.cf_handle(&name).ok_or("missing v7 family")?;
        let populated = match name.as_str() {
            "block_final_note_commitment_roots" | "daily_value_pool_balance" => false,
            "block_blob" => retention == RawBlobRetention::All,
            "transaction_blob" => retention != RawBlobRetention::None,
            _ => true,
        };
        let row = db
            .iterator_cf(&family, IteratorMode::Start)
            .next()
            .transpose()?;
        assert_eq!(row.is_some(), populated, "{name} under {retention}");
        if let Some((key, bytes)) = row {
            let mut digest = Sha256::new();
            digest.update(key.len().to_le_bytes());
            digest.update(key);
            digest.update(bytes);
            let fingerprint = digest.finalize();
            let receipt = fixture.join(name);
            if save {
                fs::write(receipt, fingerprint)?;
            } else {
                assert_eq!(
                    fs::read(receipt)?,
                    fingerprint.as_slice(),
                    "representative row changed"
                );
            }
        }
    }
    Ok(())
}

macro_rules! check_canonical_rows {
    ($reader:ident, $activations:ident, $retention:ident) => {{
        let height = REPLACED_TIP.height;
        assert_eq!($reader.chain_epoch()?.id, ChainEpochId::new(2));
        assert_eq!(
            $reader
                .block_header_at(height)?
                .ok_or("missing header")?
                .block_hash,
            REPLACED_TIP.hash
        );
        assert_eq!(
            $reader.block_hash_lookup(REPLACED_TIP.hash)?,
            BlockHashLookup::Resolved(REPLACED_TIP)
        );
        assert_eq!(
            $reader
                .compact_block_at(height)?
                .ok_or("missing compact block")?
                .block_hash(),
            REPLACED_TIP.hash
        );
        assert_eq!(
            $reader
                .tree_state_checkpoint_at(height, $activations)?
                .ok_or("missing checkpoint")?
                .block_id,
            REPLACED_TIP
        );
        let location = $reader
            .transaction_location(MINED_ID)?
            .ok_or("missing transaction location")?;
        assert_eq!(location.block_height, height);
        assert_eq!(
            $reader
                .transaction_blob(location)?
                .map(|blob| blob.raw_transaction_bytes),
            ($retention != RawBlobRetention::None).then(|| vec![0x44])
        );
        assert_eq!(
            $reader
                .block_blob_at(height)?
                .map(|blob| blob.raw_block_bytes),
            ($retention == RawBlobRetention::All).then(|| vec![0x44])
        );
        let roots = $reader.subtree_roots(SubtreeRootRange::new(
            ShieldedProtocol::Sapling,
            SubtreeRootIndex::new(0),
            NonZeroU32::MIN,
        ))?;
        assert_eq!(roots, [root_zero()]);
        assert_eq!(
            $reader
                .canonical_event_history(CanonicalEventHistoryRequest::new(
                    None,
                    NonZeroU32::new(2).ok_or("zero limit")?
                ))?
                .len(),
            2
        );
    }};
}

fn read_store(
    fixture: &Path,
    activations: &NetworkUpgradeActivations,
    retention: RawBlobRetention,
) -> ProbeResult<()> {
    let path = fixture.join("primary");
    let reorg = CanonicalReorgPolicy::new(2)?;
    let budget = RocksDbResourceBudget::for_local_tests();
    let writer = RocksDbCanonicalStore::open_ready(
        &path,
        activations,
        CanonicalStoreWorkload::Wallet,
        retention,
        reorg,
        budget,
    )?;
    check_canonical_rows!(writer, activations, retention);
    assert_eq!(writer.scan_canonical_replay()?.count(), 2);
    assert_eq!(
        writer
            .displaced_block_by_hash(BUILD_TIP.hash)?
            .ok_or("missing displaced block")?
            .header
            .height,
        BUILD_TIP.height
    );
    let events =
        writer.mempool_event_history(MempoolEventHistoryRequest::with_default_limit(None))?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].transaction_id(), MINED_ID);
    save_or_check_rows(&writer.bounded_open.db, fixture, retention, false)?;

    let mut secondary = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
        &path,
        fixture.join("secondary"),
        Network::ZcashTestnet,
        CanonicalStoreWorkload::Wallet,
        retention,
        reorg,
        budget,
    )?;
    secondary.try_catch_up()?;
    check_canonical_rows!(secondary, activations, retention);
    let replay = secondary
        .block_replay_facts_at(REPLACED_TIP.height)?
        .ok_or("missing replay facts")?;
    assert_eq!(replay.block_header.block_hash, REPLACED_TIP.hash);
    save_or_check_rows(&secondary.bounded_open.db, fixture, retention, false)?;
    Ok(())
}
