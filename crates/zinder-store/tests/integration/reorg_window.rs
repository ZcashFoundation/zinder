#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use eyre::eyre;
use tempfile::tempdir;
use zinder_core::{
    BlockHash, BlockHeaderArtifact, BlockHeight, BlockHeightRange, ChainEpoch, ChainEpochId,
    ChainTipMetadata, CompactBlockArtifact, Network, ShieldedProtocol, SubtreeRootArtifact,
    SubtreeRootHash, SubtreeRootIndex, SubtreeRootRange, TransactionId,
    TransactionIntrinsicValueBalances, TransactionIntrinsicValueBalancesArtifact,
    TreeStateArtifact, UnixTimestampMillis,
};
use zinder_store::{
    ChainEpochArtifacts, ChainEvent, ChainStoreOptions, PrimaryChainStore, ReorgWindowChange,
    StoreError,
};

#[test]
fn replacing_non_settled_tip_state_emits_chain_reorged() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;

    let (initial_epoch, _initial_block, _initial_compact_block) =
        synthetic_epoch(1, 2, 1, block_hash(2), block_hash(1));
    store.commit_chain_epoch(artifacts_from_height_one_to_tip(initial_epoch))?;

    let replacement_hash = block_hash(20);
    let (replacement_epoch, replacement_block, replacement_compact_block) =
        synthetic_epoch(2, 2, 1, replacement_hash, block_hash(1));
    let reorged = store.commit_chain_epoch(
        super::synthetic_chain_epoch_artifacts(
            replacement_epoch,
            vec![replacement_block.clone()],
            vec![replacement_compact_block],
        )
        .with_reorg_window_change(ReorgWindowChange::Replace {
            from_height: BlockHeight::new(2),
        }),
    )?;

    let ChainEvent::ChainReorged {
        reverted,
        committed,
    } = &reorged.event
    else {
        return Err(eyre!("expected ChainReorged, got {:?}", reorged.event));
    };

    assert_eq!(reorged.chain_epoch, replacement_epoch);
    assert_eq!(reverted.chain_epoch, initial_epoch);
    assert_eq!(reverted.block_range.start, BlockHeight::new(2));
    assert_eq!(reverted.block_range.end, BlockHeight::new(2));
    assert_eq!(committed.chain_epoch, replacement_epoch);

    let reader = store.current_chain_epoch_reader()?;
    assert_eq!(
        reader.block_header_at(BlockHeight::new(2))?,
        Some(replacement_block)
    );

    Ok(())
}

#[test]
fn replacement_cannot_cross_settled_tip_height() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;

    let (chain_epoch, _block, _compact_block) =
        synthetic_epoch(1, 2, 2, block_hash(2), block_hash(1));
    store.commit_chain_epoch(artifacts_from_height_one_to_tip(chain_epoch))?;

    let (attempted_epoch, attempted_block, attempted_compact_block) =
        synthetic_epoch(2, 2, 1, block_hash(20), block_hash(1));
    let error = match store.commit_chain_epoch(
        super::synthetic_chain_epoch_artifacts(
            attempted_epoch,
            vec![attempted_block],
            vec![attempted_compact_block],
        )
        .with_reorg_window_change(ReorgWindowChange::Replace {
            from_height: BlockHeight::new(2),
        }),
    ) {
        Ok(event) => return Err(eyre!("expected reorg-window error, got {event:?}")),
        Err(error) => error,
    };

    assert!(matches!(error, StoreError::ReorgWindowExceeded { .. }));
    assert_eq!(store.current_chain_epoch()?, Some(chain_epoch));

    Ok(())
}

#[test]
fn replacement_beyond_reorg_window_is_rejected() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;

    let (chain_epoch, _block, _compact_block) =
        synthetic_epoch(1, 102, 0, block_hash(102), block_hash(101));
    store.commit_chain_epoch(artifacts_from_height_one_to_tip(chain_epoch))?;

    let (attempted_epoch, attempted_block, attempted_compact_block) =
        synthetic_epoch(2, 102, 0, block_hash(202), block_hash(101));
    let error = match store.commit_chain_epoch(
        super::synthetic_chain_epoch_artifacts(
            attempted_epoch,
            vec![attempted_block],
            vec![attempted_compact_block],
        )
        .with_reorg_window_change(ReorgWindowChange::Replace {
            from_height: BlockHeight::new(2),
        }),
    ) {
        Ok(event) => return Err(eyre!("expected reorg-window error, got {event:?}")),
        Err(error) => error,
    };

    assert!(matches!(error, StoreError::ReorgWindowExceeded { .. }));
    assert_eq!(store.current_chain_epoch()?, Some(chain_epoch));

    Ok(())
}

#[test]
fn replacement_start_after_current_tip_is_rejected() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;

    let (initial_epoch, _initial_block, _initial_compact_block) =
        synthetic_epoch(1, 2, 1, block_hash(2), block_hash(1));
    store.commit_chain_epoch(artifacts_from_height_one_to_tip(initial_epoch))?;

    let (attempted_epoch, attempted_block, attempted_compact_block) =
        synthetic_epoch(2, 3, 1, block_hash(3), block_hash(2));
    let error = match store.commit_chain_epoch(
        super::synthetic_chain_epoch_artifacts(
            attempted_epoch,
            vec![attempted_block],
            vec![attempted_compact_block],
        )
        .with_reorg_window_change(ReorgWindowChange::Replace {
            from_height: BlockHeight::new(3),
        }),
    ) {
        Ok(event) => {
            return Err(eyre!("expected invalid replacement error, got {event:?}"));
        }
        Err(error) => error,
    };

    assert!(matches!(
        error,
        StoreError::InvalidChainEpochArtifacts { .. }
    ));
    assert_eq!(store.current_chain_epoch()?, Some(initial_epoch));

    Ok(())
}

#[test]
fn settled_tip_height_can_advance_without_new_block_artifacts() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;

    let (initial_epoch, _initial_block, _initial_compact_block) =
        synthetic_epoch(1, 2, 1, block_hash(2), block_hash(1));
    store.commit_chain_epoch(artifacts_from_height_one_to_tip(initial_epoch))?;
    let initial_reader = store.current_chain_epoch_reader()?;
    let visible_tip_block = initial_reader
        .block_header_at(BlockHeight::new(2))?
        .ok_or_else(|| eyre!("expected visible tip block"))?;

    let settled_tip_epoch = ChainEpoch {
        id: ChainEpochId::new(2),
        settled_tip_height: BlockHeight::new(2),
        settled_tip_hash: block_hash(2),
        created_at: UnixTimestampMillis::new(1_774_668_101_000),
        ..initial_epoch
    };
    let committed = store.commit_chain_epoch(
        super::synthetic_chain_epoch_artifacts(
            settled_tip_epoch,
            Vec::<zinder_core::BlockHeaderArtifact>::new(),
            Vec::new(),
        )
        .with_reorg_window_change(ReorgWindowChange::AdvanceSettledTipTo {
            height: BlockHeight::new(2),
        }),
    )?;

    assert_eq!(committed.chain_epoch, settled_tip_epoch);
    assert_eq!(
        committed.event_envelope.settled_tip_height,
        BlockHeight::new(2)
    );
    assert!(matches!(
        committed.event,
        ChainEvent::ChainCommitted { committed }
            if committed.chain_epoch == settled_tip_epoch
                && committed.block_range == BlockHeightRange::empty_at(BlockHeight::new(2))
    ));
    assert_eq!(store.current_chain_epoch()?, Some(settled_tip_epoch));
    let settled_tip_reader = store.current_chain_epoch_reader()?;
    assert_eq!(
        settled_tip_reader.block_header_at(BlockHeight::new(2))?,
        Some(visible_tip_block)
    );

    Ok(())
}

#[test]
fn unchanged_commit_without_visible_transition_is_rejected() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;

    let (initial_epoch, _initial_block, _initial_compact_block) =
        synthetic_epoch(1, 2, 1, block_hash(2), block_hash(1));
    store.commit_chain_epoch(artifacts_from_height_one_to_tip(initial_epoch))?;

    let attempted_epoch = ChainEpoch {
        id: ChainEpochId::new(2),
        created_at: UnixTimestampMillis::new(1_774_668_101_000),
        ..initial_epoch
    };
    let error = match store.commit_chain_epoch(
        super::synthetic_chain_epoch_artifacts(
            attempted_epoch,
            Vec::<zinder_core::BlockHeaderArtifact>::new(),
            Vec::new(),
        )
        .with_reorg_window_change(ReorgWindowChange::Unchanged),
    ) {
        Ok(event) => return Err(eyre!("expected no-op commit error, got {event:?}")),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        StoreError::InvalidChainEpochArtifacts { reason }
            if reason == "at least one settled-tip block artifact is required"
    ));
    assert_eq!(store.current_chain_epoch()?, Some(initial_epoch));

    Ok(())
}

#[test]
fn replacement_start_after_attempted_tip_reports_boundary_error() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;

    let (initial_epoch, _initial_block, _initial_compact_block) =
        synthetic_epoch(1, 5, 1, block_hash(5), block_hash(4));
    store.commit_chain_epoch(artifacts_from_height_one_to_tip(initial_epoch))?;

    let (attempted_epoch, attempted_block, attempted_compact_block) =
        synthetic_epoch(2, 4, 1, block_hash(40), block_hash(3));
    let error = match store.commit_chain_epoch(
        super::synthetic_chain_epoch_artifacts(
            attempted_epoch,
            vec![attempted_block],
            vec![attempted_compact_block],
        )
        .with_reorg_window_change(ReorgWindowChange::Replace {
            from_height: BlockHeight::new(5),
        }),
    ) {
        Ok(event) => {
            return Err(eyre!("expected invalid replacement error, got {event:?}"));
        }
        Err(error) => error,
    };

    assert!(matches!(
        error,
        StoreError::InvalidChainEpochArtifacts { reason }
            if reason == "replacement start height cannot exceed tip height"
    ));
    assert_eq!(store.current_chain_epoch()?, Some(initial_epoch));

    Ok(())
}

#[test]
fn advance_settled_tip_can_target_height_below_current_settled_tip() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;

    let (initial_epoch, _initial_block, _initial_compact_block) =
        synthetic_epoch(1, 3, 1, block_hash(3), block_hash(2));
    store.commit_chain_epoch(artifacts_from_height_one_to_tip(initial_epoch))?;

    let settled_tip_epoch = ChainEpoch {
        id: ChainEpochId::new(2),
        settled_tip_height: BlockHeight::new(3),
        settled_tip_hash: block_hash(3),
        created_at: UnixTimestampMillis::new(1_774_668_101_000),
        ..initial_epoch
    };
    let committed = store.commit_chain_epoch(
        super::synthetic_chain_epoch_artifacts(
            settled_tip_epoch,
            Vec::<zinder_core::BlockHeaderArtifact>::new(),
            Vec::new(),
        )
        .with_reorg_window_change(ReorgWindowChange::AdvanceSettledTipTo {
            height: BlockHeight::new(2),
        }),
    )?;

    assert_eq!(committed.chain_epoch, settled_tip_epoch);
    assert_eq!(store.current_chain_epoch()?, Some(settled_tip_epoch));

    Ok(())
}

#[test]
fn append_extend_window_ending_at_tip_is_valid() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;

    let (initial_epoch, _initial_block, _initial_compact_block) =
        synthetic_epoch(1, 2, 1, block_hash(2), block_hash(1));
    store.commit_chain_epoch(artifacts_from_height_one_to_tip(initial_epoch))?;

    let (attempted_epoch, appended_block, appended_compact_block) =
        synthetic_epoch(2, 3, 1, block_hash(3), block_hash(2));
    let committed = store.commit_chain_epoch(
        super::synthetic_chain_epoch_artifacts(
            attempted_epoch,
            vec![appended_block],
            vec![appended_compact_block],
        )
        .with_reorg_window_change(ReorgWindowChange::Extend {
            block_range: BlockHeightRange::inclusive(BlockHeight::new(3), BlockHeight::new(3)),
        }),
    )?;

    assert_eq!(committed.chain_epoch, attempted_epoch);
    assert_eq!(store.current_chain_epoch()?, Some(attempted_epoch));

    Ok(())
}

#[test]
fn extend_window_end_after_tip_is_rejected() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;

    let (initial_epoch, _initial_block, _initial_compact_block) =
        synthetic_epoch(1, 2, 1, block_hash(2), block_hash(1));
    store.commit_chain_epoch(artifacts_from_height_one_to_tip(initial_epoch))?;

    let (attempted_epoch, appended_block, appended_compact_block) =
        synthetic_epoch(2, 3, 1, block_hash(3), block_hash(2));
    let error = match store.commit_chain_epoch(
        super::synthetic_chain_epoch_artifacts(
            attempted_epoch,
            vec![appended_block],
            vec![appended_compact_block],
        )
        .with_reorg_window_change(ReorgWindowChange::Extend {
            block_range: BlockHeightRange::inclusive(BlockHeight::new(3), BlockHeight::new(4)),
        }),
    ) {
        Ok(event) => return Err(eyre!("expected extend-window error, got {event:?}")),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        StoreError::InvalidChainEpochArtifacts { reason }
            if reason == "reorg-window extension cannot exceed tip height"
    ));
    assert_eq!(store.current_chain_epoch()?, Some(initial_epoch));

    Ok(())
}

#[test]
fn replacement_cannot_lower_settled_tip_anchor() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;

    let (chain_epoch, _block, _compact_block) =
        synthetic_epoch(1, 6, 5, block_hash(6), block_hash(5));
    store.commit_chain_epoch(artifacts_from_height_one_to_tip(chain_epoch))?;

    let (attempted_epoch, attempted_block, attempted_compact_block) =
        synthetic_epoch(2, 6, 4, block_hash(60), block_hash(5));
    let error = match store.commit_chain_epoch(
        super::synthetic_chain_epoch_artifacts(
            attempted_epoch,
            vec![attempted_block],
            vec![attempted_compact_block],
        )
        .with_reorg_window_change(ReorgWindowChange::Replace {
            from_height: BlockHeight::new(6),
        }),
    ) {
        Ok(event) => return Err(eyre!("expected settled-tip-anchor error, got {event:?}")),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        StoreError::InvalidChainEpochArtifacts { .. }
    ));
    assert_eq!(store.current_chain_epoch()?, Some(chain_epoch));

    Ok(())
}

#[test]
fn replacement_cannot_change_settled_tip_anchor_hash() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;

    let (chain_epoch, _block, _compact_block) =
        synthetic_epoch(1, 6, 5, block_hash(6), block_hash(5));
    store.commit_chain_epoch(artifacts_from_height_one_to_tip(chain_epoch))?;

    let (mut attempted_epoch, attempted_block, attempted_compact_block) =
        synthetic_epoch(2, 6, 5, block_hash(60), block_hash(5));
    attempted_epoch.settled_tip_hash = block_hash(50);
    let error = match store.commit_chain_epoch(
        super::synthetic_chain_epoch_artifacts(
            attempted_epoch,
            vec![attempted_block],
            vec![attempted_compact_block],
        )
        .with_reorg_window_change(ReorgWindowChange::Replace {
            from_height: BlockHeight::new(6),
        }),
    ) {
        Ok(event) => return Err(eyre!("expected settled-tip-anchor error, got {event:?}")),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        StoreError::InvalidChainEpochArtifacts { .. }
    ));
    assert_eq!(store.current_chain_epoch()?, Some(chain_epoch));

    Ok(())
}

#[test]
fn non_replacement_commit_cannot_lower_tip_height() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;

    let (chain_epoch, _block, _compact_block) =
        synthetic_epoch(1, 2, 1, block_hash(2), block_hash(1));
    store.commit_chain_epoch(artifacts_from_height_one_to_tip(chain_epoch))?;

    let (attempted_epoch, attempted_block, attempted_compact_block) =
        synthetic_epoch(2, 1, 1, block_hash(1), block_hash(0));
    let error = match store.commit_chain_epoch(
        super::synthetic_chain_epoch_artifacts(
            attempted_epoch,
            vec![attempted_block],
            vec![attempted_compact_block],
        )
        .with_reorg_window_change(ReorgWindowChange::Extend {
            block_range: BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(1)),
        }),
    ) {
        Ok(event) => return Err(eyre!("expected invalid epoch error, got {event:?}")),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        StoreError::InvalidChainEpochArtifacts { .. }
    ));
    assert_eq!(store.current_chain_epoch()?, Some(chain_epoch));

    Ok(())
}

#[test]
fn non_replacement_commit_cannot_lower_settled_tip_height() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;

    let (chain_epoch, _block, _compact_block) =
        synthetic_epoch(1, 2, 2, block_hash(2), block_hash(1));
    store.commit_chain_epoch(artifacts_from_height_one_to_tip(chain_epoch))?;

    let (attempted_epoch, attempted_block, attempted_compact_block) =
        synthetic_epoch(2, 3, 1, block_hash(3), block_hash(2));
    let error = match store.commit_chain_epoch(super::synthetic_chain_epoch_artifacts(
        attempted_epoch,
        vec![attempted_block],
        vec![attempted_compact_block],
    )) {
        Ok(event) => return Err(eyre!("expected invalid epoch error, got {event:?}")),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        StoreError::InvalidChainEpochArtifacts { .. }
    ));
    assert_eq!(store.current_chain_epoch()?, Some(chain_epoch));

    Ok(())
}

#[test]
fn append_commit_cannot_publish_block_for_existing_visible_height() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;

    let (initial_epoch, _block, _compact_block) =
        synthetic_epoch(1, 2, 1, block_hash(2), block_hash(1));
    store.commit_chain_epoch(artifacts_from_height_one_to_tip(initial_epoch))?;

    let (attempted_epoch, appended_block, appended_compact_block) =
        synthetic_epoch(2, 3, 1, block_hash(3), block_hash(2));
    let rewritten_block = super::synthetic_block_header(
        BlockHeight::new(2),
        block_hash(20),
        block_hash(1),
        b"rewritten-block-2",
    );
    let error = match store.commit_chain_epoch(super::synthetic_chain_epoch_artifacts(
        attempted_epoch,
        vec![rewritten_block, appended_block],
        vec![appended_compact_block],
    )) {
        Ok(event) => return Err(eyre!("expected invalid epoch error, got {event:?}")),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        StoreError::InvalidChainEpochArtifacts { .. }
    ));
    assert_eq!(store.current_chain_epoch()?, Some(initial_epoch));

    Ok(())
}

#[test]
fn append_commit_must_link_first_new_block_to_current_tip() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;

    let (initial_epoch, _block, _compact_block) =
        synthetic_epoch(1, 2, 1, block_hash(2), block_hash(1));
    store.commit_chain_epoch(artifacts_from_height_one_to_tip(initial_epoch))?;

    let (attempted_epoch, appended_block, appended_compact_block) =
        synthetic_epoch(2, 3, 1, block_hash(3), block_hash(99));
    let error = match store.commit_chain_epoch(super::synthetic_chain_epoch_artifacts(
        attempted_epoch,
        vec![appended_block],
        vec![appended_compact_block],
    )) {
        Ok(event) => return Err(eyre!("expected invalid epoch error, got {event:?}")),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        StoreError::InvalidChainEpochArtifacts { .. }
    ));
    assert_eq!(store.current_chain_epoch()?, Some(initial_epoch));

    Ok(())
}

#[test]
fn settled_tip_hash_must_match_existing_visible_block() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;

    let (initial_epoch, _block, _compact_block) =
        synthetic_epoch(1, 2, 1, block_hash(2), block_hash(1));
    store.commit_chain_epoch(artifacts_from_height_one_to_tip(initial_epoch))?;

    let (mut attempted_epoch, appended_block, appended_compact_block) =
        synthetic_epoch(2, 3, 2, block_hash(3), block_hash(2));
    attempted_epoch.settled_tip_hash = block_hash(99);
    let error = match store.commit_chain_epoch(super::synthetic_chain_epoch_artifacts(
        attempted_epoch,
        vec![appended_block],
        vec![appended_compact_block],
    )) {
        Ok(event) => return Err(eyre!("expected invalid epoch error, got {event:?}")),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        StoreError::InvalidChainEpochArtifacts { .. }
    ));
    assert_eq!(store.current_chain_epoch()?, Some(initial_epoch));

    Ok(())
}

#[test]
fn transaction_lookup_ignores_reverted_branch_artifacts() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;

    let transaction_id = TransactionId::from_bytes([9; 32]);
    let old_hash = block_hash(2);
    let (transaction_index, transaction_location, old_transaction_facts, _) =
        super::synthetic_transaction_rows(
            transaction_id,
            BlockHeight::new(2),
            old_hash,
            0,
            b"old-transaction",
        );
    let (initial_epoch, _initial_block, _initial_compact_block) =
        synthetic_epoch(1, 2, 1, old_hash, block_hash(1));
    store.commit_chain_epoch(super::with_synthetic_block_replay_envelopes(
        artifacts_from_height_one_to_tip(initial_epoch)
            .with_block_transaction_index(vec![transaction_index])
            .with_transaction_locations(vec![transaction_location])
            .with_transaction_facts(vec![old_transaction_facts.clone()])
            .with_transaction_intrinsic_value_balances(vec![
                TransactionIntrinsicValueBalancesArtifact::new(
                    transaction_location,
                    TransactionIntrinsicValueBalances::default(),
                ),
            ]),
    ))?;

    let initial_reader = store.current_chain_epoch_reader()?;
    assert_eq!(
        initial_reader.transaction_facts_by_id(transaction_id)?,
        Some(old_transaction_facts.clone())
    );

    let replacement_hash = block_hash(20);
    let (replacement_epoch, replacement_block, replacement_compact_block) =
        synthetic_epoch(2, 2, 1, replacement_hash, block_hash(1));
    store.commit_chain_epoch(
        super::synthetic_chain_epoch_artifacts(
            replacement_epoch,
            vec![replacement_block],
            vec![replacement_compact_block],
        )
        .with_reorg_window_change(ReorgWindowChange::Replace {
            from_height: BlockHeight::new(2),
        }),
    )?;

    assert_eq!(
        initial_reader.transaction_facts_by_id(transaction_id)?,
        Some(old_transaction_facts)
    );

    let replacement_reader = store.current_chain_epoch_reader()?;
    assert_eq!(
        replacement_reader.transaction_facts_by_id(transaction_id)?,
        None
    );
    // Complete history makes the reorged-out transaction a proven absence at
    // the replacement epoch, while the initial epoch still holds it.
    assert_eq!(
        replacement_reader.transaction_facts_or_proven_absence(transaction_id)?,
        None
    );
    assert!(
        initial_reader
            .transaction_facts_or_proven_absence(transaction_id)?
            .is_some()
    );
    assert_eq!(
        replacement_reader
            .transaction_facts_or_proven_absence(TransactionId::from_bytes([0x44; 32]))?,
        None
    );

    Ok(())
}

#[test]
fn subtree_root_lookup_ignores_reverted_branch_artifacts() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;

    let old_hash = block_hash(2);
    let old_subtree_root = SubtreeRootArtifact::new(
        ShieldedProtocol::Sapling,
        SubtreeRootIndex::new(0),
        SubtreeRootHash::from_bytes([7; 32]),
        BlockHeight::new(2),
        old_hash,
    );
    let subtree_range = SubtreeRootRange::new(
        ShieldedProtocol::Sapling,
        SubtreeRootIndex::new(0),
        std::num::NonZeroU32::new(1).ok_or_else(|| eyre!("invalid subtree root range"))?,
    );
    let (initial_epoch, _initial_block, _initial_compact_block) =
        synthetic_epoch(1, 2, 1, old_hash, block_hash(1));
    store.commit_chain_epoch(
        artifacts_from_height_one_to_tip(initial_epoch)
            .with_subtree_roots(vec![old_subtree_root.clone()]),
    )?;

    let initial_reader = store.current_chain_epoch_reader()?;
    assert_eq!(
        initial_reader.subtree_roots(subtree_range)?,
        vec![Some(old_subtree_root.clone())]
    );

    let replacement_hash = block_hash(20);
    let (replacement_epoch, replacement_block, replacement_compact_block) =
        synthetic_epoch(2, 2, 1, replacement_hash, block_hash(1));
    store.commit_chain_epoch(
        super::synthetic_chain_epoch_artifacts(
            replacement_epoch,
            vec![replacement_block],
            vec![replacement_compact_block],
        )
        .with_reorg_window_change(ReorgWindowChange::Replace {
            from_height: BlockHeight::new(2),
        }),
    )?;

    assert_eq!(
        initial_reader.subtree_roots(subtree_range)?,
        vec![Some(old_subtree_root)]
    );

    let replacement_reader = store.current_chain_epoch_reader()?;
    assert_eq!(replacement_reader.subtree_roots(subtree_range)?, vec![None]);

    Ok(())
}

#[test]
fn tree_state_lookup_ignores_reverted_branch_artifacts() -> eyre::Result<()> {
    let tempdir = tempdir()?;
    let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;

    let old_hash = block_hash(2);
    let old_tree_state =
        TreeStateArtifact::new(BlockHeight::new(2), old_hash, 0, b"old-tree-state".to_vec());
    let (initial_epoch, _initial_block, _initial_compact_block) =
        synthetic_epoch(1, 2, 1, old_hash, block_hash(1));
    store.commit_chain_epoch(
        artifacts_from_height_one_to_tip(initial_epoch)
            .with_tree_states(vec![old_tree_state.clone()]),
    )?;

    let initial_reader = store.current_chain_epoch_reader()?;
    assert_eq!(
        initial_reader.tree_state_checkpoint_at_or_before(BlockHeight::new(2))?,
        Some(old_tree_state.clone())
    );

    let replacement_hash = block_hash(20);
    let (replacement_epoch, replacement_block, replacement_compact_block) =
        synthetic_epoch(2, 2, 1, replacement_hash, block_hash(1));
    store.commit_chain_epoch(
        super::synthetic_chain_epoch_artifacts(
            replacement_epoch,
            vec![replacement_block],
            vec![replacement_compact_block],
        )
        .with_reorg_window_change(ReorgWindowChange::Replace {
            from_height: BlockHeight::new(2),
        }),
    )?;

    assert_eq!(
        initial_reader.tree_state_checkpoint_at_or_before(BlockHeight::new(2))?,
        Some(old_tree_state)
    );

    let replacement_reader = store.current_chain_epoch_reader()?;
    assert_eq!(
        replacement_reader.tree_state_checkpoint_at_or_before(BlockHeight::new(2))?,
        None
    );

    Ok(())
}

fn synthetic_epoch(
    chain_epoch_id: u64,
    height: u32,
    settled_tip_height: u32,
    hash: BlockHash,
    parent_hash: BlockHash,
) -> (ChainEpoch, BlockHeaderArtifact, CompactBlockArtifact) {
    let block_height = BlockHeight::new(height);
    let block = super::synthetic_block_header(
        block_height,
        hash,
        parent_hash,
        format!("raw-block-{chain_epoch_id}-{height}").as_bytes(),
    );
    let compact = super::empty_compact_block_for_header(&block, ChainTipMetadata::empty());

    (
        ChainEpoch {
            id: ChainEpochId::new(chain_epoch_id),
            network: Network::ZcashRegtest,
            visible_tip_height: block_height,
            visible_tip_hash: hash,
            settled_tip_height: BlockHeight::new(settled_tip_height),
            settled_tip_hash: block_hash(settled_tip_height),
            artifact_schema_version: zinder_store::CURRENT_ARTIFACT_SCHEMA_VERSION,
            tip_metadata: ChainTipMetadata::empty(),
            created_at: UnixTimestampMillis::new(1_774_668_100_000 + u64::from(height)),
        },
        block,
        compact,
    )
}

fn artifacts_from_height_one_to_tip(chain_epoch: ChainEpoch) -> ChainEpochArtifacts {
    let mut blocks = Vec::new();
    let mut compact_blocks = Vec::new();

    for height in 1..=chain_epoch.visible_tip_height.value() {
        let block_height = BlockHeight::new(height);
        let block_hash = if block_height == chain_epoch.visible_tip_height {
            chain_epoch.visible_tip_hash
        } else {
            self::block_hash(height)
        };
        let parent_hash = self::block_hash(height.saturating_sub(1));

        let block = super::synthetic_block_header(
            block_height,
            block_hash,
            parent_hash,
            format!("raw-block-{}-{height}", chain_epoch.id.value()).as_bytes(),
        );
        compact_blocks.push(super::empty_compact_block_for_header(
            &block,
            ChainTipMetadata::empty(),
        ));
        blocks.push(block);
    }

    super::synthetic_chain_epoch_artifacts(chain_epoch, blocks, compact_blocks)
}

fn block_hash(seed: u32) -> BlockHash {
    let mut bytes = [0; 32];
    for chunk in bytes.chunks_exact_mut(4) {
        chunk.copy_from_slice(&seed.to_be_bytes());
    }
    BlockHash::from_bytes(bytes)
}
