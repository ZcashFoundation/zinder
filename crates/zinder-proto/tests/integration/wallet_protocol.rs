#![allow(
    missing_docs,
    reason = "Integration test names describe the native protocol contract under test."
)]

use eyre::eyre;
use prost::Message;
use zinder_core::wire::{
    decode_rpc_block_hash_hex, decode_rpc_transaction_id_hex, encode_rpc_block_hash_hex,
    encode_rpc_transaction_id_hex,
};
use zinder_core::{
    BlockHash, CompactSaplingOutput, CompactSaplingSpend, CompactShieldedAction,
    CompactTransactionData, CompactTransparentInput, CompactTransparentOutput, MempoolEntry,
    MempoolObservation, RawTransactionBytes, TransactionId, UnixTimestampMillis,
};
use zinder_proto::v1::{ingest, ops, wallet};

#[test]
fn chain_epoch_round_trips_through_prost() -> eyre::Result<()> {
    let chain_epoch = synthetic_chain_epoch();
    let decoded_chain_epoch = round_trip(&chain_epoch)?;

    assert_eq!(decoded_chain_epoch.chain_epoch_id, 7);
    assert_eq!(decoded_chain_epoch.network_name, "zcash-regtest");
    let visible_tip = decoded_chain_epoch
        .visible_tip
        .ok_or_else(|| eyre!("visible_tip missing"))?;
    let settled_tip = decoded_chain_epoch
        .settled_tip
        .ok_or_else(|| eyre!("settled_tip missing"))?;
    assert_eq!(visible_tip.height, 42);
    assert_eq!(visible_tip.hash, "11".repeat(32));
    assert_eq!(settled_tip.height, 40);
    assert_eq!(settled_tip.hash, "22".repeat(32));
    assert_eq!(decoded_chain_epoch.ironwood_commitment_tree_size, 99);

    Ok(())
}

#[test]
fn chain_view_round_trips_through_prost() -> eyre::Result<()> {
    let chain_view = wallet::ChainView {
        chain_epoch: Some(synthetic_chain_epoch()),
        indexed_tip: Some(wallet::IndexedTip {
            tip: Some(wallet::BlockTip {
                height: 41,
                hash: "33".repeat(32),
            }),
            block_time_unix_seconds: 1_774_670_000,
        }),
        upstream_tip: Some(wallet::UpstreamTip {
            committed_height: Some(42),
            estimated_height: Some(44),
        }),
        materialized_views: Some(wallet::MaterializedViewStatus {
            health: wallet::MaterializedViewHealth::CatchingUp as i32,
            indexed_height: 41,
            lag_blocks: 1,
            observed_at_millis: 1_774_670_400_000,
        }),
    };
    let decoded = round_trip(&chain_view)?;

    assert!(decoded.chain_epoch.is_some());
    let indexed_tip = decoded
        .indexed_tip
        .ok_or_else(|| eyre!("indexed_tip missing"))?;
    assert_eq!(
        indexed_tip
            .tip
            .ok_or_else(|| eyre!("indexed tip missing"))?
            .height,
        41
    );
    let upstream_tip = decoded
        .upstream_tip
        .ok_or_else(|| eyre!("upstream_tip missing"))?;
    assert_eq!(upstream_tip.committed_height, Some(42));
    assert_eq!(upstream_tip.estimated_height, Some(44));
    assert_eq!(
        decoded
            .materialized_views
            .ok_or_else(|| eyre!("materialized-view status missing"))?
            .indexed_height,
        41
    );

    Ok(())
}

#[test]
fn compact_block_round_trips_through_prost() -> eyre::Result<()> {
    let compact_block = wallet::CompactBlock {
        height: 42,
        block_hash: "33".repeat(32),
        previous_block_hash: "22".repeat(32),
        time: 1_774_670_000,
        transactions: vec![wallet::CompactTransaction {
            index: 3,
            transaction_id: vec![0x44; 32],
            data: Some(wallet::CompactTransactionData {
                fee_zat: Some(1_000),
                sapling_spends: vec![wallet::CompactSaplingSpend {
                    nullifier: vec![0x45; 32],
                }],
                sapling_outputs: vec![wallet::CompactSaplingOutput {
                    commitment: vec![0x46; 32],
                    ephemeral_key: vec![0x47; 32],
                    ciphertext: vec![0x48; 52],
                }],
                orchard_actions: vec![wallet::CompactShieldedAction {
                    nullifier: vec![0x49; 32],
                    commitment: vec![0x4a; 32],
                    ephemeral_key: vec![0x4b; 32],
                    ciphertext: vec![0x4c; 52],
                }],
                ironwood_actions: vec![wallet::CompactShieldedAction {
                    nullifier: vec![0x4d; 32],
                    commitment: vec![0x4e; 32],
                    ephemeral_key: vec![0x4f; 32],
                    ciphertext: vec![0x50; 52],
                }],
                transparent_inputs: vec![wallet::CompactTransparentInput {
                    previous_transaction_id: vec![0x51; 32],
                    previous_output_index: 7,
                }],
                transparent_outputs: vec![wallet::CompactTransparentOutput {
                    value_zat: 2_000,
                    script_pub_key: vec![0x52, 0x53],
                }],
            }),
        }],
        chain_metadata: Some(wallet::CompactChainMetadata {
            sapling_commitment_tree_size: 10,
            orchard_commitment_tree_size: 11,
            ironwood_commitment_tree_size: 12,
        }),
    };
    let decoded_compact_block = round_trip(&compact_block)?;

    assert_eq!(decoded_compact_block, compact_block);

    Ok(())
}

#[test]
fn compact_block_decoder_rejects_non_increasing_transaction_indexes() {
    let data = wallet::CompactTransactionData::default();
    let block = wallet::CompactBlock {
        height: 42,
        block_hash: "33".repeat(32),
        previous_block_hash: "22".repeat(32),
        time: 1,
        transactions: vec![
            wallet::CompactTransaction {
                index: 4,
                transaction_id: vec![0x44; 32],
                data: Some(data.clone()),
            },
            wallet::CompactTransaction {
                index: 4,
                transaction_id: vec![0x45; 32],
                data: Some(data),
            },
        ],
        chain_metadata: Some(wallet::CompactChainMetadata::default()),
    };

    assert_eq!(
        zinder_proto::wire::compact_block_from_message(block),
        Err(zinder_proto::wire::WalletWireDecodeError::InvalidCompactTransactionOrder)
    );
}

#[test]
fn chain_epoch_decoder_rejects_settled_tip_above_visible_tip() -> eyre::Result<()> {
    let mut epoch = synthetic_chain_epoch();
    epoch
        .settled_tip
        .as_mut()
        .ok_or_else(|| eyre!("fixture settled tip missing"))?
        .height = 43;
    assert_eq!(
        zinder_proto::wire::chain_epoch_from_message(epoch),
        Err(zinder_proto::wire::WalletWireDecodeError::InvalidChainEpoch)
    );
    Ok(())
}

#[test]
fn chain_epoch_decoder_rejects_distinct_hashes_at_same_height() -> eyre::Result<()> {
    let mut epoch = synthetic_chain_epoch();
    let visible = epoch
        .visible_tip
        .as_ref()
        .ok_or_else(|| eyre!("fixture visible tip missing"))?
        .clone();
    let settled = epoch
        .settled_tip
        .as_mut()
        .ok_or_else(|| eyre!("fixture settled tip missing"))?;
    settled.height = visible.height;
    settled.hash = "99".repeat(32);
    assert_eq!(
        zinder_proto::wire::chain_epoch_from_message(epoch),
        Err(zinder_proto::wire::WalletWireDecodeError::InvalidChainEpoch)
    );
    Ok(())
}

#[test]
fn mempool_entry_round_trips_shared_all_pool_scan_data() -> eyre::Result<()> {
    let transaction_id = TransactionId::from_bytes([0x44; 32]);
    let scan_data = CompactTransactionData {
        fee_zat: None,
        sapling_spends: vec![CompactSaplingSpend {
            nullifier: [0x45; 32],
        }],
        sapling_outputs: vec![CompactSaplingOutput {
            commitment: [0x46; 32],
            ephemeral_key: [0x47; 32],
            ciphertext: [0x48; 52],
        }],
        orchard_actions: vec![CompactShieldedAction {
            nullifier: [0x49; 32],
            commitment: [0x4a; 32],
            ephemeral_key: [0x4b; 32],
            ciphertext: [0x4c; 52],
        }],
        ironwood_actions: vec![CompactShieldedAction {
            nullifier: [0x4d; 32],
            commitment: [0x4e; 32],
            ephemeral_key: [0x4f; 32],
            ciphertext: [0x50; 52],
        }],
        transparent_inputs: vec![CompactTransparentInput {
            previous_transaction_id: TransactionId::from_bytes([0x51; 32]),
            previous_output_index: 7,
        }],
        transparent_outputs: vec![CompactTransparentOutput {
            value_zat: 2_000,
            script_pub_key: vec![0x52, 0x53],
        }],
    };
    let entry = MempoolEntry::new(
        transaction_id,
        None,
        RawTransactionBytes::new(vec![0x01, 0x02]),
        scan_data,
        MempoolObservation {
            first_seen_unix_millis: UnixTimestampMillis::new(1_774_670_000_000),
            first_seen_chain_epoch: zinder_proto::wire::chain_epoch_from_message(
                synthetic_chain_epoch(),
            )?,
        },
    )?;

    let message = zinder_proto::wire::mempool_entry_message(&entry);
    let scan_message = message
        .compact_transaction_data
        .as_ref()
        .ok_or_else(|| eyre!("compact transaction data missing"))?;
    assert_eq!(scan_message.fee_zat, None);
    assert_eq!(scan_message.orchard_actions.len(), 1);
    assert_eq!(scan_message.ironwood_actions.len(), 1);
    assert_eq!(message.transaction_id, "44".repeat(32));
    assert_eq!(message.transparent_outputs.len(), 1);
    assert_eq!(message.transparent_spends.len(), 1);

    let decoded = zinder_proto::wire::mempool_entry_from_message(round_trip(&message)?)?;
    assert_eq!(decoded, entry);
    Ok(())
}

#[test]
fn full_block_round_trips_through_prost() -> eyre::Result<()> {
    let full_block = wallet::FullBlock {
        height: 42,
        block_hash: "33".repeat(32),
        payload_bytes: vec![0x04, 0x05, 0x06],
        parent_block_hash: "22".repeat(32),
    };
    let decoded_full_block = round_trip(&full_block)?;

    assert_eq!(decoded_full_block.height, 42);
    assert_eq!(decoded_full_block.block_hash, "33".repeat(32));
    assert_eq!(decoded_full_block.payload_bytes, vec![0x04, 0x05, 0x06]);
    assert_eq!(decoded_full_block.parent_block_hash, "22".repeat(32));

    Ok(())
}

#[test]
fn subtree_root_round_trips_through_prost() -> eyre::Result<()> {
    let subtree_root = wallet::SubtreeRoot {
        subtree_index: 9,
        root_hash: vec![0x44; 32],
        completing_block_hash: "45".repeat(32),
        completing_block_height: 123,
    };
    let decoded_subtree_root = round_trip(&subtree_root)?;

    assert_eq!(decoded_subtree_root.subtree_index, 9);
    assert_eq!(decoded_subtree_root.root_hash, vec![0x44; 32]);
    assert_eq!(decoded_subtree_root.completing_block_hash, "45".repeat(32));
    assert_eq!(decoded_subtree_root.completing_block_height, 123);

    Ok(())
}

#[test]
fn visible_tip_block_response_round_trips_through_prost() -> eyre::Result<()> {
    let response = wallet::VisibleTipBlockResponse {
        chain_view: Some(synthetic_chain_view()),
        visible_tip_block: Some(wallet::BlockId {
            height: 42,
            block_hash: "55".repeat(32),
        }),
    };
    let decoded_response = round_trip(&response)?;
    let visible_tip_block = decoded_response
        .visible_tip_block
        .ok_or_else(|| eyre!("decoded visible-tip block response is missing block identity"))?;

    assert_eq!(visible_tip_block.height, 42);
    assert_eq!(visible_tip_block.block_hash, "55".repeat(32));
    assert!(decoded_response.chain_view.is_some());

    Ok(())
}

#[test]
fn broadcast_transaction_response_round_trips_through_prost() -> eyre::Result<()> {
    let response = wallet::BroadcastTransactionResponse {
        outcome: Some(wallet::broadcast_transaction_response::Outcome::Rejected(
            wallet::BroadcastRejected {
                error_code: Some(-25),
                message: "bad-txns-invalid".to_owned(),
                kind: wallet::BroadcastRejectionReason::InvalidSignature as i32,
            },
        )),
    };
    let decoded_response = round_trip(&response)?;

    assert!(matches!(
        decoded_response.outcome,
        Some(wallet::broadcast_transaction_response::Outcome::Rejected(rejected))
            if rejected.error_code == Some(-25)
                && rejected.message == "bad-txns-invalid"
                && rejected.kind == wallet::BroadcastRejectionReason::InvalidSignature as i32
    ));

    Ok(())
}

#[test]
fn broadcast_transaction_response_carries_queued_outcome() -> eyre::Result<()> {
    let response = wallet::BroadcastTransactionResponse {
        outcome: Some(wallet::broadcast_transaction_response::Outcome::Queued(
            wallet::BroadcastQueued {
                message: "already queued for download".to_owned(),
            },
        )),
    };
    let decoded_response = round_trip(&response)?;

    assert!(matches!(
        decoded_response.outcome,
        Some(wallet::broadcast_transaction_response::Outcome::Queued(queued))
            if queued.message == "already queued for download"
    ));

    Ok(())
}

#[test]
fn transaction_status_response_carries_mined_location() -> eyre::Result<()> {
    let response = wallet::TransactionStatusResponse {
        chain_view: Some(synthetic_chain_view()),
        location: Some(wallet::TransactionLocation {
            location: Some(wallet::transaction_location::Location::Mined(
                wallet::MinedTransaction {
                    location: Some(wallet::MinedBlockLocation {
                        transaction_id: "ab".repeat(32),
                        block_height: 42,
                        block_hash: "cd".repeat(32),
                        tx_index_in_block: 3,
                    }),
                    chain_context: Some(wallet::MinedTransactionChainContext {
                        consensus_branch_id: 0xc2d6_d0b4,
                        block_time: 1_774_670_000,
                        confirmations: 6,
                    }),
                    raw_transaction_bytes: Some(vec![
                        0x05, 0x00, 0x00, 0x80, 0xde, 0xad, 0xbe, 0xef,
                    ]),
                },
            )),
        }),
    };
    let decoded = round_trip(&response)?;

    let location = decoded
        .location
        .and_then(|location| location.location)
        .ok_or_else(|| eyre!("location oneof missing"))?;
    let wallet::transaction_location::Location::Mined(mined) = location else {
        return Err(eyre!("expected mined arm"));
    };
    let block_location = mined
        .location
        .ok_or_else(|| eyre!("mined location missing"))?;
    assert_eq!(block_location.block_height, 42);
    assert_eq!(block_location.tx_index_in_block, 3);
    assert_eq!(
        mined
            .chain_context
            .ok_or_else(|| eyre!("chain context missing"))?
            .confirmations,
        6
    );
    assert_eq!(
        mined.raw_transaction_bytes,
        Some(vec![0x05, 0x00, 0x00, 0x80, 0xde, 0xad, 0xbe, 0xef]),
        "the mined arm must round-trip its serialized transaction bytes",
    );

    Ok(())
}

#[test]
fn mined_transaction_round_trips_absent_raw_transaction_bytes() -> eyre::Result<()> {
    let mined = wallet::MinedTransaction {
        location: Some(wallet::MinedBlockLocation {
            transaction_id: "ab".repeat(32),
            block_height: 42,
            block_hash: "cd".repeat(32),
            tx_index_in_block: 3,
        }),
        chain_context: Some(wallet::MinedTransactionChainContext {
            consensus_branch_id: 0xc2d6_d0b4,
            block_time: 1_774_670_000,
            confirmations: 6,
        }),
        raw_transaction_bytes: None,
    };
    let decoded = round_trip(&mined)?;
    assert_eq!(
        decoded.raw_transaction_bytes, None,
        "an absent raw-transaction-bytes field must round-trip as None, not empty bytes",
    );

    Ok(())
}

#[test]
fn transaction_status_response_carries_in_mempool_location() -> eyre::Result<()> {
    let response = wallet::TransactionStatusResponse {
        chain_view: Some(synthetic_chain_view()),
        location: Some(wallet::TransactionLocation {
            location: Some(wallet::transaction_location::Location::InMempool(
                wallet::MempoolEntry {
                    transaction_id: "07".repeat(32),
                    auth_digest: String::new(),
                    raw_transaction_bytes: vec![0x07, 0x08, 0x09],
                    compact_transaction_data: Some(wallet::CompactTransactionData::default()),
                    first_seen_unix_millis: 1_774_670_111_000,
                    first_seen_chain_epoch: Some(synthetic_chain_epoch()),
                    transparent_outputs: Vec::new(),
                    transparent_spends: Vec::new(),
                },
            )),
        }),
    };
    let decoded = round_trip(&response)?;

    let location = decoded
        .location
        .and_then(|location| location.location)
        .ok_or_else(|| eyre!("location oneof missing"))?;
    let wallet::transaction_location::Location::InMempool(mempool) = location else {
        return Err(eyre!("expected in_mempool arm"));
    };
    assert_eq!(mempool.raw_transaction_bytes, vec![0x07, 0x08, 0x09]);
    assert_eq!(mempool.first_seen_unix_millis, 1_774_670_111_000);

    Ok(())
}

#[test]
fn network_upgrade_activations_round_trip_through_prost() -> eyre::Result<()> {
    let response = wallet::NetworkUpgradeActivationsResponse {
        activations: vec![wallet::NetworkUpgradeActivation {
            consensus_branch_id: 0xc8e7_1055,
            name: "NU6".to_owned(),
            activation_height: 2,
        }],
    };
    let decoded = round_trip(&response)?;

    assert_eq!(decoded, response);
    Ok(())
}

#[test]
fn chain_event_envelope_round_trips_through_prost() -> eyre::Result<()> {
    let response = wallet::ChainEventEnvelope {
        cursor: vec![0x99; 82],
        event_sequence: 11,
        chain_view: Some(synthetic_chain_view()),
        event: Some(wallet::chain_event_envelope::Event::ChainCommitted(
            wallet::ChainCommitted {
                committed: Some(wallet::ChainEpochCommitted {
                    chain_epoch: Some(synthetic_chain_epoch()),
                    start_height: 40,
                    end_height: 42,
                }),
            },
        )),
    };
    let decoded_response = round_trip(&response)?;

    assert_eq!(decoded_response.cursor, vec![0x99; 82]);
    assert_eq!(decoded_response.event_sequence, 11);
    // The settled tip height is folded onto chain_view.chain_epoch.settled_tip;
    // the envelope no longer carries a separate settled_tip_height field.
    let settled_tip = decoded_response
        .chain_view
        .as_ref()
        .and_then(|chain_view| chain_view.chain_epoch.as_ref())
        .and_then(|chain_epoch| chain_epoch.settled_tip.as_ref())
        .ok_or_else(|| eyre!("chain_view.chain_epoch.settled_tip missing"))?;
    assert_eq!(settled_tip.height, 40);
    assert!(matches!(
        decoded_response.event,
        Some(wallet::chain_event_envelope::Event::ChainCommitted(committed))
            if committed.committed.as_ref().is_some_and(|inner| {
                inner.start_height == 40 && inner.end_height == 42
            })
    ));

    Ok(())
}

#[test]
fn transparent_unspent_outputs_chunk_header_round_trips_through_prost() -> eyre::Result<()> {
    let header = wallet::TransparentUnspentOutputsChunk {
        body: Some(wallet::transparent_unspent_outputs_chunk::Body::Header(
            synthetic_chain_view(),
        )),
    };
    let decoded = round_trip(&header)?;
    let Some(wallet::transparent_unspent_outputs_chunk::Body::Header(chain_view)) = decoded.body
    else {
        return Err(eyre!("decoded chunk is not a header"));
    };
    assert_eq!(
        chain_view
            .chain_epoch
            .ok_or_else(|| eyre!("header chain_view.chain_epoch missing"))?
            .chain_epoch_id,
        7
    );
    Ok(())
}

#[test]
fn transparent_unspent_outputs_chunk_item_round_trips_through_prost() -> eyre::Result<()> {
    let item = wallet::TransparentUnspentOutputsChunk {
        body: Some(wallet::transparent_unspent_outputs_chunk::Body::Item(
            wallet::TransparentUnspentOutput {
                address_script_hash: vec![0xAB; 32],
                script_pub_key: vec![0x76, 0xa9],
                outpoint: Some(wallet::OutPoint {
                    transaction_id: "33".repeat(32),
                    output_index: 2,
                }),
                value_zat: 5000,
                block_height: 41,
                block_hash: "44".repeat(32),
            },
        )),
    };
    let decoded = round_trip(&item)?;
    assert!(matches!(
        decoded.body,
        Some(wallet::transparent_unspent_outputs_chunk::Body::Item(output))
            if output.value_zat == 5000 && output.block_height == 41
    ));
    Ok(())
}

#[test]
fn transparent_address_unspent_outputs_request_round_trips_at_epoch_id() -> eyre::Result<()> {
    let pinned = wallet::TransparentAddressUnspentOutputsRequest {
        address: Some(wallet::AddressLookup {
            selector: Some(wallet::address_lookup::Selector::ScriptHash(vec![0xAB; 32])),
        }),
        start_height: 7,
        at_epoch_id: Some(42),
    };
    assert_eq!(round_trip(&pinned)?.at_epoch_id, Some(42));

    let live = wallet::TransparentAddressUnspentOutputsRequest {
        at_epoch_id: None,
        ..pinned
    };
    assert_eq!(round_trip(&live)?.at_epoch_id, None);
    Ok(())
}

#[test]
fn transparent_address_tx_ids_chunk_item_round_trips_through_prost() -> eyre::Result<()> {
    let item = wallet::TransparentAddressTxIdsChunk {
        body: Some(wallet::transparent_address_tx_ids_chunk::Body::Item(
            wallet::TransparentAddressTxId {
                transaction_id: "55".repeat(32),
                block_height: 41,
                tx_index_in_block: 3,
                block_hash: "66".repeat(32),
                cursor: vec![0x01, 0x02],
            },
        )),
    };
    let decoded = round_trip(&item)?;
    assert!(matches!(
        decoded.body,
        Some(wallet::transparent_address_tx_ids_chunk::Body::Item(entry))
            if entry.block_height == 41 && entry.tx_index_in_block == 3
    ));
    Ok(())
}

#[test]
fn writer_status_response_round_trips_through_prost() -> eyre::Result<()> {
    let response = ingest::WriterStatusResponse {
        chain_view: Some(synthetic_chain_view()),
        network_name: "zcash-regtest".to_owned(),
        phase: ingest::WriterPhase::FollowingTip.into(),
        gap_blocks: Some(1),
        upstream_not_ready: Some(ops::UpstreamNotReadyDetail {
            upstream_committed_height: Some(42),
            upstream_estimated_height: Some(42),
            upstream_verification_progress: Some(1.0),
            upstream_health_source: "zebra_ready_endpoint".to_owned(),
            upstream_health_reason: "ok".to_owned(),
        }),
        reorg_window_exceeded: None,
    };
    let decoded_response = round_trip(&response)?;

    assert_eq!(decoded_response.network_name, "zcash-regtest");
    let decoded_chain_epoch = decoded_response
        .chain_view
        .clone()
        .and_then(|chain_view| chain_view.chain_epoch)
        .ok_or_else(|| eyre!("chain_view.chain_epoch missing"))?;
    assert_eq!(decoded_chain_epoch.chain_epoch_id, 7);
    assert_eq!(
        decoded_chain_epoch
            .visible_tip
            .ok_or_else(|| eyre!("visible_tip missing"))?
            .height,
        42
    );
    assert_eq!(
        decoded_chain_epoch
            .settled_tip
            .ok_or_else(|| eyre!("settled_tip missing"))?
            .height,
        40
    );
    assert_eq!(decoded_response.phase(), ingest::WriterPhase::FollowingTip);
    assert_eq!(decoded_response.gap_blocks, Some(1));
    let detail = decoded_response
        .upstream_not_ready
        .ok_or_else(|| eyre!("upstream_not_ready missing"))?;
    assert_eq!(detail.upstream_health_source, "zebra_ready_endpoint");
    assert_eq!(detail.upstream_health_reason, "ok");

    Ok(())
}

#[test]
fn checkpoint_build_plan_raw_blob_retention_round_trips_through_prost() -> eyre::Result<()> {
    let build_plan = ingest::CanonicalOwnerCheckpointBuildPlanEvidence {
        activation_fingerprint_version: 1,
        activation_fingerprint: vec![0x11; 32],
        reorg_window_blocks: 100,
        history_preceding_checkpoint: None,
        history_predecessor: None,
        build_tip: None,
        raw_blob_retention: "all".to_owned(),
    };

    assert_eq!(round_trip(&build_plan)?.raw_blob_retention, "all");
    Ok(())
}

#[test]
fn canonical_writer_status_admitted_schedule_round_trips_through_prost() -> eyre::Result<()> {
    let activations = zinder_core::NetworkUpgradeActivations::new(
        zinder_core::Network::ZcashTestnet,
        vec![
            zinder_core::NetworkUpgradeActivation {
                branch_id: zinder_core::ConsensusBranchId::new(0x37a5_165b),
                activation_height: zinder_core::BlockHeight::new(4_134_000),
                name: "NU6.3".to_owned(),
            },
            zinder_core::NetworkUpgradeActivation {
                branch_id: zinder_core::ConsensusBranchId::new(0x7719_0ad9),
                activation_height: zinder_core::BlockHeight::new(4_465_026),
                name: "NU7".to_owned(),
            },
        ],
    )?;
    let response = ingest::CanonicalWriterStatusResponse {
        network_name: "zcash-testnet".to_owned(),
        fence: None,
        oldest_retained_event_sequence: 1,
        network_upgrade_schedule: Some(
            zinder_proto::wire::encode_canonical_network_upgrade_schedule(&activations, 2),
        ),
    };
    let schedule = round_trip(&response)?
        .network_upgrade_schedule
        .ok_or_else(|| eyre!("network_upgrade_schedule missing"))?;
    let (decoded, revision) = zinder_proto::wire::decode_canonical_network_upgrade_schedule(
        schedule,
        zinder_core::Network::ZcashTestnet,
    )?;

    assert_eq!(decoded, activations);
    assert_eq!(revision, 2);
    Ok(())
}

#[test]
fn writer_phase_enum_round_trips_each_variant() -> eyre::Result<()> {
    for phase in [
        ingest::WriterPhase::Unspecified,
        ingest::WriterPhase::AwaitingUpstream,
        ingest::WriterPhase::BulkCatchup,
        ingest::WriterPhase::FollowingTip,
    ] {
        let response = ingest::WriterStatusResponse {
            chain_view: None,
            network_name: "zcash-regtest".to_owned(),
            phase: phase.into(),
            gap_blocks: None,
            upstream_not_ready: None,
            reorg_window_exceeded: None,
        };
        let decoded = round_trip(&response)?;
        assert_eq!(decoded.phase(), phase, "phase round-trip failed: {phase:?}");
    }

    Ok(())
}

#[test]
fn parked_writer_status_round_trips_its_reorg_halt_without_a_chain_view() -> eyre::Result<()> {
    let response = ingest::WriterStatusResponse {
        chain_view: None,
        network_name: "zcash-regtest".to_owned(),
        phase: ingest::WriterPhase::FollowingTip.into(),
        gap_blocks: None,
        upstream_not_ready: None,
        reorg_window_exceeded: Some(ops::ReorgWindowExceededDetail {
            depth: 1_001,
            configured: 1_000,
            covered: Some(1_000),
        }),
    };
    let decoded = round_trip(&response)?;
    assert_eq!(decoded.chain_view, None);
    assert_eq!(
        decoded.reorg_window_exceeded,
        Some(ops::ReorgWindowExceededDetail {
            depth: 1_001,
            configured: 1_000,
            covered: Some(1_000),
        })
    );
    // A reader built before the field decodes the same bytes and sees only
    // the absent chain view.
    let legacy = ingest::WriterStatusResponse {
        reorg_window_exceeded: None,
        ..decoded
    };
    assert_eq!(round_trip(&legacy)?.chain_view, None);
    Ok(())
}

#[test]
fn tree_state_checkpoint_response_round_trips_through_prost() -> eyre::Result<()> {
    let response = wallet::TreeStateResponse {
        chain_view: Some(synthetic_chain_view()),
        height: 42,
        block_hash: "66".repeat(32),
        payload_bytes: br#"{"hash":"block"}"#.to_vec(),
        block_time_seconds: Some(1_774_668_700),
    };
    let decoded_response = round_trip(&response)?;

    assert_eq!(decoded_response.height, 42);
    assert_eq!(decoded_response.block_hash, "66".repeat(32));
    assert_eq!(decoded_response.payload_bytes, br#"{"hash":"block"}"#);
    assert_eq!(decoded_response.block_time_seconds, Some(1_774_668_700));
    assert!(decoded_response.chain_view.is_some());

    Ok(())
}

/// Locks the wallet-plane txid wire-shape contract.
///
/// Every wallet-plane `transaction_id` wire field must carry the
/// canonical RPC-form hex encoded through
/// [`encode_rpc_transaction_id_hex`], and consumers must be able to
/// decode it back with [`decode_rpc_transaction_id_hex`] into the same
/// internal-form `TransactionId` the upstream observed. A regression to
/// internal byte order on the wire (the bug ADR-0021 fixes) fails this
/// test deterministically because the assert compares against the RPC-
/// form string a Zexplorer user paste would produce.
#[test]
fn transaction_id_wire_field_carries_rpc_form_hex() -> eyre::Result<()> {
    let internal_txid = TransactionId::from_bytes([
        0x36, 0x94, 0x55, 0xb7, 0x8a, 0xfc, 0xa3, 0xdc, 0xb5, 0x2b, 0xec, 0xfd, 0x38, 0x72, 0xba,
        0xf5, 0xd0, 0x51, 0xb3, 0x2e, 0x81, 0x65, 0xbc, 0x2c, 0x79, 0x61, 0x06, 0x9e, 0xe6, 0x0c,
        0xca, 0xc3,
    ]);
    let canonical_rpc_form = "c3ca0ce69e0661792cbc65812eb351d0f5ba7238fdec2bb5dca3fc8ab7559436";

    let request = wallet::TransactionRequest {
        transaction_id: encode_rpc_transaction_id_hex(internal_txid),
        at_epoch_id: None,
    };
    let decoded_request = round_trip(&request)?;
    assert_eq!(decoded_request.transaction_id, canonical_rpc_form);
    assert_eq!(
        decode_rpc_transaction_id_hex(&decoded_request.transaction_id)?,
        internal_txid,
    );

    let accepted = wallet::BroadcastAccepted {
        transaction_id: encode_rpc_transaction_id_hex(internal_txid),
    };
    let decoded_accepted = round_trip(&accepted)?;
    assert_eq!(decoded_accepted.transaction_id, canonical_rpc_form);

    Ok(())
}

/// Locks the wallet-plane block-hash wire-shape contract.
///
/// Two responses carry block hashes on different wire shapes
/// (`ChainEpoch.visible_tip.hash` and `BlockId.block_hash`); both must
/// encode the canonical RPC form so the consumer can compare strings
/// byte-for-byte without knowing which response produced the field.
#[test]
fn block_hash_wire_field_carries_rpc_form_hex() -> eyre::Result<()> {
    let internal_block_hash = BlockHash::from_bytes([
        0xee, 0xce, 0xfc, 0x22, 0xf4, 0xa0, 0x9f, 0xe4, 0x30, 0x6f, 0x40, 0xaf, 0xa3, 0xa6, 0xf3,
        0xdb, 0x17, 0x3f, 0x1a, 0x5e, 0x3a, 0x0c, 0xcc, 0x3d, 0x8f, 0xeb, 0x22, 0xc6, 0xba, 0xf1,
        0x33, 0x00,
    ]);
    let canonical_rpc_form = "0033f1bac622eb8f3dcc0c3a5e1a3f17dbf3a6a3af406f30e49fa0f422fcceee";

    let block_id = wallet::BlockId {
        height: 4_031_230,
        block_hash: encode_rpc_block_hash_hex(internal_block_hash),
    };
    let decoded_block_id = round_trip(&block_id)?;
    assert_eq!(decoded_block_id.block_hash, canonical_rpc_form);
    assert_eq!(
        decode_rpc_block_hash_hex(&decoded_block_id.block_hash)?,
        internal_block_hash,
    );

    Ok(())
}

fn synthetic_chain_epoch() -> wallet::ChainEpoch {
    wallet::ChainEpoch {
        chain_epoch_id: 7,
        network_name: "zcash-regtest".to_owned(),
        artifact_schema_version: 1,
        created_at_millis: 1_774_670_400_000,
        visible_tip: Some(wallet::BlockTip {
            height: 42,
            hash: "11".repeat(32),
        }),
        settled_tip: Some(wallet::BlockTip {
            height: 40,
            hash: "22".repeat(32),
        }),
        sapling_commitment_tree_size: 0,
        orchard_commitment_tree_size: 0,
        ironwood_commitment_tree_size: 99,
    }
}

fn synthetic_chain_view() -> wallet::ChainView {
    wallet::ChainView {
        chain_epoch: Some(synthetic_chain_epoch()),
        indexed_tip: None,
        upstream_tip: None,
        materialized_views: None,
    }
}

#[test]
fn transparent_utxo_set_summary_response_round_trips_through_prost() -> eyre::Result<()> {
    let response = wallet::TransparentUtxoSetSummaryResponse {
        chain_view: Some(synthetic_chain_view()),
        utxo_count: 4096,
        total_value_zat: 2_100_000_000_000_000,
        summarized_height: 2_500_000,
        commitment: Some(wallet::TransparentUtxoSetCommitment {
            scheme: wallet::UtxoSetCommitmentScheme::Lthash16 as i32,
            commitment: vec![0xab; 2048],
        }),
    };

    let decoded = round_trip(&response)?;

    assert!(decoded.chain_view.is_some());
    assert_eq!(decoded.utxo_count, 4096);
    assert_eq!(decoded.total_value_zat, 2_100_000_000_000_000);
    assert_eq!(decoded.summarized_height, 2_500_000);
    let commitment = decoded
        .commitment
        .ok_or_else(|| eyre::eyre!("commitment present after round-trip"))?;
    assert_eq!(
        commitment.scheme,
        wallet::UtxoSetCommitmentScheme::Lthash16 as i32
    );
    assert_eq!(commitment.commitment.len(), 2048);
    Ok(())
}

#[test]
fn transparent_utxo_set_summary_response_omits_absent_commitment() -> eyre::Result<()> {
    let response = wallet::TransparentUtxoSetSummaryResponse {
        chain_view: Some(synthetic_chain_view()),
        utxo_count: 0,
        total_value_zat: 0,
        summarized_height: 0,
        commitment: None,
    };

    let decoded = round_trip(&response)?;

    assert!(decoded.commitment.is_none());
    Ok(())
}

#[test]
fn transparent_utxo_set_summary_request_round_trips_the_epoch_pin() -> eyre::Result<()> {
    let request = wallet::TransparentUtxoSetSummaryRequest {
        at_epoch_id: Some(77),
    };

    let decoded = round_trip(&request)?;

    assert_eq!(decoded.at_epoch_id, Some(77));
    Ok(())
}

#[test]
fn transparent_spends_by_outpoint_response_round_trips_through_prost() -> eyre::Result<()> {
    let spending_transaction_id = TransactionId::from_bytes([0xAB; 32]);
    let spending_block_hash = BlockHash::from_bytes([0xCD; 32]);
    let response = wallet::TransparentSpendsByOutpointResponse {
        chain_view: Some(synthetic_chain_view()),
        spends: vec![wallet::TransparentSpend {
            spent_outpoint: Some(wallet::OutPoint {
                transaction_id: "11".repeat(32),
                output_index: 3,
            }),
            spending_transaction_id: encode_rpc_transaction_id_hex(spending_transaction_id),
            input_index: 2,
            spending_block: Some(wallet::BlockTip {
                height: 808,
                hash: encode_rpc_block_hash_hex(spending_block_hash),
            }),
        }],
    };

    let decoded = round_trip(&response)?;

    assert!(decoded.chain_view.is_some());
    assert_eq!(decoded.spends.len(), 1);
    let spend = decoded
        .spends
        .first()
        .ok_or_else(|| eyre!("decoded response is missing the spend entry"))?;
    let spent_outpoint = spend
        .spent_outpoint
        .as_ref()
        .ok_or_else(|| eyre!("decoded spend is missing its spent outpoint"))?;
    assert_eq!(spent_outpoint.transaction_id, "11".repeat(32));
    assert_eq!(spent_outpoint.output_index, 3);
    assert_eq!(spend.input_index, 2);
    assert_eq!(
        decode_rpc_transaction_id_hex(&spend.spending_transaction_id)?,
        spending_transaction_id
    );
    let spending_block = spend
        .spending_block
        .as_ref()
        .ok_or_else(|| eyre!("decoded spend is missing its spending block"))?;
    assert_eq!(spending_block.height, 808);
    assert_eq!(
        decode_rpc_block_hash_hex(&spending_block.hash)?,
        spending_block_hash
    );
    Ok(())
}

#[test]
fn transparent_unspent_outputs_by_outpoint_response_round_trips_through_prost() -> eyre::Result<()>
{
    let response = wallet::TransparentUnspentOutputsByOutpointResponse {
        chain_view: Some(synthetic_chain_view()),
        entries: vec![wallet::TransparentOutputEntry {
            outpoint: Some(wallet::OutPoint {
                transaction_id: "22".repeat(32),
                output_index: 4,
            }),
            output: Some(wallet::TransparentOutput {
                value_zat: 9_999,
                script_pub_key: vec![0x76, 0xa9, 0x14, 0x88, 0xac],
            }),
        }],
    };

    let decoded = round_trip(&response)?;

    assert!(decoded.chain_view.is_some());
    assert_eq!(decoded.entries.len(), 1);
    let entry = decoded
        .entries
        .first()
        .ok_or_else(|| eyre!("decoded response is missing the entry"))?;
    let outpoint = entry
        .outpoint
        .as_ref()
        .ok_or_else(|| eyre!("decoded entry is missing its outpoint"))?;
    assert_eq!(outpoint.transaction_id, "22".repeat(32));
    assert_eq!(outpoint.output_index, 4);
    let output = entry
        .output
        .as_ref()
        .ok_or_else(|| eyre!("decoded entry is missing its output"))?;
    assert_eq!(output.value_zat, 9_999);
    assert_eq!(output.script_pub_key, vec![0x76, 0xa9, 0x14, 0x88, 0xac]);
    Ok(())
}

#[test]
fn event_stream_start_round_trips_each_position() -> eyre::Result<()> {
    let after_cursor = wallet::EventStreamStart {
        position: Some(wallet::event_stream_start::Position::AfterCursor(vec![
            0xAB;
            82
        ])),
    };
    assert_eq!(
        round_trip(&after_cursor)?.position,
        Some(wallet::event_stream_start::Position::AfterCursor(vec![
            0xAB;
            82
        ]))
    );

    let earliest = wallet::EventStreamStart {
        position: Some(wallet::event_stream_start::Position::EarliestRetained(
            wallet::EarliestRetained {},
        )),
    };
    assert!(matches!(
        round_trip(&earliest)?.position,
        Some(wallet::event_stream_start::Position::EarliestRetained(_))
    ));

    let live_tail = wallet::EventStreamStart {
        position: Some(wallet::event_stream_start::Position::LiveTail(
            wallet::LiveTail {},
        )),
    };
    assert!(matches!(
        round_trip(&live_tail)?.position,
        Some(wallet::event_stream_start::Position::LiveTail(_))
    ));

    let unset = wallet::EventStreamStart { position: None };
    assert!(round_trip(&unset)?.position.is_none());

    Ok(())
}

#[test]
fn chain_events_request_round_trips_start_and_family() -> eyre::Result<()> {
    let request = wallet::ChainEventsRequest {
        start: Some(wallet::EventStreamStart {
            position: Some(wallet::event_stream_start::Position::LiveTail(
                wallet::LiveTail {},
            )),
        }),
        family: wallet::ChainEventStreamFamily::Settled as i32,
        address_filter: vec!["t1deadbeef".to_owned()],
    };
    let decoded = round_trip(&request)?;

    assert!(matches!(
        decoded.start.and_then(|start| start.position),
        Some(wallet::event_stream_start::Position::LiveTail(_))
    ));
    assert_eq!(
        decoded.family,
        wallet::ChainEventStreamFamily::Settled as i32
    );
    assert_eq!(decoded.address_filter, vec!["t1deadbeef".to_owned()]);
    Ok(())
}

#[test]
fn mempool_events_request_round_trips_start() -> eyre::Result<()> {
    let request = wallet::MempoolEventsRequest {
        start: Some(wallet::EventStreamStart {
            position: Some(wallet::event_stream_start::Position::AfterCursor(vec![
                0x0F;
                82
            ])),
        }),
    };
    let decoded = round_trip(&request)?;

    assert_eq!(
        decoded.start.and_then(|start| start.position),
        Some(wallet::event_stream_start::Position::AfterCursor(vec![
            0x0F;
            82
        ]))
    );
    Ok(())
}

#[test]
fn mempool_snapshot_response_round_trips_events_resume_cursor() -> eyre::Result<()> {
    let response = wallet::MempoolSnapshotResponse {
        chain_view: Some(wallet::ChainView {
            chain_epoch: Some(synthetic_chain_epoch()),
            indexed_tip: None,
            upstream_tip: None,
            materialized_views: None,
        }),
        events_resume_cursor: vec![0xC4; 82],
        snapshot_age_millis: 250,
        entries: Vec::new(),
        next_cursor: vec![0xD5; 114],
        source_tip: Some(wallet::BlockTip {
            height: 100,
            hash: "11".repeat(32),
        }),
    };
    let decoded = round_trip(&response)?;

    assert_eq!(decoded.events_resume_cursor, vec![0xC4; 82]);
    assert_eq!(decoded.snapshot_age_millis, 250);
    assert_eq!(decoded.next_cursor, vec![0xD5; 114]);
    assert_eq!(decoded.source_tip.map(|tip| tip.height), Some(100));
    Ok(())
}

#[test]
fn chain_value_pools_response_preserves_source_tip_presence() -> eyre::Result<()> {
    let response = wallet::ChainValuePoolsAtTipResponse {
        chain_view: None,
        pools: vec![wallet::ChainValuePool {
            id: "transparent".to_owned(),
            monitored: true,
            chain_value_zat: Some(42),
        }],
        source_tip: Some(wallet::BlockTip {
            height: 1_234,
            hash: "ab".repeat(32),
        }),
    };
    let decoded = round_trip(&response)?;
    let source_tip = decoded
        .source_tip
        .ok_or_else(|| eyre!("source_tip missing"))?;

    assert_eq!(source_tip.height, 1_234);
    assert_eq!(source_tip.hash, "ab".repeat(32));
    assert!(
        round_trip(&wallet::ChainValuePoolsAtTipResponse::default())?
            .source_tip
            .is_none()
    );
    assert!(
        wallet::ChainValuePoolsAtTipResponse::decode([0x18, 0x2a].as_slice())?
            .source_tip
            .is_none()
    );
    Ok(())
}

#[test]
fn ops_server_info_round_trips_contract_revision() -> eyre::Result<()> {
    let server_info = ops::ServerInfo {
        network: "zcash-regtest".to_owned(),
        service_name: "zinder-query".to_owned(),
        service_version: "0.1.0".to_owned(),
        build_git_commit: "0123456789abcdef0123456789abcdef01234567".to_owned(),
        capabilities: vec![zinder_proto::capabilities::WALLET_EVENTS_CHAIN_V1.to_owned()],
        contract_revision: zinder_proto::CONTRACT_REVISION,
        materialized_view_preset: "wallet".to_owned(),
        materialized_view_identities: vec!["transparent_outpoint_spend".to_owned()],
    };
    let decoded = round_trip(&server_info)?;

    assert_eq!(decoded.contract_revision, zinder_proto::CONTRACT_REVISION);
    assert_eq!(
        decoded.build_git_commit,
        "0123456789abcdef0123456789abcdef01234567"
    );
    assert_eq!(decoded.materialized_view_preset, "wallet");
    assert_eq!(
        decoded.materialized_view_identities,
        vec!["transparent_outpoint_spend"]
    );
    Ok(())
}

fn round_trip<MessageType>(message: &MessageType) -> Result<MessageType, prost::DecodeError>
where
    MessageType: Message + Default,
{
    let encoded_message = message.encode_to_vec();
    MessageType::decode(encoded_message.as_slice())
}
