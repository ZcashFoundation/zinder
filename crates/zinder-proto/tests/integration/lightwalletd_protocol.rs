#![allow(
    missing_docs,
    reason = "Integration test names describe the protocol contract under test."
)]

use eyre::eyre;
use prost::Message;
use zinder_proto::compat::lightwalletd::{self, LIGHTWALLETD_PROTOCOL_VERSION};

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "this end-to-end wire round-trip test asserts every vendored lightwalletd field in one place"
)]
fn compact_block_decodes_current_lightwalletd_fields() -> eyre::Result<()> {
    let transaction_id = vec![0x42; 32];
    let transparent_output_transaction_id = vec![0x43; 32];
    let compact_block = lightwalletd::CompactBlock {
        height: 42,
        hash: vec![0x44; 32],
        prev_hash: vec![0x45; 32],
        time: 1_774_670_400,
        header: Vec::new(),
        vtx: vec![lightwalletd::CompactTx {
            index: 3,
            txid: transaction_id.clone(),
            fee: 1_000,
            spends: vec![lightwalletd::CompactSaplingSpend { nf: vec![0x46; 32] }],
            outputs: vec![lightwalletd::CompactSaplingOutput {
                cmu: vec![0x47; 32],
                ephemeral_key: vec![0x48; 32],
                ciphertext: vec![0x49; 52],
            }],
            actions: vec![lightwalletd::CompactOrchardAction {
                nullifier: vec![0x50; 32],
                cmx: vec![0x51; 32],
                ephemeral_key: vec![0x52; 32],
                ciphertext: vec![0x53; 52],
            }],
            ironwood_actions: vec![lightwalletd::CompactOrchardAction {
                nullifier: vec![0x54; 32],
                cmx: vec![0x55; 32],
                ephemeral_key: vec![0x56; 32],
                ciphertext: vec![0x57; 52],
            }],
            vin: vec![lightwalletd::CompactTxIn {
                prevout_txid: transparent_output_transaction_id.clone(),
                prevout_index: 7,
            }],
            vout: vec![lightwalletd::TxOut {
                value: 5_000,
                script_pub_key: vec![0x76, 0xa9],
            }],
        }],
        chain_metadata: Some(lightwalletd::ChainMetadata {
            sapling_commitment_tree_size: 123,
            orchard_commitment_tree_size: 456,
            ironwood_commitment_tree_size: 789,
        }),
    };

    let decoded_compact_block = round_trip(&compact_block)?;
    let compact_transaction = decoded_compact_block
        .vtx
        .first()
        .ok_or_else(|| eyre!("decoded compact block is missing transaction"))?;
    let transparent_input = compact_transaction
        .vin
        .first()
        .ok_or_else(|| eyre!("decoded compact transaction is missing transparent input"))?;
    let transparent_output = compact_transaction
        .vout
        .first()
        .ok_or_else(|| eyre!("decoded compact transaction is missing transparent output"))?;
    let ironwood_action = compact_transaction
        .ironwood_actions
        .first()
        .ok_or_else(|| eyre!("decoded compact transaction is missing Ironwood action"))?;
    let chain_metadata = decoded_compact_block
        .chain_metadata
        .ok_or_else(|| eyre!("decoded compact block is missing chain metadata"))?;

    assert!(decoded_compact_block.header.is_empty());
    assert_eq!(compact_transaction.txid, transaction_id);
    assert_eq!(
        transparent_input.prevout_txid,
        transparent_output_transaction_id
    );
    assert_eq!(transparent_input.prevout_index, 7);
    assert_eq!(transparent_output.value, 5_000);
    assert_eq!(transparent_output.script_pub_key, vec![0x76, 0xa9]);
    assert_eq!(ironwood_action.nullifier, vec![0x54; 32]);
    assert_eq!(ironwood_action.cmx, vec![0x55; 32]);
    assert_eq!(ironwood_action.ephemeral_key, vec![0x56; 32]);
    assert_eq!(ironwood_action.ciphertext, vec![0x57; 52]);
    assert_eq!(chain_metadata.sapling_commitment_tree_size, 123);
    assert_eq!(chain_metadata.orchard_commitment_tree_size, 456);
    assert_eq!(chain_metadata.ironwood_commitment_tree_size, 789);

    Ok(())
}

#[test]
fn block_range_decodes_pool_type_filters() -> eyre::Result<()> {
    let block_range = lightwalletd::BlockRange {
        start: Some(lightwalletd::BlockId {
            height: 1,
            hash: Vec::new(),
        }),
        end: Some(lightwalletd::BlockId {
            height: 2,
            hash: Vec::new(),
        }),
        pool_types: vec![
            lightwalletd::PoolType::Sapling as i32,
            lightwalletd::PoolType::Orchard as i32,
            lightwalletd::PoolType::Ironwood as i32,
        ],
    };
    let decoded_block_range = round_trip(&block_range)?;

    assert_eq!(
        decoded_block_range.pool_types,
        vec![
            lightwalletd::PoolType::Sapling as i32,
            lightwalletd::PoolType::Orchard as i32,
            lightwalletd::PoolType::Ironwood as i32,
        ]
    );

    Ok(())
}

#[test]
fn lightd_info_decodes_protocol_version_fields() -> eyre::Result<()> {
    let lightd_info = lightwalletd::LightdInfo {
        version: "zinder-v1".to_owned(),
        vendor: "zinder".to_owned(),
        taddr_support: false,
        chain_name: "regtest".to_owned(),
        sapling_activation_height: 1,
        consensus_branch_id: "00000000".to_owned(),
        block_height: 2,
        git_commit: "test".to_owned(),
        branch: "main".to_owned(),
        build_date: "2026-04-27".to_owned(),
        build_user: "zinder".to_owned(),
        estimated_height: 2,
        zcashd_build: "zebra".to_owned(),
        zcashd_subversion: "/Zebra:test/".to_owned(),
        donation_address: String::new(),
        upgrade_name: "NU7".to_owned(),
        upgrade_height: 3,
        lightwallet_protocol_version: LIGHTWALLETD_PROTOCOL_VERSION.to_owned(),
    };
    let decoded_lightd_info = round_trip(&lightd_info)?;

    assert_eq!(
        decoded_lightd_info.lightwallet_protocol_version,
        LIGHTWALLETD_PROTOCOL_VERSION
    );
    assert_eq!(decoded_lightd_info.upgrade_name, "NU7");
    assert_eq!(decoded_lightd_info.upgrade_height, 3);

    Ok(())
}

#[test]
fn tree_state_decodes_sapling_orchard_and_ironwood_tree_payloads() -> eyre::Result<()> {
    let tree_state = lightwalletd::TreeState {
        network: "regtest".to_owned(),
        height: 2,
        hash: "block-hash".to_owned(),
        time: 1_774_670_400,
        sapling_tree: "sapling-tree".to_owned(),
        orchard_tree: "orchard-tree".to_owned(),
        ironwood_tree: "ironwood-tree".to_owned(),
    };
    let decoded_tree_state = round_trip(&tree_state)?;

    assert_eq!(decoded_tree_state.sapling_tree, "sapling-tree");
    assert_eq!(decoded_tree_state.orchard_tree, "orchard-tree");
    assert_eq!(decoded_tree_state.ironwood_tree, "ironwood-tree");

    Ok(())
}

#[test]
fn subtree_root_messages_decode_protocol_and_completion_fields() -> eyre::Result<()> {
    let subtree_request = lightwalletd::GetSubtreeRootsArg {
        start_index: 4,
        shielded_protocol: lightwalletd::ShieldedProtocol::Ironwood as i32,
        max_entries: 16,
    };
    let decoded_subtree_request = round_trip(&subtree_request)?;

    assert_eq!(decoded_subtree_request.start_index, 4);
    assert_eq!(
        decoded_subtree_request.shielded_protocol,
        lightwalletd::ShieldedProtocol::Ironwood as i32
    );
    assert_eq!(decoded_subtree_request.max_entries, 16);

    let subtree_root = lightwalletd::SubtreeRoot {
        root_hash: vec![0x61; 32],
        completing_block_hash: vec![0x62; 32],
        completing_block_height: 10,
    };
    let decoded_subtree_root = round_trip(&subtree_root)?;

    assert_eq!(decoded_subtree_root.root_hash, vec![0x61; 32]);
    assert_eq!(decoded_subtree_root.completing_block_hash, vec![0x62; 32]);
    assert_eq!(decoded_subtree_root.completing_block_height, 10);

    Ok(())
}

#[test]
fn mempool_request_decodes_txid_suffixes_and_pool_filters() -> eyre::Result<()> {
    let mempool_request = lightwalletd::GetMempoolTxRequest {
        exclude_txid_suffixes: vec![vec![0x70, 0x71]],
        pool_types: vec![lightwalletd::PoolType::Transparent as i32],
    };
    let decoded_mempool_request = round_trip(&mempool_request)?;

    assert_eq!(
        decoded_mempool_request.exclude_txid_suffixes,
        vec![vec![0x70, 0x71]]
    );
    assert_eq!(
        decoded_mempool_request.pool_types,
        vec![lightwalletd::PoolType::Transparent as i32]
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
