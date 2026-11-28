//! Wire round trips for the test-control protocol.

use eyre::{ContextCompat, Result};
use prost::Message;
use prost_types::FileDescriptorSet;
use zinder_proto::{
    DARKSIDE_FILE_DESCRIPTOR_SET, LIGHTWALLETD_COMPAT_FILE_DESCRIPTOR_SET,
    compat::lightwalletd::{
        DarksideMetaState, DarksideSubtreeRoots, ShieldedProtocol, SubtreeRoot,
    },
};

#[test]
fn darkside_messages_round_trip() -> Result<()> {
    let reset = DarksideMetaState {
        sapling_activation: 100,
        branch_id: "c2d6d0b4".to_owned(),
        chain_name: "regtest".to_owned(),
        start_sapling_commitment_tree_size: 11,
        start_orchard_commitment_tree_size: 12,
        start_ironwood_commitment_tree_size: 13,
    };
    assert_eq!(
        DarksideMetaState::decode(reset.encode_to_vec().as_slice())?,
        reset
    );

    let roots = DarksideSubtreeRoots {
        shielded_protocol: ShieldedProtocol::Orchard.into(),
        start_index: 2,
        subtree_roots: vec![SubtreeRoot {
            root_hash: vec![42; 32],
            completing_block_hash: vec![12; 32],
            completing_block_height: 125,
        }],
    };
    assert_eq!(
        DarksideSubtreeRoots::decode(roots.encode_to_vec().as_slice())?,
        roots
    );
    Ok(())
}

#[test]
fn darkside_descriptor_reuses_the_wallet_contract() -> Result<()> {
    let darkside = FileDescriptorSet::decode(DARKSIDE_FILE_DESCRIPTOR_SET)?;
    let wallet = FileDescriptorSet::decode(LIGHTWALLETD_COMPAT_FILE_DESCRIPTOR_SET)?;
    for file in wallet.file {
        assert!(darkside.file.contains(&file));
    }
    let schema = darkside
        .file
        .iter()
        .find(|file| file.name() == "darkside.proto")
        .wrap_err("missing Darkside schema")?;
    assert_eq!(schema.package(), "cash.z.wallet.sdk.rpc");
    assert_eq!(schema.dependency, ["service.proto"]);
    let streamer = schema
        .service
        .first()
        .wrap_err("missing Darkside service")?;
    assert_eq!(streamer.name(), "DarksideStreamer");
    assert!(
        streamer
            .method
            .iter()
            .any(|method| method.name() == "StageBlocksStream" && method.client_streaming())
    );
    assert!(
        streamer
            .method
            .iter()
            .any(|method| method.name() == "GetIncomingTransactions" && method.server_streaming())
    );
    Ok(())
}
