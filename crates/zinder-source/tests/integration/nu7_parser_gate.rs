use std::io::Cursor;

use eyre::{Result, eyre};
use zcash_primitives::transaction::TxVersion;
use zebra_chain::{
    block::Block,
    serialization::{ZcashDeserialize, ZcashSerialize},
};
use zinder_core::{
    BlockHeight, ConsensusBranchId, Network, NetworkUpgradeActivation, NetworkUpgradeActivations,
    TransactionVersion,
};
use zinder_source::parse_transaction_public_fact_set;

const ASSIGNED_NU7_BRANCH_ID: u32 = 0x7719_0ad9;
const NU7_V6_BLOCK: &str = include_str!("../fixtures/nu7/zakura-1.6.0-regtest-v6-block.hex");

#[test]
#[ignore = "published zebra-chain 13.0.1 and zcash_protocol 0.10.6 do not recognize assigned NU7 branch ID 0x77190ad9"]
fn assigned_nu7_v6_fixture_parses_with_its_reported_branch_id() -> Result<()> {
    let block_bytes = hex::decode(NU7_V6_BLOCK.trim())?;
    let mut cursor = Cursor::new(block_bytes.as_slice());
    let block = Block::zcash_deserialize(&mut cursor)?;
    eyre::ensure!(
        usize::try_from(cursor.position())? == block_bytes.len(),
        "block parser did not consume the complete captured block",
    );

    let transaction = block
        .transactions
        .iter()
        .find(|transaction| transaction.tx_version() == TxVersion::V6)
        .ok_or_else(|| eyre!("captured NU7 block contains no V6 transaction"))?;
    let mut transaction_bytes = Vec::new();
    transaction.zcash_serialize(&mut transaction_bytes)?;
    let activations = NetworkUpgradeActivations::new(
        Network::ZcashRegtest,
        vec![NetworkUpgradeActivation {
            branch_id: ConsensusBranchId::new(ASSIGNED_NU7_BRANCH_ID),
            activation_height: BlockHeight::new(3),
            name: "NU7".to_owned(),
        }],
    )?;
    let facts = parse_transaction_public_fact_set(
        &transaction_bytes,
        Some(BlockHeight::new(3)),
        &activations,
    )?;

    assert_eq!(facts.public_facts.version, TransactionVersion::V6);
    assert_eq!(
        facts.public_facts.consensus_branch_id,
        Some(ConsensusBranchId::new(ASSIGNED_NU7_BRANCH_ID)),
    );
    Ok(())
}
