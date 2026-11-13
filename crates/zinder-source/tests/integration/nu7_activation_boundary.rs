use std::io::Cursor;

use eyre::{Result, eyre};
use serde_json::Value;
use zebra_chain::{
    block::Block,
    serialization::{ZcashDeserialize, ZcashSerialize},
};
use zinder_core::{
    BlockHeight, ConsensusBranchId, Network, NetworkUpgradeActivations, TransactionVersion,
    wire::{encode_rpc_auth_digest_hex, encode_rpc_transaction_id_hex},
};
use zinder_source::{
    SourceBlock, SourceError, decode_rpc_block_hash, parse_transaction_public_fact_set,
};

const NU6_2_BRANCH_ID: u32 = 0x5437_f330;
const NU6_3_BRANCH_ID: u32 = 0x37a5_165b;
const NU7_BRANCH_ID: u32 = 0x7719_0ad9;
const ZAKURA_ORCHARD_FIXTURE: &str = include_str!(
    "../../../../services/zinder-ingest/tests/fixtures/zakura-1.8.6-regtest-orchard-block-111.json"
);
const ZAKURA_IRONWOOD_FIXTURE: &str = include_str!(
    "../../../../services/zinder-ingest/tests/fixtures/zakura-1.8.6-regtest-ironwood-block-132.json"
);
const ZAKURA_NU7_FIXTURE: &str = include_str!(
    "../../../../services/zinder-ingest/tests/fixtures/zakura-1.8.6-regtest-nu7-block-162.json"
);
const ZEBRA_7_NU7_FIXTURE: &str = include_str!(
    "../../../../services/zinder-ingest/tests/fixtures/zebra-7.0.0-regtest-nu7-block-159.json"
);

/// A pre-NU6.3 Zakura 1.8.6 block with a version-5 transparent-to-Orchard
/// shielding spend.
#[test]
fn zakura_orchard_block_keeps_node_reported_identifiers() -> Result<()> {
    assert_node_reported_identifiers(
        ZAKURA_ORCHARD_FIXTURE,
        TransactionVersion::V5,
        NU6_2_BRANCH_ID,
    )
}

/// A post-NU6.3 Zakura 1.8.6 block with an Ironwood coinbase, a version-6
/// transparent-to-Ironwood shielding spend, and a version-6 transparent spend.
#[test]
fn zakura_ironwood_block_keeps_node_reported_identifiers() -> Result<()> {
    assert_node_reported_identifiers(
        ZAKURA_IRONWOOD_FIXTURE,
        TransactionVersion::V6,
        NU6_3_BRANCH_ID,
    )
}

/// The same transaction shapes as the Ironwood block, under NU7.
#[test]
fn zakura_nu7_block_keeps_node_reported_identifiers() -> Result<()> {
    assert_node_reported_identifiers(ZAKURA_NU7_FIXTURE, TransactionVersion::V6, NU7_BRANCH_ID)
}

/// A post-NU7 Zebra 7.0.0 block with an Ironwood coinbase and a version-6
/// transparent spend.
#[test]
fn zebra_7_nu7_block_keeps_node_reported_identifiers() -> Result<()> {
    assert_node_reported_identifiers(ZEBRA_7_NU7_FIXTURE, TransactionVersion::V6, NU7_BRANCH_ID)
}

#[test]
fn unassigned_branch_ids_and_transaction_versions_fail_closed() -> Result<()> {
    let fixture: Value = serde_json::from_str(ZAKURA_NU7_FIXTURE)?;
    let coinbase_bytes = captured_transaction_bytes(&fixture)?
        .into_iter()
        .next()
        .ok_or_else(|| eyre!("captured block must contain its coinbase"))?;
    // A v5+ transaction header is the version, version group ID, and
    // consensus branch ID, each four bytes little-endian.
    let mut retired_branch_id = coinbase_bytes.clone();
    retired_branch_id[8..12].copy_from_slice(&0x7719_0ad8_u32.to_le_bytes());
    let mut future_version = coinbase_bytes;
    future_version[0..4].copy_from_slice(&0x8000_0007_u32.to_le_bytes());

    for malformed_bytes in [retired_branch_id, future_version] {
        assert!(matches!(
            parse_transaction_public_fact_set(
                &malformed_bytes,
                None,
                &NetworkUpgradeActivations::empty(Network::ZcashRegtest),
            ),
            Err(SourceError::RawTransactionParseFailed { .. })
        ));
    }
    Ok(())
}

/// Checks every transaction of a captured block against the node's reports.
///
/// Each transaction goes through the byte-oriented parser that mempool
/// hydration uses, and must keep the txid and authorization digest the
/// capturing node reported, with `expected_version` and `expected_branch_id`.
fn assert_node_reported_identifiers(
    fixture_json: &str,
    expected_version: TransactionVersion,
    expected_branch_id: u32,
) -> Result<()> {
    let fixture: Value = serde_json::from_str(fixture_json)?;
    let raw_block_bytes = hex::decode(string_field(&fixture, "raw_block_hex")?)?;
    let height = fixture
        .get("height")
        .and_then(Value::as_u64)
        .ok_or_else(|| eyre!("fixture field height must be an integer"))?;
    let height = BlockHeight::new(u32::try_from(height)?);
    let source_block =
        SourceBlock::from_raw_block_bytes(Network::ZcashRegtest, height, raw_block_bytes)?;
    assert_eq!(
        source_block.hash,
        decode_rpc_block_hash(string_field(&fixture, "hash")?)?
    );
    assert_eq!(
        source_block.parent_hash,
        decode_rpc_block_hash(string_field(&fixture, "previousblockhash")?)?
    );

    let transactions = captured_transaction_bytes(&fixture)?;
    let expected_transactions = fixture
        .get("transactions")
        .and_then(Value::as_array)
        .ok_or_else(|| eyre!("fixture field transactions must be an array"))?;
    assert_eq!(transactions.len(), expected_transactions.len());
    for (index, (transaction_bytes, expected)) in
        transactions.iter().zip(expected_transactions).enumerate()
    {
        let fact_set = parse_transaction_public_fact_set(
            transaction_bytes,
            None,
            &NetworkUpgradeActivations::empty(Network::ZcashRegtest),
        )?;
        let facts = fact_set.public_facts;
        assert_eq!(
            encode_rpc_transaction_id_hex(facts.transaction_id),
            string_field(expected, "txid")?
        );
        assert_eq!(
            facts.auth_digest.map(encode_rpc_auth_digest_hex).as_deref(),
            Some(string_field(expected, "authdigest")?)
        );
        assert_eq!(facts.version, expected_version);
        assert_eq!(
            facts.consensus_branch_id,
            Some(ConsensusBranchId::new(expected_branch_id))
        );
        assert_eq!(facts.is_coinbase, index == 0);
        if facts.is_coinbase {
            assert_eq!(facts.expiry_height, Some(height));
            assert!(fact_set.transparent_inputs.is_empty());
        }
    }
    Ok(())
}

/// Returns the serialized transactions of a captured block, in block order.
fn captured_transaction_bytes(fixture: &Value) -> Result<Vec<Vec<u8>>> {
    let raw_block_bytes = hex::decode(string_field(fixture, "raw_block_hex")?)?;
    let mut cursor = Cursor::new(raw_block_bytes.as_slice());
    let block = Block::zcash_deserialize(&mut cursor)?;
    eyre::ensure!(
        usize::try_from(cursor.position())? == raw_block_bytes.len(),
        "block parser did not consume the complete captured block",
    );
    Ok(block
        .transactions
        .iter()
        .map(ZcashSerialize::zcash_serialize_to_vec)
        .collect::<Result<_, _>>()?)
}

fn string_field<'fixture>(fixture: &'fixture Value, field_name: &str) -> Result<&'fixture str> {
    fixture
        .get(field_name)
        .and_then(Value::as_str)
        .ok_or_else(|| eyre!("fixture field {field_name} must be a string"))
}
