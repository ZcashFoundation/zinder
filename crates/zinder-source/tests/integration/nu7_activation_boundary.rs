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

const NU6_3_BRANCH_ID: u32 = 0x37a5_165b;
const NU7_BRANCH_ID: u32 = 0x7719_0ad9;
const NU7_FIXTURE: &str = include_str!(
    "../../../../services/zinder-ingest/tests/fixtures/zakura-regtest-nu7-block-3.json"
);

#[test]
fn pre_nu7_coinbase_keeps_node_reported_identifiers() -> Result<()> {
    assert_captured_coinbase_facts(
        include_str!(
            "../../../../services/zinder-ingest/tests/fixtures/zakura-regtest-ironwood-block-3.json"
        ),
        NU6_3_BRANCH_ID,
    )
}

#[test]
fn nu7_coinbase_keeps_node_reported_identifiers() -> Result<()> {
    assert_captured_coinbase_facts(NU7_FIXTURE, NU7_BRANCH_ID)
}

#[test]
fn unassigned_branch_ids_and_transaction_versions_fail_closed() -> Result<()> {
    let fixture: Value = serde_json::from_str(NU7_FIXTURE)?;
    let coinbase_bytes = captured_coinbase_bytes(&fixture)?;
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

/// Decodes a captured single-coinbase block through the byte-oriented parser
/// that mempool hydration uses, and compares the result with the identifiers
/// the capturing node reported.
fn assert_captured_coinbase_facts(fixture_json: &str, expected_branch_id: u32) -> Result<()> {
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

    let fact_set = parse_transaction_public_fact_set(
        &captured_coinbase_bytes(&fixture)?,
        None,
        &NetworkUpgradeActivations::empty(Network::ZcashRegtest),
    )?;
    let facts = fact_set.public_facts;

    assert_eq!(
        encode_rpc_transaction_id_hex(facts.transaction_id),
        string_field(&fixture, "coinbase_txid")?
    );
    assert_eq!(
        facts.auth_digest.map(encode_rpc_auth_digest_hex).as_deref(),
        Some(string_field(&fixture, "coinbase_authdigest")?)
    );
    assert_eq!(facts.version, TransactionVersion::V6);
    assert_eq!(
        facts.consensus_branch_id,
        Some(ConsensusBranchId::new(expected_branch_id))
    );
    assert!(facts.is_coinbase);
    assert_eq!(facts.expiry_height, Some(height));
    assert!(fact_set.transparent_inputs.is_empty());
    assert_eq!(fact_set.transparent_outputs.len(), 1);
    Ok(())
}

fn captured_coinbase_bytes(fixture: &Value) -> Result<Vec<u8>> {
    let raw_block_bytes = hex::decode(string_field(fixture, "raw_block_hex")?)?;
    let mut cursor = Cursor::new(raw_block_bytes.as_slice());
    let block = Block::zcash_deserialize(&mut cursor)?;
    eyre::ensure!(
        usize::try_from(cursor.position())? == raw_block_bytes.len(),
        "block parser did not consume the complete captured block",
    );
    let [coinbase] = block.transactions.as_slice() else {
        return Err(eyre!("captured block must contain only its coinbase"));
    };
    Ok(coinbase.zcash_serialize_to_vec()?)
}

fn string_field<'fixture>(fixture: &'fixture Value, field_name: &str) -> Result<&'fixture str> {
    fixture
        .get(field_name)
        .and_then(Value::as_str)
        .ok_or_else(|| eyre!("fixture field {field_name} must be a string"))
}
