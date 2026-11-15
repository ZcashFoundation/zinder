use eyre::{Result, eyre};
use serde_json::Value;
use tempfile::{TempDir, tempdir};
use zinder_bench::{
    capture::measure_fixture_blocks,
    fixture::{
        ActivationRecord, FIXTURE_CONTRACT_IDENTITY, FIXTURE_FORMAT_VERSION, FixtureManifest,
        SubtreeRootSet, write_segment,
    },
};
use zinder_core::{BlockHeight, Network, wire::encode_zinder_native_chain_name};
use zinder_source::SourceBlock;
use zinder_testkit::sample_regtest_upgrade_activations;

const REGTEST_BLOCK_603: &str =
    include_str!("../../../zinder-ingest/tests/fixtures/z3-regtest-ironwood-block-603.json");

pub(crate) fn write_regtest_fixture() -> Result<(TempDir, SourceBlock)> {
    let fixture: Value = serde_json::from_str(REGTEST_BLOCK_603)?;
    let height = fixture
        .get("height")
        .and_then(Value::as_u64)
        .and_then(|height| u32::try_from(height).ok())
        .ok_or_else(|| eyre!("fixture height must fit in u32"))?;
    let raw_block_hex = fixture
        .get("raw_block_hex")
        .and_then(Value::as_str)
        .ok_or_else(|| eyre!("fixture raw_block_hex must be a string"))?;
    let block = SourceBlock::from_raw_block_bytes(
        Network::ZcashRegtest,
        BlockHeight::new(height),
        hex::decode(raw_block_hex)?,
    )?;
    let fixture_directory = tempdir()?;
    let descriptor = write_segment(fixture_directory.path(), 0, std::slice::from_ref(&block))?;
    let measurements = measure_fixture_blocks(
        std::slice::from_ref(&block),
        &sample_regtest_upgrade_activations(),
    )?;
    FixtureManifest {
        contract_identity: FIXTURE_CONTRACT_IDENTITY.to_owned(),
        fixture_format_version: FIXTURE_FORMAT_VERSION,
        network: encode_zinder_native_chain_name(Network::ZcashRegtest).to_owned(),
        from_height: height,
        to_height: height,
        block_count: 1,
        workload_density: measurements.workload_density,
        canonical_artifact_schema_version: zinder_store::CURRENT_ARTIFACT_SCHEMA_VERSION.value(),
        canonical_block_facts_digest_evidence: measurements
            .canonical_block_facts_digest_evidence()?,
        tip_hash_hex: hex::encode(block.hash.as_bytes()),
        network_upgrade_activations: regtest_activation_records(),
        segments: vec![descriptor],
        subtree_roots: SubtreeRootSet::default(),
    }
    .write(fixture_directory.path())?;
    Ok((fixture_directory, block))
}

pub(crate) fn regtest_activation_records() -> Vec<ActivationRecord> {
    sample_regtest_upgrade_activations()
        .activations()
        .iter()
        .map(|activation| ActivationRecord {
            branch_id: activation.branch_id.value(),
            activation_height: activation.activation_height.value(),
            name: activation.name.clone(),
        })
        .collect()
}
