#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::error::Error;

use serde_json::Value;
use sha2::{Digest as _, Sha256};
use zebra_chain::{
    block::Block as ZebraBlock,
    serialization::{ZcashDeserializeInto, ZcashSerialize},
};
use zinder_core::{
    BlockHash, BlockHeight, CanonicalBlockFactsDigestVersion,
    CanonicalBlockFactsSequenceDigestBuilder, CanonicalBlockFactsSequenceDigestVersion,
    CanonicalTransactionFacts, ConsensusBranchId, Network, TransactionFactsArtifact,
    TransactionLocation, TransparentInputFact, decode_canonical_block_replay,
    wire::encode_rpc_transaction_id_hex,
};
use zinder_ingest::{
    BlockMismatchField, CanonicalBlockConstructionError, CommitmentTreeSizes,
    PositionedCanonicalBlock, RawBlobPolicy, position_canonical_block, prepare_canonical_block,
};
use zinder_materialized_views::project_block_summary_record;
use zinder_source::{SourceBlock, decode_rpc_block_hash};
use zinder_store::encode_compact_block_artifact;
use zinder_testkit::{
    FixtureTransactionRows, encode_fixture_block_replay_with_raw_block,
    sample_regtest_upgrade_activations,
};

/// Runs the production two-stage pipeline against an empty offset.
///
/// Calls `prepare_canonical_block` then `position_canonical_block` on `source_block`
/// with a zeroed running commitment-tree position, returning the
/// [`PositionedCanonicalBlock`]. Use this when a test wants the block-local facts
/// together with position-dependent compact metadata.
fn build_positioned_block_for_test(
    source_block: &SourceBlock,
) -> Result<PositionedCanonicalBlock, CanonicalBlockConstructionError> {
    let activations = sample_regtest_upgrade_activations();
    let prepared = prepare_canonical_block(source_block, &activations, RawBlobPolicy::All)?;
    let mut tree_sizes = CommitmentTreeSizes::default();
    position_canonical_block(prepared, &mut tree_sizes)
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "this end-to-end fixture test asserts the full artifact surface in one place"
)]
fn fixture_block_builds_canonical_facts() -> Result<(), Box<dyn Error>> {
    let source_block = fixture_source_block()?;
    let block = build_positioned_block_for_test(&source_block)?;
    assert_transaction_bytes_match_parsed_source(&source_block, &block)?;
    let block_header_artifact = block.facts.block_header.clone();
    let block_blob_artifact = block
        .retained_raw_blobs
        .block_blob
        .clone()
        .ok_or("all-retention fixture must include a block blob")?;
    let mut transaction_rows = Vec::with_capacity(block.facts.transactions.len());
    for (tx_index_in_block, transaction) in block.facts.transactions.iter().enumerate() {
        let tx_index_in_block = u32::try_from(tx_index_in_block)?;
        let transaction_id = transaction.public_facts.transaction_id;
        let location = TransactionLocation::new(
            transaction_id,
            block_header_artifact.height,
            block_header_artifact.block_hash,
            tx_index_in_block,
        );
        let mut rows =
            FixtureTransactionRows::from_public_facts(location, transaction.public_facts.clone())
                .with_intrinsic_value_balances(transaction.intrinsic_value_balances);
        rows.facts = TransactionFactsArtifact::new(location, transaction.public_facts.clone())
            .with_transparent_facts(
                transaction.transparent_inputs.clone(),
                transaction.transparent_outputs.clone(),
            );
        rows.blob = Some(
            block
                .retained_raw_blobs
                .transaction_blobs
                .iter()
                .find(|blob| blob.location == location)
                .ok_or("all-retention fixture must include every transaction blob")?
                .clone(),
        );
        transaction_rows.push(rows);
    }
    let expected_replay_envelope = encode_fixture_block_replay_with_raw_block(
        &block_header_artifact,
        &source_block.raw_block_bytes,
        &transaction_rows,
    );
    assert_eq!(block.replay_envelope, expected_replay_envelope);
    let expected_block_summary = project_block_summary_record(&block.facts);
    let replay_envelope = block.replay_envelope;
    let compact_block_artifact = block.compact_block;

    assert_eq!(block_header_artifact.height, BlockHeight::new(1));
    assert_eq!(block_header_artifact.block_hash, source_block.hash);
    assert_eq!(block_header_artifact.parent_hash, source_block.parent_hash);
    assert_eq!(block_header_artifact.block_time, 1_296_694_002);
    assert_eq!(block_blob_artifact.height, BlockHeight::new(1));
    assert_eq!(block_blob_artifact.block_hash, source_block.hash);
    assert_eq!(block_blob_artifact.parent_hash, source_block.parent_hash);
    assert_eq!(
        block_blob_artifact.raw_block_bytes,
        source_block.raw_block_bytes
    );

    assert_eq!(compact_block_artifact.height(), BlockHeight::new(1));
    assert_eq!(compact_block_artifact.block_hash(), source_block.hash);
    assert_eq!(
        compact_block_artifact.previous_block_hash(),
        source_block.parent_hash
    );
    assert_eq!(compact_block_artifact.time(), 1_296_694_002);
    let chain_metadata = compact_block_artifact.chain_metadata();
    assert_eq!(chain_metadata.sapling_commitment_tree_size, 0);
    assert_eq!(chain_metadata.orchard_commitment_tree_size, 0);

    let compact_transaction = compact_block_artifact
        .transactions()
        .first()
        .ok_or("compact block missing coinbase compact transaction")?;
    assert_eq!(compact_block_artifact.transactions().len(), 1);
    assert_eq!(compact_transaction.index, 0);
    assert_eq!(compact_transaction.transaction_id.as_bytes().len(), 32);
    assert!(compact_transaction.data.transparent_inputs.is_empty());
    assert!(compact_transaction.data.sapling_spends.is_empty());
    assert!(compact_transaction.data.sapling_outputs.is_empty());
    assert!(compact_transaction.data.orchard_actions.is_empty());

    let transparent_output = compact_transaction
        .data
        .transparent_outputs
        .first()
        .ok_or("compact transaction missing coinbase output")?;
    assert_eq!(compact_transaction.data.transparent_outputs.len(), 1);
    assert_eq!(transparent_output.value_zat, 625_000_000);
    assert_eq!(
        hex::encode(&transparent_output.script_pub_key),
        "76a914b75028cd1ea0ca554fd5e7c8cc7ad70a89b8dd4f88ac"
    );

    let decoded_replay = decode_canonical_block_replay(replay_envelope.as_bytes())?;
    assert_eq!(
        project_block_summary_record(decoded_replay.facts()),
        expected_block_summary,
        "the replay envelope must reproduce the BlockSummary materialized view without hydration"
    );

    Ok(())
}

#[test]
fn canonical_block_facts_keep_transparent_state_block_local() -> Result<(), Box<dyn Error>> {
    let source_block = fixture_source_block_from(
        Network::ZcashRegtest,
        include_str!("../fixtures/z3-regtest-ironwood-block-603.json"),
    )?;
    let PositionedCanonicalBlock { facts, .. } = build_positioned_block_for_test(&source_block)?;
    let transparent_input = facts
        .transactions
        .iter()
        .flat_map(|transaction| &transaction.transparent_inputs)
        .find(|input| !input.spent_outpoint.is_coinbase_sentinel())
        .ok_or("Ironwood fixture must contain a non-coinbase transparent input")?;
    let &TransparentInputFact {
        input_index,
        spent_outpoint,
    } = transparent_input;

    assert_eq!(input_index, 0);
    assert!(!spent_outpoint.is_coinbase_sentinel());
    assert!(
        facts
            .transactions
            .iter()
            .any(|transaction| !transaction.transparent_outputs.is_empty())
    );

    Ok(())
}

#[test]
fn canonical_block_facts_digest_is_stable_and_content_sensitive() -> Result<(), Box<dyn Error>> {
    let source_block = fixture_source_block()?;
    let facts = build_positioned_block_for_test(&source_block)?.facts;
    let repeated_facts = build_positioned_block_for_test(&source_block)?.facts;
    let digest = facts.digest(CanonicalBlockFactsDigestVersion::CURRENT);

    assert_eq!(
        digest,
        repeated_facts.digest(CanonicalBlockFactsDigestVersion::CURRENT)
    );
    assert_eq!(
        hex::encode(digest.as_bytes()),
        "76474516b2f4184e9f434750ed6970ed8eac53e5fbb698e1959dcc6c33c5456f"
    );

    let mut changed_facts = facts;
    changed_facts.block_header.block_time = changed_facts
        .block_header
        .block_time
        .checked_add(1)
        .ok_or("fixture block time cannot be incremented")?;
    let changed_digest = changed_facts.digest(CanonicalBlockFactsDigestVersion::CURRENT);
    assert_ne!(digest, changed_digest);

    let mut reference_builder = CanonicalBlockFactsSequenceDigestBuilder::new(
        CanonicalBlockFactsSequenceDigestVersion::CURRENT,
    );
    reference_builder.try_append(digest)?;
    reference_builder.try_append(changed_digest)?;
    let reference_sequence_digest = reference_builder.finish();
    assert_eq!(
        reference_sequence_digest.version(),
        CanonicalBlockFactsSequenceDigestVersion::CURRENT
    );
    assert_eq!(
        hex::encode(reference_sequence_digest.as_bytes()),
        "ed2a1aad9e6f996361f05f507b02f4ed8a7da714ad02ebaf81747cccea6f5170"
    );

    let mut repeated_builder = CanonicalBlockFactsSequenceDigestBuilder::new(
        CanonicalBlockFactsSequenceDigestVersion::CURRENT,
    );
    repeated_builder.try_append(digest)?;
    repeated_builder.try_append(changed_digest)?;
    assert_eq!(reference_sequence_digest, repeated_builder.finish());
    assert_eq!(reference_sequence_digest.block_count(), 2);

    let mut reordered_builder = CanonicalBlockFactsSequenceDigestBuilder::new(
        CanonicalBlockFactsSequenceDigestVersion::CURRENT,
    );
    reordered_builder.try_append(changed_digest)?;
    reordered_builder.try_append(digest)?;
    assert_ne!(reference_sequence_digest, reordered_builder.finish());

    Ok(())
}

#[test]
fn ironwood_canonical_block_facts_digest_matches_known_answer() -> Result<(), Box<dyn Error>> {
    let source_block = fixture_source_block_from(
        Network::ZcashRegtest,
        include_str!("../fixtures/z3-regtest-ironwood-block-603.json"),
    )?;
    let block = build_positioned_block_for_test(&source_block)?;
    assert_transaction_bytes_match_parsed_source(&source_block, &block)?;
    assert_compact_transaction_ids_match_canonical_facts(&block)?;
    let facts = block.facts;

    assert!(facts.transactions.len() > 1);
    assert!(facts.transactions.iter().any(|transaction| {
        transaction.public_facts.version == zinder_core::TransactionVersion::V6
            && transaction.public_facts.auth_digest.is_some()
            && !transaction.transparent_inputs.is_empty()
    }));
    assert_eq!(
        hex::encode(
            facts
                .digest(CanonicalBlockFactsDigestVersion::CURRENT)
                .as_bytes()
        ),
        "86cb193efe15992d994f9813e061d8f3b247fd86980d50a2585ee1137a0d3f70"
    );

    Ok(())
}

/// Known answers for every captured historical block: the canonical facts
/// digest and the SHA-256 of the stored compact-block encoding.
#[test]
fn historical_fixture_blocks_keep_known_facts_and_compact_block_bytes() -> Result<(), Box<dyn Error>>
{
    let fixtures = [
        (
            Network::ZcashRegtest,
            include_str!("../fixtures/z3-regtest-block-1.json"),
        ),
        (
            Network::ZcashRegtest,
            include_str!("../fixtures/z3-regtest-ironwood-block-603.json"),
        ),
        (
            Network::ZcashTestnet,
            include_str!("../fixtures/zcash-testnet-sapling-block-1842432.json"),
        ),
        (
            Network::ZcashTestnet,
            include_str!("../fixtures/zcash-testnet-orchard-block-1842462.json"),
        ),
        (
            Network::ZcashRegtest,
            include_str!("../fixtures/zakura-regtest-ironwood-block-3.json"),
        ),
    ];
    let mut observed = Vec::with_capacity(fixtures.len());
    for (network, fixture_json) in fixtures {
        let source_block = fixture_source_block_from(network, fixture_json)?;
        let block = build_positioned_block_for_test(&source_block)?;
        let compact_block_bytes = encode_compact_block_artifact(&block.compact_block);
        observed.push((
            source_block.height.value(),
            hex::encode(
                block
                    .facts
                    .digest(CanonicalBlockFactsDigestVersion::CURRENT)
                    .as_bytes(),
            ),
            compact_block_bytes.len(),
            hex::encode(Sha256::digest(&compact_block_bytes)),
        ));
    }

    assert_eq!(
        observed,
        [
            (
                1,
                "76474516b2f4184e9f434750ed6970ed8eac53e5fbb698e1959dcc6c33c5456f".to_owned(),
                215,
                "0bc6254121524d279308856f11d27f5e758b7786e405446108ef3004c262c740".to_owned(),
            ),
            (
                603,
                "86cb193efe15992d994f9813e061d8f3b247fd86980d50a2585ee1137a0d3f70".to_owned(),
                613,
                "fa06b824eeed703ed33a0e277fc2a352f2cb3d2804a8ed19c7997f7f0afc73cb".to_owned(),
            ),
            (
                1_842_432,
                "9b04c3c83f7a00b6abab9f25f64506e8b0b6e8f0d46d073824b461029fc050b3".to_owned(),
                2_282,
                "6b45d365d02561d69a8ab566fd8c34dbf27d4d0247d20a8be6cd9a233ee35408".to_owned(),
            ),
            (
                1_842_462,
                "867c271ea799a896212975f9b764898c8d00166df7f27f5ddba7743c77999be3".to_owned(),
                748,
                "48abd79ee8d53c7b9101d919d9d7ea0aad40dca27d996b90a1acb1beac8f3fae".to_owned(),
            ),
            (
                3,
                "6d95a049efc9e3c0fc4ecea22b0f1dba46decaa3435c2a3134c86a3596fe0f93".to_owned(),
                215,
                "bdaaf9b9cacd89c2e92cab82c0cc4e6a698f20b03fa7f7c75d603b7fb47a64ee".to_owned(),
            ),
        ]
    );

    Ok(())
}

#[test]
fn nu7_activation_block_prepares_canonical_facts() -> Result<(), Box<dyn Error>> {
    let fixture_json = include_str!("../fixtures/zakura-regtest-nu7-block-3.json");
    let fixture: Value = serde_json::from_str(fixture_json)?;
    let source_block = fixture_source_block_from(Network::ZcashRegtest, fixture_json)?;
    let block = build_positioned_block_for_test(&source_block)?;
    assert_transaction_bytes_match_parsed_source(&source_block, &block)?;
    assert_compact_transaction_ids_match_canonical_facts(&block)?;

    let [coinbase] = block.facts.transactions.as_slice() else {
        return Err("NU7 activation fixture must contain only its coinbase".into());
    };
    assert_eq!(
        encode_rpc_transaction_id_hex(coinbase.public_facts.transaction_id),
        string_field(&fixture, "coinbase_txid")?
    );
    assert_eq!(
        coinbase.public_facts.version,
        zinder_core::TransactionVersion::V6
    );
    assert_eq!(
        coinbase.public_facts.consensus_branch_id,
        Some(ConsensusBranchId::new(0x7719_0ad9))
    );

    let compact_block_bytes = encode_compact_block_artifact(&block.compact_block);
    assert_eq!(
        (
            hex::encode(
                block
                    .facts
                    .digest(CanonicalBlockFactsDigestVersion::CURRENT)
                    .as_bytes(),
            ),
            compact_block_bytes.len(),
            hex::encode(Sha256::digest(&compact_block_bytes)),
        ),
        (
            "3238484161539ded1f5cb8e6a3b330691d66ceaaebda5f2c19f771c57a854d75".to_owned(),
            214,
            "b1eb170b76cb3ce176d91844eef33ab5f942b36fdc02556367e20f0500aeb709".to_owned(),
        )
    );

    Ok(())
}

fn assert_compact_transaction_ids_match_canonical_facts(
    block: &PositionedCanonicalBlock,
) -> Result<(), Box<dyn Error>> {
    for compact_transaction in block.compact_block.transactions() {
        let transaction_index = usize::try_from(compact_transaction.index)?;
        let transaction_facts = block
            .facts
            .transactions
            .get(transaction_index)
            .ok_or("compact transaction index is outside canonical facts")?;
        assert_eq!(
            compact_transaction.transaction_id, transaction_facts.public_facts.transaction_id,
            "compact transaction id must match the same-position canonical fact"
        );
    }
    Ok(())
}

#[test]
fn fixture_block_separates_ordered_facts_from_retained_transaction_bytes()
-> Result<(), Box<dyn Error>> {
    let source_block = fixture_source_block()?;
    let block = build_positioned_block_for_test(&source_block)?;
    let transaction_facts = &block.facts.transactions;
    assert_eq!(
        transaction_facts.len(),
        1,
        "regtest fixture block 1 has a single coinbase transaction"
    );
    let coinbase_facts = transaction_facts
        .first()
        .ok_or("transaction facts vector is empty")?;
    let coinbase_blob = block
        .retained_raw_blobs
        .transaction_blobs
        .first()
        .ok_or("coinbase transaction blob is missing")?;
    assert!(
        !coinbase_blob.raw_transaction_bytes.is_empty(),
        "coinbase serialized payload bytes must be present"
    );
    assert_eq!(
        coinbase_blob.location.transaction_id,
        coinbase_facts.public_facts.transaction_id
    );
    assert_eq!(coinbase_facts.transparent_outputs.len(), 1);
    assert_eq!(coinbase_facts.transparent_outputs[0].output_index, 0);
    assert_coinbase_branch_id_uses_activation_table(transaction_facts, &source_block)?;

    Ok(())
}

fn assert_transaction_bytes_match_parsed_source(
    source_block: &SourceBlock,
    block: &PositionedCanonicalBlock,
) -> Result<(), Box<dyn Error>> {
    let parsed_block: ZebraBlock = source_block
        .raw_block_bytes
        .as_slice()
        .zcash_deserialize_into()?;
    assert_eq!(
        parsed_block.transactions.len(),
        block.facts.transactions.len(),
        "canonical facts must preserve every parsed transaction"
    );
    assert_eq!(
        parsed_block.transactions.len(),
        block.retained_raw_blobs.transaction_blobs.len(),
        "all-retention construction must retain every parsed transaction"
    );

    for ((parsed_transaction, transaction_facts), transaction_blob) in parsed_block
        .transactions
        .iter()
        .zip(&block.facts.transactions)
        .zip(&block.retained_raw_blobs.transaction_blobs)
    {
        let parsed_transaction_bytes = parsed_transaction.zcash_serialize_to_vec()?;
        assert_eq!(
            transaction_blob.raw_transaction_bytes, parsed_transaction_bytes,
            "retained transaction bytes must be the exact source transaction slice"
        );
        assert_eq!(
            transaction_blob.location.transaction_id, transaction_facts.public_facts.transaction_id,
            "retained transaction order must match canonical fact order"
        );
        assert_eq!(
            transaction_facts.serialized_bytes_digest,
            zinder_core::SerializedBytesDigest::from_serialized_bytes(&parsed_transaction_bytes),
            "canonical transaction facts must bind the exact source transaction bytes"
        );
    }

    Ok(())
}

fn assert_coinbase_branch_id_uses_activation_table(
    transaction_facts: &[CanonicalTransactionFacts],
    source_block: &SourceBlock,
) -> Result<(), Box<dyn Error>> {
    let coinbase_facts = transaction_facts
        .first()
        .ok_or("transaction facts vector is empty")?;
    assert_eq!(
        coinbase_facts.public_facts.consensus_branch_id,
        Some(sample_regtest_upgrade_activations().consensus_branch_id_at(source_block.height)),
        "mined transaction facts must use the network-upgrade activation table"
    );
    Ok(())
}

#[test]
fn testnet_sapling_block_compact_artifact_carries_sapling_outputs() -> Result<(), Box<dyn Error>> {
    let source_block = fixture_source_block_from(
        Network::ZcashTestnet,
        include_str!("../fixtures/zcash-testnet-sapling-block-1842432.json"),
    )?;
    let compact_block = build_positioned_block_for_test(&source_block)?.compact_block;
    let chain_metadata = compact_block.chain_metadata();

    assert_eq!(compact_block.height(), BlockHeight::new(1_842_432));
    assert_eq!(chain_metadata.sapling_commitment_tree_size, 1);
    assert_eq!(chain_metadata.orchard_commitment_tree_size, 0);

    let sapling_transaction = compact_block
        .transactions()
        .iter()
        .find(|transaction| !transaction.data.sapling_outputs.is_empty())
        .ok_or("compact block missing Sapling-bearing transaction")?;
    assert_eq!(sapling_transaction.data.sapling_outputs.len(), 1);
    assert!(sapling_transaction.data.orchard_actions.is_empty());

    let sapling_output = sapling_transaction
        .data
        .sapling_outputs
        .first()
        .ok_or("compact transaction missing Sapling output")?;
    assert_eq!(sapling_output.commitment.len(), 32);
    assert_eq!(sapling_output.ephemeral_key.len(), 32);
    assert_eq!(sapling_output.ciphertext.len(), 52);

    Ok(())
}

#[test]
fn testnet_orchard_block_compact_artifact_carries_orchard_actions() -> Result<(), Box<dyn Error>> {
    let source_block = fixture_source_block_from(
        Network::ZcashTestnet,
        include_str!("../fixtures/zcash-testnet-orchard-block-1842462.json"),
    )?;
    let compact_block = build_positioned_block_for_test(&source_block)?.compact_block;
    let chain_metadata = compact_block.chain_metadata();

    assert_eq!(compact_block.height(), BlockHeight::new(1_842_462));
    assert_eq!(chain_metadata.sapling_commitment_tree_size, 0);
    assert_eq!(chain_metadata.orchard_commitment_tree_size, 2);

    let orchard_transaction = compact_block
        .transactions()
        .iter()
        .find(|transaction| !transaction.data.orchard_actions.is_empty())
        .ok_or("compact block missing Orchard-bearing transaction")?;
    assert_eq!(orchard_transaction.data.orchard_actions.len(), 2);
    assert!(orchard_transaction.data.sapling_outputs.is_empty());

    for action in &orchard_transaction.data.orchard_actions {
        assert_eq!(action.nullifier.len(), 32);
        assert_eq!(action.commitment.len(), 32);
        assert_eq!(action.ephemeral_key.len(), 32);
        assert_eq!(action.ciphertext.len(), 52);
    }

    Ok(())
}

#[test]
fn regtest_ironwood_block_compact_artifact_carries_ironwood_actions() -> Result<(), Box<dyn Error>>
{
    let source_block = fixture_source_block_from(
        Network::ZcashRegtest,
        include_str!("../fixtures/z3-regtest-ironwood-block-603.json"),
    )?;
    let compact_block = build_positioned_block_for_test(&source_block)?.compact_block;
    let chain_metadata = compact_block.chain_metadata();

    assert_eq!(compact_block.height(), BlockHeight::new(603));
    assert_eq!(chain_metadata.orchard_commitment_tree_size, 0);
    assert_eq!(chain_metadata.ironwood_commitment_tree_size, 2);

    let ironwood_transaction = compact_block
        .transactions()
        .iter()
        .find(|transaction| !transaction.data.ironwood_actions.is_empty())
        .ok_or("compact block missing Ironwood-bearing transaction")?;
    assert_eq!(ironwood_transaction.data.ironwood_actions.len(), 2);
    assert!(ironwood_transaction.data.orchard_actions.is_empty());
    assert!(ironwood_transaction.data.transparent_outputs.is_empty());
    assert_eq!(ironwood_transaction.data.transparent_inputs.len(), 1);

    for action in &ironwood_transaction.data.ironwood_actions {
        assert_eq!(action.nullifier.len(), 32);
        assert_eq!(action.commitment.len(), 32);
        assert_eq!(action.ephemeral_key.len(), 32);
        assert_eq!(action.ciphertext.len(), 52);
    }

    Ok(())
}

#[test]
fn regtest_block_without_orchard_actions_carries_forward_tree_size() -> Result<(), Box<dyn Error>> {
    let source_block = fixture_source_block()?;
    let activations = sample_regtest_upgrade_activations();
    let prepared = prepare_canonical_block(&source_block, &activations, RawBlobPolicy::All)?;

    assert_eq!(prepared.tree_size_additions.orchard, 0);
    assert_eq!(prepared.tree_size_additions.sapling, 0);
    assert_eq!(prepared.tree_size_additions.ironwood, 0);

    let mut running_tree_sizes = CommitmentTreeSizes {
        sapling: 11,
        orchard: 42,
        ironwood: 7,
    };
    let positioned_block = position_canonical_block(prepared, &mut running_tree_sizes)?;

    assert_eq!(running_tree_sizes.orchard, 42);
    assert_eq!(running_tree_sizes.sapling, 11);
    assert_eq!(running_tree_sizes.ironwood, 7);

    let compact_block = positioned_block.compact_block;
    let chain_metadata = compact_block.chain_metadata();
    assert_eq!(chain_metadata.orchard_commitment_tree_size, 42);
    assert_eq!(chain_metadata.sapling_commitment_tree_size, 11);
    assert_eq!(chain_metadata.ironwood_commitment_tree_size, 7);
    assert!(
        compact_block
            .transactions()
            .iter()
            .all(|transaction| transaction.data.orchard_actions.is_empty()
                && transaction.data.ironwood_actions.is_empty()),
        "regtest block 1 carries no Orchard or Ironwood actions"
    );

    Ok(())
}

#[test]
fn compact_block_builder_rejects_source_identity_mismatch() -> Result<(), Box<dyn Error>> {
    let mut mismatched_hash_block = fixture_source_block()?;
    mismatched_hash_block.hash = changed_block_hash(mismatched_hash_block.hash);
    assert_compact_block_mismatch(&mismatched_hash_block, BlockMismatchField::Hash)?;

    let mut mismatched_parent_block = fixture_source_block()?;
    mismatched_parent_block.parent_hash = changed_block_hash(mismatched_parent_block.parent_hash);
    assert_compact_block_mismatch(&mismatched_parent_block, BlockMismatchField::ParentHash)?;

    let mut mismatched_time_block = fixture_source_block()?;
    mismatched_time_block.block_time_seconds = mismatched_time_block
        .block_time_seconds
        .checked_add(1)
        .ok_or("fixture block time cannot be incremented")?;
    assert_compact_block_mismatch(&mismatched_time_block, BlockMismatchField::Time)?;

    Ok(())
}

pub(super) fn fixture_source_block() -> Result<SourceBlock, Box<dyn Error>> {
    fixture_source_block_from(
        Network::ZcashRegtest,
        include_str!("../fixtures/z3-regtest-block-1.json"),
    )
}

pub(super) fn fixture_ironwood_source_block() -> Result<SourceBlock, Box<dyn Error>> {
    fixture_source_block_from(
        Network::ZcashRegtest,
        include_str!("../fixtures/z3-regtest-ironwood-block-603.json"),
    )
}

pub(super) fn fixture_sapling_source_block() -> Result<SourceBlock, Box<dyn Error>> {
    fixture_source_block_from(
        Network::ZcashTestnet,
        include_str!("../fixtures/zcash-testnet-sapling-block-1842432.json"),
    )
}

pub(super) fn fixture_orchard_source_block() -> Result<SourceBlock, Box<dyn Error>> {
    fixture_source_block_from(
        Network::ZcashTestnet,
        include_str!("../fixtures/zcash-testnet-orchard-block-1842462.json"),
    )
}

fn fixture_source_block_from(
    network: Network,
    fixture_json: &str,
) -> Result<SourceBlock, Box<dyn Error>> {
    let fixture: Value = serde_json::from_str(fixture_json)?;
    let raw_block_hex = string_field(&fixture, "raw_block_hex")?;
    let raw_block_bytes = hex::decode(raw_block_hex)?;
    let height = u32_field(&fixture, "height")?;
    let source_block =
        SourceBlock::from_raw_block_bytes(network, BlockHeight::new(height), raw_block_bytes)?;

    assert_eq!(
        source_block.hash,
        decode_rpc_block_hash(string_field(&fixture, "hash")?)?
    );
    assert_eq!(
        source_block.parent_hash,
        decode_rpc_block_hash(string_field(&fixture, "previousblockhash")?)?
    );
    assert_eq!(
        source_block.block_time_seconds,
        u32_field(&fixture, "time")?
    );

    Ok(source_block)
}

fn changed_block_hash(hash: BlockHash) -> BlockHash {
    let mut hash_bytes = hash.as_bytes();
    hash_bytes[0] ^= 0xff;
    BlockHash::from_bytes(hash_bytes)
}

fn assert_compact_block_mismatch(
    source_block: &SourceBlock,
    expected_field: BlockMismatchField,
) -> Result<(), Box<dyn Error>> {
    let error = match build_positioned_block_for_test(source_block) {
        Ok(block) => {
            return Err(format!("expected compact block build failure, got {block:?}").into());
        }
        Err(error) => error,
    };

    let CanonicalBlockConstructionError::SourceBlockMismatch { field, .. } = error else {
        return Err(format!("expected source block mismatch, got {error:?}").into());
    };
    if field != expected_field {
        return Err(format!("expected mismatch on {expected_field:?}, got {field:?}").into());
    }
    Ok(())
}

fn string_field<'fixture>(
    fixture: &'fixture Value,
    field_name: &'static str,
) -> Result<&'fixture str, Box<dyn Error>> {
    fixture
        .get(field_name)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("fixture field {field_name} must be a string").into())
}

fn u32_field(fixture: &Value, field_name: &'static str) -> Result<u32, Box<dyn Error>> {
    let number = fixture
        .get(field_name)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("fixture field {field_name} must be an integer"))?;

    Ok(u32::try_from(number)?)
}
