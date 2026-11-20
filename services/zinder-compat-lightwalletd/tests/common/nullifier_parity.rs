use eyre::{Result, ensure};
use zinder_proto::compat::lightwalletd;

pub(crate) fn assert_nullifier_range_matches(
    zinder_blocks: &[lightwalletd::CompactBlock],
    reference_blocks: &[lightwalletd::CompactBlock],
) -> Result<()> {
    ensure!(
        zinder_blocks.len() == reference_blocks.len(),
        "nullifier range length differs: Zinder {}, reference {}",
        zinder_blocks.len(),
        reference_blocks.len()
    );
    for (zinder, reference) in zinder_blocks.iter().zip(reference_blocks) {
        assert_nullifier_compact_block_matches(zinder, reference)?;
    }
    Ok(())
}

pub(crate) fn assert_nullifier_compact_block_matches(
    zinder: &lightwalletd::CompactBlock,
    reference: &lightwalletd::CompactBlock,
) -> Result<()> {
    assert_nullifier_response_redacts_non_nullifier_payloads(zinder)?;
    assert_nullifier_response_redacts_non_nullifier_payloads(reference)?;
    ensure!(
        zinder == reference,
        "nullifier block differs at reference height {}",
        reference.height
    );
    Ok(())
}

fn assert_nullifier_response_redacts_non_nullifier_payloads(
    block: &lightwalletd::CompactBlock,
) -> Result<()> {
    ensure!(
        block.chain_metadata == Some(lightwalletd::ChainMetadata::default()),
        "nullifier block requires present, zero-sized tree metadata at height {}",
        block.height
    );
    for tx in &block.vtx {
        ensure!(
            tx.vin.is_empty() && tx.vout.is_empty() && tx.outputs.is_empty(),
            "nullifier block includes transparent fields or Sapling outputs at height {}, transaction {}",
            block.height,
            tx.index
        );
        for action in tx.actions.iter().chain(&tx.ironwood_actions) {
            ensure!(
                action.cmx.is_empty()
                    && action.ephemeral_key.is_empty()
                    && action.ciphertext.is_empty(),
                "nullifier block includes action payload at height {}, transaction {}",
                block.height,
                tx.index
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        assert_nullifier_compact_block_matches, assert_nullifier_range_matches, lightwalletd,
    };

    fn reference_block() -> lightwalletd::CompactBlock {
        lightwalletd::CompactBlock {
            height: 1,
            vtx: vec![lightwalletd::CompactTx {
                txid: vec![7; 32],
                fee: 10_000,
                ..Default::default()
            }],
            chain_metadata: Some(lightwalletd::ChainMetadata::default()),
            ..Default::default()
        }
    }

    #[test]
    fn accepts_present_metadata_with_zero_tree_sizes() -> eyre::Result<()> {
        assert_nullifier_compact_block_matches(&reference_block(), &reference_block())
    }

    #[test]
    fn rejects_missing_metadata_even_when_both_blocks_omit_it() {
        let reference = reference_block();
        let zinder = lightwalletd::CompactBlock {
            chain_metadata: None,
            ..reference.clone()
        };
        assert!(assert_nullifier_compact_block_matches(&zinder, &zinder).is_err());
        assert!(assert_nullifier_compact_block_matches(&zinder, &reference).is_err());
    }

    #[test]
    fn rejects_dropped_transaction() {
        let reference = reference_block();
        let zinder = lightwalletd::CompactBlock {
            vtx: Vec::new(),
            ..reference.clone()
        };
        assert!(assert_nullifier_compact_block_matches(&zinder, &reference).is_err());
    }

    #[test]
    fn rejects_fee_mismatch() {
        let reference = reference_block();
        let mut zinder = reference.clone();
        zinder.vtx[0].fee += 1;
        assert!(assert_nullifier_compact_block_matches(&zinder, &reference).is_err());
    }

    #[test]
    fn rejects_missing_block() {
        assert!(assert_nullifier_range_matches(&[], &[reference_block()]).is_err());
    }

    #[test]
    fn rejects_nonzero_tree_sizes() {
        let reference = reference_block();
        let zinder = lightwalletd::CompactBlock {
            chain_metadata: Some(lightwalletd::ChainMetadata {
                orchard_commitment_tree_size: 1,
                ..Default::default()
            }),
            ..reference.clone()
        };
        assert!(assert_nullifier_compact_block_matches(&zinder, &zinder).is_err());
        assert!(assert_nullifier_compact_block_matches(&reference, &zinder).is_err());
    }
}
