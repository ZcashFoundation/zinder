//! Unit tests for the `ExplorerQuery.Search` classifier.
//!
//! Pinned by [ADR-0019](../../../../docs/adrs/0019-explorer-plane.md).
//! Every shielded input class must classify without any IO; the classifier
//! is intentionally a pure function so the privacy invariant is structural.

use eyre::{Result, eyre};
use zinder_core::Network;
use zinder_core::explorer_search::{
    SEARCH_QUERY_MAX_BYTES, SearchClassification, ShieldedReceiverKind,
    UnifiedAddressReceiverClassification, classify_search_input,
};

const MAINNET_P2PKH: &str = "t1Hsc1LR8yKnbbe3twRp88p6vFfC5t7DLbs";
const REGTEST_P2PKH: &str = "tmDpFafuBHKGUYmuwLsrxWJrwcnSyzEEtYx";
const MAINNET_SAPLING: &str =
    "zs1z7rejlpsa98s2rrrfkwmaxu53e4ue0ulcrw0h4x5g8jl04tak0d3mm47vdtahatqrlkngh9slya";
const MAINNET_UNIFIED: &str = "u1pg2aaph7jp8rpf6yhsza25722sg5fcn3vaca6ze27hqjw7\
jvvhhuxkpcg0ge9xh6drsgdkda8qjq5chpehkcpxf87rnjryjqwymdheptpvnljqqrjqzjwkc2ma6hcq\
666kgwfytxwac8eyex6ndgr6ezte66706e3vaqrd25dzvzkc69kw0jgywtd0cmq52q5lkw6uh7hyvzjs\
e8ksx";

#[test]
fn numeric_query_classifies_as_block() {
    let candidates = classify_search_input("12345", Network::ZcashMainnet);
    assert!(matches!(
        candidates.first(),
        Some(SearchClassification::Block { height: 12_345 })
    ));
}

#[test]
fn numeric_overflow_falls_through_to_unclassified() {
    let candidates = classify_search_input("99999999999999999999", Network::ZcashMainnet);
    assert!(matches!(
        candidates.as_slice(),
        [SearchClassification::Unclassified { .. }]
    ));
}

#[test]
fn empty_query_classifies_as_unclassified() {
    let candidates = classify_search_input("   ", Network::ZcashMainnet);
    assert!(matches!(
        candidates.as_slice(),
        [SearchClassification::Unclassified { .. }]
    ));
}

#[test]
fn oversized_query_classifies_as_unclassified_without_decode() -> Result<()> {
    let oversized = "a".repeat(SEARCH_QUERY_MAX_BYTES + 1);
    let candidates = classify_search_input(&oversized, Network::ZcashMainnet);
    let Some(SearchClassification::Unclassified { hint }) = candidates.first() else {
        return Err(eyre!("expected Unclassified candidate"));
    };
    assert!(hint.contains("exceeds the per-request cap"));
    Ok(())
}

#[test]
fn hex_64_chars_classifies_as_hash_candidate() {
    let query = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let candidates = classify_search_input(query, Network::ZcashMainnet);
    assert!(matches!(
        candidates.first(),
        Some(SearchClassification::HashCandidate { .. })
    ));
}

#[test]
fn transparent_p2pkh_classifies_against_network() -> Result<()> {
    let candidates = classify_search_input(MAINNET_P2PKH, Network::ZcashMainnet);
    let Some(SearchClassification::TransparentAddress(classification)) = candidates
        .iter()
        .find(|candidate| matches!(candidate, SearchClassification::TransparentAddress(_)))
    else {
        return Err(eyre!("expected TransparentAddress arm"));
    };
    assert_eq!(classification.canonical_form, MAINNET_P2PKH);
    assert!(classification.is_p2pkh);
    Ok(())
}

#[test]
fn transparent_p2pkh_with_wrong_network_classifies_as_unclassified() -> Result<()> {
    let candidates = classify_search_input(MAINNET_P2PKH, Network::ZcashRegtest);
    let Some(SearchClassification::Unclassified { hint }) = candidates
        .iter()
        .find(|candidate| matches!(candidate, SearchClassification::Unclassified { .. }))
    else {
        return Err(eyre!("expected Unclassified candidate"));
    };
    assert!(hint.contains("different network"));
    Ok(())
}

#[test]
fn regtest_p2pkh_classifies_against_regtest() -> Result<()> {
    let candidates = classify_search_input(REGTEST_P2PKH, Network::ZcashRegtest);
    let Some(SearchClassification::TransparentAddress(classification)) = candidates
        .iter()
        .find(|candidate| matches!(candidate, SearchClassification::TransparentAddress(_)))
    else {
        return Err(eyre!("expected TransparentAddress arm"));
    };
    assert_eq!(classification.canonical_form, REGTEST_P2PKH);
    Ok(())
}

#[test]
fn sapling_classifies_as_shielded_address() -> Result<()> {
    let candidates = classify_search_input(MAINNET_SAPLING, Network::ZcashMainnet);
    let Some(SearchClassification::ShieldedAddress { canonical, network }) = candidates
        .iter()
        .find(|candidate| matches!(candidate, SearchClassification::ShieldedAddress { .. }))
    else {
        return Err(eyre!("expected ShieldedAddress arm"));
    };
    assert_eq!(canonical, MAINNET_SAPLING);
    assert_eq!(*network, Network::ZcashMainnet);
    Ok(())
}

#[test]
fn unified_address_decomposes_per_receiver() -> Result<()> {
    let stripped: String = MAINNET_UNIFIED
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    let candidates = classify_search_input(&stripped, Network::ZcashMainnet);
    let Some(SearchClassification::UnifiedAddress(unified_classification)) = candidates
        .iter()
        .find(|candidate| matches!(candidate, SearchClassification::UnifiedAddress(_)))
    else {
        return Err(eyre!("expected UnifiedAddress arm"));
    };
    assert_eq!(unified_classification.network, Network::ZcashMainnet);
    let has_transparent = unified_classification.receivers.iter().any(|receiver| {
        matches!(
            receiver,
            UnifiedAddressReceiverClassification::Transparent(_)
        )
    });
    let has_orchard = unified_classification.receivers.iter().any(|receiver| {
        matches!(
            receiver,
            UnifiedAddressReceiverClassification::Shielded {
                kind: ShieldedReceiverKind::Orchard,
            }
        )
    });
    assert!(
        has_transparent,
        "unified test vector exposes a transparent receiver",
    );
    assert!(
        has_orchard,
        "unified test vector exposes an Orchard receiver",
    );
    Ok(())
}

#[test]
fn viewing_key_prefix_classifies_as_viewing_key() {
    for prefix in ["uivk1abc", "uviewtest1xyz", "zxviews1foo"] {
        let candidates = classify_search_input(prefix, Network::ZcashMainnet);
        assert!(
            candidates
                .iter()
                .any(|candidate| matches!(candidate, SearchClassification::ViewingKey)),
            "expected ViewingKey arm for {prefix}",
        );
    }
}

#[test]
fn unknown_garbage_classifies_as_unclassified() {
    let candidates = classify_search_input("not-an-address-or-anything", Network::ZcashMainnet);
    assert!(matches!(
        candidates.as_slice(),
        [SearchClassification::Unclassified { .. }]
    ));
}
