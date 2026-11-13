//! Checked-in generated artifact invariants.

use zinder_proto::compat::lightwalletd::{
    LIGHTWALLETD_PROTOCOL_COMMIT, LIGHTWALLETD_PROTOCOL_VERSION,
};
use zinder_proto::external::zebra_indexer_rpc::ZEBRA_INDEXER_PROTOCOL_COMMIT;

#[test]
fn vendored_protocol_commit_constants_are_exact_trimmed_ids() {
    assert_commit_id(
        LIGHTWALLETD_PROTOCOL_COMMIT,
        include_str!("../../proto/compat/lightwalletd/COMMIT").trim(),
    );
    assert_commit_id(
        ZEBRA_INDEXER_PROTOCOL_COMMIT,
        include_str!("../../proto/external/zebra/COMMIT").trim(),
    );
}

#[test]
fn vendored_lightwalletd_protocol_version_is_the_pinned_release_tag() {
    assert_eq!(
        LIGHTWALLETD_PROTOCOL_VERSION,
        include_str!("../../proto/compat/lightwalletd/VERSION").trim(),
    );
    let numbers = LIGHTWALLETD_PROTOCOL_VERSION
        .strip_prefix('v')
        .map(|numbers| numbers.split('.').collect::<Vec<_>>());
    assert!(
        numbers.is_some_and(|parts| parts.len() == 3
            && parts
                .iter()
                .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))),
        "{LIGHTWALLETD_PROTOCOL_VERSION} is not a vMAJOR.MINOR.PATCH tag",
    );
    let provenance = include_str!("../../proto/compat/lightwalletd/UPSTREAM.md");
    assert!(
        provenance.contains(&format!(
            "`{LIGHTWALLETD_PROTOCOL_COMMIT}` (tag `{LIGHTWALLETD_PROTOCOL_VERSION}`)"
        )),
        "UPSTREAM.md must name the pinned commit and its release tag together",
    );
}

fn assert_commit_id(actual: &str, expected: &str) {
    assert_eq!(actual, expected);
    assert_eq!(actual.len(), 40);
    assert!(actual.bytes().all(|byte| byte.is_ascii_hexdigit()));
}
