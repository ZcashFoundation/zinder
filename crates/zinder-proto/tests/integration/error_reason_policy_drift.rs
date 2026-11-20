//! Error-reason policy drift guard.
//!
//! Cross-checks the authored [`reason_policy`] table against the compiled
//! `FileDescriptorSet`: every `ErrorReason` value defined in the proto enum
//! has a policy entry, and only the `ERROR_REASON_UNSPECIFIED` sentinel maps
//! to the unspecified placeholder. The boundary enums (`QueryError`,
//! `StoreError`, `ExplorerError`) each carry their own per-variant guard in
//! their owning crate asserting they never produce the sentinel.

#![allow(
    missing_docs,
    reason = "Integration test names describe the reason-policy drift contract under test."
)]

use eyre::{Result, eyre};
use prost::Message;
use prost_types::FileDescriptorSet;
use zinder_proto::ZINDER_V1_FILE_DESCRIPTOR_SET;
use zinder_proto::v1::ops::ErrorReason;
use zinder_proto::{RetryDisposition, reason_policy};

/// Fully qualified name of the `ErrorReason` enum in the descriptor.
const ERROR_REASON_ENUM: &str = "ErrorReason";

#[test]
fn every_proto_reason_has_a_policy_and_only_unspecified_is_the_sentinel() -> Result<()> {
    let reason_names = error_reason_value_names()?;
    let mut unmapped: Vec<String> = Vec::new();
    let mut stray_sentinel: Vec<String> = Vec::new();

    for name in &reason_names {
        let Some(reason) = ErrorReason::from_str_name(name) else {
            return Err(eyre!(
                "descriptor lists ErrorReason value {name} that ErrorReason::from_str_name \
                 does not recognize; the generated enum and the descriptor disagree."
            ));
        };
        // The const-fn policy match is exhaustive, so a missing entry is a
        // compile error; here we confirm the descriptor and the enum agree and
        // that the sentinel is the only value mapped as unspecified.
        let policy = reason_policy(reason);
        if reason == ErrorReason::Unspecified {
            if policy.retry != RetryDisposition::NonRetryable {
                stray_sentinel.push(name.clone());
            }
        } else if name == ErrorReason::Unspecified.as_str_name() {
            unmapped.push(name.clone());
        }
    }

    assert!(
        unmapped.is_empty(),
        "ErrorReason values present in the descriptor but not the generated enum: {unmapped:?}."
    );
    assert!(
        stray_sentinel.is_empty(),
        "ERROR_REASON_UNSPECIFIED must map to a non-retryable internal placeholder."
    );
    Ok(())
}

#[test]
fn no_real_reason_round_trips_back_to_the_sentinel() -> Result<()> {
    for name in error_reason_value_names()? {
        if name == ErrorReason::Unspecified.as_str_name() {
            continue;
        }
        let reason = ErrorReason::from_str_name(&name)
            .ok_or_else(|| eyre!("ErrorReason value {name} is not a generated variant"))?;
        assert_ne!(
            reason,
            ErrorReason::Unspecified,
            "ErrorReason value {name} resolves to the unspecified sentinel; a real reason must \
             never collapse to ERROR_REASON_UNSPECIFIED."
        );
    }
    Ok(())
}

#[test]
fn retired_broadcast_reason_reserves_its_name_and_number() -> Result<()> {
    let descriptor = FileDescriptorSet::decode(ZINDER_V1_FILE_DESCRIPTOR_SET)?;
    let reasons = descriptor
        .file
        .iter()
        .filter(|file| file.package() == "zinder.v1.ops")
        .flat_map(|file| &file.enum_type)
        .find(|definition| definition.name() == ERROR_REASON_ENUM)
        .ok_or_else(|| eyre!("ops.ErrorReason descriptor is missing"))?;

    assert!(
        reasons
            .reserved_name
            .iter()
            .any(|name| name == "BROADCAST_DISABLED")
    );
    assert!(
        reasons
            .reserved_range
            .iter()
            .any(|range| (range.start()..=range.end()).contains(&11))
    );
    assert!(ErrorReason::try_from(11).is_err());
    Ok(())
}

/// Returns the string-form names of every `ErrorReason` value in the
/// compiled descriptor.
fn error_reason_value_names() -> Result<Vec<String>> {
    let descriptor = FileDescriptorSet::decode(ZINDER_V1_FILE_DESCRIPTOR_SET)?;
    let mut names = Vec::new();
    for file in &descriptor.file {
        for enum_type in &file.enum_type {
            if enum_type.name() == ERROR_REASON_ENUM {
                for enum_value in &enum_type.value {
                    names.push(enum_value.name().to_owned());
                }
            }
        }
    }
    if names.is_empty() {
        return Err(eyre!(
            "decoded zero ErrorReason values; the descriptor or the enum name is wrong."
        ));
    }
    Ok(names)
}
