use tempfile::TempDir;
use zinder_core::{BlockHeight, Network};

use super::{
    CanonicalEventFence, CanonicalReorgPolicy, CanonicalStoreBuildPlan, CanonicalStoreError,
    CanonicalStoreRequirements, CanonicalStoredData, RocksDbCanonicalSecondary,
    RocksDbCanonicalStore, construction_manifest::validate_ready_construction_manifest,
    live_replacement_tests::published_store_with_retention,
};
use crate::{RawBlobRetention, RocksDbResourceBudget};

const RETENTIONS: [RawBlobRetention; 2] = [RawBlobRetention::None, RawBlobRetention::All];

#[test]
fn published_sets_admit_reader_supersets_and_require_exact_writer_and_manifest_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let reorg_policy = CanonicalReorgPolicy::new(2)?;
    for retained in RETENTIONS {
        let temporary = TempDir::new()?;
        let path = temporary.path().join("canonical");
        let writer = published_store_with_retention(
            &path,
            reorg_policy.reorg_window_blocks(),
            BlockHeight::new(4),
            BlockHeight::new(2),
            retained,
        )?;
        let stored = writer.stored_set();
        let fence = writer.event_fence();
        let ready = writer.ready_evidence();
        let build_plan = writer.build_plan().clone();
        drop(writer);

        for requested in RETENTIONS {
            let exact = requested == retained;
            let writer = RocksDbCanonicalStore::open_ready(
                &path,
                &activations,
                requested,
                reorg_policy,
                RocksDbResourceBudget::for_local_tests(),
            );
            assert_writer_admission(writer, exact.then_some(fence));

            let secondary_path = temporary.path().join(format!("secondary-{requested}"));
            let reader = RocksDbCanonicalSecondary::open_ready(
                &path,
                secondary_path,
                &activations,
                reader_requirements(requested),
                reorg_policy,
                RocksDbResourceBudget::for_local_tests(),
            );
            match (reader, missing_requirement(retained, requested)) {
                (Ok(mut reader), None) => {
                    assert_eq!(reader.stored_set(), stored);
                    assert_eq!(reader.event_fence(), fence);
                    let catch_up = reader.try_catch_up()?;
                    assert_eq!(catch_up.before(), fence);
                    assert_eq!(catch_up.after(), fence);
                    assert_eq!(reader.stored_set(), stored);
                }
                (
                    Err(CanonicalStoreError::MissingStoredData {
                        kind,
                        available_from: None,
                        ..
                    }),
                    Some(expected),
                ) => {
                    assert_eq!(kind, expected);
                }
                (opened, missing) => {
                    return Err(format!(
                        "{retained} / {requested}: expected missing {missing:?}, got {:?}",
                        opened.err()
                    )
                    .into());
                }
            }

            let requested_plan = CanonicalStoreBuildPlan {
                raw_blob_retention: requested,
                ..build_plan.clone()
            };
            assert_eq!(
                validate_ready_construction_manifest(&path, &ready, &requested_plan).is_ok(),
                exact,
                "{retained} manifest / {requested}"
            );
        }
    }
    Ok(())
}

fn assert_writer_admission(
    writer: Result<RocksDbCanonicalStore, CanonicalStoreError>,
    expected_fence: Option<CanonicalEventFence>,
) {
    match writer {
        Ok(writer) => assert_eq!(Some(writer.event_fence()), expected_fence),
        Err(error) => {
            assert!(expected_fence.is_none(), "{error}");
            assert!(
                matches!(error, CanonicalStoreError::AdmissionRefused { .. }),
                "{error:?}"
            );
        }
    }
}

fn reader_requirements(requested: RawBlobRetention) -> CanonicalStoreRequirements {
    CanonicalStoreRequirements::for_blob_retention(requested)
}

fn missing_requirement(
    retained: RawBlobRetention,
    requested: RawBlobRetention,
) -> Option<CanonicalStoredData> {
    match (retained, requested) {
        (RawBlobRetention::None, RawBlobRetention::All) => {
            Some(CanonicalStoredData::TransactionBodies)
        }
        _ => None,
    }
}
