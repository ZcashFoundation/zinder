use std::path::Path;

use tempfile::TempDir;
use zinder_core::{BlockHeight, Network, NetworkUpgradeActivations};

use super::{
    CanonicalReorgPolicy, CanonicalStoreBuildPlan, CanonicalStoreError, CanonicalStoreRequirements,
    CanonicalStoreWorkload, CanonicalStoredData, RocksDbCanonicalSecondary, RocksDbCanonicalStore,
    construction_manifest::validate_ready_construction_manifest,
    live_replacement_tests::published_store_with_retention,
};
use crate::{RawBlobRetention, RocksDbResourceBudget};

const RETENTIONS: [RawBlobRetention; 3] = [
    RawBlobRetention::None,
    RawBlobRetention::Transactions,
    RawBlobRetention::All,
];

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

        for workload in [
            CanonicalStoreWorkload::Wallet,
            CanonicalStoreWorkload::Explorer,
        ] {
            for requested in RETENTIONS {
                let exact = workload == CanonicalStoreWorkload::Wallet && requested == retained;
                let writer = RocksDbCanonicalStore::open_ready(
                    &path,
                    &activations,
                    workload,
                    requested,
                    reorg_policy,
                    RocksDbResourceBudget::for_local_tests(),
                );
                match writer {
                    Ok(writer) => {
                        assert!(exact, "{retained} / {workload:?} / {requested}");
                        assert_eq!(writer.event_fence(), fence);
                    }
                    Err(error) => {
                        assert!(!exact, "{error}");
                        assert!(
                            matches!(error, CanonicalStoreError::AdmissionRefused { .. }),
                            "{error:?}"
                        );
                    }
                }

                let secondary_path = temporary
                    .path()
                    .join(format!("secondary-{workload:?}-{requested}"));
                let reader = open_reader(
                    &path,
                    &secondary_path,
                    &activations,
                    workload,
                    requested,
                    reorg_policy,
                );
                match (reader, missing_requirement(workload, retained, requested)) {
                    (Ok(mut reader), None) => {
                        assert_eq!(reader.stored_set(), stored);
                        assert_eq!(reader.event_fence(), fence);
                        let catch_up = reader.try_catch_up()?;
                        assert_eq!(catch_up.before(), fence);
                        assert_eq!(catch_up.after(), fence);
                        assert_eq!(reader.stored_set(), stored);
                    }
                    (Err(CanonicalStoreError::MissingStoredData { kind, available_from: None, .. }), Some(expected)) => {
                        assert_eq!(kind, expected);
                    }
                    (opened, missing) => return Err(format!(
                        "{retained} / {workload:?} / {requested}: expected missing {missing:?}, got {:?}",
                        opened.err()
                    ).into()),
                }

                let requested_plan = CanonicalStoreBuildPlan {
                    raw_blob_retention: requested,
                    ..build_plan.clone()
                };
                assert_eq!(
                    validate_ready_construction_manifest(&path, &ready, workload, &requested_plan)
                        .is_ok(),
                    exact,
                    "{retained} manifest / {workload:?} / {requested}"
                );
            }
        }
    }
    Ok(())
}

#[expect(
    clippy::too_many_arguments,
    reason = "the matrix exercises both named family requirements and the persisted reorg identity"
)]
fn open_reader(
    path: &Path,
    secondary_path: &Path,
    activations: &NetworkUpgradeActivations,
    workload: CanonicalStoreWorkload,
    requested: RawBlobRetention,
    reorg_policy: CanonicalReorgPolicy,
) -> Result<RocksDbCanonicalSecondary, CanonicalStoreError> {
    let mut required = match requested {
        RawBlobRetention::None => vec![],
        RawBlobRetention::Transactions => vec![CanonicalStoredData::TransactionBodies],
        RawBlobRetention::All => vec![
            CanonicalStoredData::TransactionBodies,
            CanonicalStoredData::BlockBodies,
        ],
    };
    if workload == CanonicalStoreWorkload::Explorer {
        required.push(CanonicalStoredData::FinalNoteCommitmentRoots);
    }
    RocksDbCanonicalSecondary::open_ready_requiring(
        path,
        secondary_path,
        activations,
        CanonicalStoreRequirements::new(&required),
        reorg_policy,
        RocksDbResourceBudget::for_local_tests(),
    )
}

fn missing_requirement(
    workload: CanonicalStoreWorkload,
    retained: RawBlobRetention,
    requested: RawBlobRetention,
) -> Option<CanonicalStoredData> {
    match (retained, requested) {
        (RawBlobRetention::None, RawBlobRetention::Transactions | RawBlobRetention::All) => {
            Some(CanonicalStoredData::TransactionBodies)
        }
        (RawBlobRetention::Transactions, RawBlobRetention::All) => {
            Some(CanonicalStoredData::BlockBodies)
        }
        _ if workload == CanonicalStoreWorkload::Explorer => {
            Some(CanonicalStoredData::FinalNoteCommitmentRoots)
        }
        _ => None,
    }
}
