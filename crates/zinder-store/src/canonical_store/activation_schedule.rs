//! Writer-owned network-upgrade schedule journal.
//!
//! The construction fingerprint in the build plan is immutable. Each journal
//! revision records the complete activation table the canonical writer
//! admitted, the visible tip it was admitted at, and a digest link to the
//! previous revision. Revision 1 proves that the construction table is an
//! exact prefix of its table; later revisions may change only entries above
//! their admission tip. Store control binds the latest revision by number and
//! digest.

use std::path::Path;

use rust_rocksdb::{DB, WriteBatch};
use sha2::{Digest, Sha256};
use zinder_core::{
    BlockHeight, ConsensusBranchId, Network, NetworkUpgradeActivation, NetworkUpgradeActivations,
    NetworkUpgradeActivationsFingerprintVersion,
};

use super::{CanonicalStoreError, control::ActivationScheduleBinding};

pub(super) const ACTIVATION_SCHEDULE_KEY_PREFIX: &[u8] = b"activation_schedule/";
const DIGEST_DOMAIN: &[u8] = b"zinder.canonical.activation-schedule.digest.v1\0";
const RECORD_MAGIC: &[u8] = b"ZASR";
const RECORD_VERSION: u16 = 1;
const MAX_ACTIVATION_COUNT: usize = 4096;
const MAX_ACTIVATION_NAME_BYTES: usize = 1024;
const MAX_RECORD_BYTES: usize = 8 * 1024 * 1024;
const MAX_SCHEDULE_REVISIONS: u64 = 4096;

/// One authenticated, writer-admitted network-upgrade schedule revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalActivationSchedule {
    revision: u64,
    baseline_fingerprint: [u8; 32],
    baseline_prefix_len: u32,
    previous_digest: Option<[u8; 32]>,
    admitted_tip: zinder_core::BlockId,
    activations: NetworkUpgradeActivations,
    digest: [u8; 32],
}

impl CanonicalActivationSchedule {
    pub(super) fn initial(
        activations: NetworkUpgradeActivations,
        baseline_fingerprint: [u8; 32],
        baseline_prefix_len: u32,
        admitted_tip: zinder_core::BlockId,
    ) -> Result<Self, String> {
        Self::new(
            1,
            baseline_fingerprint,
            baseline_prefix_len,
            None,
            admitted_tip,
            activations,
        )
    }

    pub(super) fn successor(
        previous: &Self,
        admitted_tip: zinder_core::BlockId,
        activations: NetworkUpgradeActivations,
    ) -> Result<Self, String> {
        let revision = previous
            .revision
            .checked_add(1)
            .ok_or_else(|| "activation schedule revision overflow".to_owned())?;
        Self::new(
            revision,
            previous.baseline_fingerprint,
            previous.baseline_prefix_len,
            Some(previous.digest),
            admitted_tip,
            activations,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "validates all fields that jointly define one persisted schedule revision"
    )]
    fn new(
        revision: u64,
        baseline_fingerprint: [u8; 32],
        baseline_prefix_len: u32,
        previous_digest: Option<[u8; 32]>,
        admitted_tip: zinder_core::BlockId,
        activations: NetworkUpgradeActivations,
    ) -> Result<Self, String> {
        if revision == 0 || (revision == 1) != previous_digest.is_none() {
            return Err("activation schedule revision linkage is invalid".to_owned());
        }
        if revision > MAX_SCHEDULE_REVISIONS {
            return Err("activation schedule revision exceeds the admission bound".to_owned());
        }
        if activations.activations().len() > MAX_ACTIVATION_COUNT {
            return Err("activation schedule contains too many entries".to_owned());
        }
        if activations
            .activations()
            .iter()
            .any(|entry| entry.name.len() > MAX_ACTIVATION_NAME_BYTES)
        {
            return Err("activation schedule contains an overlong name".to_owned());
        }
        let mut schedule = Self {
            revision,
            baseline_fingerprint,
            baseline_prefix_len,
            previous_digest,
            admitted_tip,
            activations,
            digest: [0; 32],
        };
        schedule.digest = schedule.calculate_digest()?;
        Ok(schedule)
    }

    /// Returns the monotonic schedule revision admitted by the canonical writer.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns the complete activation table admitted in this revision.
    #[must_use]
    pub const fn activations(&self) -> &NetworkUpgradeActivations {
        &self.activations
    }

    pub(super) fn encode(&self) -> Result<Vec<u8>, String> {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(RECORD_MAGIC);
        encoded.extend_from_slice(&RECORD_VERSION.to_le_bytes());
        encoded.extend_from_slice(&self.revision.to_le_bytes());
        encoded.extend_from_slice(&self.baseline_fingerprint);
        encoded.extend_from_slice(&self.baseline_prefix_len.to_le_bytes());
        match self.previous_digest {
            Some(digest) => {
                encoded.push(1);
                encoded.extend_from_slice(&digest);
            }
            None => encoded.push(0),
        }
        encoded.extend_from_slice(&self.admitted_tip.height.value().to_le_bytes());
        encoded.extend_from_slice(&self.admitted_tip.hash.as_bytes());
        encoded.extend_from_slice(&self.activations.network().id().to_le_bytes());
        let entries = self.activations.activations();
        encoded.extend_from_slice(
            &u32::try_from(entries.len())
                .map_err(|_| "activation schedule count does not fit u32".to_owned())?
                .to_le_bytes(),
        );
        for activation in entries {
            encoded.extend_from_slice(&activation.branch_id.value().to_le_bytes());
            encoded.extend_from_slice(&activation.activation_height.value().to_le_bytes());
            let name = activation.name.as_bytes();
            encoded.extend_from_slice(
                &u32::try_from(name.len())
                    .map_err(|_| "activation name length does not fit u32".to_owned())?
                    .to_le_bytes(),
            );
            encoded.extend_from_slice(name);
        }
        if encoded.len() > MAX_RECORD_BYTES {
            return Err("activation schedule record exceeds its size limit".to_owned());
        }
        Ok(encoded)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one bounded decode pass validates the exact versioned schedule record"
    )]
    pub(super) fn decode(path: &Path, encoded: &[u8]) -> Result<Self, CanonicalStoreError> {
        if encoded.len() > MAX_RECORD_BYTES {
            return Err(CanonicalStoreError::admission(
                path,
                "activation schedule record exceeds its size limit",
            ));
        }
        let mut decoder = ScheduleDecoder {
            path,
            encoded,
            at: 0,
        };
        if decoder.take(4, "record identity")? != RECORD_MAGIC {
            return Err(CanonicalStoreError::admission(
                path,
                "activation schedule record identity is invalid",
            ));
        }
        if decoder.u16("record version")? != RECORD_VERSION {
            return Err(CanonicalStoreError::admission(
                path,
                "activation schedule record version is unsupported",
            ));
        }
        let revision = decoder.u64("revision")?;
        let baseline_fingerprint = decoder.array("baseline fingerprint")?;
        let baseline_prefix_len = decoder.u32("baseline prefix length")?;
        let previous_digest = match decoder.u8("previous-record marker")? {
            0 => None,
            1 => Some(decoder.array("previous-record digest")?),
            _ => {
                return Err(CanonicalStoreError::admission(
                    path,
                    "activation schedule previous-record marker is invalid",
                ));
            }
        };
        let admitted_tip = zinder_core::BlockId::new(
            BlockHeight::new(decoder.u32("admitted tip height")?),
            zinder_core::BlockHash::from_bytes(decoder.array("admitted tip hash")?),
        );
        let network_id = decoder.u32("network id")?;
        let network = Network::from_id(network_id).ok_or_else(|| {
            CanonicalStoreError::admission(path, "activation schedule network is unknown")
        })?;
        let count = usize::try_from(decoder.u32("activation count")?).map_err(|_| {
            CanonicalStoreError::admission(path, "activation count does not fit memory size")
        })?;
        if count > MAX_ACTIVATION_COUNT {
            return Err(CanonicalStoreError::admission(
                path,
                "activation schedule contains too many entries",
            ));
        }
        let mut activations = Vec::with_capacity(count);
        for _ in 0..count {
            let branch_id = ConsensusBranchId::new(decoder.u32("branch id")?);
            let activation_height = BlockHeight::new(decoder.u32("activation height")?);
            let name_len =
                usize::try_from(decoder.u32("activation name length")?).map_err(|_| {
                    CanonicalStoreError::admission(
                        path,
                        "activation name length does not fit memory",
                    )
                })?;
            if name_len > MAX_ACTIVATION_NAME_BYTES {
                return Err(CanonicalStoreError::admission(
                    path,
                    "activation schedule contains an overlong name",
                ));
            }
            let name = std::str::from_utf8(decoder.take(name_len, "activation name")?)
                .map_err(|_| CanonicalStoreError::admission(path, "activation name is not UTF-8"))?
                .to_owned();
            activations.push(NetworkUpgradeActivation {
                branch_id,
                activation_height,
                name,
            });
        }
        decoder.finish()?;
        let activations = NetworkUpgradeActivations::new(network, activations)
            .map_err(|error| CanonicalStoreError::admission(path, error.to_string()))?;
        let schedule = Self::new(
            revision,
            baseline_fingerprint,
            baseline_prefix_len,
            previous_digest,
            admitted_tip,
            activations,
        )
        .map_err(|reason| CanonicalStoreError::admission(path, reason))?;
        if schedule
            .encode()
            .map_err(|reason| CanonicalStoreError::admission(path, reason))?
            != encoded
        {
            return Err(CanonicalStoreError::admission(
                path,
                "activation schedule record is not canonically encoded",
            ));
        }
        Ok(schedule)
    }

    pub(super) fn key(&self) -> Vec<u8> {
        schedule_key(self.revision)
    }

    pub(super) const fn binding(&self) -> ActivationScheduleBinding {
        ActivationScheduleBinding {
            revision: self.revision,
            digest: self.digest,
        }
    }

    pub(super) fn put_into(&self, batch: &mut WriteBatch) -> Result<(), String> {
        batch.put(self.key(), self.encode()?);
        Ok(())
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one admission pass verifies every linked revision and the immutable baseline"
    )]
    pub(super) fn read_bound(
        db: &DB,
        path: &Path,
        binding: ActivationScheduleBinding,
        expected_baseline_fingerprint: [u8; 32],
    ) -> Result<Self, CanonicalStoreError> {
        if binding.revision > MAX_SCHEDULE_REVISIONS {
            return Err(CanonicalStoreError::admission(
                path,
                "activation schedule revision exceeds the admission bound",
            ));
        }
        let mut previous: Option<Self> = None;
        for revision in 1..=binding.revision {
            let key = schedule_key(revision);
            let encoded = db
                .get(&key)
                .map_err(|source| CanonicalStoreError::RocksDbOperation {
                    operation: "activation schedule read",
                    source,
                })?;
            let encoded = encoded.ok_or_else(|| {
                CanonicalStoreError::admission(
                    path,
                    format!("activation schedule journal is missing revision {revision}"),
                )
            })?;
            let schedule = Self::decode(path, &encoded)?;
            if schedule.revision != revision
                || schedule.baseline_fingerprint != expected_baseline_fingerprint
            {
                return Err(CanonicalStoreError::admission(
                    path,
                    "activation schedule journal revision or construction baseline differs",
                ));
            }
            if let Some(previous_schedule) = &previous {
                if schedule.previous_digest != Some(previous_schedule.digest)
                    || !schedule.activations.preserves_through(
                        &previous_schedule.activations,
                        schedule.admitted_tip.height,
                    )
                {
                    return Err(CanonicalStoreError::admission(
                        path,
                        "activation schedule journal is not a valid future-only revision chain",
                    ));
                }
            } else {
                let baseline_prefix_len =
                    usize::try_from(schedule.baseline_prefix_len).map_err(|_| {
                        CanonicalStoreError::admission(
                            path,
                            "activation schedule baseline prefix exceeds memory size",
                        )
                    })?;
                if schedule.previous_digest.is_some()
                    || baseline_prefix_len > schedule.activations.activations().len()
                {
                    return Err(CanonicalStoreError::admission(
                        path,
                        "activation schedule baseline proof is malformed",
                    ));
                }
                let prefix = NetworkUpgradeActivations::new(
                    schedule.activations.network(),
                    schedule.activations.activations()[..baseline_prefix_len].to_vec(),
                )
                .map_err(|error| CanonicalStoreError::admission(path, error.to_string()))?;
                if prefix
                    .fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1)
                    .as_bytes()
                    != expected_baseline_fingerprint
                    || !schedule
                        .activations
                        .preserves_through(&prefix, schedule.admitted_tip.height)
                {
                    return Err(CanonicalStoreError::admission(
                        path,
                        "activation schedule does not prove the immutable baseline through its admission tip",
                    ));
                }
            }
            if revision == binding.revision && schedule.digest != binding.digest {
                return Err(CanonicalStoreError::admission(
                    path,
                    "activation schedule record digest does not match store control",
                ));
            }
            previous = Some(schedule);
        }
        previous.ok_or_else(|| {
            CanonicalStoreError::admission(path, "activation schedule journal is empty")
        })
    }

    fn calculate_digest(&self) -> Result<[u8; 32], String> {
        let mut digest = Sha256::new();
        digest.update(DIGEST_DOMAIN);
        digest.update(self.encode()?);
        Ok(digest.finalize().into())
    }
}

pub(super) fn schedule_key(revision: u64) -> Vec<u8> {
    let mut key = ACTIVATION_SCHEDULE_KEY_PREFIX.to_vec();
    key.extend_from_slice(&revision.to_be_bytes());
    key
}

struct ScheduleDecoder<'a> {
    path: &'a Path,
    encoded: &'a [u8],
    at: usize,
}

impl ScheduleDecoder<'_> {
    fn take(&mut self, length: usize, field: &'static str) -> Result<&[u8], CanonicalStoreError> {
        let end = self.at.checked_add(length).ok_or_else(|| {
            CanonicalStoreError::admission(self.path, "activation schedule offset overflow")
        })?;
        let bytes = self.encoded.get(self.at..end).ok_or_else(|| {
            CanonicalStoreError::admission(
                self.path,
                format!("activation schedule {field} is truncated"),
            )
        })?;
        self.at = end;
        Ok(bytes)
    }

    fn array<const N: usize>(
        &mut self,
        field: &'static str,
    ) -> Result<[u8; N], CanonicalStoreError> {
        let mut bytes = [0; N];
        bytes.copy_from_slice(self.take(N, field)?);
        Ok(bytes)
    }

    fn u8(&mut self, field: &'static str) -> Result<u8, CanonicalStoreError> {
        Ok(self.take(1, field)?[0])
    }

    fn u16(&mut self, field: &'static str) -> Result<u16, CanonicalStoreError> {
        Ok(u16::from_le_bytes(self.array(field)?))
    }

    fn u32(&mut self, field: &'static str) -> Result<u32, CanonicalStoreError> {
        Ok(u32::from_le_bytes(self.array(field)?))
    }

    fn u64(&mut self, field: &'static str) -> Result<u64, CanonicalStoreError> {
        Ok(u64::from_le_bytes(self.array(field)?))
    }

    fn finish(&self) -> Result<(), CanonicalStoreError> {
        if self.at == self.encoded.len() {
            Ok(())
        } else {
            Err(CanonicalStoreError::admission(
                self.path,
                "activation schedule record has trailing bytes",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use rust_rocksdb::{Options, WriteBatch};
    use tempfile::TempDir;
    use zinder_core::{BlockHash, BlockId};

    use super::*;

    fn activations(
        future_branch: u32,
        future_height: u32,
    ) -> Result<NetworkUpgradeActivations, zinder_core::NetworkUpgradeActivationsError> {
        NetworkUpgradeActivations::new(
            Network::ZcashRegtest,
            vec![
                NetworkUpgradeActivation {
                    branch_id: ConsensusBranchId::new(1),
                    activation_height: BlockHeight::new(1),
                    name: "Sapling".to_owned(),
                },
                NetworkUpgradeActivation {
                    branch_id: ConsensusBranchId::new(future_branch),
                    activation_height: BlockHeight::new(future_height),
                    name: "Future".to_owned(),
                },
            ],
        )
    }

    fn tip(height: u32) -> BlockId {
        BlockId::new(BlockHeight::new(height), BlockHash::from_bytes([8; 32]))
    }

    #[test]
    fn successor_rejects_revision_above_admission_bound_before_persistence()
    -> Result<(), Box<dyn std::error::Error>> {
        let baseline = NetworkUpgradeActivations::new(
            Network::ZcashRegtest,
            vec![NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(1),
                activation_height: BlockHeight::new(1),
                name: "Sapling".to_owned(),
            }],
        )?
        .fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1)
        .as_bytes();
        let last_admitted = CanonicalActivationSchedule::new(
            MAX_SCHEDULE_REVISIONS,
            baseline,
            1,
            Some([7; 32]),
            tip(3),
            activations(2, 10)?,
        )?;

        let result =
            CanonicalActivationSchedule::successor(&last_admitted, tip(4), activations(3, 12)?);

        assert!(result.is_err());
        assert_eq!(last_admitted.revision(), MAX_SCHEDULE_REVISIONS);
        Ok(())
    }

    #[test]
    fn revision_journal_is_durable_and_reopens_as_one_future_only_schedule()
    -> Result<(), Box<dyn std::error::Error>> {
        let legacy = NetworkUpgradeActivations::new(
            Network::ZcashRegtest,
            vec![NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(1),
                activation_height: BlockHeight::new(1),
                name: "Sapling".to_owned(),
            }],
        )?;
        let baseline = legacy
            .fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1)
            .as_bytes();
        let first = CanonicalActivationSchedule::initial(activations(2, 10)?, baseline, 1, tip(3))?;
        let second = CanonicalActivationSchedule::successor(&first, tip(5), activations(3, 12)?)?;

        let temporary = TempDir::new()?;
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = DB::open(&options, temporary.path())?;
        let mut batch = WriteBatch::default();
        first.put_into(&mut batch)?;
        second.put_into(&mut batch)?;
        db.write(&batch)?;

        let reopened = CanonicalActivationSchedule::read_bound(
            &db,
            temporary.path(),
            second.binding(),
            baseline,
        )?;
        assert_eq!(reopened, second);
        assert_eq!(reopened.revision(), 2);
        assert_eq!(reopened.activations(), &activations(3, 12)?);
        Ok(())
    }

    #[test]
    fn schedule_decoder_rejects_truncated_and_noncanonical_records()
    -> Result<(), Box<dyn std::error::Error>> {
        let baseline = NetworkUpgradeActivations::new(
            Network::ZcashRegtest,
            vec![NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(1),
                activation_height: BlockHeight::new(1),
                name: "Sapling".to_owned(),
            }],
        )?
        .fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1)
        .as_bytes();
        let schedule =
            CanonicalActivationSchedule::initial(activations(2, 10)?, baseline, 1, tip(3))?;
        let encoded = schedule.encode()?;
        assert!(CanonicalActivationSchedule::decode(Path::new("schedule"), &encoded[..5]).is_err());
        let mut trailing = encoded;
        trailing.push(0);
        assert!(CanonicalActivationSchedule::decode(Path::new("schedule"), &trailing).is_err());
        Ok(())
    }
}
