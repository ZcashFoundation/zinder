use std::{
    num::NonZeroU32,
    time::{Duration, Instant},
};

use rust_rocksdb::{DB, IteratorMode, ReadOptions, WriteBatch};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zinder_core::{
    BlockHash, BlockHeight, ChainTipMetadata, ShieldedProtocol, SubtreeRootArtifact,
    SubtreeRootHash, SubtreeRootIndex, SubtreeRootRange,
};

use super::{
    CanonicalBlockLoadEvidence, CanonicalStoreBuildPlan, CanonicalStoreError,
    block_load::{BLOCK_HEADER_VALUE_LEN, encode_block_position},
    construction_manifest::CanonicalConstructionFamilyEvidence,
    rocksdb::{BLOCK_HEADER_COLUMN_FAMILY, SUBTREE_ROOT_COLUMN_FAMILY},
};

const SUBTREE_ROOT_KEY_LEN: usize = 1 + 4;
const SUBTREE_ROOT_VALUE_LEN: usize = 32 + 4 + 32;
const SUBTREE_ROOT_SEQUENCE_DIGEST_VERSION: u16 = 2;
const SUBTREE_ROOT_SEQUENCE_DIGEST_DOMAIN: &[u8] = b"zinder.canonical.subtree-root-sequence.v2\0";
type EncodedSubtreeRoot = ([u8; SUBTREE_ROOT_KEY_LEN], [u8; SUBTREE_ROOT_VALUE_LEN]);

/// Source-authenticated subtree-root fields before canonical block identity is attached.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalBuildSubtreeRoot {
    /// Shielded protocol containing the completed subtree.
    pub protocol: ShieldedProtocol,
    /// Exact completed subtree index.
    pub subtree_index: SubtreeRootIndex,
    /// Root returned by the canonical node source.
    pub root_hash: SubtreeRootHash,
    /// Retained canonical block that completed the subtree.
    pub completing_block_height: BlockHeight,
}

/// Coverage represented by one canonical subtree-root construction load.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CanonicalSubtreeRootLoadCoverage {
    /// Roots completed by blocks retained in this canonical store.
    RetainedRange,
    /// Every completed root from index zero through the fixed build tip.
    CompletePrefix,
}

impl CanonicalSubtreeRootLoadCoverage {
    const fn digest_tag(self) -> &'static [u8] {
        match self {
            Self::RetainedRange => b"retained-range",
            Self::CompletePrefix => b"complete-prefix",
        }
    }
}

/// Measurements from one complete canonical subtree-root load.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalSubtreeRootLoadEvidence {
    /// Whether the load covers retained history or the complete index-zero prefix.
    pub coverage: CanonicalSubtreeRootLoadCoverage,
    /// Version of the domain-separated subtree-root sequence digest.
    pub sequence_digest_version: u16,
    /// Number of contiguous source-authenticated subtree roots.
    pub subtree_root_count: u64,
    /// Total key and value bytes written to the subtree-root family.
    pub subtree_root_logical_bytes: u64,
    /// Version-2 SHA-256 digest of the coverage and every persisted key/value in order.
    pub subtree_root_sequence_digest: [u8; 32],
}

pub(super) struct LoadedCanonicalSubtreeRoots {
    pub(super) evidence: CanonicalSubtreeRootLoadEvidence,
    pub(super) family_evidence: CanonicalConstructionFamilyEvidence,
}

pub(super) struct PersistedSubtreeRootValidation {
    pub(super) evidence: CanonicalSubtreeRootLoadEvidence,
    pub(super) family_evidence: CanonicalConstructionFamilyEvidence,
}

impl PersistedSubtreeRootValidation {
    fn complete(
        evidence: CanonicalSubtreeRootLoadEvidence,
        family_evidence: CanonicalConstructionFamilyEvidence,
        elapsed: Duration,
    ) -> Self {
        record_subtree_cold_family_scan(&family_evidence, elapsed);
        Self {
            evidence,
            family_evidence,
        }
    }
}

pub(super) fn required_subtree_root_ranges(
    predecessor_tip_metadata: ChainTipMetadata,
    build_tip_metadata: ChainTipMetadata,
) -> Result<Vec<SubtreeRootRange>, CanonicalStoreError> {
    let mut ranges = Vec::with_capacity(3);
    for protocol in shielded_protocols() {
        let start_index = predecessor_tip_metadata.completed_subtree_count(protocol);
        let completed_at_tip = build_tip_metadata.completed_subtree_count(protocol);
        let root_count = completed_at_tip.checked_sub(start_index).ok_or_else(|| {
            CanonicalStoreError::subtree_root_sequence(format!(
                "{protocol:?} completed-subtree count regressed from {start_index} to {completed_at_tip}"
            ))
        })?;
        if let Some(max_entries) = NonZeroU32::new(root_count) {
            ranges.push(SubtreeRootRange::new(
                protocol,
                SubtreeRootIndex::new(start_index),
                max_entries,
            ));
        }
    }
    Ok(ranges)
}

pub(super) fn load_subtree_roots(
    db: &DB,
    build_plan: &CanonicalStoreBuildPlan,
    block_evidence: &CanonicalBlockLoadEvidence,
    subtree_roots: impl IntoIterator<Item = CanonicalBuildSubtreeRoot>,
) -> Result<LoadedCanonicalSubtreeRoots, CanonicalStoreError> {
    let family = subtree_root_family(db)?;
    if let Some(first_row) = db.iterator_cf(&family, IteratorMode::Start).next() {
        first_row.map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "inspect subtree-root family before load",
            source,
        })?;
        return Err(CanonicalStoreError::SubtreeRootLoadAlreadyLoaded);
    }
    let required_ranges = required_subtree_root_ranges(
        build_plan.history_predecessor().tip_metadata(),
        block_evidence.tip_metadata,
    )?;
    let mut stager = SubtreeRootStager::new(db, &family, build_plan, subtree_roots.into_iter());
    for range in required_ranges {
        stager.stage_range(range)?;
    }
    let (mut source_rows, expected_rows, batch) = stager.finish();
    if let Some(extra) = source_rows.next() {
        return Err(CanonicalStoreError::subtree_root_sequence(format!(
            "unexpected {:?} subtree root at index {}",
            extra.protocol,
            extra.subtree_index.value()
        )));
    }
    if !expected_rows.is_empty() {
        db.write(&batch)
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "write subtree roots",
                source,
            })?;
    }
    complete_subtree_root_load(
        &expected_rows,
        CanonicalSubtreeRootLoadCoverage::RetainedRange,
    )
}

pub(super) fn load_complete_subtree_root_prefix(
    db: &DB,
    build_plan: &CanonicalStoreBuildPlan,
    block_evidence: &CanonicalBlockLoadEvidence,
    subtree_roots: impl IntoIterator<Item = SubtreeRootArtifact>,
) -> Result<LoadedCanonicalSubtreeRoots, CanonicalStoreError> {
    let family = subtree_root_family(db)?;
    if let Some(first_row) = db.iterator_cf(&family, IteratorMode::Start).next() {
        first_row.map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "inspect subtree-root family before complete-prefix load",
            source,
        })?;
        return Err(CanonicalStoreError::SubtreeRootLoadAlreadyLoaded);
    }
    let required_ranges =
        required_subtree_root_ranges(ChainTipMetadata::empty(), block_evidence.tip_metadata)?;
    let mut stager =
        CompleteSubtreeRootPrefixStager::new(db, &family, build_plan, subtree_roots.into_iter());
    for range in required_ranges {
        stager.stage_range(range)?;
    }
    let (mut source_rows, expected_rows, batch) = stager.finish();
    if let Some(extra) = source_rows.next() {
        return Err(CanonicalStoreError::subtree_root_sequence(format!(
            "unexpected {:?} complete-prefix subtree root at index {}",
            extra.protocol,
            extra.subtree_index.value()
        )));
    }
    if !expected_rows.is_empty() {
        db.write(&batch)
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "write complete subtree-root prefix",
                source,
            })?;
    }
    complete_subtree_root_load(
        &expected_rows,
        CanonicalSubtreeRootLoadCoverage::CompletePrefix,
    )
}

fn complete_subtree_root_load(
    expected_rows: &[EncodedSubtreeRoot],
    coverage: CanonicalSubtreeRootLoadCoverage,
) -> Result<LoadedCanonicalSubtreeRoots, CanonicalStoreError> {
    let mut family_evidence =
        CanonicalConstructionFamilyEvidence::accumulator(SUBTREE_ROOT_COLUMN_FAMILY);
    for (key, encoded_root) in expected_rows {
        family_evidence.observe(key, encoded_root)?;
    }
    let subtree_root_count = u64::try_from(expected_rows.len()).map_err(|_| {
        CanonicalStoreError::subtree_root_sequence("subtree-root count exceeds u64::MAX")
    })?;
    let subtree_root_row_bytes = u64::try_from(SUBTREE_ROOT_KEY_LEN + SUBTREE_ROOT_VALUE_LEN)
        .map_err(|_| {
            CanonicalStoreError::subtree_root_sequence("subtree-root row length exceeds u64::MAX")
        })?;
    let subtree_root_logical_bytes = subtree_root_count
        .checked_mul(subtree_root_row_bytes)
        .ok_or_else(|| {
            CanonicalStoreError::subtree_root_sequence("subtree-root logical bytes exceed u64::MAX")
        })?;
    let subtree_root_sequence_digest = digest_subtree_root_rows(expected_rows, coverage);
    Ok(LoadedCanonicalSubtreeRoots {
        evidence: CanonicalSubtreeRootLoadEvidence {
            coverage,
            sequence_digest_version: SUBTREE_ROOT_SEQUENCE_DIGEST_VERSION,
            subtree_root_count,
            subtree_root_logical_bytes,
            subtree_root_sequence_digest,
        },
        family_evidence: family_evidence.finish(),
    })
}

pub(super) fn validate_persisted_subtree_root_family(
    db: &DB,
    build_plan: &CanonicalStoreBuildPlan,
    block_evidence: &CanonicalBlockLoadEvidence,
    expected_evidence: CanonicalSubtreeRootLoadEvidence,
) -> Result<PersistedSubtreeRootValidation, CanonicalStoreError> {
    let started_at = Instant::now();
    let family = subtree_root_family(db)?;
    validate_subtree_root_digest_version(expected_evidence.sequence_digest_version)?;
    let required_ranges =
        persisted_subtree_root_ranges(build_plan, block_evidence, expected_evidence.coverage)?;
    let mut iteration_options = ReadOptions::default();
    iteration_options.fill_cache(false);
    iteration_options.set_readahead_size(2 * 1024 * 1024);
    let mut persisted_rows = db.raw_iterator_cf_opt(&family, iteration_options);
    persisted_rows.seek_to_first();
    let mut header_read_options = ReadOptions::default();
    header_read_options.fill_cache(false);
    let mut digest = subtree_root_sequence_digest_builder(expected_evidence.coverage);
    let mut subtree_root_count = 0_u64;
    let mut subtree_root_logical_bytes = 0_u64;
    let mut family_evidence =
        CanonicalConstructionFamilyEvidence::accumulator(SUBTREE_ROOT_COLUMN_FAMILY);
    for range in required_ranges {
        let mut previous_completion_height = None;
        for expected_index in range {
            let (key, encoded_root) = persisted_rows
                .item()
                .ok_or(CanonicalStoreError::SubtreeRootReadbackMismatch)?;
            let artifact = decode_subtree_root(key, encoded_root)?;
            if artifact.protocol != range.protocol
                || artifact.subtree_index != expected_index
                || previous_completion_height
                    .is_some_and(|previous| artifact.completing_block_height < previous)
                || !persisted_completion_block_matches(
                    db,
                    build_plan,
                    expected_evidence.coverage,
                    &header_read_options,
                    &artifact,
                )?
            {
                return Err(CanonicalStoreError::SubtreeRootReadbackMismatch);
            }
            digest.update(key.as_ref());
            digest.update(encoded_root.as_ref());
            family_evidence.observe(key, encoded_root)?;
            subtree_root_count = subtree_root_count.checked_add(1).ok_or_else(|| {
                CanonicalStoreError::subtree_root_sequence("subtree-root count exceeds u64::MAX")
            })?;
            let row_bytes = subtree_root_logical_row_bytes(key, encoded_root)?;
            subtree_root_logical_bytes = subtree_root_logical_bytes
                .checked_add(row_bytes)
                .ok_or_else(|| {
                    CanonicalStoreError::subtree_root_sequence(
                        "subtree-root logical bytes exceed u64::MAX",
                    )
                })?;
            previous_completion_height = Some(artifact.completing_block_height);
            persisted_rows.next();
        }
    }
    if persisted_rows.valid() {
        return Err(CanonicalStoreError::SubtreeRootReadbackMismatch);
    }
    persisted_rows
        .status()
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "subtree-root publication readback",
            source,
        })?;
    let evidence = CanonicalSubtreeRootLoadEvidence {
        coverage: expected_evidence.coverage,
        sequence_digest_version: SUBTREE_ROOT_SEQUENCE_DIGEST_VERSION,
        subtree_root_count,
        subtree_root_logical_bytes,
        subtree_root_sequence_digest: digest.finalize().into(),
    };
    Ok(PersistedSubtreeRootValidation::complete(
        evidence,
        family_evidence.finish(),
        started_at.elapsed(),
    ))
}

fn validate_subtree_root_digest_version(
    sequence_digest_version: u16,
) -> Result<(), CanonicalStoreError> {
    if sequence_digest_version != SUBTREE_ROOT_SEQUENCE_DIGEST_VERSION {
        return Err(CanonicalStoreError::subtree_root_sequence(format!(
            "subtree-root evidence digest version {sequence_digest_version} is not supported"
        )));
    }
    Ok(())
}

fn persisted_subtree_root_ranges(
    build_plan: &CanonicalStoreBuildPlan,
    block_evidence: &CanonicalBlockLoadEvidence,
    coverage: CanonicalSubtreeRootLoadCoverage,
) -> Result<Vec<SubtreeRootRange>, CanonicalStoreError> {
    let predecessor_metadata = match coverage {
        CanonicalSubtreeRootLoadCoverage::RetainedRange => {
            build_plan.history_predecessor().tip_metadata()
        }
        CanonicalSubtreeRootLoadCoverage::CompletePrefix => ChainTipMetadata::empty(),
    };
    required_subtree_root_ranges(predecessor_metadata, block_evidence.tip_metadata)
}

fn persisted_completion_block_matches(
    db: &DB,
    build_plan: &CanonicalStoreBuildPlan,
    coverage: CanonicalSubtreeRootLoadCoverage,
    read_options: &ReadOptions,
    artifact: &SubtreeRootArtifact,
) -> Result<bool, CanonicalStoreError> {
    if artifact.completing_block_height > build_plan.build_tip().height {
        return Ok(false);
    }
    let first_retained_height = build_plan.history_bounds().first_available_height();
    if artifact.completing_block_height < first_retained_height {
        let predecessor = build_plan.history_predecessor().block_id;
        return Ok(coverage == CanonicalSubtreeRootLoadCoverage::CompletePrefix
            && (artifact.completing_block_height < predecessor.height
                || (artifact.completing_block_height == predecessor.height
                    && artifact.completing_block_hash == predecessor.hash)));
    }
    Ok(
        retained_block_hash_with_options(db, artifact.completing_block_height, read_options)?
            == artifact.completing_block_hash,
    )
}

fn subtree_root_logical_row_bytes(
    key: &[u8],
    encoded_root: &[u8],
) -> Result<u64, CanonicalStoreError> {
    u64::try_from(key.len() + encoded_root.len()).map_err(|_| {
        CanonicalStoreError::subtree_root_sequence("subtree-root row length exceeds u64::MAX")
    })
}

fn record_subtree_cold_family_scan(
    evidence: &CanonicalConstructionFamilyEvidence,
    elapsed: Duration,
) {
    metrics::histogram!(
        "zinder_store_canonical_publication_family_scan_duration_seconds",
        "family" => evidence.family
    )
    .record(elapsed);
    metrics::counter!(
        "zinder_store_canonical_publication_family_scan_rows_total",
        "family" => evidence.family
    )
    .increment(evidence.row_count);
    metrics::counter!(
        "zinder_store_canonical_publication_family_scan_logical_bytes_total",
        "family" => evidence.family
    )
    .increment(evidence.logical_bytes);
}

fn digest_subtree_root_rows(
    rows: &[EncodedSubtreeRoot],
    coverage: CanonicalSubtreeRootLoadCoverage,
) -> [u8; 32] {
    let mut digest = subtree_root_sequence_digest_builder(coverage);
    for (key, encoded_root) in rows {
        digest.update(key);
        digest.update(encoded_root);
    }
    digest.finalize().into()
}

fn subtree_root_sequence_digest_builder(coverage: CanonicalSubtreeRootLoadCoverage) -> Sha256 {
    let mut digest = Sha256::new();
    digest.update(SUBTREE_ROOT_SEQUENCE_DIGEST_DOMAIN);
    digest.update(coverage.digest_tag());
    digest.update(b"\0");
    digest
}

struct SubtreeRootStager<'db, SourceRows> {
    db: &'db DB,
    family: &'db std::sync::Arc<rust_rocksdb::BoundColumnFamily<'db>>,
    build_plan: &'db CanonicalStoreBuildPlan,
    source_rows: SourceRows,
    expected_rows: Vec<EncodedSubtreeRoot>,
    batch: WriteBatch,
}

impl<'db, SourceRows> SubtreeRootStager<'db, SourceRows>
where
    SourceRows: Iterator<Item = CanonicalBuildSubtreeRoot>,
{
    fn new(
        db: &'db DB,
        family: &'db std::sync::Arc<rust_rocksdb::BoundColumnFamily<'db>>,
        build_plan: &'db CanonicalStoreBuildPlan,
        source_rows: SourceRows,
    ) -> Self {
        Self {
            db,
            family,
            build_plan,
            source_rows,
            expected_rows: Vec::new(),
            batch: WriteBatch::default(),
        }
    }

    fn stage_range(&mut self, range: SubtreeRootRange) -> Result<(), CanonicalStoreError> {
        let mut previous_completion_height = None;
        for expected_index in range {
            let source_root = self.source_rows.next().ok_or_else(|| {
                CanonicalStoreError::subtree_root_sequence(format!(
                    "missing {:?} subtree root at index {}",
                    range.protocol,
                    expected_index.value()
                ))
            })?;
            validate_source_subtree_root(
                self.build_plan,
                range.protocol,
                expected_index,
                previous_completion_height,
                source_root,
            )?;
            let completing_block_hash =
                retained_block_hash(self.db, source_root.completing_block_height)?;
            let artifact = SubtreeRootArtifact::new(
                source_root.protocol,
                source_root.subtree_index,
                source_root.root_hash,
                source_root.completing_block_height,
                completing_block_hash,
            );
            let encoded_key = encode_subtree_root_key(&artifact);
            let encoded_root = encode_subtree_root_value(&artifact);
            self.batch.put_cf(self.family, encoded_key, encoded_root);
            self.expected_rows.push((encoded_key, encoded_root));
            previous_completion_height = Some(source_root.completing_block_height);
        }
        Ok(())
    }

    fn finish(self) -> (SourceRows, Vec<EncodedSubtreeRoot>, WriteBatch) {
        (self.source_rows, self.expected_rows, self.batch)
    }
}

struct CompleteSubtreeRootPrefixStager<'db, SourceRows> {
    db: &'db DB,
    family: &'db std::sync::Arc<rust_rocksdb::BoundColumnFamily<'db>>,
    build_plan: &'db CanonicalStoreBuildPlan,
    source_rows: SourceRows,
    expected_rows: Vec<EncodedSubtreeRoot>,
    batch: WriteBatch,
}

impl<'db, SourceRows> CompleteSubtreeRootPrefixStager<'db, SourceRows>
where
    SourceRows: Iterator<Item = SubtreeRootArtifact>,
{
    fn new(
        db: &'db DB,
        family: &'db std::sync::Arc<rust_rocksdb::BoundColumnFamily<'db>>,
        build_plan: &'db CanonicalStoreBuildPlan,
        source_rows: SourceRows,
    ) -> Self {
        Self {
            db,
            family,
            build_plan,
            source_rows,
            expected_rows: Vec::new(),
            batch: WriteBatch::default(),
        }
    }

    fn stage_range(&mut self, range: SubtreeRootRange) -> Result<(), CanonicalStoreError> {
        let mut previous_completion_height = None;
        for expected_index in range {
            let subtree_root = self.source_rows.next().ok_or_else(|| {
                CanonicalStoreError::subtree_root_sequence(format!(
                    "missing {:?} complete-prefix subtree root at index {}",
                    range.protocol,
                    expected_index.value()
                ))
            })?;
            validate_complete_prefix_subtree_root(
                self.db,
                self.build_plan,
                ExpectedCompletePrefixSubtreeRoot {
                    protocol: range.protocol,
                    index: expected_index,
                    previous_completion_height,
                },
                &subtree_root,
            )?;
            let encoded_key = encode_subtree_root_key(&subtree_root);
            let encoded_root = encode_subtree_root_value(&subtree_root);
            self.batch.put_cf(self.family, encoded_key, encoded_root);
            self.expected_rows.push((encoded_key, encoded_root));
            previous_completion_height = Some(subtree_root.completing_block_height);
        }
        Ok(())
    }

    fn finish(self) -> (SourceRows, Vec<EncodedSubtreeRoot>, WriteBatch) {
        (self.source_rows, self.expected_rows, self.batch)
    }
}

#[derive(Clone, Copy)]
struct ExpectedCompletePrefixSubtreeRoot {
    protocol: ShieldedProtocol,
    index: SubtreeRootIndex,
    previous_completion_height: Option<BlockHeight>,
}

fn validate_complete_prefix_subtree_root(
    db: &DB,
    build_plan: &CanonicalStoreBuildPlan,
    expected: ExpectedCompletePrefixSubtreeRoot,
    subtree_root: &SubtreeRootArtifact,
) -> Result<(), CanonicalStoreError> {
    if (subtree_root.protocol, subtree_root.subtree_index) != (expected.protocol, expected.index) {
        return Err(CanonicalStoreError::subtree_root_sequence(format!(
            "expected {:?} complete-prefix subtree index {}, observed {:?} index {}",
            expected.protocol,
            expected.index.value(),
            subtree_root.protocol,
            subtree_root.subtree_index.value()
        )));
    }
    if subtree_root.completing_block_height > build_plan.build_tip().height {
        return Err(CanonicalStoreError::subtree_root_sequence(format!(
            "{:?} complete-prefix subtree index {} completes after the build tip at height {}",
            expected.protocol,
            expected.index.value(),
            subtree_root.completing_block_height.value()
        )));
    }
    if expected
        .previous_completion_height
        .is_some_and(|previous| subtree_root.completing_block_height < previous)
    {
        return Err(CanonicalStoreError::subtree_root_sequence(format!(
            "{:?} complete-prefix subtree completion heights are not ascending",
            expected.protocol
        )));
    }
    let first_retained_height = build_plan.history_bounds().first_available_height();
    if subtree_root.completing_block_height < first_retained_height {
        let predecessor = build_plan.history_predecessor().block_id;
        if subtree_root.completing_block_height > predecessor.height {
            return Err(CanonicalStoreError::subtree_root_sequence(format!(
                "{:?} complete-prefix subtree index {} completes between the authenticated predecessor and retained history",
                expected.protocol,
                expected.index.value()
            )));
        }
        if subtree_root.completing_block_height == predecessor.height
            && subtree_root.completing_block_hash != predecessor.hash
        {
            return Err(CanonicalStoreError::subtree_root_sequence(format!(
                "{:?} complete-prefix subtree index {} completing block differs from the authenticated predecessor",
                expected.protocol,
                expected.index.value()
            )));
        }
        return Ok(());
    }
    let retained_hash = retained_block_hash(db, subtree_root.completing_block_height)?;
    if retained_hash != subtree_root.completing_block_hash {
        return Err(CanonicalStoreError::subtree_root_sequence(format!(
            "{:?} complete-prefix subtree index {} completing block differs from retained canonical history",
            expected.protocol,
            expected.index.value()
        )));
    }
    Ok(())
}

fn validate_source_subtree_root(
    build_plan: &CanonicalStoreBuildPlan,
    expected_protocol: ShieldedProtocol,
    expected_index: SubtreeRootIndex,
    previous_completion_height: Option<BlockHeight>,
    source_root: CanonicalBuildSubtreeRoot,
) -> Result<(), CanonicalStoreError> {
    if source_root.protocol != expected_protocol || source_root.subtree_index != expected_index {
        return Err(CanonicalStoreError::subtree_root_sequence(format!(
            "expected {expected_protocol:?} subtree index {}, observed {:?} index {}",
            expected_index.value(),
            source_root.protocol,
            source_root.subtree_index.value()
        )));
    }
    let first_height = build_plan.history_bounds().first_available_height();
    if source_root.completing_block_height < first_height
        || source_root.completing_block_height > build_plan.build_tip().height
    {
        return Err(CanonicalStoreError::subtree_root_sequence(format!(
            "{expected_protocol:?} subtree index {} completed outside retained history at height {}",
            expected_index.value(),
            source_root.completing_block_height.value()
        )));
    }
    if previous_completion_height
        .is_some_and(|previous| source_root.completing_block_height < previous)
    {
        return Err(CanonicalStoreError::subtree_root_sequence(format!(
            "{expected_protocol:?} subtree completion heights are not ascending"
        )));
    }
    Ok(())
}

fn subtree_root_family(
    db: &DB,
) -> Result<std::sync::Arc<rust_rocksdb::BoundColumnFamily<'_>>, CanonicalStoreError> {
    db.cf_handle(SUBTREE_ROOT_COLUMN_FAMILY).ok_or_else(|| {
        CanonicalStoreError::subtree_root_sequence("subtree_root column family is absent")
    })
}

pub(super) fn retained_block_hash(
    db: &DB,
    height: BlockHeight,
) -> Result<BlockHash, CanonicalStoreError> {
    retained_block_hash_with_options(db, height, &ReadOptions::default())
}

fn retained_block_hash_with_options(
    db: &DB,
    height: BlockHeight,
    read_options: &ReadOptions,
) -> Result<BlockHash, CanonicalStoreError> {
    let family = db.cf_handle(BLOCK_HEADER_COLUMN_FAMILY).ok_or_else(|| {
        CanonicalStoreError::subtree_root_sequence("block_header column family is absent")
    })?;
    let encoded_header = db
        .get_cf_opt(&family, encode_block_position(height), read_options)
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "read subtree completing block header",
            source,
        })?
        .ok_or_else(|| {
            CanonicalStoreError::subtree_root_sequence(format!(
                "subtree completion header at height {} is absent",
                height.value()
            ))
        })?;
    if encoded_header.len() != BLOCK_HEADER_VALUE_LEN {
        return Err(CanonicalStoreError::subtree_root_sequence(format!(
            "subtree completion header at height {} has {} bytes; expected {BLOCK_HEADER_VALUE_LEN}",
            height.value(),
            encoded_header.len()
        )));
    }
    let mut block_hash = [0; 32];
    block_hash.copy_from_slice(&encoded_header[..32]);
    Ok(BlockHash::from_bytes(block_hash))
}

pub(super) fn encode_subtree_root_key(
    artifact: &SubtreeRootArtifact,
) -> [u8; SUBTREE_ROOT_KEY_LEN] {
    let mut key = [0; SUBTREE_ROOT_KEY_LEN];
    key[0] = artifact.protocol.id();
    key[1..].copy_from_slice(&artifact.subtree_index.value().to_be_bytes());
    key
}

pub(super) fn encode_subtree_root_value(
    artifact: &SubtreeRootArtifact,
) -> [u8; SUBTREE_ROOT_VALUE_LEN] {
    let mut encoded_root = [0; SUBTREE_ROOT_VALUE_LEN];
    encoded_root[..32].copy_from_slice(&artifact.root_hash.as_bytes());
    encoded_root[32..36].copy_from_slice(&artifact.completing_block_height.value().to_be_bytes());
    encoded_root[36..].copy_from_slice(&artifact.completing_block_hash.as_bytes());
    encoded_root
}

pub(in crate::canonical_store) fn decode_subtree_root(
    key: &[u8],
    encoded_root: &[u8],
) -> Result<SubtreeRootArtifact, CanonicalStoreError> {
    if key.len() != SUBTREE_ROOT_KEY_LEN || encoded_root.len() != SUBTREE_ROOT_VALUE_LEN {
        return Err(CanonicalStoreError::subtree_root_sequence(
            "subtree-root key or value has a non-v1 length",
        ));
    }
    let protocol = ShieldedProtocol::from_id(key[0]).ok_or_else(|| {
        CanonicalStoreError::subtree_root_sequence("subtree-root key has an unknown protocol")
    })?;
    let mut index = [0; 4];
    index.copy_from_slice(&key[1..]);
    let mut root_hash = [0; 32];
    root_hash.copy_from_slice(&encoded_root[..32]);
    let mut completing_height = [0; 4];
    completing_height.copy_from_slice(&encoded_root[32..36]);
    let mut completing_hash = [0; 32];
    completing_hash.copy_from_slice(&encoded_root[36..]);
    Ok(SubtreeRootArtifact::new(
        protocol,
        SubtreeRootIndex::new(u32::from_be_bytes(index)),
        SubtreeRootHash::from_bytes(root_hash),
        BlockHeight::new(u32::from_be_bytes(completing_height)),
        BlockHash::from_bytes(completing_hash),
    ))
}

const fn shielded_protocols() -> [ShieldedProtocol; 3] {
    [
        ShieldedProtocol::Sapling,
        ShieldedProtocol::Orchard,
        ShieldedProtocol::Ironwood,
    ]
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU32, path::Path};

    use incrementalmerkletree::{
        Position,
        frontier::{CommitmentTree, Frontier},
    };
    use rust_rocksdb::IteratorMode;
    use sapling::Node as SaplingNode;
    use tempfile::tempdir;
    use zcash_primitives::merkle_tree::write_commitment_tree;
    use zinder_core::{
        BlockHash, BlockHeaderArtifact, BlockHeight, BlockId, CanonicalBlockFacts,
        CanonicalBlockFactsDigestVersion, CanonicalBlockReplayFormatVersion, ChainTipMetadata,
        CommitmentTreeCheckpoint, CommitmentTreeFrontier, CommitmentTreeFrontiers,
        CompactBlockArtifact, CompactChainMetadata, FinalNoteCommitmentRoot, Network,
        SUBTREE_LEAF_COUNT, SerializedBytesDigest, ShieldedProtocol, SubtreeRootArtifact,
        SubtreeRootHash, SubtreeRootIndex, SubtreeRootRange, UnixTimestampMillis,
        encode_canonical_block_replay,
    };

    use super::{
        CanonicalSubtreeRootLoadCoverage, decode_subtree_root, encode_subtree_root_key,
        encode_subtree_root_value, required_subtree_root_ranges,
    };
    use crate::{
        CanonicalBaselinePublication, CanonicalBuildBlock, CanonicalReorgPolicy,
        CanonicalStoreBuildPlan, CanonicalStoreError, CanonicalStoreWorkload, RawBlobRetention,
        RocksDbCanonicalBuilder, RocksDbCanonicalStore, RocksDbResourceBudget,
    };

    const PREDECESSOR: BlockId =
        BlockId::new(BlockHeight::new(99), BlockHash::from_bytes([99; 32]));
    const FIRST_RETAINED: BlockId =
        BlockId::new(BlockHeight::new(100), BlockHash::from_bytes([100; 32]));
    const BUILD_TIP: BlockId =
        BlockId::new(BlockHeight::new(101), BlockHash::from_bytes([101; 32]));

    #[test]
    fn checkpointed_ranges_begin_after_predecessor_completed_subtrees()
    -> Result<(), Box<dyn std::error::Error>> {
        let predecessor = ChainTipMetadata::new(SUBTREE_LEAF_COUNT * 2 + 1, SUBTREE_LEAF_COUNT, 0);
        let tip = ChainTipMetadata::new(SUBTREE_LEAF_COUNT * 5, SUBTREE_LEAF_COUNT * 3 + 9, 0);

        let ranges = required_subtree_root_ranges(predecessor, tip)?;

        assert_eq!(
            ranges,
            vec![
                SubtreeRootRange::new(
                    ShieldedProtocol::Sapling,
                    SubtreeRootIndex::new(2),
                    NonZeroU32::new(3).ok_or("Sapling range must be nonzero")?,
                ),
                SubtreeRootRange::new(
                    ShieldedProtocol::Orchard,
                    SubtreeRootIndex::new(1),
                    NonZeroU32::new(2).ok_or("Orchard range must be nonzero")?,
                ),
            ]
        );
        Ok(())
    }

    #[test]
    fn required_ranges_reject_tree_size_regression() {
        let outcome = required_subtree_root_ranges(
            ChainTipMetadata::new(SUBTREE_LEAF_COUNT, 0, 0),
            ChainTipMetadata::empty(),
        );
        assert!(outcome.is_err());
    }

    #[test]
    fn subtree_root_v1_has_exact_known_bytes_and_round_trips()
    -> Result<(), Box<dyn std::error::Error>> {
        let artifact = SubtreeRootArtifact::new(
            ShieldedProtocol::Orchard,
            SubtreeRootIndex::new(0x0102_0304),
            SubtreeRootHash::from_bytes([5; 32]),
            BlockHeight::new(0x1112_1314),
            BlockHash::from_bytes([6; 32]),
        );
        let key = encode_subtree_root_key(&artifact);
        let value = encode_subtree_root_value(&artifact);
        assert_eq!(key, [2, 1, 2, 3, 4]);
        assert_eq!(&value[..32], &[5; 32]);
        assert_eq!(&value[32..36], &[0x11, 0x12, 0x13, 0x14]);
        assert_eq!(&value[36..], &[6; 32]);
        assert_eq!(decode_subtree_root(&key, &value)?, artifact);
        Ok(())
    }

    #[test]
    fn complete_prefix_loader_rejects_invalid_input_atomically_and_accepts_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempdir()?;
        let mut builder =
            loaded_builder_with_complete_prefix_requirements(temporary.path().join("canonical"))?;
        let valid_roots = valid_complete_prefix_roots();
        let invalid_cases = invalid_complete_prefix_cases(&valid_roots);

        for (case_name, invalid_roots, expected_error) in invalid_cases {
            let error = builder
                .load_complete_subtree_root_prefix(invalid_roots)
                .err()
                .ok_or_else(|| format!("{case_name} must be rejected"))?;
            assert!(
                error.to_string().contains(expected_error),
                "{case_name} returned the wrong error: {error}"
            );
            assert_eq!(
                persisted_subtree_root_count(&builder)?,
                0,
                "{case_name} wrote rows before rejection"
            );
        }

        let evidence = builder.load_complete_subtree_root_prefix(valid_roots)?;
        assert_eq!(
            evidence.coverage,
            CanonicalSubtreeRootLoadCoverage::CompletePrefix
        );
        assert_eq!(evidence.subtree_root_count, 3);
        assert_eq!(persisted_subtree_root_count(&builder)?, 3);
        Ok(())
    }

    #[test]
    fn cold_certification_rejects_tampered_complete_prefix_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempdir()?;
        let tip_frontiers = completed_sapling_tip_frontiers()?;
        cold_certifiable_nonempty_prefix_builder(temporary.path().join("valid"), &tip_frontiers)?
            .prepare_cold_certified_publication()?;

        let tampered = cold_certifiable_nonempty_prefix_builder(
            temporary.path().join("tampered"),
            &tip_frontiers,
        )?;
        let loaded_root = complete_prefix_root(ShieldedProtocol::Sapling, 0, 0x51, BUILD_TIP);
        let tampered_root = complete_prefix_root(ShieldedProtocol::Sapling, 0, 0x99, BUILD_TIP);
        {
            let family = tampered
                .bounded_open
                .db
                .cf_handle(super::SUBTREE_ROOT_COLUMN_FAMILY)
                .ok_or("subtree-root family must exist")?;
            tampered.bounded_open.db.put_cf(
                &family,
                encode_subtree_root_key(&loaded_root),
                encode_subtree_root_value(&tampered_root),
            )?;
        }
        tampered.bounded_open.db.flush_wal(true)?;

        let error = tampered
            .prepare_cold_certified_publication()
            .err()
            .ok_or("cold certification must reject a tampered subtree-root family")?;
        assert!(matches!(
            error,
            CanonicalStoreError::PublicationRefused { .. }
        ));
        assert!(
            error
                .to_string()
                .contains("cold subtree-root evidence differs")
        );
        Ok(())
    }

    #[test]
    fn complete_prefix_store_reports_missing_pre_floor_root_as_storage_damage()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempdir()?;
        let completed_sapling = completed_sapling_tip_frontiers()?;
        let mut builder = loaded_checkpointed_builder_from(
            temporary.path().join("canonical"),
            completed_sapling.clone(),
            completed_sapling.clone(),
            completed_sapling.clone(),
        )?;
        let pre_floor_root = complete_prefix_root(ShieldedProtocol::Sapling, 0, 0x51, PREDECESSOR);
        let evidence = builder.load_complete_subtree_root_prefix([pre_floor_root.clone()])?;
        assert_eq!(
            evidence.coverage,
            CanonicalSubtreeRootLoadCoverage::CompletePrefix
        );
        builder.confirm_source_tip_checkpoint(&CommitmentTreeCheckpoint::new(
            BUILD_TIP,
            BUILD_TIP.height.value(),
            completed_sapling,
        ))?;
        let validated = builder.prepare_cold_certified_publication()?;
        let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
            BUILD_TIP,
            UnixTimestampMillis::new(1_750_000_000_001),
        ))?;
        let store = validated.publish_baseline(publication)?;
        let range = SubtreeRootRange::new(
            ShieldedProtocol::Sapling,
            SubtreeRootIndex::new(0),
            NonZeroU32::new(1).ok_or("subtree range must be nonzero")?,
        );
        assert_eq!(
            store.subtree_roots(range)?,
            std::slice::from_ref(&pre_floor_root)
        );

        {
            let family = store
                .bounded_open
                .db
                .cf_handle(super::SUBTREE_ROOT_COLUMN_FAMILY)
                .ok_or("subtree-root family must exist")?;
            store
                .bounded_open
                .db
                .delete_cf(&family, encode_subtree_root_key(&pre_floor_root))?;
        }

        let error = store
            .subtree_roots(range)
            .err()
            .ok_or("a covered pre-floor root gap must fail closed")?;
        assert!(
            matches!(error, CanonicalStoreError::PublicationRefused { .. }),
            "unexpected missing-root error: {error:?}"
        );

        store.bounded_open.db.flush_wal(true)?;
        drop(store);
        let reopened = RocksDbCanonicalStore::open_ready(
            temporary.path().join("canonical"),
            &crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let reopened_error = reopened
            .subtree_roots(range)
            .err()
            .ok_or("a reopened covered pre-floor root gap must fail closed")?;
        assert!(
            matches!(
                reopened_error,
                CanonicalStoreError::PublicationRefused { .. }
            ),
            "unexpected reopened missing-root error: {reopened_error:?}"
        );
        Ok(())
    }

    fn loaded_builder_with_complete_prefix_requirements(
        path: impl AsRef<Path>,
    ) -> Result<RocksDbCanonicalBuilder, Box<dyn std::error::Error>> {
        let activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;
        let tip_frontiers =
            crate::canonical_store::test_checkpoint_frontiers(&activations, BUILD_TIP.height);
        let mut builder = loaded_checkpointed_builder(path, tip_frontiers)?;
        builder
            .canonical_block_evidence
            .as_mut()
            .ok_or("canonical block evidence must be loaded")?
            .tip_metadata = ChainTipMetadata::new(SUBTREE_LEAF_COUNT * 2, SUBTREE_LEAF_COUNT, 0);
        Ok(builder)
    }

    fn cold_certifiable_nonempty_prefix_builder(
        path: impl AsRef<Path>,
        tip_frontiers: &CommitmentTreeFrontiers,
    ) -> Result<RocksDbCanonicalBuilder, Box<dyn std::error::Error>> {
        let mut builder = loaded_checkpointed_builder(path, tip_frontiers.clone())?;
        let loaded_root = complete_prefix_root(ShieldedProtocol::Sapling, 0, 0x51, BUILD_TIP);
        let evidence = builder.load_complete_subtree_root_prefix([loaded_root])?;
        assert_eq!(
            evidence.coverage,
            CanonicalSubtreeRootLoadCoverage::CompletePrefix
        );
        assert_eq!(evidence.subtree_root_count, 1);
        builder.confirm_source_tip_checkpoint(&CommitmentTreeCheckpoint::new(
            BUILD_TIP,
            BUILD_TIP.height.value(),
            tip_frontiers.clone(),
        ))?;
        Ok(builder)
    }

    fn loaded_checkpointed_builder(
        path: impl AsRef<Path>,
        tip_frontiers: CommitmentTreeFrontiers,
    ) -> Result<RocksDbCanonicalBuilder, Box<dyn std::error::Error>> {
        let activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;
        loaded_checkpointed_builder_from(
            path,
            crate::canonical_store::test_checkpoint_frontiers(&activations, PREDECESSOR.height),
            crate::canonical_store::test_checkpoint_frontiers(&activations, FIRST_RETAINED.height),
            tip_frontiers,
        )
    }

    /// Loads two retained blocks above a predecessor whose trees are
    /// `predecessor_frontiers`; the first retained block holds
    /// `first_retained_frontiers`.
    fn loaded_checkpointed_builder_from(
        path: impl AsRef<Path>,
        predecessor_frontiers: CommitmentTreeFrontiers,
        first_retained_frontiers: CommitmentTreeFrontiers,
        tip_frontiers: CommitmentTreeFrontiers,
    ) -> Result<RocksDbCanonicalBuilder, Box<dyn std::error::Error>> {
        let activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;
        let build_plan = CanonicalStoreBuildPlan::checkpointed(
            &activations,
            CommitmentTreeCheckpoint::new(
                PREDECESSOR,
                PREDECESSOR.height.value(),
                predecessor_frontiers,
            ),
            BUILD_TIP,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
        )?;
        let mut builder = RocksDbCanonicalBuilder::create_fresh(
            path,
            CanonicalStoreWorkload::Wallet,
            build_plan,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let mut first = complete_prefix_test_block(
            FIRST_RETAINED,
            PREDECESSOR.hash,
            first_retained_frontiers.tip_metadata(),
        );
        first.tree_state_checkpoint = Some(CommitmentTreeCheckpoint::new(
            FIRST_RETAINED,
            FIRST_RETAINED.height.value(),
            first_retained_frontiers,
        ));
        let tip_metadata = tip_frontiers.tip_metadata();
        let mut tip = complete_prefix_test_block(BUILD_TIP, FIRST_RETAINED.hash, tip_metadata);
        tip.tree_state_checkpoint = Some(CommitmentTreeCheckpoint::new(
            BUILD_TIP,
            BUILD_TIP.height.value(),
            tip_frontiers,
        ));
        builder.bulk_load_blocks([Ok::<_, std::io::Error>(first), Ok::<_, std::io::Error>(tip)])?;
        Ok(builder)
    }

    fn complete_prefix_test_block(
        block_id: BlockId,
        parent_hash: BlockHash,
        tip_metadata: ChainTipMetadata,
    ) -> CanonicalBuildBlock {
        let facts = CanonicalBlockFacts {
            block_header: BlockHeaderArtifact::new(
                block_id.height,
                block_id.hash,
                parent_hash,
                [3; 32],
                [4; 32],
                i64::from(block_id.height.value()),
                0x1d00_ffff,
                [5; 32],
                4,
                128,
            ),
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&[]),
            transactions: Vec::new(),
        };
        let replay_envelope = encode_canonical_block_replay(
            &facts,
            CanonicalBlockReplayFormatVersion::V1,
            CanonicalBlockFactsDigestVersion::V1,
        );
        CanonicalBuildBlock {
            compact_block: CompactBlockArtifact::empty(
                block_id,
                parent_hash,
                block_id.height.value(),
                CompactChainMetadata {
                    sapling_commitment_tree_size: tip_metadata.sapling_commitment_tree_size,
                    orchard_commitment_tree_size: tip_metadata.orchard_commitment_tree_size,
                    ironwood_commitment_tree_size: tip_metadata.ironwood_commitment_tree_size,
                },
            ),
            replay_envelope,
            tip_metadata,
            tree_state_checkpoint: None,
            block_final_note_commitment_roots: None,
            transaction_blobs: Vec::new(),
            block_blob: None,
            facts,
        }
    }

    fn completed_sapling_tip_frontiers()
    -> Result<CommitmentTreeFrontiers, Box<dyn std::error::Error>> {
        let mut leaf_bytes = [0; 32];
        leaf_bytes[0] = 1;
        let sapling_leaf = Option::<SaplingNode>::from(SaplingNode::from_bytes(leaf_bytes))
            .ok_or("one must be a canonical Sapling field element")?;
        let completed_position = u64::from(SUBTREE_LEAF_COUNT)
            .checked_sub(1)
            .ok_or("subtree leaf count must be nonzero")?;
        let ommer_count = usize::try_from(SUBTREE_LEAF_COUNT.trailing_zeros())?;
        let frontier: Frontier<SaplingNode, 32> = Frontier::from_parts(
            Position::from(completed_position),
            sapling_leaf,
            vec![sapling_leaf; ommer_count],
        )
        .map_err(|error| format!("valid completed Sapling frontier rejected: {error:?}"))?;
        let tree = CommitmentTree::from_frontier(&frontier);
        let mut final_state_bytes = Vec::new();
        write_commitment_tree(&tree, &mut final_state_bytes)?;
        let mut final_root_bytes = frontier.root().to_bytes();
        final_root_bytes.reverse();
        let sapling = CommitmentTreeFrontier::from_canonical_final_state(
            ShieldedProtocol::Sapling,
            FinalNoteCommitmentRoot::from_bytes(final_root_bytes),
            final_state_bytes,
        )?;
        Ok(CommitmentTreeFrontiers::from_validated_parts(
            Some(sapling),
            Some(CommitmentTreeFrontier::empty(ShieldedProtocol::Orchard)),
            Some(CommitmentTreeFrontier::empty(ShieldedProtocol::Ironwood)),
        ))
    }

    fn valid_complete_prefix_roots() -> Vec<SubtreeRootArtifact> {
        vec![
            complete_prefix_root(ShieldedProtocol::Sapling, 0, 0x51, PREDECESSOR),
            complete_prefix_root(ShieldedProtocol::Sapling, 1, 0x52, FIRST_RETAINED),
            complete_prefix_root(ShieldedProtocol::Orchard, 0, 0x61, BUILD_TIP),
        ]
    }

    fn invalid_complete_prefix_cases(
        valid_roots: &[SubtreeRootArtifact],
    ) -> Vec<(&'static str, Vec<SubtreeRootArtifact>, &'static str)> {
        let missing_root = valid_roots[..2].to_vec();
        let mut extra_root = valid_roots.to_vec();
        extra_root.push(complete_prefix_root(
            ShieldedProtocol::Ironwood,
            0,
            0x71,
            BUILD_TIP,
        ));
        let mut wrong_protocol = valid_roots.to_vec();
        wrong_protocol[0] = complete_prefix_root(ShieldedProtocol::Orchard, 0, 0x51, PREDECESSOR);
        let mut wrong_index_order = valid_roots.to_vec();
        wrong_index_order.swap(0, 1);
        let mut retained_hash_mismatch = valid_roots.to_vec();
        retained_hash_mismatch[1] = complete_prefix_root(
            ShieldedProtocol::Sapling,
            1,
            0x52,
            BlockId::new(FIRST_RETAINED.height, BlockHash::from_bytes([0xfa; 32])),
        );
        let mut predecessor_hash_mismatch = valid_roots.to_vec();
        predecessor_hash_mismatch[0] = complete_prefix_root(
            ShieldedProtocol::Sapling,
            0,
            0x51,
            BlockId::new(PREDECESSOR.height, BlockHash::from_bytes([0xfb; 32])),
        );
        vec![
            (
                "missing root",
                missing_root,
                "missing Orchard complete-prefix subtree root at index 0",
            ),
            (
                "extra root",
                extra_root,
                "unexpected Ironwood complete-prefix subtree root at index 0",
            ),
            (
                "wrong protocol",
                wrong_protocol,
                "expected Sapling complete-prefix subtree index 0, observed Orchard index 0",
            ),
            (
                "wrong index ordering",
                wrong_index_order,
                "expected Sapling complete-prefix subtree index 0, observed Sapling index 1",
            ),
            (
                "retained completion-block hash mismatch",
                retained_hash_mismatch,
                "completing block differs from retained canonical history",
            ),
            (
                "predecessor hash mismatch",
                predecessor_hash_mismatch,
                "completing block differs from the authenticated predecessor",
            ),
        ]
    }

    fn complete_prefix_root(
        protocol: ShieldedProtocol,
        index: u32,
        root_hash_byte: u8,
        completing_block: BlockId,
    ) -> SubtreeRootArtifact {
        SubtreeRootArtifact::new(
            protocol,
            SubtreeRootIndex::new(index),
            SubtreeRootHash::from_bytes([root_hash_byte; 32]),
            completing_block.height,
            completing_block.hash,
        )
    }

    fn persisted_subtree_root_count(
        builder: &RocksDbCanonicalBuilder,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        let family = builder
            .bounded_open
            .db
            .cf_handle(super::SUBTREE_ROOT_COLUMN_FAMILY)
            .ok_or("subtree-root family must exist")?;
        let rows = builder
            .bounded_open
            .db
            .iterator_cf(&family, IteratorMode::Start)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows.len())
    }
}
