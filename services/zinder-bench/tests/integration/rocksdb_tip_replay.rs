#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader},
    num::NonZeroU32,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    sync::{
        Arc,
        mpsc::{self, RecvTimeoutError},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use eyre::{Result, eyre};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tempfile::{TempDir, tempdir};
use zinder_bench::{
    capture::measure_fixture_blocks,
    fixture::{
        FIXTURE_CONTRACT_IDENTITY, FIXTURE_FORMAT_VERSION, FixtureManifest, SubtreeRootSet,
        write_segment,
    },
    tip_replay_checkpoints::{TipReplayCheckpoints, capture_tip_replay_checkpoints},
};
use zinder_core::{
    BlockHeight, BlockId, CommitmentTreeAccumulator, CommitmentTreeCheckpoint,
    CommitmentTreeFrontiers, Network, NetworkUpgradeActivations, UnixTimestampMillis,
    wire::encode_zinder_native_chain_name,
};
use zinder_ingest::{CanonicalConstructionConfig, load_fresh_canonical};
use zinder_source::{
    NodeCapabilities, NodeSource, SourceBlock, SourceChainSegment, SourceChainSegmentLimits,
    SourceError,
};
use zinder_store::{
    CanonicalBaselinePublication, CanonicalReorgPolicy, CanonicalStoreBuildPlan,
    CanonicalStoreWorkload, RawBlobRetention, RocksDbCanonicalBuilder, RocksDbCanonicalStore,
    RocksDbResourceBudget,
};
use zinder_testkit::sample_regtest_upgrade_activations;
use zinder_wallet_rocksdb::{RocksDbWalletBuildOptions, build_wallet_from_canonical};

use crate::common::regtest_activation_records;

const REGTEST_BLOCK_1: &str =
    include_str!("../../../zinder-ingest/tests/fixtures/z3-regtest-block-1.json");
/// Byte offsets inside the z3 Regtest block-1 fixture that let one captured
/// block become a connected chain: the header's parent hash, and the
/// coinbase script's `OP_N` height push.
const SERIALIZED_HEADER_PARENT_OFFSET: usize = 4;
const SERIALIZED_HEADER_PARENT_END: usize = SERIALIZED_HEADER_PARENT_OFFSET + 32;
const COINBASE_HEIGHT_OPCODE_OFFSET: usize = 1_534;
const REORG_WINDOW_BLOCKS: u32 = 2;
const START_HEIGHT: u32 = 4;
const CHAIN_TIP_HEIGHT: u32 = 8;
/// Bounds a replay that must fail, so one that retries forever fails the test.
const REPLAY_FAILURE_DEADLINE: Duration = Duration::from_secs(15);

/// Test-local source serving a parseable Regtest chain and its checkpoints.
#[derive(Clone)]
struct ParseableChainSource {
    blocks: Arc<BTreeMap<BlockHeight, SourceBlock>>,
    checkpoints: Arc<BTreeMap<BlockHeight, CommitmentTreeCheckpoint>>,
    tip_height: BlockHeight,
}

#[async_trait]
impl NodeSource for ParseableChainSource {
    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities::default()
    }

    async fn fetch_block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
        self.blocks
            .get(&height)
            .cloned()
            .ok_or_else(|| SourceError::BlockUnavailable {
                height,
                reason: "parseable chain has no block at this height".to_owned(),
            })
    }

    async fn fetch_chain_segment(
        &self,
        limits: SourceChainSegmentLimits,
    ) -> Result<SourceChainSegment, SourceError> {
        let Some(start_height) = limits.cursor.next_connected_height() else {
            return Ok(SourceChainSegment::default());
        };
        let blocks = self
            .blocks
            .range(start_height..=self.tip_height)
            .take(usize::try_from(limits.max_connected_blocks.get()).unwrap_or(usize::MAX))
            .map(|(_, block)| block.clone())
            .collect::<Vec<_>>();
        Ok(SourceChainSegment::connected_blocks(blocks))
    }

    async fn fetch_chain_checkpoint(
        &self,
        height: BlockHeight,
        _network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<CommitmentTreeCheckpoint, SourceError> {
        self.checkpoints
            .get(&height)
            .cloned()
            .ok_or_else(|| SourceError::BlockUnavailable {
                height,
                reason: "parseable chain has no checkpoint at this height".to_owned(),
            })
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        let block = self.fetch_block_at(self.tip_height).await?;
        Ok(BlockId::new(self.tip_height, block.hash))
    }
}

/// A canonical store and wallet store at `START_HEIGHT`, and a fixture plus
/// tip replay checkpoints for the blocks after it.
struct TipReplayInputs {
    directory: TempDir,
    activations: NetworkUpgradeActivations,
}

impl TipReplayInputs {
    fn canonical_store(&self) -> PathBuf {
        self.directory.path().join("canonical")
    }

    fn wallet_store(&self) -> PathBuf {
        self.directory.path().join("wallet")
    }

    fn fixture(&self) -> PathBuf {
        self.directory.path().join("fixture")
    }
}

async fn prepare_inputs() -> Result<TipReplayInputs> {
    let directory = tempdir()?;
    let activations = sample_regtest_upgrade_activations();
    let blocks = parseable_chain(CHAIN_TIP_HEIGHT)?;
    let source = ParseableChainSource {
        checkpoints: Arc::new(checkpoints_for(&blocks, &activations)?),
        blocks: Arc::new(blocks),
        tip_height: BlockHeight::new(START_HEIGHT),
    };
    let inputs = TipReplayInputs {
        directory,
        activations,
    };
    let canonical = publish_canonical(&source, &inputs).await?;
    let wallet = build_wallet_from_canonical(
        &canonical,
        inputs.wallet_store(),
        RocksDbWalletBuildOptions {
            supported_reorg_depth: REORG_WINDOW_BLOCKS,
            ..RocksDbWalletBuildOptions::for_local_tests()
        },
    )?;
    drop(wallet.store);
    drop(canonical);

    let fixture_blocks: Vec<SourceBlock> = source
        .blocks
        .range(BlockHeight::new(START_HEIGHT + 1)..)
        .map(|(_, block)| block.clone())
        .collect();
    write_fixture(&inputs.fixture(), &fixture_blocks, &inputs.activations)?;
    capture_tip_replay_checkpoints(
        &inputs.fixture(),
        &source,
        &inputs.activations,
        NonZeroU32::MIN,
    )
    .await?;
    Ok(inputs)
}

async fn publish_canonical(
    source: &ParseableChainSource,
    inputs: &TipReplayInputs,
) -> Result<RocksDbCanonicalStore> {
    let first_block = source
        .blocks
        .get(&BlockHeight::new(1))
        .ok_or_else(|| eyre!("parseable chain must start at height 1"))?;
    let tip = block_id(source, START_HEIGHT)?;
    let build_plan = CanonicalStoreBuildPlan::complete(
        &inputs.activations,
        first_block.block_time_seconds.saturating_sub(1),
        tip,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(REORG_WINDOW_BLOCKS)?,
    )?;
    let builder = RocksDbCanonicalBuilder::create_fresh(
        inputs.canonical_store(),
        CanonicalStoreWorkload::Wallet,
        build_plan,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let construction = CanonicalConstructionConfig::for_local_tests(
        Duration::from_secs(5),
        Arc::new(inputs.activations.clone()),
    );
    let built = load_fresh_canonical(builder, source, &construction).await?;
    let validated = built.builder.prepare_cold_certified_publication()?;
    let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
        block_id(source, START_HEIGHT - REORG_WINDOW_BLOCKS)?,
        UnixTimestampMillis::new(1_783_933_200_000),
    ))?;
    Ok(validated.publish_baseline(publication)?)
}

fn parseable_chain(tip_height: u32) -> Result<BTreeMap<BlockHeight, SourceBlock>> {
    let fixture: Value = serde_json::from_str(REGTEST_BLOCK_1)?;
    let raw_block_bytes = hex::decode(
        fixture
            .get("raw_block_hex")
            .and_then(Value::as_str)
            .ok_or_else(|| eyre!("fixture raw_block_hex must be a string"))?,
    )?;
    let mut parent_hash = Network::ZcashRegtest.genesis_hash();
    let mut blocks = BTreeMap::new();
    for height in 1..=tip_height {
        let mut block_bytes = raw_block_bytes.clone();
        block_bytes[SERIALIZED_HEADER_PARENT_OFFSET..SERIALIZED_HEADER_PARENT_END]
            .copy_from_slice(&parent_hash.as_bytes());
        block_bytes[COINBASE_HEIGHT_OPCODE_OFFSET] = 0x50_u8.saturating_add(u8::try_from(height)?);
        let height = BlockHeight::new(height);
        let block = SourceBlock::from_raw_block_bytes(Network::ZcashRegtest, height, block_bytes)?;
        parent_hash = block.hash;
        blocks.insert(height, block);
    }
    Ok(blocks)
}

fn checkpoints_for(
    blocks: &BTreeMap<BlockHeight, SourceBlock>,
    activations: &NetworkUpgradeActivations,
) -> Result<BTreeMap<BlockHeight, CommitmentTreeCheckpoint>> {
    let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
        BlockHeight::new(0),
        &CommitmentTreeFrontiers::default(),
        activations,
    )?;
    let mut checkpoints = BTreeMap::new();
    for (height, block) in blocks {
        accumulator.append_block_commitments(*height, &[], &[], &[])?;
        checkpoints.insert(
            *height,
            CommitmentTreeCheckpoint::new(
                BlockId::new(*height, block.hash),
                block.block_time_seconds,
                accumulator.validated_frontiers()?,
            ),
        );
    }
    Ok(checkpoints)
}

fn write_fixture(
    directory: &Path,
    blocks: &[SourceBlock],
    activations: &NetworkUpgradeActivations,
) -> Result<()> {
    let first = blocks
        .first()
        .ok_or_else(|| eyre!("fixture needs blocks"))?;
    let last = blocks.last().ok_or_else(|| eyre!("fixture needs blocks"))?;
    std::fs::create_dir(directory)?;
    let descriptor = write_segment(directory, 0, blocks)?;
    let measurements = measure_fixture_blocks(blocks, activations)?;
    FixtureManifest {
        contract_identity: FIXTURE_CONTRACT_IDENTITY.to_owned(),
        fixture_format_version: FIXTURE_FORMAT_VERSION,
        network: encode_zinder_native_chain_name(Network::ZcashRegtest).to_owned(),
        from_height: first.height.value(),
        to_height: last.height.value(),
        block_count: u32::try_from(blocks.len())?,
        workload_density: measurements.workload_density,
        canonical_artifact_schema_version: zinder_store::CURRENT_ARTIFACT_SCHEMA_VERSION.value(),
        canonical_block_facts_digest_evidence: measurements
            .canonical_block_facts_digest_evidence()?,
        tip_hash_hex: hex::encode(last.hash.as_bytes()),
        network_upgrade_activations: regtest_activation_records(),
        segments: vec![descriptor],
        subtree_roots: SubtreeRootSet::default(),
    }
    .write(directory)?;
    Ok(())
}

fn block_id(source: &ParseableChainSource, height: u32) -> Result<BlockId> {
    let height = BlockHeight::new(height);
    let block = source
        .blocks
        .get(&height)
        .ok_or_else(|| eyre!("parseable chain is missing height {}", height.value()))?;
    Ok(BlockId::new(height, block.hash))
}

fn run_tip_replay(inputs: &TipReplayInputs, fixture: &Path, extra: &[&str]) -> Result<Output> {
    Ok(tip_replay_command(inputs, fixture, extra).output()?)
}

fn tip_replay_command(inputs: &TipReplayInputs, fixture: &Path, extra: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_zinder-bench"));
    command
        .arg("rocksdb-tip-replay")
        .arg("--fixture")
        .arg(fixture)
        .arg("--canonical-store")
        .arg(inputs.canonical_store())
        .arg("--wallet-store")
        .arg(inputs.wallet_store())
        .args(["--software-revision", "integration-test"])
        .args(["--reorg-window-blocks", &REORG_WINDOW_BLOCKS.to_string()])
        .args(["--canonical-block-cache-bytes", "8388608"])
        .args(["--canonical-secondary-block-cache-bytes", "8388608"])
        .args(["--wallet-block-cache-bytes", "8388608"])
        .args(extra);
    command
}

fn compare_reports(baseline: &Path, candidate: &Path, comparison: &Path) -> Result<Output> {
    Ok(Command::new(env!("CARGO_BIN_EXE_zinder-bench"))
        .arg("compare-tip-replay-reports")
        .arg("--baseline")
        .arg(baseline)
        .arg("--candidate")
        .arg(candidate)
        .arg("--report")
        .arg(comparison)
        .output()?)
}

/// Kills the child process if the test returns before it exits.
struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn require_success(output: &Output, action: &str) -> Result<()> {
    if output.status.success() {
        return Ok(());
    }
    Err(eyre!(
        "{action} failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    ))
}

/// Digest of every file name and byte below `directory`, to prove inputs are untouched.
fn tree_digest(directory: &Path) -> Result<String> {
    let mut files = Vec::new();
    let mut pending = vec![directory.to_path_buf()];
    while let Some(current) = pending.pop() {
        for entry in std::fs::read_dir(&current)? {
            let path = entry?.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                files.push(path);
            }
        }
    }
    files.sort();
    let mut hasher = Sha256::new();
    for file in files {
        hasher.update(file.strip_prefix(directory)?.to_string_lossy().as_bytes());
        hasher.update(std::fs::read(&file)?);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn read_json(path: &Path) -> Result<Value> {
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}

fn rebuilt_wallet_digest(inputs: &TipReplayInputs, canonical_store: &Path) -> Result<String> {
    let canonical = RocksDbCanonicalStore::open_ready(
        canonical_store,
        &inputs.activations,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(REORG_WINDOW_BLOCKS)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let rebuilt = build_wallet_from_canonical(
        &canonical,
        inputs.directory.path().join("rebuilt-wallet"),
        RocksDbWalletBuildOptions {
            supported_reorg_depth: REORG_WINDOW_BLOCKS,
            ..RocksDbWalletBuildOptions::for_local_tests()
        },
    )?;
    Ok(hex::encode(rebuilt.report.projection_digest.as_bytes()))
}

/// Checks the report of a 2-repeat replay and returns its final repeat.
fn assert_two_repeat_report(report: &Value) -> Result<&Value> {
    assert_eq!(report["contract_identity"], "rocksdb-tip-replay");
    assert_eq!(report["method"]["percentile_method"], "nearest-rank");
    assert_eq!(
        report["provenance"]["software_revision"],
        "integration-test"
    );
    assert!(report["provenance"]["build"]["cargo_profile"].is_string());
    assert!(report["process"]["cpu_seconds"].as_f64() > Some(0.0));
    assert_eq!(
        report["configuration"]["wallet_budget"]["block_cache_bytes"],
        8_388_608
    );
    assert_eq!(
        report["inputs"]["canonical_store"]["start"]["tip_height"],
        START_HEIGHT
    );
    assert_eq!(report["aggregate"]["all_blocks"]["block_count"], 8);
    assert_eq!(
        report["aggregate"]["all_blocks"]["timers"]["canonical_commit"]["count"],
        8
    );
    let repeats = report["repeats"]
        .as_array()
        .ok_or_else(|| eyre!("report must list repeats"))?;
    assert_eq!(repeats.len(), 2);
    let final_repeat = repeats
        .last()
        .ok_or_else(|| eyre!("report must list repeats"))?;
    assert_eq!(
        final_repeat["final_canonical"]["tip_height"],
        CHAIN_TIP_HEIGHT
    );
    assert_eq!(final_repeat["blocks"][0]["height"], START_HEIGHT + 1);
    assert!(final_repeat["blocks"][0]["timings_ms"]["wallet_apply"].as_f64() > Some(0.0));
    let start_utxos = report["inputs"]["wallet_store"]["start"]["utxo_count"].as_u64();
    let final_utxos = final_repeat["final_wallet"]["utxo_count"].as_u64();
    assert!(
        final_utxos > start_utxos,
        "{start_utxos:?} -> {final_utxos:?}"
    );
    Ok(final_repeat)
}

#[tokio::test]
async fn tip_replay_cli_replays_fixture_blocks_and_matches_a_rebuilt_wallet() -> Result<()> {
    let inputs = prepare_inputs().await?;
    let canonical_before = tree_digest(&inputs.canonical_store())?;
    let wallet_before = tree_digest(&inputs.wallet_store())?;
    let scratch = inputs.directory.path().join("scratch");
    let report_path = inputs.directory.path().join("report.json");

    let output = run_tip_replay(
        &inputs,
        &inputs.fixture(),
        &[
            "--scratch-dir",
            &scratch.to_string_lossy(),
            "--report",
            &report_path.to_string_lossy(),
            "--repeat",
            "2",
            "--keep-scratch",
        ],
    )?;
    require_success(&output, "tip replay")?;

    assert_eq!(tree_digest(&inputs.canonical_store())?, canonical_before);
    assert_eq!(tree_digest(&inputs.wallet_store())?, wallet_before);
    let report = read_json(&report_path)?;
    let final_repeat = assert_two_repeat_report(&report)?;

    let kept_canonical = final_repeat["kept_scratch"]["canonical_store"]
        .as_str()
        .ok_or_else(|| eyre!("--keep-scratch must name the kept canonical store"))?;
    assert!(!scratch.join("repeat-0").exists());
    assert!(report["repeats"][0]["kept_scratch"].is_null());
    assert_eq!(
        Some(rebuilt_wallet_digest(&inputs, Path::new(kept_canonical))?.as_str()),
        final_repeat["final_wallet"]["projection_digest_hex"].as_str()
    );

    let report_bytes = std::fs::read(&report_path)?;
    let rerun = run_tip_replay(
        &inputs,
        &inputs.fixture(),
        &[
            "--scratch-dir",
            &inputs.directory.path().join("rerun").to_string_lossy(),
            "--report",
            &report_path.to_string_lossy(),
        ],
    )?;
    assert!(!rerun.status.success());
    assert_eq!(std::fs::read(&report_path)?, report_bytes);
    Ok(())
}

#[tokio::test]
async fn tip_replay_reports_compare_and_name_their_setting_differences() -> Result<()> {
    let inputs = prepare_inputs().await?;
    let mut reports = Vec::new();
    for (index, setting) in [
        ["--repeat", "1"],
        ["--max-transition-logical-bytes", "268435456"],
        ["--repeat", "2"],
    ]
    .into_iter()
    .enumerate()
    {
        let scratch = inputs.directory.path().join(format!("scratch-{index}"));
        let report = inputs.directory.path().join(format!("report-{index}.json"));
        let output = run_tip_replay(
            &inputs,
            &inputs.fixture(),
            &[
                "--scratch-dir",
                &scratch.to_string_lossy(),
                "--report",
                &report.to_string_lossy(),
                setting[0],
                setting[1],
            ],
        )?;
        require_success(&output, "tip replay")?;
        assert!(!scratch.exists());
        reports.push(report);
    }
    let comparison_path = inputs.directory.path().join("comparison.json");

    let output = compare_reports(&reports[0], &reports[1], &comparison_path)?;
    require_success(&output, "report comparison")?;

    let comparison = read_json(&comparison_path)?;
    assert_eq!(
        comparison["contract_identity"],
        "rocksdb-tip-replay-comparison"
    );
    assert_eq!(comparison["workload"]["block_count"], 4);
    assert_eq!(
        comparison["setting_differences"][0]["setting"],
        "configuration.max_transition_logical_bytes"
    );
    assert_eq!(
        comparison["setting_differences"][0]["candidate"],
        268_435_456
    );
    assert_eq!(
        comparison["timers"]["all_blocks"]["baseline_block_count"],
        4
    );
    assert_eq!(
        comparison["timers"]["all_blocks"]["candidate_block_count"],
        4
    );
    assert!(
        comparison["timers"]["all_blocks"]["timers"]["wallet_apply"]["p99"]["baseline"].is_number()
    );

    let refused_path = inputs.directory.path().join("refused-comparison.json");
    let refused = compare_reports(&reports[0], &reports[2], &refused_path)?;
    assert!(!refused.status.success());
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(
        stderr.contains("baseline repeat count 1 differs from the candidate repeat count 2"),
        "{stderr}"
    );
    assert!(!refused_path.exists());
    Ok(())
}

#[tokio::test]
async fn tip_replay_comparison_refuses_reports_summarized_by_different_methods() -> Result<()> {
    let inputs = prepare_inputs().await?;
    let scratch = inputs.directory.path().join("scratch");
    let report = inputs.directory.path().join("report.json");
    let output = run_tip_replay(
        &inputs,
        &inputs.fixture(),
        &[
            "--scratch-dir",
            &scratch.to_string_lossy(),
            "--report",
            &report.to_string_lossy(),
        ],
    )?;
    require_success(&output, "tip replay")?;

    for (field, replacement, expected) in [
        (
            "stratum_rules",
            None,
            "the baseline and candidate stratum rules differ",
        ),
        (
            "percentile_method",
            Some(Value::from("linear-interpolation")),
            "the candidate percentile method linear-interpolation is not the supported nearest-rank method",
        ),
    ] {
        let mut candidate = read_json(&report)?;
        let method = &mut candidate["method"][field];
        *method = replacement.unwrap_or_else(|| {
            let mut rules = method.clone();
            rules["after_dense_window_heights"] = Value::from(1);
            rules
        });
        let mutated_path = inputs
            .directory
            .path()
            .join(format!("mutated-{field}.json"));
        std::fs::write(&mutated_path, serde_json::to_vec_pretty(&candidate)?)?;
        let refused_path = inputs
            .directory
            .path()
            .join(format!("refused-{field}.json"));

        let refused = compare_reports(&report, &mutated_path, &refused_path)?;

        assert!(!refused.status.success());
        let stderr = String::from_utf8_lossy(&refused.stderr);
        assert!(stderr.contains(expected), "{stderr}");
        assert!(!refused_path.exists());
    }
    Ok(())
}

#[tokio::test]
async fn tip_replay_fails_with_the_source_error_when_a_fixture_segment_becomes_unreadable()
-> Result<()> {
    let inputs = prepare_inputs().await?;
    let manifest = FixtureManifest::read(&inputs.fixture())?;
    let segment = inputs.fixture().join(
        &manifest
            .segments
            .first()
            .ok_or_else(|| eyre!("fixture needs a segment"))?
            .file,
    );
    let scratch = inputs.directory.path().join("scratch");
    let report = inputs.directory.path().join("report.json");
    // Enough repeats that the replay is still running when the segment goes.
    let mut child = KillOnDrop(
        tip_replay_command(
            &inputs,
            &inputs.fixture(),
            &[
                "--scratch-dir",
                &scratch.to_string_lossy(),
                "--report",
                &report.to_string_lossy(),
                "--repeat",
                "1000",
            ],
        )
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?,
    );
    let stderr = child
        .0
        .stderr
        .take()
        .ok_or_else(|| eyre!("tip replay stderr must be piped"))?;
    let (line_sender, lines) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stderr)
            .lines()
            .map_while(std::io::Result::ok)
        {
            if line_sender.send(line).is_err() {
                break;
            }
        }
    });

    let deadline = Instant::now() + REPLAY_FAILURE_DEADLINE;
    let mut stderr_lines = Vec::new();
    loop {
        match lines.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
            Ok(line) => {
                // Admission has read the segment; only the follower reads it after this.
                if line.contains("tip_replay_admitted") {
                    std::fs::remove_file(&segment)?;
                }
                stderr_lines.push(line);
            }
            Err(RecvTimeoutError::Disconnected) => break,
            Err(RecvTimeoutError::Timeout) => {
                return Err(eyre!(
                    "tip replay still ran {REPLAY_FAILURE_DEADLINE:?} after it started:\n{}",
                    stderr_lines.join("\n")
                ));
            }
        }
    }
    let status = child.0.wait()?;
    reader
        .join()
        .map_err(|_| eyre!("the stderr reader thread panicked"))?;

    let stderr = stderr_lines.join("\n");
    assert!(!status.success(), "{stderr}");
    assert!(stderr.contains("source error"), "{stderr}");
    assert!(stderr.contains("fixture read failed"), "{stderr}");
    assert!(!report.exists());
    assert!(!scratch.exists());
    Ok(())
}

#[tokio::test]
async fn tip_replay_refuses_a_fixture_that_does_not_extend_the_canonical_tip() -> Result<()> {
    let inputs = prepare_inputs().await?;
    let blocks = parseable_chain(CHAIN_TIP_HEIGHT)?;
    let gapped: Vec<SourceBlock> = blocks
        .range(BlockHeight::new(START_HEIGHT + 2)..)
        .map(|(_, block)| block.clone())
        .collect();
    let gapped_fixture = inputs.directory.path().join("gapped-fixture");
    write_fixture(&gapped_fixture, &gapped, &inputs.activations)?;
    let source = ParseableChainSource {
        checkpoints: Arc::new(checkpoints_for(&blocks, &inputs.activations)?),
        blocks: Arc::new(blocks),
        tip_height: BlockHeight::new(CHAIN_TIP_HEIGHT),
    };
    capture_tip_replay_checkpoints(
        &gapped_fixture,
        &source,
        &inputs.activations,
        NonZeroU32::MIN,
    )
    .await?;
    let scratch = inputs.directory.path().join("scratch");
    let report = inputs.directory.path().join("report.json");

    let output = run_tip_replay(
        &inputs,
        &gapped_fixture,
        &[
            "--scratch-dir",
            &scratch.to_string_lossy(),
            "--report",
            &report.to_string_lossy(),
        ],
    )?;

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("is not the parent"));
    assert!(!report.exists());
    assert!(!scratch.exists());
    Ok(())
}

#[tokio::test]
async fn tip_replay_refuses_a_report_path_that_is_the_scratch_dir() -> Result<()> {
    let inputs = prepare_inputs().await?;
    let output_path = inputs.directory.path().join("shared-output");

    let output = run_tip_replay(
        &inputs,
        &inputs.fixture(),
        &[
            "--scratch-dir",
            &output_path.to_string_lossy(),
            "--report",
            &output_path.to_string_lossy(),
            "--keep-scratch",
        ],
    )?;

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--scratch-dir and --report must not contain each other"),
        "{stderr}"
    );
    assert!(!output_path.exists());
    Ok(())
}

#[tokio::test]
async fn tip_replay_checkpoints_refuse_replacement_and_a_different_manifest() -> Result<()> {
    let directory = tempdir()?;
    let activations = sample_regtest_upgrade_activations();
    let blocks = parseable_chain(CHAIN_TIP_HEIGHT)?;
    let source = ParseableChainSource {
        checkpoints: Arc::new(checkpoints_for(&blocks, &activations)?),
        blocks: Arc::new(blocks),
        tip_height: BlockHeight::new(CHAIN_TIP_HEIGHT),
    };
    let fixture = directory.path().join("fixture");
    let fixture_blocks: Vec<SourceBlock> = source.blocks.values().cloned().collect();
    write_fixture(&fixture, &fixture_blocks, &activations)?;
    let captured =
        capture_tip_replay_checkpoints(&fixture, &source, &activations, NonZeroU32::MIN).await?;
    assert_eq!(captured.checkpoints.len(), fixture_blocks.len());
    let sidecar = std::fs::read(fixture.join("tip-replay-checkpoints.json"))?;

    let Err(replacement) =
        capture_tip_replay_checkpoints(&fixture, &source, &activations, NonZeroU32::MIN).await
    else {
        return Err(eyre!("an existing sidecar must not be replaced"));
    };
    assert!(replacement.to_string().contains("already exist"));
    assert_eq!(
        std::fs::read(fixture.join("tip-replay-checkpoints.json"))?,
        sidecar
    );

    let mut manifest = FixtureManifest::read(&fixture)?;
    assert_eq!(TipReplayCheckpoints::read(&fixture, &manifest)?, captured);
    manifest.workload_density.transaction_count += 1;
    let Err(mismatch) = TipReplayCheckpoints::read(&fixture, &manifest) else {
        return Err(eyre!("checkpoints of another manifest must be refused"));
    };
    assert!(mismatch.to_string().contains("different fixture manifest"));
    Ok(())
}
