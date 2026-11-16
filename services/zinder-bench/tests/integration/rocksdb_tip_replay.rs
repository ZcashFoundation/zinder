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
use zebra_chain::{
    block::Block,
    serialization::{ZcashDeserialize, ZcashSerialize},
    transaction::{self, Transaction},
};
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
    CommitmentTreeFrontiers, Network, NetworkUpgradeActivations, TransactionId,
    TransparentOutPoint, UnixTimestampMillis, wire::encode_zinder_native_chain_name,
};
use zinder_ingest::{CanonicalConstructionConfig, load_fresh_canonical};
use zinder_source::{
    NodeCapabilities, NodeSource, SourceBlock, SourceChainSegment, SourceChainSegmentLimits,
    SourceError,
};
use zinder_store::{
    CanonicalBaselinePublication, CanonicalReorgPolicy, CanonicalStoreBuildPlan,
    CanonicalStoreWorkload, RawBlobRetention, RocksDbCanonicalBuilder, RocksDbCanonicalSecondary,
    RocksDbCanonicalStore, RocksDbResourceBudget,
};
use zinder_testkit::{
    P2pkhSpendArgs, TRANSPARENT_BROADCAST_TEST_SEED, TransparentTestKey,
    ZIP317_FEE_ONE_IN_ONE_OUT_ZATS, sample_regtest_upgrade_activations,
};
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
/// Value of the output each test spend creates.
const SPEND_OUTPUT_VALUE_ZATS: u64 = 12_345;
/// Height the test spends sign for, past Regtest coinbase maturity.
const SPEND_TARGET_HEIGHT: u32 = 120;

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
    prepare_inputs_for(parseable_chain(CHAIN_TIP_HEIGHT)?).await
}

/// Inputs as [`prepare_inputs`] builds them, from `blocks` instead.
async fn prepare_inputs_for(blocks: BTreeMap<BlockHeight, SourceBlock>) -> Result<TipReplayInputs> {
    let directory = tempdir()?;
    let activations = sample_regtest_upgrade_activations();
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
    chain_spending_at(tip_height, None)
}

/// A parseable chain whose block at `spending_height` also spends the
/// coinbase of the block below it, and then that spend's output.
fn chain_spending_at(
    tip_height: u32,
    spending_height: Option<u32>,
) -> Result<BTreeMap<BlockHeight, SourceBlock>> {
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
        if Some(height) == spending_height {
            let below = blocks
                .get(&BlockHeight::new(height - 1))
                .ok_or_else(|| eyre!("a spending block needs the block below it"))?;
            block_bytes = with_coinbase_spends(&block_bytes, below)?;
        }
        let height = BlockHeight::new(height);
        let block = SourceBlock::from_raw_block_bytes(Network::ZcashRegtest, height, block_bytes)?;
        parent_hash = block.hash;
        blocks.insert(height, block);
    }
    Ok(blocks)
}

/// Appends to `block_bytes` a spend of `below`'s coinbase output and a spend
/// of that spend's output.
fn with_coinbase_spends(block_bytes: &[u8], below: &SourceBlock) -> Result<Vec<u8>> {
    let coinbase = Block::zcash_deserialize(below.raw_block_bytes.as_slice())?
        .transactions
        .first()
        .ok_or_else(|| eyre!("a block must hold a coinbase"))?
        .hash();
    let spend = p2pkh_spend(
        coinbase,
        SPEND_OUTPUT_VALUE_ZATS + ZIP317_FEE_ONE_IN_ONE_OUT_ZATS,
    )?;
    let descendant = p2pkh_spend(spend.hash(), SPEND_OUTPUT_VALUE_ZATS)?;
    let mut block = Block::zcash_deserialize(block_bytes)?;
    block
        .transactions
        .extend([Arc::new(spend), Arc::new(descendant)]);
    Ok(block.zcash_serialize_to_vec()?)
}

/// A one-input spend of output 0 of `spent`, which holds `spent_value_zats`.
fn p2pkh_spend(spent: transaction::Hash, spent_value_zats: u64) -> Result<Transaction> {
    let key = TransparentTestKey::from_seed(&TRANSPARENT_BROADCAST_TEST_SEED)?;
    let recipient = key.scratch_recipient_address(1);
    let bytes = key.build_p2pkh_spend(&P2pkhSpendArgs {
        coinbase_txid_be: spent.0,
        coinbase_vout: 0,
        coinbase_value_zats: spent_value_zats,
        recipient: &recipient,
        target_height: SPEND_TARGET_HEIGHT,
    })?;
    Ok(Transaction::zcash_deserialize(bytes.as_slice())?)
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
        report["method"]["scratch_page_cache"],
        if cfg!(target_os = "linux") {
            "evicted-after-copy"
        } else {
            "not-evicted"
        }
    );
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

/// Runs a one-repeat tip replay and returns its report path.
fn replay_to_report(inputs: &TipReplayInputs, name: &str, extra: &[&str]) -> Result<PathBuf> {
    let scratch = inputs.directory.path().join(format!("scratch-{name}"));
    let report = inputs.directory.path().join(format!("report-{name}.json"));
    let mut arguments = vec![
        "--scratch-dir".to_owned(),
        scratch.to_string_lossy().into_owned(),
        "--report".to_owned(),
        report.to_string_lossy().into_owned(),
    ];
    arguments.extend(extra.iter().map(|argument| (*argument).to_owned()));
    let arguments: Vec<&str> = arguments.iter().map(String::as_str).collect();
    require_success(
        &run_tip_replay(inputs, &inputs.fixture(), &arguments)?,
        "tip replay",
    )?;
    Ok(report)
}

#[tokio::test]
async fn tip_replay_reports_the_requested_io_modes_and_wallet_statistics_deltas() -> Result<()> {
    let inputs = prepare_inputs().await?;
    let report_path = replay_to_report(
        &inputs,
        "statistics",
        &[
            "--wallet-rocksdb-statistics",
            "--canonical-io-mode",
            "buffered",
            "--wallet-io-mode",
            "buffered",
        ],
    )?;

    let report = read_json(&report_path)?;

    assert_eq!(report["report_format_version"], 2);
    let io_mode = &report["configuration"]["rocksdb_io_mode"];
    assert_eq!(io_mode["requested"]["canonical"], "buffered");
    assert_eq!(io_mode["requested"]["wallet"], "buffered");
    for store in ["canonical_primary", "canonical_secondary", "wallet"] {
        assert_eq!(io_mode["resolved"][store], "buffered", "{store}");
    }
    assert_eq!(
        report["configuration"]["wallet_statistics"]["collected"],
        true
    );
    assert_eq!(
        report["configuration"]["wallet_budget"]["statistics_level"],
        "tickers"
    );
    let reported_tickers = report["configuration"]["wallet_statistics"]["tickers"]
        .as_array()
        .ok_or_else(|| eyre!("collected statistics must name their tickers"))?;
    for ticker in [
        "rocksdb.number.keys.read",
        "rocksdb.bloom.filter.useful",
        "rocksdb.bloom.filter.full.positive",
        "rocksdb.bloom.filter.full.true.positive",
        "rocksdb.block.cache.data.miss",
        "rocksdb.block.cache.data.hit",
        "rocksdb.block.cache.filter.miss",
        "rocksdb.block.cache.index.miss",
        "rocksdb.bytes.read",
        "rocksdb.compact.read.bytes",
        "rocksdb.compact.write.bytes",
        "rocksdb.flush.write.bytes",
    ] {
        assert!(reported_tickers.contains(&Value::from(ticker)), "{ticker}");
    }
    let unavailable: Vec<&Value> = report["unavailable_measurements"]
        .as_array()
        .ok_or_else(|| eyre!("report must list unavailable measurements"))?
        .iter()
        .map(|entry| &entry["measurement"])
        .collect();
    assert_eq!(
        unavailable,
        [&Value::from("canonical_store_read_statistics")]
    );
    assert_wallet_ticker_deltas(&report)
}

/// Checks the per-block and per-stratum ticker deltas of a collecting replay.
fn assert_wallet_ticker_deltas(report: &Value) -> Result<()> {
    let blocks = report["repeats"][0]["blocks"]
        .as_array()
        .ok_or_else(|| eyre!("report must list blocks"))?;
    assert!(!blocks.is_empty());
    for block in blocks {
        let deltas = &block["wallet_ticker_deltas"];
        let point_reads = deltas["rocksdb.number.keys.read"].as_u64();
        assert!(
            point_reads > Some(0),
            "height {}: {deltas}",
            block["height"]
        );
        for ticker in [
            "rocksdb.bytes.read",
            "rocksdb.compact.read.bytes",
            "rocksdb.flush.write.bytes",
        ] {
            assert!(deltas[ticker].is_u64(), "{ticker}: {deltas}");
        }
    }
    for stratum in ["all_blocks", "no_transparent_activity"] {
        let summary = &report["aggregate"][stratum]["wallet_tickers"]["rocksdb.number.keys.read"];
        assert!(summary["total"].as_u64() > Some(0), "{stratum}: {summary}");
        assert!(summary["p99"].is_u64());
    }
    assert!(report["aggregate"]["input_heavy"]["wallet_tickers"].is_null());
    Ok(())
}

#[tokio::test]
async fn tip_replay_leaves_wallet_statistics_off_unless_asked() -> Result<()> {
    let inputs = prepare_inputs().await?;
    let report_path = replay_to_report(&inputs, "default", &[])?;

    let report = read_json(&report_path)?;

    assert_eq!(
        report["configuration"]["wallet_statistics"],
        serde_json::json!({"collected": false, "tickers": []})
    );
    assert_eq!(
        report["configuration"]["wallet_budget"]["statistics_level"],
        "off"
    );
    assert_eq!(
        report["configuration"]["rocksdb_io_mode"]["requested"],
        serde_json::json!({"canonical": "direct", "wallet": "direct"})
    );
    assert!(report["repeats"][0]["blocks"][0]["wallet_ticker_deltas"].is_null());
    assert!(report["aggregate"]["all_blocks"]["wallet_tickers"].is_null());
    let unavailable = report["unavailable_measurements"]
        .as_array()
        .ok_or_else(|| eyre!("report must list unavailable measurements"))?;
    for measurement in ["point_read_counts", "rocksdb_statistics_deltas"] {
        assert!(
            unavailable
                .iter()
                .any(|entry| entry["measurement"] == measurement),
            "{measurement}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn tip_replay_comparison_names_io_mode_differences_and_refuses_mixed_statistics() -> Result<()>
{
    let inputs = prepare_inputs().await?;
    let direct = replay_to_report(&inputs, "direct", &[])?;
    let buffered = replay_to_report(&inputs, "buffered", &["--wallet-io-mode", "buffered"])?;
    let collecting = replay_to_report(&inputs, "collecting", &["--wallet-rocksdb-statistics"])?;
    let comparison_path = inputs.directory.path().join("comparison.json");

    require_success(
        &compare_reports(&direct, &buffered, &comparison_path)?,
        "report comparison",
    )?;

    let comparison = read_json(&comparison_path)?;
    let differences = comparison["setting_differences"]
        .as_array()
        .ok_or_else(|| eyre!("comparison must list setting differences"))?;
    let requested = differences
        .iter()
        .find(|difference| {
            difference["setting"] == "configuration.rocksdb_io_mode.requested.wallet"
        })
        .ok_or_else(|| eyre!("the requested wallet I/O mode must differ: {differences:?}"))?;
    assert_eq!(requested["baseline"], "direct");
    assert_eq!(requested["candidate"], "buffered");

    let refused_path = inputs.directory.path().join("refused-comparison.json");
    let refused = compare_reports(&direct, &collecting, &refused_path)?;
    assert!(!refused.status.success());
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(
        stderr.contains(
            "baseline wallet statistics collection (false) differs from the candidate (true)"
        ),
        "{stderr}"
    );
    assert!(!refused_path.exists());

    let both_collecting = replay_to_report(
        &inputs,
        "collecting-again",
        &["--wallet-rocksdb-statistics"],
    )?;
    require_success(
        &compare_reports(
            &collecting,
            &both_collecting,
            &inputs.directory.path().join("statistics-comparison.json"),
        )?,
        "statistics comparison",
    )?;
    let statistics_comparison =
        read_json(&inputs.directory.path().join("statistics-comparison.json"))?;
    assert!(
        statistics_comparison["timers"]["all_blocks"]["wallet_ticker_mean_per_block"]
            ["rocksdb.number.keys.read"]["baseline"]
            .as_f64()
            > Some(0.0)
    );
    Ok(())
}

#[tokio::test]
async fn tip_replay_comparison_refuses_other_summary_methods_and_lists_page_cache_state()
-> Result<()> {
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

    let mut other_cache_state = read_json(&report)?;
    let measured_state = other_cache_state["method"]["scratch_page_cache"].clone();
    let other_state = if measured_state == "evicted-after-copy" {
        "not-evicted"
    } else {
        "evicted-after-copy"
    };
    other_cache_state["method"]["scratch_page_cache"] = Value::from(other_state);
    let other_path = inputs.directory.path().join("other-page-cache.json");
    std::fs::write(&other_path, serde_json::to_vec_pretty(&other_cache_state)?)?;
    let comparison_path = inputs.directory.path().join("page-cache-comparison.json");
    require_success(
        &compare_reports(&report, &other_path, &comparison_path)?,
        "page-cache comparison",
    )?;
    let comparison = read_json(&comparison_path)?;
    assert_eq!(
        comparison["setting_differences"],
        serde_json::json!([{
            "setting": "method.scratch_page_cache",
            "baseline": measured_state,
            "candidate": other_state,
        }])
    );
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

/// Newest block the integration reorg replaces, and how many blocks it replaces.
const REORG_TIP_HEIGHT: u32 = CHAIN_TIP_HEIGHT - 1;
const REORG_DEPTH: u32 = REORG_WINDOW_BLOCKS;

/// The replaced block that spends the coinbase of the block below it, which
/// the branch also replaces, and then that spend's output.
const SPENDING_HEIGHT: u32 = REORG_TIP_HEIGHT;

/// The transactions [`chain_spending_at`] adds at `SPENDING_HEIGHT`, the
/// coinbase they spend, and the outpoint each spend reads.
#[derive(Debug, Eq, PartialEq)]
struct SpendLinks {
    coinbase: TransactionId,
    spend: TransactionId,
    spend_input: TransparentOutPoint,
    descendant: TransactionId,
    descendant_input: TransparentOutPoint,
}

impl SpendLinks {
    /// The links in the fixture chain.
    fn of_chain(chain: &BTreeMap<BlockHeight, SourceBlock>) -> Result<Self> {
        let block_at = |height: u32| -> Result<Block> {
            let block = chain
                .get(&BlockHeight::new(height))
                .ok_or_else(|| eyre!("the chain is missing height {height}"))?;
            Ok(Block::zcash_deserialize(block.raw_block_bytes.as_slice())?)
        };
        let id = |transaction: &Transaction| TransactionId::from_bytes(transaction.hash().0);
        let input = |transaction: &Transaction| -> Result<TransparentOutPoint> {
            let outpoint = transaction
                .spent_outpoints()
                .next()
                .ok_or_else(|| eyre!("a spend must have an input"))?;
            Ok(TransparentOutPoint::new(
                TransactionId::from_bytes(outpoint.hash.0),
                outpoint.index,
            ))
        };
        let below = block_at(SPENDING_HEIGHT - 1)?;
        let spending = block_at(SPENDING_HEIGHT)?;
        let [coinbase] = &below.transactions[..] else {
            return Err(eyre!("the block below the spends holds only its coinbase"));
        };
        let [_, spend, descendant] = &spending.transactions[..] else {
            return Err(eyre!(
                "the spending block holds its coinbase and two spends"
            ));
        };
        Ok(Self {
            coinbase: id(coinbase),
            spend: id(spend),
            spend_input: input(spend)?,
            descendant: id(descendant),
            descendant_input: input(descendant)?,
        })
    }

    /// The links in a canonical store, read through a fresh secondary.
    fn of_store(inputs: &TipReplayInputs, canonical_store: &Path, name: &str) -> Result<Self> {
        let secondary = RocksDbCanonicalSecondary::open_ready(
            canonical_store,
            inputs
                .directory
                .path()
                .join(format!("{name}-links-secondary")),
            &inputs.activations,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(REORG_WINDOW_BLOCKS)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let facts_at = |height: u32| -> Result<zinder_core::CanonicalBlockFacts> {
            secondary
                .block_replay_facts_at(BlockHeight::new(height))?
                .ok_or_else(|| eyre!("the canonical store is missing height {height}"))
        };
        let below = facts_at(SPENDING_HEIGHT - 1)?;
        let spending = facts_at(SPENDING_HEIGHT)?;
        let [coinbase] = &below.transactions[..] else {
            return Err(eyre!("the block below the spends holds only its coinbase"));
        };
        let [_, spend, descendant] = &spending.transactions[..] else {
            return Err(eyre!(
                "the spending block holds its coinbase and two spends"
            ));
        };
        let input = |transaction: &zinder_core::CanonicalTransactionFacts| {
            transaction
                .transparent_inputs
                .first()
                .map(|input| input.spent_outpoint)
                .ok_or_else(|| eyre!("a spend must have an input"))
        };
        Ok(Self {
            coinbase: coinbase.public_facts.transaction_id,
            spend: spend.public_facts.transaction_id,
            spend_input: input(spend)?,
            descendant: descendant.public_facts.transaction_id,
            descendant_input: input(descendant)?,
        })
    }

    /// Asserts each spend reads output 0 of the transaction it spends.
    fn assert_linked(&self, name: &str) {
        assert_eq!(
            self.spend_input,
            TransparentOutPoint::new(self.coinbase, 0),
            "{name}"
        );
        assert_eq!(
            self.descendant_input,
            TransparentOutPoint::new(self.spend, 0),
            "{name}"
        );
    }
}

/// A raised reorg window that replaying to `REORG_TIP_HEIGHT` cannot fill.
///
/// The input store settled at `START_HEIGHT - REORG_WINDOW_BLOCKS`, and
/// settlement holds there until the tip passes it by the whole window.
const UNFILLED_REORG_WINDOW_BLOCKS: u32 = 3 * REORG_WINDOW_BLOCKS;

fn tip_reorg_report_path(inputs: &TipReplayInputs, name: &str) -> PathBuf {
    inputs.directory.path().join(format!("{name}.json"))
}

fn tip_reorg_command(
    inputs: &TipReplayInputs,
    name: &str,
    reorg_window_blocks: u32,
    extra: &[&str],
) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_zinder-bench"));
    command
        .arg("rocksdb-tip-reorg")
        .arg("--fixture")
        .arg(inputs.fixture())
        .arg("--canonical-store")
        .arg(inputs.canonical_store())
        .arg("--wallet-store")
        .arg(inputs.wallet_store())
        .arg("--scratch-dir")
        .arg(inputs.directory.path().join(format!("{name}-scratch")))
        .arg("--report")
        .arg(tip_reorg_report_path(inputs, name))
        .args(["--software-revision", "integration-test"])
        .args(["--reorg-tip-height", &REORG_TIP_HEIGHT.to_string()])
        .args(["--depth", &REORG_DEPTH.to_string()])
        .args(["--reorg-window-blocks", &reorg_window_blocks.to_string()])
        .args(["--canonical-block-cache-bytes", "8388608"])
        .args(["--canonical-secondary-block-cache-bytes", "8388608"])
        .args(["--wallet-block-cache-bytes", "8388608"])
        .args(extra);
    command
}

fn run_tip_reorg(inputs: &TipReplayInputs, name: &str, extra: &[&str]) -> Result<(Value, Output)> {
    let output = tip_reorg_command(inputs, name, REORG_WINDOW_BLOCKS, extra).output()?;
    require_success(&output, "tip reorg")?;
    Ok((read_json(&tip_reorg_report_path(inputs, name))?, output))
}

#[tokio::test]
async fn tip_reorg_switches_to_a_fork_and_matches_a_rebuilt_wallet() -> Result<()> {
    let chain = chain_spending_at(CHAIN_TIP_HEIGHT, Some(SPENDING_HEIGHT))?;
    let fixture_links = SpendLinks::of_chain(&chain)?;
    fixture_links.assert_linked("fixture");
    let inputs = prepare_inputs_for(chain).await?;
    let canonical_before = tree_digest(&inputs.canonical_store())?;
    let wallet_before = tree_digest(&inputs.wallet_store())?;
    // A distinct-coinbases branch renames every coinbase. The spend reaches a
    // renamed coinbase of an earlier block, and the descendant reaches the
    // renamed spend in its own block.
    for (fork, rewritten_coinbases, relinked_spends) in [
        ("same-transactions", 0, 0),
        ("distinct-coinbases", REORG_DEPTH + 1, 2),
    ] {
        let (report, _) = run_tip_reorg(&inputs, fork, &["--fork", fork, "--keep-scratch"])?;

        assert_eq!(report["contract_identity"], "rocksdb-tip-reorg");
        assert_eq!(
            report["catch_up"]["blocks"],
            REORG_TIP_HEIGHT - START_HEIGHT
        );
        assert_eq!(report["fork"]["kind"], fork);
        assert_eq!(report["fork"]["first_height"], REORG_TIP_HEIGHT - 1);
        assert_eq!(report["fork"]["rewrite"]["blocks"], REORG_DEPTH + 1);
        assert_eq!(report["fork"]["rewrite"]["coinbases"], rewritten_coinbases);
        assert_eq!(
            report["fork"]["rewrite"]["spending_transactions"],
            relinked_spends
        );
        assert_eq!(
            report["fork"]["rewrite"]["relinked_inputs"],
            relinked_spends
        );
        let replacement = &report["canonical_replacement"];
        assert_eq!(replacement["replacement_prepared_blocks"], REORG_DEPTH);
        assert_eq!(replacement["before"]["tip_height"], REORG_TIP_HEIGHT);
        assert_eq!(replacement["after"]["tip_height"], CHAIN_TIP_HEIGHT);
        assert_ne!(
            replacement["after"]["sequence_digest_hex"],
            replacement["before"]["sequence_digest_hex"]
        );

        let transition = &report["wallet_transition"];
        assert_eq!(transition["outcome"], "admitted");
        let footprint = &transition["footprint"];
        let accounted = footprint["accounted_logical_bytes"].as_u64();
        assert!(accounted > Some(0) && accounted <= transition["limit_bytes"].as_u64());
        assert_eq!(
            footprint["batch_records"].as_u64(),
            footprint["planned_rows"].as_u64().map(|rows| rows + 1)
        );
        assert!(transition["memory"]["peak_rss_bytes"].as_u64() > Some(0));
        assert_eq!(
            transition["final_wallet"]["source_tip_height"],
            CHAIN_TIP_HEIGHT
        );

        let kept_canonical = report["kept_scratch"]["canonical_store"]
            .as_str()
            .ok_or_else(|| eyre!("--keep-scratch must name the kept canonical store"))?;
        let branch_links = SpendLinks::of_store(&inputs, Path::new(kept_canonical), fork)?;
        branch_links.assert_linked(fork);
        if relinked_spends == 0 {
            assert_eq!(branch_links, fixture_links);
        } else {
            assert_ne!(branch_links.coinbase, fixture_links.coinbase);
            assert_ne!(branch_links.spend, fixture_links.spend);
            assert_ne!(branch_links.descendant, fixture_links.descendant);
        }
        let rebuilt = inputs.directory.path().join("rebuilt-wallet");
        if rebuilt.exists() {
            std::fs::remove_dir_all(&rebuilt)?;
        }
        assert_eq!(
            Some(rebuilt_wallet_digest(&inputs, Path::new(kept_canonical))?.as_str()),
            transition["final_wallet"]["projection_digest_hex"].as_str(),
            "{fork}"
        );
    }
    assert_eq!(tree_digest(&inputs.canonical_store())?, canonical_before);
    assert_eq!(tree_digest(&inputs.wallet_store())?, wallet_before);
    Ok(())
}

#[tokio::test]
async fn tip_reorg_reports_a_refused_transition_with_the_wallet_unchanged() -> Result<()> {
    let inputs = prepare_inputs().await?;
    let (admitted, _) = run_tip_reorg(
        &inputs,
        "admitted",
        &[
            "--wallet-io-mode",
            "buffered",
            "--wallet-rocksdb-statistics",
        ],
    )?;
    let configuration = &admitted["configuration"];
    assert_eq!(configuration["requested_io_modes"]["canonical"], "direct");
    assert_eq!(configuration["requested_io_modes"]["wallet"], "buffered");
    assert_eq!(admitted["io_modes"]["wallet"], "buffered");
    assert_eq!(configuration["wallet_statistics"]["collected"], true);
    let keys_read =
        admitted["wallet_transition"]["wallet_ticker_deltas"]["rocksdb.number.keys.read"].as_u64();
    assert!(keys_read > Some(0), "{keys_read:?}");
    let accounted = admitted["wallet_transition"]["footprint"]["accounted_logical_bytes"]
        .as_u64()
        .ok_or_else(|| eyre!("an admitted reorg reports its accounted bytes"))?;

    let limit = (accounted - 1).to_string();
    let (refused, _) = run_tip_reorg(
        &inputs,
        "refused",
        &["--max-transition-logical-bytes", &limit],
    )?;
    let transition = &refused["wallet_transition"];
    assert_eq!(transition["outcome"], "refused");
    assert_eq!(transition["limit_bytes"], accounted - 1);
    assert!(transition["refused_required_bytes"].as_u64() >= Some(accounted));
    assert_eq!(transition["wallet_unchanged"], true);
    assert!(transition["footprint"].is_null());
    assert!(transition["final_wallet"].is_null());
    assert!(transition["wallet_ticker_deltas"].is_null());
    assert_eq!(
        refused["configuration"]["wallet_statistics"]["collected"],
        false
    );
    assert_eq!(
        refused["configuration"]["requested_io_modes"]["wallet"],
        "direct"
    );
    Ok(())
}

#[tokio::test]
async fn tip_reorg_refuses_a_reorg_window_the_replay_has_not_filled() -> Result<()> {
    let inputs = prepare_inputs().await?;
    let output =
        tip_reorg_command(&inputs, "unfilled", UNFILLED_REORG_WINDOW_BLOCKS, &[]).output()?;

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    let settled = START_HEIGHT - REORG_WINDOW_BLOCKS;
    assert!(
        stderr.contains(&format!(
            "settled through {settled} under a {UNFILLED_REORG_WINDOW_BLOCKS}-block reorg window"
        )),
        "{stderr}"
    );
    assert!(
        stderr.contains(&format!(
            "raise --reorg-tip-height to at least {}",
            settled + UNFILLED_REORG_WINDOW_BLOCKS
        )),
        "{stderr}"
    );
    assert!(!tip_reorg_report_path(&inputs, "unfilled").exists());
    Ok(())
}
