//! Four fresh construction policies over one frozen block window.

use std::{
    net::{Ipv4Addr, SocketAddr},
    num::{NonZeroU32, NonZeroU64},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use clap::Args;
use futures_util::StreamExt;
use prost::Message;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::task::AbortOnDropHandle;
use zinder_bench::{
    BenchError,
    canonical_fixture_replay::{
        CanonicalFixtureRocksDbReplayConfig, TreeStateCheckpointCoverage,
        replay_canonical_fixture_into_rocksdb,
    },
    fixture::FixtureManifest,
};
use zinder_compat_lightwalletd::{CanonicalReaderSlot, LightwalletdGrpcAdapter};
use zinder_core::{BlockHeight, BlockId, Network, NetworkUpgradeActivations, UnixTimestampMillis};
use zinder_ingest::{CanonicalConstructionConfig, CanonicalPipelineLimits, load_fresh_canonical};
use zinder_proto::compat::lightwalletd::{
    self, compact_tx_streamer_client::CompactTxStreamerClient,
};
use zinder_query::{ForwardedReadLimit, RocksDbCanonicalReader};
use zinder_source::{NodeAuth, NodeSource, ZebraJsonRpcSource};
use zinder_store::{
    CanonicalBaselinePublication, CanonicalBlockLoadEvidence, CanonicalReorgPolicy,
    CanonicalStoreBuildPlan, CanonicalStoreRequirements, RawBlobRetention, RocksDbCanonicalBuilder,
    RocksDbCanonicalSecondary, RocksDbCanonicalStore, RocksDbResourceBudget, TreeStateLookup,
};

use crate::{
    build_provenance::BuildProvenance,
    rocksdb_tree_state_replay::{LatencySeconds, commitments_between},
};

const RETENTION: RawBlobRetention = RawBlobRetention::None;
const TIMEOUT: Duration = Duration::from_secs(30);
const MAINNET_FIRST: u32 = 1_730_000;
const MAINNET_LAST: u32 = 1_731_999;
const MAINNET_TIP: &str = "0000000000044fd6a927be08a6f994f336ceea30d0438a47625af5095a74ea19";
const MAINNET_PREDECESSOR: &str =
    "0000000001f274ffb7f4c5d25fe3418ec172e1179c5d8064dd35c82caf1d9bdd";

#[derive(Args)]
pub(crate) struct CheckpointPolicyComparisonArgs {
    /// Directory containing the repository fixture and replay plan.
    #[arg(
        long,
        required_unless_present = "frozen_mainnet_rpc",
        conflicts_with = "frozen_mainnet_rpc"
    )]
    fixture: Option<PathBuf>,
    /// Loopback RPC endpoint serving the hash-verified Mainnet campaign archive.
    #[arg(long)]
    frozen_mainnet_rpc: Option<SocketAddr>,
    /// Existing parent directory for temporary comparison stores.
    #[arg(long)]
    pub(crate) scratch_directory: PathBuf,
    /// Write the comparison report to this new file instead of stdout.
    #[arg(long)]
    pub(crate) report: Option<PathBuf>,
}

#[derive(Serialize)]
pub(crate) struct CheckpointPolicyComparisonReport {
    contract_identity: &'static str,
    report_format_version: u32,
    build: BuildProvenance,
    network: &'static str,
    raw_blob_retention: RawBlobRetention,
    fixture_manifest_sha256: Option<String>,
    first_height: u32,
    last_height: u32,
    tip_hash: String,
    canonical_sequence_sha256: String,
    sampled_heights: Vec<u32>,
    response_digest_sha256: String,
    byte_equivalent: bool,
    arms: Vec<Arm>,
}

#[derive(Serialize)]
struct Arm {
    coverage: TreeStateCheckpointCoverage,
    commitment_budget: Option<NonZeroU32>,
    ready_seconds: f64,
    reopen_seconds: f64,
    checkpoint_rows: u64,
    checkpoint_logical_bytes: u64,
    checkpoint_sst_bytes: u64,
    max_replayed_blocks: u32,
    max_replayed_commitments: u64,
    stored_samples: usize,
    miss_samples: usize,
    miss_latency_seconds: Option<LatencySeconds>,
}

struct Build {
    blocks: CanonicalBlockLoadEvidence,
    coverage: TreeStateCheckpointCoverage,
    budget: Option<NonZeroU32>,
    ready: Duration,
    reopen: Duration,
}

enum Input {
    Fixture {
        path: PathBuf,
        manifest_sha256: String,
    },
    Mainnet(Box<ZebraJsonRpcSource>),
}

struct Window {
    input: Input,
    activations: Arc<NetworkUpgradeActivations>,
    first: u32,
    last: u32,
}

impl Window {
    async fn admit(args: &CheckpointPolicyComparisonArgs) -> Result<Self, BenchError> {
        if let Some(path) = &args.fixture {
            let manifest = FixtureManifest::read(path)?;
            return Ok(Self {
                activations: Arc::new(manifest.activations_typed()?),
                first: manifest.from_height,
                last: manifest.to_height,
                input: Input::Fixture {
                    path: path.clone(),
                    manifest_sha256: manifest.digest_sha256()?,
                },
            });
        }
        let address = args.frozen_mainnet_rpc.ok_or_else(|| {
            BenchError::invalid_argument("expected --fixture or --frozen-mainnet-rpc")
        })?;
        if !address.ip().is_loopback() {
            return Err(BenchError::invalid_argument(
                "--frozen-mainnet-rpc must be a loopback address",
            ));
        }
        let source = ZebraJsonRpcSource::new(
            Network::ZcashMainnet,
            format!("http://{address}"),
            NodeAuth::None,
            TIMEOUT,
        )?;
        let activations = zinder_source::discover_network_upgrade_activations(
            &source,
            "checkpoint-policy-comparison",
        )
        .await?;
        Ok(Self {
            input: Input::Mainnet(Box::new(source)),
            activations,
            first: MAINNET_FIRST,
            last: MAINNET_LAST,
        })
    }

    fn budgets(&self) -> [NonZeroU32; 2] {
        match self.input {
            Input::Fixture { .. } => [NonZeroU32::MIN, nonzero(3)],
            Input::Mainnet(_) => [nonzero(256), nonzero(4096)],
        }
    }

    fn sampled_heights(&self) -> Vec<u32> {
        let stride = match self.input {
            Input::Fixture { .. } => 1,
            Input::Mainnet(_) => 17,
        };
        let mut heights: Vec<_> = (self.first..=self.last).step_by(stride).collect();
        if heights.last() != Some(&self.last) {
            heights.push(self.last);
        }
        heights
    }

    async fn build(
        &self,
        path: &Path,
        coverage: TreeStateCheckpointCoverage,
        budget: Option<NonZeroU32>,
    ) -> Result<Build, BenchError> {
        match &self.input {
            Input::Fixture { path: fixture, .. } => {
                let outcome =
                    replay_canonical_fixture_into_rocksdb(CanonicalFixtureRocksDbReplayConfig {
                        fixture_directory: fixture.clone(),
                        canonical_store_path: path.to_path_buf(),
                        request_timeout: TIMEOUT,
                        pipeline_limits: pipeline_limits(),
                        resource_budget: RocksDbResourceBudget::for_local_tests(),
                        raw_blob_retention: RETENTION,
                        supported_reorg_depth: 1,
                        source_segment_delay: Duration::ZERO,
                        tree_state_checkpoint_coverage: coverage,
                        tree_state_commitment_budget: budget,
                    })
                    .await?;
                Ok(Build {
                    blocks: outcome.block_load_evidence,
                    coverage: outcome.tree_state_checkpoint_coverage,
                    budget: outcome.tree_state_commitment_budget,
                    ready: outcome.ready_duration,
                    reopen: outcome.reopen_duration,
                })
            }
            Input::Mainnet(source) => self.build_mainnet(source, path, coverage, budget).await,
        }
    }

    async fn build_mainnet(
        &self,
        source: &ZebraJsonRpcSource,
        path: &Path,
        coverage: TreeStateCheckpointCoverage,
        budget: Option<NonZeroU32>,
    ) -> Result<Build, BenchError> {
        let predecessor = source
            .fetch_chain_checkpoint(BlockHeight::new(self.first - 1), &self.activations)
            .await?;
        let tip = source
            .fetch_chain_checkpoint(BlockHeight::new(self.last), &self.activations)
            .await?;
        if rpc_hash(predecessor.block_id) != MAINNET_PREDECESSOR
            || rpc_hash(tip.block_id) != MAINNET_TIP
        {
            return Err(BenchError::fixture_format(
                "Mainnet checkpoint identities differ from the recorded window",
            ));
        }
        let settled = source
            .fetch_block_at(BlockHeight::new(self.last - 1))
            .await?;
        let reorg = CanonicalReorgPolicy::new(1)?;
        let resources = RocksDbResourceBudget::for_local_tests();
        let plan = CanonicalStoreBuildPlan::checkpointed(
            &self.activations,
            predecessor,
            tip.block_id,
            RETENTION,
            reorg,
        )?;
        let started = Instant::now();
        let builder = RocksDbCanonicalBuilder::create_fresh(path, plan, resources)?;
        let builder = match coverage.resolve(budget)? {
            Some(selection) => builder.with_tree_state_checkpoint_selection(selection)?,
            None => builder,
        };
        let (coverage, budget) =
            TreeStateCheckpointCoverage::from_selection(builder.tree_state_checkpoint_selection())?;
        let loaded = load_fresh_canonical(
            builder,
            source,
            &CanonicalConstructionConfig {
                request_timeout: TIMEOUT,
                pipeline_limits: pipeline_limits(),
                network_upgrade_activations: self.activations.clone(),
            },
        )
        .await?;
        let validated = loaded.builder.prepare_trusted_fresh_publication()?;
        let prepared = validated.prepare_baseline(CanonicalBaselinePublication::new(
            BlockId::new(settled.height, settled.hash),
            UnixTimestampMillis::now(),
        ))?;
        let published = validated.publish_baseline(prepared)?;
        let fence = published.event_fence();
        let ready = started.elapsed();
        drop(published);

        let started = Instant::now();
        let reopened = RocksDbCanonicalStore::open_ready(
            path,
            &self.activations,
            RETENTION,
            reorg,
            resources,
        )?;
        let anchor = reopened.append_anchor()?;
        if anchor.event_fence() != fence || anchor.tip_checkpoint() != &tip {
            return Err(BenchError::fixture_format(
                "reopened Mainnet anchor differs from publication",
            ));
        }
        Ok(Build {
            blocks: loaded.block_evidence,
            coverage,
            budget,
            ready,
            reopen: started.elapsed(),
        })
    }
}

fn nonzero(number: u32) -> NonZeroU32 {
    NonZeroU32::new(number).unwrap_or(NonZeroU32::MIN)
}

fn rpc_hash(block: BlockId) -> String {
    zinder_core::wire::encode_rpc_block_hash_hex(block.hash)
}

fn pipeline_limits() -> CanonicalPipelineLimits {
    CanonicalPipelineLimits::resolve(
        None,
        nonzero(4),
        NonZeroU64::new(16 * 1024 * 1024).unwrap_or(NonZeroU64::MIN),
    )
}

pub(crate) async fn run(
    args: &CheckpointPolicyComparisonArgs,
) -> Result<CheckpointPolicyComparisonReport, BenchError> {
    use TreeStateCheckpointCoverage::{CadenceAndTip, CommitmentBudget, EveryBlock};

    let window = Window::admit(args).await?;
    let scratch = tempfile::tempdir_in(&args.scratch_directory)
        .map_err(|err| BenchError::io(&args.scratch_directory, err))?;
    let heights = window.sampled_heights();
    let [low, high] = window.budgets();
    let mut arms = Vec::new();
    let mut baseline = None;
    for (index, (coverage, budget)) in [
        (CadenceAndTip, None),
        (CommitmentBudget, Some(low)),
        (CommitmentBudget, Some(high)),
        (EveryBlock, None),
    ]
    .into_iter()
    .enumerate()
    {
        let path = scratch.path().join(format!("arm-{index}"));
        let build = window.build(&path, coverage, budget).await?;
        let observation = observe(&window, &heights, &path).await?;
        let identity = (build.blocks.sequence_digest, build.blocks.tip_hash);
        if let Some((expected_identity, expected_responses)) = &baseline {
            if identity != *expected_identity || observation.responses != *expected_responses {
                return Err(BenchError::report_format(format!(
                    "arm {index} differs from the sparse arm's canonical sequence or response bytes"
                )));
            }
        } else {
            baseline = Some((identity, observation.responses));
        }
        arms.push(Arm::new(&build, &observation.samples, observation.maxima)?);
    }
    let ((sequence, tip), responses) =
        baseline.ok_or_else(|| BenchError::report_format("missing comparison baseline"))?;
    let mut digest = Sha256::new();
    for response in responses {
        digest.update((response.len() as u64).to_le_bytes());
        digest.update(response);
    }
    Ok(CheckpointPolicyComparisonReport {
        contract_identity: "rocksdb-checkpoint-policy-comparison",
        report_format_version: 1,
        build: BuildProvenance::of_this_binary(),
        network: zinder_core::wire::encode_zinder_native_chain_name(window.activations.network()),
        raw_blob_retention: RETENTION,
        fixture_manifest_sha256: match &window.input {
            Input::Fixture {
                manifest_sha256, ..
            } => Some(manifest_sha256.clone()),
            Input::Mainnet(_) => None,
        },
        first_height: window.first,
        last_height: window.last,
        tip_hash: zinder_core::wire::encode_rpc_block_hash_hex(tip),
        canonical_sequence_sha256: hex::encode(sequence.as_bytes()),
        sampled_heights: heights,
        response_digest_sha256: hex::encode(digest.finalize()),
        byte_equivalent: true,
        arms,
    })
}

impl Arm {
    fn new(build: &Build, samples: &[Option<f64>], maxima: (u32, u64)) -> Result<Self, BenchError> {
        let (max_replayed_blocks, max_replayed_commitments) = maxima;
        if build
            .budget
            .is_some_and(|budget| max_replayed_commitments >= u64::from(budget.get()))
        {
            return Err(BenchError::report_format(
                "observed replay meets or exceeds the commitment budget",
            ));
        }
        let miss_samples = samples.iter().flatten().count();
        Ok(Self {
            coverage: build.coverage,
            commitment_budget: build.budget,
            ready_seconds: build.ready.as_secs_f64(),
            reopen_seconds: build.reopen.as_secs_f64(),
            checkpoint_rows: build.blocks.tree_state_checkpoint_count,
            checkpoint_logical_bytes: build.blocks.tree_state_checkpoint_logical_bytes,
            checkpoint_sst_bytes: build.blocks.tree_state_checkpoint_sst_bytes,
            max_replayed_blocks,
            max_replayed_commitments,
            stored_samples: samples.len() - miss_samples,
            miss_samples,
            miss_latency_seconds: (miss_samples > 0)
                .then(|| LatencySeconds::of(samples.iter().flatten().copied())),
        })
    }
}

struct Observation {
    samples: Vec<Option<f64>>,
    responses: Vec<Vec<u8>>,
    maxima: (u32, u64),
}

async fn observe(window: &Window, heights: &[u32], path: &Path) -> Result<Observation, BenchError> {
    let secondary = tempfile::tempdir_in(
        path.parent()
            .ok_or_else(|| BenchError::invalid_argument("arm path must have a parent"))?,
    )
    .map_err(|err| BenchError::io(path, err))?;
    let store = Arc::new(RocksDbCanonicalSecondary::open_ready(
        path,
        secondary.path(),
        &window.activations,
        CanonicalStoreRequirements::for_blob_retention(RETENTION),
        CanonicalReorgPolicy::new(1)?,
        RocksDbResourceBudget::for_local_tests(),
    )?);
    let (mut client, _server) = connect(&store, window.activations.network()).await?;
    get_tree_state(&mut client, window.last).await?;
    let mut durations = Vec::with_capacity(heights.len());
    let mut responses = Vec::with_capacity(heights.len());
    for &height in heights {
        let started = Instant::now();
        let response = get_tree_state(&mut client, height).await?;
        durations.push(started.elapsed().as_secs_f64());
        responses.push(response.encode_to_vec());
    }
    if matches!(window.input, Input::Mainnet(_)) {
        check_pre_sapling_refusal(&mut client).await?;
    }
    let samples = heights
        .iter()
        .zip(durations)
        .map(|(&height, seconds)| {
            lookup(&store, &window.activations, height)
                .map(|outcome| matches!(outcome, TreeStateLookup::Replay(_)).then_some(seconds))
        })
        .collect::<Result<_, _>>()?;
    Ok(Observation {
        samples,
        responses,
        maxima: largest_replays(&store, window)?,
    })
}

type TreeClient = CompactTxStreamerClient<tonic::transport::Channel>;

async fn connect(
    store: &Arc<RocksDbCanonicalSecondary>,
    network: Network,
) -> Result<
    (
        TreeClient,
        AbortOnDropHandle<Result<(), tonic::transport::Error>>,
    ),
    BenchError,
> {
    let reader = RocksDbCanonicalReader::new(store.clone()).map_err(serving_error)?;
    // GetTreeState is entirely local. The node adapter does not forward these requests.
    let unused_node =
        ZebraJsonRpcSource::new(network, "http://127.0.0.1:1", NodeAuth::None, TIMEOUT)?;
    let adapter = LightwalletdGrpcAdapter::for_light_wallet(
        CanonicalReaderSlot::new(Arc::new(reader)),
        unused_node,
        TIMEOUT,
        NonZeroU32::MIN,
        ForwardedReadLimit::default(),
    );
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .map_err(serving_error)?;
    let address = listener.local_addr().map_err(serving_error)?;
    let incoming = TcpListenerStream::new(listener).map(|stream| {
        let stream = stream?;
        stream.set_nodelay(true)?;
        Ok::<_, std::io::Error>(stream)
    });
    let server = AbortOnDropHandle::new(tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(adapter.into_server())
            .serve_with_incoming(incoming),
    ));
    let client = CompactTxStreamerClient::connect(format!("http://{address}"))
        .await
        .map_err(serving_error)?;
    Ok((client, server))
}

async fn get_tree_state(
    client: &mut TreeClient,
    height: u32,
) -> Result<lightwalletd::TreeState, BenchError> {
    let mut request = tonic::Request::new(lightwalletd::BlockId {
        height: u64::from(height),
        hash: Vec::new(),
    });
    request.set_timeout(TIMEOUT);
    Ok(client
        .get_tree_state(request)
        .await
        .map_err(serving_error)?
        .into_inner())
}

async fn check_pre_sapling_refusal(client: &mut TreeClient) -> Result<(), BenchError> {
    let genesis = Network::ZcashMainnet.genesis_hash();
    let response = client
        .get_tree_state(lightwalletd::BlockId {
            height: 0,
            hash: zinder_core::wire::encode_rpc_block_hash_bytes(genesis).to_vec(),
        })
        .await;
    if let Err(status) = response
        && status.code() == tonic::Code::InvalidArgument
    {
        return Ok(());
    }
    Err(BenchError::report_format(
        "expected GetTreeState for pre-Sapling genesis to return INVALID_ARGUMENT",
    ))
}

fn serving_error(error: impl std::fmt::Display) -> BenchError {
    BenchError::report_format(format!("comparison tree-state request failed: {error}"))
}

fn lookup(
    store: &RocksDbCanonicalSecondary,
    activations: &NetworkUpgradeActivations,
    height: u32,
) -> Result<TreeStateLookup, BenchError> {
    store
        .tree_state_lookup(BlockHeight::new(height), activations)?
        .ok_or_else(|| BenchError::report_format(format!("missing tree state at {height}")))
}

fn largest_replays(
    store: &RocksDbCanonicalSecondary,
    window: &Window,
) -> Result<(u32, u64), BenchError> {
    let mut maxima = (0, 0);
    for height in window.first..=window.last {
        if let TreeStateLookup::Replay(replay) = lookup(store, &window.activations, height)? {
            let metadata = store
                .compact_block_at(BlockHeight::new(height))?
                .ok_or_else(|| {
                    BenchError::report_format(format!("missing compact block at {height}"))
                })?
                .chain_metadata();
            let sizes = zinder_core::ChainTipMetadata {
                sapling_commitment_tree_size: metadata.sapling_commitment_tree_size,
                orchard_commitment_tree_size: metadata.orchard_commitment_tree_size,
                ironwood_commitment_tree_size: metadata.ironwood_commitment_tree_size,
            };
            maxima.0 = maxima.0.max(replay.replayed_blocks());
            maxima.1 = maxima
                .1
                .max(commitments_between(replay.seed().tip_metadata(), sizes));
        }
    }
    Ok(maxima)
}
