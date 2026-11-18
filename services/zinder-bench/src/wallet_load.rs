//! Wallet-shaped load against a running lightwalletd-compatible endpoint.
//!
//! Each workload sends the requests one wallet engine sends while it syncs a
//! height range, for many simulated wallets at once, and reports what the
//! clients observed. The target can be Zinder's compatibility runtime, Go
//! lightwalletd, or any other `CompactTxStreamer` server, directly or through
//! a TLS proxy. The simulated wallets never scan, so each asks for its next
//! blocks as soon as the previous ones arrive: the load is the most a wallet
//! engine of that shape asks for, not an average.

mod calls;
mod classic_sdk;
mod point_reads;
mod server_metrics;
mod slipstream;
mod transport;
mod vizor;

use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use calls::{CallFailure, CallRecorder, Connector, Percentiles, RpcSummary, TreeStateHeights};
use clap::{Args, ValueEnum};
use parking_lot::Mutex;
use serde::Serialize;
use zinder_bench::BenchError;
use zinder_proto::compat::lightwalletd::{ChainSpec, CompactBlock, Empty};

const DEFAULT_WALLETS: u32 = 1;
const DEFAULT_TRANSACTIONS_PER_WALLET: u32 = 16;
const DEFAULT_POINT_READ_ROUNDS: u32 = 20;
const PROBE_DEADLINE: Duration = Duration::from_secs(30);

/// CLI contract for driving wallet-shaped load at one endpoint.
#[derive(Args)]
pub(crate) struct LightwalletdWalletLoadArgs {
    /// Endpoint URL, such as `http://127.0.0.1:9067` or `https://wallet.example:443`.
    #[arg(long)]
    endpoint: String,
    /// Wallet engine whose request pattern each simulated wallet follows.
    #[arg(long, value_enum)]
    workload: WalletWorkload,
    /// Simulated wallets syncing at once.
    #[arg(long, default_value_t = DEFAULT_WALLETS)]
    wallets: u32,
    /// First block each wallet syncs. A restored wallet's birthday is its
    /// checkpoint height plus one.
    #[arg(long = "from-height")]
    from_height: u64,
    /// Last block each wallet syncs. Defaults to the endpoint's latest block.
    #[arg(long = "to-height")]
    to_height: Option<u64>,
    /// Pause between starting one wallet and the next.
    #[arg(long = "wallet-start-interval-millis", default_value_t = 0)]
    wallet_start_interval_millis: u64,
    /// Transactions each Slipstream-shaped wallet fetches after its blocks,
    /// chosen from the blocks it received.
    #[arg(long = "transactions-per-wallet", default_value_t = DEFAULT_TRANSACTIONS_PER_WALLET)]
    transactions_per_wallet: u32,
    /// Slipstream piece size: the bytes after which a downloaded piece is
    /// handed on with its own tree-state request.
    #[arg(long = "split-bytes", default_value_t = slipstream::DEFAULT_SPLIT_BYTES)]
    split_bytes: u64,
    /// Vizor platform, which sets its batch byte budget and block cap.
    #[arg(long = "vizor-profile", value_enum, default_value_t = vizor::VizorProfile::Mobile)]
    vizor_profile: vizor::VizorProfile,
    /// Vizor batch byte budget, overriding the profile's.
    #[arg(long = "batch-byte-budget")]
    batch_byte_budget: Option<u64>,
    /// Rounds of point reads each `point-reads` wallet makes.
    #[arg(long, default_value_t = DEFAULT_POINT_READ_ROUNDS)]
    rounds: u32,
    /// Transparent address whose history and unspent outputs `point-reads`
    /// wallets request. Repeat for several addresses.
    #[arg(long = "transparent-address")]
    transparent_addresses: Vec<String>,
    /// Prometheus endpoint of the Zinder server under test, such as
    /// `http://127.0.0.1:9107/metrics`. When set, the report counts the tree
    /// states the server rebuilt and the requests it refused during the run.
    #[arg(long = "server-metrics-url")]
    server_metrics_url: Option<String>,
    /// Free-form label for the server under test, such as its name and version.
    #[arg(long = "target-label")]
    target_label: Option<String>,
    /// Write the JSON report to this path instead of stdout.
    #[arg(long)]
    report: Option<PathBuf>,
}

/// Wallet engine whose request pattern a simulated wallet follows.
#[derive(Clone, Copy, Debug, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum WalletWorkload {
    /// The Slipstream engine in the ZODL Android and iOS apps.
    Slipstream,
    /// The Vizor wallet's sync engine.
    Vizor,
    /// The classic Zcash Android SDK `CompactBlockProcessor`.
    ClassicSdk,
    /// Server info, tip, tree-state, and transparent-address reads, without
    /// block downloads.
    PointReads,
}

/// Report and optional output path produced by one load run.
pub(crate) struct LightwalletdWalletLoadOutput {
    pub(crate) report: LightwalletdWalletLoadReport,
    pub(crate) report_path: Option<PathBuf>,
}

/// What every simulated wallet observed during one run.
#[derive(Debug, Serialize)]
pub(crate) struct LightwalletdWalletLoadReport {
    contract_identity: &'static str,
    report_format_version: u16,
    workload: WalletWorkload,
    endpoint: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_label: Option<String>,
    server: ServerIdentity,
    from_height: u64,
    to_height: u64,
    wallets: u32,
    parameters: WorkloadParameters,
    wall_seconds: f64,
    wallets_completed: u32,
    wallets_failed: u32,
    /// Failed wallets by the label of the call that ended them, or
    /// `client_task_failed` for a wallet whose task panicked.
    wallet_failures: BTreeMap<&'static str, u32>,
    /// Blocks received over the run, including blocks received again on retry.
    blocks_received: u64,
    /// Protobuf-encoded size of those blocks, not bytes on the wire.
    bytes_received: u64,
    blocks_per_second: f64,
    bytes_per_second: f64,
    /// Requests a wallet sent again after a failure.
    retries: u64,
    /// From a wallet's start to its first block.
    time_to_first_block_seconds: Percentiles,
    /// From a wallet's start to its last call, over completed wallets.
    wallet_sync_seconds: Percentiles,
    rpcs: BTreeMap<&'static str, RpcSummary>,
    tree_state_heights: TreeStateHeights,
}

/// The endpoint's `GetLightdInfo` identity at the start of the run.
#[derive(Debug, Serialize)]
struct ServerIdentity {
    vendor: String,
    version: String,
    chain_name: String,
    block_height: u64,
    latest_block_height: u64,
}

/// Constants and settings the run's workload used.
#[derive(Debug, Serialize)]
#[serde(untagged)]
enum WorkloadParameters {
    Slipstream(slipstream::Parameters),
    Vizor(vizor::Parameters),
    ClassicSdk(classic_sdk::Parameters),
    PointReads(point_reads::Parameters),
}

/// Heights and chain facts every wallet of a run shares.
#[derive(Debug)]
pub(super) struct WalletPlan {
    pub(super) from_height: u64,
    pub(super) to_height: u64,
    /// Whether the endpoint serves Mainnet, where Vizor reads some tree
    /// states from its compiled table instead of the server.
    pub(super) mainnet: bool,
    pub(super) sapling_activation_height: u64,
}

/// One simulated wallet's counters, shared by its concurrent requests.
pub(super) struct WalletProgress {
    started: Instant,
    txid_limit: usize,
    tally: Mutex<WalletTally>,
}

#[derive(Default)]
struct WalletTally {
    first_block: Option<Duration>,
    blocks: u64,
    bytes: u64,
    retries: u64,
    txids: Vec<Vec<u8>>,
}

/// How one simulated wallet's sync ended.
struct WalletOutcome {
    seconds: f64,
    first_block_seconds: Option<f64>,
    blocks: u64,
    bytes: u64,
    retries: u64,
    failure: Option<CallFailure>,
}

impl WalletOutcome {
    /// A wallet whose task panicked or was cancelled before it reported.
    fn task_failed(error: &tokio::task::JoinError) -> Self {
        Self {
            seconds: 0.0,
            first_block_seconds: None,
            blocks: 0,
            bytes: 0,
            retries: 0,
            failure: Some(CallFailure {
                label: "client_task_failed",
                detail: error.to_string(),
            }),
        }
    }
}

impl WalletProgress {
    fn new(txid_limit: usize) -> Self {
        Self {
            started: Instant::now(),
            txid_limit,
            tally: Mutex::new(WalletTally::default()),
        }
    }

    /// Counts one received block and keeps its transaction IDs while the
    /// wallet still wants more.
    pub(super) fn note_block(&self, block: &CompactBlock, encoded_bytes: u64) {
        let mut tally = self.tally.lock();
        tally
            .first_block
            .get_or_insert_with(|| self.started.elapsed());
        tally.blocks = tally.blocks.saturating_add(1);
        tally.bytes = tally.bytes.saturating_add(encoded_bytes);
        let wanted = self.txid_limit.saturating_sub(tally.txids.len());
        tally
            .txids
            .extend(block.vtx.iter().take(wanted).map(|tx| tx.txid.clone()));
    }

    pub(super) fn note_retry(&self) {
        let mut tally = self.tally.lock();
        tally.retries = tally.retries.saturating_add(1);
    }

    /// Blocks and bytes received so far, for sizing the next request by
    /// density.
    pub(super) fn received(&self) -> (u64, u64) {
        let tally = self.tally.lock();
        (tally.blocks, tally.bytes)
    }

    pub(super) fn take_txids(&self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.tally.lock().txids)
    }

    fn finish(&self, outcome: Result<(), CallFailure>) -> WalletOutcome {
        let tally = self.tally.lock();
        WalletOutcome {
            seconds: self.started.elapsed().as_secs_f64(),
            first_block_seconds: tally.first_block.map(|elapsed| elapsed.as_secs_f64()),
            blocks: tally.blocks,
            bytes: tally.bytes,
            retries: tally.retries,
            failure: outcome.err(),
        }
    }
}

/// Drives the chosen workload at the endpoint and reports what the wallets saw.
pub(crate) async fn run_lightwalletd_wallet_load(
    args: LightwalletdWalletLoadArgs,
) -> Result<LightwalletdWalletLoadOutput, BenchError> {
    if args.wallets == 0 {
        return Err(BenchError::invalid_argument(
            "--wallets must be greater than zero",
        ));
    }
    if args.from_height == 0 {
        return Err(BenchError::invalid_argument(
            "--from-height must be greater than zero, because a wallet asks for the tree state below it",
        ));
    }
    let recorder = Arc::new(CallRecorder::default());
    let connector = Connector::new(&args.endpoint, Arc::clone(&recorder))
        .map_err(|reason| BenchError::invalid_argument(format!("--endpoint: {reason}")))?;
    let (server, plan) = probe(&connector, &args).await?;
    let plan = Arc::new(plan);
    let settings = Arc::new(WorkloadSettings::from_args(&args));
    let parameters = settings.parameters(args.workload, &plan);
    let metrics_before = match &args.server_metrics_url {
        Some(url) => Some(
            server_metrics::scrape(url)
                .await
                .map_err(BenchError::wallet_load_endpoint)?,
        ),
        None => None,
    };
    let started = Instant::now();
    let mut wallets = Vec::new();
    for wallet_index in 0..args.wallets {
        if wallet_index > 0 && args.wallet_start_interval_millis > 0 {
            tokio::time::sleep(Duration::from_millis(args.wallet_start_interval_millis)).await;
        }
        wallets.push(tokio::spawn(sync_wallet(
            args.workload,
            connector.clone(),
            Arc::clone(&plan),
            Arc::clone(&settings),
        )));
    }
    let mut outcomes = Vec::with_capacity(wallets.len());
    for wallet in wallets {
        outcomes.push(
            wallet
                .await
                .unwrap_or_else(|error| WalletOutcome::task_failed(&error)),
        );
    }
    let wall = started.elapsed();
    let server_observed = match (&args.server_metrics_url, metrics_before) {
        (Some(url), Some(before)) => {
            let after = server_metrics::scrape(url)
                .await
                .map_err(BenchError::wallet_load_endpoint)?;
            Some(server_metrics::ServerObserved::between(&before, &after))
        }
        _ => None,
    };
    Ok(LightwalletdWalletLoadOutput {
        report: LightwalletdWalletLoadReport::new(
            &args,
            RunEvidence {
                server,
                plan: &plan,
                parameters,
                wall,
                outcomes: &outcomes,
                recorder: &recorder,
                server_observed,
            },
        ),
        report_path: args.report,
    })
}

/// Settings each workload reads, fixed for the whole run.
pub(super) struct WorkloadSettings {
    pub(super) transactions_per_wallet: usize,
    pub(super) split_bytes: u64,
    pub(super) vizor_profile: vizor::VizorProfile,
    pub(super) batch_byte_budget: Option<u64>,
    pub(super) rounds: u32,
    pub(super) transparent_addresses: Vec<String>,
}

impl WorkloadSettings {
    fn from_args(args: &LightwalletdWalletLoadArgs) -> Self {
        Self {
            transactions_per_wallet: usize::try_from(args.transactions_per_wallet)
                .unwrap_or(usize::MAX),
            split_bytes: args.split_bytes.max(1),
            vizor_profile: args.vizor_profile,
            batch_byte_budget: args.batch_byte_budget,
            rounds: args.rounds,
            transparent_addresses: args.transparent_addresses.clone(),
        }
    }

    fn parameters(&self, workload: WalletWorkload, plan: &WalletPlan) -> WorkloadParameters {
        match workload {
            WalletWorkload::Slipstream => {
                WorkloadParameters::Slipstream(slipstream::Parameters::new(self))
            }
            WalletWorkload::Vizor => WorkloadParameters::Vizor(vizor::Parameters::new(self)),
            WalletWorkload::ClassicSdk => {
                WorkloadParameters::ClassicSdk(classic_sdk::Parameters::new())
            }
            WalletWorkload::PointReads => {
                WorkloadParameters::PointReads(point_reads::Parameters::new(self, plan))
            }
        }
    }

    fn txid_limit(&self, workload: WalletWorkload) -> usize {
        match workload {
            WalletWorkload::Slipstream => self.transactions_per_wallet,
            WalletWorkload::Vizor | WalletWorkload::ClassicSdk | WalletWorkload::PointReads => 0,
        }
    }
}

async fn sync_wallet(
    workload: WalletWorkload,
    connector: Connector,
    plan: Arc<WalletPlan>,
    settings: Arc<WorkloadSettings>,
) -> WalletOutcome {
    let progress = Arc::new(WalletProgress::new(settings.txid_limit(workload)));
    let outcome = match workload {
        WalletWorkload::Slipstream => {
            slipstream::sync_wallet(connector, plan, settings, Arc::clone(&progress)).await
        }
        WalletWorkload::Vizor => vizor::sync_wallet(connector, &plan, &settings, &progress).await,
        WalletWorkload::ClassicSdk => classic_sdk::sync_wallet(connector, &plan, &progress).await,
        WalletWorkload::PointReads => point_reads::sync_wallet(connector, &plan, &settings).await,
    };
    progress.finish(outcome)
}

/// Reads the endpoint's identity and tip, as both ZODL apps do before they
/// pick a server, and fixes the run's height range.
async fn probe(
    connector: &Connector,
    args: &LightwalletdWalletLoadArgs,
) -> Result<(ServerIdentity, WalletPlan), BenchError> {
    let unreachable = |failure: CallFailure| {
        BenchError::wallet_load_endpoint(format!(
            "probe failed with {}: {}",
            failure.label, failure.detail
        ))
    };
    let mut client = connector.connect().await.map_err(unreachable)?;
    let lightd_info = calls::unary(
        connector.recorder(),
        "GetLightdInfo",
        PROBE_DEADLINE,
        client.get_lightd_info(Empty {}),
    )
    .await
    .map_err(unreachable)?;
    let latest_block = calls::unary(
        connector.recorder(),
        "GetLatestBlock",
        PROBE_DEADLINE,
        client.get_latest_block(ChainSpec {}),
    )
    .await
    .map_err(unreachable)?;
    let to_height = args.to_height.unwrap_or(latest_block.height);
    if to_height < args.from_height {
        return Err(BenchError::invalid_argument(format!(
            "--to-height {to_height} is below --from-height {}",
            args.from_height
        )));
    }
    let plan = WalletPlan {
        from_height: args.from_height,
        to_height,
        mainnet: lightd_info.chain_name == "main",
        sapling_activation_height: lightd_info.sapling_activation_height,
    };
    let server = ServerIdentity {
        vendor: lightd_info.vendor,
        version: lightd_info.version,
        chain_name: lightd_info.chain_name,
        block_height: lightd_info.block_height,
        latest_block_height: latest_block.height,
    };
    Ok((server, plan))
}

/// What one run left behind, before it is summarized.
struct RunEvidence<'run> {
    server: ServerIdentity,
    plan: &'run WalletPlan,
    parameters: WorkloadParameters,
    wall: Duration,
    outcomes: &'run [WalletOutcome],
    recorder: &'run CallRecorder,
    server_observed: Option<server_metrics::ServerObserved>,
}

impl LightwalletdWalletLoadReport {
    fn new(args: &LightwalletdWalletLoadArgs, run: RunEvidence<'_>) -> Self {
        let RunEvidence {
            server,
            plan,
            parameters,
            wall,
            outcomes,
            recorder,
            server_observed,
        } = run;
        let mut wallet_failures = BTreeMap::new();
        for failure in outcomes
            .iter()
            .filter_map(|outcome| outcome.failure.as_ref())
        {
            *wallet_failures.entry(failure.label).or_insert(0_u32) += 1;
        }
        let wallets_failed = wallet_failures.values().sum::<u32>();
        let blocks_received = outcomes.iter().map(|outcome| outcome.blocks).sum::<u64>();
        let bytes_received = outcomes.iter().map(|outcome| outcome.bytes).sum::<u64>();
        let wall_seconds = wall.as_secs_f64();
        let per_second = |count: u64| {
            if wall_seconds > 0.0 {
                calls::count_as_f64(count) / wall_seconds
            } else {
                0.0
            }
        };
        Self {
            contract_identity: "lightwalletd-wallet-load",
            report_format_version: 1,
            workload: args.workload,
            endpoint: args.endpoint.clone(),
            target_label: args.target_label.clone(),
            server,
            from_height: plan.from_height,
            to_height: plan.to_height,
            wallets: args.wallets,
            parameters,
            wall_seconds,
            wallets_completed: args.wallets.saturating_sub(wallets_failed),
            wallets_failed,
            wallet_failures,
            blocks_received,
            bytes_received,
            blocks_per_second: per_second(blocks_received),
            bytes_per_second: per_second(bytes_received),
            retries: outcomes.iter().map(|outcome| outcome.retries).sum(),
            time_to_first_block_seconds: Percentiles::of(
                outcomes
                    .iter()
                    .filter_map(|outcome| outcome.first_block_seconds),
            ),
            wallet_sync_seconds: Percentiles::of(
                outcomes
                    .iter()
                    .filter(|outcome| outcome.failure.is_none())
                    .map(|outcome| outcome.seconds),
            ),
            rpcs: recorder.rpc_summaries(),
            tree_state_heights: server_observed.map_or_else(
                || recorder.tree_state_heights(),
                |observed| recorder.tree_state_heights().with_server_observed(observed),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc};

    use serde_json::Value;
    use tokio::{net::TcpListener, sync::oneshot};
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;
    use zinder_compat_lightwalletd::LightwalletdGrpcAdapter;
    use zinder_core::Network;
    use zinder_query::{WalletServingPairSlot, WalletServingQuery, WalletServingReadPair};
    use zinder_store::RawBlobRetention;
    use zinder_testkit::{
        CanonicalBaseline, CanonicalWriterFixture, ChainFixture, WalletServingStoreFixture,
        sample_regtest_upgrade_activations, with_note_commitments,
    };

    use super::{
        LightwalletdWalletLoadArgs, WalletWorkload, run_lightwalletd_wallet_load,
        vizor::VizorProfile,
    };

    type TestResult = Result<(), Box<dyn Error>>;

    const CHAIN_TIP: u64 = 250;
    /// One above a height off the 100-block cadence, like a restored
    /// wallet's birthday one above its bundled checkpoint.
    const BIRTHDAY: u64 = 37;

    /// Zinder's compatibility adapter serving a constructed Regtest store.
    struct CompatServer {
        url: String,
        shutdown: Option<oneshot::Sender<()>>,
        _store: WalletServingStoreFixture,
    }

    impl CompatServer {
        async fn start() -> Result<Self, Box<dyn Error>> {
            let activations = sample_regtest_upgrade_activations();
            let chain = with_note_commitments(
                &ChainFixture::new(Network::ZcashRegtest)
                    .with_raw_blob_retention(RawBlobRetention::Transactions)
                    .extend_blocks(250),
                &activations,
            )?;
            let mut store =
                CanonicalWriterFixture::publish(&CanonicalBaseline::new(&chain), &activations)?
                    .into_serving_store_fixture()?;
            let (canonical, wallet) = store.take_readers()?;
            let slot = WalletServingPairSlot::new(Arc::new(WalletServingReadPair::new(
                Arc::new(canonical),
                Arc::new(wallet),
            )?));
            let adapter =
                LightwalletdGrpcAdapter::new(WalletServingQuery::from_serving_pair_slot(slot));
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let url = format!("http://{}", listener.local_addr()?);
            let (shutdown, shutdown_rx) = oneshot::channel::<()>();
            tokio::spawn(
                Server::builder()
                    .add_service(adapter.into_server())
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                        let _ = shutdown_rx.await;
                    }),
            );
            Ok(Self {
                url,
                shutdown: Some(shutdown),
                _store: store,
            })
        }

        fn args(&self, workload: WalletWorkload, wallets: u32) -> LightwalletdWalletLoadArgs {
            LightwalletdWalletLoadArgs {
                endpoint: self.url.clone(),
                workload,
                wallets,
                from_height: BIRTHDAY,
                to_height: None,
                wallet_start_interval_millis: 0,
                transactions_per_wallet: 4,
                split_bytes: super::slipstream::DEFAULT_SPLIT_BYTES,
                vizor_profile: VizorProfile::Mobile,
                batch_byte_budget: None,
                rounds: 3,
                transparent_addresses: Vec::new(),
                server_metrics_url: None,
                target_label: Some("zinder-compat-lightwalletd test server".to_owned()),
                report: None,
            }
        }
    }

    impl Drop for CompatServer {
        fn drop(&mut self) {
            if let Some(shutdown) = self.shutdown.take() {
                let _ = shutdown.send(());
            }
        }
    }

    async fn report_json(args: LightwalletdWalletLoadArgs) -> Result<Value, Box<dyn Error>> {
        Ok(serde_json::to_value(
            run_lightwalletd_wallet_load(args).await?.report,
        )?)
    }

    /// Reads a count the report must contain.
    fn count(report: &Value, pointer: &str) -> Result<u64, Box<dyn Error>> {
        report
            .pointer(pointer)
            .and_then(Value::as_u64)
            .ok_or_else(|| format!("report has no count at {pointer}: {report}").into())
    }

    /// Reads how many requests of `method` ended with `status`, zero when
    /// none did. The method itself must appear in the report.
    fn status(report: &Value, method: &str, status: &str) -> Result<u64, Box<dyn Error>> {
        let statuses = report
            .pointer(&format!("/rpcs/{method}/statuses"))
            .and_then(Value::as_object)
            .ok_or_else(|| format!("report has no {method} statuses: {report}"))?;
        Ok(statuses.get(status).and_then(Value::as_u64).unwrap_or(0))
    }

    /// The tree-state refusal Zinder's replay limit returns. The limit is one
    /// reconstruction per CPU, so how often it refuses depends on the host.
    const REPLAY_REFUSAL: (&str, &str) = ("GetTreeState", "resource_exhausted");

    /// Asserts every wallet synced its whole range, and every request
    /// succeeded except those ending with a `tolerated` method and status.
    fn assert_clean_sync(
        report: &Value,
        wallets: u32,
        tolerated: &[(&str, &str)],
    ) -> Result<(), Box<dyn Error>> {
        let wallets = u64::from(wallets);
        assert_eq!(count(report, "/wallets_completed")?, wallets, "{report}");
        assert_eq!(count(report, "/wallets_failed")?, 0);
        assert_eq!(count(report, "/to_height")?, CHAIN_TIP);
        assert!(count(report, "/blocks_received")? >= (CHAIN_TIP - BIRTHDAY + 1) * wallets);
        let methods = report["rpcs"]
            .as_object()
            .ok_or_else(|| format!("report has no rpcs: {report}"))?;
        for method in methods.keys() {
            let mut accepted = status(report, method, "ok")?;
            for (tolerated_method, tolerated_status) in tolerated {
                if method == tolerated_method {
                    accepted += status(report, method, tolerated_status)?;
                }
            }
            assert_eq!(
                count(report, &format!("/rpcs/{method}/requests"))?,
                accepted,
                "{method} had a request that did not succeed: {}",
                report["rpcs"][method]
            );
        }
        assert_eq!(
            count(report, "/time_to_first_block_seconds/count")?,
            wallets
        );
        assert_eq!(count(report, "/server/latest_block_height")?, CHAIN_TIP);
        Ok(())
    }

    // These tests run few wallets with default sizes, so few tree states
    // need a replay slot at once. The wallets' own retries then absorb the
    // refusals even on a one-CPU host; denser tree-state traffic is what a
    // load run is for, not a unit test.
    const WALLETS: u32 = 2;

    #[tokio::test]
    async fn slipstream_shape_probes_then_streams_the_rest_in_one_chunk() -> TestResult {
        let server = CompatServer::start().await?;

        let report = report_json(server.args(WalletWorkload::Slipstream, WALLETS)).await?;

        assert_clean_sync(&report, WALLETS, &[REPLAY_REFUSAL])?;
        let wallets = u64::from(WALLETS);
        // The 64-block probe 37..=100, then one chunk sized by density that
        // reaches the tip.
        assert_eq!(status(&report, "GetBlockRange", "ok")?, 2 * wallets);
        // The seed at 36, and the probe's piece boundary at 100, a stored
        // checkpoint. The range's last piece asks for nothing.
        assert_eq!(status(&report, "GetTreeState", "ok")?, 2 * wallets);
        assert!(count(&report, "/tree_state_heights/on_cadence")? >= wallets);
        // Four transactions per wallet, from the blocks it received.
        assert_eq!(status(&report, "GetTransaction", "ok")?, 4 * wallets);
        assert_eq!(count(&report, "/parameters/fetch_streams")?, 4);
        Ok(())
    }

    #[tokio::test]
    async fn vizor_shape_asks_a_tree_state_with_each_batch() -> TestResult {
        let server = CompatServer::start().await?;

        let report = report_json(server.args(WalletWorkload::Vizor, WALLETS)).await?;

        // A refused tree state fails its batch, which cancels the paired
        // request and can abort batches queued behind it.
        assert_clean_sync(
            &report,
            WALLETS,
            &[
                REPLAY_REFUSAL,
                ("GetTreeState", "client_cancelled"),
                ("GetBlockRange", "client_cancelled"),
            ],
        )?;
        // Regtest is not Mainnet and the range starts above Sapling
        // activation, so every batch, retried or not, asks for its tree state
        // alongside its blocks: the first 10-block batch, then the rest.
        let ranges = count(&report, "/rpcs/GetBlockRange/requests")?;
        assert!(ranges >= 2 * u64::from(WALLETS), "{report}");
        assert_eq!(count(&report, "/rpcs/GetTreeState/requests")?, ranges);
        assert_eq!(count(&report, "/tree_state_heights/on_cadence")?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn classic_sdk_shape_downloads_one_batch_after_its_checkpoint_tree_state() -> TestResult {
        let server = CompatServer::start().await?;

        let report = report_json(server.args(WalletWorkload::ClassicSdk, WALLETS)).await?;

        assert_clean_sync(&report, WALLETS, &[REPLAY_REFUSAL])?;
        // The whole range fits one 1,000-block batch, after one tree state
        // 36 blocks above the checkpoint below it.
        let wallets = u64::from(WALLETS);
        assert_eq!(count(&report, "/rpcs/GetBlockRange/requests")?, wallets);
        assert_eq!(status(&report, "GetTreeState", "ok")?, wallets);
        assert_eq!(count(&report, "/tree_state_heights/on_cadence")?, 0);
        assert_eq!(
            report
                .pointer("/tree_state_heights/off_cadence_offset_blocks/max")
                .and_then(Value::as_f64),
            Some(36.0)
        );
        Ok(())
    }

    #[tokio::test]
    async fn point_reads_separate_stored_checkpoints_from_replays() -> TestResult {
        let server = CompatServer::start().await?;
        let mut args = server.args(WalletWorkload::PointReads, 1);
        args.rounds = 2;
        args.transparent_addresses = vec!["tmT1cVki2DufFCxicpR6UFwbdrKf8BWiFrn".to_owned()];

        let report = report_json(args).await?;

        assert_eq!(count(&report, "/wallets_completed")?, 1);
        // The probe adds one GetLightdInfo and one GetLatestBlock.
        assert_eq!(status(&report, "GetLightdInfo", "ok")?, 3);
        assert_eq!(status(&report, "GetLatestBlock", "ok")?, 3);
        // Below the tip of 250, the checkpoint at 100 and the height 99,
        // which replays from the checkpoint at 0.
        assert_eq!(count(&report, "/parameters/stored_checkpoint_height")?, 100);
        assert_eq!(count(&report, "/parameters/replay_height")?, 99);
        assert_eq!(
            status(&report, "GetTreeState(stored checkpoint)", "ok")?
                + status(&report, "GetTreeState(replay)", "ok")?
                + status(&report, "GetTreeState(replay)", "resource_exhausted")?,
            4
        );
        // An address with no history streams nothing, successfully.
        assert_eq!(status(&report, "GetTaddressTxids", "ok")?, 2);
        assert_eq!(count(&report, "/rpcs/GetTaddressTxids/messages")?, 0);
        assert_eq!(status(&report, "GetAddressUtxosStream", "ok")?, 2);
        Ok(())
    }

    #[tokio::test]
    async fn slipstream_shape_retries_a_failing_chunk_on_fresh_connections() -> TestResult {
        let server = CompatServer::start().await?;
        let mut args = server.args(WalletWorkload::Slipstream, 1);
        // The probe 37..=100 succeeds. The next chunk ends above the served
        // tip, so every request for it is refused with OUT_OF_RANGE.
        args.to_height = Some(CHAIN_TIP + 500);

        let report = report_json(args).await?;

        assert_eq!(count(&report, "/wallets_completed")?, 0);
        assert_eq!(count(&report, "/wallet_failures/out_of_range")?, 1);
        // The failing chunk's first request and its three retries.
        assert_eq!(count(&report, "/retries")?, 3, "{report}");
        assert_eq!(status(&report, "GetBlockRange", "ok")?, 1);
        assert_eq!(status(&report, "GetBlockRange", "out_of_range")?, 4);
        assert!(
            report
                .pointer("/rpcs/GetBlockRange/status_examples/out_of_range")
                .and_then(Value::as_str)
                .is_some_and(|message| !message.is_empty())
        );
        // The probe, the wallet's shared connection, the workers that took a
        // chunk, and a new connection for each retry. The probe's worker can
        // take the second chunk too, so one or two workers connect.
        let connects = count(&report, "/rpcs/connect/requests")?;
        assert!(
            (1 + 1 + 1 + 3..=1 + 1 + 2 + 3).contains(&connects),
            "{report}"
        );
        Ok(())
    }
}
