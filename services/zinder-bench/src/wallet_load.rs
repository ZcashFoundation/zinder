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
#[cfg(test)]
mod mock_streamer;
#[cfg(test)]
mod pepper_sync_client;
mod point_reads;
mod server_metrics;
mod setup;
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
use transport::TlsOptions;
use zinder_bench::BenchError;
use zinder_proto::compat::lightwalletd::{CompactBlock, Empty};

const DEFAULT_WALLETS: u32 = 1;
const DEFAULT_TRANSACTIONS_PER_WALLET: u32 = 16;
const DEFAULT_POINT_READ_ROUNDS: u32 = 20;
const PROBE_DEADLINE: Duration = Duration::from_secs(30);

/// CLI contract for driving wallet-shaped load at one endpoint.
#[allow(
    clippy::struct_excessive_bools,
    reason = "each bool maps to an independent opt-in CLI flag"
)]
#[derive(Args)]
pub(crate) struct LightwalletdWalletLoadArgs {
    /// Endpoint URL, such as `http://127.0.0.1:9067` or `https://wallet.example:443`.
    #[arg(long)]
    endpoint: String,
    /// Optional PEM CA certificate file for `https://` endpoints with a
    /// private or local Caddy CA.
    #[arg(long = "tls-ca-cert")]
    tls_ca_cert: Option<PathBuf>,
    /// Name the `https://` endpoint's certificate and virtual host carry,
    /// sent as the TLS server name and the HTTP/2 `:authority` when the
    /// endpoint URL dials a loopback address or an IP.
    #[arg(long = "tls-domain-name")]
    tls_domain_name: Option<String>,
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
    /// Discover this many heights in `--from-height..=--to-height` with
    /// distinct canonical `TreeState` frontier bytes before the run and cycle
    /// `point-reads` wallets across them to bypass the server's frontier memo.
    #[arg(long = "distinct-frontiers", default_value_t = 0)]
    distinct_frontiers: u32,
    /// Before the run, record what `GetSubtreeRoots(max_entries = 0)`
    /// returns for Sapling, Orchard, and Ironwood, whether a root exists past
    /// the returned ones, and the tip's commitment tree sizes.
    #[arg(long = "verify-subtree-roots")]
    verify_subtree_roots: bool,
    /// Mined transaction ID, as the 64 hex characters a block explorer shows,
    /// to fetch via `GetTransaction` in `point-reads` rounds. Repeat for
    /// several txids.
    #[arg(long = "mined-txid")]
    mined_txids: Vec<String>,
    /// Pending mempool transaction ID, as the 64 hex characters a block
    /// explorer shows, to fetch via `GetTransaction` in `point-reads` rounds.
    /// Repeat for several txids.
    #[arg(long = "pending-txid")]
    pending_txids: Vec<String>,
    /// Discover up to this many mined txids near `--to-height` before the
    /// `point-reads` run.
    #[arg(long = "discover-mined-txids", default_value_t = 0)]
    discover_mined_txids: u32,
    /// Discover up to this many pending txids via `GetMempoolTx` before the
    /// `point-reads` run.
    #[arg(long = "discover-mempool-txids", default_value_t = 0)]
    discover_mempool_txids: u32,
    /// Synthetic missing-txid `GetTransaction` checks per `point-reads` round
    /// (expects `NOT_FOUND` without failing the wallet).
    #[arg(long = "missing-tx-checks", default_value_t = 0)]
    missing_tx_checks: u32,
    /// Transparent address whose history and unspent outputs `point-reads`
    /// wallets request. Repeat for several addresses.
    #[arg(long = "transparent-address")]
    transparent_addresses: Vec<String>,
    /// Include `GetTaddressTransactions` for each `--transparent-address` in
    /// `point-reads` rounds. Requires at least one `--transparent-address`.
    #[arg(long = "include-taddress-transactions")]
    include_taddress_transactions: bool,
    /// Include unary `GetTaddressBalance` across `--transparent-address`
    /// values in `point-reads` rounds. Requires at least one
    /// `--transparent-address`.
    #[arg(long = "include-taddress-balance")]
    include_taddress_balance: bool,
    /// Include unary `GetAddressUtxos` across `--transparent-address` values
    /// in `point-reads` rounds. Requires at least one `--transparent-address`.
    #[arg(long = "include-address-utxos-unary")]
    include_address_utxos_unary: bool,
    /// Prometheus endpoint of the Zinder server under test, such as
    /// `http://127.0.0.1:9107/metrics`. When set, the report counts the tree
    /// states the server rebuilt and the requests it refused during the run,
    /// from reads before and after the run and every second during it.
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

impl WalletWorkload {
    const fn report_format_version(self) -> u16 {
        match self {
            Self::PointReads => 2,
            Self::ClassicSdk | Self::Slipstream | Self::Vizor => 1,
        }
    }
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
    tip_before: setup::ObservedBlock,
    tip_after: ClosingTip,
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
    /// Blocks the `GetBlockRange` streams delivered over the run, the same
    /// count as `rpcs.GetBlockRange.messages`. It includes blocks received
    /// again on retry and blocks a wallet rejected as out of sequence or past
    /// its range, which the wallet never processes.
    blocks_received: u64,
    /// Protobuf-encoded size of those blocks, not bytes on the wire.
    bytes_received: u64,
    /// Protobuf-encoded bytes of every response the wallets received, without
    /// the endpoint probe before them.
    total_rpc_bytes: u64,
    /// Streams the wallets saw end early, fail mid-stream, or dropped after a
    /// message.
    incomplete_streams: u64,
    blocks_per_second: f64,
    bytes_per_second: f64,
    /// Requests a wallet sent again after a failure.
    retries: u64,
    /// From a wallet's start to its first block.
    time_to_first_block_seconds: Percentiles,
    /// From a wallet's start to its last call, over completed wallets.
    wallet_sync_seconds: Percentiles,
    /// SHA-256 of the sorted per-wallet request digests. Each wallet counts
    /// its `GetBlockRange`, `GetTreeState`, and `GetTransaction` attempts.
    wallet_request_identity_sha256: String,
    rpcs: BTreeMap<&'static str, RpcSummary>,
    tree_state_heights: TreeStateHeights,
    #[serde(skip_serializing_if = "Option::is_none")]
    subtree_roots_verification: Option<setup::SubtreeRootsObservation>,
    /// Why the server's metrics could not be read after the run, or why the
    /// two reads could not be compared, such as a counter that went down
    /// because the server restarted. It leaves `server_observed` out and
    /// keeps the client-side results.
    #[serde(skip_serializing_if = "Option::is_none")]
    server_metrics_error: Option<String>,
}

/// The endpoint's `GetLightdInfo` identity at the start of the run.
#[derive(Debug, Serialize)]
struct ServerIdentity {
    vendor: String,
    version: String,
    /// The lightwallet-protocol release the server advertises, empty for a
    /// server that predates the field. Setup reads it before it filters by
    /// pool.
    lightwallet_protocol_version: String,
    chain_name: String,
    block_height: u64,
    latest_block_height: u64,
}

/// A failed closing probe preserves the wallet results while leaving the tip unverified.
#[derive(Debug, Serialize)]
#[serde(untagged)]
enum ClosingTip {
    Block(setup::ObservedBlock),
    Unavailable { error: String },
}

impl ClosingTip {
    async fn observe(connector: &Connector) -> Self {
        let probe = async {
            let mut client = connector.connect().await?;
            setup::chain_tip(connector, &mut client).await
        };
        match probe.await {
            Ok(block) => Self::Block(block),
            Err(failure) => Self::Unavailable {
                error: format!("{}: {}", failure.label, failure.detail),
            },
        }
    }
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
    retries: u64,
    failure: Option<CallFailure>,
}

impl WalletOutcome {
    /// A wallet whose task panicked or was cancelled before it reported.
    fn task_failed(error: &tokio::task::JoinError) -> Self {
        Self {
            seconds: 0.0,
            first_block_seconds: None,
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

    /// Counts one block the wallet accepted and keeps its transaction IDs
    /// while the wallet still wants more.
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
            retries: tally.retries,
            failure: outcome.err(),
        }
    }
}

fn load_tls_options(args: &LightwalletdWalletLoadArgs) -> Result<TlsOptions, BenchError> {
    let ca_cert_pem = match &args.tls_ca_cert {
        Some(path) => Some(std::fs::read(path).map_err(|error| {
            BenchError::invalid_argument(format!("--tls-ca-cert {}: {error}", path.display()))
        })?),
        None => None,
    };
    Ok(TlsOptions {
        ca_cert_pem,
        domain_name: args.tls_domain_name.clone(),
    })
}

fn validate_args(args: &LightwalletdWalletLoadArgs) -> Result<(), BenchError> {
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
    let point_read_flags = [
        ("--distinct-frontiers", args.distinct_frontiers > 0),
        ("--mined-txid", !args.mined_txids.is_empty()),
        ("--pending-txid", !args.pending_txids.is_empty()),
        ("--discover-mined-txids", args.discover_mined_txids > 0),
        ("--discover-mempool-txids", args.discover_mempool_txids > 0),
        ("--missing-tx-checks", args.missing_tx_checks > 0),
        (
            "--include-taddress-transactions",
            args.include_taddress_transactions,
        ),
        ("--include-taddress-balance", args.include_taddress_balance),
        (
            "--include-address-utxos-unary",
            args.include_address_utxos_unary,
        ),
    ];
    if !matches!(args.workload, WalletWorkload::PointReads)
        && let Some((flag, _)) = point_read_flags.iter().find(|(_, set)| *set)
    {
        return Err(BenchError::invalid_argument(format!(
            "{flag} applies only to --workload point-reads"
        )));
    }
    let transparent_read_flags = [
        (
            "--include-taddress-transactions",
            args.include_taddress_transactions,
        ),
        ("--include-taddress-balance", args.include_taddress_balance),
        (
            "--include-address-utxos-unary",
            args.include_address_utxos_unary,
        ),
    ];
    if args.transparent_addresses.is_empty()
        && let Some((flag, _)) = transparent_read_flags.iter().find(|(_, set)| *set)
    {
        return Err(BenchError::invalid_argument(format!(
            "{flag} requires at least one --transparent-address"
        )));
    }
    Ok(())
}

/// Drives the chosen workload at the endpoint and reports what the wallets saw.
pub(crate) async fn run_lightwalletd_wallet_load(
    args: LightwalletdWalletLoadArgs,
) -> Result<LightwalletdWalletLoadOutput, BenchError> {
    validate_args(&args)?;
    let tls_options = load_tls_options(&args)?;
    let recorder = Arc::new(CallRecorder::default());
    let connector = Connector::new(&args.endpoint, Arc::clone(&recorder), &tls_options)
        .map_err(|reason| BenchError::invalid_argument(format!("--endpoint: {reason}")))?;
    let (server, plan, tip_before) = probe(&connector, &args).await?;
    let probe_totals = RecordedTotals::of(&recorder);
    let plan = Arc::new(plan);
    let setup_connector = connector.with_recorder(Arc::default());
    let setup = setup::prepare(
        &args,
        &setup_connector,
        &plan,
        &server.lightwallet_protocol_version,
    )
    .await?;
    let subtree_roots_verification = setup.subtree_roots_verification;
    let settings = Arc::new(WorkloadSettings::from_args(
        &args,
        setup.distinct_frontier_heights,
        setup.mined_txids,
        setup.pending_txids,
    ));
    let parameters = settings.parameters(args.workload, &plan);
    let observation = match &args.server_metrics_url {
        Some(url) => Some(
            server_metrics::RunObservation::start(url)
                .await
                .map_err(BenchError::wallet_load_endpoint)?,
        ),
        None => None,
    };
    let started = Instant::now();
    let outcomes = sync_wallets(&args, &connector, &plan, &settings).await;
    let wall = started.elapsed();
    let (server_observed, server_metrics_error) = match observation {
        Some(observation) => observation.finish().await,
        None => (None, None),
    };
    Ok(LightwalletdWalletLoadOutput {
        report: LightwalletdWalletLoadReport::new(
            &args,
            RunEvidence {
                server,
                tip_before,
                tip_after: ClosingTip::observe(&setup_connector).await,
                plan: &plan,
                parameters,
                wall,
                outcomes: &outcomes,
                recorder: &recorder,
                probe_totals,
                subtree_roots_verification,
                server_observed,
                server_metrics_error,
            },
        ),
        report_path: args.report,
    })
}

/// Settings each workload reads, fixed for the whole run, and the rotation
/// `point-reads` wallets share through them.
pub(super) struct WorkloadSettings {
    pub(super) transactions_per_wallet: usize,
    pub(super) split_bytes: u64,
    /// Bytes of Slipstream pieces that may wait on their tree states.
    pub(super) queue_budget_bytes: u64,
    pub(super) vizor_profile: vizor::VizorProfile,
    pub(super) batch_byte_budget: Option<u64>,
    pub(super) rounds: u32,
    pub(super) distinct_frontier_heights: Vec<u64>,
    pub(super) mined_txids: Vec<Vec<u8>>,
    pub(super) pending_txids: Vec<Vec<u8>>,
    pub(super) missing_tx_checks: u32,
    pub(super) transparent_addresses: Vec<String>,
    pub(super) include_taddress_transactions: bool,
    pub(super) include_taddress_balance: bool,
    pub(super) include_address_utxos_unary: bool,
    /// Where each cycled point-read list continues, so wallets started at
    /// different times still read every entry before any entry repeats.
    pub(super) read_rotation: point_reads::ReadRotation,
}

impl WorkloadSettings {
    fn from_args(
        args: &LightwalletdWalletLoadArgs,
        distinct_frontier_heights: Vec<u64>,
        mined_txids: Vec<Vec<u8>>,
        pending_txids: Vec<Vec<u8>>,
    ) -> Self {
        Self {
            transactions_per_wallet: usize::try_from(args.transactions_per_wallet)
                .unwrap_or(usize::MAX),
            split_bytes: args.split_bytes.max(1),
            queue_budget_bytes: slipstream::DEFAULT_QUEUE_BUDGET_BYTES,
            vizor_profile: args.vizor_profile,
            batch_byte_budget: args.batch_byte_budget,
            rounds: args.rounds,
            distinct_frontier_heights,
            mined_txids,
            pending_txids,
            missing_tx_checks: args.missing_tx_checks,
            transparent_addresses: args.transparent_addresses.clone(),
            include_taddress_transactions: args.include_taddress_transactions,
            include_taddress_balance: args.include_taddress_balance,
            include_address_utxos_unary: args.include_address_utxos_unary,
            read_rotation: point_reads::ReadRotation::default(),
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

async fn sync_wallets(
    args: &LightwalletdWalletLoadArgs,
    connector: &Connector,
    plan: &Arc<WalletPlan>,
    settings: &Arc<WorkloadSettings>,
) -> Vec<WalletOutcome> {
    let mut wallets = Vec::new();
    for wallet_index in 0..args.wallets {
        if wallet_index > 0 && args.wallet_start_interval_millis > 0 {
            tokio::time::sleep(Duration::from_millis(args.wallet_start_interval_millis)).await;
        }
        wallets.push(tokio::spawn(sync_wallet(
            args.workload,
            connector.for_wallet(),
            Arc::clone(plan),
            Arc::clone(settings),
            wallet_index,
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
    outcomes
}

async fn sync_wallet(
    workload: WalletWorkload,
    connector: Connector,
    plan: Arc<WalletPlan>,
    settings: Arc<WorkloadSettings>,
    wallet_index: u32,
) -> WalletOutcome {
    let progress = Arc::new(WalletProgress::new(settings.txid_limit(workload)));
    let outcome = match workload {
        WalletWorkload::Slipstream => {
            slipstream::sync_wallet(connector, plan, settings, Arc::clone(&progress)).await
        }
        WalletWorkload::Vizor => vizor::sync_wallet(connector, &plan, &settings, &progress).await,
        WalletWorkload::ClassicSdk => classic_sdk::sync_wallet(connector, &plan, &progress).await,
        WalletWorkload::PointReads => {
            point_reads::sync_wallet(connector, &plan, &settings, wallet_index).await
        }
    };
    progress.finish(outcome)
}

/// Reads the endpoint's identity and tip, as both ZODL apps do before they
/// pick a server, and fixes the run's height range.
async fn probe(
    connector: &Connector,
    args: &LightwalletdWalletLoadArgs,
) -> Result<(ServerIdentity, WalletPlan, setup::ObservedBlock), BenchError> {
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
    let latest_block = setup::chain_tip(connector, &mut client)
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
        lightwallet_protocol_version: lightd_info.lightwallet_protocol_version,
        chain_name: lightd_info.chain_name,
        block_height: lightd_info.block_height,
        latest_block_height: latest_block.height,
    };
    Ok((server, plan, latest_block))
}

/// What one run left behind, before it is summarized.
struct RunEvidence<'run> {
    server: ServerIdentity,
    tip_before: setup::ObservedBlock,
    tip_after: ClosingTip,
    plan: &'run WalletPlan,
    parameters: WorkloadParameters,
    wall: Duration,
    outcomes: &'run [WalletOutcome],
    recorder: &'run CallRecorder,
    probe_totals: RecordedTotals,
    subtree_roots_verification: Option<setup::SubtreeRootsObservation>,
    server_observed: Option<server_metrics::ServerObserved>,
    server_metrics_error: Option<String>,
}

/// Run-wide totals the recorder held at one point, so the probe's share can
/// be left out of the wallets' totals.
#[derive(Clone, Copy, Default)]
struct RecordedTotals {
    blocks: u64,
    block_bytes: u64,
    rpc_bytes: u64,
    incomplete_streams: u64,
}

impl RecordedTotals {
    fn of(recorder: &CallRecorder) -> Self {
        let (blocks, block_bytes) = recorder.block_range_totals();
        Self {
            blocks,
            block_bytes,
            rpc_bytes: recorder.total_rpc_bytes(),
            incomplete_streams: recorder.incomplete_streams(),
        }
    }

    /// What was recorded after `earlier`.
    fn since(self, earlier: Self) -> Self {
        Self {
            blocks: self.blocks.saturating_sub(earlier.blocks),
            block_bytes: self.block_bytes.saturating_sub(earlier.block_bytes),
            rpc_bytes: self.rpc_bytes.saturating_sub(earlier.rpc_bytes),
            incomplete_streams: self
                .incomplete_streams
                .saturating_sub(earlier.incomplete_streams),
        }
    }
}

impl LightwalletdWalletLoadReport {
    fn new(args: &LightwalletdWalletLoadArgs, run: RunEvidence<'_>) -> Self {
        let RunEvidence {
            server,
            tip_before,
            tip_after,
            plan,
            parameters,
            wall,
            outcomes,
            recorder,
            probe_totals,
            subtree_roots_verification,
            server_observed,
            server_metrics_error,
        } = run;
        let mut wallet_failures = BTreeMap::new();
        for failure in outcomes
            .iter()
            .filter_map(|outcome| outcome.failure.as_ref())
        {
            *wallet_failures.entry(failure.label).or_insert(0_u32) += 1;
        }
        let wallets_failed = wallet_failures.values().sum::<u32>();
        let RecordedTotals {
            blocks: blocks_received,
            block_bytes: bytes_received,
            rpc_bytes: total_rpc_bytes,
            incomplete_streams,
        } = RecordedTotals::of(recorder).since(probe_totals);
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
            report_format_version: args.workload.report_format_version(),
            workload: args.workload,
            endpoint: args.endpoint.clone(),
            target_label: args.target_label.clone(),
            server,
            tip_before,
            tip_after,
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
            total_rpc_bytes,
            incomplete_streams,
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
            wallet_request_identity_sha256: recorder.wallet_request_identity_sha256(),
            rpcs: recorder.rpc_summaries(),
            tree_state_heights: server_observed.map_or_else(
                || recorder.tree_state_heights(),
                |observed| recorder.tree_state_heights().with_server_observed(observed),
            ),
            subtree_roots_verification,
            server_metrics_error,
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
    use zinder_proto::compat::lightwalletd::{
        ChainMetadata, GetAddressUtxosReply, PoolType, RawTransaction, ShieldedProtocol,
        SubtreeRoot,
    };
    use zinder_query::{
        RocksDbCanonicalReader, RocksDbWalletReader, WalletServingPairSlot, WalletServingQuery,
        WalletServingReadPair,
    };
    use zinder_store::RawBlobRetention;
    use zinder_testkit::{
        CanonicalBaseline, CanonicalWriterFixture, ChainFixture, WalletServingStoreFixture,
        sample_regtest_upgrade_activations, with_note_commitments,
    };

    use super::{
        LightwalletdWalletLoadArgs, WalletWorkload,
        mock_streamer::{self, BlockChainMetadata, MockStreamer, RangeEnding},
        run_lightwalletd_wallet_load,
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
                Arc::new(RocksDbCanonicalReader::new(canonical)?),
                Arc::new(RocksDbWalletReader::new(wallet)),
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
            mock_args(&self.url, workload, wallets)
        }
    }

    fn mock_args(
        endpoint: &str,
        workload: WalletWorkload,
        wallets: u32,
    ) -> LightwalletdWalletLoadArgs {
        LightwalletdWalletLoadArgs {
            endpoint: endpoint.to_owned(),
            tls_ca_cert: None,
            tls_domain_name: None,
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
            distinct_frontiers: 0,
            verify_subtree_roots: false,
            mined_txids: Vec::new(),
            pending_txids: Vec::new(),
            discover_mined_txids: 0,
            discover_mempool_txids: 0,
            missing_tx_checks: 0,
            transparent_addresses: Vec::new(),
            include_taddress_transactions: false,
            include_taddress_balance: false,
            include_address_utxos_unary: false,
            server_metrics_url: None,
            target_label: Some("zinder-compat-lightwalletd test server".to_owned()),
            report: None,
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

    /// The status the lightwalletd surface returns when Zinder's replay limit
    /// refuses a tree-state rebuild. The limit is one reconstruction per CPU,
    /// so how often it refuses depends on the host.
    const REPLAY_REFUSAL: (&str, &str) = ("GetTreeState", "unavailable");

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
    async fn point_reads_report_requested_heights_by_cadence() -> TestResult {
        let server = CompatServer::start().await?;
        let mut args = server.args(WalletWorkload::PointReads, 1);
        args.rounds = 2;
        args.transparent_addresses = vec!["tmT1cVki2DufFCxicpR6UFwbdrKf8BWiFrn".to_owned()];

        let report = report_json(args).await?;

        assert_eq!(count(&report, "/wallets_completed")?, 1);
        // The probe adds one GetLightdInfo and one GetLatestBlock.
        assert_eq!(status(&report, "GetLightdInfo", "ok")?, 3);
        assert_eq!(status(&report, "GetLatestBlock", "ok")?, 3);
        assert_eq!(count(&report, "/report_format_version")?, 2);
        assert_eq!(count(&report, "/parameters/on_cadence_height")?, 100);
        assert_eq!(count(&report, "/parameters/off_cadence_height")?, 99);
        assert_eq!(
            status(&report, "GetTreeState(on cadence)", "ok")?
                + status(&report, "GetTreeState(off cadence)", "ok")?
                + status(&report, "GetTreeState(off cadence)", "unavailable")?,
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

    async fn report_with_closing_tip(
        tip: Result<Vec<u8>, tonic::Code>,
    ) -> Result<Value, Box<dyn Error>> {
        let mock = mock_streamer::serve(MockStreamer {
            tip_height: CHAIN_TIP,
            subsequent_tip: Some(tip),
            ..MockStreamer::new(RangeEnding::Clean)
        })
        .await?;
        report_json(mock_args(&mock.url, WalletWorkload::ClassicSdk, 1)).await
    }

    #[tokio::test]
    async fn same_height_tip_replacement_is_reported_outside_the_wallet_rpcs() -> TestResult {
        let report = report_with_closing_tip(Ok((0_u8..32).collect())).await?;
        assert_eq!(report["tip_before"]["height"], CHAIN_TIP);
        assert_eq!(
            report["tip_before"]["hash_hex"],
            "00000000000000fa".repeat(4)
        );
        assert_eq!(report["tip_after"]["height"], CHAIN_TIP);
        assert_eq!(
            report["tip_after"]["hash_hex"],
            "1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100"
        );
        assert_eq!(count(&report, "/rpcs/GetLatestBlock/requests")?, 1);
        assert_eq!(count(&report, "/wallets_completed")?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn unverified_closing_tips_preserve_the_wallet_report() -> TestResult {
        let missing = report_with_closing_tip(Ok(Vec::new())).await?;
        assert_eq!(missing["tip_after"]["hash_hex"], "");
        assert_eq!(count(&missing, "/wallets_completed")?, 1);

        let failed = report_with_closing_tip(Err(tonic::Code::Unavailable)).await?;
        assert!(failed["tip_after"].get("hash_hex").is_none());
        assert!(
            failed["tip_after"]["error"]
                .as_str()
                .is_some_and(|reason| reason.contains("tip probe failed")),
            "{failed}",
        );
        assert_eq!(count(&failed, "/wallets_completed")?, 1);
        assert_eq!(count(&failed, "/rpcs/GetLatestBlock/requests")?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn oversized_tip_hashes_fail_the_probe_and_preserve_completed_wallets() -> TestResult {
        let mock = mock_streamer::serve(MockStreamer {
            tip_height: CHAIN_TIP,
            subsequent_tip: Some(Ok(vec![0xab; 33])),
            ..MockStreamer::new(RangeEnding::Clean)
        })
        .await?;
        let args = || mock_args(&mock.url, WalletWorkload::ClassicSdk, 1);

        let report = report_json(args()).await?;
        let reason = "invalid_block_hash: block hash exceeds 32 bytes (received 33)";
        assert_eq!(report["tip_after"]["error"], reason);
        assert!(report["tip_after"].get("hash_hex").is_none());
        assert_eq!(count(&report, "/wallets_completed")?, 1);

        let error = report_json(args())
            .await
            .err()
            .ok_or("opening probe accepted an oversized hash")?;
        assert!(error.to_string().contains(reason), "{error}");
        Ok(())
    }

    /// A mock endpoint with the tip at 90, serving `PointReads` over 10..=90.
    async fn serve_point_reads(
        streamer: MockStreamer,
    ) -> Result<(mock_streamer::RunningMock, LightwalletdWalletLoadArgs), Box<dyn Error>> {
        let mock = mock_streamer::serve(MockStreamer {
            tip_height: 90,
            ..streamer
        })
        .await?;
        let mut args = mock_args(&mock.url, WalletWorkload::PointReads, 1);
        args.from_height = 10;
        args.to_height = Some(90);
        Ok((mock, args))
    }

    #[tokio::test]
    async fn distinct_frontier_discovery_skips_blocks_that_repeat_a_frontier() -> TestResult {
        // Blocks without shielded outputs share their predecessor's frontier,
        // so only these 5 heights advance it.
        let mut streamer = MockStreamer::new(RangeEnding::Clean);
        streamer.shielded_heights = [11_u64, 22, 33, 44, 55].into_iter().collect();
        let (_mock, mut args) = serve_point_reads(streamer).await?;
        args.rounds = 5;
        args.distinct_frontiers = 5;

        let report = report_json(args).await?;

        assert_eq!(count(&report, "/wallets_completed")?, 1);
        assert_eq!(status(&report, "GetTreeState(distinct frontier)", "ok")?, 5);
        assert_eq!(count(&report, "/tree_state_heights/distinct_frontiers")?, 5);
        assert_eq!(
            report["parameters"]["distinct_frontier_heights"],
            serde_json::json!([55, 44, 33, 22, 11])
        );
        Ok(())
    }

    #[tokio::test]
    async fn wallets_read_every_distinct_frontier_before_any_repeats() -> TestResult {
        let mut streamer = MockStreamer::new(RangeEnding::Clean);
        streamer.shielded_heights = [11_u64, 22, 33, 44, 55, 66].into_iter().collect();
        let (_mock, mut args) = serve_point_reads(streamer).await?;
        args.wallets = 3;
        args.rounds = 2;
        args.distinct_frontiers = 6;

        let report = report_json(args).await?;

        // 3 wallets times 2 rounds is 6 reads, one for each frontier.
        assert_eq!(count(&report, "/wallets_completed")?, 3);
        assert_eq!(status(&report, "GetTreeState(distinct frontier)", "ok")?, 6);
        assert_eq!(count(&report, "/tree_state_heights/distinct_frontiers")?, 6);
        assert_eq!(count(&report, "/tree_state_heights/distinct_heights")?, 6);
        Ok(())
    }

    /// Whether the first `distinct` of `reads` name `distinct` different
    /// entries.
    fn first_reads_are_distinct<Entry: Ord>(reads: &[Entry], distinct: usize) -> bool {
        reads
            .iter()
            .take(distinct)
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            == distinct
    }

    #[tokio::test]
    async fn staggered_wallets_read_every_entry_before_any_repeats() -> TestResult {
        let mut streamer = MockStreamer::new(RangeEnding::Clean);
        streamer.shielded_heights = [22_u64, 33, 44, 55].into_iter().collect();
        let pending = (1..=4_u8).map(|byte| vec![byte; 32]).collect::<Vec<_>>();
        streamer.mempool_txids.clone_from(&pending);
        let requests = Arc::clone(&streamer.requests);
        let (_mock, mut args) = serve_point_reads(streamer).await?;
        args.wallets = 2;
        args.rounds = 3;
        // The first wallet finishes its rounds before the second starts.
        args.wallet_start_interval_millis = 500;
        args.distinct_frontiers = 4;
        args.discover_mined_txids = 4;
        args.discover_mempool_txids = 4;

        let report = report_json(args).await?;

        assert_eq!(count(&report, "/wallets_completed")?, 2, "{report}");
        // Setup's discovery reads come first, then the wallets' 6 reads.
        let tree_states = requests.tree_state_heights.lock().clone();
        let wallet_reads = &tree_states[tree_states.len().saturating_sub(6)..];
        assert!(first_reads_are_distinct(wallet_reads, 4), "{tree_states:?}");
        let txids = requests.transaction_txids.lock().clone();
        let (pending_reads, mined_reads): (Vec<_>, Vec<_>) =
            txids.into_iter().partition(|txid| pending.contains(txid));
        assert_eq!((mined_reads.len(), pending_reads.len()), (6, 6));
        assert!(first_reads_are_distinct(&mined_reads, 4), "{mined_reads:?}");
        assert!(
            first_reads_are_distinct(&pending_reads, 4),
            "{pending_reads:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn distinct_frontier_discovery_includes_the_range_start() -> TestResult {
        // Height 10's frontier holds the output at 5, below the range, and
        // differs from height 11's, the range's only output.
        let mut streamer = MockStreamer::new(RangeEnding::Clean);
        streamer.shielded_heights = [5_u64, 11].into_iter().collect();
        let (_mock, mut args) = serve_point_reads(streamer).await?;
        args.to_height = Some(11);
        args.rounds = 2;
        args.distinct_frontiers = 2;

        let report = report_json(args).await?;

        assert_eq!(
            report["parameters"]["distinct_frontier_heights"],
            serde_json::json!([11, 10])
        );
        assert_eq!(count(&report, "/tree_state_heights/distinct_frontiers")?, 2);
        Ok(())
    }

    #[tokio::test]
    async fn a_server_without_pool_filtering_gets_no_pool_types() -> TestResult {
        let mut streamer = MockStreamer::new(RangeEnding::Clean);
        streamer.shielded_heights = [11_u64, 22].into_iter().collect();
        streamer.lightwallet_protocol_version = String::new();
        let requests = Arc::clone(&streamer.requests);
        let (_mock, mut args) = serve_point_reads(streamer).await?;
        args.rounds = 2;
        args.distinct_frontiers = 2;

        let report = report_json(args).await?;

        assert_eq!(
            report["parameters"]["distinct_frontier_heights"],
            serde_json::json!([22, 11])
        );
        assert_eq!(report["server"]["lightwallet_protocol_version"], "");
        let pool_types = requests.block_range_pool_types.lock().clone();
        // One discovery window covers the range.
        assert_eq!(pool_types, [Vec::<i32>::new()]);
        Ok(())
    }

    #[tokio::test]
    async fn transaction_discovery_is_refused_without_pool_filtering() -> TestResult {
        for version in ["v0.3.0", "v0.4.0garbage"] {
            for (mined, mempool, flag) in [
                (1, 0, "--discover-mined-txids"),
                (0, 1, "--discover-mempool-txids"),
            ] {
                let mut streamer = MockStreamer::new(RangeEnding::Clean);
                streamer.lightwallet_protocol_version = version.to_owned();
                let (_mock, mut args) = serve_point_reads(streamer).await?;
                args.discover_mined_txids = mined;
                args.discover_mempool_txids = mempool;

                let refused = run_lightwalletd_wallet_load(args).await.err();

                assert!(
                    refused.is_some_and(|error| {
                        let reason = error.to_string();
                        reason.contains(flag)
                            && reason.contains(&format!("lightwalletProtocolVersion `{version}`"))
                    }),
                    "{version} {flag}"
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn setup_never_names_ironwood_to_a_v0_4_server() -> TestResult {
        let mut streamer = subtree_roots_streamer(None);
        streamer.lightwallet_protocol_version = "v0.4.1".to_owned();
        streamer.shielded_heights = [11_u64, 22].into_iter().collect();
        streamer.mempool_txids.push(vec![0xAA; 32]);
        let requests = Arc::clone(&streamer.requests);
        let (_mock, mut args) = serve_point_reads(streamer).await?;
        args.rounds = 1;
        args.distinct_frontiers = 2;
        args.discover_mined_txids = 1;
        args.discover_mempool_txids = 1;
        args.verify_subtree_roots = true;

        let report = report_json(args).await?;

        let shielded = [PoolType::Sapling as i32, PoolType::Orchard as i32];
        let every_pool = [
            PoolType::Transparent as i32,
            PoolType::Sapling as i32,
            PoolType::Orchard as i32,
        ];
        // The frontier scan comes first, then the mined-transaction scan.
        assert_eq!(
            requests.block_range_pool_types.lock().as_slice(),
            [shielded.to_vec(), every_pool.to_vec()]
        );
        assert_eq!(
            requests.mempool_pool_types.lock().as_slice(),
            [every_pool.to_vec()]
        );
        assert!(
            !requests
                .subtree_root_protocols
                .lock()
                .contains(&(ShieldedProtocol::Ironwood as i32))
        );
        let verification = &report["subtree_roots_verification"];
        assert!(verification.get("ironwood").is_none(), "{verification}");
        assert_eq!(verification["sapling"]["returned_with_zero_max"], 5);
        Ok(())
    }

    #[tokio::test]
    async fn named_and_discovered_txids_are_read_and_counted_once() -> TestResult {
        let txid = mock_streamer::transparent_txid(60);
        let rpc_hex = hex::encode(txid.iter().rev().copied().collect::<Vec<_>>());
        let mut streamer = MockStreamer::new(RangeEnding::Clean);
        streamer.transparent_heights = std::iter::once(60_u64).collect();
        streamer.transactions.insert(
            txid,
            RawTransaction {
                data: vec![1, 2, 3],
                height: 60,
            },
        );
        let (_mock, mut args) = serve_point_reads(streamer).await?;
        args.rounds = 2;
        args.mined_txids = vec![rpc_hex.clone(), rpc_hex];
        args.discover_mined_txids = 1;

        let report = report_json(args).await?;

        assert_eq!(count(&report, "/parameters/mined_txids")?, 1);
        assert_eq!(status(&report, "GetTransaction(mined)", "ok")?, 2);
        Ok(())
    }

    #[tokio::test]
    async fn distinct_frontier_discovery_counts_ironwood_frontiers() -> TestResult {
        let mut streamer = MockStreamer::new(RangeEnding::Clean);
        streamer.shielded_heights = [11_u64, 22].into_iter().collect();
        streamer.ironwood_heights = [33_u64, 44].into_iter().collect();
        let (_mock, mut args) = serve_point_reads(streamer).await?;
        args.rounds = 4;
        args.distinct_frontiers = 4;

        let report = report_json(args).await?;

        assert_eq!(
            report["parameters"]["distinct_frontier_heights"],
            serde_json::json!([44, 33, 22, 11])
        );
        assert_eq!(count(&report, "/tree_state_heights/distinct_frontiers")?, 4);
        Ok(())
    }

    #[tokio::test]
    async fn mined_txid_discovery_finds_transparent_only_transactions() -> TestResult {
        // The newest transaction in the range touches only the transparent
        // pool, and it is the only one the server can return.
        let mut streamer = MockStreamer::new(RangeEnding::Clean);
        streamer.shielded_heights = std::iter::once(50_u64).collect();
        streamer.transparent_heights = std::iter::once(60_u64).collect();
        streamer.transactions.insert(
            mock_streamer::transparent_txid(60),
            RawTransaction {
                data: vec![1, 2, 3],
                height: 60,
            },
        );
        let (_mock, mut args) = serve_point_reads(streamer).await?;
        args.rounds = 2;
        args.discover_mined_txids = 1;

        let report = report_json(args).await?;

        assert_eq!(count(&report, "/parameters/mined_txids")?, 1);
        assert_eq!(status(&report, "GetTransaction(mined)", "ok")?, 2);
        Ok(())
    }

    #[tokio::test]
    async fn operator_txids_are_rpc_hex_and_answers_report_where_the_transaction_is() -> TestResult
    {
        // The server keys transactions by internal byte order, the reverse
        // of the RPC hex an operator copies from a block explorer.
        let internal = (1..=32_u8).collect::<Vec<_>>();
        let rpc_hex = hex::encode(internal.iter().rev().copied().collect::<Vec<_>>());
        let mut streamer = MockStreamer::new(RangeEnding::Clean);
        streamer.transactions.insert(
            internal,
            RawTransaction {
                data: vec![5, 6, 7],
                height: 70,
            },
        );
        let (_mock, mut args) = serve_point_reads(streamer).await?;
        args.rounds = 2;
        args.pending_txids = vec![rpc_hex];

        let report = report_json(args).await?;

        // The pending transaction was mined before the run read it.
        assert_eq!(
            status(&report, "GetTransaction(pending, answered mined)", "ok")?,
            2
        );
        assert!(report["rpcs"].get("GetTransaction(pending)").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn total_rpc_bytes_leave_out_the_endpoint_probe() -> TestResult {
        let (_mock, mut args) = serve_point_reads(MockStreamer::new(RangeEnding::Clean)).await?;
        args.rounds = 2;

        let report = report_json(args).await?;

        // The probe's GetLightdInfo and GetLatestBlock stay in `rpcs`, one
        // of each method's 3 equally sized responses.
        let bytes = |method: &str| count(&report, &format!("/rpcs/{method}/bytes"));
        let all_responses = report["rpcs"]
            .as_object()
            .ok_or("report has rpcs")?
            .values()
            .filter_map(|summary| summary["bytes"].as_u64())
            .sum::<u64>();
        let probe = bytes("GetLightdInfo")? / 3 + bytes("GetLatestBlock")? / 3;
        assert!(probe > 0);
        assert_eq!(count(&report, "/total_rpc_bytes")?, all_responses - probe);
        Ok(())
    }

    #[tokio::test]
    async fn transparent_reads_without_an_address_are_refused() {
        for flag in [
            "--include-taddress-transactions",
            "--include-taddress-balance",
            "--include-address-utxos-unary",
        ] {
            let mut args = mock_args("http://127.0.0.1:1", WalletWorkload::PointReads, 1);
            match flag {
                "--include-taddress-transactions" => args.include_taddress_transactions = true,
                "--include-taddress-balance" => args.include_taddress_balance = true,
                _ => args.include_address_utxos_unary = true,
            }

            let error = run_lightwalletd_wallet_load(args).await.err();

            assert!(
                error.is_some_and(|error| {
                    let message = error.to_string();
                    message.contains(flag) && message.contains("--transparent-address")
                }),
                "{flag}"
            );
        }
    }

    /// A mock whose Sapling pool holds 5 subtree roots, Orchard pool 3, and
    /// Ironwood pool 2, returning at most `zero_cap` of them when
    /// `max_entries` is 0.
    fn subtree_roots_streamer(zero_cap: Option<u32>) -> MockStreamer {
        let mut streamer = MockStreamer::new(RangeEnding::Clean);
        streamer.subtree_roots.insert(0, subtree_roots(5));
        streamer.subtree_roots.insert(1, subtree_roots(3));
        streamer.subtree_roots.insert(2, subtree_roots(2));
        streamer.subtree_root_zero_cap = zero_cap;
        streamer
    }

    /// Runs one point read against `streamer` with `--verify-subtree-roots`
    /// and returns the report's `subtree_roots_verification`.
    async fn subtree_roots_observation(streamer: MockStreamer) -> Result<Value, Box<dyn Error>> {
        let (_mock, mut args) = serve_point_reads(streamer).await?;
        args.rounds = 1;
        args.verify_subtree_roots = true;
        let report = report_json(args).await?;
        assert!(report["rpcs"].get("GetSubtreeRoots").is_none(), "{report}");
        Ok(report["subtree_roots_verification"].clone())
    }

    #[tokio::test]
    async fn subtree_roots_observation_reports_each_pool_and_the_tip() -> TestResult {
        let observed = subtree_roots_observation(subtree_roots_streamer(None)).await?;

        assert_eq!(observed["attempts"], 1, "{observed}");
        assert!(observed.get("verdict").is_none(), "{observed}");
        for (pool, roots) in [("sapling", 5), ("orchard", 3), ("ironwood", 2)] {
            let pool = &observed[pool];
            assert_eq!(pool["returned_with_zero_max"], roots, "{pool}");
            assert_eq!(pool["distinct_roots"], roots, "{pool}");
            assert_eq!(pool["repeated_root_indexes"], serde_json::json!([]));
            assert_eq!(pool["root_at_returned_index"], Value::Bool(false));
            assert_eq!(pool["completed_subtrees_at_tip"], roots, "{pool}");
        }
        assert_eq!(observed["tip_before"], observed["tip_after"]);
        assert_eq!(observed["chain_metadata_block"], observed["tip_before"]);
        assert_eq!(observed["chain_metadata_block"]["height"], 90);
        assert_eq!(observed["chain_metadata_block_is_tip"], Value::Bool(true));
        assert!(observed.get("chain_metadata_error").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn subtree_roots_observation_shows_a_root_past_a_silent_cap() -> TestResult {
        let observed = subtree_roots_observation(subtree_roots_streamer(Some(2))).await?;

        for pool in ["sapling", "orchard"] {
            assert_eq!(observed[pool]["returned_with_zero_max"], 2, "{pool}");
            assert_eq!(
                observed[pool]["root_at_returned_index"],
                Value::Bool(true),
                "{pool}"
            );
        }
        assert_eq!(observed["sapling"]["completed_subtrees_at_tip"], 5);
        // Ironwood holds exactly the cap, so no root follows.
        assert_eq!(
            observed["ironwood"]["root_at_returned_index"],
            Value::Bool(false)
        );
        Ok(())
    }

    #[tokio::test]
    async fn subtree_roots_observation_shows_an_empty_response_that_stopped_early() -> TestResult {
        let observed = subtree_roots_observation(subtree_roots_streamer(Some(0))).await?;

        for pool in ["sapling", "orchard", "ironwood"] {
            assert_eq!(observed[pool]["returned_with_zero_max"], 0, "{pool}");
            assert_eq!(
                observed[pool]["root_at_returned_index"],
                Value::Bool(true),
                "{pool}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn a_subtree_roots_failure_keeps_the_roots_that_arrived_before_it() -> TestResult {
        let mut streamer = subtree_roots_streamer(None);
        streamer.subtree_root_failure_after = Some(2);

        let observed = subtree_roots_observation(streamer).await?;

        let sapling = &observed["sapling"];
        assert_eq!(sapling["returned_with_zero_max"], 2);
        assert_eq!(sapling["zero_max_error"]["roots_before_error"], 2);
        assert!(
            sapling["zero_max_error"]["error"]
                .as_str()
                .is_some_and(|error| error.starts_with("internal")),
            "{observed}"
        );
        // The follow-up is not sent after a failed first request.
        assert_eq!(sapling["root_at_returned_index"], Value::Null);
        assert!(sapling.get("follow_up_error").is_none(), "{observed}");
        Ok(())
    }

    #[tokio::test]
    async fn repeated_subtree_roots_are_counted_once_and_their_indexes_named() -> TestResult {
        let mut roots = subtree_roots(3);
        roots.insert(2, roots[1].clone());
        let mut streamer = MockStreamer::new(RangeEnding::Clean);
        streamer.subtree_roots.insert(0, roots);

        let observed = subtree_roots_observation(streamer).await?;

        let sapling = &observed["sapling"];
        assert_eq!(sapling["returned_with_zero_max"], 4, "{observed}");
        assert_eq!(sapling["distinct_roots"], 3, "{observed}");
        assert_eq!(sapling["repeated_root_indexes"], serde_json::json!([2]));
        Ok(())
    }

    /// `count` stored subtree roots.
    fn subtree_roots(count: u8) -> Vec<SubtreeRoot> {
        (0..count)
            .map(|index| SubtreeRoot {
                root_hash: vec![index; 32],
                completing_block_hash: vec![0; 32],
                completing_block_height: 100 + u64::from(index),
            })
            .collect()
    }

    /// A mock that stores `sapling_roots` Sapling roots and no others, and
    /// whose blocks carry `chain_metadata`.
    fn sapling_roots_streamer(
        sapling_roots: u8,
        chain_metadata: BlockChainMetadata,
    ) -> MockStreamer {
        let mut streamer = MockStreamer::new(RangeEnding::Clean);
        streamer
            .subtree_roots
            .insert(0, subtree_roots(sapling_roots));
        streamer.block_chain_metadata = chain_metadata;
        streamer
    }

    #[tokio::test]
    async fn subtree_roots_observation_compares_with_the_tip_tree_size() -> TestResult {
        // One Sapling root, nothing after it, and a tip whose 131,072
        // Sapling commitments complete 2 subtrees.
        let streamer = sapling_roots_streamer(
            1,
            BlockChainMetadata::Sizes(ChainMetadata {
                sapling_commitment_tree_size: 131_072,
                ..ChainMetadata::default()
            }),
        );

        let observed = subtree_roots_observation(streamer).await?;

        let sapling = &observed["sapling"];
        assert_eq!(sapling["returned_with_zero_max"], 1);
        assert_eq!(sapling["completed_subtrees_at_tip"], 2);
        assert_eq!(sapling["root_at_returned_index"], Value::Bool(false));
        Ok(())
    }

    #[tokio::test]
    async fn subtree_roots_observation_without_chain_metadata_leaves_sizes_null() -> TestResult {
        let observed =
            subtree_roots_observation(sapling_roots_streamer(1, BlockChainMetadata::Absent))
                .await?;

        for pool in ["sapling", "orchard", "ironwood"] {
            assert_eq!(
                observed[pool]["completed_subtrees_at_tip"],
                Value::Null,
                "{pool}"
            );
        }
        assert_eq!(observed["chain_metadata_block_is_tip"], Value::Bool(true));
        Ok(())
    }

    #[tokio::test]
    async fn a_follow_up_root_before_a_failure_is_reported_with_the_failure() -> TestResult {
        let mut streamer = sapling_roots_streamer(3, BlockChainMetadata::Absent);
        streamer.subtree_root_zero_cap = Some(1);
        streamer.subtree_root_page_failure = true;

        let observed = subtree_roots_observation(streamer).await?;

        let sapling = &observed["sapling"];
        assert_eq!(sapling["root_at_returned_index"], Value::Bool(true));
        assert_eq!(sapling["follow_up_error"]["roots_before_error"], 1);
        assert!(
            sapling["follow_up_error"]["error"]
                .as_str()
                .is_some_and(|error| error.starts_with("internal")),
            "{observed}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn subtree_roots_observation_names_a_chain_metadata_block_off_the_tip() -> TestResult {
        let mut streamer = subtree_roots_streamer(None);
        streamer.block_hash = Some(vec![9; 32]);

        let observed = subtree_roots_observation(streamer).await?;

        assert_eq!(observed["chain_metadata_block"]["height"], 90);
        assert_eq!(
            observed["chain_metadata_block"]["hash_hex"],
            "09".repeat(32)
        );
        assert_eq!(observed["chain_metadata_block_is_tip"], Value::Bool(false));
        // The block's tree sizes describe another block, so no pool reports
        // a tip count.
        for pool in ["sapling", "orchard", "ironwood"] {
            assert_eq!(
                observed[pool]["completed_subtrees_at_tip"],
                Value::Null,
                "{pool}"
            );
        }
        assert_eq!(observed["sapling"]["returned_with_zero_max"], 5);
        Ok(())
    }

    #[tokio::test]
    async fn subtree_observation_rejects_an_oversized_metadata_hash() -> TestResult {
        let mut streamer = subtree_roots_streamer(None);
        streamer.block_hash = Some(vec![0xab; 33]);
        let observed = subtree_roots_observation(streamer).await?;

        assert_eq!(observed["chain_metadata_block"], Value::Null);
        assert_eq!(
            observed["chain_metadata_error"],
            "invalid_block_hash: block hash exceeds 32 bytes (received 33)"
        );
        assert_eq!(observed["chain_metadata_block_is_tip"], false);
        assert_eq!(
            observed["sapling"]["completed_subtrees_at_tip"],
            Value::Null
        );
        Ok(())
    }

    #[tokio::test]
    async fn subtree_roots_observation_does_not_treat_a_missing_block_hash_as_the_tip() -> TestResult
    {
        let mut streamer = subtree_roots_streamer(None);
        streamer.block_hash = Some(Vec::new());

        let observed = subtree_roots_observation(streamer).await?;

        assert_eq!(observed["chain_metadata_block"]["hash_hex"], "");
        assert_eq!(observed["chain_metadata_block_is_tip"], Value::Bool(false));
        assert_eq!(
            observed["sapling"]["completed_subtrees_at_tip"],
            Value::Null
        );
        Ok(())
    }

    #[tokio::test]
    async fn subtree_roots_observation_repeats_while_the_tip_moves() -> TestResult {
        // The probe reads the tip once. The first attempt's two reads see
        // different tips, and the second attempt's see the same one.
        let streamer = MockStreamer {
            tip_advancing_reads: 3,
            ..subtree_roots_streamer(Some(2))
        };

        let observed = subtree_roots_observation(streamer).await?;

        assert_eq!(observed["attempts"], 2);
        assert_eq!(observed["tip_before"], observed["tip_after"]);
        assert_eq!(observed["tip_after"]["height"], 93);
        Ok(())
    }

    #[tokio::test]
    async fn subtree_roots_observation_reports_the_last_attempt_when_the_tip_never_settles()
    -> TestResult {
        let streamer = MockStreamer {
            tip_advancing_reads: u64::MAX,
            ..subtree_roots_streamer(Some(2))
        };

        let observed = subtree_roots_observation(streamer).await?;

        assert_eq!(observed["attempts"], 3);
        assert_eq!(observed["tip_before"]["height"], 96);
        assert_eq!(observed["tip_after"]["height"], 97);
        Ok(())
    }

    #[tokio::test]
    async fn a_rolled_back_tip_block_is_retried_instead_of_aborting_the_run() -> TestResult {
        // The first attempt's `GetBlock` finds the block gone and the tip
        // moves, so the second attempt reads the new tip's block.
        let streamer = MockStreamer {
            tip_advancing_reads: 3,
            block_not_found_reads: 1,
            ..subtree_roots_streamer(None)
        };

        let observed = subtree_roots_observation(streamer).await?;

        assert_eq!(observed["attempts"], 2);
        assert!(observed.get("chain_metadata_error").is_none(), "{observed}");
        assert_eq!(observed["chain_metadata_block_is_tip"], Value::Bool(true));
        assert_eq!(observed["sapling"]["completed_subtrees_at_tip"], 5);
        Ok(())
    }

    #[tokio::test]
    async fn a_failed_tip_block_read_is_reported_beside_the_roots() -> TestResult {
        let streamer = MockStreamer {
            block_not_found_reads: u64::MAX,
            ..subtree_roots_streamer(None)
        };

        let observed = subtree_roots_observation(streamer).await?;

        assert!(
            observed["chain_metadata_error"]
                .as_str()
                .is_some_and(|error| error.starts_with("not_found")),
            "{observed}"
        );
        assert!(observed.get("chain_metadata_block").is_none(), "{observed}");
        assert_eq!(observed["chain_metadata_block_is_tip"], Value::Bool(false));
        assert_eq!(observed["sapling"]["returned_with_zero_max"], 5);
        assert_eq!(
            observed["sapling"]["completed_subtrees_at_tip"],
            Value::Null
        );
        Ok(())
    }

    #[tokio::test]
    async fn zinder_returns_every_subtree_root_the_tip_completes() -> TestResult {
        let server = CompatServer::start().await?;
        let mut args = server.args(WalletWorkload::PointReads, 1);
        args.rounds = 1;
        args.verify_subtree_roots = true;

        let report = report_json(args).await?;

        let observed = &report["subtree_roots_verification"];
        assert_eq!(observed["chain_metadata_block_is_tip"], Value::Bool(true));
        for pool in ["sapling", "orchard", "ironwood"] {
            let pool = &observed[pool];
            assert!(pool.get("zero_max_error").is_none(), "{observed}");
            assert!(pool.get("follow_up_error").is_none(), "{observed}");
            assert_eq!(pool["root_at_returned_index"], Value::Bool(false));
            assert_eq!(pool["distinct_roots"], pool["returned_with_zero_max"]);
            assert_eq!(
                pool["completed_subtrees_at_tip"], pool["returned_with_zero_max"],
                "{observed}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn point_reads_mix_mined_pending_and_missing_transactions_with_transparent_reads()
    -> TestResult {
        let pending_txid = vec![0xAA; 32];
        let mut streamer = MockStreamer::new(RangeEnding::Clean);
        streamer.shielded_heights = std::iter::once(50_u64).collect();
        streamer.transactions.insert(
            50_u64.to_le_bytes().to_vec(),
            RawTransaction {
                data: vec![1, 2, 3, 4],
                height: 50,
            },
        );
        streamer.transactions.insert(
            pending_txid.clone(),
            RawTransaction {
                data: vec![5, 6, 7],
                height: 0,
            },
        );
        streamer.mempool_txids.push(pending_txid);
        streamer.transparent_txs.push(RawTransaction {
            data: vec![9; 16],
            height: 45,
        });
        streamer.transparent_balance = 42_000;
        streamer.transparent_utxos.push(GetAddressUtxosReply {
            address: "t1test".to_owned(),
            txid: vec![0xBB; 32],
            index: 0,
            script: vec![1, 2],
            value_zat: 42_000,
            height: 45,
        });
        let (_mock, mut args) = serve_point_reads(streamer).await?;
        args.rounds = 2;
        args.discover_mined_txids = 1;
        args.discover_mempool_txids = 1;
        args.missing_tx_checks = 2;
        args.transparent_addresses = vec!["t1test".to_owned()];
        args.include_taddress_transactions = true;
        args.include_taddress_balance = true;
        args.include_address_utxos_unary = true;

        let report = report_json(args).await?;

        assert_eq!(count(&report, "/wallets_completed")?, 1);
        assert_eq!(count(&report, "/wallets_failed")?, 0);
        assert_eq!(status(&report, "GetTransaction(mined)", "ok")?, 2);
        assert_eq!(status(&report, "GetTransaction(pending)", "ok")?, 2);
        assert_eq!(status(&report, "GetTransaction(missing)", "not_found")?, 4);
        // An expected miss keeps its status and has a latency sample.
        assert_eq!(
            count(
                &report,
                "/rpcs/GetTransaction(missing)/expected_miss_seconds/count"
            )?,
            4
        );
        assert_eq!(status(&report, "GetTaddressTxids", "ok")?, 2);
        assert_eq!(status(&report, "GetTaddressTransactions", "ok")?, 2);
        assert_eq!(status(&report, "GetTaddressBalance", "ok")?, 2);
        assert_eq!(status(&report, "GetAddressUtxos", "ok")?, 2);
        assert_eq!(status(&report, "GetAddressUtxosStream", "ok")?, 2);
        assert!(count(&report, "/total_rpc_bytes")? > 0);
        Ok(())
    }

    #[tokio::test]
    async fn point_read_settings_are_refused_for_block_sync_workloads() {
        let mut args = mock_args("http://127.0.0.1:1", WalletWorkload::Slipstream, 1);
        args.missing_tx_checks = 1;

        let error = run_lightwalletd_wallet_load(args).await.err();

        assert!(
            error.is_some_and(|error| error.to_string().contains("--missing-tx-checks")),
            "the error names the flag"
        );
    }

    #[tokio::test]
    async fn new_point_read_settings_stay_off_by_default() -> TestResult {
        let (_mock, mut args) = serve_point_reads(MockStreamer::new(RangeEnding::Clean)).await?;
        args.rounds = 1;

        let report = report_json(args).await?;

        assert_eq!(count(&report, "/wallets_completed")?, 1);
        assert!(report.get("subtree_roots_verification").is_none());
        assert_eq!(count(&report, "/tree_state_heights/distinct_frontiers")?, 0);
        for method in [
            "GetTreeState(distinct frontier)",
            "GetTransaction(mined)",
            "GetTransaction(pending)",
            "GetTransaction(missing)",
            "GetTaddressBalance",
        ] {
            assert!(report["rpcs"].get(method).is_none(), "{method}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn blocks_received_count_blocks_the_wallet_rejected() -> TestResult {
        let streamer = MockStreamer {
            tip_height: 90,
            ..MockStreamer::new(RangeEnding::ExtraBlock)
        };
        let mock = mock_streamer::serve(streamer).await?;
        let mut args = mock_args(&mock.url, WalletWorkload::ClassicSdk, 1);
        args.from_height = 40;
        args.to_height = Some(90);

        let report = report_json(args).await?;

        assert!(status(&report, "GetBlockRange", "out_of_sequence")? >= 1);
        let blocks = count(&report, "/blocks_received")?;
        assert_eq!(blocks, count(&report, "/rpcs/GetBlockRange/messages")?);
        assert_eq!(
            count(&report, "/bytes_received")?,
            count(&report, "/rpcs/GetBlockRange/bytes")?
        );
        // Each stream sends the block past its range as well.
        assert!(blocks > 90 - 40 + 1, "{report}");
        Ok(())
    }

    #[tokio::test]
    async fn incomplete_streams_are_reported_at_top_level_and_per_rpc() -> TestResult {
        let streamer = MockStreamer {
            tip_height: 90,
            ..MockStreamer::new(RangeEnding::Status(tonic::Code::Unavailable))
        };
        let mock = mock_streamer::serve(streamer).await?;

        let mut args = mock_args(&mock.url, WalletWorkload::ClassicSdk, 1);
        args.from_height = 40;
        args.to_height = Some(90);

        let report = report_json(args).await?;

        assert_eq!(count(&report, "/wallets_failed")?, 1);
        assert!(count(&report, "/incomplete_streams")? >= 1);
        // The failed wallet asked for the tree at 39 and attempted 40..=90 five times.
        assert_eq!(
            report["wallet_request_identity_sha256"].as_str(),
            Some("354445829c3eadb408aebf4a433fd66dd872e0d3b65d6d5d5c650d2a7b0a7b9c")
        );
        assert_eq!(
            count(&report, "/incomplete_streams")?,
            count(&report, "/rpcs/GetBlockRange/incomplete_streams")?
        );
        Ok(())
    }
}
