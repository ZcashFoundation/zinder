//! `ExplorerQuery` gRPC adapter.
//!
//! Serves [`ExplorerQuery::ServerInfo`] (advertising [`EXPLORER_SERVER_INFO_V1`])
//! and the materialized-view-backed explorer surfaces. Handlers that need canonical
//! wallet-plane reads (transaction detail, block views, search, mempool
//! activity, value pools) compose them through a `WalletQuery` channel.
//!
//! The adapter holds a single cached `WalletQuery` channel and reuses it
//! across requests. The first request that needs the channel pays the
//! handshake cost; later requests just clone the cached
//! [`AuthenticatedChannel`] (a `tonic` `Channel` is internally pooled and
//! clone-cheap) so the explorer never opens one HTTP/2 connection per
//! request.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::OnceCell;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request, Response, Status, service::interceptor::InterceptedService};
use zinder_core::{Network, NetworkUpgradeActivations, wire::encode_zinder_native_chain_name};
use zinder_proto::capabilities::{
    CapabilitySurface, EXPLORER_BLOCK_PRODUCTION_TIME_RANGE_V1,
    EXPLORER_CONVENTIONAL_FEE_DISTRIBUTION_V1, EXPLORER_PAID_FEE_DISTRIBUTION_V1,
    EXPLORER_SERVER_INFO_V1, EXPLORER_TRANSPARENT_ADDRESS_ACTIVITY_V2,
    EXPLORER_TRANSPARENT_ADDRESS_RANKING_V1, ExplorerReadiness, capabilities_for_surface,
};
use zinder_proto::v1::{
    explorer::{
        BlockActivityDistributionRequest, BlockActivityDistributionResponse, BlockDetailRequest,
        BlockDetailResponse, BlockProductionInTimeRangeRequest, BlockProductionInTimeRangeResponse,
        BlockProductionSeriesRequest, BlockProductionSeriesResponse, BlockSummariesInRangeRequest,
        BlockSummariesInRangeResponse, BlockTransactionsResponse, ChainReorgHistoryRequest,
        ChainReorgHistoryResponse, CommitmentRootSearchRequest, CommitmentRootSearchResponse,
        ConventionalFeeDistributionRequest, ConventionalFeeDistributionResponse,
        DisplacedBlockDetailRequest, DisplacedBlockDetailResponse, DisplacedBlockHistoryRequest,
        DisplacedBlockHistoryResponse, ExplorerServerInfo, FeeSummaryRequest, FeeSummaryResponse,
        MempoolActivityRequest, MempoolActivityResponse, MempoolEventCountsRequest,
        MempoolEventCountsResponse, MempoolSnapshotRequest, MempoolSnapshotResponse,
        MempoolSummaryRequest, MempoolSummaryResponse, MigrationCohortsRequest,
        MigrationCohortsResponse, MigrationDenominationsRequest, MigrationDenominationsResponse,
        MigrationOverviewRequest, MigrationOverviewResponse, NetworkUpgradeStatusRequest,
        NetworkUpgradeStatusResponse, OverviewSnapshotRequest, OverviewSnapshotResponse,
        PaidFeeDistributionRequest, PaidFeeDistributionResponse, RecentTransactionsRequest,
        SearchRequest, SearchResponse, ServerInfoRequest, ServerInfoResponse,
        TransactionComponentSummaryRequest, TransactionComponentSummaryResponse,
        TransactionDetailRequest, TransactionDetailResponse, TransactionHistoryRequest,
        TransactionHistoryResponse, TransparentAddressActivityRequest,
        TransparentAddressActivityResponse, TransparentAddressDeltasRequest,
        TransparentAddressDeltasResponse, TransparentAddressRankingRequest,
        TransparentAddressRankingResponse, ValuePoolBalanceHistoryRequest,
        ValuePoolBalanceHistoryResponse, ValuePoolFlowAmountThresholdSummaryRequest,
        ValuePoolFlowAmountThresholdSummaryResponse, ValuePoolFlowEventsInRangeRequest,
        ValuePoolFlowEventsInRangeResponse, ValuePoolFlowHistoryRequest,
        ValuePoolFlowHistoryResponse, ValuePoolFlowRoundedAmountSummaryRequest,
        ValuePoolFlowRoundedAmountSummaryResponse, ValuePoolFlowSummaryRequest,
        ValuePoolFlowSummaryResponse, ValuePoolSummaryRequest, ValuePoolSummaryResponse,
        explorer_query_server::{ExplorerQuery, ExplorerQueryServer},
    },
    ops,
    wallet::wallet_query_client::WalletQueryClient,
};
use zinder_runtime::{
    AuthenticatedChannel, BearerToken, BearerTokenConnectError, BearerTokenServerInterceptor,
    RpcMetricNames, RpcOutcome, connect_zinder_grpc, describe_rpc_metrics, record_rpc_request,
};
use zinder_source::NodeSource;

/// Metric pair the `ExplorerQuery` adapter emits per request.
const EXPLORER_RPC_METRICS: RpcMetricNames = RpcMetricNames::for_service(
    "zinder_explorer_request_duration_seconds",
    "zinder_explorer_request_total",
);

use super::block_activity::query_block_activity_distribution;
use super::block_view::{query_block_detail, query_block_summaries_in_range};
use super::chain_reorg_history::query_chain_reorg_history;
use super::conventional_fee_distribution::query_conventional_fee_distribution;
use super::error::ExplorerError;
use super::fee_summary::query_fee_summary;
use super::freshness::{
    UpstreamObservationCache, attach_upstream_observation, build_explorer_freshness,
    read_materialized_view_status, spawn_upstream_observation_probe_task,
};
use super::mempool::{query_mempool_activity, query_mempool_snapshot, query_mempool_summary};
use super::mempool_event_counts::query_mempool_event_counts;
use super::migration::{
    query_migration_cohorts, query_migration_denominations, query_migration_overview,
};
use super::network_upgrade_status::query_network_upgrade_status;
use super::overview_snapshot::query_overview_snapshot;
use super::recent_transactions::{RecentTransactionsStream, query_recent_transactions};
use super::search::query_search;
use super::transaction_component_summary::query_transaction_component_summary;
use super::transaction_detail::{TransactionDetailContext, query_transaction_detail};
use super::transaction_history::{
    TransactionHistoryContext, TransactionHistoryMaterializedViewReadApi,
    TransactionHistoryMaterializedViewReadError, TransactionHistoryMaterializedViewReader,
    TransactionHistoryMaterializedViewReadiness, transaction_history,
};
use super::transparent_address_activity::{
    TransparentAddressActivityContext, query_transparent_address_activity,
};
use super::transparent_address_deltas::query_transparent_address_deltas;
use super::transparent_address_ranking::query_transparent_address_ranking;
use super::value_pool_balance_history::query_value_pool_balance_history;
use super::value_pool_flow::{
    query_value_pool_flow_amount_threshold_summary, query_value_pool_flow_events_in_range,
    query_value_pool_flow_history, query_value_pool_flow_rounded_amount_summary,
    query_value_pool_flow_summary,
};
use super::value_pool_summary::query_value_pool_summary;
use zinder_materialized_views::MaterializedViewStore;

/// Settings the binary populates before constructing the adapter.
#[derive(Clone, Copy, Debug)]
pub struct ExplorerServerInfoSettings {
    /// Network the consumer mirrors.
    pub network: Network,
}

impl Default for ExplorerServerInfoSettings {
    fn default() -> Self {
        Self {
            network: Network::ZcashRegtest,
        }
    }
}

/// Server adapter implementing `ExplorerQuery` for `zinder-explorer`.
///
/// Construct with [`ExplorerQueryGrpcAdapter::new`] and chain
/// [`ExplorerQueryGrpcAdapter::with_wallet_query_endpoint`] to enable the
/// balance compute path. Without the endpoint the balance method returns
/// `UNAVAILABLE` and `ServerInfo` omits the corresponding capability.
#[derive(Clone)]
pub struct ExplorerQueryGrpcAdapter {
    settings: ExplorerServerInfoSettings,
    wallet_query_endpoint: Option<String>,
    wallet_query_bearer_token: Option<BearerToken>,
    bearer_token: Option<BearerToken>,
    materialized_view_store: Option<MaterializedViewStore>,
    transaction_history_materialized_view_reader:
        Option<Arc<dyn TransactionHistoryMaterializedViewReadApi>>,
    network_upgrade_activations: Arc<NetworkUpgradeActivations>,
    wallet_channel: Arc<OnceCell<AuthenticatedChannel>>,
    prevout_resolution_online: bool,
    upstream_observation_cache: UpstreamObservationCache,
}

impl ExplorerQueryGrpcAdapter {
    /// Creates a new explorer-query adapter without a federated balance path.
    #[must_use]
    pub fn new(settings: ExplorerServerInfoSettings) -> Self {
        Self {
            settings,
            wallet_query_endpoint: None,
            wallet_query_bearer_token: None,
            bearer_token: None,
            materialized_view_store: None,
            transaction_history_materialized_view_reader: None,
            network_upgrade_activations: Arc::new(NetworkUpgradeActivations::empty(
                settings.network,
            )),
            wallet_channel: Arc::new(OnceCell::new()),
            prevout_resolution_online: false,
            upstream_observation_cache: UpstreamObservationCache::empty(),
        }
    }

    /// Wires the consumer-side materialized-view store so block-view RPCs can read the
    /// materialized `BlockSummary` records.
    #[must_use]
    pub fn with_materialized_view_store(mut self, store: MaterializedViewStore) -> Self {
        self.transaction_history_materialized_view_reader = Some(Arc::new(
            TransactionHistoryMaterializedViewReader::new(store.clone()),
        ));
        self.materialized_view_store = Some(store);
        self
    }

    /// Wires the node-advertised network-upgrade activation table the
    /// `NetworkUpgradeStatus` handler projects onto the wire.
    #[must_use]
    pub fn with_network_upgrade_activations(
        mut self,
        activations: Arc<NetworkUpgradeActivations>,
    ) -> Self {
        self.network_upgrade_activations = activations;
        self
    }

    /// Configures the `WalletQuery` endpoint the balance handler reads from.
    ///
    /// The same endpoint serves canonical transparent outputs and the live
    /// mempool point lookups composed into the balance response.
    #[must_use]
    pub fn with_wallet_query_endpoint(mut self, endpoint: String) -> Self {
        self.wallet_query_endpoint = Some(endpoint);
        self
    }

    /// Attaches a shared-secret bearer token to outbound `WalletQuery` calls.
    #[must_use]
    pub fn with_wallet_query_bearer_token(mut self, bearer_token: BearerToken) -> Self {
        self.wallet_query_bearer_token = Some(bearer_token);
        self
    }

    /// Wires a shared-secret bearer token into the explorer-query adapter.
    ///
    /// When set, every gRPC request must carry an `authorization: Bearer
    /// <token>` metadata header that matches `bearer_token`. When unset,
    /// localhost-only deployments stay open by default.
    #[must_use]
    pub fn with_bearer_token(mut self, bearer_token: BearerToken) -> Self {
        self.bearer_token = Some(bearer_token);
        self
    }

    /// Marks transparent-output resolution as online so the adapter
    /// advertises the per-tx paid-fee capability.
    ///
    /// The binary sets this flag once it opens a materialized-view store with the bundled
    /// `TransactionFeesConsumer` column families. The flag is the single
    /// source of truth for whether paid-fee fields appear in
    /// `TransactionDetail` and `MempoolActivity` responses; downstream
    /// handlers branch on presence of materialized rows rather than re-reading
    /// this flag.
    #[must_use]
    pub const fn with_prevout_resolution_online(mut self, online: bool) -> Self {
        self.prevout_resolution_online = online;
        self
    }

    /// Spawns the background task that refreshes the cached
    /// [`zinder_source::UpstreamHealthSnapshot`] every `poll_interval`
    /// and seeds the shared cache the freshness builders read.
    ///
    /// Returns the spawned [`JoinHandle`]: the caller (binary entry point)
    /// awaits or drops it during shutdown. Without this call the cache
    /// stays empty and every `chain_view.upstream_tip` axis is `None`;
    /// that is the documented "probe has not fired yet" state per
    /// ADR-0011.
    #[must_use]
    pub fn spawn_upstream_observation_probe<Source>(
        &self,
        source: Arc<Source>,
        poll_interval: Duration,
        cancel: CancellationToken,
    ) -> JoinHandle<()>
    where
        Source: NodeSource + 'static,
    {
        spawn_upstream_observation_probe_task(
            source,
            self.upstream_observation_cache.clone(),
            poll_interval,
            cancel,
        )
    }

    /// Wraps the adapter into a tonic [`ExplorerQueryServer`] ready to be
    /// added to a `tonic::transport::Server` builder.
    #[must_use]
    pub fn into_server(
        self,
    ) -> InterceptedService<ExplorerQueryServer<Self>, BearerTokenServerInterceptor> {
        let interceptor = BearerTokenServerInterceptor::new(self.bearer_token.clone());
        let server = ExplorerQueryServer::new(self)
            .max_decoding_message_size(zinder_runtime::MAX_DECODING_MESSAGE_BYTES);
        InterceptedService::new(server, interceptor)
    }

    fn advertised_capability_readiness(&self) -> ExplorerReadiness {
        let wallet_query_online = self.wallet_query_endpoint.is_some();
        let transaction_history_readiness = self
            .transaction_history_materialized_view_reader
            .as_ref()
            .and_then(|reader| reader.readiness().ok());

        ExplorerReadiness {
            wallet_query_online,
            canonical_store_online: false,
            materialized_view_store_online: self
                .materialized_view_store
                .as_ref()
                .is_some_and(MaterializedViewStore::has_explorer_view_manifest),
            prevout_resolution_online: self.prevout_resolution_online && wallet_query_online,
            transaction_history_available: transaction_history_readiness
                .is_some_and(TransactionHistoryMaterializedViewReadiness::is_available),
            // Complete history must match a canonical tip, which the explorer cannot read.
            transaction_history_complete: false,
        }
    }

    /// Returns the capability strings the adapter currently advertises.
    ///
    /// Single source of truth for capability gating: `ServerInfo`, the ops
    /// endpoint `/healthz`, and any future advertisement surface all read
    /// from this method so a flag flip in one place reaches every consumer.
    /// Per ADR-0018, each capability lights up only when the upstream
    /// state it depends on is satisfied; the adapter never advertises a
    /// capability whose handler would return `Unavailable`.
    #[must_use]
    pub fn advertised_capabilities(&self) -> Vec<&'static str> {
        let readiness = self.advertised_capability_readiness();
        let ranking_active = self.materialized_view_store.as_ref().is_some_and(|store| {
            zinder_materialized_views::TransparentAddressRankingConsumer::active_metadata(store)
                .ok()
                .flatten()
                .is_some()
        });
        let conventional_fee_distribution_covered =
            self.materialized_view_store.as_ref().is_some_and(|store| {
                zinder_materialized_views::ConventionalFeeDistributionConsumer::coverage(store)
                    .ok()
                    .flatten()
                    .is_some()
            });
        let block_production_time_covered = block_production_time_materialized_view_available(
            self.materialized_view_store.as_ref(),
        );
        capabilities_for_surface(CapabilitySurface::Explorer)
            .filter(|spec| spec.policy.explorer_satisfied(readiness))
            .filter(|spec| {
                !matches!(
                    spec.string,
                    EXPLORER_TRANSPARENT_ADDRESS_ACTIVITY_V2
                        | EXPLORER_TRANSPARENT_ADDRESS_RANKING_V1
                ) || ranking_active
            })
            .filter(|spec| {
                spec.string != EXPLORER_CONVENTIONAL_FEE_DISTRIBUTION_V1
                    || conventional_fee_distribution_covered
            })
            // PaidFeeDistribution bounds its answer by canonical history, which the
            // explorer cannot read, so its handler always fails.
            .filter(|spec| spec.string != EXPLORER_PAID_FEE_DISTRIBUTION_V1)
            .filter(|spec| {
                spec.string != EXPLORER_BLOCK_PRODUCTION_TIME_RANGE_V1
                    || block_production_time_covered
            })
            .map(|spec| spec.string)
            .collect()
    }
}

fn block_production_time_materialized_view_available(
    materialized_view_store: Option<&MaterializedViewStore>,
) -> bool {
    let Some(store) = materialized_view_store else {
        return false;
    };
    let snapshot = store.read_snapshot();
    let coverage =
        zinder_materialized_views::BlockProductionTimeConsumer::coverage_snapshot(&snapshot)
            .ok()
            .flatten();
    let materialized_view_state = snapshot
        .consumer_state(zinder_materialized_views::BLOCK_PRODUCTION_TIME_CONSUMER_NAME)
        .ok()
        .flatten();
    drop(snapshot);
    coverage
        .zip(materialized_view_state)
        .is_some_and(|(coverage, materialized_view_state)| {
            coverage.complete_from_height.value() <= 1
                && coverage.complete_through_height >= materialized_view_state.tip_height
        })
}

/// Pair of operation labels used by every `ExplorerQuery` handler.
///
/// `method` mirrors the proto method name in `Status` text so operators see
/// the same identifier the generated `WalletQueryClient` would use. `metric`
/// is the `snake_case` Prometheus label so the `zinder_explorer_request_*`
/// series follows the recorder's naming convention. Colocating both in a
/// single `const` per handler stops the two forms from drifting apart.
struct OperationNames {
    method: &'static str,
    metric: &'static str,
}

#[tonic::async_trait]
impl ExplorerQuery for ExplorerQueryGrpcAdapter {
    async fn server_info(
        &self,
        _request: Request<ServerInfoRequest>,
    ) -> Result<Response<ServerInfoResponse>, Status> {
        // ServerInfo carries the standard ExplorerFreshness envelope per
        // ADR-0011 so consumers can read `chain_view.upstream_tip` from the
        // bootstrap call, before any materialized-view-backed capability is up.
        // During `bulk_catchup` this is the only explorer response that
        // is guaranteed to succeed; sync-progress UIs depend on it for
        // an honest denominator. `chain_epoch` is left unset here because
        // ServerInfo resolves no canonical follower tip and so makes no
        // snapshot-consistency claim; the materialized-view indexed tip and the
        // upstream observation carry the rest of the freshness signal.
        let freshness = attach_upstream_observation(
            &self.upstream_observation_cache,
            build_explorer_freshness(
                self.materialized_view_store.as_ref(),
                EXPLORER_SERVER_INFO_V1,
                None,
                0,
            )?,
        )
        .await;
        let materialized_view_status =
            read_materialized_view_status(self.materialized_view_store.as_ref())?;
        Ok(Response::new(ServerInfoResponse {
            freshness: Some(freshness),
            info: Some(ExplorerServerInfo {
                common: Some(ops::ServerInfo {
                    network: encode_zinder_native_chain_name(self.settings.network).to_owned(),
                    service_name: env!("CARGO_PKG_NAME").to_owned(),
                    service_version: env!("CARGO_PKG_VERSION").to_owned(),
                    build_git_commit: zinder_runtime::BUILD_GIT_COMMIT.to_owned(),
                    capabilities: self
                        .advertised_capabilities()
                        .into_iter()
                        .map(str::to_owned)
                        .collect(),
                    contract_revision: zinder_proto::CONTRACT_REVISION,
                }),
                vendor: "Zinder".to_owned(),
                materialized_view_status,
                materialized_view_identities: self.materialized_view_identities(),
            }),
        }))
    }

    async fn transaction_detail(
        &self,
        request: Request<TransactionDetailRequest>,
    ) -> Result<Response<TransactionDetailResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "TransactionDetail",
            metric: "transaction_detail",
        };
        let started = Instant::now();
        let outcome = async {
            let mut client = self.wallet_client(OP.method).await?;
            query_transaction_detail(
                &mut client,
                TransactionDetailContext {
                    materialized_view_store: self.materialized_view_store.as_ref(),
                    network_upgrade_activations: &self.network_upgrade_activations,
                    upstream_observation_cache: &self.upstream_observation_cache,
                },
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn block_summaries_in_range(
        &self,
        request: Request<BlockSummariesInRangeRequest>,
    ) -> Result<Response<BlockSummariesInRangeResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "BlockSummariesInRange",
            metric: "block_summaries_in_range",
        };
        let started = Instant::now();
        let outcome = async {
            let materialized_view_store = self.require_materialized_view_store(OP.method)?;
            let mut client = self.wallet_client(OP.method).await?;
            query_block_summaries_in_range(
                materialized_view_store,
                &mut client,
                &self.upstream_observation_cache,
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn block_production_series(
        &self,
        _request: Request<BlockProductionSeriesRequest>,
    ) -> Result<Response<BlockProductionSeriesResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "BlockProductionSeries",
            metric: "block_production_series",
        };
        let started = Instant::now();
        let outcome = self
            .require_materialized_view_store(OP.method)
            .and_then(|_| Err(canonical_store_unavailable(OP.method)));
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn block_production_in_time_range(
        &self,
        _request: Request<BlockProductionInTimeRangeRequest>,
    ) -> Result<Response<BlockProductionInTimeRangeResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "BlockProductionInTimeRange",
            metric: "block_production_in_time_range",
        };
        let started = Instant::now();
        let outcome = self
            .require_materialized_view_store(OP.method)
            .and_then(|_| Err(canonical_store_unavailable(OP.method)));
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn block_activity_distribution(
        &self,
        request: Request<BlockActivityDistributionRequest>,
    ) -> Result<Response<BlockActivityDistributionResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "BlockActivityDistribution",
            metric: "block_activity_distribution",
        };
        let started = Instant::now();
        let outcome = async {
            let materialized_view_store = self.require_materialized_view_store(OP.method)?;
            let mut client = self.wallet_client(OP.method).await?;
            query_block_activity_distribution(
                materialized_view_store,
                &mut client,
                &self.upstream_observation_cache,
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn transaction_component_summary(
        &self,
        request: Request<TransactionComponentSummaryRequest>,
    ) -> Result<Response<TransactionComponentSummaryResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "TransactionComponentSummary",
            metric: "transaction_component_summary",
        };
        let started = Instant::now();
        let outcome = async {
            let materialized_view_store = self.require_materialized_view_store(OP.method)?;
            let mut client = self.wallet_client(OP.method).await?;
            query_transaction_component_summary(
                materialized_view_store,
                &mut client,
                &self.upstream_observation_cache,
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn transparent_address_ranking(
        &self,
        request: Request<TransparentAddressRankingRequest>,
    ) -> Result<Response<TransparentAddressRankingResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "TransparentAddressRanking",
            metric: "transparent_address_ranking",
        };
        let started = Instant::now();
        let outcome = async {
            let materialized_view_store = self.require_materialized_view_store(OP.method)?;
            let mut client = self.wallet_client(OP.method).await?;
            query_transparent_address_ranking(
                materialized_view_store,
                &mut client,
                &self.upstream_observation_cache,
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn block_detail(
        &self,
        request: Request<BlockDetailRequest>,
    ) -> Result<Response<BlockDetailResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "BlockDetail",
            metric: "block_detail",
        };
        let started = Instant::now();
        let outcome = async {
            let materialized_view_store = self.require_materialized_view_store(OP.method)?;
            let mut client = self.wallet_client(OP.method).await?;
            query_block_detail(
                materialized_view_store,
                &mut client,
                &self.upstream_observation_cache,
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn block_transactions(
        &self,
        _request: Request<BlockDetailRequest>,
    ) -> Result<Response<BlockTransactionsResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "BlockTransactions",
            metric: "block_transactions",
        };
        let started = Instant::now();
        let outcome = self
            .require_materialized_view_store(OP.method)
            .and_then(|_| Err(canonical_store_unavailable(OP.method)));
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "Search",
            metric: "search",
        };
        let started = Instant::now();
        let outcome = async {
            let mut client = self.wallet_client(OP.method).await?;
            query_search(
                self.materialized_view_store.as_ref(),
                &mut client,
                self.settings.network,
                &self.upstream_observation_cache,
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn commitment_root_search(
        &self,
        _request: Request<CommitmentRootSearchRequest>,
    ) -> Result<Response<CommitmentRootSearchResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "CommitmentRootSearch",
            metric: "commitment_root_search",
        };
        let started = Instant::now();
        let outcome = self
            .require_materialized_view_store(OP.method)
            .and_then(|_| Err(canonical_store_unavailable(OP.method)));
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn mempool_summary(
        &self,
        request: Request<MempoolSummaryRequest>,
    ) -> Result<Response<MempoolSummaryResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "MempoolSummary",
            metric: "mempool_summary",
        };
        let started = Instant::now();
        let outcome = async {
            let mut client = self.wallet_client(OP.method).await?;
            query_mempool_summary(
                self.materialized_view_store.as_ref(),
                &mut client,
                self.settings.network,
                &self.upstream_observation_cache,
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn mempool_snapshot(
        &self,
        request: Request<MempoolSnapshotRequest>,
    ) -> Result<Response<MempoolSnapshotResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "MempoolSnapshot",
            metric: "mempool_snapshot",
        };
        let started = Instant::now();
        let outcome = async {
            let mut client = self.wallet_client(OP.method).await?;
            query_mempool_snapshot(
                self.materialized_view_store.as_ref(),
                &mut client,
                self.settings.network,
                &self.upstream_observation_cache,
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn mempool_activity(
        &self,
        request: Request<MempoolActivityRequest>,
    ) -> Result<Response<MempoolActivityResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "MempoolActivity",
            metric: "mempool_activity",
        };
        let started = Instant::now();
        let outcome = async {
            let mut client = self.wallet_client(OP.method).await?;
            query_mempool_activity(
                self.materialized_view_store.as_ref(),
                &mut client,
                self.settings.network,
                &self.upstream_observation_cache,
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn transparent_address_activity(
        &self,
        request: Request<TransparentAddressActivityRequest>,
    ) -> Result<Response<TransparentAddressActivityResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "TransparentAddressActivity",
            metric: "transparent_address_activity",
        };
        let started = Instant::now();
        let outcome = async {
            let materialized_view_store =
                self.materialized_view_store.as_ref().ok_or_else(|| {
                    ExplorerError::dependency_not_configured(
                        "TransparentAddressActivity requires a materialized-view store",
                    )
                })?;
            let mut client = self.wallet_client(OP.method).await?;
            query_transparent_address_activity(
                TransparentAddressActivityContext {
                    materialized_view_store,
                    network: self.settings.network,
                    upstream_observation_cache: &self.upstream_observation_cache,
                },
                &mut client,
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn transparent_address_deltas(
        &self,
        request: Request<TransparentAddressDeltasRequest>,
    ) -> Result<Response<TransparentAddressDeltasResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "TransparentAddressDeltas",
            metric: "transparent_address_deltas",
        };
        let started = Instant::now();
        let outcome = async {
            let materialized_view_store =
                self.materialized_view_store.as_ref().ok_or_else(|| {
                    ExplorerError::dependency_not_configured(
                        "TransparentAddressDeltas requires a materialized-view store",
                    )
                })?;
            let mut client = self.wallet_client(OP.method).await?;
            query_transparent_address_deltas(
                materialized_view_store,
                &mut client,
                self.settings.network,
                &self.upstream_observation_cache,
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn fee_summary(
        &self,
        request: Request<FeeSummaryRequest>,
    ) -> Result<Response<FeeSummaryResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "FeeSummary",
            metric: "fee_summary",
        };
        let started = Instant::now();
        let outcome = async {
            let materialized_view_store = self.require_materialized_view_store(OP.method)?;
            let mut client = self.wallet_client(OP.method).await?;
            query_fee_summary(
                materialized_view_store,
                &mut client,
                &self.upstream_observation_cache,
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn conventional_fee_distribution(
        &self,
        request: Request<ConventionalFeeDistributionRequest>,
    ) -> Result<Response<ConventionalFeeDistributionResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "ConventionalFeeDistribution",
            metric: "conventional_fee_distribution",
        };
        let started = Instant::now();
        let outcome = async {
            let materialized_view_store = self.require_materialized_view_store(OP.method)?;
            let mut client = self.wallet_client(OP.method).await?;
            query_conventional_fee_distribution(
                materialized_view_store,
                &mut client,
                &self.upstream_observation_cache,
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn paid_fee_distribution(
        &self,
        _request: Request<PaidFeeDistributionRequest>,
    ) -> Result<Response<PaidFeeDistributionResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "PaidFeeDistribution",
            metric: "paid_fee_distribution",
        };
        let started = Instant::now();
        let outcome = self
            .require_materialized_view_store(OP.method)
            .and_then(|_| Err(canonical_store_unavailable(OP.method)));
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn value_pool_summary(
        &self,
        request: Request<ValuePoolSummaryRequest>,
    ) -> Result<Response<ValuePoolSummaryResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "ValuePoolSummary",
            metric: "value_pool_summary",
        };
        let started = Instant::now();
        let outcome = async {
            let mut client = self.wallet_client(OP.method).await?;
            query_value_pool_summary(
                self.materialized_view_store.as_ref(),
                &mut client,
                &self.upstream_observation_cache,
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn network_upgrade_status(
        &self,
        request: Request<NetworkUpgradeStatusRequest>,
    ) -> Result<Response<NetworkUpgradeStatusResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "NetworkUpgradeStatus",
            metric: "network_upgrade_status",
        };
        let started = Instant::now();
        let outcome = async {
            let mut client = self.wallet_client(OP.method).await?;
            query_network_upgrade_status(
                self.materialized_view_store.as_ref(),
                &self.network_upgrade_activations,
                &mut client,
                &self.upstream_observation_cache,
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn value_pool_flow_history(
        &self,
        request: Request<ValuePoolFlowHistoryRequest>,
    ) -> Result<Response<ValuePoolFlowHistoryResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "ValuePoolFlowHistory",
            metric: "value_pool_flow_history",
        };
        let started = Instant::now();
        let outcome = async {
            let materialized_view_store = self.require_materialized_view_store(OP.method)?;
            let mut client = self.wallet_client(OP.method).await?;
            query_value_pool_flow_history(
                materialized_view_store,
                &mut client,
                &self.upstream_observation_cache,
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn value_pool_flow_events_in_range(
        &self,
        request: Request<ValuePoolFlowEventsInRangeRequest>,
    ) -> Result<Response<ValuePoolFlowEventsInRangeResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "ValuePoolFlowEventsInRange",
            metric: "value_pool_flow_events_in_range",
        };
        let started = Instant::now();
        let outcome = async {
            let materialized_view_store = self.require_materialized_view_store(OP.method)?;
            let mut client = self.wallet_client(OP.method).await?;
            query_value_pool_flow_events_in_range(
                materialized_view_store,
                &mut client,
                &self.upstream_observation_cache,
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn value_pool_flow_summary(
        &self,
        request: Request<ValuePoolFlowSummaryRequest>,
    ) -> Result<Response<ValuePoolFlowSummaryResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "ValuePoolFlowSummary",
            metric: "value_pool_flow_summary",
        };
        let started = Instant::now();
        let outcome = async {
            let materialized_view_store = self.require_materialized_view_store(OP.method)?;
            let mut client = self.wallet_client(OP.method).await?;
            query_value_pool_flow_summary(
                materialized_view_store,
                &mut client,
                &self.upstream_observation_cache,
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn value_pool_flow_amount_threshold_summary(
        &self,
        request: Request<ValuePoolFlowAmountThresholdSummaryRequest>,
    ) -> Result<Response<ValuePoolFlowAmountThresholdSummaryResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "ValuePoolFlowAmountThresholdSummary",
            metric: "value_pool_flow_amount_threshold_summary",
        };
        let started = Instant::now();
        let outcome = async {
            let materialized_view_store = self.require_materialized_view_store(OP.method)?;
            let mut client = self.wallet_client(OP.method).await?;
            query_value_pool_flow_amount_threshold_summary(
                materialized_view_store,
                &mut client,
                &self.upstream_observation_cache,
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn value_pool_flow_rounded_amount_summary(
        &self,
        request: Request<ValuePoolFlowRoundedAmountSummaryRequest>,
    ) -> Result<Response<ValuePoolFlowRoundedAmountSummaryResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "ValuePoolFlowRoundedAmountSummary",
            metric: "value_pool_flow_rounded_amount_summary",
        };
        let started = Instant::now();
        let outcome = async {
            let materialized_view_store = self.require_materialized_view_store(OP.method)?;
            let mut client = self.wallet_client(OP.method).await?;
            query_value_pool_flow_rounded_amount_summary(
                materialized_view_store,
                &mut client,
                &self.upstream_observation_cache,
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn value_pool_balance_history(
        &self,
        request: Request<ValuePoolBalanceHistoryRequest>,
    ) -> Result<Response<ValuePoolBalanceHistoryResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "ValuePoolBalanceHistory",
            metric: "value_pool_balance_history",
        };
        let started = Instant::now();
        let outcome = async {
            let materialized_view_store = self.require_materialized_view_store(OP.method)?;
            let mut client = self.wallet_client(OP.method).await?;
            query_value_pool_balance_history(
                materialized_view_store,
                &mut client,
                &self.upstream_observation_cache,
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn chain_reorg_history(
        &self,
        request: Request<ChainReorgHistoryRequest>,
    ) -> Result<Response<ChainReorgHistoryResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "ChainReorgHistory",
            metric: "chain_reorg_history",
        };
        let started = Instant::now();
        let outcome = async {
            let materialized_view_store =
                self.materialized_view_store.as_ref().ok_or_else(|| {
                    ExplorerError::dependency_not_configured(
                        "ChainReorgHistory requires a materialized-view store",
                    )
                })?;
            query_chain_reorg_history(
                materialized_view_store,
                &self.upstream_observation_cache,
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn displaced_block_history(
        &self,
        _request: Request<DisplacedBlockHistoryRequest>,
    ) -> Result<Response<DisplacedBlockHistoryResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "DisplacedBlockHistory",
            metric: "displaced_block_history",
        };
        let started = Instant::now();
        let outcome = Err(canonical_store_unavailable(OP.method));
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn displaced_block_detail(
        &self,
        _request: Request<DisplacedBlockDetailRequest>,
    ) -> Result<Response<DisplacedBlockDetailResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "DisplacedBlockDetail",
            metric: "displaced_block_detail",
        };
        let started = Instant::now();
        let outcome = Err(canonical_store_unavailable(OP.method));
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn mempool_event_counts(
        &self,
        request: Request<MempoolEventCountsRequest>,
    ) -> Result<Response<MempoolEventCountsResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "MempoolEventCounts",
            metric: "mempool_event_counts",
        };
        let started = Instant::now();
        let outcome = async {
            let materialized_view_store =
                self.materialized_view_store.as_ref().ok_or_else(|| {
                    ExplorerError::dependency_not_configured(
                        "MempoolEventCounts requires a materialized-view store",
                    )
                })?;
            let mut client = self.wallet_client(OP.method).await?;
            query_mempool_event_counts(
                materialized_view_store,
                &mut client,
                &self.upstream_observation_cache,
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn transaction_history(
        &self,
        request: Request<TransactionHistoryRequest>,
    ) -> Result<Response<TransactionHistoryResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "TransactionHistory",
            metric: "transaction_history",
        };
        let started = Instant::now();
        let outcome = async {
            let materialized_view_reader =
                self.require_transaction_history_materialized_view_reader()?;
            materialized_view_reader
                .readiness()
                .and_then(TransactionHistoryMaterializedViewReadiness::require_available)
                .map_err(TransactionHistoryMaterializedViewReadError::into_status)?;
            let mut client = self.wallet_client(OP.method).await?;
            transaction_history(
                TransactionHistoryContext {
                    materialized_view_reader: Arc::clone(materialized_view_reader),
                    materialized_view_store: self.materialized_view_store.as_ref(),
                    upstream_observation_cache: &self.upstream_observation_cache,
                },
                &mut client,
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    type RecentTransactionsStream = RecentTransactionsStream;

    async fn recent_transactions(
        &self,
        request: Request<RecentTransactionsRequest>,
    ) -> Result<Response<Self::RecentTransactionsStream>, Status> {
        const OP: OperationNames = OperationNames {
            method: "RecentTransactions",
            metric: "recent_transactions",
        };
        let started = Instant::now();
        let outcome = async {
            let materialized_view_store = self.require_materialized_view_store(OP.method)?;
            let mut client = self.wallet_client(OP.method).await?;
            query_recent_transactions(
                materialized_view_store,
                &mut client,
                &self.upstream_observation_cache,
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn overview_snapshot(
        &self,
        request: Request<OverviewSnapshotRequest>,
    ) -> Result<Response<OverviewSnapshotResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "OverviewSnapshot",
            metric: "overview_snapshot",
        };
        let started = Instant::now();
        let outcome = async {
            let materialized_view_store = self.require_materialized_view_store(OP.method)?;
            let mut client = self.wallet_client(OP.method).await?;
            query_overview_snapshot(
                materialized_view_store,
                &mut client,
                &self.upstream_observation_cache,
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn migration_overview(
        &self,
        request: Request<MigrationOverviewRequest>,
    ) -> Result<Response<MigrationOverviewResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "MigrationOverview",
            metric: "migration_overview",
        };
        let started = Instant::now();
        let outcome = async {
            let materialized_view_store = self.require_materialized_view_store(OP.method)?;
            let mut client = self.wallet_client(OP.method).await?;
            query_migration_overview(
                materialized_view_store,
                &mut client,
                &self.upstream_observation_cache,
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn migration_cohorts(
        &self,
        request: Request<MigrationCohortsRequest>,
    ) -> Result<Response<MigrationCohortsResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "MigrationCohorts",
            metric: "migration_cohorts",
        };
        let started = Instant::now();
        let outcome = async {
            let materialized_view_store = self.require_materialized_view_store(OP.method)?;
            let mut client = self.wallet_client(OP.method).await?;
            query_migration_cohorts(
                materialized_view_store,
                &mut client,
                &self.upstream_observation_cache,
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }

    async fn migration_denominations(
        &self,
        request: Request<MigrationDenominationsRequest>,
    ) -> Result<Response<MigrationDenominationsResponse>, Status> {
        const OP: OperationNames = OperationNames {
            method: "MigrationDenominations",
            metric: "migration_denominations",
        };
        let started = Instant::now();
        let outcome = async {
            let materialized_view_store = self.require_materialized_view_store(OP.method)?;
            let mut client = self.wallet_client(OP.method).await?;
            query_migration_denominations(
                materialized_view_store,
                &mut client,
                &self.upstream_observation_cache,
                request,
            )
            .await
        }
        .await;
        record_explorer_request(OP.metric, started.elapsed(), outcome.as_ref().err());
        outcome
    }
}

impl ExplorerQueryGrpcAdapter {
    fn require_transaction_history_materialized_view_reader(
        &self,
    ) -> Result<&Arc<dyn TransactionHistoryMaterializedViewReadApi>, Status> {
        self.transaction_history_materialized_view_reader
            .as_ref()
            .ok_or_else(|| {
                ExplorerError::dependency_not_configured(
                    "TransactionHistory requires a materialized-view reader",
                )
                .into()
            })
    }

    /// Consumer identities of the attached Explorer view store. Empty unless
    /// the store carries the exact Explorer view manifest, because only that
    /// manifest admits the view-backed readers.
    fn materialized_view_identities(&self) -> Vec<String> {
        self.materialized_view_store
            .as_ref()
            .filter(|store| store.has_explorer_view_manifest())
            .map_or_else(Vec::new, |store| {
                store
                    .consumer_identities()
                    .map(|identity| identity.as_str().to_owned())
                    .collect()
            })
    }

    fn require_materialized_view_store(
        &self,
        method: &'static str,
    ) -> Result<&MaterializedViewStore, Status> {
        let materialized_view_store = self.materialized_view_store.as_ref().ok_or_else(|| {
            Status::from(ExplorerError::dependency_not_configured(format!(
                "{method} requires the BlockSummary materialized view; configure \
                 --storage-path and start the explorer with the consumer wired"
            )))
        })?;
        if !materialized_view_store.has_explorer_view_manifest() {
            return Err(ExplorerError::unsupported(format!(
                "{method} is unavailable because the attached materialized-view store lacks \
                 the Explorer view manifest"
            ))
            .into());
        }
        Ok(materialized_view_store)
    }

    fn require_wallet_endpoint(&self, method: &'static str) -> Result<&str, Status> {
        self.wallet_query_endpoint.as_deref().ok_or_else(|| {
            ExplorerError::dependency_not_configured(format!(
                "{method} requires a wallet_query_endpoint; configure \
                 --wallet-query-endpoint"
            ))
            .into()
        })
    }

    /// Returns a `WalletQueryClient` that shares one cached HTTP/2 channel
    /// across every request handled by this adapter.
    ///
    /// The first call pays the dial cost; subsequent calls clone the cached
    /// channel, which is `tonic::transport::Channel` internally (cheap clone,
    /// transparent HTTP/2 reconnect).
    async fn wallet_client(
        &self,
        method: &'static str,
    ) -> Result<WalletQueryClient<AuthenticatedChannel>, Status> {
        let endpoint = self.require_wallet_endpoint(method)?;
        let token = self.wallet_query_bearer_token.clone();
        let channel = self
            .wallet_channel
            .get_or_try_init(|| async {
                connect_zinder_grpc(endpoint, token.as_ref())
                    .await
                    .map_err(connect_error_to_status)
            })
            .await?;
        Ok(WalletQueryClient::new(channel.clone()))
    }
}

/// Fails a method that reads canonical-store facts.
///
/// The explorer has no canonical-store read path, so these methods are never
/// advertised and fail as an unconfigured dependency on every deployment.
fn canonical_store_unavailable(method: &'static str) -> Status {
    ExplorerError::dependency_not_configured(format!(
        "{method} requires canonical-store reads, which zinder-explorer does not provide"
    ))
    .into()
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "BearerTokenConnectError is moved out of the Result by the caller; the helper takes ownership"
)]
fn connect_error_to_status(error: BearerTokenConnectError) -> Status {
    ExplorerError::upstream_unreachable(format!("WalletQuery endpoint unreachable: {error}")).into()
}

/// Registers `# HELP` and `# TYPE` text for every metric this module emits.
///
/// Call once at startup, after `install_metrics_recorder` returns and before
/// the gRPC server records its first request. Delegates to
/// [`describe_rpc_metrics`] so every Zinder service shares one description
/// template.
pub fn describe_request_metrics() {
    describe_rpc_metrics(EXPLORER_RPC_METRICS, "ExplorerQuery");
}

/// Records per-RPC duration + status counters via
/// [`record_rpc_request`].
///
/// Explorer handlers return [`tonic::Status`] directly, so the
/// `error_class` vocabulary is the tonic [`Code`] name. When the explorer
/// grows a typed domain-error type, swap [`status_error_class`] for a
/// domain-aware mapper to keep labels aligned with operator dashboards.
fn record_explorer_request(operation: &'static str, elapsed: Duration, error: Option<&Status>) {
    let outcome = error.map_or(RpcOutcome::Ok, |status| RpcOutcome::Error {
        class: status_error_class(status),
    });
    record_rpc_request(EXPLORER_RPC_METRICS, operation, elapsed, outcome);
}

/// Maps a `tonic::Status::code()` to the short string label operational
/// dashboards filter on. Mirrors `zinder-query`'s `query_error_class` so the
/// query boundaries share one vocabulary.
fn status_error_class(status: &Status) -> &'static str {
    match status.code() {
        Code::Ok => "none",
        Code::Cancelled => "cancelled",
        Code::InvalidArgument => "invalid_argument",
        Code::DeadlineExceeded => "deadline_exceeded",
        Code::NotFound => "not_found",
        Code::AlreadyExists => "already_exists",
        Code::PermissionDenied => "permission_denied",
        Code::ResourceExhausted => "resource_exhausted",
        Code::FailedPrecondition => "failed_precondition",
        Code::Aborted => "aborted",
        Code::OutOfRange => "out_of_range",
        Code::Unimplemented => "unimplemented",
        Code::Internal => "internal",
        Code::Unavailable => "unavailable",
        Code::DataLoss => "data_loss",
        Code::Unauthenticated => "unauthenticated",
        Code::Unknown => "unknown",
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        missing_docs,
        reason = "Unit test names describe the behavior under test."
    )]

    use tempfile::{TempDir, tempdir};
    use tonic_types::StatusExt as _;
    use zinder_core::{BlockHash, BlockHeight, ChainEpochId};
    use zinder_materialized_views::{
        CONVENTIONAL_FEE_DISTRIBUTION_SCHEMA, MaterializedViewCoverage, MaterializedViewState,
        MaterializedViewStoreOptions, PAID_FEE_DISTRIBUTION_SCHEMA,
        TRANSACTION_HISTORY_CONSUMER_NAME, TRANSPARENT_OUTPOINT_SPEND_CONSUMER_NAME,
        TRANSPARENT_OUTPOINT_SPEND_SCHEMA,
    };
    use zinder_proto::capabilities::{
        EXPLORER_BLOCK_FINAL_NOTE_COMMITMENT_ROOTS_V1, EXPLORER_BLOCK_PRODUCTION_SERIES_V2,
        EXPLORER_BLOCK_SUMMARY_V1, EXPLORER_BLOCK_TRANSACTIONS_V2,
        EXPLORER_CHAIN_DISPLACED_BLOCK_DETAIL_V1, EXPLORER_CHAIN_DISPLACED_BLOCK_HISTORY_V1,
        EXPLORER_COMMITMENT_ROOT_DISPLACED_MATCHES_V1, EXPLORER_COMMITMENT_ROOT_SEARCH_V1,
        EXPLORER_TRANSACTION_DETAIL_V4, EXPLORER_TRANSACTION_HISTORY_V1,
        EXPLORER_TRANSACTION_HISTORY_V2, EXPLORER_TRANSACTION_INTRINSIC_VALUE_BALANCES_V1,
    };
    use zinder_proto::v1::ops::ErrorReason;
    use zinder_store::RocksDbResourceBudget;

    use super::*;

    /// Methods that read canonical-store facts the explorer runtime does not serve.
    async fn canonical_fact_method_outcomes(
        adapter: &ExplorerQueryGrpcAdapter,
    ) -> Vec<(&'static str, Option<Status>)> {
        vec![
            (
                "BlockProductionSeries",
                adapter
                    .block_production_series(Request::new(BlockProductionSeriesRequest::default()))
                    .await
                    .err(),
            ),
            (
                "BlockProductionInTimeRange",
                adapter
                    .block_production_in_time_range(Request::new(
                        BlockProductionInTimeRangeRequest::default(),
                    ))
                    .await
                    .err(),
            ),
            (
                "BlockTransactions",
                adapter
                    .block_transactions(Request::new(BlockDetailRequest::default()))
                    .await
                    .err(),
            ),
            (
                "CommitmentRootSearch",
                adapter
                    .commitment_root_search(Request::new(CommitmentRootSearchRequest::default()))
                    .await
                    .err(),
            ),
            (
                "PaidFeeDistribution",
                adapter
                    .paid_fee_distribution(Request::new(PaidFeeDistributionRequest::default()))
                    .await
                    .err(),
            ),
            (
                "DisplacedBlockHistory",
                adapter
                    .displaced_block_history(Request::new(DisplacedBlockHistoryRequest::default()))
                    .await
                    .err(),
            ),
            (
                "DisplacedBlockDetail",
                adapter
                    .displaced_block_detail(Request::new(DisplacedBlockDetailRequest::default()))
                    .await
                    .err(),
            ),
        ]
    }

    #[tokio::test]
    async fn canonical_fact_methods_fail_as_unconfigured_and_stay_unadvertised()
    -> Result<(), Box<dyn std::error::Error>> {
        let tempdir = tempdir()?;
        let materialized_view_store = MaterializedViewStore::open_explorer_views(
            tempdir.path(),
            MaterializedViewStoreOptions {
                rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
                ..MaterializedViewStoreOptions::default()
            },
        )?;
        let adapter = ExplorerQueryGrpcAdapter::new(ExplorerServerInfoSettings::default())
            .with_materialized_view_store(materialized_view_store)
            .with_wallet_query_endpoint(String::from("http://127.0.0.1:1"));

        for (method, outcome) in canonical_fact_method_outcomes(&adapter).await {
            let status = outcome.ok_or_else(|| format!("{method} unexpectedly succeeded"))?;
            assert_eq!(status.code(), Code::FailedPrecondition, "{method}");
            let reason = status
                .get_error_details()
                .error_info()
                .map(|error_info| error_info.reason.clone());
            assert_eq!(
                reason.as_deref(),
                Some(ErrorReason::DependencyNotConfigured.as_str_name()),
                "{method}"
            );
            assert!(status.message().starts_with(method), "{}", status.message());
            assert!(
                !status.message().contains("--storage-path"),
                "{method} suggests a flag that cannot satisfy it: {}",
                status.message()
            );
        }

        let capabilities = adapter.advertised_capabilities();
        for capability in [
            EXPLORER_BLOCK_PRODUCTION_SERIES_V2,
            EXPLORER_BLOCK_PRODUCTION_TIME_RANGE_V1,
            EXPLORER_BLOCK_TRANSACTIONS_V2,
            EXPLORER_BLOCK_FINAL_NOTE_COMMITMENT_ROOTS_V1,
            EXPLORER_COMMITMENT_ROOT_SEARCH_V1,
            EXPLORER_COMMITMENT_ROOT_DISPLACED_MATCHES_V1,
            EXPLORER_PAID_FEE_DISTRIBUTION_V1,
            EXPLORER_CHAIN_DISPLACED_BLOCK_HISTORY_V1,
            EXPLORER_CHAIN_DISPLACED_BLOCK_DETAIL_V1,
            EXPLORER_TRANSACTION_INTRINSIC_VALUE_BALANCES_V1,
        ] {
            assert!(
                !capabilities.contains(&capability),
                "{capability} is advertised without a canonical read path"
            );
        }
        Ok(())
    }

    fn adapter_with_transaction_history_state(
        state: Option<MaterializedViewState>,
        wallet_query_online: bool,
    ) -> Result<(TempDir, ExplorerQueryGrpcAdapter), Box<dyn std::error::Error>> {
        let tempdir = tempdir()?;
        let materialized_view_store = MaterializedViewStore::open(
            tempdir.path(),
            MaterializedViewStoreOptions {
                consumers: MaterializedViewStore::bundled_consumers(),
                rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
                sync_writes: false,
            },
        )?;
        if let Some(state) = state {
            materialized_view_store.put_consumer_state(TRANSACTION_HISTORY_CONSUMER_NAME, state)?;
        }
        let adapter = ExplorerQueryGrpcAdapter::new(ExplorerServerInfoSettings::default())
            .with_materialized_view_store(materialized_view_store);
        let adapter = if wallet_query_online {
            adapter.with_wallet_query_endpoint(String::from("http://127.0.0.1:1"))
        } else {
            adapter
        };
        Ok((tempdir, adapter))
    }

    fn assert_transaction_history_capabilities(
        adapter: &ExplorerQueryGrpcAdapter,
        v1_available: bool,
        v2_available: bool,
    ) {
        let capabilities = adapter.advertised_capabilities();
        assert_eq!(
            capabilities.contains(&EXPLORER_TRANSACTION_HISTORY_V1),
            v1_available
        );
        assert_eq!(
            capabilities.contains(&EXPLORER_TRANSACTION_HISTORY_V2),
            v2_available
        );
    }

    #[test]
    fn transaction_history_capabilities_follow_materialized_view_state_and_dependencies()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_tempdir, adapter) = adapter_with_transaction_history_state(None, true)?;
        assert_transaction_history_capabilities(&adapter, false, false);

        let partial_state = MaterializedViewState {
            chain_epoch_id: ChainEpochId::new(7),
            tip_height: BlockHeight::new(20),
            tip_hash: BlockHash::from_bytes([0x20; 32]),
            revision: 1,
            coverage: None,
        };
        let (_tempdir, adapter) =
            adapter_with_transaction_history_state(Some(partial_state), true)?;
        assert_transaction_history_capabilities(&adapter, true, false);

        // Full coverage alone does not prove completeness: the explorer has no
        // canonical tip to match the materialized-view position against.
        let complete_state = MaterializedViewState {
            revision: 2,
            coverage: Some(MaterializedViewCoverage {
                complete_from_height: BlockHeight::new(1),
                complete_through_height: partial_state.tip_height,
                complete_through_hash: partial_state.tip_hash,
            }),
            ..partial_state
        };
        let (_tempdir, adapter) =
            adapter_with_transaction_history_state(Some(complete_state), true)?;
        assert_transaction_history_capabilities(&adapter, true, false);

        let (_tempdir, adapter) =
            adapter_with_transaction_history_state(Some(complete_state), false)?;
        assert_transaction_history_capabilities(&adapter, false, false);
        Ok(())
    }

    #[test]
    fn transaction_detail_capability_requires_wallet_and_explorer_materialized_views()
    -> Result<(), Box<dyn std::error::Error>> {
        let wallet_only = ExplorerQueryGrpcAdapter::new(ExplorerServerInfoSettings::default())
            .with_wallet_query_endpoint(String::from("http://127.0.0.1:1"));
        assert!(
            !wallet_only
                .advertised_capabilities()
                .contains(&EXPLORER_TRANSACTION_DETAIL_V4)
        );

        let tempdir = tempdir()?;
        let materialized_view_store = MaterializedViewStore::open_explorer_views(
            tempdir.path(),
            MaterializedViewStoreOptions {
                rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
                ..MaterializedViewStoreOptions::default()
            },
        )?;
        let materialized_views_only =
            ExplorerQueryGrpcAdapter::new(ExplorerServerInfoSettings::default())
                .with_materialized_view_store(materialized_view_store);
        assert!(
            !materialized_views_only
                .advertised_capabilities()
                .contains(&EXPLORER_TRANSACTION_DETAIL_V4)
        );

        let adapter =
            materialized_views_only.with_wallet_query_endpoint(String::from("http://127.0.0.1:1"));
        assert!(
            adapter
                .advertised_capabilities()
                .contains(&EXPLORER_TRANSACTION_DETAIL_V4)
        );
        Ok(())
    }

    #[tokio::test]
    async fn server_info_reports_view_identities_for_the_exact_manifest()
    -> Result<(), Box<dyn std::error::Error>> {
        let tempdir = tempdir()?;
        let materialized_view_store = MaterializedViewStore::open_explorer_views(
            tempdir.path(),
            MaterializedViewStoreOptions {
                rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
                ..MaterializedViewStoreOptions::default()
            },
        )?;
        let expected_identity_count = materialized_view_store.consumer_identities().count();
        let adapter = ExplorerQueryGrpcAdapter::new(ExplorerServerInfoSettings::default())
            .with_materialized_view_store(materialized_view_store);

        let info = adapter
            .server_info(Request::new(ServerInfoRequest {}))
            .await?
            .into_inner()
            .info
            .ok_or("explorer server info missing its descriptor")?;

        assert_eq!(
            info.materialized_view_identities.len(),
            expected_identity_count
        );
        assert!(
            info.materialized_view_identities
                .iter()
                .any(|identity| identity == TRANSPARENT_OUTPOINT_SPEND_CONSUMER_NAME.as_str())
        );
        Ok(())
    }

    #[tokio::test]
    async fn omitted_transaction_history_disables_capabilities_and_fails_before_wallet_connect()
    -> Result<(), Box<dyn std::error::Error>> {
        let tempdir = tempdir()?;
        let materialized_view_store = MaterializedViewStore::open(
            tempdir.path(),
            MaterializedViewStoreOptions {
                consumers: &[TRANSPARENT_OUTPOINT_SPEND_SCHEMA],
                rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
                ..MaterializedViewStoreOptions::default()
            },
        )?;
        let adapter = ExplorerQueryGrpcAdapter::new(ExplorerServerInfoSettings::default())
            .with_materialized_view_store(materialized_view_store)
            .with_wallet_query_endpoint(String::from("http://127.0.0.1:1"));

        let capabilities = adapter.advertised_capabilities();
        assert!(!capabilities.contains(&EXPLORER_BLOCK_SUMMARY_V1));
        assert!(!capabilities.contains(&EXPLORER_TRANSACTION_HISTORY_V1));
        assert!(!capabilities.contains(&EXPLORER_TRANSACTION_HISTORY_V2));
        let server_info = adapter
            .server_info(Request::new(ServerInfoRequest {}))
            .await?
            .into_inner();
        let info = server_info
            .info
            .ok_or("explorer server info missing its descriptor")?;
        let common = info
            .common
            .ok_or("explorer server info missing common descriptor")?;
        assert_eq!(common.build_git_commit, zinder_runtime::BUILD_GIT_COMMIT);
        assert!(info.materialized_view_identities.is_empty());
        let outcome = adapter
            .transaction_history(Request::new(TransactionHistoryRequest::default()))
            .await;
        let Err(error) = outcome else {
            return Err(
                "omitted materialized view unexpectedly reached the wallet connection".into(),
            );
        };
        assert_eq!(error.code(), Code::Unimplemented);
        let block_summaries = adapter
            .block_summaries_in_range(Request::new(BlockSummariesInRangeRequest::default()))
            .await;
        let Err(error) = block_summaries else {
            return Err("partial view store unexpectedly reached the wallet connection".into());
        };
        assert_eq!(error.code(), Code::Unimplemented);
        Ok(())
    }

    #[tokio::test]
    async fn materializing_transaction_history_fails_before_wallet_connect()
    -> Result<(), Box<dyn std::error::Error>> {
        let tempdir = tempdir()?;
        let materialized_view_store = MaterializedViewStore::open_explorer_views(
            tempdir.path(),
            MaterializedViewStoreOptions {
                rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
                ..MaterializedViewStoreOptions::default()
            },
        )?;
        let adapter = ExplorerQueryGrpcAdapter::new(ExplorerServerInfoSettings::default())
            .with_materialized_view_store(materialized_view_store)
            .with_wallet_query_endpoint(String::from("http://127.0.0.1:1"));

        let outcome = adapter
            .transaction_history(Request::new(TransactionHistoryRequest::default()))
            .await;
        let Err(error) = outcome else {
            return Err("materializing view unexpectedly reached the wallet connection".into());
        };
        assert_eq!(error.code(), Code::FailedPrecondition);
        Ok(())
    }

    #[test]
    fn fee_distribution_capabilities_are_hidden_without_materialized_view_coverage()
    -> Result<(), Box<dyn std::error::Error>> {
        let tempdir = tempdir()?;
        let materialized_view_store = MaterializedViewStore::open(
            tempdir.path(),
            MaterializedViewStoreOptions {
                consumers: &[
                    CONVENTIONAL_FEE_DISTRIBUTION_SCHEMA,
                    PAID_FEE_DISTRIBUTION_SCHEMA,
                ],
                rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
                sync_writes: false,
            },
        )?;
        let adapter = ExplorerQueryGrpcAdapter::new(ExplorerServerInfoSettings::default())
            .with_materialized_view_store(materialized_view_store)
            .with_wallet_query_endpoint(String::from("http://127.0.0.1:1"));

        assert!(
            !adapter
                .advertised_capabilities()
                .contains(&EXPLORER_CONVENTIONAL_FEE_DISTRIBUTION_V1)
        );
        assert!(
            !adapter
                .advertised_capabilities()
                .contains(&EXPLORER_PAID_FEE_DISTRIBUTION_V1)
        );
        Ok(())
    }
}
