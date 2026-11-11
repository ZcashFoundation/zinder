//! Live federation tests for the `ExplorerQuery` mempool views.
//!
//! Both handlers compose `WalletQuery.MempoolSnapshot` at request time
//! and parse every entry via `zinder_source::parse_transaction_public_fact_set`.
//! The tests stand up the production wallet endpoint in process over a
//! regtest canonical store built from genesis, with mempool reads routed to an
//! empty ingest-control fixture, and assert the wire shape, freshness envelope,
//! and pagination contract.

use eyre::{Result, eyre};
use tonic::Request;
use zinder_core::Network;
use zinder_core::wire::encode_zinder_native_chain_name;
use zinder_explorer::{ExplorerQueryGrpcAdapter, ExplorerServerInfoSettings};
use zinder_proto::capabilities::{
    EXPLORER_MEMPOOL_ACTIVITY_V1, EXPLORER_MEMPOOL_SNAPSHOT_V1, EXPLORER_MEMPOOL_SUMMARY_V1,
};
use zinder_proto::v1::explorer::{
    MempoolActivityRequest, MempoolActivityResponse, MempoolSnapshotRequest,
    MempoolSnapshotResponse, MempoolSummaryRequest, MempoolSummaryResponse,
    explorer_query_server::ExplorerQuery as ExplorerQueryService,
};
use zinder_testkit::live::{LiveTestEnv, init, require_live_for};

use crate::common::{LiveWalletEndpoint, serve_live_wallet_endpoint};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn mempool_views_return_freshness_envelope() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live_for(&[Network::ZcashRegtest])? else {
        return Ok(());
    };
    let mut fixture = MempoolFixture::open(&env).await?;
    let summary = fixture.summary().await?;
    assert_summary_freshness(&summary)?;
    let activity = fixture.activity(0).await?;
    assert_activity_freshness(&activity)?;
    let snapshot = fixture.snapshot(0).await?;
    assert_snapshot_freshness(&snapshot)?;

    tracing::info!(
        target: "zinder::live",
        event = "mempool_views_validated",
        network = %encode_zinder_native_chain_name(fixture.network),
        transaction_count = summary.transaction_count,
        activity_entries = activity.entries.len(),
        snapshot_entries = snapshot.entries.len(),
        "explorer mempool views validated against live node",
    );

    fixture.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn mempool_activity_pagination_emits_unique_entries() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live_for(&[Network::ZcashRegtest])? else {
        return Ok(());
    };
    let mut fixture = MempoolFixture::open(&env).await?;
    let first_page = fixture.activity(2).await?;
    if first_page.next_cursor.is_empty() {
        // Mempool is empty or fit in the first page; nothing further to
        // assert beyond the freshness contract. The summary path proves
        // the snapshot was readable.
        fixture.shutdown().await;
        return Ok(());
    }

    let second_page = fixture
        .activity_with_cursor(2, first_page.next_cursor)
        .await?;

    let first_ids: Vec<_> = first_page
        .entries
        .iter()
        .map(|entry| entry.transaction_id.clone())
        .collect();
    for entry in &second_page.entries {
        assert!(
            !first_ids.contains(&entry.transaction_id),
            "MempoolActivity pagination must not repeat entry {}",
            hex::encode(&entry.transaction_id),
        );
    }

    fixture.shutdown().await;
    Ok(())
}

struct MempoolFixture {
    network: Network,
    explorer_adapter: ExplorerQueryGrpcAdapter,
    wallet_endpoint: LiveWalletEndpoint,
}

impl MempoolFixture {
    async fn open(env: &LiveTestEnv) -> Result<Self> {
        let network = env.network();
        let wallet_endpoint = serve_live_wallet_endpoint(env).await?;
        let explorer_adapter =
            ExplorerQueryGrpcAdapter::new(ExplorerServerInfoSettings { network })
                .with_wallet_query_endpoint(format!("http://{}", wallet_endpoint.addr));

        Ok(Self {
            network,
            explorer_adapter,
            wallet_endpoint,
        })
    }

    async fn summary(&self) -> Result<MempoolSummaryResponse> {
        let response = ExplorerQueryService::mempool_summary(
            &self.explorer_adapter,
            Request::new(MempoolSummaryRequest { at_epoch_id: None }),
        )
        .await?
        .into_inner();
        Ok(response)
    }

    async fn activity(&self, max_entries: u32) -> Result<MempoolActivityResponse> {
        self.activity_with_cursor(max_entries, Vec::new()).await
    }

    async fn snapshot(&self, max_entries: u32) -> Result<MempoolSnapshotResponse> {
        let response = ExplorerQueryService::mempool_snapshot(
            &self.explorer_adapter,
            Request::new(MempoolSnapshotRequest {
                max_entries,
                from_cursor: Vec::new(),
            }),
        )
        .await?
        .into_inner();
        Ok(response)
    }

    async fn activity_with_cursor(
        &self,
        max_entries: u32,
        from_cursor: Vec<u8>,
    ) -> Result<MempoolActivityResponse> {
        let response = ExplorerQueryService::mempool_activity(
            &self.explorer_adapter,
            Request::new(MempoolActivityRequest {
                max_entries,
                from_cursor,
            }),
        )
        .await?
        .into_inner();
        Ok(response)
    }

    async fn shutdown(&mut self) {
        self.wallet_endpoint.server.abort();
        let _ = (&mut self.wallet_endpoint.server).await;
    }
}

fn assert_summary_freshness(response: &MempoolSummaryResponse) -> Result<()> {
    let freshness = response
        .freshness
        .as_ref()
        .ok_or_else(|| eyre!("MempoolSummary response missing freshness"))?;
    assert_eq!(freshness.capability_version, EXPLORER_MEMPOOL_SUMMARY_V1);
    assert!(
        freshness
            .chain_view
            .as_ref()
            .and_then(|chain_view| chain_view.chain_epoch.as_ref())
            .is_some(),
        "summary freshness must carry a chain epoch",
    );
    // When the snapshot is empty the totals are zero and the privacy
    // distribution has no entries; both states are valid.
    if response.transaction_count > 0 {
        assert!(
            !response.privacy_shape_distribution.is_empty(),
            "non-empty mempool must populate the privacy-shape distribution",
        );
    }
    Ok(())
}

fn assert_activity_freshness(response: &MempoolActivityResponse) -> Result<()> {
    let freshness = response
        .freshness
        .as_ref()
        .ok_or_else(|| eyre!("MempoolActivity response missing freshness"))?;
    assert_eq!(freshness.capability_version, EXPLORER_MEMPOOL_ACTIVITY_V1);
    let mut prior_first_seen: Option<u64> = None;
    for entry in &response.entries {
        assert_eq!(
            entry.transaction_id.len(),
            32,
            "transaction_id must be 32 bytes (internal byte order)",
        );
        if let Some(prior) = prior_first_seen {
            assert!(
                entry.first_seen_unix_millis <= prior,
                "MempoolActivity must be ordered newest-first",
            );
        }
        assert!(
            entry.component_counts.is_some(),
            "a hydrated mempool entry must expose parsed component counts",
        );
        prior_first_seen = Some(entry.first_seen_unix_millis);
    }
    Ok(())
}

fn assert_snapshot_freshness(response: &MempoolSnapshotResponse) -> Result<()> {
    let freshness = response
        .freshness
        .as_ref()
        .ok_or_else(|| eyre!("MempoolSnapshot response missing freshness"))?;
    assert_eq!(freshness.capability_version, EXPLORER_MEMPOOL_SNAPSHOT_V1);
    let summary = response
        .summary
        .as_ref()
        .ok_or_else(|| eyre!("MempoolSnapshot response missing summary"))?;
    assert!(response.entries.len() <= usize::from(256_u16));
    assert!(summary.transaction_count >= u32::try_from(response.entries.len())?);
    Ok(())
}
