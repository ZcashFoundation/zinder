//! Live federation tests for [`ExplorerQuery::Search`].
//!
//! Validates the typed search surface end to end against a real upstream
//! node: classification, wallet-side confirmation for hash and height
//! candidates, and the privacy refusal arms for shielded inputs. The
//! classifier in `zinder-core` is exercised by unit tests; this file
//! covers the federated handler in `services/zinder-explorer/src/grpc/search.rs`.
//! The wallet endpoint is the production serving pair built from genesis,
//! which only fits test budgets on regtest.

use eyre::{Result, eyre};
use tonic::Request;
use zebra_chain::block::Block as ZebraBlock;
use zebra_chain::serialization::ZcashDeserializeInto as _;
use zinder_core::wire::{
    encode_rpc_block_hash_hex, encode_rpc_transaction_id_hex, encode_zinder_native_chain_name,
};
use zinder_core::{BlockHash, BlockHeight, Network, TransactionId};
use zinder_explorer::{ExplorerQueryGrpcAdapter, ExplorerServerInfoSettings};
use zinder_proto::capabilities::EXPLORER_SEARCH_V1;
use zinder_proto::v1::explorer::{
    NotPubliclyIndexableReason, SearchRequest, SearchResponse,
    explorer_query_server::ExplorerQuery as ExplorerQueryService, search_candidate,
};
use zinder_source::{NodeSource as _, SourceBlock, ZebraJsonRpcSource, ZebraJsonRpcSourceOptions};
use zinder_testkit::live::{LiveTestEnv, init, require_live_for};

use crate::common::{LiveWalletEndpoint, serve_live_wallet_endpoint};
const REGTEST_TRANSPARENT_ADDRESS: &str = "tmDpFafuBHKGUYmuwLsrxWJrwcnSyzEEtYx";
const MAINNET_TRANSPARENT_ADDRESS: &str = "t1Hsc1LR8yKnbbe3twRp88p6vFfC5t7DLbs";
const MAINNET_SAPLING_ADDRESS: &str =
    "zs1z7rejlpsa98s2rrrfkwmaxu53e4ue0ulcrw0h4x5g8jl04tak0d3mm47vdtahatqrlkngh9slya";
const VIEWING_KEY_LIKE_PREFIX: &str = "uivk1examplenotarealkeyjustaprefix";

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn search_classifies_block_height_transaction_id_and_addresses() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live_for(&[Network::ZcashRegtest])? else {
        return Ok(());
    };
    let mut fixture = SearchFixture::open(&env).await?;
    let network = fixture.network;
    let tip = fixture.sample_block_height.value();

    assert_block_height_resolves(&fixture, tip).await?;
    assert_block_hash_resolves(&fixture).await?;
    assert_transaction_id_resolves(&fixture, tip).await?;
    assert_transparent_address_resolves(&fixture, network).await?;

    tracing::info!(
        target: "zinder::live",
        event = "search_classifies_validated",
        network = %encode_zinder_native_chain_name(network),
        sampled_height = tip,
        "explorer search validated against live node",
    );

    fixture.shutdown().await;
    Ok(())
}

async fn assert_block_height_resolves(fixture: &SearchFixture, tip: u32) -> Result<()> {
    let response = fixture.search(&tip.to_string()).await?;
    assert_freshness(&response)?;
    let block_candidate = find_block_match(&response)
        .ok_or_else(|| eyre!("expected BlockMatch arm for height {tip}"))?;
    assert_eq!(block_candidate.block_height, tip);
    assert_eq!(
        block_candidate.block_hash,
        encode_rpc_block_hash_hex(fixture.sample_block_hash)
    );
    Ok(())
}

async fn assert_block_hash_resolves(fixture: &SearchFixture) -> Result<()> {
    let query = encode_rpc_block_hash_hex(fixture.sample_block_hash);
    let response = fixture.search(&query).await?;
    assert!(
        find_block_match(&response).is_some(),
        "expected BlockMatch arm for block hash {query}",
    );
    Ok(())
}

async fn assert_transaction_id_resolves(fixture: &SearchFixture, tip: u32) -> Result<()> {
    let query = encode_rpc_transaction_id_hex(fixture.sample_coinbase_id);
    let response = fixture.search(&query).await?;
    let transaction = find_transaction_match(&response)
        .ok_or_else(|| eyre!("expected TransactionMatch arm for coinbase {query}"))?;
    assert!(!transaction.in_mempool, "coinbase is mined");
    assert_eq!(transaction.mined_block_height, tip);
    Ok(())
}

async fn assert_transparent_address_resolves(
    fixture: &SearchFixture,
    network: Network,
) -> Result<()> {
    let query = transparent_query_for_network(network)?;
    let response = fixture.search(query).await?;
    let transparent = find_transparent_match(&response)
        .ok_or_else(|| eyre!("expected TransparentAddress arm for {query}"))?;
    assert_eq!(transparent.canonical_form, query);
    assert!(transparent.is_p2pkh);
    Ok(())
}

fn find_block_match(response: &SearchResponse) -> Option<&zinder_proto::v1::explorer::BlockMatch> {
    response.candidates.iter().find_map(|candidate| {
        if let Some(search_candidate::Match::Block(block_match)) = candidate.r#match.as_ref() {
            Some(block_match)
        } else {
            None
        }
    })
}

fn find_transaction_match(
    response: &SearchResponse,
) -> Option<&zinder_proto::v1::explorer::TransactionMatch> {
    response.candidates.iter().find_map(|candidate| {
        if let Some(search_candidate::Match::Transaction(transaction_match)) =
            candidate.r#match.as_ref()
        {
            Some(transaction_match)
        } else {
            None
        }
    })
}

fn find_transparent_match(
    response: &SearchResponse,
) -> Option<&zinder_proto::v1::explorer::TransparentAddressMatch> {
    response.candidates.iter().find_map(|candidate| {
        if let Some(search_candidate::Match::TransparentAddress(transparent_match)) =
            candidate.r#match.as_ref()
        {
            Some(transparent_match)
        } else {
            None
        }
    })
}

fn find_shielded_match(
    response: &SearchResponse,
) -> Option<&zinder_proto::v1::explorer::ShieldedAddressMatch> {
    response.candidates.iter().find_map(|candidate| {
        if let Some(search_candidate::Match::ShieldedAddress(shielded_match)) =
            candidate.r#match.as_ref()
        {
            Some(shielded_match)
        } else {
            None
        }
    })
}

fn find_viewing_key_match(
    response: &SearchResponse,
) -> Option<&zinder_proto::v1::explorer::ViewingKeyMatch> {
    response.candidates.iter().find_map(|candidate| {
        if let Some(search_candidate::Match::ViewingKey(viewing_key_match)) =
            candidate.r#match.as_ref()
        {
            Some(viewing_key_match)
        } else {
            None
        }
    })
}

fn response_routes_to_public_history_arm(response: &SearchResponse) -> bool {
    response.candidates.iter().any(|candidate| {
        matches!(
            candidate.r#match.as_ref(),
            Some(
                search_candidate::Match::TransparentAddress(_)
                    | search_candidate::Match::TexAddress(_)
                    | search_candidate::Match::UnifiedAddress(_)
                    | search_candidate::Match::Block(_)
                    | search_candidate::Match::Transaction(_),
            )
        )
    })
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live test; see CLAUDE.md §Live Node Tests"]
async fn search_refuses_shielded_address_and_viewing_key_without_canonical_form() -> Result<()> {
    let _guard = init();
    let Some(env) = require_live_for(&[Network::ZcashRegtest])? else {
        return Ok(());
    };
    let mut fixture = SearchFixture::open(&env).await?;
    let shielded_response = fixture.search(MAINNET_SAPLING_ADDRESS).await?;
    if let Some(shielded_match) = find_shielded_match(&shielded_response) {
        let refusal = shielded_match
            .not_publicly_indexable
            .as_ref()
            .ok_or_else(|| eyre!("ShieldedAddressMatch missing NotPubliclyIndexable body"))?;
        let reason = NotPubliclyIndexableReason::try_from(refusal.reason)
            .map_err(|_| eyre!("unknown NotPubliclyIndexableReason code {}", refusal.reason))?;
        assert!(
            matches!(
                reason,
                NotPubliclyIndexableReason::ShieldedAddress
                    | NotPubliclyIndexableReason::ShieldedAddressMainnet
                    | NotPubliclyIndexableReason::ShieldedAddressTestnet,
            ),
            "expected a shielded-address refusal variant, got {reason:?}",
        );
        assert!(
            !refusal.human_reason.is_empty(),
            "human_reason must carry the canonical refusal string",
        );
    } else {
        // Mainnet Sapling does not parse on non-mainnet networks; the
        // classifier surfaces an Unclassified hint instead. Either
        // outcome is privacy-preserving (no probe, no echoed key).
        assert!(
            !response_routes_to_public_history_arm(&shielded_response),
            "shielded-address input must never route to a public-history arm",
        );
    }

    let viewing_response = fixture.search(VIEWING_KEY_LIKE_PREFIX).await?;
    let viewing_key_match = find_viewing_key_match(&viewing_response)
        .ok_or_else(|| eyre!("expected ViewingKeyMatch arm for {VIEWING_KEY_LIKE_PREFIX}"))?;
    let viewing_refusal = viewing_key_match
        .not_publicly_indexable
        .as_ref()
        .ok_or_else(|| eyre!("ViewingKeyMatch missing NotPubliclyIndexable body"))?;
    assert_eq!(
        viewing_refusal.reason,
        NotPubliclyIndexableReason::ViewingKey as i32,
    );
    assert!(
        viewing_refusal.canonical_form.is_none(),
        "viewing keys must never round-trip through canonical_form",
    );
    assert!(
        !response_routes_to_public_history_arm(&viewing_response),
        "viewing-key input must never route to a public-history arm",
    );

    fixture.shutdown().await;
    Ok(())
}

fn transparent_query_for_network(network: Network) -> Result<&'static str> {
    match network {
        Network::ZcashMainnet => Ok(MAINNET_TRANSPARENT_ADDRESS),
        Network::ZcashTestnet | Network::ZcashRegtest => Ok(REGTEST_TRANSPARENT_ADDRESS),
        other => Err(eyre!(
            "transparent_query_for_network called with unsupported network: {other:?}"
        )),
    }
}

struct SearchFixture {
    network: Network,
    sample_block_height: BlockHeight,
    sample_block_hash: BlockHash,
    sample_coinbase_id: TransactionId,
    explorer_adapter: ExplorerQueryGrpcAdapter,
    wallet_endpoint: LiveWalletEndpoint,
}

impl SearchFixture {
    async fn open(env: &LiveTestEnv) -> Result<Self> {
        let network = env.network();
        let wallet_endpoint = serve_live_wallet_endpoint(env).await?;
        let sample = sample_block_at(env, wallet_endpoint.tip_height).await?;
        let explorer_adapter =
            ExplorerQueryGrpcAdapter::new(ExplorerServerInfoSettings { network })
                .with_wallet_query_endpoint(format!("http://{}", wallet_endpoint.addr));

        Ok(Self {
            network,
            sample_block_height: sample.block_height,
            sample_block_hash: sample.block_hash,
            sample_coinbase_id: sample.coinbase_transaction_id,
            explorer_adapter,
            wallet_endpoint,
        })
    }

    async fn search(&self, query: &str) -> Result<SearchResponse> {
        let response = ExplorerQueryService::search(
            &self.explorer_adapter,
            Request::new(SearchRequest {
                query: query.to_owned(),
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

fn assert_freshness(response: &SearchResponse) -> Result<()> {
    let freshness = response
        .freshness
        .as_ref()
        .ok_or_else(|| eyre!("search response missing freshness"))?;
    assert_eq!(freshness.capability_version, EXPLORER_SEARCH_V1);
    assert!(
        freshness
            .chain_view
            .as_ref()
            .and_then(|chain_view| chain_view.chain_epoch.as_ref())
            .is_some(),
        "search freshness must carry a chain epoch",
    );
    Ok(())
}

struct SampleBlock {
    block_height: BlockHeight,
    block_hash: BlockHash,
    coinbase_transaction_id: TransactionId,
}

async fn sample_block_at(env: &LiveTestEnv, height: BlockHeight) -> Result<SampleBlock> {
    let source = ZebraJsonRpcSource::with_options(
        env.target.network,
        &env.target.json_rpc_addr,
        env.target.node_auth.clone(),
        ZebraJsonRpcSourceOptions {
            request_timeout: env.target.request_timeout,
            max_response_bytes: env.target.max_response_bytes,
            broadcast_timeout: None,
        },
    )?;
    sample_tip(&source.fetch_block_at(height).await?)
}

fn sample_tip(block: &SourceBlock) -> Result<SampleBlock> {
    let parsed: ZebraBlock = block.raw_block_bytes.as_slice().zcash_deserialize_into()?;
    let coinbase = parsed
        .transactions
        .first()
        .ok_or_else(|| eyre!("tip block has no coinbase transaction"))?;
    Ok(SampleBlock {
        block_height: block.height,
        block_hash: block.hash,
        coinbase_transaction_id: TransactionId::from_bytes(coinbase.hash().0),
    })
}
