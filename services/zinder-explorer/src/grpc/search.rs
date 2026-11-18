//! `ExplorerQuery.Search` handler.
//!
//! Routes a raw user input string through the pure-input classifier in
//! [`zinder_core::explorer_search`], confirms non-shielded candidates
//! against the upstream wallet plane, and wraps the typed candidates in
//! the cross-cutting [`ExplorerFreshness`] envelope per
//! [ADR-0019](../../../docs/adrs/0019-explorer-plane.md).
//!
//! The handler enforces the privacy invariant from
//! [ADR-0019](../../../docs/adrs/0019-explorer-plane.md):
//! shielded receivers, viewing keys, and shielded receivers inside a
//! unified address never reach a `WalletQuery` call. The wallet client
//! is only consulted to disambiguate `HashCandidate` arms and confirm
//! `Block { height }` candidates against the live tip.

use tonic::{Request, Response, Status};
use zinder_core::explorer_reasons::{
    SHIELDED_RECEIVER_IN_UNIFIED, SHIELDED_RECEIVER_MAINNET_NO_HISTORY,
    SHIELDED_RECEIVER_TESTNET_NO_HISTORY, TEX_TRANSPARENT_SOURCE_ONLY,
    UNIFIED_ADDRESS_NO_TRANSPARENT_RECEIVER, UNIFIED_RECEIVER_UNKNOWN_TYPECODE,
    VIEWING_KEY_NEVER_INDEXED,
};
use zinder_core::explorer_search::{
    SearchClassification, ShieldedReceiverKind, TexAddressClassification,
    TransparentAddressClassification, UnifiedAddressClassification,
    UnifiedAddressReceiverClassification, classify_search_input,
};
use zinder_core::{Network, wire::encode_zinder_native_chain_name};
use zinder_proto::capabilities::EXPLORER_SEARCH_V1;
use zinder_proto::v1::explorer::{
    BlockMatch, ExplorerFreshness, NotPubliclyIndexable, NotPubliclyIndexableReason,
    SearchCandidate, SearchRequest, SearchResponse, ShieldedAddressMatch, TexAddressMatch,
    TransactionMatch, TransparentAddressMatch, UnclassifiedMatch, UnifiedAddressMatch,
    UnifiedAddressReceiver, UnifiedAddressReceiverKind, ViewingKeyMatch, search_candidate,
    unified_address_receiver,
};
use zinder_proto::v1::wallet::{
    self, BlockSelector, VisibleTipBlockRequest, block_selector, transaction_location,
    wallet_query_client::WalletQueryClient,
};
use zinder_runtime::AuthenticatedChannel;

use super::error::ExplorerError;
use super::freshness::{
    UpstreamObservationCache, attach_upstream_observation, build_explorer_freshness,
};
use zinder_materialized_views::MaterializedViewStore;

const CONFIDENCE_HIGH: f32 = 1.0;
const CONFIDENCE_AMBIGUOUS: f32 = 0.5;

/// Executes one `ExplorerQuery.Search` request.
pub(crate) async fn query_search(
    materialized_view_store: Option<&MaterializedViewStore>,
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    network: Network,
    upstream_observation_cache: &UpstreamObservationCache,
    request: Request<SearchRequest>,
) -> Result<Response<SearchResponse>, Status> {
    let inner = request.into_inner();
    let classifications = classify_search_input(&inner.query, network);
    let mut candidates = Vec::with_capacity(classifications.len());
    for classification in classifications {
        candidates.extend(project_classification(wallet_client, network, classification).await?);
    }
    let freshness = attach_upstream_observation(
        upstream_observation_cache,
        build_freshness(materialized_view_store, wallet_client).await?,
    )
    .await;
    Ok(Response::new(SearchResponse {
        freshness: Some(freshness),
        candidates,
    }))
}

async fn project_classification(
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    network: Network,
    classification: SearchClassification,
) -> Result<Vec<SearchCandidate>, Status> {
    match classification {
        SearchClassification::Block { height } => probe_block_height(wallet_client, height).await,
        SearchClassification::HashCandidate {
            rpc_hex,
            internal_bytes: _,
        } => probe_hash_candidate(wallet_client, rpc_hex).await,
        SearchClassification::TransparentAddress(classification) => Ok(vec![candidate_with_match(
            search_candidate::Match::TransparentAddress(transparent_match(&classification)),
            CONFIDENCE_HIGH,
        )]),
        SearchClassification::TexAddress(classification) => Ok(vec![candidate_with_match(
            search_candidate::Match::TexAddress(tex_match(&classification)),
            CONFIDENCE_HIGH,
        )]),
        SearchClassification::UnifiedAddress(classification) => {
            let has_transparent = classification.receivers.iter().any(|receiver| {
                matches!(
                    receiver,
                    zinder_core::explorer_search::UnifiedAddressReceiverClassification::Transparent(
                        _
                    )
                )
            });
            let mut candidates = vec![candidate_with_match(
                search_candidate::Match::UnifiedAddress(unified_match(
                    network,
                    classification.clone(),
                )),
                CONFIDENCE_HIGH,
            )];
            if !has_transparent {
                candidates.push(candidate_with_match(
                    search_candidate::Match::ShieldedAddress(ShieldedAddressMatch {
                        not_publicly_indexable: Some(NotPubliclyIndexable {
                            reason: NotPubliclyIndexableReason::UnifiedAddressNoTransparentReceiver
                                as i32,
                            human_reason: UNIFIED_ADDRESS_NO_TRANSPARENT_RECEIVER.to_owned(),
                            canonical_form: Some(classification.canonical_form),
                        }),
                    }),
                    CONFIDENCE_HIGH,
                ));
            }
            Ok(candidates)
        }
        SearchClassification::ShieldedAddress {
            canonical,
            network: address_network,
        } => Ok(vec![candidate_with_match(
            search_candidate::Match::ShieldedAddress(ShieldedAddressMatch {
                not_publicly_indexable: Some(NotPubliclyIndexable {
                    reason: shielded_address_reason(address_network) as i32,
                    human_reason: shielded_address_human_reason(address_network).to_owned(),
                    canonical_form: Some(canonical),
                }),
            }),
            CONFIDENCE_HIGH,
        )]),
        SearchClassification::ViewingKey => Ok(vec![candidate_with_match(
            search_candidate::Match::ViewingKey(ViewingKeyMatch {
                not_publicly_indexable: Some(NotPubliclyIndexable {
                    reason: NotPubliclyIndexableReason::ViewingKey as i32,
                    human_reason: VIEWING_KEY_NEVER_INDEXED.to_owned(),
                    canonical_form: None,
                }),
            }),
            CONFIDENCE_HIGH,
        )]),
        SearchClassification::Unclassified { hint } => Ok(vec![candidate_with_match(
            search_candidate::Match::Unclassified(UnclassifiedMatch { hint }),
            CONFIDENCE_HIGH,
        )]),
    }
}

async fn probe_block_height(
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    height: u32,
) -> Result<Vec<SearchCandidate>, Status> {
    let response = wallet_client
        .block_id_by_selector(Request::new(wallet::BlockSelectorRequest {
            selector: Some(BlockSelector {
                selector: Some(block_selector::Selector::Height(height)),
            }),
            at_epoch_id: None,
        }))
        .await;
    match response {
        Ok(envelope) => match envelope.into_inner().block_id {
            Some(block_id) => Ok(vec![candidate_with_match(
                search_candidate::Match::Block(BlockMatch {
                    block_height: block_id.height,
                    block_hash: block_id.block_hash,
                }),
                CONFIDENCE_HIGH,
            )]),
            None => Ok(Vec::new()),
        },
        Err(status) if status.code() == tonic::Code::NotFound => Ok(Vec::new()),
        Err(status) => Err(status),
    }
}

async fn probe_hash_candidate(
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    rpc_hex: String,
) -> Result<Vec<SearchCandidate>, Status> {
    let mut candidates = Vec::new();
    if let Some(block_id) = resolve_block_by_hash(wallet_client, &rpc_hex).await? {
        candidates.push(candidate_with_match(
            search_candidate::Match::Block(BlockMatch {
                block_height: block_id.height,
                block_hash: block_id.block_hash,
            }),
            CONFIDENCE_AMBIGUOUS,
        ));
    }
    if let Some(transaction_match) =
        resolve_transaction_by_hash(wallet_client, rpc_hex.clone()).await?
    {
        candidates.push(candidate_with_match(
            search_candidate::Match::Transaction(transaction_match),
            CONFIDENCE_AMBIGUOUS,
        ));
    }
    if candidates.len() == 1
        && let Some(only) = candidates.first_mut()
    {
        only.confidence = CONFIDENCE_HIGH;
    }
    Ok(candidates)
}

async fn resolve_block_by_hash(
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    rpc_hex: &str,
) -> Result<Option<wallet::BlockId>, Status> {
    match wallet_client
        .block_id_by_selector(Request::new(wallet::BlockSelectorRequest {
            selector: Some(BlockSelector {
                selector: Some(block_selector::Selector::Hash(rpc_hex.to_owned())),
            }),
            at_epoch_id: None,
        }))
        .await
    {
        Ok(response) => Ok(response.into_inner().block_id),
        Err(status) if status.code() == tonic::Code::NotFound => Ok(None),
        Err(status) => Err(status),
    }
}

async fn resolve_transaction_by_hash(
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
    rpc_hex: String,
) -> Result<Option<TransactionMatch>, Status> {
    let response = wallet_client
        .transaction(Request::new(wallet::TransactionRequest {
            transaction_id: rpc_hex.clone(),
            at_epoch_id: None,
        }))
        .await;
    match response {
        Ok(envelope) => Ok(envelope
            .into_inner()
            .location
            .and_then(|location| location.location)
            .and_then(|location| build_transaction_match(rpc_hex, location))),
        Err(status) if status.code() == tonic::Code::NotFound => Ok(None),
        Err(status) => Err(status),
    }
}

fn build_transaction_match(
    rpc_hex: String,
    location: transaction_location::Location,
) -> Option<TransactionMatch> {
    match location {
        transaction_location::Location::Mined(mined) => {
            let block_location = mined.location?;
            Some(TransactionMatch {
                transaction_id: rpc_hex,
                in_mempool: false,
                mined_block_height: block_location.block_height,
                mined_block_hash: block_location.block_hash,
            })
        }
        transaction_location::Location::InMempool(_) => Some(TransactionMatch {
            transaction_id: rpc_hex,
            in_mempool: true,
            mined_block_height: 0,
            mined_block_hash: String::new(),
        }),
    }
}

fn transparent_match(classification: &TransparentAddressClassification) -> TransparentAddressMatch {
    TransparentAddressMatch {
        canonical_form: classification.canonical_form.clone(),
        address_script_hash: classification.address_script_hash.as_bytes().to_vec(),
        is_p2pkh: classification.is_p2pkh,
    }
}

fn tex_match(classification: &TexAddressClassification) -> TexAddressMatch {
    TexAddressMatch {
        canonical_tex_form: classification.canonical_tex_form.clone(),
        equivalent_p2pkh_form: classification.equivalent_p2pkh_form.clone(),
        transparent: Some(transparent_match(&classification.transparent)),
        transparent_source_only: true,
        spend_side_note: TEX_TRANSPARENT_SOURCE_ONLY.to_owned(),
    }
}

fn unified_match(
    network: Network,
    classification: UnifiedAddressClassification,
) -> UnifiedAddressMatch {
    UnifiedAddressMatch {
        canonical_form: classification.canonical_form,
        network: encode_zinder_native_chain_name(network).to_owned(),
        receivers: classification
            .receivers
            .into_iter()
            .map(unified_receiver)
            .collect(),
    }
}

fn unified_receiver(receiver: UnifiedAddressReceiverClassification) -> UnifiedAddressReceiver {
    match receiver {
        UnifiedAddressReceiverClassification::Transparent(classification) => {
            let kind = if classification.is_p2pkh {
                UnifiedAddressReceiverKind::P2pkh
            } else {
                UnifiedAddressReceiverKind::P2sh
            };
            UnifiedAddressReceiver {
                kind: kind as i32,
                body: Some(unified_address_receiver::Body::Transparent(
                    transparent_match(&classification),
                )),
            }
        }
        UnifiedAddressReceiverClassification::Shielded { kind } => {
            let receiver_kind = match kind {
                ShieldedReceiverKind::Sapling => UnifiedAddressReceiverKind::Sapling,
                ShieldedReceiverKind::Orchard => UnifiedAddressReceiverKind::Orchard,
            };
            UnifiedAddressReceiver {
                kind: receiver_kind as i32,
                body: Some(unified_address_receiver::Body::Shielded(
                    NotPubliclyIndexable {
                        reason: NotPubliclyIndexableReason::ShieldedReceiverInUnified as i32,
                        human_reason: SHIELDED_RECEIVER_IN_UNIFIED.to_owned(),
                        canonical_form: None,
                    },
                )),
            }
        }
        UnifiedAddressReceiverClassification::Unknown { typecode } => UnifiedAddressReceiver {
            kind: UnifiedAddressReceiverKind::Unknown as i32,
            body: Some(unified_address_receiver::Body::Shielded(
                NotPubliclyIndexable {
                    reason: NotPubliclyIndexableReason::ShieldedReceiverInUnified as i32,
                    human_reason: format!(
                        "{UNIFIED_RECEIVER_UNKNOWN_TYPECODE} (typecode 0x{typecode:02x})"
                    ),
                    canonical_form: None,
                },
            )),
        },
    }
}

const fn candidate_with_match(
    match_arm: search_candidate::Match,
    confidence: f32,
) -> SearchCandidate {
    SearchCandidate {
        r#match: Some(match_arm),
        confidence,
    }
}

#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "Network is non-exhaustive; any future variant must default to the privacy-conservative testnet vocabulary"
)]
const fn shielded_address_reason(network: zinder_core::Network) -> NotPubliclyIndexableReason {
    match network {
        zinder_core::Network::ZcashMainnet => NotPubliclyIndexableReason::ShieldedAddressMainnet,
        _ => NotPubliclyIndexableReason::ShieldedAddressTestnet,
    }
}

#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "Network is non-exhaustive; any future variant must default to the privacy-conservative testnet vocabulary"
)]
const fn shielded_address_human_reason(network: zinder_core::Network) -> &'static str {
    match network {
        zinder_core::Network::ZcashMainnet => SHIELDED_RECEIVER_MAINNET_NO_HISTORY,
        _ => SHIELDED_RECEIVER_TESTNET_NO_HISTORY,
    }
}

async fn build_freshness(
    materialized_view_store: Option<&MaterializedViewStore>,
    wallet_client: &mut WalletQueryClient<AuthenticatedChannel>,
) -> Result<ExplorerFreshness, Status> {
    let chain_epoch = wallet_client
        .visible_tip_block(Request::new(VisibleTipBlockRequest { at_epoch_id: None }))
        .await?
        .into_inner()
        .chain_view
        .and_then(|chain_view| chain_view.chain_epoch)
        .ok_or_else(|| {
            ExplorerError::internal("VisibleTipBlockResponse.chain_view.chain_epoch missing")
        })?;
    build_explorer_freshness(
        materialized_view_store,
        EXPLORER_SEARCH_V1,
        Some(chain_epoch),
        0,
    )
}
