//! Canonical `human_reason` strings the explorer surface renders verbatim.
//!
//! Centralizes the prose the explorer plane returns alongside structured
//! refusal and unavailability reason codes. Every UI consuming the
//! `ExplorerQuery` surface either renders these strings directly or branches
//! on the structured reason while displaying its own copy. Concentrating the
//! prose here means a single PR changes the language for every consumer.
//!
//! Strings are English-only in v1 per
//! [ADR-0019](../../../docs/adrs/0019-explorer-plane.md);
//! per-locale rendering is a UI concern.

/// Sapling, Orchard, and Sprout addresses have no public history by
/// protocol design. Used for `ShieldedAddressMatch.not_publicly_indexable`.
pub const SHIELDED_RECEIVER_NO_HISTORY: &str =
    "Shielded receiver: no public history by protocol design";

/// Unified viewing keys and Sapling extended viewing keys are not indexed.
///
/// Echoing the key bytes alongside this string would itself be a privacy
/// regression, so `canonical_form` is omitted whenever this reason is used.
pub const VIEWING_KEY_NEVER_INDEXED: &str =
    "Viewing key: not indexed for public history; never decoded server-side";

/// A shielded receiver inside a unified address. Used for the per-receiver
/// `NotPubliclyIndexable` body when the enclosing `UnifiedAddressMatch` may
/// still route transparent receivers from the same input.
pub const SHIELDED_RECEIVER_IN_UNIFIED: &str =
    "Shielded receiver within unified address: no public history";

/// The receiver typecode is newer than this parser version recognizes.
///
/// Used as the `human_reason` when a unified-address receiver carries an
/// unknown typecode and routes to `UnifiedAddressReceiverKind::Unknown`.
pub const UNIFIED_RECEIVER_UNKNOWN_TYPECODE: &str =
    "Unified address receiver: typecode unsupported by this parser version";

/// Returned in the `spend_side_note` of every `TexAddressMatch`. ZIP-320
/// addresses constrain the sender to transparent inputs only; on chain the
/// output is indistinguishable from the underlying P2PKH.
pub const TEX_TRANSPARENT_SOURCE_ONLY: &str = "TEX address: transparent inputs only";

/// Sapling, Orchard, or Sprout address on the mainnet HRP family.
pub const SHIELDED_RECEIVER_MAINNET_NO_HISTORY: &str =
    "Shielded receiver (mainnet): no public history by protocol design";

/// Sapling, Orchard, or Sprout address on the testnet or regtest HRP family.
pub const SHIELDED_RECEIVER_TESTNET_NO_HISTORY: &str =
    "Shielded receiver (testnet/regtest): no public history by protocol design";

/// Unified address that decoded successfully but exposes no transparent
/// receiver, so the explorer has nothing publicly indexable to route to.
pub const UNIFIED_ADDRESS_NO_TRANSPARENT_RECEIVER: &str =
    "Unified address: no transparent receiver to route to public history";

/// An optional explorer field whose required wallet capability is absent.
///
/// Used with `UnavailableReason::UNAVAILABLE_UPSTREAM_NOT_SUPPORTED`; the
/// accompanying `UnavailableField.field_path` identifies the affected field.
pub const WALLET_QUERY_CAPABILITY_NOT_SUPPORTED: &str =
    "The wallet query does not support this field.";

/// Transparent output spend status the spend index cannot answer at the
/// response's chain epoch.
///
/// Used with `UnavailableReason::UNAVAILABLE_STALE` while the Explorer's
/// transparent-outpoint spend view does not cover the epoch the wallet plane
/// resolved, such as during view replay or while the view and the wallet
/// reader are one block apart at the tip.
pub const TRANSPARENT_OUTPUT_SPEND_STATUS_NOT_INDEXED: &str = "Transparent output spend status is unknown because the spend index does not cover this chain epoch.";
