//! Typed classifier for `ExplorerQuery.Search` raw user input.
//!
//! Owns the privacy-critical decision of which input forms route to a
//! refusal arm before any storage read. The classifier is a pure function
//! of `(query, network)`; it never touches the canonical store, the
//! wallet plane, or the network. Privacy-refusing arms
//! ([`SearchClassification::ShieldedAddress`],
//! [`SearchClassification::ViewingKey`], shielded receivers inside
//! [`SearchClassification::UnifiedAddress`]) short-circuit before the
//! handler in `zinder-explorer` issues any `WalletQuery` lookup, which is
//! the structural invariant required by
//! [ADR-0012](../../../docs/adrs/0012-typed-explorer-search-and-privacy-refusal.md).
//!
//! The handler in `services/zinder-explorer/src/grpc/search.rs` composes
//! the classifier output with optional `WalletQuery` confirmations:
//! [`SearchClassification::Block`] and
//! [`SearchClassification::HashCandidate`] route to existence probes;
//! transparent and unified-address arms route through without probing,
//! because address existence on chain is "may or may not have history,"
//! not "does not exist."
//!
//! The classifier deliberately rejects empty and oversized queries. The
//! caller is expected to wrap the response in
//! [`SEARCH_QUERY_MAX_BYTES`]-bounded transport limits as well.

use crate::wire::decode_rpc_transaction_id_hex;
use crate::{Network, TransparentAddressScriptHash};
use zcash_address::unified::{Container as _, Receiver as UnifiedReceiver, Uitem};
use zcash_address::{
    ConversionError, ParseError as ZcashAddressParseError, ToAddress, TryFromAddress, ZcashAddress,
    unified::Address as UnifiedAddressData,
};
use zcash_protocol::consensus::NetworkType as ZcashNetworkType;

/// Hard cap on the raw query length the classifier accepts.
///
/// Bounds the worst-case Bech32m / base58check decode cost a single
/// request can trigger. The cap is generous enough to fit the longest
/// currently supported Unified Address (well under 1 KiB).
pub const SEARCH_QUERY_MAX_BYTES: usize = 4 * 1024;

/// One structural classification of the raw query.
///
/// The caller decides what confidence value to attach when projecting
/// this onto the wire `SearchCandidate.confidence` field; a value of
/// `1.0` is appropriate for unambiguous arms and `0.5` for ambiguous
/// cases such as a 64-character hex string that could be either a block
/// hash or a transaction id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SearchClassification {
    /// Numeric input that parses cleanly as a `u32` block height.
    Block {
        /// Parsed block height.
        height: u32,
    },

    /// 64-character lowercase or uppercase hex the user pasted in
    /// canonical RPC byte order (matching `zcash-cli`, wallets, and every
    /// block explorer). The handler resolves whether it names a block or
    /// a transaction by probing both `WalletQuery.BlockIdBySelector` and
    /// `WalletQuery.Transaction`.
    HashCandidate {
        /// User-provided hex in RPC byte order, lowercase, exactly 64
        /// characters. Forwarded verbatim to the upstream wallet plane
        /// (which expects RPC-form strings on its hash-shaped fields per
        /// ADR-0021).
        rpc_hex: String,
        /// Decoded 32 bytes in internal byte order, ready for storage
        /// lookups and round-trip validation through
        /// [`decode_rpc_transaction_id_hex`].
        internal_bytes: [u8; 32],
    },

    /// Transparent P2PKH or P2SH address that decoded against the
    /// configured `network`.
    TransparentAddress(TransparentAddressClassification),

    /// ZIP-320 TEX address. The classifier extracts the underlying P2PKH
    /// hash so the handler can echo both the canonical `tex*` form and
    /// the equivalent `t*` form alongside a routable transparent match.
    TexAddress(TexAddressClassification),

    /// ZIP-316 Unified address whose receiver typecodes are returned in
    /// declared order. Transparent receivers carry their own
    /// [`TransparentAddressClassification`]; shielded receivers carry
    /// the typed refusal marker.
    UnifiedAddress(UnifiedAddressClassification),

    /// Sapling, Orchard, or Sprout shielded address. The classifier
    /// echoes the canonical form back; the handler routes it to the
    /// typed refusal arm without any storage read.
    ShieldedAddress {
        /// Canonical re-encoding of the input the handler may echo.
        canonical: String,
        /// Network the address belongs to, used by the handler to pick
        /// the mainnet vs testnet refusal vocabulary.
        network: Network,
    },

    /// Viewing key (UIVK, UFVK, or Sapling extended viewing key). The
    /// canonical form is intentionally not surfaced; echoing the key
    /// bytes would be a privacy regression even when the server never
    /// persists them.
    ViewingKey,

    /// Input the classifier could not route to any other arm. The hint
    /// is the operator-readable explanation a UI may render verbatim.
    Unclassified {
        /// Short reason explaining what was expected.
        hint: String,
    },
}

/// Structured transparent-address classification carried by both the
/// transparent and TEX arms.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransparentAddressClassification {
    /// Canonical base58check form of the address.
    pub canonical_form: String,

    /// SHA-256 of the canonical scriptPubKey.
    pub address_script_hash: TransparentAddressScriptHash,

    /// `true` for P2PKH receivers; `false` for P2SH.
    pub is_p2pkh: bool,
}

/// Structured TEX classification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TexAddressClassification {
    /// Canonical Bech32m `tex1.../textest1...` re-encoding.
    pub canonical_tex_form: String,

    /// Equivalent `t1.../tm...` P2PKH re-encoding routable for history.
    pub equivalent_p2pkh_form: String,

    /// Underlying P2PKH classification the handler routes for indexable
    /// history.
    pub transparent: TransparentAddressClassification,
}

/// Structured ZIP-316 unified-address classification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnifiedAddressClassification {
    /// Canonical Bech32m re-encoding of the input.
    pub canonical_form: String,

    /// Network the address belongs to, matching the configured
    /// [`Network`] when the address parsed.
    pub network: Network,

    /// Per-receiver routing in the order ZIP-316 emitted them.
    pub receivers: Vec<UnifiedAddressReceiverClassification>,
}

/// One receiver inside a [`UnifiedAddressClassification`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UnifiedAddressReceiverClassification {
    /// P2PKH or P2SH transparent receiver inside the UA. Routable for
    /// public history through the equivalent transparent-address page.
    Transparent(TransparentAddressClassification),

    /// Sapling or Orchard shielded receiver. The handler routes this to
    /// `NotPubliclyIndexable` without any storage read.
    Shielded {
        /// Discriminates Sapling and Orchard so the UI can render the
        /// correct receiver chip; both route to the same refusal.
        kind: ShieldedReceiverKind,
    },

    /// Receiver typecode the classifier does not recognize. Surfaces as
    /// a typed unknown rather than an `Unclassified` because the
    /// enclosing UA still parsed successfully.
    Unknown {
        /// Raw ZIP-316 typecode of the unknown receiver.
        typecode: u32,
    },
}

/// Shielded receiver kind inside a unified address.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShieldedReceiverKind {
    /// Sapling receiver (typecode `0x02`).
    Sapling,
    /// Orchard receiver (typecode `0x03`).
    Orchard,
}

/// Classifies a raw query string into typed candidates.
///
/// Returns one or more [`SearchClassification`] entries in ascending
/// preference order. Empty `query` returns a single
/// [`SearchClassification::Unclassified`]; oversized `query` is rejected
/// before any decode attempt.
#[must_use]
pub fn classify_search_input(query: &str, network: Network) -> Vec<SearchClassification> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return vec![SearchClassification::Unclassified {
            hint: "empty query; expected block height, transaction id, or supported address form"
                .to_owned(),
        }];
    }
    if trimmed.len() > SEARCH_QUERY_MAX_BYTES {
        return vec![SearchClassification::Unclassified {
            hint: format!(
                "query length {} exceeds the per-request cap of {SEARCH_QUERY_MAX_BYTES} bytes",
                trimmed.len()
            ),
        }];
    }

    let mut candidates = Vec::new();

    if let Some(height) = parse_height(trimmed) {
        candidates.push(SearchClassification::Block { height });
    }

    if let Some(candidate) = parse_rpc_hash_hex(trimmed) {
        candidates.push(candidate);
    }

    if let Some(address_classification) = classify_zcash_address(trimmed, network) {
        candidates.push(address_classification);
    }

    if candidates.is_empty() {
        candidates.push(SearchClassification::Unclassified {
            hint: "could not classify; expected block height, transaction id, or supported \
                   address form"
                .to_owned(),
        });
    }
    candidates
}

fn parse_height(query: &str) -> Option<u32> {
    if !query.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    query.parse::<u32>().ok()
}

fn parse_rpc_hash_hex(query: &str) -> Option<SearchClassification> {
    if query.len() != 64 || !query.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    // Decode through the wire helper so the result is in internal byte
    // order, matching the storage form. Both txid and block-hash helpers
    // share the same 32-byte RPC-form decoding; pick either, but use the
    // txid one since the classifier itself does not yet know which arm
    // the hex will resolve to.
    let transaction_id = decode_rpc_transaction_id_hex(query).ok()?;
    Some(SearchClassification::HashCandidate {
        rpc_hex: query.to_ascii_lowercase(),
        internal_bytes: transaction_id.as_bytes(),
    })
}

/// HRPs the classifier treats as viewing-key forms. Detection is by
/// prefix only; the classifier never decodes the body because echoing a
/// viewing key on the response surface would be a privacy regression.
const VIEWING_KEY_HRPS: &[&str] = &[
    "uivk",
    "uivktest",
    "uivkregtest",
    "uview",
    "uviewtest",
    "uviewregtest",
    "zxviews",
    "zxviewtestsapling",
    "zviews",
    "zviewtestsapling",
];

fn classify_zcash_address(query: &str, network: Network) -> Option<SearchClassification> {
    match ZcashAddress::try_from_encoded(query) {
        Ok(address) => address
            .convert::<ClassifiedAddress>()
            .ok()
            .map(|classified| classified.into_classification(query, network)),
        Err(ZcashAddressParseError::NotZcash) => detect_viewing_key(query),
        Err(_) => detect_viewing_key(query).or_else(|| {
            Some(SearchClassification::Unclassified {
                hint: "input matched a Zcash address encoding but the body did not decode cleanly"
                    .to_owned(),
            })
        }),
    }
}

fn detect_viewing_key(query: &str) -> Option<SearchClassification> {
    let lower = query.to_ascii_lowercase();
    VIEWING_KEY_HRPS
        .iter()
        .any(|prefix| lower.starts_with(prefix))
        .then_some(SearchClassification::ViewingKey)
}

/// Intermediate value the `TryFromAddress` impl produces; we re-encode
/// the address using the canonical Zcash encoders inside
/// `into_classification` to populate the canonical-form fields.
enum ClassifiedAddress {
    Sprout {
        net: ZcashNetworkType,
        bytes: [u8; 64],
    },
    Sapling {
        net: ZcashNetworkType,
        bytes: [u8; 43],
    },
    Unified {
        net: ZcashNetworkType,
        unified: UnifiedAddressData,
    },
    P2pkh {
        net: ZcashNetworkType,
        hash160: [u8; 20],
    },
    P2sh {
        net: ZcashNetworkType,
        hash160: [u8; 20],
    },
    Tex {
        net: ZcashNetworkType,
        hash160: [u8; 20],
    },
}

impl TryFromAddress for ClassifiedAddress {
    type Error = core::convert::Infallible;

    fn try_from_sprout(
        net: ZcashNetworkType,
        bytes: [u8; 64],
    ) -> Result<Self, ConversionError<Self::Error>> {
        Ok(Self::Sprout { net, bytes })
    }

    fn try_from_sapling(
        net: ZcashNetworkType,
        bytes: [u8; 43],
    ) -> Result<Self, ConversionError<Self::Error>> {
        Ok(Self::Sapling { net, bytes })
    }

    fn try_from_unified(
        net: ZcashNetworkType,
        unified: UnifiedAddressData,
    ) -> Result<Self, ConversionError<Self::Error>> {
        Ok(Self::Unified { net, unified })
    }

    fn try_from_transparent_p2pkh(
        net: ZcashNetworkType,
        hash160: [u8; 20],
    ) -> Result<Self, ConversionError<Self::Error>> {
        Ok(Self::P2pkh { net, hash160 })
    }

    fn try_from_transparent_p2sh(
        net: ZcashNetworkType,
        hash160: [u8; 20],
    ) -> Result<Self, ConversionError<Self::Error>> {
        Ok(Self::P2sh { net, hash160 })
    }

    fn try_from_tex(
        net: ZcashNetworkType,
        hash160: [u8; 20],
    ) -> Result<Self, ConversionError<Self::Error>> {
        Ok(Self::Tex { net, hash160 })
    }
}

impl ClassifiedAddress {
    fn into_classification(self, original: &str, network: Network) -> SearchClassification {
        match self {
            Self::Sprout { net, bytes } => {
                if !network_matches(net, network) {
                    return mismatched_network_classification(original);
                }
                SearchClassification::ShieldedAddress {
                    canonical: ZcashAddress::from_sprout(net, bytes).to_string(),
                    network,
                }
            }
            Self::Sapling { net, bytes } => {
                if !network_matches(net, network) {
                    return mismatched_network_classification(original);
                }
                SearchClassification::ShieldedAddress {
                    canonical: ZcashAddress::from_sapling(net, bytes).to_string(),
                    network,
                }
            }
            Self::Unified { net, unified } => {
                if !network_matches(net, network) {
                    return mismatched_network_classification(original);
                }
                let receivers = unified
                    .items_as_parsed()
                    .iter()
                    .filter_map(|parsed_receiver| match parsed_receiver {
                        Uitem::Data(receiver) => Some(classify_unified_receiver(net, receiver)),
                        Uitem::Metadata(_) => None,
                    })
                    .collect();
                SearchClassification::UnifiedAddress(UnifiedAddressClassification {
                    canonical_form: ZcashAddress::from_unified(net, unified).to_string(),
                    network,
                    receivers,
                })
            }
            Self::P2pkh { net, hash160 } => {
                if !network_matches(net, network) {
                    return mismatched_network_classification(original);
                }
                SearchClassification::TransparentAddress(transparent_classification_p2pkh(
                    ZcashAddress::from_transparent_p2pkh(net, hash160).to_string(),
                    hash160,
                ))
            }
            Self::P2sh { net, hash160 } => {
                if !network_matches(net, network) {
                    return mismatched_network_classification(original);
                }
                SearchClassification::TransparentAddress(transparent_classification_p2sh(
                    ZcashAddress::from_transparent_p2sh(net, hash160).to_string(),
                    hash160,
                ))
            }
            Self::Tex { net, hash160 } => {
                if !network_matches(net, network) {
                    return mismatched_network_classification(original);
                }
                let canonical_tex_form = ZcashAddress::from_tex(net, hash160).to_string();
                let equivalent_p2pkh_form =
                    ZcashAddress::from_transparent_p2pkh(net, hash160).to_string();
                let transparent =
                    transparent_classification_p2pkh(equivalent_p2pkh_form.clone(), hash160);
                SearchClassification::TexAddress(TexAddressClassification {
                    canonical_tex_form,
                    equivalent_p2pkh_form,
                    transparent,
                })
            }
        }
    }
}

fn classify_unified_receiver(
    net: ZcashNetworkType,
    receiver: &UnifiedReceiver,
) -> UnifiedAddressReceiverClassification {
    match receiver {
        UnifiedReceiver::Orchard(_) => UnifiedAddressReceiverClassification::Shielded {
            kind: ShieldedReceiverKind::Orchard,
        },
        UnifiedReceiver::Sapling(_) => UnifiedAddressReceiverClassification::Shielded {
            kind: ShieldedReceiverKind::Sapling,
        },
        UnifiedReceiver::P2pkh(hash160) => {
            UnifiedAddressReceiverClassification::Transparent(transparent_classification_p2pkh(
                ZcashAddress::from_transparent_p2pkh(net, *hash160).to_string(),
                *hash160,
            ))
        }
        UnifiedReceiver::P2sh(hash160) => {
            UnifiedAddressReceiverClassification::Transparent(transparent_classification_p2sh(
                ZcashAddress::from_transparent_p2sh(net, *hash160).to_string(),
                *hash160,
            ))
        }
        UnifiedReceiver::Unknown { typecode, .. } => {
            UnifiedAddressReceiverClassification::Unknown {
                typecode: *typecode,
            }
        }
    }
}

fn transparent_classification_p2pkh(
    canonical_form: String,
    hash160: [u8; 20],
) -> TransparentAddressClassification {
    TransparentAddressClassification {
        canonical_form,
        address_script_hash: TransparentAddressScriptHash::of_script_pub_key(
            &p2pkh_script_pub_key(hash160),
        ),
        is_p2pkh: true,
    }
}

fn transparent_classification_p2sh(
    canonical_form: String,
    hash160: [u8; 20],
) -> TransparentAddressClassification {
    TransparentAddressClassification {
        canonical_form,
        address_script_hash: TransparentAddressScriptHash::of_script_pub_key(&p2sh_script_pub_key(
            hash160,
        )),
        is_p2pkh: false,
    }
}

fn p2pkh_script_pub_key(hash160: [u8; 20]) -> [u8; 25] {
    let mut script = [0_u8; 25];
    script[0] = 0x76;
    script[1] = 0xa9;
    script[2] = 0x14;
    script[3..23].copy_from_slice(&hash160);
    script[23] = 0x88;
    script[24] = 0xac;
    script
}

fn p2sh_script_pub_key(hash160: [u8; 20]) -> [u8; 23] {
    let mut script = [0_u8; 23];
    script[0] = 0xa9;
    script[1] = 0x14;
    script[2..22].copy_from_slice(&hash160);
    script[22] = 0x87;
    script
}

fn mismatched_network_classification(query: &str) -> SearchClassification {
    SearchClassification::Unclassified {
        hint: format!(
            "address {query} decoded against a different network than the server is configured for"
        ),
    }
}

/// True when an address that decoded as `decoded` is valid against the
/// `expected` Zinder network.
///
/// Mainnet is strict; testnet and regtest share base58 version bytes for
/// transparent (P2PKH/P2SH/TEX) addresses, so a `tm.../t2.../tex...`
/// input decodes as [`ZcashNetworkType::Test`] in `zcash_address` even
/// when the operator is running regtest. Address forms whose HRP is
/// regtest-specific (Sapling `zregtestsapling`, unified `uregtest`)
/// decode as [`ZcashNetworkType::Regtest`] and are matched strictly.
const fn network_matches(decoded: ZcashNetworkType, expected: Network) -> bool {
    matches!(
        (decoded, expected),
        (ZcashNetworkType::Main, Network::ZcashMainnet)
            | (
                ZcashNetworkType::Test,
                Network::ZcashTestnet | Network::ZcashRegtest,
            )
            | (ZcashNetworkType::Regtest, Network::ZcashRegtest)
    )
}
