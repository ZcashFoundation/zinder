//! Transparent-address compatibility against the configured [`Network`].
//!
//! Wallet-facing read surfaces accept transparent addresses parsed by
//! [`zebra_chain::transparent::Address`] and must reject inputs that target a
//! different network than the one the deployment indexes. The check is a
//! pure mapping from `zebra-chain`'s [`ZebraNetworkKind`] to Zinder's
//! [`Network`]; both `zinder-query` and `zinder-compat-lightwalletd` import
//! it instead of carrying their own copies.

use zebra_chain::parameters::NetworkKind as ZebraNetworkKind;
use zebra_chain::transparent::Address as ZebraTransparentAddress;
use zinder_core::Network;

/// Longest transparent-address text [`parse_transparent_address`] decodes.
///
/// A `Base58Check` address is 35 characters and the longest ZIP 320 TEX
/// encoding (`texregtest1…`) is 49, so this leaves margin while keeping
/// arbitrarily long request strings out of the decoder.
pub const MAX_TRANSPARENT_ADDRESS_TEXT_BYTES: usize = 64;

/// Why [`parse_transparent_address`] refused its input.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum TransparentAddressParseError {
    /// The text is longer than any valid transparent-address encoding.
    #[error(
        "transparent address is {length} bytes, longer than any valid encoding \
         ({MAX_TRANSPARENT_ADDRESS_TEXT_BYTES} bytes)"
    )]
    TooLong {
        /// Length of the refused text in bytes.
        length: usize,
    },
    /// The text is not a valid transparent-address encoding.
    #[error("transparent address could not be parsed: {reason}")]
    Invalid {
        /// The decoder's description of the failure.
        reason: String,
    },
}

/// Parses transparent-address text from a request.
///
/// Every request surface parses through this function. Base58 decoding is
/// quadratic in the input length, so text longer than
/// [`MAX_TRANSPARENT_ADDRESS_TEXT_BYTES`] is refused before decoding.
///
/// # Errors
///
/// Returns [`TransparentAddressParseError::TooLong`] for oversized text and
/// [`TransparentAddressParseError::Invalid`] for text that does not decode.
pub fn parse_transparent_address(
    text: &str,
) -> Result<ZebraTransparentAddress, TransparentAddressParseError> {
    if text.len() > MAX_TRANSPARENT_ADDRESS_TEXT_BYTES {
        return Err(TransparentAddressParseError::TooLong { length: text.len() });
    }
    text.parse()
        .map_err(|source: zebra_chain::serialization::SerializationError| {
            TransparentAddressParseError::Invalid {
                reason: source.to_string(),
            }
        })
}

/// Returns true when `address_kind` is compatible with `network`.
///
/// Zinder distinguishes regtest from testnet (`ZcashRegtest` vs
/// `ZcashTestnet`) but Zebra collapses them: a `ZebraNetworkKind::Testnet`
/// address is valid on both Zinder testnet and regtest deployments. A
/// `ZebraNetworkKind::Regtest` address is only valid on Zinder regtest.
#[must_use]
#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "Network is non_exhaustive; future variants must fail closed against transparent address validation until they are explicitly handled"
)]
pub fn transparent_address_matches_network(
    address_kind: ZebraNetworkKind,
    network: Network,
) -> bool {
    match network {
        Network::ZcashMainnet => address_kind == ZebraNetworkKind::Mainnet,
        Network::ZcashTestnet => address_kind == ZebraNetworkKind::Testnet,
        Network::ZcashRegtest => matches!(
            address_kind,
            ZebraNetworkKind::Testnet | ZebraNetworkKind::Regtest
        ),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_address_text_is_refused_before_decoding() {
        let oversized = "t".repeat(MAX_TRANSPARENT_ADDRESS_TEXT_BYTES + 1);
        assert_eq!(
            parse_transparent_address(&oversized),
            Err(TransparentAddressParseError::TooLong {
                length: MAX_TRANSPARENT_ADDRESS_TEXT_BYTES + 1
            })
        );
    }

    #[test]
    fn valid_address_text_parses_and_garbage_is_invalid() {
        let encoded =
            ZebraTransparentAddress::from_pub_key_hash(ZebraNetworkKind::Mainnet, [7; 20])
                .to_string();
        assert!(parse_transparent_address(&encoded).is_ok());
        assert!(matches!(
            parse_transparent_address("t1notanaddress"),
            Err(TransparentAddressParseError::Invalid { .. })
        ));
    }

    #[test]
    fn mainnet_address_only_matches_mainnet_network() {
        assert!(transparent_address_matches_network(
            ZebraNetworkKind::Mainnet,
            Network::ZcashMainnet
        ));
        assert!(!transparent_address_matches_network(
            ZebraNetworkKind::Mainnet,
            Network::ZcashTestnet
        ));
        assert!(!transparent_address_matches_network(
            ZebraNetworkKind::Mainnet,
            Network::ZcashRegtest
        ));
    }

    #[test]
    fn testnet_address_matches_testnet_and_regtest_networks() {
        assert!(transparent_address_matches_network(
            ZebraNetworkKind::Testnet,
            Network::ZcashTestnet
        ));
        assert!(transparent_address_matches_network(
            ZebraNetworkKind::Testnet,
            Network::ZcashRegtest
        ));
        assert!(!transparent_address_matches_network(
            ZebraNetworkKind::Testnet,
            Network::ZcashMainnet
        ));
    }

    #[test]
    fn regtest_address_only_matches_regtest_network() {
        assert!(transparent_address_matches_network(
            ZebraNetworkKind::Regtest,
            Network::ZcashRegtest
        ));
        assert!(!transparent_address_matches_network(
            ZebraNetworkKind::Regtest,
            Network::ZcashTestnet
        ));
        assert!(!transparent_address_matches_network(
            ZebraNetworkKind::Regtest,
            Network::ZcashMainnet
        ));
    }
}
