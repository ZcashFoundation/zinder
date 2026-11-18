//! In-process refusal of public binds on Zinder's plaintext serving surfaces.
//!
//! Zinder ships no server TLS by design (ADR-0011 assigns encryption and
//! public exposure to a reverse proxy), so every serving and operational
//! listener is plaintext. A listener bound to an unspecified
//! (`0.0.0.0`, `::`) or globally-routable address therefore exposes
//! unauthenticated chain data directly to the network.
//!
//! [`guard_serving_bind`] classifies each resolved listen address at
//! config-validation time. Loopback and private-range binds are always
//! allowed; unspecified and public binds are refused unless the operator
//! opts in through `[security] allow_public_bind`, in which case the bind
//! proceeds with a `tracing::warn!` naming the surface and address.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use crate::config::ConfigError;

/// Network-reachability class of a resolved listen address.
///
/// The std library exposes `is_loopback`, `is_unspecified`, `is_private`,
/// and `is_link_local` on stable; `is_global` is nightly-only. "Public" is
/// therefore defined as the complement of every range we can name with
/// stable predicates plus the documentation and benchmarking ranges that
/// are not globally routable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum BindAddressClass {
    /// `127.0.0.0/8` or `::1`. The safe default; reachable only from the
    /// host itself.
    Loopback,
    /// RFC 1918 / RFC 4193 / link-local: `10/8`, `172.16/12`, `192.168/16`,
    /// `169.254/16`, `fc00::/7`, `fe80::/10`. Reachable on a private mesh
    /// or LAN, which covers the Railway private network and local
    /// development.
    Private,
    /// `0.0.0.0` or `::`. Binds every interface, including any public one.
    Unspecified,
    /// Anything not covered above: a globally-routable address.
    Public,
}

/// Classifies `address` by network reachability using stable std
/// predicates.
#[must_use]
pub fn classify_bind_address(address: IpAddr) -> BindAddressClass {
    match address {
        IpAddr::V4(address) => classify_ipv4(address),
        IpAddr::V6(address) => classify_ipv6(address),
    }
}

fn classify_ipv4(address: Ipv4Addr) -> BindAddressClass {
    if address.is_loopback() {
        BindAddressClass::Loopback
    } else if address.is_unspecified() {
        BindAddressClass::Unspecified
    } else if address.is_private() || address.is_link_local() {
        BindAddressClass::Private
    } else {
        BindAddressClass::Public
    }
}

fn classify_ipv6(address: Ipv6Addr) -> BindAddressClass {
    if address.is_loopback() {
        BindAddressClass::Loopback
    } else if address.is_unspecified() {
        BindAddressClass::Unspecified
    } else if is_unique_local_v6(address) || is_link_local_v6(address) {
        BindAddressClass::Private
    } else {
        BindAddressClass::Public
    }
}

/// `fc00::/7` (RFC 4193 unique-local). `Ipv6Addr::is_unique_local` is
/// nightly-only, so the prefix is matched by hand.
fn is_unique_local_v6(address: Ipv6Addr) -> bool {
    (address.segments()[0] & 0xfe00) == 0xfc00
}

/// `fe80::/10` (RFC 4291 link-local unicast). `Ipv6Addr::is_unicast_link_local`
/// is nightly-only, so the prefix is matched by hand.
fn is_link_local_v6(address: Ipv6Addr) -> bool {
    (address.segments()[0] & 0xffc0) == 0xfe80
}

/// Validates one serving-surface bind address against the public-bind
/// policy.
///
/// `surface` names the listener for the operator-facing message, e.g.
/// `query.listen_addr` or `ops.listen_addr`. Loopback and private binds
/// always pass. Unspecified and public binds return
/// [`ConfigError::Invalid`] when `allow_public_bind` is `false`; when it
/// is `true` they pass with a `tracing::warn!`.
pub fn guard_serving_bind(
    surface: &str,
    bind_addr: SocketAddr,
    allow_public_bind: bool,
) -> Result<(), ConfigError> {
    match classify_bind_address(bind_addr.ip()) {
        BindAddressClass::Loopback | BindAddressClass::Private => Ok(()),
        class @ (BindAddressClass::Unspecified | BindAddressClass::Public) => {
            if allow_public_bind {
                tracing::warn!(
                    target: "zinder::runtime",
                    surface,
                    bind_addr = %bind_addr,
                    class = ?class,
                    "binding a plaintext serving surface to a non-loopback address; \
                     terminate TLS and authorization at a reverse proxy (ADR-0011)"
                );
                Ok(())
            } else {
                Err(ConfigError::invalid(format!(
                    "{surface} {bind_addr} binds a plaintext serving surface to a public or \
                     unspecified address; Zinder ships no server TLS (ADR-0011). Front it with a \
                     reverse proxy and set security.allow_public_bind = true \
                     (ZINDER_SECURITY__ALLOW_PUBLIC_BIND=true) to opt in."
                )))
            }
        }
    }
}

/// Validates an optional serving-surface bind address.
///
/// Surfaces that resolve to `None` when disabled (the operational endpoint,
/// the `IngestControl` writer) call this so callers do not branch on the
/// `Option` at every site.
pub fn guard_optional_serving_bind(
    surface: &str,
    bind_addr: Option<SocketAddr>,
    allow_public_bind: bool,
) -> Result<(), ConfigError> {
    bind_addr.map_or(Ok(()), |bind_addr| {
        guard_serving_bind(surface, bind_addr, allow_public_bind)
    })
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use super::*;

    fn v4(address: Ipv4Addr) -> IpAddr {
        IpAddr::V4(address)
    }

    fn v6(address: Ipv6Addr) -> IpAddr {
        IpAddr::V6(address)
    }

    #[test]
    fn loopback_addresses_classify_as_loopback() {
        assert_eq!(
            classify_bind_address(v4(Ipv4Addr::LOCALHOST)),
            BindAddressClass::Loopback
        );
        assert_eq!(
            classify_bind_address(v4(Ipv4Addr::new(127, 5, 6, 7))),
            BindAddressClass::Loopback
        );
        assert_eq!(
            classify_bind_address(v6(Ipv6Addr::LOCALHOST)),
            BindAddressClass::Loopback
        );
    }

    #[test]
    fn private_and_link_local_addresses_classify_as_private() {
        assert_eq!(
            classify_bind_address(v4(Ipv4Addr::new(10, 0, 0, 1))),
            BindAddressClass::Private
        );
        assert_eq!(
            classify_bind_address(v4(Ipv4Addr::new(172, 16, 0, 1))),
            BindAddressClass::Private
        );
        assert_eq!(
            classify_bind_address(v4(Ipv4Addr::new(192, 168, 1, 1))),
            BindAddressClass::Private
        );
        assert_eq!(
            classify_bind_address(v4(Ipv4Addr::new(169, 254, 1, 1))),
            BindAddressClass::Private
        );
        assert_eq!(
            classify_bind_address(v6(Ipv6Addr::new(0xfc00, 0, 0, 0, 0, 0, 0, 1))),
            BindAddressClass::Private
        );
        assert_eq!(
            classify_bind_address(v6(Ipv6Addr::new(0xfd12, 0, 0, 0, 0, 0, 0, 1))),
            BindAddressClass::Private
        );
        assert_eq!(
            classify_bind_address(v6(Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1))),
            BindAddressClass::Private
        );
    }

    #[test]
    fn unspecified_addresses_classify_as_unspecified() {
        assert_eq!(
            classify_bind_address(v4(Ipv4Addr::UNSPECIFIED)),
            BindAddressClass::Unspecified
        );
        assert_eq!(
            classify_bind_address(v6(Ipv6Addr::UNSPECIFIED)),
            BindAddressClass::Unspecified
        );
    }

    #[test]
    fn globally_routable_addresses_classify_as_public() {
        assert_eq!(
            classify_bind_address(v4(Ipv4Addr::new(8, 8, 8, 8))),
            BindAddressClass::Public
        );
        assert_eq!(
            classify_bind_address(v4(Ipv4Addr::new(203, 0, 113, 5))),
            BindAddressClass::Public
        );
        assert_eq!(
            classify_bind_address(v6(Ipv6Addr::new(0x2001, 0x4860, 0, 0, 0, 0, 0, 0x8888))),
            BindAddressClass::Public
        );
    }

    #[test]
    fn loopback_bind_passes_without_opt_in() -> Result<(), ConfigError> {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9106);
        guard_serving_bind("query.listen_addr", addr, false)
    }

    #[test]
    fn private_bind_passes_without_opt_in() -> Result<(), ConfigError> {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 9106);
        guard_serving_bind("query.listen_addr", addr, false)
    }

    #[test]
    fn unspecified_bind_is_refused_without_opt_in() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 9101);
        let outcome = guard_serving_bind("query.listen_addr", addr, false);
        assert!(matches!(outcome, Err(ConfigError::Invalid { .. })));
    }

    #[test]
    fn public_bind_is_refused_without_opt_in() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 5)), 9101);
        let outcome = guard_serving_bind("query.listen_addr", addr, false);
        assert!(matches!(outcome, Err(ConfigError::Invalid { .. })));
    }

    #[test]
    fn unspecified_bind_passes_with_opt_in() -> Result<(), ConfigError> {
        let addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 9101);
        guard_serving_bind("query.listen_addr", addr, true)
    }

    #[test]
    fn public_bind_passes_with_opt_in() -> Result<(), ConfigError> {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)), 9101);
        guard_serving_bind("query.listen_addr", addr, true)
    }

    #[test]
    fn optional_none_passes_without_opt_in() -> Result<(), ConfigError> {
        guard_optional_serving_bind("ops.listen_addr", None, false)
    }

    #[test]
    fn optional_unspecified_is_refused_without_opt_in() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 9105);
        let outcome = guard_optional_serving_bind("ops.listen_addr", Some(addr), false);
        assert!(matches!(outcome, Err(ConfigError::Invalid { .. })));
    }
}
