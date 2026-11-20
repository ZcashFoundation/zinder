//! Canonical store-damage detail mapping.
//!
//! A canonical writer that finds a damaged stored row reports it in
//! [`CanonicalStoreDamagedDetail`]: the row's height and, when one pool's
//! frontier failed, that pool by its node RPC name. Writers and readers build
//! and read the detail here, so both agree on the pool vocabulary.

use zinder_core::{BlockHeight, ShieldedProtocol};

use crate::v1::ops::CanonicalStoreDamagedDetail;

/// Returns the detail that reports a damaged row at `height`, naming `pool`
/// when the failure is in that pool's frontier.
#[must_use]
pub fn canonical_store_damaged_detail(
    height: BlockHeight,
    pool: Option<ShieldedProtocol>,
) -> CanonicalStoreDamagedDetail {
    CanonicalStoreDamagedDetail {
        height: height.value(),
        pool: pool.map(|pool| pool.rpc_pool_name().to_owned()),
    }
}

/// Returns the pool `detail` names, or `None` when it names no pool or one
/// this release does not know.
#[must_use]
pub fn canonical_store_damaged_pool(
    detail: &CanonicalStoreDamagedDetail,
) -> Option<ShieldedProtocol> {
    detail
        .pool
        .as_deref()
        .and_then(ShieldedProtocol::from_rpc_pool_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_pool_round_trips_through_the_detail() {
        for pool in [
            ShieldedProtocol::Sapling,
            ShieldedProtocol::Orchard,
            ShieldedProtocol::Ironwood,
        ] {
            let detail = canonical_store_damaged_detail(BlockHeight::new(42), Some(pool));
            assert_eq!(detail.height, 42);
            assert_eq!(canonical_store_damaged_pool(&detail), Some(pool));
        }
    }

    #[test]
    fn an_absent_or_unknown_pool_decodes_as_none() {
        let without_pool = canonical_store_damaged_detail(BlockHeight::new(7), None);
        assert_eq!(without_pool.pool, None);
        assert_eq!(canonical_store_damaged_pool(&without_pool), None);
        let unknown = CanonicalStoreDamagedDetail {
            height: 7,
            pool: Some("sprout".to_owned()),
        };
        assert_eq!(canonical_store_damaged_pool(&unknown), None);
    }
}
