//! Per-client admission for wallet block-range requests and responses.

use std::{
    collections::{HashMap, hash_map::Entry},
    net::{IpAddr, Ipv6Addr},
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use http::Request;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::time::{Instant, sleep_until};
use tokio_util::task::AbortOnDropHandle;
use tonic::{Status, body::Body};
use zinder_proto::{status_for_reason, v1::ops::ErrorReason};

use super::{
    ClientAddressSource, RetryPushback, WalletRpcMetricsLayer, proxy_named_client,
    receive_slots::ClientFairnessKey, remote_address,
};

const MAX_TRACKED_CLIENTS: usize = 4096;
const SCHEDULING_PROBE_PERIOD: Duration = Duration::from_millis(25);
const RANGE_REFUSALS: &str = "zinder_grpc_block_range_admission_refusals_total";

/// How the wallet listener groups simultaneous range requests into client quotas.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RangeClientKey {
    /// Key by IP address and source port when both are available.
    #[default]
    Connection,
    /// Key by IPv4 address or IPv6 /64, across ports and connections.
    Subnet,
}

/// Admission policy for block-range calls on one public wallet listener.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct GrpcRangeAdmission {
    /// Maximum open block-range calls per client, including calls waiting to start.
    pub max_streams_per_client: NonZeroU32,
    /// How to group connections for the per-client quota.
    pub client_key: RangeClientKey,
    /// Scheduling delay in milliseconds that triggers shedding, when configured.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_scheduling_delay_ms: Option<NonZeroU32>,
}

impl GrpcRangeAdmission {
    /// Returns whether every field equals the section default.
    #[must_use]
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

impl Default for GrpcRangeAdmission {
    fn default() -> Self {
        Self {
            max_streams_per_client: NonZeroU32::MIN.saturating_add(3),
            client_key: RangeClientKey::Connection,
            max_scheduling_delay_ms: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Refusal {
    ClientQuota,
    ClientCapacity,
    SchedulingDelay,
}

impl Refusal {
    const fn label(self) -> &'static str {
        match self {
            Self::ClientQuota => "client_quota",
            Self::ClientCapacity => "client_capacity",
            Self::SchedulingDelay => "scheduling_delay",
        }
    }

    fn status(self, surface: WalletRpcMetricsLayer) -> Status {
        let message = match self {
            Self::ClientQuota => "block-range stream limit reached for this client",
            Self::ClientCapacity => "block-range client tracking limit reached",
            Self::SchedulingDelay => "runtime overloaded; retry the block range",
        };
        let status = match surface {
            WalletRpcMetricsLayer::WalletQuery => {
                status_for_reason(ErrorReason::BlockRangeAdmissionRefused, message)
            }
            WalletRpcMetricsLayer::Lightwalletd => Status::unavailable(message),
        };
        RetryPushback::After(Duration::from_millis(250)).apply(status)
    }
}

#[derive(Debug)]
pub(super) struct RangeAdmissions {
    source: ClientAddressSource,
    policy: GrpcRangeAdmission,
    clients: Mutex<HashMap<Option<ClientFairnessKey>, u32>>,
    probe: Option<SchedulingProbe>,
}

impl RangeAdmissions {
    pub(super) fn new(
        policy: GrpcRangeAdmission,
        source: ClientAddressSource,
        surface: &'static str,
    ) -> Arc<Self> {
        metrics::describe_counter!(
            RANGE_REFUSALS,
            metrics::Unit::Count,
            "Wallet block-range calls refused by the public listener, by surface and reason."
        );
        for reason in [
            Refusal::ClientQuota,
            Refusal::ClientCapacity,
            Refusal::SchedulingDelay,
        ] {
            metrics::counter!(RANGE_REFUSALS, "surface" => surface, "reason" => reason.label())
                .absolute(0);
        }
        Arc::new(Self {
            source,
            policy,
            clients: Mutex::default(),
            probe: policy.max_scheduling_delay_ms.map(SchedulingProbe::spawn),
        })
    }

    pub(super) fn claim(
        self: &Arc<Self>,
        request: &Request<Body>,
        surface: WalletRpcMetricsLayer,
    ) -> Result<RangePermit, Status> {
        let peer = || remote_address(request).map(ClientFairnessKey::Connection);
        let client = match self.source {
            ClientAddressSource::Connection => peer(),
            ClientAddressSource::ProxyHeader => {
                proxy_named_client(request.headers()).ok().or_else(peer)
            }
        };
        self.claim_client(client).map_err(|reason| {
            metrics::counter!(RANGE_REFUSALS, "surface" => surface.label(), "reason" => reason.label())
                .increment(1);
            reason.status(surface)
        })
    }

    fn claim_client(
        self: &Arc<Self>,
        client: Option<ClientFairnessKey>,
    ) -> Result<RangePermit, Refusal> {
        if self
            .probe
            .as_ref()
            .is_some_and(|probe| probe.delayed.load(Ordering::Relaxed))
        {
            return Err(Refusal::SchedulingDelay);
        }
        let client = client.map(|client| self.policy.client_key.bucket(client));
        {
            let mut clients = self.clients.lock();
            let tracked_clients = clients.len();
            match clients.entry(client) {
                Entry::Occupied(mut entry) => {
                    let open = entry.get_mut();
                    if *open >= self.policy.max_streams_per_client.get() {
                        return Err(Refusal::ClientQuota);
                    }
                    *open += 1;
                }
                Entry::Vacant(entry) => {
                    if tracked_clients >= MAX_TRACKED_CLIENTS {
                        return Err(Refusal::ClientCapacity);
                    }
                    entry.insert(1);
                }
            }
        }
        Ok(RangePermit {
            admissions: Arc::clone(self),
            client,
        })
    }
}

impl RangeClientKey {
    const fn bucket(self, client: ClientFairnessKey) -> ClientFairnessKey {
        let ip = match (self, client) {
            (Self::Connection, ClientFairnessKey::Connection(_)) => return client,
            (_, ClientFairnessKey::Connection(address)) => address.ip(),
            (_, ClientFairnessKey::Address(ip)) => ip,
        }
        .to_canonical();
        let ip = match (self, ip) {
            (Self::Subnet, IpAddr::V6(ip)) => {
                IpAddr::V6(Ipv6Addr::from_bits(ip.to_bits() & (u128::MAX << 64)))
            }
            (_, ip) => ip,
        };
        ClientFairnessKey::Address(ip)
    }
}

pub(super) struct RangePermit {
    admissions: Arc<RangeAdmissions>,
    client: Option<ClientFairnessKey>,
}

impl Drop for RangePermit {
    fn drop(&mut self) {
        let mut clients = self.admissions.clients.lock();
        if let Entry::Occupied(mut entry) = clients.entry(self.client) {
            if *entry.get() == 1 {
                entry.remove();
            } else {
                *entry.get_mut() -= 1;
            }
        }
    }
}

#[derive(Debug)]
struct SchedulingProbe {
    delayed: Arc<AtomicBool>,
    _task: AbortOnDropHandle<()>,
}

impl SchedulingProbe {
    fn spawn(threshold_ms: NonZeroU32) -> Self {
        let threshold = Duration::from_millis(u64::from(threshold_ms.get()));
        let delayed = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn({
            let delayed = Arc::clone(&delayed);
            async move {
                loop {
                    let due = Instant::now() + SCHEDULING_PROBE_PERIOD;
                    sleep_until(due).await;
                    delayed.store(due.elapsed() >= threshold, Ordering::Relaxed);
                }
            }
        });
        Self {
            delayed,
            _task: AbortOnDropHandle::new(task),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};

    use eyre::{Result, eyre};

    use super::*;

    #[test]
    fn subnet_quota_groups_ipv6_prefixes_and_mapped_ipv4() -> Result<()> {
        let admissions = RangeAdmissions::new(
            GrpcRangeAdmission {
                max_streams_per_client: NonZeroU32::MIN,
                client_key: RangeClientKey::Subnet,
                ..GrpcRangeAdmission::default()
            },
            ClientAddressSource::Connection,
            "wallet_query",
        );
        for (first, same_subnet, different_subnet) in [
            (
                "[2001:db8:1::1]:50",
                "[2001:db8:1::2]:51",
                "[2001:db8:2::1]:50",
            ),
            ("192.0.2.1:50", "[::ffff:192.0.2.1]:51", "192.0.2.2:50"),
        ] {
            let client = |addr: &str| {
                addr.parse::<SocketAddr>()
                    .map(ClientFairnessKey::Connection)
                    .map(Some)
            };
            let held = admissions
                .claim_client(client(first)?)
                .map_err(|reason| eyre!("{reason:?}"))?;
            assert_eq!(
                admissions.claim_client(client(same_subnet)?).err(),
                Some(Refusal::ClientQuota)
            );
            assert!(admissions.claim_client(client(different_subnet)?).is_ok());
            drop(held);
            assert!(admissions.claim_client(client(same_subnet)?).is_ok());
        }
        assert!(admissions.clients.lock().is_empty());
        Ok(())
    }

    #[test]
    fn refuses_new_clients_at_capacity_and_recovers_after_a_release() -> Result<()> {
        let admissions = RangeAdmissions::new(
            GrpcRangeAdmission::default(),
            ClientAddressSource::Connection,
            "wallet_query",
        );
        let client = |index| Some(ClientFairnessKey::Address(Ipv4Addr::from(index).into()));
        let capacity = u32::try_from(MAX_TRACKED_CLIENTS)?;
        let mut permits = (0..capacity)
            .map(|index| admissions.claim_client(client(index)))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|reason| eyre!("{reason:?}"))?;
        assert_eq!(
            admissions.claim_client(client(capacity)).err(),
            Some(Refusal::ClientCapacity)
        );
        assert!(admissions.claim_client(client(0)).is_ok());
        drop(permits.pop());
        assert!(admissions.claim_client(client(capacity)).is_ok());
        drop(permits);
        assert!(admissions.clients.lock().is_empty());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn sheds_on_a_late_probe_and_recovers_on_the_first_timely_probe() {
        let admissions = RangeAdmissions::new(
            GrpcRangeAdmission {
                max_scheduling_delay_ms: NonZeroU32::new(20),
                ..GrpcRangeAdmission::default()
            },
            ClientAddressSource::Connection,
            "wallet_query",
        );
        tokio::task::yield_now().await;
        let existing = admissions.claim_client(None);
        assert!(existing.is_ok());
        tokio::time::advance(SCHEDULING_PROBE_PERIOD + Duration::from_millis(30)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            admissions.claim_client(None).err(),
            Some(Refusal::SchedulingDelay)
        );
        assert_eq!(admissions.clients.lock().len(), 1);
        tokio::time::advance(SCHEDULING_PROBE_PERIOD).await;
        tokio::task::yield_now().await;
        assert!(admissions.claim_client(None).is_ok());
    }
}
