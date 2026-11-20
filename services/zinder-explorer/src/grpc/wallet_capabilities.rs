//! Bounded discovery for Explorer methods that depend on wallet capabilities.

use std::{sync::Arc, time::Duration};

use tokio::{sync::Mutex, time::Instant};
use tonic::Status;
use zinder_proto::v1::wallet::WalletServerInfo;

const PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const REFRESH_INTERVAL: Duration = Duration::from_secs(60);
const RETRY_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone, Default)]
pub(super) struct WalletCapabilityDiscovery {
    last_probe: Arc<Mutex<Option<WalletCapabilityProbe>>>,
}

struct WalletCapabilityProbe {
    descriptor: Option<Arc<WalletServerInfo>>,
    refresh_at: Instant,
}

impl WalletCapabilityDiscovery {
    pub(super) fn cached(&self) -> Option<Arc<WalletServerInfo>> {
        self.last_probe
            .try_lock()
            .ok()?
            .as_ref()
            .filter(|probe| Instant::now() < probe.refresh_at)
            .and_then(|probe| probe.descriptor.clone())
    }

    pub(super) async fn current(
        &self,
        probe: impl Future<Output = Result<Option<WalletServerInfo>, Status>>,
    ) -> Option<Arc<WalletServerInfo>> {
        // Share one dial and RPC when concurrent discovery requests arrive.
        let mut last = self.last_probe.lock().await;
        if let Some(cached) = &*last
            && Instant::now() < cached.refresh_at
        {
            return cached.descriptor.clone();
        }
        // A cancelled caller must leave a retry deadline for the next request.
        *last = Some(WalletCapabilityProbe {
            descriptor: None,
            refresh_at: Instant::now() + RETRY_INTERVAL,
        });
        let descriptor = tokio::time::timeout(PROBE_TIMEOUT, probe)
            .await
            .ok()
            .and_then(Result::ok)
            .flatten()
            .map(Arc::new);
        let interval = if descriptor.is_some() {
            REFRESH_INTERVAL
        } else {
            RETRY_INTERVAL
        };
        *last = Some(WalletCapabilityProbe {
            descriptor: descriptor.clone(),
            refresh_at: Instant::now() + interval,
        });
        descriptor
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn cancelled_probe_waits_for_retry_before_contacting_wallet_again() {
        let discovery = WalletCapabilityDiscovery::default();
        let completed = tokio::select! {
            biased;
            _ = discovery.current(std::future::pending()) => true,
            () = std::future::ready(()) => false,
        };
        assert!(!completed);
        let successful_probe = || std::future::ready(Ok(Some(WalletServerInfo::default())));
        assert!(discovery.current(successful_probe()).await.is_none());
        tokio::time::advance(RETRY_INTERVAL).await;
        assert!(discovery.current(successful_probe()).await.is_some());
    }
}
