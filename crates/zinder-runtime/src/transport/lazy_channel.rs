//! A Zinder gRPC channel that dials on first use and shares that dial.

use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::watch;
use zinder_source::redact_endpoint_url;

use super::connect_zinder_grpc;
use crate::auth::{AuthenticatedChannel, BearerToken, BearerTokenConnectError};

/// What a dial ended with, as every caller waiting on it sees it.
type DialOutcome = Result<AuthenticatedChannel, Arc<BearerTokenConnectError>>;

/// A gRPC channel to another Zinder service that dials on first use, caches
/// the channel once a dial succeeds, and shares one dial among the callers
/// that arrive while it is in flight.
///
/// The dial runs in its own task under
/// [`super::DEFAULT_ZINDER_GRPC_CONNECT_TIMEOUT`], so every caller waiting on
/// it sees one outcome within that bound, and a caller that stops waiting
/// does not stop the dial. A failed dial leaves nothing cached, and the next
/// call dials again. Clones share the cached channel.
#[derive(Clone)]
pub struct LazyZinderGrpcChannel {
    shared: Arc<LazyChannelShared>,
}

struct LazyChannelShared {
    endpoint: String,
    bearer_token: Option<BearerToken>,
    state: Mutex<DialState>,
}

enum DialState {
    Idle,
    Dialing(watch::Receiver<Option<DialOutcome>>),
    Connected(AuthenticatedChannel),
}

impl LazyZinderGrpcChannel {
    /// Creates a channel to `endpoint` that has not dialed yet.
    #[must_use]
    pub fn new(endpoint: impl Into<String>, bearer_token: Option<BearerToken>) -> Self {
        Self {
            shared: Arc::new(LazyChannelShared {
                endpoint: endpoint.into(),
                bearer_token,
                state: Mutex::new(DialState::Idle),
            }),
        }
    }

    /// Returns the cached channel, or waits for the dial in flight, starting
    /// one when none is.
    ///
    /// Must be called within a Tokio runtime, which runs the dial.
    pub async fn channel(&self) -> Result<AuthenticatedChannel, Arc<BearerTokenConnectError>> {
        let mut dial = {
            let mut state = self.shared.state.lock();
            match &*state {
                DialState::Connected(channel) => return Ok(channel.clone()),
                DialState::Dialing(dial) => dial.clone(),
                DialState::Idle => {
                    let (outcome_sender, dial) = watch::channel(None);
                    *state = DialState::Dialing(dial.clone());
                    drop(state);
                    tokio::spawn(Arc::clone(&self.shared).dial(outcome_sender));
                    dial
                }
            }
        };
        if let Ok(outcome) = dial.wait_for(Option::is_some).await
            && let Some(outcome) = outcome.as_ref()
        {
            return outcome.clone();
        }
        // The dial task ended without reporting, which happens only when its
        // runtime shuts down. Dialing here keeps this caller from waiting on
        // a dial nobody runs.
        self.shared.reset_abandoned_dial(&dial);
        connect_zinder_grpc(&self.shared.endpoint, self.shared.bearer_token.as_ref())
            .await
            .map_err(Arc::new)
    }
}

impl LazyChannelShared {
    async fn dial(self: Arc<Self>, outcome_sender: watch::Sender<Option<DialOutcome>>) {
        let outcome = connect_zinder_grpc(&self.endpoint, self.bearer_token.as_ref())
            .await
            .map_err(Arc::new);
        *self.state.lock() = outcome.as_ref().map_or(DialState::Idle, |channel| {
            DialState::Connected(channel.clone())
        });
        outcome_sender.send_replace(Some(outcome));
    }

    fn reset_abandoned_dial(&self, abandoned: &watch::Receiver<Option<DialOutcome>>) {
        let mut state = self.state.lock();
        if let DialState::Dialing(dial) = &*state
            && dial.same_channel(abandoned)
        {
            *state = DialState::Idle;
        }
    }
}

impl std::fmt::Debug for LazyZinderGrpcChannel {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = match &*self.shared.state.lock() {
            DialState::Idle => "idle",
            DialState::Dialing(_) => "dialing",
            DialState::Connected(_) => "connected",
        };
        formatter
            .debug_struct("LazyZinderGrpcChannel")
            .field("endpoint", &redact_endpoint_url(&self.shared.endpoint))
            .field("bearer_token_attached", &self.shared.bearer_token.is_some())
            .field("state", &state)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        missing_docs,
        reason = "Unit test names describe the behavior under test."
    )]

    use std::time::Duration;

    use tokio::net::TcpListener;

    use super::LazyZinderGrpcChannel;
    use crate::{DEFAULT_ZINDER_GRPC_CONNECT_TIMEOUT, auth::BearerTokenConnectError};

    #[tokio::test(start_paused = true)]
    async fn concurrent_first_callers_share_one_dial() -> Result<(), Box<dyn std::error::Error>> {
        // The listener's backlog accepts the TCP connection, but nothing
        // answers the TLS handshake.
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let lazy = LazyZinderGrpcChannel::new(format!("https://{}", listener.local_addr()?), None);

        let started_at = tokio::time::Instant::now();
        let (first, second, third) = tokio::join!(lazy.channel(), lazy.channel(), lazy.channel());
        let elapsed = started_at.elapsed();

        let outcomes: [_; 3] = (first, second, third).into();
        for outcome in outcomes {
            assert!(
                outcome.as_ref().is_err_and(|error| matches!(
                    **error,
                    BearerTokenConnectError::ConnectTimeout { .. }
                )),
                "every caller sees the shared dial's timeout, got {outcome:?}"
            );
        }
        assert!(
            elapsed < DEFAULT_ZINDER_GRPC_CONNECT_TIMEOUT + Duration::from_secs(1),
            "callers waited {elapsed:?}, longer than one dial"
        );
        drop(listener);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_dial_is_not_cached() -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let lazy = LazyZinderGrpcChannel::new(format!("https://{}", listener.local_addr()?), None);

        let first = lazy.channel().await;
        let started_at = tokio::time::Instant::now();
        let second = lazy.channel().await;

        assert!(first.is_err() && second.is_err());
        assert!(
            started_at.elapsed() >= DEFAULT_ZINDER_GRPC_CONNECT_TIMEOUT,
            "the second call must dial again instead of reusing the failure"
        );
        assert!(format!("{lazy:?}").contains("idle"));
        drop(listener);
        Ok(())
    }

    #[test]
    fn debug_output_redacts_endpoint_credentials() {
        let lazy = LazyZinderGrpcChannel::new(
            "https://reader:hunter2@ingest.internal:9100/?token=s3cret",
            None,
        );

        let debug = format!("{lazy:?}");

        assert!(
            !debug.contains("hunter2") && !debug.contains("s3cret"),
            "{debug}"
        );
        assert!(debug.contains("ingest.internal:9100"), "{debug}");
    }
}
