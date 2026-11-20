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
    /// Must be called within a Tokio runtime, which runs the dial. The call
    /// fails with [`BearerTokenConnectError::DialAbandoned`] when that runtime
    /// shuts down, or the dial panics, before the dial reports.
    pub async fn channel(&self) -> Result<AuthenticatedChannel, Arc<BearerTokenConnectError>> {
        loop {
            let (mut dial, started_here) = {
                let mut state = self.shared.state.lock();
                match &*state {
                    DialState::Connected(channel) => return Ok(channel.clone()),
                    DialState::Dialing(dial) => (dial.clone(), false),
                    DialState::Idle => {
                        let (outcome_sender, dial) = watch::channel(None);
                        *state = DialState::Dialing(dial.clone());
                        drop(state);
                        tokio::spawn(Arc::clone(&self.shared).dial(outcome_sender));
                        (dial, true)
                    }
                }
            };
            if let Ok(outcome) = dial.wait_for(Option::is_some).await
                && let Some(outcome) = outcome.as_ref()
            {
                return outcome.clone();
            }
            // The dial task ended without reporting: its runtime shut down, or
            // the task panicked. A dial another caller started may have run on
            // a runtime that has since stopped, so this caller starts one on
            // its own runtime that later callers share. A dial this caller
            // started would end the same way again.
            self.shared.reset_abandoned_dial(&dial);
            if started_here {
                return Err(Arc::new(BearerTokenConnectError::DialAbandoned));
            }
        }
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

    use std::{
        net::SocketAddr,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use tokio::{
        net::{TcpListener, TcpStream},
        sync::mpsc,
        task::JoinHandle,
    };
    use tokio_stream::{StreamExt as _, wrappers::TcpListenerStream};
    use tonic::transport::Server;

    use super::LazyZinderGrpcChannel;
    use crate::{
        DEFAULT_ZINDER_GRPC_CONNECT_TIMEOUT,
        auth::{AuthenticatedChannel, BearerTokenConnectError},
    };

    /// Accepts every connection and holds it open without answering, sending
    /// the client address of each one on the returned channel in the order
    /// the listener accepted them.
    fn spawn_silent_peer(
        listener: TcpListener,
    ) -> (JoinHandle<()>, mpsc::UnboundedReceiver<SocketAddr>) {
        let (accepted, accepted_addresses) = mpsc::unbounded_channel();
        let peer = tokio::spawn(async move {
            while let Ok((mut stream, client_address)) = listener.accept().await {
                if accepted.send(client_address).is_err() {
                    break;
                }
                tokio::spawn(async move {
                    let _ = tokio::io::copy(&mut stream, &mut tokio::io::sink()).await;
                });
            }
        });
        (peer, accepted_addresses)
    }

    /// Counts the connections the peer accepted before a sentinel connection
    /// opened now, which the listener accepts after every earlier one.
    async fn connections_accepted_so_far(
        address: SocketAddr,
        accepted_addresses: &mut mpsc::UnboundedReceiver<SocketAddr>,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        let sentinel = TcpStream::connect(address).await?;
        let sentinel_address = sentinel.local_addr()?;
        let mut accepted = 0;
        while accepted_addresses
            .recv()
            .await
            .ok_or("the peer stopped accepting")?
            != sentinel_address
        {
            accepted += 1;
        }
        Ok(accepted)
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_first_callers_share_one_dial() -> Result<(), Box<dyn std::error::Error>> {
        // The peer accepts the connection but never sends an HTTP/2 frame.
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (peer, mut accepted_addresses) = spawn_silent_peer(listener);
        let lazy = LazyZinderGrpcChannel::new(format!("http://{address}"), None);

        let started_at = tokio::time::Instant::now();
        let (first, second, third) = tokio::join!(lazy.channel(), lazy.channel(), lazy.channel());
        let elapsed = started_at.elapsed();
        let dialed = connections_accepted_so_far(address, &mut accepted_addresses).await?;
        peer.abort();

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
        assert_eq!(dialed, 1, "concurrent callers must open one connection");
        assert!(
            elapsed < DEFAULT_ZINDER_GRPC_CONNECT_TIMEOUT + Duration::from_secs(1),
            "callers waited {elapsed:?}, longer than one dial"
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_dial_is_not_cached() -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let lazy = LazyZinderGrpcChannel::new(format!("http://{}", listener.local_addr()?), None);

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

    #[tokio::test]
    async fn callers_after_an_abandoned_dial_share_one_new_dial()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let accepted = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&accepted);
        let incoming = TcpListenerStream::new(listener).map(move |connection| {
            counted.fetch_add(1, Ordering::SeqCst);
            connection
        });
        let (_reporter, health_service) = tonic_health::server::health_reporter();
        let server = tokio::spawn(
            Server::builder()
                .add_service(health_service)
                .serve_with_incoming(incoming),
        );
        let lazy = LazyZinderGrpcChannel::new(format!("http://{address}"), None);

        // This test's runtime cannot serve while the thread below blocks it,
        // so the dial started on that thread's runtime never completes, and
        // dropping the runtime abandons the dial.
        let abandoning = lazy.clone();
        std::thread::spawn(move || -> std::io::Result<()> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let _ = runtime.block_on(async {
                tokio::time::timeout(Duration::from_millis(200), abandoning.channel()).await
            });
            Ok(())
        })
        .join()
        .map_err(|_| "the abandoning thread panicked")??;

        let (first, second) = tokio::join!(lazy.channel(), lazy.channel());
        let accepted = accepted.load(Ordering::SeqCst);
        server.abort();

        assert!(
            first.is_ok() && second.is_ok(),
            "callers dial again after an abandoned dial, got {first:?} and {second:?}"
        );
        assert_eq!(
            accepted, 2,
            "the abandoned dial and one dial the callers share"
        );
        assert!(format!("{lazy:?}").contains("connected"));
        Ok(())
    }

    /// Runs `call` on a thread of its own and returns what it returns, or an
    /// error when it has not returned within five seconds.
    fn returned_within_five_seconds<T: Send + 'static>(
        call: impl FnOnce() -> T + Send + 'static,
    ) -> Result<T, Box<dyn std::error::Error>> {
        let (returned, outcome) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = returned.send(call());
        });
        outcome
            .recv_timeout(Duration::from_secs(5))
            .map_err(|_| "the call did not return within five seconds".into())
    }

    fn is_dial_abandoned(
        outcome: &Result<AuthenticatedChannel, Arc<BearerTokenConnectError>>,
    ) -> bool {
        outcome
            .as_ref()
            .is_err_and(|error| matches!(**error, BearerTokenConnectError::DialAbandoned))
    }

    #[test]
    fn a_dial_on_a_runtime_that_shut_down_fails_instead_of_dialing_again()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        let handle = runtime.handle().clone();
        runtime.shutdown_background();
        let lazy = LazyZinderGrpcChannel::new("http://127.0.0.1:9", None);

        // The shut-down runtime drops every task spawned on it unpolled.
        let outcome = returned_within_five_seconds(move || handle.block_on(lazy.channel()))?;

        assert!(is_dial_abandoned(&outcome), "got {outcome:?}");
        Ok(())
    }

    #[test]
    fn a_dial_that_panics_fails_instead_of_dialing_again() -> Result<(), Box<dyn std::error::Error>>
    {
        // Without a time driver, the dial panics when it arms its timeout.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()?;
        let lazy = LazyZinderGrpcChannel::new("http://127.0.0.1:9", None);

        let outcome = returned_within_five_seconds(move || runtime.block_on(lazy.channel()))?;

        assert!(is_dial_abandoned(&outcome), "got {outcome:?}");
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
