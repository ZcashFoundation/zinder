//! Per-call cost of ending a supervised listener's calls.
//!
//! Every call a supervised gRPC listener serves passes through
//! [`GrpcCallTerminationLayer`], which registers the call so that shutdown
//! can end it. This benchmark reports the nanoseconds that layer adds to a
//! unary call whose response carries one data frame, with 1, 4, and 12 tasks
//! calling at once on a 12-worker runtime. Timings depend on the host and the
//! build profile, so it asserts nothing about them. Run it in release on an
//! idle host:
//!
//! ```text
//! cargo nextest run -p zinder-runtime --profile=ci-perf \
//!   --cargo-profile release --run-ignored=all --no-capture \
//!   -E 'test(/^perf::/)'
//! ```
//!
//! It lives in the crate rather than under `tests/perf/` because the layer is
//! built only by [`crate::SupervisedRuntime`].

use std::{convert::Infallible, future, hint::black_box, sync::Arc, time::Instant};

use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use tonic::body::Body;
use tower::{Layer as _, Service, ServiceExt as _, service_fn};

use crate::public_grpc_server::{GrpcCallTerminationLayer, OpenGrpcCalls};

const CALLS_PER_TASK: u32 = 20_000;
const ROUNDS: usize = 7;
const CONCURRENT_TASKS: [u32; 3] = [1, 4, 12];

type BoxError = Box<dyn std::error::Error + Send + Sync>;

fn reply_service() -> impl Service<
    http::Request<Body>,
    Response = http::Response<Body>,
    Error = Infallible,
    Future = future::Ready<Result<http::Response<Body>, Infallible>>,
> + Clone
+ Send
+ 'static {
    service_fn(|_request: http::Request<Body>| {
        future::ready(Ok(http::Response::new(Body::new(Full::new(
            Bytes::from_static(b"reply"),
        )))))
    })
}

async fn call_repeatedly<S>(mut service: S, calls: u32) -> Result<(), BoxError>
where
    S: Service<http::Request<Body>, Response = http::Response<Body>>,
    S::Error: Into<BoxError>,
{
    for _ in 0..calls {
        let response = service
            .ready()
            .await
            .map_err(Into::into)?
            .call(http::Request::new(Body::empty()))
            .await
            .map_err(Into::into)?;
        let mut body = response.into_body();
        while let Some(frame) = body.frame().await {
            black_box(frame?);
        }
    }
    Ok(())
}

/// Returns the median nanoseconds per call over [`ROUNDS`] rounds of `tasks`
/// tasks that each make [`CALLS_PER_TASK`] calls on a clone of `service`.
async fn nanoseconds_per_call<S>(service: S, tasks: u32) -> Result<u128, BoxError>
where
    S: Service<http::Request<Body>, Response = http::Response<Body>> + Clone + Send + 'static,
    S::Future: Send,
    S::Error: Into<BoxError>,
{
    let mut rounds = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        let started = Instant::now();
        let callers: Vec<_> = (0..tasks)
            .map(|_| tokio::spawn(call_repeatedly(service.clone(), CALLS_PER_TASK)))
            .collect();
        for caller in callers {
            caller.await??;
        }
        rounds.push(started.elapsed().as_nanos() / u128::from(tasks * CALLS_PER_TASK));
    }
    rounds.sort_unstable();
    Ok(rounds.get(ROUNDS / 2).copied().unwrap_or_default())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
#[ignore = "manual report-only benchmark; run in release on an idle host"]
async fn grpc_call_termination_reports_nanoseconds_per_call() -> Result<(), BoxError> {
    for tasks in CONCURRENT_TASKS {
        let bare = nanoseconds_per_call(reply_service(), tasks).await?;
        let calls = Arc::new(OpenGrpcCalls::default());
        let terminable = GrpcCallTerminationLayer::new(Arc::clone(&calls)).layer(reply_service());
        let layered = nanoseconds_per_call(terminable, tasks).await?;
        #[allow(
            clippy::print_stderr,
            reason = "report-only benchmark prints its measurements for operator review"
        )]
        {
            eprintln!(
                "grpc_call_termination tasks={tasks} bare_ns={bare} layered_ns={layered} \
                 overhead_ns={} (calls_per_task={CALLS_PER_TASK} rounds={ROUNDS} median)",
                layered.saturating_sub(bare),
            );
        }
    }
    Ok(())
}
