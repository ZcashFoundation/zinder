//! The deadline a client sets with the `grpc-timeout` request header.
//!
//! A response that has not started by the deadline ends with `CANCELLED`,
//! as tonic's own server ends it. The deadline does not bound a response
//! stream once its headers are sent.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use tonic::{Status, body::Body};
use tower::Service;

const GRPC_TIMEOUT_HEADER: &str = "grpc-timeout";

/// The longest `TimeoutValue` the gRPC specification allows. The bound also
/// keeps the hour conversion from overflowing.
const MAX_TIMEOUT_DIGITS: usize = 8;

/// The time `headers` give the response to start, when they carry a valid
/// `grpc-timeout`. An invalid value is ignored.
fn client_deadline(headers: &http::HeaderMap) -> Option<Duration> {
    let field = headers.get(GRPC_TIMEOUT_HEADER)?.to_str().ok()?;
    let (amount, unit) = field.split_at_checked(field.len().checked_sub(1)?)?;
    if amount.len() > MAX_TIMEOUT_DIGITS {
        return None;
    }
    let amount: u64 = amount.parse().ok()?;
    match unit {
        "H" => Some(Duration::from_secs(amount * 60 * 60)),
        "M" => Some(Duration::from_secs(amount * 60)),
        "S" => Some(Duration::from_secs(amount)),
        "m" => Some(Duration::from_millis(amount)),
        "u" => Some(Duration::from_micros(amount)),
        "n" => Some(Duration::from_nanos(amount)),
        _ => None,
    }
}

/// The Tower service that bounds response start time after readiness.
#[derive(Clone, Debug)]
pub struct GrpcDeadline<S> {
    inner: S,
}

impl<S> GrpcDeadline<S> {
    pub(super) const fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S, B> Service<http::Request<Body>> for GrpcDeadline<S>
where
    S: Service<http::Request<Body>, Response = http::Response<B>>,
    S::Error: 'static,
    S::Future: Send + 'static,
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<axum::BoxError>,
{
    type Response = http::Response<Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let deadline = client_deadline(request.headers());
        Box::pin(respond_within(deadline, self.inner.call(request)))
    }
}

async fn respond_within<B, E>(
    deadline: Option<Duration>,
    response: impl Future<Output = Result<http::Response<B>, E>>,
) -> Result<http::Response<Body>, E>
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<axum::BoxError>,
{
    let outcome = match deadline {
        Some(deadline) => match tokio::time::timeout(deadline, response).await {
            Ok(outcome) => outcome,
            Err(_) => return Ok(Status::cancelled("Timeout expired").into_http()),
        },
        None => response.await,
    };
    outcome.map(|response| response.map(Body::new))
}

/// A service error carrying a status becomes a response. Other errors reach
/// hyper so it can reset the stream, as in tonic's server.
pub(super) fn transport_response<B, E>(
    outcome: Result<http::Response<B>, E>,
) -> Result<http::Response<Body>, axum::BoxError>
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<axum::BoxError>,
    E: Into<axum::BoxError>,
{
    match outcome {
        Ok(response) => Ok(response.map(Body::new)),
        Err(error) => Status::try_from_error(error.into()).map(Status::into_http),
    }
}

#[cfg(test)]
mod tests {
    use std::{future, time::Duration};

    use http::{HeaderMap, HeaderValue};
    use tonic::body::Body;

    use super::{GRPC_TIMEOUT_HEADER, client_deadline, respond_within, transport_response};

    fn deadline_of(value: &'static str) -> Option<Duration> {
        let mut headers = HeaderMap::new();
        headers.insert(GRPC_TIMEOUT_HEADER, HeaderValue::from_static(value));
        client_deadline(&headers)
    }

    #[test]
    fn every_unit_of_a_valid_timeout_is_read() {
        for (value, expected) in [
            ("3H", Duration::from_hours(3)),
            ("2M", Duration::from_mins(2)),
            ("10S", Duration::from_secs(10)),
            ("250m", Duration::from_millis(250)),
            ("7u", Duration::from_micros(7)),
            ("99999999n", Duration::from_nanos(99_999_999)),
            ("99999999H", Duration::from_hours(99_999_999)),
        ] {
            assert_eq!(deadline_of(value), Some(expected), "{value}");
        }
    }

    #[test]
    fn an_invalid_timeout_is_ignored() {
        assert_eq!(client_deadline(&HeaderMap::new()), None);
        for value in ["", "S", "10", "10x", "123456789S", "1.5S", "-1S"] {
            assert_eq!(deadline_of(value), None, "{value:?}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_response_that_misses_the_deadline_is_cancelled()
    -> Result<(), std::convert::Infallible> {
        let response = respond_within(
            Some(Duration::from_millis(10)),
            future::pending::<Result<http::Response<Body>, std::convert::Infallible>>(),
        )
        .await?;
        let status = tonic::Status::from_header_map(response.headers());
        assert_eq!(
            status.map(|status| status.code()),
            Some(tonic::Code::Cancelled)
        );
        Ok(())
    }

    #[test]
    fn a_service_error_becomes_its_status() -> Result<(), axum::BoxError> {
        let response = transport_response(Err::<http::Response<Body>, _>(
            tonic::Status::not_found("missing"),
        ))?;
        let status = tonic::Status::from_header_map(response.headers());
        assert_eq!(
            status.map(|status| status.code()),
            Some(tonic::Code::NotFound)
        );
        Ok(())
    }

    #[test]
    fn a_service_error_without_a_status_reaches_the_transport() {
        let outcome = transport_response(Err::<http::Response<Body>, _>(std::io::Error::other(
            "not a status",
        )));
        assert!(outcome.is_err(), "a non-status error became a response");
    }
}
