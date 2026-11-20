use std::{convert::Infallible, future::Future, net::SocketAddr, time::Duration};

use http::{HeaderName, HeaderValue, Method, Request, Response, StatusCode, Version, header};
use tonic::{Status, body::Body, server::NamedService, transport::server::TcpIncoming};
use tonic_web::GrpcWebLayer;
use tower::{Layer, Service, ServiceBuilder};
use tower_http::{
    cors::{AllowOrigin, CorsLayer},
    validate_request::{ValidateRequest, ValidateRequestHeaderLayer},
};
use zinder_proto::compat::lightwalletd::compact_tx_streamer_server::SERVICE_NAME;

use super::config::{LightwalletdConfig, LightwalletdConfigError};

pub(super) fn bind(addr: SocketAddr) -> Result<(TcpIncoming, SocketAddr), LightwalletdConfigError> {
    let listen_error = |source| LightwalletdConfigError::Listen { addr, source };
    // Tonic's address-based server enables TCP_NODELAY on accepted sockets.
    let incoming = TcpIncoming::bind(addr)
        .map_err(listen_error)?
        .with_nodelay(Some(true));
    let bound_addr = incoming.local_addr().map_err(listen_error)?;
    Ok((incoming, bound_addr))
}

pub(super) fn serve<S>(
    runtime: &zinder_runtime::SupervisedRuntime,
    wallet_service: S,
    incoming: TcpIncoming,
    config: &LightwalletdConfig,
    readiness: &zinder_runtime::Readiness,
) -> Result<
    impl Future<Output = Result<(), tonic::transport::Error>> + use<S>,
    tonic_reflection::server::Error,
>
where
    S: Service<Request<Body>, Response = Response<Body>, Error = Infallible>
        + NamedService
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
{
    let browser_origins = &config.browser_origins;
    let enabled = !browser_origins.is_empty();
    let transport = transport_layers(browser_origins);
    let router = runtime.public_grpc_router_with_transport(
        (
            wallet_service,
            zinder_proto::LIGHTWALLETD_COMPAT_FILE_DESCRIPTOR_SET,
        ),
        config.client_address_source,
        readiness,
        |server| server.accept_http1(enabled).layer(transport),
    )?;
    Ok(Box::pin(router.serve_with_incoming_shutdown(
        incoming,
        runtime.grpc_server_shutdown(),
    )))
}

fn transport_layers<S>(
    browser_origins: &[HeaderValue],
) -> impl Layer<
    S,
    Service: Service<
        Request<Body>,
        Response = Response<Body>,
        Error = S::Error,
        Future: Send + 'static,
    > + Clone
                 + Send
                 + 'static,
> + Clone
+ use<S>
where
    S: Service<Request<Body>, Response = Response<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    let enabled = !browser_origins.is_empty();
    ServiceBuilder::new()
        .layer(ValidateRequestHeaderLayer::custom(CompatRequest {
            browser_enabled: enabled,
        }))
        .option_layer(enabled.then(|| browser_cors(browser_origins)))
        .option_layer(enabled.then(GrpcWebLayer::new))
        .layer(zinder_runtime::WalletRpcMetricsLayer::Lightwalletd)
        .option_layer(enabled.then(|| ValidateRequestHeaderLayer::custom(UnsupportedBrowserCall)))
        .into_inner()
}

fn browser_cors(origins: &[HeaderValue]) -> CorsLayer {
    // A single origin (including "*") is a constant response. With several
    // origins the grant depends only on the request's Origin header.
    let cors = match origins {
        [origin] => CorsLayer::new().allow_origin(origin.clone()).vary([]),
        origins => CorsLayer::new()
            .allow_origin(AllowOrigin::list(origins.iter().cloned()))
            .vary([header::ORIGIN]),
    };
    cors.max_age(Duration::from_mins(10))
        .allow_methods([Method::POST])
        .allow_headers([
            header::ACCEPT,
            header::AUTHORIZATION,
            header::CONTENT_TYPE,
            HeaderName::from_static("x-grpc-web"),
            HeaderName::from_static("x-user-agent"),
            HeaderName::from_static("grpc-timeout"),
            HeaderName::from_static("grpc-encoding"),
            HeaderName::from_static("grpc-accept-encoding"),
        ])
        .expose_headers([
            Status::GRPC_STATUS,
            Status::GRPC_MESSAGE,
            Status::GRPC_STATUS_DETAILS,
        ])
}

#[derive(Clone, Copy)]
struct CompatRequest {
    browser_enabled: bool,
}

impl ValidateRequest<Body> for CompatRequest {
    type ResponseBody = Body;

    fn validate(&mut self, request: &mut Request<Body>) -> Result<(), Response<Body>> {
        let content_type = request
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok());
        if request.version() == Version::HTTP_2
            && request.method() == Method::POST
            && content_type.is_some_and(|mime| {
                mime == "application/grpc" || mime.starts_with("application/grpc+")
            })
        {
            return Ok(());
        }
        if !self.browser_enabled {
            return Err(rejection(StatusCode::UNSUPPORTED_MEDIA_TYPE));
        }
        let Some((SERVICE_NAME, method)) = request
            .uri()
            .path()
            .strip_prefix('/')
            .and_then(|p| p.split_once('/'))
        else {
            return Err(rejection(StatusCode::NOT_FOUND));
        };
        if request.method() == Method::OPTIONS {
            return Ok(());
        }
        if request.method() != Method::POST {
            return Err(rejection(StatusCode::METHOD_NOT_ALLOWED));
        }
        if !matches!(
            content_type,
            Some(
                "application/grpc-web"
                    | "application/grpc-web+proto"
                    | "application/grpc-web-text"
                    | "application/grpc-web-text+proto"
            )
        ) {
            return Err(rejection(StatusCode::UNSUPPORTED_MEDIA_TYPE));
        }
        if method == "GetTaddressBalanceStream" {
            request.extensions_mut().insert(UnsupportedBrowserCall);
        }
        Ok(())
    }
}

fn rejection(status: StatusCode) -> Response<Body> {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = status;
    response
}

// Reject the client-streaming method inside the translator so the browser
// receives its negotiated gRPC-Web status, including on a trailers-only reply.
#[derive(Clone, Copy)]
struct UnsupportedBrowserCall;

impl ValidateRequest<Body> for UnsupportedBrowserCall {
    type ResponseBody = Body;

    fn validate(&mut self, request: &mut Request<Body>) -> Result<(), Response<Body>> {
        if request.extensions().get::<Self>().is_some() {
            Err(Status::unimplemented(
                "use GetTaddressBalance for a unary balance request over gRPC-Web",
            )
            .into_http())
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod receive_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use tower::{Layer, ServiceExt, service_fn};

    #[tokio::test]
    async fn cors_varies_by_origin_only_when_it_selects_from_a_list() -> eyre::Result<()> {
        let wallet = HeaderValue::from_static("https://wallet.example");
        let second = HeaderValue::from_static("https://other.example");
        let any = HeaderValue::from_static("*");
        for (origins, grant, vary) in [
            (vec![wallet.clone()], Some(wallet.clone()), None),
            (vec![any.clone()], Some(any), None),
            (
                vec![wallet.clone(), second.clone()],
                Some(second.clone()),
                Some("origin"),
            ),
            (
                vec![wallet, HeaderValue::from_static("https://third.example")],
                None,
                Some("origin"),
            ),
        ] {
            for method in [Method::POST, Method::OPTIONS] {
                let service = browser_cors(&origins).layer(service_fn(|_: Request<()>| async {
                    Ok::<_, Infallible>(Response::new(()))
                }));
                let request = Request::builder()
                    .method(method)
                    .header(header::ORIGIN, second.clone())
                    .body(())?;
                let response = service.oneshot(request).await?;
                let headers = response.headers();
                assert_eq!(
                    headers.get(header::ACCESS_CONTROL_ALLOW_ORIGIN),
                    grant.as_ref()
                );
                assert_eq!(
                    headers.get(header::VARY).map(HeaderValue::as_bytes),
                    vary.map(str::as_bytes)
                );
            }
        }
        Ok(())
    }

    #[test]
    fn only_native_http2_requests_reach_reflection() -> Result<(), http::Error> {
        let mut policy = CompatRequest {
            browser_enabled: true,
        };
        for (version, content_type, admitted) in [
            (Version::HTTP_2, "application/grpc", true),
            (Version::HTTP_11, "application/grpc", false),
            (Version::HTTP_2, "application/grpc-web+proto", false),
        ] {
            for version_name in ["v1", "v1alpha"] {
                let mut request = Request::post(format!(
                    "/grpc.reflection.{version_name}.ServerReflection/ServerReflectionInfo"
                ))
                .version(version)
                .header(header::CONTENT_TYPE, content_type)
                .body(Body::empty())?;
                assert_eq!(policy.validate(&mut request).is_ok(), admitted);
            }
        }
        Ok(())
    }

    #[test]
    fn compat_calls_require_a_grpc_media_type_on_both_http_versions() -> Result<(), http::Error> {
        let mut policy = CompatRequest {
            browser_enabled: true,
        };
        for version in [Version::HTTP_11, Version::HTTP_2] {
            let mut request = Request::post(format!("/{SERVICE_NAME}/GetLatestBlock"))
                .version(version)
                .header(header::CONTENT_TYPE, "text/plain")
                .body(Body::empty())?;
            assert_eq!(
                policy
                    .validate(&mut request)
                    .err()
                    .map(|response| response.status()),
                Some(StatusCode::UNSUPPORTED_MEDIA_TYPE),
            );
        }
        Ok(())
    }
}
