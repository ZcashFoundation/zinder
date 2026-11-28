use std::{convert::Infallible, future::Future, net::SocketAddr, pin::Pin};

use http::{Request, Response};
use tokio::net::TcpListener;
use tonic::{Status, body::Body, server::NamedService};
use tower::{Layer, Service, ServiceBuilder};
use tower_http::validate_request::{ValidateRequest, ValidateRequestHeaderLayer};
use zinder_proto::compat::lightwalletd::compact_tx_streamer_server::SERVICE_NAME;

use zinder_runtime::{
    BrowserOrigins, ClientAddressSource, GrpcProxyProtocol, GrpcRangeAdmission,
    WalletRpcMetricsLayer, is_grpc_web_request,
};

use super::config::{LightwalletdConfig, LightwalletdConfigError};

pub(super) fn bind(addr: SocketAddr) -> Result<(TcpListener, SocketAddr), LightwalletdConfigError> {
    let listen_error = |source| LightwalletdConfigError::Listen { addr, source };
    let listener = std::net::TcpListener::bind(addr).map_err(listen_error)?;
    listener.set_nonblocking(true).map_err(listen_error)?;
    let listener = TcpListener::from_std(listener).map_err(listen_error)?;
    let bound_addr = listener.local_addr().map_err(listen_error)?;
    Ok((listener, bound_addr))
}

type GrpcServer = Pin<Box<dyn Future<Output = Result<(), std::io::Error>> + Send>>;

pub(super) fn serve<S>(
    runtime: &zinder_runtime::SupervisedRuntime,
    wallet_service: S,
    incoming: TcpListener,
    config: &LightwalletdConfig,
    readiness: &zinder_runtime::Readiness,
) -> Result<GrpcServer, tonic_reflection::server::Error>
where
    S: Service<Request<Body>, Response = Response<Body>, Error = Infallible>
        + NamedService
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
{
    let listener = BrowserListener {
        browser_origins: &config.browser_origins,
        client_address: config.client_address_source,
        proxy_protocol: config.proxy_protocol.clone(),
        range_admission: config.range_admission,
    };
    serve_listener(runtime, wallet_service, incoming, &listener, readiness)
}

/// The settings of the compatibility listener that its transport reads.
pub(super) struct BrowserListener<'a> {
    pub(super) browser_origins: &'a BrowserOrigins,
    pub(super) client_address: ClientAddressSource,
    pub(super) proxy_protocol: GrpcProxyProtocol,
    pub(super) range_admission: GrpcRangeAdmission,
}

pub(super) fn serve_listener<S>(
    runtime: &zinder_runtime::SupervisedRuntime,
    wallet_service: S,
    incoming: TcpListener,
    listener: &BrowserListener<'_>,
    readiness: &zinder_runtime::Readiness,
) -> Result<GrpcServer, tonic_reflection::server::Error>
where
    S: Service<Request<Body>, Response = Response<Body>, Error = Infallible>
        + NamedService
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
{
    let router = runtime.public_grpc_router_with_transport(
        (
            wallet_service,
            zinder_proto::LIGHTWALLETD_COMPAT_FILE_DESCRIPTOR_SET,
        ),
        listener.client_address,
        readiness,
        |server| {
            server
                .with_proxy_protocol(listener.proxy_protocol.clone())
                .with_browser_transport(listener.browser_origins, SERVICE_NAME)
                .layer(wallet_layers(
                    listener.range_admission,
                    listener.client_address,
                ))
        },
    )?;
    Ok(Box::pin(router.serve_with_incoming_shutdown(
        incoming,
        runtime.grpc_server_shutdown(),
    )))
}

fn wallet_layers<S>(
    policy: GrpcRangeAdmission,
    source: ClientAddressSource,
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
    ServiceBuilder::new()
        .layer(WalletRpcMetricsLayer::Lightwalletd.with_range_admission(policy, source))
        .layer(ValidateRequestHeaderLayer::custom(UnsupportedBrowserCall))
        .into_inner()
}

// Reject the client-streaming method inside the translator so the browser
// receives its negotiated gRPC-Web status, including on a trailers-only reply.
#[derive(Clone, Copy)]
struct UnsupportedBrowserCall;

impl ValidateRequest<Body> for UnsupportedBrowserCall {
    type ResponseBody = Body;

    fn validate(&mut self, request: &mut Request<Body>) -> Result<(), Response<Body>> {
        if is_grpc_web_request(request)
            && request.uri().path().ends_with("/GetTaddressBalanceStream")
        {
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
mod stall_tests;
