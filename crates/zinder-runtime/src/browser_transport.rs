//! gRPC-Web admission, translation, and CORS for public gRPC listeners.

use std::{sync::Arc, time::Duration};

use http::{HeaderName, HeaderValue, Method, Request, Response, StatusCode, Version, header};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use tonic::{Status, body::Body};
use tonic_web::GrpcWebLayer;
use tower::{Layer, Service, ServiceBuilder, layer::util::Stack};
use tower_http::{
    cors::{AllowOrigin, CorsLayer},
    validate_request::{ValidateRequest, ValidateRequestHeaderLayer},
};
use url::Url;
use zinder_proto::wire::CHAIN_EVENT_FAMILY_HEADER;

use crate::PublicGrpcServer;

/// Validated browser origins for one public listener.
///
/// Accepts a TOML array or a comma-separated environment variable. An empty
/// list disables gRPC-Web. A lone wildcard permits any origin without
/// credentialed CORS.
#[derive(Clone, Debug, Default)]
pub struct BrowserOrigins(Arc<[HeaderValue]>);

impl<'de> Deserialize<'de> for BrowserOrigins {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(
            untagged,
            expecting = "an array of browser origins or a comma-separated list"
        )]
        enum Origins {
            List(Vec<String>),
            CommaSeparated(String),
        }

        let origins = match Origins::deserialize(deserializer)? {
            Origins::List(origins) => {
                let mut trimmed = Vec::with_capacity(origins.len());
                for origin in origins {
                    let candidate = origin.trim();
                    if candidate.is_empty() {
                        return Err(D::Error::custom(
                            "browser_origins must not contain empty elements",
                        ));
                    }
                    trimmed.push(candidate.to_owned());
                }
                trimmed
            }
            Origins::CommaSeparated(raw) => {
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    return Err(D::Error::custom(
                        "empty value is not allowed; omit the setting, use \"none\", or use [] to disable",
                    ));
                }
                if trimmed == crate::DISABLED_SENTINEL {
                    Vec::new()
                } else {
                    let parts: Vec<String> = trimmed
                        .split(',')
                        .map(|origin| origin.trim().to_owned())
                        .collect();
                    if parts.iter().any(String::is_empty) {
                        return Err(D::Error::custom(
                            "browser_origins must not contain empty elements",
                        ));
                    }
                    parts
                }
            }
        };
        if origins.iter().any(|origin| origin == "*") {
            if origins.len() == 1 {
                return Ok(Self(vec![HeaderValue::from_static("*")].into()));
            }
            return Err(D::Error::custom("must use '*' alone"));
        }

        let invalid_origin = || {
            D::Error::custom(
                "requires an exact HTTP or HTTPS origin such as https://wallet.example",
            )
        };
        let mut headers = Vec::with_capacity(origins.len());
        for origin in origins {
            let (scheme, authority) = origin
                .strip_suffix('/')
                .unwrap_or(&origin)
                .split_once("://")
                .ok_or_else(invalid_origin)?;
            if !(scheme.eq_ignore_ascii_case("https") || scheme.eq_ignore_ascii_case("http"))
                || authority.contains(['/', '\\', '@', '?', '#'])
                || authority.bytes().any(|b| b.is_ascii_control() || b == b' ')
            {
                return Err(invalid_origin());
            }
            let url = Url::parse(&origin).map_err(|_| invalid_origin())?;
            let serialized = url.origin().ascii_serialization();
            if serialized.contains('*') {
                return Err(invalid_origin());
            }
            headers.push(HeaderValue::try_from(serialized).map_err(|_| invalid_origin())?);
        }
        Ok(Self(headers.into()))
    }
}

impl Serialize for BrowserOrigins {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(
            self.0
                .iter()
                .map(|origin| String::from_utf8_lossy(origin.as_bytes())),
        )
    }
}

impl PartialEq for BrowserOrigins {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_ref() == other.0.as_ref()
    }
}

impl Eq for BrowserOrigins {}

impl BrowserOrigins {
    /// Returns whether browser gRPC-Web transport is disabled (no permitted origins).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns whether the listener explicitly permits every browser origin.
    #[must_use]
    pub fn allows_any_origin(&self) -> bool {
        matches!(self.0.as_ref(), [origin] if origin == "*")
    }

    /// Builds the HTTP adapter for the named application RPC service.
    ///
    /// Add this outside middleware that consumes native gRPC bodies.
    /// Use [`PublicGrpcServer::with_browser_transport`] to configure a listener.
    #[must_use]
    pub fn transport_layer<S>(
        &self,
        service_name: &'static str,
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
        let enabled = !self.0.is_empty();
        ServiceBuilder::new()
            .layer(ValidateRequestHeaderLayer::custom(BrowserRequestPolicy {
                service_name,
                origins: self.clone(),
            }))
            .option_layer(enabled.then(|| browser_cors(&self.0)))
            .option_layer(enabled.then(GrpcWebLayer::new))
            .into_inner()
    }
}

impl<L, T> PublicGrpcServer<L, T> {
    /// Enables gRPC-Web on `service_name` when browser origins are configured.
    ///
    /// Native gRPC, health, and reflection remain available over HTTP/2.
    #[must_use]
    #[allow(
        clippy::type_complexity,
        reason = "the return type preserves the existing Tower layers while adding the browser adapter"
    )]
    pub fn with_browser_transport<S>(
        self,
        origins: &BrowserOrigins,
        service_name: &'static str,
    ) -> PublicGrpcServer<
        L,
        Stack<
            impl Layer<
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
            + use<L, T, S>,
            T,
        >,
    >
    where
        S: Service<Request<Body>, Response = Response<Body>> + Clone + Send + 'static,
        S::Future: Send + 'static,
    {
        self.accept_http1(!origins.0.is_empty())
            .transport_layer(origins.transport_layer(service_name))
    }
}

/// Reports whether the browser transport admitted this request as gRPC-Web,
/// including after the translator rewrites its content type.
#[must_use]
pub fn is_grpc_web_request(request: &Request<Body>) -> bool {
    request.extensions().get::<GrpcWebRequest>().is_some()
}

#[derive(Clone, Copy)]
struct GrpcWebRequest;

fn browser_cors(origins: &[HeaderValue]) -> CorsLayer {
    let cors = match origins {
        [origin] if origin == "*" => CorsLayer::new().allow_origin(origin.clone()).vary([]),
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
            HeaderName::from_static(CHAIN_EVENT_FAMILY_HEADER),
            HeaderName::from_static("grpc-encoding"),
            HeaderName::from_static("grpc-retry-pushback-ms"),
        ])
}

#[derive(Clone)]
struct BrowserRequestPolicy {
    service_name: &'static str,
    origins: BrowserOrigins,
}

impl ValidateRequest<Body> for BrowserRequestPolicy {
    type ResponseBody = Body;

    fn validate(&mut self, request: &mut Request<Body>) -> Result<(), Response<Body>> {
        let content_type = request
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|content_type| content_type.to_str().ok());
        if request.version() == Version::HTTP_2
            && request.method() == Method::POST
            && content_type.is_some_and(|mime| {
                mime == "application/grpc" || mime.starts_with("application/grpc+")
            })
        {
            return Ok(());
        }
        if self.origins.0.is_empty() {
            return Err(rejection(StatusCode::UNSUPPORTED_MEDIA_TYPE));
        }
        if request
            .uri()
            .path()
            .strip_prefix('/')
            .and_then(|path| path.split_once('/'))
            .is_none_or(|(service_name, _)| service_name != self.service_name)
        {
            return Err(rejection(StatusCode::NOT_FOUND));
        }

        let origin_headers = request.headers().get_all(header::ORIGIN);
        let mut origins = origin_headers.iter();
        let allowed = origins.next().map_or_else(
            || request.method() != Method::OPTIONS,
            |origin| {
                self.origins
                    .0
                    .iter()
                    .any(|allowed| allowed == "*" || allowed == origin)
            },
        );
        if !allowed || origins.next().is_some() {
            let mut response = rejection(StatusCode::FORBIDDEN);
            response
                .headers_mut()
                .insert(header::VARY, HeaderValue::from_static("origin"));
            return Err(response);
        }
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
        request.extensions_mut().insert(GrpcWebRequest);
        Ok(())
    }
}

fn rejection(status: StatusCode) -> Response<Body> {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = status;
    response
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use tower::{ServiceExt as _, service_fn};

    use super::*;

    #[tokio::test]
    async fn wildcard_grants_cors_without_credentials() -> eyre::Result<()> {
        let origins: BrowserOrigins = toml::Value::String("*".to_owned()).try_into()?;
        let service =
            origins
                .transport_layer("wallet.Query")
                .layer(service_fn(|_: Request<Body>| async {
                    Ok::<_, Infallible>(Response::new(Body::empty()))
                }));
        let preflight = Request::builder()
            .method(Method::OPTIONS)
            .uri("/wallet.Query/ServerInfo")
            .header(header::ORIGIN, "https://wallet.example")
            .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
            .header(header::ACCESS_CONTROL_REQUEST_HEADERS, "authorization")
            .body(Body::empty())?;
        let response = service.clone().oneshot(preflight).await?;
        assert_eq!(response.status(), StatusCode::OK);
        let headers = response.headers();
        assert_eq!(headers[header::ACCESS_CONTROL_ALLOW_ORIGIN], "*");
        assert!(!headers.contains_key(header::ACCESS_CONTROL_ALLOW_CREDENTIALS));
        let post = Request::post("/wallet.Query/ServerInfo")
            .header(header::ORIGIN, "https://wallet.example")
            .header(header::CONTENT_TYPE, "application/grpc-web+proto")
            .body(Body::empty())?;
        let response = service.oneshot(post).await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response.headers()[header::ACCESS_CONTROL_EXPOSE_HEADERS]
                .to_str()?
                .split(',')
                .any(|name| name.trim() == "grpc-encoding")
        );
        Ok(())
    }

    #[test]
    fn only_native_http2_requests_reach_reflection() -> eyre::Result<()> {
        let mut policy = BrowserRequestPolicy {
            service_name: "wallet.Query",
            origins: toml::Value::String("*".to_owned()).try_into()?,
        };
        for (version, content_type, admitted) in [
            (Version::HTTP_2, "application/grpc", true),
            (Version::HTTP_11, "application/grpc", false),
            (Version::HTTP_2, "application/grpc-web+proto", false),
        ] {
            for reflection_version in ["v1", "v1alpha"] {
                let mut request = Request::post(format!(
                    "/grpc.reflection.{reflection_version}.ServerReflection/ServerReflectionInfo"
                ))
                .version(version)
                .header(header::CONTENT_TYPE, content_type)
                .body(Body::empty())?;
                assert_eq!(policy.validate(&mut request).is_ok(), admitted);
            }
        }
        Ok(())
    }
}
