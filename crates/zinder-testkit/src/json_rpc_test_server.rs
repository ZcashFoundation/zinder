//! Method-keyed JSON-RPC test server.
//!
//! Tests exercise `zinder-source`'s Zebra JSON-RPC adapters by stubbing
//! responses per RPC method, not per connection. The server dispatches each
//! request to the stub registered for its method, so callers stay decoupled
//! from internal call order — parallelization, batching, or new probes in
//! the source layer do not break tests that don't care about those changes.
//!
//! # Usage
//!
//! ```text
//! use zinder_testkit::json_rpc_test_server::{JsonRpcTestServer, RpcReply, method};
//!
//! let server = JsonRpcTestServer::start([
//!     method("getblockhash").reply(RpcReply::result(serde_json::json!("...hash..."))),
//!     method("getblockheader").reply(RpcReply::result(serde_json::json!({}))),
//! ])?;
//! // Point the system under test at `server.url()`, then inspect
//! // `server.requests()?` for received calls.
//! ```
//!
//! # Concurrency
//!
//! The server spawns one OS thread per accepted connection, so callers that
//! issue several requests in parallel (`tokio::join!`) are served
//! concurrently and the test observes the same wire surface as a real
//! `zebrad` instance.

use std::{
    collections::{HashMap, VecDeque},
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use eyre::{Result, eyre};
use parking_lot::Mutex;
use serde_json::{Value, json};

const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(1);
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// A test-time HTTP server that answers JSON-RPC requests with per-method stubs.
///
/// Stubs are registered by JSON-RPC method name through [`method`]. Multiple
/// stubs for the same method form a FIFO per-method queue consumed in
/// registration order. Calls to unregistered methods receive a JSON-RPC
/// `-32601 Method not found` error.
#[must_use = "the server stops accepting connections on drop"]
pub struct JsonRpcTestServer {
    address: SocketAddr,
    state: Arc<ServerState>,
    accept_handle: Option<JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
}

struct ServerState {
    stubs: Mutex<HashMap<&'static str, VecDeque<RpcReply>>>,
    requests: Mutex<Vec<JsonRpcRequest>>,
}

/// One registered stub: a JSON-RPC method name and the reply to return.
#[derive(Debug)]
pub struct JsonRpcStub {
    method: &'static str,
    reply: RpcReply,
}

/// Builder returned by [`method`]. Call [`Self::reply`] to finish the stub.
#[derive(Debug)]
#[must_use = "JsonRpcStubBuilder is a builder; call reply() to finish it"]
pub struct JsonRpcStubBuilder {
    method: &'static str,
}

impl JsonRpcStubBuilder {
    /// Sets the reply this stub returns when the registered method is called.
    #[must_use]
    pub const fn reply(self, reply: RpcReply) -> JsonRpcStub {
        JsonRpcStub {
            method: self.method,
            reply,
        }
    }
}

/// Names a JSON-RPC method to stub. Combine with [`JsonRpcStubBuilder::reply`].
pub const fn method(name: &'static str) -> JsonRpcStubBuilder {
    JsonRpcStubBuilder { method: name }
}

/// One recorded JSON-RPC request the server received.
#[derive(Clone, Debug)]
pub struct JsonRpcRequest {
    /// JSON-RPC method name from the request body.
    pub method: String,
    /// Request `params` field, or `Value::Null` when absent.
    pub params: Value,
    /// Value of the request's `Authorization:` HTTP header, when present.
    pub authorization: Option<String>,
    /// Request `id` field, or `Value::Null` when absent.
    pub id: Value,
}

/// Reply the server returns for a stubbed request.
#[derive(Debug)]
#[non_exhaustive]
pub enum RpcReply {
    /// Successful JSON-RPC result value.
    Result(Value),
    /// JSON-RPC error object with optional numeric code and message.
    Error {
        /// Error code (`None` falls back to `-32603 Internal error`).
        code: Option<i64>,
        /// Error message.
        message: String,
    },
    /// Non-200 HTTP status code with an empty JSON body.
    HttpStatus(u16),
    /// The HTTP 401 answer Zebra 7.1.0 and later send for missing or stale
    /// cookie credentials: a `WWW-Authenticate` challenge and a JSON-RPC
    /// error body with code 401.
    Unauthenticated,
    /// JSON-RPC response with neither `result` nor `error` set.
    Empty,
}

impl RpcReply {
    /// Builds a `result`-bearing reply.
    #[must_use]
    pub const fn result(rpc_result: Value) -> Self {
        Self::Result(rpc_result)
    }

    /// Builds an error reply with the default `-8` code used by node-side
    /// validation failures.
    #[must_use]
    pub fn error(message: impl Into<String>) -> Self {
        Self::error_with_code(-8, message)
    }

    /// Builds an error reply with an explicit numeric error code.
    #[must_use]
    pub fn error_with_code(code: i64, message: impl Into<String>) -> Self {
        Self::Error {
            code: Some(code),
            message: message.into(),
        }
    }

    /// Builds an error reply with no numeric error code.
    #[must_use]
    pub fn error_without_code(message: impl Into<String>) -> Self {
        Self::Error {
            code: None,
            message: message.into(),
        }
    }

    /// Builds a non-200 HTTP status reply.
    #[must_use]
    pub const fn http_status(status_code: u16) -> Self {
        Self::HttpStatus(status_code)
    }

    /// Builds Zebra's HTTP 401 refusal of missing or stale cookie credentials.
    #[must_use]
    pub const fn unauthenticated() -> Self {
        Self::Unauthenticated
    }

    /// Builds an empty JSON-RPC response (no `result` and no `error` field).
    #[must_use]
    pub const fn empty() -> Self {
        Self::Empty
    }

    fn into_http_response(self, request_id: &Value) -> HttpResponse {
        match self {
            Self::Result(rpc_result) => HttpResponse::ok(
                json!({"jsonrpc": "2.0", "id": request_id, "result": rpc_result}).to_string(),
            ),
            Self::Error { code, message } => {
                let body = code.map_or_else(
                    || {
                        json!({
                            "jsonrpc": "2.0",
                            "id": request_id,
                            "error": {"message": message, "code": -32603},
                        })
                        .to_string()
                    },
                    |code| {
                        json!({
                            "jsonrpc": "2.0",
                            "id": request_id,
                            "error": {"code": code, "message": message},
                        })
                        .to_string()
                    },
                );
                HttpResponse::ok(body)
            }
            Self::HttpStatus(status_code) => HttpResponse {
                status_code,
                reason_phrase: match status_code {
                    503 => "Service Unavailable",
                    _ => "Status",
                },
                extra_headers: "",
                body: "{}".to_owned(),
            },
            Self::Unauthenticated => HttpResponse {
                status_code: 401,
                reason_phrase: "Unauthorized",
                extra_headers: "WWW-Authenticate: Basic realm=\"jsonrpc\"\r\n",
                body: json!({
                    "jsonrpc": "2.0",
                    "error": {"code": 401, "message": "unauthenticated method"},
                    "id": null,
                })
                .to_string(),
            },
            Self::Empty => {
                HttpResponse::ok(json!({"jsonrpc": "2.0", "id": request_id}).to_string())
            }
        }
    }
}

impl JsonRpcTestServer {
    /// Starts a server bound to an ephemeral local port and registers the
    /// supplied stubs. The server runs until the returned handle is dropped.
    pub fn start(stubs: impl IntoIterator<Item = JsonRpcStub>) -> Result<Self> {
        let mut grouped: HashMap<&'static str, VecDeque<RpcReply>> = HashMap::new();
        for stub in stubs {
            grouped
                .entry(stub.method)
                .or_default()
                .push_back(stub.reply);
        }

        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let state = Arc::new(ServerState {
            stubs: Mutex::new(grouped),
            requests: Mutex::new(Vec::new()),
        });
        let shutdown = Arc::new(AtomicBool::new(false));
        let accept_state = Arc::clone(&state);
        let accept_shutdown = Arc::clone(&shutdown);
        let accept_handle = thread::spawn(move || {
            accept_loop(&listener, &accept_state, &accept_shutdown);
        });

        Ok(Self {
            address,
            state,
            accept_handle: Some(accept_handle),
            shutdown,
        })
    }

    /// Returns the `http://...` URL clients should target.
    #[must_use]
    pub fn url(&self) -> String {
        format!("http://{}", self.address)
    }

    /// Returns a snapshot of every request the server has recorded so far,
    /// in arrival order.
    pub fn requests(&self) -> Result<Vec<JsonRpcRequest>> {
        Ok(self.state.requests.lock().clone())
    }

    /// Returns the requests recorded for `method`, in arrival order.
    pub fn requests_for(&self, method: &str) -> Result<Vec<JsonRpcRequest>> {
        self.requests()
            .map(|all| all.into_iter().filter(|req| req.method == method).collect())
    }
}

impl Drop for JsonRpcTestServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(handle) = self.accept_handle.take()
            && handle.join().is_err()
        {
            tracing::warn!(
                target: "zinder::testkit",
                "JsonRpcTestServer accept thread panicked during shutdown"
            );
        }
    }
}

fn accept_loop(listener: &TcpListener, state: &Arc<ServerState>, shutdown: &Arc<AtomicBool>) {
    while !shutdown.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((stream, _addr)) => {
                let state = Arc::clone(state);
                thread::spawn(move || handle_connection(stream, &state));
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(ACCEPT_POLL_INTERVAL);
            }
            Err(_) => return,
        }
    }
}

fn handle_connection(mut stream: TcpStream, state: &Arc<ServerState>) {
    // The listener is non-blocking; accepted streams may inherit that flag
    // on some platforms, which makes the read_timeout ineffective. Force the
    // stream back to blocking so reads behave as the protocol expects.
    if stream.set_nonblocking(false).is_err() {
        return;
    }
    let _ = stream.set_read_timeout(Some(READ_TIMEOUT));
    let Ok(request) = read_http_request(&mut stream) else {
        return;
    };

    let reply = state
        .stubs
        .lock()
        .get_mut(request.method.as_str())
        .and_then(VecDeque::pop_front);

    state.requests.lock().push(request.clone());

    let response = match reply {
        Some(reply) => reply.into_http_response(&request.id),
        None => HttpResponse::ok(
            json!({
                "jsonrpc": "2.0",
                "id": request.id,
                "error": {
                    "code": -32601,
                    "message": format!("no stub registered for method {}", request.method),
                },
            })
            .to_string(),
        ),
    };

    let _ = write_http_response(&mut stream, &response);
}

struct HttpResponse {
    status_code: u16,
    reason_phrase: &'static str,
    /// Header lines beyond the standard set, each ending in `\r\n`.
    extra_headers: &'static str,
    body: String,
}

impl HttpResponse {
    fn ok(body: String) -> Self {
        Self {
            status_code: 200,
            reason_phrase: "OK",
            extra_headers: "",
            body,
        }
    }
}

fn read_http_request(stream: &mut TcpStream) -> Result<JsonRpcRequest> {
    let mut request_bytes = Vec::new();
    let mut buffer = [0; 1024];
    let header_end = loop {
        let byte_count = stream.read(&mut buffer)?;
        if byte_count == 0 {
            return Err(eyre!("HTTP request ended before headers"));
        }
        request_bytes.extend_from_slice(&buffer[..byte_count]);
        if let Some(header_end) = find_header_end(&request_bytes) {
            break header_end;
        }
    };

    let headers = String::from_utf8(request_bytes[..header_end].to_vec())
        .map_err(|error| eyre!("HTTP request headers are not UTF-8: {error}"))?;
    let content_length = content_length(&headers)?;
    let body_start = header_end + 4;
    while request_bytes.len() < body_start + content_length {
        let byte_count = stream.read(&mut buffer)?;
        if byte_count == 0 {
            return Err(eyre!("HTTP request ended before body"));
        }
        request_bytes.extend_from_slice(&buffer[..byte_count]);
    }

    let body = String::from_utf8(request_bytes[body_start..body_start + content_length].to_vec())
        .map_err(|error| eyre!("HTTP request body is not UTF-8: {error}"))?;
    let body_json: Value = serde_json::from_str(&body)?;
    let method = body_json
        .get("method")
        .and_then(Value::as_str)
        .ok_or_else(|| eyre!("JSON-RPC request missing method"))?
        .to_owned();
    let params = body_json.get("params").cloned().unwrap_or(Value::Null);
    let id = body_json.get("id").cloned().unwrap_or(Value::Null);

    Ok(JsonRpcRequest {
        method,
        params,
        authorization: authorization_header(&headers),
        id,
    })
}

fn write_http_response(stream: &mut TcpStream, response: &HttpResponse) -> Result<()> {
    let formatted = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\n{}Content-Length: {}\r\nConnection: close\r\n\r\n{}",
        response.status_code,
        response.reason_phrase,
        response.extra_headers,
        response.body.len(),
        response.body
    );
    stream.write_all(formatted.as_bytes())?;
    Ok(())
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes
        .array_windows::<4>()
        .position(|window| window == b"\r\n\r\n")
}

fn content_length(headers: &str) -> Result<usize> {
    let raw = headers
        .lines()
        .find_map(|line| {
            let (name, header_value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| header_value.trim())
        })
        .ok_or_else(|| eyre!("HTTP request missing content-length"))?;
    raw.parse::<usize>()
        .map_err(|error| eyre!("HTTP content-length is not a usize: {error}"))
}

fn authorization_header(headers: &str) -> Option<String> {
    headers.lines().find_map(|line| {
        let (name, header_value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("authorization")
            .then(|| header_value.trim().to_owned())
    })
}
