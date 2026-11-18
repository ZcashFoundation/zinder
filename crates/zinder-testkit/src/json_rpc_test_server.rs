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
//!     method("getblock").reply_with(|params| RpcReply::result(block_hex_for(params))),
//! ])?;
//! // Point the system under test at `server.url()`, then inspect
//! // `server.requests()?` for received calls.
//! ```
//!
//! # Replies
//!
//! [`JsonRpcStubBuilder::reply`] queues one reply that is consumed by the
//! next call to that method. [`JsonRpcStubBuilder::reply_with`] registers a
//! scripted reply computed from the request `params`; it is never consumed,
//! so it can serve any number of calls (for example, a `getblock` stub that
//! answers every height). Queued replies take precedence: a method answers
//! from its queue first and falls back to its scripted reply once the queue
//! is empty.
//!
//! [`RpcReply::Delay`] holds a reply back on the connection's OS thread
//! before answering, and [`RpcReply::Close`] closes the connection without
//! writing a response.
//!
//! # Batches
//!
//! A JSON-RPC batch (a JSON array of request objects) is answered with a
//! JSON array holding one response per member, in request order. Every
//! member is recorded in [`JsonRpcTestServer::requests`] exactly like a
//! single request, and [`JsonRpcTestServer::batch_sizes`] records the member
//! count of each batch. When a member's reply is an HTTP-level reply
//! ([`RpcReply::HttpStatus`], [`RpcReply::Unauthenticated`]) or
//! [`RpcReply::Close`], that reply answers the whole batch and the remaining
//! members are neither recorded nor answered.
//!
//! [`JsonRpcTestServer::shape_batch_responses`] reshapes the response array
//! before it is written ([`BatchResponseShape`]): reordered, truncated, with
//! shifted or duplicated ids, or replaced by a single error object, the shape
//! a node uses when the batch response would exceed its own limit.
//!
//! # Concurrency
//!
//! The server spawns one OS thread per accepted connection, so callers that
//! issue several requests in parallel (`tokio::join!`) are served
//! concurrently and the test observes the same wire surface as a real
//! `zebrad` instance.

use std::{
    collections::{HashMap, VecDeque},
    fmt,
    io::{Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
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

type ScriptedReply = Arc<dyn Fn(&Value) -> RpcReply + Send + Sync>;
type BatchShaper = Arc<dyn Fn(usize) -> Vec<BatchResponseShape> + Send + Sync>;

/// A test-time HTTP server that answers JSON-RPC requests with per-method stubs.
///
/// Stubs are registered by JSON-RPC method name through [`method`]. Multiple
/// queued stubs for the same method form a FIFO per-method queue consumed in
/// registration order; a scripted stub answers once that queue is empty.
/// Calls to unregistered methods, or to a method whose replies are exhausted,
/// receive a JSON-RPC `-32601 Method not found` error.
#[must_use = "the server stops accepting connections on drop"]
pub struct JsonRpcTestServer {
    address: SocketAddr,
    state: Arc<ServerState>,
    accept_handle: Option<JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
}

struct ServerState {
    stubs: Mutex<HashMap<&'static str, MethodStubs>>,
    requests: Mutex<Vec<JsonRpcRequest>>,
    batch_sizes: Mutex<Vec<usize>>,
    batch_shaper: Mutex<Option<BatchShaper>>,
}

#[derive(Default)]
struct MethodStubs {
    queued: VecDeque<RpcReply>,
    scripted: Option<ScriptedReply>,
}

/// One registered stub: a JSON-RPC method name and the reply to return.
#[derive(Debug)]
pub struct JsonRpcStub {
    method: &'static str,
    reply: StubReply,
}

enum StubReply {
    Queued(RpcReply),
    Scripted(ScriptedReply),
}

impl fmt::Debug for StubReply {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Queued(reply) => formatter.debug_tuple("Queued").field(reply).finish(),
            Self::Scripted(_) => formatter.write_str("Scripted(..)"),
        }
    }
}

/// Builder returned by [`method`]. Call [`Self::reply`] or
/// [`Self::reply_with`] to finish the stub.
#[derive(Debug)]
#[must_use = "JsonRpcStubBuilder is a builder; call reply() or reply_with() to finish it"]
pub struct JsonRpcStubBuilder {
    method: &'static str,
}

impl JsonRpcStubBuilder {
    /// Queues a reply consumed by the next call to the registered method.
    #[must_use]
    pub const fn reply(self, reply: RpcReply) -> JsonRpcStub {
        JsonRpcStub {
            method: self.method,
            reply: StubReply::Queued(reply),
        }
    }

    /// Registers a reply computed from each call's `params`.
    ///
    /// The scripted reply is not consumed: it answers every call to the
    /// method once the method's queued replies are exhausted. Registering a
    /// second scripted reply for the same method replaces the first.
    #[must_use]
    pub fn reply_with(
        self,
        reply: impl Fn(&Value) -> RpcReply + Send + Sync + 'static,
    ) -> JsonRpcStub {
        JsonRpcStub {
            method: self.method,
            reply: StubReply::Scripted(Arc::new(reply)),
        }
    }
}

/// Names a JSON-RPC method to stub. Combine with [`JsonRpcStubBuilder::reply`]
/// or [`JsonRpcStubBuilder::reply_with`].
pub const fn method(name: &'static str) -> JsonRpcStubBuilder {
    JsonRpcStubBuilder { method: name }
}

/// One recorded JSON-RPC request the server received.
///
/// Each member of a batch is recorded as its own request.
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
    /// Waits for the duration on the connection's OS thread, then answers
    /// with the inner reply.
    Delay(Duration, Box<Self>),
    /// Closes the connection without writing a response.
    Close,
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

    /// Builds a reply that answers with `reply` after `delay`.
    #[must_use]
    pub fn delayed(delay: Duration, reply: Self) -> Self {
        Self::Delay(delay, Box::new(reply))
    }

    /// Builds a reply that closes the connection without a response.
    #[must_use]
    pub const fn close() -> Self {
        Self::Close
    }

    fn method_not_found(method_name: &str) -> Self {
        Self::error_with_code(
            -32601,
            format!("no stub registered for method {method_name}"),
        )
    }

    fn resolve(self, request_id: &Value) -> ResolvedReply {
        match self {
            Self::Result(rpc_result) => ResolvedReply::Body(
                json!({"jsonrpc": "2.0", "id": request_id, "result": rpc_result}),
            ),
            Self::Error { code, message } => ResolvedReply::Body(json!({
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": code.unwrap_or(-32603), "message": message},
            })),
            Self::HttpStatus(status_code) => ResolvedReply::Http(HttpResponse {
                status_code,
                reason_phrase: match status_code {
                    503 => "Service Unavailable",
                    _ => "Status",
                },
                extra_headers: "",
                body: "{}".to_owned(),
            }),
            Self::Unauthenticated => ResolvedReply::Http(HttpResponse {
                status_code: 401,
                reason_phrase: "Unauthorized",
                extra_headers: "WWW-Authenticate: Basic realm=\"jsonrpc\"\r\n",
                body: json!({
                    "jsonrpc": "2.0",
                    "error": {"code": 401, "message": "unauthenticated method"},
                    "id": null,
                })
                .to_string(),
            }),
            Self::Empty => ResolvedReply::Body(json!({"jsonrpc": "2.0", "id": request_id})),
            Self::Delay(delay, reply) => {
                thread::sleep(delay);
                reply.resolve(request_id)
            }
            Self::Close => ResolvedReply::Close,
        }
    }
}

/// One transformation applied to a batch response array before it is written.
///
/// Shapes apply in order. [`Self::SingleErrorObject`] replaces the array, so
/// shapes after it have no effect.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum BatchResponseShape {
    /// Reverses the order of the response members.
    Reverse,
    /// Replaces the whole array with one JSON-RPC error object whose `id` is
    /// `null`, the answer a node gives when the batch response would exceed
    /// its own response limit.
    SingleErrorObject {
        /// JSON-RPC error code.
        code: i64,
        /// JSON-RPC error message.
        message: String,
    },
    /// Removes the last response member.
    DropLast,
    /// Adds the offset to every numeric response `id`.
    ShiftIds(i64),
    /// Appends a copy of the first response member.
    DuplicateFirst,
}

impl BatchResponseShape {
    /// Builds a [`Self::SingleErrorObject`] shape.
    #[must_use]
    pub fn single_error_object(code: i64, message: impl Into<String>) -> Self {
        Self::SingleErrorObject {
            code,
            message: message.into(),
        }
    }
}

impl JsonRpcTestServer {
    /// Starts a server bound to an ephemeral local port and registers the
    /// supplied stubs. The server runs until the returned handle is dropped.
    pub fn start(stubs: impl IntoIterator<Item = JsonRpcStub>) -> Result<Self> {
        let mut grouped: HashMap<&'static str, MethodStubs> = HashMap::new();
        for stub in stubs {
            let method_stubs = grouped.entry(stub.method).or_default();
            match stub.reply {
                StubReply::Queued(reply) => method_stubs.queued.push_back(reply),
                StubReply::Scripted(reply) => method_stubs.scripted = Some(reply),
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let state = Arc::new(ServerState {
            stubs: Mutex::new(grouped),
            requests: Mutex::new(Vec::new()),
            batch_sizes: Mutex::new(Vec::new()),
            batch_shaper: Mutex::new(None),
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
    /// in arrival order. Batch members appear in request order.
    pub fn requests(&self) -> Result<Vec<JsonRpcRequest>> {
        Ok(self.state.requests.lock().clone())
    }

    /// Returns the requests recorded for `method`, in arrival order.
    pub fn requests_for(&self, method: &str) -> Result<Vec<JsonRpcRequest>> {
        self.requests()
            .map(|all| all.into_iter().filter(|req| req.method == method).collect())
    }

    /// Returns the member count of every batch received so far, in arrival
    /// order. Single (non-batch) requests are not counted.
    #[must_use]
    pub fn batch_sizes(&self) -> Vec<usize> {
        self.state.batch_sizes.lock().clone()
    }

    /// Reshapes later batch responses.
    ///
    /// `shaper` receives each batch's member count and returns the shapes to
    /// apply to that batch's response; an empty list writes the response
    /// unchanged. Replaces any earlier shaper.
    pub fn shape_batch_responses(
        &self,
        shaper: impl Fn(usize) -> Vec<BatchResponseShape> + Send + Sync + 'static,
    ) {
        *self.state.batch_shaper.lock() = Some(Arc::new(shaper));
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

impl ServerState {
    fn answer(&self, request: JsonRpcRequest) -> ResolvedReply {
        let reply = self.next_reply(&request);
        let request_id = request.id.clone();
        self.requests.lock().push(request);
        reply.resolve(&request_id)
    }

    fn next_reply(&self, request: &JsonRpcRequest) -> RpcReply {
        let mut stubs = self.stubs.lock();
        let Some(method_stubs) = stubs.get_mut(request.method.as_str()) else {
            return RpcReply::method_not_found(&request.method);
        };
        if let Some(reply) = method_stubs.queued.pop_front() {
            return reply;
        }
        let scripted = method_stubs.scripted.clone();
        drop(stubs);
        scripted.map_or_else(
            || RpcReply::method_not_found(&request.method),
            |scripted| scripted(&request.params),
        )
    }

    fn answer_batch(&self, members: &[Value], authorization: Option<&str>) -> ResolvedReply {
        self.batch_sizes.lock().push(members.len());
        let mut responses = Vec::with_capacity(members.len());
        for member in members {
            let resolved = parse_request(member, authorization).map_or_else(
                || {
                    ResolvedReply::Body(json!({
                        "jsonrpc": "2.0",
                        "id": null,
                        "error": {"code": -32600, "message": "batch member is not a JSON-RPC request"},
                    }))
                },
                |request| self.answer(request),
            );
            match resolved {
                ResolvedReply::Body(response) => responses.push(response),
                whole_batch_reply @ (ResolvedReply::Http(_) | ResolvedReply::Close) => {
                    return whole_batch_reply;
                }
            }
        }

        let batch_shaper = self.batch_shaper.lock().clone();
        let shapes = batch_shaper
            .map(|shape_batch| shape_batch(members.len()))
            .unwrap_or_default();
        ResolvedReply::Body(shape_batch_response(responses, &shapes))
    }
}

fn shape_batch_response(mut responses: Vec<Value>, shapes: &[BatchResponseShape]) -> Value {
    for shape in shapes {
        match shape {
            BatchResponseShape::Reverse => responses.reverse(),
            BatchResponseShape::SingleErrorObject { code, message } => {
                return json!({
                    "jsonrpc": "2.0",
                    "id": null,
                    "error": {"code": code, "message": message},
                });
            }
            BatchResponseShape::DropLast => {
                responses.pop();
            }
            BatchResponseShape::ShiftIds(offset) => {
                for response in &mut responses {
                    if let Some(id) = response.get_mut("id")
                        && let Some(numeric_id) = id.as_i64()
                    {
                        *id = Value::from(numeric_id.saturating_add(*offset));
                    }
                }
            }
            BatchResponseShape::DuplicateFirst => {
                if let Some(first) = responses.first().cloned() {
                    responses.push(first);
                }
            }
        }
    }
    Value::Array(responses)
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

fn handle_connection(mut stream: TcpStream, state: &ServerState) {
    // The listener is non-blocking; accepted streams may inherit that flag
    // on some platforms, which makes the read_timeout ineffective. Force the
    // stream back to blocking so reads behave as the protocol expects.
    if stream.set_nonblocking(false).is_err() {
        return;
    }
    let _ = stream.set_read_timeout(Some(READ_TIMEOUT));
    let Ok(http_request) = read_http_request(&mut stream) else {
        return;
    };
    let authorization = http_request.authorization.as_deref();

    let resolved = match &http_request.body {
        Value::Array(members) => state.answer_batch(members, authorization),
        single @ (Value::Null
        | Value::Bool(_)
        | Value::Number(_)
        | Value::String(_)
        | Value::Object(_)) => {
            let Some(request) = parse_request(single, authorization) else {
                return;
            };
            state.answer(request)
        }
    };

    match resolved {
        ResolvedReply::Body(body) => {
            let _ = write_http_response(&mut stream, &HttpResponse::ok(body.to_string()));
        }
        ResolvedReply::Http(response) => {
            let _ = write_http_response(&mut stream, &response);
        }
        ResolvedReply::Close => {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }
}

enum ResolvedReply {
    Body(Value),
    Http(HttpResponse),
    Close,
}

struct HttpResponse {
    status_code: u16,
    reason_phrase: &'static str,
    /// Header lines beyond the standard set, each ending in `\r\n`.
    extra_headers: &'static str,
    body: String,
}

impl HttpResponse {
    const fn ok(body: String) -> Self {
        Self {
            status_code: 200,
            reason_phrase: "OK",
            extra_headers: "",
            body,
        }
    }
}

struct HttpRequest {
    authorization: Option<String>,
    body: Value,
}

fn parse_request(request_body: &Value, authorization: Option<&str>) -> Option<JsonRpcRequest> {
    let method = request_body.get("method")?.as_str()?.to_owned();
    Some(JsonRpcRequest {
        method,
        params: request_body.get("params").cloned().unwrap_or(Value::Null),
        authorization: authorization.map(str::to_owned),
        id: request_body.get("id").cloned().unwrap_or(Value::Null),
    })
}

fn read_http_request(stream: &mut TcpStream) -> Result<HttpRequest> {
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

    let body = serde_json::from_slice(&request_bytes[body_start..body_start + content_length])?;
    Ok(HttpRequest {
        authorization: authorization_header(&headers),
        body,
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
