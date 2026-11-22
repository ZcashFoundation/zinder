//! A verified `GetBlockRange` wire probe for a running lightwalletd-compatible
//! server.
//!
//! The probe checks what a server puts on the wire, not what a client decodes
//! from it. Each response frame is hashed as the raw protobuf payload, length
//! framed, so a server that reorders blocks, drops one, or changes a byte fails
//! even when a wallet would still parse the result. Every request is compared
//! with an oracle (frame count, payload bytes, and digest) that comes from a
//! file or from a different, reference endpoint, so the server under test never
//! grades itself.
//!
//! One run opens one HTTP/2 connection per client, runs the untimed warmup
//! passes, then times every client's passes together. Chain identity and the
//! tip are read before and after the timed interval, outside it. A server
//! process, given by pid, is sampled read-only from `/proc` around the same
//! interval.

use std::{path::PathBuf, sync::Arc, time::Duration};

use clap::Args;
use prost::{
    Message as _,
    bytes::{Buf as _, Bytes},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::{task::JoinHandle, time::Instant};
use tonic::{
    Status,
    client::Grpc,
    codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder},
    codegen::http::uri::PathAndQuery,
    transport::Channel,
};
use zinder_bench::BenchError;
use zinder_core::wire::{decode_internal_block_hash, encode_rpc_block_hash_hex};
use zinder_proto::compat::lightwalletd::{
    BlockId, BlockRange, ChainSpec, Empty, LightdInfo,
    compact_tx_streamer_client::CompactTxStreamerClient,
};

mod counters;

use counters::{CounterSource, CountersReport, Selector};

use super::transport::{self, TlsOptions};

const ORACLE_SCHEMA: &str = "zinder-bench/raw-range-oracle/v1";
const REPORT_SCHEMA: &str = "zinder-bench/raw-range-probe/v1";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const UNARY_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_REPORTED_PROBLEMS: usize = 20;
const SHA256_HEX_LEN: usize = 64;

/// Arguments of `zinder-bench raw-range-probe`.
#[derive(Args)]
pub(crate) struct RawRangeProbeArgs {
    /// Server under test, such as `http://127.0.0.1:9067`. Plaintext HTTP/2
    /// and `https` with the platform's trusted roots are supported.
    #[arg(long)]
    endpoint: String,
    /// Expected results, written earlier by `--write-oracle` or by any other
    /// tool that follows the oracle schema in the bench README.
    #[arg(
        long,
        value_name = "FILE",
        required_unless_present = "oracle_endpoint",
        conflicts_with_all = ["oracle_endpoint", "ranges", "both_directions", "write_oracle"]
    )]
    oracle: Option<PathBuf>,
    /// A different, reference server that produces the expected results for
    /// every `--range` before the probe starts.
    #[arg(long, value_name = "URL", requires = "ranges")]
    oracle_endpoint: Option<String>,
    /// A range to request as `START:END`. `START` above `END` streams the
    /// blocks in descending order.
    #[arg(
        long = "range",
        value_name = "START:END",
        value_parser = parse_range,
        requires = "oracle_endpoint"
    )]
    ranges: Vec<(u64, u64)>,
    /// Also request every `--range` in the opposite direction.
    #[arg(long, requires = "ranges")]
    both_directions: bool,
    /// Write the expected results taken from `--oracle-endpoint` to a new file.
    #[arg(long, value_name = "FILE", requires = "oracle_endpoint")]
    write_oracle: Option<PathBuf>,
    /// Concurrent clients. Each opens its own connection.
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..))]
    clients: u32,
    /// Timed passes over every range, per client.
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..))]
    repetitions: u32,
    /// Untimed, verified passes over every range, per client, before the timed
    /// interval.
    #[arg(long, default_value_t = 0)]
    warmup_repetitions: u32,
    /// Longest one request may take, from sending it to the end of its stream.
    #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(1..))]
    request_timeout_secs: u64,
    /// Local server process to sample read-only from `/proc` (Linux): CPU time
    /// and resident memory around the timed interval.
    #[arg(long, requires = "server_executable")]
    server_pid: Option<u32>,
    /// Expected running reader executable for process CPU and memory attribution.
    #[arg(long, requires = "server_pid", value_name = "FILE")]
    server_executable: Option<PathBuf>,
    /// The server's Prometheus `/metrics` URL, read around the timed interval
    /// for the `--server-counter` series and `--rocksdb-multiget-counters`.
    #[arg(long, value_name = "URL", requires_all = ["counter_series", "freshness_witness"])]
    server_metrics_url: Option<String>,
    /// A series whose increase over the timed interval is reported, as
    /// `[ALIAS=]NAME[{label=value,...}]`, summed over every other label.
    #[arg(
        long = "server-counter",
        value_name = "SELECTOR",
        value_parser = counters::parse_selector,
        requires = "server_metrics_url",
        group = "counter_series"
    )]
    server_counters: Vec<Selector>,
    /// Report the canonical secondary's `RocksDB` `MultiGet` call, key, and
    /// byte tickers as `multiget_get`, `multiget_keys_read`, and
    /// `multiget_bytes_read`.
    #[arg(long, requires = "server_metrics_url", group = "counter_series")]
    rocksdb_multiget_counters: bool,
    /// A series the server increments with every publication of its counters,
    /// such as `zinder_wallet_serving_pair_publisher_refresh_total{status=ok}`.
    /// Counters are read only from a scrape after it advances, so a stale
    /// publication cannot read as zero. Without a witness that advances, the
    /// counters are reported unavailable.
    #[arg(
        long,
        value_name = "SELECTOR",
        value_parser = counters::parse_selector,
        requires = "server_metrics_url",
        group = "freshness_witness"
    )]
    server_freshness_counter: Option<Selector>,
    /// Longest each counter read waits for the witness to advance.
    #[arg(long, default_value_t = 6, value_parser = clap::value_parser!(u64).range(1..))]
    server_counter_wait_secs: u64,
    /// Write the report to this new file instead of stdout.
    #[arg(long)]
    report: Option<PathBuf>,
}

fn parse_range(text: &str) -> Result<(u64, u64), String> {
    let (start, end) = text
        .split_once(':')
        .ok_or_else(|| "expected START:END".to_owned())?;
    let height = |label: &str, digits: &str| {
        digits
            .parse::<u64>()
            .map_err(|error| format!("{label} height `{digits}`: {error}"))
    };
    Ok((height("start", start)?, height("end", end)?))
}

/// The probe's report and where to write it.
pub(crate) struct ProbeOutput {
    pub(crate) report: ProbeReport,
    pub(crate) report_path: Option<PathBuf>,
}

impl ProbeOutput {
    /// Fails unless every request matched its oracle.
    pub(crate) fn require_verified(&self) -> Result<(), BenchError> {
        if self.report.verified {
            return Ok(());
        }
        Err(BenchError::raw_range_probe(format!(
            "range verification failed: {}",
            self.report
                .problems
                .first()
                .map_or("no timed request completed", String::as_str)
        )))
    }
}

/// What one range must return.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct OracleJob {
    start: u64,
    end: u64,
    blocks: u64,
    bytes: u64,
    /// SHA-256 over each frame's payload length as a big-endian `u32`
    /// followed by the payload, in stream order.
    framed_sha256: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct OracleFile {
    schema: String,
    jobs: Vec<OracleJob>,
}

impl OracleFile {
    fn validate(&self) -> Result<(), BenchError> {
        if self.schema != ORACLE_SCHEMA {
            return Err(BenchError::invalid_argument(format!(
                "oracle schema `{}` is not `{ORACLE_SCHEMA}`",
                self.schema
            )));
        }
        if self.jobs.is_empty() {
            return Err(BenchError::invalid_argument("oracle lists no ranges"));
        }
        for job in &self.jobs {
            let expected_blocks = job.start.abs_diff(job.end).saturating_add(1);
            let digest_is_hex = job.framed_sha256.len() == SHA256_HEX_LEN
                && job
                    .framed_sha256
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
            if job.blocks != expected_blocks || job.bytes == 0 || !digest_is_hex {
                return Err(BenchError::invalid_argument(format!(
                    "oracle range {}:{} is inconsistent: {} blocks for a span of {expected_blocks}, \
                     {} bytes, digest `{}`",
                    job.start, job.end, job.blocks, job.bytes, job.framed_sha256
                )));
            }
        }
        Ok(())
    }

    fn digest(&self) -> Result<String, BenchError> {
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&self.jobs)?)))
    }
}

/// Hashes response frames the way the oracle defines them.
struct FramedDigest {
    hasher: Sha256,
    frames: u64,
    bytes: u64,
}

impl FramedDigest {
    fn new() -> Self {
        Self {
            hasher: Sha256::new(),
            frames: 0,
            bytes: 0,
        }
    }

    fn push(&mut self, payload: &[u8]) {
        let length = u32::try_from(payload.len()).unwrap_or(u32::MAX);
        self.hasher.update(length.to_be_bytes());
        self.hasher.update(payload);
        self.frames += 1;
        self.bytes += u64::try_from(payload.len()).unwrap_or(u64::MAX);
    }
}

/// A codec that sends a typed `BlockRange` and returns each response frame as
/// the raw protobuf payload, without decoding it.
struct RawFrames;
struct RangeEncoder;
struct FrameDecoder;

impl Encoder for RangeEncoder {
    type Item = BlockRange;
    type Error = Status;

    fn encode(&mut self, request: BlockRange, buffer: &mut EncodeBuf<'_>) -> Result<(), Status> {
        request
            .encode(buffer)
            .map_err(|error| Status::internal(error.to_string()))
    }
}

impl Decoder for FrameDecoder {
    type Item = Bytes;
    type Error = Status;

    fn decode(&mut self, frame: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        Ok(Some(frame.copy_to_bytes(frame.remaining())))
    }
}

impl Codec for RawFrames {
    type Encode = BlockRange;
    type Decode = Bytes;
    type Encoder = RangeEncoder;
    type Decoder = FrameDecoder;

    fn encoder(&mut self) -> RangeEncoder {
        RangeEncoder
    }

    fn decoder(&mut self) -> FrameDecoder {
        FrameDecoder
    }
}

/// What one `GetBlockRange` stream delivered.
struct Streamed {
    frames: u64,
    bytes: u64,
    framed_sha256: String,
    first_frame: Duration,
    completion: Duration,
}

/// Streams `start` to `end` and hashes every frame. The timing starts when the
/// request is sent, on a ready channel.
async fn stream_range(
    client: &mut Grpc<Channel>,
    start: u64,
    end: u64,
    timeout: Duration,
) -> Result<Streamed, String> {
    let block = |height| {
        Some(BlockId {
            height,
            hash: Vec::new(),
        })
    };
    let request = BlockRange {
        start: block(start),
        end: block(end),
        pool_types: Vec::new(),
    };
    let stream = async {
        client
            .ready()
            .await
            .map_err(|error| format!("channel not ready: {error}"))?;
        let started = Instant::now();
        let mut frames = client
            .server_streaming(
                tonic::Request::new(request),
                PathAndQuery::from_static("/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetBlockRange"),
                RawFrames,
            )
            .await
            .map_err(|status| status_text(&status))?
            .into_inner();
        let mut digest = FramedDigest::new();
        let mut first_frame = None;
        while let Some(frame) = frames
            .message()
            .await
            .map_err(|status| status_text(&status))?
        {
            first_frame.get_or_insert_with(|| started.elapsed());
            digest.push(&frame);
        }
        let completion = started.elapsed();
        Ok::<_, String>(Streamed {
            frames: digest.frames,
            bytes: digest.bytes,
            framed_sha256: hex::encode(digest.hasher.finalize()),
            first_frame: first_frame.unwrap_or(completion),
            completion,
        })
    };
    tokio::time::timeout(timeout, stream)
        .await
        .map_err(|_| format!("stream did not finish within {}s", timeout.as_secs_f64()))?
}

fn status_text(status: &Status) -> String {
    format!("{:?}: {}", status.code(), status.message())
}

/// `None` when the stream is exactly what the oracle expects, otherwise what
/// differs.
fn difference(job: &OracleJob, streamed: &Streamed) -> Option<String> {
    let mut differences = Vec::new();
    if streamed.frames != job.blocks {
        differences.push(format!("frames {} != {}", streamed.frames, job.blocks));
    }
    if streamed.bytes != job.bytes {
        differences.push(format!("bytes {} != {}", streamed.bytes, job.bytes));
    }
    if streamed.framed_sha256 != job.framed_sha256 {
        differences.push(format!(
            "framed_sha256 {} != {}",
            streamed.framed_sha256, job.framed_sha256
        ));
    }
    (!differences.is_empty()).then(|| differences.join("; "))
}

/// One request's outcome, kept as measured.
#[derive(Clone, Debug, Serialize)]
struct Observation {
    client: u32,
    repetition: u32,
    job: usize,
    start: u64,
    end: u64,
    frames: u64,
    bytes: u64,
    framed_sha256: String,
    /// Offset of the request from the start of its phase.
    started_ns: u64,
    first_frame_ns: u64,
    completion_ns: u64,
    /// `verified`, `mismatch`, or `error`.
    outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// What every client of one phase repeats.
#[derive(Clone)]
struct Pass {
    jobs: Arc<[OracleJob]>,
    repetitions: u32,
    phase_start: Instant,
    timeout: Duration,
}

/// One client's passes over every job, in order, on its own connection.
async fn run_client(client_index: u32, channel: Channel, pass: Pass) -> Vec<Observation> {
    let mut client = Grpc::new(channel);
    let mut observations = Vec::new();
    for repetition in 1..=pass.repetitions {
        for (job_index, job) in pass.jobs.iter().enumerate() {
            let started_ns = nanos(pass.phase_start.elapsed());
            let outcome = stream_range(&mut client, job.start, job.end, pass.timeout).await;
            let mut observation = Observation {
                client: client_index,
                repetition,
                job: job_index,
                start: job.start,
                end: job.end,
                frames: 0,
                bytes: 0,
                framed_sha256: String::new(),
                started_ns,
                first_frame_ns: 0,
                completion_ns: 0,
                outcome: "verified",
                detail: None,
            };
            match outcome {
                Ok(streamed) => {
                    observation.frames = streamed.frames;
                    observation.bytes = streamed.bytes;
                    observation.first_frame_ns = nanos(streamed.first_frame);
                    observation.completion_ns = nanos(streamed.completion);
                    if let Some(detail) = difference(job, &streamed) {
                        observation.outcome = "mismatch";
                        observation.detail = Some(detail);
                    }
                    observation.framed_sha256 = streamed.framed_sha256;
                }
                Err(detail) => {
                    observation.outcome = "error";
                    observation.detail = Some(detail);
                }
            }
            observations.push(observation);
        }
    }
    observations
}

struct Phase {
    wall: Duration,
    observations: Vec<Observation>,
}

/// Runs every client together, each over its own connection, and waits for all.
async fn run_phase(
    channels: &[Channel],
    jobs: &Arc<[OracleJob]>,
    repetitions: u32,
    timeout: Duration,
) -> Result<Phase, BenchError> {
    let phase_start = Instant::now();
    let pass = Pass {
        jobs: Arc::clone(jobs),
        repetitions,
        phase_start,
        timeout,
    };
    let handles = channels
        .iter()
        .zip(0_u32..)
        .map(|(channel, client_index)| {
            tokio::spawn(run_client(client_index, channel.clone(), pass.clone()))
        })
        .collect::<Vec<JoinHandle<_>>>();
    let mut observations = Vec::new();
    for handle in handles {
        observations.extend(
            handle
                .await
                .map_err(|error| BenchError::raw_range_probe(format!("client task: {error}")))?,
        );
    }
    Ok(Phase {
        wall: phase_start.elapsed(),
        observations,
    })
}

async fn open_channel(endpoint: &str) -> Result<Channel, BenchError> {
    let endpoint = transport::wallet_endpoint(endpoint, CONNECT_TIMEOUT, &TlsOptions::default())
        .map_err(BenchError::wallet_load_endpoint)?;
    transport::connect(&endpoint, CONNECT_TIMEOUT)
        .await
        .map_err(BenchError::wallet_load_endpoint)
}

/// Reads the oracle for `ranges` from a reference server.
async fn fetch_oracle(
    endpoint: &str,
    ranges: &[(u64, u64)],
    timeout: Duration,
) -> Result<OracleFile, BenchError> {
    let mut client = Grpc::new(open_channel(endpoint).await?);
    let mut jobs = Vec::with_capacity(ranges.len());
    for &(start, end) in ranges {
        let streamed = stream_range(&mut client, start, end, timeout)
            .await
            .map_err(|reason| {
                BenchError::raw_range_probe(format!("oracle range {start}:{end}: {reason}"))
            })?;
        jobs.push(OracleJob {
            start,
            end,
            blocks: streamed.frames,
            bytes: streamed.bytes,
            framed_sha256: streamed.framed_sha256,
        });
    }
    let oracle = OracleFile {
        schema: ORACLE_SCHEMA.to_owned(),
        jobs,
    };
    oracle.validate()?;
    Ok(oracle)
}

fn write_new_file(path: &std::path::Path, contents: &[u8]) -> Result<(), BenchError> {
    use std::io::Write as _;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| BenchError::io(path, source))?;
    file.write_all(contents)
        .and_then(|()| file.sync_all())
        .map_err(|source| BenchError::io(path, source))
}

async fn endpoint_addresses(
    endpoint: &str,
    timeout: Duration,
) -> Result<Vec<std::net::SocketAddr>, BenchError> {
    let uri = endpoint
        .parse::<tonic::codegen::http::Uri>()
        .map_err(|error| BenchError::invalid_argument(format!("invalid endpoint URI: {error}")))?;
    let host = uri
        .host()
        .ok_or_else(|| BenchError::invalid_argument("endpoint must include a host"))?
        .trim_start_matches('[')
        .trim_end_matches(']');
    let port = uri.port_u16().unwrap_or_else(|| {
        if uri.scheme_str() == Some("https") {
            443
        } else {
            80
        }
    });
    tokio::time::timeout(timeout, tokio::net::lookup_host((host, port)))
        .await
        .map_err(|_| BenchError::raw_range_probe("endpoint address lookup timed out"))?
        .map(Iterator::collect)
        .map_err(|error| BenchError::raw_range_probe(format!("endpoint address lookup: {error}")))
}

/// The oracle to check against and where it came from.
async fn load_oracle(
    args: &RawRangeProbeArgs,
    timeout: Duration,
) -> Result<(OracleFile, String), BenchError> {
    if let Some(path) = &args.oracle {
        let oracle: OracleFile = serde_json::from_slice(
            &std::fs::read(path).map_err(|source| BenchError::io(path, source))?,
        )?;
        oracle.validate()?;
        return Ok((oracle, format!("file:{}", path.display())));
    }
    let reference = args.oracle_endpoint.as_deref().unwrap_or_default();
    let reference_addrs = endpoint_addresses(reference, timeout).await?;
    let measured_addrs = endpoint_addresses(&args.endpoint, timeout).await?;
    if reference_addrs
        .iter()
        .any(|addr| measured_addrs.contains(addr))
    {
        return Err(BenchError::invalid_argument(
            "--oracle-endpoint must differ from --endpoint, or the server grades itself",
        ));
    }
    let mut ranges = args.ranges.clone();
    if args.both_directions {
        ranges.extend(
            args.ranges
                .iter()
                .filter(|(start, end)| start != end)
                .map(|&(start, end)| (end, start)),
        );
    }
    let oracle = fetch_oracle(reference, &ranges, timeout).await?;
    if let Some(path) = &args.write_oracle {
        write_new_file(path, &serde_json::to_vec_pretty(&oracle)?)?;
    }
    Ok((oracle, format!("endpoint:{reference}")))
}

/// Chain identity and tip, as a wallet would read them.
#[derive(Serialize)]
struct ChainReport {
    name: String,
    vendor: String,
    version: String,
    lightwallet_protocol_version: String,
    sapling_activation_height: u64,
    consensus_branch_id: String,
    tip_before: Tip,
    tip_after: Tip,
    tip_unchanged: bool,
}

/// A block's height and its hash in display (RPC byte) order.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct Tip {
    height: u64,
    hash: String,
}

async fn read_tip(client: &mut CompactTxStreamerClient<Channel>) -> Result<Tip, BenchError> {
    let block = tokio::time::timeout(UNARY_TIMEOUT, client.get_latest_block(ChainSpec {}))
        .await
        .map_err(|_| BenchError::raw_range_probe("GetLatestBlock timed out"))?
        .map_err(|status| BenchError::raw_range_probe(status_text(&status)))?
        .into_inner();
    let hash = decode_internal_block_hash(&block.hash)
        .map_or_else(|_| hex::encode(&block.hash), encode_rpc_block_hash_hex);
    Ok(Tip {
        height: block.height,
        hash,
    })
}

/// One process's cumulative CPU and resident memory, read from `/proc`.
#[derive(Clone, Debug)]
struct ServerSample {
    cpu_ticks: u64,
    start_ticks: u64,
    executable: PathBuf,
    rss_kib: u64,
    hwm_kib: u64,
}

fn sample_server(pid: u32) -> Result<ServerSample, String> {
    let read = |file: &str| {
        std::fs::read_to_string(format!("/proc/{pid}/{file}"))
            .map_err(|error| format!("/proc/{pid}/{file}: {error}"))
    };
    let stat = read("stat")?;
    let status = read("status")?;
    let (cpu_ticks, start_ticks) = process_ticks(&stat).ok_or("unreadable /proc stat")?;
    Ok(ServerSample {
        cpu_ticks,
        start_ticks,
        executable: std::fs::read_link(format!("/proc/{pid}/exe"))
            .map_err(|error| error.to_string())?,
        rss_kib: parse_status_kib(&status, "VmRSS:").ok_or("no VmRSS in /proc status")?,
        hwm_kib: parse_status_kib(&status, "VmHWM:").ok_or("no VmHWM in /proc status")?,
    })
}

/// User plus system CPU ticks. The command name, field 2, may hold spaces and
/// parentheses, so the fields are counted after its closing parenthesis.
fn process_ticks(stat: &str) -> Option<(u64, u64)> {
    let fields = stat
        .rsplit_once(')')?
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    let utime = fields.get(11)?.parse::<u64>().ok()?;
    let stime = fields.get(12)?.parse::<u64>().ok()?;
    Some((utime.checked_add(stime)?, fields.get(19)?.parse().ok()?))
}

fn parse_status_kib(status: &str, field: &str) -> Option<u64> {
    status
        .lines()
        .find_map(|line| line.strip_prefix(field))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

#[derive(Serialize)]
struct ServerReport {
    pid: u32,
    start_time_ticks: u64,
    executable: PathBuf,
    /// CPU time the process used over the timed interval, in whole clock
    /// ticks (`clock_ticks_per_second`), so a short run is coarse.
    cpu_ms: f64,
    /// `cpu_ms` per MiB of verified payload.
    cpu_ms_per_mib: Option<f64>,
    clock_ticks_per_second: u64,
    rss_kib_before: u64,
    rss_kib_after: u64,
    /// The process's lifetime high-water mark. It is read, never reset, so it
    /// covers everything the process did, not only the timed interval.
    hwm_kib_before: u64,
    hwm_kib_after: u64,
}

fn server_report(pid: u32, before: ServerSample, after: &ServerSample, mib: f64) -> ServerReport {
    let ticks_per_second = rustix::param::clock_ticks_per_second().max(1);
    let ticks = after.cpu_ticks.saturating_sub(before.cpu_ticks);
    let cpu_ms = as_f64(ticks) * 1000.0 / as_f64(ticks_per_second);
    ServerReport {
        pid,
        start_time_ticks: before.start_ticks,
        executable: before.executable,
        cpu_ms,
        cpu_ms_per_mib: (mib > 0.0).then(|| cpu_ms / mib),
        clock_ticks_per_second: ticks_per_second,
        rss_kib_before: before.rss_kib,
        rss_kib_after: after.rss_kib,
        hwm_kib_before: before.hwm_kib,
        hwm_kib_after: after.hwm_kib,
    }
}

#[allow(
    clippy::cast_precision_loss,
    reason = "counts and nanoseconds in a report stay far below 2^52, where f64 is exact"
)]
const fn as_f64(count: u64) -> f64 {
    count as f64
}

/// Nearest-rank summary of latencies, in milliseconds.
#[derive(Clone, Copy, Debug, Default, Serialize)]
struct Latency {
    count: u64,
    min: f64,
    p50: f64,
    p95: f64,
    max: f64,
}

impl Latency {
    fn of(mut samples_ns: Vec<u64>) -> Self {
        samples_ns.sort_unstable();
        let milliseconds = |rank: Option<&u64>| as_f64(rank.copied().unwrap_or_default()) / 1e6;
        let nearest_rank = |percentile: usize| {
            let rank = samples_ns.len().saturating_mul(percentile).div_ceil(100);
            milliseconds(samples_ns.get(rank.saturating_sub(1)))
        };
        Self {
            count: u64::try_from(samples_ns.len()).unwrap_or(u64::MAX),
            min: milliseconds(samples_ns.first()),
            p50: nearest_rank(50),
            p95: nearest_rank(95),
            max: milliseconds(samples_ns.last()),
        }
    }
}

#[derive(Serialize)]
struct Totals {
    /// Timed requests that matched their oracle.
    requests: u64,
    failed_requests: u64,
    blocks: u64,
    bytes: u64,
    wall_ns: u64,
    blocks_per_second: f64,
    mib_per_second: f64,
}

#[derive(Serialize)]
struct LatencyReport {
    first_frame: Latency,
    completion: Latency,
}

#[derive(Serialize)]
struct JobReport {
    start: u64,
    end: u64,
    direction: &'static str,
    requests: u64,
    blocks: u64,
    bytes: u64,
    first_frame_ms: Latency,
    completion_ms: Latency,
}

#[derive(Serialize)]
struct OracleReport {
    /// `file:PATH` or `endpoint:URL`.
    source: String,
    /// SHA-256 of the ranges and their expected results.
    sha256: String,
    jobs: Vec<OracleJob>,
}

#[derive(Serialize)]
struct ConfigReport {
    clients: u32,
    repetitions: u32,
    warmup_repetitions: u32,
    request_timeout_secs: u64,
}

#[derive(Serialize)]
struct ClientReport {
    /// CPU time of this probe process over the timed interval.
    cpu_ms: f64,
}

/// The report `raw-range-probe` writes.
#[derive(Serialize)]
pub(crate) struct ProbeReport {
    schema: &'static str,
    /// Every warmup and timed request matched its oracle, and at least one
    /// timed request ran.
    verified: bool,
    endpoint: String,
    oracle: OracleReport,
    config: ConfigReport,
    chain: ChainReport,
    totals: Totals,
    latency_ms: LatencyReport,
    by_job: Vec<JobReport>,
    client: ClientReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    server: Option<ServerReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    counters: Option<CountersReport>,
    /// The first requests that failed, by phase and position.
    problems: Vec<String>,
    warmup: Vec<Observation>,
    observations: Vec<Observation>,
}

fn problems(phase: &str, observations: &[Observation]) -> Vec<String> {
    observations
        .iter()
        .filter(|observation| observation.outcome != "verified")
        .map(|observation| {
            format!(
                "{phase} client {} pass {} range {}:{}: {}",
                observation.client,
                observation.repetition,
                observation.start,
                observation.end,
                observation.detail.as_deref().unwrap_or(observation.outcome)
            )
        })
        .collect()
}

fn job_reports(jobs: &[OracleJob], observations: &[Observation]) -> Vec<JobReport> {
    jobs.iter()
        .enumerate()
        .map(|(index, job)| {
            let ok = || {
                observations.iter().filter(move |observation| {
                    observation.job == index && observation.outcome == "verified"
                })
            };
            JobReport {
                start: job.start,
                end: job.end,
                direction: if job.start > job.end {
                    "descending"
                } else {
                    "ascending"
                },
                requests: u64::try_from(ok().count()).unwrap_or(u64::MAX),
                blocks: ok().map(|observation| observation.frames).sum(),
                bytes: ok().map(|observation| observation.bytes).sum(),
                first_frame_ms: Latency::of(ok().map(|o| o.first_frame_ns).collect()),
                completion_ms: Latency::of(ok().map(|o| o.completion_ns).collect()),
            }
        })
        .collect()
}

/// CPU time of every thread of this process.
fn process_cpu_ms() -> f64 {
    let cpu = rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime);
    as_f64(u64::try_from(cpu.tv_sec).unwrap_or(0)).mul_add(
        1000.0,
        as_f64(u64::try_from(cpu.tv_nsec).unwrap_or(0)) / 1e6,
    )
}

/// Everything measured around the timed interval.
struct Timed {
    phase: Phase,
    client_cpu_ms: f64,
    tip_before: Tip,
    tip_after: Tip,
    server: Option<(ServerSample, ServerSample)>,
    counters: Option<CountersReport>,
}

fn sample_pid(pid: Option<u32>) -> Result<Option<ServerSample>, BenchError> {
    pid.map(|pid| sample_server(pid).map_err(BenchError::raw_range_probe))
        .transpose()
}

/// Runs the timed passes.
///
/// Reading the tip, waiting for a fresh counter publication, and sampling the
/// server process all happen outside the interval the CPU and wall figures
/// cover.
async fn run_timed(
    args: &RawRangeProbeArgs,
    channels: &[Channel],
    jobs: &Arc<[OracleJob]>,
    chain_client: &mut CompactTxStreamerClient<Channel>,
) -> Result<Timed, BenchError> {
    let timeout = Duration::from_secs(args.request_timeout_secs);
    let counters = counter_source(args);
    let tip_before = read_tip(chain_client).await?;
    let counters_before = match &counters {
        Some(source) => Some(source.fresh_snapshot().await),
        None => None,
    };
    let server_before = sample_pid(args.server_pid)?;
    if let (Some(sample), Some(expected)) = (&server_before, &args.server_executable) {
        let path = expected
            .canonicalize()
            .map_err(|error| BenchError::io(expected, error))?;
        if sample.executable != path {
            return Err(BenchError::invalid_argument(
                "server PID is running a different executable",
            ));
        }
    }
    let client_cpu_before = process_cpu_ms();
    let phase = run_phase(channels, jobs, args.repetitions, timeout).await?;
    let client_cpu_ms = process_cpu_ms() - client_cpu_before;
    let server_after = sample_pid(args.server_pid)?;
    if let (Some(before), Some(after)) = (&server_before, &server_after)
        && (before.start_ticks != after.start_ticks
            || before.executable != after.executable
            || after.cpu_ticks < before.cpu_ticks)
    {
        return Err(BenchError::raw_range_probe(
            "server process changed during the timed period",
        ));
    }
    let tip_after = read_tip(chain_client).await?;
    let counters = match (&counters, counters_before) {
        (Some(source), Some(before)) => Some(counters::report(
            source,
            before,
            source.fresh_snapshot().await,
        )),
        _ => None,
    };
    Ok(Timed {
        phase,
        client_cpu_ms,
        tip_before,
        tip_after,
        server: server_before.zip(server_after),
        counters,
    })
}

fn counter_source(args: &RawRangeProbeArgs) -> Option<CounterSource> {
    let mut selectors = args.server_counters.clone();
    if args.rocksdb_multiget_counters {
        selectors.extend(counters::rocksdb_multiget());
    }
    Some(CounterSource {
        url: args.server_metrics_url.clone()?,
        witness: args.server_freshness_counter.clone()?,
        counters: selectors,
        wait: Duration::from_secs(args.server_counter_wait_secs),
    })
}

/// Runs the probe.
pub(crate) async fn run_raw_range_probe(
    args: RawRangeProbeArgs,
) -> Result<ProbeOutput, BenchError> {
    let timeout = Duration::from_secs(args.request_timeout_secs);
    let (oracle, oracle_source) = load_oracle(&args, timeout).await?;
    let jobs: Arc<[OracleJob]> = oracle.jobs.clone().into();

    let mut channels = Vec::new();
    for _ in 0..args.clients {
        channels.push(open_channel(&args.endpoint).await?);
    }
    let first_channel = channels
        .first()
        .cloned()
        .ok_or_else(|| BenchError::invalid_argument("--clients must be at least 1"))?;
    let mut chain_client = CompactTxStreamerClient::new(first_channel);
    let lightd = tokio::time::timeout(UNARY_TIMEOUT, chain_client.get_lightd_info(Empty {}))
        .await
        .map_err(|_| BenchError::raw_range_probe("GetLightdInfo timed out"))?
        .map_err(|status| BenchError::raw_range_probe(status_text(&status)))?
        .into_inner();

    let warmup = if args.warmup_repetitions > 0 {
        run_phase(&channels, &jobs, args.warmup_repetitions, timeout)
            .await?
            .observations
    } else {
        Vec::new()
    };
    let timed = run_timed(&args, &channels, &jobs, &mut chain_client).await?;
    let oracle = OracleReport {
        source: oracle_source,
        sha256: oracle.digest()?,
        jobs: oracle.jobs,
    };
    let report = build_report(&args, oracle, lightd, warmup, timed);
    Ok(ProbeOutput {
        report,
        report_path: args.report,
    })
}

fn totals(verified: &[&Observation], timed_requests: usize, wall: Duration) -> Totals {
    let blocks = verified
        .iter()
        .map(|observation| observation.frames)
        .sum::<u64>();
    let bytes = verified
        .iter()
        .map(|observation| observation.bytes)
        .sum::<u64>();
    let requests = u64::try_from(verified.len()).unwrap_or(u64::MAX);
    let wall_seconds = wall.as_secs_f64();
    Totals {
        requests,
        failed_requests: u64::try_from(timed_requests)
            .unwrap_or(u64::MAX)
            .saturating_sub(requests),
        blocks,
        bytes,
        wall_ns: nanos(wall),
        blocks_per_second: as_f64(blocks) / wall_seconds,
        mib_per_second: as_f64(bytes) / f64::from(1_u32 << 20) / wall_seconds,
    }
}

fn build_report(
    args: &RawRangeProbeArgs,
    oracle: OracleReport,
    lightd: LightdInfo,
    warmup: Vec<Observation>,
    timed: Timed,
) -> ProbeReport {
    let Timed {
        phase,
        client_cpu_ms,
        tip_before,
        tip_after,
        server,
        counters,
    } = timed;
    let verified_requests = phase
        .observations
        .iter()
        .filter(|observation| observation.outcome == "verified")
        .collect::<Vec<_>>();
    let totals = totals(&verified_requests, phase.observations.len(), phase.wall);
    let (requests, payload_mib) = (
        totals.requests,
        as_f64(totals.bytes) / f64::from(1_u32 << 20),
    );

    let mut problems = problems("warmup", &warmup);
    problems.extend(self::problems("timed", &phase.observations));
    let verified = problems.is_empty() && requests > 0;
    problems.truncate(MAX_REPORTED_PROBLEMS);
    let latency = |nanos: fn(&Observation) -> u64| {
        Latency::of(verified_requests.iter().map(|o| nanos(o)).collect())
    };
    ProbeReport {
        schema: REPORT_SCHEMA,
        verified,
        endpoint: args.endpoint.clone(),
        by_job: job_reports(&oracle.jobs, &phase.observations),
        oracle,
        config: ConfigReport {
            clients: args.clients,
            repetitions: args.repetitions,
            warmup_repetitions: args.warmup_repetitions,
            request_timeout_secs: args.request_timeout_secs,
        },
        chain: ChainReport {
            name: lightd.chain_name,
            vendor: lightd.vendor,
            version: lightd.version,
            lightwallet_protocol_version: lightd.lightwallet_protocol_version,
            sapling_activation_height: lightd.sapling_activation_height,
            consensus_branch_id: lightd.consensus_branch_id,
            tip_unchanged: tip_before == tip_after,
            tip_before,
            tip_after,
        },
        totals,
        latency_ms: LatencyReport {
            first_frame: latency(|o| o.first_frame_ns),
            completion: latency(|o| o.completion_ns),
        },
        client: ClientReport {
            cpu_ms: client_cpu_ms,
        },
        server: match (args.server_pid, server) {
            (Some(pid), Some((before, after))) => {
                Some(server_report(pid, before, &after, payload_mib))
            }
            _ => None,
        },
        counters,
        problems,
        warmup,
        observations: phase.observations,
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::{
        FramedDigest, OracleFile, OracleJob, RawRangeProbeArgs, difference, parse_range,
        parse_status_kib, process_ticks, run_raw_range_probe, sample_server,
    };
    use crate::wallet_load::mock_streamer::{MockStreamer, RangeEnding, serve, tip_hash};

    fn args(endpoint: &str, oracle_endpoint: &str) -> RawRangeProbeArgs {
        RawRangeProbeArgs {
            endpoint: endpoint.to_owned(),
            oracle: None,
            oracle_endpoint: Some(oracle_endpoint.to_owned()),
            ranges: vec![(10, 20)],
            both_directions: true,
            write_oracle: None,
            clients: 3,
            repetitions: 2,
            warmup_repetitions: 1,
            request_timeout_secs: 10,
            server_pid: None,
            server_executable: None,
            server_metrics_url: None,
            server_counters: Vec::new(),
            rocksdb_multiget_counters: false,
            server_freshness_counter: None,
            server_counter_wait_secs: 6,
            report: None,
        }
    }

    fn mock(shielded: &[u64], ironwood: &[u64], ending: RangeEnding) -> MockStreamer {
        let mut streamer = MockStreamer::new(ending);
        streamer.shielded_heights = shielded.iter().copied().collect();
        streamer.ironwood_heights = ironwood.iter().copied().collect();
        streamer
    }

    #[test]
    fn framed_digest_is_length_prefixed_payload_sha256() {
        let mut digest = FramedDigest::new();
        digest.push(b"ab");
        digest.push(b"");
        assert_eq!((digest.frames, digest.bytes), (2, 2));
        // printf '\0\0\0\2ab\0\0\0\0' | sha256sum
        assert_eq!(
            hex::encode(sha2::Digest::finalize(digest.hasher)),
            "421fb38456f1afe5c04ed5c0c11c88c415c03ba8cc8eab2528a1fd847fa83cd7"
        );
    }

    #[test]
    fn difference_names_every_mismatched_field() {
        let job = OracleJob {
            start: 1,
            end: 2,
            blocks: 2,
            bytes: 10,
            framed_sha256: "a".repeat(64),
        };
        let streamed = |frames, bytes, digest: &str| super::Streamed {
            frames,
            bytes,
            framed_sha256: digest.to_owned(),
            first_frame: std::time::Duration::ZERO,
            completion: std::time::Duration::ZERO,
        };
        assert_eq!(difference(&job, &streamed(2, 10, &"a".repeat(64))), None);
        let same_size = difference(&job, &streamed(2, 10, &"b".repeat(64)));
        assert!(same_size.is_some_and(|text| text.starts_with("framed_sha256 ")));
        let all = difference(&job, &streamed(3, 11, &"b".repeat(64)));
        assert!(all.is_some_and(|text| text.contains("frames") && text.contains("bytes")));
    }

    #[test]
    fn oracle_file_rejects_inconsistent_ranges() {
        let job = |blocks| OracleJob {
            start: 5,
            end: 1,
            blocks,
            bytes: 1,
            framed_sha256: "0".repeat(64),
        };
        let file = |jobs| OracleFile {
            schema: super::ORACLE_SCHEMA.to_owned(),
            jobs,
        };
        assert!(file(vec![job(5)]).validate().is_ok());
        assert!(file(vec![job(4)]).validate().is_err());
        assert!(file(Vec::new()).validate().is_err());
    }

    #[test]
    fn range_syntax_is_start_colon_end() {
        assert_eq!(parse_range("7:3"), Ok((7, 3)));
        assert!(parse_range("7").is_err());
        assert!(parse_range("a:3").is_err());
    }

    #[test]
    fn proc_stat_parsing_survives_a_command_name_with_spaces() {
        let stat = "42 (zinder (compat) x) S 1 42 42 0 -1 4194560 100 0 0 0 7 5 0 0 20 0 9 0 100 1000 200 18446744073709551615";
        assert_eq!(process_ticks(stat), Some((12, 100)));
        assert_eq!(
            parse_status_kib("Name:\tx\nVmHWM:\t  2048 kB\n", "VmHWM:"),
            Some(2048)
        );
        assert_eq!(parse_status_kib("Name:\tx\n", "VmHWM:"), None);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn samples_this_process() -> Result<(), Box<dyn Error>> {
        let sample = sample_server(std::process::id())?;
        assert!(sample.hwm_kib >= sample.rss_kib && sample.rss_kib > 0);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn refuses_to_charge_cpu_to_an_unrelated_executable() -> Result<(), Box<dyn Error>> {
        let reference = serve(mock(&[12], &[], RangeEnding::Clean)).await?;
        let server = serve(mock(&[12], &[], RangeEnding::Clean)).await?;
        let mut options = args(&server.url, &reference.url);
        options.server_pid = Some(std::process::id());
        options.server_executable = Some(std::env::temp_dir());
        let error = run_raw_range_probe(options)
            .await
            .err()
            .ok_or("accepted the wrong executable")?;
        assert!(error.to_string().contains("different executable"));
        Ok(())
    }

    #[tokio::test]
    async fn verifies_both_directions_against_a_different_server() -> Result<(), Box<dyn Error>> {
        let reference = serve(mock(&[12, 15], &[], RangeEnding::Clean)).await?;
        let mut streamer = mock(&[12, 15], &[], RangeEnding::Clean);
        streamer.tip_height = 250;
        let server = serve(streamer).await?;
        let output = run_raw_range_probe(args(&server.url, &reference.url)).await?;
        let report = &output.report;
        assert!(report.verified, "{:?}", report.problems);
        output.require_verified()?;
        // 3 clients x 2 passes x 2 ranges, each 11 blocks.
        assert_eq!(report.totals.requests, 12);
        assert_eq!(report.totals.blocks, 12 * 11);
        assert_eq!(report.observations.len(), 12);
        assert_eq!(report.warmup.len(), 6);
        assert_eq!(report.oracle.jobs.len(), 2);
        assert_eq!(report.oracle.jobs[0].bytes, report.oracle.jobs[1].bytes);
        // Direction changes the bytes on the wire even when the sizes agree.
        assert_ne!(
            report.oracle.jobs[0].framed_sha256,
            report.oracle.jobs[1].framed_sha256
        );
        assert_eq!(report.by_job[1].direction, "descending");
        assert_eq!(report.latency_ms.completion.count, 12);
        assert!(report.latency_ms.first_frame.p50 <= report.latency_ms.completion.p50);
        let mut expected_hash = tip_hash(250);
        expected_hash.reverse();
        assert_eq!(report.chain.tip_before.hash, hex::encode(expected_hash));
        assert_eq!(report.chain.tip_before.height, 250);
        assert!(report.chain.tip_unchanged);
        Ok(())
    }

    #[tokio::test]
    async fn report_exposes_the_documented_campaign_fields() -> Result<(), Box<dyn Error>> {
        let reference = serve(mock(&[12], &[], RangeEnding::Clean)).await?;
        let server = serve(mock(&[12], &[], RangeEnding::Clean)).await?;
        let mut options = args(&server.url, &reference.url);
        options.server_pid = Some(std::process::id());
        options.server_executable = Some(std::env::current_exe()?);
        let output = run_raw_range_probe(options).await?;
        let report = serde_json::to_value(&output.report)?;
        for pointer in [
            "/schema",
            "/verified",
            "/oracle/sha256",
            "/oracle/source",
            "/oracle/jobs/1/framed_sha256",
            "/chain/name",
            "/chain/tip_before/hash",
            "/chain/tip_after/height",
            "/chain/tip_unchanged",
            "/totals/requests",
            "/totals/blocks",
            "/totals/bytes",
            "/totals/blocks_per_second",
            "/latency_ms/first_frame/p95",
            "/latency_ms/completion/p50",
            "/by_job/0/completion_ms/p95",
            "/client/cpu_ms",
            "/server/cpu_ms",
            "/server/hwm_kib_after",
            "/warmup/0/framed_sha256",
            "/observations/11/first_frame_ns",
        ] {
            assert!(report.pointer(pointer).is_some(), "missing {pointer}");
        }
        assert_eq!(report.pointer("/verified"), Some(&serde_json::json!(true)));
        // Counters are absent, not zero, when none were requested.
        assert!(report.pointer("/counters").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn rejects_a_same_size_payload_change() -> Result<(), Box<dyn Error>> {
        let reference = serve(mock(&[12], &[], RangeEnding::Clean)).await?;
        // A Sapling output and an Ironwood action at the same height encode to
        // the same number of bytes, so only the digest can tell them apart.
        let server = serve(mock(&[], &[12], RangeEnding::Clean)).await?;
        let mut options = args(&server.url, &reference.url);
        options.warmup_repetitions = 0;
        let output = run_raw_range_probe(options).await?;
        let report = &output.report;
        assert!(!report.verified);
        assert_eq!(report.totals.failed_requests, 12);
        assert!(report.observations.iter().all(|observation| {
            observation.outcome == "mismatch"
                && observation.bytes == report.oracle.jobs[observation.job].bytes
                && observation.frames == report.oracle.jobs[observation.job].blocks
        }));
        assert!(output.require_verified().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn rejects_an_extra_frame_and_a_failed_stream() -> Result<(), Box<dyn Error>> {
        let reference = serve(mock(&[], &[], RangeEnding::Clean)).await?;
        let extra = serve(mock(&[], &[], RangeEnding::ExtraBlock)).await?;
        let output = run_raw_range_probe(args(&extra.url, &reference.url)).await?;
        assert!(!output.report.verified);
        assert!(
            output.report.problems[0].contains("frames 12 != 11"),
            "{:?}",
            output.report.problems
        );
        let failing = serve(mock(&[], &[], RangeEnding::Status(tonic::Code::Internal))).await?;
        let output = run_raw_range_probe(args(&failing.url, &reference.url)).await?;
        assert!(!output.report.verified);
        assert!(
            output
                .report
                .observations
                .iter()
                .all(|o| o.outcome == "error")
        );
        Ok(())
    }

    #[tokio::test]
    async fn reports_a_tip_that_moved_during_the_run() -> Result<(), Box<dyn Error>> {
        let reference = serve(mock(&[], &[], RangeEnding::Clean)).await?;
        let mut moving = mock(&[], &[], RangeEnding::Clean);
        moving.tip_advancing_reads = 2;
        let server = serve(moving).await?;
        let output = run_raw_range_probe(args(&server.url, &reference.url)).await?;
        assert!(output.report.verified);
        assert!(!output.report.chain.tip_unchanged);
        assert_eq!(
            output.report.chain.tip_after.height,
            output.report.chain.tip_before.height + 1
        );
        Ok(())
    }

    #[tokio::test]
    async fn refuses_to_grade_a_server_against_itself() -> Result<(), Box<dyn Error>> {
        let server = serve(mock(&[], &[], RangeEnding::Clean)).await?;
        assert!(
            run_raw_range_probe(args(
                &server.url,
                &server.url.replace("127.0.0.1", "localhost")
            ))
            .await
            .is_err()
        );
        Ok(())
    }
}
