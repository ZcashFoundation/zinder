//! Binary gRPC-Web framing assertions for HTTP integration tests.

use eyre::{ContextCompat, ensure, eyre};

/// Frames one protobuf payload for a binary gRPC-Web request.
pub fn encode_grpc_web_frame(payload: &[u8]) -> eyre::Result<Vec<u8>> {
    let length = u32::try_from(payload.len())?;
    let mut framed = Vec::with_capacity(5 + payload.len());
    framed.push(0);
    framed.extend_from_slice(&length.to_be_bytes());
    framed.extend_from_slice(payload);
    Ok(framed)
}

/// Decodes one binary gRPC-Web response and checks its final success trailer.
pub fn decode_grpc_web_message<M: prost::Message + Default>(bytes: &[u8]) -> eyre::Result<M> {
    let [message] = decode_grpc_web_messages(bytes)?
        .try_into()
        .map_err(|messages: Vec<M>| {
            eyre!("expected one gRPC-Web message, got {}", messages.len())
        })?;
    Ok(message)
}

/// Decodes all binary gRPC-Web messages in order and requires a final success trailer.
pub fn decode_grpc_web_messages<M: prost::Message + Default>(
    mut bytes: &[u8],
) -> eyre::Result<Vec<M>> {
    let mut messages = Vec::new();
    loop {
        let (&[flag, a, b, c, d], remainder) = bytes
            .split_first_chunk::<5>()
            .wrap_err("gRPC-Web response truncated or missing trailers")?;
        let frame_len = usize::try_from(u32::from_be_bytes([a, b, c, d]))?;
        let (payload, remainder) = remainder
            .split_at_checked(frame_len)
            .wrap_err("gRPC-Web frame extends past body")?;
        match flag {
            0 => messages.push(M::decode(payload)?),
            0x80 => {
                ensure!(
                    remainder.is_empty(),
                    "unexpected bytes after gRPC-Web trailers"
                );
                check_success_trailers(payload)?;
                return Ok(messages);
            }
            _ => return Err(eyre!("unsupported gRPC-Web frame flag {flag:#x}")),
        }
        bytes = remainder;
    }
}

fn check_success_trailers(payload: &[u8]) -> eyre::Result<()> {
    let trailers = std::str::from_utf8(payload)?;
    let mut grpc_status = None;
    for line in trailers.split("\r\n").filter(|line| !line.is_empty()) {
        let (name, header) = line
            .split_once(':')
            .ok_or_else(|| eyre!("malformed gRPC-Web trailer line: {line:?}"))?;
        if name.trim().eq_ignore_ascii_case("grpc-status") {
            ensure!(grpc_status.is_none(), "duplicate gRPC-Web status trailers");
            grpc_status = Some(header.trim());
        }
    }
    ensure!(
        grpc_status == Some("0"),
        "expected trailing grpc-status: 0, got {grpc_status:?} in {trailers:?}"
    );
    Ok(())
}
