//! Binary gRPC-Web framing assertions for HTTP integration tests.

use eyre::{ensure, eyre};

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
    ensure!(bytes.len() >= 5, "grpc-web response truncated");
    ensure!(bytes[0] == 0, "expected gRPC data frame header 0");
    let frame_len = usize::try_from(u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]))?;
    let payload_end = 5 + frame_len;
    ensure!(
        bytes.len() >= payload_end,
        "grpc-web frame extends past body"
    );
    let trailer_bytes = &bytes[payload_end..];
    ensure!(
        trailer_bytes.len() >= 5,
        "grpc-web response missing trailers frame"
    );
    ensure!(
        trailer_bytes[0] == 0x80,
        "expected uncompressed grpc-web trailers frame header 0x80, got {:#x}",
        trailer_bytes[0]
    );
    let trailer_len = usize::try_from(u32::from_be_bytes([
        trailer_bytes[1],
        trailer_bytes[2],
        trailer_bytes[3],
        trailer_bytes[4],
    ]))?;
    let trailer_end = 5 + trailer_len;
    ensure!(
        trailer_bytes.len() == trailer_end,
        "expected single complete final grpc-web trailers frame (len={}, got {})",
        trailer_end,
        trailer_bytes.len()
    );
    let trailer_text = std::str::from_utf8(&trailer_bytes[5..trailer_end])?;
    let mut grpc_status = None;
    for raw_line in trailer_text.split("\r\n") {
        if raw_line.is_empty() {
            continue;
        }
        let (header_name, header_val) = raw_line
            .split_once(':')
            .ok_or_else(|| eyre!("malformed grpc-web trailer line: {raw_line:?}"))?;
        if header_name.trim().eq_ignore_ascii_case("grpc-status") {
            grpc_status = Some(header_val.trim().to_owned());
        }
    }
    ensure!(
        grpc_status.as_deref() == Some("0"),
        "expected trailing grpc-status: 0, got {grpc_status:?} in {trailer_text:?}"
    );
    Ok(M::decode(&bytes[5..payload_end])?)
}
