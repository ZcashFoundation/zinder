use std::{error::Error, fmt::Write, fs, path::Path};

// tonic-prost-build fixes each server response to its decoded protobuf type.
// Reuse its generated gRPC settings for both representations of GetBlockRange.
pub(super) fn enable(path: &Path) -> Result<(), Box<dyn Error>> {
    const RANGE_STREAM: &str =
        "        /// Server streaming response type for the GetBlockRange method.";
    const RANGE_HOOK: &str = r"        /// Provides encoded protobuf responses for GetBlockRange when available.
        fn encoded_block_range(&self) -> Option<&dyn super::EncodedBlockRange> {
            None
        }
";
    const METHOD: &str = "                        let method = GetBlockRangeSvc(inner);\n";
    const CALL: &str =
        "                        let res = grpc.server_streaming(method, req).await;";

    let source = fs::read_to_string(path)?;
    let (before_hook, after_hook) = split_unique(&source, RANGE_STREAM)?;
    let (before_method, method_body) = split_unique(after_hook, METHOD)?;
    let (setup, after_call) = method_body
        .split_once(CALL)
        .ok_or("missing GetBlockRange call")?;
    let (codec_start, codec_end) = split_unique(setup, "tonic_prost::ProstCodec::default()")?;
    let setup_with_codec =
        format!("{codec_start}crate::encoded_block_range::EncodedBlockCodec{codec_end}");
    let mut encoded_setup = String::new();
    for line in setup_with_codec.lines() {
        writeln!(encoded_setup, "    {line}")?;
    }

    let source = format!(
        "{before_hook}{RANGE_HOOK}{RANGE_STREAM}{before_method}\
         {indent}if let Some(encoded) = inner.encoded_block_range() {{\n\
         {encoded_setup}\
         {indent}    return Ok(grpc.server_streaming(encoded, req).await);\n\
         {indent}}}\n\
         {METHOD}{setup}{CALL}{after_call}",
        indent = "                        ",
    );
    Ok(fs::write(path, source)?)
}

fn split_unique<'a>(source: &'a str, marker: &str) -> Result<(&'a str, &'a str), Box<dyn Error>> {
    let (before, after) = source
        .split_once(marker)
        .ok_or_else(|| format!("missing generated GetBlockRange marker: {}", marker.trim()))?;
    if after.contains(marker) {
        return Err(format!(
            "ambiguous generated GetBlockRange marker: {}",
            marker.trim()
        )
        .into());
    }
    Ok((before, after))
}
