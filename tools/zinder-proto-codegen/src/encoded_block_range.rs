use std::{error::Error, fmt::Write, fs, path::Path};

// tonic-prost-build fixes each server response to its decoded protobuf type.
// Reuse its generated gRPC settings for the encoded representations of the
// unary and streaming block methods.
pub(super) fn enable(path: &Path) -> Result<(), Box<dyn Error>> {
    inject(path, &Method::GET_BLOCK_RANGE)?;
    inject(path, &Method::GET_BLOCK_RANGE_NULLIFIERS)?;
    inject_unary(path)
}

/// One server-streaming range method the generated server can serve encoded.
struct Method {
    /// The generated method name, e.g. `GetBlockRange`.
    name: &'static str,
    /// The trait hook the injection adds, e.g. `encoded_block_range`.
    hook: &'static str,
    /// The extension trait the hook returns.
    extension: &'static str,
    /// The helper that serves the method's encoded calls.
    serve: &'static str,
}

impl Method {
    const GET_BLOCK_RANGE: Self = Self {
        name: "GetBlockRange",
        hook: "encoded_block_range",
        extension: "EncodedBlockRange",
        serve: "server_streaming",
    };

    const GET_BLOCK_RANGE_NULLIFIERS: Self = Self {
        name: "GetBlockRangeNullifiers",
        hook: "encoded_block_range_nullifiers",
        extension: "EncodedBlockRangeNullifiers",
        serve: "server_streaming_nullifiers",
    };

    /// The doc line above the method's stream type alias in the trait.
    fn stream_marker(&self) -> String {
        format!(
            "        /// Server streaming response type for the {} method.",
            self.name
        )
    }

    fn hook_code(&self) -> String {
        format!(
            "        /// Provides encoded protobuf responses for {} when available.\n\
             \x20       fn {}(&self) -> Option<&dyn super::{}> {{\n\
             \x20           None\n\
             \x20       }}\n",
            self.name, self.hook, self.extension
        )
    }

    fn method_line(&self) -> String {
        format!(
            "                        let method = {}Svc(inner);\n",
            self.name
        )
    }

    fn call_line() -> &'static str {
        "                        let res = grpc.server_streaming(method, req).await;"
    }
}

fn inject(path: &Path, method: &Method) -> Result<(), Box<dyn Error>> {
    let source = fs::read_to_string(path)?;
    let (before_hook, after_hook) = split_unique(&source, &method.stream_marker())?;
    let (before_method, method_body) = split_unique(after_hook, &method.method_line())?;
    let (setup, after_call) = method_body
        .split_once(Method::call_line())
        .ok_or("missing generated encoded range call")?;
    let encoded_setup = encoded_grpc_setup(setup, "EncodedBlockCodec")?;
    let indent = "                        ";

    let source = format!(
        "{before_hook}{hook}{stream_marker}{before_method}\
         {indent}if let Some(encoded) = inner.{hook_name}() {{\n\
         {encoded_setup}\
         {indent}    return Ok(crate::encoded_block_range::{serve}(\
         &mut grpc, encoded, req, send_compression_encodings, max_encoding_message_size\
         ).await);\n\
         {indent}}}\n\
         {method_line}{setup}{call_line}{after_call}",
        hook = method.hook_code(),
        hook_name = method.hook,
        stream_marker = method.stream_marker(),
        method_line = method.method_line(),
        call_line = Method::call_line(),
        serve = method.serve,
    );
    Ok(fs::write(path, source)?)
}

fn inject_unary(path: &Path) -> Result<(), Box<dyn Error>> {
    let mut source = fs::read_to_string(path)?;
    let marker =
        "        /// Provides encoded protobuf responses for GetBlockRange when available.";
    let (before, after) = split_unique(&source, marker)?;
    let hook = concat!(
        "        /// Provides encoded protobuf responses for unary block calls when available.\n",
        "        fn encoded_unary_blocks(&self) -> Option<&dyn super::EncodedUnaryBlocks> {\n",
        "            None\n",
        "        }\n",
    );
    source = format!("{before}{hook}{marker}{after}");
    for (method, variant) in [("GetBlock", "Full"), ("GetBlockNullifiers", "Nullifiers")] {
        let marker = format!("                        let method = {method}Svc(inner);\n");
        let (before, rest) = split_unique(&source, &marker)?;
        let (setup, _) = rest
            .split_once("                        let res = grpc.unary(method, req).await;")
            .ok_or("missing generated unary block call")?;
        let encoded_setup = encoded_grpc_setup(setup, "EncodedUnaryBlockCodec")?;
        let indent = "                        ";
        source = format!(
            "{before}{indent}if let Some(encoded) = inner.encoded_unary_blocks() {{\n\
             {encoded_setup}\
             {indent}    return Ok(grpc.unary(crate::encoded_block_range::EncodedUnaryBlock(encoded, \
             crate::encoded_block_range::UnaryBlockMethod::{variant}), req).await);\n\
             {indent}}}\n{marker}{rest}",
        );
    }
    Ok(fs::write(path, source)?)
}

fn encoded_grpc_setup(setup: &str, codec: &str) -> Result<String, Box<dyn Error>> {
    let (before, after) = split_unique(setup, "tonic_prost::ProstCodec::default()")?;
    let setup = format!("{before}crate::encoded_block_range::{codec}{after}");
    let mut indented = String::new();
    for line in setup.lines() {
        writeln!(indented, "    {line}")?;
    }
    Ok(indented)
}

fn split_unique<'a>(source: &'a str, marker: &str) -> Result<(&'a str, &'a str), Box<dyn Error>> {
    let (before, after) = source
        .split_once(marker)
        .ok_or_else(|| format!("missing generated encoded range marker: {}", marker.trim()))?;
    if after.contains(marker) {
        return Err(format!(
            "ambiguous generated encoded range marker: {}",
            marker.trim()
        )
        .into());
    }
    Ok((before, after))
}
