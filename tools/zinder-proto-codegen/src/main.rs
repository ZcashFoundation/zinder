//! Repository-only generator for checked-in `zinder-proto` Rust and descriptors.

use std::env;
use std::error::Error;
use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    let mut arguments = env::args_os().skip(1);
    let repository_root = arguments
        .next()
        .map(PathBuf::from)
        .ok_or("missing repository-root argument")?;
    let output_dir = arguments
        .next()
        .map(PathBuf::from)
        .ok_or("missing output-directory argument")?;
    if arguments.next().is_some() {
        return Err("unexpected extra arguments".into());
    }

    generate(&repository_root, &output_dir)
}

fn generate(repository_root: &Path, output_dir: &Path) -> Result<(), Box<dyn Error>> {
    std::fs::create_dir_all(output_dir)?;
    let proto_root = repository_root.join("crates/zinder-proto/proto");
    let compat_root = proto_root.join("compat/lightwalletd");
    let zebra_root = proto_root.join("external/zebra");
    let native_files = [
        proto_root.join("zinder/v1/wallet/wallet.proto"),
        proto_root.join("zinder/v1/ingest/ingest.proto"),
        proto_root.join("zinder/v1/explorer/explorer.proto"),
        proto_root.join("zinder/v1/ops/readiness.proto"),
        proto_root.join("zinder/v1/ops/error.proto"),
        proto_root.join("zinder/v1/ops/server_info.proto"),
    ];
    let compat_files = [
        compat_root.join("compact_formats.proto"),
        compat_root.join("service.proto"),
    ];
    let zebra_files = [zebra_root.join("indexer.proto")];

    tonic_prost_build::configure()
        .out_dir(output_dir)
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(output_dir.join("lightwalletd_compat_descriptor.bin"))
        .compile_protos(&compat_files, std::slice::from_ref(&compat_root))?;
    tonic_prost_build::configure()
        .out_dir(output_dir)
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(output_dir.join("zinder_v1_descriptor.bin"))
        .compile_protos(&native_files, std::slice::from_ref(&proto_root))?;
    tonic_prost_build::configure()
        .out_dir(output_dir)
        .build_server(true)
        .build_client(true)
        .compile_protos(&zebra_files, std::slice::from_ref(&zebra_root))?;
    write_trimmed_commit(
        &compat_root.join("COMMIT"),
        &output_dir.join("lightwalletd_protocol_commit.txt"),
    )?;
    write_trimmed_version(
        &compat_root.join("VERSION"),
        &output_dir.join("lightwalletd_protocol_version.txt"),
    )?;
    write_trimmed_commit(
        &zebra_root.join("COMMIT"),
        &output_dir.join("zebra_indexer_protocol_commit.txt"),
    )?;

    Ok(())
}

fn write_trimmed_commit(source: &Path, destination: &Path) -> Result<(), Box<dyn Error>> {
    let commit = std::fs::read_to_string(source)?;
    let commit = commit.trim();
    if commit.len() != 40 || !commit.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(format!("{} is not a 40-character commit id", source.display()).into());
    }
    std::fs::write(destination, commit)?;
    Ok(())
}

fn write_trimmed_version(source: &Path, destination: &Path) -> Result<(), Box<dyn Error>> {
    let version = std::fs::read_to_string(source)?;
    let version = version.trim();
    let is_release_tag = version.strip_prefix('v').is_some_and(|numbers| {
        let parts: Vec<&str> = numbers.split('.').collect();
        parts.len() == 3
            && parts
                .iter()
                .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
    });
    if !is_release_tag {
        return Err(format!(
            "{} is not a vMAJOR.MINOR.PATCH release tag",
            source.display()
        )
        .into());
    }
    std::fs::write(destination, version)?;
    Ok(())
}
