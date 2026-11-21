#!/usr/bin/env python3
"""Build the v0.0.65 Vizor wallet engine driver from its pinned source archive."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile

SOURCE = "d0edb5b13890dd33d9bb93b5c4e50fd9973fa4e6"
ARCHIVE_SHA256 = "0641e4f5ad8a37250dde1f90bd4faf36b981ed4bf07ff9048966d94ea5667d0c"


def sha256(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", required=True, type=Path, help="Vizor source .tar.gz at the pinned commit")
    parser.add_argument("--output", required=True, type=Path, help="fresh private build directory")
    args = parser.parse_args()
    if sha256(args.archive) != ARCHIVE_SHA256:
        parser.error("source archive differs from Vizor v0.0.65 at " + SOURCE)
    os.umask(0o077)
    output = args.output.resolve()
    output.mkdir(parents=True)
    with tarfile.open(args.archive) as archive:
        archive.extractall(output, filter="data")
    root = output / ("vizor-wallet-" + SOURCE) / "rust"
    with (root / "Cargo.toml").open("a") as config:
        config.write("\n[workspace]\n")
    driver = root / "examples/vizor-regtest.rs"
    shutil.copy2(Path(__file__).with_name("vizor-regtest.rs"), driver)
    driver_digest = sha256(driver)
    lock = sha256(root / "Cargo.lock")
    target = output / "target"
    subprocess.run(["cargo", "build", "--locked", "--example", "vizor-regtest", "--jobs", "2",
                    "--manifest-path", str(root / "Cargo.toml"), "--target-dir", str(target)],
                   cwd=root, check=True)
    if sha256(driver) != driver_digest:
        parser.error("copied Vizor driver changed during compilation")
    binary = output / "vizor-regtest"
    shutil.copy2(target / "debug/examples/vizor-regtest", binary)
    provenance = {"schema_version": 2, "vizor_source": SOURCE, "vizor_release": "v0.0.65", "release_archive_sha256": ARCHIVE_SHA256,
                  "driver_sha256": driver_digest, "lock_sha256": lock, "engine_sha256": sha256(binary),
                  "rustc": subprocess.check_output(["rustc", "--version"], text=True, cwd=root).strip()}
    (output / "engine-source.json").write_text(json.dumps(provenance, indent=2) + "\n")


if __name__ == "__main__":
    main()
