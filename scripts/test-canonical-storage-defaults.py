#!/usr/bin/env python3
"""Compare the canonical store paths printed by the debug binaries."""

import os
from pathlib import Path
import subprocess
import tomllib
import unittest


def storage_config(binary, *args):
    executable = Path(os.environ.get("CARGO_TARGET_DIR", "target")) / "debug" / binary
    output = subprocess.check_output(
        [executable, "--print-config", "--network", "zcash-regtest", *args],
        env={},
        text=True,
    )
    return tomllib.loads(output)["storage"]


class CanonicalStorageDefaults(unittest.TestCase):
    def test_ingest_explorer_and_projector_share_the_canonical_store(self):
        ingest = storage_config(
            "zinder-ingest",
            "--node-source", "zebra-json-rpc",
            "--json-rpc-addr", "http://127.0.0.1:18232",
        )
        explorer = storage_config("zinder-explorer")
        projector = storage_config(
            "zinder-projector",
            "--build-owner-hex", "00112233445566778899aabbccddeeff",
            "--lease-duration-seconds", "14400",
        )

        canonical = projector["canonical_path"]
        self.assertEqual(canonical, "/var/lib/zinder/canonical")
        for binary, path in (
            ("zinder-ingest", ingest["path"]), ("zinder-explorer", explorer["path"]),
        ):
            with self.subTest(binary=binary):
                self.assertEqual(path, canonical)


if __name__ == "__main__":
    unittest.main()
