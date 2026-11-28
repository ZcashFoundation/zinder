#!/usr/bin/env python3
"""Compare the canonical store paths printed by the unified zinder CLI."""

import os
from pathlib import Path
import subprocess
import tomllib
import unittest


def run_role_storage_config(role, *args):
    executable = Path(os.environ.get("CARGO_TARGET_DIR", "target")) / "debug" / "zinder"
    output = subprocess.check_output(
        [executable, "run", role, "--print-config", "--network", "zcash-regtest", *args],
        env={},
        text=True,
    )
    return tomllib.loads(output)["storage"]


def unified_storage_config(subcommand, *args):
    executable = Path(os.environ.get("CARGO_TARGET_DIR", "target")) / "debug" / "zinder"
    output = subprocess.check_output(
        [executable, subcommand, "--print-config", *args],
        env={
            "ZINDER_NETWORK__NAME": "zcash-regtest",
            "ZINDER_NODE__JSON_RPC_ADDR": "http://127.0.0.1:18232",
        },
        text=True,
    )
    return tomllib.loads(output)["storage"]


class CanonicalStorageDefaults(unittest.TestCase):
    def test_ingest_explorer_and_projector_share_the_canonical_store(self):
        ingest = run_role_storage_config(
            "ingest",
            "--node-source", "zebra-json-rpc",
            "--json-rpc-addr", "http://127.0.0.1:18232",
        )
        explorer = run_role_storage_config("explorer")
        projector = run_role_storage_config(
            "projector",
            "--build-owner-hex", "00112233445566778899aabbccddeeff",
            "--lease-duration-seconds", "14400",
        )

        canonical = projector["path"]
        self.assertEqual(canonical, "/var/lib/zinder/canonical")
        for role, path in (
            ("ingest", ingest["path"]), ("explorer", explorer["path"]),
        ):
            with self.subTest(role=role):
                self.assertEqual(path, canonical)

        for subcommand in ("index", "serve"):
            with self.subTest(subcommand=subcommand):
                storage = unified_storage_config(subcommand)
                self.assertEqual(storage["path"], canonical)
                self.assertNotIn("canonical_path", storage)


if __name__ == "__main__":
    unittest.main()
