#!/usr/bin/env python3
"""Check the Zally harness build recipe: standalone lockfile and recompilation.

The out-of-tree harness must bootstrap a standalone `Cargo.lock` that covers the
`zally-payment-harness` package while preserving the pinned Zally dependency
cohort, then enforce `--locked` on every later build. Cargo is always invoked so
an existing executable never masks stale source. These checks mock `cargo` and
`shutil` so they stay hermetic; the real build is verified separately.
"""
import importlib
import json
import tempfile
import unittest
from subprocess import CompletedProcess
from pathlib import Path
from unittest import mock

zally = importlib.import_module("zally-payment")

HARNESS = "zally-payment-harness"

# A minimal lockfile that already contains the harness package.
HARNESS_LOCK = """# generated
version = 4

[[package]]
name = "zally-payment-harness"
version = "0.1.0"
dependencies = ["serde_json", "tokio", "zcash_protocol"]

[[package]]
name = "tokio"
version = "1.54.1"

[[package]]
name = "serde_json"
version = "1.0.151"

[[package]]
name = "zcash_protocol"
version = "0.11.0"
"""

# The pinned Zally workspace lock, which never contains the harness package.
SOURCE_LOCK = """# generated
version = 4

[[package]]
name = "tokio"
version = "1.54.1"

[[package]]
name = "serde_json"
version = "1.0.151"

[[package]]
name = "zcash_protocol"
version = "0.11.0"
"""


class ZallyBuild(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root / "zally-source"
        (self.source / "crates" / "zally-wallet").mkdir(parents=True)
        (self.source / "crates" / "zally-wallet" / "Cargo.toml").write_text(
            '[package]\nname = "zally-wallet"\n')
        (self.source / "Cargo.lock").write_text(SOURCE_LOCK)
        self.harness = self.root / "zally-payment.rs"
        self.harness.write_text("// harness\n")
        self.build_dir = self.root / "build"
        self.binary = self.build_dir / "target" / "debug" / HARNESS
        self.calls = []

    def fake_run(self, materialize_lock=True, materialize_binary=True):
        def run(command, **kwargs):
            self.calls.append(("run", command, kwargs))
            if materialize_lock:
                (self.build_dir / "Cargo.lock").write_text(HARNESS_LOCK)
            if materialize_binary:
                self.binary.parent.mkdir(parents=True, exist_ok=True)
                self.binary.write_text("binary")
            artifact = {"reason": "compiler-artifact", "target": {"name": HARNESS, "kind": ["bin"]},
                        "executable": str(self.binary)}
            return CompletedProcess(command, 0, stdout=json.dumps(artifact) if materialize_binary else "")
        return run

    def fake_copy(self, src, dst):
        self.calls.append(("copy", str(src), str(dst)))
        return str(dst)

    def build(self, **kwargs):
        with mock.patch.object(zally.subprocess, "run", self.fake_run()):
            with mock.patch.object(zally.shutil, "copy2", self.fake_copy):
                return zally.build(self.source, self.build_dir, self.harness, **kwargs)

    def run_commands(self):
        return [command for kind, command, _ in self.calls if kind == "run"]

    def copies(self):
        return [(src, dst) for kind, src, dst in self.calls if kind == "copy"]

    def test_lockfile_covers_harness_detects_the_package(self):
        lock = self.root / "lock"
        lock.write_text(HARNESS_LOCK)
        self.assertTrue(zally.lockfile_covers_harness(lock))
        lock.write_text(SOURCE_LOCK)
        self.assertFalse(zally.lockfile_covers_harness(lock))
        lock.unlink()
        self.assertFalse(zally.lockfile_covers_harness(lock))

    def test_copy_harness_rewrites_only_on_content_change(self):
        source = self.root / "harness.rs"
        source.write_text("v1\n")
        destination = self.root / "main.rs"
        zally.copy_harness(source, destination)
        self.assertEqual(destination.read_text(), "v1\n")
        stamp = (destination.stat().st_mtime_ns, destination.stat().st_size)
        # Identical content must not disturb the destination mtime, so Cargo's
        # incremental build keeps reusing the unchanged source.
        zally.copy_harness(source, destination)
        self.assertEqual((destination.stat().st_mtime_ns, destination.stat().st_size),
                         stamp)
        # A real content change must rewrite the destination.
        source.write_text("v2\n")
        zally.copy_harness(source, destination)
        self.assertEqual(destination.read_text(), "v2\n")

    def test_fresh_build_bootstraps_without_locked_and_preserves_seed(self):
        self.build()
        commands = self.run_commands()
        self.assertEqual(len(commands), 1)
        self.assertNotIn("--locked", commands[0])
        self.assertIn("--offline", commands[0])
        self.assertIn("--jobs", commands[0])
        self.assertEqual(commands[0][commands[0].index("--jobs") + 1], "2")
        copies = self.copies()
        self.assertIn((str(self.source / "Cargo.lock"), str(self.build_dir / "Cargo.lock")),
                      copies)
        self.assertEqual((self.build_dir / "src" / "main.rs").read_bytes(),
                         self.harness.read_bytes())
        manifest = (self.build_dir / "Cargo.toml").read_text()
        self.assertIn('name = "zally-payment-harness"', manifest)
        self.assertIn('rust-version = "1.100"', manifest)
        self.assertIn(f'path = "{self.source}/crates/zally-wallet"', manifest)

    def test_reused_build_always_invokes_cargo_with_locked(self):
        self.build_dir.mkdir(parents=True)
        (self.build_dir / "Cargo.lock").write_text(HARNESS_LOCK)
        self.binary.parent.mkdir(parents=True, exist_ok=True)
        self.binary.write_text("stale")
        self.build()
        commands = self.run_commands()
        self.assertEqual(len(commands), 1)
        self.assertIn("--locked", commands[0])
        self.assertIn("--offline", commands[0])
        # The seed copy must not happen when the standalone lock already exists.
        self.assertNotIn((str(self.source / "Cargo.lock"), str(self.build_dir / "Cargo.lock")),
                         self.copies())

    def test_reused_lock_must_match_the_selected_source(self):
        self.build_dir.mkdir()
        (self.build_dir / "Cargo.lock").write_text(HARNESS_LOCK.replace("1.54.1", "1.54.2"))
        with self.assertRaisesRegex(ValueError, "dependency pins"):
            self.build()
        self.assertEqual(self.run_commands(), [])

    def test_bootstrap_refuses_packages_outside_the_workspace_lock(self):
        run = self.fake_run()
        def drift(command, **kwargs):
            result = run(command, **kwargs)
            (self.build_dir / "Cargo.lock").write_text(HARNESS_LOCK.replace("1.0.151", "1.0.152"))
            return result
        with mock.patch.object(zally.subprocess, "run", drift):
            with self.assertRaisesRegex(ValueError, "dependency pins"):
                zally.build(self.source, self.build_dir, self.harness)

    def test_no_reported_binary_rejects_stale_executable(self):
        self.binary.parent.mkdir(parents=True)
        self.binary.touch()
        with mock.patch.object(zally.subprocess, "run", self.fake_run(materialize_binary=False)):
            with self.assertRaisesRegex(ValueError, "no binary"):
                zally.build(self.source, self.build_dir, self.harness)

    def test_jobs_parameter_reaches_cargo(self):
        self.build(jobs=3)
        commands = self.run_commands()
        self.assertEqual(commands[0][commands[0].index("--jobs") + 1], "3")

    def test_bootstrap_without_a_harness_lock_raises(self):
        with mock.patch.object(zally.subprocess, "run", self.fake_run(materialize_lock=False)):
            with mock.patch.object(zally.shutil, "copy2", self.fake_copy):
                with self.assertRaisesRegex(ValueError, "no standalone lockfile"):
                    zally.build(self.source, self.build_dir, self.harness)

    def test_missing_binary_raises(self):
        with mock.patch.object(zally.subprocess, "run",
                               self.fake_run(materialize_binary=False)):
            with mock.patch.object(zally.shutil, "copy2", self.fake_copy):
                with self.assertRaisesRegex(ValueError, "no binary"):
                    zally.build(self.source, self.build_dir, self.harness)

    def test_source_without_wallet_crate_raises(self):
        (self.source / "crates" / "zally-wallet" / "Cargo.toml").unlink()
        with self.assertRaisesRegex(ValueError, "does not contain crates/zally-wallet"):
            zally.build(self.source, self.build_dir, self.harness)

    def test_source_without_lockfile_raises(self):
        (self.source / "Cargo.lock").unlink()
        with self.assertRaisesRegex(ValueError, "does not contain Cargo.lock"):
            zally.build(self.source, self.build_dir, self.harness)


if __name__ == "__main__":
    unittest.main()
