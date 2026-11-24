"""Print the BuildKit Cargo cache key inputs as GitHub step outputs.

The key covers only what decides the third-party release artifacts: the
toolchain file, the Dockerfile up to the stage that copies the workspace, and
every Cargo.lock package that is not a workspace member. A pull request that
changes only workspace code therefore restores the same cache that `main`
saved, and the workspace crates recompile on top of it.

Usage: cache_key.py ARCH RUST_VERSION
"""

import glob
import hashlib
import json
import pathlib
import sys
import tomllib


def workspace_package_names(root: pathlib.Path) -> list[str]:
    manifest = tomllib.loads((root / "Cargo.toml").read_text())
    workspace = manifest["workspace"]
    excluded = {(root / path).resolve() for path in workspace.get("exclude", [])}
    names = set()
    if "package" in manifest:
        names.add(manifest["package"]["name"])
    for pattern in workspace["members"]:
        for member in sorted(glob.glob(str(root / pattern))):
            member_path = pathlib.Path(member).resolve()
            if member_path in excluded:
                continue
            member_manifest = tomllib.loads((member_path / "Cargo.toml").read_text())
            names.add(member_manifest["package"]["name"])
    if not names:
        raise SystemExit("Cargo.toml lists no workspace members")
    return sorted(names)


def third_party_lock_digest(root: pathlib.Path, workspace_names: set[str]) -> str:
    lock = tomllib.loads((root / "Cargo.lock").read_text())
    packages = [
        package
        for package in lock.get("package", [])
        if package["name"] not in workspace_names
    ]
    if any("source" not in package for package in packages):
        local = sorted(package["name"] for package in packages if "source" not in package)
        raise SystemExit(f"Cargo.lock has path packages outside the workspace: {local}")
    canonical = json.dumps(packages, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def dockerfile_prefix_digest(root: pathlib.Path) -> str:
    digest = hashlib.sha256((root / "rust-toolchain.toml").read_bytes())
    for line in (root / "deploy" / "Dockerfile").read_text().splitlines(keepends=True):
        if line.rstrip("\n") == "COPY . .":
            break
        digest.update(line.encode())
    return digest.hexdigest()


def main() -> None:
    arch, rust_version = sys.argv[1], sys.argv[2]
    root = pathlib.Path.cwd()
    names = workspace_package_names(root)
    lock_digest = third_party_lock_digest(root, set(names))
    prefix = f"v1-bk-cargo-{arch}-{rust_version}-{dockerfile_prefix_digest(root)[:12]}-"
    print(f"key={prefix}{lock_digest[:16]}")
    print(f"restore-key={prefix}")
    print(f"workspace-packages={' '.join(names)}")


if __name__ == "__main__":
    main()
