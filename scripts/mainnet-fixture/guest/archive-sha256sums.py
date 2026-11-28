#!/usr/bin/env python3
"""archive-sha256sums.py <archive_dir>: print a sha256sum listing of a sealed
private backup from its recovery-archive.json, plus that manifest itself.
The archive was hashed when it was sealed; this avoids reading it again."""
import hashlib, json, os, sys
root = sys.argv[1]
manifest_path = os.path.join(root, "recovery-archive.json")
raw = open(manifest_path, "rb").read()
manifest = json.loads(raw)
lines = [(f["sha256"], f["path"]) for f in manifest["payload_files"]]
lines.append((hashlib.sha256(raw).hexdigest(), "recovery-archive.json"))
on_disk = set()
for d, _, files in os.walk(root):
    for name in files:
        on_disk.add(os.path.relpath(os.path.join(d, name), root))
listed = {p for _, p in lines}
if on_disk != listed:
    sys.exit(f"archive files and manifest disagree: extra={sorted(on_disk - listed)[:5]} missing={sorted(listed - on_disk)[:5]}")
for digest, path in sorted(lines, key=lambda x: x[1]):
    print(f"{digest}  {path}")
