"""Hash raw evidence into manifests/raw_hashes.json.

raw/ is local-only (gitignored); the manifest records sha256 of every
file so immutability is auditable without shipping the evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default="raw")
    ap.add_argument("--out", default="manifests/raw_hashes.json")
    args = ap.parse_args(argv)

    entries = {}
    for root, _dirs, files in os.walk(args.raw):
        for name in sorted(files):
            p = os.path.join(root, name)
            rel = os.path.relpath(p, args.raw).replace(os.sep, "/")
            entries[rel] = {
                "sha256": _sha256(p),
                "bytes": os.path.getsize(p),
            }
    doc = {
        "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "raw_dir": args.raw,
        "n_files": len(entries),
        "files": entries,
    }
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2)
    print(json.dumps({"hashed": len(entries), "out": args.out}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
