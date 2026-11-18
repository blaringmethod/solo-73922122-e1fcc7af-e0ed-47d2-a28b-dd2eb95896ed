#!/usr/bin/env python3
"""Verify a downloaded bundle.zip against its own manifest and ZIP digest.

Usage:
    python scripts/verify_bundle.py BUNDLE.zip [EXPECTED_SHA256]

Checks:
  * ZIP integrity (CRCs of every entry);
  * archive SHA-256 equals EXPECTED_SHA256, if given (the X-Bundle-SHA256
    response header of the API);
  * each mate entry is exactly the concatenation described by
    manifest.json, sliced by the per-lane sealed sizes, and every lane slice
    hashes to the sealed SHA-256;
  * both mates open as one gzip stream with the same number of FASTQ lines.

Exit code 0 on success, 1 on any mismatch. Never writes anything.
"""
from __future__ import annotations

import gzip
import hashlib
import io
import json
import sys
import zipfile
from pathlib import Path

CHUNK = 1024 * 1024


def verify(path: Path, expected_sha256: str | None) -> list[str]:
    errors: list[str] = []
    raw = path.read_bytes()
    actual_sha = hashlib.sha256(raw).hexdigest()
    if expected_sha256 and actual_sha != expected_sha256.lower():
        errors.append(
            f"archive sha256 {actual_sha} != expected {expected_sha256}")

    try:
        zf = zipfile.ZipFile(io.BytesIO(raw))
    except zipfile.BadZipFile as exc:
        return [f"not a ZIP archive: {exc}"]
    with zf:
        bad = zf.testzip()
        if bad is not None:
            return errors + [f"corrupt ZIP member: {bad}"]
        required = {"manifest.json", "README.txt"}
        names = set(zf.namelist())
        if not required <= names:
            return errors + [f"missing entries: {sorted(required - names)}"]
        manifest = json.loads(zf.read("manifest.json"))
        sample = manifest["sample_id"]
        lanes = manifest["lanes"]
        detail = {d["lane"]: d for d in manifest["lanes_detail"]}
        if list(detail) != lanes:
            errors.append(
                f"manifest lane order {list(detail)} != lanes {lanes}")
        if [d["lane_seq"] for d in manifest["lanes_detail"]] != sorted(
                d["lane_seq"] for d in manifest["lanes_detail"]):
            errors.append("lanes_detail is not in frozen lane order")

        for mate in ("r1", "r2"):
            entry = f"{sample}.{mate}.fastq.gz"
            if entry not in names:
                errors.append(f"missing mate entry {entry}")
                continue
            try:
                data = zf.read(entry)
            except zipfile.BadZipFile as exc:
                errors.append(f"{entry}: unreadable: {exc}")
                continue
            offset = 0
            for lane in lanes:
                want = detail[lane][mate]
                slice_ = data[offset:offset + want["size"]]
                got = hashlib.sha256(slice_).hexdigest()
                if len(slice_) != want["size"]:
                    errors.append(
                        f"{entry} lane {lane}: short slice "
                        f"({len(slice_)} != {want['size']})")
                elif got != want["sha256"]:
                    errors.append(
                        f"{entry} lane {lane}: sha256 {got} != sealed "
                        f"{want['sha256']}")
                offset += want["size"]
            if offset != len(data):
                errors.append(
                    f"{entry}: {len(data) - offset} trailing bytes after "
                    "the last lane")
            total_pairs = sum(detail[lane]["read_pairs"] for lane in lanes)
            try:
                with gzip.open(io.BytesIO(data), "rb") as g:
                    lines = 0
                    while True:
                        block = g.read(CHUNK)
                        if not block:
                            break
                        lines += block.count(b"\n")
            except (OSError, EOFError) as exc:
                errors.append(f"{entry}: not a readable gzip stream: {exc}")
                continue
            if lines != total_pairs * 4:
                errors.append(
                    f"{entry}: {lines} lines, expected {total_pairs * 4} "
                    f"({total_pairs} pairs)")

        archive_digest = hashlib.sha256(raw).hexdigest()
        print(f"snapshot: {manifest['snapshot_id']}")
        print(f"sample:   {sample}")
        print(f"lanes:    {', '.join(lanes)}")
        print(f"zip sha256: {archive_digest}")
    return errors


def main(argv: list[str]) -> int:
    if len(argv) not in (2, 3):
        print(__doc__, file=sys.stderr)
        return 2
    path = Path(argv[1])
    errors = verify(path, argv[2] if len(argv) == 3 else None)
    if errors:
        for e in errors:
            print(f"FAIL: {e}", file=sys.stderr)
        return 1
    print("OK: all lanes, mates and the archive match the sealed manifest")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
