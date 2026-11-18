"""Sealing migration and packaging memory profile."""
import gzip
import hashlib
import io
import json
import os
import resource
import sqlite3
import time
import zipfile
from pathlib import Path

from fastapi.testclient import TestClient


def test_init_db_backfills_null_evidence_for_pre_sealing_lane(data_dir):
    """A DB created before sealing gets explicit NULL evidence rows.

    Such lanes stay visible for normal downloads but can never be packaged
    into a verified bundle without being rejected.
    """
    from app import config, db

    # Simulate a pre-sealing published lane: publish rows + files, no manifests.
    lane_dir = config.PUBLISHED_DIR / "lanes" / "oldLane"
    lane_dir.mkdir(parents=True)
    r1 = lane_dir / "s1.r1.fastq.gz"
    r2 = lane_dir / "s1.r2.fastq.gz"
    for p in (r1, r2):
        with gzip.open(p, "wb") as g:
            g.write(b"@x\nTT\n+\nII\n")
    conn = db.get_conn()
    with db.tx() as c:
        c.execute("INSERT INTO lane_publishes (lane, job_id, published_at, seq) "
                  "VALUES ('oldLane','legacyJob',?,1)", (db.utcnow(),))
        c.execute("INSERT INTO lane_counts VALUES ('oldLane','s1',?,?,1)",
                  (str(r1), str(r2)))
    assert conn.execute(
        "SELECT COUNT(*) FROM lane_manifests WHERE lane='oldLane'"
    ).fetchone()[0] == 0

    db.init_db()  # migration runs
    row = conn.execute(
        "SELECT * FROM lane_manifests WHERE lane='oldLane'").fetchone()
    assert row is not None
    assert row["r1_sha256"] is None and row["r2_sha256"] is None


def test_legacy_lane_visible_for_plain_download_but_not_bundle(
        client, helpers, tmp_path):
    _post_and_wait = None
    from app import db as dbmod
    # Publish a lane, then strip its digests to emulate a legacy row.
    r1 = tmp_path / "R1.fastq.gz"
    r2 = tmp_path / "R2.fastq.gz"
    with gzip.open(r1, "wb") as g:
        g.write(b"@a\nAAAAAAAATTTT\n+\nIIIIIIIIIIII\n")
    with gzip.open(r2, "wb") as g:
        g.write(b"@a\nCCCCCCCCGGGG\n+\nJJJJJJJJJJJJ\n")
    barcodes = [{"sample_id": "s1", "barcode_r1": "AAAAAAAA",
                 "barcode_r2": "CCCCCCCC"}]
    with open(r1, "rb") as f1, open(r2, "rb") as f2:
        resp = client.post(
            "/jobs",
            data={"lane": "legacyLane", "barcodes": json.dumps(barcodes)},
            files={"r1": ("R1.fastq.gz", f1, "application/gzip"),
                   "r2": ("R2.fastq.gz", f2, "application/gzip")})
    job_id = resp.json()["job"]["job_id"]
    for _ in range(300):
        if client.get(f"/jobs/{job_id}").json()["status"] in (
                "published", "failed"):
            break
        time.sleep(0.05)

    dbmod.get_conn().execute(
        "UPDATE lane_manifests SET r1_sha256=NULL, r2_sha256=NULL, "
        "sealed_at=NULL WHERE lane='legacyLane'")
    # Plain per-lane download still works (backwards compatible).
    assert client.get("/lanes/legacyLane/samples/s1/r1").status_code == 200
    snap = client.post("/snapshots").json()
    resp = client.get(
        f"/snapshots/{snap['snapshot_id']}/samples/s1/bundle.zip")
    assert resp.status_code == 409
    assert "legacyLane" in resp.json()["detail"]


def test_bundle_build_memory_stays_bounded(client, helpers, tmp_path):
    """Building a ZIP must not buffer the archive or mates in RAM."""
    from app import package

    # ~40 MiB of decompressed FASTQ per lane, two lanes.
    pairs = 120_000
    records = []
    for directory, lane in (("a", "mA"), ("b", "mB")):
        d = tmp_path / directory
        d.mkdir()
        r1 = []
        r2 = []
        for i in range(pairs):
            r1.append((f"r{i}".encode(), b"AAAAAAAA" + b"ACGT" * 8, b"I" * 40))
            r2.append((f"r{i}".encode(), b"CCCCCCCC" + b"TGCA" * 8, b"J" * 40))
        records.append((lane, helpers.make_pair(r1, r2, d)))
    barcodes = [{"sample_id": "s1", "barcode_r1": "AAAAAAAA",
                 "barcode_r2": "CCCCCCCC"}]
    for lane, (p1, p2) in records:
        with open(p1, "rb") as f1, open(p2, "rb") as f2:
            resp = client.post(
                "/jobs",
                data={"lane": lane, "barcodes": json.dumps(barcodes)},
                files={"r1": ("R1.fastq.gz", f1, "application/gzip"),
                       "r2": ("R2.fastq.gz", f2, "application/gzip")})
        job_id = resp.json()["job"]["job_id"]
        for _ in range(600):
            if client.get(f"/jobs/{job_id}").json()["status"] in (
                    "published", "failed"):
                break
            time.sleep(0.05)
        assert client.get(f"/jobs/{job_id}").json()["status"] == "published"

    snap = client.post("/snapshots").json()
    rss_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    plan = package.plan_bundle(snap["snapshot_id"], "s1")
    out = tmp_path / "bundle.zip"
    info = package.write_bundle(plan, out)
    rss_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    growth_mib = (rss_after - rss_before) / 1024
    print(f"\nbundle size {out.stat().st_size} bytes on disk, "
          f"RSS growth {growth_mib:.1f} MiB")
    assert out.stat().st_size == info["archive"]["size"]
    assert sum(m["size"] for m in info["members"].values()) > 1_000_000
    assert growth_mib < 64, f"RSS grew {growth_mib:.1f} MiB building the bundle"

    with zipfile.ZipFile(out) as zf:
        with gzip.open(io.BytesIO(zf.read("s1.r1.fastq.gz")), "rb") as g:
            data = g.read()
    # Two lanes x `pairs` trimmed 32bp reads, 4 FASTQ lines each.
    assert data.count(b"\n") == 2 * pairs * 4
    assert len(data) > 2 * pairs * 60  # ~90 MiB of decompressed R1
