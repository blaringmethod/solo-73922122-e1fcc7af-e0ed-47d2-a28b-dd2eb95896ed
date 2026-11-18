"""Snapshot bundle API: sealing, verification, all-or-nothing packaging."""
import gzip
import hashlib
import io
import json
import time
import zipfile
from pathlib import Path

import pytest

BARCODES = [
    {"sample_id": "s1", "barcode_r1": "AAAAAAAA", "barcode_r2": "CCCCCCCC"},
    {"sample_id": "s2", "barcode_r1": "GGGGGGGG", "barcode_r2": "TTTTTTTT"},
]


def _post(client, lane, barcodes, r1_path, r2_path):
    with open(r1_path, "rb") as f1, open(r2_path, "rb") as f2:
        return client.post(
            "/jobs",
            data={"lane": lane, "barcodes": json.dumps(barcodes)},
            files={"r1": ("R1.fastq.gz", f1, "application/gzip"),
                   "r2": ("R2.fastq.gz", f2, "application/gzip")},
        )


def _wait(client, job_id, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        row = client.get(f"/jobs/{job_id}").json()
        if row["status"] in ("published", "failed"):
            return row
        time.sleep(0.05)
    raise AssertionError("job timeout")


def _lane_records(count=4):
    r1, r2 = [], []
    for i in range(count):
        r1.append((f"r{i}".encode(), b"AAAAAAAATTTT", b"I" * 12))
        r2.append((f"r{i}".encode(), b"CCCCCCCCGGGG", b"J" * 12))
    # one read only for s1 / s2 differentiation input is same lane content
    return r1, r2


def _publish(client, helpers, tmp_path, lane, barcodes=BARCODES, records=None):
    records = records or _lane_records()
    d = tmp_path / lane
    d.mkdir(exist_ok=True)
    p1, p2 = helpers.make_pair(records[0], records[1], d)
    resp = _post(client, lane, barcodes, p1, p2)
    assert resp.status_code == 201, resp.text
    done = _wait(client, resp.json()["job"]["job_id"])
    assert done["status"] == "published", done
    return done


def test_bundle_contains_two_verified_mates(client, helpers, tmp_path):
    _publish(client, helpers, tmp_path, "L1")
    snap = client.post("/snapshots").json()

    r = client.get(f"/snapshots/{snap['snapshot_id']}/samples/s1/bundle.zip")
    assert r.status_code == 200, r.text
    assert r.headers["content-type"] == "application/zip"
    assert r.headers["x-snapshot-id"] == snap["snapshot_id"]
    assert r.headers["x-bundle-lanes"] == "L1"
    body = r.content
    assert hashlib.sha256(body).hexdigest() == r.headers["x-bundle-sha256"]
    assert int(r.headers["x-bundle-size"]) == len(body)

    with zipfile.ZipFile(io.BytesIO(body)) as zf:
        names = set(zf.namelist())
        assert {"s1.r1.fastq.gz", "s1.r2.fastq.gz",
                "manifest.json", "README.txt"} <= names
        manifest = json.loads(zf.read("manifest.json"))
        assert manifest["snapshot_id"] == snap["snapshot_id"]
        assert manifest["sample_id"] == "s1"
        assert manifest["lanes"] == ["L1"]
        detail = manifest["lanes_detail"][0]
        assert detail["sample_id"] == "s1" and detail["read_pairs"] == 4

        for mate, ext in (("r1", "r1"), ("r2", "r2")):
            raw = zf.read(f"s1.{ext}.fastq.gz")
            assert hashlib.sha256(raw).hexdigest() == detail[mate]["sha256"]
            assert len(raw) == detail[mate]["size"]
            assert manifest["members"][f"s1.{ext}.fastq.gz"]["sha256_hex"] \
                == detail[mate]["sha256"]
            # gzip opens as one stream and carries exactly the lane's reads.
            with gzip.open(io.BytesIO(raw)) as g:
                assert len(g.read().splitlines()) == 4 * 4


def test_bundle_concatenates_multiple_lanes_in_snapshot_order(
        client, helpers, tmp_path):
    _publish(client, helpers, tmp_path, "LZ")   # created first
    _publish(client, helpers, tmp_path, "LA")   # created second
    snap = client.post("/snapshots").json()
    assert snap["lanes"] == ["LZ", "LA"]  # creation order, not alphabetical

    r = client.get(f"/snapshots/{snap['snapshot_id']}/samples/s1/bundle.zip")
    assert r.status_code == 200
    assert r.headers["x-bundle-lanes"] == "LZ,LA"
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        manifest = json.loads(zf.read("manifest.json"))
        assert [d["lane"] for d in manifest["lanes_detail"]] == ["LZ", "LA"]
        raw = zf.read("s1.r1.fastq.gz")
        # Two complete gzip members back to back (each starts with 1f 8b).
        assert raw[:2] == b"\x1f\x8b"
        with gzip.open(io.BytesIO(raw)) as g:
            lines = g.read().splitlines()
        assert len(lines) == 8 * 4  # 2 lanes x 4 pairs x 4 lines

    # Snapshot order is the frozen lane order even if lanes are later added.
    _publish(client, helpers, tmp_path, "LM")
    r2 = client.get(f"/snapshots/{snap['snapshot_id']}/samples/s1/bundle.zip")
    assert r2.headers["x-bundle-lanes"] == "LZ,LA"


def test_new_lane_never_mixes_into_existing_snapshot(client, helpers, tmp_path):
    _publish(client, helpers, tmp_path, "oldLane")
    snap = client.post("/snapshots").json()
    before = client.get(
        f"/snapshots/{snap['snapshot_id']}/samples/s1/bundle.zip")
    _publish(client, helpers, tmp_path, "newLane")
    after = client.get(
        f"/snapshots/{snap['snapshot_id']}/samples/s1/bundle.zip")
    assert after.status_code == 200
    assert after.headers["x-bundle-lanes"] == "oldLane"
    assert after.content == before.content  # byte-identical, lane excluded


def test_lane_without_sample_is_skipped(client, helpers, tmp_path):
    only_s2 = [BARCODES[1]]
    _publish(client, helpers, tmp_path, "s2only", barcodes=only_s2)
    _publish(client, helpers, tmp_path, "both", barcodes=BARCODES)
    snap = client.post("/snapshots").json()
    r = client.get(f"/snapshots/{snap['snapshot_id']}/samples/s1/bundle.zip")
    assert r.status_code == 200
    assert r.headers["x-bundle-lanes"] == "both"
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        detail = json.loads(zf.read("manifest.json"))["lanes_detail"]
        assert [d["lane"] for d in detail] == ["both"]


def test_sample_absent_from_snapshot_errors(client, helpers, tmp_path):
    _publish(client, helpers, tmp_path, "L1")
    snap = client.post("/snapshots").json()
    r = client.get(f"/snapshots/{snap['snapshot_id']}/samples/ghost/bundle.zip")
    assert r.status_code == 404
    assert "not present" in r.json()["detail"]


def test_unknown_snapshot_and_bad_sample_id(client):
    assert client.get("/snapshots/nope/samples/s1/bundle.zip").status_code == 404
    assert client.get("/snapshots/nope/samples/a%2Fb/bundle.zip").status_code in (
        400, 404, 422)


def test_tampered_file_fails_whole_bundle(client, helpers, tmp_path):
    _publish(client, helpers, tmp_path, "L1")
    _publish(client, helpers, tmp_path, "L2")
    snap = client.post("/snapshots").json()
    assert client.get(
        f"/snapshots/{snap['snapshot_id']}/samples/s1/bundle.zip").status_code == 200

    # Flip one byte on disk in the second lane's R2 output. The current file
    # hash is not evidence: the sealed digest must reject it.
    good = client.get("/lanes/L2/samples/s1/r2").content
    from app import db as dbmod
    row = dbmod.get_conn().execute(
        "SELECT * FROM lane_manifests WHERE lane='L2' AND sample_id='s1'"
    ).fetchone()
    path = Path(row["r2_file"])
    data = bytearray(path.read_bytes())
    data[len(data) // 2] ^= 0xFF
    path.write_bytes(bytes(data))
    assert path.read_bytes() != good

    r = client.get(f"/snapshots/{snap['snapshot_id']}/samples/s1/bundle.zip")
    assert r.status_code == 409
    assert "digest" in r.json()["detail"].lower()
    assert r.headers["content-type"] != "application/zip"


def test_missing_mate_fails_whole_bundle(client, helpers, tmp_path):
    _publish(client, helpers, tmp_path, "L1")
    snap = client.post("/snapshots").json()
    from app import db as dbmod
    row = dbmod.get_conn().execute(
        "SELECT * FROM lane_manifests WHERE lane='L1' AND sample_id='s1'"
    ).fetchone()
    Path(row["r1_file"]).unlink()
    r = client.get(f"/snapshots/{snap['snapshot_id']}/samples/s1/bundle.zip")
    assert r.status_code == 409
    assert "missing" in r.json()["detail"].lower()


def test_legacy_lane_without_sealed_digest_rejected(
        client, helpers, tmp_path):
    _publish(client, helpers, tmp_path, "L1")
    from app import db as dbmod
    conn = dbmod.get_conn()
    # Simulate a lane published before sealing existed: no digest evidence.
    conn.execute(
        "UPDATE lane_manifests SET r1_sha256=NULL, r2_sha256=NULL, "
        "sealed_at=NULL WHERE lane='L1'")
    snap = client.post("/snapshots").json()
    r = client.get(f"/snapshots/{snap['snapshot_id']}/samples/s1/bundle.zip")
    assert r.status_code == 409
    assert "no sealed digest" in r.json()["detail"]


def test_lane_without_digests_rejects_snapshot_period(client, helpers, tmp_path):
    """A legacy lane can never be silently dropped from a verified bundle."""
    _publish(client, helpers, tmp_path, "legacy")
    from app import db as dbmod
    conn = dbmod.get_conn()
    conn.execute(
        "UPDATE lane_manifests SET r1_sha256=NULL, r2_sha256=NULL, "
        "sealed_at=NULL WHERE lane='legacy'")
    _publish(client, helpers, tmp_path, "modern")   # has sealed evidence
    snap = client.post("/snapshots").json()
    r = client.get(f"/snapshots/{snap['snapshot_id']}/samples/s1/bundle.zip")
    assert r.status_code == 409
    assert "legacy" in r.json()["detail"]


def test_snapshot_freezes_evidence_not_current_file(client, helpers, tmp_path):
    """Old evidence is the sealed value, not a later re-hash of the file."""
    _publish(client, helpers, tmp_path, "L1")
    snap = client.post("/snapshots").json()
    from app import db as dbmod
    conn = dbmod.get_conn()
    row = conn.execute(
        "SELECT * FROM snapshot_manifests WHERE snapshot_id=? AND lane='L1'",
        (snap["snapshot_id"],)).fetchone()
    assert row["r1_sha256"] is not None

    # Mutate the live lane evidence to a different digest; the snapshot copy
    # must still detect the disagreement and refuse.
    conn.execute(
        "UPDATE lane_manifests SET r1_sha256=? WHERE lane='L1' AND sample_id='s1'",
        ("0" * 64,))
    r = client.get(f"/snapshots/{snap['snapshot_id']}/samples/s1/bundle.zip")
    assert r.status_code == 500
    assert "disagrees" in r.json()["detail"]

    # And an independently corrupted file does not pass by hashing "now".
    conn.execute(
        "UPDATE lane_manifests SET r1_sha256=(SELECT r1_sha256 "
        "FROM snapshot_manifests WHERE snapshot_id=? AND lane='L1' "
        "AND sample_id='s1') WHERE lane='L1' AND sample_id='s1'",
        (snap["snapshot_id"],))
    path = Path(conn.execute(
        "SELECT r1_file FROM lane_manifests WHERE lane='L1' AND sample_id='s1'"
    ).fetchone()["r1_file"])
    data = bytearray(path.read_bytes())
    data[-5] ^= 0x01
    path.write_bytes(bytes(data))
    assert client.get(
        f"/snapshots/{snap['snapshot_id']}/samples/s1/bundle.zip"
    ).status_code == 409


def test_quarantine_bucket_not_offered_as_sample(client, helpers, tmp_path):
    _publish(client, helpers, tmp_path, "L1")
    snap = client.post("/snapshots").json()
    r = client.get(
        f"/snapshots/{snap['snapshot_id']}/samples/__quarantine__/bundle.zip")
    # Reserved bucket name: explicitly rejected, never served as a sample.
    assert r.status_code == 400


def test_bundle_endpoint_not_shadowed_by_mate_route(client, helpers, tmp_path):
    _publish(client, helpers, tmp_path, "L1")
    snap = client.post("/snapshots").json()
    r = client.get(f"/snapshots/{snap['snapshot_id']}/samples/s1/bundle.zip")
    assert r.headers["content-type"] == "application/zip"
    r_mate = client.get(f"/snapshots/{snap['snapshot_id']}/samples/s1/r1")
    assert r_mate.headers["content-type"] == "application/gzip"


def test_lane_reports_sealed_digests_matching_download(client, helpers,
                                                       tmp_path):
    _publish(client, helpers, tmp_path, "L1")
    lane = client.get("/lanes/L1").json()
    assert lane["sealed"] is True
    by_sample = {m["sample_id"]: m for m in lane["sealed_manifests"]}
    assert set(by_sample) == {"s1", "s2", "__quarantine__"}
    for sample_id in ("s1", "s2", "__quarantine__"):
        for mate in ("r1", "r2"):
            entry = by_sample[sample_id][mate]
            if sample_id == "__quarantine__":
                body = client.get(f"/lanes/L1/quarantine/{mate}").content
            else:
                body = client.get(
                    f"/lanes/L1/samples/{sample_id}/{mate}").content
            assert hashlib.sha256(body).hexdigest() == entry["sha256"]
            assert len(body) == entry["size"]


def test_seal_happens_in_same_transaction_as_publish(client, helpers,
                                                    tmp_path):
    """No published lane may exist without its manifest rows."""
    _publish(client, helpers, tmp_path, "L1")
    from app import db as dbmod
    conn = dbmod.get_conn()
    missing = conn.execute(
        "SELECT p.lane FROM lane_publishes p WHERE NOT EXISTS ("
        "  SELECT 1 FROM lane_manifests m WHERE m.lane=p.lane)"
    ).fetchall()
    assert missing == []
    # Every bucket in lane_counts has a matching manifest row.
    mismatch = conn.execute(
        "SELECT c.lane, c.sample_id FROM lane_counts c WHERE NOT EXISTS ("
        "  SELECT 1 FROM lane_manifests m WHERE m.lane=c.lane "
        "  AND m.sample_id=c.sample_id)").fetchall()
    assert mismatch == []


def test_shipped_verifier_accepts_and_rejects(tmp_path, client, helpers):
    import subprocess
    import sys

    _publish(client, helpers, tmp_path, "L1")
    snap = client.post("/snapshots").json()
    resp = client.get(
        f"/snapshots/{snap['snapshot_id']}/samples/s1/bundle.zip")
    out = tmp_path / "bundle.zip"
    out.write_bytes(resp.content)
    script = (__import__("pathlib").Path(__file__).resolve().parent.parent
              / "scripts" / "verify_bundle.py")
    ok = subprocess.run(
        [sys.executable, "-I", str(script), str(out),
         resp.headers["x-bundle-sha256"]],
        capture_output=True, text=True)
    assert ok.returncode == 0, ok.stderr
    assert "OK:" in ok.stdout

    # Wrong expected digest fails without needing to parse the archive.
    bad = subprocess.run(
        [sys.executable, "-I", str(script), str(out), "0" * 64],
        capture_output=True, text=True)
    assert bad.returncode == 1
    assert "archive sha256" in bad.stderr


def test_current_file_digest_cannot_substitute_for_old_evidence(
        client, helpers, tmp_path):
    """Rewrite a file *and* the live sealed row: the snapshot copy wins.

    This is the 'do not treat today's file hash as old evidence' rule. Even
    when the live lane_manifests row is re-sealed to match the new bytes, the
    snapshot's frozen evidence must flag the mismatch (500) and no ZIP is
    returned.
    """
    _publish(client, helpers, tmp_path, "L1")
    snap = client.post("/snapshots").json()
    snap_id = snap["snapshot_id"]
    assert client.get(
        f"/snapshots/{snap_id}/samples/s1/bundle.zip").status_code == 200

    from app import db as dbmod, processing
    conn = dbmod.get_conn()
    row = conn.execute(
        "SELECT * FROM lane_manifests WHERE lane='L1' AND sample_id='s1'"
    ).fetchone()
    path = Path(row["r1_file"])
    # Replace the file with different *valid gzip* content, then re-seal the
    # live row to the new digest (what a naive implementation might do).
    with gzip.open(path, "wb") as g:
        g.write(b"@replacement\nTTTT\n+\nIIII\n")
    size, digest = processing.hash_file(path)
    conn.execute(
        "UPDATE lane_manifests SET r1_size=?, r1_sha256=? "
        "WHERE lane='L1' AND sample_id='s1'", (size, digest))
    assert digest != row["r1_sha256"]

    r = client.get(f"/snapshots/{snap_id}/samples/s1/bundle.zip")
    assert r.status_code == 500
    assert "disagrees" in r.json()["detail"]
    assert r.headers["content-type"] != "application/zip"

    # A snapshot taken *after* the change would legitimately carry the new
    # sealed evidence, proving the check is about the frozen copy only.
    fresh = client.post("/snapshots").json()
    ok = client.get(f"/snapshots/{fresh['snapshot_id']}/samples/s1/bundle.zip")
    assert ok.status_code == 200


def test_packaging_failure_leaves_no_temp_archive(client, helpers, tmp_path):
    from app import config, db as dbmod
    _publish(client, helpers, tmp_path, "L1")
    snap = client.post("/snapshots").json()
    conn = dbmod.get_conn()
    row = conn.execute(
        "SELECT * FROM lane_manifests WHERE lane='L1' AND sample_id='s1'"
    ).fetchone()
    Path(row["r2_file"]).unlink()
    before = set(config.BUNDLE_TMP_DIR.glob("*.zip"))
    r = client.get(f"/snapshots/{snap['snapshot_id']}/samples/s1/bundle.zip")
    assert r.status_code == 409
    after = set(config.BUNDLE_TMP_DIR.glob("*.zip"))
    assert after == before  # no partial/leftover archive


def test_successful_bundle_removes_temp_archive(client, helpers, tmp_path):
    from app import config
    _publish(client, helpers, tmp_path, "L1")
    snap = client.post("/snapshots").json()
    r = client.get(f"/snapshots/{snap['snapshot_id']}/samples/s1/bundle.zip")
    assert r.status_code == 200
    assert list(config.BUNDLE_TMP_DIR.glob("*.zip")) == []
