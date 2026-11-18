"""SQLite persistence layer (WAL, safe for two processes on one volume)."""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

from . import config

_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=30000;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS jobs (
    job_id          TEXT PRIMARY KEY,
    lane            TEXT NOT NULL UNIQUE,
    status          TEXT NOT NULL CHECK (status IN
                        ('queued','processing','published','failed')),
    input_hash      TEXT NOT NULL,
    barcode_table   TEXT NOT NULL,
    r1_incoming     TEXT NOT NULL,
    r2_incoming     TEXT NOT NULL,
    error           TEXT,
    instance_id     TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    heartbeat_at    TEXT
);

CREATE TABLE IF NOT EXISTS job_samples (
    job_id      TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    sample_id   TEXT NOT NULL,
    barcode_r1  TEXT NOT NULL,
    barcode_r2  TEXT NOT NULL,
    PRIMARY KEY (job_id, sample_id)
);

CREATE TABLE IF NOT EXISTS lane_publishes (
    lane            TEXT PRIMARY KEY,
    job_id          TEXT NOT NULL,
    published_at    TEXT NOT NULL,
    seq             INTEGER NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS lane_counts (
    lane        TEXT NOT NULL REFERENCES lane_publishes(lane),
    sample_id   TEXT NOT NULL,           -- '' denotes the quarantine bucket
    r1_file     TEXT NOT NULL,
    r2_file     TEXT NOT NULL,
    read_pairs  INTEGER NOT NULL,
    PRIMARY KEY (lane, sample_id)
);

CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id   TEXT PRIMARY KEY,
    created_at    TEXT NOT NULL,
    lanes_json    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshot_lanes (
    snapshot_id TEXT NOT NULL REFERENCES snapshots(snapshot_id) ON DELETE CASCADE,
    lane        TEXT NOT NULL,
    seq         INTEGER NOT NULL,
    PRIMARY KEY (snapshot_id, lane)
);

-- Sealed digests of every published bucket file. Written in the same
-- transaction as the lane publish, appended during snapshot creation so a
-- snapshot keeps the evidence that existed when it was taken. Digest columns
-- are NULL only for legacy lanes published before sealing was introduced.
CREATE TABLE IF NOT EXISTS lane_manifests (
    lane        TEXT NOT NULL REFERENCES lane_publishes(lane),
    sample_id   TEXT NOT NULL,
    r1_file     TEXT,
    r1_size     INTEGER,
    r1_sha256   TEXT,
    r2_file     TEXT,
    r2_size     INTEGER,
    r2_sha256   TEXT,
    sealed_at   TEXT,
    PRIMARY KEY (lane, sample_id)
);

CREATE TABLE IF NOT EXISTS snapshot_manifests (
    snapshot_id TEXT NOT NULL REFERENCES snapshots(snapshot_id) ON DELETE CASCADE,
    lane        TEXT NOT NULL,
    sample_id   TEXT NOT NULL,
    seq         INTEGER NOT NULL,
    r1_file     TEXT NOT NULL,
    r1_size     INTEGER,
    r1_sha256   TEXT,
    r2_file     TEXT NOT NULL,
    r2_size     INTEGER,
    r2_sha256   TEXT,
    sealed_at   TEXT,
    PRIMARY KEY (snapshot_id, lane, sample_id)
);
"""

_local = threading.local()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def utcnow() -> str:
    return _now()


def _configure(conn: sqlite3.Connection) -> None:
    """Cross-process settings. busy_timeout must be set before WAL toggling:
    switching journal mode needs a brief exclusive lock, and without a busy
    handler a second instance starting at the same moment gets SQLITE_BUSY."""
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    for attempt in range(50):
        try:
            # Idempotent once any instance has converted the DB to WAL.
            conn.execute("PRAGMA journal_mode=WAL")
            break
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt == 49:
                raise
            time.sleep(0.1)
    conn.execute("PRAGMA synchronous=NORMAL")


def get_conn() -> sqlite3.Connection:
    """One connection per worker thread, configured for cross-process use."""
    conn = getattr(_local, "conn", None)
    if conn is None:
        conn = sqlite3.connect(
            config.DB_PATH, timeout=30, isolation_level=None, check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        _configure(conn)
        _local.conn = conn
    return conn


@contextmanager
def tx() -> Iterator[sqlite3.Connection]:
    conn = get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def init_db() -> None:
    conn = get_conn()
    conn.executescript(_SCHEMA)
    # Best-effort: give pre-sealing published lanes an explicit NULL evidence
    # row. They remain visible but can never be mistaken for verified data.
    try:
        conn.execute(
            """INSERT OR IGNORE INTO lane_manifests
                   (lane, sample_id, r1_file, r1_size, r1_sha256,
                    r2_file, r2_size, r2_sha256, sealed_at)
               SELECT c.lane, c.sample_id, c.r1_file, 0, NULL,
                      c.r2_file, 0, NULL, NULL
               FROM lane_counts c
               JOIN lane_publishes p ON p.lane = c.lane
               WHERE NOT EXISTS (
                   SELECT 1 FROM lane_manifests m
                   WHERE m.lane = c.lane AND m.sample_id = c.sample_id)"""
        )
    except sqlite3.Error:  # pragma: no cover - defensive, e.g. read-only fs
        pass


# ---------- job helpers ----------

def create_job(row: dict[str, Any], samples: list[dict[str, str]]) -> None:
    with tx() as conn:
        conn.execute(
            """INSERT INTO jobs (job_id, lane, status, input_hash, barcode_table,
                                  r1_incoming, r2_incoming, error, instance_id,
                                  created_at, updated_at, heartbeat_at)
               VALUES (:job_id,:lane,'queued',:input_hash,:barcode_table,
                       :r1_incoming,:r2_incoming,NULL,NULL,
                       :created_at,:created_at,NULL)""",
            row,
        )
        conn.executemany(
            """INSERT INTO job_samples (job_id, sample_id, barcode_r1, barcode_r2)
               VALUES (:job_id,:sample_id,:barcode_r1,:barcode_r2)""",
            [
                {
                    "job_id": row["job_id"],
                    "sample_id": s["sample_id"],
                    "barcode_r1": s["barcode_r1"],
                    "barcode_r2": s["barcode_r2"],
                }
                for s in samples
            ],
        )


def get_job_by_lane(lane: str) -> sqlite3.Row | None:
    return get_conn().execute("SELECT * FROM jobs WHERE lane=?", (lane,)).fetchone()


def get_job(job_id: str) -> sqlite3.Row | None:
    return get_conn().execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()


def list_jobs() -> list[sqlite3.Row]:
    return get_conn().execute(
        "SELECT * FROM jobs ORDER BY created_at").fetchall()


def get_samples(job_id: str) -> list[sqlite3.Row]:
    return get_conn().execute(
        "SELECT * FROM job_samples WHERE job_id=? ORDER BY rowid", (job_id,)
    ).fetchall()


def claim_next_job(instance_id: str) -> sqlite3.Row | None:
    """Atomically take one queued job. Returns None when the queue is empty."""
    with tx() as conn:
        row = conn.execute(
            "SELECT * FROM jobs WHERE status='queued' ORDER BY created_at LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        conn.execute(
            """UPDATE jobs SET status='processing', instance_id=?,
                               updated_at=?, heartbeat_at=?
               WHERE job_id=?""",
            (instance_id, _now(), _now(), row["job_id"]),
        )
    return get_job(row["job_id"])


def heartbeat(job_id: str, instance_id: str) -> None:
    get_conn().execute(
        "UPDATE jobs SET heartbeat_at=? WHERE job_id=? AND instance_id=?",
        (_now(), job_id, instance_id),
    )


def mark_failed(job_id: str, instance_id: str, error: str) -> None:
    with tx() as conn:
        # Never demote a lane that was durably published: a late error after
        # the publish commit (e.g. cleanup) must not make it look failed.
        conn.execute(
            """UPDATE jobs SET status='failed', error=?, updated_at=?
               WHERE job_id=? AND instance_id=? AND status != 'published'""",
            (error[:2000], _now(), job_id, instance_id),
        )


def requeue_job(job_id: str) -> bool:
    with tx() as conn:
        row = conn.execute(
            "SELECT status FROM jobs WHERE job_id=?", (job_id,)
        ).fetchone()
        if row is None:
            return False
        if row["status"] != "failed":
            return False
        conn.execute(
            """UPDATE jobs SET status='queued', error=NULL, instance_id=NULL,
                               updated_at=?, heartbeat_at=NULL WHERE job_id=?""",
            (_now(), job_id),
        )
    return True


def publish_job(job_id: str, lane: str, counts: list[dict[str, Any]],
                manifests: list[dict[str, Any]]) -> None:
    """Publish metadata atomically; files are moved in beforehand.

    The manifest rows are the sealed SHA-256 evidence for the exact bytes
    that were just moved into the published tree. They are inserted in the
    same transaction as the lane row, so a lane is never visible without
    digests unless it predates sealing (``manifest is NULL`` for such rows).
    """
    with tx() as conn:
        next_seq = conn.execute(
            "SELECT COALESCE(MAX(seq),0)+1 AS s FROM lane_publishes").fetchone()["s"]
        conn.execute(
            """INSERT INTO lane_publishes (lane, job_id, published_at, seq)
               VALUES (?,?,?,?)""",
            (lane, job_id, _now(), next_seq),
        )
        conn.executemany(
            """INSERT INTO lane_counts (lane, sample_id, r1_file, r2_file, read_pairs)
               VALUES (:lane,:sample_id,:r1_file,:r2_file,:read_pairs)""",
            [{**c, "lane": lane} for c in counts],
        )
        sealed_at = _now()
        conn.executemany(
            """INSERT INTO lane_manifests
                   (lane, sample_id, r1_file, r1_size, r1_sha256,
                    r2_file, r2_size, r2_sha256, sealed_at)
               VALUES (:lane,:sample_id,:r1_file,:r1_size,:r1_sha256,
                       :r2_file,:r2_size,:r2_sha256,:sealed_at)""",
            [{**m, "lane": lane, "sealed_at": sealed_at} for m in manifests],
        )
        conn.execute(
            "UPDATE jobs SET status='published', updated_at=? WHERE job_id=?",
            (_now(), job_id),
        )


# ---------- published lanes / snapshots ----------

def published_lanes() -> list[sqlite3.Row]:
    return get_conn().execute(
        "SELECT * FROM lane_publishes ORDER BY seq").fetchall()


def lane_counts(lane: str) -> list[sqlite3.Row]:
    return get_conn().execute(
        "SELECT * FROM lane_counts WHERE lane=? ORDER BY sample_id", (lane,)
    ).fetchall()


def create_snapshot(snapshot_id: str, lanes: list[str]) -> dict[str, Any]:
    """Freeze the published lane list *and* its sealed digest evidence.

    The manifest rows are copied into the snapshot inside one transaction, so
    the evidence a snapshot reports can never be replaced by a later re-seal
    of the live lane row.
    """
    now = _now()
    with tx() as conn:
        rows = conn.execute(
            "SELECT lane, seq FROM lane_publishes WHERE lane IN (%s) ORDER BY seq"
            % ",".join("?" * len(lanes)),
            lanes,
        ).fetchall()
        fixed = [r["lane"] for r in rows]
        conn.execute(
            "INSERT INTO snapshots (snapshot_id, created_at, lanes_json) VALUES (?,?,?)",
            (snapshot_id, now, json.dumps(fixed)),
        )
        conn.executemany(
            "INSERT INTO snapshot_lanes (snapshot_id, lane, seq) VALUES (?,?,?)",
            [(snapshot_id, r["lane"], r["seq"]) for r in rows],
        )
        for r in rows:
            manifests = conn.execute(
                "SELECT * FROM lane_manifests WHERE lane=? ORDER BY sample_id",
                (r["lane"],),
            ).fetchall()
            conn.executemany(
                """INSERT INTO snapshot_manifests
                       (snapshot_id, lane, sample_id, seq,
                        r1_file, r1_size, r1_sha256,
                        r2_file, r2_size, r2_sha256, sealed_at)
                   VALUES (:snapshot_id,:lane,:sample_id,:seq,
                           :r1_file,:r1_size,:r1_sha256,
                           :r2_file,:r2_size,:r2_sha256,:sealed_at)""",
                [
                    {
                        "snapshot_id": snapshot_id,
                        "lane": r["lane"],
                        "sample_id": m["sample_id"],
                        "seq": r["seq"],
                        "r1_file": m["r1_file"],
                        "r1_size": m["r1_size"],
                        "r1_sha256": m["r1_sha256"],
                        "r2_file": m["r2_file"],
                        "r2_size": m["r2_size"],
                        "r2_sha256": m["r2_sha256"],
                        "sealed_at": m["sealed_at"],
                    }
                    for m in manifests
                ],
            )
    return {"snapshot_id": snapshot_id, "created_at": now, "lanes": fixed}


def get_snapshot(snapshot_id: str) -> dict[str, Any] | None:
    row = get_conn().execute(
        "SELECT * FROM snapshots WHERE snapshot_id=?", (snapshot_id,)
    ).fetchone()
    if row is None:
        return None
    return {
        "snapshot_id": row["snapshot_id"],
        "created_at": row["created_at"],
        "lanes": json.loads(row["lanes_json"]),
    }


def list_snapshots() -> list[dict[str, Any]]:
    return [
        {
            "snapshot_id": r["snapshot_id"],
            "created_at": r["created_at"],
            "lanes": json.loads(r["lanes_json"]),
        }
        for r in get_conn().execute(
            "SELECT * FROM snapshots ORDER BY created_at").fetchall()
    ]


def snapshot_manifests(snapshot_id: str) -> list[sqlite3.Row]:
    return get_conn().execute(
        "SELECT * FROM snapshot_manifests WHERE snapshot_id=? "
        "ORDER BY seq, sample_id",
        (snapshot_id,),
    ).fetchall()
