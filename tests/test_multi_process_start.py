"""Two processes opening a fresh shared DB at the same moment."""
import multiprocessing
import os
import sys
from pathlib import Path


def _init_worker(data_dir: str, barrier, out):
    os.environ["DEMUX_DATA_DIR"] = data_dir
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    try:
        from app import config, db
        barrier.wait(timeout=20)
        db.init_db()
        # Touch the DB through a transaction the way a request would.
        with db.tx() as conn:
            conn.execute(
                "INSERT INTO snapshots (snapshot_id, created_at, lanes_json) "
                "VALUES (?,?,?)", (f"s-{os.getpid()}", "now", "[]"))
        out.put(("ok", os.getpid()))
    except Exception as exc:  # pragma: no cover - failure path
        out.put(("error", f"{type(exc).__name__}: {exc}"))


def test_concurrent_first_start_does_not_fail_on_wal_switch(tmp_path):
    data_dir = str(tmp_path / "shared")
    Path(data_dir).mkdir()
    ctx = multiprocessing.get_context("spawn")
    barrier = ctx.Barrier(4)
    out = ctx.Queue()
    procs = [
        ctx.Process(target=_init_worker, args=(data_dir, barrier, out))
        for _ in range(4)
    ]
    for p in procs:
        p.start()
    results = [out.get(timeout=40) for _ in procs]
    for p in procs:
        p.join(timeout=20)
    errors = [r for r in results if r[0] != "ok"]
    assert not errors, errors
    assert len(results) == 4
