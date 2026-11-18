"""Snapshot-scoped R1/R2 bundle: verify sealed bytes, then pack one ZIP.

Rules implemented here (see /docs):

* Only lanes frozen into the snapshot are considered, in the snapshot's
  fixed creation order. Lanes published later are never consulted.
* Only lanes whose bucket table contains the requested sample contribute.
  A sample absent from every snapshot lane is an error (no empty bundle).
* Both mates are required. Every participating lane must have sealed
  evidence (the snapshot's frozen copy, or the live row when no copy
  exists) with non-NULL digests. A missing mate, a missing file, a
  size/digest mismatch, or a legacy lane without sealed evidence aborts the
  whole request. Nothing partial is emitted.
* The bytes that are hashed are the very bytes written into the ZIP member
  streams: each file is read once, fed to SHA-256, and the same chunk is
  handed to the ZIP writer. After all members are staged, the finished ZIP
  is re-opened and each member is hashed again and compared with the sealed
  manifest, so the archive the client downloads was proven intact end to end.
* The current on-disk digest is *never* accepted as evidence: it is only
  compared against the digest sealed when the lane was published (and, if
  available, the copy frozen into the snapshot row).
"""
from __future__ import annotations

import hashlib
import json
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from . import config, db, processing

_IO_CHUNK = 1024 * 1024
_ZIP_DATE = (1980, 1, 1, 0, 0, 0)
_SAMPLE_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")


class PackagingError(Exception):
    """Any condition that must abort the whole bundle (no partial output)."""

    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


@dataclass(frozen=True)
class BundleMember:
    """One lane's contribution to one mate stream, backed by sealed evidence."""
    lane: str
    lane_seq: int
    sample_id: str
    mate: str
    path: Path
    size: int
    sha256: str
    read_pairs: int
    sealed_at: str


@dataclass
class BundlePlan:
    snapshot_id: str
    sample_id: str
    lanes: list[str]          # every lane in snapshot order
    participants: list[str]   # lanes that actually contain the sample
    manifests: list[dict]     # canonical JSON-ready evidence rows
    members: dict[str, list[BundleMember]]  # "r1"/"r2" -> ordered members


def _safe_published_path(path_str: str) -> Path:
    """Resolve a stored path and require it inside the published tree."""
    root = config.PUBLISHED_DIR.resolve()
    try:
        p = Path(path_str).resolve()
    except OSError as exc:
        raise PackagingError(500, f"unresolvable published path: {exc}") from exc
    if root not in p.parents:
        raise PackagingError(500, "published path escapes the data directory")
    return p


def _validate_sample_id(sample_id: str) -> None:
    if not _SAMPLE_RE.match(sample_id or ""):
        raise PackagingError(400, "sample_id must match [A-Za-z0-9_.-]{1,64}")
    if sample_id.startswith("__"):
        raise PackagingError(400, "sample_id uses a reserved prefix")


def plan_bundle(snapshot_id: str, sample_id: str) -> BundlePlan:
    """Resolve and verify sealed evidence. Pure reads; emits no output."""
    _validate_sample_id(sample_id)
    snap = db.get_snapshot(snapshot_id)
    if snap is None:
        raise PackagingError(404, "snapshot not found")

    conn = db.get_conn()
    # Freeze order and seq come from the snapshot tables, not from the live
    # table: a later publish must not be able to renumber an old snapshot.
    frozen_lanes = conn.execute(
        "SELECT lane, seq FROM snapshot_lanes WHERE snapshot_id=? ORDER BY seq",
        (snapshot_id,)).fetchall()
    seq_by_lane = {r["lane"]: r["seq"] for r in frozen_lanes}
    lanes: list[str] = [r["lane"] for r in frozen_lanes]
    if not lanes:
        # A snapshot created before snapshot_lanes was populated (or an empty
        # snapshot) still resolves through its stored JSON list.
        lanes = list(snap["lanes"])
    snap_evidence = {
        (r["lane"], r["sample_id"]): r for r in db.snapshot_manifests(snapshot_id)
    }

    participants: list[str] = []
    manifests: list[dict] = []
    members: dict[str, list[BundleMember]] = {"r1": [], "r2": []}

    for lane in lanes:
        count_row = conn.execute(
            "SELECT * FROM lane_counts WHERE lane=? AND sample_id=?",
            (lane, sample_id)).fetchone()
        if count_row is None:
            continue  # sample is genuinely not part of this lane
        participants.append(lane)

        sealed = conn.execute(
            "SELECT * FROM lane_manifests WHERE lane=? AND sample_id=?",
            (lane, sample_id)).fetchone()
        frozen = snap_evidence.get((lane, sample_id))

        def _has_evidence(row) -> bool:
            return row is not None and row["r1_sha256"] is not None \
                and row["r2_sha256"] is not None

        # The snapshot copy is the authoritative old evidence. The live row
        # is only a cross-check (never a substitute for old evidence).
        if _has_evidence(frozen):
            evidence = frozen
            if sealed is not None:
                # Any non-NULL field must agree; a NULL-only live row is a
                # legacy placeholder, not contradicting evidence.
                for col in ("r1_sha256", "r2_sha256", "r1_size", "r2_size"):
                    if sealed[col] is not None and sealed[col] != frozen[col]:
                        raise PackagingError(
                            500,
                            f"lane {lane!r} sealed evidence disagrees with "
                            f"the snapshot copy for sample {sample_id!r}")
        elif _has_evidence(sealed):
            evidence = sealed
        else:
            raise PackagingError(
                409,
                f"lane {lane!r} has no sealed digest evidence for sample "
                f"{sample_id!r}; it cannot be included in a verified bundle")

        lane_seq = seq_by_lane.get(lane, 0)
        for mate in ("r1", "r2"):
            path = _safe_published_path(evidence[f"{mate}_file"])
            if not path.is_file():
                raise PackagingError(
                    409,
                    f"lane {lane!r} is missing its {mate.upper()} file for "
                    f"sample {sample_id!r}")
            members[mate].append(
                BundleMember(
                    lane=lane, lane_seq=lane_seq, sample_id=sample_id,
                    mate=mate, path=path,
                    size=evidence[f"{mate}_size"],
                    sha256=evidence[f"{mate}_sha256"],
                    read_pairs=count_row["read_pairs"],
                    sealed_at=evidence["sealed_at"],
                )
            )
        manifests.append({
            "lane": lane,
            "lane_seq": lane_seq,
            "sample_id": sample_id,
            "read_pairs": count_row["read_pairs"],
            "r1": {
                "file": Path(evidence["r1_file"]).name,
                "size": evidence["r1_size"],
                "sha256": evidence["r1_sha256"],
            },
            "r2": {
                "file": Path(evidence["r2_file"]).name,
                "size": evidence["r2_size"],
                "sha256": evidence["r2_sha256"],
            },
            "sealed_at": evidence["sealed_at"],
        })

    if not participants:
        raise PackagingError(
            404, f"sample {sample_id!r} is not present in snapshot {snapshot_id!r}")

    return BundlePlan(
        snapshot_id=snapshot_id,
        sample_id=sample_id,
        lanes=lanes,
        participants=participants,
        manifests=manifests,
        members=members,
    )


def _member_name(plan: BundlePlan, mate: str) -> str:
    return f"{plan.sample_id}.{mate}.fastq.gz"


def readme_text(plan: BundlePlan) -> bytes:
    lines = [
        "Snapshot-scoped paired-end read bundle",
        "",
        f"snapshot_id: {plan.snapshot_id}",
        f"sample_id:   {plan.sample_id}",
        f"lanes ({len(plan.participants)}, frozen order):",
    ]
    lines += [f"  {i + 1}. {lane}" for i, lane in enumerate(plan.participants)]
    lines += [
        "",
        f"{_member_name(plan, 'r1')} and {_member_name(plan, 'r2')} are",
        "concatenations of the per-lane gzip members in the order above.",
        "gzip member concatenation is a valid single gzip stream (RFC 1952),",
        "so each file opens as one FASTQ stream.",
        "",
        "manifest.json lists, per lane and mate, the size and SHA-256 sealed",
        "at lane publication. Files were verified against those digests",
        "before and after being written into this zip.",
        "",
        "NO_PARTIAL_OUTPUT: verification failures abort the whole archive.",
    ]
    return ("\n".join(lines) + "\n").encode()


def manifest_json(plan: BundlePlan, member_digests: dict) -> bytes:
    payload = {
        "snapshot_id": plan.snapshot_id,
        "sample_id": plan.sample_id,
        "lanes": plan.participants,
        "snapshot_lanes": plan.lanes,
        "created_at": db.get_snapshot(plan.snapshot_id)["created_at"],
        "members": {
            _member_name(plan, "r1"): member_digests.get(
                _member_name(plan, "r1")),
            _member_name(plan, "r2"): member_digests.get(
                _member_name(plan, "r2")),
        },
        "lanes_detail": plan.manifests,
    }
    return (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode()


def _zipinfo(name: str, size: int) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo(name, date_time=_ZIP_DATE)
    info.compress_type = zipfile.ZIP_STORED
    info.external_attr = 0o644 << 16
    info.file_size = size
    return info


def _copy_hashing(src: Path, out: BinaryIO,
                  whole: "hashlib._Hash") -> tuple[int, str]:
    """Read ``src`` once; hash and write each chunk as it arrives."""
    hasher = hashlib.sha256()
    size = 0
    try:
        with open(src, "rb") as fh:
            while True:
                chunk = fh.read(_IO_CHUNK)
                if not chunk:
                    break
                hasher.update(chunk)
                whole.update(chunk)
                out.write(chunk)
                size += len(chunk)
    except OSError as exc:
        raise PackagingError(
            409, f"cannot read published file {src.name}: {exc}") from exc
    return size, hasher.hexdigest()


def write_bundle(plan: BundlePlan, out: Path) -> dict:
    """Stream a ZIP to ``out``; return digests of its members and the archive.

    Every source file is opened once. The chunk read is the chunk hashed and
    the chunk written into the archive, so the verified bytes and delivered
    bytes cannot diverge. After sealing, the archive is reopened and each
    member is hashed again as an independent cross-check.
    """
    try:
        return _write_bundle(plan, out)
    except PackagingError:
        raise
    except (zipfile.BadZipFile, ValueError, OSError) as exc:
        # zipfile raises ValueError when a streamed member's length disagrees
        # with its declared size (e.g. a concurrently truncated source file).
        # Normalise this into the all-or-nothing error contract.
        raise PackagingError(
            409, f"could not build a verified bundle: {exc}") from exc


def _write_bundle(plan: BundlePlan, out: Path) -> dict:
    member_digests: dict[str, dict] = {}
    with zipfile.ZipFile(out, "w", allowZip64=True) as zf:
        for mate in ("r1", "r2"):
            name = _member_name(plan, mate)
            total = sum(m.size for m in plan.members[mate])
            whole = hashlib.sha256()
            written = 0
            # ZIP_STORED: the sealed gzip bytes are wrapped verbatim; the
            # sealed total size is known, so no data descriptor is needed.
            with zf.open(_zipinfo(name, total), "w") as member:
                for item in plan.members[mate]:
                    size, digest = _copy_hashing(item.path, member, whole)
                    if size != item.size or digest != item.sha256:
                        raise PackagingError(
                            409,
                            f"lane {item.lane!r} {mate.upper()} file does "
                            "not match its sealed digest (digest mismatch)")
                    written += size
            if written != total:  # pragma: no cover - guarded above
                raise PackagingError(500, "internal size accounting mismatch")
            member_digests[name] = {
                "size": written, "sha256_hex": whole.hexdigest()}

        # Metadata files last, built only from the frozen plan/evidence.
        metadata = [
            ("manifest.json", manifest_json(plan, member_digests)),
            ("README.txt", readme_text(plan)),
        ]
        for name, payload in metadata:
            zf.writestr(_zipinfo(name, len(payload)), payload)

    verify_archive(out, plan)
    archive_size, archive_sha = processing.hash_file(out)
    return {
        "members": member_digests,
        "archive": {"size": archive_size, "sha256_hex": archive_sha},
    }


def verify_archive(path: Path, plan: BundlePlan) -> None:
    """Independent post-check: every delivered stream equals its sealed bytes.

    Re-reads the finished ZIP (not the source files) and walks each member
    stream in sealed-size slices, hashing every slice. A lane whose slice
    does not match its sealed digest, a short/long member, or missing
    metadata all abort the request. Nothing has been streamed to the client
    yet: the archive only exists in a private temp directory.
    """
    with zipfile.ZipFile(path, "r") as zf:
        names = set(zf.namelist())
        if "manifest.json" not in names or "README.txt" not in names:
            raise PackagingError(500, "archive is missing its manifest")
        for mate in ("r1", "r2"):
            name = _member_name(plan, mate)
            if name not in names:
                raise PackagingError(500, "archive is missing a mate member")
            items = plan.members[mate]
            with zf.open(name, "r") as fh:
                for item in items:
                    remaining = item.size
                    lane_hasher = hashlib.sha256()
                    while remaining:
                        chunk = fh.read(min(_IO_CHUNK, remaining))
                        if not chunk:
                            raise PackagingError(
                                500, "archive truncated while verifying")
                        lane_hasher.update(chunk)
                        remaining -= len(chunk)
                    if lane_hasher.hexdigest() != item.sha256:
                        raise PackagingError(
                            409,
                            f"lane {item.lane!r} {item.mate.upper()} digest "
                            "mismatch in the finished archive")
                if fh.read(1):
                    raise PackagingError(
                        500, "archive member longer than its sealed evidence")

        manifest = json.loads(zf.read("manifest.json"))
        if manifest["snapshot_id"] != plan.snapshot_id:
            raise PackagingError(500, "manifest snapshot id mismatch")
        if manifest["sample_id"] != plan.sample_id:
            raise PackagingError(500, "manifest sample id mismatch")
        if manifest["lanes"] != plan.participants:
            raise PackagingError(500, "manifest lane list mismatch")
