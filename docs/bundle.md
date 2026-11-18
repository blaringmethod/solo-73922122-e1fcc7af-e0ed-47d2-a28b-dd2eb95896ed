# Snapshot sample bundles (pair-end ZIP handoff)

This document specifies the handoff artifact proven unchanged by SHA-256:
`GET /snapshots/{snapshot_id}/samples/{sample_id}/bundle.zip`.

It exists so a sequencing engineer can hand a **paired-end read package**
to a comparison job together with evidence that the files have not changed
since publication.

## 1. When evidence is sealed

Submitting a lane produces per-bucket files (`<sample>.r1.fastq.gz`,
`<sample>.r2.fastq.gz`, plus the `__quarantine__` bucket). Immediately
before publish, each file is moved into `published/lanes/<lane>/` and its
**size and SHA-256** are computed from the moved file. Those values are
inserted into `lane_manifests` **inside the same SQLite transaction** that
inserts `lane_publishes`, `lane_counts` and flips the job to `published`.

Consequences:

* A lane can never be visible (`GET /lanes`, snapshots, downloads) without
  its sealed digests. If sealing cannot be completed the job fails and the
  lane is not published.
* `GET /lanes/{lane}` exposes the evidence:

  ```json
  {
    "sealed": true,
    "sealed_manifests": [
      {"sample_id": "s1",
       "r1": {"file": "s1.r1.fastq.gz", "size": 1234, "sha256": "…"},
       "r2": {"file": "s1.r2.fastq.gz", "size": 1234, "sha256": "…"},
       "sealed_at": "…"}
    ]
  }
  ```

* Lanes published by a database created **before** this feature have no
  digests. They remain available through the old per-lane/snapshot download
  endpoints but are **rejected** by the bundle API (`409`).

## 2. Snapshots freeze old evidence

`POST /snapshots` copies the current sealed rows into `snapshot_manifests`
in one transaction. Later re-sealing, lane deletion or tampering with the
live `lane_manifests` row cannot change what the snapshot reports: a
mismatch between the snapshot copy and the live row is itself an error
(`500`), never silently resolved in favour of newer data.

## 3. Bundle contents

For snapshot `S` and sample `p`:

| Entry | Meaning |
|---|---|
| `<p>.r1.fastq.gz` | Concatenation of the per-lane R1 files, snapshot order |
| `<p>.r2.fastq.gz` | Concatenation of the per-lane R2 files, snapshot order |
| `manifest.json` | Snapshot id, lane order, per-lane/per-mate size + SHA-256, read counts, member digests |
| `README.txt` | Human-readable summary and the lane order |

* **Participating lanes**: only lanes frozen in `S` whose count table
  contains `p`. Lanes without the sample are skipped, not treated as empty.
  A sample present in *no* snapshot lane returns `404`; an empty bundle is
  never produced.
* **Order** is the snapshot's frozen creation order (`lane_publishes.seq`
  at snapshot time), not alphabetical and not the order of later publishes.
  New lanes published after the snapshot are never included, even if they
  contain the sample.
* Each mate is a valid **multi-member gzip stream** (RFC 1952 §2.2): plain
  `gzip`, `zcat`, `python -m gzip` and `samtools` read it as one FASTQ
  stream. ZIP entries are stored uncompressed so the sealed bytes appear
  verbatim inside the archive.

## 4. Verification and all-or-nothing rules

The server:

1. Resolves the frozen plan (snapshot lanes, sample presence, evidence).
2. For each participating lane and mate, requires a sealed digest; a
   missing row or NULL digest → `409` naming the lane.
3. Streams each source file **once**: every chunk is hashed and the same
   chunk is written into the ZIP member. The computed digest and length must
   equal the sealed values, otherwise `409` with "digest mismatch".
4. Re-opens the finished ZIP and re-hashes each member stream in
   sealed-size slices, cross-checking every lane. It also validates
   `manifest.json` against the frozen plan.
5. Only then returns the archive. Errors happen before any body byte, so a
   client can never receive a truncated or partial ZIP. On error no
   temporary archive is kept.

The current on-disk file is **never** hashed and then trusted: the only
accepted reference is the digest sealed at publication (cross-checked with
the snapshot copy). Successful responses carry:

* `X-Bundle-SHA256` — SHA-256 of the ZIP exactly as sent
* `X-Bundle-Size` — its length
* `X-Bundle-Lanes` — comma-separated participating lanes in order
* `X-Snapshot-Id`

## 5. Errors

| Status | Cause |
|---|---|
| `400` | Bad `sample_id` (syntax or reserved `__` prefix) |
| `404` | Snapshot unknown, or sample absent from every frozen lane |
| `409` | Missing mate/file, size or digest mismatch, or a participating lane with no sealed evidence |
| `500` | Snapshot evidence disagrees with the live evidence, or verification of the finished archive failed |

All non-2xx responses are JSON `{"detail": "..."}` and carry no ZIP body.

## 6. Verifying a downloaded package

The repository ships `scripts/verify_bundle.py` (standard library only):

```bash
curl -sS -OJ -D headers.txt \
  http://localhost:8001/snapshots/<SNAP>/samples/sampleA/bundle.zip

# X-Bundle-SHA256 from the headers proves the ZIP arrived unchanged.
python3 scripts/verify_bundle.py \
  sampleA.snapshot-<SNAP>.zip \
  "$(grep -i '^x-bundle-sha256:' headers.txt | tr -d '\r' | awk '{print $2}')"
```

It exits non-zero if any of these fail:

* ZIP CRC test on every entry;
* archive SHA-256 differs from `X-Bundle-SHA256`;
* any per-lane slice of a mate stream differs from
  `manifest.json → lanes_detail[i][mate].sha256` / `size`;
* trailing bytes after the last lane, a short slice, or a lane order that
  disagrees with `manifest.json → lanes`;
* either mate does not open as a single gzip stream with exactly
  `sum(read_pairs) * 4` FASTQ lines.

## 7. Behaviour under the deployment limits

* Two instances on one shared volume both serve bundles: the API is
  read-only with respect to published data and uses per-thread SQLite
  connections in WAL mode. `bundle-tmp/<uuid>.zip` filenames are unique per
  request, so simultaneous bundles on either instance cannot collide.
* Memory stays flat: the archive is built in
  `<data>/bundle-tmp/<uuid>.zip` on the same filesystem, then streamed in
  1 MiB chunks. Nothing is buffered whole, and the 128 MiB cap is not
  affected by lane size. Building a bundle runs in the request handler, not
  the single job-worker thread, so uploads, queries, downloads and a lane
  being processed on the other instance are all unaffected.
* Temporary archives are removed after the response (also on error);
  startup recovery removes stale `bundle-tmp/*.zip` files older than one
  hour, in case an instance died mid-response.
