"""Compressed, checksummed storage of raw API response bodies.

Layout: ``<data_dir>/raw/<run_id>/<request_id>.json.gz``. Each file is a gzip
JSON envelope::

    {"request_id", "run_id", "path", "params", "url", "status",
     "requested_at", "received_at", "headers", "sha256", "parser_version",
     "body": "<exact response text>"}

``sha256`` is the digest of the exact response bytes, so a file can be
verified independently of the database. Files are written atomically
(temporary file -> fsync -> ``os.replace``) *before* the corresponding
database transaction commits; a crash in between leaves an "orphan" file that
:func:`find_orphans` reports and that is safe to delete (the work is redone).
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import tempfile
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

from .http import ResponseRecord
from .timeutil import iso


def body_sha256(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


class RawStore:
    """Writes raw response envelopes under ``root`` (unless retention is 'none')."""

    def __init__(self, root: Path, retention: str = "keep") -> None:
        self.root = root
        self.retention = retention

    def write(self, run_id: str, record: ResponseRecord, parser_version: str) -> str | None:
        """Atomically persist ``record``; returns the path relative to ``root``
        (or None if raw retention is disabled)."""
        if self.retention == "none":
            return None
        rel = Path(run_id) / f"{record.request_id}.json.gz"
        target = self.root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        envelope = {
            "request_id": record.request_id,
            "run_id": run_id,
            "path": record.path,
            "params": record.params,
            "url": record.url,
            "status": record.status,
            "requested_at": iso(record.requested_at),
            "received_at": iso(record.received_at),
            "attempts": record.attempts,
            "headers": record.headers,
            "sha256": body_sha256(record.body),
            "parser_version": parser_version,
            "body": record.body.decode("utf-8", "surrogateescape"),
        }
        data = gzip.compress(json.dumps(envelope).encode("utf-8"))
        fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=target.parent)
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, target)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise
        _fsync_dir(target.parent)
        return rel.as_posix()

    def iter_files(self) -> Iterator[Path]:
        if self.root.exists():
            yield from self.root.rglob("*.json.gz")

    def temp_files(self) -> list[Path]:
        """Leftover temporary files from interrupted writes."""
        return list(self.root.rglob(".tmp-*")) if self.root.exists() else []


def read_envelope(path: Path) -> dict[str, Any]:
    """Load a raw envelope and verify its checksum.

    Raises:
        ValueError: if the stored body does not match its recorded sha256.
    """
    env: dict[str, Any] = json.loads(gzip.decompress(path.read_bytes()))
    body = env["body"].encode("utf-8", "surrogateescape")
    if body_sha256(body) != env["sha256"]:
        raise ValueError(f"Checksum mismatch for {path}")
    return env


def find_orphans(store: RawStore, known_paths: Iterable[str]) -> list[Path]:
    """Raw files with no matching ``requests`` row (crash between write and commit)."""
    known = set(known_paths)
    return [p for p in store.iter_files() if p.relative_to(store.root).as_posix() not in known]


def _fsync_dir(path: Path) -> None:
    # Directory fsync makes the rename durable on POSIX; not supported on Windows.
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)
