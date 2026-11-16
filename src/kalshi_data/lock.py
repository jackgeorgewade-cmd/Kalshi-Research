"""Single-writer lock for a data directory.

Only one process may write to a data directory at a time. The lock is a file
created with ``O_EXCL`` containing the owner's PID and host. A lock whose PID
is no longer running on this host is considered stale and is replaced.
Read-only commands (status, coverage, export, runs) do not take the lock;
SQLite WAL mode lets them read while a writer is active.
"""

from __future__ import annotations

import json
import os
import socket
from pathlib import Path
from types import TracebackType

from .errors import LockError
from .timeutil import iso, utcnow


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


class WriterLock:
    """Context manager acquiring the exclusive writer lock."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._held = False

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        info = {"pid": os.getpid(), "host": socket.gethostname(), "since": iso(utcnow())}
        for _ in range(2):
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                if self._clear_if_stale():
                    continue
                raise LockError(
                    f"Data directory is locked by another writer ({self._describe()}). "
                    f"If no other kalshi-data process is running, delete {self.path}."
                ) from None
            with os.fdopen(fd, "w") as fh:
                json.dump(info, fh)
            self._held = True
            return
        raise LockError(f"Could not acquire lock {self.path}")

    def _describe(self) -> str:
        try:
            return self.path.read_text()
        except OSError:
            return "unknown owner"

    def _clear_if_stale(self) -> bool:
        try:
            info = json.loads(self.path.read_text())
        except (OSError, ValueError):
            return False
        if info.get("host") == socket.gethostname() and not _pid_alive(int(info.get("pid", -1))):
            self.path.unlink(missing_ok=True)
            return True
        return False

    def release(self) -> None:
        if self._held:
            self.path.unlink(missing_ok=True)
            self._held = False

    def __enter__(self) -> WriterLock:
        self.acquire()
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None,
                 tb: TracebackType | None) -> None:
        self.release()
