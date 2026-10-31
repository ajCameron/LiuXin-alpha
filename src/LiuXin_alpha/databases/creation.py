"""
Publish complete SQLite databases without implicitly replacing caller data.

Creation uses an exclusive hard link from a private sibling workspace. Replacement
is a separate offline operation: the caller must stop all users of the database,
choose a new backup path, and retain that backup for recovery.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import stat
from collections.abc import Callable
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory


# Todo: These could be public and - probably - in utils
def _path(value: str | os.PathLike[str]) -> Path:
    """Resolve the parent while retaining the final component for collision checks."""
    path = Path(value).expanduser()
    if str(path) == ":memory:":
        raise ValueError("Database file creation requires a filesystem path.")
    return path.parent.resolve() / path.name


def _require_absent(path: Path) -> None:
    """Reject every existing entry, including a dangling symlink."""
    if os.path.lexists(path):
        raise FileExistsError(f"Database destination already exists: {path}")


def _require_no_sidecars(path: Path) -> None:
    """Refuse offline replacement while journals or WAL coordination files remain."""
    for suffix in ("-wal", "-shm", "-journal"):
        if os.path.lexists(str(path) + suffix):
            raise RuntimeError(
                f"Close all database users and checkpoint/recover SQLite before replacement: {path}{suffix}"
            )


def _identity(path: Path) -> tuple[int, int, int, int, int]:
    """Identify a regular file and detect observed changes during preparation."""
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError(f"Database replacement requires a regular file: {path}")
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def _sync(path: Path) -> None:
    """Flush completed file contents before publication."""
    with path.open("rb") as stream:
        os.fsync(stream.fileno())


def create_sqlite_file(
    path: str | os.PathLike[str], build: Callable[[Path], None]
) -> None:
    """
    Build privately and publish only if the target is still absent.

    The builder must close its connections before returning. A failed build or
    publication removes only private scratch; a competing target is never opened
    or deleted. Filesystems without hard-link support fail rather than falling back
    to a check followed by an overwriting rename.

    :param path:
    :param build:

    :return:
    """
    target = _path(path)
    _require_absent(target)
    _require_no_sidecars(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=".liuxin-create-", dir=target.parent) as scratch:
        candidate = Path(scratch) / "database.sqlite"
        build(candidate)
        _require_no_sidecars(candidate)
        _sync(candidate)
        _require_no_sidecars(target)
        os.link(candidate, target)


def replace_sqlite_file(
    path: str | os.PathLike[str],
    *,
    backup_path: str | os.PathLike[str],
    build: Callable[[Path], None],
) -> Path:
    """Replace an offline database only after preparing a new schema and backup.

    The backup is a verified SQLite snapshot published create-only. Preparation,
    backup or final-rename failure leaves the original target in place. Once the
    backup is published it is retained even if replacement fails. Restore it with
    database users stopped; this operation never automatically overwrites a backup.

    The caller must exclude other database users for the entire operation.
    Identity checks detect observed changes but are not a cross-process lease.
    SQLite journals/sidecars and symlink targets are rejected before effects.
    """
    target, backup = _path(path), _path(backup_path)
    if backup == target or backup in {
        Path(str(target) + suffix) for suffix in ("-wal", "-shm", "-journal")
    }:
        raise ValueError(
            "The backup must be separate from the database and its SQLite sidecars."
        )
    original = _identity(target)
    _require_no_sidecars(target)
    _require_absent(backup)
    backup.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=".liuxin-replace-", dir=target.parent) as scratch:
        candidate = Path(scratch) / "database.sqlite"
        build(candidate)
        _require_no_sidecars(candidate)
        shutil.copymode(target, candidate)
        _sync(candidate)

        def snapshot(destination: Path) -> None:
            with closing(
                # rw opens only an existing file and lets SQLite clean up its own
                # empty WAL coordination files when this offline reader closes.
                sqlite3.connect(f"{target.as_uri()}?mode=rw", uri=True, timeout=1)
            ) as source:
                # Obtain a read lock with a bounded busy timeout before backup(),
                # whose default busy retry loop would otherwise be unbounded.
                source.execute("BEGIN")
                source.execute("SELECT count(*) FROM sqlite_schema").fetchone()
                with closing(sqlite3.connect(destination)) as output:
                    source.backup(output)
                    if output.execute("PRAGMA integrity_check").fetchone() != ("ok",):
                        raise RuntimeError(
                            "Database backup failed its integrity check."
                        )
            shutil.copymode(target, destination)

        _require_no_sidecars(target)
        if _identity(target) != original:
            raise RuntimeError("Database changed while preparing replacement.")
        create_sqlite_file(backup, snapshot)
        _require_no_sidecars(target)
        if _identity(target) != original:
            raise RuntimeError(
                f"Database changed; replacement refused. Backup retained at {backup}"
            )
        os.replace(candidate, target)
    return backup
