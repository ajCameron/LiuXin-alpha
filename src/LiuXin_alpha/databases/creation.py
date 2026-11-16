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
from contextlib import AbstractContextManager, closing
from pathlib import Path
from tempfile import TemporaryDirectory

from LiuXin_alpha.databases.search_functions import register_search_functions
from LiuXin_alpha.databases.sqlite_uri import sqlite_file_uri


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


def require_no_sqlite_sidecars(path: Path) -> None:
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
    """Flush completed file contents before publication.

    Windows' CRT commit operation requires a writable descriptor. These are
    private builder-owned files; opening read/write does not truncate them.
    """
    with path.open("r+b" if os.name == "nt" else "rb") as stream:
        os.fsync(stream.fileno())


def sync_directory(path: Path) -> None:
    """Persist a published directory entry on POSIX.

    Windows has no equivalent directory descriptor in this API. A POSIX failure
    propagates even though publication may already have succeeded; callers must
    inspect the destination before retrying.
    """
    if os.name != "posix":
        return
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def create_sqlite_file(
    path: str | os.PathLike[str], build: Callable[[Path], None]
) -> None:
    """
    Build a SQLite file privately and publish only while the target is absent.

    The builder must finish a standalone database and close its handles before
    returning. A hard link publishes the candidate without overwriting a
    competing target. Unsupported hard links fail without a rename fallback.
    Private scratch is removed on exit; newly created parent directories remain.

    After publication, a directory-sync failure can raise while leaving the new
    target in place. Inspect that target before retrying. This helper checks for
    sidecars but does not independently validate the builder's schema or data.

    Example:
        >>> def build_empty(path):
        ...     with closing(sqlite3.connect(path)) as connection:
        ...         connection.execute("CREATE TABLE example (id INTEGER PRIMARY KEY)").close()
        >>> with TemporaryDirectory() as directory:
        ...     target = Path(directory) / "new.sqlite"
        ...     create_sqlite_file(target, build_empty)
        ...     print(target.is_file())
        True


    :param path: Destination filesystem path; existing entries, including
        symlinks, are rejected.
    :param build: Callback receiving a private candidate Path; create and close
        the complete file there.
    :return: None after publication and the platform-supported durability flush.
    :raises FileExistsError: The destination exists initially or another creator
        wins publication.
    :raises RuntimeError: SQLite journal or WAL sidecars are present at a
        checked path.
    """
    target = _path(path)
    _require_absent(target)
    require_no_sqlite_sidecars(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=".liuxin-create-", dir=target.parent) as scratch:
        candidate = Path(scratch) / "database.sqlite"
        build(candidate)
        require_no_sqlite_sidecars(candidate)
        _sync(candidate)
        require_no_sqlite_sidecars(target)
        os.link(candidate, target)
        sync_directory(target.parent)


def replace_sqlite_file(
    path: str | os.PathLike[str],
    *,
    backup_path: str | os.PathLike[str],
    build: Callable[[Path], None],
    backup_connection_factory: Callable[
        [Path], AbstractContextManager[sqlite3.Connection]
    ]
    | None = None,
) -> Path:
    """
    Replace an offline SQLite file after preparing a new schema and recovery backup.

    The caller must exclude every other database user for the entire operation.
    File identity checks detect observed changes but do not acquire a process
    lease. The target must be a regular file without SQLite journal/WAL sidecars.

    Build and flush the candidate first, then publish an integrity-checked SQLite
    snapshot of the original at a separate, absent backup path. Preparation,
    backup and final-rename failures leave the original target in place. Once
    published, the backup remains available even if later steps fail. A directory
    sync can fail after replacement has already succeeded; inspect both paths
    before retrying. Restoration is a separate offline operation.

    Example:
        >>> backup = replace_sqlite_file(path, backup_path=recovery, build=build_schema)  # doctest: +SKIP


    :param path: Existing database file whose users have been stopped.
    :param backup_path: Absent recovery path, separate from the target and its
        sidecars.
    :param build: Callback creating a closed, standalone replacement at the
        supplied private Path.
    :param backup_connection_factory: Optional context factory opening the
        backup destination; it must close its handle on exit.
    :return: The resolved backup Path, retained for caller-directed recovery.
    """
    target, backup = _path(path), _path(backup_path)
    if backup == target or backup in {
        Path(str(target) + suffix) for suffix in ("-wal", "-shm", "-journal")
    }:
        raise ValueError(
            "The backup must be separate from the database and its SQLite sidecars."
        )
    original = _identity(target)
    require_no_sqlite_sidecars(target)
    _require_absent(backup)
    backup.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=".liuxin-replace-", dir=target.parent) as scratch:
        candidate = Path(scratch) / "database.sqlite"
        build(candidate)
        require_no_sqlite_sidecars(candidate)
        shutil.copymode(target, candidate)
        _sync(candidate)

        def snapshot(destination: Path) -> None:
            """Build and integrity-check a consistent backup before publishing replacement.

            Take a bounded read lock on the existing source, let SQLite copy committed
            state, and preserve file permissions. The enclosing replacement owns publication
            and verifies that no competing user changed the source identity."""
            with closing(
                # rw opens only an existing file and lets SQLite clean up its own
                # empty WAL coordination files when this offline reader closes.
                sqlite3.connect(sqlite_file_uri(target, mode="rw"), uri=True, timeout=1)
            ) as source:
                # Obtain a read lock with a bounded busy timeout before backup(),
                # whose default busy retry loop would otherwise be unbounded.
                source.execute("BEGIN")
                source.execute("SELECT count(*) FROM sqlite_schema").fetchone()
                output_owner = (
                    closing(sqlite3.connect(destination))
                    if backup_connection_factory is None
                    else backup_connection_factory(destination)
                )
                with output_owner as output:
                    register_search_functions(output)
                    source.backup(output)
                    if output.execute("PRAGMA integrity_check").fetchone() != ("ok",):
                        raise RuntimeError(
                            "Database backup failed its integrity check."
                        )
            shutil.copymode(target, destination)

        require_no_sqlite_sidecars(target)
        if _identity(target) != original:
            raise RuntimeError("Database changed while preparing replacement.")
        create_sqlite_file(backup, snapshot)
        require_no_sqlite_sidecars(target)
        if _identity(target) != original:
            raise RuntimeError(
                f"Database changed; replacement refused. Backup retained at {backup}"
            )
        os.replace(candidate, target)
        sync_directory(target.parent)
    return backup
