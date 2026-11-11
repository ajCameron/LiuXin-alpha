"""Prepare verified SQLite snapshots without touching the caller's transaction."""

from __future__ import annotations

import os
import sqlite3
from collections.abc import Iterable, Iterator
from contextlib import closing, contextmanager
from pathlib import Path

from LiuXin_alpha.databases.creation import (
    create_sqlite_file,
    require_no_sqlite_sidecars,
)
from LiuXin_alpha.databases.db_types.driver_types import SQLConnection


def require_idle_sqlite_connections(connections: Iterable[SQLConnection]) -> None:
    """Refuse a file rebuild that would discard any tracked pending transaction."""
    for connection in connections:
        if isinstance(connection, sqlite3.Connection):
            try:
                active = connection.in_transaction
            except sqlite3.ProgrammingError:
                continue
            if active:
                raise RuntimeError(
                    "Commit or roll back pending writes before rebuilding the database"
                )


def require_sqlite_replacement_ready(
    path: str, connections: Iterable[SQLConnection]
) -> None:
    """Refuse publication after a failed close or while old journals remain.

    The caller still coordinates exclusive access. Never unlink a WAL that may
    belong to a retained native reader or an external connection.
    """
    if any(True for _ in connections):
        raise RuntimeError("Close all owned connections before rebuilding the database")
    require_no_sqlite_sidecars(Path(path))


def replay_sqlite_dump(connection: sqlite3.Connection, lines: Iterable[str]) -> None:
    """Replay complete SQLite statements, including triggers and quoted semicolons."""
    pending = ""
    for line in lines:
        start = 0
        for position, character in enumerate(line):
            if character == ";":
                pending += line[start : position + 1]
                start = position + 1
                if sqlite3.complete_statement(pending):
                    connection.execute(pending).close()
                    pending = ""
        pending += line[start:]
    if pending.strip():
        connection.execute(pending).close()


@contextmanager
def configured_sqlite_connection(
    path: str | os.PathLike[str],
) -> Iterator[sqlite3.Connection]:
    """Own a configured, isolated connection for restore and integrity checking.

    Reuse the complete SQLite callback/collation registration rather than a
    subset that silently diverges from normal catalogue connections. This owner
    has no catalogue maintenance runtime and never borrows the caller's handle.
    """
    from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver import (
        DatabaseDriver,
    )

    owner = DatabaseDriver({"database_path": str(path)}, set_conn=False)
    try:
        owner.conn = owner._open_connection(seed_languages=False)
        connection = owner.conn
        if not isinstance(connection, sqlite3.Connection):
            raise TypeError("SQLite file operations require a native connection")
        yield connection
    finally:
        owner.close()


def validate_sqlite_connection(connection: sqlite3.Connection) -> None:
    """Reject corrupt pages/indexes and dangling foreign keys before publication."""
    with closing(connection.execute("PRAGMA integrity_check")) as cursor:
        if cursor.fetchall() != [("ok",)]:
            raise RuntimeError("SQLite snapshot failed its integrity check")
    with closing(connection.execute("PRAGMA foreign_key_check")) as cursor:
        violation = cursor.fetchone()
        if violation is not None:
            raise RuntimeError(
                f"SQLite snapshot has a foreign key violation: {violation!r}"
            )


def validate_sqlite_file(path: str | os.PathLike[str]) -> None:
    """Check a completed private restore with the full SQLite callback set."""
    with configured_sqlite_connection(path) as connection:
        connection.execute("PRAGMA query_only=ON").close()
        validate_sqlite_connection(connection)


def backup_sqlite_file(
    source_path: str | os.PathLike[str], destination: str | os.PathLike[str]
) -> None:
    """Publish a new verified snapshot of committed state, including WAL contents.

    A separate read-only source pins a snapshot before copying. Pending caller
    writes are excluded and untouched. Writers may continue in WAL mode; other
    journal modes retain SQLite's read-lock behavior. Initial lock acquisition
    has a bounded busy timeout. Existing destinations are never overwritten.
    """
    if str(source_path) == ":memory:":
        raise ValueError("File backup requires a file-backed SQLite database")
    source = Path(source_path).expanduser().resolve()
    target = Path(destination).expanduser()
    target = target.parent.resolve() / target.name
    if target in {
        source,
        *(Path(str(source) + suffix) for suffix in ("-wal", "-shm", "-journal")),
    }:
        raise ValueError(
            "Backup must be separate from the source and its SQLite sidecars"
        )

    def snapshot(candidate: Path) -> None:
        """Copy only a pinned committed snapshot and close both owned handles."""
        with closing(
            sqlite3.connect(f"{source.as_uri()}?mode=ro", uri=True, timeout=5)
        ) as reader:
            reader.execute("BEGIN").close()
            with closing(
                reader.execute("SELECT count(*) FROM sqlite_schema")
            ) as cursor:
                cursor.fetchone()
            with configured_sqlite_connection(candidate) as output:
                reader.backup(output, pages=256)
                validate_sqlite_connection(output)

    create_sqlite_file(target, snapshot)
