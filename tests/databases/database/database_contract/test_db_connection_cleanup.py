"""Verify facade shutdown releases owned handles without waiting for collection."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest


def _open_descriptors(identity: tuple[int, int]) -> list[str]:
    """Find this process's descriptors for an inode, including unlinked files."""
    matches = []
    for descriptor in Path("/proc/self/fd").iterdir():
        try:
            stat = descriptor.stat()
        except FileNotFoundError:
            continue  # The directory iterator's own descriptor may have closed.
        if (stat.st_dev, stat.st_ino) == identity:
            matches.append(os.readlink(descriptor))
    return matches


def test_close_releases_auxiliary_connections(open_db):
    """A retained auxiliary handle must reject SQL after its facade closes."""
    driver = open_db.driver
    extra = driver.get_connection()
    assert extra.execute("SELECT 1").fetchall() == [(1,)]
    try:
        open_db.close()
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            extra.execute("SELECT 1")
    finally:
        # Keep a failing regression from leaking into later tests.
        driver.close()


@pytest.mark.skipif(
    not Path("/proc/self/fd").is_dir(), reason="requires Linux descriptor inspection"
)
def test_close_releases_deleted_database_without_gc(open_db, db_path):
    """An unlinked database must have no descriptors after explicit shutdown."""
    driver = open_db.driver
    extra = driver.get_connection()
    extra.execute("SELECT name FROM sqlite_master").fetchall()
    stat = db_path.stat()
    identity = (stat.st_dev, stat.st_ino)
    assert _open_descriptors(identity), "Probe must see the live database first"
    try:
        # Model aggressive scratch cleanup while deliberately retaining Python
        # references: unlinking must not hide handles still consuming storage.
        db_path.unlink()
        assert any("(deleted)" in name for name in _open_descriptors(identity))
        open_db.close()
        assert _open_descriptors(identity) == []
    finally:
        driver.close()


@pytest.mark.parametrize("writer", ["primary", "auxiliary", "commit_guard"])
def test_close_preserves_transaction_ownership(open_db, db_path, writer, monkeypatch):
    """Closing resources discards pending writes and never requests a commit."""
    driver = open_db.driver
    primary = driver.conn
    primary.execute("CREATE TABLE close_transaction_probe (value INTEGER)")
    primary.commit()
    connection = driver.get_connection() if writer == "auxiliary" else primary
    connection.execute("INSERT INTO close_transaction_probe VALUES (42)")
    commits = []
    if writer == "commit_guard":

        def fail_commit():
            """Record accidental commit attempts even if close suppresses errors."""
            commits.append(True)
            raise sqlite3.OperationalError("injected commit failure")

        monkeypatch.setattr(primary, "commit", fail_commit)
    try:
        open_db.close()
        assert commits == []
        probe = sqlite3.connect(str(db_path))
        try:
            rows = probe.execute("SELECT value FROM close_transaction_probe").fetchall()
            assert rows == []
        finally:
            probe.close()
    finally:
        driver.close()
