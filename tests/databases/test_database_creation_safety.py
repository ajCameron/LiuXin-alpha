"""Exercise creation collisions and recoverable replacement on real database files."""

from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from threading import Barrier

import pytest

from LiuXin_alpha.core.library_services import LibraryServices
from LiuXin_alpha.databases import creation
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.database_driver_plugins.registry import load_database_driver


@pytest.fixture
def database_dir(tmp_path):
    root = tmp_path / "databases"
    root.mkdir()
    return root


def _sentinel(path: Path) -> bytes:
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("CREATE TABLE sentinel (value TEXT)")
        connection.execute("INSERT INTO sentinel VALUES ('keep me')")
        connection.commit()
    return path.read_bytes()


def _open(path, driver_spec, **kwargs):
    return Database(
        metadata={"database_path": str(path)},
        db_type=driver_spec.db_type,
        enable_storage_manager=False,
        enable_maintenance=False,
        **kwargs,
    )


@pytest.mark.parametrize("backup", [False, True])
def test_create_rejects_existing_file_before_opening_it(
    database_dir, driver_spec, backup
):
    path = database_dir / "existing.sqlite"
    before = _sentinel(path)
    with pytest.raises(FileExistsError):
        _open(path, driver_spec, create=True, backup=backup)
    assert path.read_bytes() == before
    assert sorted(p.name for p in database_dir.iterdir()) == [path.name]


def test_core_create_rejects_existing_file(database_dir, driver_spec):
    path = database_dir / "existing.sqlite"
    before = _sentinel(path)
    with pytest.raises(FileExistsError):
        LibraryServices.open(
            database_path=path, db_type=driver_spec.db_type, create=True
        )
    assert path.read_bytes() == before


@pytest.mark.parametrize("explicit", [False, True])
def test_new_creation_and_reopen_work(database_dir, driver_spec, explicit):
    path = database_dir / "nested" / "new.sqlite"
    with _open(path, driver_spec, create=explicit) as database:
        database.driver.conn.execute("CREATE TABLE sentinel (value TEXT)")
        database.driver.conn.execute("INSERT INTO sentinel VALUES ('keep me')")
        database.driver.conn.commit()
    with _open(path, driver_spec) as database:
        assert database.driver.conn.execute(
            "SELECT value FROM sentinel"
        ).fetchone() == ("keep me",)
        with pytest.raises(FileExistsError):
            database.create_new_database(blank=True, backup=True)
        assert database.driver.conn.execute(
            "SELECT value FROM sentinel"
        ).fetchone() == ("keep me",)


def test_create_rejects_dangling_symlink(database_dir, driver_spec):
    path = database_dir / "existing.sqlite"
    target = database_dir / "not-created.sqlite"
    path.symlink_to(target)
    with pytest.raises(FileExistsError):
        _open(path, driver_spec, create=True)
    assert path.is_symlink()
    assert not target.exists()


def test_target_appearing_during_build_survives(database_dir, driver_spec, monkeypatch):
    path = database_dir / "race.sqlite"
    original_link = creation.os.link
    captured = []

    def race(source, destination):
        captured.append(_sentinel(path))
        return original_link(source, destination)

    monkeypatch.setattr(creation.os, "link", race)
    with pytest.raises(FileExistsError):
        _open(path, driver_spec, create=True)
    assert path.read_bytes() == captured[0]
    assert sorted(p.name for p in database_dir.iterdir()) == [path.name]


def test_two_creators_publish_only_one_complete_database(database_dir):
    path = database_dir / "race.sqlite"
    ready = Barrier(2)

    def create(label):
        def build(candidate):
            with closing(sqlite3.connect(candidate)) as connection:
                connection.execute("CREATE TABLE sentinel (value TEXT)")
                connection.execute("INSERT INTO sentinel VALUES (?)", (label,))
                connection.commit()
            ready.wait(timeout=10)

        try:
            creation.create_sqlite_file(path, build)
            return label
        except FileExistsError:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(create, ["first", "second"]))
    (winner,) = [label for label in results if label is not None]
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("SELECT value FROM sentinel").fetchone() == (winner,)
    assert sorted(p.name for p in database_dir.iterdir()) == [path.name]


def test_failed_schema_build_never_publishes(database_dir, driver_spec, monkeypatch):
    driver_type = load_database_driver(driver_spec.db_type)
    driver = driver_type(
        {"database_path": str(database_dir / "failed.sqlite")}, set_conn=False
    )

    def fail(connection):
        connection.execute("CREATE TABLE partial (value TEXT)")
        connection.commit()
        raise RuntimeError("injected schema failure")

    monkeypatch.setattr(driver, "_create_new_database", fail)
    try:
        with pytest.raises(RuntimeError, match="injected"):
            driver.direct_create_new_database()
    finally:
        driver.close()
    assert list(database_dir.iterdir()) == []


def test_explicit_replacement_retains_readable_original_backup(
    database_dir, driver_spec
):
    path, backup = database_dir / "old.sqlite", database_dir / "recovery.sqlite"
    with _open(path, driver_spec, create=True):
        pass
    _sentinel(path)
    path.chmod(0o600)
    result = LibraryServices.replace_database(
        database_path=path, backup_path=backup, db_type=driver_spec.db_type
    )
    assert result == backup
    assert path.stat().st_mode & 0o777 == 0o600
    assert backup.stat().st_mode & 0o777 == 0o600
    with closing(sqlite3.connect(backup)) as connection:
        assert connection.execute("SELECT value FROM sentinel").fetchone() == (
            "keep me",
        )
    with _open(path, driver_spec) as database:
        assert "works" in database.driver.direct_get_tables()
        assert "sentinel" not in database.driver.direct_get_tables()


@pytest.mark.parametrize("failure", ["build", "backup", "publish"])
def test_replacement_failure_preserves_original(
    database_dir, driver_spec, monkeypatch, failure
):
    path, backup = database_dir / "old.sqlite", database_dir / "recovery.sqlite"
    before = _sentinel(path)
    driver_type = load_database_driver(driver_spec.db_type)

    def fail(*args, **kwargs):
        raise RuntimeError("injected failure")

    if failure == "build":
        monkeypatch.setattr(driver_type, "_build_database_file", fail)
    elif failure == "backup":
        monkeypatch.setattr(creation, "create_sqlite_file", fail)
    else:
        monkeypatch.setattr(creation.os, "replace", fail)
    with pytest.raises(RuntimeError, match="injected"):
        Database.replace_database(
            {"database_path": str(path)},
            backup_path=backup,
            db_type=driver_spec.db_type,
        )
    assert path.read_bytes() == before
    assert backup.exists() == (failure == "publish")
    assert not any(p.name.startswith(".liuxin-") for p in database_dir.iterdir())
    if backup.exists():
        with closing(sqlite3.connect(backup)) as connection:
            assert connection.execute("SELECT value FROM sentinel").fetchone() == (
                "keep me",
            )


@pytest.mark.parametrize("suffix", ["-wal", "-shm", "-journal"])
def test_replacement_refuses_unrecovered_sidecars(database_dir, suffix):
    path, backup = database_dir / "old.sqlite", database_dir / "recovery.sqlite"
    before = _sentinel(path)
    sidecar = Path(str(path) + suffix)
    sidecar.write_bytes(b"caller-owned")
    with pytest.raises(RuntimeError, match="checkpoint/recover"):
        Database.replace_database({"database_path": str(path)}, backup_path=backup)
    assert path.read_bytes() == before
    assert sidecar.read_bytes() == b"caller-owned"
    assert not backup.exists()


def test_replacement_never_overwrites_an_existing_backup(database_dir):
    path, backup = database_dir / "old.sqlite", database_dir / "recovery.sqlite"
    before = _sentinel(path)
    backup.write_bytes(b"earlier recovery")
    with pytest.raises(FileExistsError):
        Database.replace_database({"database_path": str(path)}, backup_path=backup)
    assert path.read_bytes() == before
    assert backup.read_bytes() == b"earlier recovery"


def test_creation_refuses_orphaned_wal(database_dir):
    path = database_dir / "missing.sqlite"
    sidecar = Path(str(path) + "-wal")
    sidecar.write_bytes(b"retained WAL")
    with pytest.raises(RuntimeError, match="checkpoint/recover"):
        Database(metadata={"database_path": str(path)}, create=True)
    assert not path.exists()
    assert sidecar.read_bytes() == b"retained WAL"


@pytest.mark.parametrize("replacement", [False, True])
def test_terminal_wizard_applies_explicit_creation_policy(database_dir, replacement):
    from LiuXin_alpha.surfaces.terminal.database_creation import (
        DatabaseCreationWizardConfig,
        create_database_from_wizard,
    )

    path, backup = database_dir / "library.sqlite", database_dir / "recovery.sqlite"
    if replacement:
        _sentinel(path)
    config = DatabaseCreationWizardConfig(
        path,
        "SQLite",
        replacement,
        False,
        False,
        False,
        replace_existing=replacement,
        backup_path=backup if replacement else None,
    )
    assert create_database_from_wizard(config) == path
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE name='works'"
        ).fetchone() == ("works",)
    assert backup.exists() == replacement
    if replacement:
        with closing(sqlite3.connect(backup)) as connection:
            assert connection.execute("SELECT value FROM sentinel").fetchone() == (
                "keep me",
            )


def test_terminal_wizard_old_backup_flag_cannot_authorize_replacement(database_dir):
    from LiuXin_alpha.surfaces.terminal.database_creation import (
        DatabaseCreationWizardConfig,
        create_database_from_wizard,
    )

    path = database_dir / "library.sqlite"
    before = _sentinel(path)
    config = DatabaseCreationWizardConfig(path, "SQLite", True, False, False, False)
    with pytest.raises(FileExistsError):
        create_database_from_wizard(config)
    assert path.read_bytes() == before


def test_registry_creation_uses_same_create_only_contract(database_dir, driver_spec):
    from LiuXin_alpha.databases.database_driver_plugins.registry import (
        create_new_database,
    )

    path = database_dir / "registry.sqlite"
    create_new_database(driver_spec.db_type, path)
    with _open(path, driver_spec) as database:
        assert "works" in database.driver.direct_get_tables()
    before = path.read_bytes()
    with pytest.raises(FileExistsError):
        create_new_database(driver_spec.db_type, path)
    assert path.read_bytes() == before


def test_busy_database_backup_refuses_without_replacing_original(database_dir):
    path, backup = database_dir / "locked.sqlite", database_dir / "recovery.sqlite"
    before = _sentinel(path)
    with closing(sqlite3.connect(path)) as writer:
        writer.execute("BEGIN EXCLUSIVE")
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            Database.replace_database({"database_path": str(path)}, backup_path=backup)
    assert path.read_bytes() == before
    assert not backup.exists()


@pytest.mark.parametrize("publication_fails", [False, True])
def test_closed_wal_database_replacement_and_backup(
    database_dir, monkeypatch, publication_fails
):
    path, backup = database_dir / "wal.sqlite", database_dir / "recovery.sqlite"
    _sentinel(path)
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("PRAGMA journal_mode=WAL")
    before = path.read_bytes()

    def fail(*args):
        raise OSError("injected replacement failure")

    if publication_fails:
        monkeypatch.setattr(creation.os, "replace", fail)
        with pytest.raises(OSError, match="injected"):
            Database.replace_database({"database_path": str(path)}, backup_path=backup)
        assert path.read_bytes() == before
    else:
        Database.replace_database({"database_path": str(path)}, backup_path=backup)
    assert not Path(str(path) + "-wal").exists()
    assert not Path(str(path) + "-shm").exists()
    with closing(sqlite3.connect(backup)) as connection:
        assert connection.execute("SELECT value FROM sentinel").fetchone() == (
            "keep me",
        )
