"""Require validated round trips of populated WEMI and extension graphs."""

import importlib
import shutil
import sqlite3
from pathlib import Path

import pytest

from LiuXin_alpha.databases.database import Database
from tests.databases import test_row_write_safety

catalogue_template = test_row_write_safety.template


@pytest.fixture
def driver(catalogue_template, tmp_path):
    backend, source = catalogue_template
    path = tmp_path / "catalogue.sqlite"
    shutil.copyfile(source, path)
    with Database(
        metadata={"database_path": str(path)},
        db_type=backend,
        repair_bootstrap_rows=False,
        enable_storage_manager=False,
        enable_maintenance=False,
    ) as database:
        yield database.driver


def populate(driver):
    """Include out-of-order parents, a deferred cycle and SQL callback consumers."""
    connection = driver.conn
    connection.execute(
        "INSERT INTO manifestations (manifestation_id) VALUES (101)"
    ).close()
    connection.execute(
        "INSERT INTO items (item_id, item_manifestation_id) VALUES (202, 101)"
    ).close()
    connection.commit()
    connection.executescript(
        "CREATE TABLE a_child (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES z_parent(id));"
        "CREATE TABLE z_parent (id INTEGER PRIMARY KEY);"
        "INSERT INTO z_parent VALUES (1); INSERT INTO a_child VALUES (2,1);"
        "CREATE TABLE cycle_a (id INTEGER PRIMARY KEY, other INTEGER REFERENCES cycle_b(id) "
        "DEFERRABLE INITIALLY DEFERRED);"
        "CREATE TABLE cycle_b (id INTEGER PRIMARY KEY, other INTEGER REFERENCES cycle_a(id) "
        "DEFERRABLE INITIALLY DEFERRED);"
        "BEGIN; INSERT INTO cycle_a VALUES (1,2); INSERT INTO cycle_b VALUES (2,1); COMMIT;"
        "CREATE TABLE callback_values (value TEXT COLLATE PYNOCASE "
        "CHECK(title_sort(value) IS NOT NULL));"
        "INSERT INTO callback_values VALUES ('The Novel; a question?');"
        "CREATE INDEX callback_values_normalized ON callback_values(liuxin_text_key_v1(value));"
        "PRAGMA user_version=123;"
    ).close()


def contents(driver):
    return {
        table: driver.conn.execute(f'SELECT * FROM "{table}"').fetchall()
        for table in (
            "manifestations",
            "items",
            "a_child",
            "z_parent",
            "cycle_a",
            "cycle_b",
            "callback_values",
        )
    }


def test_restore_preserves_populated_wemi_cycles_extensions_and_callbacks(driver):
    populate(driver)
    before = contents(driver)
    driver.dump_and_restore(
        sql="CREATE TABLE restored_marker (value TEXT); INSERT INTO restored_marker VALUES ('a;b');"
    )
    assert contents(driver) == before
    assert driver.conn.execute("SELECT value FROM restored_marker").fetchall() == [
        ("a;b",)
    ]
    assert driver.conn.execute("PRAGMA user_version").fetchone() == (123,)
    assert driver.conn.execute("PRAGMA foreign_keys").fetchone() == (1,)
    assert driver.conn.execute("PRAGMA foreign_key_check").fetchall() == []
    assert driver.conn.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
    assert driver.conn.execute(
        "SELECT title_sort(value) FROM callback_values"
    ).fetchone()[0]
    with pytest.raises(sqlite3.IntegrityError):
        driver.conn.execute("INSERT INTO a_child VALUES (3,999)")
    driver.conn.rollback()


@pytest.mark.parametrize("failure", ["sql", "foreign_key", "validation", "publication"])
def test_restore_failure_preserves_the_original_database(driver, monkeypatch, failure):
    from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver import (
        sqlite_files,
    )

    populate(driver)
    before = contents(driver)
    original_inode = Path(driver.database_path).stat().st_ino
    prefix = None
    if failure == "sql":
        prefix = "THIS IS NOT SQL;"
    elif failure == "foreign_key":
        prefix = (
            "CREATE TABLE bad_child (id INTEGER REFERENCES z_parent(id));"
            "INSERT INTO bad_child VALUES (999);"
        )
    elif failure == "validation":

        def reject(_):
            raise RuntimeError("injected validation failure")

        monkeypatch.setattr(sqlite_files, "validate_sqlite_connection", reject)
    else:

        def reject(*_):
            raise OSError("injected publication failure")

        monkeypatch.setattr(
            importlib.import_module(type(driver).__module__), "atomic_rename", reject
        )
    with pytest.raises(Exception) as error:
        driver.dump_and_restore(sql=prefix)
    if failure == "foreign_key":
        assert "foreign key violation" in str(error.value)
    assert contents(driver) == before
    assert Path(driver.database_path).stat().st_ino == original_inode
    assert (
        driver.conn.execute(
            "SELECT name FROM sqlite_master WHERE name='bad_child'"
        ).fetchall()
        == []
    )


@pytest.mark.parametrize("owner", ["primary", "auxiliary", "callback"])
def test_restore_refuses_to_discard_pending_writes(driver, owner):
    connection = driver.get_connection() if owner == "auxiliary" else driver.conn

    def start_pending(_=None):
        connection.execute(
            "INSERT INTO review_items (review_item_id) VALUES (1)"
        ).close()

    if owner != "callback":
        start_pending()
    started = []

    def callback(message):
        if not started:
            started.append(True)
            start_pending()

    with pytest.raises(RuntimeError, match="pending writes"):
        driver.dump_and_restore(callback=callback if owner == "callback" else None)
    assert connection.in_transaction
    assert connection.execute("SELECT COUNT(*) FROM review_items").fetchone() == (1,)
    connection.rollback()
    assert driver.conn.execute("SELECT COUNT(*) FROM review_items").fetchone() == (0,)


def test_validation_does_not_repair_the_candidate_before_checking_it(driver):
    """Missing language rows must fail validation, not be silently reseeded."""
    from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.sqlite_files import (
        validate_sqlite_file,
    )

    connection = driver.conn
    language_id = connection.execute(
        "SELECT language_id FROM languages LIMIT 1"
    ).fetchone()[0]
    connection.execute(
        "CREATE TABLE language_ref (id INTEGER REFERENCES languages(language_id))"
    ).close()
    connection.execute("INSERT INTO language_ref VALUES (?)", (language_id,)).close()
    connection.commit()
    connection.execute("PRAGMA foreign_keys=OFF").close()
    triggers = connection.execute(
        "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='languages'"
    ).fetchall()
    for (name,) in triggers:
        connection.execute('DROP TRIGGER "' + name.replace('"', '""') + '"').close()
    connection.execute("DELETE FROM languages").close()
    connection.commit()
    with pytest.raises(RuntimeError, match="foreign key violation"):
        validate_sqlite_file(driver.database_path)
    assert connection.execute("SELECT count(*) FROM languages").fetchone() == (0,)
    assert (
        connection.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='languages'"
        ).fetchall()
        == []
    )
