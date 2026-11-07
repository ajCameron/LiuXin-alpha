"""Exercise completion tokens and file publication against real backend state."""

import gc
import sqlite3
import weakref
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace

import pytest

from LiuXin_alpha.databases.db_types.maintenance_types import MaintenanceMode
from LiuXin_alpha.databases.maintenance.plugins import (
    MaintenancePluginBase,
    MaintenancePluginResult,
)
from LiuXin_alpha.databases.maintenance.runtime import MaintenanceRuntime
from LiuXin_alpha.errors import DatabaseDriverError
from tests.databases import test_driver_plugin_postgres_safety
from tests.databases.test_driver_plugin_sqlite_safety import driver as driver

pg_driver = test_driver_plugin_postgres_safety.driver


class Prompt(MaintenancePluginBase):
    name = "completion_prompt"
    tables = ("review_items",)
    mode = MaintenanceMode.PROMPT

    def __init__(self):
        self.calls = 0
        self.fail = False

    def handle_events(self, context, events):
        self.calls += 1
        for event in events:
            context.connection.execute(
                "INSERT INTO effects VALUES (?)", (event.row_id,)
            ).close()
        if self.fail:
            raise RuntimeError("prompt failed")
        return MaintenancePluginResult(handled=len(events))


def install(driver):
    if getattr(driver, "schema", None):
        driver.conn.execute(
            "CREATE TABLE review_items(review_item_id INTEGER PRIMARY KEY, review_item_value TEXT)"
        ).close()
    driver.conn.execute("CREATE TABLE effects(id INTEGER)").close()
    driver.conn.commit()
    plugin = Prompt()
    runtime = MaintenanceRuntime(
        SimpleNamespace(driver=driver, uuid="completion"), [plugin]
    )
    runtime.install()
    return plugin


def committed(driver):
    """Use a separate reader and bypass hooks that might complete pending work."""
    with closing(driver.get_connection()) as reader:
        raw = getattr(reader, "raw_connection", reader)
        with closing(raw.cursor()) as cursor:
            cursor.execute("SELECT review_item_id FROM review_items ORDER BY 1")
            source = cursor.fetchall()
            cursor.execute("SELECT id FROM effects ORDER BY 1")
            return source, cursor.fetchall()


@pytest.mark.parametrize(
    "prefix",
    ["", "\f", "\ufeff", "; /* gap */ \ufeff\f", "/* outer /* inner */ "],
    ids=["plain", "formfeed", "bom", "empty_comments", "sqlite_comment"],
)
@pytest.mark.parametrize(
    "control", ["BEGIN", "COMMIT", "END", "ROLLBACK", "SAVEPOINT s", "RELEASE s"]
)
def test_script_controls_are_rejected_before_any_effect(driver, prefix, control):
    plugin = install(driver)
    with pytest.raises(
        DatabaseDriverError, match="cannot contain transaction controls"
    ):
        driver.direct_executescript(
            f"INSERT INTO review_items VALUES (1, 'first'); {prefix}{control}; "
            "INSERT INTO review_items VALUES (1, 'duplicate');"
        )
    assert committed(driver) == ([], [])
    assert plugin.calls == 0


@pytest.mark.parametrize("fail", [False, True])
def test_script_quoted_controls_remain_data_and_late_errors_roll_back(driver, fail):
    install(driver)
    sql = (
        "INSERT INTO review_items VALUES (1, '\fCOMMIT; /* literal */');"
        f"INSERT INTO review_items VALUES ({1 if fail else 2}, 'second');"
    )
    if fail:
        with pytest.raises(DatabaseDriverError):
            driver.direct_executescript(sql)
    else:
        driver.direct_executescript(sql)
    expected = [] if fail else [(1,), (2,)]
    assert committed(driver) == (expected, expected)


PG_PREFIXES = [
    "",
    "\f",
    "/* outer /* nested */ tail */ ",
    "-- line\r",
    "; /* empty */ ; ",
]
PG_PREFIX_IDS = ["plain", "formfeed", "nested", "cr_comment", "empty"]


@pytest.mark.parametrize("prefix", PG_PREFIXES, ids=PG_PREFIX_IDS)
@pytest.mark.parametrize("operation", ["COMMIT", "END"])
@pytest.mark.parametrize("fail", [False, True])
def test_postgres_prefixed_completion_runs_prompt_or_rolls_back(
    pg_driver, prefix, operation, fail
):
    plugin = install(pg_driver)
    plugin.fail = fail
    pg_driver.conn.execute("INSERT INTO review_items VALUES (1, 'source')").close()
    if fail:
        with pytest.raises(RuntimeError, match="prompt failed"):
            pg_driver.conn.execute(prefix + operation)
    else:
        pg_driver.conn.execute(prefix + operation).close()
    expected = [] if fail else [(1,)]
    assert committed(pg_driver) == (expected, expected)
    assert plugin.calls == 1


@pytest.mark.parametrize("prefix", PG_PREFIXES, ids=PG_PREFIX_IDS)
@pytest.mark.parametrize(
    "operation", ["SELECT id FROM effects", "RELEASE SAVEPOINT ready"]
)
def test_postgres_prefixed_read_and_release_cannot_skip_prompt(
    pg_driver, prefix, operation
):
    plugin = install(pg_driver)
    pg_driver.conn.execute("SAVEPOINT ready").close()
    pg_driver.conn.execute("INSERT INTO review_items VALUES (1, 'source')").close()
    plugin.fail = True
    with pytest.raises(RuntimeError, match="prompt failed"):
        pg_driver.conn.execute(prefix + operation)
    assert plugin.calls == 1
    assert committed(pg_driver) == ([], [])
    pg_driver.conn.rollback()


@pytest.mark.parametrize(
    "literal,expected",
    [("$body$a;COMMIT;$body$", "a;COMMIT;"), ("E'a\\';COMMIT;'", "a';COMMIT;")],
    ids=["dollar", "escaped"],
)
def test_postgres_handler_uses_native_quoted_text_and_comment_rules(
    pg_driver, literal, expected
):
    class NativePrompt(Prompt):
        def handle_events(self, context, events):
            assert context.connection.execute(
                "/* outer /* nested */ tail */ SELECT " + literal
            ).fetchone() == (expected,)
            return super().handle_events(context, events)

    pg_driver.conn.execute(
        "CREATE TABLE review_items(review_item_id INTEGER PRIMARY KEY, review_item_value TEXT)"
    ).close()
    pg_driver.conn.execute("CREATE TABLE effects(id INTEGER)").close()
    pg_driver.conn.commit()
    runtime = MaintenanceRuntime(
        SimpleNamespace(driver=pg_driver, uuid="native"), [NativePrompt()]
    )
    runtime.install()
    pg_driver.conn.execute("INSERT INTO review_items VALUES (1, 'source')").close()
    pg_driver.conn.commit()
    assert committed(pg_driver) == ([(1,)], [(1,)])
    from LiuXin_alpha.databases.maintenance.bound_connection import (
        MaintenanceTransactionError,
    )

    with pytest.raises(MaintenanceTransactionError, match="one row statement"):
        pg_driver.conn.execute(
            "/* outer /* nested */ tail */ SELECT " + literal + "; COMMIT"
        )


def wal_rows(driver):
    driver.conn.execute("PRAGMA journal_mode=WAL").close()
    driver.conn.executemany(
        "INSERT INTO review_items VALUES (?, ?)", ((i, "x" * 500) for i in range(50))
    ).close()
    driver.conn.commit()


@pytest.mark.parametrize("owner", ["primary", "auxiliary"])
@pytest.mark.parametrize("cursor_kind", ["default", "custom", "closed"])
def test_rebuild_finishes_owned_readers_and_publishes_exact_candidate(
    driver, owner, cursor_kind
):
    wal_rows(driver)
    connection = driver.conn if owner == "primary" else driver.get_connection()

    class CustomCursor(sqlite3.Cursor):
        pass

    cursor = (
        connection.cursor(factory=CustomCursor)
        if cursor_kind == "custom"
        else connection.cursor()
    )
    cursor.execute("SELECT * FROM review_items ORDER BY review_item_id")
    cursor.fetchone()
    if cursor_kind == "closed":
        cursor.close()
    assert not connection.in_transaction
    driver.dump_and_restore(sql="CREATE TABLE restored_marker(id INTEGER);")
    with closing(sqlite3.connect(driver.database_path)) as reader:
        assert reader.execute(
            "SELECT name FROM sqlite_master WHERE name='restored_marker'"
        ).fetchall() == [("restored_marker",)]
        assert reader.execute("SELECT count(*) FROM review_items").fetchone() == (50,)
        assert reader.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
    with pytest.raises(sqlite3.ProgrammingError):
        cursor.fetchone()


def test_rebuild_refuses_surviving_external_wal_and_can_retry(driver):
    wal_rows(driver)
    path = Path(driver.database_path)
    original = path.stat().st_ino
    callbacks, readers = [], []

    def callback(_):
        callbacks.append(True)
        if len(callbacks) == 2:
            reader = sqlite3.connect(path)
            readers.append(reader)
            reader.execute("BEGIN").close()
            assert reader.execute("SELECT count(*) FROM review_items").fetchone() == (
                50,
            )

    try:
        with pytest.raises(RuntimeError, match="checkpoint/recover SQLite"):
            driver.dump_and_restore(
                callback=callback, sql="CREATE TABLE restored_marker(id INTEGER);"
            )
        assert path.stat().st_ino == original
        assert Path(str(path) + "-wal").exists()
        assert (
            driver.conn.execute(
                "SELECT name FROM sqlite_master WHERE name='restored_marker'"
            ).fetchall()
            == []
        )
        assert driver.conn.execute("SELECT count(*) FROM review_items").fetchone() == (
            50,
        )
    finally:
        for reader in readers:
            reader.close()
    driver.dump_and_restore(sql="CREATE TABLE restored_marker(id INTEGER);")
    assert driver.conn.execute(
        "SELECT name FROM sqlite_master WHERE name='restored_marker'"
    ).fetchall() == [("restored_marker",)]


def test_rebuild_refuses_failed_owned_close_without_publishing(driver, monkeypatch):
    original = Path(driver.database_path).stat().st_ino
    old = driver.conn

    def fail():
        raise RuntimeError("cannot close")

    with monkeypatch.context() as patch:
        patch.setattr(old, "close", fail)
        with pytest.raises(RuntimeError, match="Close all owned connections"):
            driver.dump_and_restore(sql="CREATE TABLE restored_marker(id INTEGER);")
        assert Path(driver.database_path).stat().st_ino == original
        assert old in driver._open_connections
    driver.dump_and_restore(sql="CREATE TABLE restored_marker(id INTEGER);")
    assert driver.conn.execute(
        "SELECT name FROM sqlite_master WHERE name='restored_marker'"
    ).fetchall() == [("restored_marker",)]


def test_unwrapped_native_reader_cannot_silently_change_published_candidate(driver):
    wal_rows(driver)
    path = Path(driver.database_path)
    original = path.stat().st_ino
    cursor = sqlite3.Connection.execute(driver.conn, "SELECT * FROM review_items")
    cursor.fetchone()
    try:
        driver.dump_and_restore(sql="CREATE TABLE restored_marker(id INTEGER);")
    except RuntimeError as error:
        assert "checkpoint/recover SQLite" in str(error)
        assert path.stat().st_ino == original
        expected = []
    else:
        expected = [("restored_marker",)]
    with closing(sqlite3.connect(path)) as reader:
        assert (
            reader.execute(
                "SELECT name FROM sqlite_master WHERE name='restored_marker'"
            ).fetchall()
            == expected
        )
        assert reader.execute("SELECT count(*) FROM review_items").fetchone() == (50,)
    del cursor
    gc.collect()


def test_cursor_tracking_does_not_retain_abandoned_readers(driver):
    wal_rows(driver)
    cursor = driver.conn.execute("SELECT * FROM review_items")
    cursor.fetchone()
    reference = weakref.ref(cursor)
    del cursor
    gc.collect()
    assert reference() is None
    # Closing rolls back unrelated pending writes and releases all owned WAL use.
    driver.conn.execute("INSERT INTO review_items VALUES (100, 'pending')").close()
    driver.close()
    assert not Path(driver.database_path + "-wal").exists()
    with closing(sqlite3.connect(driver.database_path)) as reader:
        assert reader.execute("SELECT count(*) FROM review_items").fetchone() == (50,)
