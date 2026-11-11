"""Protect backup, export and live resource ownership on both SQLite drivers."""

import gc
import sqlite3
from contextlib import closing

import pytest

from LiuXin_alpha.databases.database_driver_plugins.registry import load_database_driver
from LiuXin_alpha.errors import DatabaseDriverError

CTE_NAMES = ["source", "名", '"名"', "e\u0301", "x$y", "📚", "\u00a0"]
CTE_IDS = ["ascii", "unicode", "quoted", "combining", "dollar", "emoji", "nbsp"]


@pytest.mark.parametrize("name", CTE_NAMES, ids=CTE_IDS)
@pytest.mark.parametrize("operation", ["insert", "update", "delete"])
def test_cte_identifiers_preserve_failed_statement_rollback(driver, name, operation):
    """Legal names cannot bypass the transaction around SQLite FAIL conflicts."""
    driver.conn.executemany(
        "INSERT INTO review_items VALUES (?, ?)", [(1, "one"), (2, "two")]
    ).close()
    driver.conn.execute(
        f"CREATE TRIGGER refuse_second BEFORE {operation} ON review_items "
        f"WHEN {'NEW' if operation == 'insert' else 'OLD'}.review_item_id = "
        f"{4 if operation == 'insert' else 2} "
        "BEGIN SELECT RAISE(FAIL, 'second row refused'); END"
    ).close()
    driver.conn.commit()
    statement = {
        "insert": f"INSERT INTO review_items SELECT value+2, 'changed' FROM {name}",
        "update": f"UPDATE review_items SET review_item_value='changed' WHERE review_item_id IN {name}",
        "delete": f"DELETE FROM review_items WHERE review_item_id IN {name}",
    }[operation]
    with pytest.raises(DatabaseDriverError, match="second row refused"):
        driver.direct_execute(
            f"\ufeff ; /* prefix */ ; WITH {name}(value) AS (VALUES (1), (2)) {statement}"
        )
    assert not driver.conn.in_transaction
    with closing(sqlite3.connect(driver.database_path)) as reader:
        assert reader.execute("SELECT * FROM review_items ORDER BY 1").fetchall() == [
            (1, "one"),
            (2, "two"),
        ]


@pytest.mark.parametrize("name", CTE_NAMES, ids=CTE_IDS)
def test_cte_identifiers_finish_returning_but_stream_selects(driver, name):
    with closing(
        driver.direct_execute(
            f"WITH RECURSIVE {name}(id) AS NOT MATERIALIZED (VALUES (1), (2)), "
            f"source2 AS (SELECT * FROM {name}) "
            "INSERT INTO review_items SELECT id, 'value' FROM source2 RETURNING review_item_id"
        )
    ) as cursor:
        with closing(sqlite3.connect(driver.database_path)) as reader:
            assert reader.execute("SELECT COUNT(*) FROM review_items").fetchone() == (
                2,
            )
        assert cursor.fetchall() == [(1,), (2,)]
    visits = []
    driver.conn.create_function(
        "visited", 1, lambda value: visits.append(value) or value
    )
    with closing(
        driver.direct_execute(
            f"WITH {name}(id) AS (VALUES (1), (2), (3)) SELECT visited(id) FROM {name}"
        )
    ) as cursor:
        assert visits == [1]
        assert cursor.fetchall() == [(1,), (2,), (3,)]
        assert visits == [1, 2, 3]
    assert not driver.conn.in_transaction


@pytest.mark.parametrize("name", CTE_NAMES, ids=CTE_IDS)
def test_cte_identifiers_reach_prompt_maintenance_and_reject_replace(driver, name):
    from types import SimpleNamespace

    from LiuXin_alpha.databases.db_types.maintenance_types import MaintenanceMode
    from LiuXin_alpha.databases.maintenance.bound_connection import (
        MaintenanceTransactionError,
    )
    from LiuXin_alpha.databases.maintenance.plugins import (
        MaintenancePluginBase,
        MaintenancePluginResult,
    )
    from LiuXin_alpha.databases.maintenance.runtime import MaintenanceRuntime

    class Prompt(MaintenancePluginBase):
        name = "cte_prompt"
        tables = ("review_items",)
        mode = MaintenanceMode.PROMPT
        fail = False

        def handle_events(self, context, events):
            if self.fail:
                raise RuntimeError("prompt failed")
            return MaintenancePluginResult(handled=len(events))

    plugin = Prompt()
    runtime = MaintenanceRuntime(
        SimpleNamespace(driver=driver, uuid="cte-test"), [plugin]
    )
    runtime.install()
    sql = f"; /* prefix */ WITH {name}(id) AS (VALUES (?)) INSERT INTO review_items SELECT id, 'value' FROM {name}"
    with closing(driver.get_connection()) as connection:
        with connection:
            connection.execute(sql, (1,)).close()
        plugin.fail = True
        with pytest.raises(RuntimeError, match="prompt failed"):
            with connection:
                connection.execute(sql, (2,)).close()
        plugin.fail = False
        with pytest.raises(MaintenanceTransactionError, match="REPLACE"):
            with connection:
                connection.execute(
                    f"WITH {name}(id) AS (VALUES (1)) INSERT OR REPLACE INTO review_items SELECT id, 'lost' FROM {name}"
                ).close()
    with closing(sqlite3.connect(driver.database_path)) as reader:
        assert reader.execute("SELECT * FROM review_items").fetchall() == [(1, "value")]


@pytest.mark.parametrize("operation", ["insert", "update", "delete"])
@pytest.mark.parametrize("pending", [False, True], ids=["idle", "pending"])
@pytest.mark.parametrize("failure", ["iterator", "sql", "success"])
def test_cte_batch_owns_a_real_transaction(driver, operation, pending, failure):
    """Leading WITH writes have the same durability contract as ordinary DML."""
    driver.conn.executemany(
        "INSERT INTO review_items VALUES (?, ?)", [(1, "one"), (2, "two")]
    ).close()
    driver.conn.execute(
        "CREATE TRIGGER refuse_three BEFORE " + operation.upper() + " ON review_items "
        "WHEN " + ("OLD" if operation == "delete" else "NEW") + ".review_item_id = 3 "
        "BEGIN SELECT RAISE(ABORT, 'refused'); END"
    ).close()
    if operation != "insert":
        driver.conn.execute("INSERT INTO review_items VALUES (3, 'three')").close()
    driver.conn.commit()
    before = driver.conn.execute("SELECT * FROM review_items ORDER BY 1").fetchall()
    if pending:
        driver.conn.execute("INSERT INTO review_items VALUES (9, 'pending')").close()
    sql = {
        "insert": "INSERT INTO review_items SELECT id, 'new' FROM supplied",
        "update": "UPDATE review_items SET review_item_value = 'changed' WHERE review_item_id IN supplied",
        "delete": "DELETE FROM review_items WHERE review_item_id IN supplied",
    }[operation]
    visits = []

    def rows():
        visits.append(1)
        yield (4 if operation == "insert" else 1,)
        if failure == "iterator":
            raise ValueError("late input error")
        visits.append(2)
        yield (3 if failure == "sql" else (5 if operation == "insert" else 2),)

    statement = "/* batch */ WITH supplied(id) AS (VALUES (?)) " + sql
    if failure == "success":
        driver.direct_executemany(statement, rows())
    else:
        with pytest.raises(DatabaseDriverError) as caught:
            driver.direct_executemany(statement, rows())
        assert isinstance(
            caught.value.__cause__,
            ValueError if failure == "iterator" else sqlite3.IntegrityError,
        )
    assert visits == ([1] if failure == "iterator" else [1, 2])
    assert not driver.conn.in_transaction
    with closing(sqlite3.connect(driver.database_path)) as reader:
        stored = reader.execute("SELECT * FROM review_items ORDER BY 1").fetchall()
    if failure != "success":
        assert stored == before
    else:
        expected = {
            "insert": [(1, "one"), (2, "two"), (4, "new"), (5, "new")],
            "update": [(1, "changed"), (2, "changed"), (3, "three")],
            "delete": [(3, "three")],
        }[operation]
        assert stored == expected + ([(9, "pending")] if pending else [])


@pytest.mark.parametrize("operation", ["insert", "update", "delete"])
@pytest.mark.parametrize("cte", [False, True], ids=["plain", "cte"])
def test_returning_write_commits_before_cursor_consumption(driver, operation, cte):
    """Returning rows remain readable after commit and early close loses no writes."""
    driver.conn.executemany(
        "INSERT INTO review_items VALUES (?, ?)", [(1, "one"), (2, "two")]
    ).close()
    driver.conn.commit()
    statements = {
        "insert": "INSERT INTO review_items VALUES (3, 'three'), (4, 'four')",
        "update": "UPDATE review_items SET review_item_value = 'updated'",
        "delete": "DELETE FROM review_items",
    }
    statement = ("WITH unused AS (SELECT 'RETURNING') " if cte else "") + statements[
        operation
    ]
    cursor = driver.direct_execute(statement + " RETURNING review_item_id")
    with closing(sqlite3.connect(driver.database_path)) as reader:
        actual = reader.execute("SELECT * FROM review_items ORDER BY 1").fetchall()
    assert (
        actual
        == {
            "insert": [(1, "one"), (2, "two"), (3, "three"), (4, "four")],
            "update": [(1, "updated"), (2, "updated")],
            "delete": [],
        }[operation]
    )
    assert cursor.fetchone() == ((3,) if operation == "insert" else (1,))
    assert cursor.fetchall() == ([(4,)] if operation == "insert" else [(2,)])
    assert cursor.fetchone() is None
    cursor.close()
    cursor.close()
    with closing(
        driver.direct_execute(
            "INSERT INTO review_items VALUES (7, 'seven') RETURNING review_item_id"
        )
    ):
        pass
    with closing(sqlite3.connect(driver.database_path)) as reader:
        assert reader.execute(
            "SELECT review_item_value FROM review_items WHERE review_item_id=7"
        ).fetchone() == ("seven",)


def test_returning_write_failure_rolls_back_pending_work(driver):
    driver.conn.execute("INSERT INTO review_items VALUES (1, 'pending')").close()
    with pytest.raises(DatabaseDriverError):
        driver.direct_execute(
            "INSERT INTO review_items VALUES (2, 'ok'), (1, 'duplicate') RETURNING review_item_id"
        )
    with closing(sqlite3.connect(driver.database_path)) as reader:
        assert reader.execute("SELECT * FROM review_items").fetchall() == []


@pytest.mark.parametrize("name", ["n", "replace", '"insert"'])
def test_select_results_remain_streaming(driver, name):
    """A Python callback makes fetching ahead observably different from streaming."""
    visits = []
    driver.conn.create_function(
        "observe", 1, lambda value: visits.append(value) or value
    )
    cursor = driver.direct_execute(
        f"WITH RECURSIVE {name}(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM {name} WHERE x<100) SELECT observe(x) FROM {name}"
    )
    assert len(visits) < 100
    assert cursor.fetchone() == (1,)
    cursor.close()
    assert len(visits) < 100


@pytest.mark.parametrize("cte", [False, True], ids=["plain", "cte"])
def test_returning_consumption_failure_does_not_publish_writes(driver, cte):
    """A Python row factory can fail while draining an already executed write."""
    previous = driver.conn.row_factory

    def fail_late(cursor, row):
        if cursor.description[0][0] == "review_item_id" and row[0] == 2:
            raise ValueError("late row conversion failure")
        return row

    driver.conn.row_factory = fail_late
    try:
        with pytest.raises(DatabaseDriverError):
            driver.direct_execute(
                ("WITH unused AS (SELECT 1) " if cte else "")
                + "INSERT INTO review_items VALUES (1, 'one'), (2, 'two') RETURNING review_item_id"
            )
    finally:
        driver.conn.row_factory = previous
    with closing(sqlite3.connect(driver.database_path)) as reader:
        assert reader.execute("SELECT * FROM review_items").fetchall() == []


@pytest.mark.parametrize("db_type", ["SQLite", "SQLite_apsw"])
def test_catalogue_wrapper_owns_cte_batches_and_returning(tmp_path, db_type):
    """The public WEMI facade works without optional maintenance protection."""
    if db_type == "SQLite_apsw":
        pytest.importorskip("apsw")
    from LiuXin_alpha.databases.database import Database

    path = tmp_path / "catalogue.sqlite"
    with closing(
        Database(
            metadata={"database_path": str(path)},
            db_type=db_type,
            create=True,
            repair_bootstrap_rows=False,
            enable_storage_manager=False,
            enable_maintenance=False,
        )
    ) as db:
        db.driver_wrapper.execute(
            "CREATE TABLE review_items(id INTEGER PRIMARY KEY)"
        ).close()

        def rows():
            yield (1,)
            raise ValueError("late input error")

        with pytest.raises(DatabaseDriverError):
            db.driver_wrapper.executemany(
                "WITH supplied(id) AS (VALUES (?)) INSERT INTO review_items SELECT * FROM supplied",
                rows(),
            )
        with closing(sqlite3.connect(path)) as reader:
            assert reader.execute("SELECT * FROM review_items").fetchall() == []
        with closing(
            db.driver_wrapper.execute(
                "WITH supplied(id) AS (VALUES (2)) INSERT INTO review_items SELECT * FROM supplied RETURNING id"
            )
        ) as cursor:
            with closing(sqlite3.connect(path)) as reader:
                assert reader.execute("SELECT * FROM review_items").fetchall() == [(2,)]
            assert cursor.fetchall() == [(2,)]


@pytest.mark.parametrize("operation", ["delete", "read"])
def test_book_group_failure_releases_owned_connection(driver, operation):
    driver.conn.execute(
        "CREATE TABLE new_books(new_book_id INTEGER PRIMARY KEY, new_book_group_id INTEGER)"
    ).close()
    driver.conn.execute("INSERT INTO new_books VALUES (1, 1)").close()
    driver.conn.execute(
        "CREATE TRIGGER protect_group BEFORE DELETE ON new_books BEGIN SELECT RAISE(ABORT, 'protected'); END"
    ).close()
    driver.conn.commit()
    if operation == "read":
        driver.conn.execute("DROP TABLE new_books").close()
    with pytest.raises(sqlite3.DatabaseError):
        if operation == "delete":
            driver.direct_delete_book_group(1)
        else:
            driver.direct_get_next_book_group()
    assert tuple(driver._open_connections) == (driver.conn,)
    with closing(sqlite3.connect(driver.database_path, timeout=0.1)) as writer:
        writer.execute("INSERT INTO review_items VALUES (1, 'still writable')")
        writer.commit()
        if operation == "delete":
            assert writer.execute("SELECT * FROM new_books").fetchall() == [(1, 1)]


@pytest.mark.parametrize(
    "layout", ["primary", "composite", "quoted", "partial", "expression", "nonunique"]
)
def test_unique_layout_matches_physical_schema_and_cardinality(driver, layout):
    from types import SimpleNamespace

    from LiuXin_alpha.databases.driver_wrapper import DriverWrapper

    definition = (
        "left_id INTEGER"
        + (" PRIMARY KEY" if layout == "primary" else "")
        + ", right_id INTEGER"
    )
    if layout == "composite":
        definition += ", PRIMARY KEY(left_id, right_id)"
    driver.conn.execute("CREATE TABLE review_links(" + definition + ")").close()
    indexes = {
        "quoted": 'CREATE UNIQUE INDEX "a valid ""index" ON review_links(left_id)',
        "partial": "CREATE UNIQUE INDEX partial_key ON review_links(left_id) WHERE right_id>0",
        "expression": "CREATE UNIQUE INDEX expression_key ON review_links((left_id+1))",
        "nonunique": "CREATE INDEX ordinary ON review_links(left_id)",
    }
    if layout in indexes:
        driver.conn.execute(indexes[layout]).close()
    driver.conn.commit()
    driver.conn.execute("INSERT INTO review_items VALUES (1, 'pending')").close()
    expected = (
        (("left_id", "right_id"),)
        if layout == "composite"
        else ((("left_id",),) if layout in {"primary", "quoted"} else ())
    )
    assert driver._get_unique_column_groups("review_links") == expected
    physical = driver.direct_get_physical_table_spec("review_links")
    if layout in {"primary", "composite"}:
        assert expected == (physical.primary_key,)
    shape, _ = DriverWrapper._link_constraint_shape(
        SimpleNamespace(driver=driver), "review_links", "left_id", "right_id", None
    )
    assert shape.value == (
        "many_to_many"
        if layout == "composite"
        else ("many_to_one" if expected else "unknown")
    )
    assert driver.conn.in_transaction
    with closing(sqlite3.connect(driver.database_path)) as reader:
        assert reader.execute("SELECT * FROM review_items").fetchall() == []
    driver.conn.rollback()


@pytest.fixture(params=["SQLite", "SQLite_apsw"])
def driver(request, tmp_path):
    """Use real connections without paying for an unrelated catalogue schema."""
    if request.param == "SQLite_apsw":
        pytest.importorskip("apsw")
    instance = load_database_driver(request.param)(
        {"database_path": str(tmp_path / "source.sqlite")}
    )
    instance.conn.execute(
        "CREATE TABLE review_items (review_item_id INTEGER PRIMARY KEY, "
        "review_item_value TEXT)"
    ).close()
    instance.conn.commit()
    try:
        yield instance
    finally:
        instance.close()


@pytest.mark.parametrize("failure", ["iterator", "adapter", "constraint"])
def test_executemany_failure_never_commits_or_retries_a_prefix(driver, failure):
    """A failure after one good row must not turn into a successful partial batch."""
    attempts = []

    class FailsOnce:
        def __conform__(self, protocol):
            attempts.append(protocol)
            if len(attempts) == 1:
                raise ValueError("conversion failed")
            return "retry must not happen"

    def broken_rows():
        yield (1, "prefix")
        raise ValueError("input failed")

    batches = {
        "iterator": broken_rows(),
        # Auto-assigned IDs let a retry duplicate the prefix without hitting a
        # uniqueness error first. The adapter succeeds on a forbidden retry.
        "adapter": [("prefix",), (FailsOnce(),)],
        "constraint": [(1, "prefix"), (1, "duplicate")],
    }
    sql = (
        "INSERT INTO review_items(review_item_value) VALUES (?)"
        if failure == "adapter"
        else "INSERT INTO review_items VALUES (?, ?)"
    )
    with pytest.raises(DatabaseDriverError) as error:
        driver.direct_executemany(sql, batches[failure])
    cause = sqlite3.IntegrityError if failure == "constraint" else ValueError
    assert isinstance(error.value.__cause__, cause)
    if failure == "adapter":
        assert len(attempts) == 1
    assert not driver.conn.in_transaction
    with closing(sqlite3.connect(driver.database_path)) as reader:
        assert reader.execute("SELECT * FROM review_items").fetchall() == []
    assert driver.conn.execute("SELECT * FROM review_items").fetchall() == []


@pytest.mark.parametrize("shape", ["rows", "scalars", "iterator", "mappings"])
def test_executemany_consumes_supported_batches_once(driver, shape):
    """Scalar tuples, binding rows and a single-use iterator retain their values."""
    visits = []

    def once():
        for value in ("one", "two"):
            visits.append(value)
            yield (value,)

    values = {
        "rows": [("one",), ("two",)],
        "scalars": ("one", "two"),
        "iterator": once(),
        "mappings": [{"value": "one"}, {"value": "two"}],
    }
    placeholder = ":value" if shape == "mappings" else "?"
    driver.direct_executemany(
        f"INSERT INTO review_items(review_item_value) VALUES ({placeholder})",
        values[shape],
    )
    if shape == "iterator":
        assert visits == ["one", "two"]
    with closing(sqlite3.connect(driver.database_path)) as reader:
        assert reader.execute(
            "SELECT review_item_value FROM review_items ORDER BY review_item_id"
        ).fetchall() == [("one",), ("two",)]


def test_scratch_switching_is_not_a_database_capability(driver):
    """Do not restore an API that claims locking while redirecting only a path."""
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.api.driver_api import DatabaseDriverAPI
    from LiuXin_alpha.databases.database import Database

    assert not hasattr(DatabaseAPI, "lock_writing")
    assert not hasattr(Database, "lock_writing")
    assert not hasattr(DatabaseDriverAPI, "make_scratch")
    assert not hasattr(driver, "make_scratch")


def test_verified_backup_opens_as_an_independent_driver(driver, tmp_path):
    """Working on a copy includes committed WAL data and never retargets the source."""
    source = driver.database_path
    primary = driver.conn
    primary.execute("PRAGMA journal_mode=WAL").close()
    primary.execute("PRAGMA wal_autocheckpoint=0").close()
    primary.execute("INSERT INTO review_items VALUES (1, 'committed')").close()
    primary.commit()
    primary.execute("INSERT INTO review_items VALUES (2, 'pending')").close()
    copy_path = driver.direct_backup(tmp_path / "working-copy.sqlite")
    with closing(type(driver)({"database_path": str(copy_path)})) as copy:
        assert copy.conn.execute("SELECT * FROM review_items").fetchall() == [
            (1, "committed")
        ]
        copy.direct_execute("INSERT INTO review_items VALUES (3, 'copy only')").close()
        assert copy.conn.execute("PRAGMA database_list").fetchone()[2] == str(copy_path)
    assert driver.database_path == source
    assert driver.conn is primary
    assert primary.in_transaction
    assert primary.execute("PRAGMA database_list").fetchone()[2] == source
    primary.rollback()
    driver.direct_execute("INSERT INTO review_items VALUES (4, 'source only')").close()
    with closing(sqlite3.connect(source)) as reader:
        assert reader.execute(
            "SELECT * FROM review_items ORDER BY review_item_id"
        ).fetchall() == [(1, "committed"), (4, "source only")]
    with closing(sqlite3.connect(copy_path)) as reader:
        assert reader.execute(
            "SELECT * FROM review_items ORDER BY review_item_id"
        ).fetchall() == [(1, "committed"), (3, "copy only")]


def test_backup_contains_committed_wal_and_preserves_pending_work(driver, tmp_path):
    """A readable, successful backup must include committed WAL content."""
    primary = driver.conn
    primary.execute("PRAGMA journal_mode=WAL").close()
    primary.execute("PRAGMA wal_autocheckpoint=0").close()
    primary.execute("INSERT INTO review_items VALUES (1, 'committed')").close()
    primary.commit()
    primary.execute("INSERT INTO review_items VALUES (2, 'pending')").close()
    destination = tmp_path / "backup.sqlite"
    assert str(driver.direct_backup(destination)) == str(destination)
    with closing(sqlite3.connect(destination)) as reader:
        assert reader.execute("SELECT * FROM review_items").fetchall() == [
            (1, "committed")
        ]
        assert reader.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
    assert primary.in_transaction
    assert primary.execute("SELECT COUNT(*) FROM review_items").fetchone() == (2,)
    primary.rollback()
    assert primary.execute("SELECT COUNT(*) FROM review_items").fetchone() == (1,)


@pytest.mark.parametrize("destination_kind", ["existing", "source", "wal", "symlink"])
def test_backup_never_overwrites_an_existing_or_source_entry(
    driver, tmp_path, destination_kind
):
    """Backup publication is create-only and cannot touch source coordination files."""
    source = tmp_path / "source.sqlite"
    destination = tmp_path / "backup.sqlite"
    if destination_kind == "existing":
        destination.write_bytes(b"existing recovery copy")
    elif destination_kind == "source":
        destination = source
    elif destination_kind == "wal":
        destination = tmp_path / "source.sqlite-wal"
    else:
        destination.symlink_to(source)
    source_before = source.read_bytes()
    destination_before = destination.read_bytes() if destination.exists() else None
    with pytest.raises((FileExistsError, ValueError)):
        driver.direct_backup(destination)
    assert source.read_bytes() == source_before
    if destination_before is not None:
        assert destination.read_bytes() == destination_before
    else:
        assert not destination.exists()


@pytest.mark.parametrize(
    "operation", ["max", "min", "values", "execute", "table_sql", "tree"]
)
@pytest.mark.parametrize("fails", [False, True])
def test_materialized_direct_helpers_close_owned_connections(
    driver, monkeypatch, operation, fails
):
    """Success and SQL failure release handles before the driver is shut down."""
    driver.conn.execute("INSERT INTO review_items VALUES (1, 'value')").close()
    if operation == "tree":
        driver.conn.execute(
            "CREATE TABLE review_nodes (review_node_id INTEGER PRIMARY KEY, "
            "review_node TEXT, review_node_parent INTEGER, review_node_tree_id TEXT)"
        ).close()
        driver.conn.execute(
            "INSERT INTO review_nodes VALUES (1, 'root', NULL, NULL)"
        ).close()
    driver.conn.commit()
    opened = []
    acquire = driver.get_connection

    def capture():
        connection = acquire()
        opened.append(connection)
        if fails:
            connection.set_authorizer(lambda *_: sqlite3.SQLITE_DENY)
        return connection

    monkeypatch.setattr(driver, "get_connection", capture)
    actions = {
        "max": lambda: driver.direct_get_max("review_item_id"),
        "min": lambda: driver.direct_get_min("review_item_id"),
        "values": lambda: driver.direct_get_all_values(
            "review_items", "review_item_value"
        ),
        "execute": lambda: driver.direct_execute_sql("SELECT 1"),
        "table_sql": lambda: driver.direct_get_table_sqlite("review_items"),
        "tree": lambda: driver.direct_set_tree_ids("review_nodes"),
    }
    if fails:
        with pytest.raises(sqlite3.DatabaseError):
            actions[operation]()
    else:
        for _ in range(12):
            actions[operation]()
    gc.collect()
    assert opened
    for connection in opened:
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            connection.execute("SELECT 1")
    assert tuple(driver._open_connections) == (driver.conn,)
    assert driver.conn.execute("SELECT COUNT(*) FROM review_items").fetchone() == (1,)


def test_borrowed_table_sql_connection_keeps_pending_work(driver):
    """An explicitly supplied schema reader remains owned by its caller."""
    primary = driver.conn
    primary.execute("INSERT INTO review_items VALUES (1, 'pending')").close()
    assert "review_items" in driver.direct_get_table_sqlite("review_items", primary)
    assert primary.in_transaction
    primary.rollback()
    assert primary.execute("SELECT COUNT(*) FROM review_items").fetchone() == (0,)


def test_closed_temporary_handles_leave_tracking_but_live_handles_close_with_driver(
    driver,
):
    """Retaining closed Python objects must not grow the driver's registry."""
    retained = []
    for _ in range(30):
        connection = driver.get_connection()
        retained.append(connection)
        connection.close()
        connection.close()
        assert tuple(driver._open_connections) == (driver.conn,)
    live = [driver.get_connection(), driver.get_connection()]
    driver.close()
    assert not driver._open_connections
    for connection in live:
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            connection.execute("SELECT 1")


@pytest.mark.parametrize("completion", ["exhaust", "close", "sink_error"])
def test_dump_never_completes_the_primary_transaction(driver, completion):
    """Export consumption cannot commit or discard unrelated pending rows."""
    primary = driver.conn
    primary.execute("INSERT INTO review_items VALUES (1, 'pending')").close()
    dump = driver.sql_dump()
    try:
        if completion == "exhaust":
            lines = list(dump)
            assert any("pending" in line for line in lines)
        elif completion == "close":
            next(dump)
            dump.close()
        else:
            next(dump)
            with pytest.raises(OSError, match="sink failed"):
                dump.throw(OSError("sink failed"))
        assert driver.conn is primary
        assert primary.in_transaction
        assert primary.execute("SELECT COUNT(*) FROM review_items").fetchone() == (1,)
        with closing(sqlite3.connect(driver.database_path)) as reader:
            assert reader.execute("SELECT COUNT(*) FROM review_items").fetchone() == (
                0,
            )
    finally:
        dump.close()
    primary.rollback()
    assert primary.execute("SELECT COUNT(*) FROM review_items").fetchone() == (0,)


def test_backup_pins_one_snapshot_while_another_connection_commits(
    driver, tmp_path, monkeypatch
):
    """A writer between backup pages cannot produce a torn or mixed snapshot."""
    from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver import (
        sqlite_files,
    )

    driver.conn.execute("PRAGMA journal_mode=WAL").close()
    driver.conn.execute("INSERT INTO review_items VALUES (1, 'before')").close()
    driver.conn.commit()
    connect = sqlite3.connect
    committed = []

    class InterleavedBackup(sqlite3.Connection):
        def backup(self, target, *, pages=-1, **options):
            def write_during_backup(status, remaining, total):
                if not committed and remaining > 0:
                    assert status == sqlite3.SQLITE_OK and total > remaining
                    with closing(connect(driver.database_path)) as writer:
                        writer.execute(
                            "UPDATE review_items SET review_item_value='after'"
                        )
                        writer.execute("INSERT INTO review_items VALUES (2, 'after')")
                        writer.commit()
                    committed.append(True)

            return super().backup(
                target, pages=1, progress=write_during_backup, **options
            )

    def reader(*args, **kwargs):
        return connect(*args, **kwargs, factory=InterleavedBackup)

    monkeypatch.setattr(sqlite_files.sqlite3, "connect", reader)
    destination = tmp_path / "concurrent.sqlite"
    driver.direct_backup(destination)
    assert committed
    with closing(connect(destination)) as copied:
        assert copied.execute("SELECT * FROM review_items").fetchall() == [
            (1, "before")
        ]
    assert driver.conn.execute(
        "SELECT * FROM review_items ORDER BY review_item_id"
    ).fetchall() == [
        (1, "after"),
        (2, "after"),
    ]


@pytest.mark.parametrize("failure", ["validation", "publication_race"])
def test_failed_backup_never_publishes_partial_data(
    driver, tmp_path, monkeypatch, failure
):
    from LiuXin_alpha.databases import creation
    from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver import (
        sqlite_files,
    )

    destination = tmp_path / "backup.sqlite"
    driver.conn.execute("INSERT INTO review_items VALUES (1, 'original')").close()
    driver.conn.commit()
    if failure == "validation":

        def reject(_):
            raise RuntimeError("validation rejected")

        monkeypatch.setattr(sqlite_files, "validate_sqlite_connection", reject)
        error = RuntimeError
    else:
        link = creation.os.link

        def competing_publication(source, target):
            destination.write_bytes(b"competing recovery copy")
            return link(source, target)

        monkeypatch.setattr(creation.os, "link", competing_publication)
        error = FileExistsError
    with pytest.raises(error):
        driver.direct_backup(destination)
    if failure == "validation":
        assert not destination.exists()
    else:
        assert destination.read_bytes() == b"competing recovery copy"
    assert not list(tmp_path.glob(".liuxin-create-*"))
    assert driver.conn.execute("SELECT * FROM review_items").fetchall() == [
        (1, "original")
    ]


def test_failed_connection_configuration_closes_the_partial_handle(driver, monkeypatch):
    import importlib

    module = importlib.import_module(type(driver).__module__)
    connection_type = module.SQLite_Connection
    opened = []

    class Captured(connection_type):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            opened.append(self)

    def reject(_):
        raise RuntimeError("configuration rejected")

    monkeypatch.setattr(module, "SQLite_Connection", Captured)
    monkeypatch.setattr(driver, "_configure_progress_handler", reject)
    with pytest.raises(RuntimeError, match="configuration rejected"):
        driver.get_connection()
    assert len(opened) == 1
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        opened[0].execute("SELECT 1")
    assert tuple(driver._open_connections) == (driver.conn,)


def test_failed_close_remains_owned_for_retry(driver, monkeypatch):
    connection = driver.get_connection()
    close = connection.close
    failures = [True]

    def fail_once():
        if failures:
            failures.pop()
            raise RuntimeError("close interrupted")
        close()

    monkeypatch.setattr(connection, "close", fail_once)
    driver.close()
    assert tuple(driver._open_connections) == (connection,)
    assert connection.execute("SELECT 1").fetchone() == (1,)
    driver.close()
    assert not driver._open_connections
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        connection.execute("SELECT 1")


@pytest.mark.parametrize("operation", ["execute", "refresh"])
def test_cancelled_liveness_probe_keeps_the_primary_transaction(driver, operation):
    primary = driver.conn
    primary.execute("INSERT INTO review_items VALUES (1, 'pending')").close()
    driver.set_progress_callbacks((lambda: True,), instruction_interval=1)
    try:
        with pytest.raises(sqlite3.OperationalError, match="interrupted"):
            if operation == "execute":
                driver.direct_execute("SELECT 2")
            else:
                driver.refresh()
    finally:
        driver.set_progress_callbacks()
    assert driver.conn is primary
    assert primary.in_transaction
    assert primary.execute("SELECT COUNT(*) FROM review_items").fetchone() == (1,)
    primary.rollback()


def test_concurrent_temporary_connection_closes_preserve_tracking(driver):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    rendezvous = Barrier(4)

    def use_connections():
        for _ in range(12):
            with closing(driver.get_connection()) as connection:
                assert connection.execute("SELECT 1").fetchone() == (1,)
                rendezvous.wait(timeout=10)

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(use_connections) for _ in range(4)]
        for future in futures:
            future.result(timeout=30)
    assert tuple(driver._open_connections) == (driver.conn,)
