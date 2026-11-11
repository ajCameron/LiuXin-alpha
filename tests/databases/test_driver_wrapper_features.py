"""Exercise portable wrapper features against real SQLite/APSW databases."""

from contextlib import closing

import pytest

from LiuXin_alpha.databases.db_types.backend_capabilities import BackendFeature
from LiuXin_alpha.databases.driver_wrapper import diagnostics
from LiuXin_alpha.databases.transactions import transaction_active
from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError
from tests.databases import test_row_write_safety as fixtures

db = fixtures.db
template = fixtures.template


def populate(db):
    connection = db.driver.conn
    connection.execute(
        "CREATE TABLE feature_rows (feature_row_id INTEGER PRIMARY KEY, value TEXT NOT NULL, category TEXT DEFAULT 'book')"
    ).close()
    connection.executemany(
        "INSERT INTO feature_rows(feature_row_id,value) VALUES (?,?)",
        [(number, f"row {number}") for number in range(1, 8)],
    ).close()
    connection.commit()
    db.driver_wrapper.get_tables(force_refresh=True)


def test_physical_schema_and_capabilities(db):
    populate(db)
    wrapper = db.driver_wrapper
    spec = wrapper.get_table_spec("feature_rows", force_refresh=True)
    columns = {column.name: column for column in spec.columns}
    assert spec.primary_key == ("feature_row_id",)
    assert spec.id_column == "feature_row_id"
    assert spec.schema == getattr(db.driver, "schema", "main")
    assert columns["feature_row_id"].is_primary_key
    assert columns["feature_row_id"].nullable is False
    assert columns["value"].nullable is False
    assert columns["category"].has_default
    assert "book" in columns["category"].default_sql
    capabilities = wrapper.get_backend_capabilities()
    assert capabilities.version
    assert capabilities.supports(BackendFeature.PHYSICAL_SCHEMA)
    assert capabilities.supports(BackendFeature.READ_SNAPSHOTS)
    capabilities.require(BackendFeature.SAVEPOINTS)
    if capabilities.backend == "PostgreSQL":
        assert not capabilities.supports(BackendFeature.CUSTOM_COLUMN_DDL)
        with pytest.raises(NotImplementedError, match="custom_column_ddl"):
            capabilities.require(BackendFeature.CUSTOM_COLUMN_DDL)
    else:
        assert capabilities.max_bind_parameters > 0


def test_fetches_preserve_pending_primary_writes(db):
    populate(db)
    connection = db.driver.conn
    connection.execute(
        "UPDATE feature_rows SET value='pending' WHERE feature_row_id=1"
    ).close()
    wrapper = db.driver_wrapper
    assert wrapper.fetch_one(
        "SELECT value FROM feature_rows WHERE feature_row_id=?", (1,)
    ) == ("pending",)
    assert (
        wrapper.fetch_scalar(
            "SELECT value FROM feature_rows WHERE feature_row_id=?",
            (1,),
            connection=connection,
        )
        == "pending"
    )
    assert wrapper.fetch_all(
        "SELECT value FROM feature_rows WHERE feature_row_id=1", connection=connection
    ) == [("pending",)]
    assert (
        wrapper.fetch_one("SELECT value FROM feature_rows WHERE feature_row_id=-1")
        is None
    )
    assert wrapper.fetch_scalar("SELECT NULL") is None
    assert wrapper.fetch_scalar("SELECT '100%' AS value") == "100%"
    assert wrapper.fetch_all("SELECT '100%' AS value") == [("100%",)]
    with pytest.raises(InputIntegrityError, match="one result column"):
        wrapper.fetch_scalar("SELECT 1, 2")
    assert transaction_active(connection)
    with closing(db.driver.get_connection()) as reader:
        assert reader.execute(
            "SELECT value FROM feature_rows WHERE feature_row_id=1"
        ).fetchone() == ("row 1",)
    connection.rollback()


def test_reads_and_schema_join_active_macro_connection(db):
    populate(db)
    with pytest.raises(RuntimeError, match="undo"):
        with db.macros.transaction() as connection:
            connection.execute(
                "UPDATE feature_rows SET value='macro' WHERE feature_row_id=1"
            ).close()
            connection.execute(
                "CREATE TABLE feature_pending (arbitrary_key INTEGER PRIMARY KEY, required TEXT NOT NULL)"
            ).close()
            wrapper = db.driver_wrapper
            assert (
                wrapper.fetch_scalar(
                    "SELECT value FROM feature_rows WHERE feature_row_id=1"
                )
                == "macro"
            )
            spec = wrapper.get_table_spec("feature_pending")
            assert spec.id_column == "arbitrary_key"
            assert spec.columns[1].nullable is False
            assert (
                wrapper.get_table_spec("feature_pending", connection=connection) == spec
            )
            raise RuntimeError("undo")
    with pytest.raises(ValueError, match="No such relation"):
        db.driver_wrapper.get_table_spec("feature_pending", force_refresh=True)


def test_scans_project_filter_and_resume_without_skipping(db):
    populate(db)
    wrapper = db.driver_wrapper
    page = wrapper.scan_page("feature_rows", columns=["value"], batch_size=3)
    assert page.rows == tuple({"value": f"row {number}"} for number in range(1, 4))
    assert page.next_after_id == 3 and not page.complete
    following = list(
        wrapper.iter_row_batches(
            "feature_rows", columns=["value"], batch_size=2, after_id=page.next_after_id
        )
    )
    assert [len(part.rows) for part in following] == [2, 2]
    assert following[-1].complete
    assert [row["value"] for part in following for row in part.rows] == [
        f"row {number}" for number in range(4, 8)
    ]
    assert (
        list(wrapper.iter_row_batches("feature_rows", where={"category": None})) == []
    )
    assert wrapper.scan_page("feature_rows", after_id=100).next_after_id == 100


def test_scans_define_changes_between_pages(db):
    populate(db)
    wrapper = db.driver_wrapper
    first = wrapper.scan_page("feature_rows", batch_size=3)
    connection = db.driver.conn
    connection.execute("DELETE FROM feature_rows WHERE feature_row_id IN (2,4)").close()
    connection.execute(
        "UPDATE feature_rows SET value='edited' WHERE feature_row_id=5"
    ).close()
    connection.execute(
        "INSERT INTO feature_rows(feature_row_id,value) VALUES (8,'new')"
    ).close()
    connection.commit()
    rest = list(
        wrapper.iter_row_batches(
            "feature_rows", batch_size=2, after_id=first.next_after_id
        )
    )
    assert [row["feature_row_id"] for page in rest for row in page.rows] == [5, 6, 7, 8]
    assert rest[0].rows[0]["value"] == "edited"


def test_snapshot_scan_preserves_original_rows_during_other_writer(db):
    populate(db)
    if db.driver_wrapper.get_backend_capabilities().backend == "SQLite":
        db.driver.conn.execute("PRAGMA journal_mode=WAL").close()
    db.driver.conn.rollback()
    with db.macros.read_transaction() as snapshot:
        first = db.driver_wrapper.scan_page(
            "feature_rows", batch_size=3, connection=snapshot
        )
        with closing(db.driver.get_connection()) as writer:
            writer.execute(
                "UPDATE feature_rows SET value='concurrent' WHERE feature_row_id=5"
            ).close()
            writer.execute(
                "INSERT INTO feature_rows(feature_row_id,value) VALUES (8,'new')"
            ).close()
            writer.commit()
        remaining = list(
            db.driver_wrapper.iter_row_batches(
                "feature_rows", after_id=first.next_after_id, connection=snapshot
            )
        )
        assert [row["feature_row_id"] for page in remaining for row in page.rows] == [
            4,
            5,
            6,
            7,
        ]
        assert remaining[0].rows[1]["value"] == "row 5"


@pytest.mark.parametrize(
    "options",
    [
        {"batch_size": 0},
        {"batch_size": True},
        {"after_id": "1"},
        {"columns": []},
        {"columns": ["missing"]},
        {"where": {"missing": 1}},
    ],
)
def test_scan_rejects_invalid_requests(db, options):
    populate(db)
    with pytest.raises(InputIntegrityError):
        db.driver_wrapper.scan_page("feature_rows", **options)


def test_diagnostics_disabled_has_no_timer_and_observers_are_isolated(db, monkeypatch):
    wrapper = db.driver_wrapper
    if wrapper.get_backend_capabilities().backend == "PostgreSQL":
        from psycopg2 import DatabaseError
    else:
        from sqlite3 import DatabaseError

    def forbidden():
        raise AssertionError("disabled diagnostics read the clock")

    with monkeypatch.context() as disabled:
        disabled.setattr(diagnostics, "perf_counter", forbidden)
        assert wrapper.fetch_scalar("SELECT 1") == 1
    seen = []

    def bad(event):
        raise ValueError("observer fault")

    def recursive(event):
        assert wrapper.fetch_scalar("SELECT 2") == 2

    wrapper.set_query_observers((bad, recursive, seen.append))
    assert wrapper.fetch_all("SELECT 3") == [(3,)]
    assert len(seen) == 1
    assert seen[0].operation == "fetch_all"
    assert seen[0].rows_read == 1
    assert seen[0].duration_seconds >= 0
    assert seen[0].exception_type is None
    with pytest.raises(DatabaseError) as caught:
        wrapper.fetch_one("SELECT * FROM feature_missing_relation")
    assert len(seen) == 2 and seen[-1].exception_type is not None
    assert seen[-1].exception_type == type(caught.value).__name__
    db.driver.conn.rollback()
    wrapper.set_query_observers()
    wrapper.fetch_one("SELECT 4")
    assert len(seen) == 2


def test_diagnostics_legacy_reads_writes_and_page_counts(db):
    populate(db)
    seen = []
    wrapper = db.driver_wrapper
    wrapper.set_query_observers((seen.append,))
    wrapper.get_all_rows("feature_rows")
    wrapper.update_column("feature_rows", 1, "value", "changed")
    wrapper.scan_page("feature_rows", batch_size=2)
    assert [(event.operation, event.rows_read) for event in seen] == [
        ("get_all_rows", 7),
        ("update_column", None),
        ("scan_page", 2),
    ]


def test_view_reader_uses_pending_schema_and_rows(db):
    populate(db)
    with pytest.raises(RuntimeError, match="undo"):
        with db.macros.transaction() as connection:
            connection.execute(
                'CREATE VIEW "feature quoted view" AS SELECT feature_row_id AS id,value FROM feature_rows'
            ).close()
            connection.execute(
                "UPDATE feature_rows SET value='pending' WHERE feature_row_id=1"
            ).close()
            wrapper = db.driver_wrapper
            assert wrapper.get_view_row_from_id("feature quoted view", 1) == {
                "id": 1,
                "value": "pending",
            }
            assert (
                wrapper.get_view_row_from_id(
                    "feature quoted view", -1, connection=connection
                )
                is None
            )
            connection.execute(
                "CREATE VIEW feature_duplicates AS SELECT 1 AS id UNION ALL SELECT 1"
            ).close()
            with pytest.raises(DatabaseIntegrityError, match="multiple rows"):
                wrapper.get_view_row_from_id("feature_duplicates", 1)
            assert transaction_active(connection)
            raise RuntimeError("undo")


@pytest.mark.parametrize("method", ["walk", "get_linear_row_list"])
def test_hierarchy_cycles_raise_instead_of_looping(db, method):
    connection = db.driver.conn
    connection.execute(
        "CREATE TABLE feature_nodes(feature_node_id INTEGER PRIMARY KEY, feature_node_parent INTEGER)"
    ).close()
    connection.execute("INSERT INTO feature_nodes VALUES (1,2),(2,1)").close()
    connection.commit()
    db.driver_wrapper.get_tables(force_refresh=True)
    with pytest.raises(InputIntegrityError, match="Cycle"):
        list(
            getattr(db.driver_wrapper, method)(
                {"feature_node_id": 1, "feature_node_parent": 2}
            )
        )


class TrackedCursor:
    def __init__(self, cursor, fail_fetch=False, fail_iteration=False):
        self.cursor = cursor
        self.closed = False
        self.fail_fetch = fail_fetch
        self.fail_iteration = fail_iteration
        self.rows_read = 0

    def fetchone(self):
        if self.fail_fetch:
            raise RuntimeError("injected fetch failure")
        return self.cursor.fetchone()

    def fetchall(self):
        if self.fail_fetch:
            raise RuntimeError("injected fetch failure")
        return self.cursor.fetchall()

    def __iter__(self):
        if self.fail_iteration:
            raise RuntimeError("injected iteration failure")
        for row in self.cursor:
            self.rows_read += 1
            yield row

    def close(self):
        self.closed = True
        self.cursor.close()


class TrackedConnection:
    def __init__(self, connection, fail_fetch=False, fail_iteration=False):
        self.connection = connection
        self.cursors = []
        self.closed = False
        self.fail_fetch = fail_fetch
        self.fail_iteration = fail_iteration

    def execute(self, sql, values=()):
        cursor = TrackedCursor(
            self.connection.execute(sql, values),
            self.fail_fetch,
            self.fail_iteration and sql.startswith("SELECT * FROM"),
        )
        self.cursors.append(cursor)
        return cursor

    def commit(self):
        raise AssertionError("read helper committed a borrowed transaction")

    def rollback(self):
        raise AssertionError("read helper rolled back a borrowed transaction")

    def close(self):
        self.closed = True
        self.connection.close()


@pytest.mark.parametrize("operation", ["fetch_one", "fetch_all"])
@pytest.mark.parametrize("fail", [False, True])
def test_fetch_cursor_cleanup_and_borrowed_ownership(db, operation, fail):
    probe = TrackedConnection(db.driver.conn, fail_fetch=fail)
    if fail:
        with pytest.raises(RuntimeError, match="injected fetch"):
            getattr(db.driver_wrapper, operation)("SELECT 1", connection=probe)
    else:
        getattr(db.driver_wrapper, operation)("SELECT 1", connection=probe)
    assert probe.cursors and all(cursor.closed for cursor in probe.cursors)
    assert not probe.closed


def test_fetch_closes_only_a_fresh_connection(db, monkeypatch):
    fresh = db.driver.get_connection()
    probe = TrackedConnection(fresh)
    with monkeypatch.context() as patch:
        patch.setattr(db.driver, "conn", None)
        patch.setattr(db.driver, "get_connection", lambda: probe)
        assert db.driver_wrapper.fetch_scalar("SELECT 1") == 1
    assert probe.closed and all(cursor.closed for cursor in probe.cursors)
    assert db.driver.conn.execute("SELECT 2").fetchone() == (2,)


@pytest.mark.parametrize("failure", ["query", "conversion"])
def test_view_error_closes_all_cursors_without_ending_borrowed_work(
    db, monkeypatch, failure
):
    populate(db)
    db.driver.conn.execute(
        "CREATE VIEW feature_view AS SELECT feature_row_id AS id,value FROM feature_rows"
    ).close()
    db.driver.conn.commit()
    db.driver.conn.execute(
        "UPDATE feature_rows SET value='pending' WHERE feature_row_id=1"
    ).close()
    probe = TrackedConnection(db.driver.conn, fail_iteration=failure == "query")
    if failure == "conversion":

        def fail(*args, **kwargs):
            raise RuntimeError("injected conversion failure")

        monkeypatch.setattr(db.driver, "_coerce_db_value", fail)
    with pytest.raises(RuntimeError, match="injected"):
        db.driver_wrapper.get_view_row_from_id("feature_view", 1, connection=probe)
    assert probe.cursors and all(cursor.closed for cursor in probe.cursors)
    assert not probe.closed and transaction_active(db.driver.conn)
    db.driver.conn.rollback()


@pytest.mark.parametrize("kind", ["function", "object"])
def test_async_observer_is_rejected_without_replacing_existing_hooks(db, kind):
    seen = []
    db.driver_wrapper.set_query_observers((seen.append,))

    async def observe(event):
        return None

    class AsyncObserver:
        async def __call__(self, event):
            return None

    with pytest.raises(TypeError, match="synchronous"):
        db.driver_wrapper.set_query_observers(
            (observe if kind == "function" else AsyncObserver(),)
        )
    db.driver_wrapper.fetch_one("SELECT 1")
    assert len(seen) == 1


def test_scan_closes_resources_before_yield(db, monkeypatch):
    populate(db)
    probes = []
    get_connection = db.driver.get_connection

    def fresh():
        probe = TrackedConnection(get_connection())
        probes.append(probe)
        return probe

    with monkeypatch.context() as patch:
        patch.setattr(db.driver, "conn", None)
        patch.setattr(db.driver, "get_connection", fresh)
        batches = db.driver_wrapper.iter_row_batches("feature_rows", batch_size=2)
        first = next(batches)
        assert len(first.rows) == 2 and not first.complete
        assert probes and all(probe.closed for probe in probes)
        assert all(cursor.closed for probe in probes for cursor in probe.cursors)
        assert probes[-1].cursors[-1].rows_read == 3  # two returned, one lookahead
        # Early exit does not have to exhaust or close the generator to release
        # its database resources. Explicit close remains good caller hygiene.
        batches.close()


@pytest.mark.parametrize("key", [1.5, "bad key"])
def test_sqlite_scan_rejects_noninteger_stored_keys(db, key):
    connection = db.driver.conn
    connection.execute(
        "CREATE TABLE feature_bad_keys(id INT NOT NULL PRIMARY KEY)"
    ).close()
    connection.execute("INSERT INTO feature_bad_keys VALUES (?)", (key,)).close()
    with pytest.raises(InputIntegrityError, match="integer primary-key values"):
        db.driver_wrapper.scan_page("feature_bad_keys", batch_size=1)


@pytest.mark.parametrize("method", ["execute", "executemany"])
def test_postgres_adapter_closes_cursor_when_execution_fails(monkeypatch, method):
    from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
        PostgresConnectionAdapter,
    )

    error = RuntimeError("injected execution failure")

    class FailingCursor:
        closed = False

        def execute(self, *args):
            raise error

        executemany = execute

        def close(self):
            self.closed = True

    cursor = FailingCursor()
    adapter = PostgresConnectionAdapter(object())
    monkeypatch.setattr(adapter, "cursor", lambda: cursor)
    with pytest.raises(RuntimeError) as caught:
        getattr(adapter, method)(
            "SELECT ?", [(1,)] if method == "executemany" else (1,)
        )
    assert caught.value is error and cursor.closed
