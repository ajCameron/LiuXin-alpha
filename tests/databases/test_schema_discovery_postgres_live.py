"""Bound schema discovery work without weakening freshness or transaction ownership."""

from __future__ import annotations

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, nullcontext

import pytest

from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.connection import (
    PostgresConnectionAdapter,
)
from LiuXin_alpha.databases.driver_wrapper import DriverWrapper
from LiuXin_alpha.databases.transactions import transaction_active
from LiuXin_alpha.errors import DatabaseIntegrityError
from tests.databases.test_driver_plugin_postgres_safety import driver as driver


@pytest.fixture
def wrapper(driver):
    """Use native physical columns, interlinks, intralinks and a view in a tiny schema."""
    with driver.conn:
        for name in ("works", "agents"):
            base = driver.direct_get_column_base(name)
            driver.conn.execute(
                f'CREATE TABLE "{name}" ("{base}_id" INTEGER PRIMARY KEY, title TEXT)'
            ).close()
        driver.conn.execute(
            "CREATE TABLE agent_work_links (agent_work_link_id INTEGER PRIMARY KEY, "
            "agent_work_link_agent_id INTEGER REFERENCES agents(agent_id), "
            "agent_work_link_work_id INTEGER REFERENCES works(work_id), "
            "agent_work_link_type TEXT, agent_work_link_priority INTEGER, "
            "UNIQUE(agent_work_link_agent_id, agent_work_link_work_id))"
        ).close()
        driver.conn.execute(
            "CREATE TABLE work_work_intralinks (work_work_intralink_id INTEGER PRIMARY KEY, "
            "work_work_intralink_primary_id INTEGER REFERENCES works(work_id), "
            "work_work_intralink_secondary_id INTEGER REFERENCES works(work_id))"
        ).close()
        driver.conn.execute(
            "CREATE VIEW work_titles AS SELECT title FROM works"
        ).close()
    result = DriverWrapper(driver)
    result.main_tables = {"works", "agents"}
    result.interlink_tables = {"agent_work_links"}
    result.intralink_tables = {"work_work_intralinks"}
    result.helper_tables = {"schema_revision", "pending"}
    try:
        yield result
    finally:
        result.close()


@pytest.fixture
def probes(monkeypatch):
    """Count real fingerprint SQL, independently of how many getters are invoked."""
    counts = Counter()
    execute = PostgresConnectionAdapter.execute

    def counted(connection, sql, *args, **kwargs):
        if "as schema_fingerprint" in sql.lower():
            counts["fingerprints"] += 1
        return execute(connection, sql, *args, **kwargs)

    monkeypatch.setattr(PostgresConnectionAdapter, "execute", counted)
    return counts


@pytest.mark.parametrize("extra_tables", [0, 6])
def test_complete_schema_uses_bounded_probes_and_preserves_specs(
    driver, wrapper, probes, monkeypatch, extra_tables
):
    """Two real probes suffice as the table-pair search grows; all schema facts agree."""
    names = ("tags", "languages", "series", "subjects", "publishers", "identifiers")
    with driver.conn:
        for name in names[:extra_tables]:
            base = driver.direct_get_column_base(name)
            driver.conn.execute(
                f'CREATE TABLE "{name}" ("{base}_id" INTEGER PRIMARY KEY)'
            ).close()
            wrapper.main_tables.add(name)

    # Execute the original unscoped discovery path as a differential reference.
    with monkeypatch.context() as patch:
        patch.setattr(driver, "schema_read_scope", nullcontext)
        expected = wrapper.get_schema_spec(force_refresh=True)
    unscoped = probes["fingerprints"]
    probes.clear()
    actual = wrapper.get_schema_spec(force_refresh=True)
    assert actual == expected
    assert len(actual.interlinks) == len(actual.intralinks) == 1
    assert actual.tables["work_titles"].relation_kind.value == "view"
    assert actual.interlinks[0].typed and actual.interlinks[0].ordered
    assert unscoped > 20
    assert probes["fingerprints"] <= 2
    # Reusing a completed spec remains a cheap, explicitly refreshed contract.
    assert wrapper.get_schema_spec() is actual
    assert probes["fingerprints"] <= 2


def test_nested_scope_reuses_version_but_independent_reads_probe(driver, probes):
    with driver.schema_read_scope():
        version = driver.direct_get_schema_version()
        with driver.schema_read_scope():
            for _ in range(10):
                assert driver.direct_get_schema_version() == version
        assert probes["fingerprints"] == 1
    assert probes["fingerprints"] == 2
    assert driver.direct_get_schema_version() == version
    assert probes["fingerprints"] == 3
    with driver.schema_read_scope():
        assert driver.direct_get_schema_version() == version
    assert probes["fingerprints"] == 5


def test_external_ddl_is_visible_on_next_discovery(driver, wrapper):
    before = wrapper.get_schema_spec(force_refresh=True)
    driver.direct_get_declared_types_for_table("works")
    driver.conn.rollback()  # Release physical-reader locks before independent DDL.
    with closing(driver.get_connection()) as other:
        with other:
            other.execute("ALTER TABLE works ADD COLUMN external_value INTEGER").close()
    after = wrapper.get_schema_spec(force_refresh=True)
    assert "external_value" not in {
        column.name for column in before.tables["works"].columns
    }
    assert "external_value" in {column.name for column in after.tables["works"].columns}
    assert (
        driver.direct_get_declared_types_for_table("works")["external_value"]
        == "integer"
    )


def test_standalone_link_lookup_detects_ddl_after_scope(driver, wrapper):
    """A warm link-name cache must still detect an independent committed removal."""
    wrapper.get_schema_spec(force_refresh=True)
    assert wrapper.get_link_table_name("agents", "works") == "agent_work_links"
    driver.conn.rollback()
    with closing(driver.get_connection()) as other:
        with other:
            other.execute("DROP TABLE agent_work_links").close()
    assert wrapper.get_link_table_name("works", "agents") is False
    assert wrapper.get_schema_spec(force_refresh=True).interlinks == ()


def test_schema_change_during_discovery_rejects_partial_result(
    driver, wrapper, monkeypatch
):
    original = driver.direct_get_physical_table_spec
    changed = False

    def interleaved(table, **kwargs):
        nonlocal changed
        result = original(table, **kwargs)
        if not changed:
            changed = True
            with closing(driver.get_connection()) as other:
                with other:
                    other.execute(
                        "CREATE TABLE late_arrival (id INTEGER PRIMARY KEY)"
                    ).close()
        return result

    with monkeypatch.context() as patch:
        patch.setattr(driver, "direct_get_physical_table_spec", interleaved)
        with pytest.raises(
            DatabaseIntegrityError, match="Schema changed during discovery"
        ):
            wrapper.get_schema_spec(force_refresh=True)
    assert wrapper._cached_schema_spec is None
    assert wrapper._cached_table_specs == {}
    assert driver.tables_and_columns is None
    assert "late_arrival" in wrapper.get_schema_spec().tables


@pytest.mark.parametrize("phase", ["entry", "body", "exit", "cancel"])
def test_failed_discovery_preserves_pending_writes_and_recovers(
    driver, wrapper, monkeypatch, phase
):
    wrapper.get_schema_spec(force_refresh=True)
    connection = driver.conn
    connection.execute("INSERT INTO pending VALUES (42)").close()
    execute = PostgresConnectionAdapter.execute
    physical = driver.direct_get_physical_table_spec
    probes = 0

    def failing_execute(conn, sql, *args, **kwargs):
        nonlocal probes
        if "as schema_fingerprint" in sql.lower():
            probes += 1
            if (phase == "entry" and probes == 1) or (phase == "exit" and probes == 2):
                raise RuntimeError("failed fingerprint")
        return execute(conn, sql, *args, **kwargs)

    def failing_physical(table, **kwargs):
        if table == "works" and phase in {"body", "cancel"}:
            assert wrapper._cached_table_specs  # Fail after partial discovery.
            if phase == "cancel":
                raise KeyboardInterrupt("cancelled discovery")
            raise RuntimeError("failed physical discovery")
        return physical(table, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(PostgresConnectionAdapter, "execute", failing_execute)
        patch.setattr(driver, "direct_get_physical_table_spec", failing_physical)
        with pytest.raises(KeyboardInterrupt if phase == "cancel" else RuntimeError):
            wrapper.get_schema_spec(force_refresh=True)
    assert wrapper._cached_schema_spec is None
    assert wrapper._cached_table_specs == {}
    assert driver.tables_and_columns is None
    assert driver.conn is connection
    assert driver._open_connections == [connection]
    assert transaction_active(connection)
    assert connection.execute("SELECT value FROM pending").fetchall() == [(42,)]
    with closing(driver.get_connection()) as reader:
        assert reader.execute("SELECT value FROM pending").fetchall() == []
    assert "works" in wrapper.get_schema_spec().tables
    assert driver._open_connections == [connection]
    assert transaction_active(connection)
    connection.rollback()
    assert connection.execute("SELECT value FROM pending").fetchall() == []


def test_successful_discovery_preserves_pending_writes(driver, wrapper):
    connection = driver.conn
    connection.execute("INSERT INTO pending VALUES (42)").close()
    assert "works" in wrapper.get_schema_spec(force_refresh=True).tables
    assert driver.conn is connection
    assert driver._open_connections == [connection]
    assert transaction_active(connection)
    with closing(driver.get_connection()) as reader:
        assert reader.execute("SELECT value FROM pending").fetchall() == []
    assert connection.execute("SELECT value FROM pending").fetchall() == [(42,)]
    connection.rollback()


def test_other_thread_does_not_inherit_scoped_fingerprint(driver, probes):
    with pytest.raises(DatabaseIntegrityError, match="Schema changed during discovery"):
        with driver.schema_read_scope():
            before = driver.direct_get_schema_version()
            with closing(driver.get_connection()) as other:
                with other:
                    other.execute(
                        "CREATE TABLE concurrent_discovery (value INTEGER)"
                    ).close()
            with ThreadPoolExecutor(max_workers=1) as pool:
                observed = pool.submit(driver.direct_get_schema_version).result(
                    timeout=10
                )
            assert observed != before
            assert driver.direct_get_schema_version() == before
    assert probes["fingerprints"] == 3
    assert driver.direct_get_schema_version() == observed
