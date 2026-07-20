"""Database contract: surface + lifecycle.

This module is the first slice of Database-level tests. It focuses on:

* Construction surface: driver/wrapper/macros/queues are wired.
* Deterministic shutdown: Database.close() + context manager behavior.
* Handle release: the on-disk DB file can be renamed after close (Windows-meaningful).
* Driver reload hygiene: Database.set_driver() tears down old wrapper + driver resources.

These tests are intentionally *light* on heavy queries. Their job is to catch
resource leaks, thread leaks, and reference-cycle bugs early.
"""

from __future__ import annotations

from dataclasses import replace
import os
import sqlite3
import types
import uuid
from pathlib import Path

import pytest

from LiuXin_alpha.databases.column_metadata import (
    ColumnEmptyValuePolicy,
    ColumnMergePolicy,
    ColumnNormalizationProfile,
    ColumnSemanticRole,
    ColumnValidationProfile,
)
from LiuXin_alpha.databases.schema_specs import LinkKind
from LiuXin_alpha.errors import DatabaseIntegrityError


def _assert_any_operation_fails(conn) -> None:
    """Assert that *conn* appears to be closed.

    We intentionally accept *any* exception type here, because different
    backends (sqlite3 / APSW / wrappers) raise different errors.
    """

    # Some connections expose .execute, some prefer cursor().execute.
    ops = []
    if hasattr(conn, "execute"):
        ops.append(lambda: conn.execute("SELECT 1"))
    if hasattr(conn, "cursor"):
        ops.append(lambda: conn.cursor().execute("SELECT 1"))
    if not ops:
        # If we cannot probe, treat as closed (best effort).
        return

    ok = False
    for op in ops:
        try:
            op()
        except Exception:
            ok = True
            break

    assert ok, "Expected closed connection to reject SQL operations"


def _rename_db_file(db_path: Path) -> Path:
    """Rename the db file, returning the new path.

    This is a reliable proxy for "no open SQLite handles" on Windows.
    """

    new_path = db_path.with_name(db_path.name + ".renamed")
    if new_path.exists():
        new_path.unlink()
    os.replace(db_path, new_path)
    assert new_path.exists()
    return new_path


def test_database_construction_surface(open_db):
    """Database should expose the key surfaces and wire them consistently."""

    db = open_db

    assert db.driver is not None
    assert db.driver_wrapper is not None
    assert db.macros is not None

    # Convenience aliases (a legacy quirk, but relied upon by much code).
    assert getattr(db, "conn", None) is db.driver.conn
    assert callable(getattr(db, "get", None))

    # Lock connection should exist and be distinct from the primary conn.
    assert getattr(db, "lock", None) is db.driver_wrapper.lock
    assert db.driver_wrapper.lock is not None
    assert db.driver.conn is not None
    assert db.driver_wrapper.lock is not db.driver.conn


def test_declared_column_datatype_propagates_through_database_layers(open_db):
    table = "database_metadata"
    column = "database_metadata_unique_id"

    assert open_db.driver.direct_get_declared_column_datatype(table, column) == "TEXT"
    assert open_db.driver_wrapper.get_declared_column_datatype(table, column) == "TEXT"
    assert open_db.get_declared_column_datatype(table, column) == "TEXT"


def test_link_capabilities_propagate_through_database_layers(open_db):
    table1 = "agents"
    table2 = "works"

    direct = open_db.driver.direct_get_link_capabilities(table1, table2)
    wrapped = open_db.driver_wrapper.get_link_capabilities(table1, table2)
    public = open_db.get_link_capabilities(table1, table2)

    assert direct is not None
    assert wrapped == direct
    assert public == direct
    assert public.kind is LinkKind.TYPED_PRIORITY
    assert public.typed is True
    assert public.priority is True
    assert public.both is True

    spec = open_db.driver_wrapper.get_link_spec(table1, table2)
    assert spec is not None
    assert spec.link_table == public.link_table
    assert spec.type_link_col == public.type_column
    assert spec.priority_link_col == public.priority_column
    assert spec.typed is public.typed
    assert spec.ordered is public.priority


def test_column_case_sensitivity_propagates_through_database_layers(open_db):
    table = "works"
    column = "work_title"

    assert open_db.driver.direct_get_case_sensitivity(table, column) is False
    assert open_db.driver_wrapper.get_case_sensitivity(table, column) is False
    assert open_db.get_case_sensitivity(table, column) is False
    metadata = open_db.get_column_metadata(table, column)
    assert open_db.driver.direct_get_column_metadata(table, column) == metadata
    assert open_db.driver_wrapper.get_column_metadata(table, column) == metadata

    changed_metadata = replace(
        metadata,
        merge_policy=ColumnMergePolicy.PRESERVE_EXISTING,
    )
    try:
        open_db.set_column_metadata(changed_metadata)
        assert open_db.driver_wrapper.get_column_metadata(table, column) == changed_metadata
        assert open_db.driver.direct_get_column_metadata(table, column) == changed_metadata

        open_db.set_case_sensitivity(table, column, True)
        assert open_db.driver_wrapper.get_case_sensitivity(table, column) is True
        assert open_db.driver.direct_get_case_sensitivity(table, column) is True
        assert open_db.is_column_case_sensitive(table, column) is True
        assert open_db.get_column_metadata(table, column).case_sensitive is True
    finally:
        open_db.set_column_metadata(metadata)


def test_column_metadata_field_accessors_propagate_through_database_layers(open_db):
    table = "works"
    column = "work_title"
    original = open_db.get_column_metadata(table, column)

    getters = (
        ("get_semantic_role", original.semantic_role),
        ("get_normalization_profile", original.normalization_profile),
        ("get_comparison_column", original.comparison_column),
        ("get_empty_value_policy", original.empty_value_policy),
        ("get_merge_policy", original.merge_policy),
        ("get_validation_profile", original.validation_profile),
    )
    for method_name, expected_value in getters:
        assert getattr(open_db.driver, f"direct_{method_name}")(table, column) == expected_value
        assert getattr(open_db.driver_wrapper, method_name)(table, column) == expected_value
        assert getattr(open_db, method_name)(table, column) == expected_value

    expected = replace(
        original,
        semantic_role=ColumnSemanticRole.LABEL,
        normalization_profile=ColumnNormalizationProfile.UNICODE_NFC,
        comparison_column="work_sort_title",
        empty_value_policy=ColumnEmptyValuePolicy.PRESERVE,
        merge_policy=ColumnMergePolicy.PRESERVE_EXISTING,
        validation_profile=ColumnValidationProfile.VERBATIM_TEXT,
    )
    try:
        open_db.set_semantic_role(table, column, expected.semantic_role)
        open_db.set_normalization_profile(
            table,
            column,
            expected.normalization_profile,
        )
        open_db.set_comparison_column(table, column, expected.comparison_column)
        open_db.set_empty_value_policy(
            table,
            column,
            expected.empty_value_policy,
        )
        open_db.set_merge_policy(table, column, expected.merge_policy)
        open_db.set_validation_profile(
            table,
            column,
            expected.validation_profile,
        )

        assert open_db.get_column_metadata(table, column) == expected
        assert open_db.driver_wrapper.get_column_metadata(table, column) == expected
        assert open_db.driver.direct_get_column_metadata(table, column) == expected
    finally:
        open_db.set_column_metadata(original)


def test_normalized_identity_and_canonical_query_propagate_through_database_layers(
    open_db,
):
    table = "tags"
    column = "tag"
    driver_spec = open_db.driver.direct_get_normalized_identity_spec(table, column)

    assert driver_spec is not None
    assert (
        open_db.driver_wrapper.get_normalized_identity_spec(table, column)
        == driver_spec
    )
    assert open_db.get_normalized_identity_spec(table, column) == driver_spec
    assert driver_spec in tuple(open_db.driver.direct_iter_normalized_identity_specs())
    assert driver_spec in tuple(open_db.driver_wrapper.iter_normalized_identity_specs())
    assert driver_spec in tuple(open_db.iter_normalized_identity_specs())
    assert open_db.get_normalized_identity_spec("works", "work_title") is None

    canonical = f"Canonical Tag {uuid.uuid4().hex}"
    row_id = open_db.macros.ensure_table_value(table, column, canonical)
    key = open_db.derive_identity_value(table, column, canonical.swapcase())
    identity = open_db.get_canonical_identity_by_key(table, column, key)
    assert identity is not None
    assert identity.row_id == row_id
    assert identity.canonical_value == canonical
    assert open_db.get_canonical_value(
        table,
        column,
        f"  {canonical.swapcase()}  ",
    ) == canonical


def test_legacy_database_without_column_metadata_uses_inferred_read_policy(
    db_path: Path,
    db_metadata: dict,
    driver_spec,
):
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("DROP TABLE column_metadata")

    from LiuXin_alpha.databases.database import Database

    with Database(
        metadata=db_metadata,
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
        enable_maintenance=False,
    ) as db:
        assert "column_metadata" not in db.all_tables

        metadata = db.get_column_metadata("works", "work_title")
        assert metadata.case_sensitive is False
        assert metadata.semantic_role is ColumnSemanticRole.TITLE
        assert (
            metadata.normalization_profile
            is ColumnNormalizationProfile.UNICODE_NFC_TRIM_CASEFOLD
        )
        assert db.get_case_sensitivity("works", "work_title") is False

        with pytest.raises(DatabaseIntegrityError, match="no column_metadata table"):
            db.set_column_metadata(metadata)


def test_dirty_records_queue_is_shared(open_db):
    """Database should provide a single shared dirty-record queue."""

    db = open_db
    q = getattr(db, "dirty_records_queue", None)
    assert q is not None
    assert getattr(db.driver, "dirty_records_queue", None) is q
    assert getattr(db.driver_wrapper, "dirty_records_queue", None) is q


def test_maintenance_thread_is_started(open_db):
    """The Maintainer should spawn the background thread on startup."""

    db = open_db
    maint = getattr(db, "maintenance", None)
    assert maint is not None
    thread = getattr(maint, "maintainer", None)
    assert thread is not None
    # We don't assert is_alive() because scheduling can be noisy; we only
    # care that the object exists and is stop()-capable.
    assert hasattr(thread, "stop")


def test_close_stops_maintenance_thread(open_db):
    """close() should request the maintainer thread to stop."""

    db = open_db
    thread = db.maintenance.maintainer
    assert getattr(thread, "keep_running", True) is True

    db.close()

    # The thread may still be sleeping (daemon thread), but it must have been
    # asked to stop.
    assert getattr(thread, "keep_running", False) is False


def test_close_is_idempotent(open_db):
    """Calling close() multiple times should not raise."""

    db = open_db
    db.close()
    db.close()
    db.close()


def test_close_clears_convenience_aliases(open_db):
    """close() should clear the legacy convenience aliases that keep handles alive."""

    db = open_db
    assert getattr(db, "conn", None) is not None
    assert getattr(db, "get", None) is not None
    assert getattr(db, "lock", None) is not None

    db.close()

    assert getattr(db, "conn", None) is None
    assert getattr(db, "get", None) is None
    assert getattr(db, "lock", None) is None


def test_close_releases_driver_and_wrapper_connections(open_db):
    """close() should close both the primary and lock connections."""

    db = open_db
    driver = db.driver
    wrapper = db.driver_wrapper
    conn = driver.conn
    lock_conn = wrapper.lock

    assert conn is not None
    assert lock_conn is not None

    db.close()

    # Driver wrapper and driver should have been cleaned.
    assert getattr(driver, "conn", None) is None
    assert getattr(wrapper, "lock", None) is None

    # The captured connection objects should behave like closed resources.
    _assert_any_operation_fails(conn)
    _assert_any_operation_fails(lock_conn)


def test_close_allows_renaming_db_file(db_path: Path, driver_spec, db_metadata: dict):
    """After Database.close(), the database file should be renamable.

    This is a strong proxy for "no open SQLite handles" on Windows.
    """

    from LiuXin_alpha.databases.database import Database

    db = Database(metadata=db_metadata, db_type=driver_spec.db_type, create=False, backup=False)
    try:
        assert db.driver.conn is not None
        assert db.driver_wrapper.lock is not None
    finally:
        db.close()

    _rename_db_file(db_path)


def test_context_manager_closes_on_normal_exit(db_path: Path, driver_spec, db_metadata: dict):
    """Using Database as a context manager should always close resources."""

    from LiuXin_alpha.databases.database import Database

    with Database(metadata=db_metadata, db_type=driver_spec.db_type, create=False, backup=False) as db:
        assert db.driver.conn is not None
        assert db.driver_wrapper.lock is not None

    _rename_db_file(db_path)


def test_database_can_open_without_maintenance_service(db_path: Path, driver_spec, db_metadata: dict):
    """Read-only callers can skip the background maintainer for faster startup."""

    from LiuXin_alpha.databases.database import Database

    with Database(
        metadata=db_metadata,
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
        enable_maintenance=False,
    ) as db:
        assert db.maintenance is None
        assert db.maintainer is None
        assert db.get_record_count("items") >= 0

    _rename_db_file(db_path)


def test_context_manager_closes_and_propagates_exceptions(db_path: Path, driver_spec, db_metadata: dict):
    """__exit__ should close resources and not swallow exceptions."""

    from LiuXin_alpha.databases.database import Database

    class _Sentinel(Exception):
        pass

    with pytest.raises(_Sentinel):
        with Database(metadata=db_metadata, db_type=driver_spec.db_type, create=False, backup=False):
            raise _Sentinel("boom")

    _rename_db_file(db_path)


def test_break_cycles_then_close_is_safe(open_db):
    """Even if break_cycles() runs early, close() should not explode."""

    db = open_db
    db.break_cycles()
    db.close()


def test_wrapper_close_then_database_close_is_safe(open_db):
    """If the wrapper is closed early, Database.close() should remain safe."""

    db = open_db
    wrapper = db.driver_wrapper
    wrapper.close()
    db.close()


def test_existing_driver_init_requires_metadata_none(db_metadata: dict, driver_spec):
    """existing_driver init path should enforce metadata=None (by design)."""

    from LiuXin_alpha.databases.database import Database
    from LiuXin_alpha.databases.database_driver_plugins import loadDatabaseDriver

    Driver = loadDatabaseDriver(driver_spec.db_type)
    drv = Driver(db_metadata, db=None, set_conn=True)
    try:
        with pytest.raises(AssertionError):
            Database(metadata=db_metadata, existing_driver=drv)
    finally:
        try:
            drv.close()
        except Exception:
            pass


def test_existing_driver_init_wires_db_refs(db_metadata: dict, driver_spec):
    """Database(existing_driver=...) should backfill db refs on driver + macros."""

    from LiuXin_alpha.databases.database import Database
    from LiuXin_alpha.databases.database_driver_plugins import loadDatabaseDriver

    Driver = loadDatabaseDriver(driver_spec.db_type)
    drv = Driver(db_metadata, db=None, set_conn=True)

    db = Database(metadata=None, existing_driver=drv)
    try:
        assert db.driver is drv
        assert getattr(drv, "db", None) is db
        assert getattr(db.driver_wrapper, "db", None) is db
        assert getattr(drv, "macros", None) is not None
        assert getattr(drv.macros, "db", None) is db
    finally:
        db.close()


def test_existing_driver_init_close_releases_handles(db_path: Path, db_metadata: dict, driver_spec):
    """Database(existing_driver=...) should still release file handles on close."""

    from LiuXin_alpha.databases.database import Database
    from LiuXin_alpha.databases.database_driver_plugins import loadDatabaseDriver

    Driver = loadDatabaseDriver(driver_spec.db_type)
    drv = Driver(db_metadata, db=None, set_conn=True)
    db = Database(metadata=None, existing_driver=drv)
    db.close()

    _rename_db_file(db_path)


def test_set_driver_replaces_wrapper_and_closes_old_resources(open_db, db_metadata: dict, driver_spec):
    """set_driver() should close the previous wrapper lock connection and driver conn."""

    from LiuXin_alpha.databases.database_driver_plugins import loadDatabaseDriver

    db = open_db

    old_driver = db.driver
    old_wrapper = db.driver_wrapper
    old_conn = old_driver.conn
    old_lock = old_wrapper.lock

    assert old_conn is not None
    assert old_lock is not None

    Driver = loadDatabaseDriver(driver_spec.db_type)
    new_driver = Driver(db_metadata, db=None, set_conn=True)

    db.set_driver(new_driver)

    # Old resources should be torn down.
    assert getattr(old_driver, "conn", None) is None
    assert getattr(old_wrapper, "lock", None) is None
    _assert_any_operation_fails(old_conn)
    _assert_any_operation_fails(old_lock)

    # New surfaces should be live.
    assert db.driver is new_driver
    assert db.driver.conn is not None
    assert db.driver_wrapper is not None
    assert db.driver_wrapper.lock is not None
    assert db.driver_wrapper.lock is not old_lock


def test_set_driver_with_same_driver_keeps_primary_conn(open_db):
    """Calling set_driver with the same driver should not close the primary conn."""

    db = open_db
    driver = db.driver
    conn = driver.conn
    old_wrapper = db.driver_wrapper
    old_lock = old_wrapper.lock

    db.set_driver(driver)

    # Primary connection remains usable.
    assert driver.conn is conn
    assert conn is not None
    assert driver.conn is not None

    # But the wrapper lock conn should have been replaced (old one closed).
    assert db.driver_wrapper is not old_wrapper
    assert db.driver_wrapper.lock is not None
    assert db.driver_wrapper.lock is not old_lock
    _assert_any_operation_fails(old_lock)


def test_set_driver_then_close_cleans_new_driver(open_db, db_metadata: dict, driver_spec):
    """If we swap drivers, a subsequent close() should clean the new resources too."""

    from LiuXin_alpha.databases.database_driver_plugins import loadDatabaseDriver

    db = open_db
    Driver = loadDatabaseDriver(driver_spec.db_type)
    new_driver = Driver(db_metadata, db=None, set_conn=True)
    db.set_driver(new_driver)

    # Capture new connection objects before close.
    conn = new_driver.conn
    lock_conn = db.driver_wrapper.lock

    db.close()

    assert getattr(new_driver, "conn", None) is None
    assert getattr(db.driver_wrapper, "lock", None) is None
    _assert_any_operation_fails(conn)
    _assert_any_operation_fails(lock_conn)


def test_wrapper_derived_schema_caches_reset_on_force_refresh(open_db, monkeypatch):
    """Wrapper-level derived schema caches should avoid redundant recompute and reset on refresh."""

    wrapper = open_db.driver_wrapper

    pair = None
    for left in sorted(wrapper.main_tables):
        for right in sorted(wrapper.main_tables):
            left_base = wrapper.driver.direct_get_column_base(left)
            right_base = wrapper.driver.direct_get_column_base(right)
            if left == right:
                candidate = "{}_{}_intralinks".format(left_base, left_base)
            else:
                ordered = sorted([left_base, right_base])
                candidate = "{}_{}_links".format(ordered[0], ordered[1])
            if candidate in wrapper.interlink_tables or candidate in wrapper.intralink_tables:
                pair = (left, right)
                break
        if pair is not None:
            break

    if pair is None:
        pytest.skip("No linkable table pair found in this contract DB")

    calls = {"count": 0}
    original = wrapper.driver.direct_get_column_base

    def counted(table_name):
        calls["count"] += 1
        return original(table_name)

    monkeypatch.setattr(wrapper.driver, "direct_get_column_base", counted)

    first = wrapper.get_link_table_name(*pair)
    assert first
    after_first = calls["count"]
    assert after_first > 0

    second = wrapper.get_link_table_name(*pair)
    assert second == first
    assert calls["count"] == after_first

    wrapper.get_tables(force_refresh=True)

    third = wrapper.get_link_table_name(*pair)
    assert third == first
    assert calls["count"] > after_first
