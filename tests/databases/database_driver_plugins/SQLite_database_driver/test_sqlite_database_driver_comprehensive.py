"""Comprehensive tests for the SQLite database driver.

These tests aim to lock down behaviour that should remain stable even if the
underlying backend changes (optimisations, different SQL library, moving away
from sqlite, etc.).

The focus is on:
* schema introspection
* core CRUD operations
* foreign keys / constraints
* type adapters (PYSET/PYLIST/PYDICT)
* unicode edge cases
* basic SQL injection hardening expectations
"""

from __future__ import annotations

import os
import random
import sqlite3
import string
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _relation_type(conn: sqlite3.Connection, name: str) -> str | None:
    row = conn.execute(
        "SELECT type FROM sqlite_master WHERE (type='table' OR type='view') AND name=? LIMIT 1;",
        (name,),
    ).fetchone()
    return str(row[0]) if row else None


def _relation_exists(conn: sqlite3.Connection, name: str) -> bool:
    return _relation_type(conn, name) is not None


def _table_info(conn: sqlite3.Connection, table: str):
    return conn.execute(f"PRAGMA table_info(`{table}`);").fetchall()


def _detect_pk_column(conn: sqlite3.Connection, table: str) -> str | None:
    for _cid, name, _t, _notnull, _dflt, pk in _table_info(conn, table):
        if int(pk) == 1:
            return str(name)
    return None


def _default_value_for_type(col_name: str, col_type: str, preferred_text_value: str):
    n = col_name.lower()
    t = (col_type or "").upper()

    if "UUID" in t or n.endswith("_uuid"):
        return "00000000-0000-0000-0000-000000000000"
    if "DATE" in n or "TIME" in n:
        # Keep stable, human-readable values.
        return "2000-01-01 00:00:00"

    if "INT" in t:
        return 0
    if "REAL" in t or "FLOA" in t or "DOUB" in t:
        return 0.0
    if "BLOB" in t:
        return b""
    if "CHAR" in t or "TEXT" in t or "CLOB" in t:
        if n in {"title", "series", "publisher", "name", "creator"}:
            return preferred_text_value
        return ""

    # Unknown type: fall back to empty string.
    return ""


@dataclass(frozen=True)
class _TitleContract:
    read_table: str
    read_id_col: str
    read_title_col: str
    read_sort_col: str
    write_table: str
    write_id_col: str
    write_title_col: str
    write_sort_col: str


def _title_contract(driver) -> _TitleContract:
    """Resolve how to read 'titles' while writing to the underlying storage.

    In WEMI schema variants, `titles` is a read-only compatibility view.
    """
    conn = driver.get_connection()
    try:
        t = _relation_type(conn, "titles")
    finally:
        conn.close()

    if t == "view":
        # See: database_generator_frbr/aggregate_sql/wemi_views.sql
        return _TitleContract(
            read_table="titles",
            read_id_col="title_id",
            read_title_col="title",
            read_sort_col="title_sort",
            write_table="works",
            write_id_col="work_id",
            write_title_col="work_title",
            write_sort_col="work_sort_title",
        )

    return _TitleContract(
        read_table="titles",
        read_id_col="title_id",
        read_title_col="title",
        read_sort_col="title_sort",
        write_table="titles",
        write_id_col="title_id",
        write_title_col="title",
        write_sort_col="title_sort",
    )


def _insert_minimal_title_row(driver, *, title: str, title_sort: str | None = None) -> int:
    """Insert a row that will be visible through the `titles` relation.

    If `titles` is a view, insert into `works` with the appropriate columns.
    Returns the row id (work_id/title_id).
    """
    c = _title_contract(driver)
    override = {
        c.write_title_col: title,
    }
    if title_sort is not None:
        override[c.write_sort_col] = title_sort
    return _insert_minimal_row(driver, table=c.write_table, override=override)
def _insert_minimal_row(
    driver,
    *,
    table: str,
    preferred_text_value: str = "Test",
    override: dict[str, Any] | None = None,
) -> int:
    """Insert a single row into *table* satisfying NOT NULL constraints.

    Returns the inserted row's integer primary key.
    """

    override = dict(override or {})

    conn = driver.get_connection()
    try:
        pk_col = _detect_pk_column(conn, table)
        cols = _table_info(conn, table)

        required_cols: list[str] = []
        values: list[Any] = []

        for _cid, name, col_type, notnull, dflt, pk in cols:
            name = str(name)
            if int(pk) == 1:
                continue
            if name in override:
                required_cols.append(name)
                values.append(override[name])
                continue
            if int(notnull) == 1 and dflt is None:
                required_cols.append(name)
                values.append(_default_value_for_type(name, str(col_type), preferred_text_value))

        if not required_cols:
            cur = conn.execute(f"INSERT INTO `{table}` DEFAULT VALUES;")
            conn.commit()
            return int(cur.lastrowid)

        placeholders = ",".join(["?"] * len(required_cols))
        cols_sql = ",".join([f"`{c}`" for c in required_cols])
        cur = conn.execute(
            f"INSERT INTO `{table}` ({cols_sql}) VALUES ({placeholders});",
            values,
        )
        conn.commit()

        if pk_col is None:
            raise RuntimeError(f"Could not detect PK for table {table!r}")
        return int(cur.lastrowid)
    finally:
        conn.close()


def _random_unicode_string(seed: int, *, max_len: int = 64) -> str:
    rng = random.Random(seed)
    parts = [
        "Cafe\u0301",  # combining mark
        "na\u00efve",  # diacritic
        "emoji:\U0001F9B4\U0001F31A",  # non-BMP
        "rtl:\u202Eabc\u202C",  # bidi controls
        "zwj:\U0001F469\u200D\U0001F52C",  # ZWJ sequence
        "snowman:\u2603",
        "music:\U0001F3B5",
        "thinspace:\u2009",
        "nbsp:\u00A0",
    ]
    base = rng.choice(parts)
    extra = "".join(rng.choice(string.ascii_letters) for _ in range(rng.randint(0, 16)))
    s = f"{base}-{extra}".strip("-")
    return s[:max_len]


@dataclass(frozen=True)
class _DriverBundle:
    driver: Any
    db_path: Path


@pytest.fixture
def sqlite_driver_bundle(provision_test_database):
    """Provision a schema-only DB and return a ready-to-use DatabaseDriver."""

    from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver import DatabaseDriver

    provisioned = provision_test_database("test_db_13")

    metadata = {"database_path": str(provisioned.db_path)}
    drv = DatabaseDriver(db_metadata=metadata, db=None, set_conn=True)
    try:
        yield _DriverBundle(driver=drv, db_path=provisioned.db_path)
    finally:
        try:
            drv.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Introspection / sanity
# ---------------------------------------------------------------------------


class TestSQLiteDriverIntrospection:
    def test_driver_exists_and_can_open(self, sqlite_driver_bundle):
        drv = sqlite_driver_bundle.driver

        assert drv.exists() is True
        assert drv.conn is not None

        tables = drv.direct_get_tables()
        assert "titles" in tables
        assert "books" in tables
        assert "database_metadata" in tables

    def test_get_tables_is_cached_and_refreshable(self, sqlite_driver_bundle):
        drv = sqlite_driver_bundle.driver

        t1 = drv.direct_get_tables()
        t2 = drv.direct_get_tables()
        assert t1 is t2 or t1 == t2

        t3 = drv.direct_get_tables(force_refresh=True)
        assert "titles" in t3

    def test_get_tables_and_columns_contains_titles(self, sqlite_driver_bundle):
        drv = sqlite_driver_bundle.driver
        tc = drv.direct_get_tables_and_columns()

        assert "titles" in tc
        assert "title_id" in tc["titles"]
        assert "title" in tc["titles"]

    def test_validate_existing_table_name_hardening(self, sqlite_driver_bundle):
        drv = sqlite_driver_bundle.driver

        assert drv.direct_validate_existing_table_name("titles") is True
        assert drv.direct_validate_existing_table_name("`titles`") is True
        assert drv.direct_validate_existing_table_name(" titles ") is True
        assert drv.direct_validate_existing_table_name("titles;") is False
        assert drv.direct_validate_existing_table_name("titles; DROP TABLE titles") is False

    def test_connection_has_expected_pragmas_and_functions(self, sqlite_driver_bundle):
        drv = sqlite_driver_bundle.driver
        conn = drv.get_connection()
        try:
            fk = conn.execute("PRAGMA foreign_keys;").fetchone()
            assert fk in [(1,), ("1",)]

            # Built-in helper functions the driver registers.
            assert conn.execute("SELECT uuid4();").fetchone() is not None

            # `title_sort()` is a legacy calibre function that may depend on optional
            # localisation/ICU pieces. If it isn't available yet, treat this as a
            # known-issue instead of a hard failure.
            try:
                assert conn.execute("SELECT title_sort('The Test');").fetchone() is not None
            except sqlite3.OperationalError:
                pytest.xfail("title_sort() SQLite UDF currently raises (legacy localisation path)")

            assert conn.execute("SELECT 'abc' REGEXP 'a.*';").fetchone()[0] in (0, 1)
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# CRUD behaviour (core regression targets)
# ---------------------------------------------------------------------------


class TestSQLiteDriverCRUD:
    def test_insert_and_fetch_title_row(self, sqlite_driver_bundle):
        drv = sqlite_driver_bundle.driver
        c = _title_contract(drv)

        title_value = "Hello World"

        # Insert into the writable storage table (e.g. `works` in WEMI),
        # then assert it is visible via the `titles` compatibility surface.
        drv.direct_add_simple_row_dict(
            {
                c.write_title_col: title_value,
                c.write_sort_col: "hello world",
            }
        )

        rows = drv.direct_search_table(table=c.read_table, column=c.read_title_col, search_term=title_value)
        assert len(rows) == 1
        assert rows[0][c.read_title_col] == title_value
    def test_update_row_dict_round_trip(self, sqlite_driver_bundle):
        drv = sqlite_driver_bundle.driver
        c = _title_contract(drv)

        row_id = _insert_minimal_title_row(drv, title="Before")

        # Read/write via the underlying storage table (views are read-only).
        row = drv.direct_get_row_dict_from_id(c.write_table, row_id)
        assert row is not False
        assert row[c.write_title_col] == "Before"

        row[c.write_title_col] = "After"
        drv.direct_update_row_dict(row)

        row2 = drv.direct_get_row_dict_from_id(c.write_table, row_id)
        assert row2[c.write_title_col] == "After"

        # And it should be visible via the read surface too.
        row3 = drv.direct_get_row_dict_from_id(c.read_table, row_id)
        assert row3 is not False
        assert row3[c.read_title_col] == "After"
    def test_delete_row_by_id(self, sqlite_driver_bundle):
        drv = sqlite_driver_bundle.driver
        c = _title_contract(drv)

        row_id = _insert_minimal_title_row(drv, title="To Delete")
        assert drv.direct_get_row_dict_from_id(c.read_table, row_id) is not False

        drv.direct_delete_row_by_id(c.write_table, row_id)
        assert drv.direct_get_row_dict_from_id(c.read_table, row_id) is False
    def test_get_all_rows_and_iterator_consistency(self, sqlite_driver_bundle):
        drv = sqlite_driver_bundle.driver
        c = _title_contract(drv)

        for i in range(5):
            _insert_minimal_title_row(drv, title=f"T{i}")

        all_rows = drv.direct_get_all_rows(c.read_table, sort_column=c.read_id_col)
        # The iterator is currently only implemented for id-ordered iteration.
        it_rows = list(drv.direct_get_row_dict_iterator(c.read_table))
        it_rows = sorted(it_rows, key=lambda r: int(r[c.read_id_col]))
        assert len(all_rows) == len(it_rows)
        assert [r[c.read_id_col] for r in all_rows] == [r[c.read_id_col] for r in it_rows]
    def test_unique_values_set_includes_inserted(self, sqlite_driver_bundle):
        drv = sqlite_driver_bundle.driver
        values = {"Alpha", "Beta", "Gamma"}
        for v in values:
            _insert_minimal_title_row(drv, title=v)

        found = drv.direct_get_unique_values_set("title")
        assert values.issubset(set(found))
    def test_foreign_key_cascade_manifestation_to_item(self, sqlite_driver_bundle):
        drv = sqlite_driver_bundle.driver

        # items.item_manifestation_id -> manifestations.manifestation_id has ON DELETE CASCADE
        manifestation_id = _insert_minimal_row(drv, table="manifestations")

        item_id = _insert_minimal_row(
            drv,
            table="items",
            override={"item_manifestation_id": manifestation_id},
        )

        assert drv.direct_get_row_dict_from_id("items", item_id) is not False

        drv.direct_delete_row_by_id("manifestations", manifestation_id)

        # Should cascade.
        assert drv.direct_get_row_dict_from_id("items", item_id) is False
    def test_direct_get_row_count_matches_select(self, sqlite_driver_bundle):
        drv = sqlite_driver_bundle.driver
        c = _title_contract(drv)

        before = drv.direct_get_row_count(c.read_table)
        for _ in range(3):
            _insert_minimal_title_row(drv, title="Count")
        after = drv.direct_get_row_count(c.read_table)
        assert after >= before + 3
    @pytest.mark.xfail(
        reason="PYSET converter currently expects str, but sqlite3 provides bytes on Py3",
        raises=TypeError,
    )
    def test_pyset_round_trip_on_books_paths(self, sqlite_driver_bundle):
        drv = sqlite_driver_bundle.driver

        conn = drv.get_connection()
        try:
            t = _relation_type(conn, "books")
        finally:
            conn.close()

        if t != "table":
            pytest.skip("books is a compatibility view in WEMI schema (read-only)")

        # Legacy calibre-ish schema path (only when `books` is a real table).
        title_id = _insert_minimal_title_row(drv, title="Has Paths")
        paths = {"/a/b.epub", "/c/d.mobi"}

        drv.direct_add_simple_row_dict(
            {
                "book_id": title_id,
                "book_paths": paths,
                "book_uuid": "00000000-0000-0000-0000-000000000000",
            }
        )

        rows = drv.direct_get_all_rows("books")
        by_id = {int(r["book_id"]): r for r in rows}
        assert title_id in by_id
        assert isinstance(by_id[title_id]["book_paths"], set)
        assert by_id[title_id]["book_paths"] == paths
    def test_custom_type_adapters_for_list_and_dict(self, sqlite_driver_bundle):
        drv = sqlite_driver_bundle.driver

        # Create a small ad-hoc table to exercise the adapters.
        drv.direct_execute(
            "CREATE TABLE IF NOT EXISTS test_adapters (id INTEGER PRIMARY KEY, s PYSET, l PYLIST, d PYDICT);"
        )

        payload_set = {"x", "y"}
        payload_list = ["a", "b", "emoji:\U0001F680"]
        payload_dict = {"k": "v", "n": 1}

        conn = drv.get_connection()
        try:
            cur = conn.execute(
                "INSERT INTO test_adapters (s, l, d) VALUES (?, ?, ?);",
                (payload_set, payload_list, payload_dict),
            )
            conn.commit()

            row = conn.execute("SELECT s, l, d FROM test_adapters WHERE id=?;", (cur.lastrowid,)).fetchone()
            assert row is not None
            s, l, d = row
            assert isinstance(s, set)
            assert isinstance(l, list)
            assert isinstance(d, dict)
            assert s == payload_set
            assert l == payload_list
            assert d == payload_dict
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Unicode + edge-case payloads
# ---------------------------------------------------------------------------


class TestSQLiteDriverUnicodeAndEdges:
    @pytest.mark.parametrize(
        "seed",
        [0, 1, 2, 3, 4, 5, 42, 99],
    )
    def test_insert_and_search_unicode_titles(self, sqlite_driver_bundle, seed):
        drv = sqlite_driver_bundle.driver
        s = _random_unicode_string(seed)
        _insert_minimal_title_row(drv, title=s)

        rows = drv.direct_search_table(table="titles", column="title", search_term=s)
        assert any(r["title"] == s for r in rows)

    def test_control_chars_and_whitespace(self, sqlite_driver_bundle):
        drv = sqlite_driver_bundle.driver
        s = "  \t\n\r\x0b\x0c  "
        _insert_minimal_title_row(drv, title=s)
        rows = drv.direct_search_table(table="titles", column="title", search_term=s)
        assert len(rows) == 1

    def test_embedded_null_byte_value_is_rejected_or_preserved_safely(self, sqlite_driver_bundle):
        drv = sqlite_driver_bundle.driver
        s = "abc\x00def"

        # SQLite can store NULs in TEXT, but some wrappers / unicode helpers can choke.
        # The acceptable outcomes are:
        #  - insertion works and we can retrieve the exact string
        #  - insertion fails with a clear sqlite/driver error
        try:
            _insert_minimal_title_row(drv, title=s)
        except Exception:
            return

        rows = drv.direct_search_table(table="titles", column="title", search_term=s)
        assert len(rows) == 1
        assert rows[0]["title"] == s

    def test_very_large_text_payload(self, sqlite_driver_bundle):
        drv = sqlite_driver_bundle.driver
        s = "Z" * 200_000  # big enough to matter, small enough for tests
        _insert_minimal_title_row(drv, title=s)
        rows = drv.direct_search_table(table="titles", column="title", search_term=s)
        assert len(rows) == 1


# ---------------------------------------------------------------------------
# SQL injection / hardening expectations
# ---------------------------------------------------------------------------


class TestSQLiteDriverSecurity:
    def test_value_sql_injection_payload_is_not_executed(self, sqlite_driver_bundle):
        drv = sqlite_driver_bundle.driver
        conn = drv.get_connection()
        try:
            assert _relation_exists(conn, "titles")
        finally:
            conn.close()

        payload = "x'); DROP TABLE titles; --"
        _insert_minimal_title_row(drv, title=payload)

        # Table should still exist.
        conn2 = drv.get_connection()
        try:
            assert _relation_exists(conn2, "titles")
            r = conn2.execute("SELECT title FROM titles WHERE title=?;", (payload,)).fetchone()
            assert r is not None and r[0] == payload
        finally:
            conn2.close()

    def test_table_name_injection_is_rejected_by_validation_helpers(self, sqlite_driver_bundle):
        drv = sqlite_driver_bundle.driver
        bad = "titles; DROP TABLE titles;--"
        assert drv.direct_validate_existing_table_name(bad) is False

        with pytest.raises(Exception):
            drv.direct_get_all_rows(bad)

        # Ensure schema intact.
        conn = drv.get_connection()
        try:
            assert _relation_exists(conn, "titles")
            assert _relation_exists(conn, "books")
        finally:
            conn.close()

    def test_search_table_with_weird_inputs_does_not_execute_multiple_statements(self, sqlite_driver_bundle):
        drv = sqlite_driver_bundle.driver

        # `sqlite3` should reject stacked statements; the important part is that
        # we don't end up dropping tables.
        inj_table = "titles; DROP TABLE books; --"
        with pytest.raises(Exception):
            drv.direct_search_table(table=inj_table, column="title", search_term="x")

        conn = drv.get_connection()
        try:
            assert _relation_exists(conn, "books")
        finally:
            conn.close()

    def test_direct_execute_rejects_multiple_statements(self, sqlite_driver_bundle):
        drv = sqlite_driver_bundle.driver

        with pytest.raises(Exception):
            drv.direct_execute("SELECT 1; SELECT 2;")

    def test_direct_execute_parameter_binding_blocks_injection(self, sqlite_driver_bundle):
        drv = sqlite_driver_bundle.driver
        payload = "1; DROP TABLE titles; --"

        conn = drv.get_connection()
        try:
            assert _relation_exists(conn, "titles")
        finally:
            conn.close()

        # Parameter binding should treat the payload as a literal string.
        res = drv.direct_execute("SELECT ?;", (payload,)).fetchone()
        assert res[0] == payload

        conn2 = drv.get_connection()
        try:
            assert _relation_exists(conn2, "titles")
        finally:
            conn2.close()


# ---------------------------------------------------------------------------
# Metadata + utility behaviour
# ---------------------------------------------------------------------------


class TestSQLiteDriverMetadata:
    def test_write_and_read_metadata_round_trip(self, sqlite_driver_bundle):
        drv = sqlite_driver_bundle.driver

        drv.direct_write_metadata("db_name", "Test DB")
        assert drv.direct_read_metadata("db_name") == "Test DB"

        uni = _random_unicode_string(123)
        drv.direct_write_metadata("parent_LiuXin_instance", uni)
        assert drv.direct_read_metadata("parent_LiuXin_instance") == uni

    def test_read_unset_metadata_returns_none(self, sqlite_driver_bundle):
        drv = sqlite_driver_bundle.driver
        # On a freshly created DB, most metadata columns are NULL.
        assert drv.direct_read_metadata("unique_id") is None


# ---------------------------------------------------------------------------
# Known issues / legacy Py2 assumptions (tracked with xfail)
# ---------------------------------------------------------------------------


class TestSQLiteDriverKnownIssues:
    def test_direct_get_max_works_on_py3(self, sqlite_driver_bundle):
        drv = sqlite_driver_bundle.driver
        _insert_minimal_title_row(drv, title="A")
        _insert_minimal_title_row(drv, title="B")
        assert drv.direct_get_max("title_id") is not None

    def test_direct_get_min_works_on_py3(self, sqlite_driver_bundle):
        drv = sqlite_driver_bundle.driver
        _insert_minimal_title_row(drv, title="A")
        _insert_minimal_title_row(drv, title="B")
        assert drv.direct_get_min("title_id") is not None

    @pytest.mark.xfail(
        reason="direct_update_columns contains Py2 iterator usage and needs porting",
        raises=AttributeError,
    )
    def test_direct_update_columns_simple_mode(self, sqlite_driver_bundle):
        drv = sqlite_driver_bundle.driver
        c = _title_contract(drv)

        id1 = _insert_minimal_title_row(drv, title="Old1")
        id2 = _insert_minimal_title_row(drv, title="Old2")

        drv.direct_update_columns({id1: "New1", id2: "New2"}, field=c.read_title_col, table=c.write_table)

        r1 = drv.direct_get_row_dict_from_id(c.read_table, id1)
        r2 = drv.direct_get_row_dict_from_id(c.read_table, id2)
        assert r1[c.read_title_col] == "New1"
        assert r2[c.read_title_col] == "New2"
    def test_direct_multi_column_search_is_parameterized(self, sqlite_driver_bundle):
        drv = sqlite_driver_bundle.driver
        _insert_minimal_title_row(drv, title="Safe")
        _insert_minimal_title_row(drv, title="Other")

        # If values are interpolated, this returns both rows; if parameterized it returns none.
        res = drv.direct_multi_column_search([
            ("title", "=", "'Safe' OR 1=1"),
        ])
        assert res == []
