"""
Driver contract: hashes and dedup.

This module targets the driver helper:

    * direct_get_all_hashes()

which is expected to return a set containing all known hashes across:
  - files.file_hash
  - compressed_files.compressed_file_hash_1 / compressed_file_hash_2
  - new_books.new_book_hash_1 / new_book_hash_2
  - hashes.hash

These tests are strict ("fail" mode): if a backend diverges or if the
helper accidentally includes NULLs, misses values, or double-counts, we
want loud failures.

NOTE: We temporarily disable foreign-key enforcement to allow minimal-row
inserts focused on the hash columns (this test is about the hash collector,
not referential integrity).
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Sequence, Tuple

import pytest


def _table_exists(driver, table: str) -> bool:
    try:
        return bool(driver.validate_existing_table_name(table))
    except Exception:
        return table in set(driver.direct_get_tables())


def _available_hash_columns(driver, table: str) -> list[str]:
    if not _table_exists(driver, table):
        return []
    headings = set(driver.direct_get_column_headings(table))
    candidates_by_table = {
        "files": ["file_hash", "file_hash_sha256", "file_hash_blake3"],
        "compressed_files": ["compressed_file_hash_1", "compressed_file_hash_2"],
        "new_books": ["new_book_hash_1", "new_book_hash_2"],
        "hashes": ["hash"],
    }
    return [column for column in candidates_by_table[table] if column in headings]


def _fetchall(cursor) -> list:
    try:
        return cursor.fetchall()
    except Exception:
        return list(cursor)


def _pragma_table_info(driver, table: str) -> list[tuple]:
    conn = driver.get_connection()
    # sqlite3: cursor has fetchall; apsw: cursor is iterable
    cur = conn.execute(f"PRAGMA table_info(`{table}`)")
    return _fetchall(cur)


def _required_columns(driver, table: str) -> list[tuple[str, str]]:
    """
    Return a list of (name, declared_type) for NOT NULL columns without defaults
    that are not part of the primary key.
    """
    info = _pragma_table_info(driver, table)
    required: list[tuple[str, str]] = []
    for row in info:
        # (cid, name, type, notnull, dflt_value, pk)
        name = row[1]
        declared_type = (row[2] or "").upper()
        notnull = int(row[3] or 0)
        dflt = row[4]
        pk = int(row[5] or 0)
        if pk:
            continue
        if notnull and dflt is None:
            required.append((name, declared_type))
    return required


def _dummy_for_type(col_name: str, declared_type: str) -> Any:
    t = (declared_type or "").upper()
    # Heuristics: keep things small + deterministic.
    if "INT" in t or "BOOL" in t:
        return 1
    if "REAL" in t or "FLOA" in t or "DOUB" in t:
        return 1.0
    if "BLOB" in t:
        return b"\x00"
    # Default: TEXT-ish.
    # Prefer something innocuous (not injection-shaped), because this is only
    # to satisfy NOT NULL columns.
    return f"contract_{col_name}"


def _insert_minimal_row(driver, table: str, values: dict) -> None:
    """
    Insert a row into `table` by supplying `values` plus any additional required
    columns (NOT NULL with no default). Uses driver.direct_add_simple_row_dict
    so that we exercise the driver's insert path.
    """
    row: dict = dict(values)
    for col_name, declared_type in _required_columns(driver, table):
        if col_name in row:
            continue
        row[col_name] = _dummy_for_type(col_name, declared_type)

    # Ensure table can be identified; do not pass a 'table' key.
    if "table" in row:
        row.pop("table", None)

    driver.direct_add_simple_row_dict(row)


def _disable_foreign_keys(driver) -> None:
    try:
        driver.direct_execute("PRAGMA foreign_keys=OFF")
    except Exception:
        # Some backends may require this through raw connection.
        conn = driver.get_connection()
        conn.execute("PRAGMA foreign_keys=OFF")


def _enable_foreign_keys(driver) -> None:
    try:
        driver.direct_execute("PRAGMA foreign_keys=ON")
    except Exception:
        conn = driver.get_connection()
        conn.execute("PRAGMA foreign_keys=ON")


def _clear_hash_tables(driver) -> None:
    for t in ("files", "compressed_files", "new_books", "hashes"):
        if _table_exists(driver, t):
            driver.direct_clear_table(t)


def test_direct_get_all_hashes_union_and_dedup(driver, pick_payload, assert_integrity):
    _disable_foreign_keys(driver)
    _clear_hash_tables(driver)

    file_hash_columns = _available_hash_columns(driver, "files")
    available_tables = [t for t in ("files", "compressed_files", "new_books", "hashes") if _table_exists(driver, t)]
    if not available_tables:
        pytest.skip("No hash-bearing tables are present in this schema")

    # Build deterministic test hashes. We intentionally include duplicates
    # across tables to ensure deduping is handled by the set semantics.
    h_shared_1 = pick_payload(100)
    h_shared_2 = pick_payload(101)
    h_file_only = pick_payload(102)
    h_cf_only = pick_payload(103)
    h_nb_only = pick_payload(104)
    h_other_only = pick_payload(105)

    expected = set()

    # files.*hash*
    if file_hash_columns:
        primary_file_hash_column = file_hash_columns[0]
        secondary_file_hash_column = file_hash_columns[1] if len(file_hash_columns) > 1 else primary_file_hash_column
        _insert_minimal_row(driver, "files", {primary_file_hash_column: h_shared_1})
        _insert_minimal_row(driver, "files", {secondary_file_hash_column: h_file_only})
        expected.update({h_shared_1, h_file_only})

    # compressed_files.compressed_file_hash_1 / _2
    if _table_exists(driver, "compressed_files"):
        _insert_minimal_row(
            driver,
            "compressed_files",
            {
                "compressed_file_hash_1": h_shared_1,
                "compressed_file_hash_2": h_cf_only,
            },
        )
        _insert_minimal_row(
            driver,
            "compressed_files",
            {
                "compressed_file_hash_1": h_shared_2,
                "compressed_file_hash_2": h_shared_1,  # duplicate on purpose
            },
        )
        expected.update({h_shared_1, h_shared_2, h_cf_only})

    # new_books.new_book_hash_1 / _2
    if _table_exists(driver, "new_books"):
        _insert_minimal_row(
            driver,
            "new_books",
            {
                "new_book_hash_1": h_nb_only,
                "new_book_hash_2": h_shared_2,
            },
        )
        expected.update({h_nb_only, h_shared_2})

    # hashes.hash
    if _table_exists(driver, "hashes"):
        _insert_minimal_row(driver, "hashes", {"hash": h_other_only})
        _insert_minimal_row(driver, "hashes", {"hash": h_shared_1})  # duplicate across tables
        expected.update({h_other_only, h_shared_1})

    got = driver.direct_get_all_hashes()
    assert isinstance(got, set), f"Expected set, got {type(got)}"
    assert got == expected

    # No NULLs should leak into the returned set in this controlled scenario.
    assert None not in got
    assert all(isinstance(x, str) for x in got)

    assert_integrity(driver)
    _enable_foreign_keys(driver)


def test_direct_get_all_hashes_is_stable_and_updates(driver, pick_payload):
    _disable_foreign_keys(driver)
    _clear_hash_tables(driver)

    file_hash_columns = _available_hash_columns(driver, "files")
    if not file_hash_columns and not _table_exists(driver, "hashes"):
        pytest.skip("Schema lacks both files and hashes tables needed for this contract")
    primary_file_hash_column = file_hash_columns[0] if file_hash_columns else None

    # Seed once.
    h1 = pick_payload(200)
    h2 = pick_payload(201)
    expected = set()
    seeded_hashes: list[str] = []
    if primary_file_hash_column is not None:
        _insert_minimal_row(driver, "files", {primary_file_hash_column: h1})
        expected.add(h1)
        seeded_hashes.append(h1)
    if _table_exists(driver, "hashes"):
        _insert_minimal_row(driver, "hashes", {"hash": h2})
        expected.add(h2)
        seeded_hashes.append(h2)

    got1 = driver.direct_get_all_hashes()
    got2 = driver.direct_get_all_hashes()
    assert got1 == got2 == expected

    # Add a new hash into a different table; result should grow.
    h3 = pick_payload(202)
    if _table_exists(driver, "new_books"):
        duplicate_hash = seeded_hashes[0]
        _insert_minimal_row(
            driver,
            "new_books",
            {"new_book_hash_1": h3, "new_book_hash_2": duplicate_hash},
        )
        expected.add(h3)
    elif _table_exists(driver, "hashes"):
        _insert_minimal_row(driver, "hashes", {"hash": h3})
        expected.add(h3)
    else:
        pytest.skip("Schema has no writable secondary hash table to test update behavior")
    got3 = driver.direct_get_all_hashes()
    assert got3 == expected

    _enable_foreign_keys(driver)
