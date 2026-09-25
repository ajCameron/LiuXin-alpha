"""
Check the hash collector’s set union, duplicate removal, and visibility of later inserts across available hash tables.

Tests clear hash-bearing tables in an isolated fixture and request foreign-key
checks off for minimal inserts. Checks are enabled again only on the normal success
path; a skip or failure can leave the fixture connection with checks disabled.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_hashes_and_dedup.py
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Sequence, Tuple

import pytest


def _table_exists(driver, table: str) -> bool:
    """
    Ask the driver to validate a table name and coerce the result to bool.

    On any ordinary validation exception, fall back to membership in the driver’s table
    list.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_hashes_and_dedup.py


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :param table: Trusted test table name, interpolated into SQL where needed.
    :return: Whether the selected probe recognizes the table; fallback errors propagate.
    """
    try:
        return bool(driver.direct_validate_existing_table_name(table))
    except Exception:
        return table in set(driver.direct_get_tables())


def _available_hash_columns(driver, table: str) -> list[str]:
    """
    Filter a known table’s ordered hash-column candidates against its actual headings.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_hashes_and_dedup.py


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :param table: Trusted test table name, interpolated into SQL where needed.
    :return: Available candidate names in configured order, or an empty list for an
        absent table. An existing unsupported table raises KeyError.
    """
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
    """
    Fetch all cursor rows, falling back to list(cursor) after any ordinary fetchall exception.

    Example:
        >>> _fetchall(iter([(1,), (2,)]))
        [(1,), (2,)]


    :param cursor: Caller-owned cursor supporting fetchall or iteration.
    :return: Fetched rows or materialized remaining iterator rows; a partially consumed
        failed fetch is not rewound.
    """
    try:
        return cursor.fetchall()
    except Exception:
        return list(cursor)


def _pragma_table_info(driver, table: str) -> list[tuple]:
    """
    Inspect the trusted table through the driver connection using PRAGMA table_info.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_hashes_and_dedup.py


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :param table: Trusted test table name, interpolated into SQL where needed.
    :return: Materialized PRAGMA rows; does not commit or close the connection or
        cursor.
    """
    conn = driver.get_connection()
    # sqlite3: cursor has fetchall; apsw: cursor is iterable
    cur = conn.execute(f"PRAGMA table_info(`{table}`)")
    return _fetchall(cur)


def _required_columns(driver, table: str) -> list[tuple[str, str]]:
    """
    Select non-primary-key NOT NULL columns with no SQL default from table_info.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_hashes_and_dedup.py


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :param table: Trusted test table name, interpolated into SQL where needed.
    :return: Ordered (name, uppercase declared type) pairs for fields needing supplied
        values.
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
    """
    Choose a small deterministic placeholder using ordered declared-type substring checks.

    Example:
        >>> (_dummy_for_type('x', 'INTEGER'), _dummy_for_type('x', 'TEXT'))
        (1, 'contract_x')
        >>> _dummy_for_type('x', 'BLOB') == bytes([0])
        True


    :param col_name: Column name used in the default text placeholder.
    :param declared_type: Declared type string; falsy values are treated as empty.
    :return: 1 for integer/bool types, 1.0 for real/float/double, one NUL byte for blob,
        or contract_ plus the column name.
    """
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
    Copy supplied values, fill missing required fields with type placeholders, remove the table key, and insert via the driver.

    Existing values, including None, are retained. The driver infers the insertion
    target from column names; placeholder values need not satisfy foreign keys or other
    constraints.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_hashes_and_dedup.py


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :param table: Trusted test table name, interpolated into SQL where needed.
    :param values: Initial column/value mapping copied before required fields are
        filled.
    :return: None; inserts one row without modifying the caller’s mapping.
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
    """
    Request foreign-key enforcement off through direct_execute, falling back to the raw connection on Exception.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_hashes_and_dedup.py


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :return: None; neither verifies the resulting setting nor restores the previous
        value.
    """
    try:
        driver.direct_execute("PRAGMA foreign_keys=OFF")
    except Exception:
        # Some backends may require this through raw connection.
        conn = driver.get_connection()
        conn.execute("PRAGMA foreign_keys=OFF")


def _enable_foreign_keys(driver) -> None:
    """
    Request foreign-key enforcement on through direct_execute, falling back to the raw connection on Exception.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_hashes_and_dedup.py


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :return: None; neither verifies the resulting setting nor restores an earlier value.
    """
    try:
        driver.direct_execute("PRAGMA foreign_keys=ON")
    except Exception:
        conn = driver.get_connection()
        conn.execute("PRAGMA foreign_keys=ON")


def _clear_hash_tables(driver) -> None:
    """
    Clear each existing files, compressed_files, new_books, and hashes table in that order.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_hashes_and_dedup.py


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :return: None; deletes all rows in available hash-bearing tables.
    """
    for t in ("files", "compressed_files", "new_books", "hashes"):
        if _table_exists(driver, t):
            driver.direct_clear_table(t)


def test_direct_get_all_hashes_union_and_dedup(driver, pick_payload, assert_integrity):
    """
    Seed shared and distinct hashes across available tables and require the exact string-only set without None.

    Skip if no hash-bearing table exists. Request foreign keys off before setup and on
    only after all assertions and the integrity helper succeed.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_hashes_and_dedup.py::test_direct_get_all_hashes_union_and_dedup


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :param pick_payload: Fixture callable selecting corpus strings by a wrapping integer
        index.
    :param assert_integrity: Fixture callable checking the first retained
        integrity_check result for ok.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Require two initial hash reads to equal the seeded set and a later read to include an additional hash.

    Skip when suitable initial or secondary storage is missing. Foreign-key checks are
    requested on only at the successful end of the test.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_hashes_and_dedup.py::test_direct_get_all_hashes_is_stable_and_updates


    :param driver: Driver owned by the isolated database fixture; teardown attempts to
        close it.
    :param pick_payload: Fixture callable selecting corpus strings by a wrapping integer
        index.
    :return: None; failed expectations raise AssertionError.
    """
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
