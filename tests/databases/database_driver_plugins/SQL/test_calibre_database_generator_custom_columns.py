"""
Check Calibre builder custom-column table layouts and text, multi-text, integer, and series value round trips.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_driver_plugins/SQL/test_calibre_database_generator_custom_columns.py
"""
from __future__ import annotations


def _table_exists(conn, name: str) -> bool:
    """
    Check sqlite_master for a table or view with the exact bound name.

    Example:
        >>> import sqlite3
        >>> connection = sqlite3.connect(':memory:')
        >>> _table_exists(connection, 'absent')
        False
        >>> connection.close()


    :param conn: Caller-owned SQLite connection used for inspection.
    :param name: Exact relation name passed as a bound value.
    :return: True when a matching relation row exists; no commit or close occurs.
    """
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table','view') AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def test_calibre_library_builder_custom_column_text(provision_populated_calibre_library):
    """
    Create a text custom column, check both dynamic relations exist, and read back hello from a new book.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/SQL/test_calibre_database_generator_custom_columns.py::test_calibre_library_builder_custom_column_text


    :param provision_populated_calibre_library: Fixture factory returning an isolated
        blank Calibre library and its builder; skips without required SQLite FTS5
        support.
    :return: None; failed expectations raise AssertionError.
    """
    lib, builder = provision_populated_calibre_library(name="calibre_cc_text")

    num = builder.create_custom_column(label="cc_text", name="CC Text", datatype="text")

    # Verify dynamic tables exist
    conn = builder.connect()
    try:
        value_table, link_table = builder.custom_table_names(int(num))
        assert _table_exists(conn, value_table)
        assert _table_exists(conn, link_table)
    finally:
        conn.close()

    book = builder.add_book(title="T", authors=["A"], custom_values={"cc_text": "hello"})
    conn = builder.connect()
    try:
        assert builder.get_custom_value(conn, book_id=book.book_id, label="cc_text") == "hello"
    finally:
        conn.close()


def test_calibre_library_builder_custom_column_text_multiple(provision_populated_calibre_library):
    """
    Write two values to a multi-text custom column and require the exact ordered list on readback.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/SQL/test_calibre_database_generator_custom_columns.py::test_calibre_library_builder_custom_column_text_multiple


    :param provision_populated_calibre_library: Fixture factory returning an isolated
        blank Calibre library and its builder; skips without required SQLite FTS5
        support.
    :return: None; failed expectations raise AssertionError.
    """
    lib, builder = provision_populated_calibre_library(name="calibre_cc_text_multi")

    builder.create_custom_column(label="cc_multi", name="CC Multi", datatype="text", is_multiple=True)
    book = builder.add_book(title="T", authors=["A"], custom_values={"cc_multi": ["x", "y"]})

    conn = builder.connect()
    try:
        assert builder.get_custom_value(conn, book_id=book.book_id, label="cc_multi") == ["x", "y"]
    finally:
        conn.close()


def test_calibre_library_builder_custom_column_int_scalar(provision_populated_calibre_library):
    """
    Check an integer custom column has a value relation without a link relation, then read back forty-two.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/SQL/test_calibre_database_generator_custom_columns.py::test_calibre_library_builder_custom_column_int_scalar


    :param provision_populated_calibre_library: Fixture factory returning an isolated
        blank Calibre library and its builder; skips without required SQLite FTS5
        support.
    :return: None; failed expectations raise AssertionError.
    """
    lib, builder = provision_populated_calibre_library(name="calibre_cc_int")

    num = builder.create_custom_column(label="cc_int", name="CC Int", datatype="int")

    # int is non-normalized, so only the value table exists
    conn = builder.connect()
    try:
        value_table, link_table = builder.custom_table_names(int(num))
        assert _table_exists(conn, value_table)
        assert not _table_exists(conn, link_table)
    finally:
        conn.close()

    book = builder.add_book(title="T", authors=["A"], custom_values={"cc_int": 42})
    conn = builder.connect()
    try:
        assert builder.get_custom_value(conn, book_id=book.book_id, label="cc_int") == 42
    finally:
        conn.close()


def test_calibre_library_builder_custom_column_series_index(provision_populated_calibre_library):
    """
    Check explicit and omitted series indexes read back as Saga with 2.0 and 1.0 respectively.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/SQL/test_calibre_database_generator_custom_columns.py::test_calibre_library_builder_custom_column_series_index


    :param provision_populated_calibre_library: Fixture factory returning an isolated
        blank Calibre library and its builder; skips without required SQLite FTS5
        support.
    :return: None; failed expectations raise AssertionError.
    """
    _lib, builder = provision_populated_calibre_library(name="calibre_cc_series")

    builder.create_custom_column(label="cc_series", name="CC Series", datatype="series")

    book1 = builder.add_book(title="T1", authors=["A"], custom_values={"cc_series": ("Saga", 2)})
    book2 = builder.add_book(title="T2", authors=["A"], custom_values={"cc_series": "Saga"})

    conn = builder.connect()
    try:
        assert builder.get_custom_value(conn, book_id=book1.book_id, label="cc_series") == ("Saga", 2.0)
        # If no explicit index is provided, Calibre treats it as 1.0
        assert builder.get_custom_value(conn, book_id=book2.book_id, label="cc_series") == ("Saga", 1.0)
    finally:
        conn.close()
