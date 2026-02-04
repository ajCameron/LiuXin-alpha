from __future__ import annotations


def _table_exists(conn, name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table','view') AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def test_calibre_library_builder_custom_column_text(provision_populated_calibre_library):
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
    lib, builder = provision_populated_calibre_library(name="calibre_cc_text_multi")

    builder.create_custom_column(label="cc_multi", name="CC Multi", datatype="text", is_multiple=True)
    book = builder.add_book(title="T", authors=["A"], custom_values={"cc_multi": ["x", "y"]})

    conn = builder.connect()
    try:
        assert builder.get_custom_value(conn, book_id=book.book_id, label="cc_multi") == ["x", "y"]
    finally:
        conn.close()


def test_calibre_library_builder_custom_column_int_scalar(provision_populated_calibre_library):
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
