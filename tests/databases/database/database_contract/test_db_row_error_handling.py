"""Database contract: defensive error handling + 'bad input' surfaces (chunk 06).

These tests assert that malformed identifiers and awkward values fail loudly
with LiuXin errors (primarily InputIntegrityError / DatabaseIntegrityError),
and that Row enforces basic invariants (unknown columns, read-only sync).
"""

from __future__ import annotations

from typing import Iterable, Tuple

import pytest

from LiuXin_alpha.errors import InputIntegrityError, RowReadOnlyError


def _choose_table_and_column(db) -> Tuple[str, str]:
    """Pick a real table and a non-id column we can safely interact with."""

    tables = list(db.get_tables())
    if not tables:
        raise RuntimeError("Test database contains no tables")

    preferred = ["titles", "authors", "series", "books", "creators"]
    table = next((t for t in preferred if t in tables), tables[0])

    id_col = db.driver_wrapper.get_id_column(table)
    headings = list(db.get_column_headings(table))
    non_id_cols = [h for h in headings if h != id_col]
    if not non_id_cols:
        # Extremely unlikely, but keep the helper total.
        non_id_cols = headings

    return table, non_id_cols[0]


def _weird_identifiers() -> Iterable[str]:
    # A small curated list: SQL-ish, unicode-ish, and just plain wrong.
    return [
        "definitely_not_a_table",
        "titles__does_not_exist__42",
        "titles; DROP TABLE titles",  # should be rejected before any SQL executes
        "tïtles",  # latin + diacritics
        "表",  # CJK
        "עברית",  # RTL
        "العربية",  # RTL
        "𝕥𝕚𝕥𝕝𝕖𝕤",  # math letters
        "titles\x00null",  # embedded NUL (as a literal backslash sequence)
        "".join(["x"] * 512),
    ]


@pytest.mark.parametrize("bad_table", list(_weird_identifiers()))
def test_get_column_headings_invalid_table_raises_input_integrityerror(open_db, bad_table: str):
    with pytest.raises(InputIntegrityError):
        open_db.get_column_headings(bad_table)


@pytest.mark.parametrize("bad_table", list(_weird_identifiers()))
def test_get_record_count_invalid_table_raises_input_integrityerror(open_db, bad_table: str):
    with pytest.raises(InputIntegrityError):
        open_db.get_record_count(bad_table)


def test_get_blank_row_invalid_table_raises_input_integrityerror(open_db):
    with pytest.raises(InputIntegrityError):
        open_db.get_blank_row("no_such_table")


@pytest.mark.parametrize(
    "bad_column",
    [
        "definitely_not_a_column",
        "title)",
        "title(",
        "title--",
        "tïtłê",  # odd unicode
        "\"title\"",  # quoting doesn't make it a real column
        "title OR 1=1",  # expression-like
    ],
)
def test_search_invalid_column_raises_input_integrityerror(open_db, bad_column: str):
    table, good_col = _choose_table_and_column(open_db)

    # Also ensure failures don't mutate the DB.
    before = open_db.get_record_count(table)
    with pytest.raises(InputIntegrityError):
        open_db.search(table, bad_column, "anything")
    after = open_db.get_record_count(table)
    assert before == after


def test_search_missing_search_term_raises_input_integrityerror(open_db):
    table, good_col = _choose_table_and_column(open_db)
    with pytest.raises(InputIntegrityError):
        open_db.search(table, good_col, None)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"reverse": True},
        {"sort_column": "id"},
    ],
)
def test_get_all_rows_iterator_mode_disallows_reverse_and_sort(open_db, kwargs):
    table, _ = _choose_table_and_column(open_db)
    with pytest.raises(NotImplementedError):
        open_db.get_all_rows(table, iterator_return=True, **kwargs)


def test_chunk_iterator_invalid_column_raises_input_integrityerror(open_db):
    with pytest.raises(InputIntegrityError):
        list(open_db.chunk_iterator(column="definitely_not_a_column"))


def test_row_unknown_column_setitem_raises_keyerror(open_db):
    table, good_col = _choose_table_and_column(open_db)
    row = open_db.get_blank_row(table)
    row[good_col] = "ok"
    with pytest.raises(KeyError):
        row["this_column_is_not_real"] = "nope"


def test_row_unknown_column_getitem_raises_keyerror(open_db):
    table, _ = _choose_table_and_column(open_db)
    row = open_db.get_blank_row(table)
    with pytest.raises(KeyError):
        _ = row["this_column_is_not_real"]


def test_row_read_only_sync_raises(open_db):
    table, good_col = _choose_table_and_column(open_db)
    row = open_db.get_blank_row(table)
    row[good_col] = "hello"
    row.make_read_only()
    with pytest.raises(RowReadOnlyError):
        row.sync()
