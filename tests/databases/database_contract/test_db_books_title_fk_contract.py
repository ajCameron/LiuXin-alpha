"""Database contract: books/title FK pairing.

The schema declares `books.book_id` as a FOREIGN KEY to `titles.title_id`.
That means creating a blank `books` row must also ensure a matching `titles` row exists.

This is a regression guard for `Database.driver_wrapper.get_blank_row("books")`.
"""

from __future__ import annotations

import pytest


def _require_table(db, table: str) -> None:
    if table not in db.get_tables(force_refresh=True):
        pytest.skip(f"Table {table!r} not present in provisioned contract DB")


def test_get_blank_row_books_creates_matching_title(db) -> None:
    _require_table(db, "books")
    _require_table(db, "titles")

    book = db.get_blank_row("books")
    assert book.row_id is not None

    matches = db.search("titles", "title_id", int(book.row_id))
    assert len(matches) == 1
