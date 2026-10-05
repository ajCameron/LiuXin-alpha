"""
Check a newly inserted work is visible through the titles compatibility projection.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database/database_contract/test_db_books_title_fk_contract.py
"""

from __future__ import annotations

import pytest


def _require_table(db, table: str) -> None:
    """
    Refresh the reported relation names and skip when the required name is absent.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_books_title_fk_contract.py


    :param db: Open Database supplied by the shared fixture or caller; this helper does
        not close the Database.
    :param table: Required relation name; no table-versus-view distinction is made.
    :return: None if present; otherwise raises a pytest skip.
    """
    if table not in db.get_tables(force_refresh=True):
        pytest.skip(f"Table {table!r} not present in provisioned contract DB")


def test_get_blank_row_works_creates_matching_title(db) -> None:
    """
    Create a blank work and check its non-None ID finds exactly one title-view row.

    Skip if works or titles is absent from refreshed relation names.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_books_title_fk_contract.py::test_get_blank_row_works_creates_matching_title


    :param db: Open Database supplied by the shared fixture or caller; this helper does
        not close the Database.
    :return: None; failed expectations raise AssertionError.
    """
    _require_table(db, "works")
    _require_table(db, "titles")

    work = db.get_blank_row("works")
    assert work.row_id is not None

    matches = db.search("titles", "title_id", int(work.row_id))
    assert len(matches) == 1
