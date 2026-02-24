"""Database contract: works -> titles compatibility projection.

In the FRBR-first schema, `titles` is a *read-only* compatibility view projected from
the insertable `works` table (see `titles_v` in the generator SQL).

Contract: inserting a blank `works` row must immediately be visible via the `titles`
view with the same id (`titles.title_id == works.work_id`).
"""

from __future__ import annotations

import pytest


def _require_table(db, table: str) -> None:
    if table not in db.get_tables(force_refresh=True):
        pytest.skip(f"Table {table!r} not present in provisioned contract DB")


def test_get_blank_row_works_creates_matching_title(db) -> None:
    _require_table(db, "works")
    _require_table(db, "titles")

    work = db.get_blank_row("works")
    assert work.row_id is not None

    matches = db.search("titles", "title_id", int(work.row_id))
    assert len(matches) == 1
