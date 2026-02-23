"""Database contract: Row.from_idless_row_dict (chunk 05b).

These tests cover the new, preferred workflow for creating rows without the
get_blank_row() pattern:

    Row.from_idless_row_dict(db, {...})

This should INSERT a new row (letting SQLite assign the id) and return a Row
instance representing the inserted record.
"""

from __future__ import annotations

import pytest

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import RowReadOnlyError


@pytest.mark.parametrize(
    "payload",
    [
        "simple ascii",
        "unicode-emoji 😀🤖🧠",
        "sql-injection-ish'); DROP TABLE works; --",
    ],
)
def test_row_from_idless_row_dict_inserts_and_returns_loaded_row(open_db, payload: str) -> None:
    row = Row.from_idless_row_dict(open_db, {"work_title": payload})

    assert row.table == "works"
    assert row.row_id is not None

    # Reloaded row should contain the inserted content (and defaults).
    assert row["work_title"] == payload
    assert "work_created_timestamp_ep_k" in row.row_dict


def test_row_from_idless_row_dict_omits_none_id(open_db) -> None:
    row = Row.from_idless_row_dict(open_db, {"work_id": None, "work_title": "hello"})

    assert row.table == "works"
    assert row.row_id is not None
    assert row["work_title"] == "hello"


def test_row_from_idless_row_dict_read_only(open_db) -> None:
    row = Row.from_idless_row_dict(open_db, {"work_title": "ro"}, read_only=True)
    with pytest.raises(RowReadOnlyError):
        row.sync()
