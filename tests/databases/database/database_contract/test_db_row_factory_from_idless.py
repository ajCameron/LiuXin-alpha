"""
Check Row.from_idless_row_dict insertion, loaded fields, and read-only behavior on the works table.

Payload cases include ASCII, emoji, and SQL-looking text, all passed as row values.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database/database_contract/test_db_row_factory_from_idless.py
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
    """
    Insert an ID-less work and check its table, assigned ID, exact title, and presence of the created-timestamp field.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_row_factory_from_idless.py::test_row_from_idless_row_dict_inserts_and_returns_loaded_row


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param payload: Parametrized text payload to insert and read back.
    :return: None; failed expectations raise AssertionError.
    """
    row = Row.from_idless_row_dict(open_db, {"work_title": payload})

    assert row.table == "works"
    assert row.row_id is not None

    # Reloaded row should contain the inserted content (and defaults).
    assert row["work_title"] == payload
    assert "work_created_timestamp_ep_k" in row.row_dict


def test_row_from_idless_row_dict_omits_none_id(open_db) -> None:
    """
    Supply work_id=None and check insertion assigns an ID while preserving the title.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_row_factory_from_idless.py::test_row_from_idless_row_dict_omits_none_id


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :return: None; failed expectations raise AssertionError.
    """
    row = Row.from_idless_row_dict(open_db, {"work_id": None, "work_title": "hello"})

    assert row.table == "works"
    assert row.row_id is not None
    assert row["work_title"] == "hello"


def test_row_from_idless_row_dict_read_only(open_db) -> None:
    """
    Create a work with read_only=True and check subsequent sync raises RowReadOnlyError.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_row_factory_from_idless.py::test_row_from_idless_row_dict_read_only


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :return: None; failed expectations raise AssertionError.
    """
    row = Row.from_idless_row_dict(open_db, {"work_title": "ro"}, read_only=True)
    with pytest.raises(RowReadOnlyError):
        row.sync()
