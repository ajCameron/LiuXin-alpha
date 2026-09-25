"""
Check bootstrap rating and sentinel rows, repair opt-out, and direct repair helpers across selected drivers.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database/database_contract/test_db_bootstrap_integrity_helpers.py
"""

from __future__ import annotations

from pathlib import Path

import pytest

from LiuXin_alpha.databases.bootstrap_constants import AGENTS_NULL_CANONICAL_NAME


def _expected_rating_value(rating_id: int) -> float:
    # rating_id 1..11 => 0.0..5.0 step 0.5
    """
    Map a rating ID to the half-step value (ID minus one) divided by two.

    Example:
        >>> [_expected_rating_value(i) for i in (1, 3, 11)]
        [0.0, 1.0, 5.0]


    :param rating_id: Numeric rating ID used in the arithmetic.
    :return: Floating-point expected rating; the helper does not restrict the input to
        IDs one through eleven.
    """
    return float(rating_id - 1) / 2.0


def _fresh_get(db, stmt: str, *, all: bool = True):
    """
    Query through a new driver connection to avoid stale aliases on Database.

    Attempt to close the connection in finally and suppress ordinary close exceptions.
    Connection creation and query failures propagate.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_bootstrap_integrity_helpers.py


    :param db: Open Database supplied by the shared fixture or caller; this helper does
        not close the Database.
    :param stmt: SQL query forwarded without bound parameters.
    :param all: Result-shape flag passed to conn.get; defaults to True.
    :return: Result returned by conn.get with the requested all flag.
    """

    conn = db.driver.get_connection()
    try:
        return conn.get(stmt, all=all)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _fetch_ratings(db) -> list[tuple[int, float]]:
    """
    Read ratings by ID through a fresh connection and convert ID/value pairs to int and float.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_bootstrap_integrity_helpers.py


    :param db: Open Database supplied by the shared fixture or caller; this helper does
        not close the Database.
    :return: List of ordered (rating_id, rating_value) tuples; query/conversion errors
        propagate.
    """
    rows = _fresh_get(db, "SELECT rating_id, rating FROM ratings ORDER BY rating_id")
    out: list[tuple[int, float]] = []
    for rid, rating in rows:
        out.append((int(rid), float(rating)))
    return out


def test_fresh_database_bootstrap_creates_ratings_and_null_rows(tmp_path: Path, driver_spec):
    """
    Open a new database path and check eleven half-step ratings, the null series, and the canonical organisation sentinel; close in finally.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_bootstrap_integrity_helpers.py::test_fresh_database_bootstrap_creates_ratings_and_null_rows


    :param tmp_path: Pytest-provided temporary directory for isolated database or TOML
        files.
    :param driver_spec: Selected database driver specification, including its ID and
        Database db_type.
    :return: None; failed expectations raise AssertionError.
    """

    from LiuXin_alpha.databases.database import Database

    db_path = tmp_path / f"fresh_bootstrap_{driver_spec.id}.db"
    meta = {"database_path": str(db_path)}

    db = Database(metadata=meta, db_type=driver_spec.db_type, create=False, backup=False)
    try:
        # Ratings table should have 11 rows with expected values.
        ratings = _fetch_ratings(db)
        assert len(ratings) == 11
        assert [rid for rid, _ in ratings] == list(range(1, 12))
        for rid, val in ratings:
            assert val == _expected_rating_value(rid)

        # Null rows should exist and be set to None.
        series0 = db.driver_wrapper.get_row_from_id("series", 0)
        assert series0
        assert series0.get("series") is None

        agent0 = db.driver_wrapper.get_row_from_id("agents", 0)
        assert agent0
        assert agent0.get("agent_type") == "organisation"
        assert agent0.get("agent_canonical_name") == AGENTS_NULL_CANONICAL_NAME
    finally:
        db.close()


def test_database_init_can_skip_bootstrap_repairs(tmp_path: Path, driver_spec):
    """
    Corrupt a rating and null-series value, reopen with bootstrap repair disabled, and check both values remain changed; close both Database instances.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_bootstrap_integrity_helpers.py::test_database_init_can_skip_bootstrap_repairs


    :param tmp_path: Pytest-provided temporary directory for isolated database or TOML
        files.
    :param driver_spec: Selected database driver specification, including its ID and
        Database db_type.
    :return: None; failed expectations raise AssertionError.
    """

    from LiuXin_alpha.databases.database import Database

    db_path = tmp_path / f"skip_bootstrap_repairs_{driver_spec.id}.db"
    meta = {"database_path": str(db_path)}

    db = Database(
        metadata=meta,
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
        enable_storage_manager=False,
    )
    try:
        rating_row = db.get_row_from_id("ratings", 1)
        assert rating_row is not None
        rating_row["rating"] = 99
        rating_row.sync()

        series_row = db.get_row_from_id("series", 0)
        assert series_row is not None
        series_row["series"] = "BROKEN"
        series_row.sync()
    finally:
        db.close()

    reopened = Database(
        metadata=meta,
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
        enable_storage_manager=False,
        repair_bootstrap_rows=False,
    )
    try:
        assert float(reopened.get_row_from_id("ratings", 1)["rating"]) == 99
        assert reopened.get_row_from_id("series", 0)["series"] == "BROKEN"
    finally:
        reopened.close()


def test_check_rating_table_is_idempotent(open_db):
    """
    Call rating repair once and check the complete ordered rating snapshot is unchanged.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_bootstrap_integrity_helpers.py::test_check_rating_table_is_idempotent


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :return: None; failed expectations raise AssertionError.
    """

    db = open_db
    before = _fetch_ratings(db)
    db.check_rating_table()
    after = _fetch_ratings(db)
    assert before == after


@pytest.mark.parametrize("rating_id", [1, 2, 3, 6, 11])
def test_check_rating_table_repairs_corrupt_rating_value(open_db, rating_id: int):
    """
    Replace a selected rating with an incorrect value and check repair restores its expected half-step value.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_bootstrap_integrity_helpers.py::test_check_rating_table_repairs_corrupt_rating_value


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param rating_id: Selected existing rating ID whose value is deliberately corrupted.
    :return: None; failed expectations raise AssertionError.
    """

    db = open_db
    row = db.get_row_from_id("ratings", rating_id)
    assert row is not None

    # Corrupt it.
    row["rating"] = 123.456
    row.sync()

    db.check_rating_table()

    fixed = db.get_row_from_id("ratings", rating_id)
    assert fixed is not None
    assert float(fixed["rating"]) == _expected_rating_value(rating_id)


@pytest.mark.parametrize("missing_id", [1, 5, 10, 11])
def test_check_rating_table_reinserts_missing_rows(open_db, missing_id: int):
    """
    Delete a selected rating row and check repair recreates it with the expected value.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_bootstrap_integrity_helpers.py::test_check_rating_table_reinserts_missing_rows


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :param missing_id: Selected rating ID deleted before testing reinsertion.
    :return: None; failed expectations raise AssertionError.
    """

    db = open_db
    row = db.get_row_from_id("ratings", missing_id)
    assert row is not None

    db.delete(row)
    assert db.get_row_from_id("ratings", missing_id) is None

    db.check_rating_table()

    restored = db.get_row_from_id("ratings", missing_id)
    assert restored is not None
    assert float(restored["rating"]) == _expected_rating_value(missing_id)


def test_check_rating_table_accepts_string_ids(open_db):
    """
    Check the wrapper accepts string ID three and returns rating row three.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_bootstrap_integrity_helpers.py::test_check_rating_table_accepts_string_ids


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :return: None; failed expectations raise AssertionError.
    """

    db = open_db
    # We use the underlying wrapper call, because it returns raw dicts.
    row = db.driver_wrapper.get_row_from_id("ratings", "3")
    assert row
    assert int(row["rating_id"]) == 3


def test_ensure_null_rows_is_idempotent(open_db):
    """
    Call sentinel repair three times and check the expected series and agent sentinel fields.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_bootstrap_integrity_helpers.py::test_ensure_null_rows_is_idempotent


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :return: None; failed expectations raise AssertionError.
    """

    db = open_db
    db.ensure_null_rows()
    db.ensure_null_rows()
    db.ensure_null_rows()

    series0 = db.driver_wrapper.get_row_from_id("series", 0)
    assert series0
    assert series0.get("series") is None

    agent0 = db.driver_wrapper.get_row_from_id("agents", 0)
    assert agent0
    assert agent0.get("agent_type") == "organisation"
    assert agent0.get("agent_canonical_name") == AGENTS_NULL_CANONICAL_NAME


def test_ensure_null_rows_repairs_series_null_value(open_db):
    """
    Change the null series to text and check sentinel repair restores None.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_bootstrap_integrity_helpers.py::test_ensure_null_rows_repairs_series_null_value


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :return: None; failed expectations raise AssertionError.
    """

    db = open_db
    row = db.get_row_from_id("series", 0)
    assert row is not None

    row["series"] = "NOT NULL"
    row.sync()

    db.ensure_null_rows()

    repaired = db.driver_wrapper.get_row_from_id("series", 0)
    assert repaired
    assert repaired.get("series") is None


def test_ensure_null_rows_repairs_agents_null_value(open_db):
    """
    Change the agent sentinel type/name and check repair restores the canonical organisation fields.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_bootstrap_integrity_helpers.py::test_ensure_null_rows_repairs_agents_null_value


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :return: None; failed expectations raise AssertionError.
    """

    db = open_db
    row = db.get_row_from_id("agents", 0)
    assert row is not None

    # Break the sentinel row.
    row["agent_type"] = "person"
    row["agent_canonical_name"] = "NOT NULL"
    row.sync()

    db.ensure_null_rows()

    repaired = db.driver_wrapper.get_row_from_id("agents", 0)
    assert repaired
    assert repaired.get("agent_type") == "organisation"
    assert repaired.get("agent_canonical_name") == AGENTS_NULL_CANONICAL_NAME


def test_rating_table_expected_shape(open_db):
    """
    Check the complete rating list equals IDs one through eleven with their expected half-step values.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database/database_contract/test_db_bootstrap_integrity_helpers.py::test_rating_table_expected_shape


    :param open_db: Open Database for the selected driver; its fixture attempts close at
        teardown.
    :return: None; failed expectations raise AssertionError.
    """

    db = open_db
    ratings = _fetch_ratings(db)
    assert ratings == [(rid, _expected_rating_value(rid)) for rid in range(1, 12)]
