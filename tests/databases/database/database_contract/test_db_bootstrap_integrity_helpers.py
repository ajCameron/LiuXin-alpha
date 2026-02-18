"""Database contract: bootstrap integrity helpers.

This chunk validates the Database startup invariants enforced by:

* :meth:`LiuXin_alpha.databases.database.Database.check_rating_table`
* :meth:`LiuXin_alpha.databases.database.Database.ensure_null_rows`

These methods are called during Database initialization, but we also exercise
them directly to ensure they remain:

* correct
* idempotent
* able to repair mild corruption
"""

from __future__ import annotations

from pathlib import Path

import pytest

from LiuXin_alpha.databases.bootstrap_constants import AGENTS_NULL_CANONICAL_NAME


def _expected_rating_value(rating_id: int) -> float:
    # rating_id 1..11 => 0.0..5.0 step 0.5
    return float(rating_id - 1) / 2.0


def _fresh_get(db, stmt: str, *, all: bool = True):
    """Run a query on a fresh short-lived driver connection.

    The Database object exposes convenience aliases (db.get/db.conn). Those are
    deliberately treated as *best-effort* conveniences: drivers may refresh or
    replace their long-lived `driver.conn` during cache invalidation or
    introspection, and the bound method alias can become stale.

    Using a fresh connection here keeps these tests deterministic while still
    validating database contents.
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
    rows = _fresh_get(db, "SELECT rating_id, rating FROM ratings ORDER BY rating_id")
    out: list[tuple[int, float]] = []
    for rid, rating in rows:
        out.append((int(rid), float(rating)))
    return out


def test_fresh_database_bootstrap_creates_ratings_and_null_rows(tmp_path: Path, driver_spec):
    """A brand-new DB should end up with ratings + null rows after init."""

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


def test_check_rating_table_is_idempotent(open_db):
    """Calling check_rating_table repeatedly should not change correct data."""

    db = open_db
    before = _fetch_ratings(db)
    db.check_rating_table()
    after = _fetch_ratings(db)
    assert before == after


@pytest.mark.parametrize("rating_id", [1, 2, 3, 6, 11])
def test_check_rating_table_repairs_corrupt_rating_value(open_db, rating_id: int):
    """If a rating row's value is wrong, check_rating_table should correct it."""

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
    """If a rating row is missing, check_rating_table should recreate it."""

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
    """The ratings helper historically used string ids; ensure it still works."""

    db = open_db
    # We use the underlying wrapper call, because it returns raw dicts.
    row = db.driver_wrapper.get_row_from_id("ratings", "3")
    assert row
    assert int(row["rating_id"]) == 3


def test_ensure_null_rows_is_idempotent(open_db):
    """ensure_null_rows should be safe to call repeatedly."""

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
    """If the series null row has a value, ensure_null_rows should reset it to None."""

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
    """If the agents null row is missing required values, ensure_null_rows should repair it."""

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
    """Sanity: ratings table should contain exactly the expected ids + values."""

    db = open_db
    ratings = _fetch_ratings(db)
    assert ratings == [(rid, _expected_rating_value(rid)) for rid in range(1, 12)]
