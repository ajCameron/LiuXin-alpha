"""
Check database-backed preferences through the selected driver.

Uses an isolated copy of test_db_1, closes Database after each test and checks
persisted values as well as in-memory access. Random keys prevent collisions with
existing preferences.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/test_dbprefs_legacy_port.py
"""
from __future__ import annotations

import uuid

import pytest

from LiuXin_alpha.databases.dbprefs import DBPrefs


@pytest.fixture
def db_with_test_db_1(provision_named_test_database, driver_spec):
    """
    Open an isolated test_db_1 copy with creation and backup disabled.

    Yields the Database and closes it in finally, including after test failure.
    Provisioning and construction errors propagate.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_dbprefs_legacy_port.py


    :param provision_named_test_database: Fixture factory that copies a named test
        database into an isolated location.
    :param driver_spec: Selected database driver specification, including its ID and
        Database db_type.
    :return: Generator yielding one open Database; closes it when the fixture resumes
        for teardown.
    """
    from LiuXin_alpha.databases.database import Database

    provisioned = provision_named_test_database("test_db_1")
    db = Database(
        metadata={"database_path": str(provisioned.db_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
    )
    try:
        yield db
    finally:
        db.close()


def test_dbprefs_init_and_load_runs_without_error(db_with_test_db_1) -> None:
    """
    Construct DBPrefs from the database and require a dict instance.

    This smoke assertion does not check which existing preferences were loaded.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_dbprefs_legacy_port.py::test_dbprefs_init_and_load_runs_without_error


    :param db_with_test_db_1: Open database fixture provisioned from test_db_1; the
        fixture closes it after use.
    :return: None; failed expectations raise AssertionError.
    """
    prefs = DBPrefs(db=db_with_test_db_1)
    assert isinstance(prefs, dict)


def test_dbprefs_set_get_and_reload_roundtrip(db_with_test_db_1) -> None:
    """
    Persist a nested preference value and reload it from a new DBPrefs instance.

    Checks one backing row, its to_raw serialization and the original nested value on
    reload.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_dbprefs_legacy_port.py::test_dbprefs_set_get_and_reload_roundtrip


    :param db_with_test_db_1: Open database fixture provisioned from test_db_1; the
        fixture closes it after use.
    :return: None; failed expectations raise AssertionError.
    """
    key = f"legacy_port_test_key_{uuid.uuid4()}"
    value = {"alpha": 1, "beta": ["x", 2], "nested": {"k": "v"}}

    prefs = DBPrefs(db=db_with_test_db_1)
    prefs[key] = value
    assert prefs[key] == value

    rows = list(
        db_with_test_db_1.driver_wrapper.search(
            table="preferences",
            column="preference_key",
            search_term=key,
        )
    )
    assert len(rows) == 1
    assert rows[0]["preference_value"] == prefs.to_raw(value)

    reloaded = DBPrefs(db=db_with_test_db_1)
    assert reloaded[key] == value


def test_dbprefs_delitem_removes_db_row(db_with_test_db_1) -> None:
    """
    Delete a preference from both the live mapping and its backing preferences table.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_dbprefs_legacy_port.py::test_dbprefs_delitem_removes_db_row


    :param db_with_test_db_1: Open database fixture provisioned from test_db_1; the
        fixture closes it after use.
    :return: None; failed expectations raise AssertionError.
    """
    key = f"legacy_port_delete_key_{uuid.uuid4()}"

    prefs = DBPrefs(db=db_with_test_db_1)
    prefs[key] = "delete-me"
    assert key in prefs

    del prefs[key]
    assert key not in prefs

    rows = list(
        db_with_test_db_1.driver_wrapper.search(
            table="preferences",
            column="preference_key",
            search_term=key,
        )
    )
    assert rows == []


def test_dbprefs_namespaced_accessors(db_with_test_db_1) -> None:
    """
    Round-trip a ui/layout preference through live and newly loaded accessors.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_dbprefs_legacy_port.py::test_dbprefs_namespaced_accessors


    :param db_with_test_db_1: Open database fixture provisioned from test_db_1; the
        fixture closes it after use.
    :return: None; failed expectations raise AssertionError.
    """
    prefs = DBPrefs(db=db_with_test_db_1)
    prefs.set_namespaced("ui", "layout", {"left_panel": True, "right_panel": False})

    assert prefs.get_namespaced("ui", "layout") == {"left_panel": True, "right_panel": False}
    assert DBPrefs(db=db_with_test_db_1).get_namespaced("ui", "layout") == {
        "left_panel": True,
        "right_panel": False,
    }


def test_dbprefs_namespaced_rejects_colons(db_with_test_db_1) -> None:
    """
    Reject colons in either the namespace or namespaced key with KeyError.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_dbprefs_legacy_port.py::test_dbprefs_namespaced_rejects_colons


    :param db_with_test_db_1: Open database fixture provisioned from test_db_1; the
        fixture closes it after use.
    :return: None; failed expectations raise AssertionError.
    """
    prefs = DBPrefs(db=db_with_test_db_1)

    with pytest.raises(KeyError):
        prefs.set_namespaced("ui:bad", "layout", 1)
    with pytest.raises(KeyError):
        prefs.set_namespaced("ui", "layout:bad", 1)
