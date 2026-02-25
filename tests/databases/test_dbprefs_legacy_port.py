from __future__ import annotations

import uuid

import pytest

from LiuXin_alpha.databases.dbprefs import DBPrefs


@pytest.fixture
def db_with_test_db_1(provision_named_test_database, driver_spec):
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
    prefs = DBPrefs(db=db_with_test_db_1)
    assert isinstance(prefs, dict)


def test_dbprefs_set_get_and_reload_roundtrip(db_with_test_db_1) -> None:
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
    prefs = DBPrefs(db=db_with_test_db_1)
    prefs.set_namespaced("ui", "layout", {"left_panel": True, "right_panel": False})

    assert prefs.get_namespaced("ui", "layout") == {"left_panel": True, "right_panel": False}
    assert DBPrefs(db=db_with_test_db_1).get_namespaced("ui", "layout") == {
        "left_panel": True,
        "right_panel": False,
    }


def test_dbprefs_namespaced_rejects_colons(db_with_test_db_1) -> None:
    prefs = DBPrefs(db=db_with_test_db_1)

    with pytest.raises(KeyError):
        prefs.set_namespaced("ui:bad", "layout", 1)
    with pytest.raises(KeyError):
        prefs.set_namespaced("ui", "layout:bad", 1)
