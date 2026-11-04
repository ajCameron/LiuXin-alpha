"""Check live collaborator contracts and cleanup against real database facades."""

from types import SimpleNamespace

import pytest

from LiuXin_alpha.databases.database import TelemetryMaintainerProxy
from LiuXin_alpha.databases.runtime import initialise_database_runtime
from LiuXin_alpha.errors import DatabaseIntegrityError
from tests.databases import test_row_write_safety as fixtures

db = fixtures.db
template = fixtures.template


@pytest.mark.parametrize("name", ["driver", "driver_wrapper", "macros"])
def test_live_collaborator_access_rejects_closed_database(db, name):
    assert getattr(db, name) is not None
    db.close()
    assert getattr(db, "_" + name) is None
    with pytest.raises(DatabaseIntegrityError, match="not bound|closed"):
        getattr(db, name)
    db.close()


def test_runtime_helper_rebinds_database_owners(db, monkeypatch):
    from LiuXin_alpha.databases.maintenance import service
    from LiuXin_alpha.databases import runtime

    maintainer = SimpleNamespace(clean=lambda table, item_ids: None)
    factory = lambda database: maintainer
    monkeypatch.setattr(service, "Maintainer", factory)
    monkeypatch.setattr(runtime, "Maintainer", factory)
    driver, wrapper, macros, metadata_sql = (
        db.driver,
        db.driver_wrapper,
        db.macros,
        db.metadata_sql,
    )
    for collaborator in (driver, wrapper, macros, metadata_sql):
        collaborator.db = None
    preferences = {"runtime-test": True}
    initialise_database_runtime(
        db, callback_proxy_cls=TelemetryMaintainerProxy, preferences_obj=preferences
    )
    assert db.preferences is preferences
    assert db.maintenance is db.maintainer is maintainer
    assert driver.maintainer_callback is db._maintainer_callback_proxy
    for collaborator in (driver, wrapper, macros, metadata_sql):
        assert collaborator.db is db


def test_closed_database_preference_write_preserves_cached_value(db):
    from LiuXin_alpha.databases.dbprefs import DBPrefs

    preferences = DBPrefs(db)
    preferences.disable_setting = True
    preferences["lifecycle_probe"] = 7
    preferences.disable_setting = False
    db.close()
    with pytest.raises(DatabaseIntegrityError, match="closed database"):
        preferences["lifecycle_probe"] = 8
    assert preferences["lifecycle_probe"] == 7
