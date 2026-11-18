"""Keep LiuXin field semantics independent of optional Calibre adapters."""

import subprocess
import sys

import pytest

from LiuXin_alpha.caches.custom_fields import CustomFieldCache
from LiuXin_alpha.caches.field_metadata import FieldMetadata
from LiuXin_alpha.databases.custom_columns import CustomFieldError, SeriesValue
from tests.databases.test_native_custom_fields import native as native
from tests.databases.test_row_write_safety import db as db
from tests.databases.test_row_write_safety import template as template


def test_database_custom_field_boundary_has_no_implicit_calibre_adapters():
    subprocess.run(
        [
            sys.executable,
            "-c",
            """
import importlib
import sys
from LiuXin_alpha.databases import api, custom_columns
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.driver_wrapper import DriverWrapper
from LiuXin_alpha.databases.database_driver_plugins.SQL.macros import SQLiteDatabaseMacros

from pathlib import Path
from tempfile import TemporaryDirectory
with TemporaryDirectory() as scratch:
    with Database(metadata={"database_path": str(Path(scratch) / "native.sqlite")}, create=True, repair_bootstrap_rows=False, enable_maintenance=False) as db:
        fields = custom_columns.CustomFieldService(db)
        field = fields.create_field("works", "native_only")
        owner = custom_columns.OwnerRef("works", db.macros.insert_row("works", {"work_title": "Native"}))
        fields.set_value(owner, field.id, "value")
        assert fields.get_value(owner, field.id) == "value"
assert not any(n.startswith('LiuXin_alpha.utils.calibre_compat.custom_columns') for n in sys.modules)
for owner, methods in (
    (DriverWrapper, ('create_custom_column', 'set_custom_column_metadata', 'delete_custom_column', 'get_custom_tables', 'custom_table_names', 'direct_get_custom_extra')),
    (SQLiteDatabaseMacros, ('ensure_custom_column_value', 'get_cc_id_from_value', 'create_cc_table', 'create_cc_temp_tables')),
    (Database, ('get_interlinked_rows_cc',)),
    (api, ('CustomColumnsAPI', 'CustomColumnMetadata', 'CustomColumnDataAdapter')),
):
    for method in methods:
        assert not hasattr(owner, method), (owner, method)
for suffix in (
    'api.custom_columns_api', 'notify', 'database.custom_columns_mixin',
    'driver_wrapper.driver_wrapper_custom_columns_mixin',
    'database_driver_plugins.SQL.macros.cc_macros_mixin',
):
    name = 'LiuXin_alpha.databases.' + suffix
    try:
        importlib.import_module(name)
    except ModuleNotFoundError:
        pass
    else:
        raise AssertionError('Removed compatibility path still resolves: ' + name)
""",
        ],
        check=True,
    )


def test_native_series_does_not_reserve_a_companion_label(native):
    service, (owner, _) = native
    metadata = FieldMetadata()
    projection = CustomFieldCache(service, metadata)
    with projection.write():
        series = service.create_field("works", "reading", datatype="series")
        independent = service.create_field("works", "reading_index", datatype="int")
        service.set_value(owner, series.id, SeriesValue("Saga", 2.5))
        service.set_value(owner, independent.id, 42)
    assert metadata[independent.key]["id"] == independent.id
    assert metadata[independent.key]["datatype"] == "int"
    assert service.get_value(owner, series.id) == SeriesValue("Saga", 2.5)
    with projection.write():
        service.retire_field(series.id)
    assert series.key not in metadata
    assert metadata[independent.key]["id"] == independent.id
    assert service.get_value(owner, independent.id) == 42


def test_native_definitions_require_an_explicit_attachment(native):
    service, _ = native
    field = service.create_field("works", "attachment")
    with service.write() as connection:
        connection.execute(
            "UPDATE custom_columns SET custom_column_in_table=NULL WHERE custom_column_id=?",
            (field.id,),
        ).close()
    with pytest.raises(CustomFieldError, match="explicit attachment"):
        service.get_field(field.id)
    assert any(issue.code == "invalid_definition" for issue in service.audit())


def test_calibre_category_views_are_explicit_and_refresh_after_native_ddl(native):
    from LiuXin_alpha.databases.schema_readers import rows
    from LiuXin_alpha.utils.calibre_compat.custom_columns.schema import (
        refresh_category_views,
    )

    service, _ = native
    field = service.create_field("works", "categories")
    view = f"tag_browser_custom_column_{field.id}"
    with service.read() as connection:
        assert (
            rows(connection, "SELECT name FROM sqlite_master WHERE name=?", (view,))
            == []
        )
    refresh_category_views(service.db)
    with service.read() as connection:
        assert rows(connection, f'SELECT * FROM "{view}"') == []
    service.apply_field_migration(
        service.plan_field_migration(field.id, {"attachment": "manifestations"})
    )
    refresh_category_views(service.db)
    with service.read() as connection:
        assert rows(connection, f'SELECT * FROM "{view}"') == []
    service.retire_field(field.id)
    service.apply_purge(service.plan_purge(field.id))
    refresh_category_views(service.db)
    with service.read() as connection:
        assert (
            rows(connection, "SELECT name FROM sqlite_master WHERE name=?", (view,))
            == []
        )
