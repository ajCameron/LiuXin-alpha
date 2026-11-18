"""Prove the compatibility boundary cannot introduce a second storage engine."""

import importlib.util
import subprocess
import sys

import pytest

from LiuXin_alpha.databases.custom_columns import (
    UnsupportedStorage,
)
from LiuXin_alpha.databases.custom_columns.native_repository import names, stem
from LiuXin_alpha.utils.calibre_compat.custom_columns import (
    CustomColumns as CalibreCustomColumns,
)
from LiuXin_alpha.utils.calibre_compat.custom_columns.adapter import CustomColumnAdapter
from LiuXin_alpha.utils.calibre_compat.custom_columns.database import CustomColumnRows
from LiuXin_alpha.utils.calibre_compat.custom_columns.errors import (
    CompatibilityError,
)
from LiuXin_alpha.utils.calibre_compat.custom_columns.macros import CustomColumnMacros
from tests.databases.test_native_custom_fields import native as native
from tests.databases.test_row_write_safety import db as db
from tests.databases.test_row_write_safety import template as template


def test_native_import_does_not_load_compatibility_implementation():
    subprocess.run(
        [
            sys.executable,
            "-c",
            """
import importlib
import sys
from LiuXin_alpha.databases import custom_columns as native

assert native.CustomFieldService.__module__.endswith(".native_service")
assert "__getattr__" not in vars(native)
assert not hasattr(native.CustomFieldService, "_prepare_fields")
assert not hasattr(native.CustomFieldService, "_prepare_storage_registry")
assert not any(name.startswith("LiuXin_alpha.caches") for name in sys.modules)
assert "LiuXin_alpha.utils.calibre_compat.metadata.field_metadata" not in sys.modules
assert not hasattr(native.native_types, "CompatibilityError")
for name in (
    "CustomColumns", "CustomColumnsManager", "CustomColumnsAPI", "CompatibilityError",
    "CustomColumnDataAdapter", "CustomColumnMetadata",
):
    assert name not in native.__all__
    assert not hasattr(native, name), name
for module in (
    "custom_columns", "custom_columns_manager", "cc_crud_columns_mixin",
    "cc_delete_methods_mixin", "cc_get_methods_mixin", "cc_names_mixin",
    "cc_set_methods_mixin", "value_writes",
):
    path = native.__name__ + "." + module
    try:
        importlib.import_module(path)
    except ModuleNotFoundError as error:
        assert error.name == path
    else:
        raise AssertionError("Removed compatibility import still resolves: " + path)
assert not any(
    name.startswith("LiuXin_alpha.utils.calibre_compat.custom_columns")
    for name in sys.modules
)
""",
        ],
        check=True,
    )
    assert (
        importlib.util.find_spec(
            "LiuXin_alpha.utils.calibre_compat.custom_columns.legacy_conversion"
        )
        is None
    )


@pytest.mark.parametrize("route", ["facade", "wrapper", "core"])
def test_every_compatibility_creation_route_uses_native_storage(native, route):
    service, (owner, _) = native
    facade = CalibreCustomColumns(service.db, table="works")
    if route == "core":
        from types import SimpleNamespace

        from LiuXin_alpha.configuration import current_preferences
        from LiuXin_alpha.core.commands import CoreCommand
        from LiuXin_alpha.core.runtime import CoreRuntime

        runtime = CoreRuntime(
            library=SimpleNamespace(database=service.db, storage=None),
            preferences=current_preferences(),
        )
        try:
            result = runtime.execute_command(
                CoreCommand(
                    "custom-fields.create",
                    {
                        "name": "Labels",
                        "label": "labels",
                        "table": "works",
                        "is_multiple": True,
                    },
                )
            )
            assert result.ok
            number = result.result["num"]
        finally:
            runtime.shutdown()
    else:
        creator = facade if route == "facade" else CustomColumnAdapter(service.db)
        number = creator.create_custom_column(
            "Labels", label="labels", in_table="works", is_multiple=True
        )
    field = service.get_field(number)
    assert field.mode == "set"
    service.set_value(owner, number, ["Alpha", "alpha", "Beta"])
    facade.refresh_db_custom_columns_metadata()
    assert facade.get_custom(owner.id, num=number, index_is_id=True) == [
        "Alpha",
        "Beta",
    ]
    with service.read() as conn:
        assert service.repository.version(conn, field) == 2
        table, link = names(field)
        assert stem(table) + "_key" in service.repository.columns(conn, table)
        assert stem(link) + "_ordinal" in service.repository.columns(conn, link)
    assert not hasattr(service, "apply_upgrade")
    assert not hasattr(service, "plan_upgrade")


def test_empty_old_definition_schema_is_rejected_without_changes(native):
    service, _ = native
    with service.write() as conn:
        conn.execute(
            "ALTER TABLE custom_columns DROP COLUMN custom_column_storage_version"
        ).close()
    with service.read() as conn:
        before = service.repository.fingerprint(conn)
    for operation in (
        service.list_fields,
        lambda: CalibreCustomColumns(service.db, table="works"),
        lambda: service.create_field("works", "forbidden"),
    ):
        with pytest.raises(
            UnsupportedStorage, match="v1 format support has been removed"
        ):
            operation()
    assert any(issue.code == "unsupported_storage" for issue in service.audit())
    with service.read() as conn:
        assert service.repository.fingerprint(conn) == before


def test_compatibility_macros_use_native_identity_ordering_and_rollback(native):
    service, (first, second) = native
    field = service.create_field("works", "macros", mode="set")
    table, link = names(field)
    macros = CustomColumnMacros(service.db)
    revision = service.get_field(field.id).revision
    one = macros.ensure_custom_column_value(table, "Café")
    assert service.get_field(field.id).revision == revision + 1
    assert macros.ensure_custom_column_value(table, "CAFE\u0301") == one
    assert service.get_field(field.id).revision == revision + 1
    two = macros.add_cc_table_value(table, "Second")
    with service.write() as conn:
        macros.add_cc_link_with_extra_multi(
            link, [(first.id, one), (first.id, two), (second.id, one)], conn=conn
        )
    assert [entry.value for entry in service.get_value(first, field.id)] == [
        "Café",
        "Second",
    ]
    before = service.get_field(field.id).revision
    with pytest.raises(RuntimeError, match="abort"):
        with service.write() as conn:
            macros.delete_cc_item(table, link, one, conn=conn)
            assert [entry.value for entry in service.get_value(first, field.id)] == [
                "Second"
            ]
            raise RuntimeError("abort")
    assert service.get_field(field.id).revision == before
    assert len(service.get_value(first, field.id)) == 2
    with pytest.raises(CompatibilityError, match="managed transaction"):
        macros.delete_cc_item(table, link, one, conn=service.db.driver.conn)
    assert service.audit() == ()


def test_nonrepresentable_compatibility_creation_has_no_effects(native):
    service, _ = native
    facade = CalibreCustomColumns(service.db, table="works")
    with service.read() as conn:
        before = service.repository.fingerprint(conn)
    with pytest.raises(NotImplementedError, match="native API"):
        facade.create_custom_column(
            "Series", label="series", datatype="series", is_multiple=True
        )
    with service.read() as conn:
        assert service.repository.fingerprint(conn) == before


def test_series_bulk_positions_keep_legacy_positional_mapping(native):
    service, (first, second) = native
    facade = CalibreCustomColumns(service.db, table="works")
    number = facade.create_custom_column("Series", label="series", datatype="series")
    facade.set_custom_bulk(
        [first.id, second.id], "Saga", num=number, extras={0: 2.5, 1: 7.0}
    )
    assert service.get_value(first, number).position == 2.5
    assert service.get_value(second, number).position == 7.0


def test_storage_registry_publishes_schema_changes_only_after_commit(native):
    service, _ = native
    database = service.db
    registry = database.custom_tables
    before = set(registry)
    with pytest.raises(RuntimeError, match="abort"):
        with service.write():
            service.create_field("works", "aborted")
            assert registry == before
            raise RuntimeError("abort")
    assert registry == before
    with service.write():
        field = service.create_field("works", "registry")
        assert registry == before
    registry = database.custom_tables
    assert database.driver_wrapper.custom_tables is registry
    assert set(names(field)) <= registry
    assert set(names(field)) <= database.all_tables
    changed = service.plan_field_migration(field.id, {"attachment": "manifestations"})
    old_link = names(field)[1]
    with service.write():
        service.apply_field_migration(changed)
        assert old_link in registry
    field = service.get_field(field.id)
    registry = database.custom_tables
    assert database.driver_wrapper.custom_tables == registry
    assert old_link not in registry and old_link not in database.all_tables
    assert set(names(field)) <= registry
    service.retire_field(field.id)
    service.apply_purge(service.plan_purge(field.id))
    assert database.custom_tables == before
    assert database.driver_wrapper.custom_tables is database.custom_tables
    assert not (set(names(field)) & database.all_tables)


@pytest.mark.parametrize("datatype", ["text", "comments"])
def test_database_row_projection_follows_native_entry_order(native, datatype):
    service, (owner, _) = native
    field = service.create_field(
        "works", "ordered_rows", datatype=datatype, mode="list"
    )
    service.set_value(owner, field.id, ["First", "Second"])
    entries = service.get_value(owner, field.id)
    service.edit_entries(
        owner, field.id, order=[entry.id for entry in reversed(entries)]
    )
    parent = service.db.get_row_from_id("works", owner.id)
    table, _ = names(field)
    result = CustomColumnRows(service.db).get_interlinked_rows_cc(
        parent, table, link_table=field.normalized
    )
    assert [row[stem(table) + "_value"] for row in result] == ["Second", "First"]
