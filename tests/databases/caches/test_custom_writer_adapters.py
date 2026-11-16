"""Legacy writers explicitly adapt both raw databases and Catalog hosts."""

from types import SimpleNamespace

import pytest

from LiuXin_alpha.caches.write.base_writer import BaseWriter
from LiuXin_alpha.caches.write.generic_writers.one_to_one_writer import OneToOneWriter
from LiuXin_alpha.catalog.catalog import Catalog
from LiuXin_alpha.databases.custom_columns import CustomFieldService, OwnerRef
from LiuXin_alpha.databases.custom_columns.native_repository import names
from tests.databases.custom_field_fixtures import sqlite_db as sqlite_db
from tests.databases.custom_field_fixtures import sqlite_template as sqlite_template


@pytest.mark.parametrize("host_kind", ["database", "catalog"])
def test_custom_writer_updates_and_deletes_with_either_host(sqlite_db, host_kind):
    service = CustomFieldService(sqlite_db)
    definition = service.create_field("works", "writer_value")
    first, second = (
        OwnerRef("works", sqlite_db.macros.insert_row("works", {"work_title": title}))
        for title in ("First", "Second")
    )
    service.set_value(first, definition.id, "old")
    service.set_value(second, definition.id, "untouched")
    replacement = service.ensure_value(definition.id, "replacement")
    field = SimpleNamespace(metadata={"table": names(definition)[1]})
    host = sqlite_db if host_kind == "database" else Catalog(sqlite_db)

    OneToOneWriter.cc_one_one_updater(host, field, {first.id: replacement})
    assert service.get_value(first, definition.id) == "replacement"
    assert service.get_value(second, definition.id) == "untouched"

    BaseWriter.custom_delete_one_to_one_in_other(host, field, [(first.id,)])
    assert service.get_value(first, definition.id) is None
    assert service.get_value(second, definition.id) == "untouched"
