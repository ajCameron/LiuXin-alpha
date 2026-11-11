"""Check lazy plugin resolution and explicit file-location requirements."""

import sqlite3
from contextlib import closing
from types import ModuleType

import pytest

from LiuXin_alpha.databases.database_driver_plugins import registry
from LiuXin_alpha.errors import DatabaseDriverError


@pytest.fixture
def isolated_registry(monkeypatch):
    monkeypatch.setattr(registry, "_DRIVER_REGISTRY", {})
    monkeypatch.setattr(registry, "_BUILDER_CACHE", {})
    monkeypatch.setattr(registry, "_DIRECT_ACCESS_CACHE", {})
    registry.register_database_driver(
        "Example",
        driver_module="example.driver",
        builder_module="example.builder",
        direct_access_module="example.direct",
        aliases=("alias",),
    )


def test_aliases_share_lazy_module_caches(isolated_registry, monkeypatch):
    modules = {
        name: ModuleType(name)
        for name in ("example.driver", "example.builder", "example.direct")
    }
    imported = []

    def load(name):
        imported.append(name)
        return modules[name]

    monkeypatch.setattr(registry.importlib, "import_module", load)
    assert imported == []
    assert registry.get_direct_access_module(
        "Example"
    ) is registry.get_direct_access_module("ALIAS")
    assert registry.get_database_builder_module(
        "alias"
    ) is registry.get_database_builder_module("Example")
    assert imported == ["example.direct", "example.builder"]


def test_namespace_package_requires_explicit_location(isolated_registry, monkeypatch):
    package = ModuleType("example")
    monkeypatch.setattr(registry.importlib, "import_module", lambda name: package)
    with pytest.raises(DatabaseDriverError, match="package_dir"):
        registry.get_driver_location("Example")
    registry.register_database_driver(
        "Example", driver_module="example.driver", package_dir="/plugins/example"
    )
    assert registry.get_driver_location("Example") == "/plugins/example"


def test_builder_results_remain_extension_defined(
    isolated_registry, monkeypatch, tmp_path
):
    builder = ModuleType("example.builder")
    monkeypatch.setattr(registry.importlib, "import_module", lambda name: builder)
    assert registry.create_new_database("Example", tmp_path) is builder
    result = object()
    received = []

    def create(target):
        received.append(target)
        return result

    monkeypatch.setattr(builder, "create_new_database", create, raising=False)
    assert registry.create_new_database("alias", tmp_path) is result
    assert received == [tmp_path]


@pytest.mark.parametrize("db_type", ["SQLite", "sqlite", "SQLite_apsw", "apsw"])
def test_builtin_builder_lookup_creates_calibre_schema(db_type):
    """Keep the explicit Calibre builder usable through each SQLite registration."""
    builder = registry.get_database_builder_module(db_type)
    with closing(sqlite3.connect(":memory:")) as connection:
        builder.create_new_database(connection)
        builder.validate_metadata_database(connection)
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
    assert {"books", "authors", "library_id"} <= tables
    assert "works" not in tables
