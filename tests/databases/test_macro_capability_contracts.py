"""Distinguish common SQL macros from the optional legacy SQLite surface."""

import pytest

from LiuXin_alpha.databases.api.portable_macros_api import DatabaseMacrosAPI
from LiuXin_alpha.databases.api.macros_api import MacrosAPI
from LiuXin_alpha.databases.database_driver_plugins._macro_state import (
    require_legacy_macros,
)
from LiuXin_alpha.databases.database_driver_plugins.PostgreSQL.databasedriver import (
    PostgresDatabaseMacros,
)
from LiuXin_alpha.databases.database_driver_plugins.SQL.macros import (
    SQLiteDatabaseMacros,
)
from LiuXin_alpha.errors import DatabaseDriverError


def test_sqlite_explicitly_provides_both_contracts():
    provider = SQLiteDatabaseMacros(None)
    assert isinstance(provider, DatabaseMacrosAPI)
    assert isinstance(provider, MacrosAPI)
    assert require_legacy_macros(provider) is provider


def test_postgres_rejects_legacy_capability_without_opening_a_database():
    provider = PostgresDatabaseMacros(None)
    assert isinstance(provider, DatabaseMacrosAPI)
    assert not isinstance(provider, MacrosAPI)
    with pytest.raises(DatabaseDriverError, match="requires legacy SQLite macros"):
        require_legacy_macros(provider)
