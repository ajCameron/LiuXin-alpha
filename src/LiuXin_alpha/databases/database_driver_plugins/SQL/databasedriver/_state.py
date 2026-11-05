"""Type sibling calls in SQL driver mixins without constructing a backend.

Concrete backends compose these partial implementations. The canonical driver
API is checked by signature-parity tests; partial mixins remain usable with
explicit collaborators and do not become abstract driver implementations.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast, Any

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.driver_api import DatabaseDriverAPI
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.db_types.driver_types import SQLConnection


class SQLDriverMixinState:
    """Expose the complete driver's operations at the composition boundary."""

    db: DatabaseAPI | None
    conn: SQLConnection | None
    tables: list[str] | None
    tables_and_columns: dict[str, list[str]] | None
    categorized_tables: dict[str, list[str]] | None
    all_column_names: set[str]
    locations: dict[str, Any] | None


def driver_operations(host: object) -> DatabaseDriverAPI:
    """Type sibling operations supplied by a composed driver or partial test host.

    A function also supports unbound mixin helpers called with a structural host;
    it does not require that host to inherit this state class.
    """
    return cast("DatabaseDriverAPI", host)
