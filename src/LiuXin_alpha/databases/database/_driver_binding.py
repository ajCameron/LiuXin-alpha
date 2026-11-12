"""Prepare replacement driver state without publishing or closing live resources."""

from __future__ import annotations

import re
from collections.abc import Set
from pathlib import Path
from typing import TYPE_CHECKING, Any

from LiuXin_alpha.databases.api import DatabaseDriverAPI
from LiuXin_alpha.databases.database.constants import OPTIONAL_HELPER_TABLES
from LiuXin_alpha.errors import DatabaseIntegrityError

if TYPE_CHECKING:
    from LiuXin_alpha.databases.driver_wrapper import DriverWrapper


def table_category(table: str, helpers: Set[str]) -> str:
    """Classify a known name with the facade's ordered naming conventions."""
    if table.startswith("allowed_types__"):
        return "allowed_types"
    if table in helpers:
        return "helper"
    if table.startswith("custom_column_") or re.match(
        r"[a-zA-Z0-9_]+_custom_column_[0-9]+_link", table
    ):
        return "custom"
    if re.match(r"[\sa-zA-Z0-9_]+_links", table):
        return "interlink"
    if re.match(r"[\sa-zA-Z0-9_]+_intralink", table):
        return "intralink"
    return "main"


def schema_bindings(wrapper: DriverWrapper, helpers: Set[str]) -> dict[str, Any]:
    """Validate and materialize categories and identity using only the candidate driver."""
    tables = set(wrapper.get_tables(force_refresh=True))
    missing = set(helpers) - set(OPTIONAL_HELPER_TABLES) - tables
    if missing:
        raise DatabaseIntegrityError(
            f"Replacement database is missing required helper tables: {sorted(missing)}"
        )
    groups: dict[str, set[str]] = {
        name: set()
        for name in (
            "main",
            "helper",
            "custom",
            "interlink",
            "intralink",
            "allowed_types",
        )
    }
    for table in tables:
        groups[table_category(table, helpers)].add(table)
    groups["main"].discard("sqlite_sequence")
    dirty = groups["main"] | groups["interlink"] | groups["intralink"]
    wrapper.all_tables = tables
    wrapper.main_tables = groups["main"]
    wrapper.custom_tables = groups["custom"]
    wrapper.interlink_tables = groups["interlink"]
    wrapper.intralink_tables = groups["intralink"]
    wrapper.helper_tables = helpers
    wrapper.dirtiable_tables = dirty
    return {
        "all_tables": tables,
        "_main_tables": groups["main"],
        "custom_tables": groups["custom"],
        "_interlink_tables": groups["interlink"],
        "intralink_tables": groups["intralink"],
        "allowed_type_tables": groups["allowed_types"],
        "dirtiable_tables": dirty,
        "_uuid": wrapper.get_uuid(),
        "_library_id_": None,
        "_database_version_": None,
        "_link_has_priority": {},
        "_exists": True,
    }


def same_catalogue(first: DatabaseDriverAPI, second: DatabaseDriverAPI) -> bool:
    """Conservatively compare physical targets, never treating a file copy as the same DB."""
    if first is second:
        return True
    first_schema, second_schema = (
        getattr(first, "schema", None),
        getattr(second, "schema", None),
    )
    if first_schema is not None or second_schema is not None:
        return first_schema == second_schema and getattr(
            first, "connection_target", None
        ) == getattr(second, "connection_target", None)
    first_path = getattr(first, "database_path", None)
    second_path = getattr(second, "database_path", None)
    if first_path in (None, ":memory:") or second_path in (None, ":memory:"):
        return False
    return Path(first_path).resolve() == Path(second_path).resolve()


def connection_metadata(driver: DatabaseDriverAPI) -> dict[str, Any]:
    """Return the candidate's metadata and registered backend name when recognizable."""
    values: dict[str, Any] = {"metadata": dict(getattr(driver, "db_metadata", {}))}
    for backend in ("SQLite", "SQLite_apsw", "PostgreSQL"):
        if f".database_driver_plugins.{backend}." in type(driver).__module__:
            values["db_type"] = backend
            break
    return values


def close_driver(driver: DatabaseDriverAPI) -> None:
    """Release retired connections without committing any pending primary work."""
    connection = getattr(driver, "conn", None)
    if connection is not None:
        try:
            connection.rollback()
        except Exception:
            pass
        try:
            connection.close()
        except Exception:
            pass
    driver.conn = None
    driver.close()
