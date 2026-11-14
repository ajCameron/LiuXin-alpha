"""Expose metadata-owned comparison rules to SQLite without new value policies.

Function names include a policy version because expression indexes persist their
results. A change to these functions' normalization semantics requires a new
version and an index migration, including when upgrading Unicode data versions.
"""

from __future__ import annotations

import json
import unicodedata
from collections.abc import Callable
from typing import Protocol, runtime_checkable
from LiuXin_alpha.databases.db_types.driver_types import RowSQLConnection
from LiuXin_alpha.errors import DatabaseIntegrityError

from LiuXin_alpha.metadata.identifiers import normalise_identifier_pair
from LiuXin_alpha.metadata.text_identity import normalize_identity_text

TEXT_KEY_FUNCTION = "liuxin_text_key_v1"
IDENTIFIER_KEY_FUNCTION = "liuxin_identifier_key_v1"


def search_index_semantics() -> str:
    """Identify the persisted comparison contract, including Python's Unicode data."""
    return json.dumps(
        {"text": TEXT_KEY_FUNCTION, "identifier": IDENTIFIER_KEY_FUNCTION,
         "policy_revision": 1, "unicode": unicodedata.unidata_version},
        sort_keys=True,
    )


def record_search_index_semantics(connection: RowSQLConnection) -> None:
    """Record the current contract only after creating or rebuilding all search indexes."""
    connection.execute(
        "CREATE TABLE IF NOT EXISTS search_index_metadata ("
        "search_index_metadata_id INTEGER PRIMARY KEY CHECK(search_index_metadata_id=1), "
        "search_index_metadata_version TEXT NOT NULL)"
    ).close()
    connection.execute(
        "INSERT INTO search_index_metadata VALUES(1, ?) ON CONFLICT(search_index_metadata_id) "
        "DO UPDATE SET search_index_metadata_version=excluded.search_index_metadata_version",
        (search_index_semantics(),),
    ).close()


def validate_search_index_semantics(connection: RowSQLConnection) -> None:
    """Refuse unversioned or incompatible expression indexes before normal access.

    An explicit rebuild is required even for read access, because SQLite may use
    persisted expression results without evaluating the newly registered function.
    """
    indexes = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='index' AND "
        "(sql LIKE '%liuxin_text_key_%' OR sql LIKE '%liuxin_identifier_key_%') LIMIT 1"
    ).fetchone()
    if indexes is None:
        return
    table = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='search_index_metadata'"
    ).fetchone()
    row = None if table is None else connection.execute(
        "SELECT search_index_metadata_version FROM search_index_metadata WHERE search_index_metadata_id=1"
    ).fetchone()
    if row is None or row[0] != search_index_semantics():
        raise DatabaseIntegrityError(
            "Search index semantics are unversioned or incompatible; explicitly run "
            "rebuild_sqlite_search_indexes before opening this catalogue"
        )


def text_key(value: object) -> str | None:
    """Normalize a searchable stored value; absent/empty fields have no key."""
    if value is None or value == "":
        return None
    return normalize_identity_text(str(value), "unicode_nfkc_whitespace_casefold")


def identifier_key(scheme: object, value: object) -> str | None:
    """Encode the canonical identifier pair, skipping invalid stored observations."""
    try:
        pair = normalise_identifier_pair(str(scheme or ""), str(value or ""))
    except (TypeError, ValueError):
        return None
    return normalized_identifier_key(pair.identifier_type, pair.normalised_value)


def normalized_identifier_key(scheme: str, value: str) -> str:
    """Serialize already-normalized components without changing their meaning."""
    return json.dumps((scheme, value), ensure_ascii=True, separators=(",", ":"))


@runtime_checkable
class SQLiteSearchConnection(Protocol):
    """Register the deterministic scalar functions used by search indexes."""

    def create_function(
        self, name: str, narg: int, func: Callable[..., str | None], /,
        *, deterministic: bool = False,
    ) -> None: ...


@runtime_checkable
class APSWSearchConnection(Protocol):
    """APSW uses a different argument order for native function registration."""

    def create_scalar_function(
        self, name: str, func: Callable[..., str | None], numargs: int = -1, /,
        *, deterministic: bool = False,
    ) -> None: ...


def register_search_functions(
    connection: SQLiteSearchConnection | APSWSearchConnection,
) -> None:
    """Register deterministic functions on a stdlib SQLite or APSW connection."""
    for name, count, function in (
        (TEXT_KEY_FUNCTION, 1, text_key),
        (IDENTIFIER_KEY_FUNCTION, 2, identifier_key),
    ):
        if isinstance(connection, APSWSearchConnection):
            connection.create_scalar_function(name, function, count, deterministic=True)
        else:
            connection.create_function(name, count, function, deterministic=True)
