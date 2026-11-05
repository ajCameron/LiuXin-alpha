"""Expose metadata-owned comparison rules to SQLite without new value policies.

Function names include a policy version because expression indexes persist their
results. A change to these functions' normalization semantics requires a new
version and an index migration, including when upgrading Unicode data versions.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Protocol, runtime_checkable

from LiuXin_alpha.metadata.identifiers import normalise_identifier_pair
from LiuXin_alpha.metadata.text_identity import normalize_identity_text

TEXT_KEY_FUNCTION = "liuxin_text_key_v1"
IDENTIFIER_KEY_FUNCTION = "liuxin_identifier_key_v1"


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
