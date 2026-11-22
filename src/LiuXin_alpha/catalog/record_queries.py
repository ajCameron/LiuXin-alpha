"""Select shared metadata using literal public fields, without resolving identity.

The caller supplies rows from one read source. These pure comparisons preserve
Catalog's exact text policy; suggestions never invoke fuzzy Agent matching.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from math import isfinite
from typing import TYPE_CHECKING, Literal, cast

from LiuXin_alpha.catalog.matching.agent_matcher import AgentMatcher
from LiuXin_alpha.catalog.matching.entity_specs import EXACT_ENTITY_SPEC_BY_TABLE
from LiuXin_alpha.catalog.matching.exact_matcher import _normalise_exact_text

if TYPE_CHECKING:
    from LiuXin_alpha.catalog.repositories.base import BaseRepository


def filters(
    repository: BaseRepository,
    fields: Mapping[str, object],
    *,
    allow_empty: bool = False,
) -> dict[str, object]:
    """Validate aliases and scalar operands before any record population is read."""
    if not isinstance(fields, Mapping) or (not fields and not allow_empty):
        raise ValueError("lookup requires a nonempty public-field mapping")
    result: dict[str, object] = {}
    for alias, value in fields.items():
        if alias not in repository.input_aliases:
            raise ValueError(f"Unknown metadata field: {alias!r}")
        column = repository.input_aliases[alias]
        declared = repository._wrapper.get_declared_column_datatype(
            repository.table_name, column
        ).upper()
        if value is not None:
            if "INT" in declared:
                valid = type(value) is int
            elif any(
                t in declared for t in ("REAL", "FLOAT", "DOUBLE", "NUMERIC", "DECIMAL")
            ):
                valid = (
                    isinstance(value, (int, float))
                    and not isinstance(value, bool)
                    and isfinite(value)
                )
            elif any(t in declared for t in ("CHAR", "TEXT", "CLOB")):
                valid = isinstance(value, str)
            elif "BLOB" in declared:
                valid = isinstance(value, bytes)
            else:
                valid = type(value) in (str, int, float, bool, bytes)
            if not valid:
                raise TypeError(f"Invalid operand for {alias!r}: expected {declared}")
        if column in result and result[column] != value:
            raise ValueError(f"Conflicting aliases for {alias!r}")
        result[column] = value
    return result


def normalized(table: str, column: str, value: str) -> str:
    """Apply exact field case rules while preserving punctuation and literal syntax."""
    spec = EXACT_ENTITY_SPEC_BY_TABLE.get(table)
    casefold = table == "agents" or (
        spec is not None and column in spec.casefold_fields
    )
    return _normalise_exact_text(value, casefold=casefold)


def matches(
    table: str, row: Mapping[str, object], expected: Mapping[str, object]
) -> bool:
    """AND exact public-field comparisons; None means NULL, not omitted scope."""
    for column, wanted in expected.items():
        actual = row[column]
        if isinstance(actual, str) and isinstance(wanted, str):
            if normalized(table, column, actual) != normalized(table, column, wanted):
                return False
        elif actual != wanted:
            return False
    return True


def text_fields(table: str, row: Mapping[str, object]) -> tuple[tuple[str, str], ...]:
    """Select literal candidate fields, including parsed Agent aliases."""
    if table == "agents":
        values = [
            (column, row.get(column))
            for column in ("agent_canonical_name", "agent_sort_name")
        ]
        values.extend(
            ("agent_aliases", alias)
            for alias in AgentMatcher._aliases(row.get("agent_aliases"))
        )
    else:
        spec = EXACT_ENTITY_SPEC_BY_TABLE[table]
        values = [(column, row.get(column)) for column in spec.scalar_fields]
    return tuple((column, value) for column, value in values if isinstance(value, str))


def select(
    repository: BaseRepository,
    rows: Iterable[Mapping[str, object]],
    expected: Mapping[str, object],
    *,
    text: str | None = None,
    mode: Literal["exact", "prefix", "contains"] = "prefix",
) -> tuple[Mapping[str, object], ...]:
    """Return all exact candidates or literal suggestions in deterministic order."""
    table = repository.table_name
    selected = []
    for row in rows:
        identity = row[repository.id_column]
        if type(identity) is not int:
            raise TypeError("Shared metadata identity must be an integer")
        if not matches(table, row, expected):
            continue
        if text is not None:
            found = False
            for column, value in text_fields(table, row):
                needle, haystack = (
                    normalized(table, column, text),
                    normalized(table, column, value),
                )
                found = bool(needle) and (
                    haystack == needle
                    if mode == "exact"
                    else haystack.startswith(needle)
                    if mode == "prefix"
                    else needle in haystack
                )
                if found:
                    break
            if not found:
                continue
        selected.append(row)
    if text is None:
        selected.sort(key=lambda row: cast(int, row[repository.id_column]))
    else:
        primary = (
            "agent_canonical_name"
            if table == "agents"
            else EXACT_ENTITY_SPEC_BY_TABLE[table].primary_field
        )

        def key(row: Mapping[str, object]) -> tuple[str, str, int]:
            """Order display labels independently of which alias matched."""
            value = row[primary]
            label = "" if value is None else str(value)
            identity = row[repository.id_column]
            assert type(identity) is int
            return normalized(table, primary, label), label, identity

        selected.sort(key=key)
    return tuple(selected)
