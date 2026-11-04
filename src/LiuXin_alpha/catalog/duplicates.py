"""Inspect duplicate values without deciding that their entities should merge.

Catalog owns comparison and identity scope; portable macros own bounded SQL.
The result contains complete groups for the selected rows, or the scan fails
explicitly when its budget is exceeded. No repair or background work runs.
"""

from __future__ import annotations

import unicodedata
from collections.abc import Mapping
from typing import Any

from LiuXin_alpha.catalog.api.common import DatabaseHandle
from LiuXin_alpha.databases.normalized_identities import normalize_identity_value
from LiuXin_alpha.databases.schema_specs import RelationKind
from LiuXin_alpha.metadata.value_policies import ColumnNormalizationProfile

DEFAULT_MAX_ROWS = 10_000
MAX_ROWS = 100_000
COMPARISONS = ("identity", "nocase", "exact")


class DuplicateScanLimit(ValueError):
    """The selected rows exceed the inspection budget; no partial report exists."""


def find_duplicates(
    db: DatabaseHandle,
    table: str,
    column: str,
    *,
    comparison: str = "identity",
    max_rows: int = DEFAULT_MAX_ROWS,
    where: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Report repeated text values within their declared identity scopes.

    ``identity`` uses the database's declared normalization profile, falling
    back to NFKC/whitespace/casefold for undeclared display text. ``nocase``
    uses NFC/casefold without trimming; ``exact`` preserves the stored text.
    All modes retain declared scope columns, including when inspecting a
    derived key. NULL and whitespace-only values are ignored. Equal Work
    titles, for example, are candidates for review, not proof of one Work.

    ``where`` supplies physical equality filters to the portable reader.
    One ID-ordered query loads at most max_rows + 1 rows. Exceeding the budget
    raises DuplicateScanLimit rather than returning incomplete groups. This
    limits row count, not byte size. Views and non-text values are refused.

    Example:
        ``find_duplicates(db, "genres", "genre")`` keeps different parents
        separate, including the NULL parent scope for root genres.
    """
    if comparison not in COMPARISONS:
        raise ValueError(f"comparison must be one of {', '.join(COMPARISONS)}")
    if isinstance(max_rows, bool) or not isinstance(max_rows, int):
        raise ValueError("max_rows must be an integer")
    if not 1 <= max_rows <= MAX_ROWS:
        raise ValueError(f"max_rows must be between 1 and {MAX_ROWS}")
    if where is not None and not isinstance(where, Mapping):
        raise ValueError("where must be an object of physical column values")

    wrapper = db.driver_wrapper
    table_spec = wrapper.get_table_spec(table)
    if table_spec.relation_kind != RelationKind.TABLE or not table_spec.id_column:
        raise ValueError("Duplicate inspection requires a physical table with a row ID")
    if column not in {entry.name for entry in table_spec.columns}:
        raise ValueError(f"Unknown column {column!r} in {table!r}")
    identity = next(
        (
            spec
            for spec in wrapper.iter_normalized_identity_specs(table=table)
            if column in (spec.value_column, spec.identity_column)
        ),
        None,
    )
    scope_columns = identity.scope_columns if identity else ()
    profile = (
        identity.normalization_profile
        if identity
        else ColumnNormalizationProfile.UNICODE_NFKC_WHITESPACE_CASEFOLD
    )
    rows, complete = db.macros.get_rows_page(table, where=where, limit=max_rows)
    if not complete:
        raise DuplicateScanLimit(
            f"Duplicate inspection exceeds max_rows={max_rows}; narrow the where "
            f"filters or raise max_rows (maximum {MAX_ROWS}). No partial report returned."
        )

    groups: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        value = row[column]
        if value is None:
            continue
        if not isinstance(value, str):
            raise ValueError(f"Duplicate inspection requires text values in {column!r}")
        if not value.strip():
            continue
        if comparison == "identity":
            value = normalize_identity_value(value, profile)
        elif comparison == "nocase":
            value = unicodedata.normalize("NFC", value).casefold()
        scope = tuple(row[name] for name in scope_columns)
        group = groups.setdefault(
            (value, *scope),
            {
                "value": value,
                "scope": dict(zip(scope_columns, scope, strict=True)),
                "row_ids": [],
            },
        )
        group["row_ids"].append(int(row[table_spec.id_column]))

    return {
        "table": table,
        "column": column,
        "comparison": comparison,
        "normalization_profile": profile.value if comparison == "identity" else None,
        "scope_columns": list(scope_columns),
        "scanned_rows": len(rows),
        "duplicates": [group for group in groups.values() if len(group["row_ids"]) > 1],
    }
