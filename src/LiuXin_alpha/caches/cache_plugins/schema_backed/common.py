"""
Shared helpers for schema-backed storage cache implementations.
"""

from __future__ import annotations

import dataclasses

from typing import Any, Optional

from LiuXin_alpha.databases.schema_specs import StorageTableSpec


def _column_type_map(spec: StorageTableSpec) -> dict[str, str]:
    return {
        col.name: col.declared_type or col.affinity or "UNKNOWN"
        for col in spec.columns
    }


def _default_value_column(spec: StorageTableSpec) -> Optional[str]:
    skip = {
        spec.id_column,
        spec.parent_column,
        spec.datestamp_column,
        spec.scratch_column,
    }
    for col in spec.columns:
        if col.name not in skip:
            return col.name
    return spec.id_column


def _sort_key(value: Any) -> tuple[int, str]:
    if value is None:
        return (1, "")
    return (0, str(value))


def _ensure_db(
    current_db: Any,
    passed_db: Any = None,
):
    db = passed_db if passed_db is not None else current_db
    if db is None:
        raise RuntimeError("Storage cache requires an attached database")
    return db


def _canonical_field_key(table_name: str, column_name: str) -> str:
    return f"{table_name}.{column_name}"


@dataclasses.dataclass(slots=True)
class _CachedLinkRecord:
    src_id: int
    dst_id: int
    row_dict: dict[str, Any]
    row_id: Optional[int]
    link_type: Optional[str]
    priority: Optional[float]
    sequence: int


__all__ = [
    "_CachedLinkRecord",
    "_canonical_field_key",
    "_column_type_map",
    "_default_value_column",
    "_ensure_db",
    "_sort_key",
]
