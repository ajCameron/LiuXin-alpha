"""
Share schema-cache field naming, type metadata, ordering and link-record values.

These helpers retain schema declarations and caller payloads rather than
normalizing stored text or performing database reads. Database selection
returns a reference; the calling cache decides whether to retain it.
"""

from __future__ import annotations

import dataclasses

from typing import Any, Optional

from LiuXin_alpha.databases.schema_specs import StorageTableSpec


def _column_type_map(spec: StorageTableSpec) -> dict[str, str]:
    """
    Describe each schema column using its declared type, affinity or fallback.

    Duplicate column names overwrite earlier entries. Type names are retained
    as supplied rather than normalized for a particular database driver.

    Example:
        A column with no declared type and affinity TEXT is reported as TEXT.


    :param spec: StorageTableSpec whose columns supply names and type metadata.
    :return: New dict from column name to first truthy declared_type/affinity, otherwise UNKNOWN.
    """
    return {
        col.name: col.declared_type or col.affinity or "UNKNOWN"
        for col in spec.columns
    }


def _default_value_column(spec: StorageTableSpec) -> Optional[str]:
    """
    Choose the first column outside the table's identity and bookkeeping roles.

    Skip names equal to id_column, parent_column, datestamp_column or
    scratch_column. Do not inspect data types or validate the fallback column.

    Example:
        A schema ordered as id, stamp, title with id/stamp assigned bookkeeping
        roles selects title; a table with only id falls back to id.


    :param spec: Table specification with ordered columns and optional role names.
    :return: First non-role column name, otherwise spec.id_column, possibly None.
    """
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
    """
    Order non-null values by string form and place None last.

    This is lexical ordering rather than numeric, locale or Unicode-normalized
    ordering. Different values with the same string form share a key.

    Example:
        >>> sorted([2, None, 10], key=_sort_key)
        [10, 2, None]


    :param value: Value converted with str unless it is None.
    :return: (0, string value) for non-null values, otherwise (1, empty string).
    """
    if value is None:
        return (1, "")
    return (0, str(value))


def _ensure_db(
    current_db: Any,
    passed_db: Any = None,
):
    """
    Choose an explicitly supplied database or retain the current attachment.

    Example:
        >>> current = object()
        >>> _ensure_db(current) is current
        True
        >>> _ensure_db(current, False)
        False


    :param current_db: Currently attached database, possibly None.
    :param passed_db: Override used whenever it is not None, including false-valued objects.
    :return: Selected database object unchanged; does not modify an attachment.
    :raises RuntimeError: Both database arguments are None.
    """
    db = passed_db if passed_db is not None else current_db
    if db is None:
        raise RuntimeError("Storage cache requires an attached database")
    return db


def _canonical_field_key(table_name: str, column_name: str) -> str:
    """
    Join table and column string forms into a qualified field key.

    Example:
        >>> _canonical_field_key("books", "title")
        'books.title'


    :param table_name: Table component inserted by string formatting.
    :param column_name: Column component inserted by string formatting.
    :return: table.column string without escaping, validation or whitespace stripping.
    """
    return f"{table_name}.{column_name}"


@dataclasses.dataclass(slots=True)
class _CachedLinkRecord:
    """
    Store one oriented link row with lookup and ordering metadata.

    src_id/dst_id are endpoint identities, row_dict is the retained row payload,
    and row_id is the optional physical link identity. link_type and priority
    are optional values normalized by the reader; sequence is the original row
    iteration index and breaks priority ties. This slotted dataclass is mutable
    and does not validate or copy constructor arguments.

    Example:
        >>> record = _CachedLinkRecord(1, 7, {"id": 9}, 9, "tag", 2.0, 0)
        >>> record.src_id, record.dst_id, record.sequence
        (1, 7, 0)
    """
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
