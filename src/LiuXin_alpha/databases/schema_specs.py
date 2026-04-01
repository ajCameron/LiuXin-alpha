
"""
Specifications for elements of the schema.
"""

from __future__ import annotations

from dataclasses import dataclass, field, make_dataclass
from enum import Enum
from types import NoneType
from typing import Any, Iterable, Mapping, Optional


class RelationKind(str, Enum):
    TABLE = "table"
    VIEW = "view"


class LinkCardinality(str, Enum):
    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class StorageColumnSpec:
    name: str
    ordinal: int
    declared_type: Optional[str] = None
    affinity: Optional[str] = None
    nullable: bool = True
    has_default: bool = False
    default_value: Any = None
    is_primary_key: bool = False
    is_unique: bool = False
    references_table: Optional[str] = None
    references_column: Optional[str] = None


@dataclass(frozen=True, slots=True)
class StorageTableSpec:
    name: str
    relation_kind: RelationKind
    columns: tuple[StorageColumnSpec, ...]
    id_column: Optional[str] = None
    parent_column: Optional[str] = None
    datestamp_column: Optional[str] = None
    scratch_column: Optional[str] = None

    is_main_table: bool = False
    is_link_table: bool = False
    is_intralink_table: bool = False

    linked_tables: tuple[str, ...] = ()
    extra: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class StorageLinkSpec:
    primary_table: str
    secondary_table: str
    link_table: str
    cardinality: LinkCardinality = LinkCardinality.UNKNOWN

    primary_id_col: str = "id"
    secondary_id_col: str = "id"
    primary_link_col: str = ""
    secondary_link_col: str = ""

    priority_link_col: Optional[str] = None
    type_link_col: Optional[str] = None

    ordered: bool = False
    typed: bool = False
    nullable_fks: bool = False
    symmetric: bool = False

    allowed_types_table: Optional[str] = None
    allowed_types: tuple[str, ...] = ()
    extra_link_columns: tuple[StorageColumnSpec, ...] = ()


@dataclass(frozen=True, slots=True)
class StorageSchemaSpec:
    tables: Mapping[str, StorageTableSpec]
    interlinks: tuple[StorageLinkSpec, ...]
    intralinks: tuple[StorageLinkSpec, ...]


def build_row_dataclass_for_table(spec: StorageTableSpec) -> type:
    """
    Build a plain dataclass representing one row in the given table.

    This is intentionally simple: field names + Python-ish broad types.
    """
    dc_fields: list[tuple] = []

    for col in spec.columns:
        py_type = Any
        affinity = (col.affinity or "").upper()

        if affinity == "INTEGER":
            py_type = Optional[int] if col.nullable else int
        elif affinity == "REAL":
            py_type = Optional[float] if col.nullable else float
        elif affinity == "BLOB":
            py_type = Optional[bytes] if col.nullable else bytes
        elif affinity == "TEXT":
            py_type = Optional[str] if col.nullable else str
        else:
            py_type = Any

        if col.has_default:
            dc_fields.append((col.name, py_type, col.default_value))
        else:
            dc_fields.append((col.name, py_type))

    cls_name = "".join(part.capitalize() for part in spec.name.split("_")) + "Row"
    return make_dataclass(cls_name, dc_fields, slots=True, frozen=False)
