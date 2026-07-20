
"""
Specifications for elements of the schema.

We need to be able to describe the
"""

from __future__ import annotations

from dataclasses import dataclass, field, make_dataclass
from enum import Enum
from typing import Any, Iterable, Mapping, Optional


# Todo: This isn't a super descriptive name at the moment - TableKind might be better?
class RelationKind(str, Enum):
    TABLE = "table"
    VIEW = "view"


# Todo: Think more about if we need to spec a link is to unique entities?
#       Probably the answer is no - we should never assume it, and the fact they are can be a hint for an inter-relation
#       E.g. if two items on two different books have the same isbn
#       (Though many items, being derived, may effectively have the same isbn)
class LinkCardinality(str, Enum):
    """
    What type of link are we dealing with?

    Options are
     - one-to-one - one item is linked to one other item
     - one-to-many - one item is linked to many other items (may or may not be unique)
    """
    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"
    UNKNOWN = "unknown"


class LinkKind(str, Enum):
    """Classify the optional semantics carried by a link table."""

    PLAIN = "plain"
    TYPED = "typed"
    PRIORITY = "priority"
    TYPED_PRIORITY = "typed_priority"


@dataclass(frozen=True, slots=True)
class LinkCapabilities:
    """Describe whether a physical link carries type and/or priority data."""

    primary_table: str
    secondary_table: str
    link_table: str
    type_column: Optional[str] = None
    priority_column: Optional[str] = None

    @property
    def typed(self) -> bool:
        """Return whether the link has a type column."""

        return self.type_column is not None

    @property
    def priority(self) -> bool:
        """Return whether the link has a priority column."""

        return self.priority_column is not None

    @property
    def ordered(self) -> bool:
        """Compatibility spelling matching :class:`StorageLinkSpec`."""

        return self.priority

    @property
    def both(self) -> bool:
        """Return whether the link carries both type and priority."""

        return self.typed and self.priority

    @property
    def kind(self) -> LinkKind:
        """Return the exhaustive four-way link classification."""

        if self.both:
            return LinkKind.TYPED_PRIORITY
        if self.typed:
            return LinkKind.TYPED
        if self.priority:
            return LinkKind.PRIORITY
        return LinkKind.PLAIN


# Todo: Check this doc string is accurate
@dataclass(frozen=True, slots=True)
class StorageColumnSpec:
    """
    Represents a column in a table on the schema.
    """
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
    """
    Represents a table in the schema.
    """
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
    """
    Represents a link in the schema.

    These are the properties of a link between two tables.
    """
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
    # Strict typed links identify a row by the endpoint pair and may update its
    # type. Non-exclusive role links identify rows by (pair, type).
    type_part_of_identity: bool = False
    nullable_fks: bool = False
    symmetric: bool = False

    allowed_types_table: Optional[str] = None
    allowed_types: tuple[str, ...] = ()
    extra_link_columns: tuple[StorageColumnSpec, ...] = ()


# Todo: Not very sure what this does?
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
