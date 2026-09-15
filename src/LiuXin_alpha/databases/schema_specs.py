
"""
Describe relational schema structure without retaining backend row objects.

Frozen slotted dataclasses record tables, columns and relationship semantics for introspection and portable consumers. They perform no schema validation or deep freezing of supplied mappings/default values. The row factory builds ordinary mutable value dataclasses, separate from database-bound Row objects.
"""

from __future__ import annotations

from dataclasses import dataclass, field, make_dataclass
from enum import Enum
from typing import Any, Iterable, Mapping, Optional


# Todo: This isn't a super descriptive name at the moment - TableKind might be better?
class RelationKind(str, Enum):
    """
    Distinguish a physical table from a view in schema descriptions.

    The string-valued members are TABLE="table" and VIEW="view"; this enum does not inspect the database.

    Example:
        >>> RelationKind.TABLE == "table"
        True
    """

    TABLE = "table"
    VIEW = "view"


# Todo: Think more about if we need to spec a link is to unique entities?
#       Probably the answer is no - we should never assume it, and the fact they are can be a hint for an inter-relation
#       E.g. if two items on two different books have the same isbn
#       (Though many items, being derived, may effectively have the same isbn)
class LinkCardinality(str, Enum):
    """
    Describe endpoint multiplicity from the primary side to the secondary side.

    ONE_TO_ONE, ONE_TO_MANY, MANY_TO_ONE and MANY_TO_MANY describe uniqueness expectations; UNKNOWN records unresolved cardinality. These labels do not establish destination ownership or enforce constraints.

    Example:
        >>> LinkCardinality.MANY_TO_ONE.value
        'many_to_one'
    """
    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"
    UNKNOWN = "unknown"


class LinkKind(str, Enum):
    """
    Classify a link by its type and priority column capabilities.

    PLAIN has neither capability, TYPED has type only, PRIORITY has priority only, and TYPED_PRIORITY has both. Cardinality and lifecycle ownership are independent.

    Example:
        >>> LinkKind.TYPED_PRIORITY.value
        'typed_priority'
    """

    PLAIN = "plain"
    TYPED = "typed"
    PRIORITY = "priority"
    TYPED_PRIORITY = "typed_priority"


@dataclass(frozen=True, slots=True)
class LinkCapabilities:
    """
    Record endpoint/table names and optional type/priority column names.

    Derived properties test only whether column names are None; even an empty string counts as present. Construction does not verify identifiers or inspect a schema. Frozen fields prevent reassignment but do not validate their values.

    Example:
        >>> cap = LinkCapabilities("works", "agents", "work_agent_links", priority_column="priority")
        >>> cap.kind is LinkKind.PRIORITY
        True
    """

    primary_table: str
    secondary_table: str
    link_table: str
    type_column: Optional[str] = None
    priority_column: Optional[str] = None

    @property
    def typed(self) -> bool:
        """
        Report whether a type column was supplied.

        Example:
            >>> LinkCapabilities("a", "b", "links", type_column="role").typed
            True


        :return: True when type_column is not None, including an empty string.
        """

        return self.type_column is not None

    @property
    def priority(self) -> bool:
        """
        Report whether a priority column was supplied.

        Example:
            >>> LinkCapabilities("a", "b", "links").priority
            False


        :return: True when priority_column is not None, including an empty string.
        """

        return self.priority_column is not None

    @property
    def ordered(self) -> bool:
        """
        Expose priority capability under the StorageLinkSpec-compatible spelling.

        Example:
            >>> LinkCapabilities("a", "b", "links", priority_column="rank").ordered
            True


        :return: The same boolean as priority.
        """

        return self.priority

    @property
    def both(self) -> bool:
        """
        Report whether both type and priority column names are present.

        Example:
            >>> LinkCapabilities("a", "b", "links", type_column="role", priority_column="rank").both
            True


        :return: True only when typed and priority are both true.
        """

        return self.typed and self.priority

    @property
    def kind(self) -> LinkKind:
        """
        Return the four-way type/priority classification for these capabilities.

        Example:
            >>> LinkCapabilities("a", "b", "links").kind is LinkKind.PLAIN
            True


        :return: LinkKind selected from the typed and priority flags.
        """

        if self.both:
            return LinkKind.TYPED_PRIORITY
        if self.typed:
            return LinkKind.TYPED
        if self.priority:
            return LinkKind.PRIORITY
        return LinkKind.PLAIN


@dataclass(frozen=True, slots=True)
class StorageColumnSpec:
    """
    Record a column’s name, ordinal, type, defaults and constraint metadata.

    declared_type preserves backend declaration text; affinity is the broad type category used by row generation. nullable, primary-key and unique flags describe introspection results without enforcing them. has_default distinguishes no default from a default whose value is None. Reference fields identify an optional foreign target. default_value is retained directly and can be mutable.

    Example:
        >>> col = StorageColumnSpec("work_id", 0, affinity="INTEGER", nullable=False, is_primary_key=True)
        >>> col.is_primary_key, col.has_default
        (True, False)
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
    Describe a table/view’s ordered columns and catalog-specific roles.

    Store its relation_kind, optional ID/parent/datestamp/scratch column names, table-role flags and linked table names. extra is a fresh empty dictionary by default but supplied mappings are retained directly. Frozen fields do not make extra or nested default values immutable, nor do they validate consistency with columns.

    Example:
        >>> spec = StorageTableSpec("works", RelationKind.TABLE, (StorageColumnSpec("work_id", 0),), id_column="work_id")
        >>> spec.columns[0].name
        'work_id'
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
    Describe link endpoints, logical identity, capabilities and ownership.

    Endpoint ID columns identify rows in their own tables; link columns reference those endpoints. Cardinality describes endpoint uniqueness, while destination_owned separately records lifecycle ownership. typed/ordered and their optional column names must be consistent for validating consumers. type_part_of_identity distinguishes multiple roles for one endpoint pair from a single pair whose role can change. allowed_types_table and allowed_types describe type registries; extra_link_columns describe additional stored properties. Construction alone accepts inconsistent declarations.

    Example:
        >>> link = StorageLinkSpec("works", "agents", "work_agent_links", cardinality=LinkCardinality.MANY_TO_MANY)
        >>> link.destination_owned, link.type_part_of_identity
        (False, False)
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

    # Cardinality describes endpoint uniqueness; it does not imply lifecycle
    # ownership. Owned destinations may be updated in place by catalog writers.
    destination_owned: bool = False

    allowed_types_table: Optional[str] = None
    allowed_types: tuple[str, ...] = ()
    extra_link_columns: tuple[StorageColumnSpec, ...] = ()


@dataclass(frozen=True, slots=True)
class StorageSchemaSpec:
    """
    Collect table descriptions and interlink/intralink specs into one schema graph.

    tables maps relation names to StorageTableSpec values; interlinks and intralinks retain their supplied tuple order. This frozen outer record does not copy/freeze the tables mapping or validate that link endpoints and column names resolve.

    Example:
        >>> graph = StorageSchemaSpec(tables={}, interlinks=(), intralinks=())
        >>> graph.tables, graph.interlinks
        ({}, ())
    """

    tables: Mapping[str, StorageTableSpec]
    interlinks: tuple[StorageLinkSpec, ...]
    intralinks: tuple[StorageLinkSpec, ...]


def build_row_dataclass_for_table(spec: StorageTableSpec) -> type:
    """
    Generate a mutable slotted value dataclass from the supplied column sequence.

    Use int/float/bytes/str for INTEGER/REAL/BLOB/TEXT affinity, wrapping nullable fields in Optional; all other affinities become Any. These are annotations, not runtime value validators. nullable alone does not give a default. has_default passes default_value directly to dataclasses, without evaluating SQL expressions or reordering fields. Invalid/duplicate field names, unsuitable defaults and required fields following defaulted ones propagate factory errors. The generated class has no database connection or persistence methods.

    Example:
        >>> spec = StorageTableSpec("work_values", RelationKind.TABLE, (StorageColumnSpec("count", 0, affinity="INTEGER", nullable=False),))
        >>> ValueRow = build_row_dataclass_for_table(spec)
        >>> ValueRow(count=3).count, ValueRow.__name__
        (3, 'WorkValuesRow')


    :param spec: Table description whose column names, order, affinities, nullability and literal defaults define fields.
    :return: New class on each call, named from capitalized underscore-separated table-name parts plus Row.
    :raises TypeError: Dataclass field names or ordering are invalid.
    :raises ValueError: A field default is unsuitable for a dataclass.
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
