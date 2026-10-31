"""Describe physical row predicates without embedding caller-supplied SQL.

Catalog can map semantic fields and paths to these values. Database execution
validates every table/column and binds all values. They carry no WEMI policy.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from LiuXin_alpha.databases.db_types.macro_types import LINK_TYPE_UNSET
from LiuXin_alpha.databases.schema_specs import StorageLinkSpec
from LiuXin_alpha.errors import DatabaseIntegrityError


class MissingSearchReference(DatabaseIntegrityError):
    """A traversed physical relationship refers to a missing endpoint."""

    def __init__(self, table):
        self.table = table
        super().__init__(f"Missing linked {table} row during predicate search")


@dataclass(frozen=True, slots=True)
class TextMatch:
    """Match a normalized term against any named physical text column.

    Exact matches compare complete keys; other matches search for a literal
    substring. The caller prepares the term using the shared text policy.
    """

    columns: tuple[str, ...]
    term: str
    exact: bool = False


@dataclass(frozen=True, slots=True)
class ValueMatch:
    """Compare one physical column with a bound value; None means SQL NULL."""

    column: str
    value: Any


@dataclass(frozen=True, slots=True)
class IdentifierMatch:
    """Compare stored scheme/value columns with an already-normalized pair.

    Stored values use the shared identifier rules; caller-provided normalized
    overrides remain authoritative for the requested pair.
    """

    scheme_column: str
    value_column: str
    normalized_scheme: str
    normalized_value: str


@dataclass(frozen=True, slots=True)
class BooleanMatch:
    """Combine physical predicates with AND, OR or unary NOT.

    The compiler validates the operator and requires exactly one NOT operand.
    Empty AND is true; empty OR is false.
    """

    operator: Literal["and", "or", "not"]
    operands: tuple[RowPredicate, ...]


@dataclass(frozen=True, slots=True)
class LinkedMatch:
    """Require a linked endpoint satisfying a nested physical predicate.

    The declared link specification determines direction and identity. Omitted
    link_type includes every type; None explicitly selects an untyped value.
    """

    link_spec: StorageLinkSpec
    predicate: RowPredicate
    link_type: object = LINK_TYPE_UNSET


@dataclass(frozen=True, slots=True)
class ForeignMatch:
    """Follow a physical column equality into another table's predicate.

    require_target requests integrity checking for non-null local references;
    reverse foreign-key searches can leave it false when having no child is valid.
    """

    table: str
    local_column: str
    remote_column: str
    predicate: RowPredicate
    # True for an optional single-reference field; NULL is not a missing row.
    require_target: bool = False


RowPredicate = (
    TextMatch | ValueMatch | IdentifierMatch | BooleanMatch | LinkedMatch | ForeignMatch
)
