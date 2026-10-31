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
    columns: tuple[str, ...]
    term: str
    exact: bool = False


@dataclass(frozen=True, slots=True)
class ValueMatch:
    column: str
    value: Any


@dataclass(frozen=True, slots=True)
class IdentifierMatch:
    scheme_column: str
    value_column: str
    normalized_scheme: str
    normalized_value: str


@dataclass(frozen=True, slots=True)
class BooleanMatch:
    operator: Literal["and", "or", "not"]
    operands: tuple[RowPredicate, ...]


@dataclass(frozen=True, slots=True)
class LinkedMatch:
    link_spec: StorageLinkSpec
    predicate: RowPredicate
    link_type: object = LINK_TYPE_UNSET


@dataclass(frozen=True, slots=True)
class ForeignMatch:
    table: str
    local_column: str
    remote_column: str
    predicate: RowPredicate
    # True for an optional single-reference field; NULL is not a missing row.
    require_target: bool = False


RowPredicate = (
    TextMatch | ValueMatch | IdentifierMatch | BooleanMatch | LinkedMatch | ForeignMatch
)
