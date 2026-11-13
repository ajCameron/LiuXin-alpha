"""Immutable native custom-field identities, values, plans and error contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

type Scalar = str | int | float | bool | datetime
type CollectionMode = Literal["scalar", "set", "list"]
type FieldRef = int | tuple[str, str]


class CustomFieldError(ValueError):
    """A custom-field operation cannot satisfy its declared contract."""


class UnsupportedStorage(CustomFieldError):
    """The catalogue uses an unsupported custom-field storage format."""


class StalePlan(CustomFieldError):
    """The source catalogue changed after a plan was prepared."""


@dataclass(frozen=True, order=True)
class OwnerRef:
    """Identify one persistent owner within its physical attachment table."""

    table: str
    id: int


@dataclass(frozen=True)
class FieldDefinition:
    """Snapshot a scoped field definition; display metadata is a detached dictionary."""

    id: int
    attachment: str
    label: str
    name: str
    datatype: str = "text"
    mode: CollectionMode = "scalar"
    editable: bool = True
    display: dict[str, Any] = field(default_factory=dict)
    revision: int = 0
    retired: bool = False

    @property
    def normalized(self) -> bool:
        return self.datatype in {"text", "enumeration", "series", "rating"}

    @property
    def key(self) -> str:
        return f"#{self.attachment}.{self.label}"


@dataclass(frozen=True)
class SeriesValue:
    """Supply a Series name and the position belonging to this membership."""

    value: str
    position: float = 1.0


@dataclass(frozen=True)
class ValueEntry:
    """Return one typed collection membership with stable ID, order and position."""

    id: int
    value: Scalar
    ordinal: int
    position: float | None = None
    value_id: int | None = None


@dataclass(frozen=True)
class FieldPatch:
    """Replace one owner/field value, optionally requiring its current revision."""

    owner: OwnerRef
    field: FieldRef
    value: Any
    expected_revision: int | None = None


@dataclass(frozen=True)
class WriteReceipt:
    """Report affected owners, field revisions and explicit migration identity mappings."""

    owners: tuple[OwnerRef, ...]
    fields: tuple[int, ...]
    revisions: dict[int, int]
    entry_ids: dict[int, int | None] = field(default_factory=dict)
    value_ids: dict[int, int | None] = field(default_factory=dict)


@dataclass(frozen=True)
class Predicate:
    """Filter a stored field by a typed operation and any/all collection semantics."""

    field: FieldRef
    operator: Literal["eq", "in", "contains", "prefix", "range", "unset"]
    value: Any = None
    quantifier: Literal["any", "all"] = "any"


@dataclass(frozen=True)
class Page:
    """Return an ordered page and an exclusive continuation cursor, or None at end."""

    items: tuple[Any, ...]
    next_cursor: int | None


@dataclass(frozen=True)
class Issue:
    """Locate an integrity fault without inferring a replacement or performing repair."""

    code: str
    field_id: int | None
    detail: str
    owner_id: int | None = None
    entry_id: int | None = None


@dataclass(frozen=True)
class ChangePlan:
    """Carry reviewed intent, blockers, effects and a source fingerprint for revalidation."""

    kind: Literal["migration", "purge", "repair"]
    fingerprint: str
    payload: dict[str, Any]
    issues: tuple[Issue, ...] = ()
    effects: tuple[str, ...] = ()
