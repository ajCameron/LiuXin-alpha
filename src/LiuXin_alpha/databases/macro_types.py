"""Typed values shared by portable database macro implementations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from LiuXin_alpha.databases.schema_specs import StorageLinkSpec


class _UnsetLinkType:
    """Sentinel used when a typed-link filter was not supplied."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "LINK_TYPE_UNSET"


LINK_TYPE_UNSET = _UnsetLinkType()


@dataclass(frozen=True, slots=True)
class LinkValue:
    """Desired secondary id and writable properties for one link."""

    secondary_id: Any
    link_type: Any = None
    priority: int | float | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class LinkRow:
    """Backend-neutral complete link row, including non-standard columns."""

    primary_id: Any
    secondary_id: Any
    link_type: Any = None
    priority: int | float | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class UnreferencedRowsSpec:
    """Instructions for pruning rows which are absent from every supplied link."""

    table: str
    link_specs: tuple[StorageLinkSpec, ...]
    id_column: str | None = None
    protected_ids: tuple[Any, ...] = ()


@dataclass(frozen=True, slots=True)
class CanonicalIdentity:
    """One stored canonical value resolved through its derived identity."""

    table: str
    row_id: Any
    value_column: str
    canonical_value: Any
    identity_column: str
    identity_value: Any
    scope_values: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class NormalizedIdentityCollision:
    """Rows which would share one declared normalized identity."""

    table: str
    value_column: str
    identity_column: str
    identity_value: Any
    scope_values: Mapping[str, Any]
    row_ids: tuple[Any, ...]
    canonical_values: tuple[Any, ...]


@dataclass(frozen=True, slots=True)
class NormalizedIdentityMigrationReport:
    """Result of auditing or migrating declared normalized identities."""

    declarations_checked: int
    rows_examined: int
    rows_needing_update: int
    rows_updated: int
    columns_added: tuple[str, ...] = ()
    indexes_created: tuple[str, ...] = ()
    collisions: tuple[NormalizedIdentityCollision, ...] = ()

    @property
    def clean(self) -> bool:
        return not self.collisions


__all__ = [
    "LINK_TYPE_UNSET",
    "CanonicalIdentity",
    "LinkRow",
    "LinkValue",
    "NormalizedIdentityCollision",
    "NormalizedIdentityMigrationReport",
    "UnreferencedRowsSpec",
]
