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


__all__ = [
    "LINK_TYPE_UNSET",
    "LinkRow",
    "LinkValue",
    "UnreferencedRowsSpec",
]
