"""Shared catalog API types.

Keep these deliberately lightweight. The catalog API should be usable before the
final metadata dataclasses are settled, and can be tightened later as the domain
model stabilises.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    Mapping,
    MutableMapping,
    Protocol,
    Sequence,
    TypeAlias,
)

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import PortableMacrosAPI

EntityId: TypeAlias = int
RowMapping: TypeAlias = Mapping[str, Any]
RowInput: TypeAlias = MutableMapping[str, Any] | Mapping[str, Any]
WemiLevel: TypeAlias = Literal["work", "expression", "manifestation", "item"]


class CatalogError(RuntimeError):
    """Base error for catalog-layer failures."""


class CatalogNotFoundError(CatalogError, KeyError):
    """Raised when a required catalog entity cannot be found."""


class CatalogMutationError(CatalogError):
    """Raised when a catalog mutation is rejected by policy or validation."""


class DatabaseHandle(Protocol):
    """
    Minimal structural placeholder for the raw database object.

    This is intentionally tiny. Concrete repositories should adapt to the real
    database API through small helper methods rather than forcing the raw database
    package to import catalog concepts.
    """

    @property
    def macros(self) -> "PortableMacrosAPI":
        """Return the portable database macro surface."""

        ...


@dataclass(frozen=True, slots=True)
class MetadataCandidate:
    """Generic candidate object for matching or creation."""

    data: RowMapping
    source: str | None = None
    hints: RowMapping = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class IdentifierCandidate:
    """Identifier candidate used for ISBNs, URNs, URLs, Calibre ids, etc."""

    identifier_type: str
    value: str
    normalised_value: str | None = None
    source: str | None = None
    hints: RowMapping = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MatchResult:
    """Result returned by catalog matchers."""

    entity_id: EntityId | None
    confidence: float
    reason: str
    matched_on: tuple[str, ...] = ()
    candidate: RowMapping | None = None

    @property
    def is_match(self) -> bool:
        return self.entity_id is not None


@dataclass(frozen=True, slots=True)
class WemiBundle:
    """A coherent slice through Work / Expression / Manifestation / Item metadata."""

    work: RowMapping | None = None
    expression: RowMapping | None = None
    manifestation: RowMapping | None = None
    item: RowMapping | None = None
    agents: Sequence[RowMapping] = ()
    identifiers: Sequence[RowMapping] = ()
    titles: Sequence[RowMapping] = ()
    notes: Sequence[RowMapping] = ()
    links: Sequence[RowMapping] = ()
