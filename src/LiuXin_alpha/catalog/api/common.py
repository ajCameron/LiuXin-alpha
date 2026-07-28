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
MatchDecision: TypeAlias = Literal["match", "no_match", "ambiguous", "conflict"]
MatchEvidenceKind: TypeAlias = Literal[
    "identifier",
    "exact",
    "approximate",
    "corroborating",
    "conflict",
]


class CatalogError(RuntimeError):
    """
    Base error for catalog-layer failures.
    """


class CatalogNotFoundError(CatalogError, KeyError):
    """
    Raised when a required catalog entity cannot be found.
    """


class CatalogMutationError(CatalogError):
    """Raised when a catalog mutation is rejected by policy or validation."""


class CatalogMatchError(CatalogError):
    """Base error for catalog identity decisions which require intervention."""

    def __init__(self, message: str, result: "MatchResult") -> None:
        """Store the unresolved match result with the error.

        :param message: Human-readable explanation of the failed automation.
        :param result: Match decision which requires caller intervention.
        :return: None.
        """

        super().__init__(message)
        self.result: MatchResult = result


class CatalogAmbiguousMatchError(CatalogMatchError):
    """Raised when several entities remain plausible matches."""


class CatalogMatchConflictError(CatalogMatchError):
    """Raised when decisive matching evidence contradicts itself."""


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
    """
    Generic candidate object for matching or creation.
    """

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
class MatchEvidence:
    """One normalized observation used to make a match decision."""

    field: str
    kind: MatchEvidenceKind
    score: float
    weight: float
    reason: str
    candidate_value: Any = None
    existing_value: Any = None
    decisive: bool = False

    def __post_init__(self) -> None:
        """Validate normalized evidence boundaries."""

        if not isinstance(self.field, str) or not self.field:
            raise ValueError("evidence field must be a non-empty string")
        if self.kind not in {
            "identifier",
            "exact",
            "approximate",
            "corroborating",
            "conflict",
        }:
            raise ValueError(f"unknown evidence kind: {self.kind!r}")
        if not isinstance(self.score, (int, float)) or isinstance(self.score, bool):
            raise TypeError("evidence score must be numeric")
        if not isinstance(self.weight, (int, float)) or isinstance(self.weight, bool):
            raise TypeError("evidence weight must be numeric")
        if not 0.0 <= float(self.score) <= 1.0:
            raise ValueError("evidence score must be between zero and one")
        if float(self.weight) < 0.0:
            raise ValueError("evidence weight cannot be negative")
        if not isinstance(self.decisive, bool):
            raise TypeError("decisive must be a boolean")
        object.__setattr__(self, "score", float(self.score))
        object.__setattr__(self, "weight", float(self.weight))


@dataclass(frozen=True, slots=True)
class MatchResult:
    """Explained identity decision returned by catalog matchers."""

    entity_id: EntityId | None
    confidence: float
    reason: str
    matched_on: tuple[str, ...] = ()
    candidate: RowMapping | None = None
    decision: MatchDecision | None = None
    evidence: tuple[MatchEvidence, ...] = ()
    alternatives: tuple[EntityId, ...] = ()

    def __post_init__(self) -> None:
        """Derive the legacy-compatible default decision and validate shape."""

        if not isinstance(self.confidence, (int, float)) or isinstance(
            self.confidence,
            bool,
        ):
            raise TypeError("confidence must be numeric")
        if not 0.0 <= float(self.confidence) <= 1.0:
            raise ValueError("confidence must be between zero and one")
        object.__setattr__(self, "confidence", float(self.confidence))

        decision = self.decision
        if decision is None:
            decision = "match" if self.entity_id is not None else "no_match"
            object.__setattr__(self, "decision", decision)
        if decision not in {"match", "no_match", "ambiguous", "conflict"}:
            raise ValueError(f"unknown match decision: {decision!r}")
        if decision == "match" and self.entity_id is None:
            raise ValueError("a match decision requires an entity_id")
        if decision != "match" and self.entity_id is not None:
            raise ValueError("only a match decision can select an entity_id")
        if any(
            not isinstance(entity_id, int) or isinstance(entity_id, bool)
            for entity_id in self.alternatives
        ):
            raise TypeError("alternatives must contain integer entity IDs")

    @property
    def is_match(self) -> bool:
        """Return whether one entity was safely selected."""

        return self.decision == "match" and self.entity_id is not None

    @property
    def requires_resolution(self) -> bool:
        """
        Return whether automation must stop for caller intervention.

        :return:
        """

        return self.decision in {"ambiguous", "conflict"}


# Todo: WEMIBundle is better English?
@dataclass(frozen=True, slots=True)
class WemiBundle:
    """
    A coherent slice through Work / Expression / Manifestation / Item metadata.
    """

    work: RowMapping | None = None
    expression: RowMapping | None = None
    manifestation: RowMapping | None = None
    item: RowMapping | None = None
    agents: Sequence[RowMapping] = ()
    identifiers: Sequence[RowMapping] = ()
    titles: Sequence[RowMapping] = ()
    notes: Sequence[RowMapping] = ()
    links: Sequence[RowMapping] = ()
