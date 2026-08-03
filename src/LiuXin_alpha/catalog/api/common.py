"""Shared values returned and accepted by the semantic Catalog API.

Repositories deliberately accept mappings rather than schema-specific
dataclasses.  Public aliases such as ``{"title": "Frankenstein"}`` are
normalized by the concrete repository; returned mappings use actual database
column names such as ``work_title``.

Matching is non-boolean. A :class:`MatchResult` distinguishes a safe match, a
genuine non-match, an ambiguity, and contradictory evidence::

    result = catalog.matching.works.best(
        MetadataCandidate({"title": "Frankenstein"})
    )
    if result.is_match:
        work = catalog.works.require(result.entity_id)
    elif result.requires_resolution:
        present_choices(result.alternatives, result.evidence)
    else:
        work_id = catalog.works.create({"title": "Frankenstein"})
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
WemiDirection: TypeAlias = Literal["children", "parents"]
MatchDecision: TypeAlias = Literal["match", "no_match", "ambiguous", "conflict"]
MatchEvidenceKind: TypeAlias = Literal[
    "identifier",
    "exact",
    "approximate",
    "corroborating",
    "conflict",
]


class CatalogError(RuntimeError):
    """Base error for caller-visible Catalog failures.

    Catch a narrower subclass when recovery differs between absence, rejected
    mutation, ambiguity, and contradictory identity evidence.
    """


class CatalogNotFoundError(CatalogError, KeyError):
    """Raised by ``repository.require(id)`` when the entity is absent.

    Use ``repository.get(id)`` when absence is an expected outcome. The missing
    table/entity and ID are retained in the exception message.
    """


class CatalogMutationError(CatalogError):
    """Raised when a mutation violates schema, ownership, or policy rules.

    Semantic mutation helpers use transactions, so this error normally means
    the coordinated operation was rejected or rolled back.
    """


class CatalogMatchError(CatalogError):
    """Base error when ``match_or_create`` cannot automate identity safely.

    The unresolved :class:`MatchResult` is available as :attr:`result`, so a
    caller can show alternatives and evidence without repeating the match.
    """

    def __init__(self, message: str, result: "MatchResult") -> None:
        """Store the unresolved match result with the error.

        :param message: Human-readable explanation of the failed automation.
        :param result: Match decision which requires caller intervention.
        :return: None.
        """

        super().__init__(message)
        self.result: MatchResult = result


class CatalogAmbiguousMatchError(CatalogMatchError):
    """Raised when several entities remain plausible matches.

    ``error.result.alternatives`` contains their IDs and
    ``error.result.evidence`` explains why they qualified.
    """


class CatalogMatchConflictError(CatalogMatchError):
    """Raised when decisive matching evidence points to conflicting entities.

    Do not automatically choose one alternative or create a new entity; inspect
    ``error.result.evidence`` and resolve the conflicting source data.
    """


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
    """Candidate row used by repositories and metadata matchers.

    ``data`` contains public aliases or storage columns. ``source`` describes
    provenance (for example ``"opf"`` or ``"manual"``). ``hints`` carries
    structured, non-persisted evidence understood by a matcher.

    Example::

        candidate = MetadataCandidate(
            {"title": "Frankenstein", "original_year": 1818},
            source="opf",
            hints={"identifiers": {"isbn13": "9780141439471"}},
        )
    """

    data: RowMapping
    source: str | None = None
    hints: RowMapping = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class IdentifierCandidate:
    """Scheme-aware identifier candidate.

    ``normalised_value`` is normally left as ``None`` by callers; the
    Identifier repository fills it according to ``identifier_type``.  Scheme
    aliases and punctuation are normalized before comparison.

    Example::

        candidate = IdentifierCandidate(
            identifier_type="ISBN-13",
            value="978-0-14-143947-1",
            source="publisher metadata",
        )
    """

    identifier_type: str
    value: str
    normalised_value: str | None = None
    source: str | None = None
    hints: RowMapping = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MatchEvidence:
    """One normalized observation contributing to an identity decision.

    ``score`` is normalized to ``0.0..1.0`` and ``weight`` expresses the
    policy importance of the field. Decisive identifier or conflict evidence
    can determine the outcome independently of weaker approximate evidence.
    """

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
    """Explained identity decision returned by Catalog matchers.

    Do not treat ``entity_id is None`` as permission to create.  Creation is
    safe only for ``decision == "no_match"``.  ``ambiguous`` and ``conflict``
    require intervention; repository ``match_or_create`` methods raise
    :class:`CatalogAmbiguousMatchError` or
    :class:`CatalogMatchConflictError` for those outcomes.

    Example::

        decision = catalog.matching.works.best(candidate)
        if decision.is_match:
            selected_id = decision.entity_id
        elif decision.requires_resolution:
            choices = decision.alternatives
    """

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
        """Return whether policy safely selected exactly one entity."""

        return self.decision == "match" and self.entity_id is not None

    @property
    def requires_resolution(self) -> bool:
        """Return whether ambiguity or conflict requires intervention."""

        return self.decision in {"ambiguous", "conflict"}


# Todo: WEMIBundle is better English?
@dataclass(frozen=True, slots=True)
class WemiBundle:
    """A coherent Work/Expression/Manifestation/Item metadata slice.

    A retriever follows one deterministic path through the requested root.
    ``for_item`` walks upward; broader roots choose the first relationship in
    repository priority/ID order at each lower level. Bundles are convenient
    coherent slices, not exhaustive descendant trees.

    Attached collections are plain row mappings. Relationship-specific
    metadata may be present under ``"_catalog_link"``.
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


@dataclass(frozen=True, slots=True)
class CreatedWemiStack:
    """IDs produced by one atomic Work-to-Items creation operation.

    The value is deliberately transport-friendly: Core can return it through
    either the local or HTTP client without exposing repository objects.
    """

    work_id: EntityId
    expression_id: EntityId
    manifestation_id: EntityId
    item_ids: tuple[EntityId, ...] = ()


@dataclass(frozen=True, slots=True)
class WemiAdjacency:
    """One direction of immediate WEMI hierarchy traversal.

    ``entities`` contains only the adjacent level. Relationship metadata
    remains attached to mappings under ``"_catalog_link"`` when the relation
    is represented by a link table.
    """

    level: WemiLevel
    entity_id: EntityId
    direction: WemiDirection
    related_level: WemiLevel
    entities: tuple[RowMapping, ...] = ()


@dataclass(frozen=True, slots=True)
class WemiGraph:
    """A bounded, exhaustive-within-limits descendant graph for one Work.

    Unlike :class:`WemiBundle`, this value retains every selected descendant
    and every selected Work/Expression, Expression/Manifestation, and
    Manifestation/Item edge. ``truncated_levels`` states exactly where caller
    limits prevented a complete result.
    """

    work: RowMapping
    expressions: tuple[RowMapping, ...] = ()
    manifestations: tuple[RowMapping, ...] = ()
    items: tuple[RowMapping, ...] = ()
    links: tuple[RowMapping, ...] = ()
    truncated_levels: tuple[WemiLevel, ...] = ()
