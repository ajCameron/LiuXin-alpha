"""
Shared candidate, matching, error, and WEMI result values for Catalog callers.

Repositories interpret row mappings using their own field aliases and return
database column names. The aliases in this module describe types; they do not
validate IDs, column names, or WEMI levels at runtime. Candidate and retrieval
dataclasses retain supplied values. Only MatchEvidence and MatchResult implement
additional constructor checks, and their frozen fields do not freeze nested data.

Matching distinguishes match, no_match, ambiguous, and conflict. A missing selected
ID alone does not permit automatic creation; inspect the decision or use the
repository's match_or_create operation to apply its identity safeguards.

Example:
    >>> result = MatchResult(None, 1, "duplicate names", decision="ambiguous")
    >>> result.is_match, result.requires_resolution
    (False, True)
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
    """
    Base exception for failures reported through the Catalog error hierarchy.

    Catch a more specific subclass when absence, rejected mutation, and unresolved identity need
    different recovery. Underlying database or validation errors are not necessarily wrapped in this
    hierarchy.

    Example:
        >>> isinstance(CatalogNotFoundError("missing Work"), CatalogError)
        True
    """


class CatalogNotFoundError(CatalogError, KeyError):
    """
    Report an absent required Catalog entity.

    Repository require operations use this error when lookup cannot supply a row; use get when a
    missing row is an expected result. It also inherits KeyError, and its message is supplied by the
    raising operation.

    Example:
        >>> error = CatalogNotFoundError("missing Work 42")
        >>> isinstance(error, KeyError)
        True
    """


class CatalogMutationError(CatalogError):
    """
    Report a rejected Catalog mutation or an unsupported mutation policy.

    This exception carries the caller-supplied explanation. It performs no rollback itself:
    atomicity and any completed effects depend on the operation that raises it. Database and
    validation failures may propagate as other exception types.

    Example:
        >>> error = CatalogMutationError("unsupported merge policy")
        >>> error.args
        ('unsupported merge policy',)
    """


class CatalogMatchError(CatalogError):
    """
    Carry an identity decision that a Catalog operation cannot resolve automatically.

    Matching errors retain the same result object so callers can inspect evidence and alternatives
    without repeating the query. The constructor neither validates that object nor requires an
    ambiguous or conflicting decision.

    Example:
        >>> result = MatchResult(None, 0.8, "two candidates", decision="ambiguous")
        >>> error = CatalogMatchError("choose an identity", result)
        >>> error.result is result
        True


    :ivar result: Decision supplied to the constructor, retained by reference.
    """

    def __init__(self, message: str, result: "MatchResult") -> None:
        """
        Attach the supplied decision and initialize the exception message.

        Only message enters the exception args tuple. The result is assigned unchanged; constructing
        an error does not raise it or apply a matching policy.

        Example:
            >>> error = CatalogMatchError("review required", MatchResult(None, 0, "absent"))
            >>> error.args
            ('review required',)


        :param message: Human-readable reason automatic identity handling stopped.
        :param result: Matching decision to expose on the exception without copying or validation.
        :return: None after the message and result have been stored.
        """

        super().__init__(message)
        self.result: MatchResult = result


class CatalogAmbiguousMatchError(CatalogMatchError):
    """
    Signal that multiple plausible identities need caller resolution.

    Repository match_or_create operations raise this instead of selecting an arbitrary alternative.
    Inspect result.alternatives and result.evidence; the inherited constructor does not enforce
    their completeness or decision value.

    Example:
        >>> result = MatchResult(None, 1, "duplicate names", decision="ambiguous", alternatives=(2, 3))
        >>> error = CatalogAmbiguousMatchError("choose a record", result)
        >>> error.result.alternatives
        (2, 3)
    """


class CatalogMatchConflictError(CatalogMatchError):
    """
    Signal contradictory identity evidence that prevents automatic matching.

    Inspect the attached decision before correcting source data or choosing an identity. The error
    records the reported conflict; its inherited constructor does not itself compare candidates or
    verify that the decision is conflict.

    Example:
        >>> result = MatchResult(None, 1, "identifiers disagree", decision="conflict")
        >>> CatalogMatchConflictError("review identifiers", result).result.requires_resolution
        True
    """


class DatabaseHandle(Protocol):
    """
    Describe the minimal portable-macro attribute expected of a database handle.

    This typing protocol is a small dependency placeholder, not the complete database contract
    required by all Catalog operations. Concrete repositories and writers can require additional row
    and schema APIs. The protocol is not runtime-checkable and does not open, validate, or close a
    database.

    Example:
        >>> handle: DatabaseHandle = db  # doctest: +SKIP
        >>> macros = handle.macros  # doctest: +SKIP
    """

    @property
    def macros(self) -> "PortableMacrosAPI":
        """
        Expose the database handle's portable macro operations.

        Implementations provide the adapter for database-independent row and link operations. This
        protocol property specifies access without constructing an adapter or guaranteeing
        connection readiness.

        Example:
            >>> macros = handle.macros  # doctest: +SKIP


        :return: Portable macro surface supplied by the concrete database handle.
        """

        ...


@dataclass(frozen=True, slots=True)
class MetadataCandidate:
    """
    Carry proposed metadata, provenance, and optional matching hints.

    Repositories interpret public aliases or storage column names in data; matchers interpret hints
    as auxiliary evidence. Construction performs no normalization, validation, database lookup, or
    persistence. Frozen fields prevent rebinding but retain the supplied mappings, including mutable
    content. An omitted hints argument receives a fresh empty dictionary.

    Example:
        >>> row = {"title": "Frankenstein"}
        >>> candidate = MetadataCandidate(row, source="opf")
        >>> candidate.data is row
        True
        >>> candidate.hints
        {}


    :ivar data: Proposed field values using aliases or database columns understood by the recipient.
    :ivar source: Optional provenance label such as opf or manual; not checked here.
    :ivar hints: Additional evidence for the consuming matcher, retained without copying.
    """

    data: RowMapping
    source: str | None = None
    hints: RowMapping = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class IdentifierCandidate:
    """
    Carry a scheme, original identifier text, and optional normalized text.

    Construction preserves every supplied value. Scheme aliases and punctuation are interpreted by
    repository or matching normalization, not by this frozen record. In particular, leaving
    normalised_value unset does not fill it later on this instance. Supplied hints remain shared;
    omitted hints receive a new dict.

    Example:
        >>> identifier = IdentifierCandidate("ISBN-13", "978-0-306-40615-7")
        >>> identifier.identifier_type, identifier.normalised_value
        ('ISBN-13', None)


    :ivar identifier_type: Scheme name supplied by the caller, without alias normalization here.
    :ivar value: Original identifier value, preserved without stripping or punctuation changes.
    :ivar normalised_value: Optional already-normalized value for the consumer; defaults to None.
    :ivar source: Optional provenance label, independent of the identifier scheme.
    :ivar hints: Additional matcher evidence; nested values are not frozen or validated.
    """

    identifier_type: str
    value: str
    normalised_value: str | None = None
    source: str | None = None
    hints: RowMapping = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MatchEvidence:
    """
    Describe one scored observation used to explain a matching decision.

    Construction validates field presence, evidence kind, numeric score/weight, and the boolean
    decisive flag. Scores must be in the inclusive unit interval; weights cannot be negative. Both
    become floats. This value does not calculate similarity or apply policy. Reason and compared
    values are retained unchecked, and frozen attributes do not freeze nested values.

    Example:
        >>> evidence = MatchEvidence("title", "exact", 1, 2, "same title")
        >>> evidence.score, evidence.weight, evidence.decisive
        (1.0, 2.0, False)


    :ivar field: Nonempty field name; whitespace-only strings are accepted unchanged.
    :ivar kind: Identifier, exact, approximate, corroborating, or conflict evidence category.
    :ivar score: Integer or float in [0, 1], excluding bool; stored as a float.
    :ivar weight: Numeric policy weight excluding bool; negative values fail, but NaN and positive infinity are accepted.
    :ivar reason: Human-readable explanation supplied by the matcher, not validated here.
    :ivar candidate_value: Optional observed candidate value, retained by reference.
    :ivar existing_value: Optional stored value used for comparison, retained by reference.
    :ivar decisive: Boolean indicating policy-significant evidence; no policy is executed here.
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
        """
        Validate evidence fields and convert score and weight to floats.

        Called by the dataclass constructor. Field is checked for nonempty string content without
        stripping; score and weight accept int/float but reject bool. Score rejects nonfinite
        values, while weight only rejects values below zero and therefore permits NaN and positive
        infinity. Kind must be a known category and decisive must be bool. Reason and compared
        values are not inspected.

        Example:
            >>> evidence = MatchEvidence("title", "exact", 1, 0, "equal")
            >>> evidence.score, evidence.weight
            (1.0, 0.0)
            >>> MatchEvidence("title", "exact", 2, 1, "invalid")
            Traceback (most recent call last):
            ...
            ValueError: evidence score must be between zero and one


        :return: None after validating the fields and storing float score/weight values.
        :raises TypeError: If numeric/boolean fields have unsupported types or kind is unhashable.
        :raises ValueError: If field is empty or not a string, kind is unknown, score is out of range, or weight is negative.
        :raises OverflowError: If an integer score or weight cannot be converted to float.
        """

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
    """
    Record an explained match, non-match, ambiguity, or identity conflict.

    An omitted decision is derived from whether entity_id is None. Only match may carry a selected
    ID; confidence is converted to a float in [0, 1]. Alternatives must contain integers excluding
    bool, but need not be positive or distinct. Neither the selected ID's type nor database
    existence is checked. Other fields and supplied containers are retained without copying or
    validation.

    A missing entity_id alone is not a create instruction: ambiguous and conflict require
    resolution. Repository match_or_create methods use no_match as the creation branch and raise
    matching errors for unresolved outcomes. This passive record does not establish identity safety
    or create anything.

    Example:
        >>> result = MatchResult(None, 1, "duplicate exact names", decision="ambiguous", alternatives=(2, 3))
        >>> result.is_match, result.requires_resolution
        (False, True)
        >>> MatchResult(7, 0.9, "selected").decision
        'match'


    :ivar entity_id: Selected database ID for match, otherwise None; no ID-type check is performed.
    :ivar confidence: Integer or float in [0, 1], excluding bool, stored as a float without recomputing evidence.
    :ivar reason: Explanation of the decision, retained without content validation.
    :ivar matched_on: Names of matching fields, conventionally a tuple; not coerced or checked.
    :ivar candidate: Optional candidate or matched-row mapping provided by the matcher, retained by reference.
    :ivar decision: Explicit outcome, or None to derive match/no_match from entity_id.
    :ivar evidence: Supporting observations supplied by the matcher; contents are not validated here.
    :ivar alternatives: Candidate IDs requiring inspection; entries are checked but the container is retained.
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
        """
        Normalize confidence, derive the default decision, and check result shape.

        Called during construction. A selected ID requires match, and match requires a non-None ID.
        Alternatives are iterated to reject non-integer or boolean entries without checking
        positivity, uniqueness, or database existence. The iterable is not copied; a one-shot
        iterable is consumed by validation. Confidence and a default decision are assigned before
        subsequent checks, so manually invoking this method on a tampered object is not an atomic
        repair.

        Example:
            >>> result = MatchResult(None, 0, "no candidate")
            >>> result.confidence, result.decision
            (0.0, 'no_match')
            >>> MatchResult(None, 1, "missing ID", decision="match")
            Traceback (most recent call last):
            ...
            ValueError: a match decision requires an entity_id


        :return: None after assigning normalized fields and validating decision/ID consistency.
        :raises TypeError: If confidence has an unsupported type, alternatives contain non-integer IDs, or supplied values cannot be iterated/hashed.
        :raises ValueError: If confidence is out of range or decision and selected ID are inconsistent.
        :raises OverflowError: If confidence cannot be converted from an integer to float.
        """

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
        """
        Test whether the recorded decision selects a non-None identity.

        This reads decision and entity_id only. It neither applies a confidence threshold nor
        verifies the evidence or existence of the selected database row.

        Example:
            >>> MatchResult(7, 0, "explicit selection").is_match
            True
            >>> MatchResult(None, 0, "absent").is_match
            False


        :return: True exactly when decision is match and entity_id is not None.
        """

        return self.decision == "match" and self.entity_id is not None

    @property
    def requires_resolution(self) -> bool:
        """
        Test whether the recorded decision is ambiguous or conflicting.

        This is a classification of the stored outcome, independent of confidence and whether
        alternatives or evidence have been supplied.

        Example:
            >>> MatchResult(None, 0, "conflicting source", decision="conflict").requires_resolution
            True
            >>> MatchResult(None, 0, "absent").requires_resolution
            False


        :return: True for ambiguous or conflict; False for match and no_match.
        """

        return self.decision in {"ambiguous", "conflict"}


# Todo: WEMIBundle is better English?
@dataclass(frozen=True, slots=True)
class WemiBundle:
    """
    Carry a Work/Expression/Manifestation/Item slice and attached metadata rows.

    Bundle retrieval follows one path, taking the first available repository result when several
    relatives exist. It can leave absent levels as None; use graph retrieval for multiple
    descendants. Relationship metadata may appear under _catalog_link on rows. This record itself
    performs no retrieval, path validation, or copying, and may be constructed empty. Its frozen
    fields leave supplied row mappings and collection contents mutable.

    Example:
        >>> row = {"work_id": 7, "work_title": "Frankenstein"}
        >>> bundle = WemiBundle(work=row)
        >>> bundle.work is row, bundle.item is None
        (True, True)


    :ivar work: Selected Work row, or None when unavailable.
    :ivar expression: Selected Expression row, or None when unavailable.
    :ivar manifestation: Selected Manifestation row, or None when unavailable.
    :ivar item: Selected Item row, or None when unavailable.
    :ivar agents: Agent rows collected from the selected levels.
    :ivar identifiers: Identifier rows collected from the selected levels.
    :ivar titles: Title rows collected from the selected levels.
    :ivar notes: Note rows collected from the selected levels.
    :ivar links: Relationship metadata supplied by the retriever, not an exhaustive descendant edge list.
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
    """
    Return identifiers from a coordinated Work-to-Items creation operation.

    The value carries IDs without exposing repository instances. Creation and transaction guarantees
    belong to the producing mutation operation: this frozen container neither creates rows nor
    validates IDs, hierarchy links, or the type of the supplied item_ids collection.

    Example:
        >>> created = CreatedWemiStack(1, 2, 3, item_ids=(4, 5))
        >>> created.work_id, created.item_ids
        (1, (4, 5))


    :ivar work_id: ID of the created Work.
    :ivar expression_id: ID of its created Expression.
    :ivar manifestation_id: ID of the created Manifestation.
    :ivar item_ids: IDs of created Items, empty when no Items were requested.
    """

    work_id: EntityId
    expression_id: EntityId
    manifestation_id: EntityId
    item_ids: tuple[EntityId, ...] = ()


@dataclass(frozen=True, slots=True)
class WemiAdjacency:
    """
    Describe one direction of immediate WEMI relationship traversal.

    Retrieval supplies the neighboring level and its rows, with _catalog_link metadata where
    available. This frozen record preserves supplied values without querying, validating level
    adjacency, or copying mutable rows. It does not represent a recursive traversal.

    Example:
        >>> adjacent = WemiAdjacency("work", 7, "children", "expression")
        >>> adjacent.related_level, adjacent.entities
        ('expression', ())


    :ivar level: WEMI level of the traversal root.
    :ivar entity_id: ID of the root entity.
    :ivar direction: Requested traversal direction, parents or children.
    :ivar related_level: WEMI level represented by the neighboring rows.
    :ivar entities: Adjacent rows in retriever order, retained without validation or copying.
    """

    level: WemiLevel
    entity_id: EntityId
    direction: WemiDirection
    related_level: WemiLevel
    entities: tuple[RowMapping, ...] = ()


@dataclass(frozen=True, slots=True)
class WemiGraph:
    """
    Carry a Work's selected descendants and the edges connecting them.

    Graph retrieval applies per-level limits and reports possible incompleteness in
    truncated_levels. Truncating an upstream level also marks downstream levels, even if their own
    limits were not reached. Row mappings and edge metadata preserve the selected relationships. The
    container itself applies no bounds, consistency checks, copying, or database reads; frozen
    fields do not make the nested mappings immutable.

    Example:
        >>> graph = WemiGraph({"work_id": 7}, truncated_levels=("expression", "manifestation", "item"))
        >>> graph.work["work_id"], graph.expressions
        (7, ())


    :ivar work: Root Work row returned by the retriever or supplied by a caller.
    :ivar expressions: Selected Expression rows, conventionally in repository traversal order.
    :ivar manifestations: Selected Manifestation rows, deduplicated by the concrete graph retriever.
    :ivar items: Selected Item rows, deduplicated by the concrete graph retriever.
    :ivar links: Edge mappings with parent/child levels and IDs plus relationship metadata.
    :ivar truncated_levels: Levels potentially incomplete because of their own or upstream limits.
    """

    work: RowMapping
    expressions: tuple[RowMapping, ...] = ()
    manifestations: tuple[RowMapping, ...] = ()
    items: tuple[RowMapping, ...] = ()
    links: tuple[RowMapping, ...] = ()
    truncated_levels: tuple[WemiLevel, ...] = ()
