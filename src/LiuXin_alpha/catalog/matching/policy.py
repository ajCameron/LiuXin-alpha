"""
Normalize matching evidence and apply shared Catalog identity-decision rules.

Text comparison, scheme-specific identifier normalization, and hint parsing feed
explained confidence and final selection. MatchingPolicy sets numeric boundaries;
decide_best prioritizes decisive evidence before confidence and can report ambiguity.
It does not combine terminal conflicts, so specialized matchers retain that duty.
Contextual matching consumes caller-scoped rows, while identifier_owner_rows reads
identifier rows with owner references. These helpers do not create or update entities.

Example:
    >>> normalise_match_text("Mary  Shelley & Co.")
    'mary shelley and co'
    >>> decide_best((), subject="Work").decision
    'no_match'
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from difflib import SequenceMatcher
import re
from typing import Any
import unicodedata
from uuid import UUID

from ..api.common import (
    CatalogAmbiguousMatchError,
    CatalogMatchConflictError,
    IdentifierCandidate,
    MatchEvidence,
    MatchResult,
    MetadataCandidate,
    RowMapping,
)


@dataclass(frozen=True, slots=True)
class MatchingPolicy:
    """
    Hold the four numeric boundaries used by Catalog identity decisions.

    Construction checks each value independently against [0, 1], rejecting booleans and nonfinite
    numbers. Values are retained as supplied rather than converted to floats; no ordering
    relationship between thresholds is enforced. The record is frozen. Individual matchers determine
    which evidence qualifies before the common acceptance and ambiguity rules select a result.

    Example:
        >>> policy = MatchingPolicy(acceptance_threshold=1, ambiguity_margin=0)
        >>> policy.acceptance_threshold, policy.ambiguity_margin
        (1, 0)


    :ivar acceptance_threshold: Inclusive minimum confidence for the top-ranked candidate; defaults to 0.85.
    :ivar approximate_text_threshold: Inclusive similarity cutoff used by approximate text comparisons; defaults to 0.88.
    :ivar identifier_conflict_threshold: Descriptive similarity below which specialized matchers can reject identifier evidence; defaults to 0.45.
    :ivar ambiguity_margin: Inclusive confidence gap for peers with the same decisive-evidence status; defaults to 0.03.
    """

    acceptance_threshold: float = 0.85
    approximate_text_threshold: float = 0.88
    identifier_conflict_threshold: float = 0.45
    ambiguity_margin: float = 0.03

    def __post_init__(self) -> None:
        """
        Validate each configured threshold without changing or relating the values.

        The dataclass constructor invokes this check in field order. Every field must be int or
        float, excluding bool, and compare within the inclusive unit interval. NaN and infinities
        fail the range check. Integer values remain integers, and acceptance may exceed the
        approximate-text threshold.

        Example:
            >>> MatchingPolicy(ambiguity_margin=True)
            Traceback (most recent call last):
            ...
            TypeError: ambiguity_margin must be numeric
            >>> MatchingPolicy(acceptance_threshold=1).acceptance_threshold
            1


        :return: None when all four values satisfy their independent checks.
        :raises TypeError: If a field is not int/float or is a boolean.
        :raises ValueError: If a field is outside [0, 1], including a nonfinite float.
        """

        for name in (
            "acceptance_threshold",
            "approximate_text_threshold",
            "identifier_conflict_threshold",
            "ambiguity_margin",
        ):
            value = getattr(self, name)
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise TypeError(f"{name} must be numeric")
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be between zero and one")


DEFAULT_MATCHING_POLICY = MatchingPolicy()

_DOI_PREFIX = re.compile(
    r"^(?:doi\s*:\s*|https?://(?:dx\.)?doi\.org/)",
    flags=re.IGNORECASE,
)
_ISBN_SCHEMES = frozenset({"isbn", "isbn10", "isbn13"})


def normalise_match_text(value: object) -> str:
    """
    Normalize an object's text for punctuation-tolerant identity comparisons.

    Convert with str, apply Unicode NFKC and case folding, expand ampersands to and, then replace
    runs of non-word characters with spaces and collapse whitespace. Unicode word characters and
    underscores remain. This does not remove accents, identify synonyms, or treat None as missing
    data.

    Example:
        >>> normalise_match_text("  Ａ&B_1 — Café!  ")
        'a and b_1 café'
        >>> normalise_match_text(None)
        'none'


    :param value: Object whose string representation should be compared as human-readable text.
    :return: Case-folded normalized words separated by single spaces, possibly an empty string.
    """

    text = unicodedata.normalize("NFKC", str(value)).casefold()
    text = text.replace("&", " and ")
    return " ".join(re.sub(r"[^\w]+", " ", text).split())


def text_similarity(left: object, right: object) -> float:
    """
    Compare the character sequences of two normalized text values.

    Both values pass through normalise_match_text. Return zero if either normalized string is empty,
    even when both are empty, and one for a nonempty exact equality. Other pairs use
    SequenceMatcher.ratio with its default heuristics. This is a character similarity score, not
    semantic equivalence or a guaranteed symmetric distance metric.

    Example:
        >>> text_similarity("A & B", "a and b")
        1.0
        >>> text_similarity("", "!!!")
        0.0
        >>> text_similarity("abc", "ab")
        0.8


    :param left: First object, converted and normalized before comparison.
    :param right: Second object, converted and normalized before comparison.
    :return: A similarity float in [0, 1], with zero for any empty normalized operand.
    """

    normalised_left = normalise_match_text(left)
    normalised_right = normalise_match_text(right)
    if not normalised_left or not normalised_right:
        return 0.0
    if normalised_left == normalised_right:
        return 1.0
    return SequenceMatcher(None, normalised_left, normalised_right).ratio()


def _normalise_scheme(value: str) -> str:
    """
    Canonicalize an identifier scheme through ASCII separators and explicit aliases.

    Case-fold the string, replace non-ASCII-letter/digit runs with underscores, and strip outer
    underscores. Known aliases are recognized after removing those separators; unknown schemes
    retain the normalized underscore spelling. Empty results are returned for the caller to reject.

    Example:
        >>> _normalise_scheme(" ISBN-13 ")
        'isbn13'
        >>> _normalise_scheme("Local Call")
        'local-call'
        >>> _normalise_scheme("Publisher Code")
        'publisher_code'


    :param value: Scheme string to normalize; this helper does not coerce other types.
    :return: Canonical known alias or normalized unknown scheme, possibly empty.
    """
    scheme = re.sub(r"[^a-z0-9]+", "_", value.casefold()).strip("_")
    compact = scheme.replace("_", "")
    aliases = {
        "archiveid": "archive-id",
        "assetid": "asset-id",
        "imdbid": "imdb_id",
        "isbn": "isbn",
        "isbn10": "isbn10",
        "isbn13": "isbn13",
        "calibreuuid": "calibre_uuid",
        "localcall": "local-call",
        "publisherphash": "publisher_phash",
        "uuidish": "uuid-ish",
        "wikipediaurl": "wikipedia_url",
    }
    return aliases.get(compact, scheme)


def _check_isbn(value: str) -> str | None:
    """
    Compact an ISBN-shaped string and check its ten- or thirteen-digit checksum.

    Remove everything except ASCII digits and X/x. Ten-character values permit X only as the final
    check digit and use the mod-11 rule; thirteen-character values use alternating weights one and
    three. This checks shape/checksum only, without validating ISBN prefixes, registration, or
    publication identity.

    Example:
        >>> _check_isbn("0-306-40615-2")
        '0306406152'
        >>> _check_isbn("978-0-306-40615-8") is None
        True
        >>> _check_isbn("0000000000000")
        '0000000000000'


    :param value: Text from which digit and possible ISBN-10 check-letter content is extracted.
    :return: Compact checksum-valid text with uppercase X, or None for unsupported shape/checksum.
    """
    compact = re.sub(r"[^0-9Xx]", "", value)
    if len(compact) == 10 and compact[:9].isdigit() and (
        compact[-1].isdigit() or compact[-1] in "Xx"
    ):
        total = sum(
            (10 - index) * (10 if digit in "Xx" else int(digit))
            for index, digit in enumerate(compact)
        )
        return compact.upper() if total % 11 == 0 else None
    if len(compact) == 13 and compact.isdigit():
        total = sum(
            int(digit) * (1 if index % 2 == 0 else 3)
            for index, digit in enumerate(compact)
        )
        return compact if total % 10 == 0 else None
    return None


def normalise_identifier(candidate: IdentifierCandidate) -> IdentifierCandidate:
    """
    Create a candidate with a canonical scheme and scheme-specific comparison value.

    Require an IdentifierCandidate with string scheme and original value. A truthy normalised_value
    takes precedence over the original value, including when it strips to empty; an empty override
    falls back to the original. The override itself is not type-checked before string operations.

    ISBN values receive checksum and explicit-length checks, uuid/calibre_uuid use UUID parsing, and DOI
    values lose one recognized prefix, case-fold, and need only a 10. prefix plus a slash. OCLC
    values lose one leading oclc/ocm/ocn/on token without requiring a delimiter or numeric suffix;
    that step can produce an empty result. Other schemes retain stripped case-sensitive text.

    Return a new candidate with stripped original value and the same source/hints. No network
    lookup, registration check, database write, or mutation of the input candidate is performed.

    Example:
        >>> original = IdentifierCandidate("DOI", " https://doi.org/10.1000/ABC ")
        >>> normalized = normalise_identifier(original)
        >>> normalized.identifier_type, normalized.normalised_value
        ('doi', '10.1000/abc')
        >>> original.normalised_value is None, normalized.hints is original.hints
        (True, True)
        >>> normalise_identifier(IdentifierCandidate("oclc", "oclc")).normalised_value
        ''


    :param candidate: Identifier value to canonicalize, preserving its provenance and hint references.
    :return: A new IdentifierCandidate carrying the canonical scheme and comparison text.
    :raises TypeError: If candidate or its original scheme/value has the wrong type, or an override cannot support the selected operations.
    :raises ValueError: If initial scheme/value text is empty or ISBN, UUID, or DOI checks fail.
    :raises AttributeError: If a truthy normalized override lacks required string operations.
    """

    if not isinstance(candidate, IdentifierCandidate):
        raise TypeError("candidate must be an IdentifierCandidate")
    if not isinstance(candidate.identifier_type, str) or not isinstance(
        candidate.value,
        str,
    ):
        raise TypeError("identifier scheme and value must be strings")
    scheme = _normalise_scheme(candidate.identifier_type)
    raw_value = candidate.normalised_value or candidate.value
    value = raw_value.strip()
    if not scheme or not value:
        raise ValueError("identifier scheme and value must be non-empty")

    if scheme in _ISBN_SCHEMES:
        checked = _check_isbn(value)
        if checked is None:
            raise ValueError(f"invalid ISBN: {candidate.value!r}")
        expected_length = (
            10 if scheme == "isbn10" else 13 if scheme == "isbn13" else None
        )
        if expected_length is not None and len(checked) != expected_length:
            raise ValueError(
                f"{candidate.identifier_type!r} does not contain an ISBN-{expected_length}"
            )
        scheme = f"isbn{len(checked)}"
        normalised_value = checked
    elif scheme in {"uuid", "calibre_uuid"}:
        try:
            normalised_value = str(UUID(value))
        except ValueError as error:
            raise ValueError(f"invalid UUID: {candidate.value!r}") from error
    elif scheme == "doi":
        normalised_value = _DOI_PREFIX.sub("", value).strip().casefold()
        if not normalised_value.startswith("10.") or "/" not in normalised_value:
            raise ValueError(f"invalid DOI: {candidate.value!r}")
    elif scheme == "oclc":
        normalised_value = re.sub(
            r"^(?:oclc|ocm|ocn|on)",
            "",
            value,
            flags=re.IGNORECASE,
        )
        normalised_value = normalised_value.strip().casefold()
    else:
        normalised_value = value

    return IdentifierCandidate(
        identifier_type=scheme,
        value=candidate.value.strip(),
        normalised_value=normalised_value,
        source=candidate.source,
        hints=candidate.hints,
    )


def identifier_candidates(candidate: MetadataCandidate) -> tuple[IdentifierCandidate, ...]:
    """
    Parse and normalize the identifiers entry in a metadata candidate's hints.

    Accept a scheme-to-value mapping, one mapping with scheme/identifier_type and value, or a
    sequence of IdentifierCandidate objects, such mappings, or two-string scheme/value sequences. A
    single two-string pair must itself be wrapped in an outer sequence. Strings, bytes, and general
    iterators are rejected as the outer container; absent hints produce an empty tuple.

    Mapping and pair forms use candidate.source and fresh empty nested hints; supplied
    IdentifierCandidate objects retain their own provenance and hints. A scheme key takes precedence
    over identifier_type even when its value is invalid. Normalize every entry in order without
    deduplication, and propagate malformed entries instead of returning a partial result.

    Example:
        >>> candidate = MetadataCandidate({}, source="opf", hints={"identifiers": {"DOI": "doi:10.1000/ABC"}})
        >>> hints = identifier_candidates(candidate)
        >>> hints[0].identifier_type, hints[0].normalised_value, hints[0].source
        ('doi', '10.1000/abc', 'opf')


    :param candidate: Metadata candidate whose hints mapping may contain structured identifiers.
    :return: Normalized IdentifierCandidate objects in declaration order, preserving duplicate entries.
    :raises TypeError: If hint containers, entry shapes, or mapping field types are unsupported.
    :raises ValueError: If an entry fails scheme-specific normalization.
    :raises AttributeError: If the supplied candidate/hints or an unchecked override lacks required attributes.
    """

    raw_hints = candidate.hints.get("identifiers", ())
    if isinstance(raw_hints, (str, bytes)):
        raise TypeError("identifier hints must be structured values")
    if isinstance(raw_hints, Mapping):
        if "scheme" in raw_hints or "identifier_type" in raw_hints:
            hints: Sequence[object] = (raw_hints,)
        else:
            hints = tuple(raw_hints.items())
    elif isinstance(raw_hints, Sequence):
        hints = raw_hints
    else:
        raise TypeError("identifier hints must be a mapping or sequence")

    result: list[IdentifierCandidate] = []
    for raw_hint in hints:
        if isinstance(raw_hint, IdentifierCandidate):
            parsed = raw_hint
        elif isinstance(raw_hint, Mapping):
            scheme = raw_hint.get("scheme", raw_hint.get("identifier_type"))
            value = raw_hint.get("value")
            if not isinstance(scheme, str) or not isinstance(value, str):
                raise TypeError("identifier hint mappings require string scheme and value")
            normalised_value = raw_hint.get("normalised_value")
            if normalised_value is not None and not isinstance(normalised_value, str):
                raise TypeError("normalised identifier values must be strings")
            parsed = IdentifierCandidate(
                scheme,
                value,
                normalised_value=normalised_value,
                source=candidate.source,
            )
        elif (
            isinstance(raw_hint, Sequence)
            and not isinstance(raw_hint, (str, bytes))
            and len(raw_hint) == 2
            and isinstance(raw_hint[0], str)
            and isinstance(raw_hint[1], str)
        ):
            parsed = IdentifierCandidate(raw_hint[0], raw_hint[1], source=candidate.source)
        else:
            raise TypeError("unsupported identifier hint")
        result.append(normalise_identifier(parsed))
    return tuple(result)


def agent_names(candidate: MetadataCandidate) -> tuple[str, ...]:
    """
    Extract unique normalized Agent names from metadata hints in encounter order.

    The agents entry accepts a string, one mapping, or a sequence of strings and mappings. A mapping
    uses name when present, otherwise canonical_name; an invalid present name does not fall back.
    Normalize with normalise_match_text, discard empty results, and keep the first occurrence of
    each normalized name. This helper resolves no Agent IDs and does not query credited
    relationships.

    Example:
        >>> candidate = MetadataCandidate({}, hints={"agents": ["Mary Shelley", {"canonical_name": "MARY  SHELLEY"}, "!!!"]})
        >>> agent_names(candidate)
        ('mary shelley',)


    :param candidate: Metadata candidate whose hints may contain the agents entry; absence means no names.
    :return: Distinct nonempty normalized names as a tuple in first-seen order.
    :raises TypeError: If the outer container or an individual name hint has an unsupported shape.
    :raises AttributeError: If candidate or its hints lacks the expected attribute/mapping interface.
    """

    raw_agents = candidate.hints.get("agents", ())
    if isinstance(raw_agents, (str, Mapping)):
        values: Sequence[object] = (raw_agents,)
    elif isinstance(raw_agents, Sequence):
        values = raw_agents
    else:
        raise TypeError("agent hints must be a string, mapping, or sequence")
    result: list[str] = []
    for raw_agent in values:
        if isinstance(raw_agent, str):
            name = raw_agent
        elif isinstance(raw_agent, Mapping):
            name = raw_agent.get("name", raw_agent.get("canonical_name"))
            if not isinstance(name, str):
                raise TypeError("agent hint mappings require a string name")
        else:
            raise TypeError("agent hints must be strings or name mappings")
        normalised = normalise_match_text(name)
        if normalised and normalised not in result:
            result.append(normalised)
    return tuple(result)


def explained_confidence(evidence: Sequence[MatchEvidence]) -> float:
    """
    Calculate a weighted mean using only evidence with a strictly positive weight.

    Omit zero/negative weights and weights whose comparison with zero is false, including NaN. No
    qualifying evidence returns zero. Otherwise divide the weighted score sum by the weight sum
    without clamping or finite checks. Valid scores with finite, non-overflowing positive weights
    give a unit-interval result; infinite weights or overflowing sums can instead produce NaN or
    other floating-point artifacts. Evidence categories and decisive flags are ignored.

    Example:
        >>> evidence = (MatchEvidence("title", "exact", 1, 3, "same"), MatchEvidence("year", "conflict", 0, 1, "different"))
        >>> explained_confidence(evidence)
        0.75
        >>> explained_confidence(())
        0.0


    :param evidence: Observations whose score and weight attributes participate in the weighted mean.
    :return: Zero for no positive weights, otherwise the arithmetic weighted mean without range or finite-value enforcement.
    """

    weighted = tuple(item for item in evidence if item.weight > 0)
    if not weighted:
        return 0.0
    return sum(item.score * item.weight for item in weighted) / sum(
        item.weight for item in weighted
    )


def decide_best(
    candidates: Sequence[MatchResult],
    *,
    subject: str,
    policy: MatchingPolicy = DEFAULT_MATCHING_POLICY,
) -> MatchResult:
    """
    Rank selected-ID candidates and return one match or an explicit non-selection.

    Ignore entries with entity_id=None, including precomputed conflict outcomes. Rank decisive
    evidence first, then descending confidence, then ascending ID with -1 as the key for a
    false-valued ID. If the leading candidate is below acceptance, return no_match even if a
    lower-ranked nondecisive candidate has higher confidence. Callers must handle terminal conflict
    decisions themselves.

    Peers share the leader's decisive status and fall within the inclusive ambiguity margin. They
    need not meet acceptance individually, and repeated IDs are not deduplicated. Several peers
    produce ambiguity using the leader's confidence/evidence and ranked alternative IDs. Otherwise
    return the original leading object. New no-match/ambiguous results omit its row and matched_on.

    Example:
        >>> first = MatchResult(2, 0.9, "first")
        >>> second = MatchResult(3, 0.89, "second")
        >>> decide_best((first, second), subject="Tag").alternatives
        (2, 3)
        >>> decide_best((first,), subject="Tag") is first
        True


    :param candidates: Already-formed possible matches; entries without selected IDs are excluded.
    :param subject: Human-readable entity label used in newly constructed explanations.
    :param policy: Acceptance and ambiguity boundaries; existing evidence scores are not recomputed.
    :return: The original winning candidate or a new no_match/ambiguous MatchResult; this helper does not create conflict decisions.
    """

    ranked = sorted(
        (candidate for candidate in candidates if candidate.entity_id is not None),
        key=lambda candidate: (
            -int(any(item.decisive for item in candidate.evidence)),
            -candidate.confidence,
            candidate.entity_id or -1,
        ),
    )
    if not ranked:
        return MatchResult(None, 0.0, f"no {subject} candidates", decision="no_match")
    top = ranked[0]
    if top.confidence < policy.acceptance_threshold:
        return MatchResult(
            None,
            top.confidence,
            f"no {subject} candidate met the matching policy",
            decision="no_match",
            evidence=top.evidence,
        )

    top_decisive = any(item.decisive for item in top.evidence)
    peers = tuple(
        candidate
        for candidate in ranked
        if any(item.decisive for item in candidate.evidence) == top_decisive
        and top.confidence - candidate.confidence <= policy.ambiguity_margin
    )
    if len(peers) > 1:
        return MatchResult(
            None,
            top.confidence,
            f"several {subject} candidates remain within the ambiguity margin",
            decision="ambiguous",
            evidence=top.evidence,
            alternatives=tuple(
                candidate.entity_id
                for candidate in peers
                if candidate.entity_id is not None
            ),
        )
    return top


def raise_for_unresolved(result: MatchResult) -> None:
    """
    Translate an ambiguous or conflicting decision into its Catalog exception.

    Inspect decision only. The exception uses result.reason as its message and retains the same
    result object. Match and no_match return normally; confidence, alternatives, and the supplied
    object's type are not independently validated.

    Example:
        >>> raise_for_unresolved(MatchResult(None, 0, "no candidate"))
        >>> result = MatchResult(None, 1, "choose an identity", decision="ambiguous")
        >>> try:
        ...     raise_for_unresolved(result)
        ... except CatalogAmbiguousMatchError as error:
        ...     print(error.result is result)
        True


    :param result: Matching decision to classify and attach to any raised error.
    :return: None for a decision other than ambiguous or conflict.
    :raises CatalogAmbiguousMatchError: If decision is ambiguous.
    :raises CatalogMatchConflictError: If decision is conflict.
    """

    if result.decision == "ambiguous":
        raise CatalogAmbiguousMatchError(result.reason, result)
    if result.decision == "conflict":
        raise CatalogMatchConflictError(result.reason, result)


def contextual_match(
    repository: Any,
    rows: Sequence[RowMapping],
    candidate: MetadataCandidate,
    *,
    identity_fields: Sequence[str],
    corroborating_fields: Sequence[str],
    subject: str,
    policy: MatchingPolicy = DEFAULT_MATCHING_POLICY,
) -> MatchResult:
    """
    Score candidate identity within caller-supplied WEMI child rows.

    Normalize aliases through the repository while ignoring unknown columns; the caller must supply
    rows from the intended parent scope. Compare only pairs with non-None values, using normalized
    text similarity for two strings and equality otherwise. Identity fields weigh five,
    corroborating fields one. A row qualifies with any exact identity field, or an approximate
    identity field plus exact corroboration. Other disagreements still lower confidence.

    Retain qualifying integer-ID rows and use decide_best for acceptance and ambiguity. No
    independent parent check or data write occurs; repository normalization can still consult
    schema. Conflicting evidence affects the score but this helper does not produce a conflict
    decision. Stored rows are retained by reference in possible matches; isinstance(int) also admits
    bool IDs.

    Example:
        >>> from types import SimpleNamespace
        >>> repository = SimpleNamespace(id_column="id", normalise_input=lambda data, **options: dict(data))
        >>> result = contextual_match(
        ...     repository, ({"id": 7, "label": "English text"},),
        ...     MetadataCandidate({"label": "English text"}),
        ...     identity_fields=("label",), corroborating_fields=(), subject="Expression",
        ... )
        >>> result.entity_id
        7


    :param repository: Owner providing normalise_input and id_column; no parent lookup is requested here.
    :param rows: Existing child mappings already restricted to the desired parent by the caller.
    :param candidate: MetadataCandidate with aliases or storage columns; repository validation may reject ID inputs.
    :param identity_fields: Ordered columns which can establish identity, each carrying weight five.
    :param corroborating_fields: Ordered supporting columns, each carrying weight one.
    :param subject: Entity/scope description used in result explanations.
    :param policy: Approximate similarity, acceptance, and ambiguity boundaries.
    :return: A match, no_match, or ambiguous result over the supplied rows.
    :raises TypeError: If candidate is not a MetadataCandidate.
    :raises Exception: Repository normalization, malformed-row, and evidence-validation failures propagate.
    """

    if not isinstance(candidate, MetadataCandidate):
        raise TypeError("candidate must be a MetadataCandidate")
    data = repository.normalise_input(candidate.data, ignore_unknown=True)
    considered: list[MatchResult] = []
    for row in rows:
        evidence: list[MatchEvidence] = []
        identity_present = False
        identity_exact = False
        identity_approximate = False
        corroborated = False
        for field in identity_fields:
            expected = data.get(field)
            actual = row.get(field)
            if expected is None or actual is None:
                continue
            identity_present = True
            score = (
                text_similarity(expected, actual)
                if isinstance(expected, str) and isinstance(actual, str)
                else float(expected == actual)
            )
            if score == 1.0:
                identity_exact = True
            else:
                identity_approximate = identity_approximate or (
                    score >= policy.approximate_text_threshold
                )
            evidence.append(
                MatchEvidence(
                    field,
                    "exact" if score == 1.0 else "approximate",
                    score,
                    5.0,
                    f"compared scoped identity field {field}",
                    expected,
                    actual,
                )
            )
        for field in corroborating_fields:
            expected = data.get(field)
            actual = row.get(field)
            if expected is None or actual is None:
                continue
            score = (
                text_similarity(expected, actual)
                if isinstance(expected, str) and isinstance(actual, str)
                else float(expected == actual)
            )
            corroborated = corroborated or score == 1.0
            evidence.append(
                MatchEvidence(
                    field,
                    "corroborating" if score == 1.0 else "conflict",
                    score,
                    1.0,
                    f"compared scoped corroborating field {field}",
                    expected,
                    actual,
                )
            )
        confidence = explained_confidence(evidence)
        qualifies = identity_present and (
            identity_exact or (identity_approximate and corroborated)
        )
        row_id = row.get(repository.id_column)
        if qualifies and isinstance(row_id, int):
            considered.append(
                MatchResult(
                    row_id,
                    confidence,
                    f"{subject}: identity evidence met policy",
                    matched_on=tuple(
                        item.field for item in evidence if item.score == 1.0
                    ),
                    candidate=row,
                    evidence=tuple(evidence),
                )
            )
    return decide_best(considered, subject=subject, policy=policy)


def identifier_owner_rows(
    repositories: Any,
    candidate: MetadataCandidate,
    *,
    level: str,
) -> tuple[tuple[IdentifierCandidate, tuple[RowMapping, ...]], ...]:
    """
    Pair normalized identifier hints with matching identifier rows at one owner level.

    Read repositories.identifiers._all_rows once before parsing hints, including when no identifiers
    are supplied. For each hint, compare only rows whose entity_identifier_entity_type equals level.
    Normalize stored scheme/value text and skip stored entries raising TypeError or ValueError;
    invalid incoming hints propagate. Return identifier rows carrying owner references, without
    fetching or validating the referenced Work/Agent/other entity itself.

    Hint and row order, duplicate matches, and the original row objects are retained. Unknown owner
    levels simply have no matches. The repository must supply rows that can be iterated again for
    each hint.

    Example:
        >>> from types import SimpleNamespace
        >>> row = {"entity_identifier_entity_type": "work", "entity_identifier_scheme": "doi", "entity_identifier_value": "10.1000/abc"}
        >>> repositories = SimpleNamespace(identifiers=SimpleNamespace(_all_rows=lambda: (row,)))
        >>> candidate = MetadataCandidate({}, hints={"identifiers": {"DOI": "doi:10.1000/ABC"}})
        >>> identifier_owner_rows(repositories, candidate, level="work")[0][1][0] is row
        True


    :param repositories: Bound group exposing an identifier repository and its reiterable row snapshot.
    :param candidate: Metadata candidate containing supported identifier-hint shapes.
    :param level: Exact owner-type discriminator to match, such as work or agent.
    :return: A tuple of (normalized hint, matching identifier-row tuple) pairs, including hints with no rows.
    :raises Exception: Row retrieval and incoming-hint errors propagate; only stored normalization TypeError/ValueError are skipped.
    """

    resolved: list[tuple[IdentifierCandidate, tuple[RowMapping, ...]]] = []
    rows = repositories.identifiers._all_rows()
    for hint in identifier_candidates(candidate):
        owners: list[RowMapping] = []
        for row in rows:
            if row.get("entity_identifier_entity_type") != level:
                continue
            try:
                stored = normalise_identifier(
                    IdentifierCandidate(
                        str(row.get("entity_identifier_scheme") or ""),
                        str(row.get("entity_identifier_value") or ""),
                    )
                )
            except (TypeError, ValueError):
                continue
            if (
                stored.identifier_type == hint.identifier_type
                and stored.normalised_value == hint.normalised_value
            ):
                owners.append(row)
        resolved.append((hint, tuple(owners)))
    return tuple(resolved)


__all__ = [
    "DEFAULT_MATCHING_POLICY",
    "MatchingPolicy",
    "agent_names",
    "contextual_match",
    "decide_best",
    "explained_confidence",
    "identifier_candidates",
    "identifier_owner_rows",
    "normalise_identifier",
    "normalise_match_text",
    "raise_for_unresolved",
    "text_similarity",
]
