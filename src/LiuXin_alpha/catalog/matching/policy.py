"""Shared identity policy primitives for catalog matchers."""

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
    """Configure the common catalog identity decision boundaries.

    :param acceptance_threshold: Minimum explained confidence for a match.
    :param approximate_text_threshold: Minimum similarity for approximate text.
    :param identifier_conflict_threshold: Similarity below which a decisive
        identifier contradicts supplied descriptive text.
    :param ambiguity_margin: Maximum confidence gap between unresolved peers.
    """

    acceptance_threshold: float = 0.85
    approximate_text_threshold: float = 0.88
    identifier_conflict_threshold: float = 0.45
    ambiguity_margin: float = 0.03

    def __post_init__(self) -> None:
        """Validate configured matching boundaries."""

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
    """Return punctuation-tolerant Unicode text for identity comparison.

    :param value: Human-readable value to normalize.
    :return: Case-folded words separated by single spaces.
    """

    text = unicodedata.normalize("NFKC", str(value)).casefold()
    text = text.replace("&", " and ")
    return " ".join(re.sub(r"[^\w]+", " ", text).split())


def text_similarity(left: object, right: object) -> float:
    """Return normalized text similarity between zero and one.

    :param left: First human-readable value.
    :param right: Second human-readable value.
    :return: Exact or sequence similarity score.
    """

    normalised_left = normalise_match_text(left)
    normalised_right = normalise_match_text(right)
    if not normalised_left or not normalised_right:
        return 0.0
    if normalised_left == normalised_right:
        return 1.0
    return SequenceMatcher(None, normalised_left, normalised_right).ratio()


def _normalise_scheme(value: str) -> str:
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
    """Apply scheme-specific canonicalization to an identifier candidate.

    :param candidate: Raw identifier candidate.
    :return: Candidate carrying a canonical scheme and comparison value.
    :raises TypeError: If ``candidate`` is not an IdentifierCandidate.
    :raises ValueError: If the scheme or value is empty or structurally invalid.
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
    """Return structured identifier hints declared on a metadata candidate.

    Supported forms are IdentifierCandidate objects, ``scheme``/``value``
    mappings, two-item scheme/value sequences, and scheme-to-value mappings.

    :param candidate: Metadata candidate containing optional hints.
    :return: Normalized identifier candidates in declaration order.
    :raises TypeError: If an identifier hint has an unsupported shape.
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
    """Return non-empty normalized Agent names declared as Work hints.

    :param candidate: Work metadata candidate containing optional Agent hints.
    :return: Stable unique normalized Agent names.
    :raises TypeError: If an Agent hint is not a string or name mapping.
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
    """Return a weighted confidence from present evidence only.

    :param evidence: Evidence produced for one possible entity.
    :return: Weighted confidence between zero and one.
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
    """Select one separated candidate or return an explicit non-selection.

    :param candidates: Policy-qualified possible matches.
    :param subject: Human-readable entity label for explanations.
    :param policy: Matching decision boundaries.
    :return: Match, no-match, or ambiguous result.
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
    """Raise the catalog error represented by an unresolved match result.

    :param result: Match result returned by a catalog matcher.
    :return: None.
    :raises CatalogAmbiguousMatchError: If several entities remain plausible.
    :raises CatalogMatchConflictError: If decisive evidence contradicts itself.
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
    """Match a WEMI child inside its already-resolved parent scope.

    :param repository: Repository providing input aliases and its ID column.
    :param rows: Existing children in the parent scope.
    :param candidate: Incoming child metadata.
    :param identity_fields: Columns capable of establishing child identity.
    :param corroborating_fields: Columns which can only support identity.
    :param subject: Human-readable scoped entity label.
    :param policy: Matching decision boundaries.
    :return: Explained identity decision.
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
    """Resolve candidate identifier hints to their owned catalog rows.

    :param repositories: Catalog repository group.
    :param candidate: Metadata candidate containing identifier hints.
    :param level: Required identifier owner type.
    :return: Each hint paired with matching rows owned at ``level``.
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
