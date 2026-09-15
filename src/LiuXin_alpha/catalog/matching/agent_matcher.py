"""
Match Agent names and aliases, with a separate identifier-owner decision path.

Without identifier ownership, the matcher requires exact normalized name equality
and rejects incompatible supplied Agent types. Approximate name-only matching is
not enabled by lowering a policy threshold. Known identifier owners instead undergo
type/name contradiction checks and return a terminal decision without the final
acceptance gate. Repository mutation policy remains outside this read-only matcher.

Example:
    >>> AgentMatcher._aliases("Mary Shelley(#BREAK#)M. Shelley")
    ('Mary Shelley', 'M. Shelley')
"""

from __future__ import annotations

from collections.abc import Sequence
import json
import re
from typing import Any, cast

from ..api.common import (
    DatabaseHandle,
    MatchEvidence,
    MatchResult,
    MetadataCandidate,
    RowMapping,
)
from .policy import (
    DEFAULT_MATCHING_POLICY,
    MatchingPolicy,
    decide_best,
    explained_confidence,
    identifier_owner_rows,
    normalise_match_text,
    text_similarity,
)


class AgentMatcher:
    """
    Resolve Agents from exact normalized names/aliases or known identifier ownership.

    Descriptive-only matching requires an exact normalized canonical name, sort name, or stored
    alias; a conflicting supplied Agent type rejects the row. There is no approximate name-only
    opt-in. Identifier-backed matching instead checks the unique owner's type and nearest name using
    the configured conflict cutoff. That terminal path can return a match below ordinary confidence
    acceptance. The matcher reads through repositories without creating entities.

    Example:
        >>> matcher = catalog.matching.agents  # doctest: +SKIP
        >>> decision = matcher.exact("Mary Shelley")  # doctest: +SKIP


    :ivar db: Borrowed database context retained without lifecycle management.
    :ivar repositories: Group supplying Agent rows and curated identifier owner references.
    :ivar policy: Identifier-name contradiction and evidence-label thresholds plus final descriptive selection boundaries.
    """

    def __init__(
        self,
        db: DatabaseHandle,
        repositories: Any,
        policy: MatchingPolicy = DEFAULT_MATCHING_POLICY,
    ) -> None:
        """
        Retain the database context, repository group, and Agent matching policy.

        Assign all three objects by reference without type checks, queries, copying, or policy
        rebinding. Actual reads use the supplied repositories; db is retained as context and is not
        opened or closed by this matcher.

        Example:
            >>> matcher = AgentMatcher(None, None)
            >>> matcher.policy is DEFAULT_MATCHING_POLICY
            True


        :param db: Borrowed database context retained on the matcher; repository objects perform the reads.
        :param repositories: Group exposing the Agent and identifier repositories plus any supporting owners.
        :param policy: Matching boundaries retained by reference; defaults to the shared frozen policy.
        :return: None after assigning the three dependencies.
        """

        self.db = db
        self.repositories = repositories
        self.policy = policy

    @staticmethod
    def _candidate_name(data: RowMapping) -> object | None:
        """
        Choose canonical name before sort name, accepting any non-None value.

        An empty or false-valued canonical name still wins over a supplied sort name. This helper
        performs no text normalization, alias expansion, or type validation.

        Example:
            >>> AgentMatcher._candidate_name({"agent_canonical_name": "", "agent_sort_name": "Shelley, Mary"})
            ''
            >>> AgentMatcher._candidate_name({}) is None
            True


        :param data: Normalized candidate mapping containing canonical-name and/or sort-name columns.
        :return: First non-None candidate name object, or None when neither column supplies one.
        """
        for field in ("agent_canonical_name", "agent_sort_name"):
            value = data.get(field)
            if value is not None:
                return cast(object, value)
        return None

    @staticmethod
    def _aliases(value: object) -> tuple[str, ...]:
        """
        Read stored aliases from a JSON-array string or the legacy delimiter format.

        Non-string and whitespace-only inputs yield no aliases. Text beginning with an opening
        bracket after leading whitespace is attempted as JSON; a decoded list keeps only string
        entries, without stripping, deduplicating, or removing empty strings. Caught JSON type/value
        errors fall back to delimiter parsing. The fallback splits on (#BREAK#), semicolons,
        vertical bars, and newlines, strips each piece, and omits empty pieces. This parser performs
        no persistence.

        Example:
            >>> AgentMatcher._aliases('[" Mary ", 7, "", "Mary"]')
            (' Mary ', '', 'Mary')
            >>> AgentMatcher._aliases("Mary(#BREAK#) Percy;|Mary")
            ('Mary', 'Percy', 'Mary')


        :param value: Stored alias value; only strings are interpreted.
        :return: Alias strings in stored order, with JSON and delimiter forms retaining their distinct whitespace/empty-value behavior.
        """
        if not isinstance(value, str) or not value.strip():
            return ()
        if value.lstrip().startswith("["):
            try:
                decoded = json.loads(value)
            except (TypeError, ValueError):
                decoded = None
            if isinstance(decoded, list):
                return tuple(item for item in decoded if isinstance(item, str))
        return tuple(
            part.strip()
            for part in re.split(r"\(#BREAK#\)|[;|\n]", value)
            if part.strip()
        )

    def _row_names(self, row: RowMapping) -> tuple[tuple[str, object], ...]:
        """
        Collect truthy stored names followed by parsed aliases in their original order.

        Canonical and sort names are included only when truthy, unlike candidate-name selection's
        non-None check. Parsed alias strings are appended under the agent_aliases field label,
        including blanks retained by the JSON-array path. Neither names nor aliases are normalized
        or deduplicated at this stage.

        Example:
            >>> matcher = AgentMatcher(None, None)
            >>> matcher._row_names({"agent_canonical_name": "", "agent_sort_name": "Shelley", "agent_aliases": '[" Mary ", ""]'})
            (('agent_sort_name', 'Shelley'), ('agent_aliases', ' Mary '), ('agent_aliases', ''))


        :param row: Stored Agent mapping whose canonical name, sort name, and aliases should be collected.
        :return: An ordered tuple of (stored field name, comparison value) pairs.
        """
        names = [
            (field, row[field])
            for field in ("agent_canonical_name", "agent_sort_name")
            if row.get(field)
        ]
        names.extend(
            ("agent_aliases", alias)
            for alias in self._aliases(row.get("agent_aliases"))
        )
        return tuple(names)

    def _evaluate_row(
        self,
        row: RowMapping,
        data: RowMapping,
    ) -> MatchResult | None:
        """
        Build an Agent candidate only for an exact normalized name and compatible type.

        Choose the stored name/alias with greatest normalized similarity, taking the first on ties,
        and reject every score other than one. Name evidence weighs six. When both Agent types are
        non-None, compare normalized text: equality adds weight-two corroboration, while inequality
        rejects the row immediately rather than emitting a conflict result.

        Require an integer agent_id for a returned candidate, accepting bool under Python's integer
        check and imposing no positivity rule. Retained evidence scores are all one, so confidence
        is one. The result references the original row and names the actual selected field plus any
        matching type in matched_on.

        Example:
            >>> matcher = AgentMatcher(None, None)
            >>> row = {"agent_id": 7, "agent_canonical_name": "Mary Shelley", "agent_type": "person"}
            >>> matcher._evaluate_row(row, {"agent_canonical_name": "MARY SHELLEY"}).confidence
            1.0
            >>> matcher._evaluate_row(row, {"agent_canonical_name": "Mary Shelley", "agent_type": "organisation"}) is None
            True


        :param row: Existing Agent row with available name variants and an integer ID for returned matches.
        :param data: Normalized candidate name/type fields; identifiers are handled by a separate path.
        :return: An exact descriptive candidate, or None for missing/inexact names, incompatible type, or an invalid ID type.
        """
        expected_name = self._candidate_name(data)
        names = self._row_names(row)
        if expected_name is None or not names:
            return None
        name_field, actual_name = max(
            names,
            key=lambda item: text_similarity(expected_name, item[1]),
        )
        name_score = text_similarity(expected_name, actual_name)
        if name_score != 1.0:
            return None
        evidence: list[MatchEvidence] = [
            MatchEvidence(
                name_field,
                "exact",
                1.0,
                6.0,
                "exact normalized Agent name",
                expected_name,
                actual_name,
            )
        ]
        expected_type = data.get("agent_type")
        actual_type = row.get("agent_type")
        if expected_type is not None and actual_type is not None:
            type_score = float(
                normalise_match_text(expected_type) == normalise_match_text(actual_type)
            )
            evidence.append(
                MatchEvidence(
                    "agent_type",
                    "corroborating" if type_score == 1.0 else "conflict",
                    type_score,
                    2.0,
                    "compared Agent type",
                    expected_type,
                    actual_type,
                )
            )
            if type_score == 0.0:
                return None
        row_id = row.get("agent_id")
        if not isinstance(row_id, int):
            return None
        return MatchResult(
            row_id,
            explained_confidence(evidence),
            "Agent identity evidence met policy",
            matched_on=tuple(item.field for item in evidence if item.score == 1.0),
            candidate=row,
            evidence=tuple(evidence),
        )

    def _identifier_decision(
        self,
        candidate: MetadataCandidate,
        data: RowMapping,
    ) -> MatchResult | None:
        """
        Resolve identifier owners, then check the selected Agent's type and closest name.

        Ignore hints with no usable integer owner IDs. A hint shared by several Agents yields
        ambiguity over all owners before cross-hint conflict is considered; another narrower hint
        does not disambiguate it. Different singleton owners yield conflict. A sole owner is
        fetched, with absence reported as conflict containing the ID but no evidence items.

        Existing-owner identifier evidence weighs ten. If both types are present and normalize
        differently, append weight-two conflict evidence and return before comparing names.
        Compatible types do not add evidence on this path. Compare a supplied name with the nearest
        canonical/sort/alias value when available, labeling its weight-six evidence
        agent_canonical_name regardless of the actual source field. Similarity strictly below
        identifier_conflict_threshold yields conflict; otherwise return a match without final
        confidence acceptance. Conflict-labeled name evidence can therefore coexist with an overall
        match.

        Example:
            >>> terminal = matcher._identifier_decision(candidate, normalized_data)  # doctest: +SKIP


        :param candidate: MetadataCandidate whose structured identifiers may resolve Agent owners.
        :param data: Repository-normalized name/type fields used to check a uniquely identified owner.
        :return: An identifier-backed match/ambiguous/conflict result, or None when no usable owner exists.
        :raises Exception: Identifier parsing, owner retrieval, malformed IDs, and evidence failures propagate.
        """
        resolved = identifier_owner_rows(
            self.repositories,
            candidate,
            level="agent",
        )
        owner_ids_by_hint: tuple[frozenset[int], ...] = tuple(
            frozenset(
                row["entity_identifier_entity_id"]
                for row in rows
                if isinstance(row.get("entity_identifier_entity_id"), int)
            )
            for _, rows in resolved
            if rows
        )
        owner_ids_by_hint = tuple(
            owner_ids for owner_ids in owner_ids_by_hint if owner_ids
        )
        if not owner_ids_by_hint:
            return None
        all_owner_ids: frozenset[int] = frozenset().union(*owner_ids_by_hint)
        identifier_evidence = tuple(
            MatchEvidence(
                f"identifier:{hint.identifier_type}",
                "identifier",
                1.0,
                10.0,
                "exact identifier ownership",
                hint.normalised_value,
                tuple(
                    sorted(
                        row["entity_identifier_entity_id"]
                        for row in rows
                        if isinstance(row.get("entity_identifier_entity_id"), int)
                    )
                ),
                decisive=True,
            )
            for hint, rows in resolved
            if rows
        )
        if any(len(owner_ids) > 1 for owner_ids in owner_ids_by_hint):
            return MatchResult(
                None,
                1.0,
                "one exact identifier is owned by several Agents",
                decision="ambiguous",
                evidence=identifier_evidence,
                alternatives=tuple(sorted(all_owner_ids)),
            )
        if len(all_owner_ids) > 1:
            return MatchResult(
                None,
                1.0,
                "supplied identifiers resolve to different Agents",
                decision="conflict",
                evidence=identifier_evidence,
                alternatives=tuple(sorted(all_owner_ids)),
            )
        agent_id = next(iter(all_owner_ids))
        row = self.repositories.agents.get(agent_id)
        if row is None:
            return MatchResult(
                None,
                1.0,
                f"identifier refers to missing Agent {agent_id}",
                decision="conflict",
                alternatives=(agent_id,),
            )
        evidence: list[MatchEvidence] = [
            MatchEvidence(
                "identifiers",
                "identifier",
                1.0,
                10.0,
                "exact identifier ownership",
                decisive=True,
            )
        ]
        expected_type = data.get("agent_type")
        actual_type = row.get("agent_type")
        if expected_type is not None and actual_type is not None and normalise_match_text(
            expected_type
        ) != normalise_match_text(actual_type):
            evidence.append(
                MatchEvidence(
                    "agent_type",
                    "conflict",
                    0.0,
                    2.0,
                    "identifier owner has an incompatible Agent type",
                    expected_type,
                    actual_type,
                )
            )
            return MatchResult(
                None,
                explained_confidence(evidence),
                "exact Agent identifier conflicts with the supplied Agent type",
                decision="conflict",
                evidence=tuple(evidence),
                alternatives=(agent_id,),
            )
        expected_name = self._candidate_name(data)
        names = self._row_names(row)
        if expected_name is not None and names:
            _, actual_name = max(
                names,
                key=lambda item: text_similarity(expected_name, item[1]),
            )
            name_score = text_similarity(expected_name, actual_name)
            evidence.append(
                MatchEvidence(
                    "agent_canonical_name",
                    (
                        "exact"
                        if name_score == 1.0
                        else "approximate"
                        if name_score >= self.policy.approximate_text_threshold
                        else "conflict"
                    ),
                    name_score,
                    6.0,
                    "checked supplied name against identifier owner",
                    expected_name,
                    actual_name,
                )
            )
            if name_score < self.policy.identifier_conflict_threshold:
                return MatchResult(
                    None,
                    explained_confidence(evidence),
                    "exact Agent identifier conflicts with the supplied name",
                    decision="conflict",
                    evidence=tuple(evidence),
                    alternatives=(agent_id,),
                )
        return MatchResult(
            agent_id,
            explained_confidence(evidence),
            "exact identifier uniquely identifies an existing Agent",
            matched_on=("identifiers",),
            candidate=row,
            evidence=tuple(evidence),
        )

    def _evaluated_candidates(
        self,
        candidate: MetadataCandidate,
    ) -> tuple[tuple[MatchResult, ...], MatchResult | None]:
        """
        Separate terminal identifier outcomes from descriptive Agent scan candidates.

        Require a MetadataCandidate, then normalize fields while ignoring unknown columns and
        retaining the repository's other alias/ID validation. Identifier rows are read first,
        including when the candidate supplies no identifiers. A terminal match is both the sole
        candidate and terminal result; terminal ambiguity/conflict supplies an empty candidate
        tuple. Only an unresolved identifier lookup with no terminal decision scans all Agent rows.

        Example:
            >>> possible, terminal = matcher._evaluated_candidates(candidate)  # doctest: +SKIP


        :param candidate: MetadataCandidate supplying Agent name/type fields and optional identifier hints.
        :return: A (possible-match tuple, optional terminal decision) pair before display ranking.
        :raises TypeError: If candidate is not a MetadataCandidate.
        :raises Exception: Repository normalization, identifier lookup, and descriptive-evaluation failures propagate.
        """
        if not isinstance(candidate, MetadataCandidate):
            raise TypeError("candidate must be a MetadataCandidate")
        repository = self.repositories.agents
        data = repository.normalise_input(candidate.data, ignore_unknown=True)
        identifier_decision = self._identifier_decision(candidate, data)
        if identifier_decision is not None:
            if identifier_decision.is_match:
                return (identifier_decision,), identifier_decision
            return (), identifier_decision
        results = tuple(
            result
            for row in repository._all_rows()
            if (result := self._evaluate_row(row, data)) is not None
        )
        return results, None

    def candidates(
        self,
        candidate: MetadataCandidate,
        *,
        limit: int = 20,
    ) -> Sequence[MatchResult]:
        """
        Collect possible Agent matches and apply a display limit after ranking.

        The concrete matcher evaluates identifier ownership first, otherwise scanning descriptive
        rows. Decisive evidence ranks ahead of descending confidence, then ascending entity ID with
        -1 used for a false-valued ID. Acceptance and ambiguity are not resolved by this method.
        Identifier ambiguity or conflict is suppressed as an empty candidate tuple; use best to
        distinguish it.

        A limit of zero still performs normalization and matching before returning empty. Candidate
        records retain their stored row mappings and can have confidence below acceptance. No
        catalogue entity is created or changed.

        Example:
            >>> candidates = catalog.matching.agents.candidates(candidate, limit=5)  # doctest: +SKIP


        :param candidate: MetadataCandidate containing Agent fields and supported structured hints.
        :param limit: Nonnegative integer result cap applied after evaluation; booleans are rejected.
        :return: Ranked possible matches, returned as a tuple by the concrete implementation.
        :raises TypeError: If limit is not an integer, is a boolean, or candidate is not a MetadataCandidate.
        :raises ValueError: If limit is negative or a supplied identifier fails normalization.
        :raises Exception: Repository access, input/hint parsing, and evidence failures propagate.
        """

        if not isinstance(limit, int) or isinstance(limit, bool):
            raise TypeError("limit must be an integer")
        if limit < 0:
            raise ValueError("limit cannot be negative")
        results, _ = self._evaluated_candidates(candidate)
        ranked = sorted(
            results,
            key=lambda result: (
                -int(any(item.decisive for item in result.evidence)),
                -result.confidence,
                result.entity_id or -1,
            ),
        )
        return tuple(ranked[:limit])

    def best(self, candidate: MetadataCandidate) -> MatchResult:
        """
        Return the complete Agent decision, giving resolved identifier ownership priority.

        Identifier-backed match, ambiguity, and conflict results return directly. In particular, a
        uniquely identified owner can remain a match below the ordinary acceptance threshold, and an
        individual conflict evidence item need not make the overall decision conflict. Specialized
        contradiction checks determine the terminal result. Descriptive-only candidates instead pass
        through common acceptance and ambiguity selection over the full set.

        The example uses in-memory repository stand-ins to show the distinction between weighted
        confidence and a terminal identifier decision. Matching itself performs no persistence.

        Example:
            >>> from types import SimpleNamespace
            >>> owner = {"agent_id": 7, "agent_canonical_name": "abcd"}
            >>> identifier = {
            ...     "entity_identifier_entity_type": "agent",
            ...     "entity_identifier_scheme": "local", "entity_identifier_value": "42",
            ...     "entity_identifier_entity_id": 7,
            ... }
            >>> repository = SimpleNamespace(
            ...     get=lambda entity_id: owner,
            ...     normalise_input=lambda data, **options: dict(data),
            ... )
            >>> repositories = SimpleNamespace(
            ...     agents=repository,
            ...     identifiers=SimpleNamespace(_all_rows=lambda: (identifier,)),
            ... )
            >>> matcher = AgentMatcher(None, repositories)
            >>> candidate = MetadataCandidate(
            ...     {"agent_canonical_name": "abxy"}, hints={"identifiers": {"local": "42"}},
            ... )
            >>> result = matcher.best(candidate)
            >>> result.decision, result.confidence, result.evidence[-1].kind
            ('match', 0.8125, 'conflict')


        :param candidate: MetadataCandidate carrying Agent fields and optional identifier/other supported hints.
        :return: A match, no_match, ambiguous, or conflict result; selected records retain the stored row mapping.
        :raises TypeError: If candidate is not a MetadataCandidate.
        :raises Exception: Repository, input/hint normalization, and evidence failures propagate.
        """

        results, terminal = self._evaluated_candidates(candidate)
        if terminal is not None:
            return terminal
        return decide_best(results, subject="Agent", policy=self.policy)

    def exact(self, candidate_str: str) -> MatchResult:
        """
        Apply final matching policy to one normalized Agent name string.

        This convenience supplies only the name, without identifier or supporting hints. Matching is
        Unicode/case/punctuation tolerant, not byte equality; empty normalized text does not
        establish identity. Duplicate qualifying rows can produce ambiguity. The concrete
        implementation wraps the string in a MetadataCandidate and calls best.

        Example:
            >>> result = catalog.matching.agents.exact("Mary Shelley")  # doctest: +SKIP


        :param candidate_str: Agent name supplied as a string; positional calls are portable across the protocol and implementation.
        :return: A normalized exact match, no_match, or ambiguous result for the string-only input.
        :raises TypeError: If the supplied title/name is not a string.
        :raises Exception: Repository and delegated matching failures propagate.
        """

        if not isinstance(candidate_str, str):
            raise TypeError("candidate_str must be a string")
        return self.best(MetadataCandidate({"name": candidate_str}))


__all__ = ["AgentMatcher"]
