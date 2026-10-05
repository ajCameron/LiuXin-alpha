"""
Match Works using normalized titles, supporting metadata, and identifier ownership.

The descriptive path requires exact title evidence or a sufficiently similar title
with corroboration, then applies common confidence/ambiguity selection. Identifier
ownership is resolved first and can produce a terminal match, ambiguity, or conflict
without the final acceptance gate. Candidate lists hide terminal non-selections;
use best when deciding whether a repository operation may reuse or create a Work.
Reads are delegated to repositories, and the matcher performs no entity writes.

Example:
    >>> WorkMatcher._candidate_title({"work_canonical_title": "Frankenstein"})
    'Frankenstein'
"""

from __future__ import annotations

from collections.abc import Sequence
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
    agent_names,
    decide_best,
    explained_confidence,
    identifier_owner_rows,
    normalise_match_text,
    text_similarity,
)


class WorkMatcher:
    """
    Resolve Work identity from titles, supporting metadata, and identifier ownership.

    Without a known identifier owner, exact normalized titles qualify directly; approximate titles
    need the configured similarity cutoff and exact supporting field or credited-Agent evidence.
    Weighted confidence then participates in acceptance and ambiguity selection. Known identifier
    ownership takes a separate terminal path, which can select an owner below the acceptance
    threshold after its specific title-contradiction check. No catalogue writes are performed.

    Example:
        >>> matcher = catalog.matching.works  # doctest: +SKIP
        >>> decision = matcher.best(MetadataCandidate({"title": "Frankenstein"}))  # doctest: +SKIP


    :ivar db: Borrowed database context, retained without lifecycle management.
    :ivar repositories: Owners supplying Work rows, curated identifier rows, and credited Agents.
    :ivar policy: Approximate-title, identifier-conflict, acceptance, and ambiguity boundaries.
    """

    def __init__(
        self,
        db: DatabaseHandle,
        repositories: Any,
        policy: MatchingPolicy = DEFAULT_MATCHING_POLICY,
    ) -> None:
        """
        Retain the database context, repository group, and Work matching policy.

        Assign all three objects by reference without type checks, queries, copying, or policy
        rebinding. Actual reads use the supplied repositories; db is retained as context and is not
        opened or closed by this matcher.

        Example:
            >>> matcher = WorkMatcher(None, None)
            >>> matcher.policy is DEFAULT_MATCHING_POLICY
            True


        :param db: Borrowed database context retained on the matcher; repository objects perform the reads.
        :param repositories: Group exposing the Work and identifier repositories plus any supporting owners.
        :param policy: Matching boundaries retained by reference; defaults to the shared frozen policy.
        :return: None after assigning the three dependencies.
        """

        self.db = db
        self.repositories = repositories
        self.policy = policy

    @staticmethod
    def _candidate_title(data: RowMapping) -> object | None:
        """
        Choose the first non-None candidate title in declared field precedence.

        Try work_title, work_canonical_title, then work_sort_title. Empty strings, zero, and other
        false-valued objects still take precedence; the helper does not normalize text or validate
        types.

        Example:
            >>> WorkMatcher._candidate_title({"work_title": "", "work_canonical_title": "Other"})
            ''
            >>> WorkMatcher._candidate_title({}) is None
            True


        :param data: Normalized candidate mapping containing storage column names.
        :return: The first non-None title object, or None if all three fields are absent/None.
        """
        for field in ("work_title", "work_canonical_title", "work_sort_title"):
            value = data.get(field)
            if value is not None:
                return cast(object, value)
        return None

    @staticmethod
    def _row_titles(row: RowMapping) -> tuple[tuple[str, object], ...]:
        """
        Collect stored title variants in title, canonical-title, then sort-title order.

        Preserve every non-None value and its column name, including empty strings or non-string
        values. That order breaks equal-similarity ties in later comparisons. The returned tuple
        does not copy the referenced field values.

        Example:
            >>> WorkMatcher._row_titles({"work_title": None, "work_canonical_title": "", "work_sort_title": "Shelley"})
            (('work_canonical_title', ''), ('work_sort_title', 'Shelley'))


        :param row: Stored Work mapping whose available title variants should be collected.
        :return: An ordered tuple of (stored title column, non-None value) pairs.
        """
        return tuple(
            (field, row[field])
            for field in ("work_title", "work_canonical_title", "work_sort_title")
            if row.get(field) is not None
        )

    def _agent_evidence(
        self,
        work_id: int,
        candidate: MetadataCandidate,
    ) -> MatchEvidence | None:
        """
        Compare hinted names with credited Agents' normalized canonical names.

        No name hints means no relationship read. Otherwise fetch Agents linked to the Work without
        a role filter, normalize canonical names, and omit evidence when no normalized stored names
        remain. Any shared name yields score one; a disjoint nonempty set yields zero. This is not
        an all-contributors check, and stored aliases are not searched. A present None name passes
        through str normalization as none, while an absent canonical-name key normalizes to empty.

        Example:
            >>> from types import SimpleNamespace
            >>> agents = SimpleNamespace(list_for_wemi=lambda **scope: ({"agent_canonical_name": "Mary Shelley"},))
            >>> matcher = WorkMatcher(None, SimpleNamespace(agents=agents))
            >>> candidate = MetadataCandidate({}, hints={"agents": ["MARY SHELLEY", "Another Writer"]})
            >>> evidence = matcher._agent_evidence(7, candidate)
            >>> evidence.score, evidence.weight, evidence.decisive
            (1.0, 2.0, False)


        :param work_id: Work ID passed to the Agent relationship lookup without independent validation here.
        :param candidate: MetadataCandidate whose agents hints are parsed into normalized names.
        :return: Weight-two corroborating/conflict evidence, or None when either side has no usable names.
        :raises Exception: Agent-hint parsing and relationship-read failures propagate.
        """
        expected_names = agent_names(candidate)
        if not expected_names:
            return None
        linked = self.repositories.agents.list_for_wemi(
            level="work",
            entity_id=work_id,
        )
        existing_names = tuple(
            normalise_match_text(row.get("agent_canonical_name", "")) for row in linked
        )
        existing_names = tuple(name for name in existing_names if name)
        if not existing_names:
            return None
        score = float(bool(set(expected_names) & set(existing_names)))
        return MatchEvidence(
            "agents",
            "corroborating" if score == 1.0 else "conflict",
            score,
            2.0,
            "compared credited Work Agents",
            expected_names,
            existing_names,
        )

    def _field_evidence(
        self,
        row: RowMapping,
        data: RowMapping,
    ) -> tuple[MatchEvidence, ...]:
        """
        Score available descriptive field pairs without treating absence as disagreement.

        Original year, original language ID, and creator sort each weigh two; type and medium each
        weigh one. Compare only pairs whose values are non-None. Two strings use normalized text
        similarity; other pairs use equality. Score one is corroborating evidence, and every lower
        score is labeled conflict without independently rejecting the Work.

        Example:
            >>> matcher = WorkMatcher(None, None)
            >>> evidence = matcher._field_evidence({"work_original_year": 1818}, {"work_original_year": 1900, "work_medium": "text"})
            >>> [(item.field, item.score, item.weight) for item in evidence]
            [('work_original_year', 0.0, 2.0)]


        :param row: Stored Work fields used as existing comparison values.
        :param data: Normalized candidate fields; missing/None comparisons contribute no evidence.
        :return: Evidence tuple in the fixed descriptive-field order, retaining compared values.
        """
        specifications = (
            ("work_original_year", 2.0),
            ("work_original_language_id", 2.0),
            ("work_creator_sort", 2.0),
            ("work_type", 1.0),
            ("work_medium", 1.0),
        )
        result: list[MatchEvidence] = []
        for field, weight in specifications:
            expected = data.get(field)
            actual = row.get(field)
            if expected is None or actual is None:
                continue
            score = (
                text_similarity(expected, actual)
                if isinstance(expected, str) and isinstance(actual, str)
                else float(expected == actual)
            )
            result.append(
                MatchEvidence(
                    field,
                    "corroborating" if score == 1.0 else "conflict",
                    score,
                    weight,
                    f"compared Work field {field}",
                    expected,
                    actual,
                )
            )
        return tuple(result)

    def _evaluate_row(
        self,
        row: RowMapping,
        candidate: MetadataCandidate,
        data: RowMapping,
    ) -> MatchResult | None:
        """
        Build a descriptive Work candidate when title and corroboration rules qualify it.

        Compare the first supplied title with every stored variant, choosing the greatest similarity
        and the first variant on ties. Title evidence weighs six; append available field evidence
        and, for integer-ID rows, credited-Agent evidence. An exact title qualifies without support;
        an approximate title must meet its cutoff and have at least one exact corroborating item.

        Qualification does not enforce the final acceptance threshold. Disagreements can reduce the
        returned confidence below acceptance, as the example shows. The row is retained by
        reference, matched_on lists all score-one fields, and the integer-ID check admits booleans
        without requiring positive IDs.

        Example:
            >>> matcher = WorkMatcher(None, None)
            >>> candidate = MetadataCandidate({"work_title": "ABCD", "work_original_year": 1900})
            >>> result = matcher._evaluate_row(
            ...     {"work_id": 7, "work_title": "ABCD", "work_original_year": 1818},
            ...     candidate, candidate.data,
            ... )
            >>> result.confidence, result.is_match
            (0.75, True)


        :param row: Existing Work row containing title variants and an integer work_id for a returned result.
        :param candidate: Original candidate carrying optional credited-Agent name hints.
        :param data: Repository-normalized candidate fields used for title and descriptive comparisons.
        :return: A scored possible match, or None for missing title evidence, invalid ID type, or failed title/support qualification.
        :raises Exception: Hint parsing, relationship lookup, and comparison/evidence failures propagate.
        """
        expected_title = self._candidate_title(data)
        titles = self._row_titles(row)
        if expected_title is None or not titles:
            return None
        title_field, actual_title = max(
            titles,
            key=lambda item: text_similarity(expected_title, item[1]),
        )
        title_score = text_similarity(expected_title, actual_title)
        title_evidence = MatchEvidence(
            title_field,
            "exact" if title_score == 1.0 else "approximate",
            title_score,
            6.0,
            "compared normalized Work title",
            expected_title,
            actual_title,
        )
        evidence = [title_evidence, *self._field_evidence(row, data)]
        row_id = row.get("work_id")
        if not isinstance(row_id, int):
            return None
        agent_evidence = self._agent_evidence(row_id, candidate)
        if agent_evidence is not None:
            evidence.append(agent_evidence)
        corroborated = any(
            item.kind == "corroborating" and item.score == 1.0 for item in evidence[1:]
        )
        qualifies = title_score == 1.0 or (
            title_score >= self.policy.approximate_text_threshold and corroborated
        )
        if not qualifies:
            return None
        return MatchResult(
            row_id,
            explained_confidence(evidence),
            "Work identity evidence met policy",
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
        Resolve identifier owner sets before descriptive Work matching.

        Hints with no matching integer owner IDs do not constrain the result. Any hint owned by
        multiple Works produces ambiguity over the union of owners, even if another hint could
        narrow that set. Otherwise distinct singleton owners produce conflict. A sole owner is
        fetched; an absent Work produces conflict with its ID but no evidence items.

        For an existing owner, identifier evidence weighs ten. Compare the first supplied title with
        the closest stored variant when both are available, adding weight-six evidence labeled
        work_title. Similarity strictly below identifier_conflict_threshold produces conflict.
        Otherwise append available descriptive field evidence and return a match without final
        acceptance or Agent-hint checks. Low-scoring conflict-labeled evidence can coexist with this
        match; matched_on contains only identifiers.

        Example:
            >>> terminal = matcher._identifier_decision(candidate, normalized_data)  # doctest: +SKIP


        :param candidate: MetadataCandidate with structured identifier hints normalized by the shared owner lookup.
        :param data: Normalized Work fields used to check the selected identifier owner.
        :return: An identifier-backed match/ambiguous/conflict decision, or None when no usable owner is found.
        :raises Exception: Identifier normalization, repository access, malformed owner IDs, and evidence failures propagate.
        """
        resolved = identifier_owner_rows(
            self.repositories,
            candidate,
            level="work",
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
                "one exact identifier is owned by several Works",
                decision="ambiguous",
                evidence=identifier_evidence,
                alternatives=tuple(sorted(all_owner_ids)),
            )
        if len(all_owner_ids) > 1:
            return MatchResult(
                None,
                1.0,
                "supplied identifiers resolve to different Works",
                decision="conflict",
                evidence=identifier_evidence,
                alternatives=tuple(sorted(all_owner_ids)),
            )
        work_id = next(iter(all_owner_ids))
        row = self.repositories.works.get(work_id)
        if row is None:
            return MatchResult(
                None,
                1.0,
                f"identifier refers to missing Work {work_id}",
                decision="conflict",
                alternatives=(work_id,),
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
        expected_title = self._candidate_title(data)
        titles = self._row_titles(row)
        if expected_title is not None and titles:
            _, actual_title = max(
                titles,
                key=lambda item: text_similarity(expected_title, item[1]),
            )
            title_score = text_similarity(expected_title, actual_title)
            title_evidence = MatchEvidence(
                "work_title",
                (
                    "exact"
                    if title_score == 1.0
                    else "approximate"
                    if title_score >= self.policy.approximate_text_threshold
                    else "conflict"
                ),
                title_score,
                6.0,
                "checked supplied title against identifier owner",
                expected_title,
                actual_title,
            )
            evidence.append(title_evidence)
            if title_score < self.policy.identifier_conflict_threshold:
                return MatchResult(
                    None,
                    explained_confidence(evidence),
                    "exact Work identifier conflicts with the supplied title",
                    decision="conflict",
                    evidence=tuple(evidence),
                    alternatives=(work_id,),
                )
        evidence.extend(self._field_evidence(row, data))
        return MatchResult(
            work_id,
            explained_confidence(evidence),
            "exact identifier uniquely identifies an existing Work",
            matched_on=("identifiers",),
            candidate=row,
            evidence=tuple(evidence),
        )

    def _evaluated_candidates(
        self,
        candidate: MetadataCandidate,
    ) -> tuple[tuple[MatchResult, ...], MatchResult | None]:
        """
        Normalize Work input and separate terminal identifier decisions from scan candidates.

        Validate the candidate type and normalize its data with unknown columns ignored; repository
        ID/alias rules still apply. Resolve identifier ownership first, which reads identifier rows
        even without supplied hints. A terminal match appears both as the sole candidate and as the
        terminal result; ambiguity or conflict returns no candidates. Only absence of a terminal
        decision causes a full Work-row scan and descriptive evaluation.

        Example:
            >>> possible, terminal = matcher._evaluated_candidates(candidate)  # doctest: +SKIP


        :param candidate: MetadataCandidate with Work fields and optional structured matching hints.
        :return: A (possible-match tuple, optional terminal decision) pair before candidate display sorting.
        :raises TypeError: If candidate is not a MetadataCandidate.
        :raises Exception: Repository normalization, identifier resolution, and row-evaluation failures propagate.
        """
        if not isinstance(candidate, MetadataCandidate):
            raise TypeError("candidate must be a MetadataCandidate")
        repository = self.repositories.works
        data = repository.normalise_input(candidate.data, ignore_unknown=True)
        identifier_decision = self._identifier_decision(candidate, data)
        if identifier_decision is not None:
            if identifier_decision.is_match:
                return (identifier_decision,), identifier_decision
            return (), identifier_decision
        results = tuple(
            result
            for row in repository._all_rows()
            if (result := self._evaluate_row(row, candidate, data)) is not None
        )
        return results, None

    def candidates(
        self,
        candidate: MetadataCandidate,
        *,
        limit: int = 20,
    ) -> Sequence[MatchResult]:
        """
        Collect possible Work matches and apply a display limit after ranking.

        The concrete matcher evaluates identifier ownership first, otherwise scanning descriptive
        rows. Decisive evidence ranks ahead of descending confidence, then ascending entity ID with
        -1 used for a false-valued ID. Acceptance and ambiguity are not resolved by this method.
        Identifier ambiguity or conflict is suppressed as an empty candidate tuple; use best to
        distinguish it.

        A limit of zero still performs normalization and matching before returning empty. Candidate
        records retain their stored row mappings and can have confidence below acceptance. No
        catalogue entity is created or changed.

        Example:
            >>> candidates = catalog.matching.works.candidates(candidate, limit=5)  # doctest: +SKIP


        :param candidate: MetadataCandidate containing Work fields and supported structured hints.
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
        Return the complete Work decision, giving resolved identifier ownership priority.

        Identifier-backed match, ambiguity, and conflict results return directly. In particular, a
        uniquely identified owner can remain a match below the ordinary acceptance threshold, and an
        individual conflict evidence item need not make the overall decision conflict. Specialized
        contradiction checks determine the terminal result. Descriptive-only candidates instead pass
        through common acceptance and ambiguity selection over the full set.

        The example uses in-memory repository stand-ins to show the distinction between weighted
        confidence and a terminal identifier decision. Matching itself performs no persistence.

        Example:
            >>> from types import SimpleNamespace
            >>> owner = {"work_id": 7, "work_title": "abcd"}
            >>> identifier = {
            ...     "entity_identifier_entity_type": "work",
            ...     "entity_identifier_scheme": "local", "entity_identifier_value": "42",
            ...     "entity_identifier_entity_id": 7,
            ... }
            >>> repository = SimpleNamespace(
            ...     get=lambda entity_id: owner,
            ...     normalise_input=lambda data, **options: dict(data),
            ... )
            >>> repositories = SimpleNamespace(
            ...     works=repository,
            ...     identifiers=SimpleNamespace(_all_rows=lambda: (identifier,)),
            ... )
            >>> matcher = WorkMatcher(None, repositories)
            >>> candidate = MetadataCandidate(
            ...     {"work_title": "abxy"}, hints={"identifiers": {"local": "42"}},
            ... )
            >>> result = matcher.best(candidate)
            >>> result.decision, result.confidence, result.evidence[-1].kind
            ('match', 0.8125, 'conflict')


        :param candidate: MetadataCandidate carrying Work fields and optional identifier/other supported hints.
        :return: A match, no_match, ambiguous, or conflict result; selected records retain the stored row mapping.
        :raises TypeError: If candidate is not a MetadataCandidate.
        :raises Exception: Repository, input/hint normalization, and evidence failures propagate.
        """

        results, terminal = self._evaluated_candidates(candidate)
        if terminal is not None:
            return terminal
        return decide_best(results, subject="Work", policy=self.policy)

    def exact(self, candidate_str: str) -> MatchResult:
        """
        Apply final matching policy to one normalized Work title string.

        This convenience supplies only the title, without identifier or supporting hints. Matching
        is Unicode/case/punctuation tolerant, not byte equality; empty normalized text does not
        establish identity. Duplicate qualifying rows can produce ambiguity. The concrete
        implementation wraps the string in a MetadataCandidate and calls best.

        Example:
            >>> result = catalog.matching.works.exact("Frankenstein")  # doctest: +SKIP


        :param candidate_str: Work title supplied as a string; positional calls are portable across the protocol and implementation.
        :return: A normalized exact match, no_match, or ambiguous result for the string-only input.
        :raises TypeError: If the supplied title/name is not a string.
        :raises Exception: Repository and delegated matching failures propagate.
        """

        if not isinstance(candidate_str, str):
            raise TypeError("candidate_str must be a string")
        return self.best(MetadataCandidate({"title": candidate_str}))


__all__ = ["WorkMatcher"]
