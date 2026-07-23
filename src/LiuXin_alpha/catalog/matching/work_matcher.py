"""Evidence-based Work identity matching."""

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
    """Match incoming metadata candidates to existing Works.

    :param db: Catalog database handle.
    :param repositories: Bound catalog repository group.
    :param policy: Matching decision boundaries.
    """

    def __init__(
        self,
        db: DatabaseHandle,
        repositories: Any,
        policy: MatchingPolicy = DEFAULT_MATCHING_POLICY,
    ) -> None:
        """Store the database, repository group, and policy.

        :param db: Catalog database handle.
        :param repositories: Bound catalog repository group.
        :param policy: Matching decision boundaries.
        :return: None.
        """

        self.db = db
        self.repositories = repositories
        self.policy = policy

    @staticmethod
    def _candidate_title(data: RowMapping) -> object | None:
        for field in ("work_title", "work_canonical_title", "work_sort_title"):
            value = data.get(field)
            if value is not None:
                return cast(object, value)
        return None

    @staticmethod
    def _row_titles(row: RowMapping) -> tuple[tuple[str, object], ...]:
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
        """Return policy-qualified Work candidates in deterministic order.

        :param candidate: Candidate Work metadata and structured hints.
        :param limit: Maximum candidates to return.
        :return: Qualified candidates ordered by evidence and confidence.
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
        """Return the final Work identity decision for ``candidate``.

        :param candidate: Candidate Work metadata and structured hints.
        :return: Explained match, no-match, ambiguity, or conflict.
        """

        results, terminal = self._evaluated_candidates(candidate)
        if terminal is not None:
            return terminal
        return decide_best(results, subject="Work", policy=self.policy)

    def exact(self, candidate_str: str) -> MatchResult:
        """Apply the final policy to a Work title string.

        :param candidate_str: Work title to normalize and match.
        :return: Exact match, no-match, or ambiguity result.
        """

        if not isinstance(candidate_str, str):
            raise TypeError("candidate_str must be a string")
        return self.best(MetadataCandidate({"title": candidate_str}))


__all__ = ["WorkMatcher"]
