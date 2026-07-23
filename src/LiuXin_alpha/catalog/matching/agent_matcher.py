"""Evidence-based Agent identity matching."""

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
    """Match incoming metadata candidates to existing Agents.

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
    def _candidate_name(data: RowMapping) -> object | None:
        for field in ("agent_canonical_name", "agent_sort_name"):
            value = data.get(field)
            if value is not None:
                return cast(object, value)
        return None

    @staticmethod
    def _aliases(value: object) -> tuple[str, ...]:
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
            part.strip() for part in re.split(r"[;|\n]", value) if part.strip()
        )

    def _row_names(self, row: RowMapping) -> tuple[tuple[str, object], ...]:
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
        """Return exact policy-qualified Agent candidates.

        :param candidate: Candidate Agent metadata and structured hints.
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
        """Return the final Agent identity decision for ``candidate``.

        :param candidate: Candidate Agent metadata and structured hints.
        :return: Explained match, no-match, ambiguity, or conflict.
        """

        results, terminal = self._evaluated_candidates(candidate)
        if terminal is not None:
            return terminal
        return decide_best(results, subject="Agent", policy=self.policy)

    def exact(self, candidate_str: str) -> MatchResult:
        """Apply the final policy to an Agent name string.

        :param candidate_str: Agent name to normalize and match.
        :return: Exact match, no-match, or ambiguity result.
        """

        if not isinstance(candidate_str, str):
            raise TypeError("candidate_str must be a string")
        return self.best(MetadataCandidate({"name": candidate_str}))


__all__ = ["AgentMatcher"]
