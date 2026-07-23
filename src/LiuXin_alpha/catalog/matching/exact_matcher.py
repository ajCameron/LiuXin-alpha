"""Exact-default matching for catalog value and vocabulary entities."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, cast
import unicodedata

from ..api.common import MatchEvidence, MatchResult, MetadataCandidate, RowMapping
from .policy import (
    DEFAULT_MATCHING_POLICY,
    MatchingPolicy,
    decide_best,
    text_similarity,
)


def _empty_aliases() -> dict[str, str]:
    return {}


def _empty_scope_defaults() -> dict[str, object | None]:
    return {}


@dataclass(frozen=True, slots=True)
class ExactEntitySpec:
    """Describe exact identity for one catalog entity table.

    :param entity_name: Singular human-readable entity name.
    :param table_name: Storage table containing the entities.
    :param id_column: Storage primary-key column.
    :param primary_field: Field populated by scalar exact lookups.
    :param identity_fields: Fields which jointly describe exact identity.
    :param scalar_fields: Row fields searched by a scalar exact lookup.
    :param casefold_fields: Text fields compared case-insensitively.
    :param input_aliases: Public candidate aliases for storage columns.
    :param scope_fields: Optional fields which constrain matching when supplied.
    :param scope_defaults: Default values for omitted scope fields.
    :param required_scope_fields: Scope fields required before matching.
    :param required_identity_fields: Identity fields required before matching.
    :param policy_field: Display field eligible for opt-in approximate matching.
    :param reusable: Whether ``match_or_create`` may reuse an exact match.
    :param mutable: Whether repository CRUD may mutate the table.
    :param normalized_storage_fields: Derived normalized destination/source pairs.
    """

    entity_name: str
    table_name: str
    id_column: str
    primary_field: str
    identity_fields: tuple[str, ...]
    scalar_fields: tuple[str, ...]
    casefold_fields: frozenset[str] = frozenset()
    input_aliases: Mapping[str, str] = field(default_factory=_empty_aliases)
    scope_fields: tuple[str, ...] = ()
    scope_defaults: Mapping[str, object | None] = field(
        default_factory=_empty_scope_defaults
    )
    required_scope_fields: tuple[str, ...] = ()
    required_identity_fields: tuple[str, ...] = ()
    policy_field: str | None = None
    reusable: bool = True
    mutable: bool = True
    normalized_storage_fields: tuple[tuple[str, str], ...] = ()


def _normalise_exact_text(value: object, *, casefold: bool) -> str:
    text = unicodedata.normalize("NFKC", str(value))
    text = " ".join(text.split())
    return text.casefold() if casefold else text


def _exact_equal(spec: ExactEntitySpec, field_name: str, left: object, right: object) -> bool:
    if isinstance(left, str) and isinstance(right, str):
        return _normalise_exact_text(
            left,
            casefold=field_name in spec.casefold_fields,
        ) == _normalise_exact_text(
            right,
            casefold=field_name in spec.casefold_fields,
        )
    return left == right


class ExactEntityMatcher:
    """Match one configured catalog entity exactly unless policy is requested.

    :param repository: Bound repository for the configured entity.
    :param spec: Exact entity identity description.
    :param matching_policy: Approximate matching boundaries used only when
        ``use_policy=True`` is passed by the caller.
    """

    def __init__(
        self,
        repository: Any,
        spec: ExactEntitySpec,
        matching_policy: MatchingPolicy = DEFAULT_MATCHING_POLICY,
    ) -> None:
        """Store repository and matching configuration.

        :param repository: Bound entity repository.
        :param spec: Exact entity identity description.
        :param matching_policy: Opt-in approximate matching boundaries.
        :return: None.
        """

        self.repository = repository
        self.spec = spec
        self.matching_policy = matching_policy

    def _candidate_data(self, candidate: MetadataCandidate) -> dict[str, object]:
        data = self.repository.normalise_input(
            candidate.data,
            allow_id=True,
            ignore_unknown=True,
        )
        data.pop(self.spec.id_column, None)
        for field_name, default in self.spec.scope_defaults.items():
            data.setdefault(field_name, default)
        return cast(dict[str, object], data)

    def _scope_is_complete(self, data: Mapping[str, object]) -> bool:
        return all(
            data.get(field_name) is not None
            for field_name in self.spec.required_scope_fields
        )

    def _identity_is_complete(self, data: Mapping[str, object]) -> bool:
        return all(
            data.get(field_name) is not None
            for field_name in self.spec.required_identity_fields
        )

    def _scope_matches(self, row: RowMapping, data: Mapping[str, object]) -> bool:
        for field_name in self.spec.scope_fields:
            if field_name not in data:
                continue
            if not _exact_equal(
                self.spec,
                field_name,
                data.get(field_name),
                row.get(field_name),
            ):
                return False
        for field_name in self.spec.required_scope_fields:
            if not _exact_equal(
                self.spec,
                field_name,
                data.get(field_name),
                row.get(field_name),
            ):
                return False
        return True

    def _identity_values(self, data: Mapping[str, object]) -> tuple[tuple[str, object], ...]:
        return tuple(
            (field_name, data[field_name])
            for field_name in self.spec.identity_fields
            if data.get(field_name) is not None
        )

    def _row_identity_match(
        self,
        row: RowMapping,
        field_name: str,
        expected: object,
    ) -> str | None:
        row_fields = (
            self.spec.scalar_fields
            if field_name == self.spec.primary_field
            else (field_name,)
        )
        return next(
            (
                row_field
                for row_field in row_fields
                if row.get(row_field) is not None
                and _exact_equal(self.spec, row_field, expected, row[row_field])
            ),
            None,
        )

    def _exact_results(
        self,
        candidate: MetadataCandidate,
    ) -> tuple[tuple[MatchResult, ...], MatchResult | None]:
        data = self._candidate_data(candidate)
        if not self._scope_is_complete(data):
            return (), MatchResult(
                None,
                0.0,
                f"{self.spec.entity_name} matching requires explicit scope",
                decision="no_match",
            )
        if not self._identity_is_complete(data):
            return (), MatchResult(
                None,
                0.0,
                f"{self.spec.entity_name} matching requires complete identity fields",
                decision="no_match",
            )
        identity_values = self._identity_values(data)
        if not identity_values:
            return (), MatchResult(
                None,
                0.0,
                f"no {self.spec.entity_name} identity fields supplied",
                decision="no_match",
            )

        scoped_rows = tuple(
            row
            for row in self.repository._all_rows()
            if self._scope_matches(row, data)
        )
        results: list[MatchResult] = []
        for row in scoped_rows:
            matched_fields = tuple(
                self._row_identity_match(row, field_name, expected)
                for field_name, expected in identity_values
            )
            if any(field_name is None for field_name in matched_fields):
                continue
            row_id = row.get(self.spec.id_column)
            if not isinstance(row_id, int):
                continue
            evidence = tuple(
                MatchEvidence(
                    matched_field,
                    "exact",
                    1.0,
                    1.0,
                    f"exact normalized {self.spec.entity_name} field",
                    expected,
                    row[matched_field],
                    decisive=True,
                )
                for (_candidate_field, expected), matched_field in zip(
                    identity_values,
                    matched_fields,
                    strict=True,
                )
                if matched_field is not None
            )
            results.append(
                MatchResult(
                    row_id,
                    1.0,
                    f"exact normalized {self.spec.entity_name} identity",
                    matched_on=tuple(
                        field_name
                        for field_name in matched_fields
                        if field_name is not None
                    ),
                    candidate=row,
                    evidence=evidence,
                )
            )

        if results:
            return tuple(results), None

        individual_matches: list[tuple[str, set[int]]] = []
        for field_name, expected in identity_values:
            row_ids = {
                row_id
                for row in scoped_rows
                if self._row_identity_match(row, field_name, expected) is not None
                and isinstance((row_id := row.get(self.spec.id_column)), int)
            }
            if row_ids:
                individual_matches.append((field_name, row_ids))
        if len(individual_matches) > 1:
            intersection: set[int] = set(individual_matches[0][1])
            alternatives: set[int] = set()
            for _, row_ids in individual_matches:
                intersection.intersection_update(row_ids)
                alternatives.update(row_ids)
            if not intersection and len(alternatives) > 1:
                return (), MatchResult(
                    None,
                    1.0,
                    f"exact {self.spec.entity_name} identity fields resolve differently",
                    decision="conflict",
                    evidence=tuple(
                        MatchEvidence(
                            field_name,
                            "conflict",
                            0.0,
                            1.0,
                            "exact identity field resolves to another entity",
                            data[field_name],
                            tuple(sorted(row_ids)),
                            decisive=True,
                        )
                        for field_name, row_ids in individual_matches
                    ),
                    alternatives=tuple(sorted(alternatives)),
                )
        return (), None

    def _policy_results(self, candidate: MetadataCandidate) -> tuple[MatchResult, ...]:
        policy_field = self.spec.policy_field
        if policy_field is None:
            return ()
        data = self._candidate_data(candidate)
        expected = data.get(policy_field)
        if expected is None or not self._scope_is_complete(data):
            return ()
        results: list[MatchResult] = []
        for row in self.repository._all_rows():
            actual = row.get(policy_field)
            row_id = row.get(self.spec.id_column)
            if (
                actual is None
                or not isinstance(row_id, int)
                or not self._scope_matches(row, data)
            ):
                continue
            score = text_similarity(expected, actual)
            if score < self.matching_policy.approximate_text_threshold:
                continue
            evidence = (
                MatchEvidence(
                    policy_field,
                    "approximate",
                    score,
                    1.0,
                    f"opt-in approximate {self.spec.entity_name} policy",
                    expected,
                    actual,
                ),
            )
            results.append(
                MatchResult(
                    row_id,
                    score,
                    f"opt-in approximate {self.spec.entity_name} identity",
                    candidate=row,
                    evidence=evidence,
                )
            )
        return tuple(results)

    def candidates(
        self,
        candidate: MetadataCandidate,
        *,
        limit: int = 20,
        use_policy: bool = False,
    ) -> Sequence[MatchResult]:
        """Return exact candidates, with approximate policy explicitly opt-in.

        :param candidate: Candidate entity metadata.
        :param limit: Maximum candidates to return.
        :param use_policy: Permit approximate matching after exact matching fails.
        :return: Deterministically ranked possible matches.
        """

        if limit < 0:
            raise ValueError("limit cannot be negative")
        exact_results, terminal = self._exact_results(candidate)
        if terminal is not None or exact_results:
            results = exact_results
        elif use_policy:
            results = self._policy_results(candidate)
        else:
            results = ()
        return tuple(
            sorted(
                results,
                key=lambda result: (-result.confidence, result.entity_id or -1),
            )[:limit]
        )

    def best(
        self,
        candidate: MetadataCandidate,
        *,
        use_policy: bool = False,
    ) -> MatchResult:
        """Return an exact-default identity decision.

        :param candidate: Candidate entity metadata.
        :param use_policy: Permit approximate matching after exact matching fails.
        :return: Match, no-match, ambiguity, or conflict decision.
        """

        exact_results, terminal = self._exact_results(candidate)
        if terminal is not None:
            return terminal
        if exact_results:
            return decide_best(
                exact_results,
                subject=self.spec.entity_name,
                policy=self.matching_policy,
            )
        if not use_policy:
            return MatchResult(
                None,
                0.0,
                f"no exact {self.spec.entity_name} match",
                decision="no_match",
            )
        return decide_best(
            self._policy_results(candidate),
            subject=self.spec.entity_name,
            policy=self.matching_policy,
        )

    def exact(
        self,
        value: object,
        **scope: object,
    ) -> MatchResult:
        """Match one scalar value across the entity's declared scalar fields.

        :param value: Scalar identity value.
        :param scope: Optional public scope aliases, such as ``parent_id``.
        :return: Exact match, no-match, or ambiguity decision.
        """

        scope_data = self.repository.normalise_input(scope, ignore_unknown=False)
        for field_name, default in self.spec.scope_defaults.items():
            scope_data.setdefault(field_name, default)
        if not self._scope_is_complete(scope_data):
            return MatchResult(
                None,
                0.0,
                f"{self.spec.entity_name} matching requires explicit scope",
                decision="no_match",
            )
        results: list[MatchResult] = []
        for row in self.repository._all_rows():
            if not self._scope_matches(row, scope_data):
                continue
            matched_fields = tuple(
                field_name
                for field_name in self.spec.scalar_fields
                if row.get(field_name) is not None
                and _exact_equal(self.spec, field_name, value, row[field_name])
            )
            row_id = row.get(self.spec.id_column)
            if not matched_fields or not isinstance(row_id, int):
                continue
            results.append(
                MatchResult(
                    row_id,
                    1.0,
                    f"exact normalized {self.spec.entity_name} value",
                    matched_on=matched_fields,
                    candidate=row,
                    evidence=tuple(
                        MatchEvidence(
                            field_name,
                            "exact",
                            1.0,
                            1.0,
                            f"exact normalized {self.spec.entity_name} value",
                            value,
                            row[field_name],
                            decisive=True,
                        )
                        for field_name in matched_fields
                    ),
                )
            )
        return decide_best(
            results,
            subject=self.spec.entity_name,
            policy=self.matching_policy,
        )


__all__ = ["ExactEntityMatcher", "ExactEntitySpec"]
